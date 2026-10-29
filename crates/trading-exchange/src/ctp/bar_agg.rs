//! CTP tick → bar 聚合 sink（K线集作用域）。
//!
//! 持有 `合约 → CandleAggregator`：每笔 K线集 tick 喂入对应聚合器，分钟翻转时产出
//! 定稿 [`Kline`]，**一律**经注入的 [`BarSink`]（即 [`crate::ctp::market_stream::BarRouter`]）
//! 上抛；本类型不做主力过滤。
//!
//! 「品种订阅只收主力 / 具体合约订阅直收」的路由判定在 `BarRouter::dispatch`：
//! 它掌握每个消费者的订阅符号，精确合约命中无条件放行，仅品种前缀命中才查主力表。
//! 聚合器对 K线集内所有合约（含非主力）一律聚合，使 tick→bar 采集不因过滤丢失。
//!
//! 本类型**不做主力选举**（选举在 [`crate::ctp::dominant`] + 收盘任务）。
//!
//! 纯内存、单写者（CTP drain 线程），方法同步廉价；TickTable 写入在 drain 调用点
//! 与本聚合同路径完成（见 B4 接线）。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rust_decimal::Decimal;

use crate::ctp::candle::{CandleAggregator, CandleTick};
use crate::market::{BarSink, Kline, KlineInterval};
use crate::market_hub::{safe_notify, NotifyOutcome};

/// 无条件 sink 单次回调摘除阈值（与 BarRouter 同口径：目标 <1µs，1ms 兜底）。
const PERSIST_BUDGET: Duration = Duration::from_millis(1);

/// K线集 tick 聚合器（单连接一份）。
pub struct BarAggSink {
    venue: String,
    /// K线周期（本期 M1；聚合器归桶与墙钟定稿边界都按它派生，不写死 60_000）。
    interval: KlineInterval,
    /// 合约 → 聚合器（按出现的合约懒创建）。用 BTreeMap 使 sweep/flush 定稿序确定
    /// （按符号有序），多合约定稿聚成一条有序 `kline_1m_batch`。
    aggregators: BTreeMap<String, CandleAggregator>,
    /// 定稿 bar 的下游（BarRouter 按订阅意图 + 主力表路由给 K线消费者）。
    sink: Arc<dyn BarSink>,
    /// 无条件下游（如全量持久化 sink）：**绕过订阅/主力路由**，每根定稿 bar
    /// （含非主力、零订阅）都通知；坏/慢者经 safe_notify 隔离摘除，不牵连主下游。
    unconditional: Vec<Arc<dyn BarSink>>,
}

impl BarAggSink {
    pub fn new(venue: impl Into<String>, sink: Arc<dyn BarSink>, interval: KlineInterval) -> Self {
        Self {
            venue: venue.into(),
            interval,
            aggregators: BTreeMap::new(),
            sink,
            unconditional: Vec::new(),
        }
    }

    /// 注册一个无条件下游：此后每根定稿 bar（on_tick 翻转与 flush_all 收口）都通知它，
    /// 不经订阅/主力过滤，用于「有 tick 定稿即全量落盘」。须在喂 tick 前注册一次。
    pub fn add_unconditional_sink(&mut self, sink: Arc<dyn BarSink>) {
        self.unconditional.push(sink);
    }

    /// 逐个隔离通知无条件 sink；panic/超时者摘除，绝不影响主下游与行情连接。
    fn notify_unconditional(&mut self, bar: &Kline) {
        let mut survivors = Vec::with_capacity(self.unconditional.len());
        for sink in self.unconditional.drain(..) {
            match safe_notify(sink.as_ref(), bar, PERSIST_BUDGET) {
                NotifyOutcome::Ok => survivors.push(sink),
                outcome => tracing::warn!(
                    ?outcome,
                    "无条件 bar sink 异常，已摘除（不影响其余持久化/路由）"
                ),
            }
        }
        self.unconditional = survivors;
    }

    /// 定稿 bar 的统一双下游出口：先全量持久化（绕过订阅/主力路由），再交 Router 分发。
    /// 墙钟定稿（tick 事件时间触发 / poll 拍 sweep）与停机/日切 flush 都必须经此，
    /// 保证每根 bar 恰好各一次、顺序一致，杜绝重复 append。
    fn emit_finalized(&mut self, bar: &Kline) {
        tracing::debug!(
            symbol = %bar.symbol,
            trading_day = bar.trading_day,
            start_ms = bar.start_ms,
            end_ms = bar.end_ms,
            open = %bar.open,
            high = %bar.high,
            low = %bar.low,
            close = %bar.close,
            volume = bar.volume,
            "CTP 1m K线定稿"
        );
        self.notify_unconditional(bar);
        self.sink.on_bar(bar);
    }

    /// 喂入一笔 K线集 tick：更新对应合约聚合器，分钟翻转即把定稿 bar 交下游路由。
    /// 是否真正推给某消费者（品种订阅需主力）由下游 BarRouter 判定。
    /// 同路径的 TickTable/LastVolume 写入由 drain 调用点负责。
    pub fn on_tick(&mut self, symbol: &str, t: &CandleTick) {
        let venue = self.venue.clone();
        let interval = self.interval;
        let agg = self
            .aggregators
            .entry(symbol.to_string())
            .or_insert_with(|| CandleAggregator::new(venue, symbol, interval));
        if let Some(bar) = agg.on_tick(t) {
            self.emit_finalized(&bar);
        }
    }

    /// 墙钟到边界 sweep（grace=0；md 线程每 poll 节拍调用，空闲超时拍也调）：把**所有已收到
    /// 过 tick** 且到点（`start + interval <= now_ms`）的合约上一周期桶一次性定稿。
    ///
    /// - 只遍历已存在聚合器的在途桶：没 tick 的合约/分钟无桶，天然跳过、不补空 bar；
    /// - BTreeMap 按符号有序定稿，多合约天然聚成一条有序 `kline_1m_batch`；
    /// - 先取出全部定稿 bar（结束聚合器借用）再做下游通知，避免自借用冲突；
    /// - 同一桶若已被 `on_tick` 的事件时间墙钟 take，则此处为 `None`，最多定稿一次。
    pub fn sweep_finalized(&mut self, now_ms: i64) -> Vec<Kline> {
        let mut finished = Vec::new();
        for agg in self.aggregators.values_mut() {
            if let Some(bar) = agg.finalize_if_due(now_ms) {
                finished.push(bar);
            }
        }
        for bar in &finished {
            self.emit_finalized(bar);
        }
        finished
    }

    /// 强制定稿所有未闭桶（日切/停机/测试）：返回全部定稿 bar，先全量持久化再交路由。
    pub fn flush_all(&mut self) -> Vec<Kline> {
        // 先只取出定稿 bar（聚合器借用于此结束），再做下游通知，避免自借用冲突。
        let mut finished = Vec::new();
        for agg in self.aggregators.values_mut() {
            if let Some(bar) = agg.flush() {
                finished.push(bar);
            }
        }
        for bar in &finished {
            self.emit_finalized(bar);
        }
        finished
    }
}

/// 把 CTP 行情快照字段映射为聚合输入（供 drain 调用；无 IO）。
///
/// `exchange_id` 为真实交易所代码（SHFE/DCE/…，取自 FFI ExchangeID），随聚合定稿进
/// bar extras；其余量额为当日累计口径，差分在 candle 内完成。
pub fn to_candle_tick(
    trading_day: u32,
    exchange_id: impl Into<String>,
    update_ms: i64,
    price: Decimal,
    cum_volume: i64,
    cum_turnover: Decimal,
    open_interest: Decimal,
) -> CandleTick {
    CandleTick {
        trading_day,
        exchange_id: exchange_id.into(),
        update_ms,
        price,
        cum_volume,
        cum_turnover,
        open_interest,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// 记录收到 bar 的 sink。
    #[derive(Default)]
    struct RecSink {
        bars: Mutex<Vec<String>>,
    }
    impl BarSink for RecSink {
        fn on_bar(&self, bar: &Kline) {
            self.bars.lock().unwrap().push(bar.symbol.clone());
        }
    }

    fn tick(day: u32, ms: i64, price: i64, vol: i64) -> CandleTick {
        CandleTick {
            trading_day: day,
            exchange_id: "SHFE".into(),
            update_ms: ms,
            price: Decimal::from(price),
            cum_volume: vol,
            cum_turnover: Decimal::ZERO,
            open_interest: Decimal::ZERO,
        }
    }

    #[test]
    fn finalized_bar_emitted_for_every_contract_without_dominant_filter() {
        // 聚合层不再懂主力：任何 K线集合约跨分钟定稿都上抛下游（过滤在 BarRouter）。
        let sink = Arc::new(RecSink::default());
        let mut agg = BarAggSink::new("ctp", sink.clone() as Arc<dyn BarSink>, KlineInterval::M1);

        // 两个合约（含一个"非主力"角色的 cu2601）各跨一分钟：都应上抛。
        agg.on_tick("cu2601", &tick(20261023, 600_000, 100, 10));
        agg.on_tick("cu2601", &tick(20261023, 660_000, 101, 11));
        agg.on_tick("cu2512", &tick(20261023, 600_000, 200, 20));
        agg.on_tick("cu2512", &tick(20261023, 660_000, 201, 25));

        let got = sink.bars.lock().unwrap().clone();
        assert_eq!(got, vec!["cu2601".to_string(), "cu2512".to_string()]);
    }

    #[test]
    fn no_bar_before_minute_rollover() {
        // 同一分钟内多笔不定稿，下游收不到。
        let sink = Arc::new(RecSink::default());
        let mut agg = BarAggSink::new("ctp", sink.clone() as Arc<dyn BarSink>, KlineInterval::M1);
        agg.on_tick("cu2512", &tick(20261023, 600_000, 100, 10));
        agg.on_tick("cu2512", &tick(20261023, 630_000, 101, 11));
        assert!(sink.bars.lock().unwrap().is_empty());
    }

    /// panic 的 sink（验证无条件 sink 隔离/摘除）。
    struct PanicSink;
    impl BarSink for PanicSink {
        fn on_bar(&self, _bar: &Kline) {
            panic!("boom");
        }
    }

    #[test]
    fn unconditional_sink_receives_every_finalized_bar() {
        // 主下游（Router 角色）与无条件持久化 sink 各自记录。
        let router = Arc::new(RecSink::default());
        let persist = Arc::new(RecSink::default());
        let mut agg = BarAggSink::new("ctp", router.clone() as Arc<dyn BarSink>, KlineInterval::M1);
        agg.add_unconditional_sink(persist.clone() as Arc<dyn BarSink>);

        // 非主力合约跨分钟定稿：无条件 sink 与主下游都收到（持久化不依赖 Router 客户端）。
        agg.on_tick("cu2601", &tick(20261023, 600_000, 100, 10));
        agg.on_tick("cu2601", &tick(20261023, 660_000, 101, 11));
        assert_eq!(router.bars.lock().unwrap().clone(), vec!["cu2601"]);
        assert_eq!(persist.bars.lock().unwrap().clone(), vec!["cu2601"]);

        // flush_all 收口所有未闭桶：cu2601 在 660_000 开的新桶与 rb2601 首桶都定稿，
        // 且收口 bar 同样送无条件 sink。
        agg.on_tick("rb2601", &tick(20261023, 600_000, 200, 20));
        let flushed = agg.flush_all();
        // HashMap 迭代顺序不定，按符号多重集断言：cu2601（660_000 开的新桶）+ rb2601 首桶。
        let mut flushed_syms: Vec<&str> = flushed.iter().map(|b| b.symbol.as_str()).collect();
        flushed_syms.sort_unstable();
        assert_eq!(flushed_syms, vec!["cu2601", "rb2601"]);
        // 持久化 sink 累计收到 3 根（与顺序无关）：跨分钟 cu2601 + flush 的 cu2601/rb2601。
        let mut got: Vec<String> = persist.bars.lock().unwrap().clone();
        got.sort();
        assert_eq!(got, vec!["cu2601", "cu2601", "rb2601"]);
    }

    #[test]
    fn panicky_unconditional_sink_is_evicted_without_blocking_others() {
        let router = Arc::new(RecSink::default());
        let good = Arc::new(RecSink::default());
        let mut agg = BarAggSink::new("ctp", router.clone() as Arc<dyn BarSink>, KlineInterval::M1);
        // 一个必 panic 的持久化 sink + 一个正常 sink。
        agg.add_unconditional_sink(Arc::new(PanicSink));
        agg.add_unconditional_sink(good.clone() as Arc<dyn BarSink>);

        // 跨分钟定稿：panic sink 被吞住并摘除，正常 sink 与主下游不受影响、逐根收到。
        agg.on_tick("cu2512", &tick(20261023, 600_000, 100, 10));
        agg.on_tick("cu2512", &tick(20261023, 660_000, 101, 11));
        agg.on_tick("cu2512", &tick(20261023, 720_000, 102, 12));
        agg.on_tick("cu2512", &tick(20261023, 780_000, 103, 13));

        // 3 次跨分钟定稿（660/720/780 各一根）。
        assert_eq!(router.bars.lock().unwrap().len(), 3);
        assert_eq!(good.bars.lock().unwrap().len(), 3);
    }

    // ===== 墙钟 sweep_finalized（多合约一次性到点定稿）=====

    // 多合约同分钟到点：一次 sweep 全部定稿，按符号有序返回，无需等下一周期 tick。
    #[test]
    fn sweep_finalizes_all_due_contracts_in_symbol_order() {
        let sink = Arc::new(RecSink::default());
        let mut agg = BarAggSink::new("ctp", sink.clone() as Arc<dyn BarSink>, KlineInterval::M1);
        agg.on_tick("cu2701", &tick(20261027, 600_000, 100, 10));
        agg.on_tick("rb2701", &tick(20261027, 600_000, 200, 20));
        agg.on_tick("jm2701", &tick(20261027, 600_000, 300, 30));
        // 未到收盘边界（660_000）不出棒：659_999 仍属同一桶。
        assert!(agg.sweep_finalized(659_999).is_empty());
        let bars = agg.sweep_finalized(660_000);
        // BTreeMap → 严格按符号有序。
        assert_eq!(
            bars.iter().map(|b| b.symbol.as_str()).collect::<Vec<_>>(),
            vec!["cu2701", "jm2701", "rb2701"]
        );
        assert!(bars
            .iter()
            .all(|b| b.start_ms == 600_000 && b.end_ms == 660_000));
        // 同一拍/再拍不重复定稿（take 语义）。
        assert!(agg.sweep_finalized(660_000).is_empty());
        assert!(agg.sweep_finalized(999_999_999).is_empty());
    }

    // sweep 出的每根 bar 恰好一次进 unconditional（落盘）与 routed（主下游），顺序一致。
    #[test]
    fn sweep_routes_each_bar_once_to_unconditional_and_router() {
        let router = Arc::new(RecSink::default());
        let persist = Arc::new(RecSink::default());
        let mut agg = BarAggSink::new("ctp", router.clone() as Arc<dyn BarSink>, KlineInterval::M1);
        agg.add_unconditional_sink(persist.clone() as Arc<dyn BarSink>);
        agg.on_tick("cu2701", &tick(20261027, 600_000, 100, 10));
        agg.on_tick("rb2701", &tick(20261027, 600_000, 200, 20));
        let bars = agg.sweep_finalized(660_000);
        assert_eq!(bars.len(), 2);
        // 双下游都按 BTreeMap 有序、各收一次。
        assert_eq!(
            router.bars.lock().unwrap().clone(),
            vec!["cu2701".to_string(), "rb2701".to_string()]
        );
        assert_eq!(
            persist.bars.lock().unwrap().clone(),
            vec!["cu2701".to_string(), "rb2701".to_string()]
        );
    }

    // 无 tick 合约不产棒；sweep 不影响尚未到边界的新桶。
    #[test]
    fn sweep_with_no_aggregators_or_not_due_emits_nothing() {
        let sink = Arc::new(RecSink::default());
        let mut agg = BarAggSink::new("ctp", sink.clone() as Arc<dyn BarSink>, KlineInterval::M1);
        // 从无任何 tick。
        assert!(agg.sweep_finalized(999_999_999).is_empty());
        // 一桶已定稿、另一桶属更晚周期、尚未到边界：只出到期的。
        agg.on_tick("cu2701", &tick(20261027, 600_000, 100, 10));
        assert_eq!(agg.sweep_finalized(660_000).len(), 1);
        agg.on_tick("rb2701", &tick(20261027, 720_000, 200, 20));
        // rb 桶 720_000，到 781_000 才定稿；780_999 sweep 不出。
        assert!(agg.sweep_finalized(779_999).is_empty());
        assert_eq!(agg.sweep_finalized(780_000).len(), 1);
        assert!(sink.bars.lock().unwrap().len() == 2);
    }

    // sweep 路径上 unconditional sink panic 同样被隔离摘除，不阻塞主下游与其余 bar。
    #[test]
    fn panicky_unconditional_sink_is_evicted_on_sweep_path() {
        let router = Arc::new(RecSink::default());
        let good = Arc::new(RecSink::default());
        let mut agg = BarAggSink::new("ctp", router.clone() as Arc<dyn BarSink>, KlineInterval::M1);
        agg.add_unconditional_sink(Arc::new(PanicSink));
        agg.add_unconditional_sink(good.clone() as Arc<dyn BarSink>);
        agg.on_tick("cu2701", &tick(20261027, 600_000, 100, 10));
        agg.on_tick("rb2701", &tick(20261027, 600_000, 200, 20));
        // 不 panic 即隔离生效；主下游与好 sink 各收 2 根（有序）。
        let bars = agg.sweep_finalized(660_000);
        assert_eq!(bars.len(), 2);
        assert_eq!(router.bars.lock().unwrap().len(), 2);
        assert_eq!(good.bars.lock().unwrap().len(), 2);
    }

    #[test]
    fn to_candle_tick_carries_fields() {
        let t = to_candle_tick(
            20261023,
            "DCE",
            1,
            Decimal::from(3),
            4,
            Decimal::from(5),
            Decimal::from(6),
        );
        assert_eq!(t.trading_day, 20261023);
        assert_eq!(t.exchange_id, "DCE");
        assert_eq!(t.cum_volume, 4);
        assert_eq!(t.open_interest, Decimal::from(6));
    }
}
