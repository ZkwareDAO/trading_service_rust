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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rust_decimal::Decimal;

use crate::ctp::candle::{CandleAggregator, CandleTick};
use crate::market::{BarSink, Kline};
use crate::market_hub::{safe_notify, NotifyOutcome};

/// 无条件 sink 单次回调摘除阈值（与 BarRouter 同口径：目标 <1µs，1ms 兜底）。
const PERSIST_BUDGET: Duration = Duration::from_millis(1);

/// K线集 tick 聚合器（单连接一份）。
pub struct BarAggSink {
    venue: String,
    /// 合约 → 聚合器（按出现的合约懒创建）。
    aggregators: HashMap<String, CandleAggregator>,
    /// 定稿 bar 的下游（BarRouter 按订阅意图 + 主力表路由给 K线消费者）。
    sink: Arc<dyn BarSink>,
    /// 无条件下游（如全量持久化 sink）：**绕过订阅/主力路由**，每根定稿 bar
    /// （含非主力、零订阅）都通知；坏/慢者经 safe_notify 隔离摘除，不牵连主下游。
    unconditional: Vec<Arc<dyn BarSink>>,
}

impl BarAggSink {
    pub fn new(venue: impl Into<String>, sink: Arc<dyn BarSink>) -> Self {
        Self {
            venue: venue.into(),
            aggregators: HashMap::new(),
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

    /// 喂入一笔 K线集 tick：更新对应合约聚合器，分钟翻转即把定稿 bar 交下游路由。
    /// 是否真正推给某消费者（品种订阅需主力）由下游 BarRouter 判定。
    /// 同路径的 TickTable/LastVolume 写入由 drain 调用点负责。
    pub fn on_tick(&mut self, symbol: &str, t: &CandleTick) {
        let venue = self.venue.clone();
        let agg = self
            .aggregators
            .entry(symbol.to_string())
            .or_insert_with(|| CandleAggregator::new(venue, symbol));
        if let Some(bar) = agg.on_tick(t) {
            tracing::debug!(
                symbol,
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
            // 先全量持久化（绕过路由），再交 Router 按订阅/主力分发。
            self.notify_unconditional(&bar);
            self.sink.on_bar(&bar);
        }
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
            self.notify_unconditional(bar);
            self.sink.on_bar(bar);
        }
        finished
    }
}

/// 把 CTP 行情快照字段映射为聚合输入（供 drain 调用；无 IO）。
pub fn to_candle_tick(
    trading_day: u32,
    update_ms: i64,
    price: Decimal,
    cum_volume: i64,
    cum_turnover: Decimal,
    open_interest: Decimal,
) -> CandleTick {
    CandleTick {
        trading_day,
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
        let mut agg = BarAggSink::new("ctp", sink.clone() as Arc<dyn BarSink>);

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
        let mut agg = BarAggSink::new("ctp", sink.clone() as Arc<dyn BarSink>);
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
        let mut agg = BarAggSink::new("ctp", router.clone() as Arc<dyn BarSink>);
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
        let mut agg = BarAggSink::new("ctp", router.clone() as Arc<dyn BarSink>);
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

    #[test]
    fn to_candle_tick_carries_fields() {
        let t = to_candle_tick(
            20261023,
            1,
            Decimal::from(3),
            4,
            Decimal::from(5),
            Decimal::from(6),
        );
        assert_eq!(t.trading_day, 20261023);
        assert_eq!(t.cum_volume, 4);
        assert_eq!(t.open_interest, Decimal::from(6));
    }
}
