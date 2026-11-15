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

use rust_decimal::Decimal;

use crate::ctp::candle::{CandleAggregator, CandleDiagnostics, CandleTick};
use crate::market::{BarSink, Kline, KlineInterval};
use crate::market_hub::UnconditionalSinkSet;

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
    /// （含非主力、零订阅）都通知；策略/边沿告警/panic 摘除与自愈统一由
    /// [`UnconditionalSinkSet`] 处理，不牵连主下游。
    unconditional: UnconditionalSinkSet,
    /// 已处理 tick 的最大事件时间（行情时钟水位）；0 = 尚无 tick。
    /// 定稿判定用它而非本机 SystemTime，避免行情/处理滞后时提前收口。
    watermark_ms: i64,
    /// 全局最早到期边界（各在途桶 `start + interval` 的最小值）；无在途桶 = `i64::MAX`。
    /// 水位 sweep 据此 O(1) 短路，把遍历从「每 tick」降到「每分钟边界一次」。
    next_due_ms: i64,
}

impl BarAggSink {
    pub fn new(venue: impl Into<String>, sink: Arc<dyn BarSink>, interval: KlineInterval) -> Self {
        Self {
            venue: venue.into(),
            interval,
            aggregators: BTreeMap::new(),
            sink,
            unconditional: UnconditionalSinkSet::new(),
            watermark_ms: 0,
            next_due_ms: i64::MAX,
        }
    }

    /// 注册本地普通无条件下游（LocalBounded：超时仅告警不摘除）。
    /// 须在喂 tick 前注册一次。
    pub fn add_unconditional_sink(&mut self, sink: Arc<dyn BarSink>) {
        self.unconditional.register(sink);
    }

    /// 注册全量持久化下游（LocalPersist：不计耗时、panic 后自愈、重复注册幂等）。
    /// 此后每根定稿 bar（on_tick 翻转与 flush_all/sweep 收口）都通知它，
    /// 不经订阅/主力过滤，用于「有 tick 定稿即全量落盘」。
    pub fn register_persistent_sink(&mut self, sink: Arc<dyn BarSink>) -> bool {
        self.unconditional.register_persistent(sink)
    }

    /// 定稿 bar 的统一双下游出口：先全量持久化（绕过订阅/主力路由），再交 Router 分发。
    /// 定稿（tick 事件时间触发 / 水位或空闲拍 sweep）与停机/日切 flush 都必须经此，
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
        // 策略/告警/摘除/自愈全在集合内部；慢持久化 sink 不再被误摘。
        self.unconditional.notify_all(bar);
        self.sink.on_bar(bar);
    }

    /// 喂入一笔 K线集 tick：更新对应合约聚合器，分钟翻转即把定稿 bar 交下游路由。
    /// 是否真正推给某消费者（品种订阅需主力）由下游 BarRouter 判定。
    /// 同路径的 TickTable/LastVolume 写入由 drain 调用点负责。
    pub fn on_tick(&mut self, symbol: &str, t: &CandleTick) {
        let venue = self.venue.clone();
        let interval = self.interval;
        // 水位 = 已处理 tick 的最大事件时间（行情时钟）；与 SystemTime 无关。
        self.watermark_ms = self.watermark_ms.max(t.update_ms);
        let (bar, due) = {
            let agg = self
                .aggregators
                .entry(symbol.to_string())
                .or_insert_with(|| CandleAggregator::new(venue, symbol, interval));
            // 事件时间本笔跨边界时的定稿走这里；其余静默合约由 sweep 按水位收口。
            (agg.on_tick(t), agg.next_due_ms())
        };
        if let Some(due) = due {
            self.next_due_ms = self.next_due_ms.min(due);
        }
        if let Some(bar) = bar {
            self.emit_finalized(&bar);
        }
    }

    /// 行情时间水位 sweep（md 线程每个事件后调用，O(1) 短路）：只有水位越过全局最早
    /// 到期边界时才遍历全部聚合器，把所有到点（`start + interval <= watermark`）的
    /// 在途桶收口。
    ///
    /// 与 [`Self::sweep_finalized`]（墙钟）的根本区别：判定用**已处理 tick 的最大事件
    /// 时间**，而不是本机 `SystemTime`。行情到达/处理滞后或本机时钟相对行情偏移时，
    /// 不会在桶尾 tick 尚未处理前提前定稿，因此不会产生旧实现的“每分钟只吃到前
    /// 20–40 秒”的截断。
    pub fn sweep_by_watermark(&mut self) -> Vec<Kline> {
        let watermark = self.watermark_ms;
        if watermark <= 0 || watermark < self.next_due_ms {
            return Vec::new();
        }
        self.sweep_due(watermark)
    }

    /// 统一 sweep 内核：定稿所有到点（`start + interval <= now_ms`）的在途桶，
    /// 刷新全局到期边界并通知下游。水位/空闲拍两条路径共用，保证定稿语义一致。
    fn sweep_due(&mut self, now_ms: i64) -> Vec<Kline> {
        let mut finished = Vec::new();
        let mut next_due = i64::MAX;
        for agg in self.aggregators.values_mut() {
            if let Some(bar) = agg.finalize_if_due(now_ms) {
                finished.push(bar);
            }
            if let Some(due) = agg.next_due_ms() {
                next_due = next_due.min(due);
            }
        }
        self.next_due_ms = next_due;
        for bar in &finished {
            self.emit_finalized(bar);
        }
        finished
    }

    /// 已处理 tick 的最大事件时间（行情时钟水位）；0 表示尚无 tick。
    pub fn watermark_ms(&self) -> i64 {
        self.watermark_ms
    }

    /// 读取并清零各合约聚合器的诊断计数（迟到/乱序/疑似丢快照，周期告警取增量）。
    pub fn take_diagnostics(&mut self) -> CandleDiagnostics {
        let mut total = CandleDiagnostics::default();
        for agg in self.aggregators.values_mut() {
            total.merge(agg.take_diagnostics());
        }
        total
    }

    /// 墙钟到边界 sweep（**仅空闲拍兜底**：md 线程在 `next_event` 超时、队列已空时调用，
    /// 用于收盘/静默收口最后一根；活跃行情请用 [`Self::sweep_by_watermark`]）：把所有
    /// **已收到过 tick** 且到点（`start + interval <= now_ms`）的合约上一周期桶一次性定稿。
    ///
    /// - 只遍历已存在聚合器的在途桶：没 tick 的合约/分钟无桶，天然跳过、不补空 bar；
    /// - BTreeMap 按符号有序定稿，多合约天然聚成一条有序 `kline_1m_batch`；
    /// - 先取出全部定稿 bar（结束聚合器借用）再做下游通知，避免自借用冲突；
    /// - 同一桶若已被 `on_tick` 的事件时间收口 take，则此处为 `None`，最多定稿一次。
    pub fn sweep_finalized(&mut self, now_ms: i64) -> Vec<Kline> {
        self.sweep_due(now_ms)
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
        // 全部在途桶已收口：水位 sweep 的到期边界回到“无”。
        self.next_due_ms = i64::MAX;
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
    use std::time::Duration;

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

    /// 延时 + 计数的 sink（持久化不测耗时的回归用）。
    struct SleepCountSink {
        delay: Duration,
        bars: Mutex<Vec<String>>,
    }
    impl SleepCountSink {
        fn new(delay: Duration) -> Arc<Self> {
            Arc::new(Self {
                delay,
                bars: Mutex::new(Vec::new()),
            })
        }
        fn count(&self) -> usize {
            self.bars.lock().unwrap().len()
        }
    }
    impl BarSink for SleepCountSink {
        fn on_bar(&self, bar: &Kline) {
            std::thread::sleep(self.delay);
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

    // ===== 持久化 sink（LocalPersist：不测耗时、重复注册幂等） =====

    #[test]
    fn finalized_bars_reach_persistent_sink_once() {
        let router = Arc::new(RecSink::default());
        let persist = Arc::new(RecSink::default());
        let mut agg = BarAggSink::new("ctp", router.clone() as Arc<dyn BarSink>, KlineInterval::M1);
        // 重复注册被幂等忽略：每根定稿 bar 只落一次。
        assert!(agg.register_persistent_sink(persist.clone() as Arc<dyn BarSink>));
        assert!(!agg.register_persistent_sink(persist.clone() as Arc<dyn BarSink>));

        agg.on_tick("cu2601", &tick(20261023, 600_000, 100, 10));
        agg.on_tick("cu2601", &tick(20261023, 660_000, 101, 11));

        assert_eq!(router.bars.lock().unwrap().clone(), vec!["cu2601"]);
        assert_eq!(persist.bars.lock().unwrap().clone(), vec!["cu2601"]);
    }

    /// 回归 2026-11-02 事故：持久化 sink 单次回调 >1ms 时曾被判 Slow 永久摘除，
    /// 导致其后所有 bar 不再落盘。LocalPersist 策略不测耗时，全部收到。
    #[test]
    fn slow_persistent_sink_still_receives_all_swept_bars() {
        let router = Arc::new(RecSink::default());
        let persist = SleepCountSink::new(Duration::from_millis(2));
        let mut agg = BarAggSink::new("ctp", router.clone() as Arc<dyn BarSink>, KlineInterval::M1);
        agg.register_persistent_sink(persist.clone() as Arc<dyn BarSink>);

        agg.on_tick("cu2701", &tick(20261027, 600_000, 100, 10));
        agg.on_tick("rb2701", &tick(20261027, 600_000, 200, 20));
        agg.on_tick("jm2701", &tick(20261027, 600_000, 300, 30));

        let bars = agg.sweep_finalized(660_000);
        assert_eq!(bars.len(), 3);
        // 每根回调都 >1ms，但一根都不丢。
        assert_eq!(persist.count(), 3);
        assert_eq!(router.bars.lock().unwrap().len(), 3);

        // 后续分钟继续定稿，持久化 sink 仍在（未被摘除）。
        agg.on_tick("cu2701", &tick(20261027, 660_000, 101, 11));
        assert_eq!(agg.sweep_finalized(720_000).len(), 1);
        assert_eq!(persist.count(), 4);
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

    // ===== 事件时间水位 sweep（替代墙钟定稿，防止滞后把桶尾巴截掉）=====

    #[test]
    fn watermark_sweep_does_not_close_before_boundary() {
        let sink = Arc::new(RecSink::default());
        let mut agg = BarAggSink::new("ctp", sink.clone() as Arc<dyn BarSink>, KlineInterval::M1);
        agg.on_tick("cu2612", &tick(20261023, 600_000, 100, 10));
        agg.on_tick("cu2612", &tick(20261023, 659_500, 101, 12));
        // 水位 659_500 未越过 660_000：即使本机墙钟早已过点也不得定稿。
        assert!(agg.sweep_by_watermark().is_empty());
        assert!(sink.bars.lock().unwrap().is_empty());
        assert_eq!(agg.watermark_ms(), 659_500);
    }

    #[test]
    fn watermark_crossing_finalizes_quiet_symbol_with_full_volume() {
        let sink = Arc::new(RecSink::default());
        let mut agg = BarAggSink::new("ctp", sink.clone() as Arc<dyn BarSink>, KlineInterval::M1);
        // cu 的桶在 10:00，末尾 tick 到 10:00:59.5（完整一分钟）。
        agg.on_tick("cu2612", &tick(20261023, 600_000, 100, 10));
        agg.on_tick("cu2612", &tick(20261023, 659_500, 101, 20));
        // 另一合约跨过边界，把全局水位推到 660_000，cu 桶由 sweep 收口。
        agg.on_tick("rb2701", &tick(20261023, 660_000, 3800, 5));
        let bars = agg.sweep_by_watermark();
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].symbol, "cu2612");
        assert_eq!(bars[0].start_ms, 600_000);
        // 完整一分钟的量：20 − 首笔基线 10 = 10（未被墙钟提前收口截掉尾段）。
        assert_eq!(bars[0].volume, 10);
        assert_eq!(sink.bars.lock().unwrap().clone(), vec!["cu2612"]);
    }

    #[test]
    fn watermark_sweep_is_noop_without_ticks() {
        let sink = Arc::new(RecSink::default());
        let mut agg = BarAggSink::new("ctp", sink.clone() as Arc<dyn BarSink>, KlineInterval::M1);
        assert!(agg.sweep_by_watermark().is_empty());
        assert_eq!(agg.watermark_ms(), 0);
    }

    #[test]
    fn late_tick_after_watermark_finalize_is_counted() {
        let sink = Arc::new(RecSink::default());
        let mut agg = BarAggSink::new("ctp", sink.clone() as Arc<dyn BarSink>, KlineInterval::M1);
        agg.on_tick("cu2612", &tick(20261023, 600_000, 100, 10));
        agg.on_tick("cu2612", &tick(20261023, 660_000, 101, 11)); // 事件时间收口 10:00
        agg.on_tick("cu2612", &tick(20261023, 600_500, 99, 12)); // 定稿后的迟到 tick
        let d = agg.take_diagnostics();
        assert_eq!(d.late_dropped, 1);
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

    /// 真实 tick 回放（默认忽略；需本地数据）：
    /// `CTP_TICK_CSV=... CTP_EXPECT_CSV=... cargo test -p trading-exchange --features ctp --lib \
    ///   ctp::bar_agg::tests::replay_real_ticks_against_expected_bars -- --ignored --nocapture`
    ///
    /// 期望 CSV 由独立口径（事件时间聚合 oracle）生成；逐 bar 比对 7 个数值字段，
    /// 用于验证线上水位定稿链路与交易所口径一致。
    #[test]
    #[ignore = "需要 CTP_TICK_CSV / CTP_EXPECT_CSV 本地数据"]
    fn replay_real_ticks_against_expected_bars() {
        struct Collect {
            bars: Mutex<Vec<Kline>>,
        }
        impl BarSink for Collect {
            fn on_bar(&self, bar: &Kline) {
                self.bars.lock().unwrap().push(bar.clone());
            }
        }

        let tick_path = std::env::var("CTP_TICK_CSV").expect("CTP_TICK_CSV");
        let expect_path = std::env::var("CTP_EXPECT_CSV").expect("CTP_EXPECT_CSV");
        let begin: i64 = std::env::var("CTP_REPLAY_BEGIN_MS")
            .unwrap_or_else(|_| "1794531600000".into())
            .parse()
            .unwrap();
        let end: i64 = std::env::var("CTP_REPLAY_END_MS")
            .unwrap_or_else(|_| "1794535200000".into())
            .parse()
            .unwrap();

        let out = Arc::new(Collect {
            bars: Mutex::new(Vec::new()),
        });
        let mut agg = BarAggSink::new("ctp", out.clone() as Arc<dyn BarSink>, KlineInterval::M1);
        let text = std::fs::read_to_string(&tick_path).unwrap();
        for line in text.lines().skip(1) {
            let c: Vec<&str> = line.split(',').collect();
            if c.len() < 13 {
                continue;
            }
            let t = CandleTick {
                trading_day: c[4].parse().unwrap(),
                exchange_id: c[1].to_string(),
                update_ms: c[3].parse().unwrap(),
                price: c[5].parse().unwrap(),
                cum_volume: c[10].parse().unwrap(),
                cum_turnover: c[11].parse().unwrap(),
                open_interest: c[12].parse().unwrap(),
            };
            // 与 md 线程同型：每笔 tick 后按水位 sweep，停机 flush 收尾。
            agg.on_tick(c[2], &t);
            let _ = agg.sweep_by_watermark();
        }
        let _ = agg.flush_all();

        let got: Vec<Kline> = out
            .bars
            .lock()
            .unwrap()
            .iter()
            .filter(|b| begin <= b.start_ms && b.start_ms < end)
            .cloned()
            .collect();
        let expect = std::fs::read_to_string(&expect_path).unwrap();
        let exp: Vec<(&str, [&str; 7])> = expect
            .lines()
            .skip(1)
            .map(|l| l.split(',').collect::<Vec<_>>())
            .map(|c| (c[9], [c[3], c[4], c[5], c[6], c[7], c[8], c[12]]))
            .collect();
        assert_eq!(got.len(), exp.len(), "bar 数量不一致");
        for (b, (start, fields)) in got.iter().zip(&exp) {
            assert_eq!(b.start_ms.to_string(), *start);
            let oi = b
                .extras
                .get("open_interest")
                .and_then(|e| e.as_decimal())
                .unwrap();
            let mine = [
                b.open.to_string(),
                b.high.to_string(),
                b.low.to_string(),
                b.close.to_string(),
                b.volume.to_string(),
                b.quote_volume.to_string(),
                oi.to_string(),
            ];
            assert_eq!(mine, (*fields).map(str::to_string), "bar {start} 不一致");
        }
    }
}
