//! CTP tick → 1 分钟 K线聚合的纯状态机（仅 CTP 使用，不放通用层）。
//!
//! K线聚合口径：
//! - **按交易日归桶，夜盘归下一交易日**：直接采用 CTP 报文中的 `TradingDay`
//!   （柜台已把 21:00 后的夜盘算入下一交易日），本状态机不重复实现夜盘日历；
//! - **成交量必须差分**：CTP 给的是当日累计量，bar.volume = 本桶末累计量 −
//!   上一桶末累计量；累计额同理；持仓量取桶末值；
//! - 跨交易日累计量归零 → 重置差分基线；同桶内累计量回退（乱序/重发）差分钳到 0；
//! - 每个合约一个 [`CandleAggregator`]（由 `bar_agg.rs` 按 symbol 各持一份），
//!   换月即换实例，互不影响。
//!
//! 本类型**不挂钟、不 IO、不 await**：桶归属与定稿完全是输入 tick 的纯函数，
//! 因此无需注入 Clock 即可确定性单测。

use rust_decimal::Decimal;

use crate::market::{Kline, KlineInterval};

/// 一分钟的毫秒数（测试构造 1m 桶/边界用；生产周期来自 `KlineInterval::duration_ms()`）。
#[cfg(test)]
const MINUTE_MS: i64 = 60_000;

/// 聚合器输入：一笔已映射好的 tick（B4 由 FFI 快照转换）。
#[derive(Debug, Clone, Copy)]
pub struct CandleTick {
    /// 归属交易日（YYYYMMDD；CTP TradingDay，夜盘已归次日）。
    pub trading_day: u32,
    /// 交易所时间（ActionDay+UpdateTime+UpdateMillisec 合成的 UTC 毫秒）。
    pub update_ms: i64,
    /// 最新价。
    pub price: Decimal,
    /// 当日累计成交量（手）。
    pub cum_volume: i64,
    /// 当日累计成交额。
    pub cum_turnover: Decimal,
    /// 当前总持仓量。
    pub open_interest: Decimal,
}

/// 把时间戳向下取整到所属周期桶的绝对起点（含）：`floor(ms / interval_ms)`。
///
/// 用 [`i64::div_euclid`] 保证 epoch 之前的负时间戳也向负无穷归桶（边界严格每
/// `interval_ms` 一个），周期边界由此函数按 `interval_ms` 派生，绝不写死 60_000。
fn floor_interval(ms: i64, interval_ms: i64) -> i64 {
    ms.div_euclid(interval_ms) * interval_ms
}

/// 累计量差分（非负钳制：乱序/重发/日切归零都不应产生负成交量）。
fn volume_delta(curr: i64, base: i64) -> i64 {
    (curr - base).max(0)
}

/// 累计额差分（非负）。
fn turnover_delta(curr: Decimal, base: Decimal) -> Decimal {
    (curr - base).max(Decimal::ZERO)
}

/// 一根尚未定稿的桶内中间状态。
struct OpenCandle {
    trading_day: u32,
    start_ms: i64,
    /// 桶周期（定稿 end_ms/`Kline.interval` 都由它派生，不写死 1 分钟）。
    interval: KlineInterval,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    /// 本桶起点的累计量基线（= 上一桶最后一笔 tick 的累计量；跨日重置为首笔值）。
    base_volume: i64,
    /// 本桶起点的累计额基线。
    base_turnover: Decimal,
    /// 上一笔 tick 的累计量（翻转时作为下一桶基线）。
    last_cum_volume: i64,
    /// 上一笔 tick 的累计额。
    last_cum_turnover: Decimal,
    /// 桶内成交量（累计量差分，非负）。
    volume: i64,
    /// 桶内成交额（差分，非负）。
    turnover: Decimal,
    /// 桶末持仓量（每笔覆盖）。
    open_interest: Decimal,
}

impl OpenCandle {
    /// 用新桶的第一笔 tick 建立中间状态；首笔自身相对基线的差分计入桶量。
    fn first(
        t: &CandleTick,
        start_ms: i64,
        interval: KlineInterval,
        base_volume: i64,
        base_turnover: Decimal,
    ) -> Self {
        Self {
            trading_day: t.trading_day,
            start_ms,
            interval,
            open: t.price,
            high: t.price,
            low: t.price,
            close: t.price,
            base_volume,
            base_turnover,
            last_cum_volume: t.cum_volume,
            last_cum_turnover: t.cum_turnover,
            volume: volume_delta(t.cum_volume, base_volume),
            turnover: turnover_delta(t.cum_turnover, base_turnover),
            open_interest: t.open_interest,
        }
    }

    /// 同桶内追加一笔：更新 OHLC/持仓量，并以「当前累计 − 桶基线」重算桶内差分。
    fn fold(&mut self, t: &CandleTick) {
        self.close = t.price;
        self.high = self.high.max(t.price);
        self.low = self.low.min(t.price);
        self.volume = volume_delta(t.cum_volume, self.base_volume);
        self.turnover = turnover_delta(t.cum_turnover, self.base_turnover);
        self.open_interest = t.open_interest;
        self.last_cum_volume = t.cum_volume;
        self.last_cum_turnover = t.cum_turnover;
    }

    /// 定稿为不可变 [`Kline`]（结束时间为半开区间 [start, start+interval)）。
    fn finalize(&self, venue: &str, symbol: &str) -> Kline {
        Kline {
            venue: venue.to_string(),
            symbol: symbol.to_string(),
            interval: self.interval,
            trading_day: self.trading_day,
            start_ms: self.start_ms,
            end_ms: self.start_ms + self.interval.duration_ms(),
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            turnover: self.turnover,
            open_interest: self.open_interest,
        }
    }
}

/// 单合约周期聚合器（本期仅 1 分钟；周期由 [`KlineInterval`] 参数化）。
pub struct CandleAggregator {
    /// 交易所标识（写入 Kline.venue）。
    venue: String,
    /// 合约代码（如实写入 Kline.symbol，如 cu2512）。
    symbol: String,
    /// 桶周期（归桶边界/end_ms/`Kline.interval` 都由它派生；本期 M1，为 M30 留口）。
    interval: KlineInterval,
    open: Option<OpenCandle>,
    /// 最近一次已定稿桶的（交易日, 绝对起点）；用于丢弃定稿后到达的迟到 tick。
    last_finalized: Option<(u32, i64)>,
    /// 最近一笔已并入 tick 的差分基线 `(交易日, 当日累计量, 当日累计额)`。**独立于在途桶
    /// 存活**：墙钟 take 走桶后仍保留，使同日下一桶（含跨空档首笔）正确差分、量守恒；
    /// 交易日变化时按本笔重置。`None` = 尚无 tick。
    baseline: Option<(u32, i64, Decimal)>,
}

impl CandleAggregator {
    pub fn new(
        venue: impl Into<String>,
        symbol: impl Into<String>,
        interval: KlineInterval,
    ) -> Self {
        Self {
            venue: venue.into(),
            symbol: symbol.into(),
            interval,
            open: None,
            last_finalized: None,
            baseline: None,
        }
    }

    /// 该 tick 是否为「已定稿桶的迟到 tick」：同交易日且桶起点不晚于最近定稿起点。
    /// 按 (交易日, 起点) 双键判定，跨交易日即便 ms 数值更小也不算迟到（见跨日测试）。
    fn is_late(&self, trading_day: u32, start: i64) -> bool {
        matches!(self.last_finalized, Some((d, s)) if d == trading_day && start <= s)
    }

    /// 记录某桶已定稿（事件时间墙钟 / poll 拍墙钟 / flush 收口都经此，防重复出棒）。
    fn mark_finalized(&mut self, trading_day: u32, start: i64) {
        self.last_finalized = Some((trading_day, start));
    }

    /// 丢弃 tick 的统一 debug 日志（迟到 / 乱序两路共用），不改动任何状态。
    fn trace_drop(&self, t: &CandleTick, start: i64, reason: &str) {
        tracing::debug!(
            venue = %self.venue,
            symbol = %self.symbol,
            trading_day = t.trading_day,
            tick_ms = t.update_ms,
            bucket_start = start,
            "丢弃 CTP tick：{reason}"
        );
    }

    /// 喂入一笔 tick。**定稿唯一由墙钟判定**（grace=0，到 `start+interval` 边界即定稿）：
    /// 1. 先以本笔事件时间 [`Self::finalize_if_due`] 收口到边界的旧桶；若在途桶属上一交易日
    ///    （trading_day 独立于分钟墙钟、跨日 ms 不可比）再按日切收口——两处都是 take，最多出一根；
    /// 2. 迟到 tick（所属 `(交易日,起点)` 已定稿）丢弃并 `debug!`，不重开/不并入；
    /// 3. 同桶 fold，否则开新桶。
    ///
    /// 差分基线独立存在聚合器外层 `baseline`（见 [`Self::open_candle`]），故墙钟 take 走旧桶后
    /// 跨空档首笔的增量仍正确并入、量守恒。返回本笔触发收口的定稿 [`Kline`]（否则 `None`）。
    pub fn on_tick(&mut self, t: &CandleTick) -> Option<Kline> {
        let start = floor_interval(t.update_ms, self.interval.duration_ms());
        // 1) 事件时间墙钟收口；在途桶属上一交易日则再按日切收口。
        let mut finished = self.finalize_if_due(t.update_ms);
        if matches!(&self.open, Some(c) if c.trading_day != t.trading_day) {
            finished = self.take_and_finalize().or(finished);
        }
        // 2) 迟到防护：收口后本笔仍落在已定稿桶 → 丢弃（刚收口的更早桶定稿照常上抛）。
        if self.is_late(t.trading_day, start) {
            self.trace_drop(t, start, "迟到：所属桶已墙钟定稿");
            return finished;
        }
        // 2b) 乱序防护：同交易日、tick 桶起点早于**当前在途桶**（介于最近定稿桶与在途桶之间的
        // 空窗乱序/重连快照）。它无桶可并，丢弃并保留在途桶与基线，绝不用旧桶覆盖在途桶。
        if matches!(&self.open, Some(c) if c.trading_day == t.trading_day && start < c.start_ms) {
            self.trace_drop(t, start, "乱序：早于当前在途桶");
            return finished;
        }
        // 3) 同桶 fold；否则（首笔/墙钟已收口/跨日，在途必为空）开新桶。
        match self.open.take() {
            Some(mut c) if c.trading_day == t.trading_day && c.start_ms == start => {
                c.fold(t);
                self.open = Some(c);
            }
            _ => self.open = Some(self.open_candle(t, start)),
        }
        // 推进外层差分基线（独立于在途桶存活，墙钟 take 不清它）。
        self.baseline = Some((t.trading_day, t.cum_volume, t.cum_turnover));
        finished
    }

    /// 开新桶并定差分基线：同交易日接上一笔累计量（该笔可能属于已被墙钟 take 的旧桶，
    /// 故基线存聚合器外层而非桶内）；跨交易日或当日首笔则累计归零、基线取本笔。
    fn open_candle(&self, t: &CandleTick, start: i64) -> OpenCandle {
        let (base_volume, base_turnover) = match self.baseline {
            Some((day, vol, ref turn)) if day == t.trading_day => (vol, *turn),
            _ => (t.cum_volume, t.cum_turnover),
        };
        OpenCandle::first(t, start, self.interval, base_volume, base_turnover)
    }

    /// `take` 出在途桶，登记定稿点并定稿为 [`Kline`]；无桶返回 `None`。
    /// 只动 `open`/`last_finalized`，**不清外层 `baseline`**，跨空档下一桶据此续算差分。
    fn take_and_finalize(&mut self) -> Option<Kline> {
        self.open.take().map(|c| {
            self.mark_finalized(c.trading_day, c.start_ms);
            c.finalize(&self.venue, &self.symbol)
        })
    }

    /// 墙钟到边界定稿（grace=0）：存在在途桶且 `start + interval <= now_ms` 即收口。
    /// 两个调用点共用此唯一判定——md 线程每拍的空闲 sweep（`now_ms`=SystemTime）与
    /// `on_tick` 的事件时间收口（`now_ms`=tick 时间）。未到点或无桶返回 `None`；
    /// 同一桶最多定稿一次（take 语义）。
    pub fn finalize_if_due(&mut self, now_ms: i64) -> Option<Kline> {
        // 无在途桶直接 None；借用在语句末结束（i64 Copy），随后可安全 take。
        let due_at = self.open.as_ref()?.start_ms + self.interval.duration_ms();
        if now_ms < due_at {
            return None;
        }
        self.take_and_finalize()
    }

    /// 强制定稿当前未闭桶（日切/停机/测试用）；无桶返回 `None`，并清空当前桶。
    pub fn flush(&mut self) -> Option<Kline> {
        self.take_and_finalize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(v: i64) -> Decimal {
        Decimal::from(v)
    }

    fn tick(day: u32, ms: i64, price: i64, vol: i64, turnover: i64, oi: i64) -> CandleTick {
        CandleTick {
            trading_day: day,
            update_ms: ms,
            price: Decimal::from(price),
            cum_volume: vol,
            cum_turnover: Decimal::from(turnover),
            open_interest: Decimal::from(oi),
        }
    }

    // 同一分钟两笔：不开新 bar，OHLC 在桶内累积。
    #[test]
    fn ticks_within_same_minute_update_ohlc_no_finalize() {
        let mut agg = CandleAggregator::new("ctp", "cu2512", KlineInterval::M1);
        // 10:00:00.000 与 10:00:30.000（同桶起点 600_000）。
        assert!(agg
            .on_tick(&tick(20261023, 600_000, 100, 10, 1000, 5))
            .is_none());
        assert!(agg
            .on_tick(&tick(20261023, 630_000, 110, 15, 1600, 7))
            .is_none());
        // flush 得到当前桶：高低收与差分正确。
        let bar = agg.flush().expect("当前桶");
        assert_eq!(bar.venue, "ctp");
        assert_eq!(bar.symbol, "cu2512");
        assert_eq!(bar.trading_day, 20261023);
        assert_eq!(bar.start_ms, 600_000);
        assert_eq!(bar.end_ms, 660_000);
        assert_eq!(bar.open, dec(100));
        assert_eq!(bar.high, dec(110));
        assert_eq!(bar.low, dec(100));
        assert_eq!(bar.close, dec(110));
        // 量 = 桶末累计 15 − 首笔基线 10 = 5；额 = 1600−1000 = 600；持仓取桶末 7。
        assert_eq!(bar.volume, 5);
        assert_eq!(bar.turnover, dec(600));
        assert_eq!(bar.open_interest, dec(7));
    }

    // 跨分钟：第二分钟首笔触发上一分钟定稿，量为分钟末累计差分。
    #[test]
    fn minute_flip_finalizes_previous_with_volume_diff() {
        let mut agg = CandleAggregator::new("ctp", "rb2601", KlineInterval::M1);
        // 第一分钟 10:00 两笔：累计 10→15。
        agg.on_tick(&tick(20261023, 600_000, 3800, 10, 100, 1));
        agg.on_tick(&tick(20261023, 630_000, 3801, 15, 160, 2));
        // 第二分钟 10:01 首笔，累计 15→17：定稿上一桶。
        let finished = agg
            .on_tick(&tick(20261023, 660_000, 3802, 17, 200, 3))
            .expect("上一分钟定稿");
        assert_eq!(finished.start_ms, 600_000);
        assert_eq!(finished.end_ms, 660_000);
        // 上一分钟量 = 15 − 10 = 5（不是累计 15）。
        assert_eq!(finished.volume, 5);
        assert_eq!(finished.turnover, dec(60));
        assert_eq!(finished.close, dec(3801));
        assert_eq!(finished.open_interest, dec(2));
        // 当前新桶再 flush：基线=上桶末累计 15，量 = 17−15 = 2。
        let cur = agg.flush().expect("新桶");
        assert_eq!(cur.start_ms, 660_000);
        assert_eq!(cur.volume, 2);
        assert_eq!(cur.turnover, dec(40));
    }

    // 夜盘归次日：TradingDay 由柜台给次日，21:00 与 21:01 同交易日，分钟桶正常翻转。
    #[test]
    fn night_session_uses_counter_trading_day() {
        let mut agg = CandleAggregator::new("ctp", "ag2512", KlineInterval::M1);
        // 周一 21:00:00 / 21:00:30 同分钟，累计 100→108，TradingDay 已为周二 20261027。
        agg.on_tick(&tick(20261027, 75_600_000, 8000, 100, 1000, 10));
        agg.on_tick(&tick(20261027, 75_630_000, 8004, 108, 1090, 11));
        // 21:01 首笔翻转，定稿的夜盘第一根 bar 归次日交易日。
        let night = agg
            .on_tick(&tick(20261027, 75_660_000, 8005, 112, 1150, 12))
            .expect("夜盘上一分钟定稿");
        assert_eq!(night.trading_day, 20261027);
        assert_eq!(night.start_ms, 75_600_000);
        assert_eq!(night.volume, 8);
        assert_eq!(night.turnover, dec(90));
        assert_eq!(night.open_interest, dec(11));
    }

    // 跨交易日：累计量归零，新交易日首桶基线重置，不出现负量，定稿旧桶不被污染。
    #[test]
    fn trading_day_change_resets_cumulative_baseline() {
        let mut agg = CandleAggregator::new("ctp", "cu2512", KlineInterval::M1);
        // 前一日 14:59 累计到 50000。
        agg.on_tick(&tick(20261023, 53_940_000, 70_000, 50_000, 9_999, 100));
        // 次日 09:00 累计重新从小计数（CTP 日初归零/集合竞价），trading_day 变化。
        let finished = agg
            .on_tick(&tick(20261024, 32_400_000, 70_100, 2, 3, 101))
            .expect("前一日桶定稿");
        assert_eq!(finished.trading_day, 20261023);
        assert_eq!(finished.volume, 0); // 前一桶只有首笔，量 0
                                        // 新桶再来一笔累计 5 → 基线已重置为 2，量 3，绝不为负。
        agg.on_tick(&tick(20261024, 32_430_000, 70_110, 5, 9, 102));
        let cur = agg.flush().expect("新交易日桶");
        assert_eq!(cur.trading_day, 20261024);
        assert_eq!(cur.volume, 3);
        assert_eq!(cur.turnover, dec(6));
    }

    // 同桶累计量回退（乱序/重发）：差分钳到 0，不产生负成交量。
    #[test]
    fn same_bucket_cumulative_regression_clamps_to_zero() {
        let mut agg = CandleAggregator::new("ctp", "rb2601", KlineInterval::M1);
        agg.on_tick(&tick(20261023, 600_000, 3800, 100, 1000, 1));
        agg.on_tick(&tick(20261023, 620_000, 3801, 90, 900, 1));
        let bar = agg.flush().unwrap();
        assert_eq!(bar.volume, 0);
        assert_eq!(bar.turnover, dec(0));
    }

    // 最低价在下跌中更新。
    #[test]
    fn high_low_track_extremes() {
        let mut agg = CandleAggregator::new("ctp", "cu2512", KlineInterval::M1);
        agg.on_tick(&tick(20261023, 600_000, 100, 0, 0, 0));
        agg.on_tick(&tick(20261023, 605_000, 99, 0, 0, 0));
        agg.on_tick(&tick(20261023, 610_000, 101, 0, 0, 0));
        let bar = agg.flush().unwrap();
        assert_eq!(bar.low, dec(99));
        assert_eq!(bar.high, dec(101));
        assert_eq!(bar.interval, KlineInterval::M1);
    }

    #[test]
    fn flush_empty_is_none() {
        let mut agg = CandleAggregator::new("ctp", "x", KlineInterval::M1);
        assert!(agg.flush().is_none());
    }

    #[test]
    fn floor_minute_buckets_correctly() {
        assert_eq!(floor_interval(0, MINUTE_MS), 0);
        assert_eq!(floor_interval(59_999, MINUTE_MS), 0);
        assert_eq!(floor_interval(60_000, MINUTE_MS), 60_000);
        assert_eq!(floor_interval(61_234, MINUTE_MS), 60_000);
    }

    // 绝对边界必须按 interval 参数算、不写死 60_000：用 30m（1_800_000ms）验证
    // 对齐到每半小时间的绝对 epoch 边界（:00/:30），为未来 KlineInterval::M30 留口。
    #[test]
    fn floor_interval_aligns_arbitrary_interval_to_absolute_epoch_edge() {
        const M30_MS: i64 = 30 * 60_000;
        // 00:29:59.999 → 00:00；00:30:00.000 → 00:30；00:45:10.234 → 00:30。
        assert_eq!(floor_interval(30 * 60_000 - 1, M30_MS), 0);
        assert_eq!(floor_interval(M30_MS, M30_MS), M30_MS);
        assert_eq!(floor_interval(M30_MS + 910_234, M30_MS), M30_MS);
        // 跨小时：01:15:00 → 01:00（= 2 * M30）。
        assert_eq!(floor_interval(75 * 60_000, M30_MS), 60 * 60_000);
        // 边界序列严格每 interval 一个：边界后任意偏移都落回本边界。
        for k in 0..4 {
            let edge = k * M30_MS;
            assert_eq!(floor_interval(edge + 123_456, M30_MS), edge);
        }
    }

    // 特征钉死（方案①·事件时间归属）：稀疏 tick 跨分钟缺口时，缺口期空分钟不出 bar，
    // 增量随「周期结束后第一根 tick」归入其所在桶。样例：22/23/27/29，cum 30/60/80/90/100。
    #[test]
    fn sparse_gap_ticks_attribute_volume_by_event_time() {
        const DAY: u32 = 20261027;
        // 分钟:秒 → 当日毫秒（与 update_ms 同坐标系）。
        let m = |mm: i64, sec: i64| mm * 60_000 + sec * 1_000;
        let mut agg = CandleAggregator::new("ctp", "cu2701", KlineInterval::M1);
        let mut finished = Vec::new();

        // t1 22:59 cum=30（当日首观测点，无基线）。
        assert!(agg.on_tick(&tick(DAY, m(22, 59), 1000, 30, 0, 1)).is_none());
        // t2 23:10 cum=60 → 定稿 22 桶。
        if let Some(b) = agg.on_tick(&tick(DAY, m(23, 10), 1010, 60, 0, 2)) {
            finished.push(b);
        }
        // t3 27:05 cum=80 → 定稿 23 桶（跨过 24/25/26，空分钟不出 bar）。
        if let Some(b) = agg.on_tick(&tick(DAY, m(27, 5), 1030, 80, 0, 3)) {
            finished.push(b);
        }
        // t4 27:58 cum=90 同桶 fold，不产 bar。
        assert!(agg.on_tick(&tick(DAY, m(27, 58), 1025, 90, 0, 4)).is_none());
        // t5 29:03 cum=100 → 定稿 27 桶（跨过 28）。
        if let Some(b) = agg.on_tick(&tick(DAY, m(29, 3), 1050, 100, 0, 5)) {
            finished.push(b);
        }

        // on_tick 仅定稿 3 根：22 / 23 / 27。
        assert_eq!(finished.len(), 3);
        let starts: Vec<i64> = finished.iter().map(|b| b.start_ms).collect();
        assert_eq!(starts, vec![m(22, 0), m(23, 0), m(27, 0)]);

        // 22 桶：单笔 1000，当日首观测点差分记 0，持仓取桶末 1。
        let b22 = &finished[0];
        assert_eq!(b22.open, dec(1000));
        assert_eq!(b22.high, dec(1000));
        assert_eq!(b22.low, dec(1000));
        assert_eq!(b22.close, dec(1000));
        assert_eq!(b22.volume, 0);
        assert_eq!(b22.open_interest, dec(1));

        // 23 桶：单笔 1010，量 = 60−30 = 30，持仓 2。
        let b23 = &finished[1];
        assert_eq!(b23.open, dec(1010));
        assert_eq!(b23.volume, 30);
        assert_eq!(b23.open_interest, dec(2));

        // 27 桶：O=1030（t3），H=1030，L=C=1025（t4），量 = 90−60 = 30，持仓取桶末 4。
        let b27 = &finished[2];
        assert_eq!(b27.open, dec(1030));
        assert_eq!(b27.high, dec(1030));
        assert_eq!(b27.low, dec(1025));
        assert_eq!(b27.close, dec(1025));
        assert_eq!(b27.volume, 30);
        assert_eq!(b27.open_interest, dec(4));

        // 缺口空分钟 24/25/26/28 绝不出 bar。
        assert!(finished
            .iter()
            .all(|b| ![m(24, 0), m(25, 0), m(26, 0), m(28, 0)].contains(&b.start_ms)));

        // flush 收口最后活跃桶 29：单笔 1050，量 = 100−90 = 10，持仓 5。
        let b29 = agg.flush().expect("最后活跃桶");
        assert_eq!(b29.start_ms, m(29, 0));
        assert_eq!(b29.open, dec(1050));
        assert_eq!(b29.volume, 10);
        assert_eq!(b29.open_interest, dec(5));

        // 总量守恒：0+30+30+10 = 70 = 末累计 100 − 首观测点基线 30。
        let total: i64 = finished.iter().map(|b| b.volume).sum::<i64>() + b29.volume;
        assert_eq!(total, 70);
        // flush 后再 flush 无桶。
        assert!(agg.flush().is_none());
    }

    // 成交额与成交量同口径：按累计额差分（非负），跨分钟缺口同样归入恢复桶。
    #[test]
    fn sparse_gap_turnover_uses_cumulative_delta() {
        const DAY: u32 = 20261027;
        let m = |mm: i64, sec: i64| mm * 60_000 + sec * 1_000;
        let mut agg = CandleAggregator::new("ctp", "cu2701", KlineInterval::M1);
        agg.on_tick(&tick(DAY, m(23, 10), 1010, 60, 600_000, 2));
        // 跨过 24/25/26，27:05 累计额 800_000：定稿 23 桶，额 = 600_000 − 首笔基线 0。
        let b23 = agg
            .on_tick(&tick(DAY, m(27, 5), 1030, 80, 800_000, 3))
            .expect("23 桶定稿");
        // 23 桶首笔基线为其自身累计额，故差分 0。
        assert_eq!(b23.turnover, dec(0));
        // 27 桶再来一笔累计额 900_000，flush 收口：额 = 900_000 − 600_000 = 300_000。
        agg.on_tick(&tick(DAY, m(27, 58), 1025, 90, 900_000, 4));
        let b27 = agg.flush().expect("27 桶");
        assert_eq!(b27.turnover, dec(300_000));
    }

    // 跨桶首笔累计量回退（乱序/重发导致比上一桶末累计还小）：新桶差分钳 0，不为负。
    #[test]
    fn cross_bucket_cumulative_regression_clamps_to_zero() {
        const DAY: u32 = 20261027;
        let m = |mm: i64, sec: i64| mm * 60_000 + sec * 1_000;
        let mut agg = CandleAggregator::new("ctp", "rb2601", KlineInterval::M1);
        agg.on_tick(&tick(DAY, m(10, 0), 3800, 100, 1_000, 1));
        // 下一分钟首笔累计量异常回退到 90（< 上一桶末 100）：先定稿旧桶。
        let prev = agg
            .on_tick(&tick(DAY, m(11, 0), 3799, 90, 900, 1))
            .expect("上一桶定稿");
        assert_eq!(prev.volume, 0); // 上一桶仅首笔，差分 0
                                    // 新桶基线=100，本笔 90 回退 → 钳 0，绝不出现负成交量。
        let cur = agg.flush().expect("新桶");
        assert_eq!(cur.volume, 0);
        assert_eq!(cur.turnover, dec(0));
    }

    // ===== 墙钟到点定稿（不再等下一周期首笔 tick）=====

    // 到 start+interval 墙钟边界即定稿（grace=0）：finalize_if_due 出上一桶，OHLC/量差分正确。
    #[test]
    fn wallclock_finalize_emits_bucket_at_boundary() {
        let mut agg = CandleAggregator::new("ctp", "cu2701", KlineInterval::M1);
        agg.on_tick(&tick(20261027, 600_000, 100, 10, 1000, 1));
        agg.on_tick(&tick(20261027, 630_000, 110, 15, 1600, 2));
        // 收盘边界 660_000 之前不定稿。
        assert!(agg.finalize_if_due(659_999).is_none());
        // 到边界即定稿：无需等到 10:01 的 tick。
        let bar = agg
            .finalize_if_due(660_000)
            .expect("墙钟到边界应定稿 10:00 桶");
        assert_eq!(bar.start_ms, 600_000);
        assert_eq!(bar.end_ms, 660_000);
        assert_eq!(bar.interval, KlineInterval::M1);
        assert_eq!(bar.open, dec(100));
        assert_eq!(bar.high, dec(110));
        assert_eq!(bar.low, dec(100));
        assert_eq!(bar.close, dec(110));
        assert_eq!(bar.volume, 5);
        assert_eq!(bar.turnover, dec(600));
        assert_eq!(bar.open_interest, dec(2));
    }

    // 空窗（从无 tick 的合约/分钟）无聚合器桶 → 不产棒；take 语义下同拍/再拍不重复出。
    #[test]
    fn wallclock_empty_and_repeat_sweep_emit_nothing() {
        let mut agg = CandleAggregator::new("ctp", "idle", KlineInterval::M1);
        assert!(agg.finalize_if_due(10_000_000).is_none());
        agg.on_tick(&tick(20261027, 600_000, 100, 10, 0, 1));
        assert!(agg.finalize_if_due(660_000).is_some());
        assert!(agg.finalize_if_due(660_000).is_none());
        assert!(agg.finalize_if_due(999_999_999).is_none());
    }

    // 墙钟定稿后到达的「本桶迟到 tick」必须丢弃，绝不重开旧桶/重复出棒/并入量。
    #[test]
    fn wallclock_late_tick_after_sweep_is_dropped_without_reopen() {
        let mut agg = CandleAggregator::new("ctp", "cu2701", KlineInterval::M1);
        agg.on_tick(&tick(20261027, 600_000, 100, 10, 0, 1));
        agg.on_tick(&tick(20261027, 630_000, 101, 12, 0, 1));
        assert!(agg.finalize_if_due(660_000).is_some());
        // 时间戳仍属 10:00 桶（floor == 已定稿起点）：丢弃。
        assert!(agg
            .on_tick(&tick(20261027, 600_500, 99, 99, 0, 9))
            .is_none());
        assert!(agg.finalize_if_due(999_999_999).is_none());
        assert!(agg.flush().is_none());
    }

    // 定稿后下一周期的正常 tick 照常开新桶（迟到防护不得越界误杀）；基线在聚合器外层
    // 持久化，跨空档（sweep 已 take 旧桶）的累计增量并入新桶，总量守恒不丢。
    #[test]
    fn wallclock_then_gap_tick_attributes_increment_to_new_bucket() {
        let mut agg = CandleAggregator::new("ctp", "cu2701", KlineInterval::M1);
        agg.on_tick(&tick(20261027, 600_000, 100, 10, 0, 1));
        assert!(agg.finalize_if_due(660_000).is_some());
        // 10:01:30（floor 660_000 > 定稿 600_000）：开新桶，不产棒。
        assert!(agg
            .on_tick(&tick(20261027, 690_000, 102, 20, 0, 2))
            .is_none());
        let cur = agg.flush().expect("新周期桶");
        assert_eq!(cur.start_ms, 660_000);
        assert_eq!(cur.end_ms, 720_000);
        // 基线持久化为上一定稿桶末累计 10：空档+本桶首笔增量 20−10 = 10，不丢、不回填旧桶。
        assert_eq!(cur.volume, 10);
    }

    // 唯一墙钟定稿：新周期首笔 tick 自带事件时间已到边界 → on_tick 内嵌墙钟先收旧桶，
    // 再开新桶；与 poll 拍 sweep 对同一在途桶最多各出一次、绝不重复。
    #[test]
    fn tick_driven_and_poll_driven_wallclock_finalize_at_most_once() {
        let mut agg = CandleAggregator::new("ctp", "rb2601", KlineInterval::M1);
        agg.on_tick(&tick(20261027, 600_000, 3800, 10, 0, 1));
        // 10:01 首笔（事件时间 660_000 恰好到边界）：内嵌墙钟定稿 10:00（首桶量 0）。
        let finished = agg
            .on_tick(&tick(20261027, 660_000, 3801, 12, 0, 2))
            .expect("tick 驱动墙钟定稿 10:00");
        assert_eq!(finished.start_ms, 600_000);
        // poll 拍再 sweep：在途已是 10:01 桶（未到 720_000），10:00 不会再出。
        assert!(agg.finalize_if_due(660_000).is_none());
        // 10:01 到边界 sweep 只出 10:01：基线接上一桶末累计 10，量 = 12−10 = 2。
        let swept = agg.finalize_if_due(720_000).expect("墙钟定稿 10:01");
        assert_eq!(swept.start_ms, 660_000);
        assert_eq!(swept.volume, 2);
        assert!(agg.finalize_if_due(999_999_999).is_none());
    }

    // 真机回归（jm2701 2026-10-28）：sweep 定稿 → 跨 31 分钟空档 → 新桶首笔，
    // 空档成交（34→40，6 手）必须并入新桶而非丢失。
    #[test]
    fn wallclock_long_gap_conserves_cumulative_volume() {
        let mut agg = CandleAggregator::new("ctp", "jm2701", KlineInterval::M1);
        let m = |mm: i64, sec: i64| mm * 60_000 + sec * 1_000;
        // 13:52:51 累计 34（13:52 桶，分钟按当日 0 点起算：13:52=832m）。
        agg.on_tick(&tick(20261028, m(832, 51), 1378, 34, 0, 1883));
        // 13:53 边界墙钟定稿 13:52 桶。
        let b52 = agg.finalize_if_due(m(833, 0)).expect("13:52 定稿");
        assert_eq!(b52.start_ms, m(832, 0));
        // 31 分钟后 14:24:24 首笔，累计跳到 40：开 14:24 桶，不产棒。
        assert!(agg
            .on_tick(&tick(20261028, m(864, 24), 1390, 40, 0, 1884))
            .is_none());
        // 14:25 边界定稿 14:24 桶：量 = 40−34 = 6（空档成交不丢）。
        let b24 = agg.finalize_if_due(m(865, 0)).expect("14:24 定稿");
        assert_eq!(b24.start_ms, m(864, 0));
        assert_eq!(b24.volume, 6);
    }

    // 迟到防护按 (交易日, 起点) 判定：跨交易日即便 ms 数值更小也必须放行并重置基线。
    #[test]
    fn wallclock_sweep_then_new_trading_day_tick_is_accepted() {
        let mut agg = CandleAggregator::new("ctp", "cu2512", KlineInterval::M1);
        // 前一交易日 14:59 桶墙钟定稿（grace=0：到 start+interval 即收口）。
        agg.on_tick(&tick(20261023, 53_940_000, 70_000, 50_000, 0, 100));
        let d1 = agg
            .finalize_if_due(53_940_000 + 60_000)
            .expect("前日桶定稿");
        assert_eq!(d1.trading_day, 20261023);
        // 次日 09:00（日相对 ms 32_400_000 < 前日 53_940_000；trading_day 不同）：放行。
        assert!(agg
            .on_tick(&tick(20261024, 32_400_000, 70_100, 2, 0, 101))
            .is_none());
        let d2 = agg.flush().expect("新交易日桶");
        assert_eq!(d2.trading_day, 20261024);
        assert_eq!(d2.start_ms, 32_400_000);
        assert_eq!(d2.volume, 0);
    }

    // 乱序 tick（floor 落在「最近定稿桶之后、当前在途桶之前」的空窗）必须丢弃，
    // 绝不能用更早的桶覆盖当前在途桶（否则在途桶连同其累计量被静默顶丢）。
    #[test]
    fn out_of_order_gap_tick_is_dropped_without_clobbering_open_bucket() {
        let mut agg = CandleAggregator::new("ctp", "rb2601", KlineInterval::M1);
        // 10:00 桶并墙钟定稿。
        agg.on_tick(&tick(20261027, 600_000, 3800, 10, 0, 1));
        assert_eq!(
            agg.finalize_if_due(660_000).map(|b| b.start_ms),
            Some(600_000)
        );
        // 10:05（900_000）正常 tick 开在途桶。
        assert!(agg
            .on_tick(&tick(20261027, 900_000, 3810, 20, 0, 2))
            .is_none());
        // 乱序来一笔 10:02（720_000，空窗）：不产棒，且 10:05 在途桶必须存活。
        assert!(agg
            .on_tick(&tick(20261027, 720_000, 3805, 15, 0, 2))
            .is_none());
        // 到 10:06 边界定稿的仍是 10:05 桶（未被 10:02 覆盖）。
        let b = agg
            .finalize_if_due(960_000)
            .expect("10:05 在途桶应完好并到点定稿");
        assert_eq!(b.start_ms, 900_000);
        // 量守恒：基线 10→20，乱序笔 15（更小，乱序）不计入 → 10:05 量 10。
        assert_eq!(b.volume, 10);
    }
}
