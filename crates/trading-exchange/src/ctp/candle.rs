//! CTP tick → 1 分钟 K线聚合的纯状态机（仅 CTP 使用，不放通用层）。
//!
//! 设计口径（见 `docs/internal/k线服务涉及.md` §K线聚合口径）：
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

/// 一分钟的毫秒数。
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

/// 把时间戳向下取整到所属分钟桶的起点（含）。
fn floor_minute(ms: i64) -> i64 {
    ms.div_euclid(MINUTE_MS) * MINUTE_MS
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
    fn first(t: &CandleTick, start_ms: i64, base_volume: i64, base_turnover: Decimal) -> Self {
        Self {
            trading_day: t.trading_day,
            start_ms,
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

    /// 定稿为不可变 [`Kline`]（结束时间为半开区间 [start, start+1m)）。
    fn finalize(&self, venue: &str, symbol: &str) -> Kline {
        Kline {
            venue: venue.to_string(),
            symbol: symbol.to_string(),
            interval: KlineInterval::M1,
            trading_day: self.trading_day,
            start_ms: self.start_ms,
            end_ms: self.start_ms + MINUTE_MS,
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

/// 单合约 1 分钟聚合器。
pub struct CandleAggregator {
    /// 交易所标识（写入 Kline.venue）。
    venue: String,
    /// 合约代码（如实写入 Kline.symbol，如 cu2512）。
    symbol: String,
    open: Option<OpenCandle>,
}

impl CandleAggregator {
    pub fn new(venue: impl Into<String>, symbol: impl Into<String>) -> Self {
        Self {
            venue: venue.into(),
            symbol: symbol.into(),
            open: None,
        }
    }

    /// 喂入一笔 tick：
    /// - 同桶：更新 OHLC/差分，返回 `None`；
    /// - 换桶/换日：定稿并返回**上一桶**的 [`Kline`]，内部开新桶；
    /// - 首笔：仅开桶，返回 `None`。
    pub fn on_tick(&mut self, t: &CandleTick) -> Option<Kline> {
        let start = floor_minute(t.update_ms);
        match self.open.take() {
            // 首笔：基线取本笔累计量，故首笔自身差分记 0。
            None => {
                self.open = Some(OpenCandle::first(t, start, t.cum_volume, t.cum_turnover));
                None
            }
            Some(mut c) => {
                if c.trading_day == t.trading_day && c.start_ms == start {
                    // 同交易日同分钟：桶内累积。
                    c.fold(t);
                    self.open = Some(c);
                    None
                } else {
                    // 换分钟桶或跨交易日：先定稿旧桶。
                    let finished = c.finalize(&self.venue, &self.symbol);
                    // 新桶基线：同日换桶接旧桶末累计；跨交易日累计归零，基线取本笔。
                    let (base_volume, base_turnover) = if c.trading_day == t.trading_day {
                        (c.last_cum_volume, c.last_cum_turnover)
                    } else {
                        (t.cum_volume, t.cum_turnover)
                    };
                    self.open = Some(OpenCandle::first(t, start, base_volume, base_turnover));
                    Some(finished)
                }
            }
        }
    }

    /// 强制定稿当前未闭桶（日切/停机/测试用）；无桶返回 `None`，并清空当前桶。
    pub fn flush(&mut self) -> Option<Kline> {
        self.open
            .take()
            .map(|c| c.finalize(&self.venue, &self.symbol))
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
        let mut agg = CandleAggregator::new("ctp", "cu2512");
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
        let mut agg = CandleAggregator::new("ctp", "rb2601");
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
        let mut agg = CandleAggregator::new("ctp", "ag2512");
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
        let mut agg = CandleAggregator::new("ctp", "cu2512");
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
        let mut agg = CandleAggregator::new("ctp", "rb2601");
        agg.on_tick(&tick(20261023, 600_000, 3800, 100, 1000, 1));
        agg.on_tick(&tick(20261023, 620_000, 3801, 90, 900, 1));
        let bar = agg.flush().unwrap();
        assert_eq!(bar.volume, 0);
        assert_eq!(bar.turnover, dec(0));
    }

    // 最低价在下跌中更新。
    #[test]
    fn high_low_track_extremes() {
        let mut agg = CandleAggregator::new("ctp", "cu2512");
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
        let mut agg = CandleAggregator::new("ctp", "x");
        assert!(agg.flush().is_none());
    }

    #[test]
    fn floor_minute_buckets_correctly() {
        assert_eq!(floor_minute(0), 0);
        assert_eq!(floor_minute(59_999), 0);
        assert_eq!(floor_minute(60_000), 60_000);
        assert_eq!(floor_minute(61_234), 60_000);
    }
}
