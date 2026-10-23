//! 行情快照：柜台 `CThostFtdcDepthMarketDataField` 的安全副本。

/// 一笔行情 tick（盘口一档 + 最新价 + 成交量 + K线聚合所需字段）。
#[derive(Debug, Clone)]
pub struct TickSnapshot {
    /// 合约代码。
    pub instrument: String,
    /// 最新价。
    pub last_price: f64,
    /// 买一价。
    pub bid_price: f64,
    /// 买一量（手）。
    pub bid_volume: i32,
    /// 卖一价。
    pub ask_price: f64,
    /// 卖一量（手）。
    pub ask_volume: i32,
    /// 当日累计成交量（手）。
    pub volume: i32,
    /// 行情时间（`HH:MM:SS`）。
    pub update_time: String,
    /// 行情毫秒部分。
    pub update_millisec: i32,
    /// 交易日（`YYYYMMDD`；CTP TradingDay，夜盘已归下一交易日）。
    pub trading_day: String,
    /// 业务/动作日期（`YYYYMMDD`；实际发生的自然日，夜盘为当日）。
    pub action_day: String,
    /// 当日累计成交额。
    pub turnover: f64,
    /// 当前总持仓量。
    pub open_interest: f64,
}

impl TickSnapshot {
    /// 买卖一价差；盘口缺一侧时返回 `None`。
    pub fn spread(&self) -> Option<f64> {
        let bid_ok = self.bid_price > 1e-9;
        let ask_ok = self.ask_price > 1e-9 && self.ask_price < f64::MAX;
        match (bid_ok, ask_ok) {
            (true, true) => Some(self.ask_price - self.bid_price),
            _ => None,
        }
    }

    /// 归属交易日（YYYYMMDD → u32）；非法为 0。
    pub fn trading_day_u32(&self) -> u32 {
        self.trading_day.trim().parse().unwrap_or(0)
    }

    /// 交易所时间（ActionDay+UpdateTime+毫秒，北京时区）合成的 UTC 毫秒时间戳。
    /// 任一字段非法返回 None。
    pub fn update_utc_ms(&self) -> Option<i64> {
        md_timestamp_utc_ms(&self.action_day, &self.update_time, self.update_millisec)
    }
}

/// 北京时区（UTC+8）的 `YYYYMMDD` + `HH:MM:SS` + 毫秒 → UTC 毫秒时间戳。
///
/// CTP 行情时间均为柜台本地北京时区；ActionDay 是实际自然日（夜盘为当日，
/// 与已归次日的 TradingDay 区分）。纯函数便于确定性单测。
pub fn md_timestamp_utc_ms(action_day: &str, update_time: &str, millisec: i32) -> Option<i64> {
    let day = action_day.trim();
    let t = update_time.trim();
    if day.len() != 8 || t.len() != 8 {
        return None;
    }
    let year: i32 = day.get(0..4)?.parse().ok()?;
    let month: u32 = day.get(4..6)?.parse().ok()?;
    let dom: u32 = day.get(6..8)?.parse().ok()?;
    let hour: u32 = t.get(0..2)?.parse().ok()?;
    let minute: u32 = t.get(3..5)?.parse().ok()?;
    let second: u32 = t.get(6..8)?.parse().ok()?;
    use chrono::{FixedOffset, TimeZone};
    // 直接用北京固定时区 UTC+8 构造 civil time，timestamp_millis() 即对应 UTC 毫秒。
    let beijing = FixedOffset::east_opt(8 * 3600)?;
    let ms = beijing
        .with_ymd_and_hms(year, month, dom, hour, minute, second)
        .single()?
        .timestamp_millis();
    Some(ms + millisec as i64)
}

#[cfg(test)]
mod tests {
    use super::{md_timestamp_utc_ms, TickSnapshot};

    fn tick(bid: f64, ask: f64) -> TickSnapshot {
        TickSnapshot {
            instrument: "rb2611".into(),
            last_price: 3800.0,
            bid_price: bid,
            bid_volume: 10,
            ask_price: ask,
            ask_volume: 5,
            volume: 1000,
            update_time: "13:30:01".into(),
            update_millisec: 500,
            trading_day: "20261023".into(),
            action_day: "20261023".into(),
            turnover: 0.0,
            open_interest: 0.0,
        }
    }

    #[test]
    fn spread_is_ask_minus_bid() {
        assert_eq!(tick(3799.0, 3801.0).spread(), Some(2.0));
    }

    #[test]
    fn spread_is_none_when_bid_missing() {
        // 柜台以 f64::MAX（或 DBL_MAX）表示无对手价；CTP 实际用最大值填充。
        assert_eq!(tick(0.0, 3801.0).spread(), None);
    }

    #[test]
    fn spread_is_none_when_ask_is_max_sentinel() {
        assert_eq!(tick(3799.0, f64::MAX).spread(), None);
    }

    #[test]
    fn beijing_civil_time_to_utc_ms() {
        use chrono::{FixedOffset, TimeZone};
        let expect_ms = |y, mo, d, h, mi, s, milli: i64| {
            FixedOffset::east_opt(8 * 3600)
                .unwrap()
                .with_ymd_and_hms(y, mo, d, h, mi, s)
                .single()
                .unwrap()
                .timestamp_millis()
                + milli
        };
        // 北京 2026-10-23 09:30:00.500。
        assert_eq!(
            md_timestamp_utc_ms("20261023", "09:30:00", 500),
            Some(expect_ms(2026, 10, 23, 9, 30, 0, 500))
        );
        // 夜盘 ActionDay 当日 21:00（北京）。
        assert_eq!(
            md_timestamp_utc_ms("20261023", "21:00:00", 0),
            Some(expect_ms(2026, 10, 23, 21, 0, 0, 0))
        );
        // 北京 00:00 → UTC 前一日 16:00，比北京 08:00 正好早 8h（验证时区偏移）。
        let midnight = md_timestamp_utc_ms("20261023", "00:00:00", 0).unwrap();
        let eight = md_timestamp_utc_ms("20261023", "08:00:00", 0).unwrap();
        assert_eq!(eight - midnight, 8 * 3_600_000);
    }

    #[test]
    fn bad_day_or_time_is_none() {
        assert!(md_timestamp_utc_ms("20261", "09:30:00", 0).is_none());
        assert!(md_timestamp_utc_ms("20261023", "9:30", 0).is_none());
        assert!(md_timestamp_utc_ms("xxxxxxxx", "09:30:00", 0).is_none());
    }

    #[test]
    fn snapshot_day_and_timestamp_helpers() {
        let t = tick(1.0, 2.0);
        assert_eq!(t.trading_day_u32(), 20261023);
        assert!(t.update_utc_ms().is_some());
    }
}
