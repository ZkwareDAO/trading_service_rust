//! 行情快照：柜台 `CThostFtdcDepthMarketDataField` 的安全副本。

/// 一笔行情 tick（盘口一档 + 最新价 + 成交量）。
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
}

#[cfg(test)]
mod tests {
    use super::TickSnapshot;

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
}
