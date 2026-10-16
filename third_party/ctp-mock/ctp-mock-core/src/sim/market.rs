//! 行情纯内核：确定性游走报价 + `CThostFtdcDepthMarketDataField` 帧构造。
//!
//! 不做订单簿：脚本/自动模式给出 LastPrice 与一档买卖价即可。自动模式保证
//! `bid < last < ask` 且 `last > 0`，使订阅侧仓位持续重估、浮盈可见变化；
//! 缺侧用 `f64::MAX`（对齐 trading-exchange `market.rs` 的无对手价判定）。

use ctp2rs::v1alpha1::CThostFtdcDepthMarketDataField;

use crate::fields::put_fixed;

/// 无对手价时的占位（与服务端行情判定一致）。
pub const NO_PRICE: f64 = f64::MAX;

/// 一帧报价（与 bindgen 解耦的领域形态）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Quote {
    pub last: f64,
    pub bid: f64,
    pub ask: f64,
    pub bid_volume: i32,
    pub ask_volume: i32,
    pub volume: i32,
}

impl Quote {
    /// 显式构造一帧（脚本精确推价用）。
    pub fn exact(last: f64, bid: f64, ask: f64) -> Self {
        Self {
            last,
            bid,
            ask,
            bid_volume: 10,
            ask_volume: 10,
            volume: 100,
        }
    }

    /// 造缺侧：买侧或卖侧置 [`NO_PRICE`]。
    pub fn missing_bid(last: f64, ask: f64) -> Self {
        Self {
            last,
            bid: NO_PRICE,
            ask,
            bid_volume: 0,
            ask_volume: 10,
            volume: 100,
        }
    }
    pub fn missing_ask(last: f64, bid: f64) -> Self {
        Self {
            last,
            bid,
            ask: NO_PRICE,
            bid_volume: 10,
            ask_volume: 0,
            volume: 100,
        }
    }
}

/// 确定性自动游走器：在基准价附近按固定步长小幅来回，保证为正且 bid<last<ask。
#[derive(Debug, Clone)]
pub struct Walk {
    base: f64,
    tick_size: f64,
    n: u64,
}

/// 一个游走周期内相对基准的跳动（单位：tick），确定性且有正有负。
const AMP_STEPS: [i32; 8] = [0, 1, 2, 1, 0, -1, -2, -1];

impl Walk {
    pub fn new(base: f64, tick_size: f64) -> Self {
        Self {
            base,
            tick_size: tick_size.max(1.0),
            n: 0,
        }
    }

    /// 推进并返回下一帧报价（调用一次 = 一个 tick）。
    pub fn next_quote(&mut self) -> Quote {
        let amp = AMP_STEPS[(self.n as usize) % AMP_STEPS.len()];
        self.n += 1;
        let last = (self.base + amp as f64 * self.tick_size).max(self.tick_size);
        Quote {
            last,
            bid: last - self.tick_size,
            ask: last + self.tick_size,
            bid_volume: 10,
            ask_volume: 10,
            // 累计量单调递增，贴近真实行情。
            volume: 100 + self.n as i32,
        }
    }
}

/// 构造深度行情回报帧。
pub fn depth_frame(
    instrument: &str,
    exchange_id: &str,
    q: &Quote,
    trading_day: &str,
    hms: &str,
    millisec: i32,
) -> CThostFtdcDepthMarketDataField {
    let mut d = CThostFtdcDepthMarketDataField::default();
    put_fixed(&mut d.InstrumentID, instrument);
    put_fixed(&mut d.ExchangeID, exchange_id);
    put_fixed(&mut d.TradingDay, trading_day);
    put_fixed(&mut d.ActionDay, trading_day);
    put_fixed(&mut d.UpdateTime, hms);
    d.UpdateMillisec = millisec;
    d.LastPrice = q.last;
    d.BidPrice1 = q.bid;
    d.AskPrice1 = q.ask;
    d.BidVolume1 = q.bid_volume;
    d.AskVolume1 = q.ask_volume;
    d.Volume = q.volume;
    d.PreSettlementPrice = q.last;
    d.PreClosePrice = q.last;
    d.OpenPrice = q.last;
    d.HighestPrice = q.last;
    d.LowestPrice = q.last;
    d.ClosePrice = q.last;
    d.AveragePrice = q.last;
    d.UpperLimitPrice = q.last * 1.1;
    d.LowerLimitPrice = q.last * 0.9;
    d
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn walk_keeps_positive_and_bid_below_last_below_ask() {
        let mut w = Walk::new(3200.0, 1.0);
        for _ in 0..16 {
            let q = w.next_quote();
            assert!(q.last > 0.0);
            assert!(q.bid < q.last, "bid {} < last {}", q.bid, q.last);
            assert!(q.last < q.ask, "last {} < ask {}", q.last, q.ask);
        }
    }

    #[test]
    fn walk_is_deterministic_across_cycles() {
        let mut a = Walk::new(3200.0, 1.0);
        let mut b = Walk::new(3200.0, 1.0);
        for _ in 0..8 {
            assert_eq!(a.next_quote(), b.next_quote());
        }
    }

    #[test]
    fn exact_quote_uses_given_prices() {
        let q = Quote::exact(3210.0, 3209.0, 3211.0);
        assert_eq!(q.last, 3210.0);
        assert_eq!(q.bid, 3209.0);
        assert_eq!(q.ask, 3211.0);
    }

    #[test]
    fn missing_side_uses_f64_max() {
        let q = Quote::missing_bid(3210.0, 3211.0);
        assert_eq!(q.bid, f64::MAX);
        assert_eq!(q.ask, 3211.0);
        let q2 = Quote::missing_ask(3210.0, 3209.0);
        assert_eq!(q2.ask, f64::MAX);
    }

    #[test]
    fn depth_frame_carries_prices_and_id() {
        use crate::fields::get_fixed;
        let q = Quote::exact(3210.0, 3209.0, 3211.0);
        let f = depth_frame("rb2510", "SHFE", &q, "20261015", "09:30:00", 123);
        assert_eq!(get_fixed(&f.InstrumentID), "rb2510");
        assert_eq!(f.LastPrice, 3210.0);
        assert_eq!(f.BidPrice1, 3209.0);
        assert_eq!(f.AskPrice1, 3211.0);
        assert_eq!(f.UpdateMillisec, 123);
        assert_eq!(get_fixed(&f.UpdateTime), "09:30:00");
    }
}
