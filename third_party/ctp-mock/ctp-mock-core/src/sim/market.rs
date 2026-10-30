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

/// 历史 tick 映射出的一档盘口快照（值口径：CTP 帧字段）。
///
/// 与脚本/自动游走用的 [`Quote`] 不同：这里**只含数据源确实给出的字段**，
/// 昨结/昨收/涨跌停/均价/2~5 档等无源数据，一律不在此出现（构帧时保持 default 0）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BookQuote {
    /// 最新价。
    pub last: f64,
    /// 今开。
    pub open: f64,
    /// 最高。
    pub high: f64,
    /// 最低。
    pub low: f64,
    /// 当日累计成交量（CTP Volume 为 i32，已饱和截断）。
    pub volume: i32,
    /// 当日累计成交金额（Turnover）。
    pub turnover: f64,
    /// 当前市场总持仓（OpenInterest，LargeVolume=double）。
    pub open_interest: f64,
    /// 买一 (价, 量)；无报价为 None。
    pub bid: Option<(f64, i32)>,
    /// 卖一 (价, 量)；无报价为 None。
    pub ask: Option<(f64, i32)>,
}

impl BookQuote {
    /// 由历史行 [`crate::sim::replay::HistTick`] 转换；累计量四舍五入并饱和到 i32。
    pub fn from_hist(h: &crate::sim::replay::HistTick) -> Self {
        let vol = h.cum_volume.round().clamp(0.0, i32::MAX as f64) as i32;
        Self {
            last: h.last,
            open: h.open,
            high: h.high,
            low: h.low,
            volume: vol,
            turnover: h.cum_amount,
            open_interest: h.cum_position,
            bid: h.bid,
            ask: h.ask,
        }
    }
}

/// 由历史 tick 快照构造深度行情回报帧（忠实映射，不用夹具伪造源中没有的字段）。
///
/// - 数据值取 [`BookQuote`]；缺侧价发 [`NO_PRICE`]（=`f64::MAX`，柜台无对手价契约，
///   服务端 norm_price 会归一为 0）、量发 0；2~5 档源中没有，保持 0。
/// - 时间字段（trading_day/hms/millisec）由调用方盖成**推送当下的北京墙钟**，
///   与「此刻在线的真实前置」一致；数据本身可以是任意历史日。
/// - 昨结/昨收/今收/结算价/涨跌停/均价/虚实度/带价等源中没有，保持 default 0。
pub fn depth_frame_from_hist(
    instrument: &str,
    exchange_id: &str,
    b: &BookQuote,
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

    d.LastPrice = b.last;
    d.OpenPrice = b.open;
    d.HighestPrice = b.high;
    d.LowestPrice = b.low;
    d.Volume = b.volume;
    d.Turnover = b.turnover;
    d.OpenInterest = b.open_interest;

    let (bp, bv) = b.bid.unwrap_or((NO_PRICE, 0));
    let (ap, av) = b.ask.unwrap_or((NO_PRICE, 0));
    d.BidPrice1 = bp;
    d.BidVolume1 = bv;
    d.AskPrice1 = ap;
    d.AskVolume1 = av;
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

#[cfg(test)]
mod replay_frame_tests {
    use super::*;
    use crate::fields::get_fixed;
    use crate::sim::replay::HistTick;

    fn hist() -> HistTick {
        HistTick {
            exchange: "SHFE".into(),
            symbol: "CU2706".into(),
            open: 108000.0,
            high: 109000.0,
            low: 107500.0,
            last: 108650.0,
            cum_volume: 342.0,
            cum_amount: 1.87e8,
            cum_position: 9134.0,
            bid: Some((108640.0, 7)),
            ask: Some((108660.0, 3)),
            ts_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn from_hist_maps_scalars_and_saturates_volume() {
        let b = BookQuote::from_hist(&hist());
        assert_eq!(b.last, 108650.0);
        assert_eq!(b.open, 108000.0);
        assert_eq!(b.volume, 342);
        assert_eq!(b.turnover, 1.87e8);
        assert_eq!(b.open_interest, 9134.0);
        assert_eq!(b.bid, Some((108640.0, 7)));

        let mut huge = hist();
        huge.cum_volume = 1e12;
        assert_eq!(BookQuote::from_hist(&huge).volume, i32::MAX);
    }

    #[test]
    fn frame_carries_mapped_fields_and_current_time_not_historical() {
        let b = BookQuote::from_hist(&hist());
        // 时间由调用方盖成推送当下墙钟（刻意与历史值不同）。
        let f = depth_frame_from_hist("CU2706", "SHFE", &b, "20261029", "10:11:12", 456);
        assert_eq!(get_fixed(&f.InstrumentID), "CU2706");
        assert_eq!(get_fixed(&f.ExchangeID), "SHFE");
        assert_eq!(f.LastPrice, 108650.0);
        assert_eq!(f.OpenPrice, 108000.0);
        assert_eq!(f.HighestPrice, 109000.0);
        assert_eq!(f.LowestPrice, 107500.0);
        assert_eq!(f.Volume, 342);
        assert_eq!(f.Turnover, 1.87e8);
        assert_eq!(f.OpenInterest, 9134.0);
        assert_eq!(f.BidPrice1, 108640.0);
        assert_eq!(f.BidVolume1, 7);
        assert_eq!(f.AskPrice1, 108660.0);
        assert_eq!(f.AskVolume1, 3);
        assert_eq!(get_fixed(&f.TradingDay), "20261029");
        assert_eq!(get_fixed(&f.ActionDay), "20261029");
        assert_eq!(get_fixed(&f.UpdateTime), "10:11:12");
        assert_eq!(f.UpdateMillisec, 456);
    }

    #[test]
    fn frame_leaves_unmapped_fields_at_default_zero() {
        let b = BookQuote::from_hist(&hist());
        let f = depth_frame_from_hist("CU2706", "SHFE", &b, "20261029", "10:11:12", 0);
        // 数据源没有的字段一律 default（不用夹具伪造）。
        assert_eq!(f.PreSettlementPrice, 0.0);
        assert_eq!(f.PreClosePrice, 0.0);
        assert_eq!(f.PreOpenInterest, 0.0);
        assert_eq!(f.ClosePrice, 0.0);
        assert_eq!(f.SettlementPrice, 0.0);
        assert_eq!(f.UpperLimitPrice, 0.0);
        assert_eq!(f.LowerLimitPrice, 0.0);
        assert_eq!(f.AveragePrice, 0.0);
        assert_eq!(f.BidPrice2, 0.0);
        assert_eq!(f.AskPrice5, 0.0);
        assert_eq!(f.BidVolume5, 0);
    }

    #[test]
    fn frame_missing_side_uses_f64_max_and_zero_volume() {
        let mut h = hist();
        h.bid = None;
        h.ask = None;
        let b = BookQuote::from_hist(&h);
        let f = depth_frame_from_hist("CU2706", "SHFE", &b, "20261029", "10:11:12", 0);
        assert_eq!(f.BidPrice1, f64::MAX);
        assert_eq!(f.BidVolume1, 0);
        assert_eq!(f.AskPrice1, f64::MAX);
        assert_eq!(f.AskVolume1, 0);
    }
}
