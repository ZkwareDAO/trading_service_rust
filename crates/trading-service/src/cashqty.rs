//! 下单金额（order_cash）→ 委托数量（quantity）的纯换算。
//!
//! WS 下单二选一取量：传了 `quantity` 用 `quantity`；否则用 `order_cash ÷ 参考价`
//! 换算出数量。参考价口径：
//! * 限价单：直接用委托价 `price`；
//! * 市价单：优先吃单侧盘口（买单吃卖一 ask、卖单吃买一 bid），盘口缺失回退估值价
//!   （mark > mid > last，与仓位重估同口径）；都没有则无法换算，调用方拒绝下单。
//!
//! 零 IO：行情表只读引用、不联网；venue/kind 映射当前仅接入 Binance
//! （现货/合约），新交易所在 [`market_key`] 扩一行。

use rust_decimal::Decimal;
use rust_decimal::RoundingStrategy;
use trading_core::quote::QuoteTable;
use trading_models::{Instrument, InstrumentType, MarketKind, OrderType, Side, Venue};

/// 现金换算数量保留的小数位（与交易所下单数量精度宽松对齐，超额精度截断到 8 位）。
const QTY_SCALE: u32 = 8;

/// (交易所精确名, 合约形状) → 行情键 (venue, market kind)；未接入的交易所返回 None。
///
/// crate 内共享：下单现金换算与规则跨标的取价（本所优先的 preferred 键）同一口径。
pub(crate) fn market_key(exchange: &str, itype: InstrumentType) -> Option<(Venue, MarketKind)> {
    match (exchange.trim(), itype) {
        ("binance", InstrumentType::Spot) => Some((Venue::BinanceSpot, MarketKind::Spot)),
        ("binance", InstrumentType::Futures | InstrumentType::Perpetual) => {
            Some((Venue::BinanceFutures, MarketKind::Futures))
        }
        ("deribit", InstrumentType::Option) => Some((Venue::Deribit, MarketKind::Option)),
        ("hyperliquid", InstrumentType::Futures | InstrumentType::Perpetual) => {
            Some((Venue::Hyperliquid, MarketKind::Futures))
        }
        _ => None,
    }
}

/// 取现金换算用参考价；任何一环取不到（未接入所、无行情、限价无价）返回 None。
///
/// - 限价单：用委托价 `limit_price`（须为正）；
/// - 市价单：买取卖一、卖取买一，盘口未订阅/缺侧时回退估值价（mark>mid>last）。
pub fn reference_price(
    quotes: &QuoteTable,
    exchange: &str,
    itype: InstrumentType,
    symbol: &str,
    side: Side,
    order_type: OrderType,
    limit_price: Option<Decimal>,
) -> Option<Decimal> {
    if matches!(order_type, OrderType::Limit) {
        return limit_price.filter(|p| *p > Decimal::ZERO);
    }
    let (venue, kind) = market_key(exchange, itype)?;
    let quote = quotes.get(&Instrument::new(venue, kind, symbol))?;
    // 买单吃卖一、卖单吃买一；盘口未订阅（当前 B/H 为 None）回退估值价。
    let book = quote.book.and_then(|b| match side {
        Side::Buy => b.ask,
        Side::Sell => b.bid,
    });
    book.or_else(|| quote.valuation_price())
        .filter(|p| *p > Decimal::ZERO)
}

/// 现金 ÷ 参考价 → 委托数量（截断到 8 位小数）；参考价缺失或结果非正返回 None。
pub fn cash_to_quantity(cash: Decimal, reference_price: Decimal) -> Option<Decimal> {
    if cash <= Decimal::ZERO || reference_price <= Decimal::ZERO {
        return None;
    }
    // 向零取整（截断）到 8 位：现金是预算上限，宁可少买/卖也不超支。
    let qty = (cash / reference_price).round_dp_with_strategy(QTY_SCALE, RoundingStrategy::ToZero);
    (qty > Decimal::ZERO).then_some(qty)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use trading_models::Quote;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn table_with(venue: Venue, kind: MarketKind, symbol: &str, q: Quote) -> QuoteTable {
        let t = QuoteTable::new();
        t.put(&Instrument::new(venue, kind, symbol), q);
        t
    }

    fn quote_with_book(bid: Option<&str>, ask: Option<&str>) -> Quote {
        Quote {
            last: None,
            mark: None,
            mid: None,
            book: Some(trading_models::TopOfBook::new(bid.map(dec), ask.map(dec))),
            ts_exchange_ms: 1,
            ts_recv_ms: 1,
        }
    }

    #[test]
    fn limit_order_uses_limit_price_without_quotes() {
        let t = QuoteTable::new();
        let p = reference_price(
            &t,
            "binance",
            InstrumentType::Futures,
            "BTCUSDT",
            Side::Buy,
            OrderType::Limit,
            Some(dec("80000")),
        );
        assert_eq!(p, Some(dec("80000")));
    }

    #[test]
    fn limit_with_nonpositive_price_is_none() {
        let t = QuoteTable::new();
        assert_eq!(
            reference_price(
                &t,
                "binance",
                InstrumentType::Futures,
                "BTCUSDT",
                Side::Buy,
                OrderType::Limit,
                Some(Decimal::ZERO)
            ),
            None
        );
    }

    #[test]
    fn market_buy_hits_ask_and_sell_hits_bid() {
        // 有盘口：买取 ask=100、卖取 bid=99。
        let q = quote_with_book(Some("99"), Some("100"));
        let t = table_with(Venue::BinanceFutures, MarketKind::Futures, "BTCUSDT", q);
        let buy = reference_price(
            &t,
            "binance",
            InstrumentType::Futures,
            "BTCUSDT",
            Side::Buy,
            OrderType::Market,
            None,
        );
        assert_eq!(buy, Some(dec("100")));
        let sell = reference_price(
            &t,
            "binance",
            InstrumentType::Futures,
            "BTCUSDT",
            Side::Sell,
            OrderType::Market,
            None,
        );
        assert_eq!(sell, Some(dec("99")));
    }

    #[test]
    fn market_falls_back_to_valuation_when_book_absent() {
        // 币安当前不订盘口（book=None）：回退 mark>mid>last。
        let q = Quote {
            last: Some(dec("101")),
            mark: Some(dec("100")),
            mid: None,
            book: None,
            ts_exchange_ms: 1,
            ts_recv_ms: 1,
        };
        let t = table_with(Venue::BinanceFutures, MarketKind::Futures, "BTCUSDT", q);
        let p = reference_price(
            &t,
            "binance",
            InstrumentType::Futures,
            "BTCUSDT",
            Side::Buy,
            OrderType::Market,
            None,
        );
        assert_eq!(p, Some(dec("100"))); // mark 优先
    }

    #[test]
    fn market_without_quote_is_none() {
        let t = QuoteTable::new();
        assert_eq!(
            reference_price(
                &t,
                "binance",
                InstrumentType::Futures,
                "BTCUSDT",
                Side::Buy,
                OrderType::Market,
                None
            ),
            None
        );
    }

    #[test]
    fn unsupported_exchange_has_no_reference() {
        let t = QuoteTable::new();
        assert_eq!(
            reference_price(
                &t,
                "xili",
                InstrumentType::Futures,
                "BTCUSDT",
                Side::Buy,
                OrderType::Market,
                None
            ),
            None
        );
    }

    #[test]
    fn cash_division_truncates_to_8dp_never_overspends() {
        assert_eq!(
            cash_to_quantity(dec("100"), dec("80000")),
            Some(dec("0.00125"))
        );
        // 10/3 = 3.333... 截断到 8 位。
        assert_eq!(
            cash_to_quantity(dec("10"), dec("3")),
            Some(dec("3.33333333"))
        );
        // 第 9 位为 9：四舍五入会进位（超预算），向零取整保持截断。
        // 1 / 1.00000001 ≈ 0.999999990... → 0.99999999。
        assert_eq!(
            cash_to_quantity(dec("1"), dec("1.00000001")),
            Some(dec("0.99999999"))
        );
    }

    #[test]
    fn nonpositive_cash_or_price_rejected() {
        assert_eq!(cash_to_quantity(Decimal::ZERO, dec("100")), None);
        assert_eq!(cash_to_quantity(dec("100"), Decimal::ZERO), None);
    }

    #[test]
    fn market_key_maps_each_known_exchange_and_shape() {
        // Binance 现货 / 合约保持原映射。
        assert_eq!(
            market_key("binance", InstrumentType::Spot),
            Some((Venue::BinanceSpot, MarketKind::Spot))
        );
        assert_eq!(
            market_key("binance", InstrumentType::Futures),
            Some((Venue::BinanceFutures, MarketKind::Futures))
        );
        // Deribit 期权：精确名大小写敏感。
        assert_eq!(
            market_key("deribit", InstrumentType::Option),
            Some((Venue::Deribit, MarketKind::Option))
        );
        // 大小写不一致 / 未知所 / 形状不匹配：查无。
        assert_eq!(market_key("Deribit", InstrumentType::Option), None);
        assert_eq!(market_key("deribit", InstrumentType::Futures), None);
        assert_eq!(market_key("x", InstrumentType::Option), None);
    }
}
