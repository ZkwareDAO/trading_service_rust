//! 下单金额（order_cash）→ 委托数量（quantity）的纯换算。
//!
//! 名义价值口径：`notional = price × quantity × multiplier`，
//! 因此原始数量 `qty_raw = order_cash ÷ (参考价 × 合约乘数)`；有手数步长时
//! **向下取整**到整步（期货整数手不超预算），否则向零截断到 8 位。
//! 乘数/步长由 [`resolve_spec`] 按「规则行 → 交易所 provider 静态规格 → 缺省 1」
//! 解析；CTP 缺乘数时 fail-closed（宁可拒单，不按 1 放大手数）。
//!
//! WS 下单二选一取量：传了 `quantity` 用 `quantity`；否则按上述公式换算。
//! 参考价口径：
//! * 限价单：直接用委托价 `price`；
//! * 市价单：优先吃单侧盘口（买单吃卖一 ask、卖单吃买一 bid），盘口缺失回退估值价
//!   （mark > mid > last，与仓位重估同口径）；都没有则无法换算，调用方拒绝下单。
//!
//! 零 IO：行情表只读引用、不联网；venue/kind 映射当前仅接入 Binance
//! （现货/合约），新交易所在 [`market_key`] 扩一行。

use rust_decimal::Decimal;
use rust_decimal::RoundingStrategy;
use trading_core::quote::QuoteTable;
use trading_exchange::api::ContractSpec;
use trading_models::{Instrument, InstrumentType, MarketKind, OrderRule, OrderType, Side, Venue};

/// 现金换算数量保留的小数位（与交易所下单数量精度宽松对齐，超额精度截断到 8 位）。
const QTY_SCALE: u32 = 8;

/// 名义换算规格：名义价值 = price × quantity × multiplier。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotionalSpec {
    /// 合约乘数（每手对应的标的数量；现货=1）。
    pub multiplier: Decimal,
    /// 手数步长：`Some(s>0)` 向下取整到 s（期货整数手=1）；`None` 按 8 位向零截断。
    pub lot_step: Option<Decimal>,
}

/// 换算规格解析失败原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizingError {
    /// 该所要求合约乘数，但规则行与 provider 规格都未就绪（fail-closed）。
    MultiplierUnavailable,
}

impl std::fmt::Display for SizingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MultiplierUnavailable => write!(f, "合约乘数未就绪"),
        }
    }
}

impl std::error::Error for SizingError {}

/// 解析换算规格，优先级：规则行 → provider 静态规格 → 缺省 1。
///
/// `required` 为 true（CTP）时，前两级都拿不到正乘数即返回 Err：宁可拒单，
/// 也不能按乘数 1 换算而放大手数。
pub fn resolve_spec(
    rule: Option<&OrderRule>,
    venue: Option<ContractSpec>,
    required: bool,
) -> Result<NotionalSpec, SizingError> {
    // 规则行乘数/步长由交易所同步而来，可信度最高。
    if let Some(rule) = rule {
        if rule.multiplier > Decimal::ZERO {
            return Ok(NotionalSpec {
                multiplier: rule.multiplier,
                lot_step: positive_step(Some(rule.qty_step)),
            });
        }
    }
    if let Some(spec) = venue {
        if spec.multiplier > Decimal::ZERO {
            return Ok(NotionalSpec {
                multiplier: spec.multiplier,
                lot_step: positive_step(spec.lot_step),
            });
        }
    }
    if required {
        return Err(SizingError::MultiplierUnavailable);
    }
    // 非必需所（币安/HL 有规则行情、xili 等）维持旧口径：乘数缺省 1。
    Ok(NotionalSpec {
        multiplier: Decimal::ONE,
        lot_step: None,
    })
}

/// 非正步长视为「无步长」（防御非法配置）。
fn positive_step(step: Option<Decimal>) -> Option<Decimal> {
    step.filter(|s| *s > Decimal::ZERO)
}

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
        ("ctp", InstrumentType::Futures) => Some((Venue::Ctp, MarketKind::Futures)),
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

/// 现金 ÷（参考价 × 乘数）→ 委托数量。
///
/// 有手数步长时向下取整到整步（期货整数手不超预算）；否则向零截断到 8 位。
/// 任一输入非正或结果非正返回 None（调用方拒单）。
pub fn cash_to_quantity(
    cash: Decimal,
    reference_price: Decimal,
    spec: &NotionalSpec,
) -> Option<Decimal> {
    if cash <= Decimal::ZERO || reference_price <= Decimal::ZERO || spec.multiplier <= Decimal::ZERO
    {
        return None;
    }
    let raw = cash / (reference_price * spec.multiplier);
    let qty = match positive_step(spec.lot_step) {
        // 向下取整到整步：现金是预算上限，宁可少买/卖也不超支。
        Some(step) => (raw / step).floor() * step,
        // 无手数步长（现货/小数手）：向零截断到 8 位。
        None => raw.round_dp_with_strategy(QTY_SCALE, RoundingStrategy::ToZero),
    };
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

    fn spec(multiplier: &str, lot_step: Option<&str>) -> NotionalSpec {
        NotionalSpec {
            multiplier: dec(multiplier),
            lot_step: lot_step.map(dec),
        }
    }

    fn spot_spec() -> NotionalSpec {
        spec("1", None)
    }

    #[test]
    fn spot_division_truncates_to_8dp_never_overspends() {
        assert_eq!(
            cash_to_quantity(dec("100"), dec("80000"), &spot_spec()),
            Some(dec("0.00125"))
        );
        // 10/3 = 3.333... 截断到 8 位。
        assert_eq!(
            cash_to_quantity(dec("10"), dec("3"), &spot_spec()),
            Some(dec("3.33333333"))
        );
        // 第 9 位为 9：四舍五入会进位（超预算），向零取整保持截断。
        // 1 / 1.00000001 ≈ 0.999999990... → 0.99999999。
        assert_eq!(
            cash_to_quantity(dec("1"), dec("1.00000001"), &spot_spec()),
            Some(dec("0.99999999"))
        );
    }

    #[test]
    fn futures_multiplier_is_accounted() {
        // rb 乘数 10、价 3000、现金 30000 → 30000/(3000×10) = 1 手
        //（旧口径 30000/3000=10 手，放大 10 倍）。
        assert_eq!(
            cash_to_quantity(dec("30000"), dec("3000"), &spec("10", Some("1"))),
            Some(dec("1"))
        );
        // 精确整数边界：60000/(3000×10)=2 恰好整除，不应因除法舍入多取一手。
        assert_eq!(
            cash_to_quantity(dec("60000"), dec("3000"), &spec("10", Some("1"))),
            Some(dec("2"))
        );
    }

    #[test]
    fn futures_floors_to_lot_step() {
        // 29999/(3000×10)=0.99996... 向下取整到整数手 → 0 → 拒单。
        assert_eq!(
            cash_to_quantity(dec("29999"), dec("3000"), &spec("10", Some("1"))),
            None
        );
        // 59999/(3000×10)=1.99996... → 1 手（不超预算）。
        assert_eq!(
            cash_to_quantity(dec("59999"), dec("3000"), &spec("10", Some("1"))),
            Some(dec("1"))
        );
        // 非整数手步长同样向下取整：1000/(100×1)=10 按 0.3 取整 → 9.9。
        assert_eq!(
            cash_to_quantity(dec("1000"), dec("100"), &spec("1", Some("0.3"))),
            Some(dec("9.9"))
        );
    }

    #[test]
    fn invalid_spec_is_rejected() {
        // 乘数非正：换算无意义，一律拒（尤其是 CTP 未预热必须以 0 表示「未知」）。
        assert_eq!(
            cash_to_quantity(dec("100"), dec("10"), &spec("0", Some("1"))),
            None
        );
        assert_eq!(
            cash_to_quantity(dec("100"), dec("10"), &spec("-10", Some("1"))),
            None
        );
        // 非正步长视为「无步长」→ 回退 8 位截断（防御非法配置，不静默归零）。
        assert_eq!(
            cash_to_quantity(dec("10"), dec("3"), &spec("1", Some("0"))),
            Some(dec("3.33333333"))
        );
    }

    #[test]
    fn nonpositive_cash_or_price_rejected() {
        assert_eq!(
            cash_to_quantity(Decimal::ZERO, dec("100"), &spot_spec()),
            None
        );
        assert_eq!(
            cash_to_quantity(dec("100"), Decimal::ZERO, &spot_spec()),
            None
        );
    }

    fn rule_row(multiplier: &str, qty_step: &str) -> trading_models::OrderRule {
        trading_models::OrderRule::new(
            "binance",
            "BTCUSDT",
            InstrumentType::Futures,
            trading_models::QtyUnit::Base,
            dec(multiplier),
            "USDT",
            "USDT",
            dec(qty_step),
            "test",
            0,
        )
    }

    #[test]
    fn resolve_spec_prefers_rule_row() {
        let rule = rule_row("2", "0.001");
        let provider = trading_exchange::api::ContractSpec {
            multiplier: dec("10"),
            lot_step: Some(Decimal::ONE),
        };
        let spec = resolve_spec(Some(&rule), Some(provider), true).unwrap();
        assert_eq!(spec.multiplier, dec("2"));
        assert_eq!(spec.lot_step, Some(dec("0.001")));
    }

    #[test]
    fn resolve_spec_falls_back_to_provider() {
        let provider = trading_exchange::api::ContractSpec {
            multiplier: dec("10"),
            lot_step: Some(Decimal::ONE),
        };
        let spec = resolve_spec(None, Some(provider), true).unwrap();
        assert_eq!(spec.multiplier, dec("10"));
        assert_eq!(spec.lot_step, Some(Decimal::ONE));
    }

    #[test]
    fn resolve_spec_defaults_to_one_when_not_required() {
        let spec = resolve_spec(None, None, false).unwrap();
        assert_eq!(spec.multiplier, Decimal::ONE);
        assert_eq!(spec.lot_step, None);
    }

    #[test]
    fn resolve_spec_fails_closed_when_multiplier_required() {
        assert_eq!(
            resolve_spec(None, None, true),
            Err(SizingError::MultiplierUnavailable)
        );
        // provider 规格非法（乘数 0）等同缺省。
        assert_eq!(
            resolve_spec(
                None,
                Some(trading_exchange::api::ContractSpec {
                    multiplier: Decimal::ZERO,
                    lot_step: Some(Decimal::ONE),
                }),
                true,
            ),
            Err(SizingError::MultiplierUnavailable)
        );
    }

    #[test]
    fn resolve_spec_ignores_invalid_rule_and_uses_provider() {
        let rule = rule_row("0", "0.001");
        let provider = trading_exchange::api::ContractSpec {
            multiplier: dec("10"),
            lot_step: Some(Decimal::ONE),
        };
        let spec = resolve_spec(Some(&rule), Some(provider), true).unwrap();
        assert_eq!(spec.multiplier, dec("10"));
        assert_eq!(spec.lot_step, Some(Decimal::ONE));
    }

    #[test]
    fn resolve_spec_normalizes_nonpositive_lot_step() {
        let provider = trading_exchange::api::ContractSpec {
            multiplier: dec("10"),
            lot_step: Some(Decimal::ZERO),
        };
        let spec = resolve_spec(None, Some(provider), true).unwrap();
        assert_eq!(spec.lot_step, None);
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
