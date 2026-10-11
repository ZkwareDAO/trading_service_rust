//! Binance 下单/撤查单的「纯装配」层：把领域请求 + 规则行变成可直接喂给 SDK 的
//! 字符串参数与单号路由，不碰网络/SDK，便于单测。SDK 调用层只做参数搬运。
//!
//! 纯装配内核已由单测覆盖；SDK 调用层在 `impl ExchangeApi` 接线后成为消费方，
//! 此前非测试构建暂无调用，允许 dead_code。
#![allow(dead_code)]

use rust_decimal::Decimal;
use trading_core::order_rule::verify_binance;
use trading_models::{OrderRule, OrderType, PositionMode, Side, TimeInForce};

use crate::api::{OrderRef, PlaceOrderRequest};

// ============ 撤单 / 查单单号路由 ============

/// 撤/查单使用的单号键。Binance：origClientOrderId 首选，orderId 兜底。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderKey {
    ClientOrderId(String),
    ExchangeOrderId(String),
}

/// 单号路由：首选键 + 运行时首选查不到时的兜底键。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderRoute {
    pub primary: OrderKey,
    pub fallback: Option<OrderKey>,
}

/// Binance 单号路由：始终首选本系统 client_order_id；exchange_order_id 仅作兜底。
/// 撤单与查单同此路由。
pub fn route_order_ref(order: &OrderRef) -> OrderRoute {
    OrderRoute {
        primary: OrderKey::ClientOrderId(order.client_order_id.clone()),
        fallback: order
            .exchange_order_id
            .clone()
            .map(OrderKey::ExchangeOrderId),
    }
}

// ============ positionSide / reduceOnly ============

/// 交易所字符串口径的 positionSide → 领域枚举；None/未知按 BOTH（单向）。
pub fn map_position_side(raw: Option<&str>) -> crate::api::OrderPositionSide {
    match raw.unwrap_or("BOTH").trim().to_ascii_uppercase().as_str() {
        "LONG" => crate::api::OrderPositionSide::Long,
        "SHORT" => crate::api::OrderPositionSide::Short,
        _ => crate::api::OrderPositionSide::Both,
    }
}

/// 账户模式 + 开/平语义 → 逐单持仓方向（领域枚举）。
///
/// 这是"持仓方向"的唯一权威实现，三条下单路径共用：
/// * 单向：恒 [`OrderPositionSide::Both`]（不发 positionSide，行为不变）；
/// * hedge：开仓按 side；平仓按 side.opposite()（锁定被平持仓，平仓不变开仓）。
pub fn order_position_side(
    mode: PositionMode,
    side: Side,
    closing: bool,
) -> crate::api::OrderPositionSide {
    map_position_side(hedge_position_side(mode, side, closing))
}

/// 单向持仓不发 positionSide（交易所默认 BOTH）；hedge 模式按方向发 LONG/SHORT。
pub fn position_side_param(mode: PositionMode, side: Side) -> Option<&'static str> {
    hedge_position_side(mode, side, false)
}

/// 逐单持仓方向（Binance positionSide）：
///
/// * 单向持仓（one-way）：恒返回 [`None`]——不发该参数（交易所默认 BOTH），
///   开仓/平仓行为保持原样；
/// * 双向持仓（hedge）：
///   - 开仓（`closing=false`）：按 `side` 映射（BUY→LONG、SELL→SHORT）；
///   - 平仓（`closing=true`）：按 `side.opposite()` 映射。平仓单的下单方向是持仓的
///     反向，必须据此锁定**被平掉的那条持仓**：平多=SELL+LONG、平空=BUY+SHORT。
///     若只按 side 映射，平仓会变成反向开仓（SELL→SHORT），属资金事故。
pub fn hedge_position_side(mode: PositionMode, side: Side, closing: bool) -> Option<&'static str> {
    match mode {
        PositionMode::OneWay => None,
        PositionMode::Hedge => {
            // 平仓按持仓方向（=下单方向取反）；开仓直接按下单方向。
            let position_side = if closing { side.opposite() } else { side };
            Some(if position_side == Side::Buy {
                "LONG"
            } else {
                "SHORT"
            })
        }
    }
}

/// reduceOnly 只能在单向(BOTH) 模式发送；hedge 模式发送会被交易所拒。
pub fn can_send_reduce_only(mode: PositionMode) -> bool {
    mode == PositionMode::OneWay
}

/// 比对交易所实际持仓模式与本地配置（**纯决策**）：
///
/// 交易所应答 `dualSidePosition`：true=hedge、false=one-way。返回 [`None`] 表示
/// 一致；`Some(actual)` 表示不一致、调用方应记 error 日志（**只读、绝不自动修改
/// 用户交易所设置**）。
pub fn detect_mode_mismatch(
    configured: PositionMode,
    remote_dual_side: bool,
) -> Option<PositionMode> {
    let actual = if remote_dual_side {
        PositionMode::Hedge
    } else {
        PositionMode::OneWay
    };
    (actual != configured).then_some(actual)
}

// ============ 下单前校验 + 参数装配 ============

/// 限价单装配结果（Binance 以字符串提交价/量）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedLimit {
    pub price: String,
    pub quantity: String,
    pub time_in_force: &'static str,
}

/// 装配好的 Binance 订单：真市价只带数量（价格由市场决定）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreparedOrder {
    Limit(PreparedLimit),
    Market { quantity: String },
}

/// 装配失败原因。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrepareError {
    /// 该标的规则尚未在内存表（Binance 严格模式：不下发）。
    RuleMissing,
    /// 限价单缺价格。
    LimitPriceMissing,
    /// 命中交易规则（区间/步长/名义额等），附带可读原因。
    Invalid(String),
}

/// 下单前处理：取规则 → [`verify_binance`] 截断/区间/名义额 → 产出字符串参数。
///
/// `row` 为 `None`（规则缺失）时 Binance 路径直接失败，不把明显非法的单子下发。
pub fn prepare_order(
    row: Option<&OrderRule>,
    req: &PlaceOrderRequest,
) -> Result<PreparedOrder, PrepareError> {
    let Some(row) = row else {
        return Err(PrepareError::RuleMissing);
    };

    match req.order_type {
        OrderType::Limit => {
            // 限价必须有价。
            let price = req.price.ok_or(PrepareError::LimitPriceMissing)?;
            let aligned = verify_binance(row, OrderType::Limit, price, req.quantity)
                .map_err(|e| PrepareError::Invalid(format!("{e:?}")))?;
            let tif = req.time_in_force.unwrap_or(TimeInForce::Gtc).wire();
            Ok(PreparedOrder::Limit(PreparedLimit {
                price: aligned.price,
                quantity: aligned.quantity,
                time_in_force: tif,
            }))
        }
        // 真市价 / 止损市价 / 止盈市价：价格由市场决定，只校数量。
        OrderType::Market | OrderType::StopMarket | OrderType::TakeMarket => {
            let aligned = verify_binance(row, OrderType::Market, Decimal::ZERO, req.quantity)
                .map_err(|e| PrepareError::Invalid(format!("{e:?}")))?;
            Ok(PreparedOrder::Market {
                quantity: aligned.quantity,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::OrderPositionSide;
    use std::str::FromStr;
    use trading_models::{InstrumentType, QtyUnit};

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn binance_row() -> OrderRule {
        let mut r = OrderRule::new(
            "binance",
            "BTCUSDT",
            InstrumentType::Futures,
            QtyUnit::Base,
            Decimal::ONE,
            "USDT",
            "USDT",
            dec("0.001"),
            "binance:fapi:exchangeInfo",
            1,
        );
        r.price_tick = Some(dec("0.1"));
        r.price_min = Some(dec("1000"));
        r.price_max = Some(dec("10000000"));
        r.qty_min = Some(dec("0.001"));
        r.qty_max = Some(dec("1000"));
        r.notional_min = Some(dec("5"));
        r
    }

    fn req_limit(price: Option<&str>, qty: &str) -> PlaceOrderRequest {
        PlaceOrderRequest {
            instrument: "BTCUSDT".into(),
            instrument_type: InstrumentType::Futures,
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: price.map(dec),
            quantity: dec(qty),
            reduce_only: false,
            position_side: OrderPositionSide::Both,
            time_in_force: None,
            client_order_id: "0xabc".into(),
        }
    }

    fn req_market(qty: &str) -> PlaceOrderRequest {
        let mut r = req_limit(None, qty);
        r.order_type = OrderType::Market;
        r
    }

    // ---------- 单号路由 ----------

    #[test]
    fn route_prefers_client_id_with_exchange_fallback() {
        let o = OrderRef::new("0xabc").with_exchange("58167917688");
        assert_eq!(
            route_order_ref(&o),
            OrderRoute {
                primary: OrderKey::ClientOrderId("0xabc".into()),
                fallback: Some(OrderKey::ExchangeOrderId("58167917688".into())),
            }
        );
    }

    #[test]
    fn route_without_exchange_id_has_no_fallback() {
        let o = OrderRef::new("0xabc");
        let route = route_order_ref(&o);
        assert_eq!(route.primary, OrderKey::ClientOrderId("0xabc".into()));
        assert_eq!(route.fallback, None);
    }

    // ---------- positionSide / reduceOnly ----------

    #[test]
    fn one_way_omits_position_side() {
        assert_eq!(position_side_param(PositionMode::OneWay, Side::Buy), None);
        assert_eq!(position_side_param(PositionMode::OneWay, Side::Sell), None);
    }

    #[test]
    fn hedge_sets_long_short_by_side() {
        assert_eq!(
            position_side_param(PositionMode::Hedge, Side::Buy),
            Some("LONG")
        );
        assert_eq!(
            position_side_param(PositionMode::Hedge, Side::Sell),
            Some("SHORT")
        );
    }

    #[test]
    fn reduce_only_only_in_one_way_mode() {
        assert!(can_send_reduce_only(PositionMode::OneWay));
        assert!(!can_send_reduce_only(PositionMode::Hedge));
    }

    // hedge 逐单持仓方向必须区分开仓/平仓：
    // 开仓按 side；平仓按 side.opposite()（平仓方向是持仓的反向）。
    #[test]
    fn hedge_open_uses_side_as_position_side() {
        // 开多：BUY → LONG；开空：SELL → SHORT。
        assert_eq!(
            hedge_position_side(PositionMode::Hedge, Side::Buy, false),
            Some("LONG")
        );
        assert_eq!(
            hedge_position_side(PositionMode::Hedge, Side::Sell, false),
            Some("SHORT")
        );
    }

    #[test]
    fn hedge_close_uses_opposite_side_as_position_side() {
        // 平多：下单 SELL，但 positionSide 必须是 LONG（不是 SHORT，否则变开空）。
        assert_eq!(
            hedge_position_side(PositionMode::Hedge, Side::Sell, true),
            Some("LONG")
        );
        // 平空：下单 BUY，positionSide 必须是 SHORT（不是 LONG，否则变开多）。
        assert_eq!(
            hedge_position_side(PositionMode::Hedge, Side::Buy, true),
            Some("SHORT")
        );
    }

    #[test]
    fn mode_mismatch_detection() {
        // 一致：None。
        assert_eq!(detect_mode_mismatch(PositionMode::OneWay, false), None);
        assert_eq!(detect_mode_mismatch(PositionMode::Hedge, true), None);
        // 不一致：返回交易所实际模式。
        assert_eq!(
            detect_mode_mismatch(PositionMode::OneWay, true),
            Some(PositionMode::Hedge)
        );
        assert_eq!(
            detect_mode_mismatch(PositionMode::Hedge, false),
            Some(PositionMode::OneWay)
        );
    }

    #[test]
    fn one_way_never_sends_position_side_even_when_closing() {
        // 单向持仓：开/平都不发 positionSide（保持原行为不回归）。
        assert_eq!(
            hedge_position_side(PositionMode::OneWay, Side::Buy, false),
            None
        );
        assert_eq!(
            hedge_position_side(PositionMode::OneWay, Side::Sell, true),
            None
        );
    }

    // ---------- 下单装配 ----------

    #[test]
    fn limit_is_verified_and_formatted() {
        let req = req_limit(Some("65000.19"), "0.01239");
        let out = prepare_order(Some(&binance_row()), &req).unwrap();
        assert_eq!(
            out,
            PreparedOrder::Limit(PreparedLimit {
                price: "65000.1".into(),
                quantity: "0.012".into(),
                time_in_force: "GTC",
            })
        );
    }

    #[test]
    fn limit_honors_explicit_ioc_tif() {
        let mut req = req_limit(Some("65000"), "0.012");
        req.time_in_force = Some(TimeInForce::Ioc);
        let out = prepare_order(Some(&binance_row()), &req).unwrap();
        match out {
            PreparedOrder::Limit(l) => assert_eq!(l.time_in_force, "IOC"),
            _ => panic!("expected limit"),
        }
    }

    #[test]
    fn market_only_formats_quantity() {
        let req = req_market("0.0129");
        let out = prepare_order(Some(&binance_row()), &req).unwrap();
        assert_eq!(
            out,
            PreparedOrder::Market {
                quantity: "0.012".into()
            }
        );
    }

    #[test]
    fn missing_rule_fails_closed() {
        let err = prepare_order(None, &req_limit(Some("65000"), "0.012")).unwrap_err();
        assert_eq!(err, PrepareError::RuleMissing);
    }

    #[test]
    fn limit_without_price_is_error() {
        let err = prepare_order(Some(&binance_row()), &req_limit(None, "0.012")).unwrap_err();
        assert_eq!(err, PrepareError::LimitPriceMissing);
    }

    #[test]
    fn rule_violation_maps_to_invalid() {
        // 名义额 1000*0.001=1 < 5。
        let err =
            prepare_order(Some(&binance_row()), &req_limit(Some("1000"), "0.001")).unwrap_err();
        assert!(matches!(err, PrepareError::Invalid(_)));
    }
}
