//! 命中规则 → 内存订单 / 下单请求的纯构造。
//!
//! 零 IO：只根据规则行与（risk 的）持仓快照计算订单要素，不连交易所、不写仓储。
//! * `new`：按规则行自带的交易所/标的/方向/价/量开仓，方向取 [`WatchRule::side`]；
//! * `risk`：方向取持仓反向、`reduce_only=true`，平仓量按
//!   `quantity>0 → 绝对量`，否则 `持仓量 × quantity_pct`。
//!
//! 两种订单都写 `rule_id`，供订单终态回报时回溯规则并将其 retire。

use rust_decimal::Decimal;
use trading_exchange::api::PlaceOrderRequest;
use trading_models::{Order, OrderType, Position, TimeInForce, WatchRule, WatchRuleType};

/// 规则单构造失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleOrderError {
    /// risk 规则必须能按 position_id 取到活跃持仓。
    PositionMissing,
    /// 计算出的下单量非正数（绝对量为 0、比例为 0、new 未给量等）。
    QuantityNotPositive,
}

/// risk 规则的平仓量：绝对量优先（且不超过实时持仓量），否则持仓量 × 比例。
///
/// 绝对量取 `min(规则量, 持仓量)`：规则可能在创建后因行情/其它规则先平部分仓位，
/// 触发时实时持仓已小于规则量，reduce_only 超量部分交易所不会成交，故按实际持仓
/// 下发，从源头避免"请求 5、成交 0.001，订单却记成 5"。
/// 比例口径天然 ≤ 持仓量，无需截断。
fn close_quantity(rule: &WatchRule, position: &Position) -> Option<Decimal> {
    if rule.quantity > Decimal::ZERO {
        return Some(rule.quantity.min(position.quantity));
    }
    (rule.quantity_pct > Decimal::ZERO).then(|| position.quantity * rule.quantity_pct)
}

/// 订单落库价：市价单恒为 0，其余用规则限价。
fn entry_price(rule: &WatchRule) -> Decimal {
    if matches!(rule.order_type, OrderType::Market) {
        Decimal::ZERO
    } else {
        rule.price
    }
}

/// 由命中规则构造内存订单（不落库、不联网）；`id`/时间戳留待 `OrderRepo::insert` 分配。
///
/// * risk 必须给 `position`：交易所/标的/合约形状取自持仓，方向取反，reduce_only；
/// * new 不读持仓：交易所/标的/方向/价/量全部取规则行。
pub fn build_rule_order(
    rule: &WatchRule,
    position: Option<&Position>,
    is_testnet: bool,
    client_order_id: String,
) -> Result<Order, RuleOrderError> {
    let (exchange, instrument, instrument_type, side, price, quantity, reduce_only, leverage) =
        match rule.rule_type {
            WatchRuleType::Risk => {
                let pos = position.ok_or(RuleOrderError::PositionMissing)?;
                let qty = close_quantity(rule, pos)
                    .filter(|q| *q > Decimal::ZERO)
                    .ok_or(RuleOrderError::QuantityNotPositive)?;
                (
                    pos.exchange.clone(),
                    pos.instrument.clone(),
                    pos.instrument_type,
                    pos.side.opposite(),
                    entry_price(rule),
                    qty,
                    true,
                    // 风控平仓沿用持仓开仓杠杆（一般交易所平仓不再改杠杆，仅随单记录）。
                    pos.leverage,
                )
            }
            WatchRuleType::New => {
                if rule.quantity <= Decimal::ZERO {
                    return Err(RuleOrderError::QuantityNotPositive);
                }
                (
                    rule.exchange.clone(),
                    rule.instrument.clone(),
                    rule.instrument_type,
                    rule.side,
                    entry_price(rule),
                    rule.quantity,
                    false,
                    rule.leverage,
                )
            }
        };

    let mut order = Order::new(rule.account_id, client_order_id, exchange, instrument, side);
    order.rule_id = rule.id;
    order.instrument_type = instrument_type;
    order.test_net = is_testnet;
    order.order_type = rule.order_type;
    order.order_price = price;
    order.order_quantity = quantity;
    order.reduce_only = reduce_only;
    order.leverage = leverage.max(1);
    // 规则单统一单向持仓、GTC；市价单的有效期由适配器按所规处理。
    order.time_in_force = TimeInForce::Gtc;
    Ok(order)
}

/// 订单若已到终态且由规则单产生，返回需要 retire 的 rule_id；否则 None。
///
/// WS 回报（event.rs）与未来的 REST 拉单补偿共用同一判定，保证订单经**任一途径**
/// 被推进到终态（FILLED/CANCELED/REJECTED/EXPIRED）时，关联规则都 in_use→retired。
pub fn terminal_rule_id(order: &Order) -> Option<u64> {
    (order.rule_id != 0 && order.status.is_terminal()).then_some(order.rule_id)
}

/// 内存订单 → 交易所统一下单请求（与策略命令路径同构）。
///
/// `mode`：账户持仓模式，仅 Binance 据此决定 positionSide（单向=BOTH；
/// hedge 开仓按 side、平仓按持仓反向）。Deribit 不消费该字段。
pub fn build_place_request(order: &Order, mode: trading_models::PositionMode) -> PlaceOrderRequest {
    PlaceOrderRequest {
        instrument: order.instrument.clone(),
        instrument_type: order.instrument_type,
        side: order.side,
        order_type: order.order_type,
        price: if matches!(order.order_type, OrderType::Market) {
            None
        } else {
            Some(order.order_price)
        },
        quantity: order.order_quantity,
        reduce_only: order.reduce_only,
        // 持仓方向统一收口到 binance::prep（单向=BOTH；hedge 平仓锁定持仓、不反手）。
        position_side: trading_exchange::binance::prep::order_position_side(
            mode,
            order.side,
            order.reduce_only,
        ),
        time_in_force: Some(order.time_in_force),
        client_order_id: order.client_order_id.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use trading_exchange::api::OrderPositionSide;
    use trading_models::{InstrumentType, Side, WatchConditionKind, WatchRuleAction};

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn cid() -> String {
        "0x0123456789abcdef0123456789abcdef".to_string()
    }

    fn long_position() -> Position {
        let mut p = Position::new(7, "binance", "BTCUSDT", Side::Buy);
        p.id = 100;
        p.quantity = dec("2");
        p
    }

    /// 动作 self 的 risk 规则。
    fn risk_rule(order_type: OrderType, quantity: &str, pct: &str) -> WatchRule {
        let mut r = WatchRule::new_risk(
            7,
            100,
            WatchConditionKind::Roi,
            None,
            ">=",
            dec("0.12"),
            WatchRuleAction::UseSelf,
            order_type,
            dec(quantity),
            dec(pct),
        );
        r.id = 10;
        r
    }

    fn open_rule(side: Side, order_type: OrderType, price: &str, qty: &str) -> WatchRule {
        let mut r = WatchRule::new_open(
            7,
            0,
            "binance",
            "BTCUSDT",
            InstrumentType::Futures,
            side,
            order_type,
            dec(price),
            dec(qty),
            1,
        );
        r.id = 20;
        r
    }

    #[test]
    fn risk_close_long_by_pct_sells_reduce_only() {
        let rule = risk_rule(OrderType::Market, "0", "0.5"); // 半仓
        let pos = long_position();
        let order = build_rule_order(&rule, Some(&pos), false, cid()).unwrap();

        assert_eq!(order.rule_id, 10);
        assert_eq!(order.account_id, 7);
        assert_eq!(order.side, Side::Sell); // 平多反向
        assert!(order.reduce_only);
        assert_eq!(order.order_type, OrderType::Market);
        assert_eq!(order.order_quantity, dec("1")); // 2 × 0.5
        assert_eq!(order.instrument, "BTCUSDT");
        assert_eq!(order.order_price, Decimal::ZERO); // 市价 0
        assert!(!order.test_net);
    }

    #[test]
    fn risk_close_request_always_carries_reduce_only_flag() {
        // 端到端：risk 规则单 → 最终发给交易所的请求，reduce_only 必须为 true
        //（平多/平空都一样）。这是"绝不开反向单"的硬保证。
        let rule = risk_rule(OrderType::Market, "0", "0.5");
        let pos = long_position();
        let order = build_rule_order(&rule, Some(&pos), false, cid()).unwrap();
        let req = build_place_request(&order, trading_models::PositionMode::OneWay);
        assert!(req.reduce_only, "平仓请求必须带 reduce_only");

        // 平空方向同样必须带。
        let mut short_pos = long_position();
        short_pos.side = Side::Sell;
        let order2 = build_rule_order(&rule, Some(&short_pos), false, cid()).unwrap();
        let req2 = build_place_request(&order2, trading_models::PositionMode::OneWay);
        assert!(req2.reduce_only);
    }

    #[test]
    fn risk_close_short_buys_back() {
        let rule = risk_rule(OrderType::Market, "0", "1"); // 全平
        let mut pos = long_position();
        pos.side = Side::Sell;
        let order = build_rule_order(&rule, Some(&pos), true, cid()).unwrap();
        assert_eq!(order.side, Side::Buy); // 平空反向
        assert_eq!(order.order_quantity, dec("2")); // 2 × 1
        assert!(order.test_net);
    }

    #[test]
    fn risk_absolute_quantity_takes_precedence_over_pct() {
        let rule = risk_rule(OrderType::Limit, "0.7", "1");
        let order = build_rule_order(&rule, Some(&long_position()), false, cid()).unwrap();
        assert_eq!(order.order_quantity, dec("0.7")); // 绝对量优先，不读 pct
        assert_eq!(order.order_type, OrderType::Limit);
    }

    // 触发期保护：绝对平仓量超过实时持仓时，按实际持仓量下发（不发超量），
    // 避免 reduce_only 超仓后订单却把请求量当成交量。
    #[test]
    fn risk_absolute_quantity_is_clamped_to_position_size() {
        // 持仓 2，规则绝对量 5 → 实际下单量 2。
        let rule = risk_rule(OrderType::Market, "5", "0");
        let order = build_rule_order(&rule, Some(&long_position()), false, cid()).unwrap();
        assert_eq!(order.order_quantity, dec("2"));
    }

    #[test]
    fn risk_limit_order_carries_price() {
        // risk 也可下限价平仓：规则行 price 作为限价。
        let mut rule = risk_rule(OrderType::Limit, "0", "1");
        rule.price = dec("90000");
        let order = build_rule_order(&rule, Some(&long_position()), false, cid()).unwrap();
        assert_eq!(order.order_type, OrderType::Limit);
        assert_eq!(order.order_price, dec("90000"));
    }

    #[test]
    fn risk_without_position_is_an_error() {
        let rule = risk_rule(OrderType::Market, "0", "1");
        let err = build_rule_order(&rule, None, false, cid()).unwrap_err();
        assert_eq!(err, RuleOrderError::PositionMissing);
    }

    #[test]
    fn risk_rejects_non_positive_quantity() {
        let rule = risk_rule(OrderType::Market, "0", "0"); // 绝对量与比例都为 0
        let err = build_rule_order(&rule, Some(&long_position()), false, cid()).unwrap_err();
        assert_eq!(err, RuleOrderError::QuantityNotPositive);
    }

    #[test]
    fn new_open_uses_rule_fields_and_side() {
        let rule = open_rule(Side::Buy, OrderType::Limit, "50000", "0.1");
        let order = build_rule_order(&rule, None, false, cid()).unwrap();
        assert_eq!(order.rule_id, 20);
        assert_eq!(order.side, Side::Buy);
        assert_eq!(order.exchange, "binance");
        assert_eq!(order.instrument, "BTCUSDT");
        assert_eq!(order.instrument_type, InstrumentType::Futures);
        assert_eq!(order.order_price, dec("50000"));
        assert_eq!(order.order_quantity, dec("0.1"));
        assert!(!order.reduce_only); // 开仓不标 reduce_only
        assert_eq!(order.order_type, OrderType::Limit);
        // 开仓单默认 1x（open_rule 测试构造器传 1）。
        assert_eq!(order.leverage, 1);
    }

    #[test]
    fn new_open_carries_rule_leverage() {
        let mut rule = open_rule(Side::Buy, OrderType::Market, "0", "1");
        rule.leverage = 2;
        let order = build_rule_order(&rule, None, false, cid()).unwrap();
        assert_eq!(order.leverage, 2);
    }

    #[test]
    fn risk_close_carries_position_leverage() {
        let rule = risk_rule(OrderType::Market, "0", "1");
        let mut pos = long_position();
        pos.leverage = 3;
        let order = build_rule_order(&rule, Some(&pos), false, cid()).unwrap();
        assert_eq!(order.leverage, 3);
    }

    #[test]
    fn new_open_market_uses_zero_price() {
        let rule = open_rule(Side::Sell, OrderType::Market, "0", "1");
        let order = build_rule_order(&rule, None, false, cid()).unwrap();
        assert_eq!(order.side, Side::Sell);
        assert_eq!(order.order_price, Decimal::ZERO);
        assert_eq!(order.order_quantity, dec("1"));
    }

    #[test]
    fn new_open_rejects_non_positive_quantity() {
        let rule = open_rule(Side::Buy, OrderType::Market, "0", "0");
        let err = build_rule_order(&rule, None, false, cid()).unwrap_err();
        assert_eq!(err, RuleOrderError::QuantityNotPositive);
    }

    #[test]
    fn place_request_mirrors_order_position_side_by_mode() {
        let mut rule = risk_rule(OrderType::Limit, "0", "0.5");
        rule.price = dec("90000");
        let order = build_rule_order(&rule, Some(&long_position()), false, cid()).unwrap();
        // 单向持仓：positionSide=Both（原行为不回归）。
        let req = build_place_request(&order, trading_models::PositionMode::OneWay);
        assert_eq!(req.instrument, "BTCUSDT");
        assert_eq!(req.instrument_type, InstrumentType::Futures);
        assert_eq!(req.side, Side::Sell);
        assert_eq!(req.order_type, OrderType::Limit);
        assert_eq!(req.price, Some(dec("90000")));
        assert_eq!(req.quantity, dec("1"));
        assert!(req.reduce_only);
        assert_eq!(req.position_side, OrderPositionSide::Both);
        assert_eq!(req.client_order_id, order.client_order_id);
        assert!(req.time_in_force.is_some());

        // hedge 风控平仓：下单 SELL，但 positionSide 必须是 LONG（平多，不是开空）。
        let hedge_req = build_place_request(&order, trading_models::PositionMode::Hedge);
        assert_eq!(hedge_req.position_side, OrderPositionSide::Long);

        // 市价请求价格为 None。
        let market = risk_rule(OrderType::Market, "0", "1");
        let mo = build_rule_order(&market, Some(&long_position()), false, cid()).unwrap();
        assert_eq!(
            build_place_request(&mo, trading_models::PositionMode::OneWay).price,
            None
        );
    }
}
