//! RuleDTO：盯盘/风控规则 REST 投影（文档 §3.3）。

use rust_decimal::Decimal;
use serde::Serialize;
use trading_models::{
    InstrumentType, OrderType, Side, WatchConditionKind, WatchRule, WatchRuleStatus, WatchRuleType,
};

/// 规则 REST 投影。`condition_name` 为只读组合串（roi/price_X/position_X/profit_drawdown）。
#[derive(Debug, Clone, Serialize)]
pub struct RuleDTO {
    pub id: u64,
    pub position_id: u64,
    pub rule_type: WatchRuleType,
    pub condition_kind: WatchConditionKind,
    pub condition_instrument: Option<String>,
    /// 只读组合形态串。
    pub condition_name: String,
    pub operator: String,
    #[serde(with = "trading_core::decimal_wire")]
    pub value: Decimal,
    pub action: String,
    pub status: WatchRuleStatus,
    pub exchange: String,
    pub account_id: u64,
    pub instrument: String,
    pub instrument_type: InstrumentType,
    /// new=开仓方向；risk 行为无意义默认值（调用方忽略，平仓方向由持仓反推）。
    pub side: Side,
    #[serde(with = "trading_core::decimal_wire")]
    pub price: Decimal,
    #[serde(with = "trading_core::decimal_wire")]
    pub quantity: Decimal,
    /// new 规则现金口径金额（与 quantity 二选一）；risk 为 0。
    #[serde(with = "trading_core::decimal_wire")]
    pub order_cash: Decimal,
    pub order_type: OrderType,
    #[serde(with = "trading_core::decimal_wire")]
    pub quantity_pct: Decimal,
    pub leverage: u64,
    pub deleted: bool,
    pub created_at: u64,
    pub updated_at: u64,
}

impl From<&WatchRule> for RuleDTO {
    fn from(r: &WatchRule) -> Self {
        Self {
            id: r.id,
            position_id: r.position_id,
            rule_type: r.rule_type,
            condition_kind: r.condition_kind,
            condition_instrument: r.condition_instrument.clone(),
            condition_name: r.condition_name(),
            operator: r.operator.clone(),
            value: r.value,
            action: r.action.clone(),
            status: r.status,
            exchange: r.exchange.clone(),
            account_id: r.account_id,
            instrument: r.instrument.clone(),
            instrument_type: r.instrument_type,
            side: r.side,
            price: r.price,
            quantity: r.quantity,
            order_cash: r.order_cash,
            order_type: r.order_type,
            quantity_pct: r.quantity_pct,
            leverage: r.leverage,
            deleted: r.deleted,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use trading_models::{
        InstrumentType, OrderType, Side, WatchConditionKind, WatchRule, WatchRuleAction,
        WatchRuleStatus, WatchRuleType,
    };

    use super::RuleDTO;

    #[test]
    fn rule_dto_maps_all_fields_and_condition_name() {
        // Arrange：一条链式 pending 的 risk 规则。
        let mut r = WatchRule::new_risk(
            7,
            42,
            WatchConditionKind::Price,
            Some("ETHUSDT".into()),
            "<=",
            Decimal::from(80_000),
            WatchRuleAction::ActivateRule { rule_id: 99 },
            OrderType::Market,
            Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
            Decimal::ZERO,
            Decimal::ONE,
        );
        r.id = 101;
        r.status = WatchRuleStatus::Pending;
        r.deleted = false;
        r.created_at = 1_700_000_000_000;
        r.updated_at = 1_700_000_000_200;

        // Act
        let v = serde_json::to_value(RuleDTO::from(&r)).unwrap();

        // Assert
        assert_eq!(v["id"], 101);
        assert_eq!(v["position_id"], 42);
        assert_eq!(v["rule_type"], "risk");
        assert_eq!(v["condition_kind"], "price");
        assert_eq!(v["condition_instrument"], "ETHUSDT");
        // 只读组合串：price_ETHUSDT。
        assert_eq!(v["condition_name"], "price_ETHUSDT");
        assert_eq!(v["operator"], "<=");
        assert_eq!(v["value"], serde_json::json!(80000));
        assert_eq!(v["action"], "rule:99");
        assert_eq!(v["status"], "pending");
        assert_eq!(v["exchange"], "");
        assert_eq!(v["account_id"], 7);
        assert_eq!(v["instrument"], "");
        assert_eq!(v["instrument_type"], "futures");
        // risk 行 side 为无意义默认值，仍按 model 原值出（调用方忽略）。
        assert_eq!(v["side"], "BUY");
        assert_eq!(v["price"], serde_json::json!(0));
        assert_eq!(v["quantity"], serde_json::json!(0));
        // order_type 出参按 §2.4 字典为大写（rule_type/condition_kind/status 才小写）。
        assert_eq!(v["order_type"], "MARKET");
        assert_eq!(v["quantity_pct"], serde_json::json!(1));
        assert_eq!(v["leverage"], 1);
        assert_eq!(v["deleted"], false);
        assert_eq!(v["created_at"], 1_700_000_000_000u64);
        assert_eq!(v["updated_at"], 1_700_000_000_200u64);
        // 文档 §3.3 共 24 个键。
        assert_eq!(v.as_object().unwrap().len(), 24, "实际键集: {v}");
    }

    #[test]
    fn new_open_rule_dto_uses_lowercase_enums_and_self_action() {
        // Arrange
        let mut r = WatchRule::new_open(
            7,
            0,
            "binance",
            "BTCUSDT",
            InstrumentType::Futures,
            Side::Sell,
            OrderType::Limit,
            Decimal::from(80_000),
            Decimal::new(1, 1),
            3,
        );
        r.id = 5;
        r.condition_kind = WatchConditionKind::Roi;
        r.operator = ">=".into();
        r.value = Decimal::new(12, 2);
        assert_eq!(r.rule_type, WatchRuleType::New);
        // Act
        let v = serde_json::to_value(RuleDTO::from(&r)).unwrap();
        // Assert
        assert_eq!(v["rule_type"], "new");
        assert_eq!(v["condition_kind"], "roi");
        assert_eq!(v["condition_name"], "roi");
        assert_eq!(v["action"], "self");
        assert_eq!(v["status"], "active");
        assert_eq!(v["side"], "SELL");
        assert_eq!(v["order_type"], "LIMIT");
    }
}
