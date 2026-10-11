//! OrderDTO：订单 REST 报文（与 CSV 持久化 model 分离）。

use rust_decimal::Decimal;
use serde::Serialize;
use trading_models::{InstrumentType, Order, OrderStatus, OrderType, Side, TimeInForce};

/// 订单 REST 投影（文档 §3.1）。枚举直接内嵌 model 枚举（serde rename 已与
/// 线上字典逐字一致）；Decimal 走精确 JSON 数字；bool 出 true/false。
#[derive(Debug, Clone, Serialize)]
pub struct OrderDTO {
    pub id: u64,
    pub client_order_id: String,
    pub account_id: u64,
    pub signal_id: Option<String>,
    /// 触发本单的规则 id；手工/信号单为 0。
    pub rule_id: u64,
    pub strategy_name: Option<String>,
    pub exchange: String,
    pub instrument_type: InstrumentType,
    pub test_net: bool,
    pub instrument: String,
    pub side: Side,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    #[serde(with = "trading_core::decimal_wire")]
    pub order_price: Decimal,
    #[serde(with = "trading_core::decimal_wire")]
    pub order_quantity: Decimal,
    #[serde(with = "trading_core::decimal_wire")]
    pub order_cash: Decimal,
    #[serde(with = "trading_core::decimal_wire")]
    pub trigger_price: Decimal,
    pub reduce_only: bool,
    pub leverage: u64,
    pub exchange_order_id: Option<String>,
    pub exchange_order_ref: Option<String>,
    pub status: OrderStatus,
    #[serde(with = "trading_core::decimal_wire")]
    pub filled_price: Decimal,
    #[serde(with = "trading_core::decimal_wire")]
    pub filled_quantity: Decimal,
    pub deleted: bool,
    pub exchange_update_time: u64,
    pub created_at: u64,
    pub updated_at: u64,
}

impl From<&Order> for OrderDTO {
    fn from(o: &Order) -> Self {
        Self {
            id: o.id,
            client_order_id: o.client_order_id.clone(),
            account_id: o.account_id,
            signal_id: o.signal_id.clone(),
            rule_id: o.rule_id,
            strategy_name: o.strategy_name.clone(),
            exchange: o.exchange.clone(),
            instrument_type: o.instrument_type,
            test_net: o.test_net,
            instrument: o.instrument.clone(),
            side: o.side,
            order_type: o.order_type,
            time_in_force: o.time_in_force,
            order_price: o.order_price,
            order_quantity: o.order_quantity,
            order_cash: o.order_cash,
            trigger_price: o.trigger_price,
            reduce_only: o.reduce_only,
            leverage: o.leverage,
            exchange_order_id: o.exchange_order_id.clone(),
            exchange_order_ref: o.exchange_order_ref.clone(),
            status: o.status,
            filled_price: o.filled_price,
            filled_quantity: o.filled_quantity,
            deleted: o.deleted,
            exchange_update_time: o.exchange_update_time,
            created_at: o.created_at,
            updated_at: o.updated_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use std::str::FromStr;
    use trading_models::{Order, Side};

    use super::OrderDTO;

    #[test]
    fn order_dto_maps_all_wire_fields() {
        // Arrange：一条带全部非默认字段的订单。
        let mut o = Order::new(
            7,
            "0x0123456789abcdef0123456789abcdef",
            "binance",
            "btcusdt",
            Side::Sell,
        );
        o.id = 123;
        o.signal_id = Some("sig-1".into());
        o.rule_id = 9;
        o.strategy_name = Some("manual-rest".into());
        o.instrument_type = trading_models::InstrumentType::Futures;
        o.test_net = true;
        // 入站小写标的已在编排层归一为大写。
        o.instrument = "BTCUSDT".into();
        o.order_type = trading_models::OrderType::StopMarket;
        o.time_in_force = trading_models::TimeInForce::Ioc;
        o.order_price = Decimal::from(80_000);
        o.order_quantity = Decimal::new(1, 1);
        o.order_cash = Decimal::ZERO;
        o.trigger_price = Decimal::from(79_000);
        o.reduce_only = true;
        o.leverage = 3;
        o.exchange_order_id = Some("ex-9".into());
        o.exchange_order_ref = None;
        o.status = trading_models::OrderStatus::Partial;
        o.filled_price = Decimal::new(5, 1);
        o.filled_quantity = Decimal::new(5, 2);
        o.deleted = false;
        o.exchange_update_time = 1_700_000_000_100;
        o.created_at = 1_700_000_000_000;
        o.updated_at = 1_700_000_000_200;

        // Act
        let v = serde_json::to_value(OrderDTO::from(&o)).unwrap();

        // Assert：标量/字符串字段。
        assert_eq!(v["id"], 123);
        assert_eq!(v["client_order_id"], "0x0123456789abcdef0123456789abcdef");
        assert_eq!(v["account_id"], 7);
        assert_eq!(v["signal_id"], "sig-1");
        assert_eq!(v["rule_id"], 9);
        assert_eq!(v["strategy_name"], "manual-rest");
        assert_eq!(v["exchange"], "binance");
        assert_eq!(v["instrument_type"], "futures");
        assert_eq!(v["test_net"], true);
        assert_eq!(v["instrument"], "BTCUSDT");
        assert_eq!(v["side"], "SELL");
        assert_eq!(v["order_type"], "STOP_MARKET");
        assert_eq!(v["time_in_force"], "IOC");
        assert_eq!(v["reduce_only"], true);
        assert_eq!(v["leverage"], 3);
        assert_eq!(v["exchange_order_id"], "ex-9");
        assert!(v["exchange_order_ref"].is_null());
        assert_eq!(v["status"], "PARTIAL");
        assert_eq!(v["deleted"], false);
        assert_eq!(v["exchange_update_time"], 1_700_000_000_100u64);
        assert_eq!(v["created_at"], 1_700_000_000_000u64);
        assert_eq!(v["updated_at"], 1_700_000_000_200u64);

        // Decimal 必须是 JSON 数字（非字符串），且不经 f64。
        assert_eq!(v["order_price"], serde_json::json!(80000));
        assert_eq!(v["order_quantity"], serde_json::json!(0.1));
        assert_eq!(v["trigger_price"], serde_json::json!(79000));
        assert_eq!(v["filled_price"], serde_json::json!(0.5));
        assert_eq!(v["filled_quantity"], serde_json::json!(0.05));
        assert!(v["order_price"].is_number());

        // 无多余/缺失键：与文档 §3.1 字段集严格一致（28 个）。
        assert_eq!(v.as_object().unwrap().len(), 28, "实际键集: {v}");
    }

    #[test]
    fn decimal_preserves_scale_without_f64() {
        // Arrange
        let mut o = Order::new(
            1,
            "0x0123456789abcdef0123456789abcdef",
            "xili",
            "BTCUSDT",
            Side::Buy,
        );
        o.order_price = Decimal::from_str("80000.5").unwrap();
        // Act
        let v = serde_json::to_value(OrderDTO::from(&o)).unwrap();
        // Assert：精确数字 80000.5，非字符串、无 f64 尾数。
        assert_eq!(v["order_price"].to_string(), "80000.5");
    }
}
