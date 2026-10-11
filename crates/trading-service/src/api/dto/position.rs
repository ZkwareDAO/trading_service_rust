//! PositionDTO：内存持仓 REST 投影（不暴露 qty_unit）。

use rust_decimal::Decimal;
use serde::Serialize;
use trading_models::{InstrumentType, Position, Side};

/// 内存持仓 REST 投影（文档 §3.2）。刻意不含 model 的 `qty_unit`
/// （无业务消费方，纯扩展预留，REST 一律不展示）。
#[derive(Debug, Clone, Serialize)]
pub struct PositionDTO {
    pub id: u64,
    pub account_id: u64,
    /// 最近一次风控平仓的触发规则；无则 0。
    pub rule_id: u64,
    pub exchange: String,
    pub instrument_type: InstrumentType,
    pub instrument: String,
    pub side: Side,
    #[serde(with = "trading_core::decimal_wire")]
    pub quantity: Decimal,
    #[serde(with = "trading_core::decimal_wire")]
    pub multiplier: Decimal,
    #[serde(with = "trading_core::decimal_wire")]
    pub pos_price: Decimal,
    #[serde(with = "trading_core::decimal_wire")]
    pub current_price: Decimal,
    #[serde(with = "trading_core::decimal_wire")]
    pub notional: Decimal,
    pub leverage: u64,
    #[serde(with = "trading_core::decimal_wire")]
    pub margin: Decimal,
    pub settle_currency: String,
    #[serde(with = "trading_core::decimal_wire")]
    pub pnl_value: Decimal,
    #[serde(with = "trading_core::decimal_wire")]
    pub realized_pnl: Decimal,
    #[serde(with = "trading_core::decimal_wire")]
    pub roi: Decimal,
    #[serde(with = "trading_core::decimal_wire")]
    pub max_roi: Decimal,
    #[serde(with = "trading_core::decimal_wire")]
    pub profit_drawdown: Decimal,
    pub expires_at: Option<u64>,
    pub deleted: bool,
    pub close_time: Option<u64>,
    pub exchange_update_time: u64,
    pub created_at: u64,
    pub updated_at: u64,
}

impl From<&Position> for PositionDTO {
    fn from(p: &Position) -> Self {
        Self {
            id: p.id,
            account_id: p.account_id,
            rule_id: p.rule_id,
            exchange: p.exchange.clone(),
            instrument_type: p.instrument_type,
            instrument: p.instrument.clone(),
            side: p.side,
            quantity: p.quantity,
            multiplier: p.multiplier,
            pos_price: p.pos_price,
            current_price: p.current_price,
            notional: p.notional,
            leverage: p.leverage,
            margin: p.margin,
            settle_currency: p.settle_currency.clone(),
            pnl_value: p.pnl_value,
            realized_pnl: p.realized_pnl,
            roi: p.roi,
            max_roi: p.max_roi,
            profit_drawdown: p.profit_drawdown,
            expires_at: p.expires_at,
            deleted: p.deleted,
            close_time: p.close_time,
            exchange_update_time: p.exchange_update_time,
            created_at: p.created_at,
            updated_at: p.updated_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use trading_models::{Position, Side};

    use super::PositionDTO;

    #[test]
    fn position_dto_maps_fields_and_omits_qty_unit() {
        // Arrange：一条估值字段齐全的活跃多仓。
        let mut p = Position::new(7, "binance", "BTCUSDT", Side::Buy);
        p.id = 42;
        p.rule_id = 5;
        p.instrument_type = trading_models::InstrumentType::Futures;
        p.quantity = Decimal::new(2, 1);
        p.multiplier = Decimal::ONE;
        p.pos_price = Decimal::from(80_000);
        p.current_price = Decimal::from(80_800);
        p.notional = Decimal::from(16_160);
        p.leverage = 10;
        p.margin = Decimal::from(1616);
        p.settle_currency = "USDT".into();
        p.pnl_value = Decimal::from(160);
        p.realized_pnl = Decimal::from(5);
        p.roi = Decimal::new(1, 1);
        p.max_roi = Decimal::new(2, 1);
        p.profit_drawdown = Decimal::new(5, 1);
        p.expires_at = None;
        p.deleted = false;
        p.close_time = None;
        p.exchange_update_time = 1_700_000_000_100;
        p.created_at = 1_700_000_000_000;
        p.updated_at = 1_700_000_000_200;

        // Act
        let v = serde_json::to_value(PositionDTO::from(&p)).unwrap();

        // Assert
        assert_eq!(v["id"], 42);
        assert_eq!(v["account_id"], 7);
        assert_eq!(v["rule_id"], 5);
        assert_eq!(v["exchange"], "binance");
        assert_eq!(v["instrument_type"], "futures");
        assert_eq!(v["instrument"], "BTCUSDT");
        assert_eq!(v["side"], "BUY");
        assert_eq!(v["quantity"], serde_json::json!(0.2));
        assert_eq!(v["multiplier"], serde_json::json!(1));
        assert_eq!(v["pos_price"], serde_json::json!(80000));
        assert_eq!(v["current_price"], serde_json::json!(80800));
        assert_eq!(v["notional"], serde_json::json!(16160));
        assert_eq!(v["leverage"], 10);
        assert_eq!(v["margin"], serde_json::json!(1616));
        assert_eq!(v["settle_currency"], "USDT");
        assert_eq!(v["pnl_value"], serde_json::json!(160));
        assert_eq!(v["realized_pnl"], serde_json::json!(5));
        assert_eq!(v["roi"], serde_json::json!(0.1));
        assert_eq!(v["max_roi"], serde_json::json!(0.2));
        assert_eq!(v["profit_drawdown"], serde_json::json!(0.5));
        assert!(v["expires_at"].is_null());
        assert_eq!(v["deleted"], false);
        assert!(v["close_time"].is_null());
        assert_eq!(v["exchange_update_time"], 1_700_000_000_100u64);
        assert_eq!(v["created_at"], 1_700_000_000_000u64);
        assert_eq!(v["updated_at"], 1_700_000_000_200u64);

        // 安全/契约红线：qty_unit 绝不出现在报文里。
        assert!(v.get("qty_unit").is_none(), "DTO 不得暴露 qty_unit: {v}");
        // 文档 §3.2 共 26 个键。
        assert_eq!(v.as_object().unwrap().len(), 26, "实际键集: {v}");
    }

    #[test]
    fn closed_position_keeps_optional_timestamps() {
        // Arrange
        let mut p = Position::new(1, "xili", "ETHUSDT", Side::Sell);
        p.id = 3;
        p.deleted = true;
        p.expires_at = Some(1_800_000_000_000);
        p.close_time = Some(1_750_000_000_000);
        // Act
        let v = serde_json::to_value(PositionDTO::from(&p)).unwrap();
        // Assert
        assert_eq!(v["deleted"], true);
        assert_eq!(v["expires_at"], 1_800_000_000_000u64);
        assert_eq!(v["close_time"], 1_750_000_000_000u64);
    }
}
