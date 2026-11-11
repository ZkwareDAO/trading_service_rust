//! BalanceDTO：交易所账户余额 REST 投影。
//!
//! 与 `trading_exchange::api::BalanceInfo` 一一对应；Decimal 走 `decimal_wire`
//! 出精确 JSON 数字，可选金额出 `null`（不省略字段，便于消费方稳定取键）。

use rust_decimal::Decimal;
use serde::Serialize;
use trading_exchange::api::BalanceInfo;

/// 账户余额 REST 投影（一行一个结算币种）。
#[derive(Debug, Clone, Serialize)]
pub struct BalanceDTO {
    /// 结算币种（USDT / BTC / ETH / CNY / USDC 等）。
    pub currency: String,
    /// 账户权益（含未实现盈亏）。
    #[serde(with = "trading_core::decimal_wire")]
    pub equity: Decimal,
    /// 可用资金。
    #[serde(with = "trading_core::decimal_wire")]
    pub available: Decimal,
    /// 占用保证金；交易所未提供时为 null。
    #[serde(with = "trading_core::decimal_wire::opt")]
    pub margin_used: Option<Decimal>,
    /// 未实现盈亏；交易所未提供时为 null。
    #[serde(with = "trading_core::decimal_wire::opt")]
    pub unrealized_pnl: Option<Decimal>,
    /// 交易所侧数据时间（UTC 毫秒）；无毫秒口径时为 null。
    pub update_ms: Option<i64>,
}

impl From<&BalanceInfo> for BalanceDTO {
    fn from(b: &BalanceInfo) -> Self {
        Self {
            currency: b.currency.clone(),
            equity: b.equity,
            available: b.available,
            margin_used: b.margin_used,
            unrealized_pnl: b.unrealized_pnl,
            update_ms: b.update_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use trading_exchange::api::BalanceInfo;

    use super::BalanceDTO;

    fn info() -> BalanceInfo {
        BalanceInfo {
            currency: "USDT".into(),
            equity: Decimal::from_str_exact("10000.5").unwrap(),
            available: Decimal::from_str_exact("8000.25").unwrap(),
            margin_used: Some(Decimal::from_str_exact("2000.25").unwrap()),
            unrealized_pnl: Some(Decimal::from_str_exact("-500.5").unwrap()),
            update_ms: Some(1_731_081_600_000),
        }
    }

    #[test]
    fn maps_all_fields() {
        // Act
        let dto = BalanceDTO::from(&info());

        // Assert：字段逐项透传，不做换算。
        assert_eq!(dto.currency, "USDT");
        assert_eq!(dto.equity, Decimal::from_str_exact("10000.5").unwrap());
        assert_eq!(dto.available, Decimal::from_str_exact("8000.25").unwrap());
        assert_eq!(
            dto.margin_used,
            Some(Decimal::from_str_exact("2000.25").unwrap())
        );
        assert_eq!(
            dto.unrealized_pnl,
            Some(Decimal::from_str_exact("-500.5").unwrap())
        );
        assert_eq!(dto.update_ms, Some(1_731_081_600_000));
    }

    #[test]
    fn serializes_decimals_as_json_numbers_and_none_as_null() {
        // Arrange：可选金额缺失（CTP 无 update_ms、HL 无未实现盈亏的形态）。
        let mut b = info();
        b.margin_used = None;
        b.unrealized_pnl = None;
        b.update_ms = None;

        // Act
        let v = serde_json::to_value(BalanceDTO::from(&b)).unwrap();

        // Assert：Decimal 是数字而非字符串；Option::None 出 null 且键仍存在。
        assert!(v["equity"].is_number(), "equity 必须是 JSON 数字");
        assert!(v["available"].is_number(), "available 必须是 JSON 数字");
        assert_eq!(v["equity"], serde_json::json!(10000.5));
        assert!(v["margin_used"].is_null());
        assert!(v["unrealized_pnl"].is_null());
        assert!(v["update_ms"].is_null());
    }
}
