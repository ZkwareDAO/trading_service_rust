//! AccountDTO：账户安全投影（只出 4 个非敏感字段）。

use serde::Serialize;
use trading_models::{Account, PositionMode};
use trading_store::AccountBrief;

/// 账户安全投影：只暴露 name/exchange/position_mode/is_testnet。
///
/// 红线：id、api_key、api_secret、api_passphrase、deleted、时间戳等
/// 一律不下发；账户相关接口（含 /state 聚合）必须经此投影，禁止直接序列化 Account。
#[derive(Debug, Clone, Serialize)]
pub struct AccountDTO {
    pub name: String,
    pub exchange: String,
    pub position_mode: PositionMode,
    pub is_testnet: bool,
}

impl From<&Account> for AccountDTO {
    fn from(a: &Account) -> Self {
        Self {
            name: a.name.clone(),
            exchange: a.exchange.clone(),
            position_mode: a.position_mode,
            is_testnet: a.is_testnet,
        }
    }
}

/// 从不含密钥的账户简报投影（列表查询走此口径，无需解密全字段行）。
impl From<&AccountBrief> for AccountDTO {
    fn from(a: &AccountBrief) -> Self {
        Self {
            name: a.name.clone(),
            exchange: a.exchange.clone(),
            position_mode: a.position_mode,
            is_testnet: a.is_testnet,
        }
    }
}

#[cfg(test)]
mod tests {
    use trading_models::{Account, PositionMode};

    use super::AccountDTO;

    #[test]
    fn account_dto_exposes_only_four_safe_fields() {
        // Arrange：含密钥/内部字段的完整账户行（凭证为测试假值）。
        let mut a = Account::new("test_binance", "binance");
        a.id = 7;
        a.api_key = "AK".into();
        a.api_secret = "SK".into();
        a.api_passphrase = Some("PASS".into());
        a.position_mode = PositionMode::Hedge;
        a.is_testnet = true;
        a.deleted = false;
        a.created_at = 1_700_000_000_000;
        a.updated_at = 1_700_000_000_200;

        // Act
        let v = serde_json::to_value(AccountDTO::from(&a)).unwrap();

        // Assert：恰好 4 个键（JSON 对象键序无语义，serde_json 默认字母序，按集合比）。
        let mut keys: Vec<_> = v.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        assert_eq!(
            keys,
            vec!["exchange", "is_testnet", "name", "position_mode"]
        );
        assert_eq!(v["name"], "test_binance");
        assert_eq!(v["exchange"], "binance");
        assert_eq!(v["position_mode"], "hedge");
        assert_eq!(v["is_testnet"], true);

        // 安全红线：密钥与内部字段绝不出现（用完整 JSON 串兜底防漏）。
        let raw = v.to_string();
        for leaked in [
            "AK",
            "SK",
            "PASS",
            "api_key",
            "api_secret",
            "api_passphrase",
            "\"id\"",
            "deleted",
            "created_at",
            "updated_at",
        ] {
            assert!(!raw.contains(leaked), "账户报文疑似泄漏 `{leaked}`: {raw}");
        }
    }

    #[test]
    fn one_way_mainnet_account_serializes_lowercase_enum() {
        // Arrange
        let a = Account::new("default", "xili");
        // Act
        let v = serde_json::to_value(AccountDTO::from(&a)).unwrap();
        // Assert
        assert_eq!(v["position_mode"], "one_way");
        assert_eq!(v["is_testnet"], false);
    }
}
