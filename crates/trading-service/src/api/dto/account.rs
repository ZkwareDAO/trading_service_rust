//! AccountDTO：账户安全投影（只出 4 个非敏感字段）。

use serde::Serialize;
use trading_models::PositionMode;
use trading_store::AccountBrief;

/// 账户安全投影：只暴露 name/exchange/position_mode/is_testnet。
///
/// 红线：id、credentials（密文/明文 JSON）、deleted、时间戳等一律不下发；
/// 账户相关接口（含 /state 聚合）必须经此投影，禁止直接序列化 Account。
/// 自迁移方案 B 起，`position_mode`/`is_testnet` 属于 credentials JSON 内的
/// 账户级属性，投影统一以 store 解出的不含密钥简报（[`AccountBrief`]）为源
/// ——凭证明文不经过 DTO 层。
#[derive(Debug, Clone, Serialize)]
pub struct AccountDTO {
    pub name: String,
    pub exchange: String,
    pub position_mode: PositionMode,
    pub is_testnet: bool,
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
    use trading_models::PositionMode;
    use trading_store::AccountBrief;

    use super::AccountDTO;

    #[test]
    fn account_dto_exposes_only_four_safe_fields() {
        // Arrange：不含密钥的账户简报（store 从 credentials JSON 解出属性）。
        let brief = AccountBrief {
            name: "test_binance".into(),
            exchange: "binance".into(),
            position_mode: PositionMode::Hedge,
            is_testnet: true,
        };

        // Act
        let v = serde_json::to_value(AccountDTO::from(&brief)).unwrap();

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

        // 安全红线：密钥相关字段与内部字段绝不出现（兜底防漏）。
        let raw = v.to_string();
        for leaked in [
            "api_key",
            "api_secret",
            "api_passphrase",
            "credentials",
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
        // Arrange：缺省简报（正式网 + 单向）。
        let brief = AccountBrief {
            name: "default".into(),
            exchange: "xili".into(),
            position_mode: PositionMode::OneWay,
            is_testnet: false,
        };
        // Act
        let v = serde_json::to_value(AccountDTO::from(&brief)).unwrap();
        // Assert
        assert_eq!(v["position_mode"], "one_way");
        assert_eq!(v["is_testnet"], false);
    }
}
