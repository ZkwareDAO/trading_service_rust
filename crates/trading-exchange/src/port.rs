//! 交易所接入层对宿主服务提出的出站端口（依赖倒置）。
//!
//! trading-exchange 不认识 trading-service：适配器只通过本模块的两个 trait
//! 向外取凭证、回报订单事件，具体实现由宿主侧提供（见 trading-service 的
//! `AccountManager` / `AppState` 的 impl）。这样适配器搬迁/复用都不反向依赖服务内部状态。

use async_trait::async_trait;
use rust_decimal::Decimal;
use trading_core::protocol::OrderEvent;
use trading_models::{MarketKind, PositionMode, Venue};

use crate::api::PositionInfo;

/// 解密后的账户凭证快照（交易所接入层口径，不含账户名/不落盘）。
///
/// 由宿主的凭证解析端口在下单/建连时现取现解密，适配器不长期缓存 secret。
/// Debug 刻意不打印任何密钥字段。
///
/// 按 venue 形态分为两类：REST 签名所（Binance/Deribit/Hyperliquid）用
/// [`ExchangeCredential::ApiSecret`]；CTP 期货用 [`ExchangeCredential::Ctp`]。
/// 编译期隔离两种凭证形态，venue 侧误用对方字段即编译错误。
#[derive(Clone)]
pub enum ExchangeCredential {
    /// REST 签名所凭证（api_key/api_secret[/passphrase]）。
    ApiSecret {
        api_key: String,
        api_secret: String,
        /// 部分所（如 Deribit/cf）需要的 passphrase；Binance 为 None。
        api_passphrase: Option<String>,
        /// 持仓模式：one_way（BOTH）/ hedge（LONG/SHORT）。
        position_mode: PositionMode,
        /// 用户级选网：true=交易所测试网。下单 REST 与账户事件流据此建连；
        /// 交易所级公共行情/规则不随账户区分、恒定正式网。
        is_testnet: bool,
    },
    /// CTP 期货凭证（看穿式认证 7 件套）。
    Ctp {
        broker_id: String,
        user_id: String,
        password: String,
        app_id: String,
        auth_code: String,
        /// 投资者代码；空则回退 user_id。
        investor_id: String,
        /// 交易前置地址，如 "tcp://182.254.243.31:40001"。
        front_address: String,
        /// 评测版 API（v6.7.11+）接 SimNow 等仿真环境必须 false，生产柜台 true。
        is_production_mode: bool,
    },
}

/// CTP 凭证借用视图（[`ExchangeCredential::as_ctp`] 的返回类型）。
pub struct CtpCredsRef<'a> {
    pub broker_id: &'a str,
    pub user_id: &'a str,
    pub password: &'a str,
    pub app_id: &'a str,
    pub auth_code: &'a str,
    pub investor_id: &'a str,
    pub front_address: &'a str,
    pub is_production_mode: bool,
}

impl ExchangeCredential {
    /// CTP 凭证的便捷取用：非 CTP 凭证返回 None（调用方按所匹配，天然不会取错）。
    pub fn as_ctp(&self) -> Option<CtpCredsRef<'_>> {
        match self {
            ExchangeCredential::Ctp {
                broker_id,
                user_id,
                password,
                app_id,
                auth_code,
                investor_id,
                front_address,
                is_production_mode,
            } => Some(CtpCredsRef {
                broker_id,
                user_id,
                password,
                app_id,
                auth_code,
                investor_id,
                front_address,
                is_production_mode: *is_production_mode,
            }),
            ExchangeCredential::ApiSecret { .. } => None,
        }
    }
}

impl std::fmt::Debug for ExchangeCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExchangeCredential::ApiSecret {
                api_passphrase,
                position_mode,
                ..
            } => f
                .debug_struct("ExchangeCredential::ApiSecret")
                .field("api_key", &"<redacted>")
                .field("api_secret", &"<redacted>")
                .field(
                    "api_passphrase",
                    &api_passphrase.as_ref().map(|_| "<redacted>"),
                )
                .field("position_mode", position_mode)
                .finish(),
            ExchangeCredential::Ctp { .. } => f
                .debug_struct("ExchangeCredential::Ctp")
                .field("<all fields redacted>", &true)
                .finish(),
        }
    }
}

// =====================================================================
// credentials JSON schema（迁移方案 B：account.csv 凭证列的各所反序列化目标）
// =====================================================================
//
// account.csv 的 `credentials` 列解密后为各所 credentials JSON；schema 归属
// 各自 venue：REST 签名所（Binance/Deribit/Hyperliquid）同构共用
// [`ApiSecretCreds`]，CTP 期货用 [`CtpCreds`]。二者与枚举 variant 字段一一
// 对应，经 `From` 转换进 [`ExchangeCredential`]，四个 venue 消费方零改动。
//
// `CtpCreds` 刻意定义在本模块而非 `ctp/` 子模块：ctp 整体在 feature="ctp"
// 下编译，而 trading-store 的解析路由（exchange 名 → schema）必须无条件可用，
// schema 必须不被 feature 门隔离（trading-models 因此不含任何 venue 专属字段）。

/// REST 签名所 credentials JSON schema（`credentials` 列明文的反序列化目标）。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ApiSecretCreds {
    /// API key / 公钥。
    pub api_key: String,
    /// API secret / 私钥。
    pub api_secret: String,
    /// 第三凭证/口令（Deribit/cf 需要；binance/hyperliquid 缺省 null）。
    #[serde(default)]
    pub api_passphrase: Option<String>,
    /// 持仓模式：one_way（默认，BOTH）/ hedge（LONG/SHORT）。
    #[serde(default)]
    pub position_mode: PositionMode,
    /// 是否连接交易所测试网（缺省正式网）。
    #[serde(default)]
    pub is_testnet: bool,
}

impl From<ApiSecretCreds> for ExchangeCredential {
    fn from(c: ApiSecretCreds) -> Self {
        ExchangeCredential::ApiSecret {
            api_key: c.api_key,
            api_secret: c.api_secret,
            api_passphrase: c.api_passphrase,
            position_mode: c.position_mode,
            is_testnet: c.is_testnet,
        }
    }
}

/// CTP 期货 credentials JSON schema（`credentials` 列明文的反序列化目标）。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CtpCreds {
    /// 经纪公司代码（如 SimNow "9999"）。
    pub broker_id: String,
    /// 客户资金账号。
    pub user_id: String,
    /// 资金密码（明文口令，由 credentials 密文保护）。
    pub password: String,
    /// 客户端认证 AppID（看穿式）。
    pub app_id: String,
    /// 客户端认证授权码（看穿式）。
    pub auth_code: String,
    /// 投资者代码；空则运行期回退 user_id。
    #[serde(default)]
    pub investor_id: String,
    /// 交易前置地址，如 "tcp://182.254.243.31:40001"。
    pub front_address: String,
    /// 评测版 API（v6.7.11+）接 SimNow 等仿真环境必须 false，生产柜台 true。
    #[serde(default)]
    pub is_production_mode: bool,
}

impl From<CtpCreds> for ExchangeCredential {
    fn from(c: CtpCreds) -> Self {
        ExchangeCredential::Ctp {
            broker_id: c.broker_id,
            user_id: c.user_id,
            password: c.password,
            app_id: c.app_id,
            auth_code: c.auth_code,
            investor_id: c.investor_id,
            front_address: c.front_address,
            is_production_mode: c.is_production_mode,
        }
    }
}

/// 按 exchange 名路由 credentials JSON 明文 → 强类型凭证枚举。
///
/// 解析路由（迁移方案 B）：`ctp` → [`CtpCreds`]，其余所（binance/deribit/
/// hyperliquid/xili 同构）→ [`ApiSecretCreds`]。JSON 非法/缺必填字段时报错，
/// 由调用方决定拒载口径，不静默吞错。
pub fn credential_from_json(exchange: &str, json: &str) -> anyhow::Result<ExchangeCredential> {
    if exchange == "ctp" {
        Ok(serde_json::from_str::<CtpCreds>(json)?.into())
    } else {
        Ok(serde_json::from_str::<ApiSecretCreds>(json)?.into())
    }
}

/// 凭证解析端口：按账户名与交易所精确名解析当前活动账户。
///
/// 交易所名全局唯一、大小写敏感、无别名；账户行 exchange 与该名字面不一致或
/// 无活动账户时返回 None。
pub trait CredentialResolver: Send + Sync {
    fn resolve(&self, account: &str, exchange: &str) -> Option<ExchangeCredential>;
}

/// 订单回报出口：账户事件流把映射后的 [`OrderEvent`] 推进宿主订单状态机，
/// 由宿主负责落内存与按策略回推。
///
/// * `source`：日志区分来源（如 "binance" / "deribit"）；
/// * `account`：该回报所属**账户名**（account.csv 的 name）。通知 A 据此反查
///   account_id / 账户名做订单事件播报；业务处理本身不依赖该参数。
#[async_trait]
pub trait OrderEventSink: Send + Sync {
    async fn on_order_event(&self, ev: &OrderEvent, source: &str, account: &str);
}

/// 期望账户目标供给端口：宿主周期提供当前应保持在线的账户目标集，
/// 交易所接入层据此对账（新增起连、移除停连、版本变化停旧起新）。
/// 典型实现是宿主的账户管理器；版本在账户重载（改密/切网）后变化。
pub trait AccountTargets: Send + Sync {
    fn active_targets(&self) -> Vec<crate::runtime::ConnTarget>;
}

/// 仓位快照出口：账户事件流去抖到点后，适配器拉到 REST 真值，把某账户的
/// 交易所持仓快照（`PositionInfo`，交易所口径）交给宿主做映射/对账/落库。
///
/// 适配器只负责「拉」，不认识宿主的仓位表与裁决内核；实现需快速返回
/// （重活应自行 spawn），避免阻塞账户事件消费循环。
#[async_trait]
pub trait PositionSnapshotSink: Send + Sync {
    async fn on_position_snapshot(&self, exchange: &str, account: &str, positions: &[PositionInfo]);
}

/// 行情报价观察口：公共行情流每写一笔最新价即同步回调一次。
///
/// 与订单/仓位回报不同，仓位重估是纯内存操作，故本口为**同步**：
/// 实现内部按 venue/kind/symbol 只刷新对应交易所的活跃仓位，不得做网络 IO。
/// 各交易所推送频率不同，互不干扰。
pub trait QuoteObserver: Send + Sync {
    fn on_quote(&self, venue: Venue, kind: MarketKind, symbol: &str, price: Decimal);
}

// =====================================================================
// 动态行情订阅与价差告警（新增接缝；无强制实现方，Binance/xili 零影响）
// =====================================================================

/// 一个需要被行情流订阅的标的（venue + 市场形态 + 原生符号）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterestKey {
    pub venue: Venue,
    pub kind: MarketKind,
    /// 交易所原生/规范化符号。
    pub symbol: String,
}

impl InterestKey {
    pub fn new(venue: Venue, kind: MarketKind, symbol: impl Into<String>) -> Self {
        Self {
            venue,
            kind,
            symbol: symbol.into(),
        }
    }
}

/// 行情订阅兴趣供给：宿主读 TradingStore（活跃仓位 ∪ 规则涉及标的）给出当前并集。
///
/// 规则涉及标的需同时包含**条件标的**与**动作（开/平仓）标的**：new rule 的两者
/// 可能不同。市场侧据此周期 reconcile 订阅集。
pub trait MarketInterest: Send + Sync {
    fn interests(&self) -> Vec<InterestKey>;
}

/// Deribit 平仓价差过宽的告警载荷（venue 在 claim 前预检时产出）。
#[derive(Debug, Clone)]
pub struct SpreadAlert {
    pub instrument: String,
    pub bid: Decimal,
    pub ask: Decimal,
    /// 买一卖一绝对价差。
    pub spread: Decimal,
    /// 触发阈值。
    pub threshold: Decimal,
}

/// 价差告警出口：fire-and-forget，由宿主侧 NotifyHub 实现冷却 + 企业微信投递；
/// 任何失败只记日志，不影响交易/阻断语义。
#[async_trait]
pub trait SpreadAlertSink: Send + Sync {
    async fn on_spread_alert(&self, alert: SpreadAlert);
}

#[cfg(test)]
mod credential_schema_tests {
    use super::*;
    use trading_models::SecretCipher;

    #[test]
    fn api_secret_creds_json_roundtrip_and_conversion() {
        // Arrange：binance 风格全字段 credentials JSON。
        let json = r#"{"api_key":"abc123","api_secret":"s3cret","api_passphrase":null,"position_mode":"hedge","is_testnet":true}"#;

        // Act：反序列化 → 枚举转换 → 再序列化。
        let creds: ApiSecretCreds = serde_json::from_str(json).unwrap();
        let cred: ExchangeCredential = creds.clone().into();
        let round: ApiSecretCreds =
            serde_json::from_str(&serde_json::to_string(&creds).unwrap()).unwrap();

        // Assert：往返无损；枚举字段逐一就位。
        assert_eq!(round, creds);
        match cred {
            ExchangeCredential::ApiSecret {
                api_key,
                api_secret,
                api_passphrase,
                position_mode,
                is_testnet,
            } => {
                assert_eq!(api_key, "abc123");
                assert_eq!(api_secret, "s3cret");
                assert_eq!(api_passphrase, None);
                assert_eq!(position_mode, PositionMode::Hedge);
                assert!(is_testnet);
            }
            ExchangeCredential::Ctp { .. } => panic!("应为 ApiSecret 凭证"),
        }
    }

    #[test]
    fn api_secret_creds_minimal_json_defaults() {
        // Arrange：deribit/hyperliquid 风格最小 JSON（省略可选字段）。
        let json = r#"{"api_key":"client-id","api_secret":"client-secret"}"#;

        // Act：反序列化 → 转换。
        let creds: ApiSecretCreds = serde_json::from_str(json).unwrap();
        let cred: ExchangeCredential = creds.into();

        // Assert：可选字段按缺省归一（正式网、单向、无 passphrase）。
        match cred {
            ExchangeCredential::ApiSecret {
                api_passphrase,
                position_mode,
                is_testnet,
                ..
            } => {
                assert_eq!(api_passphrase, None);
                assert_eq!(position_mode, PositionMode::OneWay);
                assert!(!is_testnet);
            }
            ExchangeCredential::Ctp { .. } => panic!("应为 ApiSecret 凭证"),
        }
    }

    #[test]
    fn ctp_creds_json_roundtrip_and_conversion() {
        // Arrange：迁移方案示例的 CTP credentials JSON（含空 investor_id）。
        let json = r#"{"broker_id":"9999","user_id":"100001","password":"pwd","app_id":"simnow_client","auth_code":"0000000000000000","investor_id":"","front_address":"tcp://182.254.243.31:40001","is_production_mode":false}"#;

        // Act：反序列化 → 枚举转换 → 再序列化。
        let creds: CtpCreds = serde_json::from_str(json).unwrap();
        let round: CtpCreds =
            serde_json::from_str(&serde_json::to_string(&creds).unwrap()).unwrap();
        let cred: ExchangeCredential = creds.into();

        // Assert：往返无损；as_ctp 视图字段逐一就位。
        assert_eq!(round.investor_id, "");
        assert!(!round.is_production_mode);
        let view = cred.as_ctp().expect("CTP 凭证应可取 CTP 视图");
        assert_eq!(view.broker_id, "9999");
        assert_eq!(view.user_id, "100001");
        assert_eq!(view.app_id, "simnow_client");
        assert_eq!(view.auth_code, "0000000000000000");
        assert_eq!(view.front_address, "tcp://182.254.243.31:40001");
        assert!(!view.is_production_mode);
    }

    #[test]
    fn ctp_creds_minimal_json_defaults() {
        // Arrange：CTP 最小 JSON（省略 investor_id / is_production_mode）。
        let json = r#"{"broker_id":"9999","user_id":"100001","password":"pwd","app_id":"app","auth_code":"0000","front_address":"tcp://f:1"}"#;

        // Act / Assert：可选字段缺省（investor_id 空、仿真模式）。
        let creds: CtpCreds = serde_json::from_str(json).unwrap();
        assert_eq!(creds.investor_id, "");
        assert!(!creds.is_production_mode);
    }

    #[test]
    fn credential_from_json_routes_by_exchange() {
        // Act / Assert：ctp 路由 CtpCreds；其余所（含未知名）路由 ApiSecretCreds。
        let ctp = r#"{"broker_id":"9999","user_id":"u","password":"p","app_id":"a","auth_code":"c","front_address":"tcp://f:1"}"#;
        assert!(credential_from_json("ctp", ctp).unwrap().as_ctp().is_some());

        let rest = r#"{"api_key":"k","api_secret":"s"}"#;
        for exchange in ["binance", "deribit", "hyperliquid", "xili"] {
            assert!(
                matches!(
                    credential_from_json(exchange, rest).unwrap(),
                    ExchangeCredential::ApiSecret { .. }
                ),
                "{exchange} 应路由 ApiSecretCreds"
            );
        }

        // 非法 JSON 报错（调用方拒载，不静默）。
        assert!(credential_from_json("binance", "not-json").is_err());
    }

    #[test]
    fn four_venue_credentials_json_encrypt_roundtrip() {
        // Arrange：四所 credentials JSON 明文（三所共用 ApiSecretCreds，CTP 独立 schema）。
        let binance = r#"{"api_key":"bk","api_secret":"bs","api_passphrase":null,"position_mode":"hedge","is_testnet":true}"#;
        let deribit =
            r#"{"api_key":"dk","api_secret":"ds","position_mode":"one_way","is_testnet":false}"#;
        let hyperliquid = r#"{"api_key":"0xaddr","api_secret":"0xpriv"}"#;
        let ctp = r#"{"broker_id":"9999","user_id":"100001","password":"pwd","app_id":"app","auth_code":"0000","investor_id":"","front_address":"tcp://f:1","is_production_mode":false}"#;

        let cipher = SecretCipher::from_key_bytes(&SecretCipher::random_key()).unwrap();

        // Act / Assert：加密（enc.v1.）→ 解密 → 按所 schema 反序列化，往返无损。
        for (exchange, plain) in [
            ("binance", binance),
            ("deribit", deribit),
            ("hyperliquid", hyperliquid),
            ("ctp", ctp),
        ] {
            let stored = cipher.encrypt(plain).unwrap();
            assert!(
                stored.starts_with(trading_models::SECRET_PREFIX),
                "{exchange}"
            );
            let back = cipher.decrypt(&stored).unwrap();
            assert_eq!(back, plain, "{exchange}");
            match exchange {
                "ctp" => {
                    let _: CtpCreds = serde_json::from_str(&back).unwrap();
                }
                _ => {
                    let _: ApiSecretCreds = serde_json::from_str(&back).unwrap();
                }
            }
        }
    }
}

#[cfg(test)]
mod interest_alert_tests {
    use super::*;

    /// 假的兴趣供给：返回一个 deribit 期权标的，用于钉死端口契约。
    struct StubInterests;
    impl MarketInterest for StubInterests {
        fn interests(&self) -> Vec<InterestKey> {
            vec![InterestKey::new(
                Venue::Deribit,
                MarketKind::Option,
                "BTC-1",
            )]
        }
    }

    /// 记录告警的假接收口。
    #[derive(Default)]
    struct StubAlertSink {
        seen: std::sync::Mutex<Vec<SpreadAlert>>,
    }
    #[async_trait]
    impl SpreadAlertSink for StubAlertSink {
        async fn on_spread_alert(&self, alert: SpreadAlert) {
            self.seen.lock().unwrap().push(alert);
        }
    }

    #[test]
    fn market_interest_returns_interest_keys() {
        let keys = StubInterests.interests();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].venue, Venue::Deribit);
        assert_eq!(keys[0].kind, MarketKind::Option);
        assert_eq!(keys[0].symbol, "BTC-1");
    }

    #[tokio::test]
    async fn spread_alert_sink_receives_alert_payload() {
        let sink = StubAlertSink::default();
        sink.on_spread_alert(SpreadAlert {
            instrument: "BTC-1".into(),
            bid: Decimal::from(1) / Decimal::from(10),
            ask: Decimal::from(2) / Decimal::from(10),
            spread: Decimal::from(1) / Decimal::from(10),
            threshold: Decimal::from(5) / Decimal::from(1000),
        })
        .await;
        let seen = sink.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].instrument, "BTC-1");
        assert_eq!(seen[0].spread, Decimal::from(1) / Decimal::from(10));
    }
}
