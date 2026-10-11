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
#[derive(Clone)]
pub struct ExchangeCredential {
    pub api_key: String,
    pub api_secret: String,
    /// 部分所（如 Deribit/cf）需要的 passphrase；Binance 为 None。
    pub api_passphrase: Option<String>,
    /// 持仓模式：one_way（BOTH）/ hedge（LONG/SHORT）。
    pub position_mode: PositionMode,
    /// 用户级选网：true=交易所测试网。下单 REST 与账户事件流据此建连；
    /// 交易所级公共行情/规则不随账户区分、恒定正式网。
    pub is_testnet: bool,
}

impl std::fmt::Debug for ExchangeCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExchangeCredential")
            .field("api_key", &"<redacted>")
            .field("api_secret", &"<redacted>")
            .field(
                "api_passphrase",
                &self.api_passphrase.as_ref().map(|_| "<redacted>"),
            )
            .field("position_mode", &self.position_mode)
            .finish()
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
