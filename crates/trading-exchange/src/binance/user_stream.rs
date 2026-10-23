//! Binance per-account 连接器：解析账户主体并启动账户事件消费。
//!
//! 消费循环（订单→sink、脏信号→去抖→REST 对账、退避重连、在途防重叠）全部
//! 复用 venue 无关的 [`crate::account_stream`]；本模块只保留：
//! - [`BinanceUserProvider`]：按账户名取 [`BinanceUser`]；
//! - [`BinanceVenueConnector`]：supervisor 用的 per-account 工厂。
//!
//! [`BinanceUser`] 同时实现 `ExchangeApi + AccountEventStream`，经
//! [`crate::account_stream`] 的 blanket impl 直接就是 `AccountUser`，
//! 无需再写机械转发壳。

use std::sync::Arc;
use std::time::Duration;

use crate::account_stream::{self, AccountUser};
use crate::binance::venue::BinanceUser;
use crate::port::{CredentialResolver, ExchangeCredential, OrderEventSink, PositionSnapshotSink};

/// binance 唯一规范名（大小写敏感精确匹配，无别名）。
const EXCHANGE_NAME: &str = "binance";

/// 用户主体供应端口：按账户名构造/复用具体 [`BinanceUser`]
/// （REST 与账户事件共用同一用户主体）。由宿主的交易所路由器实现。
pub trait BinanceUserProvider: Send + Sync {
    fn binance_user(&self, account: &str) -> anyhow::Result<Arc<BinanceUser>>;
}

/// per-account 连接器。
pub struct BinanceVenueConnector {
    /// 凭证解析端口（账户热加载、解密由宿主负责）。
    resolver: Arc<dyn CredentialResolver>,
    /// 订单回报出口（推进宿主订单状态机 + 回推策略）。
    sink: Arc<dyn OrderEventSink>,
    /// 仓位快照出口（去抖到点、拉完 REST 后交宿主对账落库）。
    position_sink: Arc<dyn PositionSnapshotSink>,
    /// 具体 BinanceUser 供应端口（复用 SDK client 缓存与共享规则表）。
    users: Arc<dyn BinanceUserProvider>,
    /// 仓位脏信号去抖窗口。
    debounce: Duration,
    /// 仓位定时全量对账周期。
    full_interval: Duration,
}

impl BinanceVenueConnector {
    pub fn new(
        resolver: Arc<dyn CredentialResolver>,
        sink: Arc<dyn OrderEventSink>,
        users: Arc<dyn BinanceUserProvider>,
        position_sink: Arc<dyn PositionSnapshotSink>,
        debounce: Duration,
        full_interval: Duration,
    ) -> Self {
        // 选网内含在 BinanceUser（users 供应端口按账户凭证 is_testnet 构造）。
        Self {
            resolver,
            sink,
            position_sink,
            users,
            debounce,
            full_interval,
        }
    }

    /// 仅精确名为 binance 的账户返回解析后的凭证；别名/大小写不一致/未知返回 None。
    pub fn resolve_supported(&self, exchange: &str, account: &str) -> Option<ExchangeCredential> {
        if exchange != EXCHANGE_NAME {
            return None;
        }
        self.resolver.resolve(account, EXCHANGE_NAME)
    }

    /// 启动某账户的账户事件流消费任务；账户不受支持/缺失凭证则返回 None。
    pub fn spawn_for(&self, exchange: &str, account: &str) -> Option<tokio::task::JoinHandle<()>> {
        self.resolve_supported(exchange, account)?;
        let users = self.users.clone();
        let sink = self.sink.clone();
        let position_sink = self.position_sink.clone();
        let debounce = self.debounce;
        let full_interval = self.full_interval;
        let account_name = account.to_string();

        Some(tokio::spawn(async move {
            // BinanceUser 经 blanket impl 直接就是 AccountUser，连接器直传主体。
            let user_for = |acc: &str| -> anyhow::Result<Arc<dyn AccountUser>> {
                let user = users.binance_user(acc)?;
                Ok(user as Arc<dyn AccountUser>)
            };
            account_stream::run(
                EXCHANGE_NAME,
                &account_name,
                &user_for,
                &sink,
                &position_sink,
                account_stream::positive_millis(debounce),
                account_stream::positive_millis(full_interval),
            )
            .await;
        }))
    }
}
