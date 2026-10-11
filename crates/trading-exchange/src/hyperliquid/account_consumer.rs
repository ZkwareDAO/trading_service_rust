//! Hyperliquid per-account 连接器：解析账户主体并启动账户事件消费。
//!
//! 消费循环（订单→sink、脏信号→去抖→REST 对账、退避重连、在途防重叠）全部
//! 复用 venue 无关的 [`crate::account_stream`]；本模块只保留：
//! - [`HlUserProvider`]：按账户名取 [`HyperliquidUser`]（provider 实现）；
//! - [`HlVenueConnector`]：supervisor 用的 per-account 工厂；
//! - [`HlAccountUser`]：把 HyperliquidUser 适配为 [`crate::account_stream::AccountUser`]。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::stream::BoxStream;

use crate::account_stream::{self, AccountUser};
use crate::api::{AccountEvent, AccountEventStream, ExchangeApi};
use crate::hyperliquid::venue::HyperliquidUser;
use crate::port::{OrderEventSink, PositionSnapshotSink};

/// 交易所规范名。
const EXCHANGE_NAME: &str = "hyperliquid";

/// 用户主体供应端口（由 provider 实现）。
pub(crate) trait HlUserProvider: Send + Sync {
    /// 按账户名构造 HyperliquidUser（REST 与账户事件流共用）。
    fn hl_user(&self, account: &str) -> anyhow::Result<Arc<HyperliquidUser>>;
}

/// per-account 连接器。
pub(crate) struct HlVenueConnector {
    users: Arc<dyn HlUserProvider>,
    sink: Arc<dyn OrderEventSink>,
    position_sink: Arc<dyn PositionSnapshotSink>,
    debounce: Duration,
    full_interval: Duration,
}

impl HlVenueConnector {
    pub(crate) fn new(
        users: Arc<dyn HlUserProvider>,
        sink: Arc<dyn OrderEventSink>,
        position_sink: Arc<dyn PositionSnapshotSink>,
        debounce: Duration,
        full_interval: Duration,
    ) -> Self {
        Self {
            users,
            sink,
            position_sink,
            debounce,
            full_interval,
        }
    }

    /// 仅处理 hyperliquid 账户：先校验主体可解析，再起通用消费任务。
    pub(crate) fn spawn_for(
        &self,
        exchange: &str,
        account: &str,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if exchange.trim() != EXCHANGE_NAME {
            return None;
        }
        // 提前验证账户主体可解析（缺凭证返回 None）。
        self.users.hl_user(account).ok()?;

        let users = self.users.clone();
        let sink = self.sink.clone();
        let position_sink = self.position_sink.clone();
        let debounce = self.debounce;
        let full_interval = self.full_interval;
        let account_name = account.to_string();

        Some(tokio::spawn(async move {
            // 闭包：把具体 HyperliquidUser 适配为 venue 无关 AccountUser。
            let user_for = |acc: &str| -> anyhow::Result<Arc<dyn AccountUser>> {
                let user = users.hl_user(acc)?;
                Ok(Arc::new(HlAccountUser(user)))
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

/// HyperliquidUser → AccountUser 适配（单元包装）。
struct HlAccountUser(Arc<HyperliquidUser>);

#[async_trait]
impl AccountUser for HlAccountUser {
    async fn subscribe_events(&self) -> anyhow::Result<BoxStream<'static, AccountEvent>> {
        self.0.subscribe_account_events().await
    }

    async fn fetch_positions(&self) -> anyhow::Result<Vec<crate::api::PositionInfo>> {
        self.0.get_positions().await
    }
}
