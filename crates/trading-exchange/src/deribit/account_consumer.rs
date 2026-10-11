//! Deribit per-account 连接器：解析账户主体并启动账户事件消费。
//!
//! 消费循环（订单→sink、脏信号→去抖→REST 对账、退避重连、在途防重叠）全部
//! 复用 venue 无关的 [`crate::account_stream`]；本模块只保留：
//! - [`DeribitUserProvider`]：按账户名取 [`DeribitUser`]（provider 实现）；
//! - [`DeribitVenueConnector`]：supervisor 用的 per-account 工厂；
//! - [`DeribitAccountUser`]：把 DeribitUser 适配为 [`crate::account_stream::AccountUser`]。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::stream::BoxStream;

use crate::account_stream::{self, AccountUser};
use crate::api::{AccountEvent, AccountEventStream, ExchangeApi};
use crate::deribit::venue::DeribitUser;
use crate::port::{OrderEventSink, PositionSnapshotSink};

const EXCHANGE_NAME: &str = "deribit";

/// 用户主体供应端口（由 [`crate::deribit::provider::DeribitProvider`] 实现）。
pub trait DeribitUserProvider: Send + Sync {
    fn deribit_user(&self, account: &str) -> anyhow::Result<Arc<DeribitUser>>;
}

/// per-account 连接器。
pub struct DeribitVenueConnector {
    users: Arc<dyn DeribitUserProvider>,
    sink: Arc<dyn OrderEventSink>,
    position_sink: Arc<dyn PositionSnapshotSink>,
    debounce: Duration,
    full_interval: Duration,
}

impl DeribitVenueConnector {
    pub fn new(
        users: Arc<dyn DeribitUserProvider>,
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

    pub fn spawn_for(&self, exchange: &str, account: &str) -> Option<tokio::task::JoinHandle<()>> {
        if exchange != EXCHANGE_NAME {
            return None;
        }
        // 提前验证账户主体可解析。
        self.users.deribit_user(account).ok()?;

        let users = self.users.clone();
        let sink = self.sink.clone();
        let position_sink = self.position_sink.clone();
        let debounce = self.debounce;
        let full_interval = self.full_interval;
        let account_name = account.to_string();

        Some(tokio::spawn(async move {
            // 闭包：按账户名把具体 DeribitUser 适配为 venue 无关 AccountUser。
            let user_for = |acc: &str| -> anyhow::Result<Arc<dyn AccountUser>> {
                let user = users.deribit_user(acc)?;
                Ok(Arc::new(DeribitAccountUser(user)))
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

/// DeribitUser → AccountUser 适配（单元包装，不含额外状态）。
struct DeribitAccountUser(Arc<DeribitUser>);

#[async_trait]
impl AccountUser for DeribitAccountUser {
    async fn subscribe_events(&self) -> anyhow::Result<BoxStream<'static, AccountEvent>> {
        self.0.subscribe_account_events().await
    }

    async fn fetch_positions(&self) -> anyhow::Result<Vec<crate::api::PositionInfo>> {
        self.0.get_positions().await
    }
}
