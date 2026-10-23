//! Deribit per-account 连接器：解析账户主体并启动账户事件消费。
//!
//! 消费循环（订单→sink、脏信号→去抖→REST 对账、退避重连、在途防重叠）全部
//! 复用 venue 无关的 [`crate::account_stream`]；本模块只保留：
//! - [`DeribitUserProvider`]：按账户名取 [`DeribitUser`]（provider 实现）；
//! - [`DeribitVenueConnector`]：supervisor 用的 per-account 工厂。
//!
//! [`DeribitUser`] 同时实现 `ExchangeApi + AccountEventStream`，经
//! [`crate::account_stream`] 的 blanket impl 直接就是 `AccountUser`，
//! 无需再写机械转发壳。

use std::sync::Arc;
use std::time::Duration;

use crate::account_stream::{self, AccountUser};
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
            // DeribitUser 经 blanket impl 直接就是 AccountUser，连接器直传主体。
            let user_for = |acc: &str| -> anyhow::Result<Arc<dyn AccountUser>> {
                let user = users.deribit_user(acc)?;
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
