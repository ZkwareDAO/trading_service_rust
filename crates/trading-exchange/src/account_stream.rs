//! 跨交易所通用的账户事件消费循环（venue 无关）。
//!
//! Binance 与 Deribit 的账户事件消费逻辑仅"用户主体类型 / exchange 名"不同，
//! 其余完全一致，故统一收口在此：
//! - [`AccountUser`]：venue 侧对"账户主体"的最小端口（订阅账户流 + 拉持仓）；
//! - [`run`]：退避重连 + biased select（订单→sink、脏信号→去抖→REST 对账）；
//! - [`InFlight`]：同一账户 REST 拉取的在途标记，防并发重叠。
//!
//! 本模块不含任何具体交易所知识。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use tokio::sync::Mutex;

use crate::api::{AccountEvent, AccountEventStream, ExchangeApi};
use crate::backoff::ReconnectBackoff;
use crate::port::{OrderEventSink, PositionSnapshotSink};
use crate::reconcile::{ReconcileAction, ReconcileDebouncer};
use crate::translate::order_event;

/// 去抖/定时驱动器的检查节拍（远小于去抖窗口，仅决定触发粒度）。
const POLL_TICK: Duration = Duration::from_millis(100);

/// venue 账户主体端口：既能订阅账户事件流，又能拉 REST 持仓快照。
///
/// 各交易所的 user 类型（BinanceUser / DeribitUser）经各自 connector 的
/// 单元适配结构实现本 trait。
#[async_trait]
pub trait AccountUser: Send + Sync {
    /// 订阅账户私有事件流（含 venue 自身的建连/鉴权/重连）。
    async fn subscribe_events(&self) -> anyhow::Result<BoxStream<'static, AccountEvent>>;

    /// 拉 REST 持仓快照。
    async fn fetch_positions(&self) -> anyhow::Result<Vec<crate::api::PositionInfo>>;
}

///  blanket impl：任何同时具备交易 REST（[`ExchangeApi`]）与账户事件流
/// （[`AccountEventStream`]）的用户主体，天然就是 [`AccountUser`]——
///
/// `subscribe_events` 直接委托 `subscribe_account_events`，`fetch_positions`
/// 直接委托 `ExchangeApi::get_positions`，两方法签名一一对应，无需各交易所
/// 再写仅做转发的机械适配壳（BinanceAccountUser / DeribitAccountUser /
/// HlAccountUser 因此全部删除）。
#[async_trait]
impl<T> AccountUser for T
where
    T: ExchangeApi + AccountEventStream,
{
    async fn subscribe_events(&self) -> anyhow::Result<BoxStream<'static, AccountEvent>> {
        AccountEventStream::subscribe_account_events(self).await
    }

    async fn fetch_positions(&self) -> anyhow::Result<Vec<crate::api::PositionInfo>> {
        ExchangeApi::get_positions(self).await
    }
}

/// 时序参数转毫秒（下限 1ms，避免零窗口忙循环）。
pub fn positive_millis(d: Duration) -> i64 {
    d.as_millis().max(1) as i64
}

/// 在途 REST 标记：拉取开始置位、结束（含出错）自动复位。
pub struct InFlight(Arc<AtomicBool>);

impl InFlight {
    /// 尝试占用：空闲置位并返回 guard；已有拉取在途则 None（跳过本轮）。
    pub fn try_enter(flag: &Arc<AtomicBool>) -> Option<Self> {
        flag.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .ok()
            .map(|_| Self(Arc::clone(flag)))
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// 持续消费某账户的事件流；取主体/订阅失败按统一退避重试，任务不主动结束。
///
/// `user_for`：按账户名产出该 venue 的账户主体（失败→退避重试）。
pub async fn run<F>(
    exchange_name: &'static str,
    account: &str,
    user_for: &F,
    sink: &Arc<dyn OrderEventSink>,
    position_sink: &Arc<dyn PositionSnapshotSink>,
    debounce_ms: i64,
    full_interval_ms: i64,
) where
    F: Fn(&str) -> anyhow::Result<Arc<dyn AccountUser>>,
{
    let mut backoff = ReconnectBackoff::new();
    let debouncer = Arc::new(Mutex::new(ReconcileDebouncer::new(
        debounce_ms,
        full_interval_ms,
    )));
    let in_flight = Arc::new(AtomicBool::new(false));

    loop {
        if let Some(wait) = backoff.next_wait() {
            tokio::time::sleep(wait).await;
        }
        let user: Arc<dyn AccountUser> = match user_for(account) {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!(exchange = exchange_name, account, error = %e, "account user unavailable, retry");
                backoff.note_retry();
                continue;
            }
        };
        let mut stream: BoxStream<'static, AccountEvent> = match user.subscribe_events().await {
            Ok(s) => {
                backoff.reset();
                tracing::info!(
                    exchange = exchange_name,
                    account,
                    "account event stream active"
                );
                s
            }
            Err(e) => {
                tracing::warn!(exchange = exchange_name, account, error = %e, "account stream unavailable, retry");
                backoff.note_retry();
                continue;
            }
        };

        // （重）连上后补一次全量：强平/漏单在断线期间也能被覆盖。
        debouncer.lock().await.note_reconnected();

        let mut ticker = tokio::time::interval(POLL_TICK);
        loop {
            tokio::select! {
                // biased：事件优先于节拍，订单回报不被对账检查延后。
                biased;
                maybe_ev = stream.next() => {
                    let Some(ev) = maybe_ev else { break };
                    match ev {
                        AccountEvent::Order(o) => {
                            let mapped = order_event(&o);
                            sink.on_order_event(&mapped, exchange_name, account).await;
                        }
                        AccountEvent::Account(dirty) => {
                            let now = Utc::now().timestamp_millis();
                            debouncer.lock().await.note_dirty(&dirty, now);
                        }
                    }
                }
                _ = ticker.tick() => {
                    let now = Utc::now().timestamp_millis();
                    let action = debouncer.lock().await.poll(now);
                    if let Some(action) = action {
                        if let ReconcileAction::Dirty(syms) = &action {
                            tracing::debug!(exchange = exchange_name, account, dirty_symbols = syms.len(), "debounced reconcile");
                        }
                        spawn_fetch(
                            Arc::clone(&user),
                            exchange_name,
                            account.to_string(),
                            Arc::clone(position_sink),
                            Arc::clone(&in_flight),
                        );
                    }
                }
            }
        }
        tracing::warn!(
            exchange = exchange_name,
            account,
            "account event stream ended, reconnecting"
        );
        backoff.note_retry();
    }
}

/// 在独立任务里拉 REST 持仓快照交宿主对账；不阻塞事件消费循环。
/// 同账户已有拉取在途则放弃本轮（下个定时全量兜底，不丢最终一致）。
fn spawn_fetch(
    user: Arc<dyn AccountUser>,
    exchange_name: &'static str,
    account: String,
    position_sink: Arc<dyn PositionSnapshotSink>,
    in_flight: Arc<AtomicBool>,
) {
    let Some(_guard) = InFlight::try_enter(&in_flight) else {
        tracing::debug!(
            exchange = exchange_name,
            account,
            "position fetch in flight, skip"
        );
        return;
    };
    tokio::spawn(async move {
        match user.fetch_positions().await {
            Ok(infos) => {
                position_sink
                    .on_position_snapshot(exchange_name, &account, &infos)
                    .await;
            }
            Err(e) => {
                tracing::warn!(exchange = exchange_name, account, error = %e, "get_positions failed")
            }
        }
        // `_guard` 在此 drop，在途标记复位。
    });
}

#[cfg(test)]
mod blanket_impl_tests {
    //! Phase A：任何同时实现 ExchangeApi + AccountEventStream 的用户主体，
    //! 都应能直接作为 AccountUser 使用（blanket impl），无需各所再写机械转发壳。

    use super::*;
    use crate::api::{
        AccountEventStream, ExchangeApi, OrderAck, OrderLookup, OrderRef, PlaceOrderRequest,
        PositionInfo, ReferencePrice,
    };
    use futures_util::stream::{self, BoxStream};
    use rust_decimal::Decimal;
    use trading_models::InstrumentType;

    /// 最小双能力用户主体：仅本测试需要的两个方法有真实返回，其余 REST 方法 bail。
    struct DualUser;

    #[async_trait]
    impl ExchangeApi for DualUser {
        fn exchange(&self) -> &str {
            "dual"
        }
        async fn place_order(&self, _: &PlaceOrderRequest) -> anyhow::Result<OrderAck> {
            anyhow::bail!("unused")
        }
        async fn cancel_order(&self, _: &str, _: &OrderRef) -> anyhow::Result<OrderAck> {
            anyhow::bail!("unused")
        }
        async fn query_order(&self, _: &str, _: &OrderRef) -> OrderLookup {
            OrderLookup::Unavailable
        }
        async fn get_positions(&self) -> anyhow::Result<Vec<PositionInfo>> {
            Ok(vec![PositionInfo {
                instrument: "BTCUSDT".into(),
                instrument_type: InstrumentType::Futures,
                position_side: crate::api::OrderPositionSide::Both,
                signed_quantity: Decimal::ONE,
                entry_price: Decimal::new(100, 0),
                mark_price: None,
                unrealized_pnl: None,
                leverage: None,
                multiplier: None,
                notional: None,
                margin: None,
                update_ms: None,
            }])
        }
        async fn set_leverage(&self, _: &str, _: u32) -> anyhow::Result<()> {
            anyhow::bail!("unused")
        }
        async fn get_reference_price(&self, _: &str) -> anyhow::Result<ReferencePrice> {
            anyhow::bail!("unused")
        }
    }

    #[async_trait]
    impl AccountEventStream for DualUser {
        async fn subscribe_account_events(
            &self,
        ) -> anyhow::Result<BoxStream<'static, crate::api::AccountEvent>> {
            Ok(Box::pin(stream::empty()))
        }
    }

    #[tokio::test]
    async fn dual_user_is_account_user_via_blanket_impl() {
        // Arrange：一个只实现 ExchangeApi + AccountEventStream、没有任何手工壳的主体。
        let dual = Arc::new(DualUser);

        // Act：若无 blanket impl，本行编译失败（trait AccountUser 未实现）——即 RED。
        let account_user: Arc<dyn AccountUser> = dual.clone();

        // Assert：经 AccountUser trait 对象调度到的就是两个底层 trait 方法。
        // subscribe_events 直接对应 subscribe_account_events（空流可立即取空）。
        let mut events = account_user.subscribe_events().await.expect("empty stream");
        assert!(events.next().await.is_none());
        // fetch_positions 直接对应 ExchangeApi::get_positions。
        let positions = account_user.fetch_positions().await.expect("positions");
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].instrument, "BTCUSDT");
    }
}
