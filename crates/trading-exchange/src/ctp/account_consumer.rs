//! CTP per-account 连接器：启动专用会话线程（长会话，事件消费在 P3 接入）。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dashmap::DashMap;

use crate::ctp::contract_dir::ContractDir;
use crate::ctp::core::account::CtpAccount;
use crate::ctp::exchange_dir::ExchangeDir;
use crate::ctp::mailbox::SessionMailbox;
use crate::ctp::order_ref::OrderRefRegistry;
use crate::ctp::venue::{
    run_session_loop, BridgeEvent, CtpUserHandle, EventContext, RequestFlags, SessionLoop,
};
use crate::ctp::CtpConfig;
use crate::port::OrderEventSink;

const EXCHANGE_NAME: &str = "ctp";

/// Duration → 毫秒（下限 1ms，复用 account_stream 的口径）。
fn account_stream_millis(d: Duration) -> i64 {
    d.as_millis().max(1) as i64
}

/// 单账户会话的私有套件：**每账户独立实例**（停机标志 / OrderRef 映射 / 信箱）。
pub struct AccountSession {
    /// 该账户当前会话的停机标志（热加载停旧起新时随实例一起更换）。
    pub shutdown: Arc<AtomicBool>,
    /// cid ↔ OrderRef 映射（本账户私有——多账户共享会串单）。
    pub order_refs: Arc<OrderRefRegistry>,
    /// 会话请求信箱（下单/撤单/查询；本账户私有——共享会被别的账户线程取走串单）。
    pub mailbox: Arc<SessionMailbox>,
    /// 合约 → 交易所目录（本账户私有；跨会话撤单定位 ExchangeID，回报/重放预热）。
    pub exchange_dir: Arc<ExchangeDir>,
    /// 合约 → 乘数目录（本账户私有；登录后合约查询预热，持仓成本折算单价）。
    pub contract_dir: Arc<ContractDir>,
}

/// 各账户当前会话注册表（**provider 级共享**，跨热加载存活）。
///
/// 连接器每次 spawn 都可能新建实例——注册表若随连接器走则永远为空、
/// 热加载置位旧标志的语义失效（rust-review CRITICAL 修复）：必须由
/// [`crate::ctp::provider::CtpProvider`] 持有并注入每个连接器。
///
/// **每账户隔离**（rust-review CRITICAL 修复）：inserts/order_refs 曾是
/// provider 级单例——多账户的会话线程互取信箱会把订单下到错误账户。
/// 现按账户名键控，place_order 经 build_api(account) 只拿到本账户实例。
#[derive(Default)]
pub struct SessionRegistry {
    sessions: DashMap<String, Arc<AccountSession>>,
}

impl SessionRegistry {
    /// 取账户当前会话（幂等创建；place_order 下单路径使用）。
    pub fn session(&self, account: &str) -> Arc<AccountSession> {
        if let Some(s) = self.sessions.get(account) {
            return Arc::clone(s.value());
        }
        let s = self.fresh_session();
        self.sessions.insert(account.to_string(), Arc::clone(&s));
        s
    }

    /// 热加载接管：置位旧会话停机标志（旧线程在下一轮循环退出），并为
    /// 本账户创建**全新**私有实例（旧 order_refs/inserts 随旧会话废弃；
    /// OrderRef 播种策略保证新实例不与历史小号空间重叠）。
    pub fn take_over(&self, account: &str) -> Arc<AccountSession> {
        let s = self.fresh_session();
        if let Some(old) = self.sessions.insert(account.to_string(), Arc::clone(&s)) {
            old.shutdown.store(true, Ordering::SeqCst);
        }
        s
    }

    /// 读取账户当前会话的停机标志（接线测试/诊断用）。
    pub fn current_shutdown(&self, account: &str) -> Option<Arc<AtomicBool>> {
        self.sessions
            .get(account)
            .map(|e| Arc::clone(&e.value().shutdown))
    }

    /// 优雅停机：置位全部当前标志（spawn_blocking 线程无法 abort，
    /// 停机标志是唯一的停机通道）。
    pub fn shutdown_all(&self) {
        for entry in self.sessions.iter() {
            entry.value().shutdown.store(true, Ordering::SeqCst);
        }
    }

    fn fresh_session(&self) -> Arc<AccountSession> {
        Arc::new(AccountSession {
            shutdown: Arc::new(AtomicBool::new(false)),
            order_refs: Arc::new(OrderRefRegistry::new()),
            mailbox: Arc::new(SessionMailbox::new()),
            exchange_dir: Arc::new(ExchangeDir::new()),
            contract_dir: Arc::new(ContractDir::new()),
        })
    }
}

/// 每账户常驻事件消费任务：FIFO 串行驱动 async 端口（保序 + 有界背压）。
///
/// 会话线程经 [`BridgeEvent`] 入队（`blocking_send`，队列满即阻塞 CTP 线程，
/// 天然背压不丢单）；Sender 随会话线程退出而 drop，本任务 drain 完自动结束。
/// 替代 `now_or_never`——后者遇 Pending 即丢事件，策略端收不到订单回报
/// （rust-review CRITICAL 修复）。
pub(crate) async fn run_event_consumer(
    account: String,
    mut rx: tokio::sync::mpsc::Receiver<BridgeEvent>,
    sink: Arc<dyn OrderEventSink>,
    position_sink: Arc<dyn crate::port::PositionSnapshotSink>,
) {
    while let Some(ev) = rx.recv().await {
        match ev {
            BridgeEvent::Order(e) => sink.on_order_event(&e, "ctp", &account).await,
            BridgeEvent::Positions(ps) => {
                position_sink
                    .on_position_snapshot("ctp", &account, &ps)
                    .await
            }
        }
    }
}

/// 消费任务守护：任务 panic/被取消时打 error（CTP 是交易路径——消费任务
/// 静默死亡 = 该账户订单回报/持仓快照全部丢失且无人感知）。
pub(crate) async fn monitor_consumer(account: String, handle: tokio::task::JoinHandle<()>) {
    if handle.await.is_err() {
        tracing::error!(
            account = %account,
            "CTP 事件消费任务异常终止（panic），该账户订单回报/持仓快照将不再分发"
        );
    }
}

/// CTP per-account 连接器（supervisor 用工厂）。
///
/// 每账户起专用会话线程 + 常驻事件消费任务；停机标志存于 provider 级
/// [`SessionRegistry`]（见其文档：热加载停旧会话的关键）。
pub struct CtpVenueConnector {
    sink: Arc<dyn OrderEventSink>,
    position_sink: Arc<dyn crate::port::PositionSnapshotSink>,
    debounce: Duration,
    full_interval: Duration,
    config: CtpConfig,
    /// 本账户的会话私有套件（由 provider 经 `take_over` 分配）。
    session: Arc<AccountSession>,
}

impl CtpVenueConnector {
    pub fn new(
        sink: Arc<dyn OrderEventSink>,
        position_sink: Arc<dyn crate::port::PositionSnapshotSink>,
        debounce: Duration,
        full_interval: Duration,
        config: CtpConfig,
        session: Arc<AccountSession>,
    ) -> Self {
        Self {
            sink,
            position_sink,
            debounce,
            full_interval,
            config,
            session,
        }
    }

    /// 为账户起专用会话线程；非本所账户返回 None。
    ///
    /// 线程内完成建连/登录三步/事件消费/退避重连；事件经事件桥投递 async
    /// 端口；生产模式开关取自账户凭证（评测版合并 API 的实例级参数）。
    pub fn spawn_for(
        &self,
        exchange: &str,
        account: &str,
        acct: CtpAccount,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if exchange != EXCHANGE_NAME {
            return None;
        }
        // 热加载：同账号新会话前先停旧会话（注册表接管，置位旧标志并分配
        // 全新私有实例）。停机标志供三处共享：CtpUserHandle（跨线程信箱）、
        // SessionLoop（登录窗口）与事件桥退出（EventContext drop）。
        let session = Arc::clone(&self.session);
        let shutdown = Arc::clone(&session.shutdown);
        let session_shutdown = Arc::clone(&shutdown);
        let handle = Arc::new(CtpUserHandle {
            account: account.to_string(),
            flags: Arc::new(RequestFlags::default()),
            session_epoch: Arc::new(AtomicU64::new(0)),
            shutdown,
            filled_by_sys_id: Arc::new(Mutex::new(std::collections::HashMap::new())),
            mailbox: Arc::clone(&session.mailbox),
        });
        // 事件桥：有界队列 + 常驻消费任务（Sender 随会话线程退出而 drop）。
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        let consumer = tokio::spawn(run_event_consumer(
            account.to_string(),
            rx,
            Arc::clone(&self.sink),
            Arc::clone(&self.position_sink),
        ));
        tokio::spawn(monitor_consumer(account.to_string(), consumer));
        // 流文件按账户隔离（每 API 实例独立 con 文件，避免多账户互踩）。
        let flow_path = format!(
            "{}/{}",
            self.config.flow_path.trim_end_matches('/'),
            account
        );
        let _ = std::fs::create_dir_all(&flow_path);
        let is_production_mode = acct.is_production_mode;
        let session_loop = SessionLoop::new(
            acct,
            self.config.trader_so.clone(),
            flow_path,
            is_production_mode,
            session_shutdown,
        );
        // 事件分发上下文：事件桥发送端 + 线程内去抖器（复用跨所通用纯决策内核）。
        let ctx = Arc::new(EventContext {
            account: account.to_string(),
            order_refs: Arc::clone(&session.order_refs),
            exchange_dir: Arc::clone(&session.exchange_dir),
            contract_dir: Arc::clone(&session.contract_dir),
            tx,
            debouncer: Arc::new(Mutex::new(crate::reconcile::ReconcileDebouncer::new(
                account_stream_millis(self.debounce),
                account_stream_millis(self.full_interval),
            ))),
        });
        let thread_handle = Arc::clone(&handle);
        Some(tokio::task::spawn_blocking(move || {
            run_session_loop(session_loop, thread_handle, ctx);
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::PositionInfo;
    use async_trait::async_trait;
    use std::sync::Mutex;

    // ---- 会话停机注册表（provider 级共享，跨热加载存活）----

    #[test]
    fn take_over_sets_previous_shutdown() {
        // Arrange / Act：同一账户连续两次接管（模拟热加载 spawn 新会话）。
        let reg = SessionRegistry::default();
        let first = reg.session("acct1");
        let second = reg.take_over("acct1");

        // Assert：旧会话停机标志被置位，新会话实例未置位。
        assert!(first.shutdown.load(Ordering::SeqCst));
        assert!(!second.shutdown.load(Ordering::SeqCst));
    }

    #[test]
    fn different_accounts_have_independent_sessions() {
        // Arrange / Act：两个账户各持各的会话实例。
        let reg = SessionRegistry::default();
        let a = reg.take_over("acctA");
        let b = reg.take_over("acctB");

        // Assert：停机标志互不影响。
        assert!(!a.shutdown.load(Ordering::SeqCst));
        assert!(!b.shutdown.load(Ordering::SeqCst));
    }

    #[test]
    fn sessions_are_per_account_isolated() {
        // Arrange：两个账户分别取得会话实例。
        let reg = SessionRegistry::default();
        let a = reg.session("acctA");
        let b = reg.session("acctB");

        // Assert：order_refs / mailbox 每账户独立（CRITICAL 回归测试——
        // 曾为 provider 级单例，多账户会话线程互取信箱导致串单）。
        assert!(!std::sync::Arc::ptr_eq(&a.order_refs, &b.order_refs));
        assert!(!std::sync::Arc::ptr_eq(&a.mailbox, &b.mailbox));
        // 同账户幂等：place_order 与会话线程拿到同一份。
        assert!(std::sync::Arc::ptr_eq(
            &reg.session("acctA").mailbox,
            &a.mailbox
        ));
    }

    #[test]
    fn shutdown_all_sets_every_flag() {
        // Arrange：两账户各持一个未置位标志。
        let reg = SessionRegistry::default();
        let a = reg.take_over("acctA");
        let b = reg.take_over("acctB");
        assert!(!a.shutdown.load(Ordering::SeqCst));
        assert!(!b.shutdown.load(Ordering::SeqCst));

        // Act：优雅停机，置位全部当前标志。
        reg.shutdown_all();

        // Assert：全部置位（所有会话线程在下一轮循环退出）。
        assert!(a.shutdown.load(Ordering::SeqCst));
        assert!(b.shutdown.load(Ordering::SeqCst));
    }

    // ---- 事件桥（有界队列 + 每账户常驻消费任务）----

    /// 记录 (种类, client_order_id) 调用序列的假接收口。
    #[derive(Default)]
    struct RecordingSink {
        calls: Mutex<Vec<(&'static str, String)>>,
    }
    #[async_trait]
    impl OrderEventSink for RecordingSink {
        async fn on_order_event(
            &self,
            ev: &trading_core::protocol::OrderEvent,
            _source: &str,
            _account: &str,
        ) {
            self.calls
                .lock()
                .unwrap()
                .push(("order", ev.client_order_id.clone()));
        }
    }
    #[async_trait]
    impl crate::port::PositionSnapshotSink for RecordingSink {
        async fn on_position_snapshot(
            &self,
            _exchange: &str,
            _account: &str,
            _positions: &[PositionInfo],
        ) {
            self.calls
                .lock()
                .unwrap()
                .push(("positions", String::new()));
        }
    }

    fn order_event(cid: &str) -> trading_core::protocol::OrderEvent {
        trading_core::protocol::OrderEvent {
            client_order_id: cid.into(),
            status: "NEW".into(),
            exchange_order_id: None,
            filled_price: None,
            filled_quantity: None,
            exchange_update_time: None,
            message: None,
            instrument: String::new(),
            direction: None,
            reduce_only: false,
            amount: None,
            limit_price: None,
        }
    }

    /// 首个订单事件即 panic 的假接收口（验证 monitor 感知消费任务异常）。
    struct PanicSink;
    #[async_trait]
    impl OrderEventSink for PanicSink {
        async fn on_order_event(&self, _ev: &trading_core::protocol::OrderEvent, _: &str, _: &str) {
            panic!("consumer exploded");
        }
    }
    #[async_trait]
    impl crate::port::PositionSnapshotSink for PanicSink {
        async fn on_position_snapshot(&self, _: &str, _: &str, _: &[PositionInfo]) {}
    }

    #[tokio::test]
    async fn consumer_panic_is_observed_by_monitor() {
        // Arrange：消费任务在首个订单事件上 panic。
        let (tx, rx) = tokio::sync::mpsc::channel::<BridgeEvent>(4);
        let consumer = tokio::spawn(run_event_consumer(
            "acct".into(),
            rx,
            Arc::new(PanicSink) as Arc<dyn OrderEventSink>,
            Arc::new(PanicSink) as Arc<dyn crate::port::PositionSnapshotSink>,
        ));

        // Act：monitor 应感知 panic 并结束（而非静默吞掉）。
        let monitored = tokio::spawn(monitor_consumer("acct".into(), consumer));
        drop(tx);
        tokio::time::timeout(std::time::Duration::from_secs(2), monitored)
            .await
            .expect("monitor 必须感知消费任务 panic 并结束")
            .unwrap();
    }

    #[tokio::test]
    async fn event_consumer_delivers_in_fifo_and_routes() {
        // Arrange：订单/仓位混排入队。
        let sink = Arc::new(RecordingSink::default());
        let (tx, rx) = tokio::sync::mpsc::channel::<BridgeEvent>(8);
        let handle = tokio::spawn(run_event_consumer(
            "acct".into(),
            rx,
            Arc::clone(&sink) as Arc<dyn OrderEventSink>,
            Arc::clone(&sink) as Arc<dyn crate::port::PositionSnapshotSink>,
        ));

        // Act：o1 → 快照 → o2（同一队列，FIFO）。
        tx.send(BridgeEvent::Order(Box::new(order_event("o1"))))
            .await
            .unwrap();
        tx.send(BridgeEvent::Positions(Vec::new())).await.unwrap();
        tx.send(BridgeEvent::Order(Box::new(order_event("o2"))))
            .await
            .unwrap();
        drop(tx); // 会话线程退出 → Sender drop → 消费任务 drain 后结束。

        handle.await.unwrap();
        let calls = sink.calls.lock().unwrap().clone();

        // Assert：跨端口类型严格保序、路由正确。
        assert_eq!(
            calls,
            vec![
                ("order", "o1".into()),
                ("positions", String::new()),
                ("order", "o2".into())
            ]
        );
    }
}
