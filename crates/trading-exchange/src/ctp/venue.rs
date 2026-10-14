//! CTP 用户主体：专用 OS 线程持有 `TraderHandle`，同步完成建连/认证/登录/
//! 结算确认，并把事件循环整体留在该线程内消费（不进入 async 栈）。
//!
//! 线程模型（对应方案 §4.1/§4.8）：
//! * `TraderHandle` 及其全部请求（下单/查询）只在专用线程内使用；
//! * 事件帧在线程内映射后经事件桥（有界 mpsc 队列 + 每账户常驻消费任务，
//!   见 [`BridgeEvent`]）投递 `Send+Sync` async 端口，逐账户 FIFO 保序；
//! * 断线重连（backoff 阶梯）与重连补全量也在该线程内完成；
//! * tokio 侧只持有 [`CtpUserHandle`]，跨线程置位请求信箱（原子标志）。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::backoff::ReconnectBackoff;
use crate::ctp::core::account::CtpAccount;
use crate::ctp::gateway::trader::TraderHandle;

/// 事件等待超时：空闲轮询粒度（同时是"空闲读超时判死连"的窗口下限）。
const EVENT_POLL_TIMEOUT: Duration = Duration::from_secs(2);

/// 登录三步总窗口：认证/登录应答秒级，结算确认在交易日切换时可能需下载
/// 结算单，放宽到 5 分钟。
const LOGIN_DEADLINE: Duration = Duration::from_secs(300);

/// 登录三步状态机（认证 → 登录 → 结算确认）；仅长会话线程使用
/// （下单已经由信箱在长会话内提交，`CtpApi` 不再直接调用）。
///
/// `acct` 目前未消费（凭据已编码进 `TraderHandle`），保留参数位供
/// 看穿式登录扩展。
pub(crate) fn login_ready(
    h: &mut TraderHandle,
    _acct: &CtpAccount,
    stop: &AtomicBool,
) -> anyhow::Result<()> {
    use crate::ctp::core::event::CtpEvent;
    let deadline = std::time::Instant::now() + LOGIN_DEADLINE;

    // ① 事件驱动等前置连接：Init 后 FrontConnected 事件未到达时，
    //    req_authenticate 会被柜台忽略（受理码 -1/-2），请求根本发不出去——
    //    盲发会导致「5 分钟等一个从未发出的请求」（登录三步超时的真实根因）。
    loop {
        if stop.load(Ordering::SeqCst) {
            anyhow::bail!("CTP 登录中止（收到停机信号）");
        }
        if std::time::Instant::now() > deadline {
            anyhow::bail!("CTP 登录超时：前置连接事件未到达");
        }
        match h.next_event(EVENT_POLL_TIMEOUT) {
            Some(CtpEvent::FrontConnected) => break,
            Some(CtpEvent::FrontDisconnected(reason)) => {
                anyhow::bail!("CTP 前置断开（连接阶段）: {reason}")
            }
            _ => {}
        }
    }

    // ② 认证：受理码非 0（前置未就绪/流控）立即失败，绝不空等应答。
    let ret = h.authenticate();
    if ret != 0 {
        anyhow::bail!("CTP 认证请求受理失败 code={ret}");
    }

    // ③ 登录三步状态机（每步受理码同样检查）。
    let mut stage = LoginStage::Authenticating;
    loop {
        if stop.load(Ordering::SeqCst) {
            anyhow::bail!("CTP 登录三步中止（收到停机信号）");
        }
        if std::time::Instant::now() > deadline {
            anyhow::bail!("CTP 登录三步超时（认证/登录/结算确认未完成）");
        }
        let Some(ev) = h.next_event(EVENT_POLL_TIMEOUT) else {
            continue;
        };
        match (stage, ev) {
            (_, CtpEvent::FrontDisconnected(reason)) => {
                anyhow::bail!("CTP 前置断开（登录阶段）: {reason}")
            }
            (LoginStage::Authenticating, CtpEvent::Authenticated { code, msg }) => {
                if code != 0 {
                    anyhow::bail!("CTP 认证被拒 code={code} msg={msg}");
                }
                let ret = h.login();
                if ret != 0 {
                    anyhow::bail!("CTP 登录请求受理失败 code={ret}");
                }
                stage = LoginStage::LoggingIn;
            }
            (LoginStage::LoggingIn, CtpEvent::Login(_)) => {
                let ret = h.confirm_settlement();
                if ret != 0 {
                    anyhow::bail!("CTP 结算确认请求受理失败 code={ret}");
                }
                stage = LoginStage::ConfirmingSettlement;
            }
            (LoginStage::ConfirmingSettlement, CtpEvent::SettlementConfirmed) => {
                return Ok(());
            }
            // 其余事件（如报单拒的 Error）在登录阶段忽略。
            _ => {}
        }
    }
}

/// 一次建连会话（connect → 登录三步 → 事件循环）的线程内状态。
pub(crate) struct SessionLoop {
    /// 账户凭证（重连重建句柄用）。
    account: CtpAccount,
    /// 柜台 .so 路径。
    lib_path: String,
    /// 流文件目录。
    flow_path: String,
    /// 评测版 API 接仿真必须 false，生产柜台 true。
    is_production_mode: bool,
    /// 停机标志（与会话线程同一份；登录三步窗口内逐轮检查，停机立即中止）。
    shutdown: Arc<AtomicBool>,
}

impl SessionLoop {
    pub(crate) fn new(
        account: CtpAccount,
        lib_path: String,
        flow_path: String,
        is_production_mode: bool,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            account,
            lib_path,
            flow_path,
            is_production_mode,
            shutdown,
        }
    }

    /// 建连并完成认证/登录/结算确认（阻塞直到就绪）。
    ///
    /// 登录三步的状态机见 [`login_ready`]（与短会话共用同一份实现）。
    pub(crate) fn connect_and_login(&self) -> anyhow::Result<TraderHandle> {
        let mut h = TraderHandle::connect(
            &self.account,
            &self.lib_path,
            &self.flow_path,
            self.is_production_mode,
        )
        .map_err(|e| anyhow::anyhow!("CTP 建连失败: {e}"))?;
        login_ready(&mut h, &self.account, &self.shutdown)?;
        Ok(h)
    }
}

/// 登录三步的阶段状态机（认证 → 登录 → 结算确认）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoginStage {
    Authenticating,
    LoggingIn,
    ConfirmingSettlement,
}

/// 跨线程请求信箱：tokio 侧置位，专用线程在事件空闲时串行执行。
///
/// 全部为原子标志，无锁、无 channel；请求串行化由专用线程保证。
#[derive(Default)]
pub(crate) struct RequestFlags {
    /// 请求全量持仓查询。
    pub query_positions: AtomicBool,
    /// 请求当日委托查询（快照恢复）。
    pub query_orders: AtomicBool,
}

/// 跨线程句柄：tokio 侧对 CTP 会话的唯一下单/查询入口。
pub struct CtpUserHandle {
    /// 账户名（日志/端口回调标识）。
    pub account: String,
    /// 请求信箱。
    pub(crate) flags: Arc<RequestFlags>,
    /// 会话代际（每次重连 +1；tokio 侧可据此观察断连）。
    pub(crate) session_epoch: Arc<AtomicU64>,
    /// 停机标志（Drop 置位，线程自行退出并释放 .so）。
    pub(crate) shutdown: Arc<AtomicBool>,
    /// OrderSysID → 累计成交量（专用线程写，tokio 侧读；Mutex 保护而非 channel）。
    /// 表项生存期 = 该单首笔成交 → 终态回报（终态钩子回收，见
    /// [`release_terminal_tables`]）；崩溃恢复不依赖本表——重连补快照的
    /// 委托回报自带柜台权威累计 `volume_traded`。
    pub(crate) filled_by_sys_id: Arc<Mutex<std::collections::HashMap<String, i64>>>,
    /// 下单请求信箱（async 侧投递 → 本线程消费；provider 级共享）。
    pub(crate) inserts: Arc<crate::ctp::mailbox::InsertMailbox>,
}

impl CtpUserHandle {
    /// 请求一次全量持仓查询（异步侧零阻塞：仅原子置位）。
    pub fn request_positions(&self) {
        self.flags.query_positions.store(true, Ordering::SeqCst);
    }

    /// 请求一次当日委托查询（重连补快照）。
    pub fn request_orders(&self) {
        self.flags.query_orders.store(true, Ordering::SeqCst);
    }

    /// 当前会话代际（断线重连后递增）。
    pub fn session_epoch(&self) -> u64 {
        self.session_epoch.load(Ordering::SeqCst)
    }

    /// 读某 OrderSysID 的累计成交量（无记录返回 0）。
    pub fn filled_of(&self, order_sys_id: &str) -> i64 {
        self.filled_by_sys_id
            .lock()
            .map(|m| m.get(order_sys_id).copied().unwrap_or(0))
            .unwrap_or(0)
    }
}

impl Drop for CtpUserHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
}

/// 会话主循环（在专用线程内运行）：建连成功即消费事件，断线按 backoff
/// 退避重建，停机标志置位后退出。事件的具体分发（订单/持仓→端口）在
/// P3/P4 接入 `inner_loop`。
pub(crate) fn run_session_loop(
    session: SessionLoop,
    handle: Arc<CtpUserHandle>,
    ctx: Arc<EventContext>,
) {
    let mut backoff = ReconnectBackoff::new();
    while !handle.shutdown.load(Ordering::SeqCst) {
        if let Some(wait) = backoff.next_wait() {
            // 阻塞线程内睡眠（std::thread，不是 tokio）；分片检查停机标志，
            // 保证优雅停机不被最长 5 分钟的退避睡眠拖住。
            sleep_interruptibly(wait, &handle.shutdown);
        }
        match session.connect_and_login() {
            Ok(mut h) => {
                backoff.reset();
                handle.session_epoch.fetch_add(1, Ordering::SeqCst);
                tracing::info!(account = %handle.account, "CTP 会话就绪");
                // 就绪后先补一次全量（对齐"重连后补全量"约束）。
                handle.flags.query_positions.store(true, Ordering::SeqCst);
                handle.flags.query_orders.store(true, Ordering::SeqCst);
                inner_loop(&mut h, &handle, &ctx);
                tracing::warn!(account = %handle.account, "CTP 会话断开，退避重连");
            }
            Err(e) => {
                tracing::warn!(account = %handle.account, error = %e, "CTP 建连失败");
            }
        }
        backoff.note_retry();
    }
    tracing::info!(account = %handle.account, "CTP 会话线程退出");
}

/// 事件内循环：消费事件帧 + 空闲时处理请求信箱。断开（返回）由
/// `FrontDisconnected` 或停机标志触发。
///
/// 事件分发（P3/P4）：
/// * `Order/Trade` → mapping → 事件桥 → `OrderEventSink`（消费任务串行 await）；
/// * `Position*N + QueryEnd` → 线程内缓冲聚合 → 事件桥 → `PositionSnapshotSink`；
/// * 订单/成交/持仓事件喂线程内去抖器（事件只当脏信号）。
fn inner_loop(h: &mut TraderHandle, handle: &Arc<CtpUserHandle>, ctx: &EventContext) {
    use crate::ctp::core::event::CtpEvent;
    // 持仓查询应答的线程内缓冲（QueryEnd 聚合，无锁无 channel）。
    let mut position_buf: Vec<crate::ctp::core::event::PositionSnapshot> = Vec::new();
    loop {
        if handle.shutdown.load(Ordering::SeqCst) {
            return;
        }
        // 请求信箱（原子标志）在每次事件等待前检查：置位即串行执行。
        if handle.flags.query_positions.swap(false, Ordering::SeqCst) {
            h.query_positions();
        }
        if handle.flags.query_orders.swap(false, Ordering::SeqCst) {
            h.query_orders("");
        }
        // 下单信箱：async 侧投递的待提交请求在本会话（已登录实例）内串行执行。
        if let Some(p) = handle.inserts.take() {
            // 受理码回执：校验/FFI 错误一并随 Result 传回 async 侧。
            let ret = h
                .insert_order(&p.req, &p.order_ref)
                .map_err(|e| anyhow::anyhow!("CTP 报单校验失败: {e}"));
            let _ = p.reply.send(ret);
        }
        match h.next_event(EVENT_POLL_TIMEOUT) {
            Some(CtpEvent::FrontDisconnected(_)) => return,
            Some(ev) => dispatch_event(ev, handle, ctx, &mut position_buf),
            None => {
                // 空闲：去抖到点则触发持仓查询（定时全量兜底也由
                // ReconcileDebouncer 的 full_interval 承担）。
                let now = now_ms();
                if let Some(_action) = ctx.debouncer.lock().unwrap().poll(now) {
                    h.query_positions();
                }
            }
        }
    }
}

/// 会话线程 → async 端口消费任务的事件桥载荷（有界队列，逐账户 FIFO 保序）。
///
/// CTP 事件产自专用阻塞线程，而端口是 async——桥接用 `tokio::sync::mpsc`
/// 有界队列：会话线程 `blocking_send`（满则阻塞 = 天然背压），连接器 spawn 的
/// 常驻消费任务串行 `recv → sink.await`。FFI/适配边界的有界队列属红线容忍区
/// （与 gateway 内部 `std::sync::mpsc` 同理）；替代 `now_or_never`（遇 Pending
/// 即丢事件，策略端收不到回报）。
#[derive(Debug)]
pub(crate) enum BridgeEvent {
    /// 委托/成交回报映射出的订单事件（Box 压平变体尺寸差）。
    Order(Box<trading_core::protocol::OrderEvent>),
    /// 一次查询收齐的持仓快照。
    Positions(Vec<crate::api::PositionInfo>),
}

/// 事件分发的共享上下文（事件桥发送端 + 去抖器），由连接器构造后传入线程。
pub(crate) struct EventContext {
    pub account: String,
    /// cid ↔ OrderRef 映射（回报反查长 cid；与下单短会话共享）。
    pub order_refs: Arc<crate::ctp::order_ref::OrderRefRegistry>,
    /// 事件桥发送端；随会话线程结束（EventContext drop）而关闭，消费任务随之退出。
    pub tx: tokio::sync::mpsc::Sender<BridgeEvent>,
    /// 线程内去抖器（复用跨所通用纯决策内核）。
    pub debouncer: Arc<std::sync::Mutex<crate::reconcile::ReconcileDebouncer>>,
}

/// 可中断睡眠：按 200ms 分片睡 `wait`，每片间隙检查停机标志。
///
/// 退避阶梯最长 5 分钟——若用整段 `thread::sleep`，优雅停机要等睡醒才能
/// 检查标志（tokio runtime drop 会等 spawn_blocking 线程，进程看似挂死）。
/// 分片后停机延迟上界 = 分片粒度。
pub(crate) fn sleep_interruptibly(wait: Duration, shutdown: &AtomicBool) {
    const SLICE: Duration = Duration::from_millis(200);
    let deadline = std::time::Instant::now() + wait;
    while std::time::Instant::now() < deadline {
        if shutdown.load(Ordering::SeqCst) {
            return;
        }
        std::thread::sleep(
            SLICE.min(deadline.saturating_duration_since(std::time::Instant::now())),
        );
    }
}

/// 回报反查：OrderRef → 本地长 cid。
///
/// 命中映射即还原；未命中（重启前订单的孤儿回报 / ≤13B 透传 cid）保留
/// 原值——宿主按 `exchange_order_id` 回查 fallback 兜底，warn 可诊断。
fn resolve_event_cid(ctx: &EventContext, order_ref: &str) -> String {
    ctx.order_refs.cid_of(order_ref).unwrap_or_else(|| {
        // debug：重连补快照会重放当日全部回报，重启后必然整批未命中；
        // 命中信号由宿主 fallback 的 info 日志承载。
        tracing::debug!(
            account = %ctx.account,
            order_ref,
            "回报 OrderRef 未命中映射，保留原值投递"
        );
        order_ref.to_string()
    })
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 单事件分发：读到即推端口，无跨帧积压。
fn dispatch_event(
    ev: crate::ctp::core::event::CtpEvent,
    handle: &Arc<CtpUserHandle>,
    ctx: &EventContext,
    position_buf: &mut Vec<crate::ctp::core::event::PositionSnapshot>,
) {
    use crate::ctp::core::event::CtpEvent;
    match ev {
        CtpEvent::Order(s) => {
            // 委托回报：映射后经事件桥投订单端口；同时喂脏信号。
            let state = crate::ctp::mapping::order_state_from_snapshot(&s);
            // 回报反查：OrderRef → 本地长 cid（未命中保留原值，宿主 fallback 兜底）。
            let mut event = crate::translate::order_event(&state);
            event.client_order_id = resolve_event_cid(ctx, &s.order_ref);
            // 终态清理钩子：反查完成后回收两张会话表（cid↔OrderRef 映射与
            // 成交累计），防会话内无限增长；迟到/重放回报经宿主 fallback 兜底。
            release_terminal_tables(&ctx.order_refs, &handle.filled_by_sys_id, &s);
            // 阻塞线程 → async 端口经有界队列投递（满则阻塞背压，不丢单）。
            if let Err(e) = ctx.tx.blocking_send(BridgeEvent::Order(Box::new(event))) {
                tracing::warn!(account = %ctx.account, "事件桥已关闭，订单事件丢弃：{e}");
            }
            ctx.debouncer.lock().unwrap().note_dirty(
                &crate::api::AccountDirty {
                    instruments: vec![s.instrument.clone()],
                },
                now_ms(),
            );
        }
        CtpEvent::Trade(t) => {
            // 成交回报：按 OrderRef 累计成交量后推订单端口。
            let key = t.order_sys_id.clone();
            let cum = {
                let mut m = handle.filled_by_sys_id.lock().unwrap();
                let e = m.entry(key.clone()).or_insert(0);
                *e += t.volume.max(0) as i64;
                *e
            };
            let state =
                crate::ctp::mapping::order_state_from_trade(&t, rust_decimal::Decimal::from(cum));
            // 回报反查：OrderRef → 本地长 cid（未命中保留原值，宿主 fallback 兜底）。
            let mut event = crate::translate::order_event(&state);
            event.client_order_id = resolve_event_cid(ctx, &t.order_ref);
            // 阻塞线程 → async 端口经有界队列投递（满则阻塞背压，不丢单）。
            if let Err(e) = ctx.tx.blocking_send(BridgeEvent::Order(Box::new(event))) {
                tracing::warn!(account = %ctx.account, "事件桥已关闭，成交回报丢弃：{e}");
            }
            ctx.debouncer.lock().unwrap().note_dirty(
                &crate::api::AccountDirty {
                    instruments: vec![t.instrument.clone()],
                },
                now_ms(),
            );
        }
        CtpEvent::Position(p) => {
            // 持仓应答逐条缓冲（数量为 0 的行也保留，由映射层过滤）。
            position_buf.push(p);
        }
        CtpEvent::QueryEnd => {
            // 一次查询收齐：聚合为快照经事件桥投仓位端口。
            let positions: Vec<crate::api::PositionInfo> = position_buf
                .drain(..)
                .filter_map(|p| crate::ctp::mapping::position_info_from_snapshot(&p))
                .collect();
            if let Err(e) = ctx.tx.blocking_send(BridgeEvent::Positions(positions)) {
                tracing::warn!(account = %ctx.account, "事件桥已关闭，持仓快照丢弃：{e}");
            }
        }
        CtpEvent::Error { code, msg } => {
            // 报单拒/错误回报：保守按 Unknown 推进（状态机等查单收敛），
            // 仅记录日志（对应自检项"不确定错误保守 Indeterminate"）。
            tracing::warn!(account = %ctx.account, code, %msg, "CTP 错误回报");
        }
        // 连接/登录/合约事件由会话状态机或后续规则同步处理。
        _ => {}
    }
}

/// 终态清理钩子：委托到达终态（全部成交/撤销）后，回收两张会话表项——
/// cid↔OrderRef 映射与 OrderSysID→累计成交量，防会话内无限增长。
/// 两表均为"丢得起、可重建"的会话态：迟到/重放的回报经宿主
/// `exchange_order_id` 回查 fallback 与补快照重放兜底，表不是真相源。
fn release_terminal_tables(
    order_refs: &crate::ctp::order_ref::OrderRefRegistry,
    filled_by_sys_id: &Mutex<std::collections::HashMap<String, i64>>,
    s: &crate::ctp::core::event::OrderSnapshot,
) {
    if !s.is_terminal() {
        return;
    }
    // 映射表项的价值窗口 = 本单在途期间；终态之后留着是纯泄漏。
    // AllTraded 单必有成交累计表项；Cancelled 单多半缺席，remove 天然 no-op。
    order_refs.remove(&s.order_ref);
    filled_by_sys_id.lock().unwrap().remove(&s.order_sys_id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctp::core::account::CtpAccount;
    use crate::ctp::core::event::OrderSnapshot;
    use crate::ctp::core::types::{Direction, OrderStatus};
    use crate::ctp::order_ref::OrderRefRegistry;

    fn account() -> CtpAccount {
        CtpAccount {
            broker_id: "9999".into(),
            user_id: "100001".into(),
            password: "pwd".into(),
            app_id: "simnow_client".into(),
            auth_code: "0000000000000000".into(),
            investor_id: String::new(),
            front_address: "tcp://182.254.243.31:40001".into(),
            is_production_mode: false,
        }
    }

    fn handle() -> Arc<CtpUserHandle> {
        Arc::new(CtpUserHandle {
            inserts: Arc::new(crate::ctp::mailbox::InsertMailbox::new()),
            account: "acct".into(),
            flags: Arc::new(RequestFlags::default()),
            session_epoch: Arc::new(AtomicU64::new(0)),
            shutdown: Arc::new(AtomicBool::new(false)),
            filled_by_sys_id: Arc::new(Mutex::new(std::collections::HashMap::new())),
        })
    }

    #[test]
    fn request_flags_roundtrip() {
        // Arrange / Act：tokio 侧置位后线程侧 swap 消费。
        let flags = Arc::new(RequestFlags::default());
        flags.query_positions.store(true, Ordering::SeqCst);
        // Assert：swap 一次取走，再取为空。
        assert!(flags.query_positions.swap(false, Ordering::SeqCst));
        assert!(!flags.query_positions.swap(false, Ordering::SeqCst));
    }

    #[test]
    fn handle_request_positions_sets_flag() {
        let h = handle();
        h.request_positions();
        assert!(h.flags.query_positions.load(Ordering::SeqCst));
        h.request_orders();
        assert!(h.flags.query_orders.load(Ordering::SeqCst));
    }

    #[test]
    fn handle_drop_sets_shutdown() {
        let h = handle();
        let shutdown = Arc::clone(&h.shutdown);
        drop(h);
        // Drop 置位（强引用计数归零后仍可读标志）。
        assert!(shutdown.load(Ordering::SeqCst));
    }

    #[test]
    fn filled_of_defaults_zero() {
        let h = handle();
        assert_eq!(h.filled_of("unknown-ref"), 0);
        h.filled_by_sys_id
            .lock()
            .unwrap()
            .insert("7".to_string(), 3);
        assert_eq!(h.filled_of("7"), 3);
    }

    // ---- 终态清理钩子：委托终态即回收两张会话表（防无限增长）----

    /// 构造委托快照（仅 status/order_ref/order_sys_id 参与钩子判定）。
    fn snapshot(status: OrderStatus, order_ref: &str, order_sys_id: &str) -> OrderSnapshot {
        OrderSnapshot {
            order_ref: order_ref.to_string(),
            instrument: "rb2510".to_string(),
            exchange_id: "SHFE".to_string(),
            order_sys_id: order_sys_id.to_string(),
            insert_time: "09:30:00".to_string(),
            direction: Direction::Buy,
            status,
            limit_price: 3500.0,
            volume_total: 2,
            volume_traded: 0,
            status_msg: String::new(),
            front_id: 1,
            session_id: 1,
        }
    }

    #[test]
    fn terminal_release_frees_both_session_tables() {
        // Arrange：长 cid 分配 OrderRef 入映射表；成交累计入累计表。
        let refs = OrderRefRegistry::new();
        let order_ref = refs.allocate(&format!("0x{}", "F".repeat(32)));
        let sys_id = "20250101-1";
        let filled: Mutex<std::collections::HashMap<String, i64>> =
            Mutex::new(std::collections::HashMap::new());
        filled.lock().unwrap().insert(sys_id.to_string(), 1);

        // Act：全部成交（终态）——钩子回收两张表。
        release_terminal_tables(
            &refs,
            &filled,
            &snapshot(OrderStatus::AllTraded, &order_ref, sys_id),
        );

        // Assert：映射与累计表项均已回收。
        assert!(
            refs.cid_of(&order_ref).is_none(),
            "终态后 OrderRef 映射应回收"
        );
        assert!(
            filled.lock().unwrap().get(sys_id).is_none(),
            "终态后成交累计表项应回收"
        );
    }

    #[test]
    fn non_terminal_keeps_both_session_tables() {
        // Arrange：排队中的委托（非终态），两张表各有一条在途记录。
        let refs = OrderRefRegistry::new();
        let order_ref = refs.allocate(&format!("0x{}", "E".repeat(32)));
        let sys_id = "20250101-2";
        let filled: Mutex<std::collections::HashMap<String, i64>> =
            Mutex::new(std::collections::HashMap::new());
        filled.lock().unwrap().insert(sys_id.to_string(), 1);

        // Act：未成交排队回报——钩子不得误清。
        release_terminal_tables(
            &refs,
            &filled,
            &snapshot(OrderStatus::NoTradeQueueing, &order_ref, sys_id),
        );

        // Assert：两张表均保留（订单仍在途，后续回报仍需反查/累计）。
        assert!(
            refs.cid_of(&order_ref).is_some(),
            "非终态不得清 OrderRef 映射"
        );
        assert!(
            filled.lock().unwrap().get(sys_id).is_some(),
            "非终态不得清成交累计"
        );
    }

    #[test]
    fn cancelled_release_frees_mapping_and_tolerates_absent_entry() {
        // Arrange：撤单终态（通常无成交，累计表缺席该 sys_id——remove 须为无害 no-op）。
        let refs = OrderRefRegistry::new();
        let order_ref = refs.allocate(&format!("0x{}", "D".repeat(32)));
        let sys_id = "20250101-3";
        let filled: Mutex<std::collections::HashMap<String, i64>> =
            Mutex::new(std::collections::HashMap::new());

        // Act
        release_terminal_tables(
            &refs,
            &filled,
            &snapshot(OrderStatus::Cancelled, &order_ref, sys_id),
        );

        // Assert：映射回收；缺席表项的移除不 panic。
        assert!(
            refs.cid_of(&order_ref).is_none(),
            "撤销终态后 OrderRef 映射应回收"
        );
    }

    #[test]
    fn session_loop_rejects_bad_account_before_io() {
        // Arrange：空 broker_id 的账户（边界校验先于动态库打开）。
        let bad = CtpAccount {
            broker_id: "  ".into(),
            ..account()
        };
        let session = SessionLoop::new(
            bad,
            "lib.so".into(),
            "/tmp/ctp-flow-test".into(),
            false,
            Arc::new(AtomicBool::new(false)),
        );
        // Act / Assert：建连在边界校验即失败，不触碰 .so。
        assert!(session.connect_and_login().is_err());
    }
}

#[cfg(test)]
mod shutdown_sleep_tests {
    use super::*;

    #[test]
    fn interruptible_sleep_returns_early_when_flag_set() {
        // Arrange：要求睡 2s；50ms 后另一线程置位停机标志。
        let flag = Arc::new(AtomicBool::new(false));
        let setter_flag = Arc::clone(&flag);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            setter_flag.store(true, Ordering::SeqCst);
        });

        // Act
        let start = std::time::Instant::now();
        sleep_interruptibly(Duration::from_secs(2), &flag);
        let elapsed = start.elapsed();

        // Assert：远早于 2s 返回（分片粒度 ~200ms），且确已看到标志。
        assert!(elapsed < Duration::from_secs(1), "elapsed = {elapsed:?}");
        assert!(flag.load(Ordering::SeqCst));
    }

    #[test]
    fn interruptible_sleep_sleeps_full_duration_when_flag_unset() {
        // Arrange / Act：无人置位，睡满指定时长（500ms）。
        let flag = Arc::new(AtomicBool::new(false));
        let start = std::time::Instant::now();
        sleep_interruptibly(Duration::from_millis(500), &flag);

        // Assert。
        assert!(start.elapsed() >= Duration::from_millis(450));
    }
}
