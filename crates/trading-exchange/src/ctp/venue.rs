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
    /// 会话请求信箱（下单/撤单/查询；async 侧投递 → 本线程消费；provider 级共享）。
    pub(crate) mailbox: Arc<crate::ctp::mailbox::SessionMailbox>,
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
    // 单在途查询闸门：None=空闲；Some=恰有一个查询待 QueryEnd（前台/后台/合约预热互斥）。
    let mut active: Option<ActiveQuery> = None;
    // 本连接是否已发起合约表预热；断线重连会重新进入 inner_loop，标志复位、重拉一次。
    let mut contracts_loaded = false;
    loop {
        if handle.shutdown.load(Ordering::SeqCst) {
            return;
        }
        // 最高优先：建连后先全量预热合约表（乘数），收齐前不发起持仓查询，
        // 保证持仓成本能按 VolumeMultiple 折算成单价。
        ensure_contracts_loaded(h, ctx, &mut active, &mut contracts_loaded);
        // 每轮都服务请求：下单/撤单即使有查询在途也立即执行；后台查询与前台
        // 查询的互斥由 issue_* 内部按 active 闸门 + 纯内核 plan_mailbox_drain 决定。
        issue_background_or_mailbox(h, handle, ctx, &mut active, contracts_loaded);
        match h.next_event(EVENT_POLL_TIMEOUT) {
            // 断线：active 随 return drop，在途 oneshot 关闭，前台得 RecvError。
            Some(CtpEvent::FrontDisconnected(_)) => return,
            Some(CtpEvent::QueryEnd) if matches!(active, Some(ActiveQuery::Contracts)) => {
                // 合约表收齐：翻预热标志（须在 finish 释放闸门前判定）。
                contracts_loaded = true;
                tracing::debug!(
                    account = %ctx.account,
                    count = ctx.contract_dir.len(),
                    "CTP 合约表预热完成（乘数已缓存）"
                );
                dispatch_event(CtpEvent::QueryEnd, handle, ctx, &mut active);
            }
            Some(ev) => dispatch_event(ev, handle, ctx, &mut active),
            None => {
                // 空闲且闸门空闲：去抖到点则触发后台持仓对账。
                if active.is_none() && ctx.debouncer.lock().unwrap().poll(now_ms()).is_some() {
                    issue_back_positions(h, &mut active);
                }
            }
        }
    }
}

/// 当前唯一在途查询（单在途闸门的状态；前台 RPC 与后台对账共用，严格互斥）。
/// 不派生 Debug：载荷含 oneshot::Sender。
enum ActiveQuery {
    /// 建连后合约表预热：Instrument 行写入 ContractDir（乘数），QueryEnd 仅释放闸门。
    /// 由 [`ensure_contracts_loaded`] 保证先于任何持仓查询完成。
    Contracts,
    /// 前台按需查单：缓冲当日委托行，QueryEnd 选定后回 OrderLookup。
    FrontOrder {
        /// 首选匹配值（OrderSysID / 受理态 OrderRef）。
        exchange_order_id: String,
        /// cid→registry 反查的 OrderRef 兜底。
        hint: Option<String>,
        /// 查询应答行缓冲。
        rows: Vec<crate::ctp::core::event::OrderSnapshot>,
        /// 三态结果回执（断线随枚举 drop 关闭 → 接收端 RecvError）。
        reply: Option<tokio::sync::oneshot::Sender<crate::api::OrderLookup>>,
    },
    /// 前台按需查仓：映射后缓冲，QueryEnd 整批回执。
    FrontPositions {
        buf: Vec<crate::api::PositionInfo>,
        reply: Option<tokio::sync::oneshot::Sender<anyhow::Result<Vec<crate::api::PositionInfo>>>>,
    },
    /// 后台委托重放（重连 query_orders("")）：查询行经订单桥恢复状态机并喂目录。
    BackOrderReplay,
    /// 后台持仓对账：已按乘数映射为单价口径的持仓缓冲，QueryEnd 整批经持仓桥。
    BackPositions { buf: Vec<crate::api::PositionInfo> },
}

/// 建连后合约表预热（仅本连接一次）：发起全量合约查询，Instrument 行经
/// `dispatch_event` 写入 [`ContractDir`]，QueryEnd 释放闸门后才放行持仓查询。
///
/// 受理码非 0（流控/未就绪）不占位也不翻 `loaded`，下一轮 2s 后重试；在途时
/// （`active=Contracts`）等待 QueryEnd。这样保证首笔持仓快照到达时乘数已就绪。
fn ensure_contracts_loaded(
    h: &mut TraderHandle,
    ctx: &EventContext,
    active: &mut Option<ActiveQuery>,
    loaded: &mut bool,
) {
    if *loaded || active.is_some() {
        return;
    }
    if h.query_instruments() == 0 {
        *active = Some(ActiveQuery::Contracts);
        tracing::debug!(account = %ctx.account, "CTP 合约表预热：已发起全量合约查询（乘数）");
    }
    // 受理失败：loaded 仍 false、不占位，下轮重试。
}

/// 一轮"发起"调度：闸门空闲时后台脏标志（委托重放 → 持仓对账）优先；前台信箱
/// **每轮**都经纯内核 [`plan_mailbox_drain`] 决策——下单/撤单即使有查询在途也
/// 立即执行，仅前台查询受在途闸门约束（Defer 回塞队首）。任一查询受理码非 0
/// （流控/未就绪）都不占位，下一轮重试。
///
/// `contracts_loaded`：合约乘数预热完成前不发起持仓查询（否则成本无法折算单价）；
/// 委托重放不依赖乘数，仍可先行。
fn issue_background_or_mailbox(
    h: &mut TraderHandle,
    handle: &Arc<CtpUserHandle>,
    ctx: &EventContext,
    active: &mut Option<ActiveQuery>,
    contracts_loaded: bool,
) {
    use crate::ctp::mailbox::{plan_mailbox_drain, MailboxAction};
    // ①② 后台查询仅在闸门空闲时发起；在途时保留标志（不消费），下一轮再试。
    if active.is_none() {
        // ① 重连委托重放（优先，保证状态恢复先于持仓；不依赖乘数）。
        if handle.flags.query_orders.swap(false, Ordering::SeqCst) {
            if h.query_orders("") == 0 {
                *active = Some(ActiveQuery::BackOrderReplay);
            }
            return;
        }
        // ② 去抖/全量持仓对账——必须等合约乘数预热完成，否则单价折算缺除数。
        if contracts_loaded && handle.flags.query_positions.swap(false, Ordering::SeqCst) {
            issue_back_positions(h, active);
            return;
        }
    }
    // ③ 前台信箱：传入真实在途状态，下单/撤单（Run）恒执行、查询按闸门 Issue/Defer。
    match plan_mailbox_drain(handle.mailbox.take_next(), active.is_some()) {
        MailboxAction::Run(req) => run_command(h, ctx, req),
        MailboxAction::Issue(req) => issue_front_query(h, active, req),
        MailboxAction::Defer(req) => handle.mailbox.requeue_front(req),
        MailboxAction::Empty => {}
    }
}

/// 立即执行非查询命令（下单/撤单）——不占 ActiveQuery、不被查询闸门阻塞。
fn run_command(h: &mut TraderHandle, ctx: &EventContext, req: crate::ctp::mailbox::SessionRequest) {
    use crate::ctp::mailbox::SessionRequest;
    match req {
        SessionRequest::Insert(p) => {
            // 受理码回执：校验/FFI 错误一并随 Result 传回 async 侧。
            let ret = h
                .insert_order(&p.req, &p.order_ref)
                .map_err(|e| anyhow::anyhow!("CTP 报单校验失败: {e}"));
            let _ = p.reply.send(ret);
        }
        SessionRequest::Cancel(c) => {
            // 撤单定位在线程内完成：三元组所需当前 FrontID/SessionID 只有会话状态掌握。
            let resolved = resolve_cancel_target(
                &ctx.order_refs,
                &ctx.exchange_dir,
                &c.instrument,
                &c.order,
                h.front_id(),
                h.session_id(),
            );
            // 可观测证据：记录本次撤单实际寻址到的路由（本会话三元组 / 跨会话系统号）。
            match &resolved {
                Ok(target) => tracing::debug!(
                    account = %ctx.account, cid = %c.order.client_order_id, route = ?target,
                    "CTP 撤单寻址命中"
                ),
                Err(e) => tracing::warn!(
                    account = %ctx.account, cid = %c.order.client_order_id, error = %e,
                    "CTP 撤单寻址失败"
                ),
            }
            let ret = resolved.map(|target| h.cancel_order(&c.instrument, &target));
            let _ = c.reply.send(ret);
        }
        // 查询类走 issue_front_query，不会进入这里。
        SessionRequest::QueryOrder(_) | SessionRequest::QueryPositions(_) => {}
    }
}

/// 发起后台持仓对账；受理码非 0（流控/未就绪）不占位，下一轮重试。
/// 调用方须保证当前无在途查询（active 为 None）。
fn issue_back_positions(h: &mut TraderHandle, active: &mut Option<ActiveQuery>) {
    if h.query_positions() == 0 {
        *active = Some(ActiveQuery::BackPositions { buf: Vec::new() });
    }
}

/// 发起前台查询并占位（受理码非 0 立即失败回执、不占位，下一轮可重试）。
fn issue_front_query(
    h: &mut TraderHandle,
    active: &mut Option<ActiveQuery>,
    req: crate::ctp::mailbox::SessionRequest,
) {
    use crate::ctp::mailbox::{QueryOrderJob, QueryPositionsJob, SessionRequest};
    match req {
        SessionRequest::QueryOrder(QueryOrderJob {
            instrument,
            exchange_order_id,
            order_ref_hint,
            reply,
        }) => {
            if h.query_orders(&instrument) == 0 {
                *active = Some(ActiveQuery::FrontOrder {
                    exchange_order_id,
                    hint: order_ref_hint,
                    rows: Vec::new(),
                    reply: Some(reply),
                });
            } else {
                // 流控/未就绪：查询本身失败 ≠ 订单不存在，回 Unavailable。
                let _ = reply.send(crate::api::OrderLookup::Unavailable);
            }
        }
        SessionRequest::QueryPositions(QueryPositionsJob { reply }) => {
            if h.query_positions() == 0 {
                *active = Some(ActiveQuery::FrontPositions {
                    buf: Vec::new(),
                    reply: Some(reply),
                });
            } else {
                let _ = reply.send(Err(anyhow::anyhow!("CTP 持仓查询受理失败（流控/未就绪）")));
            }
        }
        _ => {}
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
    /// cid ↔ OrderRef 映射（回报反查长 cid；撤单时 cid→OrderRef 反查也用它）。
    pub order_refs: Arc<crate::ctp::order_ref::OrderRefRegistry>,
    /// 合约 → 交易所代码目录（委托回报/查询行喂入；跨会话撤单定位 ExchangeID）。
    pub exchange_dir: Arc<crate::ctp::exchange_dir::ExchangeDir>,
    /// 合约 → 乘数目录（建连后合约查询预热；持仓成本折算单价用）。
    pub contract_dir: Arc<crate::ctp::contract_dir::ContractDir>,
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

/// 单事件分发：实时回报始终透传；查询行进在途闸门 [`ActiveQuery`] 的对应缓冲，
/// `QueryEnd` 按前台 oneshot / 后台桥分流收尾。
fn dispatch_event(
    ev: crate::ctp::core::event::CtpEvent,
    handle: &Arc<CtpUserHandle>,
    ctx: &EventContext,
    active: &mut Option<ActiveQuery>,
) {
    use crate::ctp::core::event::CtpEvent;
    match ev {
        // 实时委托回报：永远透传订单端口（即便有查询在途，也绝不被查询缓冲吞掉），
        // 并顺手喂合约→交易所目录（跨会话撤单/将来 SHFE 路由复用，零额外请求）。
        CtpEvent::Order(s) => {
            ctx.exchange_dir.remember(&s.instrument, &s.exchange_id);
            forward_order_snapshot(ctx, handle, s);
        }
        // 当日报单查询行：按在途闸门分流——前台查单缓冲，后台重放走订单桥。
        CtpEvent::OrderQuery(s) => match active {
            Some(ActiveQuery::FrontOrder { rows, .. }) => rows.push(s),
            // 后台重放（或无在途查询的防御情形）：与实时回报同路径透传 + 喂目录。
            _ => {
                ctx.exchange_dir.remember(&s.instrument, &s.exchange_id);
                forward_order_snapshot(ctx, handle, s);
            }
        },
        CtpEvent::Trade(t) => {
            // 成交回报：按 OrderSysID 累计成交量后推订单端口。
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
        // 合约预热行：喂入合约→乘数目录（仅在合约查询在途时；其余情形忽略）。
        CtpEvent::Instrument(i) => {
            if matches!(active, Some(ActiveQuery::Contracts)) {
                ctx.contract_dir.remember(&i.instrument, i.volume_multiple);
            }
        }
        CtpEvent::Position(p) => {
            // 乘数按合约取；预热缺失（None）时 mapper 返回 None 过滤该批，下轮补。
            let mult = ctx.contract_dir.get(&p.instrument).unwrap_or(0);
            match active {
                // 前台查仓 / 后台对账：都先按乘数映射为单价口径 PositionInfo 再缓冲
                // （数量为 0/乘数缺失的行由 mapper 过滤）；QueryEnd 再分 oneshot / 持仓桥收尾。
                Some(
                    ActiveQuery::FrontPositions { buf, .. } | ActiveQuery::BackPositions { buf },
                ) => {
                    if let Some(info) = crate::ctp::mapping::position_info_from_snapshot(&p, mult) {
                        buf.push(info);
                    }
                }
                // 无在途持仓查询却收到持仓行：不属于本轮，忽略（不污染任何缓冲）。
                _ => tracing::debug!(account = %ctx.account, instrument = %p.instrument,
                    "收到无在途查询的持仓行，已忽略"),
            }
        }
        CtpEvent::QueryEnd => finish_active_query(ctx, active),
        CtpEvent::Error { code, msg } => {
            // 报单拒/撤单异步拒绝（24/25/26 等）：终态以 on_rtn_order 为准，
            // 此处仅中文 warn 可诊断（不据错误码臆测收敛订单状态）。
            tracing::warn!(account = %ctx.account, code, %msg, "CTP 错误回报");
        }
        // 连接/登录/合约事件由会话状态机或后续规则同步处理。
        _ => {}
    }
}

/// 委托快照 → 订单端口（实时回报与后台重放共用同一路径）：映射、cid 反查、
/// 终态表回收、事件桥投递、喂去抖脏信号。
fn forward_order_snapshot(
    ctx: &EventContext,
    handle: &Arc<CtpUserHandle>,
    s: crate::ctp::core::event::OrderSnapshot,
) {
    let state = crate::ctp::mapping::order_state_from_snapshot(&s);
    // 回报反查：OrderRef → 本地长 cid（未命中保留原值，宿主 fallback 兜底）。
    let mut event = crate::translate::order_event(&state);
    event.client_order_id = resolve_event_cid(ctx, &s.order_ref);
    // 终态清理钩子：反查完成后回收两张会话表（cid↔OrderRef 映射与成交累计），
    // 防会话内无限增长；迟到/重放回报经宿主 fallback 兜底。
    release_terminal_tables(&ctx.account, &ctx.order_refs, &handle.filled_by_sys_id, &s);
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

/// 一次查询收齐（QueryEnd）：按在途查询种类前台 oneshot / 后台桥收尾，并清空闸门。
fn finish_active_query(ctx: &EventContext, active: &mut Option<ActiveQuery>) {
    let Some(current) = active.take() else {
        return;
    };
    match current {
        // 合约预热收尾：Instrument 行已逐条写入 ContractDir，这里仅释放闸门。
        ActiveQuery::Contracts => {}
        ActiveQuery::FrontOrder {
            exchange_order_id,
            hint,
            rows,
            reply,
        } => {
            // 纯匹配：系统号优先，其次受理态 OrderRef，再次 registry 反查 hint。
            let lookup = match select_order(&rows, &exchange_order_id, hint.as_deref()) {
                Some(s) => crate::api::OrderLookup::Found(
                    crate::ctp::mapping::order_state_from_snapshot(s),
                ),
                // 空/不匹配/只查当日：一律 Unavailable，绝不判 NotExists（避免把
                // UNKNOWN 单错误收敛成 REJECTED）。
                None => crate::api::OrderLookup::Unavailable,
            };
            if let Some(reply) = reply {
                let _ = reply.send(lookup);
            }
        }
        ActiveQuery::FrontPositions { buf, reply } => {
            // 合法空仓 = Ok(空)；断线场景由 Sender drop 传递，不在此伪造错误。
            if let Some(reply) = reply {
                let _ = reply.send(Ok(buf));
            }
        }
        ActiveQuery::BackOrderReplay => {
            // 重放行已在 OrderQuery 分支逐条透传订单桥，QueryEnd 仅释放闸门。
        }
        ActiveQuery::BackPositions { buf } => {
            // 后台对账：buf 已在 Position 分支按乘数映射为单价口径，整批经持仓桥。
            if let Err(e) = ctx.tx.blocking_send(BridgeEvent::Positions(buf)) {
                tracing::warn!(account = %ctx.account, "事件桥已关闭，持仓快照丢弃：{e}");
            }
        }
    }
}

/// 终态清理钩子：委托到达终态（全部成交/撤销）后，回收两张会话表项——
/// cid↔OrderRef 映射与 OrderSysID→累计成交量，防会话内无限增长。
/// 两表均为"丢得起、可重建"的会话态：迟到/重放的回报经宿主
/// `exchange_order_id` 回查 fallback 与补快照重放兜底，表不是真相源。
fn release_terminal_tables(
    account: &str,
    order_refs: &crate::ctp::order_ref::OrderRefRegistry,
    filled_by_sys_id: &Mutex<std::collections::HashMap<String, i64>>,
    s: &crate::ctp::core::event::OrderSnapshot,
) {
    if !s.is_terminal() {
        return;
    }
    // 映射表项的价值窗口 = 本单在途期间；终态之后留着是纯泄漏。
    // AllTraded 单必有成交累计表项；Cancelled 单多半缺席，remove 天然 no-op。
    // 回收前先取证：该 OrderRef 是否确在 registry 中（回收后即不可反查）。
    let present_before = order_refs.cid_of(&s.order_ref).is_some();
    order_refs.remove(&s.order_ref);
    let present_after = order_refs.cid_of(&s.order_ref).is_some();
    let filled_removed = filled_by_sys_id
        .lock()
        .unwrap()
        .remove(&s.order_sys_id)
        .is_some();
    // 可观测证据：终态（Filled/Canceled）后回收本会话表项，给出 OrderRef/OrderSysID、
    // 回收前后反查结果（true→false 证明 cid↔OrderRef 映射确已释放）与成交累计项是否移除。
    tracing::debug!(
        account, order_ref = %s.order_ref, order_sys_id = %s.order_sys_id.trim(),
        ref_present_before = present_before, ref_present_after = present_after,
        filled_acc_removed = filled_removed,
        "CTP 终态回收：cid↔OrderRef 映射与成交累计表项已释放"
    );
}

/// 撤单定位路由（纯函数；单号路由收口在适配器内，宿主不写 CTP 分支）。
///
/// 两路定位（与计划"重启后单号路由"定论一致）：
/// 1. **本会话三元组**：cid 经 registry 反查到本会话自产 OrderRef →
///    `FrontID+SessionID+OrderRef`（Front/Session 取线程当前 SessionState）；
/// 2. **跨会话系统号**：registry 未命中（重启后表空）→ `ExchangeID+OrderSysID`，
///    OrderSysID 取受理后升级的 `exchange_order_id`，ExchangeID 取 ExchangeDir 缓存。
///
/// 无法定位（无 eid 或 ExchangeDir 未预热）时返回错误——绝不凭猜测撤单。
fn resolve_cancel_target(
    registry: &crate::ctp::order_ref::OrderRefRegistry,
    dir: &crate::ctp::exchange_dir::ExchangeDir,
    instrument: &str,
    order: &crate::api::OrderRef,
    front_id: i32,
    session_id: i32,
) -> anyhow::Result<crate::ctp::mailbox::CancelTarget> {
    use crate::ctp::mailbox::CancelTarget;
    // ⓪ 入参防线：空合约绝不下发柜台（三元组不读目录，可能误装空 InstrumentID）。
    if instrument.trim().is_empty() {
        anyhow::bail!(
            "CTP 撤单无法定位：instrument 为空（cid={}）",
            order.client_order_id
        );
    }
    // ① 本会话：cid 反查到自产 OrderRef → FrontID+SessionID+OrderRef 三元组。
    if let Some(order_ref) = registry.order_ref_of_cid(&order.client_order_id) {
        return Ok(CancelTarget::ByOrderRef {
            front_id,
            session_id,
            order_ref,
        });
    }
    // ② 跨会话（重启后 registry 为空）：必须有受理后升级的交易所系统号。
    let Some(order_sys_id) = order.exchange_order_id.as_deref() else {
        anyhow::bail!(
            "CTP 撤单无法定位：cid={} 非本会话映射且缺少 exchange_order_id（OrderSysID）",
            order.client_order_id
        );
    };
    if order_sys_id.trim().is_empty() {
        anyhow::bail!(
            "CTP 撤单无法定位：exchange_order_id 为空（委托尚未取得 OrderSysID），instrument={instrument}"
        );
    }
    // ExchangeID 取自会话态目录（重连重放/实时回报预热，零额外请求）。
    let Some(exchange_id) = dir.get(instrument) else {
        anyhow::bail!(
            "CTP 撤单跨会话定位失败：instrument={instrument} 的交易所目录尚未预热，\
             请待重连当日委托重放后重试"
        );
    };
    Ok(CancelTarget::ByOrderSysId {
        exchange_id,
        order_sys_id: order_sys_id.trim().to_string(),
    })
}

/// 查单行匹配（纯函数；柜台只支持按 InstrumentID 拉当日全部委托，客户端匹配）。
///
/// 全局优先级（先系统号、后 OrderRef）：
/// 1. `order_sys_id == exchange_order_id`（首回报后/重启后的权威定位）；
/// 2. `order_ref == exchange_order_id`（受理态：此时 eid 还是自产 OrderRef）；
/// 3. `order_ref == order_ref_hint`（同进程 cid→registry 反查值）。
///
/// 查无（空/不匹配/只查当日）由调用方判 `Unavailable`，不判 NotExists。
fn select_order<'a>(
    rows: &'a [crate::ctp::core::event::OrderSnapshot],
    exchange_order_id: &str,
    order_ref_hint: Option<&str>,
) -> Option<&'a crate::ctp::core::event::OrderSnapshot> {
    use crate::ctp::core::event::OrderSnapshot;
    // ① 系统号优先（受理后/重启后的权威定位）；空系统号不参与等值（eid 非空）。
    let by_sys = |r: &&OrderSnapshot| {
        !r.order_sys_id.trim().is_empty() && r.order_sys_id == exchange_order_id
    };
    if let Some(hit) = rows.iter().find(by_sys) {
        return Some(hit);
    }
    // ② 受理态：eid 仍是自产 OrderRef（尚无系统号）。
    if let Some(hit) = rows
        .iter()
        .find(|r| !r.order_ref.trim().is_empty() && r.order_ref == exchange_order_id)
    {
        return Some(hit);
    }
    // ③ 同进程：cid→registry 反查出的 OrderRef 兜底。
    order_ref_hint.and_then(|hint| {
        rows.iter()
            .find(|r| !hint.trim().is_empty() && r.order_ref == hint)
    })
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
            mailbox: Arc::new(crate::ctp::mailbox::SessionMailbox::new()),
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
            "test",
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
            "test",
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
            "test",
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

    // ---- 撤单定位路由 / 查单行匹配（纯函数）----

    use crate::api::OrderRef;
    use crate::ctp::exchange_dir::ExchangeDir;
    use crate::ctp::mailbox::CancelTarget;

    fn long_cid(tag: &str) -> String {
        format!("0x{}", tag.repeat(32))
    }

    /// 构造查询行（仅路由相关字段参与匹配）。
    fn qrow(order_ref: &str, sys_id: &str, exchange: &str, instrument: &str) -> OrderSnapshot {
        OrderSnapshot {
            order_ref: order_ref.into(),
            instrument: instrument.into(),
            exchange_id: exchange.into(),
            order_sys_id: sys_id.into(),
            insert_time: String::new(),
            direction: Direction::Buy,
            status: OrderStatus::NoTradeQueueing,
            limit_price: 3500.0,
            volume_total: 1,
            volume_traded: 0,
            status_msg: String::new(),
            front_id: 0,
            session_id: 0,
        }
    }

    #[test]
    fn cancel_routes_by_triple_when_registry_hits_this_session() {
        // Arrange：长 cid 在本会话分配过 OrderRef；即使带了 eid 也优先三元组。
        let registry = OrderRefRegistry::new();
        let cid = long_cid("A");
        let order_ref = registry.allocate(&cid);
        let dir = ExchangeDir::new();
        let order = OrderRef::new(cid.clone()).with_exchange("20260101-9");

        // Act
        let target = resolve_cancel_target(&registry, &dir, "rb2510", &order, 3, 9001).unwrap();

        // Assert：本会话三元组，Front/Session 取当前会话。
        assert_eq!(
            target,
            CancelTarget::ByOrderRef {
                front_id: 3,
                session_id: 9001,
                order_ref,
            }
        );
    }

    #[test]
    fn cancel_routes_by_sys_id_after_restart_when_registry_cold() {
        // Arrange：重启后 registry 为空（cid 反查未命中），目录已预热该合约交易所。
        let registry = OrderRefRegistry::new();
        let dir = ExchangeDir::new();
        dir.remember("rb2510", "SHFE");
        let order = OrderRef::new(long_cid("R")).with_exchange("20260101-7");

        // Act
        let target = resolve_cancel_target(&registry, &dir, "rb2510", &order, 9, 9999).unwrap();

        // Assert：跨会话走 ExchangeID+OrderSysID（旧 Front/Session 已失效但不用）。
        assert_eq!(
            target,
            CancelTarget::ByOrderSysId {
                exchange_id: "SHFE".into(),
                order_sys_id: "20260101-7".into(),
            }
        );
    }

    #[test]
    fn cancel_errors_when_exchange_dir_not_warmed() {
        // Arrange：registry 未命中且目录未预热（重放前就撤重启旧单）。
        let registry = OrderRefRegistry::new();
        let dir = ExchangeDir::new();
        let order = OrderRef::new(long_cid("R")).with_exchange("20260101-7");

        // Act / Assert：显式报错，绝不猜测 ExchangeID。
        assert!(resolve_cancel_target(&registry, &dir, "rb2510", &order, 1, 1).is_err());
    }

    #[test]
    fn cancel_errors_when_no_exchange_order_id_after_restart() {
        // Arrange：registry 未命中（重启）且手里没有任何交易所单号（受理态孤儿）。
        let registry = OrderRefRegistry::new();
        let dir = ExchangeDir::new();
        dir.remember("rb2510", "SHFE");
        let order = OrderRef::new(long_cid("R"));

        // Act / Assert：无法定位，报错而非猜测。
        assert!(resolve_cancel_target(&registry, &dir, "rb2510", &order, 1, 1).is_err());
    }

    #[test]
    fn cancel_errors_when_instrument_blank() {
        // Arrange：即使 cid 在本会话映射命中（三元组不读目录），空合约也必须 fail-fast。
        let registry = OrderRefRegistry::new();
        let cid = long_cid("A");
        let _ = registry.allocate(&cid);
        let dir = ExchangeDir::new();
        let order = OrderRef::new(cid);

        // Act / Assert：空/纯空白 instrument 在装配柜台结构前即拒绝（两路都拦）。
        assert!(resolve_cancel_target(&registry, &dir, "", &order, 1, 1).is_err());
        assert!(resolve_cancel_target(&registry, &dir, "   ", &order, 1, 1).is_err());
    }

    #[test]
    fn select_matches_by_order_sys_id_first() {
        // Arrange：当日多行，目标行系统号 == eid（OrderRef 与 cid 完全不同）。
        let rows = vec![
            qrow("100", "20260101-1", "SHFE", "rb2510"),
            qrow("101", "20260101-2", "SHFE", "rb2510"),
        ];

        // Act
        let hit = select_order(&rows, "20260101-2", None).unwrap();

        // Assert：按系统号命中正确行。
        assert_eq!(hit.order_ref, "101");
    }

    #[test]
    fn select_matches_by_order_ref_when_still_accepted_state() {
        // Arrange：受理态尚无系统号（空），此时 eid 仍是自产 OrderRef。
        let rows = vec![
            qrow("100", "", "SHFE", "rb2510"),
            qrow("101", "", "SHFE", "rb2510"),
        ];

        // Act / Assert：order_ref == eid 命中。
        assert_eq!(select_order(&rows, "101", None).unwrap().order_sys_id, "");
        assert_eq!(select_order(&rows, "101", None).unwrap().order_ref, "101");
    }

    #[test]
    fn select_matches_by_registry_hint() {
        // Arrange：同进程 cid→registry 反查出的 OrderRef 作 hint，eid 可能已升级。
        let rows = vec![qrow("100", "20260101-1", "SHFE", "rb2510")];

        // Act / Assert：eid 不匹配任何系统号时，回退用 hint 命中。
        let hit = select_order(&rows, "0xdeadbeef", Some("100")).unwrap();
        assert_eq!(hit.order_sys_id, "20260101-1");
    }

    #[test]
    fn select_prefers_sys_id_over_ref_and_hint() {
        // Arrange：构造一行 order_ref==eid（候选 2）且另一行 sys_id==eid（候选 1）。
        let rows = vec![
            qrow("777", "", "SHFE", "rb2510"),
            qrow("777", "SAME", "DCE", "m2509"),
        ];

        // Act：eid 同时等于某行 order_ref 与另一行 sys_id——系统号优先。
        let hit = select_order(&rows, "SAME", Some("777")).unwrap();

        // Assert：命中系统号那一行（权威定位优先）。
        assert_eq!(hit.order_sys_id, "SAME");
        assert_eq!(hit.instrument, "m2509");
    }

    #[test]
    fn select_returns_none_when_empty_or_no_match() {
        // Arrange / Act / Assert：空结果或无匹配均为 None（调用方判 Unavailable，非 NotExists）。
        assert!(select_order(&[], "x", None).is_none());
        let rows = vec![qrow("100", "20260101-1", "SHFE", "rb2510")];
        assert!(select_order(&rows, "nope", Some("other")).is_none());
    }

    // ---- ActiveQuery 单在途闸门：前台/后台分流、QueryEnd 收尾、断线回执 ----

    use crate::ctp::core::event::{CtpEvent, PositionSnapshot};
    use tokio::sync::mpsc;
    use tokio::sync::oneshot::{error::TryRecvError, Receiver as OneRx};

    /// 构造不触 FFI 的事件上下文 + 桥接收端。
    fn ctx_and_rx() -> (Arc<EventContext>, mpsc::Receiver<BridgeEvent>) {
        let (tx, rx) = mpsc::channel(16);
        let ctx = Arc::new(EventContext {
            account: "acct".into(),
            order_refs: Arc::new(OrderRefRegistry::new()),
            exchange_dir: Arc::new(ExchangeDir::new()),
            contract_dir: Arc::new(crate::ctp::contract_dir::ContractDir::new()),
            tx,
            debouncer: Arc::new(Mutex::new(crate::reconcile::ReconcileDebouncer::new(
                500, 60_000,
            ))),
        });
        (ctx, rx)
    }

    fn long_position() -> PositionSnapshot {
        // 真实量纲：rb 乘数 10、单价 3205、3 手 → 总成本 3205×10×3 = 96150。
        PositionSnapshot {
            instrument: "rb2510".into(),
            direction: '2',
            position: 3,
            yd_position: 1,
            today_position: 2,
            position_cost: 96_150.0,
            use_margin: 32_050.0,
            position_profit: 55.0,
        }
    }

    /// 给合约目录预热 rb2510 乘数 10（模拟建连后合约表预热完成）。
    fn warm_rb_multiple(ctx: &EventContext) {
        ctx.contract_dir.remember("rb2510", 10);
    }

    fn front_order_active(
        eid: &str,
        hint: Option<&str>,
    ) -> (Option<ActiveQuery>, OneRx<crate::api::OrderLookup>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (
            Some(ActiveQuery::FrontOrder {
                exchange_order_id: eid.into(),
                hint: hint.map(str::to_string),
                rows: Vec::new(),
                reply: Some(tx),
            }),
            rx,
        )
    }

    #[test]
    fn front_order_buffers_query_rows_and_queryend_replies_found() {
        // Arrange：前台查单在途，喂一行系统号匹配的查询行。
        let (ctx, mut bridge_rx) = ctx_and_rx();
        let h = handle();
        let (mut active, mut lookup_rx) = front_order_active("20260101-2", None);
        dispatch_event(
            CtpEvent::OrderQuery(qrow("101", "20260101-2", "SHFE", "rb2510")),
            &h,
            &ctx,
            &mut active,
        );

        // Act：QueryEnd 收尾。
        dispatch_event(CtpEvent::QueryEnd, &h, &ctx, &mut active);

        // Assert：查询行未泄漏到订单桥；oneshot 收到 Found 且闸门已清。
        assert!(bridge_rx.try_recv().is_err(), "查询行不得透传订单桥");
        match lookup_rx.try_recv() {
            Ok(crate::api::OrderLookup::Found(st)) => {
                assert_eq!(st.exchange_order_id.as_deref(), Some("20260101-2"))
            }
            other => panic!("期望 Found，实际 {other:?}"),
        }
        assert!(active.is_none(), "QueryEnd 必须清空在途闸门");
    }

    #[test]
    fn front_order_no_match_replies_unavailable_not_notexists() {
        // Arrange：查询返回的行无一匹配。
        let (ctx, _rx) = ctx_and_rx();
        let h = handle();
        let (mut active, mut lookup_rx) = front_order_active("missing", None);
        dispatch_event(
            CtpEvent::OrderQuery(qrow("101", "20260101-9", "SHFE", "rb2510")),
            &h,
            &ctx,
            &mut active,
        );

        // Act
        dispatch_event(CtpEvent::QueryEnd, &h, &ctx, &mut active);

        // Assert：查无 = Unavailable（绝不判 NotExists，避免把 UNKNOWN 收敛成 REJECTED）。
        assert!(matches!(
            lookup_rx.try_recv(),
            Ok(crate::api::OrderLookup::Unavailable)
        ));
    }

    #[test]
    fn front_positions_queryend_replies_mapped_batch() {
        // Arrange：前台查仓在途，喂一条多头持仓行。
        let (ctx, _rx) = ctx_and_rx();
        let h = handle();
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        let mut active = Some(ActiveQuery::FrontPositions {
            buf: Vec::new(),
            reply: Some(tx),
        });
        // 合约乘数先预热（建连后合约查询的产物），持仓行才能折算单价。
        warm_rb_multiple(&ctx);
        dispatch_event(CtpEvent::Position(long_position()), &h, &ctx, &mut active);

        // Act
        dispatch_event(CtpEvent::QueryEnd, &h, &ctx, &mut active);

        // Assert：整批映射后 oneshot 回执（3 手多头；单价 96150/(3×10)=3205）。
        let positions = rx.try_recv().unwrap().unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].signed_quantity, rust_decimal::Decimal::from(3));
        assert_eq!(
            positions[0].entry_price,
            rust_decimal::Decimal::try_from(3205.0).unwrap()
        );
        assert_eq!(
            positions[0].multiplier,
            Some(rust_decimal::Decimal::from(10))
        );
        assert!(active.is_none());
    }

    #[test]
    fn front_positions_without_contract_multiple_is_filtered() {
        // 合约表未预热（乘数缺失）：宁可过滤该批，也不产出错误量纲的成本价。
        let (ctx, _rx) = ctx_and_rx();
        let h = handle();
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        let mut active = Some(ActiveQuery::FrontPositions {
            buf: Vec::new(),
            reply: Some(tx),
        });
        dispatch_event(CtpEvent::Position(long_position()), &h, &ctx, &mut active);
        dispatch_event(CtpEvent::QueryEnd, &h, &ctx, &mut active);
        let positions = rx.try_recv().unwrap().unwrap();
        assert!(positions.is_empty(), "乘数缺失时该持仓行应被过滤");
    }

    #[test]
    fn contracts_query_warms_multiple_dir_and_releases_gate() {
        // 合约预热：Instrument 行经 Contracts 在途写入 ContractDir，QueryEnd 释放闸门。
        let (ctx, _rx) = ctx_and_rx();
        let h = handle();
        let mut active = Some(ActiveQuery::Contracts);
        dispatch_event(
            CtpEvent::Instrument(crate::ctp::core::event::InstrumentInfo {
                instrument: "rb2510".into(),
                exchange_id: "SHFE".into(),
                product_id: "rb".into(),
                is_trading: true,
                volume_multiple: 10,
            }),
            &h,
            &ctx,
            &mut active,
        );
        assert_eq!(ctx.contract_dir.get("rb2510"), Some(10));
        dispatch_event(CtpEvent::QueryEnd, &h, &ctx, &mut active);
        assert!(active.is_none(), "合约预热 QueryEnd 释放闸门");
    }

    #[test]
    fn back_order_replay_forwards_query_rows_to_bridge_and_warms_dir() {
        // Arrange：后台重连委托重放在途。
        let (ctx, mut bridge_rx) = ctx_and_rx();
        let h = handle();
        let mut active = Some(ActiveQuery::BackOrderReplay);

        // Act：查询行应像实时回报一样透传订单桥（恢复状态机）并喂交易所目录。
        dispatch_event(
            CtpEvent::OrderQuery(qrow("101", "20260101-2", "SHFE", "rb2510")),
            &h,
            &ctx,
            &mut active,
        );

        // Assert：订单桥收到一帧；目录已预热（供跨会话撤单）；闸门仍在（等 QueryEnd）。
        assert!(matches!(bridge_rx.try_recv(), Ok(BridgeEvent::Order(_))));
        assert_eq!(ctx.exchange_dir.get("rb2510").as_deref(), Some("SHFE"));
        assert!(active.is_some());
        dispatch_event(CtpEvent::QueryEnd, &h, &ctx, &mut active);
        assert!(active.is_none(), "重放 QueryEnd 释放闸门");
    }

    #[test]
    fn back_positions_queryend_forwards_snapshot_to_bridge() {
        // Arrange：后台持仓对账在途。
        let (ctx, mut bridge_rx) = ctx_and_rx();
        let h = handle();
        let mut active = Some(ActiveQuery::BackPositions { buf: Vec::new() });
        warm_rb_multiple(&ctx);
        dispatch_event(CtpEvent::Position(long_position()), &h, &ctx, &mut active);

        // Act
        dispatch_event(CtpEvent::QueryEnd, &h, &ctx, &mut active);

        // Assert：聚合后经持仓桥投递（已按乘数折算为单价；与前台查仓同一 mapper）。
        match bridge_rx.try_recv() {
            Ok(BridgeEvent::Positions(ps)) => {
                assert_eq!(ps.len(), 1);
                assert_eq!(
                    ps[0].entry_price,
                    rust_decimal::Decimal::try_from(3205.0).unwrap()
                );
                assert_eq!(ps[0].multiplier, Some(rust_decimal::Decimal::from(10)));
            }
            other => panic!("expected one mapped position, got {other:?}"),
        }
        assert!(active.is_none());
    }

    #[test]
    fn realtime_order_is_never_swallowed_during_front_query() {
        // Arrange：前台查单在途（查询行进缓冲），此时来一帧**实时**委托回报。
        let (ctx, mut bridge_rx) = ctx_and_rx();
        let h = handle();
        let (mut active, _lookup_rx) = front_order_active("20260101-2", None);

        // Act：先发查询行（被缓冲），再发实时回报（必须透传）。
        dispatch_event(
            CtpEvent::OrderQuery(qrow("101", "20260101-2", "SHFE", "rb2510")),
            &h,
            &ctx,
            &mut active,
        );
        dispatch_event(
            CtpEvent::Order(qrow("101", "20260101-2", "SHFE", "rb2510")),
            &h,
            &ctx,
            &mut active,
        );

        // Assert：实时回报未被查询缓冲吞掉，订单桥收到且目录被喂。
        assert!(matches!(bridge_rx.try_recv(), Ok(BridgeEvent::Order(_))));
        assert_eq!(ctx.exchange_dir.get("rb2510").as_deref(), Some("SHFE"));
    }

    #[test]
    fn disconnect_drops_inflight_query_with_closed_receipt() {
        // Arrange：前台查单在途，持有未回执 oneshot。
        let (ctx, _rx) = ctx_and_rx();
        let h = handle();
        let (active, mut lookup_rx) = front_order_active("20260101-2", None);

        // Act：模拟断线/循环退出——inner_loop return 时 active 被 drop（发送端关闭）。
        drop(active);
        let _ = (&ctx, &h);

        // Assert：接收端立即得到 Closed（门面映射为"会话断开"），无悬挂回执。
        assert!(matches!(lookup_rx.try_recv(), Err(TryRecvError::Closed)));
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
