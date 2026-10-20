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

/// 全成委托快照先于成交回报到达时，等待 `OnRtnTrade` 补齐真实成交价的宽限（ms）。
/// 正常两路间隔仅 1~2ms（同毫秒级对齐）；仅当成交帧异常丢失时才走满此宽限，
/// 由 inner_loop 空闲 tick 兜底按已知数据投影终态，避免订单悬挂。
const TERMINAL_TRADE_GRACE_MS: i64 = 1500;

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
    /// cid → 累计成交量（专用线程写，tokio 侧读；Mutex 保护而非 channel）。
    /// 按 **cid** 累计：SHFE 跨今昨一单两腿的成交回报各自带不同 OrderSysID，
    /// 共享同一 cid，必须求和成逻辑单成交量。表项生存期 = 该单首笔成交 →
    /// cid 下所有腿终态（终态钩子回收，见 [`forward_order_snapshot`]）；
    /// 崩溃恢复不依赖本表——重连补快照的委托回报自带柜台权威 `volume_traded`。
    pub(crate) filled_by_cid: Arc<Mutex<std::collections::HashMap<String, i64>>>,
    /// 按 **cid** 累计成交额（Σ 成交价×成交量，f64）：与 [`Self::filled_by_cid`]
    /// 配套，用于在每笔成交回报上算成交均价（VWAP=成交额÷成交量）带给上游。
    /// 生存期/回收与成交量表完全一致。
    pub(crate) filled_amount_by_cid: Arc<Mutex<std::collections::HashMap<String, f64>>>,
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

    /// 读某 cid 的累计成交量（无记录返回 0）。
    pub fn filled_of_cid(&self, cid: &str) -> i64 {
        self.filled_by_cid
            .lock()
            .map(|m| m.get(cid).copied().unwrap_or(0))
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
    // 新连接入口：清空上一连接遗留的「待终态对齐」暂存（同一 ctx 跨重连复用），
    // 避免上个连接没等到成交回报的 cid 在本连接被误兜底投影。
    ctx.pending_terminal.clear();
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
                    count = ctx.instrument_dir.len(),
                    "CTP 合约表预热完成（交易所/乘数/最小变动价位已缓存）"
                );
                dispatch_event(CtpEvent::QueryEnd, handle, ctx, &mut active);
            }
            Some(ev) => dispatch_event(ev, handle, ctx, &mut active),
            None => {
                // 空闲：先兜底「待终态对齐」超时项（成交帧异常丢失时按已知数据投影，
                // 不悬挂订单）；闸门空闲再看去抖是否到点触发后台持仓对账。
                for (pending_cid, _expected) in ctx
                    .pending_terminal
                    .take_due(now_ms(), TERMINAL_TRADE_GRACE_MS)
                {
                    tracing::warn!(
                        account = %ctx.account, cid = %pending_cid,
                        grace_ms = TERMINAL_TRADE_GRACE_MS,
                        "CTP 终态对齐超时：成交回报未在宽限内到齐，按快照兜底投影终态"
                    );
                    project_terminal(ctx, handle, &pending_cid);
                }
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
    /// 建连后合约表预热：Instrument 行写入共享 InstrumentDir（交易所/乘数/tick），QueryEnd 仅释放闸门。
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
    /// 前台按需查仓：缓冲柜台**原始持仓行**（今/昨可能分两行），
    /// QueryEnd 按 (合约,方向) 聚合为一条 PositionInfo 后整批回执。
    FrontPositions {
        rows: Vec<crate::ctp::core::event::PositionSnapshot>,
        reply: Option<tokio::sync::oneshot::Sender<anyhow::Result<Vec<crate::api::PositionInfo>>>>,
    },
    /// 后台委托重放（重连 query_orders("")）：查询行经订单桥恢复状态机并喂目录。
    BackOrderReplay,
    /// 后台持仓对账：缓冲柜台原始持仓行，QueryEnd 聚合为整合仓位经持仓桥，
    /// 今/昨明细同步写 PositionDir（不落盘）。
    BackPositions {
        rows: Vec<crate::ctp::core::event::PositionSnapshot>,
    },
}

/// 建连后合约表预热（仅本连接一次）：发起全量合约查询，Instrument 行经
/// `dispatch_event` 写入 [`InstrumentDir`]，QueryEnd 释放闸门后才放行持仓查询。
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

/// 上期所交易所代码：只有 SHFE 强制显式平今/平昨。
///
/// 能源中心 INE 规则类似但首版不纳入（柜台实测后再扩成集合常量，见 plan 风险表）。
pub(crate) const SHFE_EXCHANGE_ID: &str = "SHFE";

/// 一笔平仓单的物理路由决策（纯内核，不触 FFI/目录）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClosePlan {
    /// 非上期所：单笔笼统 `Close`（柜台自动冲抵今/昨，行为与历史完全一致）。
    GenericClose,
    /// 上期所单笔显式腿（纯平今或纯平昨）。
    ShfeSingle(crate::ctp::core::types::Offset, i32),
    /// 上期所跨今昨一单两腿：**先平昨 `yesterday_n` 手、再平今 `today_n` 手**
    /// （平今手续费高，省费优先），两物理报单共享同一 cid
    /// （成交/终态由 leg_table 按 cid 聚合）。
    ShfeTwo { yesterday_n: i32, today_n: i32 },
}

/// 平仓路由决策失败原因（全部 fail-closed，绝不猜测发 Close）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClosePlanError {
    /// SHFE 但今/昨路由表未就绪（刚重连/尚未对账）：推迟到下轮对账后重试。
    SplitNotReady,
    /// 平仓量超过该方向今+昨总仓（prep 内核判不足）。
    InsufficientPosition,
}

/// 平仓纯路由内核：给定交易所与该持仓方向的 (今,昨) 分布，决定物理腿。
///
/// * `exchange != Some("SHFE")`（含目录未预热到的 None）→
///   [`ClosePlan::GenericClose`]，保持非上期所路径零变化；
/// * SHFE：`split=None`（路由表未就绪）→ [`ClosePlanError::SplitNotReady`]；
///   复用 [`prep::close_offsets`] 得 1~2 腿（**先平昨**）——平量 ≤ 昨仓→单笔
///   CloseYesterday、昨仓为 0→单笔 CloseToday、跨今昨→[`ClosePlan::ShfeTwo`]
///   （先 CloseYesterday 后 CloseToday）；总量不足→拒。
fn plan_close(
    volume: i32,
    exchange: Option<&str>,
    split: Option<(i32, i32)>,
) -> Result<ClosePlan, ClosePlanError> {
    use crate::ctp::core::types::Offset;
    use crate::ctp::prep::{close_offsets, TodayYdSplit};
    if exchange != Some(SHFE_EXCHANGE_ID) {
        return Ok(ClosePlan::GenericClose);
    }
    let (today, yesterday) = split.ok_or(ClosePlanError::SplitNotReady)?;
    let legs = close_offsets(volume, Some(TodayYdSplit { today, yesterday }))
        .map_err(|_| ClosePlanError::InsufficientPosition)?;
    // 不直接索引：release 下 debug_assert 被裁掉，裸 legs[1] 越界会 panic 掉整个
    // 账户会话线程。用切片模式匹配把"close_offsets 恒返回 1~2 腿"的隐含契约显式化，
    // 异常形态（未来改动引入 0/3+ 腿）fail-closed 拒单而非崩溃。
    match legs.as_slice() {
        [(offset, n)] => Ok(ClosePlan::ShfeSingle(*offset, *n)),
        // close_offsets 现按「先平昨、不足再平今」产出，腿序固定为昨腿在前。
        [(Offset::CloseYesterday, yesterday_n), (Offset::CloseToday, today_n)] => {
            Ok(ClosePlan::ShfeTwo {
                yesterday_n: *yesterday_n,
                today_n: *today_n,
            })
        }
        // 腿序/数量不符 prep 契约：不下单（理论不可达）。
        _ => Err(ClosePlanError::InsufficientPosition),
    }
}

/// 立即执行非查询命令（下单/撤单）——不占 ActiveQuery、不被查询闸门阻塞。
fn run_command(h: &mut TraderHandle, ctx: &EventContext, req: crate::ctp::mailbox::SessionRequest) {
    use crate::ctp::mailbox::SessionRequest;
    match req {
        SessionRequest::Insert(p) => {
            // 受理码回执：SHFE 平仓路由（可能一单两腿）/校验/FFI 错误一并随 Result
            // 传回 async 侧；未发出腿的脏 OrderRef 映射由 submit_insert 按腿清理。
            let ret = submit_insert(h, ctx, &p.req, &p.order_ref);
            let _ = p.reply.send(ret);
        }
        SessionRequest::Cancel(c) => {
            // 撤单定位在线程内完成：一单两腿时对同 cid 的在途两腿分别寻址撤销。
            let ret = submit_cancel(h, ctx, &c);
            match &ret {
                Ok(code) => tracing::debug!(
                    account = %ctx.account, cid = %c.order.client_order_id, code,
                    "CTP 撤单受理"
                ),
                Err(e) => tracing::warn!(
                    account = %ctx.account, cid = %c.order.client_order_id, error = %e,
                    "CTP 撤单失败"
                ),
            }
            let _ = c.reply.send(ret);
        }
        // 查询类走 issue_front_query，不会进入这里。
        SessionRequest::QueryOrder(_) | SessionRequest::QueryPositions(_) => {}
    }
}

/// 路由后的物理下单：单笔（开仓/非 SHFE 借用原请求零克隆，或 SHFE 改写后拥有），
/// 或跨今昨一单两腿（两笔独立 OrderRequest，共享 cid）。
#[derive(Debug)]
enum RoutedInsert<'a> {
    Single(std::borrow::Cow<'a, crate::ctp::core::order::OrderRequest>),
    Two {
        /// 先平昨腿（省费优先）。
        yesterday: crate::ctp::core::order::OrderRequest,
        /// 再平今腿。
        today: crate::ctp::core::order::OrderRequest,
    },
}

/// 下单请求的平仓路由（纯函数）：开仓原样返回；平仓按交易所+今昨分布把笼统
/// Close 改写为显式 CloseToday/CloseYesterday（SHFE 单腿或跨今昨两腿），非上期所原样返回。
///
/// `exchange` 为该合约的交易所目录查询值，`split` 为**被平持仓方向**的 (今,昨)
/// （reduce_only：SELL 平多、BUY 平空，由调用方换算后传入）。
fn routed_insert<'a>(
    req: &'a crate::ctp::core::order::OrderRequest,
    exchange: Option<&str>,
    split: Option<(i32, i32)>,
) -> Result<RoutedInsert<'a>, ClosePlanError> {
    use crate::ctp::core::types::Offset;
    use std::borrow::Cow;
    if req.offset != Offset::Close {
        return Ok(RoutedInsert::Single(Cow::Borrowed(req)));
    }
    match plan_close(req.volume, exchange, split) {
        Ok(ClosePlan::GenericClose) => Ok(RoutedInsert::Single(Cow::Borrowed(req))),
        // 单腿必为全量（close_offsets 单腿时 n==volume），仅改写开平标志。
        Ok(ClosePlan::ShfeSingle(offset, n)) => {
            debug_assert_eq!(n, req.volume);
            let mut routed = req.clone();
            routed.offset = offset;
            Ok(RoutedInsert::Single(Cow::Owned(routed)))
        }
        // 跨今昨两腿：同价/同方向，仅开平标志与手数不同；昨腿在前（先平昨省费）。
        Ok(ClosePlan::ShfeTwo {
            yesterday_n,
            today_n,
        }) => {
            let make_leg = |offset: Offset, volume: i32| {
                let mut leg = req.clone();
                leg.offset = offset;
                leg.volume = volume;
                leg
            };
            Ok(RoutedInsert::Two {
                yesterday: make_leg(Offset::CloseYesterday, yesterday_n),
                today: make_leg(Offset::CloseToday, today_n),
            })
        }
        Err(e) => Err(e),
    }
}

/// 路由失败 → 面向调用方的明确错误（未送达柜台，无副作用）。
fn close_plan_error(
    e: ClosePlanError,
    req: &crate::ctp::core::order::OrderRequest,
) -> anyhow::Error {
    use crate::ctp::place_error::CtpPlaceError;
    match e {
        // 今昨分布未就绪是可重试的临时态（重连/首轮对账未完成），判应答不确定，
        // 不终态拒绝——对账完成后规则/手动重试仍可下单。
        ClosePlanError::SplitNotReady => CtpPlaceError::indeterminate(format!(
            "CTP SHFE 平仓推迟：{} 的今/昨仓分布尚未就绪（重连/首轮持仓对账未完成），\
             请待对账后重试",
            req.instrument
        )),
        // 平仓量超过今+昨总仓：确定性拒绝（订单未送达柜台）。
        ClosePlanError::InsufficientPosition => CtpPlaceError::rejected(format!(
            "CTP SHFE 平仓被拒：{} 平仓量 {} 超过今+昨总仓",
            req.instrument, req.volume
        )),
    }
}

/// 下单提交：SHFE 平仓先经 [`routed_insert`] 把笼统 Close 改写成显式
/// CloseToday/CloseYesterday（单腿）或跨今昨一单两腿；开仓与非上期所路径零变化。
///
/// 两腿受理语义：**两受理码皆 0 才整体 Ok**；任一非 0/校验错整体 Err（错误带两腿码），
/// 已受理腿不再撤回（靠成交回报 + 持仓对账收敛，不静默）。未就绪/超量 fail-closed。
///
/// cid↔OrderRef 脏映射在本函数内按腿清理（run_command 无法区分"整单未发"与
/// "一腿已受理"）：未发出的腿移除其 ref，已受理的腿保留。
fn submit_insert(
    h: &mut TraderHandle,
    ctx: &EventContext,
    req: &crate::ctp::core::order::OrderRequest,
    order_ref: &str,
) -> anyhow::Result<i32> {
    use crate::ctp::core::types::Direction;
    use crate::ctp::core::types::Offset;
    // reduce_only 语义下：SELL 平的是多头、BUY 平的是空头。
    use crate::ctp::position_dir::PositionSide;
    let holding = match req.direction {
        Direction::Sell => PositionSide::Long,
        Direction::Buy => PositionSide::Short,
    };
    let exchange = ctx.instrument_dir.exchange(&req.instrument);
    let split = ctx.position_dir.get(&req.instrument, holding);
    let routed = match routed_insert(req, exchange.as_deref(), split) {
        Ok(r) => r,
        Err(e) => {
            // 路由失败：整单未发出，首腿（投递前已分配）的映射是脏数据。
            ctx.order_refs.remove(order_ref);
            return Err(close_plan_error(e, req));
        }
    };
    match routed {
        RoutedInsert::Single(effective) => {
            if req.offset == Offset::Close && effective.offset != Offset::Close {
                tracing::info!(
                    account = %ctx.account, instrument = %req.instrument,
                    offset = ?effective.offset, volume = effective.volume,
                    "CTP SHFE 平仓路由：Close → 显式开平标志"
                );
            }
            insert_one_leg(h, ctx, &effective, order_ref, "单腿").map(|()| 0)
        }
        RoutedInsert::Two { yesterday, today } => {
            submit_two_legs(h, ctx, req, order_ref, yesterday, today)
        }
    }
}

/// 提交一条物理腿；受理码 0 为受理，非 0 或本地校验错都判该腿失败并清理其 OrderRef 映射。
fn insert_one_leg(
    h: &mut TraderHandle,
    ctx: &EventContext,
    leg: &crate::ctp::core::order::OrderRequest,
    order_ref: &str,
    label: &str,
) -> anyhow::Result<()> {
    match h.insert_order(leg, order_ref) {
        Ok(0) => Ok(()),
        // 柜台同步受理码非 0：确定性拒绝（订单未受理）。
        Ok(code) => {
            ctx.order_refs.remove(order_ref);
            Err(crate::ctp::place_error::CtpPlaceError::rejected(format!(
                "CTP {label}报单被柜台拒绝：受理码 {code}"
            )))
        }
        // 投递前本地校验失败：确定性拒绝（订单未送达柜台）。
        Err(e) => {
            ctx.order_refs.remove(order_ref);
            Err(crate::ctp::place_error::CtpPlaceError::rejected(format!(
                "CTP {label}报单校验失败: {e}"
            )))
        }
    }
}

/// 跨今昨两腿提交：同 cid、两个不同 OrderRef，**先平昨、再平今**（省平今高手续费）。
fn submit_two_legs(
    h: &mut TraderHandle,
    ctx: &EventContext,
    req: &crate::ctp::core::order::OrderRequest,
    first_ref: &str,
    yesterday: crate::ctp::core::order::OrderRequest,
    today: crate::ctp::core::order::OrderRequest,
) -> anyhow::Result<i32> {
    // 第二腿需要本会话第二个唯一 OrderRef；只有长 cid（在 registry 有映射）能分配。
    // 短 cid（≤13B 透传）allocate 会原样返回同一串，无法支撑两腿，fail-closed。
    let Some(cid) = ctx.order_refs.cid_of(first_ref) else {
        ctx.order_refs.remove(first_ref);
        // 两腿都未发出：确定性拒绝（短 cid 不支持一单两腿）。
        return Err(crate::ctp::place_error::CtpPlaceError::rejected(format!(
            "CTP SHFE 跨今昨两腿下单失败：{} 的 cid 未在 OrderRef 映射中（短 cid 不支持两腿）",
            req.instrument
        )));
    };
    let second_ref = ctx.order_refs.allocate(&cid);
    tracing::info!(
        account = %ctx.account, instrument = %req.instrument, cid = %cid,
        yesterday_n = yesterday.volume, today_n = today.volume,
        yesterday_ref = first_ref, today_ref = second_ref,
        "CTP SHFE 平仓路由：Close → 一单两腿（先平昨 + 再平今）"
    );
    // 第一腿（平昨）失败：第二腿未发，两个 ref 都是脏数据。
    if let Err(e) = insert_one_leg(h, ctx, &yesterday, first_ref, "平昨腿") {
        ctx.order_refs.remove(&second_ref);
        return Err(e);
    }
    // 昨腿已受理：今腿失败只清第二腿映射，昨腿保留（靠回报/对账收敛，不静默）。
    // 因已有一腿送达柜台，整体应答不确定（不能当未受理拒绝），落 UNKNOWN 待收敛。
    if let Err(e) = insert_one_leg(h, ctx, &today, &second_ref, "平今腿") {
        return Err(crate::ctp::place_error::CtpPlaceError::indeterminate(
            format!(
            "CTP 一单两腿：平昨腿已受理、平今腿失败（{e}）；整体判失败，已受理腿靠成交回报/对账收敛"
        ),
        ));
    }
    Ok(0)
}

/// 一单两腿的撤单寻址（纯函数）：每个 OrderRef 各按当前会话
/// FrontID+SessionID 组装一个三元组目标（两腿在同一会话提交，Front/Session 相同）。
fn two_leg_cancel_targets(
    refs: &[String],
    front_id: i32,
    session_id: i32,
) -> Vec<(String, crate::ctp::mailbox::CancelTarget)> {
    use crate::ctp::mailbox::CancelTarget;
    refs.iter()
        .map(|order_ref| {
            (
                order_ref.clone(),
                CancelTarget::ByOrderRef {
                    front_id,
                    session_id,
                    order_ref: order_ref.clone(),
                },
            )
        })
        .collect()
}

/// 撤单提交：一单两腿时对同 cid 的在途两腿分别按本会话三元组撤销，两腿受理码皆 0
/// 才整体 Ok，任一非 0 整体 Err（带各腿码）；单腿/跨会话走既有寻址，
/// 回执语义保持不变（Ok 携带受理码，0=受理）。
fn submit_cancel(
    h: &mut TraderHandle,
    ctx: &EventContext,
    c: &crate::ctp::mailbox::CancelJob,
) -> anyhow::Result<i32> {
    let refs = ctx.order_refs.order_refs_of_cid(&c.order.client_order_id);
    if refs.len() >= 2 {
        // 跨今昨一单两腿：两腿均本会话登记，各按 FrontID+SessionID+OrderRef 撤销。
        let targets = two_leg_cancel_targets(&refs, h.front_id(), h.session_id());
        let mut codes: Vec<(String, i32)> = Vec::with_capacity(targets.len());
        for (order_ref, target) in targets {
            codes.push((order_ref.clone(), h.cancel_order(&c.instrument, &target)));
        }
        if codes.iter().all(|(_, code)| *code == 0) {
            return Ok(0);
        }
        // 受理码非 0 多为该腿已终态/不存在；柜台已收下的撤单请求不可撤回，
        // 必须把"哪些腿已受理/哪些失败"分开讲清，避免调用方误判两腿都没撤。
        let accepted: Vec<&String> = codes
            .iter()
            .filter_map(|(r, code)| (*code == 0).then_some(r))
            .collect();
        let rejected: Vec<&(String, i32)> = codes.iter().filter(|(_, code)| *code != 0).collect();
        anyhow::bail!(
            "CTP 一单两腿撤单部分失败：已受理腿 {accepted:?}，失败腿(受理码) {rejected:?}；\
             已受理的撤单不可撤回，请查订单状态确认"
        );
    }
    // 单腿/跨会话：既有寻址（registry 三元组或 ExchangeID+OrderSysID）。
    let target = resolve_cancel_target(
        &ctx.order_refs,
        &ctx.instrument_dir,
        &c.instrument,
        &c.order,
        h.front_id(),
        h.session_id(),
    )?;
    Ok(h.cancel_order(&c.instrument, &target))
}

/// 发起后台持仓对账；受理码非 0（流控/未就绪）不占位，下一轮重试。
/// 调用方须保证当前无在途查询（active 为 None）。
fn issue_back_positions(h: &mut TraderHandle, active: &mut Option<ActiveQuery>) {
    if h.query_positions() == 0 {
        *active = Some(ActiveQuery::BackPositions { rows: Vec::new() });
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
                    rows: Vec::new(),
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
    /// 进程级共享合约静态目录（交易所/乘数/最小变动价位；合约查询预热）。
    /// SHFE 平仓路由判交易所、持仓聚合取乘数、市价转限价取 tick 都用它。
    pub instrument_dir: Arc<crate::ctp::instrument_dir::InstrumentDir>,
    /// (合约,持仓方向) → (今,昨) 会话内存路由表（本账户私有；SHFE 平今/平昨路由；不落盘）。
    pub position_dir: Arc<crate::ctp::position_dir::PositionDir>,
    /// cid → 物理腿委托快照（跨今昨一单两腿的成交/终态聚合；仅会话线程用）。
    pub leg_table: Arc<crate::ctp::leg_table::LegTable>,
    /// 全成委托快照先于成交回报到达时的「待终态对齐」cid 集合（仅会话线程用）。
    pub pending_terminal: Arc<crate::ctp::pending_terminal::PendingTerminalTable>,
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
        // 实时委托回报：永远透传订单端口（即便有查询在途，也绝不被查询缓冲吞掉）。
        // 合约静态信息（含交易所）由建连后最高优先的合约预热统一写入共享 InstrumentDir，
        // 这里不再用回报单独喂交易所目录（回报缺乘数/最小变动价位，无法构成完整静态信息）。
        CtpEvent::Order(s) => {
            forward_order_snapshot(ctx, handle, s);
        }
        // 当日报单查询行：按在途闸门分流——前台查单缓冲，后台重放走订单桥。
        CtpEvent::OrderQuery(s) => match active {
            Some(ActiveQuery::FrontOrder { rows, .. }) => rows.push(s),
            // 后台重放（或无在途查询的防御情形）：与实时回报同路径透传。
            _ => forward_order_snapshot(ctx, handle, s),
        },
        CtpEvent::Trade(t) => {
            // 回报反查：OrderRef → 本地长 cid（未命中保留原值，宿主 fallback 兜底）。
            // 两腿共享同一 cid，必须先解析再按 cid 累计，两腿成交才会求和。
            let cid = resolve_event_cid(ctx, &t.order_ref);
            // 成交回报：按 cid 累计两腿成交量与成交额，算成交均价（VWAP）后推订单端口。
            let (cum, avg_price) = {
                let mut qty = handle.filled_by_cid.lock().unwrap();
                let mut amt = handle.filled_amount_by_cid.lock().unwrap();
                let q = qty.entry(cid.clone()).or_insert(0);
                *q += t.volume.max(0) as i64;
                let a = amt.entry(cid.clone()).or_insert(0.0);
                *a += t.price.max(0.0) * t.volume.max(0) as f64;
                // 同一把锁作用域内算出均价，避免跨锁读取不一致。
                let avg = if *q > 0 { *a / (*q as f64) } else { 0.0 };
                (*q, avg)
            };
            // 今/昨路由表的成交增量（主数据仍由周期持仓快照整批覆盖）：
            // 开仓增减方向=成交方向；平仓=反向（卖平多/买平空）；笼统 Close 不分今昨，
            // apply_trade 内部跳过，等快照收敛。未知开平标志（None）同样不动表。
            if let Some(offset) = t.offset {
                use crate::ctp::position_dir::PositionSide;
                let holding = if offset == crate::ctp::core::types::Offset::Open {
                    PositionSide::opened_by(t.direction)
                } else {
                    PositionSide::closed_by(t.direction)
                };
                ctx.position_dir
                    .apply_trade(&t.instrument, offset, holding, t.volume);
            }
            // 终态对齐：全成委托快照先到、本笔成交正好把累计量补齐到快照量时，
            // 经唯一收口投影带真实 VWAP 的终态，不再单独发成交 PARTIAL（终态事件
            // 已含相同累计成交量/均价）。未补齐（分批成交）则照常发带价 PARTIAL。
            if trade_completes_pending_terminal(ctx, &cid, cum) {
                project_terminal(ctx, handle, &cid);
                return;
            }
            let state = crate::ctp::mapping::order_state_from_trade(
                &t,
                rust_decimal::Decimal::from(cum),
                avg_price,
            );
            // cid 已在上方反查（两腿共享，成交按 cid 累计）。
            let mut event = crate::translate::order_event(&state);
            event.client_order_id = cid;
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
        // 合约预热行：写进程级共享静态目录（交易所/乘数/最小变动价位），
        // 仅在合约查询在途时接收（其余情形忽略，避免无在途查询的杂散行污染）。
        CtpEvent::Instrument(i) => {
            if matches!(active, Some(ActiveQuery::Contracts)) {
                ctx.instrument_dir.remember(i);
            }
        }
        CtpEvent::Position(p) => {
            // 仅缓冲柜台**原始行**：SHFE 同 (合约,方向) 的今/昨仓是两行，逐行映射会
            // 向上游泄漏两条仓位。聚合（按乘数折算单价 + 今昨喂路由表）推迟到
            // QueryEnd 收齐后在 finalize_position_batch 一次完成。
            match active {
                Some(
                    ActiveQuery::FrontPositions { rows, .. } | ActiveQuery::BackPositions { rows },
                ) => {
                    rows.push(p);
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

/// 判定柜台 StatusMsg 是否为「拒绝/出错」类原因（需 warn）。
///
/// 正常态文案：空、「全部成交」「未成交」「已撤单」「报单已提交」「部成」等。
/// 柜台把受理后拒绝/错误的原因写在 StatusMsg（GB18030），实测形如
/// `49:已撤单报单被拒绝SHFE:价格超出涨停板`、`707:…指定商品不在本交易节交易`，
/// 并常以 `错误码:`/`CTP:` 前缀给出。命中「拒绝/出错/错误/废单/不足/超出涨跌停…」
/// 等关键词即视为异常原因；保守起见只收敛明确词，宁可漏判为 debug，不可误判刷屏。
fn is_venue_reject_status_msg(msg: &str) -> bool {
    const REJECT_KEYWORDS: &[&str] = &[
        "拒绝",
        "废单",
        "出错",
        "错误",
        "不足",
        "超出涨跌停",
        "超过涨跌停",
        "涨跌停板",
        "不在本交易节",
        "不能成交",
        "禁止",
        "违规",
    ];
    REJECT_KEYWORDS.iter().any(|k| msg.contains(k))
}

/// 委托快照 → 订单端口（实时回报与后台重放共用同一路径）。
///
/// 一个 cid 可能对应跨今昨一单两腿：先把本腿最新快照写入 LegTable，按 **cid 聚合**
/// 两腿的状态/成交量后只上报一条逻辑单（一腿成一腿在途→Partial，两腿齐终态才终态）；
/// cid 全部腿终态时回收 cid↔OrderRef 映射、cid 成交累计与腿表。
fn forward_order_snapshot(
    ctx: &EventContext,
    handle: &Arc<CtpUserHandle>,
    s: crate::ctp::core::event::OrderSnapshot,
) {
    use crate::ctp::core::types::OrderStatus as CtpStatus;
    use crate::ctp::mapping::order_state_from_snapshot;
    // 回报反查：OrderRef → 本地长 cid（两腿共享同一 cid；未命中保留原值）。
    let cid = resolve_event_cid(ctx, &s.order_ref);
    let instrument = s.instrument.clone();
    // 柜台在委托快照里回填的状态/原因（GB18030 已在 FFI 层解码）。注意「受理后被柜台
    // 撤/拒」不一定伴随 OnErrRtnOrderInsert（真机：SHFE 涨停板拒单只来 OnRtnOrder
    // Cancelled + StatusMsg），只看错误帧会丢原因（#5/C11 排查关键）。正常态 StatusMsg
    // 为「全部成交/未成交/已撤单/报单已提交」等——降 debug 不刷屏；仅拒单/出错类原因
    // （见 is_venue_reject_status_msg）打 warn 便于告警与复盘。
    let status_msg = s.status_msg.trim();
    if is_venue_reject_status_msg(status_msg) {
        tracing::warn!(
            account = %ctx.account, cid = %cid, instrument = %s.instrument,
            order_ref = %s.order_ref, raw_status = ?s.status, status_msg = %status_msg,
            "CTP 委托回报：订单被柜台拒绝/撤销"
        );
    } else if !status_msg.is_empty() {
        tracing::debug!(
            account = %ctx.account, cid = %cid, order_ref = %s.order_ref,
            raw_status = ?s.status, status_msg = %status_msg,
            "CTP 委托回报状态"
        );
    }
    ctx.leg_table.upsert(&cid, s);
    // 聚合此刻该 cid 的全部腿（单腿单聚合即其自身）。
    let Some(agg) = ctx.leg_table.aggregate_cid(&cid) else {
        return;
    };
    // 预期腿数取 registry 中该 cid 登记的 OrderRef 数（下单时已登记，终态回收前都在）：
    // 首腿 AllTraded 可能先于次腿首报到达，仅凭已收腿会误判整单终态而过早回收。
    // 透传/孤儿 cid 无映射，按单腿处理。
    let expected_legs = ctx.order_refs.order_refs_of_cid(&cid).len().max(1);
    let received_legs = ctx.leg_table.leg_count(&cid);
    if received_legs > expected_legs {
        // 同一 cid 收到超出登记数的腿：可能是重连重放/OrderRef 冲突，告警但不静默吞掉。
        tracing::warn!(
            account = %ctx.account, cid = %cid, received_legs, expected_legs,
            "CTP 两腿聚合：收到腿数超过登记 OrderRef 数"
        );
    }
    // 严格相等才算收齐（不接受超额冒充完成，避免异常形态下提前收敛）。
    let legs_complete = received_legs == expected_legs;
    let is_terminal = agg.all_terminal && legs_complete;

    // 终态对齐（修真实柜台 filled_price=0）：委托快照（OnRtnOrder）管生命周期但
    // **不带成交价**，成交回报（OnRtnTrade）才带真实 VWAP。真机上全成快照常先于
    // 成交回报 1~2ms 到达。若见全成快照即抛终态，晚到成交帧会在共享状态机被当
    // 「终态后帧」丢弃 → 订单 filled_price 恒 0、通知价格/平仓盈亏错。故此处把两路
    // join 收口在 CTP 层：成交未到齐先暂不投影终态/回收，等成交到齐再 project_terminal。
    if is_terminal && should_defer_terminal_trade(ctx, handle, &cid, agg.filled) {
        ctx.pending_terminal
            .mark(&cid, agg.filled.max(0) as i64, now_ms());
        tracing::debug!(
            account = %ctx.account, cid = %cid, snapshot_filled = agg.filled,
            trade_filled = handle.filled_of_cid(&cid),
            "CTP 终态对齐：全成快照先到，暂不投影终态，等待成交回报补齐 VWAP"
        );
        // 延迟期不发任何进度帧：宿主此前已收 NEW/排队帧，订单保持其最后非终态；
        // 若此处补发「快照成交量」的 PARTIAL，会与随后逐笔成交累计量短暂不一致
        // （非单调）。唯一终态（带 VWAP）由成交到齐或超时兜底经 project_terminal 投影。
        note_order_dirty(ctx, &instrument);
        return;
    }

    if is_terminal {
        // 零成交终态（撤/拒），或成交回报已先到齐：经唯一收口投影带 VWAP 的终态并回收。
        project_terminal(ctx, handle, &cid);
        return;
    }

    // 非终态进度：腿未收齐/在途——有成交报 Partial、无成交报排队、其余取聚合状态。
    let reported_status = if agg.filled > 0 {
        CtpStatus::PartTradedQueueing
    } else if agg.all_terminal {
        CtpStatus::NoTradeQueueing
    } else {
        agg.status
    };
    // 以代表腿（优先有 OrderSysID 的腿）承载单号/合约，状态与成交量用聚合值覆盖。
    let mut state = order_state_from_snapshot(&agg.representative);
    state.status = crate::ctp::mapping::status_from_snapshot(reported_status);
    state.filled_quantity = rust_decimal::Decimal::from(agg.filled.max(0));
    let mut event = crate::translate::order_event(&state);
    event.client_order_id = cid.clone();
    // 阻塞线程 → async 端口经有界队列投递（满则阻塞背压，不丢单）。
    if let Err(e) = ctx.tx.blocking_send(BridgeEvent::Order(Box::new(event))) {
        tracing::warn!(account = %ctx.account, "事件桥已关闭，订单事件丢弃：{e}");
    }
    note_order_dirty(ctx, &instrument);
}

/// 全成委托快照是否需要**暂缓终态**等成交回报：
/// 有成交量、且是本会话 registry 内活动 cid、但成交回报累计尚未到齐快照量。
/// 零成交终态（撤/拒）无需成交价立即投影；重连回放/跨会话的孤儿 cid（registry
/// 无映射）也立即投影、维持历史行为，避免历史单被挂起。
fn should_defer_terminal_trade(
    ctx: &EventContext,
    handle: &Arc<CtpUserHandle>,
    cid: &str,
    snapshot_filled: i32,
) -> bool {
    if snapshot_filled <= 0 {
        return false;
    }
    if ctx.order_refs.order_refs_of_cid(cid).is_empty() {
        return false;
    }
    handle.filled_of_cid(cid) < snapshot_filled as i64
}

/// 读 cid 截至当前的成交均价（VWAP = Σ成交额 ÷ Σ成交量）；无成交返回 None（不臆造价）。
fn vwap_of_cid(handle: &Arc<CtpUserHandle>, cid: &str) -> Option<rust_decimal::Decimal> {
    let qty = handle.filled_of_cid(cid);
    if qty <= 0 {
        return None;
    }
    let amount = handle
        .filled_amount_by_cid
        .lock()
        .ok()?
        .get(cid)
        .copied()
        .unwrap_or(0.0);
    // Decimal::from_f64_retain 为 rust_decimal 固有方法（无需 FromPrimitive 导入）。
    rust_decimal::Decimal::from_f64_retain(amount / qty as f64)
        .filter(|p| *p > rust_decimal::Decimal::ZERO)
}

/// 终态唯一投影收口：从腿表聚合终态、附成交回报累计的真实 VWAP，向上游发**一条**
/// 终态订单事件，随后回收待对齐标记 / cid↔OrderRef 映射 / 成交累计 / 腿表。
///
/// 幂等：投影即清腿表，重复调用（成交到齐与超时兜底竞争）因腿表已空直接返回，
/// 保证终态只发一次。VWAP 在回收成交累计**之前**读取。
fn project_terminal(ctx: &EventContext, handle: &Arc<CtpUserHandle>, cid: &str) {
    use crate::ctp::core::types::OrderStatus as CtpStatus;
    // 重新聚合（投影前不允许清腿表）；已回收则 no-op。
    let Some(agg) = ctx.leg_table.aggregate_cid(cid) else {
        ctx.pending_terminal.remove(cid);
        return;
    };
    let expected_legs = ctx.order_refs.order_refs_of_cid(cid).len().max(1);
    let legs_complete = ctx.leg_table.leg_count(cid) == expected_legs;
    // 正常终态（腿齐全终态）取聚合终态 AllTraded/Cancelled…；理论上 project 只在
    // 终态或超时兜底时调用，腿表保留的是标记时的全成/终态快照，故此处恒为终态。
    let reported = if agg.all_terminal && legs_complete {
        agg.status
    } else if agg.filled > 0 {
        CtpStatus::PartTradedQueueing
    } else {
        CtpStatus::NoTradeQueueing
    };
    let vwap = vwap_of_cid(handle, cid);
    if agg.filled > 0 && vwap.is_none() {
        // 有成交却拿不到 VWAP：成交回报在宽限内未到齐（异常丢帧/超时兜底）。
        tracing::warn!(
            account = %ctx.account, cid = %cid, snapshot_filled = agg.filled,
            trade_filled = handle.filled_of_cid(cid),
            "CTP 终态投影缺成交回报 VWAP（可能丢帧），按快照成交量投影、成交价留空"
        );
    }
    let mut state = crate::ctp::mapping::order_state_from_snapshot(&agg.representative);
    state.status = crate::ctp::mapping::status_from_snapshot(reported);
    state.filled_quantity = rust_decimal::Decimal::from(agg.filled.max(0));
    // 快照本身无价；真实成交均价只来自 OnRtnTrade 累计。
    state.average_price = vwap;
    let mut event = crate::translate::order_event(&state);
    event.client_order_id = cid.to_string();
    if let Err(e) = ctx.tx.blocking_send(BridgeEvent::Order(Box::new(event))) {
        tracing::warn!(account = %ctx.account, "事件桥已关闭，终态订单事件丢弃：{e}");
    }
    // 先发后回收（VWAP 已在上方读取）。
    ctx.pending_terminal.remove(cid);
    release_terminal_cid(ctx, handle, cid);
    ctx.leg_table.remove_cid(cid);
    let instrument = agg.representative.instrument.clone();
    note_order_dirty(ctx, &instrument);
}

/// 成交回报到达时，判定其是否正好补齐某笔「待终态对齐」单：腿表已全成全终态、
/// 成交累计达到快照成交量。
fn trade_completes_pending_terminal(ctx: &EventContext, cid: &str, cumulative_qty: i64) -> bool {
    if !ctx.pending_terminal.contains(cid) {
        return false;
    }
    let Some(agg) = ctx.leg_table.aggregate_cid(cid) else {
        return false;
    };
    let expected_legs = ctx.order_refs.order_refs_of_cid(cid).len().max(1);
    let legs_complete = ctx.leg_table.leg_count(cid) == expected_legs;
    agg.all_terminal && legs_complete && cumulative_qty >= agg.filled as i64
}

/// 订单/成交事件后的持仓去抖脏信号（成交可能改持仓，触发去抖对账）。
fn note_order_dirty(ctx: &EventContext, instrument: &str) {
    ctx.debouncer.lock().unwrap().note_dirty(
        &crate::api::AccountDirty {
            instruments: vec![instrument.to_string()],
        },
        now_ms(),
    );
}

/// cid 终态回收：移除该 cid 的全部 OrderRef 映射（一单两腿有两条）与 cid 成交累计。
/// 透传/孤儿 cid（registry 无映射）仅清成交累计，remove 天然 no-op。
/// 零成交终态（Cancelled/Rejected）的 cid 从无成交累计项，此处 remove 亦为无害 no-op，
/// 不构成泄漏——成交表只在首笔成交时建项。
fn release_terminal_cid(ctx: &EventContext, handle: &Arc<CtpUserHandle>, cid: &str) {
    for order_ref in ctx.order_refs.order_refs_of_cid(cid) {
        ctx.order_refs.remove(&order_ref);
    }
    let removed_cum = handle.filled_by_cid.lock().unwrap().remove(cid).is_some();
    handle.filled_amount_by_cid.lock().unwrap().remove(cid);
    tracing::debug!(
        account = %ctx.account, cid, filled_acc_removed = removed_cum,
        "CTP 终态回收：cid↔OrderRef 映射（全部腿）与 cid 成交累计已释放"
    );
}

/// 持仓批次收尾（前台查仓/后台对账共用）：把柜台原始持仓行按 `(合约, 持仓方向)`
/// 分桶，逐桶经 [`mapping::merge_position_snapshots`] 聚合成**一条**整合 PositionInfo，
/// 同时把 (Σ今, Σ昨) 写会话内存 PositionDir（不落盘）。
///
/// 乘数未预热的合约整桶跳过（宁漏一批，等下轮对账）；position=0 空壳行用于把
/// 该方向的路由表项清零。按首次出现顺序回执（结果稳定，便于测试/诊断）。
fn finalize_position_batch(
    ctx: &EventContext,
    rows: Vec<crate::ctp::core::event::PositionSnapshot>,
) -> Vec<crate::api::PositionInfo> {
    use crate::ctp::mapping::merge_position_snapshots;
    use crate::ctp::position_dir::PositionSide;

    let (mut groups, order) = group_position_rows(rows);
    let mut out = Vec::with_capacity(order.len());
    for key in order {
        // order 仅在分桶 Vacant 时压入，key 理论上必在 groups；
        // unwrap_or_default 是不 panic 的防御兜底，正常路径恒命中。
        let group = groups.remove(&key).unwrap_or_default();
        let (instrument, dir_char) = key;
        // 分桶时已过滤 dir_char∈{'2','3'}；异常桶直接跳过，不留 panic 点。
        let Some(side) = PositionSide::from_position_char(dir_char) else {
            continue;
        };
        let mult = ctx.instrument_dir.multiple(&instrument);
        // 合约多/空保证金率与乘数同源（InstrumentDir）；UseMargin=0 时供 merge 兜底估算。
        let (long_ratio, short_ratio) = ctx.instrument_dir.margin_ratios(&instrument);
        match merge_position_snapshots(&group, mult, long_ratio, short_ratio) {
            Some(merged) => {
                // 快照是真相源：整批覆盖该 (合约,方向) 的今/昨。
                ctx.position_dir
                    .set(&instrument, side, merged.today, merged.yesterday);
                out.push(merged.info);
            }
            None if mult <= 0 => {
                // 乘数未就绪：整桶不映射也**不**动路由表（下轮对账补齐）。
            }
            None => {
                // 乘数就绪却无有效行 = 整桶空壳（该方向已无持仓）：
                // 用空壳行的今/昨（通常 0,0）覆盖，移除过期路由表项。
                let today = group
                    .iter()
                    .fold(0_i32, |acc, r| acc.saturating_add(r.today_position.max(0)));
                let yesterday = group
                    .iter()
                    .fold(0_i32, |acc, r| acc.saturating_add(r.yd_position.max(0)));
                ctx.position_dir.set(&instrument, side, today, yesterday);
            }
        }
    }
    out
}

/// 持仓分桶键：(trim 后合约, 持仓方向字符 '2'多/'3'空)。
type PositionGroupKey = (String, char);
/// 持仓分桶表：同 (合约,方向) 的今/昨/空壳原始行聚于一处。
type PositionGroups =
    std::collections::HashMap<PositionGroupKey, Vec<crate::ctp::core::event::PositionSnapshot>>;

/// 把柜台原始持仓行按 `(trim 合约, 持仓方向字符)` 分桶（含 position=0 空壳行——
/// 全空壳意味着该方向已清零，需据此移除路由表项），返回分桶表与 key 首次出现序列
/// （回执顺序稳定，便于测试/诊断）。空合约/方向非 '2'/'3' 的行丢弃。
fn group_position_rows(
    rows: Vec<crate::ctp::core::event::PositionSnapshot>,
) -> (PositionGroups, Vec<PositionGroupKey>) {
    use std::collections::hash_map::Entry;
    use std::collections::HashMap;

    let mut groups: PositionGroups = HashMap::new();
    let mut order: Vec<PositionGroupKey> = Vec::new();
    for row in rows {
        let instrument = row.instrument.trim().to_string();
        if instrument.is_empty() || !matches!(row.direction, '2' | '3') {
            // 未知方向/空合约无法归入多空任一桶，丢弃。
            continue;
        }
        match groups.entry((instrument, row.direction)) {
            Entry::Vacant(e) => {
                order.push(e.key().clone());
                e.insert(vec![row]);
            }
            Entry::Occupied(mut e) => e.get_mut().push(row),
        }
    }
    (groups, order)
}

/// 一次查询收齐（QueryEnd）：按在途查询种类前台 oneshot / 后台桥收尾，并清空闸门。
fn finish_active_query(ctx: &EventContext, active: &mut Option<ActiveQuery>) {
    let Some(current) = active.take() else {
        return;
    };
    match current {
        // 合约预热收尾：Instrument 行已逐条写入共享 InstrumentDir，这里仅释放闸门。
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
        ActiveQuery::FrontPositions { rows, reply } => {
            // QueryEnd 收齐：原始行聚合为整合仓位（顺带喂 PositionDir）。
            // 合法空仓 = Ok(空)；断线场景由 Sender drop 传递，不在此伪造错误。
            let positions = finalize_position_batch(ctx, rows);
            if let Some(reply) = reply {
                let _ = reply.send(Ok(positions));
            }
        }
        ActiveQuery::BackOrderReplay => {
            // 重放行已在 OrderQuery 分支逐条透传订单桥，QueryEnd 仅释放闸门。
        }
        ActiveQuery::BackPositions { rows } => {
            // 后台对账：原始行聚合为整合仓位（今/昨明细喂 PositionDir），整批经持仓桥。
            let positions = finalize_position_batch(ctx, rows);
            if let Err(e) = ctx.tx.blocking_send(BridgeEvent::Positions(positions)) {
                tracing::warn!(account = %ctx.account, "事件桥已关闭，持仓快照丢弃：{e}");
            }
        }
    }
}

/// 撤单定位路由（纯函数；单号路由收口在适配器内，宿主不写 CTP 分支）。
///
/// 两路定位（与计划"重启后单号路由"定论一致）：
/// 1. **本会话三元组**：cid 经 registry 反查到本会话自产 OrderRef →
///    `FrontID+SessionID+OrderRef`（Front/Session 取线程当前 SessionState）；
/// 2. **跨会话系统号**：registry 未命中（重启后表空）→ `ExchangeID+OrderSysID`，
///    OrderSysID 取受理后升级的 `exchange_order_id`，ExchangeID 取共享 InstrumentDir。
///
/// 无法定位（无 eid 或 InstrumentDir 未预热）时返回错误——绝不凭猜测撤单。
fn resolve_cancel_target(
    registry: &crate::ctp::order_ref::OrderRefRegistry,
    dir: &crate::ctp::instrument_dir::InstrumentDir,
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
    // ExchangeID 取自共享合约目录（合约查询/回报预热，零额外请求）。
    let Some(exchange_id) = dir.exchange(instrument) else {
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
            filled_by_cid: Arc::new(Mutex::new(std::collections::HashMap::new())),
            filled_amount_by_cid: Arc::new(Mutex::new(std::collections::HashMap::new())),
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
    fn filled_of_cid_defaults_zero_and_reads_cumulative() {
        let h = handle();
        assert_eq!(h.filled_of_cid("unknown-cid"), 0);
        h.filled_by_cid
            .lock()
            .unwrap()
            .insert("0xcid".to_string(), 3);
        assert_eq!(h.filled_of_cid("0xcid"), 3);
    }

    // ---- 一单两腿聚合 / cid 终态回收（dispatch 驱动）----

    /// 构造委托快照（ref/sys/status/量可配）。
    fn leg_snap(
        order_ref: &str,
        status: OrderStatus,
        total: i32,
        traded: i32,
        sys_id: &str,
    ) -> OrderSnapshot {
        OrderSnapshot {
            order_ref: order_ref.to_string(),
            instrument: "rb2510".to_string(),
            exchange_id: "SHFE".to_string(),
            order_sys_id: sys_id.to_string(),
            insert_time: "09:30:00".to_string(),
            direction: Direction::Sell,
            status,
            limit_price: 3500.0,
            volume_total: total,
            volume_traded: traded,
            status_msg: String::new(),
            front_id: 1,
            session_id: 1,
        }
    }

    /// 取桥上下一条订单事件的 (cid,status,filled)。
    fn drain_next(
        rx: &mut mpsc::Receiver<BridgeEvent>,
    ) -> (String, String, Option<rust_decimal::Decimal>) {
        match rx.try_recv().expect("应有一条订单事件") {
            BridgeEvent::Order(e) => (
                e.client_order_id.clone(),
                e.status.clone(),
                e.filled_quantity,
            ),
            other => panic!("期望订单事件，实际 {other:?}"),
        }
    }

    #[test]
    fn two_leg_order_aggregates_until_both_terminal_then_reaps() {
        // Arrange：一个长 cid 登记两个 OrderRef（平今腿/平昨腿）。
        let (ctx, mut rx) = ctx_and_rx();
        let h = handle();
        let cid = long_cid("T");
        let r1 = ctx.order_refs.allocate(&cid);
        let r2 = ctx.order_refs.allocate(&cid);
        h.filled_by_cid.lock().unwrap().insert(cid.clone(), 3);

        // Act ①：两腿都部分成交（1+2=3，总量 2+1=3… 用 2/1 腿）。
        dispatch_event(
            CtpEvent::Order(leg_snap(&r1, OrderStatus::PartTradedQueueing, 2, 1, "s1")),
            &h,
            &ctx,
            &mut None,
        );
        dispatch_event(
            CtpEvent::Order(leg_snap(&r2, OrderStatus::PartTradedQueueing, 1, 1, "s2")),
            &h,
            &ctx,
            &mut None,
        );
        let (c1, st1, f1) = drain_next(&mut rx);
        let _ = drain_next(&mut rx);
        assert_eq!(c1, cid);
        assert_eq!(st1, "PARTIAL");
        assert_eq!(
            f1,
            Some(rust_decimal::Decimal::from(1)),
            "首报时只见首腿成交 1"
        );
        assert!(ctx.leg_table.contains(&cid), "在途不得回收");

        // Act ②：两腿全成终态。
        dispatch_event(
            CtpEvent::Order(leg_snap(&r1, OrderStatus::AllTraded, 2, 2, "s1")),
            &h,
            &ctx,
            &mut None,
        );
        dispatch_event(
            CtpEvent::Order(leg_snap(&r2, OrderStatus::AllTraded, 1, 1, "s2")),
            &h,
            &ctx,
            &mut None,
        );
        // 收集桥上所有订单事件，最后一条聚合事件应是整单 FILLED、filled=3。
        let mut events = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            if let BridgeEvent::Order(e) = ev {
                events.push((
                    e.client_order_id.clone(),
                    e.status.clone(),
                    e.filled_quantity,
                ));
            }
        }
        let (cc, stc, fc) = events.last().expect("两腿终态应有事件");
        assert_eq!(*cc, cid);
        assert_eq!(stc, "FILLED");
        assert_eq!(*fc, Some(rust_decimal::Decimal::from(3)), "filled=两腿和 3");

        // Assert：两腿齐终态后回收 cid 全部映射/累计/腿表。
        assert!(ctx.order_refs.cid_of(&r1).is_none());
        assert!(ctx.order_refs.cid_of(&r2).is_none());
        assert!(!ctx.leg_table.contains(&cid));
        assert_eq!(h.filled_of_cid(&cid), 0);
    }

    /// 构造成交回报（ref/价/量；买开），供「终态快照 × 成交回报对齐」用例。
    fn trade_snap(
        order_ref: &str,
        price: f64,
        volume: i32,
    ) -> crate::ctp::core::event::TradeSnapshot {
        crate::ctp::core::event::TradeSnapshot {
            order_ref: order_ref.to_string(),
            trade_id: "t1".into(),
            instrument: "rb2510".into(),
            exchange_id: "SHFE".into(),
            order_sys_id: "s1".into(),
            direction: Direction::Buy,
            offset: Some(crate::ctp::core::types::Offset::Open),
            price,
            volume,
            trade_date: "20261019".into(),
            trade_time: "09:40:00".into(),
        }
    }

    /// 排干桥上全部订单事件，收集 (状态, 成交量, 成交价)，按发出顺序。
    fn drain_all_orders(
        rx: &mut mpsc::Receiver<BridgeEvent>,
    ) -> Vec<(
        String,
        Option<rust_decimal::Decimal>,
        Option<rust_decimal::Decimal>,
    )> {
        let mut out = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            if let BridgeEvent::Order(e) = ev {
                out.push((e.status.clone(), e.filled_quantity, e.filled_price));
            }
        }
        out
    }

    #[test]
    fn filled_terminal_snapshot_waits_for_trade_and_emits_priced_terminal() {
        // bug 复现（真机 risk-1.1）：全成 OnRtnOrder 快照先到（快照无成交价），
        // 带 VWAP 的 OnRtnTrade 晚到。旧逻辑见全成快照即抛无价 FILLED 并回收，
        // 晚到的成交帧在共享状态机被当「终态后帧」丢弃 → filled_price 恒 0。
        let (ctx, mut rx) = ctx_and_rx();
        let h = handle();
        let cid = long_cid("W");
        let r1 = ctx.order_refs.allocate(&cid);

        // ① 全成委托快照先到（traded=2，无成交价）。
        dispatch_event(
            CtpEvent::Order(leg_snap(&r1, OrderStatus::AllTraded, 2, 2, "s1")),
            &h,
            &ctx,
            &mut None,
        );

        // 此刻不得提前抛终态 FILLED；把桥上订单事件排干，最后一条也不能是终态。
        let mut saw_terminal = false;
        while let Ok(ev) = rx.try_recv() {
            if let BridgeEvent::Order(e) = ev {
                if matches!(e.status.as_str(), "FILLED" | "CANCELED" | "REJECTED") {
                    saw_terminal = true;
                }
            }
        }
        assert!(!saw_terminal, "成交回报未到齐不得提前抛终态");
        assert!(
            ctx.order_refs.cid_of(&r1).is_some(),
            "对齐期间不得回收 OrderRef 映射（否则成交帧反查不到长 cid）"
        );
        assert!(ctx.leg_table.contains(&cid), "对齐期间不得清腿表");

        // ② 成交回报到（2 手 @3972）。
        dispatch_event(
            CtpEvent::Trade(trade_snap(&r1, 3972.0, 2)),
            &h,
            &ctx,
            &mut None,
        );

        // 终态：恰好一条 FILLED，filled=2、成交价=3972（VWAP，非 0）。
        let mut finals = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            if let BridgeEvent::Order(e) = ev {
                finals.push((e.status.clone(), e.filled_quantity, e.filled_price));
            }
        }
        let filled: Vec<_> = finals.iter().filter(|(s, _, _)| s == "FILLED").collect();
        assert_eq!(filled.len(), 1, "恰好一条终态 FILLED；事件={finals:?}");
        assert_eq!(filled[0].1, Some(rust_decimal::Decimal::from(2)));
        assert_eq!(
            filled[0].2,
            Some(rust_decimal::Decimal::from(3972)),
            "终态必须带真实成交均价，不得为 0"
        );
        // 终态后才回收。
        assert!(ctx.order_refs.cid_of(&r1).is_none());
        assert!(!ctx.leg_table.contains(&cid));
    }

    #[test]
    fn zero_fill_terminal_projects_immediately_without_pending() {
        // T2：零成交终态（撤单）不需要成交价，立即投影、不入待对齐表。
        let (ctx, mut rx) = ctx_and_rx();
        let h = handle();
        let cid = long_cid("Z");
        let r1 = ctx.order_refs.allocate(&cid);

        dispatch_event(
            CtpEvent::Order(leg_snap(&r1, OrderStatus::Cancelled, 1, 0, "s1")),
            &h,
            &ctx,
            &mut None,
        );

        let evs = drain_all_orders(&mut rx);
        assert!(evs.iter().any(|(s, _, _)| s == "CANCELED"));
        assert!(!ctx.pending_terminal.contains(&cid));
        assert!(ctx.order_refs.cid_of(&r1).is_none(), "零成交终态照常回收");
    }

    #[test]
    fn reject_status_msg_classifier_distinguishes_errors_from_normal() {
        // 真机/典型拒单原因 → true（warn）。
        for m in [
            "49:已撤单报单被拒绝SHFE:价格超出涨停板",
            "707:已撤单报单被拒绝CZCE:出错: 指定商品不在本交易节交易",
            "平仓持仓不足",
            "CTP:资金不足",
            "废单",
        ] {
            assert!(is_venue_reject_status_msg(m), "应判拒单原因: {m}");
        }
        // 正常态/空 → false（debug，不刷屏）。
        for m in ["", "全部成交", "未成交", "已撤单", "报单已提交", "部成"] {
            assert!(!is_venue_reject_status_msg(m), "正常态不应判拒单: {m}");
        }
    }

    #[test]
    fn cancel_snapshot_with_status_msg_still_projects_canceled() {
        // 柜台在 StatusMsg 回填撤单原因（GB18030 已解码）：诊断日志不影响终态投影/回收。
        let (ctx, mut rx) = ctx_and_rx();
        let h = handle();
        let cid = long_cid("M");
        let r1 = ctx.order_refs.allocate(&cid);
        let mut snap = leg_snap(&r1, OrderStatus::Cancelled, 1, 0, "s1");
        snap.status_msg = "资金不足".to_string();

        dispatch_event(CtpEvent::Order(snap), &h, &ctx, &mut None);

        let evs = drain_all_orders(&mut rx);
        assert!(evs.iter().any(|(s, _, _)| s == "CANCELED"));
        assert!(ctx.order_refs.cid_of(&r1).is_none());
    }

    #[test]
    fn orphan_cid_terminal_is_not_deferred() {
        // T5：未在本会话 registry 的 cid（重连回放/跨会话孤儿）来全成快照，
        // 立即投影、不入待对齐表（历史单不被挂起）。
        let (ctx, mut rx) = ctx_and_rx();
        let h = handle();
        // 不 allocate：order_ref 反查未命中 → cid 即该 ref，registry 内无映射。
        dispatch_event(
            CtpEvent::Order(leg_snap("orphanref", OrderStatus::AllTraded, 2, 2, "s9")),
            &h,
            &ctx,
            &mut None,
        );
        let evs = drain_all_orders(&mut rx);
        assert!(
            evs.iter().any(|(s, _, _)| s == "FILLED"),
            "孤儿全成立即投影 {evs:?}"
        );
        assert!(!ctx.pending_terminal.contains("orphanref"));
    }

    #[test]
    fn terminal_after_trades_already_accumulated_emits_vwap() {
        // T3：成交回报先于全成快照到齐（累计表已有量/额），快照到即投影带 VWAP 终态。
        let (ctx, mut rx) = ctx_and_rx();
        let h = handle();
        let cid = long_cid("A");
        let r1 = ctx.order_refs.allocate(&cid);
        h.filled_by_cid.lock().unwrap().insert(cid.clone(), 2);
        h.filled_amount_by_cid
            .lock()
            .unwrap()
            .insert(cid.clone(), 2.0 * 3972.0);

        dispatch_event(
            CtpEvent::Order(leg_snap(&r1, OrderStatus::AllTraded, 2, 2, "s1")),
            &h,
            &ctx,
            &mut None,
        );

        let evs = drain_all_orders(&mut rx);
        let filled: Vec<_> = evs.iter().filter(|(s, _, _)| s == "FILLED").collect();
        assert_eq!(filled.len(), 1);
        assert_eq!(filled[0].1, Some(rust_decimal::Decimal::from(2)));
        assert_eq!(filled[0].2, Some(rust_decimal::Decimal::from(3972)));
        assert!(!ctx.pending_terminal.contains(&cid));
    }

    #[test]
    fn partial_trades_before_terminal_converge_to_vwap_filled() {
        // T4：终态快照先到（暂存）→ 分批成交：先 PARTIAL(1@3970)，补齐后唯一
        // FILLED(2, VWAP=3972)。
        let (ctx, mut rx) = ctx_and_rx();
        let h = handle();
        let cid = long_cid("P");
        let r1 = ctx.order_refs.allocate(&cid);

        dispatch_event(
            CtpEvent::Order(leg_snap(&r1, OrderStatus::AllTraded, 2, 2, "s1")),
            &h,
            &ctx,
            &mut None,
        );
        assert!(ctx.pending_terminal.contains(&cid));
        assert!(drain_all_orders(&mut rx).is_empty(), "对齐等待期不发帧");

        // 第 1 笔成交 1@3970：未补齐 → 带价 PARTIAL。
        dispatch_event(
            CtpEvent::Trade(trade_snap(&r1, 3970.0, 1)),
            &h,
            &ctx,
            &mut None,
        );
        let p1 = drain_all_orders(&mut rx);
        assert_eq!(p1.len(), 1);
        assert_eq!(p1[0].0, "PARTIAL");
        assert_eq!(p1[0].1, Some(rust_decimal::Decimal::from(1)));
        assert_eq!(p1[0].2, Some(rust_decimal::Decimal::from(3970)));
        assert!(ctx.pending_terminal.contains(&cid), "未补齐仍待对齐");

        // 第 2 笔成交 1@3974：累计 2 补齐 → 唯一 FILLED，VWAP=(3970+3974)/2=3972。
        dispatch_event(
            CtpEvent::Trade(trade_snap(&r1, 3974.0, 1)),
            &h,
            &ctx,
            &mut None,
        );
        let evs = drain_all_orders(&mut rx);
        let filled: Vec<_> = evs.iter().filter(|(s, _, _)| s == "FILLED").collect();
        assert_eq!(filled.len(), 1, "恰好一条 FILLED；{evs:?}");
        assert_eq!(filled[0].1, Some(rust_decimal::Decimal::from(2)));
        assert_eq!(filled[0].2, Some(rust_decimal::Decimal::from(3972)));
        assert!(!ctx.pending_terminal.contains(&cid));
        assert!(ctx.order_refs.cid_of(&r1).is_none());
    }

    #[test]
    fn timeout_flush_projects_terminal_without_price_and_reaps() {
        // T6：全成快照先到、成交帧异常丢失 → 暂存；超时 take_due 取出后由
        // project_terminal 兜底投影（无价 FILLED），不悬挂、表项回收。
        let (ctx, mut rx) = ctx_and_rx();
        let h = handle();
        let cid = long_cid("T");
        let r1 = ctx.order_refs.allocate(&cid);

        dispatch_event(
            CtpEvent::Order(leg_snap(&r1, OrderStatus::AllTraded, 2, 2, "s1")),
            &h,
            &ctx,
            &mut None,
        );
        assert!(ctx.pending_terminal.contains(&cid));
        assert!(drain_all_orders(&mut rx).is_empty());

        // 宽限到期（grace=0 立即到期）：取出 cid，再走兜底投影。
        let due = ctx.pending_terminal.take_due(now_ms(), 0);
        assert_eq!(due, vec![(cid.clone(), 2)]);
        assert!(!ctx.pending_terminal.contains(&cid), "take_due 已移除");
        project_terminal(&ctx, &h, &cid);

        let evs = drain_all_orders(&mut rx);
        let filled: Vec<_> = evs.iter().filter(|(s, _, _)| s == "FILLED").collect();
        assert_eq!(filled.len(), 1);
        assert_eq!(filled[0].1, Some(rust_decimal::Decimal::from(2)));
        assert_eq!(filled[0].2, None, "丢帧无价：filled_price 留空而非 0 臆造");
        assert!(ctx.order_refs.cid_of(&r1).is_none());
        assert!(!ctx.leg_table.contains(&cid));
    }

    #[test]
    fn two_leg_terminal_waits_for_trades_then_emits_priced_filled() {
        // T7：一单两腿全成快照先到（合计 filled=2，累计 0）→ 暂存；两腿成交回报
        // 累计到齐后投影一条 FILLED，filled=两腿和、price=两腿 VWAP。
        let (ctx, mut rx) = ctx_and_rx();
        let h = handle();
        let cid = long_cid("L");
        let r1 = ctx.order_refs.allocate(&cid);
        let r2 = ctx.order_refs.allocate(&cid);

        dispatch_event(
            CtpEvent::Order(leg_snap(&r1, OrderStatus::AllTraded, 1, 1, "s1")),
            &h,
            &ctx,
            &mut None,
        );
        dispatch_event(
            CtpEvent::Order(leg_snap(&r2, OrderStatus::AllTraded, 1, 1, "s2")),
            &h,
            &ctx,
            &mut None,
        );
        let _ = drain_all_orders(&mut rx); // 排干在途 PARTIAL
        assert!(
            ctx.pending_terminal.contains(&cid),
            "两腿齐终态、成交未到→暂存"
        );

        dispatch_event(
            CtpEvent::Trade(trade_snap(&r1, 100.0, 1)),
            &h,
            &ctx,
            &mut None,
        );
        assert!(drain_all_orders(&mut rx)
            .iter()
            .any(|(s, _, _)| s == "PARTIAL"));

        dispatch_event(
            CtpEvent::Trade(trade_snap(&r2, 102.0, 1)),
            &h,
            &ctx,
            &mut None,
        );
        let evs = drain_all_orders(&mut rx);
        let filled: Vec<_> = evs.iter().filter(|(s, _, _)| s == "FILLED").collect();
        assert_eq!(filled.len(), 1, "两腿成交到齐后一条 FILLED；{evs:?}");
        assert_eq!(
            filled[0].1,
            Some(rust_decimal::Decimal::from(2)),
            "filled=两腿和"
        );
        assert_eq!(
            filled[0].2,
            Some(rust_decimal::Decimal::from(101)),
            "VWAP=(100+102)/2"
        );
        assert!(ctx.order_refs.cid_of(&r1).is_none());
        assert!(ctx.order_refs.cid_of(&r2).is_none());
        assert!(!ctx.leg_table.contains(&cid));
    }

    #[test]
    fn first_leg_terminal_before_second_report_does_not_finalize() {
        // Arrange：两腿 cid，仅首腿回报且已全成（次腿首报未到）。
        let (ctx, mut rx) = ctx_and_rx();
        let h = handle();
        let cid = long_cid("E");
        let r1 = ctx.order_refs.allocate(&cid);
        let _r2 = ctx.order_refs.allocate(&cid);

        dispatch_event(
            CtpEvent::Order(leg_snap(&r1, OrderStatus::AllTraded, 2, 2, "s1")),
            &h,
            &ctx,
            &mut None,
        );
        let (_, st, f) = drain_next(&mut rx);
        // 首腿终态但次腿未报：只能报 Partial（有成交），绝不能提前 FILLED/回收。
        assert_eq!(st, "PARTIAL");
        assert_eq!(f, Some(rust_decimal::Decimal::from(2)));
        assert!(ctx.leg_table.contains(&cid));
        assert!(ctx.order_refs.cid_of(&r1).is_some(), "映射不得提前回收");
    }

    #[test]
    fn single_leg_terminal_releases_cid_tables_non_terminal_keeps() {
        // 单腿单：终态即回收；非终态保留。
        let (ctx, _rx) = ctx_and_rx();
        let h = handle();
        let cid = long_cid("S");
        let r1 = ctx.order_refs.allocate(&cid);
        h.filled_by_cid.lock().unwrap().insert(cid.clone(), 1);

        dispatch_event(
            CtpEvent::Order(leg_snap(&r1, OrderStatus::NoTradeQueueing, 1, 0, "s1")),
            &h,
            &ctx,
            &mut None,
        );
        assert!(ctx.order_refs.cid_of(&r1).is_some(), "在途不回收");
        assert_eq!(h.filled_of_cid(&cid), 1, "在途不清成交累计");

        dispatch_event(
            CtpEvent::Order(leg_snap(&r1, OrderStatus::Cancelled, 1, 0, "s1")),
            &h,
            &ctx,
            &mut None,
        );
        assert!(ctx.order_refs.cid_of(&r1).is_none(), "终态回收映射");
        assert!(!ctx.leg_table.contains(&cid));
        // 撤单无成交累计项时 remove 为无害 no-op（不 panic）。
    }

    #[test]
    fn two_leg_cancel_targets_address_both_refs_in_session() {
        // 两个 OrderRef 各得一个本会话三元组目标，Front/Session 取当前会话。
        let targets = two_leg_cancel_targets(&["r1".into(), "r2".into()], 7, 9001);
        use crate::ctp::mailbox::CancelTarget;
        assert_eq!(targets.len(), 2);
        for (orig, target) in &targets {
            match target {
                CancelTarget::ByOrderRef {
                    front_id,
                    session_id,
                    order_ref,
                } => {
                    assert_eq!((*front_id, *session_id), (7, 9001));
                    assert_eq!(order_ref, orig);
                }
                _ => panic!("两腿都在本会话，应走三元组"),
            }
        }
    }

    #[test]
    fn two_leg_trades_accumulate_by_cid() {
        // 同一 cid 的两腿各有不同 OrderRef/OrderSysID：成交按 cid 求和。
        let (ctx, mut rx) = ctx_and_rx();
        let h = handle();
        let cid = long_cid("F");
        let r1 = ctx.order_refs.allocate(&cid);
        let r2 = ctx.order_refs.allocate(&cid);

        let trade = |order_ref: &str, sys: &str, n: i32| TradeSnapshot {
            order_ref: order_ref.into(),
            trade_id: sys.into(),
            instrument: "rb2510".into(),
            exchange_id: "SHFE".into(),
            order_sys_id: sys.into(),
            direction: Direction::Sell,
            offset: Some(crate::ctp::core::types::Offset::CloseToday),
            price: 3300.0,
            volume: n,
            trade_date: "20261016".into(),
            trade_time: "10:00:00".into(),
        };
        dispatch_event(CtpEvent::Trade(trade(&r1, "s1", 1)), &h, &ctx, &mut None);
        dispatch_event(CtpEvent::Trade(trade(&r2, "s2", 2)), &h, &ctx, &mut None);

        // 两帧事件都归到同一 cid，累计量 1→3（不是各自从 0 起算）。
        let e1 = drain_next(&mut rx);
        let e2 = drain_next(&mut rx);
        assert_eq!(
            (e1.0.as_str(), e1.2),
            (cid.as_str(), Some(rust_decimal::Decimal::from(1)))
        );
        assert_eq!(
            (e2.0.as_str(), e2.2),
            (cid.as_str(), Some(rust_decimal::Decimal::from(3)))
        );
        assert_eq!(h.filled_of_cid(&cid), 3);
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
    use crate::ctp::core::event::InstrumentInfo;
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

    /// 测试用共享合约目录：可选预热一个 SHFE 合约（含交易所/乘数/tick）。
    fn instrument_dir_warmed(warm: bool) -> crate::ctp::instrument_dir::InstrumentDir {
        let dir = crate::ctp::instrument_dir::InstrumentDir::new();
        if warm {
            dir.remember(InstrumentInfo {
                instrument: "rb2510".into(),
                exchange_id: "SHFE".into(),
                product_id: "rb".into(),
                is_trading: true,
                volume_multiple: 10,
                price_tick: 1.0,
                long_margin_ratio: 0.0,
                short_margin_ratio: 0.0,
            });
        }
        dir
    }

    #[test]
    fn cancel_routes_by_triple_when_registry_hits_this_session() {
        // Arrange：长 cid 在本会话分配过 OrderRef；即使带了 eid 也优先三元组。
        let registry = OrderRefRegistry::new();
        let cid = long_cid("A");
        let order_ref = registry.allocate(&cid);
        let dir = instrument_dir_warmed(false);
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
        let dir = instrument_dir_warmed(true);
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
        let dir = instrument_dir_warmed(false);
        let order = OrderRef::new(long_cid("R")).with_exchange("20260101-7");

        // Act / Assert：显式报错，绝不猜测 ExchangeID。
        assert!(resolve_cancel_target(&registry, &dir, "rb2510", &order, 1, 1).is_err());
    }

    #[test]
    fn cancel_errors_when_no_exchange_order_id_after_restart() {
        // Arrange：registry 未命中（重启）且手里没有任何交易所单号（受理态孤儿）。
        let registry = OrderRefRegistry::new();
        let dir = instrument_dir_warmed(true);
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
        let dir = instrument_dir_warmed(false);
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

    // ---- SHFE 平仓平今/平昨路由纯内核 ----

    use crate::ctp::core::types::Offset;

    #[test]
    fn close_route_non_shfe_is_single_generic_close() {
        // 大商所/郑商所等：柜台自动冲抵今昨，笼统 Close 行为零变化。
        assert_eq!(
            plan_close(3, Some("DCE"), Some((2, 3))).unwrap(),
            ClosePlan::GenericClose
        );
        // 交易所目录尚未预热到该合约（None）：同样按笼统 Close（≠SHFE 分支），
        // SHFE 单会被柜台拒，不会发错显式标志。
        assert_eq!(plan_close(3, None, None).unwrap(), ClosePlan::GenericClose);
    }

    #[test]
    fn close_route_shfe_yesterday_sufficient_is_single_close_yesterday() {
        // 今 5 昨 3，平 3 ≤ 昨仓 → 单笔平昨，完全不碰今仓（省平今手续费）。
        assert_eq!(
            plan_close(3, Some("SHFE"), Some((5, 3))).unwrap(),
            ClosePlan::ShfeSingle(Offset::CloseYesterday, 3)
        );
        // 恰好平完全部昨仓仍是单腿平昨。
        assert_eq!(
            plan_close(3, Some("SHFE"), Some((5, 3))).unwrap(),
            ClosePlan::ShfeSingle(Offset::CloseYesterday, 3)
        );
    }

    #[test]
    fn close_route_shfe_no_yesterday_is_single_close_today() {
        // 昨 0 今 2，平 2 → 退化为单笔平今。
        assert_eq!(
            plan_close(2, Some("SHFE"), Some((2, 0))).unwrap(),
            ClosePlan::ShfeSingle(Offset::CloseToday, 2)
        );
    }

    #[test]
    fn close_route_shfe_cross_split_is_two_legs_yesterday_first() {
        // 今 2 昨 3，平 4 → 一单两腿：先平昨 3、再平今 1。
        assert_eq!(
            plan_close(4, Some("SHFE"), Some((2, 3))).unwrap(),
            ClosePlan::ShfeTwo {
                yesterday_n: 3,
                today_n: 1
            }
        );
        // 平量 ≤ 昨仓时绝不出两腿（今 2 昨 3 平 3 只平昨）。
        assert_eq!(
            plan_close(3, Some("SHFE"), Some((2, 3))).unwrap(),
            ClosePlan::ShfeSingle(Offset::CloseYesterday, 3)
        );
    }

    #[test]
    fn close_route_shfe_over_volume_is_rejected() {
        // 平仓量超过今+昨总仓：拒单（不下半截单）。
        assert_eq!(
            plan_close(6, Some("SHFE"), Some((2, 3))),
            Err(ClosePlanError::InsufficientPosition)
        );
    }

    #[test]
    fn close_route_shfe_split_not_ready_is_rejected() {
        // SHFE 但路由表无该方向今昨（刚重连/首轮对账未完成）：明确错误，绝不猜测发 Close。
        assert_eq!(
            plan_close(1, Some("SHFE"), None),
            Err(ClosePlanError::SplitNotReady)
        );
    }

    // ---- 下单请求平仓路由（routed_insert 纯函数）----

    use crate::ctp::core::order::OrderRequest;
    use std::borrow::Cow;

    fn close_req(direction: Direction, volume: i32) -> OrderRequest {
        OrderRequest::limit("rb2510", direction, Offset::Close, 3300.0, volume)
    }

    /// 取单笔路由结果（panic 于两腿，便于单腿用例断言）。
    fn single_routed<'a>(out: &'a RoutedInsert) -> &'a crate::ctp::core::order::OrderRequest {
        match out {
            RoutedInsert::Single(Cow::Borrowed(r)) => r,
            RoutedInsert::Single(Cow::Owned(r)) => r,
            RoutedInsert::Two { .. } => panic!("期望单笔，实际两腿"),
        }
    }

    #[test]
    fn routed_insert_non_shfe_close_passes_through_unchanged() {
        let req = close_req(Direction::Sell, 3);
        let out = routed_insert(&req, Some("DCE"), Some((2, 3))).unwrap();
        let r = single_routed(&out);
        assert_eq!(r.offset, Offset::Close, "非上期所保持笼统 Close");
        assert_eq!(r.volume, 3);
        assert!(
            matches!(out, RoutedInsert::Single(Cow::Borrowed(_))),
            "非改写路径零克隆借用原请求"
        );
    }

    #[test]
    fn routed_insert_shfe_close_within_yesterday_rewrites_to_close_yesterday() {
        // 卖平多头、平量 ≤ 昨仓：Close → CloseYesterday（不碰今仓），量/价/方向不变。
        let req = close_req(Direction::Sell, 3);
        let out = routed_insert(&req, Some("SHFE"), Some((5, 3))).unwrap();
        let r = single_routed(&out);
        assert_eq!(r.offset, Offset::CloseYesterday);
        assert_eq!(r.volume, 3);
        assert_eq!(r.direction, Direction::Sell);
        assert_eq!(r.price, 3300.0);
        // 原请求未被修改（改写走克隆）。
        assert_eq!(req.offset, Offset::Close);
    }

    #[test]
    fn routed_insert_shfe_close_today_when_no_yesterday() {
        // 昨仓为 0：退化为单笔平今。
        let req = close_req(Direction::Buy, 2);
        let out = routed_insert(&req, Some("SHFE"), Some((4, 0))).unwrap();
        let r = single_routed(&out);
        assert_eq!(r.offset, Offset::CloseToday);
        assert_eq!(r.volume, 2);
    }

    #[test]
    fn routed_insert_open_is_untouched_even_on_shfe() {
        // 开仓不读交易所/路由表，原样借用提交。
        let req = OrderRequest::limit("rb2510", Direction::Buy, Offset::Open, 3300.0, 2);
        let out = routed_insert(&req, Some("SHFE"), None).unwrap();
        let r = single_routed(&out);
        assert_eq!(r.offset, Offset::Open);
        assert_eq!(r.volume, 2);
    }

    #[test]
    fn routed_insert_shfe_cross_split_produces_two_legs_yesterday_first() {
        // 今 2 昨 3 平 4 → 两腿：先平昨 3、再平今 1，同价同方向，昨腿在前。
        let req = close_req(Direction::Sell, 4);
        let out = routed_insert(&req, Some("SHFE"), Some((2, 3))).unwrap();
        match out {
            RoutedInsert::Two { yesterday, today } => {
                assert_eq!(yesterday.offset, Offset::CloseYesterday);
                assert_eq!(yesterday.volume, 3);
                assert_eq!(today.offset, Offset::CloseToday);
                assert_eq!(today.volume, 1);
                assert_eq!(yesterday.direction, Direction::Sell);
                assert_eq!(yesterday.price, 3300.0);
                assert_eq!(today.price, 3300.0);
            }
            _ => panic!("期望跨今昨两腿"),
        }
    }

    #[test]
    fn routed_insert_shfe_propagates_fail_closed_errors() {
        let req = close_req(Direction::Sell, 3);
        // 未就绪 / 超量都原样冒泡，调用方据此回执明确错误。
        assert_eq!(
            routed_insert(&req, Some("SHFE"), None).unwrap_err(),
            ClosePlanError::SplitNotReady
        );
        assert_eq!(
            routed_insert(&req, Some("SHFE"), Some((1, 1))).unwrap_err(),
            ClosePlanError::InsufficientPosition
        );
    }

    // ---- ActiveQuery 单在途闸门：前台/后台分流、QueryEnd 收尾、断线回执 ----

    use crate::ctp::core::event::{CtpEvent, PositionSnapshot, TradeSnapshot};
    use tokio::sync::mpsc;
    use tokio::sync::oneshot::{error::TryRecvError, Receiver as OneRx};

    /// 构造不触 FFI 的事件上下文 + 桥接收端。
    fn ctx_and_rx() -> (Arc<EventContext>, mpsc::Receiver<BridgeEvent>) {
        let (tx, rx) = mpsc::channel(16);
        let ctx = Arc::new(EventContext {
            account: "acct".into(),
            order_refs: Arc::new(OrderRefRegistry::new()),
            instrument_dir: Arc::new(crate::ctp::instrument_dir::InstrumentDir::new()),
            position_dir: Arc::new(crate::ctp::position_dir::PositionDir::new()),
            leg_table: Arc::new(crate::ctp::leg_table::LegTable::new()),
            pending_terminal: Arc::new(crate::ctp::pending_terminal::PendingTerminalTable::new()),
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

    /// 给共享合约目录预热 rb2510（SHFE/乘数10/最小变动价位1，模拟合约表预热完成）。
    fn warm_rb_multiple(ctx: &EventContext) {
        ctx.instrument_dir.remember(InstrumentInfo {
            instrument: "rb2510".into(),
            exchange_id: "SHFE".into(),
            product_id: "rb".into(),
            is_trading: true,
            volume_multiple: 10,
            price_tick: 1.0,
            long_margin_ratio: 0.0,
            short_margin_ratio: 0.0,
        });
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
            rows: Vec::new(),
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
            rows: Vec::new(),
            reply: Some(tx),
        });
        dispatch_event(CtpEvent::Position(long_position()), &h, &ctx, &mut active);
        dispatch_event(CtpEvent::QueryEnd, &h, &ctx, &mut active);
        let positions = rx.try_recv().unwrap().unwrap();
        assert!(positions.is_empty(), "乘数缺失时该持仓行应被过滤");
    }

    #[test]
    fn contracts_query_warms_multiple_dir_and_releases_gate() {
        // 合约预热：Instrument 行经 Contracts 在途写入共享 InstrumentDir，QueryEnd 释放闸门。
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
                price_tick: 1.0,
                long_margin_ratio: 0.0,
                short_margin_ratio: 0.0,
            }),
            &h,
            &ctx,
            &mut active,
        );
        assert_eq!(ctx.instrument_dir.multiple("rb2510"), 10);
        dispatch_event(CtpEvent::QueryEnd, &h, &ctx, &mut active);
        assert!(active.is_none(), "合约预热 QueryEnd 释放闸门");
    }

    #[test]
    fn back_order_replay_forwards_query_rows_to_bridge() {
        // Arrange：后台重连委托重放在途。
        let (ctx, mut bridge_rx) = ctx_and_rx();
        let h = handle();
        let mut active = Some(ActiveQuery::BackOrderReplay);

        // Act：查询行像实时回报一样透传订单桥（恢复状态机）。
        dispatch_event(
            CtpEvent::OrderQuery(qrow("101", "20260101-2", "SHFE", "rb2510")),
            &h,
            &ctx,
            &mut active,
        );

        // Assert：订单桥收到一帧；闸门仍在（等 QueryEnd）。交易所目录由合约预热
        // 负责（见 contracts_query_warms_*），委托回报不再喂目录。
        assert!(matches!(bridge_rx.try_recv(), Ok(BridgeEvent::Order(_))));
        assert!(active.is_some());
        dispatch_event(CtpEvent::QueryEnd, &h, &ctx, &mut active);
        assert!(active.is_none(), "重放 QueryEnd 释放闸门");
    }

    #[test]
    fn back_positions_queryend_forwards_snapshot_to_bridge() {
        // Arrange：后台持仓对账在途。
        let (ctx, mut bridge_rx) = ctx_and_rx();
        let h = handle();
        let mut active = Some(ActiveQuery::BackPositions { rows: Vec::new() });
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

    /// 构造 rb2510 多头持仓分片（今/昨可分两行）。
    fn rb_row(position: i32, today: i32, yd: i32, cost: f64, margin: f64) -> PositionSnapshot {
        PositionSnapshot {
            instrument: "rb2510".into(),
            direction: '2',
            position,
            yd_position: yd,
            today_position: today,
            position_cost: cost,
            use_margin: margin,
            position_profit: 0.0,
        }
    }

    #[test]
    fn back_positions_merges_today_yesterday_rows_and_feeds_position_dir() {
        // Arrange：柜台把 rb 多头今 2/昨 3 回两行；乘数已预热。
        let (ctx, mut bridge_rx) = ctx_and_rx();
        let h = handle();
        let mut active = Some(ActiveQuery::BackPositions { rows: Vec::new() });
        warm_rb_multiple(&ctx);
        dispatch_event(
            CtpEvent::Position(rb_row(2, 2, 0, 64_000.0, 20_000.0)),
            &h,
            &ctx,
            &mut active,
        );
        dispatch_event(
            CtpEvent::Position(rb_row(3, 0, 3, 99_000.0, 30_000.0)),
            &h,
            &ctx,
            &mut active,
        );

        // Act
        dispatch_event(CtpEvent::QueryEnd, &h, &ctx, &mut active);

        // Assert：上游只收到一条整合仓位（qty=5、entry=3260），
        // 会话路由表记录今 2/昨 3（不落盘，仅供 SHFE 路由）。
        match bridge_rx.try_recv() {
            Ok(BridgeEvent::Positions(ps)) => {
                assert_eq!(ps.len(), 1, "今/昨两行必须聚合成一条");
                assert_eq!(ps[0].signed_quantity, rust_decimal::Decimal::from(5));
                assert_eq!(
                    ps[0].entry_price,
                    rust_decimal::Decimal::try_from(3260.0).unwrap()
                );
                assert_eq!(
                    ps[0].margin,
                    Some(rust_decimal::Decimal::try_from(50_000.0).unwrap())
                );
            }
            other => panic!("expected merged positions, got {other:?}"),
        }
        use crate::ctp::position_dir::PositionSide;
        assert_eq!(
            ctx.position_dir.get("rb2510", PositionSide::Long),
            Some((2, 3))
        );
        assert!(active.is_none());
    }

    #[test]
    fn front_positions_merges_rows_and_replies_one_position() {
        // Arrange：前台查仓同样聚合并喂路由表。
        let (ctx, _rx) = ctx_and_rx();
        let h = handle();
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        let mut active = Some(ActiveQuery::FrontPositions {
            rows: Vec::new(),
            reply: Some(tx),
        });
        warm_rb_multiple(&ctx);
        dispatch_event(
            CtpEvent::Position(rb_row(2, 2, 0, 64_000.0, 20_000.0)),
            &h,
            &ctx,
            &mut active,
        );
        dispatch_event(
            CtpEvent::Position(rb_row(3, 0, 3, 99_000.0, 30_000.0)),
            &h,
            &ctx,
            &mut active,
        );

        // Act
        dispatch_event(CtpEvent::QueryEnd, &h, &ctx, &mut active);

        // Assert
        let positions = rx.try_recv().unwrap().unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].signed_quantity, rust_decimal::Decimal::from(5));
        use crate::ctp::position_dir::PositionSide;
        assert_eq!(
            ctx.position_dir.get("rb2510", PositionSide::Long),
            Some((2, 3))
        );
    }

    #[test]
    fn back_positions_shell_row_clears_position_dir_entry() {
        // Arrange：先有今 2/昨 3 的路由表项，随后一轮对账柜台只回 position=0 空壳。
        let (ctx, _rx) = ctx_and_rx();
        let h = handle();
        use crate::ctp::position_dir::PositionSide;
        ctx.position_dir.set("rb2510", PositionSide::Long, 2, 3);
        warm_rb_multiple(&ctx);

        let mut active = Some(ActiveQuery::BackPositions { rows: Vec::new() });
        dispatch_event(
            CtpEvent::Position(rb_row(0, 0, 0, 0.0, 0.0)),
            &h,
            &ctx,
            &mut active,
        );
        dispatch_event(CtpEvent::QueryEnd, &h, &ctx, &mut active);

        // Assert：空壳意味着该方向已无仓，路由表项移除；上游收到空批次。
        assert_eq!(ctx.position_dir.get("rb2510", PositionSide::Long), None);
    }

    #[test]
    fn trade_incrementally_updates_position_dir_by_offset() {
        // Arrange：rb 多头今 2 昨 3。
        let (ctx, _rx) = ctx_and_rx();
        let h = handle();
        use crate::ctp::position_dir::PositionSide;
        ctx.position_dir.set("rb2510", PositionSide::Long, 2, 3);

        // Act / Assert ①：卖平今 1（direction=Sell、CloseToday → 平多头今仓）。
        dispatch_event(
            CtpEvent::Trade(TradeSnapshot {
                order_ref: "1".into(),
                trade_id: "T1".into(),
                instrument: "rb2510".into(),
                exchange_id: "SHFE".into(),
                order_sys_id: "9001".into(),
                direction: Direction::Sell,
                offset: Some(crate::ctp::core::types::Offset::CloseToday),
                price: 3300.0,
                volume: 1,
                trade_date: "20261016".into(),
                trade_time: "10:00:00".into(),
            }),
            &h,
            &ctx,
            &mut None,
        );
        assert_eq!(
            ctx.position_dir.get("rb2510", PositionSide::Long),
            Some((1, 3))
        );

        // ② 买开 2（direction=Buy、Open → 多头今仓 +2）。
        dispatch_event(
            CtpEvent::Trade(TradeSnapshot {
                trade_id: "T2".into(),
                direction: Direction::Buy,
                offset: Some(crate::ctp::core::types::Offset::Open),
                ..trade_template("2", "9002")
            }),
            &h,
            &ctx,
            &mut None,
        );
        assert_eq!(
            ctx.position_dir.get("rb2510", PositionSide::Long),
            Some((3, 3))
        );

        // ③ 笼统 Close（非上期所）不分今昨，表不动。
        dispatch_event(
            CtpEvent::Trade(TradeSnapshot {
                trade_id: "T3".into(),
                direction: Direction::Sell,
                offset: Some(crate::ctp::core::types::Offset::Close),
                ..trade_template("3", "9003")
            }),
            &h,
            &ctx,
            &mut None,
        );
        assert_eq!(
            ctx.position_dir.get("rb2510", PositionSide::Long),
            Some((3, 3))
        );
    }

    /// TradeSnapshot 夹具（rb 多头方向相关字段由测试覆盖）。
    fn trade_template(order_ref: &str, sys_id: &str) -> TradeSnapshot {
        TradeSnapshot {
            order_ref: order_ref.into(),
            trade_id: "T".into(),
            instrument: "rb2510".into(),
            exchange_id: "SHFE".into(),
            order_sys_id: sys_id.into(),
            direction: Direction::Buy,
            offset: Some(crate::ctp::core::types::Offset::Open),
            price: 3205.0,
            volume: 2,
            trade_date: "20261016".into(),
            trade_time: "10:00:01".into(),
        }
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

        // Assert：实时回报未被查询缓冲吞掉，订单桥收到。
        assert!(matches!(bridge_rx.try_recv(), Ok(BridgeEvent::Order(_))));
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
