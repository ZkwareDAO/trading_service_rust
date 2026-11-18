//! CTP 公共行情（可降级）：MdHandle → QuoteTable + QuoteObserver。
//!
//! 柜台未开行情前置时（登录码 64 等）降级：不订阅、不写行情表，
//! 仓位重估由宿主 fallback_valuation 语义处理，不阻断交易。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rust_decimal::Decimal;
use trading_core::quote::{QuoteTable, QuoteUpdate};
use trading_models::Instrument;
use trading_models::{MarketKind, TopOfBook, Venue};

use std::sync::atomic::Ordering;

use crate::backoff::ReconnectBackoff;
use crate::ctp::core::account::CtpAccount;
use crate::ctp::core::quote::TickSnapshot;
use crate::ctp::gateway::md::{MdEvent, MdHandle};
use crate::ctp::venue::sleep_interruptibly;
use crate::ctp::CtpConfig;
use crate::port::QuoteObserver;

/// 行情事件等待超时（空闲轮询粒度）。
const MD_POLL_TIMEOUT: Duration = Duration::from_secs(1);

/// 空闲拍（队列已空）墙钟收口的额外余量：本地行情时间越过桶边界后再等这么久才收口。
/// 只用于收盘/静默的最后一根；活跃行情一律由事件时间水位定稿。
const KLINE_IDLE_GRACE_MS: i64 = 2_000;

/// K线聚合诊断上报间隔（迟到/乱序/疑似丢快照/行情滞后）。
const KLINE_DIAG_INTERVAL: Duration = Duration::from_secs(60);

/// 行情静默超过该时长（休市/长空档）时不再周期上报，避免刷屏。
const KLINE_DIAG_SILENT_MS: i64 = 60_000;

/// K线静默判定：业务 tick 仍在、但 K线超过该时长无 tick 时，允许墙钟兜底收口。
const KLINE_IDLE_SWEEP_AFTER: Duration = Duration::from_secs(1);

/// 未来时间戳上限：超过 `now + 该值` 的 K线 tick 视为柜台 ActionDay 脏数据。
/// 取 30 分钟：正常时钟偏差不会被误杀，又能挡住“次日/下周一”量级的跳变。
const KLINE_MAX_FUTURE_MS: i64 = 30 * 60_000;

/// 会话内订阅集对账周期（业务 ∪ K线）；tick 很密，避免每个事件都扫仓储。
const MD_RECONCILE_INTERVAL: Duration = Duration::from_secs(1);

/// 判定“仍在补积压”的行情滞后阈值：`本机墙钟 − tick 事件时间` 超过该值即视为
/// drain 落后于柜台（正常行情 lag < 100ms）。积压期延迟 sweep，避免跨品种水位
/// 把本应入桶的尾段 tick 判成“迟到”丢弃。
const KLINE_CATCHUP_LAG_MS: i64 = 2_000;

/// 积压期最多推迟 sweep 的时长：到点强制定稿一次，防内存与推送延迟无界
/// （推送延迟上界 ≈ 10s + 2s 合帧，绝不跨分钟桶）。
const KLINE_MAX_SWEEP_DEFER_MS: i64 = 10_000;

/// 未来时间戳脏 tick 的告警限频间隔（按合约，60s 一条 + 抑制计数）。
const FUTURE_TICK_WARN_INTERVAL_MS: i64 = 60_000;

/// 兴趣集为空时的保活重查间隔：线程不退出，睡一个分片周期后重读兴趣集。
/// 固定间隔、**不走** [`ReconnectBackoff`]——兴趣集是冷启动竞态下的临时空窗
/// （持仓对账尚未载入），不能被断线退避阶梯拖住；睡眠受停机标志分片打断。
const MD_IDLE_POLL: Duration = Duration::from_secs(5);

/// CTP 行情写入目标键（venue/kind 恒为 Ctp/Futures）。
fn instrument_key(symbol: &str) -> Instrument {
    Instrument::new(Venue::Ctp, MarketKind::Futures, symbol)
}

/// 柜台以 0 / `DBL_MAX` 表示该侧无报价：归一为可选合法价格。
fn book_price(p: f64) -> Option<Decimal> {
    if p > 1e-9 && p < f64::MAX {
        Decimal::try_from(p).ok()
    } else {
        None
    }
}

/// TickSnapshot → QuoteUpdate（CTP 最新价即 last；盘口写入买一卖一；
/// 柜台以 f64::MAX 表示无对手价，视为缺侧）。
fn tick_to_update(t: &TickSnapshot, now_ms: i64) -> QuoteUpdate {
    // 业务 QuoteTable 路径本期仍只用 best1（五档全量仅落 RawTick）。
    // 两侧分别判有效，保留单边盘口供市价单取对手价（涨停只有 bid、
    // 跌停只有 ask）。两侧都缺失时写**空 book** 而非 None——None 的语义是
    // “本消息不带盘口、保留旧值”，会在停牌/涨跌停时让过期盘口继续参与下单；
    // 空 book 则明确表达“当前无盘口”。
    QuoteUpdate {
        last: Decimal::try_from(t.last_price)
            .ok()
            .filter(|p| *p > Decimal::ZERO),
        mark: None,
        mid: None,
        book: Some(TopOfBook::new(
            book_price(t.bids[0].price),
            book_price(t.asks[0].price),
        )),
        ts_exchange_ms: now_ms,
        ts_recv_ms: now_ms,
    }
}

/// 行情线程参数（由 provider 的 spawn_market_services 构造）。
pub struct MdParams {
    pub account: CtpAccount,
    pub config: CtpConfig,
}

/// K线侧依赖（None = 无任何 K线消费者，drain 只走业务行情路径，行为与历史逐字节等价）。
pub struct MdKlineDeps {
    /// CTP 统一行情流（bar clients / 三表聚合 / 主力选举）。
    pub stream: Arc<crate::ctp::market_stream::CtpMarketStream>,
}

/// 启动行情线程（阻塞线程内跑常驻循环）。
///
/// `interests`：订阅兴趣供给（与其它所同型）。`None`（未提供兴趣端口）属部署态
/// 关闭，直接退出；`Some` 时线程与账户同生命周期**常驻**——兴趣集暂空（如冷启动
/// 持仓对账尚未载入）不退出，按 [`MD_IDLE_POLL`] 周期保活重查，由空转非空后自动
/// 建连订阅。未配 `md_front` 在本函数开头守卫退出。
/// 运行期断线、登录或订阅失败则按 [`ReconnectBackoff`] 退避重连
/// （10s→30s→1m→3m→5m 封顶；空集等待不走退避），与交易连接
/// `run_session_loop` 同型，不再需要重启服务即可在前置恢复/夜盘开盘时自动恢复 tick。
pub fn spawn_market_worker(
    params: MdParams,
    interests: Option<Arc<dyn crate::port::MarketInterest>>,
    quotes: Arc<QuoteTable>,
    observer: Option<Arc<dyn QuoteObserver>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    kline: Option<MdKlineDeps>,
) -> Option<std::thread::JoinHandle<()>> {
    // 防御性守卫：空行情前置不应到达 FFI 的 register_front。
    if params.config.md_front.trim().is_empty() {
        tracing::warn!("CTP 行情前置未配置，行情线程不启动");
        return None;
    }
    std::thread::Builder::new()
        .name("ctp-md".into())
        .spawn(move || run_md_loop(&params, interests, &quotes, observer, &stop, kline))
        // 返回句柄供优雅停机 join（保证 md 的 flush_all 先于 CSV 刷盘完成）。
        .map(Some)
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "CTP 行情线程启动失败");
            None
        })
}

/// 一次连接尝试的结果（驱动退避外循环的纯决策输入）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MdSessionOutcome {
    /// 连接成功建立并订阅生效（其后断线）——阶梯归零，下次断线从 10s 起。
    Connected,
    /// 连接/登录/订阅失败或前置断开——安排退避后重连。
    Reconnect,
    /// 停机标志置位——退出线程。
    Stop,
}

/// 一次**已成功建连**的会话结束后的去向：纯决策（便于单测，不触 FFI/时钟）。
/// 停机→退出；运行中被前置断开→按 Connected 处理（外循环 `reset()` 归零阶梯后，
/// 统一 `note_retry()` 排下一次 10s 重试，与交易 `run_session_loop` 同型）。
fn md_session_end(stopped: bool) -> MdSessionOutcome {
    if stopped {
        MdSessionOutcome::Stop
    } else {
        MdSessionOutcome::Connected
    }
}

/// 行情线程每个周期对「停机 / 兴趣集」的去向决策。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MdLoopAction {
    /// 停机标志置位——唯一的正常线程退出。
    Exit,
    /// 兴趣集非空——建连、登录、订阅并消费 tick。
    Connect,
    /// 兴趣集暂空（冷启动对账未载入等临时空窗）——保活等待，不退出也不退避。
    IdleWait,
}

/// 每周期去向的纯决策（不触 FFI/时钟，便于表驱动单测）：
/// stop 优先→[`MdLoopAction::Exit`]；有订阅标的→[`MdLoopAction::Connect`]；
/// 兴趣集空→[`MdLoopAction::IdleWait`]（常驻保活，由空转非空后下一周期自动建连）。
fn next_md_action(stopped: bool, has_symbols: bool) -> MdLoopAction {
    if stopped {
        MdLoopAction::Exit
    } else if has_symbols {
        MdLoopAction::Connect
    } else {
        MdLoopAction::IdleWait
    }
}

/// [`run_md_loop`] 每个周期要执行的副作用（纯编排内核产出，FFI/时钟由调用方执行）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MdStep {
    /// 停机——线程退出。
    Exit,
    /// 兴趣集暂空：睡 [`MD_IDLE_POLL`] 后重查（退避阶梯已被清零）。
    IdleWait,
    /// 建连前先睡退避档；醒来后用**新快照**再 [`MdLoopPlanner::poll`] 一次。
    WaitBackoff(Duration),
    /// 退避已清：取订阅标的并跑一个连接会话，结束后回传
    /// [`MdLoopPlanner::note_outcome`]。
    Connect,
}

/// 行情循环的纯编排内核：持有退避阶梯，每周期按「停机 / 兴趣集快照」产出一个
/// [`MdStep`]。不触 FFI、不睡眠，全部状态机行为可确定性单测；
/// `run_md_loop` 只负责解释执行 step（睡眠 / 建连会话）。
#[derive(Debug)]
struct MdLoopPlanner {
    backoff: ReconnectBackoff,
}

impl MdLoopPlanner {
    fn new() -> Self {
        Self {
            backoff: ReconnectBackoff::new(),
        }
    }

    /// 决策本周期动作。`stopped`/`has_symbols` 取自循环顶同一份兴趣集快照：
    /// * 停机 → [`MdStep::Exit`]；
    /// * 空集 → 清零退避（恢复后立即建连，不被断线长退避拖住）→ [`MdStep::IdleWait`]；
    /// * 非空 → 有待消费退避则 [`MdStep::WaitBackoff`]，否则 [`MdStep::Connect`]。
    fn poll(&mut self, stopped: bool, has_symbols: bool) -> MdStep {
        match next_md_action(stopped, has_symbols) {
            MdLoopAction::Exit => MdStep::Exit,
            MdLoopAction::IdleWait => {
                // 空窗期清零退避阶梯（空集不走退避）。
                self.backoff.reset();
                MdStep::IdleWait
            }
            MdLoopAction::Connect => match self.backoff.next_wait() {
                Some(wait) => MdStep::WaitBackoff(wait),
                None => MdStep::Connect,
            },
        }
    }

    /// 一个连接会话结束后回传结果以更新退避阶梯（仅 [`MdStep::Connect`] 后调用）：
    /// 曾成功建连（Connected）阶梯归零后排 10s；仅失败（Reconnect）升档；
    /// Stop 不动阶梯（调用方随即退出）。
    fn note_outcome(&mut self, outcome: MdSessionOutcome) {
        match outcome {
            MdSessionOutcome::Stop => {}
            MdSessionOutcome::Connected => {
                self.backoff.reset();
                self.backoff.note_retry();
            }
            MdSessionOutcome::Reconnect => self.backoff.note_retry(),
        }
    }
}

/// 兴趣键是否属于本线程订阅口径（CTP 期货）。
fn is_ctp_futures(key: &crate::port::InterestKey) -> bool {
    key.venue == Venue::Ctp && key.kind == MarketKind::Futures
}

/// 取当前 CTP 期货订阅标的：每周期重算，合约增删随下一周期生效。
fn ctp_subscription_symbols(interests: &dyn crate::port::MarketInterest) -> Vec<String> {
    interests
        .interests()
        .iter()
        .filter(|k| is_ctp_futures(k))
        .map(|k| k.symbol.clone())
        .collect()
}

/// 行情线程主循环（常驻 + 退避重连）：参照交易 `run_session_loop`。
///
/// 编排状态机在纯内核 [`MdLoopPlanner`]（可单测），本函数只解释每周期的 [`MdStep`]：
/// 睡可中断的觉（[`MD_IDLE_POLL`] 保活 / 退避档），或新建并 Drop 一个 `MdHandle`
/// （连接→登录→订阅→tick）并把 [`MdSessionOutcome`] 回传 planner。
/// 停机是唯一正常退出；所有睡眠均经 [`sleep_interruptibly`] 受停机标志分片打断。
fn run_md_loop(
    params: &MdParams,
    interests: Option<Arc<dyn crate::port::MarketInterest>>,
    quotes: &Arc<QuoteTable>,
    observer: Option<Arc<dyn QuoteObserver>>,
    stop: &std::sync::atomic::AtomicBool,
    kline: Option<MdKlineDeps>,
) {
    // interests == None（未提供兴趣端口）属部署态关闭：直接退出，不进常驻循环；
    // 与「有端口但兴趣集暂空」严格区分，后者只保活等待。
    let Some(interests) = interests else {
        tracing::info!("CTP 行情未提供兴趣端口，跳过订阅");
        return;
    };

    let mut planner = MdLoopPlanner::new();
    // 上周期是否处于空集保活态：仅在 空↔非空 翻转时打 info，避免每 5s 刷屏。
    let mut was_idle = false;
    loop {
        // 决策只需「是否非空」：业务兴趣或 K线期望任一非空即建连。
        let has_business = interests.interests().iter().any(is_ctp_futures);
        let has_kline = kline
            .as_ref()
            .map(|k| !k.stream.bar_symbols().is_empty())
            .unwrap_or(false);
        let has_symbols = has_business || has_kline;
        match planner.poll(stop.load(Ordering::SeqCst), has_symbols) {
            MdStep::Exit => break,
            MdStep::IdleWait => {
                if !was_idle {
                    tracing::info!(
                        idle_poll_secs = MD_IDLE_POLL.as_secs(),
                        "CTP 行情兴趣集为空，保活等待（定时重查，线程不退出）"
                    );
                    was_idle = true;
                }
                sleep_interruptibly(MD_IDLE_POLL, stop);
            }
            MdStep::WaitBackoff(wait) => {
                sleep_interruptibly(wait, stop);
                // 醒来（或被停机打断）后用新快照重判：停机→Exit；等待期间兴趣集
                // 恰好转空→IdleWait 接住（reset + 5s 重查），均不建连。
            }
            MdStep::Connect => {
                // 建连前才取真实订阅标的（planner 只知非空、不知具体标的）；
                // 退避醒来后集合恰好转空的极端竞态在此兜底，交回下一周期 IdleWait。
                // 线订阅集 = 业务兴趣 ∪ K线期望（品种已在 bar_symbols 内展开为合约）。
                let mut symbols = ctp_subscription_symbols(interests.as_ref());
                if let Some(k) = kline.as_ref() {
                    for sym in k.stream.bar_symbols() {
                        if !symbols.contains(&sym) {
                            symbols.push(sym);
                        }
                    }
                }
                if symbols.is_empty() {
                    continue;
                }
                // 确认确将建连才打翻转日志（count 取最终标的，避免等待后变空的误报）。
                if was_idle {
                    tracing::info!(count = symbols.len(), "CTP 行情兴趣集转非空，建连订阅");
                    was_idle = false;
                }

                let outcome = run_md_session(
                    params,
                    &symbols,
                    quotes,
                    &observer,
                    stop,
                    kline.as_ref(),
                    interests.as_ref(),
                );
                if outcome == MdSessionOutcome::Stop {
                    break;
                }
                planner.note_outcome(outcome);
            }
        }
    }
    // 优雅停机收口：run_md_loop 仅在置位 stop 时退出（断线/失败仍在 loop 内重连，
    // 不会走到这里），故在此定稿所有未闭 1m 桶恰好一次——经无条件持久化 sink 落盘、
    // 并向南向做最后一推。重连不 flush，避免聚合器跨会话单例产生重复 bar。
    if stop.load(Ordering::SeqCst) {
        if let Some(k) = kline.as_ref() {
            let n = k.stream.flush_all().len();
            if n > 0 {
                tracing::info!(bars = n, "CTP 停机收口：定稿未落盘的未闭 1m K线");
            }
        }
    }
    tracing::info!("CTP 行情线程退出");
}

/// 单个连接会话：连接 → 登录 → 订阅 → 消费 tick，直到断线/失败/停机。
/// 返回前 `MdHandle` drop（release 刷盘），下一周期重新建连。
#[allow(clippy::too_many_arguments)]
fn run_md_session(
    params: &MdParams,
    symbols: &[String],
    quotes: &Arc<QuoteTable>,
    observer: &Option<Arc<dyn QuoteObserver>>,
    stop: &std::sync::atomic::AtomicBool,
    kline: Option<&MdKlineDeps>,
    interests: &dyn crate::port::MarketInterest,
) -> MdSessionOutcome {
    if stop.load(Ordering::SeqCst) {
        return MdSessionOutcome::Stop;
    }
    let mut md = match MdHandle::connect(
        &params.account,
        &params.config.md_front,
        &params.config.md_so,
        &params.config.flow_path,
        params.account.is_production_mode,
        params.config.md_anon,
        params.config.md_pass_is_authcode,
    ) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(error = %e, "CTP 行情前置连接失败，将退避重连");
            return MdSessionOutcome::Reconnect;
        }
    };
    // 登录受理重试（官方口径：FrontConnected 只是 TCP 建连，FTD 会话协商未完成时
    // ReqUserLogin 返回 -1，应等待重发）；可被停机标志打断（1s 分片，最长 60s）。
    let login_code = retry_until_accepted(|| md.login(), stop, 60);
    if stop.load(Ordering::SeqCst) {
        return MdSessionOutcome::Stop;
    }
    if login_code != 0 {
        tracing::warn!(
            code = login_code,
            "CTP 行情登录受理失败（重试 60s），将退避重连"
        );
        return MdSessionOutcome::Reconnect;
    }
    let refs: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();
    let sub_code = retry_until_accepted(|| md.subscribe(&refs), stop, 60);
    if stop.load(Ordering::SeqCst) {
        return MdSessionOutcome::Stop;
    }
    if sub_code != 0 {
        tracing::warn!(code = sub_code, "CTP 行情订阅受理失败，将退避重连");
        return MdSessionOutcome::Reconnect;
    }
    tracing::info!(count = symbols.len(), "CTP 行情订阅生效");

    // 本会话订阅状态：业务/K线 各自期望集（tick 路由用）+ 实际订阅并集（FFI 对账用）。
    // 会话内按 ~1s 对账，新开/平仓、规则与 K 线订阅变化无需断线重连。
    let mut subs = SubscriptionState {
        business: ctp_subscription_symbols(interests).into_iter().collect(),
        kline: kline
            .map(|k| k.stream.bar_symbols().into_iter().collect())
            .unwrap_or_default(),
        subscribed: symbols.iter().cloned().collect(),
    };
    let mut last_reconcile = std::time::Instant::now();
    let mut reconcile_due = true;
    // 已观测到的最小“本机墙钟 − 行情事件时间”（毫秒）：只用于空闲拍墙钟校正，
    // 时钟/行情持续滞后时不会把桶边界误判为已过；`i64::MAX` = 尚未观测到 tick。
    let mut min_lag_ms = i64::MAX;
    let mut last_kline_diag = std::time::Instant::now();
    // K线最近一次有 tick / 最近一次空闲拍墙钟 sweep：业务 tick 仍活跃但 K线静默时，
    // 仍需收口 K线最后一根（否则 `idle` 永远为 false）。
    let mut last_kline_tick = std::time::Instant::now();
    let mut last_wallclock_sweep = std::time::Instant::now();
    // 最近一笔 tick 的“本机墙钟 − 事件时间”滞后（积压判定输入）。
    let mut last_lag_ms: i64 = 0;
    // 最近一次**真正执行** sweep 的本地时刻；延迟拍不更新，用于硬上限计时。
    let mut last_sweep_at = std::time::Instant::now();
    // 积压期被推迟的 sweep 拍数（随 K线诊断周期上报）。
    let mut deferred_sweeps: u64 = 0;
    // 未来时间戳脏 tick 的告警限频器（按合约，避免脏数据刷爆日志）。
    let mut future_warn = FutureTickWarnLimiter::default();
    // 诊断增量基准：FFI 回调数 / 已 drain 事件数 / 通道阻塞纳秒。
    let mut drained_ticks: u64 = 0;
    let mut last_cb: u64 = 0;
    let mut last_drained: u64 = 0;
    let mut last_blocked: u64 = 0;
    let mut kline_ticks: u64 = 0;
    let mut last_kline: u64 = 0;

    // tick 消费：断开即结束本会话交外循环重连；停机即整体退出。
    while !stop.load(Ordering::SeqCst) {
        let event = md.next_event(MD_POLL_TIMEOUT);
        // 队列为空（超时）即“空闲拍”：唯一允许用墙钟兜底收口的时机。
        let idle = event.is_none();
        match event {
            // 逐符号订阅应答：成功码 0 记 debug（避免 62 条刷屏），非 0 显式 warn，
            // 用于定位「整批订阅中部分合约（如某交易所批次）被柜台静默拒绝」。
            Some(MdEvent::Subscribed {
                instrument,
                code,
                msg,
            }) => {
                if code == 0 {
                    tracing::debug!(instrument, code, "CTP 行情订阅应答：成功");
                } else {
                    tracing::warn!(instrument, code, error = %msg, "CTP 行情订阅应答：该合约被拒绝");
                }
            }
            Some(MdEvent::Tick(t)) => {
                drained_ticks += 1;
                // 事件时间只合成一次：既做空闲拍滞后校正，也用于 K 线聚合与脏时间戳防护。
                let tick_ms = t.update_utc_ms();
                if let Some(ms) = tick_ms {
                    let lag = now_ms().saturating_sub(ms).max(0);
                    min_lag_ms = min_lag_ms.min(lag);
                    // 积压判定只看最近一笔：恢复期滞后持续 > 阈值即视为仍在补积压。
                    last_lag_ms = lag;
                }
                // 诊断：逐笔 tick 合约/最新价/买卖一（trace，仅排障时开启；drain 热路径
                // 禁止同步 debug 级日志 IO，否则磁盘饱和时单批日志会膨胀成数十万行）。
                tracing::trace!(
                    instrument = %t.instrument,
                    last = t.last_price,
                    bid = t.bids[0].price,
                    ask = t.asks[0].price,
                    "CTP 行情 tick 到达"
                );
                // 作用域判定：业务兴趣集 / K线期望集（每拍对账更新，O(1) 查询）。
                let in_business = subs.business.contains(&t.instrument);
                let in_kline = subs.kline.contains(&t.instrument);
                if in_kline {
                    kline_ticks += 1;
                }

                // 业务集 → QuoteTable + observer（原样）。
                if in_business {
                    let now = now_ms();
                    quotes.apply(&instrument_key(&t.instrument), tick_to_update(&t, now));
                    if let Some(obs) = observer {
                        if let Some(price) = Decimal::try_from(t.last_price)
                            .ok()
                            .filter(|p| *p > Decimal::ZERO)
                        {
                            obs.on_quote(Venue::Ctp, MarketKind::Futures, &t.instrument, price);
                        }
                    }
                }
                // K线集 → 写 TickTable/LastTickStatTable + 聚合（与业务路径正交，可同走）。
                if in_kline {
                    if let Some(k) = kline {
                        let day = t.trading_day_u32();
                        if day != 0 {
                            if let (Some(update_ms), Some(price)) =
                                (tick_ms, Decimal::try_from(t.last_price).ok())
                            {
                                // 同一次判定只取一次墙钟：未来性判定与告警限频共用。
                                let wall_now = now_ms();
                                if is_future_tick(update_ms, wall_now) {
                                    // 柜台把 ActionDay 填成 TradingDay（次日/下周一）时，单条
                                    // 快照会把全局水位永久推到未来，随后当日 tick 全被判迟到。
                                    // 此类脏时间戳不进 K 线聚合（业务报价路径不受影响）。
                                    // 按合约限频：同合约 60s 内只告警一次，并带出抑制条数，
                                    // 避免 1M+ 条 WARN 反过来把日志 IO 变成 drain 瓶颈。
                                    if let Some(suppressed) =
                                        future_warn.should_warn(&t.instrument, wall_now)
                                    {
                                        tracing::warn!(
                                            instrument = %t.instrument,
                                            tick_ms = update_ms,
                                            suppressed,
                                            "丢弃未来时间戳的 K线 tick（疑似 ActionDay 脏数据）"
                                        );
                                    }
                                } else {
                                    let candle = crate::ctp::bar_agg::to_candle_tick(
                                        day,
                                        t.exchange_id.clone(),
                                        update_ms,
                                        price,
                                        t.volume as i64,
                                        Decimal::try_from(t.turnover).unwrap_or(Decimal::ZERO),
                                        Decimal::try_from(t.open_interest).unwrap_or(Decimal::ZERO),
                                    );
                                    // 日 OHLC + 五档随同一快照落 RawTick（CandleTick 不扩字段）。
                                    let depth =
                                        crate::ctp::market_stream::RawTickDepth::from(t.as_ref());
                                    k.stream.on_kline_tick(&t.instrument, &candle, &depth);
                                    last_kline_tick = std::time::Instant::now();
                                }
                            }
                        }
                    }
                }
            }
            Some(MdEvent::FrontDisconnected(reason)) => {
                tracing::warn!(reason, "CTP 行情前置断开，退避重连");
                return md_session_end(stop.load(Ordering::SeqCst));
            }
            Some(MdEvent::Error { code, msg }) => {
                tracing::warn!(code, error = %msg, "CTP 行情错误");
            }
            _ => {}
        }
        // 每个事件后：
        // 1) K线定稿：事件拍走事件时间水位（不读 SystemTime）；仅当“队列空”或“K线已
        //    静默 ≥1s”（业务 tick 仍在跑）时，才用校正后的墙钟兜底收口最后一根；
        // 2) 订阅对账按 ~1s 节流：tick 很密，不能每个事件都扫仓位/规则仓储；对账内容为
        //    业务兴趣 ∪ K线期望 的并集，会话内增减无需断线重连。
        if let Some(k) = kline {
            // T5：积压期延迟 sweep（最多 KLINE_MAX_SWEEP_DEFER_MS），让恢复期尾段 tick
            // 仍能并入在途桶，避免跨品种水位把本应入桶的 tick 判成“迟到”丢弃。
            let behind = last_lag_ms > KLINE_CATCHUP_LAG_MS;
            let since_last_sweep_ms =
                last_sweep_at.elapsed().as_millis().min(i64::MAX as u128) as i64;
            if should_defer_sweep(behind, since_last_sweep_ms) {
                deferred_sweeps = deferred_sweeps.saturating_add(1);
            } else {
                last_sweep_at = std::time::Instant::now();
                let kline_idle = last_kline_tick.elapsed() >= KLINE_IDLE_SWEEP_AFTER;
                if idle || (kline_idle && last_wallclock_sweep.elapsed() >= KLINE_IDLE_SWEEP_AFTER)
                {
                    last_wallclock_sweep = std::time::Instant::now();
                    // 先把本机墙钟按“已观测最小行情滞后”校正成行情时间，再留 grace——即便
                    // 本机时钟相对行情偏移，也不会在桶尾 tick 到达前提前定稿。
                    let feed_now = now_ms().saturating_sub(feed_offset_ms(min_lag_ms));
                    let swept = k
                        .stream
                        .sweep_kline_finalized(feed_now.saturating_sub(KLINE_IDLE_GRACE_MS));
                    if !swept.is_empty() {
                        tracing::debug!(count = swept.len(), "CTP 空闲拍墙钟 sweep 定稿 K线");
                    }
                } else {
                    // 事件拍：水位定稿（水位未越界时 O(1) 返回），绝不读本机墙钟。
                    let swept = k.stream.sweep_kline_by_watermark();
                    if !swept.is_empty() {
                        tracing::debug!(count = swept.len(), "CTP 水位 sweep 定稿 K线");
                    }
                }
            }
            // 周期诊断：迟到/乱序/疑似丢快照 + 行情滞后 + 回调/消费/背压增量
            // （休市静默不上报，避免刷屏）。
            if last_kline_diag.elapsed() >= KLINE_DIAG_INTERVAL {
                last_kline_diag = std::time::Instant::now();
                let cb = md.tick_callback_count();
                let blocked = md.send_blocked_nanos();
                let cb_delta = cb.saturating_sub(last_cb);
                let drained_delta = drained_ticks.saturating_sub(last_drained);
                let blocked_delta = blocked.saturating_sub(last_blocked);
                let kline_delta = kline_ticks.saturating_sub(last_kline);
                last_cb = cb;
                last_drained = drained_ticks;
                last_blocked = blocked;
                last_kline = kline_ticks;
                report_kline_diagnostics(
                    &k.stream,
                    min_lag_ms,
                    cb_delta,
                    drained_delta,
                    kline_delta,
                    blocked_delta,
                    deferred_sweeps,
                );
            }
        }
        if reconcile_due || last_reconcile.elapsed() >= MD_RECONCILE_INTERVAL {
            reconcile_due = false;
            last_reconcile = std::time::Instant::now();
            reconcile_subscriptions(&md, interests, kline, quotes, &mut subs);
        }
    }
    MdSessionOutcome::Stop
}

/// 会话订阅状态：`business`/`kline` 是 tick 路由用的期望集；
/// `subscribed` 是柜台实际订过的并集（FFI 对账基准）。
#[derive(Debug)]
struct SubscriptionState {
    business: std::collections::HashSet<String>,
    kline: std::collections::HashSet<String>,
    subscribed: std::collections::HashSet<String>,
}

/// 业务 ∪ K线 的期望并集与实际订阅并集做差，返回排序后的 (add, remove)。
///
/// H1/H2 修复：
/// * 已在 K 线订阅、后新增为业务兴趣的符号不会重复 subscribe，但会由
///   [`apply_subscription_state`] 进入 `business` 路由集；
/// * 两边都不再需要的符号会进入 remove，不会被对方旧成员永久保留。
fn plan_subscription_reconcile(
    desired_business: &std::collections::HashSet<String>,
    desired_kline: &std::collections::HashSet<String>,
    subscribed: &std::collections::HashSet<String>,
) -> (Vec<String>, Vec<String>) {
    let mut add: Vec<String> = desired_business
        .union(desired_kline)
        .filter(|s| !subscribed.contains(*s))
        .cloned()
        .collect();
    add.sort();
    let mut remove: Vec<String> = subscribed
        .iter()
        .filter(|s| !desired_business.contains(*s) && !desired_kline.contains(*s))
        .cloned()
        .collect();
    remove.sort();
    (add, remove)
}

/// 将本拍对账结果应用到会话状态（纯状态迁移，便于单测）。
///
/// 路由集始终更新为期望集（即使 FFI 受理失败）；实际订阅集只在成功时变更：
/// * add 成功 → 加入 `subscribed`；
/// * remove 成功 → 从 `subscribed` 移除。
#[allow(clippy::too_many_arguments)]
fn apply_subscription_state(
    state: &mut SubscriptionState,
    desired_business: std::collections::HashSet<String>,
    desired_kline: std::collections::HashSet<String>,
    add: &[String],
    remove: &[String],
    add_ok: bool,
    remove_ok: bool,
) {
    if add_ok {
        state.subscribed.extend(add.iter().cloned());
    }
    if remove_ok {
        for s in remove {
            state.subscribed.remove(s);
        }
    }
    state.business = desired_business;
    state.kline = desired_kline;
}

/// K 线期望集的目录未就绪保护：desired 为空但仍有 K 线需求时沿用上一拍集合，
/// 避免把尚未展开的品种误退订。
fn effective_kline(
    desired: std::collections::HashSet<String>,
    has_demand: bool,
    previous: &std::collections::HashSet<String>,
) -> std::collections::HashSet<String> {
    if desired.is_empty() && has_demand {
        previous.clone()
    } else {
        desired
    }
}

/// 业务路由释放的符号：清掉 QuoteTable 旧行，避免市价单/现金换算读取过期盘口。
///
/// 即使该符号仍被 K 线订阅（未进入退订），业务盘口也不应继续参与下单/估值；
/// 后续 tick 只会走 K 线路径，不会重新写入业务盘口。
fn remove_released_business_quotes(
    quotes: &QuoteTable,
    previous_business: &std::collections::HashSet<String>,
    desired_business: &std::collections::HashSet<String>,
) {
    for s in previous_business.difference(desired_business) {
        quotes.remove(&instrument_key(s));
    }
}

/// 会话内订阅对账：重读业务兴趣与 K 线期望，按并集增量 subscribe/unsubscribe。
///
/// K 线目录未就绪（desired 为空但仍有 K 线需求）时保留当前 K 线路由集，避免误退。
/// 注：`md.subscribe` 只返回批次受理码；单合约拒绝经 `MdEvent::Subscribed(code!=0)`
/// 异步返回，本函数只记录日志、不回收 `subscribed`（与既有 K 线对账一致），
/// 被拒合约需重连后重新构建订阅集才会重试（已知限制，后续可加拒单退避表）。
fn reconcile_subscriptions(
    md: &MdHandle,
    interests: &dyn crate::port::MarketInterest,
    kline: Option<&MdKlineDeps>,
    quotes: &QuoteTable,
    state: &mut SubscriptionState,
) {
    let desired_business: std::collections::HashSet<String> =
        ctp_subscription_symbols(interests).into_iter().collect();
    let (desired_kline, has_demand) = match kline {
        Some(k) => (
            k.stream
                .bar_symbols()
                .into_iter()
                .collect::<std::collections::HashSet<_>>(),
            k.stream.has_kline_demand(),
        ),
        None => (std::collections::HashSet::new(), false),
    };
    let effective_kline = effective_kline(desired_kline, has_demand, &state.kline);
    // 业务路由释放即清盘口（与是否成功退订无关）；仍被 K 线需要的符号保留订阅，
    // 但业务侧不再使用旧 book。
    remove_released_business_quotes(quotes, &state.business, &desired_business);
    let (to_add, to_remove) =
        plan_subscription_reconcile(&desired_business, &effective_kline, &state.subscribed);
    // 退订意图即清盘口：即使柜台受理失败、tick 可能继续，路由集已不再需要它。
    for s in &to_remove {
        quotes.remove(&instrument_key(s));
    }

    let add_ok = if to_add.is_empty() {
        true
    } else {
        let refs: Vec<&str> = to_add.iter().map(|s| s.as_str()).collect();
        let code = md.subscribe(&refs);
        if code == 0 {
            tracing::info!(count = to_add.len(), "CTP 会话内增订合约");
            true
        } else {
            tracing::warn!(code, symbols = ?to_add, "CTP 会话内增订受理失败，下拍重试");
            false
        }
    };

    let remove_ok = if to_remove.is_empty() {
        true
    } else {
        let refs: Vec<&str> = to_remove.iter().map(|s| s.as_str()).collect();
        let code = md.unsubscribe(&refs);
        if code == 0 {
            tracing::info!(count = to_remove.len(), "CTP 会话内退订合约");
            true
        } else {
            tracing::warn!(code, symbols = ?to_remove, "CTP 会话内退订受理失败，下拍重试");
            false
        }
    };

    apply_subscription_state(
        state,
        desired_business,
        effective_kline,
        &to_add,
        &to_remove,
        add_ok,
        remove_ok,
    );
}

/// 受理码重试：`attempt` 返回 0 即成功；否则每秒重试直到累计 `max_secs` 秒。
/// 停机标志置位时提前返回当前码（避免关进程时还要睡满 60s）。
fn retry_until_accepted<F: FnMut() -> i32>(
    mut attempt: F,
    stop: &std::sync::atomic::AtomicBool,
    max_secs: u64,
) -> i32 {
    let deadline = std::time::Instant::now() + Duration::from_secs(max_secs);
    loop {
        let code = attempt();
        if code == 0 {
            return 0;
        }
        if stop.load(Ordering::SeqCst) || std::time::Instant::now() >= deadline {
            return code;
        }
        // 1s 分片：循环 5 次 200ms，使停机响应粒度与交易侧一致。
        sleep_interruptibly(Duration::from_secs(1), stop);
    }
}

/// 当前毫秒时间戳。
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 是否未来时间戳脏数据：柜台把 `ActionDay` 误填成 `TradingDay`（次日/下周一）时，
/// 单条快照会把全局水位永久推到未来，随后当日 tick 全被判迟到。
fn is_future_tick(tick_ms: i64, now_ms: i64) -> bool {
    tick_ms > now_ms.saturating_add(KLINE_MAX_FUTURE_MS)
}

/// 积压期是否跳过本拍 sweep（纯判定函数，便于单测）。
///
/// * 未积压 → 正常 sweep，行为与旧实现完全一致；
/// * 积压且距上次真正 sweep 未到硬上限 → 推迟，给迟到 tick 留入桶窗口；
/// * 超过硬上限 → 强制定稿一次，防内存/延迟无界。
fn should_defer_sweep(behind: bool, since_last_sweep_ms: i64) -> bool {
    behind && since_last_sweep_ms < KLINE_MAX_SWEEP_DEFER_MS
}

/// 未来时间戳 WARN 的按合约限频器（60s 一条 + 期间抑制计数）。
///
/// 柜台把 ActionDay 填成 TradingDay 时，同一合约可能每秒刷出成百上千条脏 tick，
/// 无限制告警本身就成为日志 IO 热点，反过来加剧 drain 积压。
#[derive(Debug, Default)]
struct FutureTickWarnLimiter {
    /// 合约 → （下次允许告警的本机时间毫秒，限频窗口内被抑制的条数）。
    state: HashMap<String, (i64, u64)>,
}

impl FutureTickWarnLimiter {
    /// 允许告警时返回 `Some(上次告警以来被抑制的条数)`，否则返回 `None`。
    fn should_warn(&mut self, instrument: &str, now_ms: i64) -> Option<u64> {
        let entry = self.state.entry(instrument.to_string()).or_insert((0, 0));
        if now_ms < entry.0 {
            entry.1 = entry.1.saturating_add(1);
            return None;
        }
        entry.0 = now_ms.saturating_add(FUTURE_TICK_WARN_INTERVAL_MS);
        Some(std::mem::replace(&mut entry.1, 0))
    }
}

/// 空闲拍墙钟校正量：已观测到的“本机墙钟 − 行情事件时间”最小值。
/// `i64::MAX`（尚未观测到任何 tick）与负值都按 0 处理（不校正）。
fn feed_offset_ms(min_lag_ms: i64) -> i64 {
    if min_lag_ms == i64::MAX {
        0
    } else {
        min_lag_ms.max(0)
    }
}

/// K线聚合诊断上报：迟到/乱序丢弃、疑似丢快照、行情水位滞后与时钟偏移。
/// 行情静默（休市/长空档）时直接跳过，避免周期刷屏。
fn report_kline_diagnostics(
    stream: &crate::ctp::market_stream::CtpMarketStream,
    min_lag_ms: i64,
    tick_cb_delta: u64,
    drained_delta: u64,
    kline_delta: u64,
    blocked_ns_delta: u64,
    deferred_sweeps: u64,
) {
    let watermark = stream.kline_watermark_ms();
    if watermark <= 0 {
        return;
    }
    let drain_lag = now_ms().saturating_sub(watermark).max(0);
    if drain_lag > KLINE_DIAG_SILENT_MS {
        return;
    }
    let d = stream.take_kline_diagnostics();
    // 回调数 − 已消费数：窗口内通道积压增量（>0 说明 drain 落后）。
    let backlog = tick_cb_delta as i64 - drained_delta as i64;
    let blocked_ms = blocked_ns_delta as f64 / 1_000_000.0;
    if d.late_dropped > 0 || d.reorder_dropped > 0 || d.snapshot_gaps > 0 || blocked_ns_delta > 0 {
        tracing::warn!(
            late_dropped = d.late_dropped,
            reorder_dropped = d.reorder_dropped,
            snapshot_gaps = d.snapshot_gaps,
            worst_gap_ms = d.worst_gap_ms,
            tail_gap_bars = d.tail_gap_bars,
            worst_tail_gap_ms = d.worst_tail_gap_ms,
            tick_callbacks = tick_cb_delta,
            drained_ticks = drained_delta,
            kline_ticks = kline_delta,
            tick_queue_backlog_delta = backlog,
            send_blocked_ms = blocked_ms,
            deferred_sweeps,
            drain_lag_ms = drain_lag,
            feed_offset_ms = feed_offset_ms(min_lag_ms),
            "CTP K线聚合诊断：存在丢弃/疑似丢快照/背压"
        );
    } else {
        tracing::info!(
            tail_gap_bars = d.tail_gap_bars,
            worst_tail_gap_ms = d.worst_tail_gap_ms,
            tick_callbacks = tick_cb_delta,
            drained_ticks = drained_delta,
            kline_ticks = kline_delta,
            tick_queue_backlog_delta = backlog,
            deferred_sweeps,
            drain_lag_ms = drain_lag,
            feed_offset_ms = feed_offset_ms(min_lag_ms),
            "CTP K线聚合诊断正常"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tick(bid: f64, ask: f64) -> TickSnapshot {
        use crate::ctp::core::quote::QuoteLevel;
        let lvl = |price: f64, vol: i32| QuoteLevel::new(price, vol);
        let zero = || QuoteLevel::new(0.0, 0);
        TickSnapshot {
            instrument: "rb2510".into(),
            exchange_id: "SHFE".into(),
            last_price: 3205.0,
            bids: [lvl(bid, 10), zero(), zero(), zero(), zero()],
            asks: [lvl(ask, 5), zero(), zero(), zero(), zero()],
            day_open: 0.0,
            day_high: 0.0,
            day_low: 0.0,
            day_close: 0.0,
            volume: 1000,
            update_time: "09:30:00".into(),
            update_millisec: 0,
            trading_day: "20261023".into(),
            action_day: "20261023".into(),
            turnover: 0.0,
            open_interest: 0.0,
        }
    }

    #[test]
    fn future_tick_warn_limiter_is_per_instrument_and_counts_suppressed() {
        let mut limiter = FutureTickWarnLimiter::default();
        // 首次必告警（无抑制）。
        assert_eq!(limiter.should_warn("dce.cu2601", 1_000), Some(0));
        // 限频窗口内静默，只累计抑制计数。
        assert_eq!(limiter.should_warn("dce.cu2601", 1_001), None);
        assert_eq!(limiter.should_warn("dce.cu2601", 30_000), None);
        // 限频按合约独立：别的合约不受影响。
        assert_eq!(limiter.should_warn("dce.rb2601", 30_001), Some(0));
        // 窗口到期再告警，并带出期间被抑制的条数（2 条）。
        assert_eq!(limiter.should_warn("dce.cu2601", 61_000), Some(2));
        assert_eq!(limiter.should_warn("dce.cu2601", 61_001), None);
    }

    #[test]
    fn should_defer_sweep_only_while_behind_within_cap() {
        // 正常（lag≈0）：绝不推迟，行为与旧实现完全一致。
        assert!(!should_defer_sweep(false, 0));
        assert!(!should_defer_sweep(false, KLINE_MAX_SWEEP_DEFER_MS + 1));
        // 积压：允许推迟，给迟到 tick 留入桶窗口。
        assert!(should_defer_sweep(true, 0));
        assert!(should_defer_sweep(true, KLINE_MAX_SWEEP_DEFER_MS - 1));
        // 到达硬上限必须强制定稿一次，防内存/推送延迟无界。
        assert!(!should_defer_sweep(true, KLINE_MAX_SWEEP_DEFER_MS));
        assert!(!should_defer_sweep(true, KLINE_MAX_SWEEP_DEFER_MS + 1));
    }

    #[test]
    fn tick_maps_last_and_book() {
        let u = tick_to_update(&tick(3204.0, 3205.0), 100);
        assert_eq!(u.last, Some(Decimal::from(3205)));
        let book = u.book.unwrap();
        assert_eq!(book.bid, Some(Decimal::from(3204)));
        assert_eq!(book.ask, Some(Decimal::from(3205)));
        assert_eq!(u.ts_recv_ms, 100);
    }

    #[test]
    fn book_price_keeps_positive_finite_prices() {
        assert_eq!(book_price(3204.5), Some(Decimal::try_from(3204.5).unwrap()));
    }

    #[test]
    fn book_price_rejects_zero_negative_nan_and_sentinels() {
        for bad in [
            0.0,
            -1.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MAX,
        ] {
            assert_eq!(book_price(bad), None, "非法价格 {bad} 应视为缺侧");
        }
    }

    #[test]
    fn tick_with_only_bid_keeps_bid_and_clears_ask() {
        // 涨停：买一有价、卖一缺失。单边盘口必须保留，供市价平多取 bid。
        let u = tick_to_update(&tick(3204.0, 0.0), 100);
        let book = u.book.expect("单边盘口应保留为 Some");
        assert_eq!(book.bid, Some(Decimal::from(3204)));
        assert_eq!(book.ask, None);
        assert_eq!(u.last, Some(Decimal::from(3205)));
    }

    #[test]
    fn tick_with_only_ask_keeps_ask_and_clears_bid() {
        // 跌停：卖一有价、买一缺失。单边盘口必须保留，供市价平空取 ask。
        let u = tick_to_update(&tick(0.0, 3205.0), 100);
        let book = u.book.expect("单边盘口应保留为 Some");
        assert_eq!(book.bid, None);
        assert_eq!(book.ask, Some(Decimal::from(3205)));
    }

    #[test]
    fn tick_with_both_sides_missing_clears_book() {
        // 两侧都无价：写空 book 主动清除旧盘口，避免停牌/涨跌停沿用过期价位。
        let u = tick_to_update(&tick(0.0, f64::MAX), 100);
        assert_eq!(u.book, Some(TopOfBook::new(None, None)));
        assert_eq!(u.last, Some(Decimal::from(3205)));
    }

    #[test]
    fn one_sided_tick_clears_stale_book_in_table() {
        // 先写入双边，再收到单边 tick：缺失侧必须被清空，不能沿用旧卖一。
        let table = QuoteTable::new();
        let key = instrument_key("rb2510");
        table.apply(&key, tick_to_update(&tick(3204.0, 3205.0), 1));
        table.apply(&key, tick_to_update(&tick(3204.0, 0.0), 2));
        let q = table.get(&key).unwrap();
        let book = q.book.unwrap();
        assert_eq!(book.bid, Some(Decimal::from(3204)));
        assert_eq!(book.ask, None, "单边 tick 必须清掉旧卖一");
    }

    #[test]
    fn instrument_key_is_ctp_futures() {
        let k = instrument_key("rb2510");
        assert_eq!(k.venue, Venue::Ctp);
        assert_eq!(k.kind, MarketKind::Futures);
        assert_eq!(k.symbol, "rb2510");
    }

    #[test]
    fn next_md_action_is_table_driven() {
        // (stopped, has_symbols) → 期望动作。停机优先于一切；
        // 关键反例：未停机且兴趣集空时必须 IdleWait 保活（旧逻辑会让线程退出）。
        let cases = [
            (true, true, MdLoopAction::Exit, "停机优先（即便有标的）"),
            (true, false, MdLoopAction::Exit, "停机且无标的"),
            (false, true, MdLoopAction::Connect, "有标的即建连订阅"),
            (
                false,
                false,
                MdLoopAction::IdleWait,
                "空集保活等待，禁止退出",
            ),
        ];
        for (stopped, has_symbols, expected, note) in cases {
            assert_eq!(
                next_md_action(stopped, has_symbols),
                expected,
                "case: {note}"
            );
        }
    }

    #[test]
    fn planner_stop_always_exits_regardless_of_symbols() {
        let mut p = MdLoopPlanner::new();
        assert_eq!(p.poll(true, true), MdStep::Exit);
        assert_eq!(p.poll(true, false), MdStep::Exit);
    }

    #[test]
    fn planner_empty_set_idle_waits_without_exit_or_backoff() {
        let mut p = MdLoopPlanner::new();
        // 冷启动空窗：反复空集都只保活等待，绝不出 Exit/Connect。
        for _ in 0..3 {
            assert_eq!(p.poll(false, false), MdStep::IdleWait);
        }
    }

    #[test]
    fn planner_self_heals_immediately_when_interest_loads_after_cold_start() {
        // 核心缺陷回归：冷启动空集保活 → 持仓对账载入、兴趣集转非空后，
        // 必须立即 Connect（首次无退避），线程无需重启。
        let mut p = MdLoopPlanner::new();
        assert_eq!(p.poll(false, false), MdStep::IdleWait);
        assert_eq!(p.poll(false, false), MdStep::IdleWait);
        assert_eq!(p.poll(false, true), MdStep::Connect);
    }

    #[test]
    fn planner_climbs_backoff_after_failed_connects() {
        // 真实协议：WaitBackoff 睡完后再 poll（不 note）；只有跑完 Connect 会话才 note。
        let mut p = MdLoopPlanner::new();
        assert_eq!(p.poll(false, true), MdStep::Connect);
        p.note_outcome(MdSessionOutcome::Reconnect); // 排 10s
        assert_eq!(
            p.poll(false, true),
            MdStep::WaitBackoff(Duration::from_secs(10))
        );
        assert_eq!(p.poll(false, true), MdStep::Connect); // 睡完，pending 已消费
        p.note_outcome(MdSessionOutcome::Reconnect); // 升 30s
        assert_eq!(
            p.poll(false, true),
            MdStep::WaitBackoff(Duration::from_secs(30))
        );
    }

    #[test]
    fn planner_idle_window_clears_pending_backoff() {
        // 断线已挂 10s 退避期间兴趣集转空：空窗 reset 阶梯，恢复后立即建连，
        // 不被遗留退避拖住（空集不走退避）。
        let mut p = MdLoopPlanner::new();
        assert_eq!(p.poll(false, true), MdStep::Connect);
        p.note_outcome(MdSessionOutcome::Reconnect); // 排 10s
        assert_eq!(p.poll(false, false), MdStep::IdleWait); // 空窗清零
        assert_eq!(p.poll(false, true), MdStep::Connect); // 立即建连而非再等 10s
    }

    #[test]
    fn planner_successful_session_restarts_schedule_at_ten_seconds() {
        let mut p = MdLoopPlanner::new();
        assert_eq!(p.poll(false, true), MdStep::Connect);
        p.note_outcome(MdSessionOutcome::Reconnect); // 10s
        assert_eq!(
            p.poll(false, true),
            MdStep::WaitBackoff(Duration::from_secs(10))
        );
        assert_eq!(p.poll(false, true), MdStep::Connect);
        p.note_outcome(MdSessionOutcome::Reconnect); // 30s
        assert_eq!(
            p.poll(false, true),
            MdStep::WaitBackoff(Duration::from_secs(30))
        );
        assert_eq!(p.poll(false, true), MdStep::Connect);
        // 一次成功建连后断开：阶梯归零，重新从 10s 起。
        p.note_outcome(MdSessionOutcome::Connected);
        assert_eq!(
            p.poll(false, true),
            MdStep::WaitBackoff(Duration::from_secs(10))
        );
    }

    #[test]
    fn planner_stop_outcome_leaves_backoff_untouched() {
        // Stop 仅用于退出，不应改动退避阶梯（调用方随即 break，这里钉死语义）。
        let mut p = MdLoopPlanner::new();
        assert_eq!(p.poll(false, true), MdStep::Connect);
        p.note_outcome(MdSessionOutcome::Reconnect);
        p.note_outcome(MdSessionOutcome::Stop);
        assert_eq!(
            p.poll(false, true),
            MdStep::WaitBackoff(Duration::from_secs(10))
        );
    }

    #[test]
    fn md_session_end_stops_on_flag_else_marks_connected() {
        // 已成功建连后：停机→退出；前置断开→Connected（外循环归零后按 10s 重连）。
        let stop = std::sync::atomic::AtomicBool::new(false);
        assert_eq!(
            md_session_end(stop.load(Ordering::SeqCst)),
            MdSessionOutcome::Connected
        );
        stop.store(true, Ordering::SeqCst);
        assert_eq!(
            md_session_end(stop.load(Ordering::SeqCst)),
            MdSessionOutcome::Stop
        );
    }

    #[test]
    fn retry_until_accepted_returns_zero_on_success() {
        let stop = std::sync::atomic::AtomicBool::new(false);
        let mut n = 0u32;
        // 前两次流控（-1），第三次受理 0。
        let code = retry_until_accepted(
            || {
                n += 1;
                if n < 3 {
                    -1
                } else {
                    0
                }
            },
            &stop,
            5,
        );
        assert_eq!(code, 0);
        assert_eq!(n, 3);
    }

    #[test]
    fn retry_until_accepted_returns_last_code_after_deadline() {
        let stop = std::sync::atomic::AtomicBool::new(false);
        // 始终 -1：超时窗口 0s → 立即返回非零，不无限重试。
        let code = retry_until_accepted(|| -1, &stop, 0);
        assert_eq!(code, -1);
    }

    #[test]
    fn retry_until_accepted_aborts_on_stop_flag() {
        let stop = std::sync::atomic::AtomicBool::new(true);
        // 停机已置位：即使持续失败也应立即返回，不睡满窗口。
        let code = retry_until_accepted(|| -1, &stop, 60);
        assert_eq!(code, -1);
    }

    // ---- 订阅集合会话内增量对账（业务 ∪ K线） ----

    fn set(items: &[&str]) -> std::collections::HashSet<String> {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn plan_subscription_adds_new_business_symbol() {
        let (add, remove) = plan_subscription_reconcile(&set(&["rb2510"]), &set(&[]), &set(&[]));
        assert_eq!(add, vec!["rb2510".to_string()]);
        assert!(remove.is_empty());
    }

    #[test]
    fn plan_subscription_adds_overlap_symbol_once() {
        // 业务与 K 线同时需要、但当前未订：只增订一次。
        let (add, remove) =
            plan_subscription_reconcile(&set(&["rb2510"]), &set(&["rb2510"]), &set(&[]));
        assert_eq!(add, vec!["rb2510".to_string()]);
        assert!(remove.is_empty());
    }

    #[test]
    fn plan_subscription_removes_symbol_released_by_both_sets() {
        // H2：业务与 K 线都不再需要 → 必须退订，不能因对方旧集残留而永久保留。
        let (add, remove) = plan_subscription_reconcile(&set(&[]), &set(&[]), &set(&["rb2510"]));
        assert!(add.is_empty());
        assert_eq!(remove, vec!["rb2510".to_string()]);
    }

    #[test]
    fn plan_subscription_keeps_symbol_still_desired_by_kline() {
        // 业务释放但 K 线仍需要：不退订。
        let (add, remove) =
            plan_subscription_reconcile(&set(&[]), &set(&["rb2510"]), &set(&["rb2510"]));
        assert!(add.is_empty());
        assert!(remove.is_empty());
    }

    #[test]
    fn plan_subscription_does_not_resubscribe_overlap_already_subscribed() {
        // H1：已在 K 线订阅的符号新增为业务兴趣：不重复增订；
        // 路由集由 apply_subscription_state 更新为 desired_business。
        let (add, remove) =
            plan_subscription_reconcile(&set(&["rb2510"]), &set(&["rb2510"]), &set(&["rb2510"]));
        assert!(add.is_empty());
        assert!(remove.is_empty());
    }

    #[test]
    fn plan_subscription_sorts_multi_element_increments() {
        let (add, remove) =
            plan_subscription_reconcile(&set(&["b", "a"]), &set(&["c"]), &set(&["d", "e"]));
        assert_eq!(add, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        assert_eq!(remove, vec!["d".to_string(), "e".to_string()]);
    }

    #[test]
    fn plan_subscription_empty_sets_are_noop() {
        let (add, remove) = plan_subscription_reconcile(&set(&[]), &set(&[]), &set(&[]));
        assert!(add.is_empty());
        assert!(remove.is_empty());
    }

    #[test]
    fn apply_state_overlap_symbol_becomes_business_routable_without_resubscribe() {
        // H1：符号已在 K 线订阅，新增为业务兴趣后必须进入 business 路由集。
        let mut st = SubscriptionState {
            business: set(&[]),
            kline: set(&["rb2510"]),
            subscribed: set(&["rb2510"]),
        };
        apply_subscription_state(
            &mut st,
            set(&["rb2510"]),
            set(&["rb2510"]),
            &[],
            &[],
            true,
            true,
        );
        assert!(st.business.contains("rb2510"), "业务路由集必须包含该符号");
        assert!(st.kline.contains("rb2510"));
        assert!(st.subscribed.contains("rb2510"));
    }

    #[test]
    fn apply_state_removes_symbol_released_by_both_sets() {
        // H2：退订成功且两边都不再需要 → 实际订阅集与路由集都应清空。
        let mut st = SubscriptionState {
            business: set(&["rb2510"]),
            kline: set(&["rb2510"]),
            subscribed: set(&["rb2510"]),
        };
        apply_subscription_state(
            &mut st,
            set(&[]),
            set(&[]),
            &[],
            &["rb2510".to_string()],
            true,
            true,
        );
        assert!(st.business.is_empty());
        assert!(st.kline.is_empty());
        assert!(st.subscribed.is_empty());
    }

    #[test]
    fn apply_state_add_failure_keeps_subscribed_unchanged() {
        let mut st = SubscriptionState {
            business: set(&[]),
            kline: set(&[]),
            subscribed: set(&[]),
        };
        apply_subscription_state(
            &mut st,
            set(&["rb2510"]),
            set(&[]),
            &["rb2510".to_string()],
            &[],
            false,
            true,
        );
        assert!(st.subscribed.is_empty(), "增订失败不得记入已订阅");
        assert!(st.business.contains("rb2510"), "路由集仍按期望更新");
    }

    #[test]
    fn apply_state_remove_failure_keeps_subscribed_and_stops_routing() {
        let mut st = SubscriptionState {
            business: set(&["rb2510"]),
            kline: set(&["rb2510"]),
            subscribed: set(&["rb2510"]),
        };
        apply_subscription_state(
            &mut st,
            set(&[]),
            set(&[]),
            &[],
            &["rb2510".to_string()],
            true,
            false,
        );
        assert!(st.subscribed.contains("rb2510"), "退订失败保留实际订阅");
        assert!(st.business.is_empty(), "退订后先停止业务路由");
        assert!(st.kline.is_empty(), "退订后先停止 K 线路由");
    }

    #[test]
    fn effective_kline_keeps_previous_when_desired_empty_but_demand_exists() {
        // 目录未就绪：desired 展开为空但仍有 K 线需求 → 沿用上一拍，避免误退。
        let got = effective_kline(set(&[]), true, &set(&["rb2510"]));
        assert_eq!(got, set(&["rb2510"]));
    }

    #[test]
    fn effective_kline_uses_empty_desired_when_no_demand() {
        // 真空需求：desired 为空且无 K 线消费者 → 允许退订。
        let got = effective_kline(set(&[]), false, &set(&["rb2510"]));
        assert!(got.is_empty());
    }

    #[test]
    fn effective_kline_uses_non_empty_desired_over_previous() {
        let got = effective_kline(set(&["cu2512"]), true, &set(&["rb2510"]));
        assert_eq!(got, set(&["cu2512"]));
    }

    #[test]
    fn released_business_quote_is_removed_but_still_desired_is_kept() {
        let quotes = QuoteTable::new();
        let a = instrument_key("rb2510");
        let b = instrument_key("cu2512");
        for inst in [&a, &b] {
            let mut u = QuoteUpdate::new(1);
            u.book = Some(TopOfBook::new(Some(Decimal::ONE), Some(Decimal::ONE)));
            quotes.apply(inst, u);
        }

        remove_released_business_quotes(&quotes, &set(&["rb2510", "cu2512"]), &set(&["rb2510"]));

        assert!(quotes.get(&a).is_some(), "仍在业务集的盘口必须保留");
        assert!(quotes.get(&b).is_none(), "业务释放的盘口必须清理");
    }

    #[test]
    fn future_tick_guard_allows_skew_rejects_day_jump() {
        let now = 1_000_000_000;
        assert!(!is_future_tick(now, now));
        assert!(!is_future_tick(now - 5_000, now));
        // 上限内（含边界）放行：正常时钟偏差不能误杀。
        assert!(!is_future_tick(now + KLINE_MAX_FUTURE_MS, now));
        assert!(is_future_tick(now + KLINE_MAX_FUTURE_MS + 1, now));
        // ActionDay 填成次日：跳变数小时 → 拒绝。
        assert!(is_future_tick(now + 3 * 3_600_000, now));
    }

    #[test]
    fn feed_offset_defaults_to_zero_and_clamps_negative() {
        // 尚未观测到 tick：不校正。
        assert_eq!(feed_offset_ms(i64::MAX), 0);
        // 本机时钟落后行情（负滞后）：不校正，宁可晚收口也不提前定稿。
        assert_eq!(feed_offset_ms(-5), 0);
        // 观测到的最小滞后照常扣除。
        assert_eq!(feed_offset_ms(1_500), 1_500);
        assert_eq!(feed_offset_ms(0), 0);
    }
}
