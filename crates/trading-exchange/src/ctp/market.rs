//! CTP 公共行情（可降级）：MdHandle → QuoteTable + QuoteObserver。
//!
//! 柜台未开行情前置时（登录码 64 等）降级：不订阅、不写行情表，
//! 仓位重估由宿主 fallback_valuation 语义处理，不阻断交易。

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

/// 兴趣集为空时的保活重查间隔：线程不退出，睡一个分片周期后重读兴趣集。
/// 固定间隔、**不走** [`ReconnectBackoff`]——兴趣集是冷启动竞态下的临时空窗
/// （持仓对账尚未载入），不能被断线退避阶梯拖住；睡眠受停机标志分片打断。
const MD_IDLE_POLL: Duration = Duration::from_secs(5);

/// CTP 行情写入目标键（venue/kind 恒为 Ctp/Futures）。
fn instrument_key(symbol: &str) -> Instrument {
    Instrument::new(Venue::Ctp, MarketKind::Futures, symbol)
}

/// TickSnapshot → QuoteUpdate（CTP 最新价即 last；盘口写入买一卖一；
/// 柜台以 f64::MAX 表示无对手价，视为缺侧）。
fn tick_to_update(t: &TickSnapshot, now_ms: i64) -> QuoteUpdate {
    // 业务 QuoteTable 路径本期仍只用 best1（五档全量仅落 RawTick）。
    let bid = t.bids[0].price;
    let ask = t.asks[0].price;
    let book = if bid > 1e-9 && ask > 1e-9 && ask < f64::MAX {
        Some(TopOfBook::new(
            Decimal::try_from(bid).ok(),
            Decimal::try_from(ask).ok(),
        ))
    } else {
        None
    };
    QuoteUpdate {
        last: Decimal::try_from(t.last_price)
            .ok()
            .filter(|p| *p > Decimal::ZERO),
        mark: None,
        mid: None,
        book,
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

    // 本会话业务兴趣集合（会话内不重订阅，与历史一致；预算成集合供每笔 O(1) 判定）。
    let business_set: std::collections::HashSet<String> =
        ctp_subscription_symbols(interests).into_iter().collect();
    // K线期望集：会话开始预算当前已订合约，并在循环内按节拍动态对账（见下方
    // reconcile_kline_subscriptions）——支持配置基线品种展开与客户端运行时订阅/
    // 退订在会话内增删，无需断线重连。
    let mut kline_set: std::collections::HashSet<String> = kline
        .map(|k| k.stream.bar_symbols().into_iter().collect())
        .unwrap_or_default();

    // tick 消费：断开即结束本会话交外循环重连；停机即整体退出。
    while !stop.load(Ordering::SeqCst) {
        match md.next_event(MD_POLL_TIMEOUT) {
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
                // 诊断：逐笔 tick 合约/最新价/买卖一（debug，用于确认各所实时行情是否进表）。
                tracing::debug!(
                    instrument = %t.instrument,
                    last = t.last_price,
                    bid = t.bids[0].price,
                    ask = t.asks[0].price,
                    "CTP 行情 tick 到达"
                );
                // 作用域判定：业务兴趣集 / K线期望集（均会话开始预算，O(1) 查询）。
                let in_business = business_set.contains(&t.instrument);
                let in_kline = kline_set.contains(&t.instrument);

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
                // K线集 → 写 TickTable/LastVolumeTable + 聚合（与业务路径正交，可同走）。
                if in_kline {
                    if let Some(k) = kline {
                        let day = t.trading_day_u32();
                        if day != 0 {
                            if let (Some(update_ms), Some(price)) =
                                (t.update_utc_ms(), Decimal::try_from(t.last_price).ok())
                            {
                                let candle = crate::ctp::bar_agg::to_candle_tick(
                                    day,
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
                            }
                        }
                    }
                }
            }
            Some(MdEvent::FrontDisconnected(reason)) => {
                tracing::warn!(reason, "CTP 行情前置断开，退避重连");
                return md_session_end(stop.load(Ordering::SeqCst));
            }
            _ => {}
        }
        // 每个 poll 节拍（约 1s）对账一次 K线期望集：配置基线品种展开 ∪ 客户端
        // 运行时订阅发生增删时，会话内增量 subscribe/unsubscribe，不等断线重连。
        if let Some(k) = kline {
            reconcile_kline_subscriptions(&md, k, &business_set, &mut kline_set);
        }
    }
    MdSessionOutcome::Stop
}

/// 会话内 K线订阅对账：把柜台实际订阅的 K线集合对齐到当前 `bar_symbols()`。
///
/// - 新增（期望有、当前无）且不属于业务集合约 → 增量 `subscribe`；
/// - 消失（当前有、期望无）且不属于业务集合约 → `unsubscribe`（业务兴趣集的
///   合约即使 K线退订也不退，老路径仍在收）；
/// - 合约目录尚未预热导致展开为空时不误退（保留当前集，等下一拍）。
fn reconcile_kline_subscriptions(
    md: &MdHandle,
    kline: &MdKlineDeps,
    business: &std::collections::HashSet<String>,
    current: &mut std::collections::HashSet<String>,
) {
    let desired: std::collections::HashSet<String> =
        kline.stream.bar_symbols().into_iter().collect();

    // 目录未就绪：desired 可能暂空，跳过本拍以免误退已订合约。
    // 判据：存在配置基线/客户端品种但目录展开为空——简单起见，仅当 desired 非空
    // 或确无任何 K线需求时才动作；空需求且 current 非空属正常全退订，需区分。
    // 这里用 stream 是否有任何 bar 客户端/基线来判断"空"是真空还是未就绪。
    let has_demand = kline.stream.has_kline_demand();
    if desired.is_empty() && has_demand {
        return;
    }

    let to_add: Vec<String> = desired.difference(current).cloned().collect();
    let to_remove: Vec<String> = current
        .iter()
        .filter(|s| !desired.contains(*s) && !business.contains(*s))
        .cloned()
        .collect();

    if !to_add.is_empty() {
        let refs: Vec<&str> = to_add.iter().map(|s| s.as_str()).collect();
        let code = md.subscribe(&refs);
        if code == 0 {
            tracing::info!(symbols = to_add.len(), "CTP K线会话内增订合约");
            current.extend(to_add);
        } else {
            tracing::warn!(code, symbols = ?to_add, "CTP K线增订受理失败，下拍重试");
        }
    }
    if !to_remove.is_empty() {
        let refs: Vec<&str> = to_remove.iter().map(|s| s.as_str()).collect();
        let code = md.unsubscribe(&refs);
        if code == 0 {
            tracing::info!(symbols = refs.len(), "CTP K线会话内退订合约");
            for s in &to_remove {
                current.remove(s);
            }
        } else {
            tracing::warn!(code, symbols = ?to_remove, "CTP K线退订受理失败，下拍重试");
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn tick(bid: f64, ask: f64) -> TickSnapshot {
        use crate::ctp::core::quote::QuoteLevel;
        let lvl = |price: f64, vol: i32| QuoteLevel::new(price, vol);
        let zero = || QuoteLevel::new(0.0, 0);
        TickSnapshot {
            instrument: "rb2510".into(),
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
    fn tick_maps_last_and_book() {
        let u = tick_to_update(&tick(3204.0, 3205.0), 100);
        assert_eq!(u.last, Some(Decimal::from(3205)));
        let book = u.book.unwrap();
        assert_eq!(book.bid, Some(Decimal::from(3204)));
        assert_eq!(book.ask, Some(Decimal::from(3205)));
        assert_eq!(u.ts_recv_ms, 100);
    }

    #[test]
    fn tick_with_missing_side_has_no_book() {
        let u = tick_to_update(&tick(0.0, f64::MAX), 100);
        assert!(u.book.is_none());
        assert_eq!(u.last, Some(Decimal::from(3205)));
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
}
