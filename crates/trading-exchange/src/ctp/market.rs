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

/// CTP 行情写入目标键（venue/kind 恒为 Ctp/Futures）。
fn instrument_key(symbol: &str) -> Instrument {
    Instrument::new(Venue::Ctp, MarketKind::Futures, symbol)
}

/// TickSnapshot → QuoteUpdate（CTP 最新价即 last；盘口写入买一卖一；
/// 柜台以 f64::MAX 表示无对手价，视为缺侧）。
fn tick_to_update(t: &TickSnapshot, now_ms: i64) -> QuoteUpdate {
    let book = if t.bid_price > 1e-9 && t.ask_price > 1e-9 && t.ask_price < f64::MAX {
        Some(TopOfBook::new(
            Decimal::try_from(t.bid_price).ok(),
            Decimal::try_from(t.ask_price).ok(),
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

/// 启动行情线程（阻塞线程内跑退避重连循环）。
///
/// `interests`：订阅兴趣供给（与其它所同型，None 时不订阅、线程退出）。
/// 部署性关闭（未配 `md_front` / 无兴趣端口/兴趣集空）直接退出、不重连；
/// 运行期断线、登录或订阅失败则按 [`ReconnectBackoff`] 退避重连
/// （10s→30s→1m→3m→5m 封顶），与交易连接 `run_session_loop` 同型，
/// 不再需要重启服务即可在前置恢复/夜盘开盘时自动恢复 tick。
pub fn spawn_market_worker(
    params: MdParams,
    interests: Option<Arc<dyn crate::port::MarketInterest>>,
    quotes: Arc<QuoteTable>,
    observer: Option<Arc<dyn QuoteObserver>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) {
    // 防御性守卫：空行情前置不应到达 FFI 的 register_front。
    if params.config.md_front.trim().is_empty() {
        tracing::warn!("CTP 行情前置未配置，行情线程不启动");
        return;
    }
    std::thread::Builder::new()
        .name("ctp-md".into())
        .spawn(move || run_md_loop(&params, interests, &quotes, observer, &stop))
        .map(|_| ())
        .unwrap_or_else(|e| tracing::warn!(error = %e, "CTP 行情线程启动失败"));
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

/// 行情线程主循环（退避重连外循环）：参照交易 `run_session_loop`。
///
/// 每个周期新建并 Drop 一个 `MdHandle`（连接→登录→订阅→tick），成功过则
/// `backoff.reset()`、失败/断线 `note_retry()`；退避睡眠可被停机标志分片打断。
fn run_md_loop(
    params: &MdParams,
    interests: Option<Arc<dyn crate::port::MarketInterest>>,
    quotes: &Arc<QuoteTable>,
    observer: Option<Arc<dyn QuoteObserver>>,
    stop: &std::sync::atomic::AtomicBool,
) {
    let Some(interests) = interests else {
        tracing::info!("CTP 行情未提供兴趣端口，跳过订阅");
        return;
    };
    // 无兴趣集属于部署态（非临时故障）：直接退出，不进入退避重连。
    let has_symbols = interests
        .interests()
        .iter()
        .any(|k| k.venue == Venue::Ctp && k.kind == MarketKind::Futures);
    if !has_symbols {
        tracing::info!("CTP 行情兴趣集为空，跳过连接");
        return;
    }

    let mut backoff = ReconnectBackoff::new();
    while !stop.load(Ordering::SeqCst) {
        if let Some(wait) = backoff.next_wait() {
            sleep_interruptibly(wait, stop);
        }
        if stop.load(Ordering::SeqCst) {
            break;
        }
        // 每周期按当前兴趣集重新取订阅标的（新增/移除合约随重连生效）。
        let symbols: Vec<String> = interests
            .interests()
            .iter()
            .filter(|k| k.venue == Venue::Ctp && k.kind == MarketKind::Futures)
            .map(|k| k.symbol.clone())
            .collect();

        let outcome = if symbols.is_empty() {
            tracing::info!("CTP 行情兴趣集变空，行情线程退出");
            MdSessionOutcome::Stop
        } else {
            run_md_session(params, &symbols, quotes, &observer, stop)
        };

        match outcome {
            MdSessionOutcome::Stop => break,
            // 成功建连后被断开：阶梯归零，再排下一次等待（note_retry → 10s），
            // 与交易 run_session_loop「连接成功 reset、循环末 note_retry」一致。
            MdSessionOutcome::Connected => {
                backoff.reset();
                backoff.note_retry();
            }
            MdSessionOutcome::Reconnect => backoff.note_retry(),
        }
    }
    tracing::info!("CTP 行情线程退出");
}

/// 单个连接会话：连接 → 登录 → 订阅 → 消费 tick，直到断线/失败/停机。
/// 返回前 `MdHandle` drop（release 刷盘），下一周期重新建连。
fn run_md_session(
    params: &MdParams,
    symbols: &[String],
    quotes: &Arc<QuoteTable>,
    observer: &Option<Arc<dyn QuoteObserver>>,
    stop: &std::sync::atomic::AtomicBool,
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

    // tick 消费：断开即结束本会话交外循环重连；停机即整体退出。
    while !stop.load(Ordering::SeqCst) {
        match md.next_event(MD_POLL_TIMEOUT) {
            Some(MdEvent::Tick(t)) => {
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
            Some(MdEvent::FrontDisconnected(reason)) => {
                tracing::warn!(reason, "CTP 行情前置断开，退避重连");
                return md_session_end(stop.load(Ordering::SeqCst));
            }
            _ => {}
        }
    }
    MdSessionOutcome::Stop
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
        TickSnapshot {
            instrument: "rb2510".into(),
            last_price: 3205.0,
            bid_price: bid,
            bid_volume: 10,
            ask_price: ask,
            ask_volume: 5,
            volume: 1000,
            update_time: "09:30:00".into(),
            update_millisec: 0,
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
