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

use crate::ctp::core::account::CtpAccount;
use crate::ctp::core::quote::TickSnapshot;
use crate::ctp::gateway::md::{MdEvent, MdHandle};
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

/// 启动行情线程（阻塞线程内跑 MdHandle 循环）。
///
/// `interests`：订阅兴趣供给（与其它所同型，None 时不订阅、线程退出）。
/// 登录失败（含 code=64 行情前置未开）按 warn 降级退出：不重试风暴，
/// 由宿主重启/重载时再尝试。
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

/// 行情线程主循环：连接 → 登录 → 按兴趣订阅 → tick 写表 + 回调观察口。
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
    let symbols: Vec<String> = interests
        .interests()
        .iter()
        .filter(|k| k.venue == Venue::Ctp && k.kind == MarketKind::Futures)
        .map(|k| k.symbol.clone())
        .collect();
    if symbols.is_empty() {
        tracing::info!("CTP 行情兴趣集为空，跳过连接");
        return;
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
            tracing::warn!(error = %e, "CTP 行情前置连接失败，行情降级");
            return;
        }
    };
    // 登录受理重试（官方文档口径：FrontConnected 只是 TCP 建连，FTD 会话
    // 协商未完成时 ReqUserLogin 返回 -1，应"等待一小会重发"）。
    let login_deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let login_code = loop {
        let code = md.login();
        if code == 0 {
            break 0;
        }
        if std::time::Instant::now() > login_deadline {
            break code;
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    };
    if login_code != 0 {
        tracing::warn!(code = login_code, "CTP 行情登录受理失败（重试 60s 后放弃），行情降级");
        return;
    }
    let refs: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();
    // 订阅受理同样按官方口径重发（规则同登录）。
    let sub_code = loop {
        let code = md.subscribe(&refs);
        if code == 0 {
            break 0;
        }
        if std::time::Instant::now() > login_deadline {
            break code;
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    };
    if sub_code != 0 {
        tracing::warn!(code = sub_code, "CTP 行情订阅受理失败，行情降级");
        return;
    }
    tracing::info!(count = symbols.len(), "CTP 行情订阅生效");
    // 优雅停机：tick 轮询粒度响应停机标志（MdHandle 析构正常刷盘，避免 kill 线程损坏流文件）。
    while !stop.load(std::sync::atomic::Ordering::SeqCst) {
        match md.next_event(MD_POLL_TIMEOUT) {
            Some(MdEvent::Tick(t)) => {
                let now = now_ms();
                quotes.apply(&instrument_key(&t.instrument), tick_to_update(&t, now));
                if let Some(obs) = &observer {
                    if let Some(price) = Decimal::try_from(t.last_price)
                        .ok()
                        .filter(|p| *p > Decimal::ZERO)
                    {
                        obs.on_quote(Venue::Ctp, MarketKind::Futures, &t.instrument, price);
                    }
                }
            }
            Some(MdEvent::FrontDisconnected(reason)) => {
                tracing::warn!(reason, "CTP 行情前置断开，行情降级（不自动重连）");
                return;
            }
            _ => {}
        }
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
}
