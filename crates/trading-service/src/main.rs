//! trading-service 入口。
//!
//! 启动顺序：经 [`trading_store::init`] 一处完成开店/四表注册/最新数据日 CSV 恢复/
//! 定时持久化/账户 account.csv 首载 -> 装配共享状态（数据句柄全部取自 store）->
//! 监听南向(strategy) WS 端口；对交易所（真实所/本地模拟所）一律主动外连。
//! 退出（Ctrl-C）前停全部用户数据流连接，并对持久化句柄做最后一次 flush。

mod account_hub;
mod admin;
mod api;
mod cashqty;
mod config;
mod event;
mod hub;
mod interest;
mod notify;
mod order_query;
mod position_query;
mod positions;
mod rules;
mod service;
mod south;
mod south_auth;
mod state;
mod venue_config;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;
use store_core::SystemClock;
use tokio::net::TcpListener;
use tracing::{error, info, warn};
use trading_store::init::{InitParams, Initialized};

use config::Args;
use state::{AppState, AssemblyDeps, VenueEndpoints};

use crate::account_hub::AccountHub;

/// 启动期只读校验所有 binance 账户的实际持仓模式：
///
/// 逐账户连适配器查 `/fapi/v1/positionSide/dual`，与本地 `position_mode` 比对：
/// 不一致记 **error**（含账户/本地/远端），提示人工核对；查询失败记 warn。
/// 全部在后台独立任务进行，**绝不自动修改用户交易所设置**；非 binance 账户不查。
fn verify_position_modes(state: &AppState) {
    let binance_accounts: Vec<_> = state
        .accounts
        .list_active_briefs()
        .into_iter()
        .filter(|a| a.exchange == "binance")
        .collect();
    for acc in binance_accounts {
        let state = state.clone();
        let name = acc.name.clone();
        let configured = acc.position_mode;
        tokio::spawn(async move {
            let adapter = match state.exchanges.connect("binance", Some("futures"), &name) {
                Ok(a) => a,
                Err(e) => {
                    warn!(account = %name, error = %e, "持仓模式校验：无法连接交易所，跳过");
                    return;
                }
            };
            match adapter.query_position_mode().await {
                Ok(Some(actual)) => {
                    let remote_dual = actual == trading_models::PositionMode::Hedge;
                    if let Some(actual) = trading_exchange::binance::prep::detect_mode_mismatch(
                        configured,
                        remote_dual,
                    ) {
                        error!(
                            account = %name,
                            configured = ?configured,
                            exchange_actual = ?actual,
                            "账户持仓模式与交易所实际设置不一致，请人工核对（系统不会自动修改）"
                        );
                    } else {
                        info!(account = %name, mode = ?configured, "持仓模式校验一致");
                    }
                }
                Ok(None) => {} // 该适配器不支持校验
                Err(e) => warn!(account = %name, error = %e, "持仓模式校验查询失败"),
            }
        });
    }
}

fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,trading_service=info,trading_store=info"));
    fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init()
        .ok();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let cfg = Args::parse_validated()?;
    info!(
        south = %cfg.south_addr,
        exchanges = ?cfg.exchanges.iter().map(|m| (&m.name, &m.rest_base_url)).collect::<Vec<_>>(),
        data_dir = %cfg.data_dir,
        flush_interval = ?cfg.flush_interval,
        rest_timeout = ?cfg.rest_timeout,
        "starting trading-service"
    );

    // —— 统一引导：开店/恢复/定时落盘 + 账户首载（共享内存初始化收口在 store）——
    let Initialized {
        store,
        persist,
        accounts: account_manager,
    } = Initialized::bootstrap(InitParams {
        data_dir: &cfg.data_dir,
        clock: Arc::new(SystemClock),
        flush_interval: Some(cfg.flush_interval),
        account_file: cfg.account_file.clone().into(),
        account_key_file: cfg.account_key_file.clone().into(),
    })
    .await?;
    let recovered_accounts = store.accounts()?.list_including_deleted().len();
    let recovered_orders = store.orders()?.list_including_deleted().len();
    info!(
        accounts = recovered_accounts,
        orders = recovered_orders,
        "recovered from csv snapshots"
    );
    // Venue 可调参数（deribit 价差阈值/漂移档数；文件缺失=默认）。
    let venue_cfg = venue_config::load(std::path::Path::new(&cfg.venue_config))?;

    // 账户门面：账户域在 store，交易所/规则/仓位端口实现在服务层（newtype 委托）。
    let accounts = AccountHub::new(account_manager);
    let account_stop = Arc::new(AtomicBool::new(false));

    // —— 南向连接鉴权：单一共享密钥文件（0600，首启自动生成）——
    let south_secret =
        south_auth::load_or_create_secret(std::path::Path::new(&cfg.south_secret_file))?;
    if cfg.south_dev_insecure {
        tracing::error!(
            "⚠️  南向鉴权处于 --south-dev-insecure 旁路模式：不校验 HMAC 签名，仅限本地联调，严禁用于生产"
        );
    } else {
        info!(path = %cfg.south_secret_file, "south HMAC auth enabled");
    }
    // sweeper 与鉴权共用同一个 NonceGuard（内部 Arc 句柄，非 channel）。
    let south_nonces = south_auth::NonceGuard::new(Arc::new(SystemClock), south_auth::NONCE_TTL);
    let south_auth = south_auth::SouthAuth::from_secret(
        south_secret,
        south_nonces.clone(),
        cfg.south_dev_insecure,
    );
    south_auth::spawn_sweeper(
        south_nonces,
        south_auth::SWEEP_INTERVAL,
        Arc::clone(&account_stop),
    );

    // —— 本地模拟所静态端点覆盖（REST 基址 + 账户事件 WS 地址，同一类型直通）——
    let endpoints: VenueEndpoints = Arc::new(cfg.exchanges.clone());

    // —— 共享状态：交易所注册表/仓位引擎/规则引擎在此一处装配，数据句柄全取自 store ——
    let state = AppState::assemble(AssemblyDeps {
        store: Arc::clone(&store),
        accounts: accounts.clone(),
        south_auth,
        rest_timeout: cfg.rest_timeout,
        rule_refresh_interval: cfg.rule_refresh_interval,
        position_debounce: cfg.position_debounce,
        position_reconcile_interval: cfg.position_reconcile_interval,
        endpoints,
        channels: notify::ChannelUrls::new(
            cfg.notify_open_url,
            cfg.notify_close_url,
            cfg.notify_test_url,
            cfg.notify_manual_close_url,
            cfg.notify_position_url.clone(),
        ),
        venue: venue_cfg,
    });

    // —— 交易所级共享任务：按账户表去重出的交易所启动规则同步/公共行情（注册表 fan-out）。
    // 具体适配器类型全部封在 trading-exchange，main 不点名任何具体交易所。
    // 任务内部自带退避重连，随进程生命周期运行（行情不落盘，关停不做收尾 flush）。
    // 注意：交易所级公共服务仅在启动时按账户归属连接；运行期热加载新增的交易所
    // 需重启才会建其公共行情/规则连接（per-account 用户流仍支持热增删）。
    let active_exchanges = accounts.distinct_exchanges();
    info!(
        ?active_exchanges,
        "交易所级公共服务（规则/行情）按账户归属启动"
    );
    state.exchanges.spawn_market_services(&active_exchanges);

    // —— 持仓模式只读校验：各 binance 账户查 /fapi/v1/positionSide/dual，与本地
    // account.csv 配置不一致只记 error 日志，绝不自动修改用户交易所设置。
    verify_position_modes(&state);

    // —— 账户事件侧：各所统一按账户表周期对账 ——
    // 每个活动账户一条外向长连接（xili 与真实所一致）；重载后 version 变化即停旧起新。
    // 服务只注入订单回报出口与账户目标两个端口，不感知各所连接生命周期差异。
    let sink: Arc<dyn trading_exchange::port::OrderEventSink> = Arc::new(state.clone());
    let targets: Arc<dyn trading_exchange::port::AccountTargets> = Arc::new(state.accounts.clone());
    let account_events = state.exchanges.spawn_account_events(
        sink,
        targets,
        cfg.account_reload_interval
            .max(std::time::Duration::from_millis(200)),
        Arc::clone(&account_stop),
    );

    // —— 盯盘规则：定时扫描 active 规则（行情/仓位已由各自流水线刷新进内存）——
    // 命中的去重由 RuleRepo 的行锁 CAS 保证，故任务重叠也安全；不使用 channel。
    let rule_engine = state.rule_engine.clone();
    let rule_stop = Arc::clone(&account_stop);
    let rule_scan_interval = cfg.rule_scan_interval;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(rule_scan_interval);
        loop {
            ticker.tick().await;
            if rule_stop.load(Ordering::Relaxed) {
                break;
            }
            rule_engine.run_scan().await;
        }
    });

    // —— 规则单 REST 终态兜底：用户流 WS 断连/漏推时，按对账周期主动查单，
    // 把交易所侧已终态的规则单收敛并 retire 关联规则（与 WS 回报同一收口，幂等）。——
    let rule_engine = state.rule_engine.clone();
    let term_stop = Arc::clone(&account_stop);
    let term_interval = cfg.position_reconcile_interval;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(term_interval);
        loop {
            ticker.tick().await;
            if term_stop.load(Ordering::Relaxed) {
                break;
            }
            let n = rule_engine.reconcile_terminal_orders().await;
            if n > 0 {
                info!(converged = n, "规则单终态 REST 兜底收敛完成");
            }
        }
    });

    // —— 仓位定时播报/告警（纯通知旁路）：播报 C + deribit ROI 告警 + 未设规则仓位
    // 告警，全部发 position 通道；未配置 `--notify-position-url` 则不启动（零外联）——
    if cfg.notify_position_url.trim().is_empty() {
        info!("未配置 --notify-position-url，仓位定时播报/告警不启动");
    } else {
        // 百分数（-20）→ 比率口径（-0.20）；非法值回退默认 -20%。
        let threshold = rust_decimal::Decimal::from_f64_retain(cfg.roi_alert_threshold_pct / 100.0)
            .unwrap_or_else(notify::report::default_roi_alert_threshold);
        let reporter = notify::report::PositionReporter::new(
            state.notify.clone(),
            state.exchanges.clone(),
            state.accounts.inner().clone(),
            store.position_repo().expect("position repo after recover"),
            store.rule_repo().expect("rule repo after recover"),
            threshold,
        );
        reporter.spawn();
        info!(
            roi_alert_threshold = %threshold,
            "仓位定时播报/告警已启动（1h 周期，position 通道）"
        );
    }

    // —— 南向 WS 服务 + 管理 API（同一本地 bind 端口；交易所方向一律主动外连）——
    let south_app = Router::new()
        .route("/", get(south::ws_handler))
        .route("/ws", get(south::ws_handler))
        // 账户重载触发（预留）：POST 后重读 account.csv，连接在对账周期重建。
        .route("/admin/accounts/reload", post(admin::reload_accounts))
        // REST API（/health 与 /api/v1/*，与南向 WS 同端口）。
        .merge(api::router())
        .with_state(state.clone());

    let south_listener = TcpListener::bind(&cfg.south_addr).await?;
    info!("south (strategy) ws listening on ws://{}/", cfg.south_addr);

    let south_server = axum::serve(south_listener, south_app);

    tokio::select! {
        r = south_server => if let Err(e)=r { error!(%e,"south server error"); },
        _ = tokio::signal::ctrl_c() => info!("ctrl-c received, shutting down"),
    }

    // 停止全部账户事件流长连接，再做最后落盘。
    account_stop.store(true, Ordering::Relaxed);
    account_events.stop_all();
    if let Some(handle) = persist {
        handle.shutdown().await;
    } else {
        // 未开定时持久化时也尽力补一次同步落盘。
        if let Err(e) = store.flush() {
            error!(error = %e, "final flush failed");
        }
    }
    info!("trading-service stopped");
    Ok(())
}
