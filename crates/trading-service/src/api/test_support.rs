//! 集成测试支撑：用临时目录起真实 TradingStore 并装配 AppState/Router。
//!
//! 仅编译进测试。不连真实交易所：账户可配置但无外发连接，下单等触发网络
//! 的路径只验证同步的内存结果。

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use store_core::MockClock;
use tempfile::TempDir;
use trading_store::init::{InitParams, Initialized};

use crate::account_hub::AccountHub;
use crate::state::{AppState, AssemblyDeps, VenueEndpoints};

/// account.csv 表头（与生产一致：新 7 列，6 公共列 + credentials）。
pub const ACCOUNT_HEADER: &str = "id,name,exchange,credentials,deleted,created_at,updated_at\n";

/// 已装配的测试应用 + 其临时目录（保留句柄防止目录提前清理）。
pub struct TestApp {
    pub router: Router,
    pub state: AppState,
    pub _dir: TempDir,
}

/// 用给定 account.csv 内容（不含表头）起应用；传 `""` 表示空账户表。
pub async fn app_with_accounts(csv_body: &str) -> TestApp {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("account.csv");
    let key = dir.path().join("account.key");
    if !csv_body.is_empty() {
        std::fs::write(&csv, format!("{ACCOUNT_HEADER}{csv_body}")).unwrap();
    }

    let clock: Arc<dyn store_core::Clock> = Arc::new(MockClock::new(1_700_000_000_000));
    let init = Initialized::bootstrap(InitParams {
        data_dir: dir.path().to_str().unwrap(),
        clock: Arc::clone(&clock),
        flush_interval: None,
        account_file: csv,
        account_key_file: key,
    })
    .await
    .unwrap();

    let accounts = AccountHub::new(init.accounts);
    let endpoints: VenueEndpoints = Arc::new(Vec::new());

    let state = AppState::assemble(AssemblyDeps {
        store: init.store,
        accounts,
        // 测试用 dev 旁路鉴权；REST 不经过南向鉴权中间件。
        south_auth: dev_south_auth(Arc::clone(&clock)),
        rest_timeout: Duration::from_secs(5),
        rule_refresh_interval: Duration::from_secs(3600),
        position_debounce: Duration::from_millis(100),
        position_reconcile_interval: Duration::from_secs(3600),
        endpoints,
        // 测试默认不配置任何通知通道（五 URL 全空，静默跳过）。
        channels: crate::notify::ChannelUrls::new(
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        ),
        venue: crate::venue_config::default_configs(),
    });

    let router = super::router().with_state(state.clone());
    TestApp {
        router,
        state,
        _dir: dir,
    }
}

/// 构造测试用南向鉴权（dev 旁路）。
fn dev_south_auth(clock: Arc<dyn store_core::Clock>) -> crate::south_auth::SouthAuth {
    let nonces = crate::south_auth::NonceGuard::new(clock, Duration::from_secs(60));
    crate::south_auth::SouthAuth::from_secret("test-secret", nonces, true)
}

/// 在随机本地端口起南向 WS 服务（dev 旁路鉴权），返回可连接的 `/ws` 地址。
/// 服务跑在后台 task，随测试结束自然终止。
pub async fn serve_south_ws(state: &crate::state::AppState) -> String {
    use axum::routing::get;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new()
        .route("/ws", get(crate::south::ws_handler))
        .with_state(state.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("ws://{addr}/ws")
}
