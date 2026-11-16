//! crate 内单测支撑：委托 `crate::testkit` 的轻量装配，并额外持有临时目录句柄。
//!
//! 仅编译进测试。装配逻辑统一收口在 [`crate::testkit::app_with_accounts`]，
//! 集成测试（`tests/`）与 crate 内单测走同一路径，避免两套装配漂移。

use axum::Router;
use tempfile::TempDir;

use crate::state::AppState;

/// account.csv 表头（与生产一致：新 7 列，6 公共列 + credentials）。
pub const ACCOUNT_HEADER: &str = crate::testkit::ACCOUNT_HEADER;

/// 已装配的测试应用 + 其临时目录（保留句柄防止目录提前清理）。
pub struct TestApp {
    pub router: Router,
    pub state: AppState,
    pub _dir: TempDir,
}

/// 用给定 account.csv 内容（不含表头）起应用；传 `""` 表示空账户表。
pub async fn app_with_accounts(csv_body: &str) -> TestApp {
    let dir = TempDir::new().unwrap();
    let lite = crate::testkit::app_with_accounts(dir.path(), csv_body).await;
    TestApp {
        router: lite.router,
        state: lite.state,
        _dir: dir,
    }
}

/// 在随机本地端口起南向 WS 服务，返回可连接的 `/ws` 地址。
/// 服务跑在后台 task，随测试结束自然终止。
pub async fn serve_south_ws(state: &AppState) -> String {
    crate::testkit::serve_south_ws(state).await
}
