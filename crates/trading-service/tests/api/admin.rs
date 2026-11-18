//! 管理面端点：账户重载、K线补数校验、CTP 规格刷新。

use axum::http::StatusCode;
use serde_json::json;

use crate::support::{no_exchange_app, post_json, post_value};

/// `POST /admin/accounts/reload`：成功信封 + 账户行数。
#[tokio::test]
async fn admin_reload_accounts_returns_ok() {
    let app = no_exchange_app().await;

    let (status, v) = post_json(&app.router, "/admin/accounts/reload", "").await;

    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["success"], true);
    assert_eq!(v["accounts"], 1);
    assert!(v["error"].is_null());
}

/// `POST /admin/klines/backfill`：坏区间在触达交易所前回 40001。
#[tokio::test]
async fn admin_klines_backfill_rejects_invalid_range() {
    let app = no_exchange_app().await;

    let (status, v) = post_value(
        &app.router,
        "/admin/klines/backfill",
        &json!({
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "start_ms": 2_000,
            "end_ms": 1_000,
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(v["code"], 40001);
    assert!(v["message"].as_str().unwrap().contains("end_ms"));
}

/// `POST /admin/ctp/refresh-specs`：装配未启用 CTP provider 时回 503 + JSON。
#[cfg(feature = "ctp")]
#[tokio::test]
async fn admin_ctp_refresh_specs_returns_json() {
    // Arrange：注册表不含 CTP provider（ctp_provider() == None）。
    let app = crate::support::bare_app().await;

    // Act
    let (status, v) = post_json(&app.router, "/admin/ctp/refresh-specs", "").await;

    // Assert：确定性错误分支，不触网。
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(v["success"], false);
    assert!(v["error"].as_str().unwrap().contains("CTP"));
}
