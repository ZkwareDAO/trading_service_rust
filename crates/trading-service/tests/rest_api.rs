//! REST 接口契约测试（crate 级，不启动模拟所/交易所）。
//!
//! 只钉协议层：`/health` 裸报文、统一 `code/message/data` 信封、参数校验、
//! 分页缺省、未知交易所 40002、前置拒绝 40001、查无此行 data=null。
//! 业务链路断言在 `e2e-test` 的 scenarios 中，二者有意分层。

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use tempfile::TempDir;
use tower::ServiceExt;

use trading_service::testkit::{app_with_accounts, AppLite, ACCOUNT_HEADER};

/// 一个已配置的 binance 账户行（合成凭证，不连网）。
const BINANCE_ACCOUNT: &str =
    "0,acct,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n";

/// 起带一个 binance 账户的应用；TempDir 需由调用方持有。
async fn app_with_binance() -> (TempDir, AppLite) {
    let dir = TempDir::new().expect("temp dir");
    let app = app_with_accounts(dir.path(), BINANCE_ACCOUNT).await;
    (dir, app)
}

/// 发请求，返回 (状态码, JSON 信封)。
async fn call(
    router: &Router,
    method: &str,
    uri: &str,
    body: &str,
) -> (StatusCode, serde_json::Value) {
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("build request"),
        )
        .await
        .expect("router call");
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body");
    if bytes.is_empty() {
        return (status, serde_json::Value::Null);
    }
    (status, serde_json::from_slice(&bytes).expect("json body"))
}

/// `/health` 是裸报文，不套业务信封。
#[tokio::test]
async fn health_is_bare_healthy_200() {
    let (_dir, app) = app_with_binance().await;

    let (status, v) = call(&app.router, "GET", "/health", "").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(v, serde_json::json!({"status": "healthy"}));
    assert!(v.get("code").is_none(), "健康探针不走信封");
}

/// 列表接口成功信封：`code=0` + `{list,total,page,page_size}` 缺省分页。
#[tokio::test]
async fn list_orders_empty_uses_success_envelope_and_default_paging() {
    let (_dir, app) = app_with_binance().await;

    let (status, v) = call(&app.router, "GET", "/api/v1/orders", "").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["code"], 0);
    assert_eq!(v["message"], "success");
    assert_eq!(v["data"]["total"], 0);
    assert_eq!(v["data"]["page"], 1);
    assert_eq!(v["data"]["page_size"], 50);
    assert!(v["data"]["list"].as_array().expect("list").is_empty());
}

/// page=0 是非法分页参数 → 40001。
#[tokio::test]
async fn page_zero_returns_bad_request_40001() {
    let (_dir, app) = app_with_binance().await;

    let (status, v) = call(&app.router, "GET", "/api/v1/orders?page=0", "").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(v["code"], 40001);
    assert!(v["data"].is_null(), "错误信封 data 恒为 null");
}

/// 坏 JSON 统一收敛为 40001（不暴露 axum 的 422）。
#[tokio::test]
async fn bad_json_body_returns_40001() {
    let (_dir, app) = app_with_binance().await;

    let (status, v) = call(&app.router, "POST", "/api/v1/orders", "{not-json}").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(v["code"], 40001);
    assert!(v["message"].as_str().unwrap().contains("JSON"));
}

/// 未知交易所 / 无精确匹配账户 → 40002。
#[tokio::test]
async fn unknown_exchange_returns_account_not_configured_40002() {
    let (_dir, app) = app_with_binance().await;

    let (status, v) = call(
        &app.router,
        "POST",
        "/api/v1/orders",
        r#"{"exchange":"ghost-venue","account":"acct","instrument":"BTCUSDT",
            "side":"buy","order_type":"market","quantity":1}"#,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(v["code"], 40002);
    assert!(v["data"].is_null());
}

/// 既无 quantity 也无 order_cash → 前置拒绝 40001（在触达交易所之前）。
#[tokio::test]
async fn order_without_quantity_or_cash_returns_40001() {
    let (_dir, app) = app_with_binance().await;

    let (status, v) = call(
        &app.router,
        "POST",
        "/api/v1/orders",
        r#"{"exchange":"binance","account":"acct","instrument":"BTCUSDT",
            "side":"buy","order_type":"market"}"#,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(v["code"], 40001);
}

/// 限价单 price 非正 → 前置拒绝 40001。
#[tokio::test]
async fn limit_order_with_zero_price_returns_40001() {
    let (_dir, app) = app_with_binance().await;

    let (status, v) = call(
        &app.router,
        "POST",
        "/api/v1/orders",
        r#"{"exchange":"binance","account":"acct","instrument":"BTCUSDT",
            "side":"buy","order_type":"limit","price":0,"quantity":1}"#,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(v["code"], 40001);
    assert!(v["message"].as_str().unwrap().contains("price"));
}

/// 撤单缺少 client_order_id → 40001。
#[tokio::test]
async fn cancel_without_client_order_id_returns_40001() {
    let (_dir, app) = app_with_binance().await;

    let (status, v) = call(&app.router, "DELETE", "/api/v1/orders", "").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(v["code"], 40001);
}

/// 单条持仓查无此行：仍 200/code=0，data=null（与列表语义区分）。
#[tokio::test]
async fn get_unknown_position_returns_null_data() {
    let (_dir, app) = app_with_binance().await;

    let (status, v) = call(&app.router, "GET", "/api/v1/positions/999999", "").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["code"], 0);
    assert!(v["data"].is_null());
}

/// 空账户表下账户名无法解析 → 40002（account.csv 是唯一账户真源）。
#[tokio::test]
async fn list_orders_for_unconfigured_account_returns_40002() {
    let dir = TempDir::new().expect("temp dir");
    let app = app_with_accounts(dir.path(), "").await;

    let (status, v) = call(
        &app.router,
        "GET",
        "/api/v1/orders?account=ghost&exchange=binance",
        "",
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(v["code"], 40002);
}

/// 表头常量与生产一致（防止 account.csv 列数漂移导致静默错读）。
#[test]
fn account_header_matches_production_columns() {
    assert_eq!(
        ACCOUNT_HEADER.trim_end(),
        "id,name,exchange,credentials,deleted,created_at,updated_at"
    );
}
