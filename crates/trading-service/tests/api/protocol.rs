//! 协议层契约（自 `tests/rest_api.rs` 迁移，断言保持不变）。
//!
//! 只钉协议层：`/health` 裸报文、统一 `code/message/data` 信封、参数校验、
//! 分页缺省、未知交易所 40002、前置拒绝 40001、查无此行 data=null。

use axum::http::StatusCode;

use trading_service::testkit::ACCOUNT_HEADER;

use crate::support::{delete_json, get_json, no_exchange_app, post_json};

/// `/health` 是裸报文，不套业务信封。
#[tokio::test]
async fn health_is_bare_healthy_200() {
    let app = no_exchange_app().await;

    let (status, v) = get_json(&app.router, "/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(v, serde_json::json!({"status": "healthy"}));
    assert!(v.get("code").is_none(), "健康探针不走信封");
}

/// 列表接口成功信封：`code=0` + `{list,total,page,page_size}` 缺省分页。
#[tokio::test]
async fn list_orders_empty_uses_success_envelope_and_default_paging() {
    let app = no_exchange_app().await;

    let (status, v) = get_json(&app.router, "/api/v1/orders").await;

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
    let app = no_exchange_app().await;

    let (status, v) = get_json(&app.router, "/api/v1/orders?page=0").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(v["code"], 40001);
    assert!(v["data"].is_null(), "错误信封 data 恒为 null");
}

/// 坏 JSON 统一收敛为 40001（不暴露 axum 的 422）。
#[tokio::test]
async fn bad_json_body_returns_40001() {
    let app = no_exchange_app().await;

    let (status, v) = post_json(&app.router, "/api/v1/orders", "{not-json}").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(v["code"], 40001);
    assert!(v["message"].as_str().unwrap().contains("JSON"));
}

/// 未知交易所 / 无精确匹配账户 → 40002。
#[tokio::test]
async fn unknown_exchange_returns_account_not_configured_40002() {
    let app = no_exchange_app().await;

    let (status, v) = post_json(
        &app.router,
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
    let app = no_exchange_app().await;

    let (status, v) = post_json(
        &app.router,
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
    let app = no_exchange_app().await;

    let (status, v) = post_json(
        &app.router,
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
    let app = no_exchange_app().await;

    let (status, v) = delete_json(&app.router, "/api/v1/orders").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(v["code"], 40001);
}

/// 单条持仓查无此行：仍 200/code=0，data=null（与列表语义区分）。
#[tokio::test]
async fn get_unknown_position_returns_null_data() {
    let app = no_exchange_app().await;

    let (status, v) = get_json(&app.router, "/api/v1/positions/999999").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["code"], 0);
    assert!(v["data"].is_null());
}

/// 空账户表下账户名无法解析 → 40002（account.csv 是唯一账户真源）。
#[tokio::test]
async fn list_orders_for_unconfigured_account_returns_40002() {
    let app = crate::support::app_with_csv("").await;

    let (status, v) = get_json(&app.router, "/api/v1/orders?account=ghost&exchange=binance").await;

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
