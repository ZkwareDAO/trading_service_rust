//! 持仓接口：`GET /api/v1/positions` 与 `GET /api/v1/positions/{id}`。

use axum::http::StatusCode;

use trading_models::{Position, Side};

use crate::support::{app_with_csv, get_json, TestApi};

/// 空账户应用 + 2 条活跃持仓（BTC 多、ETH 空）。
async fn seeded_positions() -> TestApi {
    let app = app_with_csv("").await;
    let positions = app.state.store.positions().expect("position repo");
    positions
        .insert(Position::new(1, "binance", "BTCUSDT", Side::Buy))
        .expect("insert btc position");
    positions
        .insert(Position::new(1, "binance", "ETHUSDT", Side::Sell))
        .expect("insert eth position");
    app
}

/// 列表返回活跃持仓行（多空各自独立成行），不暴露内部字段。
#[tokio::test]
async fn positions_list_returns_active_rows() {
    let app = seeded_positions().await;

    let (status, v) = get_json(&app.router, "/api/v1/positions?account_id=1").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["code"], 0);
    assert_eq!(v["data"]["total"], 2);
    let list = v["data"]["list"].as_array().unwrap();
    let instruments: Vec<_> = list
        .iter()
        .map(|p| p["instrument"].as_str().unwrap())
        .collect();
    assert!(instruments.contains(&"BTCUSDT"));
    assert!(instruments.contains(&"ETHUSDT"));
    assert!(list[0].get("qty_unit").is_none(), "不暴露 qty_unit");
}

/// 单条持仓按 id 返回 PositionDTO。
#[tokio::test]
async fn position_get_by_id_returns_dto() {
    let app = seeded_positions().await;
    let id = app.state.store.position_repo().unwrap().list_active()[0].id;

    let (status, v) = get_json(&app.router, &format!("/api/v1/positions/{id}")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["code"], 0);
    assert_eq!(v["data"]["id"], id);
    assert_eq!(v["data"]["account_id"], 1);
}

/// 未知 id：200 / code=0 / data=null（与列表空集语义区分）。
#[tokio::test]
async fn position_get_unknown_returns_null() {
    let app = app_with_csv("").await;

    let (status, v) = get_json(&app.router, "/api/v1/positions/999999").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["code"], 0);
    assert!(v["data"].is_null());
}
