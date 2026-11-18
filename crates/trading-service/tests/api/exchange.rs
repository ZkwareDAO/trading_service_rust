//! 交易所实时查询：`POST /api/v1/exchange/positions` 与 `.../balance`。
//!
//! 正常路径经模拟所台账（`SimApi` 实现 `get_positions`/`get_balance`）；
//! 错误路径用无账户/坏 JSON 钉 40002/40001。

use axum::http::StatusCode;
use serde_json::json;

use trading_exchange::api::{OrderPositionSide, PlaceOrderRequest};
use trading_models::{InstrumentType, OrderType, Side};

use crate::support::{
    app_with_csv, dec, post_json, post_value, sim_app, DEFAULT_INSTRUMENT, SIM_ACCOUNT,
    SIM_EXCHANGE,
};

/// 在模拟所台账放一笔市价开仓，后续查仓即可看到该持仓。
fn place_sim_position(sim: &crate::support::SimApi) {
    let req = PlaceOrderRequest {
        instrument: DEFAULT_INSTRUMENT.into(),
        instrument_type: InstrumentType::Futures,
        side: Side::Buy,
        order_type: OrderType::Market,
        price: None,
        quantity: dec("0.5"),
        reduce_only: false,
        position_side: OrderPositionSide::Both,
        time_in_force: None,
        client_order_id: "0x0123456789abcdef0123456789abcdef".into(),
    };
    let ack = sim.ledger.place(SIM_ACCOUNT, &req).expect("模拟所下单");
    assert!(ack.accepted, "模拟所应受理");
}

/// 正常路径：返回模拟所持仓快照（不落库），并回显账户/交易所。
#[tokio::test]
async fn exchange_positions_returns_sim_snapshot() {
    let sim = sim_app().await;
    place_sim_position(&sim);

    let (status, v) = post_value(
        &sim.api.router,
        "/api/v1/exchange/positions",
        &json!({ "exchange": SIM_EXCHANGE, "account": SIM_ACCOUNT }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["code"], 0);
    assert_eq!(v["data"]["exchange"], SIM_EXCHANGE);
    assert_eq!(v["data"]["account"], SIM_ACCOUNT);
    let list = v["data"]["list"].as_array().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["instrument"], DEFAULT_INSTRUMENT);
    assert_eq!(list[0]["side"], "BUY");
    assert_eq!(list[0]["quantity"].as_f64(), Some(0.5));
}

/// 正常路径：按结算币种返回一行余额。
#[tokio::test]
async fn exchange_balance_returns_sim_balance() {
    let sim = sim_app().await;

    let (status, v) = post_value(
        &sim.api.router,
        "/api/v1/exchange/balance",
        &json!({ "exchange": SIM_EXCHANGE, "account": SIM_ACCOUNT }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["code"], 0);
    assert_eq!(v["data"]["exchange"], SIM_EXCHANGE);
    assert_eq!(v["data"]["account"], SIM_ACCOUNT);
    let balances = v["data"]["balances"].as_array().unwrap();
    assert_eq!(balances.len(), 1);
    assert_eq!(balances[0]["currency"], "USDT");
    assert_eq!(balances[0]["equity"].as_f64(), Some(100_000.0));
}

/// 账户名无法精确匹配 → 40002。
#[tokio::test]
async fn exchange_positions_unknown_account_returns_40002() {
    let app = app_with_csv("").await;

    let (status, v) = post_json(
        &app.router,
        "/api/v1/exchange/positions",
        r#"{"exchange":"binance","account":"ghost"}"#,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(v["code"], 40002);
    assert!(v["data"].is_null());
}

/// 坏 JSON 统一收敛 40001。
#[tokio::test]
async fn exchange_balance_bad_json_returns_40001() {
    let app = app_with_csv("").await;

    let (status, v) = post_json(&app.router, "/api/v1/exchange/balance", "{not-json}").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(v["code"], 40001);
}
