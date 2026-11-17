//! 订单接口：`GET/POST/DELETE /api/v1/orders`。
//!
//! 查询类用例直接种表（确定性）；下单/撤单正常路径用模拟所（真实 Router +
//! 后台受理任务 + SimLedger 台账）。

use axum::http::StatusCode;
use serde_json::json;

use trading_models::{Order, OrderStatus, Side};

use crate::support::{
    app_with_csv, delete_json, get_json, post_value, sim_app, wait_for, TestApi,
    DEFAULT_INSTRUMENT, SIM_ACCOUNT, SIM_EXCHANGE,
};

/// 空账户应用 + 3 条订单（id 递增；第 3 条不同标的/状态）。
async fn seeded_orders() -> TestApi {
    let app = app_with_csv("").await;
    let orders = app.state.store.orders().expect("order repo");
    for (i, (cid, instrument, status)) in [
        (
            "0x0123456789abcdef0123456789abcdef",
            "BTCUSDT",
            OrderStatus::New,
        ),
        (
            "0x1123456789abcdef0123456789abcdef",
            "BTCUSDT",
            OrderStatus::Placing,
        ),
        (
            "0x2123456789abcdef0123456789abcdef",
            "ETHUSDT",
            OrderStatus::Filled,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let mut o = Order::new(1, cid, "binance", instrument, Side::Buy);
        o.instrument_type = trading_models::InstrumentType::Futures;
        o.status = status;
        o.created_at = 1_700_000_000_000 + i as u64;
        orders.insert(o).expect("insert order");
    }
    app
}

/// 分页：按 id 倒序、total 为过滤后总数。
#[tokio::test]
async fn orders_list_paginates_by_id_desc() {
    let app = seeded_orders().await;

    let (status, v) = get_json(&app.router, "/api/v1/orders?account_id=1&page_size=2").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["code"], 0);
    let d = &v["data"];
    assert_eq!(d["total"], 3);
    assert_eq!(d["page"], 1);
    assert_eq!(d["page_size"], 2);
    let list = d["list"].as_array().unwrap();
    assert_eq!(list.len(), 2);
    assert!(list[0]["id"].as_u64().unwrap() > list[1]["id"].as_u64().unwrap());
}

/// 标的 + 状态过滤（`status[]=` 重复参数）。
#[tokio::test]
async fn orders_list_filters_by_instrument_and_status() {
    let app = seeded_orders().await;

    let (status, v) = get_json(
        &app.router,
        "/api/v1/orders?account_id=1&instrument=ETHUSDT&status[]=FILLED",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["data"]["total"], 1);
    assert_eq!(v["data"]["list"][0]["instrument"], "ETHUSDT");
    assert_eq!(v["data"]["list"][0]["status"], "FILLED");

    // 过滤不匹配 → 空列表（不是错误）。
    let (_, v) = get_json(
        &app.router,
        "/api/v1/orders?account_id=1&instrument=ETHUSDT&status[]=NEW",
    )
    .await;
    assert_eq!(v["data"]["total"], 0);
    assert!(v["data"]["list"].as_array().unwrap().is_empty());
}

/// 市价下单：立即返回 PLACING DTO；后台受理后收敛 NEW，模拟所台账即时成交。
#[tokio::test]
async fn orders_post_market_fills_and_returns_placing_dto() {
    // Arrange
    let sim = sim_app().await;
    let api = &sim.api;

    // Act
    let (status, v) = post_value(
        &api.router,
        "/api/v1/orders",
        &json!({
            "exchange": SIM_EXCHANGE,
            "account": SIM_ACCOUNT,
            "instrument": DEFAULT_INSTRUMENT,
            "instrument_type": "futures",
            "side": "buy",
            "order_type": "market",
            "quantity": "0.1",
        }),
    )
    .await;

    // Assert：同步返回 PLACING + 服务端 cid。
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["code"], 0);
    assert_eq!(v["data"]["status"], "PLACING");
    assert_eq!(v["data"]["exchange"], SIM_EXCHANGE);
    assert_eq!(v["data"]["instrument"], DEFAULT_INSTRUMENT);
    let cid = v["data"]["client_order_id"].as_str().unwrap().to_string();
    assert!(cid.starts_with("0x") && cid.len() == 34);

    // 后台任务：宿主单受理为 NEW（模拟所台账已成交）。
    let state = api.state.clone();
    let wait_cid = cid.clone();
    wait_for("宿主订单受理为 NEW", move || {
        state
            .store
            .order_repo()
            .ok()
            .and_then(|r| r.peek_by_cid(&wait_cid))
            .is_some_and(|o| o.status == OrderStatus::New)
    })
    .await;
    let sim_order = sim.ledger.order(&cid).expect("模拟所台账应存在该单");
    assert_eq!(sim_order.status, OrderStatus::Filled);
}

/// 限价挂单后撤单：DELETE 回显 cid，宿主单收敛 CANCELED。
#[tokio::test]
async fn orders_delete_cancels_existing() {
    let sim = sim_app().await;
    let api = &sim.api;

    // Arrange：挂一笔低于标记价的限价买单（模拟所不成交，停在 NEW）。
    let (_, created) = post_value(
        &api.router,
        "/api/v1/orders",
        &json!({
            "exchange": SIM_EXCHANGE,
            "account": SIM_ACCOUNT,
            "instrument": DEFAULT_INSTRUMENT,
            "instrument_type": "futures",
            "side": "buy",
            "order_type": "limit",
            "price": "90",
            "quantity": "0.1",
        }),
    )
    .await;
    let cid = created["data"]["client_order_id"]
        .as_str()
        .unwrap()
        .to_string();
    let state = api.state.clone();
    let wait_cid = cid.clone();
    wait_for("宿主限价单受理为 NEW", move || {
        state
            .store
            .order_repo()
            .ok()
            .and_then(|r| r.peek_by_cid(&wait_cid))
            .is_some_and(|o| o.status == OrderStatus::New)
    })
    .await;

    // Act
    let (status, v) = delete_json(
        &api.router,
        &format!(
            "/api/v1/orders?client_order_id={cid}&account={SIM_ACCOUNT}&exchange={SIM_EXCHANGE}"
        ),
    )
    .await;

    // Assert：200 + cid 回显；后台撤单收敛 CANCELED。
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["code"], 0);
    assert_eq!(v["data"]["client_order_id"], cid);
    let state = api.state.clone();
    let wait_cid = cid.clone();
    wait_for("宿主订单撤销收敛 CANCELED", move || {
        state
            .store
            .order_repo()
            .ok()
            .and_then(|r| r.peek_by_cid(&wait_cid))
            .is_some_and(|o| o.status == OrderStatus::Canceled)
    })
    .await;
}

/// 内存查无此单：200 / code=0 / data=null（非错误语义）。
#[tokio::test]
async fn orders_delete_unknown_cid_returns_null_data() {
    let app = app_with_csv("").await;

    let (status, v) = delete_json(
        &app.router,
        "/api/v1/orders?client_order_id=0x0123456789abcdef0123456789abcdef",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["code"], 0);
    assert!(v["data"].is_null());
}
