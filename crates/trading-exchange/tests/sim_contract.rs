//! 模拟所 [`ExchangeApi`] 契约测试（M1a Task 1.4）。
//!
//! 钉住宿主编排依赖的交易所语义：受理 ACK 归一、确定性拒绝、查单三态、
//! 持仓 signed qty / 乘数 / 保证金、余额字段与程序化杠杆。

use std::str::FromStr;
use std::sync::Arc;

use rust_decimal::Decimal;
use trading_exchange::api::{
    ExchangeApi, OrderLookup, OrderPositionSide, OrderRef, PlaceOrderRequest,
};
use trading_models::{InstrumentType, OrderStatus, OrderType, Side};
use trading_sim::api::SimApi;
use trading_sim::ledger::SimLedger;
use trading_sim::{QueryMode, SimConfig};

fn dec(s: &str) -> Decimal {
    Decimal::from_str(s).unwrap()
}

fn req(cid: &str, order_type: OrderType, price: Option<&str>, quantity: &str) -> PlaceOrderRequest {
    PlaceOrderRequest {
        instrument: "BTCUSDT".into(),
        instrument_type: InstrumentType::Futures,
        side: Side::Buy,
        order_type,
        price: price.map(dec),
        quantity: dec(quantity),
        reduce_only: false,
        position_side: OrderPositionSide::Both,
        time_in_force: None,
        client_order_id: cid.into(),
    }
}

fn sim() -> (Arc<SimLedger>, SimApi) {
    let ledger = Arc::new(SimLedger::new(SimConfig::default()));
    ledger.set_mark("BTCUSDT", dec("100"));
    let api = SimApi::new(Arc::clone(&ledger), "acct");
    (ledger, api)
}

#[tokio::test]
async fn ack_is_normalized_to_new_with_exchange_order_id() {
    let (_ledger, api) = sim();
    let ack = api
        .place_order(&req("c1", OrderType::Limit, Some("90"), "1"))
        .await
        .unwrap();

    // 挂单受理：accepted + 状态统一归一为 NEW + 交易所单号非空。
    assert!(ack.accepted);
    assert_eq!(ack.status, OrderStatus::New);
    assert!(ack.exchange_order_id.is_some());
    assert_eq!(ack.client_order_id, "c1");
}

#[tokio::test]
async fn deterministic_rejection_returns_accepted_false() {
    let (ledger, api) = sim();
    ledger.reject_next("余额不足");
    let ack = api
        .place_order(&req("c1", OrderType::Market, None, "1"))
        .await
        .unwrap();

    assert!(!ack.accepted);
    assert_eq!(ack.message.as_deref(), Some("余额不足"));
}

#[tokio::test]
async fn indeterminate_place_returns_err() {
    // 模拟所应答不确定（超时/断网）→ 返回 Err，宿主据此落 UNKNOWN。
    let (ledger, api) = sim();
    ledger.fail_next_place("timeout");
    assert!(api
        .place_order(&req("c1", OrderType::Market, None, "1"))
        .await
        .is_err());
}

#[tokio::test]
async fn query_order_distinguishes_found_not_exists_and_unavailable() {
    let (ledger, api) = sim();
    api.place_order(&req("c1", OrderType::Limit, Some("90"), "1"))
        .await
        .unwrap();
    let r = OrderRef::new("c1");

    assert!(matches!(
        api.query_order("BTCUSDT", &r).await,
        OrderLookup::Found(_)
    ));
    ledger.set_query_mode(QueryMode::NotExists);
    assert!(matches!(
        api.query_order("BTCUSDT", &r).await,
        OrderLookup::NotExists
    ));
    ledger.set_query_mode(QueryMode::Unavailable);
    assert!(matches!(
        api.query_order("BTCUSDT", &r).await,
        OrderLookup::Unavailable
    ));
}

#[tokio::test]
async fn positions_carry_signed_quantity_and_margin() {
    let (_ledger, api) = sim();
    api.place_order(&req("c1", OrderType::Market, None, "2"))
        .await
        .unwrap();

    let positions = api.get_positions().await.unwrap();
    assert_eq!(positions.len(), 1);
    let p = &positions[0];
    assert_eq!(p.signed_quantity, dec("2"));
    assert_eq!(p.entry_price, dec("100"));
    assert_eq!(p.mark_price, Some(dec("100")));
    // 合约乘数：模拟所按单位交易，显式 None（宿主按 1 处理）。
    assert_eq!(p.multiplier, None);
    // 保证金 = 成本 / 杠杆（100×2/10 = 20）。
    assert_eq!(p.margin, Some(dec("20")));
}

#[tokio::test]
async fn balance_reports_equity_available_and_margin() {
    let (_ledger, api) = sim();
    api.place_order(&req("c1", OrderType::Market, None, "2"))
        .await
        .unwrap();

    let balance = api.get_balance().await.unwrap();
    assert_eq!(balance.len(), 1);
    let b = &balance[0];
    assert_eq!(b.currency, "USDT");
    // 初始 100000，占用保证金 20，无浮盈。
    assert_eq!(b.equity, dec("100000"));
    assert_eq!(b.margin_used, Some(dec("20")));
    assert_eq!(b.available, dec("99980"));
    assert_eq!(b.unrealized_pnl, Some(Decimal::ZERO));
}

#[tokio::test]
async fn leverage_is_programmatic_and_recorded() {
    let (ledger, api) = sim();
    assert!(api.supports_programmatic_leverage());
    api.set_leverage("BTCUSDT", 5).await.unwrap();
    assert_eq!(ledger.leverage(), 5);
}
