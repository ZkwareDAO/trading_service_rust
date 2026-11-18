//! 模拟所后端：用同一套 `assert_*` 契约跑 [`SimApi`]，外加模拟所特有的
//! 故障注入用例（确定性拒单 / 应答不确定 / 程序化杠杆记账）。

use std::sync::Arc;

use trading_exchange::api::ExchangeApi;
use trading_models::OrderType;
use trading_sim::api::SimApi;
use trading_sim::ledger::SimLedger;
use trading_sim::{QueryMode, SimConfig};

use super::{dec, request, Backend, LookupFault, INSTRUMENT};

/// 构造一个全新模拟所后端（独立台账，互不串扰）。
fn sim() -> (Arc<SimLedger>, Backend) {
    let ledger = Arc::new(SimLedger::new(SimConfig::default()));
    ledger.set_mark(INSTRUMENT, dec("100"));
    let api: Arc<dyn ExchangeApi> = Arc::new(SimApi::new(Arc::clone(&ledger), "acct"));
    (
        ledger,
        Backend {
            name: "sim",
            api,
            mark: dec("100"),
            initial_equity: dec("100000"),
        },
    )
}

#[tokio::test]
async fn ack_is_normalized_to_new_with_exchange_order_id() {
    let (_ledger, backend) = sim();
    super::assert_ack_is_normalized(&backend).await;
}

#[tokio::test]
async fn duplicate_client_order_id_is_rejected() {
    let (_ledger, backend) = sim();
    super::assert_duplicate_cid_is_rejected(&backend).await;
}

#[tokio::test]
async fn market_fill_carries_signed_quantity_price_and_margin() {
    let (_ledger, backend) = sim();
    super::assert_market_position_fields(&backend).await;
}

#[tokio::test]
async fn balance_reports_equity_available_and_margin() {
    let (_ledger, backend) = sim();
    super::assert_balance_identity(&backend).await;
}

#[tokio::test]
async fn cancel_is_idempotent() {
    let (_ledger, backend) = sim();
    super::assert_cancel_is_idempotent(&backend).await;
}

#[tokio::test]
async fn query_order_distinguishes_found_not_exists_and_unavailable() {
    let (ledger, backend) = sim();
    super::assert_query_three_states(&backend, move |fault| {
        ledger.set_query_mode(match fault {
            LookupFault::NotExists => QueryMode::NotExists,
            LookupFault::Unavailable => QueryMode::Unavailable,
        });
    })
    .await;
}

/// 模拟所特有：确定性拒单返回 accepted=false + 原因（宿主据此拼 REJECTED 帧）。
#[tokio::test]
async fn deterministic_rejection_returns_accepted_false() {
    let (ledger, backend) = sim();
    ledger.reject_next("余额不足");
    let ack = backend
        .api
        .place_order(&request("c-r", OrderType::Market, None, dec("1")))
        .await
        .expect("拒单是业务应答，不是传输错");

    assert!(!ack.accepted);
    assert_eq!(ack.message.as_deref(), Some("余额不足"));
}

/// 模拟所特有：应答不确定（超时/断网）→ Err，宿主据此落 UNKNOWN。
#[tokio::test]
async fn indeterminate_place_returns_err() {
    let (ledger, backend) = sim();
    ledger.fail_next_place("timeout");
    assert!(backend
        .api
        .place_order(&request("c-i", OrderType::Market, None, dec("1")))
        .await
        .is_err());
}

/// 模拟所特有：程序化杠杆被台账记账（契约层无法读回，故在此断言）。
#[tokio::test]
async fn leverage_is_programmatic_and_recorded() {
    let (ledger, backend) = sim();
    assert!(backend.api.supports_programmatic_leverage());
    backend
        .api
        .set_leverage(INSTRUMENT, 5)
        .await
        .expect("设杠杆");
    assert_eq!(ledger.leverage(), 5);
}
