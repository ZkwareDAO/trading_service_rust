//! O-01 / O-02：直接市价开仓与限价开仓（经真实 REST 下单内核）。

use trading_models::{OrderStatus, OrderType};

use crate::harness::{self as common, dec, DEFAULT_INSTRUMENT};

#[tokio::test]
async fn o01_market_open_fills_and_creates_position() {
    // Arrange：模拟所报价 100，经 REST 提交市价买单。
    let app = common::start_sim().await;
    common::set_mark(&app, "100");

    // Act
    let cid = app
        .place_market(DEFAULT_INSTRUMENT, "buy", "2", false)
        .await;

    // Assert：系统订单 FILLED；系统仓位与模拟所台账逐字段一致。
    app.wait_for("系统订单 FILLED", |a| {
        a.system_order(&cid)
            .is_some_and(|o| o.status == OrderStatus::Filled)
    })
    .await;
    app.wait_for("系统仓位落库", |a| a.positions().len() == 1)
        .await;

    let position = &app.positions()[0];
    assert_eq!(position.exchange, common::SIM_EXCHANGE);
    assert_eq!(position.instrument, DEFAULT_INSTRUMENT);
    assert_eq!(position.quantity, dec("2"));
    assert_eq!(position.pos_price, dec("100"));
    assert_eq!(position.side, trading_models::Side::Buy);

    // 交易所侧独立台账：同量同价。
    assert_eq!(
        app.ledger.position(DEFAULT_INSTRUMENT),
        Some((dec("2"), dec("100")))
    );

    let order = app.system_order(&cid).unwrap();
    assert_eq!(order.order_type, OrderType::Market);
    assert_eq!(order.filled_quantity, dec("2"));
    assert!(!order.reduce_only);
    app.assert_position_matches_ledger(DEFAULT_INSTRUMENT);
}

#[tokio::test]
async fn o02_limit_open_stays_new_until_filled() {
    // Arrange：限价买单挂在 90（远离 mark=100），模拟所默认不自动成交。
    let app = common::start_sim().await;
    common::set_mark(&app, "100");

    // Act：提交限价单。
    let cid = app.place_limit(DEFAULT_INSTRUMENT, "buy", "90", "1").await;

    // Assert：系统订单 NEW、尚无仓位。
    app.wait_for("系统订单 NEW", |a| {
        a.system_order(&cid)
            .is_some_and(|o| o.status == OrderStatus::New)
    })
    .await;
    assert!(app.positions().is_empty());
    let sim_order = app.ledger.order(&cid).expect("模拟所已受理限价单");
    assert_eq!(sim_order.status, OrderStatus::New);
    assert_eq!(sim_order.price, Some(dec("90")));

    // Act：显式成交。
    assert!(app.ledger.fill_order(&cid));

    // Assert：NEW → FILLED；按委托价建仓。
    app.wait_for("系统订单 FILLED", |a| {
        a.system_order(&cid)
            .is_some_and(|o| o.status == OrderStatus::Filled)
    })
    .await;
    app.wait_for("系统仓位落库", |a| a.positions().len() == 1)
        .await;
    let position = &app.positions()[0];
    assert_eq!(position.quantity, dec("1"));
    assert_eq!(position.pos_price, dec("90"));
    assert_eq!(
        app.ledger.position(DEFAULT_INSTRUMENT),
        Some((dec("1"), dec("90")))
    );
    app.assert_position_matches_ledger(DEFAULT_INSTRUMENT);
}

/// 模拟所「应答不确定」分支：下单返回 `Err`，宿主应落 UNKNOWN（不得臆造拒单）。
#[tokio::test]
async fn indeterminate_place_marks_order_unknown() {
    // Arrange：让下一次下单模拟超时。
    let app = common::start_sim().await;
    common::set_mark(&app, "100");
    app.ledger.fail_next_place("模拟超时");

    // Act：经 REST 下单（服务同步返回 PLACING，实际下单在后台任务）。
    let cid = app
        .place_market(DEFAULT_INSTRUMENT, "buy", "1", false)
        .await;

    // Assert：宿主订单进入 UNKNOWN（非终态），交易所侧未受理。
    app.wait_for("订单进入 UNKNOWN", |a| {
        a.system_order(&cid)
            .is_some_and(|o| o.status == OrderStatus::Unknown)
    })
    .await;
    assert!(
        app.ledger.order(&cid).is_none(),
        "不确定分支不得在交易所侧建单"
    );
}
