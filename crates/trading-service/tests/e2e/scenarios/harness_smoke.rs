//! harness 自测：装配模拟所、经真实注册表下单、账户流驱动仓位对账落库，
//! 以及 testkit 最小暴露面的编译期契约。

use tower::ServiceExt;
use trading_models::{OrderStatus, OrderType, Side};

use crate::harness as common;

#[tokio::test]
async fn harness_routes_market_order_and_reconciles_position() {
    // Arrange：临时目录 + 模拟所 + 生产装配路径。
    let app = common::start_sim().await;
    common::set_mark(&app, "100");

    // 生产 REST 路由器已装配（/health 存活探针）。
    let health = app
        .router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri("/health")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), axum::http::StatusCode::OK);

    // Act：走宿主 connect 路径下的市价开仓。
    let api = common::sim_api(&app);
    let req = common::order_request(
        "0xharness0000000000000000000000001",
        Side::Buy,
        OrderType::Market,
        None,
        common::dec("1"),
        false,
    );
    let ack = api.place_order(&req).await.unwrap();
    assert!(ack.accepted);

    // Assert：市价单在受理时同步成交；随后由账户事件去抖 → REST 快照 → 落库。
    let sim_order = app
        .ledger
        .order("0xharness0000000000000000000000001")
        .expect("模拟所已受理并成交");
    assert_eq!(sim_order.status, OrderStatus::Filled);
    app.wait_for("宿主仓位落库", |a| a.positions().len() == 1)
        .await;

    let p = &app.positions()[0];
    assert_eq!(p.exchange, common::SIM_EXCHANGE);
    assert_eq!(p.instrument, common::DEFAULT_INSTRUMENT);
    assert_eq!(p.quantity, common::dec("1"));
}

/// harness 播种能力：下单规则（tick/step/min-notional）与直接仓位。
#[tokio::test]
async fn harness_seeds_order_rule_and_position() {
    // 离线装配：不启动账户流，避免对账把 seed 仓位覆盖掉。
    let dir = tempfile::TempDir::new().unwrap();
    let app = common::start_sim_at(dir.path()).await;

    let rule = app.seed_order_rule();
    let key = trading_models::OrderRuleKey::new(
        common::SIM_EXCHANGE,
        trading_models::InstrumentType::Futures,
        common::DEFAULT_INSTRUMENT,
    );
    assert!(app.state.store.order_rules().get(&key).is_some());
    assert_eq!(rule.price_tick, Some(common::dec("0.1")));

    let seeded = app
        .seed_position(
            common::DEFAULT_INSTRUMENT,
            trading_models::Side::Buy,
            common::dec("2"),
            common::dec("100"),
        )
        .expect("seed position");
    assert!(seeded.id > 0);
    assert_eq!(app.positions().len(), 1);
    assert_eq!(seeded.quantity, common::dec("2"));
    assert_eq!(seeded.pos_price, common::dec("100"));
}

/// `testkit` 最小暴露面能被外部集成测试引用（编译期契约，不启动任何连接）。
#[test]
fn testkit_surface_is_importable() {
    use trading_service::testkit::{
        router, AccountHub, AppState, AssemblyDeps, RegistryFactory, VenueEndpoints,
    };

    // 逐个引用，确保它们确为 pub 且路径稳定；同时避免 unused import 警告。
    let _ = std::any::type_name::<AppState>();
    let _ = std::any::type_name::<AssemblyDeps>();
    let _ = std::any::type_name::<VenueEndpoints>();
    let _ = std::any::type_name::<RegistryFactory>();
    let _ = std::any::type_name::<AccountHub>();
    let _ = router;
}
