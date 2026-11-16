//! O-03–O-06：条件开仓（本标的价格 / 跨标的价格 / ROI / 关联仓位）。

use rust_decimal::Decimal;
use trading_models::{
    InstrumentType, OrderType, Side, WatchConditionKind, WatchRule, WatchRuleStatus,
};

use e2e_test::harness::TestApp;
use e2e_test::harness::{self as common, dec, DEFAULT_INSTRUMENT};

/// `new` 开仓规则要素（用具名字段替代 9 个位置参数）。
struct OpenRuleSpec {
    position_id: u64,
    condition: WatchConditionKind,
    condition_instrument: Option<&'static str>,
    operator: &'static str,
    value: &'static str,
    order_type: OrderType,
    price: Decimal,
    quantity: &'static str,
}

impl OpenRuleSpec {
    /// 市价开 1 手（多数用例的默认口径）。
    fn market(
        position_id: u64,
        condition: WatchConditionKind,
        condition_instrument: Option<&'static str>,
        operator: &'static str,
        value: &'static str,
    ) -> Self {
        Self {
            position_id,
            condition,
            condition_instrument,
            operator,
            value,
            order_type: OrderType::Market,
            price: Decimal::ZERO,
            quantity: "1",
        }
    }
}

/// 构造一条 `new` 开仓规则（未落库；由 `TestApp::insert_rule` 分配 id）。
fn open_rule(app: &TestApp, spec: OpenRuleSpec) -> WatchRule {
    let mut rule = WatchRule::new_open(
        app.account_id(),
        spec.position_id,
        common::SIM_EXCHANGE,
        DEFAULT_INSTRUMENT,
        InstrumentType::Futures,
        Side::Buy,
        spec.order_type,
        spec.price,
        dec(spec.quantity),
        1,
    );
    rule.condition_kind = spec.condition;
    rule.condition_instrument = spec.condition_instrument.map(str::to_string);
    rule.operator = spec.operator.to_string();
    rule.value = dec(spec.value);
    rule
}

fn sim_orders_for_rule(app: &TestApp, rule_id: u64) -> Vec<trading_models::Order> {
    app.state
        .store
        .order_repo()
        .expect("order repo")
        .list_active()
        .into_iter()
        .filter(|o| o.rule_id == rule_id)
        .collect()
}

#[tokio::test]
async fn o03_conditional_open_on_own_symbol_price() {
    // Arrange：价格条件 <=100；初始 mark=110 不命中。
    let app = common::start_sim().await;
    let rule = app.insert_rule(open_rule(
        &app,
        OpenRuleSpec::market(
            0,
            WatchConditionKind::Price,
            Some(DEFAULT_INSTRUMENT),
            "<=",
            "100",
        ),
    ));
    common::set_mark(&app, "110");
    app.scan().await;

    // Assert：未命中不下单，规则仍 active。
    assert!(app.ledger.orders().is_empty(), "条件未命中不得下单");
    assert_eq!(
        app.rule(rule.id).unwrap().status,
        WatchRuleStatus::Active,
        "未命中规则必须保持 active"
    );

    // Act：mark 跌到 99。
    common::set_mark(&app, "99");
    app.scan().await;

    // Assert：命中开仓成交，规则退休。
    app.wait_for("条件开仓仓位落库", |a| a.positions().len() == 1)
        .await;
    app.wait_for("规则退休", |a| {
        a.rule(rule.id).unwrap().status == WatchRuleStatus::Retired
    })
    .await;
    let position = &app.positions()[0];
    assert_eq!(position.quantity, dec("1"));
    assert_eq!(position.pos_price, dec("99"));
}

#[tokio::test]
async fn o04_conditional_open_on_cross_symbol_price_with_limit() {
    // Arrange：条件为 ETHUSDT 价格 <=3000；动作是 BTCUSDT 限价 90 开仓。
    let app = common::start_sim().await;
    common::set_mark(&app, "100");
    app.sim.set_mark("ETHUSDT", dec("3100"));
    let rule = app.insert_rule(open_rule(
        &app,
        OpenRuleSpec {
            order_type: OrderType::Limit,
            price: dec("90"),
            ..OpenRuleSpec::market(0, WatchConditionKind::Price, Some("ETHUSDT"), "<=", "3000")
        },
    ));

    // Act：ETH 未穿越 → 不触发。
    app.scan().await;
    assert!(app.ledger.orders().is_empty());

    // Act：ETH 跌到 2900 → 触发限价开仓。
    app.sim.set_mark("ETHUSDT", dec("2900"));
    app.scan().await;

    // Assert：系统规则单为限价、价量正确；模拟所侧挂单同要素。
    let orders = sim_orders_for_rule(&app, rule.id);
    assert_eq!(orders.len(), 1, "条件命中应只下一单");
    let order = &orders[0];
    assert_eq!(order.order_type, OrderType::Limit);
    assert_eq!(order.order_price, dec("90"));
    assert_eq!(order.order_quantity, dec("1"));
    assert_eq!(order.side, Side::Buy);

    let sim_order = app
        .ledger
        .orders()
        .into_iter()
        .find(|o| o.client_order_id == order.client_order_id)
        .expect("模拟所已受理规则限价单");
    assert_eq!(sim_order.price, Some(dec("90")));

    // Act：显式成交后建仓、规则退休。
    assert!(app.ledger.fill_order(&order.client_order_id));
    app.wait_for("限价条件单成交建仓", |a| a.positions().len() == 1)
        .await;
    app.wait_for("规则退休", |a| {
        a.rule(rule.id).unwrap().status == WatchRuleStatus::Retired
    })
    .await;
    let position = &app.positions()[0];
    assert_eq!(position.quantity, dec("1"));
    assert_eq!(position.pos_price, dec("90"));
}

#[tokio::test]
async fn o05_conditional_open_on_bound_position_roi() {
    // Arrange：先建 2 手多仓（成本 100、杠杆 10 → 保证金 20）。
    let app = common::start_sim().await;
    common::set_mark(&app, "100");
    app.place_market(DEFAULT_INSTRUMENT, "buy", "2", false)
        .await;
    app.wait_for("底仓落库", |a| a.positions().len() == 1).await;
    let position = app.positions()[0].clone();
    assert_eq!(position.quantity, dec("2"));

    // ROI 条件 >=0.5（保证金口径），绑定该仓位。
    let rule = app.insert_rule(open_rule(
        &app,
        OpenRuleSpec::market(position.id, WatchConditionKind::Roi, None, ">=", "0.5"),
    ));

    // Act：mark=104 → roi=(104-100)*2/20=0.4，不命中。
    common::set_mark(&app, "104");
    app.scan().await;
    assert_eq!(app.ledger.orders().len(), 1, "ROI 未达阈值不得开仓");

    // Act：mark=105 → roi=0.5，命中。
    common::set_mark(&app, "105");
    app.scan().await;

    // Assert：加仓 1 手，总仓 3；规则退休。
    app.wait_for("ROI 条件加仓落库", |a| {
        a.positions()
            .first()
            .is_some_and(|p| p.quantity == dec("3"))
    })
    .await;
    app.wait_for("规则退休", |a| {
        a.rule(rule.id).unwrap().status == WatchRuleStatus::Retired
    })
    .await;
    assert_eq!(
        sim_orders_for_rule(&app, rule.id).len(),
        0,
        "ROI 命中单已成交并从活动表移出"
    );
    assert_eq!(
        app.ledger.position(DEFAULT_INSTRUMENT).map(|(q, _)| q),
        Some(dec("3"))
    );
    app.assert_position_matches_ledger(DEFAULT_INSTRUMENT);
}

#[tokio::test]
async fn o06_conditional_open_waits_for_related_position_flat() {
    // Arrange：两个标的各 1 手多仓。
    let app = common::start_sim().await;
    common::set_mark(&app, "100");
    app.sim.set_mark("ETHUSDT", dec("3000"));
    app.place_market(DEFAULT_INSTRUMENT, "buy", "1", false)
        .await;
    app.place_market("ETHUSDT", "buy", "1", false).await;
    app.wait_for("两个底仓落库", |a| a.positions().len() == 2)
        .await;

    // 条件 position_ETHUSDT == 0 命中后在 BTCUSDT 市价加 1 手。
    let rule = app.insert_rule(open_rule(
        &app,
        OpenRuleSpec::market(0, WatchConditionKind::Position, Some("ETHUSDT"), "==", "0"),
    ));

    // Act：ETH 仍有仓 → 不触发。
    app.scan().await;
    assert_eq!(app.ledger.orders().len(), 2, "关联仓位未平不得开仓");

    // Act：平掉 ETH，等待宿主仓位收敛。
    app.place_market("ETHUSDT", "sell", "1", true).await;
    app.wait_for("ETH 仓位关闭", |a| {
        !a.positions().iter().any(|p| p.instrument == "ETHUSDT")
    })
    .await;

    // Act：再扫一轮 → 触发。
    app.scan().await;
    app.wait_for("关联条件加仓落库", |a| {
        a.positions()
            .iter()
            .find(|p| p.instrument == DEFAULT_INSTRUMENT)
            .is_some_and(|p| p.quantity == dec("2"))
    })
    .await;
    app.wait_for("规则退休", |a| {
        a.rule(rule.id).unwrap().status == WatchRuleStatus::Retired
    })
    .await;

    assert_eq!(
        app.ledger.position(DEFAULT_INSTRUMENT).map(|(q, _)| q),
        Some(dec("2"))
    );
    app.assert_position_matches_ledger(DEFAULT_INSTRUMENT);
}
