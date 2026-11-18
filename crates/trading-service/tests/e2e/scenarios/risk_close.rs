//! R-01–R-09：盯盘风控平仓矩阵（价格 / 关联仓位 / ROI / 回落止盈 / 限价 / 绝对量）。

use rust_decimal::Decimal;
use trading_models::{
    OrderType, Position, Side, WatchConditionKind, WatchRule, WatchRuleAction, WatchRuleStatus,
};
use trading_sim::ledger::SimOrder;

use crate::harness::{self as common, dec, DEFAULT_INSTRUMENT};

/// risk 规则要素（用具名字段替代 10 个位置参数）。
struct RiskRuleSpec {
    position_id: u64,
    condition: WatchConditionKind,
    condition_instrument: Option<&'static str>,
    operator: &'static str,
    value: &'static str,
    order_type: OrderType,
    price: Decimal,
    quantity: Decimal,
    quantity_pct: Decimal,
}

impl RiskRuleSpec {
    /// 市价全平（多数用例的默认口径）。
    fn full_market(
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
            quantity: Decimal::ZERO,
            quantity_pct: Decimal::ONE,
        }
    }
}

/// 构造一条 risk 规则（未落库；由 `TestApp::insert_rule` 分配 id）。
fn risk_rule(app: &common::TestApp, spec: RiskRuleSpec) -> WatchRule {
    let mut rule = WatchRule::new_risk(
        app.account_id(),
        spec.position_id,
        spec.condition,
        spec.condition_instrument.map(str::to_string),
        spec.operator,
        dec(spec.value),
        WatchRuleAction::UseSelf,
        spec.order_type,
        spec.price,
        spec.quantity,
        spec.quantity_pct,
    );
    rule.exchange = common::SIM_EXCHANGE.to_string();
    rule
}

/// 建一手多仓并返回宿主仓位行。
async fn open_long(app: &common::TestApp, mark: &str, quantity: &str) -> Position {
    common::set_mark(app, mark);
    app.place_market(DEFAULT_INSTRUMENT, "buy", quantity, false)
        .await;
    app.wait_for("底仓落库", |a| a.positions().len() == 1).await;
    app.positions()[0].clone()
}

fn reduce_only_orders(app: &common::TestApp) -> Vec<SimOrder> {
    app.ledger
        .orders()
        .into_iter()
        .filter(|o| o.reduce_only)
        .collect()
}

#[tokio::test]
async fn r01_price_condition_full_close_on_own_symbol() {
    // Arrange：2 手多仓，价格 <=95 全平。
    let app = common::start_sim().await;
    let position = open_long(&app, "100", "2").await;
    let rule = app.insert_rule(risk_rule(
        &app,
        RiskRuleSpec::full_market(position.id, WatchConditionKind::Price, None, "<=", "95"),
    ));

    // Act：96 不触发；95 触发全平。
    common::set_mark(&app, "96");
    app.scan().await;
    assert!(reduce_only_orders(&app).is_empty());
    common::set_mark(&app, "95");
    app.scan().await;

    // Assert：reduce_only、数量=持仓、仓位关闭、规则退休。
    app.wait_for("全平仓位关闭", |a| a.positions().is_empty())
        .await;
    app.wait_for("规则退休", |a| {
        a.rule(rule.id).unwrap().status == WatchRuleStatus::Retired
    })
    .await;
    let closes = reduce_only_orders(&app);
    assert_eq!(closes.len(), 1);
    assert_eq!(closes[0].side, Side::Sell);
    assert_eq!(closes[0].quantity, dec("2"));
    assert_eq!(closes[0].status, trading_models::OrderStatus::Filled);
    assert_eq!(app.ledger.position(DEFAULT_INSTRUMENT), None);
}

#[tokio::test]
async fn r02_cross_symbol_price_closes_only_bound_position() {
    // Arrange：BTC 2 手 + ETH 1 手；风控绑 BTC，条件 ETHUSDT<=2900。
    let app = common::start_sim().await;
    common::set_mark(&app, "100");
    app.sim.set_mark("ETHUSDT", dec("3000"));
    app.place_market(DEFAULT_INSTRUMENT, "buy", "2", false)
        .await;
    app.place_market("ETHUSDT", "buy", "1", false).await;
    app.wait_for("两仓落库", |a| a.positions().len() == 2).await;
    let btc_position = app
        .positions()
        .into_iter()
        .find(|p| p.instrument == DEFAULT_INSTRUMENT)
        .expect("BTC 仓位存在");

    let rule = app.insert_rule(risk_rule(
        &app,
        RiskRuleSpec::full_market(
            btc_position.id,
            WatchConditionKind::Price,
            Some("ETHUSDT"),
            "<=",
            "2900",
        ),
    ));

    // Act
    app.sim.set_mark("ETHUSDT", dec("2900"));
    app.scan().await;

    // Assert：只平 BTC，ETH 保留。
    app.wait_for("BTC 仓位关闭而 ETH 保留", |a| {
        let p = a.positions();
        p.len() == 1 && p[0].instrument == "ETHUSDT"
    })
    .await;
    app.wait_for("规则退休", |a| {
        a.rule(rule.id).unwrap().status == WatchRuleStatus::Retired
    })
    .await;
    assert_eq!(app.ledger.position(DEFAULT_INSTRUMENT), None);
    assert_eq!(
        app.ledger.position("ETHUSDT").map(|(q, _)| q),
        Some(dec("1"))
    );
}

#[tokio::test]
async fn r03_half_close_keeps_remaining_position() {
    // Arrange：2 手多仓，50% 平仓。
    let app = common::start_sim().await;
    let position = open_long(&app, "100", "2").await;
    let rule = app.insert_rule(risk_rule(
        &app,
        RiskRuleSpec {
            quantity_pct: dec("0.5"),
            ..RiskRuleSpec::full_market(position.id, WatchConditionKind::Price, None, "<=", "90")
        },
    ));

    // Act
    common::set_mark(&app, "90");
    app.scan().await;

    // Assert：下单量=持仓×0.5，剩余仓位保留。
    app.wait_for("半平后剩余 1 手", |a| {
        a.positions()
            .first()
            .is_some_and(|p| p.quantity == dec("1"))
    })
    .await;
    app.wait_for("规则退休", |a| {
        a.rule(rule.id).unwrap().status == WatchRuleStatus::Retired
    })
    .await;
    let closes = reduce_only_orders(&app);
    assert_eq!(closes.len(), 1);
    assert_eq!(closes[0].quantity, dec("1"));
    assert_eq!(
        app.ledger.position(DEFAULT_INSTRUMENT).map(|(q, _)| q),
        Some(dec("1"))
    );
    app.assert_position_matches_ledger(DEFAULT_INSTRUMENT);
}

#[tokio::test]
async fn r04_related_position_flat_triggers_close() {
    // Arrange：BTC 1 手 + ETH 1 手；风控绑 BTC，条件 position_ETHUSDT==0。
    let app = common::start_sim().await;
    common::set_mark(&app, "100");
    app.sim.set_mark("ETHUSDT", dec("3000"));
    app.place_market(DEFAULT_INSTRUMENT, "buy", "1", false)
        .await;
    app.place_market("ETHUSDT", "buy", "1", false).await;
    app.wait_for("两仓落库", |a| a.positions().len() == 2).await;
    let btc_position = app
        .positions()
        .into_iter()
        .find(|p| p.instrument == DEFAULT_INSTRUMENT)
        .expect("BTC 仓位存在");

    let rule = app.insert_rule(risk_rule(
        &app,
        RiskRuleSpec::full_market(
            btc_position.id,
            WatchConditionKind::Position,
            Some("ETHUSDT"),
            "==",
            "0",
        ),
    ));

    // Act：ETH 未平 → 不触发。
    app.scan().await;
    assert!(reduce_only_orders(&app).is_empty());

    // Act：平掉 ETH 后 → 触发。
    app.place_market("ETHUSDT", "sell", "1", true).await;
    app.wait_for("ETH 仓位关闭", |a| {
        !a.positions().iter().any(|p| p.instrument == "ETHUSDT")
    })
    .await;
    app.scan().await;

    // Assert：BTC 被平，规则退休。
    app.wait_for("BTC 仓位关闭", |a| a.positions().is_empty())
        .await;
    app.wait_for("规则退休", |a| {
        a.rule(rule.id).unwrap().status == WatchRuleStatus::Retired
    })
    .await;
    assert_eq!(app.ledger.position(DEFAULT_INSTRUMENT), None);
}

#[tokio::test]
async fn r05_roi_take_profit() {
    // Arrange：2 手多仓，保证金 20；ROI>=0.5 止盈。
    let app = common::start_sim().await;
    let position = open_long(&app, "100", "2").await;
    let rule = app.insert_rule(risk_rule(
        &app,
        RiskRuleSpec::full_market(position.id, WatchConditionKind::Roi, None, ">=", "0.5"),
    ));

    // Act：104（roi 0.4）不触发；105（roi 0.5）触发。
    common::set_mark(&app, "104");
    app.scan().await;
    assert!(reduce_only_orders(&app).is_empty());
    common::set_mark(&app, "105");
    app.scan().await;

    // Assert
    app.wait_for("止盈平仓", |a| a.positions().is_empty()).await;
    app.wait_for("规则退休", |a| {
        a.rule(rule.id).unwrap().status == WatchRuleStatus::Retired
    })
    .await;
    let closes = reduce_only_orders(&app);
    assert_eq!(closes.len(), 1);
    assert_eq!(closes[0].quantity, dec("2"));
}

#[tokio::test]
async fn r06_roi_stop_loss() {
    // Arrange：2 手多仓；ROI<=-0.3 止损。
    let app = common::start_sim().await;
    let position = open_long(&app, "100", "2").await;
    let rule = app.insert_rule(risk_rule(
        &app,
        RiskRuleSpec::full_market(position.id, WatchConditionKind::Roi, None, "<=", "-0.3"),
    ));

    // Act：98（roi -0.2）不触发；97（roi -0.3）触发。
    common::set_mark(&app, "98");
    app.scan().await;
    assert!(reduce_only_orders(&app).is_empty());
    common::set_mark(&app, "97");
    app.scan().await;

    // Assert
    app.wait_for("止损平仓", |a| a.positions().is_empty()).await;
    app.wait_for("规则退休", |a| {
        a.rule(rule.id).unwrap().status == WatchRuleStatus::Retired
    })
    .await;
    assert_eq!(reduce_only_orders(&app).len(), 1);
}

#[tokio::test]
async fn r07_profit_drawdown_take_profit() {
    // Arrange：2 手多仓，先创峰值 roi=1.0（mark=110）。
    let app = common::start_sim().await;
    let position = open_long(&app, "100", "2").await;
    common::set_mark(&app, "110");
    assert_eq!(app.positions()[0].max_roi, dec("1"));

    // 回落 >=0.15 触发。
    let rule = app.insert_rule(risk_rule(
        &app,
        RiskRuleSpec::full_market(
            position.id,
            WatchConditionKind::ProfitDrawdown,
            None,
            ">=",
            "0.15",
        ),
    ));

    // Act：mark=109 → roi 0.9、回落 0.1，未武装；mark=107 → 回落 0.3 触发。
    common::set_mark(&app, "109");
    app.scan().await;
    assert!(reduce_only_orders(&app).is_empty(), "回落未达阈值不得触发");
    common::set_mark(&app, "107");
    let before = app.positions()[0].clone();
    assert_eq!(before.max_roi, dec("1"));
    assert_eq!(before.profit_drawdown, dec("0.3"));
    app.assert_position_matches_ledger(DEFAULT_INSTRUMENT);
    app.scan().await;

    // Assert
    app.wait_for("回落止盈平仓", |a| a.positions().is_empty())
        .await;
    app.wait_for("规则退休", |a| {
        a.rule(rule.id).unwrap().status == WatchRuleStatus::Retired
    })
    .await;
    assert_eq!(reduce_only_orders(&app).len(), 1);
}

#[tokio::test]
async fn r08_limit_risk_close() {
    // Arrange：2 手多仓，限价 89 平仓（触发条件价格 <=90）。
    let app = common::start_sim().await;
    let position = open_long(&app, "100", "2").await;
    let rule = app.insert_rule(risk_rule(
        &app,
        RiskRuleSpec {
            order_type: OrderType::Limit,
            price: dec("89"),
            ..RiskRuleSpec::full_market(position.id, WatchConditionKind::Price, None, "<=", "90")
        },
    ));

    // Act
    common::set_mark(&app, "90");
    app.scan().await;

    // Assert：委托价/量正确，挂单未成交。
    app.wait_for("风控限价单挂出", |a| {
        a.state
            .store
            .order_repo()
            .map(|r| {
                r.list_active()
                    .into_iter()
                    .any(|o| o.rule_id == rule.id && o.order_type == OrderType::Limit)
            })
            .unwrap_or(false)
    })
    .await;
    let close_order = app
        .state
        .store
        .order_repo()
        .unwrap()
        .list_active()
        .into_iter()
        .find(|o| o.rule_id == rule.id)
        .unwrap();
    assert_eq!(close_order.order_price, dec("89"));
    assert_eq!(close_order.order_quantity, dec("2"));
    assert!(close_order.reduce_only);
    assert_eq!(close_order.side, Side::Sell);
    assert!(!app.positions().is_empty(), "限价单未成交前仓位保留");

    // Act：成交后仓位关闭、规则退休。
    assert!(app.ledger.fill_order(&close_order.client_order_id));
    app.wait_for("限价平仓成交", |a| a.positions().is_empty())
        .await;
    app.wait_for("规则退休", |a| {
        a.rule(rule.id).unwrap().status == WatchRuleStatus::Retired
    })
    .await;
}

#[tokio::test]
async fn r09_absolute_quantity_takes_precedence_over_pct() {
    // Arrange：2 手多仓；规则 quantity=1 且 pct=1，绝对量优先。
    let app = common::start_sim().await;
    let position = open_long(&app, "100", "2").await;
    let rule = app.insert_rule(risk_rule(
        &app,
        RiskRuleSpec {
            quantity: dec("1"),
            ..RiskRuleSpec::full_market(position.id, WatchConditionKind::Price, None, "<=", "90")
        },
    ));

    // Act
    common::set_mark(&app, "90");
    app.scan().await;

    // Assert：下单量=规则绝对量 1，剩余 1 手。
    app.wait_for("绝对量平仓后剩余 1 手", |a| {
        a.positions()
            .first()
            .is_some_and(|p| p.quantity == dec("1"))
    })
    .await;
    app.wait_for("规则退休", |a| {
        a.rule(rule.id).unwrap().status == WatchRuleStatus::Retired
    })
    .await;
    let closes = reduce_only_orders(&app);
    assert_eq!(closes.len(), 1);
    assert_eq!(closes[0].quantity, dec("1"));
    assert_eq!(
        app.ledger.position(DEFAULT_INSTRUMENT).map(|(q, _)| q),
        Some(dec("1"))
    );
    app.assert_position_matches_ledger(DEFAULT_INSTRUMENT);
}

#[tokio::test]
async fn r10_price_breakout_full_close() {
    // Arrange：2 手多仓，价格 >=105 全平。
    let app = common::start_sim().await;
    let position = open_long(&app, "100", "2").await;
    let rule = app.insert_rule(risk_rule(
        &app,
        RiskRuleSpec::full_market(position.id, WatchConditionKind::Price, None, ">=", "105"),
    ));

    // Act：104 不触发；105 触发全平。
    common::set_mark(&app, "104");
    app.scan().await;
    assert!(reduce_only_orders(&app).is_empty());
    common::set_mark(&app, "105");
    app.scan().await;

    // Assert：reduce_only、数量=持仓、仓位关闭、规则退休。
    app.wait_for("突破全平仓位关闭", |a| a.positions().is_empty())
        .await;
    app.wait_for("突破全平规则退休", |a| {
        a.rule(rule.id).unwrap().status == WatchRuleStatus::Retired
    })
    .await;
    let closes = reduce_only_orders(&app);
    assert_eq!(closes.len(), 1);
    assert_eq!(closes[0].side, Side::Sell);
    assert_eq!(closes[0].quantity, dec("2"));
    assert_eq!(closes[0].status, trading_models::OrderStatus::Filled);
    assert_eq!(app.ledger.position(DEFAULT_INSTRUMENT), None);
}
