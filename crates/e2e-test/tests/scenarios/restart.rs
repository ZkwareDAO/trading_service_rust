//! 重启恢复：同一临时目录重新装配，订单/仓位/规则从 CSV 恢复且终态不重复推进。

use rust_decimal::Decimal;
use tempfile::TempDir;
use trading_models::{
    OrderStatus, OrderType, Side, WatchConditionKind, WatchRule, WatchRuleAction, WatchRuleStatus,
};

use e2e_test::harness::{self as common, dec, DEFAULT_INSTRUMENT};

#[tokio::test]
async fn restart_recovers_state_from_csv_without_re_advancing_terminal_order() {
    // Arrange：第一阶段在临时目录落一笔已成交订单、一个仓位、一条 active 规则。
    let dir = TempDir::new().unwrap();
    let app = common::start_sim_online_at(dir.path()).await;
    common::set_mark(&app, "100");
    let cid = app
        .place_market(DEFAULT_INSTRUMENT, "buy", "1", false)
        .await;
    app.wait_for("成交建仓", |a| a.positions().len() == 1).await;
    app.wait_for("订单 FILLED", |a| {
        a.system_order(&cid)
            .is_some_and(|o| o.status == OrderStatus::Filled)
    })
    .await;
    let position_id = app.positions()[0].id;

    let rule = app.insert_rule(WatchRule::new_risk(
        app.account_id(),
        position_id,
        WatchConditionKind::Price,
        None,
        "<=",
        dec("50"),
        WatchRuleAction::UseSelf,
        OrderType::Market,
        Decimal::ZERO,
        Decimal::ZERO,
        Decimal::ONE,
    ));
    let rule_id = rule.id;

    // Act：手工落盘后丢弃全部内存状态，再在**同一目录**装配（不启动账户流）。
    app.flush();
    drop(app);
    let reopened = common::start_sim_at(dir.path()).await;

    // Assert：终态订单恢复且仍是 FILLED（不重复推进）。
    let recovered_order = reopened
        .system_order(&cid)
        .expect("已成交订单应从 CSV 恢复");
    assert_eq!(recovered_order.status, OrderStatus::Filled);
    assert_eq!(recovered_order.filled_quantity, dec("1"));

    // Assert：仓位恢复（同 id、同量、同成本）。
    let positions = reopened.positions();
    assert_eq!(positions.len(), 1, "仓位应从 CSV 恢复");
    assert_eq!(positions[0].id, position_id);
    assert_eq!(positions[0].instrument, DEFAULT_INSTRUMENT);
    assert_eq!(positions[0].quantity, dec("1"));
    assert_eq!(positions[0].pos_price, dec("100"));

    // Assert：规则恢复且仍 active。
    let recovered_rule = reopened.rule(rule_id).expect("规则应从 CSV 恢复");
    assert_eq!(recovered_rule.status, WatchRuleStatus::Active);
    assert_eq!(recovered_rule.side, Side::Buy);
}
