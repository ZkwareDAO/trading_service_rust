//! 持久化契约测试（crate 级）：bootstrap → 写表 → flush → 同目录重新 bootstrap。
//!
//! 验证账户/订单/仓位/规则四类数据从 CSV 快照恢复，且主键与业务字段不漂移。

use rust_decimal::Decimal;
use tempfile::TempDir;
use trading_models::{
    Order, OrderStatus, OrderType, Position, Side, WatchConditionKind, WatchRule, WatchRuleAction,
    WatchRuleStatus,
};

use trading_service::testkit::app_with_accounts;

/// 合成 binance 账户行（不连网）。
const BINANCE_ACCOUNT: &str =
    "0,acct,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n";
/// 测试订单 cid（`0x` + 32 hex）。
const CID: &str = "0x00000000000000000000000000000001";

fn dec(s: &str) -> Decimal {
    use std::str::FromStr;
    Decimal::from_str(s).expect("decimal")
}

/// 全量往返：四类数据 flush 后在**同一目录**重新装配可原样恢复。
#[tokio::test]
async fn flush_then_rebootstrap_recovers_accounts_orders_positions_rules() {
    let dir = TempDir::new().expect("temp dir");
    let app = app_with_accounts(dir.path(), BINANCE_ACCOUNT).await;
    let account_id = app
        .state
        .resolve_trading_account("binance", "acct")
        .expect("账户应已加载")
        .id;

    // 订单（非终态，避免软删语义干扰恢复断言）。
    let mut order = Order::new(account_id, CID, "binance", "BTCUSDT", Side::Buy);
    order.order_type = OrderType::Limit;
    order.order_price = dec("100.5");
    order.order_quantity = dec("1.25");
    order.status = OrderStatus::New;
    let stored_order = app
        .state
        .store
        .order_repo()
        .expect("order repo")
        .insert(order)
        .expect("insert order");

    // 仓位。
    let mut p = Position::new(account_id, "binance", "BTCUSDT", Side::Buy);
    p.quantity = dec("2");
    p.pos_price = dec("100");
    p.current_price = dec("110");
    p.margin = dec("20");
    app.state
        .store
        .position_repo()
        .expect("position repo")
        .upsert_matching(vec![p])
        .expect("upsert position");
    let position_id = app
        .state
        .store
        .position_repo()
        .expect("position repo")
        .list_active()[0]
        .id;

    // 规则（绑定该仓位）。
    let rule = WatchRule::new_risk(
        account_id,
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
    );
    let stored_rule = app
        .state
        .store
        .rule_repo()
        .expect("rule repo")
        .insert(rule)
        .expect("insert rule");

    // 落盘后丢弃全部内存状态，再在同一目录装配（csv_body 为空不覆盖既有表）。
    app.state.store.flush().expect("flush");
    drop(app);

    let reopened = app_with_accounts(dir.path(), "").await;

    // 账户恢复（同 id）。
    assert_eq!(
        reopened
            .state
            .resolve_trading_account("binance", "acct")
            .expect("账户应从 CSV 恢复")
            .id,
        account_id
    );

    // 订单恢复（同 id / 同量 / 同状态）。
    let recovered_order = reopened
        .state
        .store
        .order_repo()
        .expect("order repo")
        .peek_by_cid(CID)
        .expect("订单应从 CSV 恢复");
    assert_eq!(recovered_order.id, stored_order.id);
    assert_eq!(recovered_order.order_quantity, dec("1.25"));
    assert_eq!(recovered_order.order_price, dec("100.5"));
    assert_eq!(recovered_order.status, OrderStatus::New);

    // 仓位恢复（同 id / 同量 / 同成本）。
    let positions = reopened
        .state
        .store
        .position_repo()
        .expect("position repo")
        .list_active();
    assert_eq!(positions.len(), 1, "仓位行数不得漂移");
    assert_eq!(positions[0].id, position_id);
    assert_eq!(positions[0].quantity, dec("2"));
    assert_eq!(positions[0].pos_price, dec("100"));

    // 规则恢复（同 id / 仍 active）。
    let recovered_rule = reopened
        .state
        .store
        .rule_repo()
        .expect("rule repo")
        .get_including_deleted(stored_rule.id)
        .expect("规则应从 CSV 恢复");
    assert_eq!(recovered_rule.status, WatchRuleStatus::Active);
    assert_eq!(recovered_rule.account_id, account_id);
}

/// 重复 flush 不产生重复行（快照是覆盖写而非追加）。
#[tokio::test]
async fn repeated_flush_does_not_duplicate_rows() {
    let dir = TempDir::new().expect("temp dir");
    let app = app_with_accounts(dir.path(), BINANCE_ACCOUNT).await;
    let account_id = app
        .state
        .resolve_trading_account("binance", "acct")
        .expect("账户应已加载")
        .id;

    let mut p = Position::new(account_id, "binance", "BTCUSDT", Side::Buy);
    p.quantity = dec("1");
    p.pos_price = dec("100");
    p.current_price = dec("100");
    let repo = app.state.store.position_repo().expect("position repo");
    repo.upsert_matching(vec![p]).expect("upsert position");
    let position_id = repo.list_active()[0].id;

    app.state.store.flush().expect("first flush");
    app.state.store.flush().expect("second flush");
    drop(app);

    let reopened = app_with_accounts(dir.path(), "").await;
    let positions = reopened
        .state
        .store
        .position_repo()
        .expect("position repo")
        .list_active();
    assert_eq!(positions.len(), 1, "重复 flush 不得追加重复行");
    assert_eq!(positions[0].id, position_id, "主键不得漂移");
}

/// 空目录（无 account.csv）也能 bootstrap：空账户表 + 空业务表。
#[tokio::test]
async fn bootstrap_without_account_csv_starts_empty_store() {
    let dir = TempDir::new().expect("temp dir");
    let app = app_with_accounts(dir.path(), "").await;

    assert!(app.state.accounts.list_active_briefs().is_empty());
    assert!(app
        .state
        .store
        .order_repo()
        .expect("order repo")
        .list_active()
        .is_empty());
    assert!(app
        .state
        .store
        .position_repo()
        .expect("position repo")
        .list_active()
        .is_empty());
}
