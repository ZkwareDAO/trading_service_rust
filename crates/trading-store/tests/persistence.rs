mod common;

use common::*;
use rust_decimal::Decimal;
use store_core::day_key;

#[test]
fn flush_writes_scoped_paths() {
    let mut h = Harness::open();
    let account =
        h.ts.accounts()
            .unwrap()
            .insert(Harness::make_account())
            .unwrap();
    h.ts.orders()
        .unwrap()
        .insert(Harness::make_order(account.id, 1))
        .unwrap();
    h.ts.positions()
        .unwrap()
        .insert(Harness::make_position(account.id))
        .unwrap();
    h.ts.watch_rules()
        .unwrap()
        .insert(Harness::make_rule(account.id))
        .unwrap();
    h.tick();

    let day = Harness::day_of_t0();
    let root = h.tmp.path();
    assert!(root
        .join(&day)
        .join("_system")
        .join("account.csv")
        .is_file());
    assert!(root.join(&day).join("alice").join("order.csv").is_file());
    assert!(root.join(&day).join("alice").join("position.csv").is_file());
    assert!(root
        .join(&day)
        .join("alice")
        .join("watch_rule.csv")
        .is_file());

    // 临时文件应已 rename 消失（只看文件名，避免误匹配 tempdir 自身路径）。
    let leftover: Vec<_> = walkdir(root)
        .into_iter()
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with('.') && n.ends_with(".tmp"))
        })
        .collect();
    assert!(leftover.is_empty(), "tmp files left behind: {leftover:?}");
}

#[test]
fn restart_recovers_rows_and_id_watermark() {
    let mut h = Harness::open();
    let account =
        h.ts.accounts()
            .unwrap()
            .insert(Harness::make_account())
            .unwrap();
    let mut order = Harness::make_order(account.id, 1);
    order.order_quantity = Decimal::new(3, 0);
    h.ts.orders().unwrap().insert(order).unwrap();
    h.tick();

    let h = h.reopen();

    let accounts = h.ts.accounts().unwrap();
    assert_eq!(accounts.list().len(), 1);
    assert_eq!(accounts.get(1).unwrap().unwrap().name, "alice");

    // 定点数 CSV 往返无损。
    let order = h.ts.orders().unwrap().get(1).unwrap().unwrap();
    assert_eq!(order.order_quantity, Decimal::new(3, 0));
    assert_eq!(order.client_order_id, cid(1));

    // id 水位恢复：新账户拿到 id=2。
    let next = accounts
        .insert(trading_models::Account::new("bob", "okx"))
        .unwrap();
    assert_eq!(next.id, 2);
}

#[test]
fn multi_day_dedup_keeps_latest_updated_at() {
    let mut h = Harness::open();
    let account =
        h.ts.accounts()
            .unwrap()
            .insert(Harness::make_account())
            .unwrap();
    let orders = h.ts.orders().unwrap();
    orders.insert(Harness::make_order(account.id, 1)).unwrap();
    h.tick(); // day1: qty=1

    h.clock.advance(DAY_MS);
    orders
        .update(1, |o| o.order_quantity = Decimal::new(5, 0))
        .unwrap();
    h.tick(); // day2: qty=5

    let h = h.reopen();
    let all = h.ts.orders().unwrap().list_including_deleted();
    assert_eq!(all.len(), 1, "same id across days must dedup");
    assert_eq!(all[0].order_quantity, Decimal::new(5, 0));
}

#[test]
fn tombstone_persisted_same_day_does_not_revive() {
    let mut h = Harness::open();
    let account =
        h.ts.accounts()
            .unwrap()
            .insert(Harness::make_account())
            .unwrap();
    let orders = h.ts.orders().unwrap();
    orders.insert(Harness::make_order(account.id, 1)).unwrap();
    h.tick();

    // 当日软删除并再落盘：tombstone 必须在快照里，重启不复活。
    orders.soft_delete(1).unwrap();
    h.tick();

    let h = h.reopen();
    let orders = h.ts.orders().unwrap();
    assert!(orders.get(1).unwrap().is_none());
    assert!(orders.get_including_deleted(1).unwrap().unwrap().deleted);
}

#[test]
fn rule_rows_survive_restart_and_soft_delete() {
    let mut h = Harness::open();
    let account =
        h.ts.accounts()
            .unwrap()
            .insert(Harness::make_account())
            .unwrap();
    let rules = h.ts.watch_rules().unwrap();
    rules.insert(Harness::make_rule(account.id)).unwrap();
    h.tick();

    // 定点阈值/平仓类型与比例/枚举 CSV 往返无损。
    let h = h.reopen();
    let rules = h.ts.watch_rules().unwrap();
    let back = rules.get(1).unwrap().unwrap();
    assert_eq!(back.position_id, 100);
    assert_eq!(back.value, Decimal::new(12, 2));
    assert_eq!(back.condition_name(), "roi");
    assert!(!back.deleted); // 0=活跃
    assert_eq!(back.order_type, trading_models::OrderType::Market);
    assert_eq!(back.quantity_pct, Decimal::ONE);
    assert_eq!(
        back.parsed_action(),
        Some(trading_models::WatchRuleAction::UseSelf)
    );

    // 软删后同日重启不复活，tombstone 仍可读回。
    rules.soft_delete(1).unwrap();
    let mut h2 = h;
    h2.tick();
    let h2 = h2.reopen();
    let rules = h2.ts.watch_rules().unwrap();
    assert!(rules.get(1).unwrap().is_none());
    assert!(rules.get_including_deleted(1).unwrap().unwrap().deleted); // 1=终止态
}

#[test]
fn rule_in_use_survives_restart_and_can_retire() {
    // 命中后处于 in_use 的规则（平仓单尚未终结）必须跨重启存活，
    // 重启后迟到的订单终态回报仍能把它 retire；active 列表不得包含它。
    let mut h = Harness::open();
    let account =
        h.ts.accounts()
            .unwrap()
            .insert(Harness::make_account())
            .unwrap();
    let repo = h.ts.rule_repo().unwrap();
    let rule = repo.insert(Harness::make_rule(account.id)).unwrap();
    assert!(repo.claim_self(rule.id).unwrap());
    assert_eq!(
        repo.get(rule.id).unwrap().status,
        trading_models::WatchRuleStatus::InUse
    );
    h.tick();

    let h = h.reopen();
    let repo = h.ts.rule_repo().unwrap();
    let back = repo.get(rule.id).unwrap();
    assert_eq!(back.status, trading_models::WatchRuleStatus::InUse);
    assert!(repo.list_active().is_empty());
    // 未终结不能重复抢占。
    assert!(!repo.claim_self(rule.id).unwrap());

    // 订单终态回报到达：in_use → retired（+deleted=1），重启不复活。
    repo.retire(rule.id).unwrap();
    let mut h2 = h;
    h2.tick();
    let h2 = h2.reopen();
    let repo2 = h2.ts.rule_repo().unwrap();
    assert!(repo2.get(rule.id).is_none());
    let tomb = h2
        .ts
        .watch_rules()
        .unwrap()
        .get_including_deleted(rule.id)
        .unwrap()
        .unwrap();
    assert_eq!(tomb.status, trading_models::WatchRuleStatus::Retired);
    assert!(tomb.deleted);
}

#[test]
fn unknown_account_goes_to_unassigned_dir() {
    let mut h = Harness::open();
    // 不建账户，直接挂 account_id=999 的订单。
    h.ts.orders()
        .unwrap()
        .insert(Harness::make_order(999, 1))
        .unwrap();
    h.tick();

    let path = h
        .tmp
        .path()
        .join(Harness::day_of_t0())
        .join("_unassigned")
        .join("order.csv");
    assert!(path.is_file(), "expected fallback path {}", path.display());

    let h = h.reopen();
    assert_eq!(h.ts.orders().unwrap().list().len(), 1);
}

#[test]
fn t0_and_next_day_keys_differ() {
    assert_eq!(Harness::day_of_t0(), day_key(T0));
    assert_ne!(day_key(T0), day_key(T0 + DAY_MS));
}

fn walkdir(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    walk(root, &mut out);
    out
}
fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else {
                out.push(p);
            }
        }
    }
}
