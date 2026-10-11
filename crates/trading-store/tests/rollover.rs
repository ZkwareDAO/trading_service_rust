mod common;

use common::*;
use store_core::Store;

#[test]
fn tick_across_utc_midnight_purges_tombstones() {
    let mut h = Harness::open();
    let account =
        h.ts.accounts()
            .unwrap()
            .insert(Harness::make_account())
            .unwrap();
    let orders = h.ts.orders().unwrap();
    orders.insert(Harness::make_order(account.id, 1)).unwrap();
    orders.insert(Harness::make_order(account.id, 2)).unwrap();
    orders.soft_delete(2).unwrap();
    h.tick(); // day1：内存 2 行（含 tombstone）

    // 跨 UTC 日界。
    h.clock.set(T0 + DAY_MS);
    let outcome = store_core::run_tick(h.ts.core(), h.ts.root(), &mut h.last_day).unwrap();

    assert!(outcome.rolled_over);
    assert_eq!(outcome.purged, 1);
    assert_eq!(outcome.day, store_core::day_key(T0 + DAY_MS));

    // tombstone 被物理清除，存活行保留。
    assert_eq!(orders.list().len(), 1);
    assert_eq!(orders.list_including_deleted().len(), 1);
    assert!(orders.get(1).unwrap().is_some());

    // 新日目录生成。
    assert!(h
        .tmp
        .path()
        .join(&outcome.day)
        .join("alice")
        .join("order.csv")
        .is_file());

    // 跨日后重开：新一天恢复只有 1 行。
    let h = h.reopen();
    assert_eq!(h.ts.orders().unwrap().list_including_deleted().len(), 1);
}

#[test]
fn same_day_restart_keeps_tombstones() {
    let mut h = Harness::open();
    let account =
        h.ts.accounts()
            .unwrap()
            .insert(Harness::make_account())
            .unwrap();
    let orders = h.ts.orders().unwrap();
    orders.insert(Harness::make_order(account.id, 1)).unwrap();
    orders.soft_delete(1).unwrap();
    h.tick();

    // 时钟不变（同日），tombstone 不得在启动时被 purge。
    let h = h.reopen();
    let orders = h.ts.orders().unwrap();
    assert_eq!(orders.list_including_deleted().len(), 1);
    assert!(orders.get(1).unwrap().is_none());
    assert!(orders.get_including_deleted(1).unwrap().unwrap().deleted);
}

#[test]
fn startup_after_offline_cross_day_purges() {
    let mut h = Harness::open();
    let account =
        h.ts.accounts()
            .unwrap()
            .insert(Harness::make_account())
            .unwrap();
    let orders = h.ts.orders().unwrap();
    orders.insert(Harness::make_order(account.id, 1)).unwrap();
    orders.insert(Harness::make_order(account.id, 2)).unwrap();
    orders.soft_delete(2).unwrap();
    h.tick(); // 最后落盘在 day1

    // 停机跨过 UTC 午夜后才重启：直接用底层 Store 观察 recover 报告。
    h.clock.set(T0 + DAY_MS + 3_600_000);
    let core = Store::new(h.clock.clone() as std::sync::Arc<dyn store_core::Clock>);
    core.register_account::<trading_models::Account>().unwrap();
    core.register::<trading_models::Order>().unwrap();
    core.register::<trading_models::Position>().unwrap();

    let report = core.recover(h.ts.root()).unwrap();
    assert_eq!(report.purged_on_startup, 1);

    let orders = core.table::<trading_models::Order>().unwrap();
    assert_eq!(orders.list_including_deleted().len(), 1);
    assert!(orders.get(1).unwrap().is_some());
}
