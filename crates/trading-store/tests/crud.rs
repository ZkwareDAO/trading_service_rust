mod common;

use common::*;
use store_core::StoreError;
use trading_models::{Account, OrderStatus};

#[test]
fn insert_assigns_id_and_timestamps() {
    let h = Harness::open();
    let acc = h.ts.accounts().unwrap();

    let a = acc.insert(Harness::make_account()).unwrap();
    assert_eq!(a.id, 1);
    assert_eq!(a.created_at, T0);
    assert_eq!(a.updated_at, T0);

    let b = acc.insert(Account::new("bob", "deribit")).unwrap();
    assert_eq!(b.id, 2);
}

#[test]
fn update_refreshes_updated_at_keeps_created_at() {
    let h = Harness::open();
    let acc = h.ts.accounts().unwrap();
    acc.insert(Harness::make_account()).unwrap();

    h.clock.advance(5_000);
    let updated = acc
        .update(1, |a| {
            a.exchange = "hyperliquid".to_string();
        })
        .unwrap();

    assert_eq!(updated.exchange, "hyperliquid");
    assert_eq!(updated.created_at, T0);
    assert_eq!(updated.updated_at, T0 + 5_000);
}

#[test]
fn get_list_and_soft_delete_filter_tombstones() {
    let h = Harness::open();
    let acc = h.ts.accounts().unwrap();
    acc.insert(Harness::make_account()).unwrap();
    acc.insert(Account::new("bob", "deribit")).unwrap();

    acc.soft_delete(2).unwrap();

    assert!(acc.get(1).unwrap().is_some());
    assert!(acc.get(2).unwrap().is_none());
    assert!(acc.get_including_deleted(2).unwrap().unwrap().deleted);
    assert_eq!(acc.list().len(), 1);
    assert_eq!(acc.list_including_deleted().len(), 2);

    // 已删除行不可再 update。
    match acc.update(2, |a| a.name = "x".into()) {
        Err(StoreError::NotFound(2)) => {}
        other => panic!("expected NotFound, got {other:?}"),
    }
}

#[test]
fn duplicate_explicit_id_rejected() {
    let h = Harness::open();
    let acc = h.ts.accounts().unwrap();
    acc.insert(Harness::make_account()).unwrap();

    let mut dup = Account::new("evil", "binance");
    dup.id = 1;
    match acc.insert(dup) {
        Err(StoreError::DuplicateId(1)) => {}
        other => panic!("expected DuplicateId, got {other:?}"),
    }
}

#[test]
fn upsert_replaces_and_preserves_created_at() {
    let h = Harness::open();
    let acc = h.ts.accounts().unwrap();
    acc.insert(Harness::make_account()).unwrap();

    h.clock.advance(1_000);
    let mut changed = acc.get(1).unwrap().unwrap();
    changed.api_key = "k2".into();
    acc.upsert(changed).unwrap();

    let got = acc.get(1).unwrap().unwrap();
    assert_eq!(got.api_key, "k2");
    assert_eq!(got.created_at, T0);
    assert_eq!(got.updated_at, T0 + 1_000);

    // id=0 的 upsert 等价插入。
    assert_eq!(acc.upsert(Account::new("carol", "okx")).unwrap().id, 2);
}

#[test]
fn concurrent_inserts_with_same_explicit_id_exactly_one_wins() {
    let h = Harness::open();
    let acc = h.ts.accounts().unwrap();

    let mut workers = Vec::new();
    for _ in 0..8 {
        let acc = acc.clone();
        workers.push(std::thread::spawn(move || {
            let mut oks = 0;
            for _ in 0..100 {
                let mut a = Account::new("dup", "binance");
                a.id = 42;
                if acc.insert(a).is_ok() {
                    oks += 1;
                }
            }
            oks
        }));
    }
    let total_ok: usize = workers.into_iter().map(|w| w.join().unwrap()).sum();
    assert_eq!(total_ok, 1, "exactly one insert of id=42 must succeed");
    assert_eq!(acc.list_including_deleted().len(), 1);
}

#[test]
fn with_row_zero_copy_read_and_order_crud() {
    let h = Harness::open();
    let acc = h.ts.accounts().unwrap();
    acc.insert(Harness::make_account()).unwrap();

    let orders = h.ts.orders().unwrap();
    let mut order = Harness::make_order(1, 7);
    order.status = OrderStatus::New;
    let inserted = orders.insert(order).unwrap();
    assert_eq!(inserted.id, 1);

    let cid = orders
        .with_row(1, |o| o.client_order_id.clone())
        .unwrap()
        .unwrap();
    assert_eq!(cid, common::cid(7));

    orders
        .update(1, |o| o.status = OrderStatus::Filled)
        .unwrap();
    let status = orders.with_row(1, |o| o.status).unwrap().unwrap();
    assert_eq!(status, OrderStatus::Filled);
}
