mod common;

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use common::*;
use rust_decimal::Decimal;

const THREADS: u64 = 8;
const PER_THREAD: u64 = 200;

#[test]
fn parallel_inserts_produce_unique_ids() {
    let h = Harness::open();
    let account =
        h.ts.accounts()
            .unwrap()
            .insert(Harness::make_account())
            .unwrap();
    let orders = h.ts.orders().unwrap();

    let mut handles = Vec::new();
    for t in 0..THREADS {
        let orders = orders.clone();
        handles.push(thread::spawn(move || {
            for i in 0..PER_THREAD {
                let n = t * PER_THREAD + i + 1;
                orders.insert(Harness::make_order(account.id, n)).unwrap();
            }
        }));
    }
    for j in handles {
        j.join().unwrap();
    }

    let all = orders.list();
    assert_eq!(all.len() as u64, THREADS * PER_THREAD);

    let ids: HashSet<u64> = all.iter().map(|o| o.id).collect();
    assert_eq!(ids.len() as u64, THREADS * PER_THREAD, "ids must be unique");

    let cids: HashSet<String> = all.iter().map(|o| o.client_order_id.clone()).collect();
    assert_eq!(cids.len() as u64, THREADS * PER_THREAD);
}

#[test]
fn contended_row_updates_are_serialized_by_row_lock() {
    let h = Harness::open();
    let account =
        h.ts.accounts()
            .unwrap()
            .insert(Harness::make_account())
            .unwrap();
    let orders = h.ts.orders().unwrap();
    orders.insert(Harness::make_order(account.id, 1)).unwrap();

    let rounds = 500u64;
    let mut handles = Vec::new();
    for _ in 0..THREADS {
        let orders = orders.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..rounds {
                orders
                    .update(1, |o| o.filled_quantity += Decimal::ONE)
                    .unwrap();
            }
        }));
    }
    for j in handles {
        j.join().unwrap();
    }

    let total = orders.with_row(1, |o| o.filled_quantity).unwrap().unwrap();
    assert_eq!(total, Decimal::from(THREADS * rounds));
}

#[test]
fn crud_running_concurrently_with_flushes_stays_consistent() {
    let h = Harness::open();
    let account =
        h.ts.accounts()
            .unwrap()
            .insert(Harness::make_account())
            .unwrap();
    let core = h.ts.core().clone();
    let root = h.ts.root().clone();

    let stop = Arc::new(AtomicBool::new(false));
    let flusher_stop = stop.clone();
    let flusher = thread::spawn(move || {
        let mut n = 0;
        while !flusher_stop.load(Ordering::Relaxed) {
            core.flush(&root, false).unwrap();
            n += 1;
        }
        n
    });

    let orders = h.ts.orders().unwrap();
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let orders = orders.clone();
        handles.push(thread::spawn(move || -> usize {
            let mut deleted = 0;
            for i in 0..PER_THREAD {
                let n = t * PER_THREAD + i + 1;
                let inserted = orders.insert(Harness::make_order(account.id, n)).unwrap();
                // 每个线程把自己前 1/4 的单软删除，制造 tombstone 压力。
                if i < PER_THREAD / 4 {
                    orders.soft_delete(inserted.id).unwrap();
                    deleted += 1;
                }
            }
            deleted
        }));
    }
    let mut deleted_total = 0;
    for j in handles {
        deleted_total += j.join().unwrap();
    }

    stop.store(true, Ordering::Relaxed);
    let flush_count = flusher.join().unwrap();
    assert!(flush_count > 0);

    let live = orders.list().len() as u64;
    let total = THREADS * PER_THREAD;
    assert_eq!(live, total - deleted_total as u64);
    assert_eq!(orders.list_including_deleted().len() as u64, total);

    // 最终落盘后重开：恢复结果与内存一致。
    h.ts.flush().unwrap();
    let h = h.reopen();
    let restored = h.ts.orders().unwrap();
    assert_eq!(restored.list().len() as u64, live);
    assert_eq!(restored.list_including_deleted().len() as u64, total);
}
