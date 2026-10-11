//! 高并发 mock 演示：
//! - 多线程对同一组表做混合 CRUD；
//! - 后台线程每 100ms 定时全量持久化（无 channel，Condvar 唤醒）；
//! - 另有手动 flush 压力线程与调度线程并发；
//! - 结束后 shutdown 收尾 flush，再用新 Store 从 CSV 恢复校验。
//!
//! 运行：`cargo run -p trading-store --example concurrency_mock`

use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use rust_decimal::Decimal;
use store_core::SystemClock;
use trading_models::{OrderStatus, Side};
use trading_store::TradingStore;

const THREADS: u64 = 8;
const ORDERS_PER_THREAD: u64 = 1_000;

fn cid(n: u64) -> String {
    format!("0x{n:032x}")
}

fn main() {
    let root = std::env::temp_dir().join("trading_store_concurrency_mock");
    let _ = fs::remove_dir_all(&root);

    let (store, handle) = TradingStore::open(
        &root,
        Arc::new(SystemClock),
        Some(Duration::from_millis(100)),
    )
    .expect("open store");
    let persist = handle.expect("persist handle");

    let account = store
        .accounts()
        .unwrap()
        .insert(trading_models::Account::new("alice", "binance"))
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    // 额外的手动 flush 压力线程，与后台调度线程同时做快照。
    let stress_stop = stop.clone();
    let stress_core = store.core().clone();
    let stress_root = root.clone();
    let stress = thread::spawn(move || {
        let mut n = 0u64;
        while !stress_stop.load(Ordering::Relaxed) {
            stress_core.flush(&stress_root, false).unwrap();
            n += 1;
            thread::sleep(Duration::from_millis(1));
        }
        n
    });

    let orders = store.orders().unwrap();
    let started = Instant::now();
    let mut workers = Vec::new();
    for t in 0..THREADS {
        let orders = orders.clone();
        workers.push(thread::spawn(move || {
            for i in 0..ORDERS_PER_THREAD {
                let n = t * ORDERS_PER_THREAD + i + 1;
                let mut order =
                    trading_models::Order::new(account.id, cid(n), "binance", "BTCUSDT", Side::Buy);
                order.order_quantity = Decimal::from(n);
                order.status = OrderStatus::New;
                let inserted = orders.insert(order).unwrap();

                // 1/10 做一次行内更新；前 1/20 软删除。
                if n.is_multiple_of(10) {
                    orders
                        .update(inserted.id, |o| o.status = OrderStatus::Filled)
                        .unwrap();
                }
                if n.is_multiple_of(20) {
                    orders.soft_delete(inserted.id).unwrap();
                }
            }
        }));
    }
    for w in workers {
        w.join().unwrap();
    }
    let elapsed = started.elapsed();

    stop.store(true, Ordering::Relaxed);
    let stress_flushes = stress.join().unwrap();
    persist.shutdown(); // 收尾 flush + join

    let total = THREADS * ORDERS_PER_THREAD;
    let deleted = total / 20;
    let live = orders.list().len() as u64;
    let all = orders.list_including_deleted().len() as u64;
    println!("== 内存 ==");
    println!("总写入: {total}, 存活: {live}, 含 tombstone: {all}");
    println!(
        "期望存活: {}, 期望 tombstone 总数: {}",
        total - deleted,
        total
    );
    println!("手动压力 flush 次数: {stress_flushes}, 耗时: {elapsed:?}");
    assert_eq!(all, total);

    // 重启恢复。
    let (store2, _) = TradingStore::open(&root, Arc::new(SystemClock), None).expect("reopen");
    let restored_all = store2.orders().unwrap().list_including_deleted().len() as u64;
    let restored_live = store2.orders().unwrap().list().len() as u64;
    println!("== 恢复 ==");
    println!(
        "恢复总数: {restored_all}, 存活: {restored_live}, 数据目录: {}",
        root.display()
    );
    assert_eq!(restored_all, total);
    assert_eq!(restored_live, live);
    println!("OK");
}
