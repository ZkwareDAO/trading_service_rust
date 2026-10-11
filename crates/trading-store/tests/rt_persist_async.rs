mod common;

use std::time::Duration;

use common::{any_csv, setup, today_csv, Item};

#[tokio::test(start_paused = true)]
async fn auto_flush_writes_only_when_dirty() {
    let ctx = setup().await;
    let handle = ctx.rt.spawn(Duration::from_millis(50));

    // 空转：无变更不落盘。
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        any_csv(ctx.tmp.path()).is_empty(),
        "clean store must not produce files"
    );

    // 有变更后等两个 tick。
    let items = ctx.store.table::<Item>().unwrap();
    items.insert(Item::new("alice")).unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(today_csv(ctx.tmp.path()).is_file());

    handle.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_flushes_pending_changes() {
    let ctx = setup().await;
    // 间隔 1 小时：保证期间没有任何 tick 落盘。
    let handle = ctx.rt.spawn(Duration::from_secs(3600));
    ctx.store
        .table::<Item>()
        .unwrap()
        .insert(Item::new("bob"))
        .unwrap();

    handle.shutdown().await;
    assert!(today_csv(ctx.tmp.path()).is_file(), "shutdown must flush");
}

#[tokio::test(start_paused = true)]
async fn sync_crud_concurrent_with_async_task_recovers() {
    let ctx = setup().await;
    let handle = ctx.rt.spawn(Duration::from_millis(20));
    let items = ctx.store.table::<Item>().unwrap();

    // 8 个 OS 线程并发做同步 CRUD，后台 tokio 任务同时落盘。
    let mut workers = Vec::new();
    for t in 0..8u64 {
        let items = items.clone();
        workers.push(std::thread::spawn(move || {
            for i in 0..200u64 {
                items.insert(Item::new(&format!("n{}-{}", t, i))).unwrap();
            }
        }));
    }
    for w in workers {
        w.join().unwrap();
    }
    tokio::time::sleep(Duration::from_millis(80)).await;
    handle.shutdown().await;
    assert_eq!(items.list().len(), 1600);

    // 用一个全新 Store 从快照恢复。
    let store2 = store_core::Store::new(std::sync::Arc::new(store_core::SystemClock));
    store2.register::<Item>().unwrap();
    let rt2 = trading_store::PersistRuntime::new(store2.clone(), ctx.tmp.path());
    rt2.recover().await.unwrap();
    assert_eq!(store2.table::<Item>().unwrap().list().len(), 1600);
}

#[tokio::test(start_paused = true)]
async fn manual_flush_dirty_semantics() {
    let ctx = setup().await;
    // 空表手动 flush：无 dirty 不产生文件。
    assert_eq!(ctx.rt.flush().await.unwrap().files, 0);
    assert!(any_csv(ctx.tmp.path()).is_empty());

    ctx.store
        .table::<Item>()
        .unwrap()
        .insert(Item::new("x"))
        .unwrap();
    let report = ctx.rt.flush().await.unwrap();
    assert_eq!(report.files, 1);
    assert!(today_csv(ctx.tmp.path()).is_file());
}
