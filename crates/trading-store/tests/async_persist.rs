use std::sync::Arc;
use std::time::Duration;

use store_core::SystemClock;
use trading_models::Account;
use trading_store::TradingStore;

#[tokio::test(start_paused = true)]
async fn open_async_auto_flush_then_sync_reopen() {
    let tmp = tempfile::tempdir().unwrap();

    let (store, handle) = TradingStore::open_async(
        tmp.path(),
        Arc::new(SystemClock),
        Some(Duration::from_millis(50)),
    )
    .await
    .unwrap();
    let handle = handle.unwrap();

    // 同步 CRUD 直接在 async 上下文调用，无需 await。
    store
        .accounts()
        .unwrap()
        .insert(Account::new("alice", "binance"))
        .unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;
    handle.shutdown().await;

    // 用同步门面重开恢复。
    let (store2, _) = TradingStore::open(tmp.path(), Arc::new(SystemClock), None).unwrap();
    assert_eq!(store2.accounts().unwrap().list().len(), 1);
    assert_eq!(store2.accounts().unwrap().list()[0].name, "alice");
}

#[tokio::test(start_paused = true)]
async fn open_async_shutdown_flushes_without_waiting_tick() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, handle) = TradingStore::open_async(
        tmp.path(),
        Arc::new(SystemClock),
        Some(Duration::from_secs(3600)),
    )
    .await
    .unwrap();

    store
        .accounts()
        .unwrap()
        .insert(Account::new("bob", "okx"))
        .unwrap();
    handle.unwrap().shutdown().await;

    let (store2, _) = TradingStore::open(tmp.path(), Arc::new(SystemClock), None).unwrap();
    assert_eq!(store2.accounts().unwrap().list().len(), 1);
}
