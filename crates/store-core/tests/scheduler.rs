mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{today_item_csv, Item};
use store_core::{day_key, MockClock, PersistOptions};

const DAY_MS: u64 = 86_400_000;

#[test]
fn background_thread_flushes_and_shutdown_flushes_pending() {
    let tmp = tempfile::tempdir().unwrap();
    let store = common::new_store();
    let items = store.register::<Item>().unwrap();

    let handle = store_core::spawn(
        store.clone(),
        tmp.path().to_path_buf(),
        PersistOptions {
            interval: Duration::from_millis(50),
        },
    );

    items.insert(Item::new("alice")).unwrap();
    std::thread::sleep(Duration::from_millis(140));
    assert!(
        today_item_csv(tmp.path()).is_file(),
        "interval tick must flush"
    );

    // 不再等 tick，shutdown 负责把最后窗口的数据落盘。
    items.insert(Item::new("bob")).unwrap();
    handle.shutdown();

    // 新 store 恢复，两条都在。
    let store2 = common::new_store();
    store2.register::<Item>().unwrap();
    let report = store2.recover(tmp.path()).unwrap();
    assert!(report.files >= 1);
    assert_eq!(store2.table::<Item>().unwrap().list().len(), 2);
}

#[test]
fn run_tick_rolls_over_utc_day_and_purges() {
    // UTC 2023-11-14 01:00。
    let t0: u64 = 1_699_923_600_000;
    let clock = Arc::new(MockClock::new(t0));
    let tmp = tempfile::tempdir().unwrap();
    let store = store_core::Store::new(clock.clone() as Arc<dyn store_core::Clock>);
    let items = store.register::<Item>().unwrap();
    items.insert(Item::new("a")).unwrap();
    items.insert(Item::new("b")).unwrap();
    items.soft_delete(2).unwrap();

    let mut last_day = None;
    let first = store_core::run_tick(&store, tmp.path(), &mut last_day).unwrap();
    assert!(!first.rolled_over);
    assert_eq!(first.day, day_key(t0));

    // 跨 UTC 午夜：tombstone 被 purge，新日文件生成。
    clock.set(t0 + DAY_MS);
    let second = store_core::run_tick(&store, tmp.path(), &mut last_day).unwrap();
    assert!(second.rolled_over);
    assert_eq!(second.purged, 1);
    assert_eq!(second.day, day_key(t0 + DAY_MS));
    assert_eq!(items.list_including_deleted().len(), 1);

    // 重启恢复（clock 已在新一天）：只剩 1 行，旧日文件不会复活 tombstone。
    let store2 = store_core::Store::new(clock.clone() as Arc<dyn store_core::Clock>);
    store2.register::<Item>().unwrap();
    store2.recover(tmp.path()).unwrap();
    assert_eq!(
        store2
            .table::<Item>()
            .unwrap()
            .list_including_deleted()
            .len(),
        1
    );
}

#[test]
fn spawn_requires_no_tokio_runtime() {
    // 纯 std 线程实现：无 runtime 环境也能工作。
    let tmp = tempfile::tempdir().unwrap();
    let store = common::new_store();
    store.register::<Item>().unwrap();
    let handle = store_core::spawn(
        store.clone(),
        tmp.path().to_path_buf(),
        PersistOptions {
            interval: Duration::from_secs(3600),
        },
    );
    handle.shutdown();
}
