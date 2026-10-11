mod common;

use std::fs;

use common::{Ghost, Item};
use store_core::StoreError;

#[test]
fn duplicate_registration_rejected() {
    let store = common::new_store();
    store.register::<Item>().unwrap();
    assert!(matches!(
        store.register::<Item>(),
        Err(StoreError::AlreadyRegistered("item"))
    ));
}

#[test]
fn unregistered_table_is_unknown_model() {
    let store = common::new_store();
    assert!(matches!(
        store.table::<Ghost>(),
        Err(StoreError::UnknownModel("ghost"))
    ));
}

#[test]
fn recover_skips_files_of_unregistered_models() {
    let tmp = tempfile::tempdir().unwrap();
    // 写一个 ghost model 的快照文件。
    let dir = tmp.path().join("2020-01-01").join("_system");
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("ghost.csv"),
        "id,deleted,created_at,updated_at\n7,0,1,1\n",
    )
    .unwrap();

    let store = common::new_store();
    store.register::<Item>().unwrap();
    let report = store.recover(tmp.path()).unwrap();
    assert_eq!(report.files, 0);
    assert_eq!(report.skipped_unknown, vec!["ghost".to_string()]);
}

#[test]
fn recover_empty_root_is_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let store = common::new_store();
    store.register::<Item>().unwrap();
    let report = store.recover(tmp.path()).unwrap();
    assert_eq!(report.files, 0);
    assert_eq!(report.rows, 0);
}
