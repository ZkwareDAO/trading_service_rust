#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use store_core::model::bool_u8;
use store_core::{day_key, Model, Store, SystemClock};

/// 测试用全局 model。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub id: u64,
    pub name: String,
    #[serde(with = "bool_u8")]
    pub deleted: bool,
    pub created_at: u64,
    pub updated_at: u64,
}

impl Item {
    pub fn new(name: &str) -> Self {
        Self {
            id: 0,
            name: name.into(),
            deleted: false,
            created_at: 0,
            updated_at: 0,
        }
    }
}

impl Model for Item {
    const MODEL_NAME: &'static str = "item";

    fn id(&self) -> u64 {
        self.id
    }
    fn set_id(&mut self, id: u64) {
        self.id = id;
    }
    fn deleted(&self) -> bool {
        self.deleted
    }
    fn set_deleted(&mut self, deleted: bool) {
        self.deleted = deleted;
    }
    fn created_at(&self) -> u64 {
        self.created_at
    }
    fn updated_at(&self) -> u64 {
        self.updated_at
    }
    fn set_timestamps(&mut self, created_at: u64, updated_at: u64) {
        self.created_at = created_at;
        self.updated_at = updated_at;
    }
}

/// 另一个未注册 model，用于 UnknownModel 测试。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ghost {
    pub id: u64,
    #[serde(with = "bool_u8")]
    pub deleted: bool,
    pub created_at: u64,
    pub updated_at: u64,
}

impl Model for Ghost {
    const MODEL_NAME: &'static str = "ghost";

    fn id(&self) -> u64 {
        self.id
    }
    fn set_id(&mut self, id: u64) {
        self.id = id;
    }
    fn deleted(&self) -> bool {
        self.deleted
    }
    fn set_deleted(&mut self, deleted: bool) {
        self.deleted = deleted;
    }
    fn created_at(&self) -> u64 {
        self.created_at
    }
    fn updated_at(&self) -> u64 {
        self.updated_at
    }
    fn set_timestamps(&mut self, created_at: u64, updated_at: u64) {
        self.created_at = created_at;
        self.updated_at = updated_at;
    }
}

pub fn new_store() -> Arc<Store> {
    Store::new(Arc::new(SystemClock))
}

pub fn today_item_csv(root: &Path) -> PathBuf {
    root.join(day_key(SystemClock::now_ms()))
        .join("_system")
        .join("item.csv")
}
