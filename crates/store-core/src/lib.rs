//! # store-core
//!
//! 通用内存表 + CSV 快照持久化框架，不含任何业务知识。
//!
//! ## 核心设计原则
//!
//! 1. **内存是唯一可信数据源**（Source of Truth）：所有 CRUD 只作用于内存。
//! 2. **CSV 只是持久化快照**：后台线程定时全量落盘，启动时按历史快照恢复。
//! 3. **外部不直接碰 CSV**：一切读写经 [`Store`] / [`Table`]。
//! 4. **并发安全**：表级 `RwLock` + 行级子锁；快照仅在锁内深 clone，序列化与 IO 在锁外。
//! 5. **不使用 channel 传递任务数据**：后台持久化线程由 `Condvar` 唤醒/停止。
//!
//! [`Store`]: crate::store::Store
//! [`Table`]: crate::table::Table

pub mod error;
pub mod fs;
pub mod model;
pub mod persistence;
pub mod store;
pub mod table;
pub mod time;

pub use error::{Result, StoreError};
pub use model::{AccountIndex, AccountModel, Model, StorageScope};
pub use persistence::{run_tick, spawn, PersistHandle, PersistOptions, TickOutcome};
pub use store::{FlushReport, RecoveryReport, Store, StoreConfig};
pub use table::Table;
pub use time::{day_key, Clock, MockClock, SystemClock};
