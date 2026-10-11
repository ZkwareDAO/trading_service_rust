//! CSV 快照持久化：文件布局、原子写、目录扫描、后台定时调度。

pub(crate) mod paths;
mod scheduler;

pub use paths::{sanitize_dir_name, scan, DayFile};
pub use scheduler::{run_tick, spawn, PersistHandle, PersistOptions, TickOutcome};
