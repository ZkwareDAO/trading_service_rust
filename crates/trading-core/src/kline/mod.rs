//! K线采集相关的「纯内存、不落盘」高频表与订阅登记（与 [`crate::quote::QuoteTable`] 同层）。
//!
//! 都**不实现 Model、不注册 store-core、不做快照/恢复/日切**——进程重启后为空，
//! 由订阅重新填充。句柄以 `Arc` 跨线程共享，方法同步、无 `.await`。
//!
//! * [`TickTable`]：CTP 原始 tick 的 WAL 待刷盘窗口（append 每笔、批量 drain 后清空）；
//! * [`BarTable`]：定稿 bar 待刷盘窗口；
//! * [`LastVolumeTable`] / [`DominantTable`]：主力选举原料与结果；
//! * [`KlineRegistry`] / [`KlineKey`]：客户端订阅登记（引用计数、断连清理）；
//! * [`RegistrySource`]：登记表在某 (venue,itype,interval) 作用域的期望符号快照。
//!
//! 唯一的文件读写在 trading-store 的 KlineRepo（append-only CSV），本层不碰 IO。

mod bar;
mod dominant;
mod gap;
mod registry;
mod source;
mod tick;

pub use bar::{BarTable, DirtyBar, SeriesKey, SeriesStarts};
pub use dominant::{DominantEntry, DominantTable, LastVolumeTable};
pub use gap::{
    coalesce_starts, detect_tail_gap, scan_gaps, stall_gap, GapRange, GapRegistry, GapSeries,
    PendingGap,
};
pub use registry::{KlineKey, KlineRegistry};
pub use source::RegistrySource;
pub use tick::TickTable;
