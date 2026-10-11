//! 仓位流程（trading-service 侧）。
//!
//! 分层约定：
//! * 纯计算（单仓重估、批量重估、对账裁决）在 **trading-models**
//!   （`Position::revalued` / `revalue_positions` / `diff_positions`）；
//! * 内存读写与持久化收口在 **trading-store** 的 `PositionRepo`；
//! * 本模块只做**编排**与交易所口径映射（[`engine::PositionEngine`]）：
//!   ACCOUNT_UPDATE 去抖到点后的 REST 快照对账、行情写价后的仅本所仓位重估。

pub mod engine;

pub use engine::{AccountLookup, PositionEngine};
