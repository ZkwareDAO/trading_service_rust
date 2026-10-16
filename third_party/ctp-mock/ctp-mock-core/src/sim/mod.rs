//! 模拟器内核：会话协议、查询闸门/分页、成交脚本、最小自洽台账、行情与场景。
//!
//! 纯逻辑集中在各子模块（L1 单测主体）；与线程/SPI 的粘合在 [`self::front`]。

pub mod frames;
pub mod front;
pub mod ledger;
pub mod market;
pub mod md_front;
pub mod script;
pub mod session;
