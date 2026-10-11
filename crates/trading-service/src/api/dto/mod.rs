//! REST DTO：与持久化 model 分离的报文投影。
//!
//! 所有 DTO 只在此子模块定义，handler 禁止直接序列化 model：
//! bool 出 `true/false`、枚举出线上字符串、Decimal 出精确 JSON 数字，
//! 且不下发 qty_unit、密钥、内部行锁等实现细节。

pub mod account;
pub mod order;
pub mod position;
pub mod rule;
