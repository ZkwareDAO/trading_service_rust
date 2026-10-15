//! CTP 国内期货适配器（feature="ctp"）。
//!
//! 分层：
//! - [`core`]：柜台无关的领域类型/枚举/校验（不依赖 ctp2rs，纯逻辑可单测）；
//! - [`gateway`]：FFI 终端边界，封装 vendored ctp2rs 的连接、SPI 事件流与
//!   请求入口（`unsafe` 仅限本层；内部 `std::sync::mpsc` 是 C++ 回调线程 →
//!   Rust 调用方的 FFI 适配惯用实现，不外泄到业务层）。
//!
//! 后续 venue 层（venue.rs / prep.rs / mapping.rs 等，实现 `VenueProvider`
//! 套件）也挂在本模块下，CTP 的同步阻塞细节全部收口于此。

pub mod account_consumer;
pub mod api;
pub mod config;
pub mod core;
pub mod exchange_dir;
pub mod gateway;
pub mod mailbox;
pub mod mapping;
pub mod market;
pub mod order_ref;
pub mod prep;
pub mod provider;
pub mod venue;

pub use config::{from_values, CtpConfig};
