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
/// K线集 tick 聚合 sink（多合约 CandleAggregator + 主力过滤）。
pub mod bar_agg;
/// tick→1m bar 聚合纯状态机（交易日归桶 / 累计量差分）。
pub mod candle;
pub mod config;
pub mod core;
/// 主力合约选举（纯函数 pick_dominant + 品种前缀）。
pub mod dominant;
pub mod gateway;
pub mod instrument_dir;
pub mod leg_table;
pub mod mailbox;
pub mod mapping;
pub mod market;
/// CTP 单连接统一行情流（业务兴趣 ∪ K线集订阅、聚合路由、主力选举任务）。
pub mod market_stream;
pub mod order_ref;
pub mod pending_terminal;
pub mod place_error;
pub mod position_dir;
pub mod prep;
pub mod provider;
/// 合约规格/手续费率每日全量拉取短会话（一次性 trader 连接，阻塞式；管理 API 调用）。
pub mod spec_fetch;
pub mod venue;

pub use config::{from_values, CtpConfig, DEFAULT_SHFE_MARKET_DRIFT_TICKS};
