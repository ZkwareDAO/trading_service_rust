//! 交易所接入层 crate（trading-exchange）
//!
//! 收口各交易所（Binance、xili 等）的行情流、交易 API、账户事件流、
//! 规则拉取与对账运行时。本 crate 只向下依赖 trading-core / trading-models，
//! 不反向依赖 trading-service；对宿主的出站需求经 [`port`] 端口倒置。

pub mod account_stream;
pub mod api;
pub mod backoff;
pub mod heartbeat;
pub mod port;
pub mod position_profile;
pub mod provider;
pub mod reconcile;
pub mod registry;
pub mod runtime;
pub mod translate;
pub(crate) mod ws;

/// 各交易所适配器实现。
pub mod binance;
/// Deribit 期权适配器（与 binance 平级、同构）。
pub mod deribit;
/// Hyperliquid 永续合约适配器（与 binance/deribit 平级、同构）。
pub mod hyperliquid;
/// 本地模拟交易所 xili 适配器（教学案例所，与 binance 平级、同构）。
pub mod xili;

/// CTP 国内期货适配器：核心类型 + FFI 网关，仅在 feature="ctp" 下编译。
/// 运行期经 libloading 动态加载柜台 .so，路径与生产模式开关走 venue 配置。
#[cfg(feature = "ctp")]
pub mod ctp;

/// 仓位流水线档案（各交易所 VenueProvider 自描述），crate 根直接可用。
pub use position_profile::PositionProfile;
