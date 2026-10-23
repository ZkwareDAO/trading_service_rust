//! 交易所接入层 crate（trading-exchange）
//!
//! 收口各交易所（Binance、Deribit、Hyperliquid、CTP、xili 等）的行情流、交易 API、
//! 账户事件流、规则拉取与对账运行时。本 crate 只向下依赖 trading-core / trading-models，
//! 不反向依赖 trading-service；对宿主的出站需求经 [`port`] 端口倒置。
//!
//! # 统一行情流（Plan「行情 Hub + 统一 MarketDataStream」）
//! - [`market`]：对外契约 [`market::MarketDataStream`]（`subscribe_prices` /
//!   `subscribe_klines`，凭证式 `Arc<dyn DesiredSource>` + guard 退订），K线/价格 tick
//!   等值类型；四家（binance/deribit/hyperliquid/ctp）实现两方法，xili 返回 Unsupported。
//! - [`market_hub`]：消费者注册表 / 全局并集 / 订阅 diff / 按 symbol 路由 /
//!   `catch_unwind`+单次耗时摘除异常 sink（纯内核、零 channel、`Arc<Mutex>` 短临界区）。
//! - [`market_ws`]：WS 三家共用的唯一连接生命周期（退避/10s Ping/5s 看门狗 20s 判死/
//!   500ms 对账/重连整体重放）+ [`market_ws::WsMarketFeed`] 协议端口（各所只留协议纯函数）。
//! - CTP 是单连接阻塞特例：业务兴趣 ∪ K线期望共用一条 `ctp-md` 线程，tick 按作用域
//!   分流（业务集→QuoteTable/observer 原样；K线集→`ctp::market_stream` 聚合/选举/三表）。
//!
//! 取行情流经 [`registry::ExchangeRegistry::market_stream`]（对称 `connect/build_api`）。

pub mod account_stream;
pub mod api;
pub mod backoff;
pub mod heartbeat;
/// 行情统一入口对外类型（MarketDataStream/DesiredSource/BarSink/Kline 等）。
pub mod market;
/// hub 纯内核：消费者注册表 / 全局并集 / 订阅 diff / 按 symbol 路由 / 异常 sink 摘除。
pub mod market_hub;
/// WS 三家共用的唯一连接生命周期 + WsMarketFeed 协议端口。
pub mod market_ws;
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
