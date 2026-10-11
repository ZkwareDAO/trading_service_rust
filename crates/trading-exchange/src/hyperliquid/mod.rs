//! Hyperliquid 永续合约适配器（与 [`crate::binance`] / [`crate::deribit`] 平级、同构）。
//!
//! 系统约定 Hyperliquid 合约的 `instrument_type=futures`；接口交易用裸币名（`BTC`），
//! 策略端的 `BTCUSDC` 在 codec 边界去后缀。Hyperliquid 无真市价单，市价在适配器内
//! 转为「mid ± slippage」的 IOC 限价单。
//!
//! 模块按职责划分：
//! * [`endpoint`]：正式 / 测试网的 REST/WS 端点常量；
//! * `meta`：`meta` 响应纯解析 → 统一 `OrderRule`；
//! * `codec`：币名（BTCUSDC↔BTC）与 coin↔asset 整数索引；
//! * `pricing`：滑点价、5 位有效数字取整、数量对齐（纯 Decimal）；
//! * `signature`：EVM 地址推导与 EIP-712 L1 action 签名；
//! * `venue`：HyperliquidUser（ExchangeApi + 账户事件流）；
//! * `user_stream` / `account_consumer`：认证 WS 与通用账户消费循环适配；
//! * `market` / `services` / `provider`：公共行情、交易所级服务、套件注册。
//!
//! 见 `docs/扩展交易所.md` 第三节、`docs/扩展交易所.md` 第六节。

pub(crate) mod account_consumer;
pub(crate) mod codec;
pub(crate) mod endpoint;
pub(crate) mod market;
pub(crate) mod meta;
pub(crate) mod pricing;
/// 对外暴露：HyperliquidConfig 由 trading-service 装配引用。
pub mod provider;
pub(crate) mod services;
pub(crate) mod signature;
pub(crate) mod user_stream;
pub(crate) mod venue;
