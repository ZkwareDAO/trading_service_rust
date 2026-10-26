//! Deribit 期权适配器（与 [`crate::binance`] 平级、同构）。
//!
//! * [`pricing`]：分档 tick 选取、市价开仓档位漂移、平仓对手价与价差闸门（纯函数，TDD）；
//! * [`spec`]：`get_instrument` 合约规格 → 统一 [`trading_models::OrderRule`]（纯解析）；
//! * [`prep`]：claim 前预检的纯转换（市价开仓漂移 / 平仓价差闸门 / 数量对齐 / 单号路由）；
//! * [`rpc`]：JSON-RPC over HTTPS（auth 换 token、各 method）与信封解析；
//! * [`venue`]：DeribitUser（ExchangeApi + 账户事件流）；
//! * [`mapping`]：私有 WS subscription 信封解析；
//! * [`market`]：主网公共行情 WS（持仓∪规则动态订阅、ticker 帧写 QuoteTable）；
//!
//! Deribit 适配器模块总览（接入与开平仓处理见各子模块）。

pub mod account_consumer;
pub mod instant;
pub mod mapping;
pub mod market;
pub mod prep;
pub mod pricing;
pub mod provider;
pub mod rpc;
pub mod spec;
pub mod user_stream;
pub mod venue;
