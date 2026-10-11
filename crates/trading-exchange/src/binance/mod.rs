//! Binance 适配器：官方 `binance-sdk` 负责签名/端点，本模块负责领域映射与生命周期。
//!
//! * [`mapping`]：用户数据流事件 -> 领域报文（纯函数，可单测）；
//! * [`prep`]：下单/撤查单的纯装配（规则校验、精度对齐、单号路由，不碰网络/SDK）；
//! * [`rules`]：`exchangeInfo` -> 统一规则行（纯解析内核）；
//! * [`client`]：按账户名 + 凭证缓存签名 SDK client；
//! * [`venue`]：BinanceUser（REST + 私有流）/ BinanceRuleFetcher（公开规则）；
//! * [`market`]：公开行情 WS 与写内存行情表的转发 worker；
//! * [`user_stream`]：账户事件流连接器（经宿主端口取凭证/用户主体/回报订单事件）。

pub mod client;
pub mod mapping;
pub mod market;
pub mod prep;
pub mod provider;
pub mod rules;
pub mod services;
pub mod user_stream;
pub mod venue;
