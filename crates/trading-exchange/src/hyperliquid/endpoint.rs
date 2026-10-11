//! Hyperliquid 交易所端点与请求类型常量。
//!
//! Hyperliquid 的 REST 只有两个入口（`/info` 公开查询、`/exchange` 签名交易），
//! 通过请求体里的 `type` 字段区分具体方法；公开与私有的实时数据共用一条 WS。
//! 账户选网（正式 / 测试）只影响用户级 REST 与私有流；交易所级公共行情恒定正式网。

/// 正式网公开查询 / 交易 / WS 根地址。
pub(crate) const MAINNET_REST_INFO: &str = "https://api.hyperliquid.xyz/info";
pub(crate) const MAINNET_REST_EXCHANGE: &str = "https://api.hyperliquid.xyz/exchange";
pub(crate) const MAINNET_WS: &str = "wss://api.hyperliquid.xyz/ws";

/// 测试网根地址（账户 `is_testnet=true` 时使用）。
pub(crate) const TESTNET_REST_INFO: &str = "https://api.hyperliquid-testnet.xyz/info";
pub(crate) const TESTNET_REST_EXCHANGE: &str = "https://api.hyperliquid-testnet.xyz/exchange";
pub(crate) const TESTNET_WS: &str = "wss://api.hyperliquid-testnet.xyz/ws";

/// 按账户选网返回（info 端点，exchange 端点，ws 地址）。
pub(crate) fn user_endpoints(is_testnet: bool) -> (&'static str, &'static str, &'static str) {
    if is_testnet {
        (TESTNET_REST_INFO, TESTNET_REST_EXCHANGE, TESTNET_WS)
    } else {
        (MAINNET_REST_INFO, MAINNET_REST_EXCHANGE, MAINNET_WS)
    }
}
