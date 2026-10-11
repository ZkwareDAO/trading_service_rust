//! Binance U 本位永续签名 SDK client 的按账户缓存。
//!
//! 官方 `binance-sdk` 负责签名/端点；本模块只按账户名 + 热加载凭证构造并缓存 SDK
//! `RestApi`（账户 key/secret 变更时整体重建）。具体的 ExchangeApi/AccountEventStream 实现
//! 在 [`super::venue`]。
//!
//! 注意：任何日志都不打印 api_key/api_secret。

use std::sync::Arc;

use binance_sdk::config::ConfigurationRestApi;
use binance_sdk::derivatives_trading_usds_futures::rest_api::RestApi;
use binance_sdk::derivatives_trading_usds_futures::DerivativesTradingUsdsFuturesRestApi;
use dashmap::DashMap;

use crate::port::ExchangeCredential;

/// Binance USDⓈ-M 环境（地址选择 + 请求超时，不含凭证/持仓模式）。
#[derive(Clone, Copy, Debug)]
pub struct BinanceFuturesOptions {
    pub testnet: bool,
    /// 单次 REST 请求超时（毫秒），由宿主 rest_timeout 透传。
    pub timeout_ms: u64,
}

impl BinanceFuturesOptions {
    pub fn new(testnet: bool, timeout_ms: u64) -> Self {
        Self {
            testnet,
            timeout_ms,
        }
    }
}

/// 缓存键：账户名 × 环境（正式网/测试网是两套独立端点与凭证）。
type CacheKey = (String, bool);

/// 一项缓存：签名 REST client + 建连时的凭证指纹（检测热加载后的 key/secret 轮换）。
struct CachedClient {
    rest: Arc<RestApi>,
    /// 与 (api_key, api_secret) 同生共死；任一变化都必须重建签名 client。
    fingerprint: (String, String),
}

impl CachedClient {
    fn new(rest: Arc<RestApi>, cred: &ExchangeCredential) -> Self {
        Self {
            rest,
            fingerprint: (cred.api_key.clone(), cred.api_secret.clone()),
        }
    }

    fn matches(&self, cred: &ExchangeCredential) -> bool {
        self.fingerprint == (cred.api_key.clone(), cred.api_secret.clone())
    }
}

/// 按 (账户, 环境) 缓存的签名 REST client。账户凭证变更或切换环境时自动重建。
#[derive(Clone, Default)]
pub struct BinanceFuturesClientCache {
    inner: Arc<DashMap<CacheKey, CachedClient>>,
}

impl BinanceFuturesClientCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// 取（或首次构造并缓存）某账户在指定环境的签名 client。
    ///
    /// 缓存键为 `(account, opts.testnet)`；命中且 (api_key, api_secret) 均未变即复用。
    /// 热加载后任一凭证变化（换钥/只改密）时原地重建，无需清空整张缓存。
    pub fn get_or_build(
        &self,
        account: &str,
        cred: &ExchangeCredential,
        opts: BinanceFuturesOptions,
    ) -> anyhow::Result<Arc<RestApi>> {
        let key = (account.to_string(), opts.testnet);
        if let Some(existing) = self.inner.get(&key) {
            if existing.matches(cred) {
                return Ok(Arc::clone(&existing.rest));
            }
        }
        let conf = ConfigurationRestApi::builder()
            .api_key(cred.api_key.clone())
            .api_secret(cred.api_secret.clone())
            // 请求超时与宿主 rest_timeout 一致：SDK 默认 1000ms 对实际下单/
            // get_positions 过短，会让长请求卡死、拉不到真值。
            .timeout(opts.timeout_ms.max(1))
            .build()?;
        let rest: RestApi = if opts.testnet {
            DerivativesTradingUsdsFuturesRestApi::testnet(conf)
        } else {
            DerivativesTradingUsdsFuturesRestApi::production(conf)
        };
        let rest = Arc::new(rest);
        self.inner.insert(
            (account.to_string(), opts.testnet),
            CachedClient::new(rest, cred),
        );
        Ok(self
            .inner
            .get(&(account.to_string(), opts.testnet))
            .unwrap()
            .rest
            .clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trading_models::PositionMode;

    fn cred_full(api_key: &str, api_secret: &str, is_testnet: bool) -> ExchangeCredential {
        ExchangeCredential {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            api_passphrase: None,
            position_mode: PositionMode::OneWay,
            is_testnet,
        }
    }

    fn cred(api_key: &str, is_testnet: bool) -> ExchangeCredential {
        cred_full(api_key, "secret", is_testnet)
    }

    #[test]
    fn prod_and_testnet_build_distinct_clients_per_account() {
        // Arrange：同一账户分别在正式网/测试网。
        let cache = BinanceFuturesClientCache::new();

        // Act
        let prod = cache
            .get_or_build(
                "alice",
                &cred("k1", false),
                BinanceFuturesOptions::new(false, 5000),
            )
            .unwrap();
        let testnet = cache
            .get_or_build(
                "alice",
                &cred("k1", true),
                BinanceFuturesOptions::new(true, 5000),
            )
            .unwrap();

        // Assert：两个环境各自独立，且命中复用同一 Arc。
        assert!(!Arc::ptr_eq(&prod, &testnet));
        let prod_again = cache
            .get_or_build(
                "alice",
                &cred("k1", false),
                BinanceFuturesOptions::new(false, 5000),
            )
            .unwrap();
        assert!(Arc::ptr_eq(&prod, &prod_again));
    }

    #[test]
    fn api_key_rotation_rebuilds_client() {
        // Arrange：账户先以 k1 建连。
        let cache = BinanceFuturesClientCache::new();
        let first = cache
            .get_or_build(
                "alice",
                &cred("k1", false),
                BinanceFuturesOptions::new(false, 5000),
            )
            .unwrap();

        // Act：热加载后 api_key 变更（同一账户、同一环境）。
        let rotated = cache
            .get_or_build(
                "alice",
                &cred("k2", false),
                BinanceFuturesOptions::new(false, 5000),
            )
            .unwrap();

        // Assert：检测到 key 轮换，重建为新 client；之后稳定复用新 client。
        assert!(!Arc::ptr_eq(&first, &rotated));
        let again = cache
            .get_or_build(
                "alice",
                &cred("k2", false),
                BinanceFuturesOptions::new(false, 5000),
            )
            .unwrap();
        assert!(Arc::ptr_eq(&rotated, &again));
    }

    #[test]
    fn secret_rotation_with_same_api_key_rebuilds_client() {
        // Arrange：账户先以 (k1, s1) 建连。
        let cache = BinanceFuturesClientCache::new();
        let first = cache
            .get_or_build(
                "alice",
                &cred_full("k1", "s1", false),
                BinanceFuturesOptions::new(false, 5000),
            )
            .unwrap();

        // Act：只重置 secret（api_key 不变）——热加载后监督器会重启账户流。
        let rotated = cache
            .get_or_build(
                "alice",
                &cred_full("k1", "s2", false),
                BinanceFuturesOptions::new(false, 5000),
            )
            .unwrap();

        // Assert：必须重建，否则旧 secret 的 client 会在 listenKey 申请上持续 401。
        assert!(!Arc::ptr_eq(&first, &rotated));
    }
}
