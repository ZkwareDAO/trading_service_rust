//! Xili 交易所套件（[`VenueProvider`] 实现，与 [`crate::binance::provider`] 平级）。
//!
//! 归属判定按端点配置的交易所名（默认含 `xili`，可经 `--exchange name=url` 覆盖/新增）；
//! 账户事件侧与真实所一致：监督器为每个活动 (xili, account) 起一条外向 WS。

use std::sync::Arc;

use reqwest::Client;
use tokio::task::JoinHandle;

use crate::api::ExchangeApi;
use crate::port::OrderEventSink;
use crate::provider::{VenueEndpoint, VenueProvider};
use crate::xili::user_stream;
use crate::xili::{with_default_endpoint, XiliExchange};

/// Xili 交易所套件：持有所属端点（含默认）与共享 HTTP client。
pub struct XiliProvider {
    endpoints: Arc<Vec<VenueEndpoint>>,
    http: Client,
}

impl XiliProvider {
    pub fn new(endpoints: Vec<VenueEndpoint>, http: Client) -> Self {
        // 未显式配置 xili 时补默认端点，使开箱即用；显式配置则以配置为准。
        Self {
            endpoints: Arc::new(with_default_endpoint(endpoints)),
            http,
        }
    }

    fn endpoint(&self, exchange: &str) -> Option<&VenueEndpoint> {
        self.endpoints.iter().find(|m| m.name == exchange)
    }
}

impl VenueProvider for XiliProvider {
    fn matches(&self, exchange: &str) -> bool {
        self.endpoint(exchange).is_some()
    }

    fn build_api(
        &self,
        exchange: &str,
        _instrument_type: Option<&str>,
        account: &str,
    ) -> anyhow::Result<Arc<dyn ExchangeApi>> {
        let ep = self
            .endpoint(exchange)
            .ok_or_else(|| anyhow::anyhow!("未配置的 xili 交易所 `{exchange}`"))?;
        Ok(Arc::new(XiliExchange::new(
            ep.name.clone(),
            ep.rest_base_url.clone(),
            self.http.clone(),
            account,
        )))
    }

    /// 与真实所一致：为每个活动账户起一条外向账户事件 WS。
    fn spawn_account_stream(
        &self,
        exchange: &str,
        account: &str,
        sink: Arc<dyn OrderEventSink>,
    ) -> Option<JoinHandle<()>> {
        let ep = self.endpoint(exchange)?.clone();
        Some(user_stream::spawn(sink, ep, account.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::xili::DEFAULT_NAME;
    use async_trait::async_trait;
    use trading_core::protocol::OrderEvent;

    /// 不联网的订单回报出口。
    struct NoopSink;
    #[async_trait]
    impl OrderEventSink for NoopSink {
        async fn on_order_event(&self, _: &OrderEvent, _: &str, _: &str) {}
    }

    fn provider_with(endpoints: Vec<VenueEndpoint>) -> XiliProvider {
        XiliProvider::new(endpoints, Client::new())
    }

    #[test]
    fn matches_default_xili_and_custom_endpoint_name() {
        // 无配置：默认 xili 可用。
        let p = provider_with(vec![]);
        assert!(p.matches("xili"));
        // 大小写敏感：XILI 不命中（异常名按未知所处理）。
        assert!(!p.matches("XILI"));
        assert!(!p.matches("binance"));

        // 自定义端点名（同构的另一个 xili 实例）也归本 provider。
        let p = provider_with(vec![VenueEndpoint {
            name: "play".into(),
            rest_base_url: "http://127.0.0.1:9300".into(),
            ws_url: "ws://127.0.0.1:9300/ws".into(),
        }]);
        assert!(p.matches("play"));
        assert!(p.matches("xili")); // 默认仍在
    }

    #[test]
    fn default_name_constant_is_stable() {
        // 明确断言默认名常量，防止命名漂移（账户匹配/端点匹配都依赖它）。
        assert_eq!(DEFAULT_NAME, "xili");
    }

    #[test]
    fn build_api_errors_for_unknown_endpoint() {
        let p = provider_with(vec![]);
        assert!(p.build_api("nope", None, "acc").is_err());
        assert!(p.build_api("xili", None, "acc").is_ok());
    }

    #[tokio::test]
    async fn account_stream_is_per_account_for_known_endpoint() {
        let p = provider_with(vec![]);
        let sink: Arc<dyn OrderEventSink> = Arc::new(NoopSink);
        // 未知端点：不支持（返回 None），注册表将走占位/报错路径。
        assert!(p
            .spawn_account_stream("nope", "acc", Arc::clone(&sink))
            .is_none());
        // 已知端点：每账户一条任务（返回 Some 句柄）。
        let handle = p.spawn_account_stream("xili", "acc", Arc::clone(&sink));
        assert!(handle.is_some());
        // 清理：避免该任务在测试进程里空转重连。
        handle.unwrap().abort();
    }
}
