//! CTP 套件 provider：实现 [`VenueProvider`]，由注册表登记。
//!
//! CTP 是"同步阻塞特例"：建连/下单/查询全部收口在专用 OS 线程
//! （见 [`crate::ctp::venue`]），本 provider 只是装配壳。

use std::sync::Arc;

use std::sync::atomic::AtomicBool;

use tokio::task::JoinHandle;

use crate::ctp::account_consumer::{CtpVenueConnector, SessionRegistry};
use crate::ctp::core::account::CtpAccount;
use crate::ctp::{api::CtpApi, CtpConfig};
use crate::port::{ExchangeCredential, OrderEventSink};
use crate::position_profile::PositionProfile;
use crate::provider::{ProviderDeps, VenueProvider};

/// ctp 唯一规范名（大小写敏感精确匹配，无别名）。
pub const NAME: &str = "ctp";

/// CTP 套件 provider（持有装配依赖与运行参数）。
pub struct CtpProvider {
    deps: ProviderDeps,
    /// CTP 运行参数（.so 路径/流目录基准，来自 venue 配置）。
    config: CtpConfig,
    /// 公共行情线程停机标志（行情是 std::thread，同样只能靠标志退出）。
    md_stop: Arc<std::sync::atomic::AtomicBool>,
    /// 各账户会话注册表（停机/映射/信箱；**provider 级 + 每账户隔离**：
    /// 连接器逐次新建，这些状态必须跨热加载存活）。
    sessions: Arc<SessionRegistry>,
}

impl CtpProvider {
    pub fn new(deps: ProviderDeps, config: CtpConfig) -> Self {
        Self {
            deps,
            config,
            md_stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            sessions: Arc::new(SessionRegistry::default()),
        }
    }

    /// 账户当前会话的停机标志（接线测试/诊断用）。
    pub fn current_shutdown(&self, account: &str) -> Option<Arc<AtomicBool>> {
        self.sessions.current_shutdown(account)
    }

    /// 优雅停机：置位全部账户会话停机标志（`spawn_blocking` 线程无法
    /// abort，停机标志是唯一的停机通道）。
    pub fn stop_account_streams(&self) {
        self.sessions.shutdown_all();
        self.md_stop
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// 解析 CTP 凭证并组装领域账户（非 CTP 形态凭证报错）。
    pub fn ctp_account(&self, account: &str) -> anyhow::Result<CtpAccount> {
        let cred: ExchangeCredential =
            self.deps
                .credentials
                .resolve(account, NAME)
                .ok_or_else(|| {
                    anyhow::anyhow!("ctp 需要在 account.csv 配置 exchange=ctp 的账户 `{account}`")
                })?;
        let c = cred
            .as_ctp()
            .ok_or_else(|| anyhow::anyhow!("ctp 凭证形态错误：应为 Ctp（账户 `{account}`）"))?;
        Ok(CtpAccount {
            broker_id: c.broker_id.to_string(),
            user_id: c.user_id.to_string(),
            password: c.password.to_string(),
            app_id: c.app_id.to_string(),
            auth_code: c.auth_code.to_string(),
            investor_id: c.investor_id.to_string(),
            front_address: c.front_address.to_string(),
            is_production_mode: c.is_production_mode,
        })
    }
}

impl CtpProvider {
    /// 行情登录用的账户凭证来源：CTP 行情登录需要 BrokerID/UserID 结构，
    /// 但交易所级行情不随用户区分，账户名由 venue.toml `[ctp].md_account`
    /// 指定（缺省不启动行情）。
    fn market_data_account(&self) -> Option<CtpAccount> {
        let name = self.config.md_account.clone()?;
        self.ctp_account(&name).ok()
    }
}

impl VenueProvider for CtpProvider {
    fn matches(&self, exchange: &str) -> bool {
        exchange.trim() == NAME
    }

    fn position_profile(&self) -> Option<PositionProfile> {
        Some(PositionProfile::ctp())
    }

    /// CTP 合约号原样（大小写敏感，仅 trim）。
    fn normalize_instrument(&self, _exchange: &str, instrument: &str) -> String {
        instrument.trim().to_string()
    }

    fn build_api(
        &self,
        _exchange: &str,
        _instrument_type: Option<&str>,
        account: &str,
    ) -> anyhow::Result<Arc<dyn crate::api::ExchangeApi>> {
        // ctp_account 兼作账户存在性校验（未知账户 fail-fast）。
        let _acct = self.ctp_account(account)?;
        // 本账户的会话私有实例（幂等创建；与回报翻译/会话线程同一份）。
        let session = self.sessions.session(account);
        Ok(Arc::new(CtpApi::new(
            account.to_string(),
            Arc::clone(&session.order_refs),
            Arc::clone(&session.inserts),
        )))
    }

    fn spawn_market_services(&self, active_exchanges: &[String]) {
        if !active_exchanges.iter().any(|name| name.trim() == NAME) {
            tracing::info!(provider = NAME, "无该交易所账户，跳过 CTP 行情连接");
            return;
        }
        // 行情前置未配置（venue.toml [ctp].md_front 为空）= 部署未启用行情。
        if self.config.md_front.trim().is_empty() {
            tracing::warn!("CTP 行情前置未配置（venue.toml [ctp].md_front），跳过行情");
            return;
        }
        // 公共行情：可降级（行情前置未开仅告警不重试）；兴趣集由宿主注入。
        crate::ctp::market::spawn_market_worker(
            crate::ctp::market::MdParams {
                // 行情前置需要合法账户结构（BrokerID/UserID 编码进登录请求），
                // 无活动账户时上面已 return，这里取行情账户的领域凭证。
                account: match self.market_data_account() {
                    Some(a) => a,
                    None => {
                        tracing::warn!(
                            "CTP 行情缺账户名来源（venue.toml [ctp].md_account），跳过行情"
                        );
                        return;
                    }
                },
                config: self.config.clone(),
            },
            self.deps.market_interest.clone(),
            Arc::clone(&self.deps.quotes),
            self.deps.quote_observer.clone(),
            Arc::clone(&self.md_stop),
        );
    }

    fn spawn_account_stream(
        &self,
        exchange: &str,
        account: &str,
        sink: Arc<dyn OrderEventSink>,
    ) -> Option<JoinHandle<()>> {
        let acct = self.ctp_account(account).ok()?;
        // 热加载接管：置位旧会话停机标志并分配全新私有实例（含信箱/映射）。
        let session = self.sessions.take_over(account);
        CtpVenueConnector::new(
            sink,
            self.deps.position_sink.clone(),
            self.deps.position_debounce,
            self.deps.position_reconcile_interval,
            self.config.clone(),
            session,
        )
        .spawn_for(exchange, account, acct)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::port::CredentialResolver;
    use std::sync::Mutex;

    /// 测试桩：按 (account, exchange) 返回预置凭证。
    struct StubResolver(Mutex<std::collections::HashMap<(String, String), ExchangeCredential>>);

    impl CredentialResolver for StubResolver {
        fn resolve(&self, account: &str, exchange: &str) -> Option<ExchangeCredential> {
            self.0
                .lock()
                .unwrap()
                .get(&(account.to_string(), exchange.to_string()))
                .cloned()
        }
    }

    fn ctp_cred() -> ExchangeCredential {
        ExchangeCredential::Ctp {
            broker_id: "9999".into(),
            user_id: "100001".into(),
            password: "pwd".into(),
            app_id: "simnow_client".into(),
            auth_code: "0000000000000000".into(),
            investor_id: String::new(),
            front_address: "tcp://182.254.243.31:40001".into(),
            is_production_mode: false,
        }
    }

    /// 空依赖桩：仅 credentials 字段被 provider 消费，其余用最小占位。
    fn deps_with(creds: Arc<dyn CredentialResolver>) -> ProviderDeps {
        ProviderDeps {
            http: reqwest::Client::new(),
            rules: Arc::new(trading_core::order_rule::OrderRuleTable::new()),
            quotes: Arc::new(trading_core::quote::QuoteTable::new()),
            credentials: creds,
            rest_timeout: std::time::Duration::from_secs(5),
            rule_refresh_interval: std::time::Duration::from_secs(3600),
            position_debounce: std::time::Duration::from_millis(500),
            position_reconcile_interval: std::time::Duration::from_secs(60),
            position_sink: Arc::new(NoopPositionSink),
            quote_observer: None,
            endpoints: Arc::new(vec![]),
            venue: crate::deribit::prep::PrepConfig {
                drift_ticks: 5,
                spread_threshold: rust_decimal::Decimal::new(5, 3),
            },
            hyperliquid: crate::hyperliquid::provider::HyperliquidConfig::default_config(),
            market_interest: None,
            ctp: test_config(),
        }
    }

    /// 空实现订单事件接收口（接线测试用）。
    struct NoopOrderSink;
    #[async_trait::async_trait]
    impl crate::port::OrderEventSink for NoopOrderSink {
        async fn on_order_event(
            &self,
            _ev: &trading_core::protocol::OrderEvent,
            _source: &str,
            _account: &str,
        ) {
        }
    }

    struct NoopPositionSink;
    #[async_trait::async_trait]
    impl crate::port::PositionSnapshotSink for NoopPositionSink {
        async fn on_position_snapshot(
            &self,
            _exchange: &str,
            _account: &str,
            _positions: &[crate::api::PositionInfo],
        ) {
        }
    }

    fn test_config() -> CtpConfig {
        CtpConfig::for_tests()
    }

    #[test]
    fn matches_is_exact_name() {
        // 大小写敏感精确匹配，无别名；trim 后相等视为匹配（与其它所口径一致）。
        let p = CtpProvider::new(
            deps_with(Arc::new(StubResolver(Mutex::new(
                std::collections::HashMap::new(),
            )))),
            test_config(),
        );
        assert!(p.matches("ctp"));
        assert!(!p.matches("CTP"));
        assert!(!p.matches("binance"));
    }

    #[test]
    fn ctp_account_builds_from_credential() {
        let mut stub = std::collections::HashMap::new();
        stub.insert(("acct".into(), "ctp".into()), ctp_cred());
        let p = CtpProvider::new(
            deps_with(Arc::new(StubResolver(Mutex::new(stub)))),
            test_config(),
        );
        let acct = p.ctp_account("acct").unwrap();
        assert_eq!(acct.broker_id, "9999");
        assert_eq!(acct.user_id, "100001");
        assert_eq!(acct.front_address, "tcp://182.254.243.31:40001");
    }

    #[test]
    fn ctp_account_carries_production_mode() {
        // Arrange：账户凭证声明生产柜台（评测版合并 API 的实例级开关归账户所有）。
        let mut stub = std::collections::HashMap::new();
        stub.insert(
            ("acct".into(), "ctp".into()),
            ExchangeCredential::Ctp {
                broker_id: "4000".into(),
                user_id: "200001".into(),
                password: "pwd".into(),
                app_id: "app".into(),
                auth_code: "code".into(),
                investor_id: String::new(),
                front_address: "tcp://prod:1".into(),
                is_production_mode: true,
            },
        );
        let p = CtpProvider::new(
            deps_with(Arc::new(StubResolver(Mutex::new(stub)))),
            test_config(),
        );

        // Act / Assert：环境归属随账户走（不再取自 toml 的进程级配置）。
        assert!(p.ctp_account("acct").unwrap().is_production_mode);
    }

    #[tokio::test]
    async fn hot_reload_second_spawn_sets_previous_shutdown_flag() {
        // Arrange：同一 provider（同一停机注册表）；stub .so 路径不存在的会话线程只用于验证接线。
        let mut stub = std::collections::HashMap::new();
        stub.insert(("acct".into(), "ctp".into()), ctp_cred());
        let p = CtpProvider::new(
            deps_with(Arc::new(StubResolver(Mutex::new(stub)))),
            test_config(),
        );
        let sink: Arc<dyn OrderEventSink> = Arc::new(NoopOrderSink);

        // Act：首次 spawn，取注册表中的当前标志。
        p.spawn_account_stream("ctp", "acct", Arc::clone(&sink))
            .expect("首次 spawn");
        let old_flag = p.current_shutdown("acct").expect("注册表应有该账户标志");
        assert!(!old_flag.load(std::sync::atomic::Ordering::SeqCst));

        // 热加载：同名账户二次 spawn（同一 provider 级注册表）。
        p.spawn_account_stream("ctp", "acct", Arc::clone(&sink))
            .expect("二次 spawn");

        // Assert：旧会话标志被置位（旧线程在下一轮退出，不再双会话并跑）。
        assert!(old_flag.load(std::sync::atomic::Ordering::SeqCst));
        // 优雅停机：置位全部标志，会话线程随后退出。
        p.stop_account_streams();
    }

    #[test]
    fn ctp_account_missing_credential_errors() {
        let p = CtpProvider::new(
            deps_with(Arc::new(StubResolver(Mutex::new(
                std::collections::HashMap::new(),
            )))),
            test_config(),
        );
        assert!(p.ctp_account("nobody").is_err());
    }

    #[test]
    fn ctp_account_rejects_apisecret_form() {
        let mut stub = std::collections::HashMap::new();
        stub.insert(
            ("acct".into(), "ctp".into()),
            ExchangeCredential::ApiSecret {
                api_key: "k".into(),
                api_secret: "s".into(),
                api_passphrase: None,
                position_mode: trading_models::PositionMode::OneWay,
                is_testnet: false,
            },
        );
        let p = CtpProvider::new(
            deps_with(Arc::new(StubResolver(Mutex::new(stub)))),
            test_config(),
        );
        assert!(p.ctp_account("acct").is_err());
    }

    #[test]
    fn position_profile_declares_ctp_futures() {
        let p = CtpProvider::new(
            deps_with(Arc::new(StubResolver(Mutex::new(
                std::collections::HashMap::new(),
            )))),
            test_config(),
        );
        let profile = p.position_profile().unwrap();
        assert_eq!(profile.exchange, "ctp");
        assert_eq!(
            profile.instrument_type,
            trading_models::InstrumentType::Futures
        );
        assert_eq!(profile.quote_venue, trading_models::Venue::Ctp);
    }
}
