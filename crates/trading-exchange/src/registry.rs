//! 交易所注册表：按名将请求路由给已登记的 [`VenueProvider`]，自身不含任何具体所知识。
//!
//! 加一家交易所 = 实现一个 provider + 在注册清单（见 `builtin`）登记一行，
//! 本文件与上层 main/service 均无需改动（开闭原则）。

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::api::ExchangeApi;
use crate::port::{AccountTargets, OrderEventSink};
use crate::position_profile::PositionProfile;
use crate::provider::VenueProvider;
use crate::runtime::{Supervisor, VenueConnector};
use crate::xili::provider::XiliProvider;

/// 已登记交易所套件的注册表（纯路由）。
pub struct ExchangeRegistry {
    providers: Vec<Arc<dyn VenueProvider>>,
}

impl ExchangeRegistry {
    /// 由一组 provider 构造（测试与自定义装配用；生产装配见 [`Self::builtin`]）。
    pub fn from_providers(providers: Vec<Arc<dyn VenueProvider>>) -> Self {
        Self { providers }
    }

    /// 生产装配：登记内置交易所套件。
    ///
    /// **扩展点**：将来加一家交易所，只需新建其 provider 模块并在此 push 一行，
    /// 本注册表路由逻辑与上层 main/service 都不改动。
    pub fn builtin(deps: crate::provider::ProviderDeps) -> Self {
        let http = deps.http.clone();
        let endpoints = (*deps.endpoints).clone();
        let deribit: Arc<dyn VenueProvider> =
            Arc::new(crate::deribit::provider::DeribitProvider::new(deps.clone()));
        let hyperliquid: Arc<dyn VenueProvider> = Arc::new(
            crate::hyperliquid::provider::HyperliquidProvider::new(deps.clone()),
        );
        let binance: Arc<dyn VenueProvider> =
            Arc::new(crate::binance::provider::BinanceProvider::new(deps.clone()));
        let xili: Arc<dyn VenueProvider> = Arc::new(XiliProvider::new(endpoints, http));
        // CTP（feature="ctp"）：同步阻塞特例，.so/生产模式开关经 CtpConfig 注入。
        #[cfg(feature = "ctp")]
        let ctp: Arc<dyn VenueProvider> = Arc::new(crate::ctp::provider::CtpProvider::new(
            deps.clone(),
            deps.ctp.clone(),
        ));
        // xili 排在前：同名端点名时本地模拟所优先（便于教学/联调覆盖）。
        #[cfg(feature = "ctp")]
        return Self::from_providers(vec![xili, deribit, hyperliquid, binance, ctp]);
        #[cfg(not(feature = "ctp"))]
        Self::from_providers(vec![xili, deribit, hyperliquid, binance])
    }

    /// 已登记 provider 列表（供复合连接器使用）。
    pub fn providers(&self) -> Vec<Arc<dyn VenueProvider>> {
        self.providers.clone()
    }

    /// 汇总所有 provider 自描述的仓位档案（用于 engine 启动装配 + `load_and_verify`）。
    ///
    /// 未接入仓位流水线的 provider（如 xili）返回 [`None`] 被自然滤掉；本方法只做
    /// 聚合，不做去重/校验——重复 exchange 等装配异常交由 engine 自检 fail-fast 暴露。
    pub fn position_profiles(&self) -> Vec<PositionProfile> {
        self.providers
            .iter()
            .filter_map(|p| p.position_profile())
            .collect()
    }

    /// 按精确名找到归属的 provider（大小写敏感、无别名）。
    fn provider(&self, exchange: &str) -> Option<&Arc<dyn VenueProvider>> {
        self.providers.iter().find(|p| p.matches(exchange))
    }

    /// 该交易所是否已注册（大小写敏感精确名）。宿主下单前据此拒绝未知所；
    /// 账户匹配直接用该精确名，不再有候选名/别名概念。
    pub fn is_supported(&self, exchange: &str) -> bool {
        self.provider(exchange).is_some()
    }

    /// 入站标的符号按归属所归一（未注册所仅 trim）。供宿主写内存订单前调用。
    pub fn normalize_instrument(&self, exchange: &str, instrument: &str) -> String {
        match self.provider(exchange) {
            Some(p) => p.normalize_instrument(exchange, instrument),
            None => instrument.trim().to_string(),
        }
    }

    /// claim/建单前预检：路由给匹配 provider；未注册的所原样透传。
    ///
    /// 引擎在 claim_self 前调用；返回 [`PlaceDefer`] 时不 claim/建单。
    pub fn prepare_placement(
        &self,
        exchange: &str,
        request: crate::api::PlaceOrderRequest,
    ) -> Result<crate::api::PlaceOrderRequest, crate::api::PlaceDefer> {
        match self.provider(exchange) {
            Some(p) => p.prepare_placement(exchange, request),
            None => Ok(request),
        }
    }

    /// 构造某 (交易所, 产品, 账户) 的下单/查询主体；未注册的所返回错误。
    pub fn connect(
        &self,
        exchange: &str,
        instrument_type: Option<&str>,
        account: &str,
    ) -> anyhow::Result<Arc<dyn ExchangeApi>> {
        match self.provider(exchange) {
            Some(p) => p.build_api(exchange, instrument_type, account),
            None => anyhow::bail!("未知交易所 `{exchange}`：未注册对应 provider/端点"),
        }
    }

    /// 启动各所的交易所级共享任务（规则同步/公共行情），逐所 fan-out。
    ///
    /// `active_exchanges` 是宿主从账户内存表去重出的交易所名；各 provider 自行判定
    /// 自己归属的所是否在列，不在列则不建连（去硬编码：无账户的所不外连）。
    pub fn spawn_market_services(&self, active_exchanges: &[String]) {
        for p in &self.providers {
            p.spawn_market_services(active_exchanges);
        }
    }

    /// 一次性启动账户事件侧：按账户的对账循环，返回关闭句柄。
    pub fn spawn_account_events(
        self: &Arc<Self>,
        sink: Arc<dyn OrderEventSink>,
        targets: Arc<dyn AccountTargets>,
        interval: Duration,
        stop: Arc<AtomicBool>,
    ) -> AccountEventsHandle {
        // per-account：supervisor 把期望目标集分派给支持的 provider。
        let supervisor = Supervisor::new();
        let connector = RegistryConnector::new(self.providers(), sink);
        let stop_connector = connector.clone();
        let running = supervisor.clone();
        tokio::spawn(async move {
            use std::sync::atomic::Ordering;
            let mut tick = tokio::time::interval(interval);
            while !stop.load(Ordering::Relaxed) {
                tick.tick().await;
                running.reconcile(&targets.active_targets(), &connector);
            }
        });
        AccountEventsHandle {
            supervisor,
            connector: stop_connector,
        }
    }
}

/// 账户事件侧句柄：优雅关闭时停掉全部按账户的长连接。
pub struct AccountEventsHandle {
    supervisor: Supervisor,
    /// 连接器（provider 停机钩子经它分发——CTP 会话线程必须靠停机标志退出）。
    connector: RegistryConnector,
}

impl AccountEventsHandle {
    pub fn stop_all(&self) {
        // 先置位阻塞线程的停机标志（abort 对 spawn_blocking 无效），再回收任务句柄。
        self.connector.stop_account_streams();
        self.supervisor.stop_all();
    }
}

/// 把 supervisor 的 per-account 连接请求按名分派给支持的 provider；
/// 无 provider 支持时返回占位任务（未知所），不影响其它所。
#[derive(Clone)]
struct RegistryConnector {
    providers: Vec<Arc<dyn VenueProvider>>,
    sink: Arc<dyn OrderEventSink>,
}

impl RegistryConnector {
    fn new(providers: Vec<Arc<dyn VenueProvider>>, sink: Arc<dyn OrderEventSink>) -> Self {
        Self { providers, sink }
    }
}

impl VenueConnector for RegistryConnector {
    fn stop_account_streams(&self) {
        for p in &self.providers {
            p.stop_account_streams();
        }
    }

    fn spawn(&self, exchange: &str, account: &str) -> JoinHandle<()> {
        for p in &self.providers {
            if p.matches(exchange) {
                if let Some(handle) =
                    p.spawn_account_stream(exchange, account, Arc::clone(&self.sink))
                {
                    return handle;
                }
            }
        }
        let ex = exchange.to_string();
        let ac = account.to_string();
        tokio::spawn(async move {
            tracing::debug!(exchange = %ex, account = %ac, "no per-account event stream (unknown)");
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use tokio::task::JoinHandle;

    use crate::api::{
        ExchangeApi, OrderAck, OrderLookup, OrderRef, PlaceOrderRequest, PositionInfo,
        ReferencePrice,
    };
    use crate::port::OrderEventSink;
    use crate::provider::VenueProvider;
    use crate::registry::{ExchangeRegistry, RegistryConnector};
    use crate::runtime::VenueConnector;

    /// 不联网的最小 ExchangeApi，仅用于验证注册表把请求路由给了哪个 provider。
    struct StubApi;
    #[async_trait]
    impl ExchangeApi for StubApi {
        fn exchange(&self) -> &str {
            "stub"
        }
        async fn place_order(&self, _: &PlaceOrderRequest) -> anyhow::Result<OrderAck> {
            anyhow::bail!("stub")
        }
        async fn cancel_order(&self, _: &str, _: &OrderRef) -> anyhow::Result<OrderAck> {
            anyhow::bail!("stub")
        }
        async fn query_order(&self, _: &str, _: &OrderRef) -> OrderLookup {
            OrderLookup::Unavailable
        }
        async fn get_positions(&self) -> anyhow::Result<Vec<PositionInfo>> {
            Ok(vec![])
        }
        async fn set_leverage(&self, _: &str, _: u32) -> anyhow::Result<()> {
            Ok(())
        }
        async fn get_reference_price(&self, _: &str) -> anyhow::Result<ReferencePrice> {
            anyhow::bail!("stub")
        }
    }

    struct NoopSink;
    #[async_trait]
    impl OrderEventSink for NoopSink {
        async fn on_order_event(&self, _: &trading_core::protocol::OrderEvent, _: &str, _: &str) {}
    }

    /// 可计数的假交易所 provider，用于钉死注册表的分派行为（开闭原则）。
    #[derive(Clone)]
    struct FakeProvider {
        names: Vec<&'static str>,
        support_account_stream: bool,
        build_calls: Arc<AtomicUsize>,
        market_calls: Arc<AtomicUsize>,
        account_calls: Arc<AtomicUsize>,
    }

    impl FakeProvider {
        fn new(names: &[&'static str]) -> Self {
            Self {
                names: names.to_vec(),
                support_account_stream: false,
                build_calls: Arc::new(AtomicUsize::new(0)),
                market_calls: Arc::new(AtomicUsize::new(0)),
                account_calls: Arc::new(AtomicUsize::new(0)),
            }
        }
        fn with_account_stream(mut self) -> Self {
            self.support_account_stream = true;
            self
        }
    }

    impl VenueProvider for FakeProvider {
        fn matches(&self, exchange: &str) -> bool {
            // 新契约：大小写敏感的精确匹配，别名/大小写不一致都不算命中。
            self.names.contains(&exchange)
        }
        fn build_api(
            &self,
            _exchange: &str,
            _instrument_type: Option<&str>,
            _account: &str,
        ) -> anyhow::Result<Arc<dyn ExchangeApi>> {
            self.build_calls.fetch_add(1, Ordering::Relaxed);
            Ok(Arc::new(StubApi))
        }
        fn spawn_account_stream(
            &self,
            _exchange: &str,
            _account: &str,
            _sink: Arc<dyn OrderEventSink>,
        ) -> Option<JoinHandle<()>> {
            self.account_calls.fetch_add(1, Ordering::Relaxed);
            self.support_account_stream.then(|| tokio::spawn(async {}))
        }
        fn spawn_market_services(&self, active_exchanges: &[String]) {
            // 与真实 provider 同门控：仅当激活名单含本 provider 归属的所时才计数。
            if active_exchanges
                .iter()
                .any(|n| self.names.iter().any(|m| m == n))
            {
                self.market_calls.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn registry_of(ps: Vec<Arc<dyn VenueProvider>>) -> ExchangeRegistry {
        ExchangeRegistry::from_providers(ps)
    }

    #[test]
    fn connect_routes_to_matching_provider_only() {
        let alpha = Arc::new(FakeProvider::new(&["alpha"]));
        let beta = Arc::new(FakeProvider::new(&["beta"]));
        let reg = registry_of(vec![alpha.clone(), beta.clone()]);

        assert!(reg.connect("beta", None, "acc").is_ok()); // 精确名命中
        assert_eq!(beta.build_calls.load(Ordering::Relaxed), 1);
        assert_eq!(alpha.build_calls.load(Ordering::Relaxed), 0);
        // 大小写不一致属异常名：不做归一，按未知所拒绝。
        assert!(reg.connect("BETA", None, "acc").is_err());
        assert_eq!(beta.build_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn connect_unknown_exchange_is_error() {
        let alpha = Arc::new(FakeProvider::new(&["alpha"]));
        let reg = registry_of(vec![alpha]);
        assert!(reg.connect("gamma", None, "acc").is_err());
    }

    #[test]
    fn is_supported_uses_exact_case_sensitive_match() {
        let xili = Arc::new(FakeProvider::new(&["xili"]));
        let binance = Arc::new(FakeProvider::new(&["binance"]));
        let reg = registry_of(vec![xili, binance]);

        // 精确名受支持。
        assert!(reg.is_supported("xili"));
        assert!(reg.is_supported("binance"));
        // 未注册所 / 别名 / 大小写不一致：一律不支持（上层据此拒绝）。
        assert!(!reg.is_supported("nope"));
        assert!(!reg.is_supported("binance_usdm"));
        assert!(!reg.is_supported("Binance"));
    }

    #[test]
    fn market_services_fan_out_to_every_active_provider() {
        let alpha = Arc::new(FakeProvider::new(&["alpha"]));
        let beta = Arc::new(FakeProvider::new(&["beta"]));
        let reg = registry_of(vec![alpha.clone(), beta.clone()]);

        // 两个所都在账户表去重名单内 → 各启动一次。
        reg.spawn_market_services(&["alpha".to_string(), "beta".to_string()]);
        assert_eq!(alpha.market_calls.load(Ordering::Relaxed), 1);
        assert_eq!(beta.market_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn market_services_skip_providers_without_accounts() {
        let alpha = Arc::new(FakeProvider::new(&["alpha"]));
        let beta = Arc::new(FakeProvider::new(&["beta"]));
        let reg = registry_of(vec![alpha.clone(), beta.clone()]);

        // 去硬编码：空名单（无任何账户）→ 任何所都不启动公共行情/规则。
        reg.spawn_market_services(&[]);
        assert_eq!(alpha.market_calls.load(Ordering::Relaxed), 0);
        assert_eq!(beta.market_calls.load(Ordering::Relaxed), 0);

        // 只有 beta 账户 → 仅 beta 启动，alpha 不被无条件拉起。
        reg.spawn_market_services(&["beta".to_string()]);
        assert_eq!(alpha.market_calls.load(Ordering::Relaxed), 0);
        assert_eq!(beta.market_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn connector_dispatches_account_stream_to_supporting_provider() {
        let real = Arc::new(FakeProvider::new(&["binance"]).with_account_stream());
        let reg = registry_of(vec![real.clone()]);
        let sink: Arc<dyn OrderEventSink> = Arc::new(NoopSink);
        let connector = RegistryConnector::new(reg.providers(), sink);

        // 受支持：分派给 provider，且返回的任务可正常完成。
        let handle = connector.spawn("binance", "alice");
        handle.await.unwrap();
        assert_eq!(real.account_calls.load(Ordering::Relaxed), 1);

        // 无 provider 支持：返回占位任务（不 panic、可完成）。
        let other = Arc::new(FakeProvider::new(&["other"]));
        let reg2 = registry_of(vec![other]);
        let sink2: Arc<dyn OrderEventSink> = Arc::new(NoopSink);
        let placeholder = RegistryConnector::new(reg2.providers(), sink2);
        placeholder.spawn("other", "u").await.unwrap();
    }
}
