//! CTP 套件 provider：实现 [`VenueProvider`]，由注册表登记。
//!
//! CTP 是"同步阻塞特例"：建连/下单/查询全部收口在专用 OS 线程
//! （见 [`crate::ctp::venue`]），本 provider 只是装配壳。

use std::sync::Arc;

use std::sync::atomic::AtomicBool;
use std::sync::Mutex;

use tokio::task::JoinHandle;

use crate::ctp::account_consumer::{CtpVenueConnector, SessionRegistry};
use crate::ctp::core::account::CtpAccount;
use crate::ctp::instrument_dir::InstrumentDir;
use crate::ctp::spec_fetch::{self, SpecSnapshot};
use crate::ctp::{api::CtpApi, CtpConfig};
use crate::port::{ExchangeCredential, OrderEventSink};
use crate::position_profile::PositionProfile;
use crate::provider::{ProviderDeps, VenueProvider};

/// ctp 唯一规范名（大小写敏感精确匹配，无别名）。
pub const NAME: &str = "ctp";

/// 一次规格刷新落库的三类条数（管理 API 应答/日志用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpecRefreshCounts {
    /// 全量合约条数（含期权/组合）。
    pub instruments: usize,
    /// 其中在交易期货条数。
    pub futures: usize,
    /// 投资者手续费率条数。
    pub commissions: usize,
}

/// 已有一次规格刷新在执行（互斥冲突）；管理 API 据此回 409 而非 500。
#[derive(Debug, thiserror::Error)]
#[error("已有一次 CTP 规格刷新在执行，请稍后再试")]
pub struct SpecRefreshBusy;

/// CTP 套件 provider（持有装配依赖与运行参数）。
pub struct CtpProvider {
    deps: ProviderDeps,
    /// CTP 运行参数（.so 路径/流目录基准，来自 venue 配置）。
    config: CtpConfig,
    /// 公共行情线程停机标志（行情是 std::thread，同样只能靠标志退出）。
    md_stop: Arc<std::sync::atomic::AtomicBool>,
    /// 公共行情线程句柄（spawn_market_services 后才有）；优雅停机 join，
    /// 保证 md 退出前的 flush_all（最后未闭桶进内存）先于 CSV 刷盘线程收尾。
    md_task: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// 各账户会话注册表（停机/映射/信箱；**provider 级 + 每账户隔离**：
    /// 连接器逐次新建，这些状态必须跨热加载存活）。
    sessions: Arc<SessionRegistry>,
    /// 进程级共享合约静态目录（交易所/乘数/最小变动价位；合约查询预热，全局一致）。
    /// 供 prepare_placement 判 SHFE/取 tick，并注入每个账户会话线程。规格刷新时
    /// 在**同一 Arc 上原地重灌**（[`InstrumentDir::replace_all`]：retain 新集合 +
    /// upsert），行情流/各会话/build_api 等既有持有方自动看到最新合约。
    instrument_dir: Arc<InstrumentDir>,
    /// 规格刷新互斥（拒绝并发刷新）：短会话固定使用 `spec-refresh/` 流目录，
    /// 同一流目录不能并存两个 TraderApi（con 文件互踩）。持有 unit guard 即占用，
    /// try_lock 失败说明已有刷新在跑（handler 回 409）。
    spec_refreshing: Mutex<()>,
    /// 交易所级统一行情流单例（业务兴趣 ∪ K线集共用一条 ctp-md 连接）。
    market_stream: std::sync::OnceLock<Arc<crate::ctp::market_stream::CtpMarketStream>>,
}

impl CtpProvider {
    pub fn new(deps: ProviderDeps, config: CtpConfig) -> Self {
        let instrument_dir = Arc::new(crate::ctp::instrument_dir::InstrumentDir::new());
        // 冷启动种子：最新一份 instruments.csv（仅在交易期货）灌入内存表，
        // 使品种订阅在交易侧在线合约查询返回前即可展开；文件缺失不阻断启动。
        if let Some(path) = config.instrument_specs_path.as_ref() {
            if path.exists() {
                match std::fs::File::open(path) {
                    Ok(f) => match instrument_dir.load_from_csv(f) {
                        Ok(n) => tracing::info!(
                            path = %path.display(),
                            instruments = n,
                            "CTP 合约规格种子已加载（冷启动）"
                        ),
                        Err(e) => tracing::warn!(
                            path = %path.display(),
                            error = %e,
                            "CTP 合约规格种子读取失败，回退在线预热"
                        ),
                    },
                    Err(e) => tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "CTP 合约规格种子打开失败，回退在线预热"
                    ),
                }
            } else {
                tracing::debug!(path = %path.display(), "CTP 合约规格种子不存在，纯在线预热");
            }
        }
        Self {
            deps,
            config,
            md_stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            md_task: Mutex::new(None),
            sessions: Arc::new(SessionRegistry::default()),
            instrument_dir,
            spec_refreshing: Mutex::new(()),
            market_stream: std::sync::OnceLock::new(),
        }
    }

    /// 懒创建并返回 CTP 统一行情流单例（三表 + 合约目录注入）。
    fn ensure_market_stream(
        &self,
    ) -> anyhow::Result<Arc<crate::ctp::market_stream::CtpMarketStream>> {
        let stream = self.market_stream.get_or_init(|| {
            let stream = crate::ctp::market_stream::CtpMarketStream::new(
                Arc::clone(&self.deps.ctp_ticks),
                Arc::clone(&self.deps.ctp_last_stats),
                Arc::clone(&self.deps.ctp_dominants),
                Arc::clone(&self.instrument_dir),
                self.config.kline_products.clone(),
            );
            // 无条件全量持久化 sink 只在单例首次创建时注册一次（闭包仅执行一次，
            // 避免 ensure 被多次调用时重复挂同一 sink 导致 bar 重复落盘）。
            if let Some(persist) = &self.deps.persist_bar_sink {
                stream.register_persistent_bar_sink(Arc::clone(persist));
            }
            stream
        });
        Ok(Arc::clone(stream))
    }

    /// 账户当前会话的停机标志（接线测试/诊断用）。
    pub fn current_shutdown(&self, account: &str) -> Option<Arc<AtomicBool>> {
        self.sessions.current_shutdown(account)
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

    /// 每日规格刷新（管理 API 在 `spawn_blocking` 内调用）：
    /// 1. 用一个独立临时 trader 短连接**阻塞**拉全量合约 + 投资者手续费率；
    /// 2. 经 `persist` 闭包交宿主持久化（service 层转 store 行写日期文件 + latest；
    ///    exchange 不依赖 trading-store、不做文件 IO）；
    /// 3. 用最新「在交易期货」快照**原地重灌**共享 [`InstrumentDir`]（同一 Arc）。
    ///
    /// 互斥：同一时刻只允许一个刷新（短会话独占 `spec-refresh/` 流目录）。
    /// 已有刷新在跑时**立即**返回错误（不排队，调用方回 409）。
    ///
    /// 顺序保证：先落盘成功才重灌内存，使内存目录不领先于磁盘冷启动种子。
    /// 落盘日期口径由 `persist` 闭包自行掌握（exchange 层不关心日期/文件）。
    pub fn refresh_specs(
        &self,
        persist: impl FnOnce(&SpecSnapshot) -> anyhow::Result<()>,
    ) -> anyhow::Result<SpecRefreshCounts> {
        // 立即 try_lock：刷新耗时数十秒，绝不让第二个 HTTP 请求在此阻塞排队。
        // 持有 unit guard 本身即占用；其生命周期覆盖整个刷新，函数返回（正常/错误/
        // panic unwind）时自动释放——无需额外布尔位。
        let _gate = match self.spec_refreshing.try_lock() {
            Ok(g) => g,
            Err(std::sync::TryLockError::WouldBlock) => {
                // 类型化错误：handler 据此回 409（不排队、不当作 500）。
                return Err(anyhow::Error::new(SpecRefreshBusy));
            }
            // 上次刷新线程 panic 中毒：恢复并继续（后续为全新短会话，状态自洽）。
            Err(std::sync::TryLockError::Poisoned(p)) => p.into_inner(),
        };
        self.refresh_specs_locked(persist)
    }

    fn refresh_specs_locked(
        &self,
        persist: impl FnOnce(&SpecSnapshot) -> anyhow::Result<()>,
    ) -> anyhow::Result<SpecRefreshCounts> {
        // 规格是交易所级全局数据，账户只用于登录鉴权；统一用 venue.toml
        // `[ctp].md_account`（行情/规格共用的登录账户），与行情线程同源。
        let name = self.config.md_account.clone().ok_or_else(|| {
            anyhow::anyhow!("规格刷新缺少账户：请在 venue.toml 配置 [ctp].md_account")
        })?;
        let acct = self.ctp_account(&name)?;

        // 1) 阻塞短会话拉全量（数十秒；调用方已置于 spawn_blocking）。
        let snapshot =
            spec_fetch::fetch_all_specs(&acct, &self.config.trader_so, &self.config.flow_path)?;
        let counts = SpecRefreshCounts {
            instruments: snapshot.instruments.len(),
            futures: snapshot.futures_count(),
            commissions: snapshot.commissions.len(),
        };

        // 2) 先落盘（失败则不重灌内存，保留旧目录）。
        persist(&snapshot)?;

        // 3) 原地重灌共享合约目录（仅在交易期货）；retain 新集合，摘牌合约被移除。
        let dir_rows = spec_fetch::to_trading_futures_dir_rows(&snapshot);
        let loaded = self.instrument_dir.replace_all(dir_rows);
        tracing::info!(
            account = %name,
            instruments = counts.instruments,
            futures = counts.futures,
            commissions = counts.commissions,
            dir_loaded = loaded,
            "CTP 合约规格刷新完成：已落盘并重灌共享合约目录"
        );
        Ok(counts)
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
    /// 优雅停机（覆盖 trait 默认空实现）：置位全部账户会话与公共行情线程的停机标志
    /// （`spawn_blocking`/`std::thread` 无法 abort，标志是唯一停机通道），并 **join
    /// 行情线程**，等它退出前的 `flush_all()` 完成（最后未闭 1m 桶已推进内存 BarTable），
    /// 使后续 CSV 刷盘线程的最后一刷能落盘。线程曾 panic 仅记录、不阻断停机。
    ///
    /// 必须是 trait 覆盖而非固有方法：注册表持 `Arc<dyn VenueProvider>` 动态分发，
    /// 固有方法不会被该路径调用（会静默落到 trait 默认空实现）。
    fn stop_account_streams(&self) {
        self.sessions.shutdown_all();
        self.md_stop
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // 先 take 出句柄使锁立即释放，再 join（绝不持锁等待线程，杜绝再入死锁）。
        let task = self.md_task.lock().unwrap().take();
        if let Some(handle) = task {
            if let Err(e) = handle.join() {
                tracing::error!(?e, "CTP 行情线程停机 join 异常");
            }
        }
    }

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

    /// 建单前同步预检：**仅 SHFE** 市价单转穿价限价（GFD），其他所/限价单透传。
    ///
    /// SHFE/INE 柜台不支持 AnyPrice；这里从共享合约目录判交易所、取最小变动价位，
    /// 从共享行情表读 best bid/ask，交纯函数 [`prep::prepare_shfe_market_limit`]
    /// 转成对手价（平仓）或对手价±n tick（开仓）限价。合约/行情未就绪 → DataNotReady，
    /// 引擎不 claim、规则保活下轮重试。非 SHFE 市价维持原生 AnyPrice+IOC。
    fn prepare_placement(
        &self,
        _venue: &str,
        request: crate::api::PlaceOrderRequest,
    ) -> Result<crate::api::PlaceOrderRequest, crate::api::PlaceDefer> {
        // 规则引擎与手动 API 共用同一转换组件（prep::prepare_ctp_market）。
        crate::ctp::prep::prepare_ctp_market(
            &self.instrument_dir,
            &self.deps.quotes,
            self.config.shfe_market_drift_ticks,
            &self.config.market_limit_exchanges,
            request,
        )
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
            Arc::clone(&session.mailbox),
            Arc::clone(&self.instrument_dir),
            Arc::clone(&self.deps.quotes),
            self.config.shfe_market_drift_ticks,
            self.config.market_limit_exchanges.clone(),
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
        let account = match self.market_data_account() {
            Some(a) => a,
            None => {
                tracing::warn!("CTP 行情缺账户名来源（venue.toml [ctp].md_account），跳过行情");
                return;
            }
        };
        // 统一行情流单例（无 bar client 时其 bar_symbols 为空，drain 走纯业务路径，
        // 与历史逐字节等价）；起收盘主力选举线程。
        let stream = match self.ensure_market_stream() {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "CTP 行情流单例构造失败，跳过行情");
                return;
            }
        };
        // 冷启动预设主力（venue.toml [ctp].preset_dominants）：盘中重启/未到
        // 15:00 选举窗口时也能让品种订阅立即只推指定合约；空配置为 no-op。
        if !self.config.preset_dominants.is_empty() {
            stream.preset_dominants(&self.config.preset_dominants);
        }
        stream.spawn_election(
            Arc::clone(&self.md_stop),
            self.config.main_contract_criteria,
            self.config.main_contract_confirm_days,
        );
        let handle = crate::ctp::market::spawn_market_worker(
            crate::ctp::market::MdParams {
                // 行情前置需要合法账户结构（BrokerID/UserID 编码进登录请求），
                // 无活动账户时上面已 return，这里取行情账户的领域凭证。
                account,
                config: self.config.clone(),
            },
            self.deps.market_interest.clone(),
            Arc::clone(&self.deps.quotes),
            self.deps.quote_observer.clone(),
            Arc::clone(&self.md_stop),
            Some(crate::ctp::market::MdKlineDeps { stream }),
        );
        // 留存句柄供优雅停机 join（None=未配前置/起线程失败，停机无需等待）。
        *self.md_task.lock().unwrap() = handle;
    }

    fn build_market_stream(
        &self,
        _exchange: &str,
        instrument_type: Option<&str>,
    ) -> anyhow::Result<Arc<dyn crate::market::MarketDataStream>> {
        let product = trading_core::protocol::normalize_instrument_type(instrument_type);
        if product != "futures" {
            anyhow::bail!("ctp 行情仅支持期货（instrument_type=futures），收到 `{product}`");
        }
        Ok(self.ensure_market_stream()? as Arc<dyn crate::market::MarketDataStream>)
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
            Arc::clone(&self.instrument_dir),
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
            ctp_ticks: Arc::new(trading_core::kline::TickTable::new()),
            ctp_last_stats: Arc::new(trading_core::kline::LastTickStatTable::new()),
            ctp_dominants: Arc::new(trading_core::kline::DominantTable::new()),
            persist_bar_sink: None,
            hist_fetch: crate::provider::HistoricalFetchConfig::default_for_backfill(),
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
    fn dyn_trait_stop_sets_md_stop_flag() {
        // 注册表持有 Arc<dyn VenueProvider>，停机走的是 trait 动态分发，
        // 必须真的置位 md_stop（而不是落到 trait 默认空实现）。
        let p = CtpProvider::new(
            deps_with(Arc::new(StubResolver(Mutex::new(
                std::collections::HashMap::new(),
            )))),
            test_config(),
        );
        use std::sync::atomic::Ordering;
        assert!(!p.md_stop.load(Ordering::SeqCst));
        let provider: &dyn VenueProvider = &p;
        provider.stop_account_streams();
        assert!(
            p.md_stop.load(Ordering::SeqCst),
            "dyn 分发停机必须置位 md_stop"
        );
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

    // ---- refresh_specs：并发闸门 / 账户缺失（均不触 FFI）----

    fn provider_without_creds() -> CtpProvider {
        CtpProvider::new(
            deps_with(Arc::new(StubResolver(Mutex::new(
                std::collections::HashMap::new(),
            )))),
            test_config(),
        )
    }

    #[test]
    fn refresh_specs_rejects_when_already_running() {
        // Arrange：持有 unit guard 模拟一次刷新正在执行（guard 存活到用例结束）。
        let p = provider_without_creds();
        let _held = p.spec_refreshing.lock().unwrap();

        // Act：persist 绝不应被调用（在闸门处即拒绝）。
        let err = p
            .refresh_specs(|_| panic!("并发刷新不应进入落盘回调"))
            .unwrap_err();

        // Assert：类型化为 SpecRefreshBusy（handler 据此回 409）。
        assert!(err.downcast_ref::<SpecRefreshBusy>().is_some());
    }

    #[test]
    fn refresh_specs_missing_account_errors_and_releases_gate() {
        // Arrange：for_tests 的 md_account=Some("test")，但 resolver 为空 → 解析失败，
        // 且失败发生在 FFI 建连之前。
        let p = provider_without_creds();

        // Act。
        let err = p.refresh_specs(|_| Ok(())).unwrap_err();

        // Assert：不是并发冲突（是账户错误）；闸门经 RAII 已释放，可再次获取。
        assert!(err.downcast_ref::<SpecRefreshBusy>().is_none());
        assert!(p.spec_refreshing.try_lock().is_ok());
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

    // ---- prepare_placement：仅 SHFE 市价转穿价限价 ----

    use crate::api::{OrderPositionSide, PlaceDefer};
    use crate::ctp::core::event::InstrumentInfo;
    use crate::provider::VenueProvider;
    use rust_decimal::Decimal;
    use trading_core::quote::{QuoteTable, QuoteUpdate};
    use trading_models::{
        Instrument as MInstrument, InstrumentType, MarketKind, OrderType, Side, TimeInForce,
        TopOfBook, Venue as TVenue,
    };

    fn market_request(side: Side, reduce_only: bool) -> crate::api::PlaceOrderRequest {
        crate::api::PlaceOrderRequest {
            instrument: "rb2510".into(),
            instrument_type: InstrumentType::Futures,
            side,
            order_type: OrderType::Market,
            price: None,
            quantity: Decimal::from(2),
            reduce_only,
            position_side: OrderPositionSide::Both,
            time_in_force: Some(TimeInForce::Ioc),
            client_order_id: "0xabc".into(),
        }
    }

    fn warm_book(quotes: &QuoteTable, bid: f64, ask: f64) {
        let mut u = QuoteUpdate::new(1);
        u.mark = Some(Decimal::try_from((bid + ask) / 2.0).unwrap());
        u.book = Some(TopOfBook::new(
            Some(Decimal::try_from(bid).unwrap()),
            Some(Decimal::try_from(ask).unwrap()),
        ));
        quotes.apply(
            &MInstrument::new(TVenue::Ctp, MarketKind::Futures, "rb2510"),
            u,
        );
    }

    fn provider_with(
        exchange: &str,
        tick: f64,
        quotes: Option<Arc<QuoteTable>>,
        drift: i64,
    ) -> CtpProvider {
        let creds: Arc<dyn CredentialResolver> =
            Arc::new(StubResolver(Mutex::new(std::collections::HashMap::new())));
        let mut deps = deps_with(creds);
        if let Some(q) = quotes {
            deps.quotes = q;
        }
        let config = CtpConfig::for_tests().with_shfe_market_drift_ticks(drift);
        let p = CtpProvider::new(deps, config);
        p.instrument_dir.remember(InstrumentInfo {
            instrument: "rb2510".into(),
            exchange_id: exchange.into(),
            product_id: "rb".into(),
            is_trading: true,
            volume_multiple: 10,
            price_tick: tick,
            max_limit_volume: 0,
            max_market_volume: 0,
            long_margin_ratio: 0.0,
            short_margin_ratio: 0.0,
        });
        p
    }

    #[test]
    fn prepare_shfe_market_buy_open_converts_to_cross_limit() {
        let quotes = Arc::new(QuoteTable::new());
        warm_book(&quotes, 3204.0, 3205.0);
        let p = provider_with("SHFE", 1.0, Some(quotes), 1);
        let out = p
            .prepare_placement(NAME, market_request(Side::Buy, false))
            .unwrap();
        assert_eq!(out.order_type, OrderType::Limit);
        // ask 3205 + 1 档穿价。
        assert_eq!(out.price, Some(Decimal::from(3206)));
    }

    #[test]
    fn prepare_shfe_market_close_long_uses_best_bid_no_drift() {
        let quotes = Arc::new(QuoteTable::new());
        warm_book(&quotes, 3204.0, 3205.0);
        // drift=5 也不得影响平仓价。
        let p = provider_with("SHFE", 1.0, Some(quotes), 5);
        let out = p
            .prepare_placement(NAME, market_request(Side::Sell, true))
            .unwrap();
        assert_eq!(out.order_type, OrderType::Limit);
        assert_eq!(out.price, Some(Decimal::from(3204)));
    }

    #[test]
    fn prepare_non_shfe_market_passes_through_unchanged() {
        // DCE 市价单不转限价（维持 AnyPrice+IOC），即使已预热盘口。
        let quotes = Arc::new(QuoteTable::new());
        warm_book(&quotes, 600.0, 601.0);
        let p = provider_with("DCE", 0.5, Some(quotes), 1);
        let out = p
            .prepare_placement(NAME, market_request(Side::Buy, false))
            .unwrap();
        assert_eq!(out.order_type, OrderType::Market);
        assert_eq!(out.price, None);
    }

    #[test]
    fn prepare_shfe_limit_passes_through_without_quote() {
        let mut req = market_request(Side::Buy, false);
        req.order_type = OrderType::Limit;
        req.price = Some(Decimal::from(3205));
        let p = provider_with("SHFE", 1.0, None, 1);
        // 限价单不读盘口，无行情也放行。
        let out = p.prepare_placement(NAME, req).unwrap();
        assert_eq!(out.order_type, OrderType::Limit);
        assert_eq!(out.price, Some(Decimal::from(3205)));
    }

    #[test]
    fn prepare_shfe_market_defers_without_contract_or_book() {
        // 合约未预热 → DataNotReady。
        let creds: Arc<dyn CredentialResolver> =
            Arc::new(StubResolver(Mutex::new(std::collections::HashMap::new())));
        let p = CtpProvider::new(deps_with(creds), CtpConfig::for_tests());
        assert_eq!(
            p.prepare_placement(NAME, market_request(Side::Buy, false))
                .unwrap_err(),
            PlaceDefer::DataNotReady
        );
        // 合约已预热但无盘口 → 同样 DataNotReady。
        let p2 = provider_with("SHFE", 1.0, None, 1);
        assert_eq!(
            p2.prepare_placement(NAME, market_request(Side::Buy, false))
                .unwrap_err(),
            PlaceDefer::DataNotReady
        );
    }
}
