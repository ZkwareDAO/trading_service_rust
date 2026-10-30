//! 进程内共享状态（行为/连接层）。
//!
//! 数据内存（账户/订单/仓位/规则表、行情表、下单规则表）统一收口在
//! [`TradingStore`]，本状态只持有：
//! * [`StrategyHub`]：strategy_name -> 南向 WS 连接；
//! * [`ExchangeRegistry`]：按交易所名路由到 provider 的适配器工厂；
//! * [`RuleEngine`]：盯盘规则扫描/下单/终态兜底的编排（自身不存数据）；
//! * [`AccountHub`]：账户域门面 + 各出站端口实现；
//! * [`SouthAuth`]：南向握手鉴权。
//!
//! 全部为廉价克隆的句柄（内部 Arc/DashMap），任务间靠共享状态协作，不使用 channel。
//! 服务不另建内存表与落盘线程——需要订单/仓位/规则仓储或行情表时一律 `store.xxx()`。

use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use trading_exchange::port::CredentialResolver;
use trading_exchange::provider::{ProviderDeps, VenueEndpoint};
use trading_exchange::registry::ExchangeRegistry;
use trading_store::TradingStore;

use crate::account_hub::AccountHub;
use crate::hub::StrategyHub;
use crate::interest::MarketInterests;
use crate::notify::NotifyHub;
use crate::positions::{AccountLookup, PositionEngine};
use crate::rules::{ExchangeRuleOrderQuerier, ExchangeRulePlacer, RuleAccountLookup, RuleEngine};
use crate::south_auth::SouthAuth;

/// 启动期静态端点覆盖（进程级，随进程生命周期）。
pub type VenueEndpoints = Arc<Vec<VenueEndpoint>>;

/// 装配 [`AppState`] 所需的进程级参数。
///
/// 数据句柄（各 repo、行情/规则表、账户表）全部由 [`TradingStore`] 统一提供，
/// 这里只传网络/时序参数与账户、鉴权两个行为对象，避免 main 手工逐表初始化。
pub struct AssemblyDeps {
    /// 共享存储根（进程一份）。
    pub store: Arc<TradingStore>,
    /// 账户门面（已绑定 store 的账户表并完成首载）。
    pub accounts: AccountHub,
    /// 南向策略连接鉴权。
    pub south_auth: SouthAuth,
    /// 单次交易所 REST 超时（构造共享 HTTP client）。
    pub rest_timeout: Duration,
    /// 交易所下单规则整表刷新周期。
    pub rule_refresh_interval: Duration,
    /// 仓位对账去抖窗口。
    pub position_debounce: Duration,
    /// 仓位定时全量对账周期。
    pub position_reconcile_interval: Duration,
    /// BarTable 每序列内存保留 bar 条数上限（缺省 1440）。
    pub kline_retain_bars: usize,
    /// BarTable 每序列内存保留时长（缺省 24h）。
    pub kline_retain_age: Duration,
    /// get_klines 历史取数缺省/最大返回根数（缺省 300/1500）。
    pub kline_history_limits: crate::kline_history::HistoryLimits,
    /// 三家 K线缺口内部扫描周期（0=关闭周期扫描，实时尾检仍在）。
    pub kline_gap_scan_interval: Duration,
    /// 三家静默停推阈值（必须 >0）。
    pub kline_stall_threshold: Duration,
    /// 本地模拟所静态端点覆盖。
    pub endpoints: VenueEndpoints,
    /// 企业微信四通道 Webhook 对照表（空串=未配置）。
    pub channels: crate::notify::ChannelUrls,
    /// 全部 venue 可调参数（deribit 价差/漂移、hyperliquid 滑点）。
    pub venue: crate::venue_config::LoadedVenueConfigs,
}

#[derive(Clone)]
pub struct AppState {
    /// 共享存储根：订单/仓位/规则仓储、行情表、下单规则表、账户表统一从此取。
    pub store: Arc<TradingStore>,
    /// strategy_name -> 南向 WS 连接。
    pub hub: StrategyHub,
    /// 交易所注册表：按名称/账户路由到对应 provider 构造适配器。
    pub exchanges: Arc<ExchangeRegistry>,
    /// 账户门面（凭证解析/账户目标/账户反查等端口在此实现）。
    pub accounts: AccountHub,
    /// 盯盘规则扫描引擎：定时评估 active 规则、命中后原子抢占并下单。
    pub rule_engine: RuleEngine,
    /// 仓位编排引擎：实时交易所持仓查询的映射复用此句柄。
    pub position_engine: Arc<PositionEngine>,
    /// 企业微信成交通知（开仓/风控平仓）；纯通知，失败不影响交易路径。
    pub notify: NotifyHub,
    /// 南向策略连接握手鉴权（单一共享密钥 + nonce 防重放；dev 旁路除外）。
    pub south_auth: SouthAuth,
    /// K线服务编排（订阅登记/fan-out/CSV 刷盘；进程一份）。
    pub kline: Arc<crate::kline::KlineService>,
}

impl AppState {
    /// 一处装配交易所注册表、仓位编排引擎与盯盘规则引擎；
    /// 所有数据句柄都取自 store，装配层不点名任何具体交易所（注册表内部登记）。
    pub fn assemble(deps: AssemblyDeps) -> Self {
        let AssemblyDeps {
            store,
            accounts,
            south_auth,
            rest_timeout,
            rule_refresh_interval,
            position_debounce,
            position_reconcile_interval,
            kline_retain_bars,
            kline_retain_age,
            kline_history_limits,
            kline_gap_scan_interval,
            kline_stall_threshold,
            endpoints,
            channels,
            venue,
        } = deps;

        // K线单表保留窗在任何 bar 到达前按 CLI/config 收紧（缺省 1440 条 / 24h）。
        store
            .bar_table()
            .set_retention(kline_retain_bars, kline_retain_age);

        // —— 数据句柄全部来自 store（进程内单例）——
        let orders = store.order_repo().expect("order repo after recover");
        let positions = store.position_repo().expect("position repo after recover");
        let watch_rules = store.rule_repo().expect("rule repo after recover");
        let rules = store.order_rules();
        let quotes = store.quotes();

        let hub = StrategyHub::new();
        let http = Client::builder()
            .timeout(rest_timeout)
            .build()
            .expect("build reqwest client");
        // 通知复用同一 HTTP client（clone 仅增引用、共享连接池），须在 http 移入
        // provider_deps 之前取出。
        let notify_http = http.clone();

        // 账户门面作为凭证解析端口注入交易所注册表；注册表不认识服务内部类型。
        let credentials: Arc<dyn CredentialResolver> = Arc::new(accounts.clone());
        // 仓位编排引擎：账户事件快照对账 + 行情驱动重估两个出站端口的宿主实现。
        let account_lookup: Arc<dyn AccountLookup> = Arc::new(accounts.clone());
        let engine = Arc::new(PositionEngine::new(
            positions.clone(),
            Arc::clone(&quotes),
            account_lookup,
            store.leverage_repo(),
        ));
        let market_interest: Arc<dyn trading_exchange::port::MarketInterest> =
            Arc::new(MarketInterests::new(positions.clone(), watch_rules.clone()));

        // K线服务提前构造：CTP 需要把其「无条件全量持久化 sink」经 ProviderDeps 注入
        // 交易所适配层（非主力/零订阅也落 CSV）；句柄全部取自 store 单例，fan-out
        // 复用南向连接 hub。assemble 只在 tokio runtime 内调用，可直接取 Handle。
        let kline_pusher = Arc::new(crate::kline::HubKlinePusher::new(
            hub.clone(),
            tokio::runtime::Handle::current(),
        ));
        // 缺口检测参数（步长取服务内 1m 常量；扫描周期允许 0=关闭周期扫描）。
        // stall>0、scan≥0 已由 config::parse_gap_timings 启动校验，此处 expect 仅表达该不变式。
        let to_ms = |d: Duration| d.as_millis().min(i64::MAX as u128) as i64;
        let gap_cfg = crate::kline_gap::GapScanConfig::new(
            crate::kline::KLINE_STEP_MS,
            to_ms(kline_stall_threshold),
            to_ms(kline_gap_scan_interval),
        )
        .expect("缺口检测参数已由 parse_gap_timings 校验为合法");
        let kline = Arc::new(crate::kline::KlineService::new(
            Arc::clone(&store),
            kline_pusher,
            crate::venue_config::kline_baselines(&venue),
            kline_history_limits,
            gap_cfg,
            crate::kline_backfill::BackfillRuntimeConfig::from(&venue.kline.backfill),
        ));

        let provider_deps = ProviderDeps {
            http,
            rules,
            quotes: Arc::clone(&quotes),
            credentials,
            rest_timeout,
            rule_refresh_interval,
            position_debounce,
            position_reconcile_interval,
            position_sink: Arc::clone(&engine)
                as Arc<dyn trading_exchange::port::PositionSnapshotSink>,
            quote_observer: Some(
                Arc::clone(&engine) as Arc<dyn trading_exchange::port::QuoteObserver>
            ),
            endpoints,
            venue: venue.deribit,
            hyperliquid: venue.hyperliquid,
            market_interest: Some(Arc::clone(&market_interest)),
            // CTP 运行参数：venue.toml [ctp] 段（venue_config 已回退 vendored 缺省）。
            #[cfg(feature = "ctp")]
            ctp: venue.ctp.clone(),
            // CTP K线三表（进程单例，纯内存不落盘，仿 quotes 由 store MemoryTables 持有）。
            #[cfg(feature = "ctp")]
            ctp_ticks: store.ctp_ticks(),
            #[cfg(feature = "ctp")]
            ctp_last_volumes: store.ctp_last_volumes(),
            #[cfg(feature = "ctp")]
            ctp_dominants: store.ctp_dominants(),
            // K线：无条件全量持久化 sink（定稿即落 CSV，绕过 symbol/主力路由）。
            // 交易所无关：四家 provider 各自在行情流单例创建时注册到自己的引擎/聚合器。
            persist_bar_sink: Some(kline.persistence_sink()),
            // K线历史回补分页/节流：读 venue.toml [kline.backfill]。
            hist_fetch: trading_exchange::provider::HistoricalFetchConfig {
                page_size: venue.kline.backfill.page_size,
                page_delay: venue.kline.backfill.page_delay,
            },
        };
        let exchanges = Arc::new(ExchangeRegistry::builtin(provider_deps));
        // 启动自检（fail-fast）：汇总各 provider 自描述的仓位档案装入引擎并校验，
        // 装配失败（空集/重复 exchange 等）直接 panic，拒绝「某所静默不对账」带病启动。
        engine
            .load_and_verify(&exchanges.position_profiles())
            .expect("position profiles 装配失败：拒绝带病启动");

        // 盯盘规则引擎：账户反查/下单/查单口均为服务层适配器，状态机走 RuleRepo，
        // 行情走 store 单例，引擎自身不自建共享内存表。
        let rule_lookup: Arc<dyn RuleAccountLookup> = Arc::new(accounts.clone());
        let rule_placer: Arc<dyn crate::rules::RuleOrderPlacer> =
            Arc::new(ExchangeRulePlacer::new(
                orders.clone(),
                Arc::clone(&exchanges),
                store.leverage_repo(),
            ));
        let rule_querier: Arc<dyn crate::rules::RuleOrderQuerier> =
            Arc::new(ExchangeRuleOrderQuerier::new(Arc::clone(&exchanges)));
        // 企业微信通知句柄：账户反查复用账户门面（已实现 RuleAccountLookup）。
        let notify = NotifyHub::new(
            notify_http,
            positions.clone(),
            watch_rules.clone(),
            Arc::new(accounts.clone()),
            channels,
        );

        let rule_engine = RuleEngine::new(
            watch_rules,
            orders,
            positions,
            quotes,
            rule_lookup,
            rule_placer,
            Arc::clone(&exchanges),
        )
        .with_order_querier(rule_querier)
        .with_notify_hub(notify.clone());

        // K线服务已在 provider_deps 之前构造（其 CTP 无条件持久化 sink 需注入 deps）。
        Self {
            store,
            hub,
            exchanges,
            accounts,
            rule_engine,
            position_engine: engine,
            notify,
            south_auth,
            kline,
        }
    }

    /// 解析下单账户凭证。
    ///
    /// 交易所名全局唯一、大小写敏感：先判定该所已注册（未知所直接拒绝），再用
    /// **精确名**在账户表匹配账户；账户行 exchange 与下单目标字面不一致即拒绝
    /// （不再有别名/候选名概念）。由调用方回 REJECTED。
    pub fn resolve_trading_account(
        &self,
        exchange: &str,
        account_name: &str,
    ) -> Result<trading_store::ResolvedAccount, String> {
        if !self.exchanges.is_supported(exchange) {
            return Err(format!("未知交易所 `{exchange}`：未注册对应 provider/端点"));
        }
        // 显式走账户管理器的 resolve（Deref 在此会与 CredentialResolver::resolve
        // 同名方法冲突），返回含 name/id 的已解密账户快照。
        self.accounts
            .inner()
            .resolve(account_name, Some(exchange))
            .ok_or_else(|| {
                format!(
                    "交易所 `{exchange}` 需要在 account.csv 配置 exchange 精确匹配的有效账户 `{account_name}`"
                )
            })
    }
}
