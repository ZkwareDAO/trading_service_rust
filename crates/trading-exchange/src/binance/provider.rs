//! Binance 交易所套件（[`VenueProvider`] 实现）。
//!
//! * [`BinanceUsers`]：按账户凭证缓存签名 SDK client、构造 [`BinanceUser`]，
//!   同时作为 per-account 账户事件连接器的用户主体供应端口；
//! * [`BinanceProvider`]：注册表用的套件入口，仅覆盖 binance 唯一精确名，
//!   并在进程级启动规则同步与公共行情任务。

use std::sync::{Arc, OnceLock};

use tokio::task::JoinHandle;

use crate::api::ExchangeApi;
use crate::binance::client::{BinanceFuturesClientCache, BinanceFuturesOptions};
use crate::binance::services::{self, spawn_market_hubs};
use crate::binance::stream::BinanceMarketStream;
use crate::binance::user_stream::{BinanceUserProvider, BinanceVenueConnector};
use crate::binance::venue::BinanceUser;
use crate::market::MarketDataStream;
use crate::port::{CredentialResolver, OrderEventSink};
use crate::provider::{ProviderDeps, VenueProvider};

/// binance 唯一规范名：全局唯一、大小写敏感精确匹配，不接受别名。
/// 合约/现货由 `instrument_type` 区分（futures / spot），不靠交易所名。
const NAME: &str = "binance";

/// 按账户构造/复用 Binance 用户主体（SDK client 缓存 + 共享规则表）。
#[derive(Clone)]
pub struct BinanceUsers {
    cache: BinanceFuturesClientCache,
    credentials: Arc<dyn CredentialResolver>,
    rules: Arc<trading_core::order_rule::OrderRuleTable>,
    /// 单次 REST 请求超时（毫秒），透传给 SDK client。
    timeout_ms: u64,
}

impl BinanceUsers {
    pub fn new(
        credentials: Arc<dyn CredentialResolver>,
        rules: Arc<trading_core::order_rule::OrderRuleTable>,
        timeout_ms: u64,
    ) -> Self {
        Self {
            cache: BinanceFuturesClientCache::new(),
            credentials,
            rules,
            timeout_ms,
        }
    }

    /// 构造具体的 Binance 用户主体（REST + 账户事件流共用，复用凭证缓存与规则表）。
    /// 选网（正式/测试）是账户属性，取自凭证的 is_testnet。
    pub fn user(&self, account: &str) -> anyhow::Result<Arc<BinanceUser>> {
        let cred = self.credentials.resolve(account, NAME).ok_or_else(|| {
            anyhow::anyhow!(
                "binance 合约需要在 account.csv 配置 exchange=binance 的账户 `{account}`"
            )
        })?;
        let crate::port::ExchangeCredential::ApiSecret {
            position_mode,
            is_testnet,
            ..
        } = &cred
        else {
            anyhow::bail!("binance 凭证形态错误：应为 ApiSecret");
        };
        let opts = BinanceFuturesOptions::new(*is_testnet, self.timeout_ms);
        let sdk = self.cache.get_or_build(account, &cred, opts)?;
        Ok(Arc::new(BinanceUser::from_sdk(
            sdk,
            Arc::clone(&self.rules),
            *position_mode,
            *is_testnet,
        )))
    }
}

/// 用户主体供应端口：per-account 连接器经此拿到具体 BinanceUser。
impl BinanceUserProvider for BinanceUsers {
    fn binance_user(&self, account: &str) -> anyhow::Result<Arc<BinanceUser>> {
        self.user(account)
    }
}

/// Binance 交易所套件（注册表中的一个 provider）。
pub struct BinanceProvider {
    users: Arc<BinanceUsers>,
    deps: ProviderDeps,
    /// 现货/合约两条本所拥有的行情流（首次取用即一起构造，进程单例）。
    market_hubs: OnceLock<(Arc<BinanceMarketStream>, Arc<BinanceMarketStream>)>,
}

impl BinanceProvider {
    pub fn new(deps: ProviderDeps) -> Self {
        let users = Arc::new(BinanceUsers::new(
            Arc::clone(&deps.credentials),
            Arc::clone(&deps.rules),
            deps.rest_timeout.as_millis().max(1) as u64,
        ));
        Self {
            users,
            deps,
            market_hubs: OnceLock::new(),
        }
    }

    /// 懒构造并返回 (现货, 合约) 两条本所行情流单例。
    fn ensure_market_hubs(&self) -> &(Arc<BinanceMarketStream>, Arc<BinanceMarketStream>) {
        self.market_hubs.get_or_init(|| {
            let hubs = spawn_market_hubs(
                Arc::clone(&self.deps.quotes),
                self.deps.quote_observer.clone(),
            );
            // 无条件全量落盘 sink 在单例首次创建闭包内注册一次（闭包仅执行一次，
            // 避免 ensure 多次调用重复挂同一 sink 导致 bar 重复 append）。
            // 本期 K线只接通合约（KlineService 作用域仅 futures）：只注册到合约引擎，
            // 现货引擎不订阅任何 kline 频道；且 Kline/CSV 路径无 instrument_type
            // 维度（venue="binance" 唯一），同时挂现货会让将来的现货/合约同名标的
            // 静默写进同一 CSV，故刻意不注册现货。
            if let Some(persist) = &self.deps.persist_bar_sink {
                hubs.1.register_unconditional_bar_sink(Arc::clone(persist));
            }
            hubs
        })
    }
}

impl VenueProvider for BinanceProvider {
    fn matches(&self, exchange: &str) -> bool {
        exchange == NAME
    }

    /// 接入仓位流水线：Binance U 本位永续档案（对账 Futures，入站/兜底均币安合约行情）。
    fn position_profile(&self) -> Option<crate::PositionProfile> {
        Some(crate::PositionProfile::binance())
    }

    /// Binance U 本位永续历史 K线回补能力（/fapi/v1/klines）。实现无状态、构造廉价
    /// （仅 clone 共享 HTTP client + Copy 配置），故每次取用新建，无需 OnceLock 缓存。
    fn market_history(&self) -> Option<Arc<dyn crate::market::HistoricalKlines>> {
        let h = self.deps.hist_fetch;
        Some(Arc::new(
            crate::binance::historical::BinanceHistorical::new(
                self.deps.http.clone(),
                h.page_size,
                h.page_delay,
            ),
        ))
    }

    /// Binance 现货/合约标的一律大写（交易所原生口径）；宿主据此在写内存订单前归一。
    fn normalize_instrument(&self, _exchange: &str, instrument: &str) -> String {
        instrument.trim().to_ascii_uppercase()
    }

    fn build_api(
        &self,
        _exchange: &str,
        instrument_type: Option<&str>,
        account: &str,
    ) -> anyhow::Result<Arc<dyn ExchangeApi>> {
        // 到达此处 exchange 已由 matches 精确限定为 "binance"；产品只看 instrument_type。
        let product = trading_core::protocol::normalize_instrument_type(instrument_type);
        match product.as_str() {
            "futures" => Ok(self.users.user(account)? as Arc<dyn ExchangeApi>),
            "spot" => {
                anyhow::bail!("binance 现货适配器尚未接入（当前仅支持 instrument_type=futures）")
            }
            other => anyhow::bail!("binance 不支持的产品 `{other}`"),
        }
    }

    fn build_market_stream(
        &self,
        _exchange: &str,
        instrument_type: Option<&str>,
    ) -> anyhow::Result<Arc<dyn MarketDataStream>> {
        let (spot, futures) = self.ensure_market_hubs();
        let product = trading_core::protocol::normalize_instrument_type(instrument_type);
        match product.as_str() {
            // 现货行情 hub 提供（现货下单 REST 尚未接入，但公共行情可用）。
            "spot" => Ok(Arc::clone(spot) as Arc<dyn MarketDataStream>),
            "futures" => Ok(Arc::clone(futures) as Arc<dyn MarketDataStream>),
            other => anyhow::bail!("binance 行情不支持产品 `{other}`"),
        }
    }

    fn spawn_account_stream(
        &self,
        exchange: &str,
        account: &str,
        sink: Arc<dyn OrderEventSink>,
    ) -> Option<JoinHandle<()>> {
        // 连接器内部再按精确名判定 + 解析凭证，不支持/缺凭证返回 None。
        // 选网在解析出具体 BinanceUser 时按账户 is_testnet 决定，连接器无需环境参数。
        BinanceVenueConnector::new(
            Arc::clone(&self.deps.credentials),
            sink,
            self.users.clone(),
            Arc::clone(&self.deps.position_sink),
            self.deps.position_debounce,
            self.deps.position_reconcile_interval,
        )
        .spawn_for(exchange, account)
    }

    fn spawn_market_services(&self, active_exchanges: &[String]) {
        // 仅当账户表里确有 binance 账户时才外连交易所级公共任务（去硬编码：
        // 没有 binance 账户的部署不会无条件连接 binance 行情/规则）。
        if !active_exchanges.iter().any(|name| name == NAME) {
            tracing::info!(
                provider = NAME,
                "无该交易所账户，跳过交易所级公共行情/规则连接"
            );
            return;
        }
        // 规则同步（保留）：启动全量 + 定时整表刷新，恒定正式网、与账户无关。
        services::spawn_rule_sync(
            Arc::clone(&self.deps.rules),
            self.deps.rule_refresh_interval,
        );
        // 公共行情：现货 last + 合约 mark 两条全市场 hub（URL 即订阅全市场，
        // 空集=全市场、建连即推）。ensure 内已 spawn 驱动任务。
        let _ = self.ensure_market_hubs();
    }
}
