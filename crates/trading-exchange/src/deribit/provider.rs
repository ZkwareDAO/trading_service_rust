//! Deribit 套件 provider（[`VenueProvider`] 实现）：注册表入口，仅覆盖 deribit
//! 唯一精确名；构造 [`DeribitUser`]、按需 spawn per-account 账户事件消费任务与
//! 公共行情 worker。与 binance provider 平级、同构。

use std::sync::{Arc, OnceLock};

use tokio::task::JoinHandle;
use trading_models::MarketKind;

use crate::api::ExchangeApi;
use crate::deribit::account_consumer::{DeribitUserProvider, DeribitVenueConnector};
use crate::deribit::market::DeribitMarketFeed;
use crate::deribit::stream::DeribitMarketStream;
use crate::deribit::venue::DeribitUser;
use crate::market::{InterestSource, MarketDataStream};
use crate::market_ws::WsHubEngine;
use crate::port::OrderEventSink;
use crate::provider::{ProviderDeps, VenueProvider};

/// deribit 唯一规范名：大小写敏感精确匹配，无别名。
const NAME: &str = "deribit";

/// Deribit 套件 provider（持有装配依赖）。
pub struct DeribitProvider {
    deps: ProviderDeps,
    /// 交易所级行情流单例（首次取用即 spawn hub 驱动，进程内复用同一条连接）。
    market_stream: OnceLock<Arc<DeribitMarketStream>>,
}

impl DeribitProvider {
    pub fn new(deps: ProviderDeps) -> Self {
        Self {
            deps,
            market_stream: OnceLock::new(),
        }
    }

    /// 懒创建并返回 deribit 行情 hub 单例（建一条 WsDriver 连接）。
    fn ensure_market_stream(&self) -> anyhow::Result<Arc<DeribitMarketStream>> {
        let stream = self.market_stream.get_or_init(|| {
            let feed = Arc::new(DeribitMarketFeed::new(
                self.deps.http.clone(),
                Arc::clone(&self.deps.rules),
            ));
            // spawn 共享 WS 引擎，再包成本所拥有的具名行情流。
            let engine = WsHubEngine::spawn_hub(
                "deribit-option",
                feed,
                Arc::clone(&self.deps.quotes),
                self.deps.quote_observer.clone(),
            );
            let stream = DeribitMarketStream::from_engine(engine);
            // 无条件全量落盘 sink：单例闭包内只注册一次（行为同 binance/CTP）。
            if let Some(persist) = &self.deps.persist_bar_sink {
                stream.register_persistent_bar_sink(Arc::clone(persist));
            }
            Arc::new(stream)
        });
        Ok(Arc::clone(stream))
    }

    /// 构造某账户的具体用户主体（REST 与账户事件共用）。
    fn user(&self, account: &str) -> anyhow::Result<Arc<DeribitUser>> {
        let cred = self
            .deps
            .credentials
            .resolve(account, NAME)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "deribit 期权需要在 account.csv 配置 exchange=deribit 的账户 `{account}`"
                )
            })?;
        let crate::port::ExchangeCredential::ApiSecret {
            api_key,
            api_secret,
            is_testnet,
            ..
        } = cred
        else {
            anyhow::bail!("deribit 凭证形态错误：应为 ApiSecret");
        };
        Ok(Arc::new(DeribitUser::new(
            self.deps.http.clone(),
            Arc::clone(&self.deps.rules),
            Arc::clone(&self.deps.quotes),
            api_key,
            api_secret,
            is_testnet,
            self.deps.venue,
        )))
    }
}

impl VenueProvider for DeribitProvider {
    fn matches(&self, exchange: &str) -> bool {
        exchange.trim() == NAME
    }

    /// 接入仓位流水线：Deribit 期权档案。**fallback_valuation 必须为 None**
    /// （历史上快照兜底对 deribit 恒为 None；抽象后缺 mark 仍拿 None，不得因
    /// 改用自身 venue 查行情而产生新估值——有专项回归测试钉死）。
    fn position_profile(&self) -> Option<crate::PositionProfile> {
        Some(crate::PositionProfile::deribit())
    }

    /// Deribit 历史 K线回补能力（v2 get_tradingview_chart_data），无状态、取用即建。
    fn market_history(&self) -> Option<Arc<dyn crate::market::HistoricalKlines>> {
        let h = self.deps.hist_fetch;
        Some(Arc::new(
            crate::deribit::historical::DeribitHistorical::new(
                self.deps.http.clone(),
                h.page_size,
                h.page_delay,
            ),
        ))
    }

    /// Deribit 合约名大小写混合，保持原样（仅 trim），不可大写。
    fn normalize_instrument(&self, _exchange: &str, instrument: &str) -> String {
        instrument.trim().to_string()
    }

    fn build_api(
        &self,
        _exchange: &str,
        instrument_type: Option<&str>,
        account: &str,
    ) -> anyhow::Result<Arc<dyn ExchangeApi>> {
        let product = trading_core::protocol::normalize_instrument_type(instrument_type);
        match product.as_str() {
            "option" => Ok(self.user(account)? as Arc<dyn ExchangeApi>),
            other => anyhow::bail!("deribit 仅支持期权（instrument_type=option），收到 `{other}`"),
        }
    }

    fn build_market_stream(
        &self,
        _exchange: &str,
        instrument_type: Option<&str>,
    ) -> anyhow::Result<Arc<dyn MarketDataStream>> {
        let product = trading_core::protocol::normalize_instrument_type(instrument_type);
        if product != "option" {
            anyhow::bail!("deribit 行情仅支持期权（instrument_type=option），收到 `{product}`");
        }
        Ok(self.ensure_market_stream()? as Arc<dyn MarketDataStream>)
    }

    fn prepare_placement(
        &self,
        _venue: &str,
        request: crate::api::PlaceOrderRequest,
    ) -> Result<crate::api::PlaceOrderRequest, crate::api::PlaceDefer> {
        // 合约规格须已懒加载到表内（缺失=DataNotReady，下轮再预检）。
        let key = trading_models::OrderRuleKey::new(
            NAME,
            trading_models::InstrumentType::Option,
            &request.instrument,
        );
        let row = match self.deps.rules.get(&key) {
            Some(r) => r,
            None => return Err(crate::api::PlaceDefer::DataNotReady),
        };
        let market = crate::deribit::prep::read_market(&self.deps.quotes, &request.instrument);
        crate::deribit::prep::prepare(&row, request, market, self.deps.venue)
    }

    fn spawn_account_stream(
        &self,
        exchange: &str,
        account: &str,
        sink: Arc<dyn OrderEventSink>,
    ) -> Option<JoinHandle<()>> {
        // provider 自身作为 user 主体供应端口（持有 deps，私有 user() 构造 DeribitUser）。
        let users = Arc::new(DeribitProviderUser {
            http: self.deps.http.clone(),
            credentials: Arc::clone(&self.deps.credentials),
            rules: Arc::clone(&self.deps.rules),
            quotes: Arc::clone(&self.deps.quotes),
            venue: self.deps.venue,
        });
        DeribitVenueConnector::new(
            users,
            sink,
            Arc::clone(&self.deps.position_sink),
            self.deps.position_debounce,
            self.deps.position_reconcile_interval,
        )
        .spawn_for(exchange, account)
    }

    fn spawn_market_services(&self, active_exchanges: &[String]) {
        if !active_exchanges.iter().any(|name| name.trim() == NAME) {
            tracing::info!(provider = NAME, "无该交易所账户，跳过交易所级公共行情连接");
            return;
        }
        // 公共行情：主网恒定；兴趣集由宿主经 deps 注入。
        let Some(interests) = self.deps.market_interest.clone() else {
            tracing::warn!(provider = NAME, "未提供行情兴趣端口，跳过公共行情");
            return;
        };
        // 建 hub 单例并把系统行情来源（活跃仓位 ∪ 规则，过滤到 deribit/option）
        // 注册为与句柄同寿的托管来源；驱动任务在 ensure 内已 spawn，空集也建连保活。
        match self.ensure_market_stream() {
            Ok(stream) => stream.register_managed_prices(Arc::new(InterestSource::new(
                interests,
                trading_models::Venue::Deribit,
                MarketKind::Option,
            ))),
            Err(e) => tracing::error!(provider = NAME, error = %e, "deribit 行情 hub 启动失败"),
        }
    }
}

/// user 主体供应实现：按账户凭证构造 [`DeribitUser`]（REST 与账户流共用）。
struct DeribitProviderUser {
    http: reqwest::Client,
    credentials: Arc<dyn crate::port::CredentialResolver>,
    rules: Arc<trading_core::order_rule::OrderRuleTable>,
    quotes: Arc<trading_core::quote::QuoteTable>,
    venue: crate::deribit::prep::PrepConfig,
}

impl DeribitUserProvider for DeribitProviderUser {
    fn deribit_user(&self, account: &str) -> anyhow::Result<Arc<DeribitUser>> {
        let cred = self
            .credentials
            .resolve(account, NAME)
            .ok_or_else(|| anyhow::anyhow!("deribit 账户凭证不可解析：{account}"))?;
        let crate::port::ExchangeCredential::ApiSecret {
            api_key,
            api_secret,
            is_testnet,
            ..
        } = cred
        else {
            anyhow::bail!("deribit 凭证形态错误：应为 ApiSecret");
        };
        Ok(Arc::new(DeribitUser::new(
            self.http.clone(),
            Arc::clone(&self.rules),
            Arc::clone(&self.quotes),
            api_key,
            api_secret,
            is_testnet,
            self.venue,
        )))
    }
}
