//! Hyperliquid 套件 provider（[`VenueProvider`] 实现）：注册表入口，仅覆盖
//! hyperliquid 唯一精确名；构造 [`HyperliquidUser`]、按需 spawn per-account 账户
//! 事件消费任务与交易所级公共服务（规则 + allMids/activeAssetCtx 行情）。
//!
//! 与 binance/deribit provider 平级、同构。见 `docs/扩展交易所.md` 第二节。

use std::sync::Arc;

use tokio::task::JoinHandle;

use crate::api::ExchangeApi;
use crate::hyperliquid::account_consumer::{HlUserProvider, HlVenueConnector};
use crate::hyperliquid::venue::HyperliquidUser;
use crate::port::OrderEventSink;
use crate::provider::{ProviderDeps, VenueProvider};

/// hyperliquid 唯一规范名（大小写敏感、无别名）。
const NAME: &str = "hyperliquid";

/// Hyperliquid 可调配置（市价滑点）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HyperliquidConfig {
    /// 市价 IOC 的滑点比率（0.05=5%）。
    pub slippage: rust_decimal::Decimal,
}

impl HyperliquidConfig {
    /// 默认 5% 滑点。
    pub fn default_config() -> Self {
        Self {
            slippage: "0.05".parse().unwrap(),
        }
    }
}

/// Hyperliquid 套件 provider（持有装配依赖）。
pub struct HyperliquidProvider {
    deps: ProviderDeps,
}

impl HyperliquidProvider {
    pub fn new(deps: ProviderDeps) -> Self {
        Self { deps }
    }

    /// 构造某账户的具体用户主体（REST 与账户事件流共用）。
    fn user(&self, account: &str) -> anyhow::Result<Arc<HyperliquidUser>> {
        let cred = self
            .deps
            .credentials
            .resolve(account, NAME)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "hyperliquid 需要在 account.csv 配置 exchange=hyperliquid 的账户 `{account}`"
                )
            })?;
        // Hyperliquid 凭证口径：api_secret=EVM 私钥（仅用于签名）；
        // api_key=钱包地址，作为下单/查单的 user（保持原始大小写）。
        HyperliquidUser::new(
            self.deps.http.clone(),
            Arc::clone(&self.deps.rules),
            Arc::clone(&self.deps.quotes),
            &cred.api_key,
            &cred.api_secret,
            cred.is_testnet,
            self.deps.hyperliquid.slippage,
        )
        .map(Arc::new)
    }
}

/// 账户主体供应实现：per-account 连接器经此拿 HyperliquidUser。
struct ProviderHlUsers {
    deps: ProviderDeps,
}

impl HlUserProvider for ProviderHlUsers {
    fn hl_user(&self, account: &str) -> anyhow::Result<Arc<HyperliquidUser>> {
        HyperliquidProvider::new(self.deps.clone()).user(account)
    }
}

impl VenueProvider for HyperliquidProvider {
    fn matches(&self, exchange: &str) -> bool {
        exchange.trim() == NAME
    }

    /// 接入仓位流水线：Hyperliquid 永续档案（对账 Futures，入站/兜底均 HL 合约行情）。
    fn position_profile(&self) -> Option<crate::PositionProfile> {
        Some(crate::PositionProfile::hyperliquid())
    }

    /// Hyperliquid 接口用裸币名（大小写敏感、不大写）。
    fn normalize_instrument(&self, _exchange: &str, instrument: &str) -> String {
        crate::hyperliquid::codec::to_native_coin(instrument.trim())
    }

    fn build_api(
        &self,
        _exchange: &str,
        instrument_type: Option<&str>,
        account: &str,
    ) -> anyhow::Result<Arc<dyn ExchangeApi>> {
        let product = trading_core::protocol::normalize_instrument_type(instrument_type);
        match product.as_str() {
            "futures" => Ok(self.user(account)? as Arc<dyn ExchangeApi>),
            other => {
                anyhow::bail!("hyperliquid 仅支持永续（instrument_type=futures），收到 `{other}`")
            }
        }
    }

    fn spawn_account_stream(
        &self,
        exchange: &str,
        account: &str,
        sink: Arc<dyn OrderEventSink>,
    ) -> Option<JoinHandle<()>> {
        let users: Arc<dyn HlUserProvider> = Arc::new(ProviderHlUsers {
            deps: self.deps.clone(),
        });
        HlVenueConnector::new(
            users,
            sink,
            Arc::clone(&self.deps.position_sink),
            self.deps.position_debounce,
            self.deps.position_reconcile_interval,
        )
        .spawn_for(exchange, account)
    }

    fn spawn_market_services(&self, active_exchanges: &[String]) {
        if !active_exchanges.iter().any(|n| n.trim() == NAME) {
            tracing::info!(provider = NAME, "无该交易所账户，跳过交易所级公共服务");
            return;
        }
        let Some(interests) = self.deps.market_interest.clone() else {
            tracing::warn!(provider = NAME, "未提供行情兴趣端口，跳过公共行情/规则");
            return;
        };
        // 规则 + 行情。activeAssetCtx 的 user 路由地址：provider 在此只拿得到交易所名
        // （active_exchanges），拿不到账户名清单，故暂传 None（allMids 的 mid 正常，
        // mark 增强待账户枚举接缝后补）。
        crate::hyperliquid::services::spawn_shared(
            self.deps.http.clone(),
            Arc::clone(&self.deps.rules),
            Arc::clone(&self.deps.quotes),
            self.deps.rule_refresh_interval,
            interests,
            self.deps.quote_observer.clone(),
            None,
        );
    }
}
