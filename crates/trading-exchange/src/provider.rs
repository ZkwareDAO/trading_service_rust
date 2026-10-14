//! 交易所「套件」扩展点：一家交易所实现一个 [`VenueProvider`] 并在注册表登记一行，
//! 装配方（main/service/registry 路由）即可零改动支持该所。
//!
//! 一家交易所的能力按生命周期分两类，均在 trait 上给默认实现，按需覆盖：
//! * **用户 × 交易所级**：[`VenueProvider::build_api`] 构造下单主体；
//!   [`VenueProvider::spawn_account_stream`] 为某账户起一条账户事件 WS；
//! * **交易所级进程单例**：[`VenueProvider::spawn_market_services`]（规则同步/公共行情）。
//!
//! 构造期一次性注入 [`ProviderDeps`]（均为廉价 clone 的 Arc 句柄）；订单回报出口
//! [`OrderEventSink`] 因处于运行期装配，在各启动方法上以参数传入。

use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use tokio::task::JoinHandle;
use trading_core::order_rule::OrderRuleTable;
use trading_core::quote::QuoteTable;

use crate::api::ExchangeApi;
use crate::deribit::prep::PrepConfig;
use crate::hyperliquid::provider::HyperliquidConfig;
use crate::port::{MarketInterest, OrderEventSink, PositionSnapshotSink, QuoteObserver};
use crate::position_profile::PositionProfile;

/// 一个交易所实例的静态连接端点（宿主 `--exchange name=url` 配置，进程生命周期）。
///
/// 对无固定内置地址的交易所（如教学所 xili）用于显式覆盖；binance 等真实所忽略此项。
#[derive(Debug, Clone)]
pub struct VenueEndpoint {
    /// 交易所实例名（策略下单的 exchange 字段，如 xili）。
    pub name: String,
    /// REST 基址，如 `http://127.0.0.1:9200`。
    pub rest_base_url: String,
    /// 账户事件 WS 地址，由 REST 基址推导（`/ws`，http→ws、https→wss）。
    pub ws_url: String,
}

impl VenueEndpoint {
    /// 由名称与 REST 基址构造；WS 地址固定为 `<base>/ws`（http→ws、https→wss）。
    pub fn new(name: impl Into<String>, rest_base_url: &str) -> anyhow::Result<Self> {
        let rest_base_url = rest_base_url.trim_end_matches('/').to_string();
        let ws_url = if let Some(rest) = rest_base_url.strip_prefix("https://") {
            format!("wss://{rest}/ws")
        } else if let Some(rest) = rest_base_url.strip_prefix("http://") {
            format!("ws://{rest}/ws")
        } else {
            anyhow::bail!("端点 url 必须以 http:// 或 https:// 开头：`{rest_base_url}`");
        };
        Ok(Self {
            name: name.into(),
            rest_base_url,
            ws_url,
        })
    }
}

/// 构造 provider 时由宿主注入的共享依赖（交易所无关）。
#[derive(Clone)]
pub struct ProviderDeps {
    /// 共享 HTTP client（带超时）。
    pub http: Client,
    /// 进程一份、所有账户共享的交易规则表（内存不落盘）。
    pub rules: Arc<OrderRuleTable>,
    /// 进程一份的行情表（公共流写入；内存不落盘）。
    pub quotes: Arc<QuoteTable>,
    /// 账户凭证解析端口（宿主实现）。
    pub credentials: Arc<dyn crate::port::CredentialResolver>,
    /// 单次交易所 REST 请求超时（透传给各 venue 的签名 SDK client）。
    pub rest_timeout: Duration,
    /// 规则整表刷新周期。
    pub rule_refresh_interval: Duration,
    /// 仓位对账去抖窗口（账户脏信号合并）。
    pub position_debounce: Duration,
    /// 仓位定时全量对账周期（漏推兜底）。
    pub position_reconcile_interval: Duration,
    /// 仓位 REST 快照出口（去抖到点拉到真值后交宿主对账落库）。
    pub position_sink: Arc<dyn PositionSnapshotSink>,
    /// 行情写价观察口（仅刷新对应交易所仓位）。
    pub quote_observer: Option<Arc<dyn QuoteObserver>>,
    /// 运行期静态端点覆盖（通用机制，非任何交易所专用）。
    pub endpoints: Arc<Vec<VenueEndpoint>>,
    /// Venue 可调参数（如 deribit 价差阈值/漂移档数）。
    pub venue: PrepConfig,
    /// CTP 运行参数（.so 路径/流目录基准；venue.toml [ctp] 段，缺失回退 vendored）。
    #[cfg(feature = "ctp")]
    pub ctp: crate::ctp::CtpConfig,
    /// Hyperliquid 可调参数（市价滑点）。
    pub hyperliquid: HyperliquidConfig,
    /// 行情订阅兴趣供给（宿主读仓位∪规则）；None=不做动态订阅。
    pub market_interest: Option<Arc<dyn MarketInterest>>,
}

/// 一家交易所的完整套件扩展点。注册表按 [`VenueProvider::matches`] 把请求路由给它。
pub trait VenueProvider: Send + Sync {
    /// 优雅停机钩子：停掉该所按账户的长连接线程。
    ///
    /// 默认无操作；CTP 等阻塞线程实现必须置位停机标志——`abort` 对
    /// `spawn_blocking` 任务无效，标志是其唯一停机通道。
    fn stop_account_streams(&self) {}

    /// 该所是否归本 provider 处理。
    ///
    /// 约定：交易所名全局唯一，做**大小写敏感的精确相等**匹配，不接受别名、
    /// 不做大小写归一；字面不一致即视为未知所，由上层拒绝。基于运行时端点配置的
    /// provider（如 xili）按配置的端点名精确匹配。
    fn matches(&self, exchange: &str) -> bool;

    /// 该所接入「仓位对账 / 行情重估」流水线的自描述档案；默认 [`None`] = 不接入
    /// （不建仓位行、不参与对账作用域、不被行情 tick 重估）。
    ///
    /// 接入的交易所（binance / deribit / hyperliquid）覆盖之返回各自
    /// [`PositionProfile`]；engine 启动时经注册表汇总所有档案并 `load_and_verify`
    /// 自检，装配失败即 fail-fast。未覆盖者（如 xili）天然安全地置身流水线外。
    fn position_profile(&self) -> Option<PositionProfile> {
        None
    }

    /// 把入站标的符号归一为该所原生口径，在宿主写内存订单**之前**调用，
    /// 保证内存订单/仓位/行情/回报四处符号一致。默认仅 trim；大小写敏感的所
    /// （如 Binance 全大写）覆盖之。**禁止**对大小写混合标的的所（Deribit 等）盲目大写。
    fn normalize_instrument(&self, _exchange: &str, instrument: &str) -> String {
        instrument.trim().to_string()
    }

    /// claim/建单前的**同步、纯内存**预检（无网络/凭证）。
    ///
    /// 默认原样透传（Binance/xili 不覆盖=行为不变）；需要市价转限价或平仓价差
    /// 闸门的交易所（Deribit）覆盖：从共享规则表/行情表读内存做转换。
    /// 返回 [`PlaceDefer`] 时引擎**不 claim、不建单**，规则保持 active、下轮重试。
    fn prepare_placement(
        &self,
        _venue: &str,
        request: crate::api::PlaceOrderRequest,
    ) -> Result<crate::api::PlaceOrderRequest, crate::api::PlaceDefer> {
        Ok(request)
    }

    /// 构造某 (交易所, 产品, 账户) 的下单/查询主体。
    fn build_api(
        &self,
        exchange: &str,
        instrument_type: Option<&str>,
        account: &str,
    ) -> anyhow::Result<Arc<dyn ExchangeApi>>;

    /// 为某账户起一条账户事件 WS 任务；该所不支持 per-account 流时返回 None
    /// （未知所由注册表兜底为占位任务）。
    fn spawn_account_stream(
        &self,
        _exchange: &str,
        _account: &str,
        _sink: Arc<dyn OrderEventSink>,
    ) -> Option<JoinHandle<()>> {
        None
    }

    /// 启动交易所级共享任务：规则同步/定时刷新、公共行情订阅。默认无。
    ///
    /// `active_exchanges` 为宿主从账户内存表去重出的、当前确有账户的交易所规范名
    /// （字典序）。实现据此**仅在本 provider 归属的交易所在列时**建连，避免在没有
    /// 任何账户时也无条件外连公共行情（去除对某一家交易所的硬编码）。
    /// 仅启动时调用一次；运行期热加载新增交易所的公共服务需重启（per-account
    /// 用户流不受此限，仍支持热增删）。
    fn spawn_market_services(&self, _active_exchanges: &[String]) {}
}
