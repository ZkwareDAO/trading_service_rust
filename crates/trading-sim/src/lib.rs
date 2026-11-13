//! 可编程模拟交易所（M1 全链路 E2E 的测试替身，dev-only）。
//!
//! 分层：
//! * [`ledger::SimLedger`]：**交易所侧独立台账**（订单/成交/持仓/保证金/余额 +
//!   事件队列）。刻意不复用宿主的仓储/计算，作为 E2E 的独立 Oracle；
//! * [`api::SimApi`]：按账户实现 [`trading_exchange::api::ExchangeApi`] 与
//!   [`trading_exchange::api::AccountEventStream`]；
//! * [`venue::SimVenue`]：实现 [`trading_exchange::provider::VenueProvider`]，
//!   注入宿主注册表；`set_mark` 写宿主行情表并回调 `QuoteObserver`。
//!
//! 语义约束（M1）：
//! * 限价单默认**不自动成交**（[`LimitFillMode::NoFill`]），测试显式
//!   `fill_order` 或切到 [`LimitFillMode::FillWhenCrosses`] 后由 `set_mark` 穿价成交；
//! * 市价单必须已有 mark 才受理，受理后即时按 mark 成交；
//! * 全部事件经共享队列 + [`tokio::sync::Notify`] 分发，不使用 channel。

pub mod api;
pub mod ledger;
pub mod venue;

use rust_decimal::Decimal;
use trading_models::{InstrumentType, MarketKind, Venue};

/// 限价单被动成交策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LimitFillMode {
    /// 不自动成交：仅响应显式 `fill_order`（默认，杜绝测试时序不确定）。
    #[default]
    NoFill,
    /// `set_mark` 穿价时自动成交（买单 mark ≤ 委托价，卖单 mark ≥ 委托价）。
    FillWhenCrosses,
}

/// 查单三态行为（契约测试据此钉住 Found / NotExists / Unavailable）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueryMode {
    /// 按台账真实状态返回 Found / NotExists（默认）。
    #[default]
    Found,
    /// 一律 NotExists（交易所明确查无）。
    NotExists,
    /// 一律 Unavailable（查询本身失败）。
    Unavailable,
}

/// 模拟所静态配置。`exchange` 为注册表精确名（测试用 "sim"）。
#[derive(Debug, Clone)]
pub struct SimConfig {
    pub exchange: &'static str,
    pub instrument_type: InstrumentType,
    /// 行情表 key 的 venue/kind（`Venue` 无 Sim 变体，测试复用既有枚举值）。
    pub quote_venue: Venue,
    pub quote_kind: MarketKind,
    pub settle_currency: String,
    pub initial_balance: Decimal,
    pub leverage: u32,
    pub limit_fill: LimitFillMode,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            exchange: "sim",
            instrument_type: InstrumentType::Futures,
            // 行情 key 仅用于测试内路由；SimVenue 是注册表里唯一的 provider，
            // 不会与真实 Binance 档案冲突。
            quote_venue: Venue::BinanceFutures,
            quote_kind: MarketKind::Futures,
            settle_currency: "USDT".to_string(),
            initial_balance: Decimal::from(100_000),
            leverage: 10,
            limit_fill: LimitFillMode::NoFill,
        }
    }
}

impl SimConfig {
    /// 以注册表精确名构造（其余取默认）。
    pub fn new(exchange: &'static str) -> Self {
        Self {
            exchange,
            ..Self::default()
        }
    }
}

/// 当前 UTC 毫秒（模拟所事件时间戳用）。
pub(crate) fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
