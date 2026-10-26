//! 行情统一入口的对外类型（跨所共用的行情流契约）。
//!
//! 本模块只定义**跨交易所共用的契约与值类型**，不含任何连接生命周期或具体所协议：
//! - [`MarketDataStream`]：每家交易所实现的两个订阅原语（系统行情 / K线）；
//! - [`DesiredSource`]：「现在想要哪些合约」的动态来源（固定快照 / 兴趣集适配）；
//! - [`BarSink`]：定稿 K线的同步回调接收方；
//! - [`SubscriptionGuard`]：drop 即退订的凭证；
//! - [`PriceTick`] / [`Kline`] / [`KlineInterval`]：行情与 K线值类型。
//!
//! 设计约束：hub 内**零 channel**，消费者以 [`std::sync::Arc`] 注册表 + 同步回调
//! 交接（同 `QuoteObserver` 模式）。本模块的值类型是**进程内传输/回调契约**，不落盘；
//! 持久化行模型在 trading-models（[`trading_models::Bar`] 与原始 tick `RawTick`），
//! 由 trading-store 的 KlineRepo 写 CSV。

use std::sync::Arc;

use rust_decimal::Decimal;
use trading_models::{MarketKind, Venue};

use crate::port::{InterestKey, MarketInterest};

/// 参考价口径：最新价 / 中间价 / 标记价（与 [`crate::api::PriceBasis`] 同语义，
/// 行情层独立一份供 hub/feed 统一路由）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriceBasis {
    Last,
    Mid,
    Mark,
}

/// K线周期。本期仅落 1 分钟，枚举预留扩展，序列化口径与南向协议一致（`1m`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KlineInterval {
    /// 1 分钟。
    M1,
}

impl KlineInterval {
    /// 周期包含的分钟数（归桶/起止时间用）。
    pub fn minutes(self) -> i64 {
        match self {
            KlineInterval::M1 => 1,
        }
    }

    /// 南向/CSV 使用的稳定字符串标识。
    pub fn as_str(self) -> &'static str {
        match self {
            KlineInterval::M1 => "1m",
        }
    }
}

/// 一笔价格 tick（hub 内部路由 + 写 QuoteTable 的统一载体）。
///
/// [`Self::basis`] 决定写 last/mid/mark 哪一列；[`Self::volume`] 携带成交量
/// （CTP 为当日累计量、聚合差分用）。
#[derive(Debug, Clone)]
pub struct PriceTick {
    /// 交易所原生符号。
    pub symbol: String,
    /// 本笔价格口径。
    pub basis: PriceBasis,
    /// 主价格（按 basis 解释为 last/mid/mark）。
    pub price: Decimal,
    /// 买一价（缺省 None）。
    pub best_bid: Option<Decimal>,
    /// 卖一价（缺省 None）。
    pub best_ask: Option<Decimal>,
    /// 成交量语义随所而定：CTP=当日累计量；币圈所可能为单笔量，不用作累计差分。
    pub volume: Option<Decimal>,
    /// 交易所事件时间（UTC 毫秒）。
    pub event_ms: i64,
}

/// 一根已定稿 K线（消费者只会收到定稿，不收分钟中间态）。
///
/// 成交量为**周期内增量**（CTP 由相邻周期末累计量差分得到，绝不直接抄累计量）；
/// `trading_day` 为 YYYYMMDD，**夜盘归下一交易日**（由 CTP 适配层归桶后给值）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Kline {
    /// 交易所标识（binance / deribit / hyperliquid / ctp）。
    pub venue: String,
    /// 标的：CTP 如实为具体合约（cu2512），不拼主力连续。
    pub symbol: String,
    /// 周期。
    pub interval: KlineInterval,
    /// 归属交易日（YYYYMMDD；夜盘归次日）。
    pub trading_day: u32,
    /// 桶开始时间（UTC 毫秒，含）。
    pub start_ms: i64,
    /// 桶结束时间（UTC 毫秒，不含）。
    pub end_ms: i64,
    /// 开盘价。
    pub open: Decimal,
    /// 最高价。
    pub high: Decimal,
    /// 最低价。
    pub low: Decimal,
    /// 收盘价。
    pub close: Decimal,
    /// 周期内成交量（增量）。
    pub volume: i64,
    /// 周期内成交额（增量）。
    pub turnover: Decimal,
    /// 周期末总持仓量。
    pub open_interest: Decimal,
}

/// 期望订阅来源：交易所侧每 500ms 调一次 [`Self::symbols`] 得到当前想要的合约。
///
/// 实现必须廉价、纯内存（读一次快照），不得做 IO；返回内容随时间变化即产生
/// 增订/退订。空列表的语义由具体 hub 策略解释（binance=全市场，deribit=空集保活）。
pub trait DesiredSource: Send + Sync {
    /// 当前期望的合约快照（调用方去重、排序后使用）。
    fn symbols(&self) -> Vec<String>;
}

/// 固定快照来源：订阅一组在生命周期内不变的具体合约（按需消费者常用）。
#[derive(Debug, Clone, Default)]
pub struct FixedSymbols {
    symbols: Vec<String>,
}

impl FixedSymbols {
    pub fn new(symbols: Vec<String>) -> Self {
        Self { symbols }
    }
}

impl DesiredSource for FixedSymbols {
    fn symbols(&self) -> Vec<String> {
        self.symbols.clone()
    }
}

/// 兴趣集适配：把宿主 [`MarketInterest`]（按 venue+kind 给 `InterestKey`）
/// 过滤到本 hub 实例对应的 (venue, kind)，只取 symbol。
///
/// 使「系统行情（活跃仓位 ∪ 规则）」与「按需订阅」成为同型的 [`DesiredSource`]。
pub struct InterestSource {
    interests: Arc<dyn MarketInterest>,
    venue: Venue,
    kind: MarketKind,
}

impl InterestSource {
    pub fn new(interests: Arc<dyn MarketInterest>, venue: Venue, kind: MarketKind) -> Self {
        Self {
            interests,
            venue,
            kind,
        }
    }
}

impl DesiredSource for InterestSource {
    fn symbols(&self) -> Vec<String> {
        self.interests
            .interests()
            .into_iter()
            .filter(|k: &InterestKey| k.venue == self.venue && k.kind == self.kind)
            .map(|k| k.symbol)
            .collect()
    }
}

/// 定稿 K线接收方。
///
/// **契约（硬约束）**：同步、纯内存、廉价（比较/差分/写内存表/入既有队列），
/// 禁止 IO、await、长锁，目标 <1µs。调用方以 `catch_unwind` + 单次耗时计时隔离，
/// panic 或超时的 sink 会被摘除，不影响连接与其它消费者。
pub trait BarSink: Send + Sync {
    /// 一根 K线定稿时被同步调用。
    fn on_bar(&self, bar: &Kline);
}

/// 退订凭证：drop 时调用注册时登记的注销动作（从 hub 注册表移除该消费者，
/// 其独占符号在下轮对账退订）。注销动作只做内存摘除，必须廉价、不 panic。
pub struct SubscriptionGuard {
    /// 注销闭包；仅在 drop 时取走执行一次。
    on_drop: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl SubscriptionGuard {
    /// 由 hub 在登记成功后构造，传入「注销该消费者」的闭包。
    pub fn new(on_drop: Box<dyn FnOnce() + Send + Sync>) -> Self {
        Self {
            on_drop: Some(on_drop),
        }
    }
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        if let Some(f) = self.on_drop.take() {
            f();
        }
    }
}

/// 统一行情流：每家交易所一个进程单例（经 `registry.market_stream(venue, itype)` 取）。
///
/// 两个原语：
/// - [`MarketDataStream::subscribe_prices`]：系统行情，tick 写 QuoteTable + observer；
/// - [`MarketDataStream::subscribe_klines`]：K线，WS 三家=原生收盘帧，CTP=内部 tick 聚合。
///
/// xili 两方法均返回 `Unsupported`。
pub trait MarketDataStream: Send + Sync {
    /// 订阅系统行情：返回凭证，drop 退订。
    fn subscribe_prices(&self, src: Arc<dyn DesiredSource>) -> anyhow::Result<SubscriptionGuard>;

    /// 订阅 K线：登记期望来源 + 定稿回调，返回凭证，drop 退订。
    fn subscribe_klines(
        &self,
        src: Arc<dyn DesiredSource>,
        sink: Arc<dyn BarSink>,
    ) -> anyhow::Result<SubscriptionGuard>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn fixed_symbols_returns_snapshot() {
        let src = FixedSymbols::new(vec!["rb2510".into(), "cu2512".into()]);
        assert_eq!(src.symbols(), vec!["rb2510", "cu2512"]);
    }

    #[test]
    fn fixed_symbols_default_is_empty() {
        assert!(FixedSymbols::default().symbols().is_empty());
    }

    /// 假兴趣供给：返回混合 venue/kind 的 InterestKey。
    struct MixedInterests;
    impl MarketInterest for MixedInterests {
        fn interests(&self) -> Vec<InterestKey> {
            vec![
                InterestKey::new(Venue::Ctp, MarketKind::Futures, "ctp-hit"),
                InterestKey::new(Venue::Ctp, MarketKind::Futures, "ctp-hit2"),
                InterestKey::new(Venue::Ctp, MarketKind::Spot, "wrong-kind"),
                InterestKey::new(Venue::BinanceFutures, MarketKind::Futures, "wrong-venue"),
            ]
        }
    }

    #[test]
    fn interest_source_filters_by_venue_and_kind() {
        let src = InterestSource::new(Arc::new(MixedInterests), Venue::Ctp, MarketKind::Futures);
        let mut got = src.symbols();
        got.sort();
        assert_eq!(got, vec!["ctp-hit", "ctp-hit2"]);
    }

    #[test]
    fn guard_drop_runs_deregister_once() {
        let called = Arc::new(AtomicBool::new(false));
        let c = Arc::clone(&called);
        let guard = SubscriptionGuard::new(Box::new(move || {
            c.store(true, Ordering::SeqCst);
        }));
        assert!(!called.load(Ordering::SeqCst));
        drop(guard);
        assert!(called.load(Ordering::SeqCst));
    }

    #[test]
    fn interval_minutes_and_tag() {
        assert_eq!(KlineInterval::M1.minutes(), 1);
        assert_eq!(KlineInterval::M1.as_str(), "1m");
    }
}
