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

use std::collections::BTreeMap;
use std::sync::Arc;

use rust_decimal::Decimal;
use trading_models::{MarketKind, Venue};

pub use trading_models::KlineExtra;

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

    /// 周期的毫秒长度（墙钟定稿按此推导绝对边界，不写死 60_000；未来加 M30 只需在此补值）。
    pub fn duration_ms(self) -> i64 {
        self.minutes() * 60_000
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
///
/// 12 个公共强类型字段四所一致；各所差异指标放 [`Self::extras`]（CTP:
/// exchange_id/open_interest；币安: trade_num/active_buy_volume/active_buy_quote_volume），
/// 新增交易所只加本所映射 + 本所 extras 键，公共结构不动。
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
    /// 周期内成交额（增量，计价货币口径；CTP=Turnover、币安=q）。
    pub quote_volume: Decimal,
    /// 各所扩展字段（CTP: exchange_id/open_interest；币安: trade_num/active_buy_*；
    /// deribit/hyperliquid 为空）。
    pub extras: BTreeMap<String, KlineExtra>,
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
/// 禁止 IO、await、长锁，目标 <1µs。调用方以 `catch_unwind` + 单次耗时计时隔离：
/// 南向网络路由消费者 panic/超时会被摘除；本地无条件下游按注册策略处理
/// （持久化 sink 不测耗时、panic 后自愈），均不影响连接与其它消费者。
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

/// 统一行情流：与 [`crate::api::ExchangeApi`] 同等级的、**每家交易所各自拥有**的契约
/// （一个进程单例，经 `registry.market_stream(venue, itype)` 取）。
///
/// 所有权模型（扩展性关键）：每家交易所在自己模块内有一个具名 impl，需要所内额外处理
/// （装饰/过滤 sink、挂第二来源、拦截订阅等）只改该 impl，不动本 trait、不动其他所：
/// - binance/deribit/hyperliquid：各自的 `XxxMarketStream` 具名 impl，内部**委托**
///   可组合引擎 [`crate::market_ws::WsHubEngine`]（引擎本身**不实现**本 trait）；
/// - ctp：`ctp::market_stream::CtpMarketStream` 独立 impl（单连接 tick 聚合，feature="ctp"）。
///
/// 两个原语：
/// - [`MarketDataStream::subscribe_prices`]：系统行情，tick 写 QuoteTable + observer；
/// - [`MarketDataStream::subscribe_klines`]：K线，WS 三家=原生收盘帧，CTP=内部 tick 聚合。
///
/// xili 不覆盖 `build_market_stream`，两方法经 provider 默认实现返回 `Unsupported`。
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

/// 历史 K线 REST 拉取能力（仅 binance/deribit/hyperliquid 三家 7×24 连续撮合所实现；
/// CTP 无公开历史接口，[`crate::provider::VenueProvider::market_history`] 默认返回 `None`）。
///
/// 用于 P4 缺口自动回补 / P5 管理面手动补区间。实现约定：
/// - 只返回**已收盘** bar；区间为半开 `[start_ms, end_ms)`（均为 UTC 毫秒）；
/// - 结果按 `start_ms` 升序，并在实现内按区间再过滤一次，杜绝端点越界；
/// - HTTP 非 2xx / 响应结构不符 / 解析失败一律以 `Err` 返回（不静默丢根）。
///
/// 回补到的 bar **只**写入内存 `BarTable` 并随后续 CSV flush 落盘，**绝不**进入实时
/// fanout/南向推送（该约束在 service 编排层保证，trait 本身只负责取数）。
#[async_trait::async_trait]
pub trait HistoricalKlines: Send + Sync {
    /// 拉取 `[start_ms, end_ms)` 内给定周期的已收盘 K线，按 `start_ms` 升序返回。
    async fn fetch_klines(
        &self,
        symbol: &str,
        interval: KlineInterval,
        start_ms: i64,
        end_ms: i64,
    ) -> anyhow::Result<Vec<Kline>>;
}

/// 把请求的半开根数区间 `[start_ms, end_ms)` 按 `page_size` 根/页切成一组连续、不重叠、
/// 首尾贴合的半开窗口 `(win_start, win_end)`（均 UTC 毫秒）。纯函数、零 IO，三家分页共用。
///
/// `step_ms<=0`、`page_size==0`、或 `end_ms<=start_ms` 返回空；最后一页为不足整页的余数。
pub(crate) fn page_windows(
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    page_size: usize,
) -> Vec<(i64, i64)> {
    if step_ms <= 0 || page_size == 0 || end_ms <= start_ms {
        return Vec::new();
    }
    let page_span = step_ms.saturating_mul(page_size as i64);
    let mut out = Vec::new();
    let mut cur = start_ms;
    while cur < end_ms {
        let win_end = cur.saturating_add(page_span).min(end_ms);
        out.push((cur, win_end));
        cur = win_end;
    }
    out
}

/// 三家历史回补共用的分页收集内核：本地按 [`page_windows`] 切出确定窗口，逐窗调用注入的
/// `fetch_window(win_start, win_end)` 取数，相邻窗之间 sleep `page_delay` 节流。
///
/// 取数完成后统一：按请求半开区间 `[start_ms,end_ms)` 再过滤一次（杜绝端点越界）、
/// 按 `start_ms` 升序、同 `start_ms` 去重（保留先到者）。窗口由本地**有限生成**，故不存在
/// 无限分页；**空窗不停止**——缺口回补的区间内可能夹着交易所无 K线的时段（未上市/停牌/
/// 已下架），早窗为空不代表更晚窗口也为空，提前 break 会错误丢弃后续真实数据。
/// `fetch_window` 返回 `Err` 立即向上传播（不静默丢根）。
pub(crate) async fn collect_pages<F, Fut>(
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    page_size: usize,
    page_delay: std::time::Duration,
    mut fetch_window: F,
) -> anyhow::Result<Vec<Kline>>
where
    F: FnMut(i64, i64) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Vec<Kline>>>,
{
    let windows = page_windows(start_ms, end_ms, step_ms, page_size);
    let mut bars: Vec<Kline> = Vec::new();
    for (idx, (ws, we)) in windows.iter().enumerate() {
        if idx > 0 && page_delay > std::time::Duration::ZERO {
            tokio::time::sleep(page_delay).await;
        }
        let page = fetch_window(*ws, *we).await?;
        // 空窗不停止：早窗为空（未上市/停牌/已下架）时更晚窗口仍可能有数据，
        // 窗口数由 page_windows 有限给出，不会无限拉取。
        bars.extend(page);
    }
    // 只保留「已收盘」桶（收盘时刻 start+step 已到）：REST 端点在 endTime 覆盖当前分钟时
    // 会返回成形中 K线（不像 WS 有收盘标志），历史回补绝不能把它当定稿写入。
    let now_ms = chrono::Utc::now().timestamp_millis();
    // 区间收口（半开 [start,end)）+ 已收盘过滤 + 升序 + 按 start_ms 去重。
    bars.retain(|k| {
        k.start_ms >= start_ms
            && k.start_ms < end_ms
            && k.start_ms.saturating_add(step_ms) <= now_ms
    });
    bars.sort_by_key(|k| k.start_ms);
    bars.dedup_by(|a, b| a.start_ms == b.start_ms);
    Ok(bars)
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

    #[test]
    fn interval_duration_ms_matches_minutes() {
        // 墙钟定稿按毫秒绝对边界算：M1 = 60_000ms，且与 minutes() 自洽。
        assert_eq!(KlineInterval::M1.duration_ms(), 60_000);
        assert_eq!(
            KlineInterval::M1.duration_ms(),
            KlineInterval::M1.minutes() * 60_000
        );
    }

    // ---- page_windows（历史回补本地分页切窗纯函数） ---------------------

    #[test]
    fn page_windows_invalid_args_yield_empty() {
        assert!(page_windows(10, 10, 1, 100).is_empty()); // start==end
        assert!(page_windows(20, 10, 1, 100).is_empty()); // 反向
        assert!(page_windows(0, 10, 0, 100).is_empty()); // step<=0
        assert!(page_windows(0, 10, 1, 0).is_empty()); // page_size=0
    }

    #[test]
    fn page_windows_single_window_covers_half_open_range() {
        // 3 根分钟、page_size=10 → 单窗 [0,180_000)（exclusive 上界）。
        assert_eq!(page_windows(0, 3 * 60_000, 60_000, 10), vec![(0, 180_000)]);
    }

    #[test]
    fn page_windows_splits_into_full_then_partial_window() {
        // 5 根、page_size=2 → 窗 [0,2step)、[2step,4step)、[4step,5step)。
        let step = 60_000_i64;
        assert_eq!(
            page_windows(0, 5 * step, step, 2),
            vec![(0, 2 * step), (2 * step, 4 * step), (4 * step, 5 * step)]
        );
    }

    #[test]
    fn page_windows_windows_are_contiguous_and_within_bounds() {
        let step = 60_000_i64;
        let (start, end) = (step, 10 * step + 1); // 非整窗整除的任意上界
        let wins = page_windows(start, end, step, 3);
        // 首尾贴合请求边界，相邻窗首尾相接、不重叠。
        assert_eq!(wins.first().unwrap().0, start);
        assert_eq!(wins.last().unwrap().1, end);
        for pair in wins.windows(2) {
            assert_eq!(pair[0].1, pair[1].0);
            assert!(pair[0].0 < pair[0].1);
        }
    }

    // ---- collect_pages（分页收集内核，闭包桩零网络） --------------------

    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::time::Duration as StdDuration;

    /// 构造一根仅 start_ms 有意义的桩 K线（其余字段 collect_pages 不关心）。
    fn bar_at(start_ms: i64) -> Kline {
        Kline {
            venue: "stub".into(),
            symbol: "X".into(),
            interval: KlineInterval::M1,
            trading_day: 0,
            start_ms,
            end_ms: start_ms + KlineInterval::M1.duration_ms(),
            open: Decimal::ZERO,
            high: Decimal::ZERO,
            low: Decimal::ZERO,
            close: Decimal::ZERO,
            volume: 0,
            quote_volume: Decimal::ZERO,
            extras: std::collections::BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn collect_pages_filters_sorts_dedups_and_keeps_half_open_range() {
        let step = KlineInterval::M1.duration_ms();
        // page_size=1 → 三个窗 [0,step) [step,2step) [2step,3step)。
        // 每窗回两根：窗起点（合法）与窗上界（越界，应被全局过滤剔除）。
        let got = collect_pages(
            0,
            3 * step,
            step,
            1,
            StdDuration::ZERO,
            |ws, we| async move { Ok(vec![bar_at(ws), bar_at(we)]) },
        )
        .await
        .unwrap();
        let starts: Vec<i64> = got.iter().map(|k| k.start_ms).collect();
        // 越界的 step/2step/3step：其中 step、2step 由下一页合法根去重保留，3step 被剔除。
        assert_eq!(starts, vec![0, step, 2 * step]);
    }

    #[tokio::test]
    async fn collect_pages_continues_through_empty_windows() {
        let step = KlineInterval::M1.duration_ms();
        let calls = std::sync::Arc::new(AtomicUsize::new(0));
        let c = std::sync::Arc::clone(&calls);
        // page_size=1 → 5 个窗。第 0/2/4 窗有数据、1/3 窗空（停牌/未上市/已下架）。
        // 正确行为：不因空窗提前停止，5 个窗都请求，仍取回后续真实数据。
        let got = collect_pages(0, 5 * step, step, 1, StdDuration::ZERO, move |ws, _we| {
            c.fetch_add(1, AtomicOrdering::Relaxed);
            let bar = if ws % (2 * step) == 0 {
                vec![bar_at(ws)]
            } else {
                vec![]
            };
            async move { Ok(bar) }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(AtomicOrdering::Relaxed), 5);
        let starts: Vec<i64> = got.iter().map(|k| k.start_ms).collect();
        assert_eq!(starts, vec![0, 2 * step, 4 * step]);
    }

    #[tokio::test]
    async fn collect_pages_propagates_fetch_error() {
        let step = KlineInterval::M1.duration_ms();
        let err = collect_pages(0, step, step, 10, StdDuration::ZERO, |_ws, _we| async {
            Err(anyhow::anyhow!("upstream 500"))
        })
        .await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn collect_pages_drops_the_still_open_current_minute() {
        let step = KlineInterval::M1.duration_ms();
        let now = chrono::Utc::now().timestamp_millis();
        let current = now - now.rem_euclid(step); // 当前成形中分钟起点（未收盘）
        let closed = current - step; // 上一分钟（已收盘）
                                     // 请求区间覆盖两根；桩把两根都返回（模拟 REST 返回成形中 K线）。
        let got = collect_pages(
            closed,
            current + step,
            step,
            10,
            StdDuration::ZERO,
            move |_ws, _we| {
                let bars = vec![bar_at(closed), bar_at(current)];
                async move { Ok(bars) }
            },
        )
        .await
        .unwrap();
        let starts: Vec<i64> = got.iter().map(|k| k.start_ms).collect();
        // 只保留已收盘的上一分钟；当前分钟尚未收盘，必须剔除。
        assert_eq!(starts, vec![closed]);
    }
}
