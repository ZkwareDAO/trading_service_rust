//! K线历史缺失 REST 回补编排（P4 自动 worker + P5 管理面手动补共用内核）。
//!
//! 硬约束：回补到的 bar **只** [`BarTable::upsert`] 进内存窗口（随后续 CSV flush 落盘），
//! 绝不调用任何推送 sink/batcher/fanout，因此回补不会实时推送给在线策略。
//!
//! 设计：
//! * [`apply_range`]：对一段 `[start,end)` 调 [`HistoricalKlines`] 取数→转 [`Bar`]→
//!   只 upsert 区间内 bar。历史能力以 trait 对象入参，便于单测注入桩，零真实网络。
//! * [`run_range`]：在 [`apply_range`] 之外补「白名单 + 取交易所能力」。
//! * [`spawn_backfill_worker`]：周期 `claim_due` → 按所信号量限并发 → 成功 complete /
//!   失败 fail 退避重试。
//!
//! 这里不持有全局共享状态：内存表/缺口集合都来自 TradingStore 进程单例，符合
//! 「共享内存统一由 store 收口」约束，本模块也不使用 channel。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use trading_core::kline::{BarTable, GapRange, GapRegistry};
use trading_exchange::market::{HistoricalKlines, KlineInterval};
use trading_exchange::registry::ExchangeRegistry;

use crate::kline::{bar_from_kline, KLINE_STEP_MS};
use crate::kline_gap::gap_supported_venue;

/// 回补运行期配置（由 venue.toml `[kline.backfill]` 强类型转换而来）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackfillRuntimeConfig {
    /// 自动回补轮询周期；`ZERO`=关闭后台自动回补（手动补仍可用）。
    pub interval: Duration,
    /// 单页根数。
    pub page_size: usize,
    /// 同所分页间隔。
    pub page_delay: Duration,
    /// 每所并发上限。
    pub concurrency: usize,
}

impl BackfillRuntimeConfig {
    /// 缺省：**关闭**后台 worker（interval=0），其余取生产缺省值；避免测试/未配置误起异步任务。
    pub fn disabled() -> Self {
        Self {
            interval: Duration::ZERO,
            page_size: 1000,
            page_delay: Duration::from_millis(500),
            concurrency: 2,
        }
    }
}

impl Default for BackfillRuntimeConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

impl From<&crate::venue_config::KlineBackfillConfig> for BackfillRuntimeConfig {
    fn from(c: &crate::venue_config::KlineBackfillConfig) -> Self {
        Self {
            interval: c.interval,
            page_size: c.page_size,
            page_delay: c.page_delay,
            concurrency: c.concurrency,
        }
    }
}

/// 一次回补（自动一段 / 手动一个区间）的结果计数；P5 直接作为信封 data。
#[derive(Debug, Clone, Serialize)]
pub struct BackfillResult {
    /// 交易所标识。
    pub exchange: String,
    /// 标的。
    pub symbol: String,
    /// 请求区间起点（UTC 毫秒，含）。
    pub start_ms: i64,
    /// 请求区间终点（UTC 毫秒，不含）。
    pub end_ms: i64,
    /// 请求区间内应有（按 1m 槽位计）的根数。
    pub requested: usize,
    /// 实际 upsert 进 BarTable 的根数。
    pub upserted: usize,
    /// 回补后区间内仍缺的槽位数（requested − upserted，不为负）。
    pub missing_after: usize,
}

/// 回补前置/取数失败分类（供 P5 映射业务码）。
#[derive(Debug)]
pub enum BackfillError {
    /// 未知交易所 / CTP / xili：无 HistoricalKlines 能力。
    UnsupportedVenue { venue: String },
    /// 区间非法（start≥end）。
    BadRange(String),
    /// 交易所 REST 调用失败。
    Fetch(String),
}

impl std::fmt::Display for BackfillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackfillError::UnsupportedVenue { venue } => {
                write!(f, "交易所 `{venue}` 不支持历史 K线回补")
            }
            BackfillError::BadRange(m) => write!(f, "回补区间非法：{m}"),
            BackfillError::Fetch(m) => write!(f, "交易所历史 K线拉取失败：{m}"),
        }
    }
}

impl std::error::Error for BackfillError {}

/// P5 手动回补的错误：忙（已有一次在跑，40901）或底层回补失败。
#[derive(Debug)]
pub enum ManualError {
    /// 已有一次手动回补在执行（互斥）。
    Busy,
    /// 回补本身失败（不支持所/坏区间/取数失败）。
    Backfill(BackfillError),
}

impl std::fmt::Display for ManualError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManualError::Busy => write!(f, "已有一个手动回补任务在执行，请稍后再试"),
            ManualError::Backfill(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ManualError {}

/// 手动回补忙标志的 RAII 守卫：抢锁成功后持有，drop 时无条件复位为 false。
pub(crate) struct ManualBusyGuard {
    flag: Arc<AtomicBool>,
}

impl ManualBusyGuard {
    pub(crate) fn new(flag: Arc<AtomicBool>) -> Self {
        Self { flag }
    }
}

impl Drop for ManualBusyGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

/// 区间内 1m 槽位数（半开 `[start,end)`）；非法入参返回 0。
pub(crate) fn slots_in_range(start_ms: i64, end_ms: i64, step_ms: i64) -> usize {
    if step_ms <= 0 || end_ms <= start_ms {
        return 0;
    }
    (end_ms - start_ms).div_euclid(step_ms) as usize
}

/// 回补一段半开区间：取数→过滤→只 upsert 区间内 bar。回补**不产生任何推送**。
///
/// `history` 以 trait 对象注入（生产为交易所 REST 实现，测试为桩）。取数错误向上传播。
pub(crate) async fn apply_range(
    bars: &BarTable,
    history: &dyn HistoricalKlines,
    venue: &str,
    symbol: &str,
    start_ms: i64,
    end_ms: i64,
) -> anyhow::Result<BackfillResult> {
    let step = KlineInterval::M1.duration_ms();
    let fetched = history
        .fetch_klines(symbol, KlineInterval::M1, start_ms, end_ms)
        .await?;

    // 只写目标半开区间内的 bar，杜绝端点越界；同 start 去重（防御跨页重叠）。
    let mut last_start: Option<i64> = None;
    let mut upserted = 0usize;
    let mut fetched = fetched;
    fetched.sort_by_key(|b| b.start_ms);
    for mut bar in fetched {
        if bar.start_ms < start_ms || bar.start_ms >= end_ms {
            continue;
        }
        if last_start == Some(bar.start_ms) {
            continue;
        }
        last_start = Some(bar.start_ms);
        // 强制 venue 与请求一致（实现本就用本所名，防桩/实现串所）。
        if bar.venue != venue {
            bar.venue = venue.to_string();
        }
        bars.upsert(bar_from_kline(&bar));
        upserted += 1;
    }

    let requested = slots_in_range(start_ms, end_ms, step);
    Ok(BackfillResult {
        exchange: venue.to_string(),
        symbol: symbol.to_string(),
        start_ms,
        end_ms,
        requested,
        upserted,
        missing_after: requested.saturating_sub(upserted),
    })
}

/// 自动/手动共用：校验白名单与能力，取该所历史能力后委托 [`apply_range`]。
pub(crate) async fn run_range(
    bars: &BarTable,
    exchanges: &ExchangeRegistry,
    venue: &str,
    symbol: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<BackfillResult, BackfillError> {
    if !gap_supported_venue(venue) {
        return Err(BackfillError::UnsupportedVenue {
            venue: venue.to_string(),
        });
    }
    if end_ms <= start_ms {
        return Err(BackfillError::BadRange(format!(
            "end_ms({end_ms}) 必须大于 start_ms({start_ms})"
        )));
    }
    let Some(history) = exchanges.market_history(venue) else {
        return Err(BackfillError::UnsupportedVenue {
            venue: venue.to_string(),
        });
    };
    apply_range(bars, history.as_ref(), venue, symbol, start_ms, end_ms)
        .await
        .map_err(|e| BackfillError::Fetch(e.to_string()))
}

// ---- 退避（纯逻辑） -----------------------------------------------------

/// 连续失败退避表：10s→30s→1m→3m→5m（封顶）；成功后由调用方把计数清零。
#[derive(Debug, Clone)]
pub(crate) struct BackoffSchedule {
    delays_ms: Vec<u64>,
}

impl Default for BackoffSchedule {
    fn default() -> Self {
        Self {
            delays_ms: vec![10_000, 30_000, 60_000, 180_000, 300_000],
        }
    }
}

impl BackoffSchedule {
    /// 第 `consecutive_failures` 次失败（从 1 起）应等待的时长；超表长取封顶值；0=不等。
    fn delay_after(&self, consecutive_failures: u32) -> Duration {
        if consecutive_failures == 0 {
            return Duration::ZERO;
        }
        let idx = (consecutive_failures as usize).saturating_sub(1);
        let ms = self
            .delays_ms
            .get(idx)
            .copied()
            .or_else(|| self.delays_ms.last().copied())
            .unwrap_or(0);
        Duration::from_millis(ms)
    }
}

/// 每所失败退避状态（worker 进程内存，随任务结束丢弃）。
#[derive(Debug, Default)]
struct VenueBackoff {
    failures: u32,
    /// 早于该时刻不再处理该所。
    not_before: Option<Instant>,
}

/// 可中断睡眠：在 `dur` 内分片检查停机标志，停机立即返回 `true`。
async fn interruptible_sleep(dur: Duration, stop: &Arc<AtomicBool>) -> bool {
    let mut waited = Duration::ZERO;
    while waited < dur {
        if stop.load(Ordering::Relaxed) {
            return true;
        }
        let chunk = Duration::from_millis(100).min(dur - waited);
        tokio::time::sleep(chunk).await;
        waited += chunk;
    }
    stop.load(Ordering::Relaxed)
}

/// 启动后台自动回补 worker（仅在配置 interval>0 时由 KlineService 调用）。
///
/// 每周期：认领全部 pending 缺口 → 按所信号量限并发逐段回补 → 成功 complete、
/// 失败 fail 退回 pending 并按所退避。停机标志置位后当拍结束即退出。
pub(crate) fn spawn_backfill_worker(
    bars: Arc<BarTable>,
    gaps: Arc<GapRegistry>,
    exchanges: Arc<ExchangeRegistry>,
    cfg: BackfillRuntimeConfig,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let schedule = BackoffSchedule::default();
        let mut backoffs: HashMap<String, VenueBackoff> = HashMap::new();
        // 启动先等一个周期，避免与启动重连/订阅争抢。
        if interruptible_sleep(cfg.interval, &stop).await {
            return;
        }
        while !stop.load(Ordering::Relaxed) {
            process_one_tick(
                &bars,
                &gaps,
                &exchanges,
                cfg,
                &schedule,
                &mut backoffs,
                Arc::clone(&stop),
            )
            .await;
            if interruptible_sleep(cfg.interval, &stop).await {
                return;
            }
        }
    })
}

/// 一拍：认领→限并发回补→收口/退避。
async fn process_one_tick(
    bars: &Arc<BarTable>,
    gaps: &Arc<GapRegistry>,
    exchanges: &Arc<ExchangeRegistry>,
    cfg: BackfillRuntimeConfig,
    schedule: &BackoffSchedule,
    backoffs: &mut HashMap<String, VenueBackoff>,
    stop: Arc<AtomicBool>,
) {
    let due = gaps.claim_due(KLINE_STEP_MS);
    if due.is_empty() {
        return;
    }
    let mut semaphores: HashMap<String, Arc<tokio::sync::Semaphore>> = HashMap::new();
    // 每段一个并发任务，结论回传 (venue, all_segments_ok) 以更新退避。
    let mut tasks: tokio::task::JoinSet<(String, bool)> = tokio::task::JoinSet::new();

    for (series, ranges) in due {
        // 退避门：仍在退避窗内的所，整组 fail 退回 pending，本轮不请求。
        let in_backoff = backoffs
            .get(&series.venue)
            .and_then(|b| b.not_before)
            .is_some_and(|t| Instant::now() < t);
        if in_backoff {
            gaps.fail(&series.venue, &series.symbol, &ranges, KLINE_STEP_MS);
            continue;
        }
        let sem = semaphores
            .entry(series.venue.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(cfg.concurrency.max(1))))
            .clone();
        let history = exchanges.market_history(&series.venue);
        for range in ranges {
            // 信号量在 spawn 前获取：超出并发上限的段在此排队等待，不形成请求风暴。
            // permit 必须 move 进任务、由任务持有到结束；若在迭代末 drop 则许可立即释放、
            // 限流失效（所有段会同时外发）。
            let Ok(permit) = sem.clone().acquire_owned().await else {
                gaps.fail(&series.venue, &series.symbol, &[range], KLINE_STEP_MS);
                continue;
            };
            let bars = Arc::clone(bars);
            let gaps_arc = Arc::clone(gaps);
            let venue = series.venue.clone();
            let symbol = series.symbol.clone();
            let history = history.clone();
            let stop = Arc::clone(&stop);
            tasks.spawn(async move {
                let _permit = permit; // 持有到本任务结束（含在飞 REST 全程）才释放许可。
                let ok = match history {
                    Some(h) => {
                        let fut = apply_range(
                            &bars,
                            h.as_ref(),
                            &venue,
                            &symbol,
                            range.start,
                            range.end + KLINE_STEP_MS,
                        );
                        run_with_stop(fut, &stop).await.is_some_and(|r| r.is_ok())
                    }
                    None => false,
                };
                if ok {
                    gaps_arc.complete(&venue, &symbol, &enumerate_starts(range, KLINE_STEP_MS));
                } else {
                    gaps_arc.fail(&venue, &symbol, &[range], KLINE_STEP_MS);
                }
                (venue, ok)
            });
        }
    }

    // 按所汇总结论：任一失败即升级退避；全成功才重置。
    let mut venue_ok: HashMap<String, bool> = HashMap::new();
    while let Some(join) = tasks.join_next().await {
        if let Ok((venue, ok)) = join {
            *venue_ok.entry(venue).or_insert(true) &= ok;
        }
    }
    for (venue, all_ok) in venue_ok {
        if all_ok {
            // 全成功：清掉该所退避状态（不保留归零条目，避免退避表只增不减）。
            backoffs.remove(&venue);
        } else {
            let st = backoffs.entry(venue.clone()).or_default();
            st.failures = st.failures.saturating_add(1);
            let delay = schedule.delay_after(st.failures);
            st.not_before = Some(Instant::now() + delay);
            tracing::warn!(%venue, failures = st.failures, ?delay, "K线回补失败，退避后重试");
        }
    }
}

/// 停机时不再等待取数 future（返回 None）；正常完成回传结果。
async fn run_with_stop<F>(fut: F, stop: &Arc<AtomicBool>) -> Option<anyhow::Result<BackfillResult>>
where
    F: std::future::Future<Output = anyhow::Result<BackfillResult>>,
{
    tokio::select! {
        biased;
        _ = async {
            loop {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        } => None,
        r = fut => Some(r),
    }
}

/// 枚举一段 `[r.start, r.end]`（两端均为分钟 start）覆盖的全部分钟 start。
fn enumerate_starts(r: GapRange, step: i64) -> Vec<i64> {
    let mut out = Vec::new();
    let mut cur = r.start;
    while cur <= r.end {
        out.push(cur);
        cur = match cur.checked_add(step) {
            Some(n) => n,
            None => break,
        };
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rust_decimal::Decimal;
    use std::sync::atomic::AtomicUsize;
    use trading_exchange::market::{Kline, KlineExtra};

    const STEP: i64 = 60_000;

    /// 对齐到分钟的近期基准，避免默认保留窗按墙钟裁掉 1970 年的测试 bar。
    fn recent_base() -> i64 {
        let now = chrono::Utc::now().timestamp_millis();
        now - now.rem_euclid(STEP) - 10 * STEP
    }

    fn kline_at(venue: &str, symbol: &str, start: i64) -> Kline {
        Kline {
            venue: venue.into(),
            symbol: symbol.into(),
            interval: KlineInterval::M1,
            trading_day: 0,
            start_ms: start,
            end_ms: start + STEP,
            open: Decimal::ONE,
            high: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::ONE,
            volume: 1,
            quote_volume: Decimal::ONE,
            extras: std::collections::BTreeMap::<String, KlineExtra>::new(),
        }
    }

    /// 可配置返回 bar/错误的历史能力桩（零网络）。
    struct FakeHistory {
        bars: Vec<Kline>,
        calls: AtomicUsize,
        fail: bool,
    }
    impl FakeHistory {
        fn with(bars: Vec<Kline>) -> Self {
            Self {
                bars,
                calls: AtomicUsize::new(0),
                fail: false,
            }
        }
    }
    #[async_trait]
    impl HistoricalKlines for FakeHistory {
        async fn fetch_klines(
            &self,
            _symbol: &str,
            _interval: KlineInterval,
            _start_ms: i64,
            _end_ms: i64,
        ) -> anyhow::Result<Vec<Kline>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if self.fail {
                anyhow::bail!("upstream 500");
            }
            Ok(self.bars.clone())
        }
    }

    /// 取 BarTable 中某序列的全部 start（升序）。
    fn starts_of(bars: &BarTable, venue: &str, symbol: &str) -> Vec<i64> {
        bars.series_starts()
            .into_iter()
            .find(|s| s.key.venue == venue && s.key.symbol == symbol)
            .map(|s| s.starts)
            .unwrap_or_default()
    }

    #[test]
    fn slots_counts_half_open_minutes() {
        assert_eq!(slots_in_range(0, 3 * STEP, STEP), 3);
        assert_eq!(slots_in_range(STEP, STEP, STEP), 0); // start==end
        assert_eq!(slots_in_range(2 * STEP, STEP, STEP), 0); // 反向
        assert_eq!(slots_in_range(0, STEP, 0), 0); // 坏步长
    }

    #[test]
    fn backoff_escalates_and_caps() {
        let b = BackoffSchedule::default();
        assert_eq!(b.delay_after(0), Duration::ZERO);
        assert_eq!(b.delay_after(1), Duration::from_secs(10));
        assert_eq!(b.delay_after(2), Duration::from_secs(30));
        assert_eq!(b.delay_after(3), Duration::from_secs(60));
        assert_eq!(b.delay_after(4), Duration::from_secs(180));
        assert_eq!(b.delay_after(5), Duration::from_secs(300));
        assert_eq!(b.delay_after(99), Duration::from_secs(300)); // 封顶
    }

    #[tokio::test]
    async fn apply_range_upserts_only_in_range_and_counts() {
        let bars = BarTable::new();
        let base = recent_base();
        // 区间内 base、base+STEP；越界 base-STEP 与 end 边界 base+2STEP 须剔除。
        let hist = FakeHistory::with(vec![
            kline_at("binance", "BTCUSDT", base),
            kline_at("binance", "BTCUSDT", base + STEP),
            kline_at("binance", "BTCUSDT", base - STEP),
            kline_at("binance", "BTCUSDT", base + 2 * STEP),
        ]);
        let r = apply_range(&bars, &hist, "binance", "BTCUSDT", base, base + 2 * STEP)
            .await
            .unwrap();
        assert_eq!(r.requested, 2);
        assert_eq!(r.upserted, 2);
        assert_eq!(r.missing_after, 0);
        assert_eq!(
            starts_of(&bars, "binance", "BTCUSDT"),
            vec![base, base + STEP]
        );
    }

    #[tokio::test]
    async fn apply_range_reports_missing_when_exchange_returns_fewer() {
        let bars = BarTable::new();
        let base = recent_base();
        // 3 个槽位只回 1 根 → missing_after=2。
        let hist = FakeHistory::with(vec![kline_at("binance", "X", base + STEP)]);
        let r = apply_range(&bars, &hist, "binance", "X", base, base + 3 * STEP)
            .await
            .unwrap();
        assert_eq!(r.requested, 3);
        assert_eq!(r.upserted, 1);
        assert_eq!(r.missing_after, 2);
    }

    #[tokio::test]
    async fn apply_range_dedups_same_start() {
        let bars = BarTable::new();
        let base = recent_base();
        let hist = FakeHistory::with(vec![
            kline_at("binance", "X", base),
            kline_at("binance", "X", base),
        ]);
        let r = apply_range(&bars, &hist, "binance", "X", base, base + STEP)
            .await
            .unwrap();
        assert_eq!(r.upserted, 1);
    }

    #[tokio::test]
    async fn apply_range_propagates_fetch_error() {
        let bars = BarTable::new();
        let hist = FakeHistory {
            bars: vec![],
            calls: AtomicUsize::new(0),
            fail: true,
        };
        let base = recent_base();
        assert!(apply_range(&bars, &hist, "binance", "X", base, base + STEP)
            .await
            .is_err());
    }

    #[test]
    fn enumerate_covers_inclusive_range() {
        assert_eq!(
            enumerate_starts(GapRange::new(STEP, 3 * STEP), STEP),
            vec![STEP, 2 * STEP, 3 * STEP]
        );
    }

    // ---- process_one_tick：claim→回补→complete/fail（注入假 provider） ----

    use trading_exchange::api::ExchangeApi;
    use trading_exchange::provider::VenueProvider;

    /// 假历史能力：返回固定 bars 或按开关报错。
    struct ProvHist {
        bars: Vec<Kline>,
        fail: Arc<AtomicBool>,
    }
    #[async_trait]
    impl HistoricalKlines for ProvHist {
        async fn fetch_klines(
            &self,
            _symbol: &str,
            _interval: KlineInterval,
            _start_ms: i64,
            _end_ms: i64,
        ) -> anyhow::Result<Vec<Kline>> {
            if self.fail.load(Ordering::Relaxed) {
                anyhow::bail!("upstream 500");
            }
            Ok(self.bars.clone())
        }
    }

    /// 最小 VenueProvider：只对 "binance" 命中并暴露历史能力，其余方法走默认/bail。
    struct FakeProv {
        fail: Arc<AtomicBool>,
        bars: Vec<Kline>,
    }
    impl VenueProvider for FakeProv {
        fn matches(&self, exchange: &str) -> bool {
            exchange == "binance"
        }
        fn build_api(
            &self,
            _exchange: &str,
            _instrument_type: Option<&str>,
            _account: &str,
        ) -> anyhow::Result<Arc<dyn ExchangeApi>> {
            anyhow::bail!("fake provider 无交易 API")
        }
        fn market_history(&self) -> Option<Arc<dyn HistoricalKlines>> {
            Some(Arc::new(ProvHist {
                bars: self.bars.clone(),
                fail: Arc::clone(&self.fail),
            }))
        }
    }

    fn registry_with(fail: Arc<AtomicBool>, bars: Vec<Kline>) -> Arc<ExchangeRegistry> {
        Arc::new(ExchangeRegistry::from_providers(vec![Arc::new(FakeProv {
            fail,
            bars,
        })]))
    }

    /// 门控共享态：每次 fetch 进入即占一个「在飞」位，并在放行门 `hold` 上阻塞，
    /// 用于钉死「同所在飞请求数不超过 concurrency」。
    struct GateState {
        inflight: AtomicUsize,
        max_seen: AtomicUsize,
        entered: AtomicUsize,
        /// 初始 0 许可：fetch 进入后阻塞；测试 add_permits 一次性放行全部。
        hold: tokio::sync::Semaphore,
    }
    struct GateHist {
        st: Arc<GateState>,
    }
    #[async_trait]
    impl HistoricalKlines for GateHist {
        async fn fetch_klines(
            &self,
            _symbol: &str,
            _interval: KlineInterval,
            _start_ms: i64,
            _end_ms: i64,
        ) -> anyhow::Result<Vec<Kline>> {
            // fetch 被调用即说明已通过外层「每所并发」信号量（task 持有其 permit）。
            let n = self.st.inflight.fetch_add(1, Ordering::Relaxed) + 1;
            self.st.max_seen.fetch_max(n, Ordering::Relaxed);
            self.st.entered.fetch_add(1, Ordering::Relaxed);
            // 在放行门前挂起，持续占住外层并发许可（模拟慢请求）。
            let _gate = self.st.hold.acquire().await.unwrap();
            self.st.inflight.fetch_sub(1, Ordering::Relaxed);
            Ok(vec![])
        }
    }
    struct GateProv {
        st: Arc<GateState>,
    }
    impl VenueProvider for GateProv {
        fn matches(&self, exchange: &str) -> bool {
            exchange == "binance"
        }
        fn build_api(
            &self,
            _exchange: &str,
            _instrument_type: Option<&str>,
            _account: &str,
        ) -> anyhow::Result<Arc<dyn ExchangeApi>> {
            anyhow::bail!("fake provider 无交易 API")
        }
        fn market_history(&self) -> Option<Arc<dyn HistoricalKlines>> {
            Some(Arc::new(GateHist {
                st: Arc::clone(&self.st),
            }))
        }
    }

    #[tokio::test]
    async fn tick_claims_fetches_upserts_and_completes_gap() {
        let bars_tbl = Arc::new(BarTable::new());
        let gaps = Arc::new(GapRegistry::new());
        let base = recent_base();
        let _ = gaps.record(
            "binance",
            "BTCUSDT",
            &GapRange::new(base, base + STEP),
            STEP,
        );
        let exchanges = registry_with(
            Arc::new(AtomicBool::new(false)),
            vec![
                kline_at("binance", "BTCUSDT", base),
                kline_at("binance", "BTCUSDT", base + STEP),
            ],
        );
        let schedule = BackoffSchedule::default();
        let mut backoffs = HashMap::new();
        let stop = Arc::new(AtomicBool::new(false));

        process_one_tick(
            &bars_tbl,
            &gaps,
            &exchanges,
            BackfillRuntimeConfig::disabled(),
            &schedule,
            &mut backoffs,
            stop,
        )
        .await;

        // 两根 bar 已 upsert，且缺口被 complete（pending/inflight 皆空）。
        assert_eq!(
            starts_of(&bars_tbl, "binance", "BTCUSDT"),
            vec![base, base + STEP]
        );
        assert!(gaps.claim_due(STEP).is_empty());
        // 不推送由 apply_range 保证（无 pusher）；退避表无失败记录。
        assert!(backoffs.is_empty());
    }

    #[tokio::test]
    async fn tick_fetch_failure_returns_gap_to_pending_and_backs_off() {
        let bars_tbl = Arc::new(BarTable::new());
        let gaps = Arc::new(GapRegistry::new());
        let base = recent_base();
        let _ = gaps.record("binance", "BTCUSDT", &GapRange::new(base, base), STEP);
        let exchanges = registry_with(Arc::new(AtomicBool::new(true)), vec![]);
        let schedule = BackoffSchedule::default();
        let mut backoffs = HashMap::new();
        let stop = Arc::new(AtomicBool::new(false));

        process_one_tick(
            &bars_tbl,
            &gaps,
            &exchanges,
            BackfillRuntimeConfig::disabled(),
            &schedule,
            &mut backoffs,
            stop,
        )
        .await;

        // 失败：分钟退回 pending（可再认领），该所退避计数=1、设有 not_before。
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps.claim_due(STEP).len(), 1);
        let st = backoffs.get("binance").expect("有退避状态");
        assert_eq!(st.failures, 1);
        assert!(st.not_before.is_some());
        // 未写任何 bar。
        assert!(starts_of(&bars_tbl, "binance", "BTCUSDT").is_empty());
    }

    #[tokio::test]
    async fn tick_caps_inflight_fetch_per_venue_by_semaphore() {
        let bars_tbl = Arc::new(BarTable::new());
        let gaps = Arc::new(GapRegistry::new());
        let base = recent_base();
        // 5 个互不相邻的单分钟缺口（隔分钟各缺一根）→ claim 聚成 5 个独立段，
        // 从而产生 5 个回补任务（连续分钟会被 coalesce 成 1 段、只有 1 个任务）。
        for off in [0i64, 2, 4, 6, 8] {
            let _ = gaps.record(
                "binance",
                "BTCUSDT",
                &GapRange::new(base + off * STEP, base + off * STEP),
                STEP,
            );
        }
        let st = Arc::new(GateState {
            inflight: AtomicUsize::new(0),
            max_seen: AtomicUsize::new(0),
            entered: AtomicUsize::new(0),
            hold: tokio::sync::Semaphore::new(0),
        });
        let exchanges = Arc::new(ExchangeRegistry::from_providers(vec![Arc::new(GateProv {
            st: Arc::clone(&st),
        })]));
        let cfg = BackfillRuntimeConfig {
            concurrency: 2, // 同所最多 2 个在飞
            ..BackfillRuntimeConfig::disabled()
        };
        let schedule = BackoffSchedule::default();
        let mut backoffs = HashMap::new();
        let stop = Arc::new(AtomicBool::new(false));

        // 在后台跑一拍；前 2 段先进入 fetch 并阻塞在 hold，后 3 段应在信号量外排队。
        let tick = tokio::spawn(async move {
            process_one_tick(
                &bars_tbl,
                &gaps,
                &exchanges,
                cfg,
                &schedule,
                &mut backoffs,
                stop,
            )
            .await;
        });
        // 等待恰好 2 个 fetch 在飞（最多 ~1s，避免调度抖动假绿）。
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while st.entered.load(Ordering::Relaxed) < 2 {
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(30)).await; // 给排队段一个误入窗口
        assert_eq!(st.entered.load(Ordering::Relaxed), 2);
        assert_eq!(st.max_seen.load(Ordering::Relaxed), 2);

        // 一次性放行全部 5 段，一拍完成。
        st.hold.add_permits(5);
        tick.await.unwrap();
        assert_eq!(st.entered.load(Ordering::Relaxed), 5);
        assert_eq!(st.max_seen.load(Ordering::Relaxed), 2); // 峰值始终不超过并发上限
    }
}
