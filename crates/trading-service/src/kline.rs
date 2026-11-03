//! KlineService：K线订阅的**服务层编排**（venue-neutral）。
//!
//! 对四家交易所走完全一致的一条代码路径——本类型不聚合 tick、不认品种前缀、不选
//! 主力（全部在 CTP 适配层内完成），只负责：
//! 1. 启动时对每个活跃且支持行情流的 (venue, itype) 调一次
//!    [`MarketDataStream::subscribe_klines`]：来源＝配置基线 ∪ 客户端订阅登记表的
//!    作用域快照，sink＝**仅南向 fan-out** 的 [`FanoutBarSink`]；订阅凭证由本类型
//!    持有，与进程同寿。
//! 2. 落盘与推送是两个交易所无关的通用 sink：落盘 sink（[`Self::persistence_sink`]）
//!    由装配层无条件注册进各交易所行情引擎（零订阅标的也落 CSV），推送 sink 经
//!    subscribe_klines 按订阅路由，二者正交、互不重复 append。
//! 3. 起独立线程按固定间隔把 [`BarTable`]/CTP [`TickTable`] 经 [`KlineRepo`]
//!    批量 append 到 CSV（行情线程绝不碰文件），停机再刷一次兜底。
//!
//! 客户端运行时订阅（K2）只改 [`KlineRegistry`]，交易所侧下轮 500ms 对账自动
//! 增订/退订，本类型不重复调用 subscribe。配置基线（[`BaselineKlines`]）只贡献线
//! 订阅并集，**不进登记表**，故不产生任何南向推送。

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use trading_core::kline::{
    detect_tail_gap, BarTable, GapRegistry, KlineRegistry, RegistrySource, SeriesKey, TickTable,
};
use trading_core::protocol::{ClientEvent, KlineEvent};
use trading_exchange::market::{
    BarSink, DesiredSource, Kline as StreamKline, MarketDataStream, SubscriptionGuard,
};
use trading_exchange::registry::ExchangeRegistry;
use trading_models::{Bar, InstrumentType};
use trading_store::{KlineFlushReport, KlineRepo, TradingStore};

use crate::kline_gap::{gap_supported_venue, GapScanConfig, GapScanner};
use crate::kline_history::{merge_and_window, HistoryLimits, HistoryPage, HistoryQuery};

/// 本期唯一 K线周期标识（南向协议 / 登记表 / CSV 目录同口径）。
pub const KLINE_INTERVAL: &str = "1m";
/// 1m K线桶步长（毫秒）。
pub const KLINE_STEP_MS: i64 = 60_000;

/// 定稿 bar 未落盘（dirty）积压的告警阈值：超过即认为 CSV 落盘可能持续故障。
/// flush 失败期 dirty 豁免裁剪、只增不减，此告警**不丢数据**，只提示内存持续增长。
const BAR_BACKLOG_WARN_THRESHOLD: usize = 100_000;

/// 积压边沿判定：只在「正常→超阈」告警一次、回落到阈值内报一次恢复，避免每拍刷屏。
/// 返回 `(新告警态, 是否刚越阈, 是否刚恢复)`。纯函数便于单测。
fn backlog_edge(was_over: bool, pending: usize, threshold: usize) -> (bool, bool, bool) {
    let now_over = pending > threshold;
    (now_over, now_over && !was_over, was_over && !now_over)
}

/// 当前 UTC 墙钟（毫秒）；历史取数懒加载窗口锚点、缺口周期扫描生产时钟。
pub(crate) fn now_utc_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// 在阻塞线程池读归档 CSV（同步文件 IO 不占 tokio worker，仿 admin::refresh-specs）。
async fn read_csv_blocking(
    repo: &KlineRepo,
    venue: &str,
    symbol: &str,
    start_ms: i64,
    end_ms: i64,
) -> anyhow::Result<trading_store::kline::ReadBars> {
    let repo = repo.clone();
    let venue = venue.to_string();
    let symbol = symbol.to_string();
    let join =
        tokio::task::spawn_blocking(move || repo.read_range(&venue, &symbol, start_ms, end_ms))
            .await?;
    join.map_err(anyhow::Error::from)
}

/// 一个 K线订阅作用域：交易所 × 标的类别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KlineScope {
    /// 交易所精确名。
    pub venue: &'static str,
    /// 标的类别（与登记表 key 一致）。
    pub itype: InstrumentType,
}

impl KlineScope {
    pub const fn new(venue: &'static str, itype: InstrumentType) -> Self {
        Self { venue, itype }
    }

    /// `registry.market_stream(exchange, Some(wire))` 的类别串（与
    /// `normalize_instrument_type` 口径一致）。
    pub fn wire(self) -> &'static str {
        match self.itype {
            InstrumentType::Option => "option",
            _ => "futures",
        }
    }
}

/// 支持统一行情流 K线订阅的所及其标的类别（声明式矩阵；xili 不在列）。
/// binance 本期仅合约（现货 K线未接入），deribit 为期权，hyperliquid/ctp 为合约。
const KLINE_SCOPES: &[KlineScope] = &[
    KlineScope::new("binance", InstrumentType::Futures),
    KlineScope::new("deribit", InstrumentType::Option),
    KlineScope::new("hyperliquid", InstrumentType::Futures),
    KlineScope::new("ctp", InstrumentType::Futures),
];

/// 从活跃交易所名过滤出需要启动 K线订阅的作用域（有序、去重，xili 天然排除）。
pub fn kline_scopes(active: &[String]) -> Vec<KlineScope> {
    let active_set: HashSet<&str> = active.iter().map(String::as_str).collect();
    KLINE_SCOPES
        .iter()
        .copied()
        .filter(|s| active_set.contains(s.venue))
        .collect()
}

/// 一路配置基线 K线订阅：交易所 × 标的类别 × 启动即订阅的具体合约。
///
/// 来源于 venue.toml（如 `[binance].kline_symbols`）：服务启动即向交易所订阅这些
/// 合约并随定稿全量落盘，**不依赖任何南向客户端在线**；它不写入 [`KlineRegistry`]，
/// 故不产生南向 fan-out（客户端要收 bar 仍需自行 subscribe_kline）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaselineKlines {
    /// 交易所精确名。
    pub venue: String,
    /// 标的类别（与登记表 key 同口径）。
    pub itype: InstrumentType,
    /// 基线合约（构造时 trim/去空/去重；保持原大小写，频道映射各自小写化）。
    pub symbols: Vec<String>,
}

impl BaselineKlines {
    /// 构造基线：trim 去空白项后去重、字典序（保持原大小写）。
    pub fn new(venue: impl Into<String>, itype: InstrumentType, symbols: Vec<String>) -> Self {
        let mut seen = BTreeSet::new();
        for raw in symbols {
            let s = raw.trim();
            if !s.is_empty() {
                seen.insert(s.to_string());
            }
        }
        Self {
            venue: venue.into(),
            itype,
            symbols: seen.into_iter().collect(),
        }
    }
}

/// 一次 flush 的两类落盘计数。
#[derive(Debug, Clone, Copy, Default)]
pub struct FlushOutcome {
    pub bars: KlineFlushReport,
    pub ticks: KlineFlushReport,
}

/// venue → 登记表/交易所使用的标的类别（与各 provider build_market_stream 口径一致）。
fn venue_itype(venue: &str) -> InstrumentType {
    if venue.eq_ignore_ascii_case("deribit") {
        InstrumentType::Option
    } else {
        // binance(合约)/hyperliquid/ctp 本期统一期货。
        InstrumentType::Futures
    }
}

/// 把交易所层定稿 [`StreamKline`] 映射为落盘行 [`Bar`]（字段一一搬运，不重算）。
///
/// 公共字段直搬；各所专有指标（CTP exchange_id/open_interest、币安 trade_num/…）
/// 整包透传 [`Bar::extras`]，本映射 venue-neutral、不为具体所写特例。
/// `pub(crate)`：实时落盘 sink 与历史回补（[`crate::kline_backfill`]）共用同一映射。
pub(crate) fn bar_from_kline(k: &StreamKline) -> Bar {
    Bar {
        exchange: k.venue.clone(),
        symbol: k.symbol.clone(),
        trading_day: k.trading_day,
        open: k.open,
        high: k.high,
        low: k.low,
        close: k.close,
        volume: k.volume,
        quote_volume: k.quote_volume,
        start_time: k.start_ms,
        end_time: k.end_ms,
        extras: k.extras.clone(),
    }
}

/// 把定稿 bar 映射为南向 `kline_1m` 载荷：公共字段直搬，extras flatten 平铺。
fn kline_event(k: &StreamKline) -> KlineEvent {
    KlineEvent {
        venue: k.venue.clone(),
        symbol: k.symbol.clone(),
        interval: k.interval.as_str().to_string(),
        trading_day: k.trading_day,
        start_time: k.start_ms,
        end_time: k.end_ms,
        open: k.open,
        high: k.high,
        low: k.low,
        close: k.close,
        volume: k.volume,
        quote_volume: k.quote_volume,
        extras: k.extras.clone(),
    }
}

/// K线南向推送端口（依赖倒置：核心只依赖 trait，生产实现 spawn 到既有 hub 队列）。
///
/// 实现必须**立即返回、绝不阻塞行情线程**：生产实现只把帧放入该连接既有异步发送
/// 队列（满则丢并计数）。核心在同步 BarSink 内调用，故方法本身不 async。
pub trait KlinePusher: Send + Sync {
    /// 向一个策略连接投递一帧；返回 false=该连接当前不可达（调用方记丢弃）。
    fn push(&self, strategy: &str, event: ClientEvent) -> bool;
}

/// 每个南向连接允许积压的 kline 发送任务上限（在途 in-flight 帧）。超过即丢最新帧，
/// 防止单条慢/断连接让 tokio 任务无限堆积；bar 为分钟级，正常在途≈1。
const PUSH_IN_FLIGHT_PER_STRATEGY: usize = 16;

/// 推送线程轮询周期：每拍关闭超龄分钟桶、并取走「已关帧」发送。出帧时机由分钟桶的
/// 有序水位线 / 安全超时决定（与本周期无关），本周期只决定发送与超时检测的反应粒度。
const KLINE_PUSH_TICK_INTERVAL: Duration = Duration::from_millis(200);

/// 分钟桶安全超时（自桶内首根 bar 到达起，ms）：同 (strategy, venue) 迟迟没有更晚分钟
/// bar 驱动水位线时，桶最迟在此年龄关桶出帧。取 2s 优先低延迟（健康连接同所多标的收盘帧
/// 散布 ~880ms，仍能合帧）；代价：WS 重连抖动 >2s 的迟到标的可能落到下一帧/拆帧，
/// 但远小于 1 分钟（不会跨到下一分钟桶）。
const KLINE_PUSH_MAX_AGE_MS: i64 = 2_000;

/// per-connection 在途发送闸门（背压）：同步 `try_acquire` 取许可，拿到的
/// [`OwnedSemaphorePermit`] 在异步发送任务结束时自动归还；该连接在途达上限则返 None。
/// 各连接独立计数，一条慢连接不牵连其它连接。
pub(crate) struct InFlightGate {
    cap: usize,
    per: dashmap::DashMap<String, Arc<tokio::sync::Semaphore>>,
}

impl InFlightGate {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            per: dashmap::DashMap::new(),
        }
    }

    /// 尝试为某连接取一个在途许可；在途已满返回 None（调用方丢帧并计数）。
    fn try_acquire(&self, strategy: &str) -> Option<tokio::sync::OwnedSemaphorePermit> {
        let sem = self
            .per
            .entry(strategy.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(self.cap)))
            .clone();
        // try_acquire 立即返回，绝不阻塞行情 drain 线程。
        sem.try_acquire_owned().ok()
    }
}

/// 生产推送实现：把帧序列化后 spawn 到既有 [`crate::hub::StrategyHub`] 的 per-connection
/// 发送任务——同步 on_bar 先过 per-connection 在途闸门（满即返 false 丢弃），再
/// try_spawn（无 runtime 失败），绝不 await。
pub struct HubKlinePusher {
    hub: crate::hub::StrategyHub,
    handle: tokio::runtime::Handle,
    gate: Arc<InFlightGate>,
}

impl HubKlinePusher {
    pub fn new(hub: crate::hub::StrategyHub, handle: tokio::runtime::Handle) -> Self {
        Self {
            hub,
            handle,
            gate: Arc::new(InFlightGate::new(PUSH_IN_FLIGHT_PER_STRATEGY)),
        }
    }
}

impl KlinePusher for HubKlinePusher {
    fn push(&self, strategy: &str, event: ClientEvent) -> bool {
        let Ok(json) = serde_json::to_value(&event) else {
            return false;
        };
        // 背压：该连接在途帧已达上限则直接丢弃（on_bar 据此 dropped++），不堆积任务。
        let permit = match self.gate.try_acquire(strategy) {
            Some(p) => p,
            None => return false,
        };
        let hub = self.hub.clone();
        let name = strategy.to_string();
        // runtime 存活时 spawn 必成功；permit move 进任务，发送结束自动归还。
        self.handle.spawn(async move {
            // 持有 permit 直到发送完成：慢连接期间在途计数真实反映未完成帧。
            let _permit = permit;
            if !hub.send(&name, &json).await {
                tracing::warn!(strategy = %name, "kline_1m_batch 推送失败（连接不可达），丢弃该帧");
            }
        });
        true
    }
}

/// 一个待推送的**分钟桶**：同一 (strategy, venue, 分钟起始 ms) 内定稿的多标的 bar，
/// 在桶关闭前持续累积，关闭时聚成一个数组帧。
struct MinuteBucket {
    /// 桶内多标的 bar（保持入序，出帧时按 symbol 排序）。
    events: Vec<KlineEvent>,
    /// 桶内**首根** bar 到达（墙钟 ms）时刻：安全超时以此为龄起点。
    first_arrival_ms: i64,
}

/// 推送聚合内核（纯内存、时钟由参数注入，不依赖 pusher/线程，便于单测）。
///
/// 解决的问题：同一分钟多标的的定稿事件并非同一刻到齐——实测 binance 测试网
/// BTC/ETH 相差约 880ms（健康连接）、WS 重连突发时可达数秒。固定短窗（200ms）会
/// 把它们拆成多条帧。改为按 **(strategy, venue, 分钟起始)** 分桶，满足以下任一
/// 条件即关桶出帧（同一分钟的多标的在关桶前都会被收进同一帧）：
/// 1. **有序水位线**：同 (strategy, venue) 到达了一根*更晚分钟*的 bar，说明更旧的
///    分钟桶已不可能再来同分钟 bar，立即关闭；
/// 2. **安全超时**：桶龄（自首根 bar 到达）达到 [`Self::max_age_ms`]，兜底无后续
///    分钟 bar 驱动水位线、或定稿长时间散布的情形（覆盖重连数秒抖动）。
///
/// 关桶后的帧进入 [`Self::outbox`]，由后台推送线程下一拍统一取走发送；行情 sink
/// 线程只做入桶（同步纯内存），**绝不直接发送**。
struct BatcherCore {
    /// 在途分钟桶：key=(strategy, venue, bar.start_time)。
    buckets: BTreeMap<(String, String, i64), MinuteBucket>,
    /// 已关桶、待推送线程取走的成帧组 (strategy, venue, bars)。
    outbox: Vec<(String, String, Vec<KlineEvent>)>,
    /// 桶安全超时（自首根 bar 到达起，ms）。
    max_age_ms: i64,
}

impl Default for BatcherCore {
    fn default() -> Self {
        Self::new(KLINE_PUSH_MAX_AGE_MS)
    }
}

impl BatcherCore {
    fn new(max_age_ms: i64) -> Self {
        Self {
            buckets: BTreeMap::new(),
            outbox: Vec::new(),
            max_age_ms,
        }
    }

    /// 入桶一根定稿 bar（同步、廉价）。先按有序水位线关闭同 (strategy, venue) 的更旧
    /// 分钟桶，再把本 bar 追加到其所属分钟桶（桶不存在则以 `now_ms` 为龄起点新建）。
    fn push(&mut self, now_ms: i64, strategy: &str, venue: &str, event: KlineEvent) {
        let bucket_start = event.start_time;
        // 水位线：关闭键序严格更旧（且同 strategy+venue）的桶。BTreeMap 范围查询后
        // 显式比对前缀，避免把别的 strategy/venue 的桶误关。
        let upper = (strategy.to_string(), venue.to_string(), bucket_start);
        let stale: Vec<(String, String, i64)> = self
            .buckets
            .range(..&upper)
            .filter(|((s, v, _), _)| s == strategy && v == venue)
            .map(|(k, _)| k.clone())
            .collect();
        for key in stale {
            if let Some(bucket) = self.buckets.remove(&key) {
                self.emit(key.0, key.1, bucket.events);
            }
        }
        // 追加到当前分钟桶（晚到的同分钟标的仍并入同一桶；仅当它晚于水位线/超时关桶
        // 之后才会另起一帧，属罕见长尾，语义不漏不重）。先取/建桶再 push：两个分支都
        // 按值消费 event，不引入 clone（KlineEvent 含多个 String）。
        let bucket = self
            .buckets
            .entry((strategy.to_string(), venue.to_string(), bucket_start))
            .or_insert_with(|| MinuteBucket {
                first_arrival_ms: now_ms,
                events: Vec::new(),
            });
        bucket.events.push(event);
    }

    /// 推送线程每拍调用：把达到安全超时的桶关入 outbox，并取走全部待发帧（取空）。
    fn poll(&mut self, now_ms: i64) -> Vec<(String, String, Vec<KlineEvent>)> {
        // BTreeMap 键序＝(strategy, venue, 分钟) 字典序，出帧顺序确定。
        let matured: Vec<(String, String, i64)> = self
            .buckets
            .iter()
            .filter(|(_, b)| now_ms.saturating_sub(b.first_arrival_ms) >= self.max_age_ms)
            .map(|(k, _)| k.clone())
            .collect();
        for key in matured {
            if let Some(bucket) = self.buckets.remove(&key) {
                self.emit(key.0, key.1, bucket.events);
            }
        }
        self.take_outbox()
    }

    /// 停机兜底：关闭全部在途桶（含未超时的）并取走，尽力推出、不丢缓冲。
    fn flush_all(&mut self) -> Vec<(String, String, Vec<KlineEvent>)> {
        let all: Vec<(String, String, i64)> = self.buckets.keys().cloned().collect();
        for key in all {
            if let Some(bucket) = self.buckets.remove(&key) {
                self.emit(key.0, key.1, bucket.events);
            }
        }
        self.take_outbox()
    }

    /// 关桶成帧：组内按 `(symbol, start_time)` 排序，帧内容确定、不依赖入序。
    fn emit(&mut self, strategy: String, venue: String, mut events: Vec<KlineEvent>) {
        events.sort_by(|a, b| {
            a.symbol
                .cmp(&b.symbol)
                .then_with(|| a.start_time.cmp(&b.start_time))
        });
        self.outbox.push((strategy, venue, events));
    }

    fn take_outbox(&mut self) -> Vec<(String, String, Vec<KlineEvent>)> {
        std::mem::take(&mut self.outbox)
    }
}

/// 当前墙钟 UTC 毫秒（生产时钟；测试直接调 [`BatcherCore`] 注入虚拟时钟）。
fn wall_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// [`BatcherCore`] 的共享句柄：行情 sink（多所 drain 线程）`push` 入桶、后台推送线程
/// `poll`/`flush_all` 取帧，`Arc<Mutex<_>>` 共享同一内核（项目约定不用 channel）。
/// 锁内只做 BTreeMap/Vec 的短操作，临界区极短。时钟默认取墙钟。
#[derive(Clone)]
pub(crate) struct KlineBatcher {
    inner: Arc<Mutex<BatcherCore>>,
}

impl Default for KlineBatcher {
    fn default() -> Self {
        Self::new(BatcherCore::default())
    }
}

impl KlineBatcher {
    fn new(core: BatcherCore) -> Self {
        Self {
            inner: Arc::new(Mutex::new(core)),
        }
    }

    /// 入桶一根定稿 bar（读取墙钟作为到达时刻）。
    fn push(&self, strategy: &str, venue: &str, event: KlineEvent) {
        self.inner
            .lock()
            .unwrap()
            .push(wall_now_ms(), strategy, venue, event);
    }

    /// 每拍：关超龄桶并取走待发帧。
    fn poll(&self) -> Vec<(String, String, Vec<KlineEvent>)> {
        self.inner.lock().unwrap().poll(wall_now_ms())
    }

    /// 停机：关闭全部在途桶并取走。
    fn flush_all(&self) -> Vec<(String, String, Vec<KlineEvent>)> {
        self.inner.lock().unwrap().flush_all()
    }
}

/// 仅负责**持久化**的 sink：把定稿 bar append 进待刷盘 BarTable（每根只由此处写一次）。
/// 纯内存、同步廉价。WS 路径与 CTP 无条件全量持久化共用。
///
/// **注册策略**：此 sink 以 LocalPersist 注册——纯内存、无 IO、不可失败，调用方
/// 不测耗时（避免偶发调度抖动导致落盘通路被误摘）；后续若引入 IO 必须改回 bounded。
///
/// 顺带承担缺口**实时尾部检测**（检测点 1，仅三家）：`upsert` 返回写入前旧末键，与本来棒
/// start 比对，跳分钟即把缺口登记进 [`GapRegistry`]。无 IO/await、分钟级触发，满足 on_bar
/// 同步契约；CTP 在白名单外，只落盘不检测。
struct PersistenceBarSink {
    bars: Arc<BarTable>,
    /// 缺口去重登记集合（store 收口；与扫描线程共享同一进程单例）。
    gaps: Arc<GapRegistry>,
}

impl BarSink for PersistenceBarSink {
    fn on_bar(&self, bar: &StreamKline) {
        tracing::debug!(
            venue = %bar.venue,
            symbol = %bar.symbol,
            trading_day = bar.trading_day,
            start_ms = bar.start_ms,
            close = %bar.close,
            volume = bar.volume,
            "定稿 bar 入待刷盘窗口（无条件全量落盘）"
        );
        // upsert 在同一把序列锁内返回写入前旧末键，尾检无竞态；四家 bar 一律落盘。
        let prev_start = self.bars.upsert(bar_from_kline(bar));
        if gap_supported_venue(&bar.venue) {
            if let Some(gap) = detect_tail_gap(prev_start, bar.start_ms, KLINE_STEP_MS) {
                let added = self
                    .gaps
                    .record(&bar.venue, &bar.symbol, &gap, KLINE_STEP_MS);
                if added > 0 {
                    tracing::warn!(
                        venue = %bar.venue,
                        symbol = %bar.symbol,
                        gap_start = gap.start,
                        gap_end = gap.end,
                        missing = added,
                        "定稿 bar 尾检发现缺口（已登记待回补集合）"
                    );
                }
            }
        }
    }
}

/// 仅负责**南向 fan-out** 的 sink：查订阅登记表，把 bar 追加进分钟桶缓冲，不写表、
/// 不直接发送。同所同分钟的多标的在关桶（水位线/安全超时）前聚成一个数组帧，由后台
/// 推送线程统一发出；CTP 全量持久化路径也用它，落盘由无条件 [`PersistenceBarSink`]
/// 单独负责，避免重复 append。
struct FanoutBarSink {
    registry: Arc<KlineRegistry>,
    /// 命中策略的 bar 入此聚合缓冲（service 与推送线程共享同一句柄）。
    batcher: KlineBatcher,
}

impl FanoutBarSink {
    fn fan_out(&self, bar: &StreamKline) {
        // 查订阅者（精确/品种前缀），逐连接把 bar 入批量缓冲；发送（满/断即丢）延后到
        // 后台推送线程，保证 on_bar 同步纯内存、绝不阻塞行情 drain（safe_notify <1ms 契约）。
        let itype = venue_itype(&bar.venue);
        let targets =
            self.registry
                .strategies_for_bar(&bar.venue, itype, bar.interval.as_str(), &bar.symbol);
        tracing::debug!(
            venue = %bar.venue,
            symbol = %bar.symbol,
            trading_day = bar.trading_day,
            volume = bar.volume,
            recipients = targets.len(),
            "定稿 bar fan-out（入批量推送缓冲）"
        );
        for strategy in targets {
            self.batcher.push(&strategy, &bar.venue, kline_event(bar));
        }
    }
}

impl BarSink for FanoutBarSink {
    fn on_bar(&self, bar: &StreamKline) {
        self.fan_out(bar);
    }
}

/// 把 core [`RegistrySource`] 薄适配为交易所层 [`DesiredSource`]（避免 core 依赖 exchange），
/// 并并入该作用域的配置基线符号（登记表 ∪ 基线，去重字典序）。
///
/// `pub(crate)`：服务内测试需经 [`DesiredSource`] 读其快照；字段保持私有。
pub(crate) struct BaselineDesired {
    source: RegistrySource,
    /// 本作用域配置基线（无基线则为空 Vec）。
    baseline: Arc<Vec<String>>,
}

impl DesiredSource for BaselineDesired {
    fn symbols(&self) -> Vec<String> {
        if self.baseline.is_empty() {
            return self.source.symbols();
        }
        let mut all: BTreeSet<String> = self.source.symbols().into_iter().collect();
        all.extend(self.baseline.iter().cloned());
        all.into_iter().collect()
    }
}

/// K线服务编排对象（进程一份；所有句柄取自/收口于 TradingStore）。
pub struct KlineService {
    /// 客户端订阅登记表（南向命令写入；sink 读路由、来源读快照）。
    registry: Arc<KlineRegistry>,
    /// 定稿 bar 待刷盘窗口。
    bars: Arc<BarTable>,
    /// CTP 原始 tick WAL（仅 CTP 适配层写；flush 线程读）。
    ticks: Arc<TickTable>,
    /// append-only CSV 仓储（进程单例）。
    repo: KlineRepo,
    /// 南向推送端口（生产＝spawn 到 hub；测试可注入假实现）。
    pusher: Arc<dyn KlinePusher>,
    /// 定稿 bar 批量推送缓冲（sink 多线程 push、推送线程 drain；同所多标的聚一帧）。
    batcher: KlineBatcher,
    /// 配置基线订阅（venue+itype → 基线合约；只贡献线订阅并集，不做南向推送）。
    baselines: Arc<Vec<BaselineKlines>>,
    /// 南向 get_klines 历史取数的根数限制（缺省/硬上限）。
    history_limits: crate::kline_history::HistoryLimits,
    /// K线缺口去重登记集合（store 收口；尾检与扫描线程共享，仅三家）。
    gaps: Arc<GapRegistry>,
    /// 缺口周期扫描器（检测点 2 内部扫描 + 检测点 3 静默停推）。
    gap_scanner: GapScanner,
    /// 缺口检测参数（步长/静默阈值/扫描周期；scan_interval=0 表示不起扫描线程）。
    gap_cfg: GapScanConfig,
    /// 历史缺失自动回补配置（interval=0 表示不起后台 worker；手动补仍可用）。
    backfill_cfg: crate::kline_backfill::BackfillRuntimeConfig,
    /// P5 手动回补互斥：true=已有一次手动补区间在跑，拒绝并发（40901）。
    /// 自动 worker 不经此锁（它用 GapRegistry 的 per-series inflight 去重）。
    manual_busy: Arc<AtomicBool>,
    /// 慢/不可达连接丢弃的 bar 帧总数（观测）。
    dropped: Arc<AtomicU64>,
    /// bar dirty 积压是否正处于超阈告警态（边沿去抖：仅越阈/恢复各报一次）。
    bar_backlog_alarmed: AtomicBool,
    /// 每个作用域一个 subscribe_klines 凭证（与进程同寿；drop 即整体退订）。
    guards: Mutex<Vec<SubscriptionGuard>>,
    /// 后台 CSV 刷盘线程句柄（start 后才有）；优雅停机 join 以保证最后一刷落盘。
    flush_task: Mutex<Option<thread::JoinHandle<()>>>,
    /// 后台批量推送线程句柄（start 后才有）；优雅停机 join 以保证最后一拍缓冲推出。
    push_task: Mutex<Option<thread::JoinHandle<()>>>,
    /// 后台缺口扫描线程句柄（start 后、且 scan_interval>0 才有）；优雅停机 join 收口。
    gap_task: Mutex<Option<thread::JoinHandle<()>>>,
}

impl KlineService {
    /// 从 TradingStore 取齐五表/repo 句柄构造，注入南向推送端口、配置基线与历史取数限额
    /// （内部全是进程单例）。基线只驱动线订阅/落盘，不写入客户端订阅登记表。
    pub fn new(
        store: Arc<TradingStore>,
        pusher: Arc<dyn KlinePusher>,
        baselines: Vec<BaselineKlines>,
        history_limits: HistoryLimits,
        gap_cfg: GapScanConfig,
        backfill_cfg: crate::kline_backfill::BackfillRuntimeConfig,
    ) -> Self {
        let bars = store.bar_table();
        let gaps = store.gap_registry();
        let gap_scanner = GapScanner::new(Arc::clone(&bars), Arc::clone(&gaps), gap_cfg);
        Self {
            registry: store.kline_registry(),
            bars,
            ticks: store.ctp_ticks(),
            repo: store.kline_repo(),
            pusher,
            batcher: KlineBatcher::default(),
            baselines: Arc::new(baselines),
            history_limits,
            gaps,
            gap_scanner,
            gap_cfg,
            backfill_cfg,
            dropped: Arc::new(AtomicU64::new(0)),
            bar_backlog_alarmed: AtomicBool::new(false),
            // P5 手动回补互斥：true 表示已有一次手动补区间在跑，拒绝并发（40901）。
            manual_busy: Arc::new(AtomicBool::new(false)),
            guards: Mutex::new(Vec::new()),
            flush_task: Mutex::new(None),
            push_task: Mutex::new(None),
            gap_task: Mutex::new(None),
        }
    }

    /// 优雅停机：等后台刷盘线程与批量推送线程退出（刷盘线程退出前已做最后一次
    /// flush_once；推送线程退出前已做最后一拍 drain）。
    ///
    /// 必须在置位停机标志**之后**调用；调用方据此获得「bar 已从内存窗口刷入 CSV、
    /// 缓冲内最后一批 bar 已尽力推送」的 happens-before 保证。未 start 或重复调用均
    /// 安全（句柄只 take/join 一次）；返回 Err 仅表示某线程曾 panic（已记录）。
    pub fn join_flush(&self) -> thread::Result<()> {
        // 逐个 take 释放锁再 join，不持锁等待线程。
        // 先 join 推送线程：保证停机前入缓冲的最后一批 bar 已发出；再收口缺口扫描线程，
        // 最后 join 刷盘线程（缺口/实时 bar 都已登记，停机补扫/补落盘后进程才退出）。
        join_take(&self.push_task)?;
        join_take(&self.gap_task)?;
        join_take(&self.flush_task)
    }

    /// 某 venue 的标的类别（deribit=option，其余本期=futures；未知默认 futures）。
    pub fn itype_for(&self, venue: &str) -> InstrumentType {
        venue_itype(venue)
    }

    /// 某作用域的期望来源视图（配置基线 ∪ 登记表快照；attach 与测试共用）。
    pub fn source_for(&self, venue: &str, itype: InstrumentType) -> BaselineDesired {
        BaselineDesired {
            source: RegistrySource::new(
                Arc::clone(&self.registry),
                venue.to_string(),
                itype,
                KLINE_INTERVAL.to_string(),
            ),
            baseline: Arc::new(self.baseline_symbols(venue, itype)),
        }
    }

    /// 取某作用域的配置基线合约克隆（无基线为空 Vec）。
    fn baseline_symbols(&self, venue: &str, itype: InstrumentType) -> Vec<String> {
        self.baselines
            .iter()
            .find(|b| b.venue == venue && b.itype == itype)
            .map(|b| b.symbols.clone())
            .unwrap_or_default()
    }

    /// **仅推送** sink（四家统一）：按订阅 fan-out，绝不写 BarTable。
    /// 全量落盘由 [`Self::persistence_sink`] 作为无条件 sink 单独负责，
    /// 两个 sink 正交，同一根 bar 不会重复 append。
    pub fn push_sink(&self) -> Arc<dyn BarSink> {
        Arc::new(self.fanout_sink_inner())
    }

    /// **仅落盘** sink：每根定稿 bar append 进 BarTable 一次，不做南向推送。
    /// 由装配层作为「绕过订阅/主力的无条件全量持久化」注入各交易所适配层。
    pub fn persistence_sink(&self) -> Arc<dyn BarSink> {
        Arc::new(self.persistence_sink_inner())
    }

    fn persistence_sink_inner(&self) -> PersistenceBarSink {
        PersistenceBarSink {
            bars: Arc::clone(&self.bars),
            gaps: Arc::clone(&self.gaps),
        }
    }

    fn fanout_sink_inner(&self) -> FanoutBarSink {
        FanoutBarSink {
            registry: Arc::clone(&self.registry),
            batcher: self.batcher.clone(),
        }
    }

    /// 累计因慢/不可达连接丢弃的 bar 帧数（测试断言；生产由 flush 周期日志消费）。
    #[cfg(test)]
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// 对一路行情流建立 K线订阅并留存凭证；流不支持（Unsupported/报错）时返回 Err
    /// 且不留凭证，调用方跳过该作用域，不影响其余所。
    pub fn attach(
        &self,
        scope: &KlineScope,
        stream: Arc<dyn MarketDataStream>,
    ) -> anyhow::Result<()> {
        // 期望来源＝配置基线 ∪ 登记表快照（随 500ms 对账动态增退订）。
        let desired = Arc::new(self.source_for(scope.venue, scope.itype)) as Arc<dyn DesiredSource>;
        // 四家统一：订阅回调只做南向 fan-out；全量落盘是另一个由装配层无条件注册的
        // sink（引擎/聚合器对每根定稿 bar 都调用，零订阅也落），二者不重复 append。
        tracing::debug!(
            venue = scope.venue,
            itype = scope.wire(),
            symbols = ?desired.symbols(),
            "K线作用域 attach（期望集＝配置基线 ∪ 客户端登记表）"
        );
        let sink = self.push_sink();
        let guard = stream.subscribe_klines(desired, sink)?;
        self.guards.lock().unwrap().push(guard);
        Ok(())
    }

    /// 南向订阅登记：把该策略对若干标的的 1m K线订阅加入登记表（幂等）。
    pub fn subscribe(&self, strategy: &str, venue: &str, symbols: &[String]) {
        let itype = self.itype_for(venue);
        for symbol in symbols {
            let symbol = symbol.trim();
            if symbol.is_empty() {
                continue;
            }
            self.registry.subscribe(
                strategy,
                trading_core::kline::KlineKey::new(venue, itype, symbol, KLINE_INTERVAL),
            );
        }
    }

    /// 南向退订：移除该策略的对应订阅（归零自动退订交易所合约）。
    pub fn unsubscribe(&self, strategy: &str, venue: &str, symbols: &[String]) {
        let itype = self.itype_for(venue);
        for symbol in symbols {
            let symbol = symbol.trim();
            if symbol.is_empty() {
                continue;
            }
            self.registry.unsubscribe(
                strategy,
                &trading_core::kline::KlineKey::new(venue, itype, symbol, KLINE_INTERVAL),
            );
        }
    }

    /// 策略断连：一次摘除其全部 K线订阅（与他人共享的 key 仅减员）。
    pub fn disconnect(&self, strategy: &str) {
        self.registry.disconnect(strategy);
    }

    /// 当前留存的订阅凭证数（仅测试断言用）。
    #[cfg(test)]
    pub fn guard_count(&self) -> usize {
        self.guards.lock().unwrap().len()
    }

    /// 把已关桶成帧组逐帧发送：每个 (strategy, venue, 分钟) 只调一次
    /// [`KlinePusher::push`]（同所同分钟多标的＝一条消息）；pusher 返 false（连接不可达/
    /// 在途满）则该帧丢弃并计入 dropped，不阻塞、不重试。
    fn send_frames(&self, frames: Vec<(String, String, Vec<KlineEvent>)>) {
        for (strategy, venue, events) in frames {
            if events.is_empty() {
                continue;
            }
            let frame = ClientEvent::kline_1m_batch(venue, events);
            if !self.pusher.push(&strategy, frame) {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// 推送线程每拍：关闭超龄分钟桶、取走水位线已关帧并发送。
    fn poll_push(&self) {
        let frames = self.batcher.poll();
        self.send_frames(frames);
    }

    /// 停机兜底：关闭全部在途分钟桶并尽力发送（断连/在途满的帧计入 dropped，不阻塞停机）。
    fn flush_remaining_push(&self) {
        let frames = self.batcher.flush_all();
        self.send_frames(frames);
    }

    /// 检查 bar dirty 积压并做边沿告警：越阈打一次 error，回落打一次恢复 info。
    /// 由刷盘线程每拍调用；数据不丢，仅可观测。
    fn check_bar_backlog(&self) {
        self.check_bar_backlog_at(BAR_BACKLOG_WARN_THRESHOLD);
    }

    /// 阈值可注入版本（测试用小阈值验证边沿状态翻转；日志副作用同生产）。
    fn check_bar_backlog_at(&self, threshold: usize) {
        let pending = self.bars.pending_dirty();
        let was = self.bar_backlog_alarmed.load(Ordering::Relaxed);
        let (now, raised, cleared) = backlog_edge(was, pending, threshold);
        self.bar_backlog_alarmed.store(now, Ordering::Relaxed);
        if raised {
            tracing::error!(
                pending,
                threshold,
                "定稿 bar 未落盘积压超阈：CSV 落盘可能持续故障，内存将持续增长（dirty 不丢、不裁剪），请检查磁盘/权限"
            );
        } else if cleared {
            tracing::info!(pending, "定稿 bar 未落盘积压已回落至阈值内（落盘恢复）");
        }
    }

    /// 立即各刷一批 bar/tick 到 CSV（同步；测试与停机兜底用）。
    /// IO 失败时 KlineRepo 已把整批放回窗口队首下轮重试（不丢数据，可能重复），
    /// 这里仅记日志、该类计数按 0 处理，绝不向 flush 线程传播 panic。
    pub fn flush_once(&self) -> FlushOutcome {
        let bars = match self.repo.flush_bars(&self.bars) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, "K线 bar 批量落盘失败，整批已留待重试");
                KlineFlushReport::default()
            }
        };
        let ticks = match self.repo.flush_ticks(&self.ticks) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, "K线 tick 批量落盘失败，整批已留待重试");
                KlineFlushReport::default()
            }
        };
        FlushOutcome { bars, ticks }
    }

    /// 南向 `get_klines`：本地两级读（内存保留窗热读 + 归档 CSV 冷读），重启冷启动先
    /// 懒加载 [`BarTable::restore`]。全程只读本地、不调交易所（三家现场回补是后续 P4）。
    pub async fn history(
        &self,
        venue: &str,
        symbol: &str,
        start: Option<i64>,
        end: Option<i64>,
        limit: Option<usize>,
    ) -> anyhow::Result<HistoryPage> {
        self.history_at(venue, symbol, start, end, limit, now_utc_ms())
            .await
    }

    /// 可注入 `now_ms` 的历史取数编排（测试确定性；生产走墙钟 [`Self::history`]）。
    async fn history_at(
        &self,
        venue: &str,
        symbol: &str,
        start: Option<i64>,
        end: Option<i64>,
        limit: Option<usize>,
        now_ms: i64,
    ) -> anyhow::Result<HistoryPage> {
        let q = HistoryQuery {
            venue: venue.to_string(),
            symbol: symbol.to_string(),
            start,
            end,
            limit: self.history_limits.normalize(limit),
        };
        let key = SeriesKey::new(venue, symbol);

        // 1) 冷启动懒加载：表内无该序列 → 读近一个保留窗 CSV 预热（restore 不置 dirty、不重写盘）。
        if self.bars.span(&key).is_none() {
            let age_ms = self.bars.retain_age().as_millis().min(i64::MAX as u128) as i64;
            let from = now_ms.saturating_sub(age_ms);
            let to = now_ms.saturating_add(KLINE_STEP_MS);
            self.restore_window(&key, from, to).await?;
        }

        // 2) 冷读闸门：显式起点早于内存最旧键 → spawn_blocking 冷读该区间归档 CSV。
        let mut cold = Vec::new();
        if let (Some(s), Some((oldest, _))) = (start, self.bars.span(&key)) {
            if s < oldest {
                let e = end.unwrap_or_else(|| now_ms.saturating_add(KLINE_STEP_MS));
                let got = read_csv_blocking(&self.repo, venue, symbol, s, e).await?;
                if got.skipped > 0 {
                    tracing::warn!(%venue, %symbol, skipped = got.skipped, "历史 CSV 冷读跳过坏行");
                }
                cold = got.bars;
            }
        }

        // 3) 热读（保留窗）+ 纯函数合并开窗（热优先去重/升序/limit/has_more）。
        let hot = self
            .bars
            .range(&key, start.unwrap_or(i64::MIN), end.unwrap_or(i64::MAX));
        Ok(merge_and_window(hot, cold, &q))
    }

    /// spawn_blocking 读 `[from,to)` 归档 CSV 并 `restore` 进表（幂等、不置 dirty）。
    async fn restore_window(&self, key: &SeriesKey, from: i64, to: i64) -> anyhow::Result<()> {
        let got = read_csv_blocking(&self.repo, &key.venue, &key.symbol, from, to).await?;
        for b in got.bars {
            self.bars.restore(b);
        }
        if got.skipped > 0 {
            tracing::warn!(?key, skipped = got.skipped, "懒加载预热 CSV 跳过坏行");
        }
        Ok(())
    }

    /// 启动：逐活跃作用域解析行情流并订阅（不支持的跳过），再 spawn 定时 flush。
    pub fn start(
        self: &Arc<Self>,
        exchanges: &Arc<ExchangeRegistry>,
        active: &[String],
        flush_interval: Duration,
        stop: Arc<AtomicBool>,
    ) {
        for scope in kline_scopes(active) {
            let wire = scope.wire();
            match exchanges.market_stream(scope.venue, Some(wire)) {
                Ok(stream) => {
                    if let Err(err) = self.attach(&scope, stream) {
                        tracing::warn!(
                            venue = scope.venue,
                            itype = wire,
                            error = %err,
                            "K线订阅建立失败，跳过该作用域"
                        );
                    } else {
                        tracing::info!(venue = scope.venue, itype = wire, "K线订阅已建立");
                    }
                }
                Err(err) => tracing::warn!(
                    venue = scope.venue,
                    itype = wire,
                    error = %err,
                    "该交易所行情流不可用，跳过 K线订阅"
                ),
            }
        }
        self.spawn_flush(flush_interval, Arc::clone(&stop));
        // 缺口周期扫描（内部扫描 + 静默停推）。scan_interval=0 时不起线程，实时尾检仍在。
        if self.gap_cfg.scan_interval_ms > 0 {
            self.spawn_gap_scan(Arc::clone(&stop));
        }
        self.spawn_push_ticker(Arc::clone(&stop));
        // 历史缺失自动 REST 回补（仅三家）。interval=0 关闭后台，但 P5 手动补仍可用。
        if self.backfill_cfg.interval > Duration::ZERO {
            self.spawn_backfill(Arc::clone(exchanges), Arc::clone(&stop));
        }
    }

    /// 启动后台历史回补 tokio 任务（非 OS 线程，fire-and-forget）。
    ///
    /// 不保存句柄、停机也不 join：回补是尽力而为，任务只持有 store 单例句柄、靠 `stop`
    /// 自退；与 flush 线程不同，它不承担「停机前最后一刷」责任（回补到的 bar 由 flush
    /// 线程统一落 CSV），故无需在 join_flush 收口。
    fn spawn_backfill(self: &Arc<Self>, exchanges: Arc<ExchangeRegistry>, stop: Arc<AtomicBool>) {
        crate::kline_backfill::spawn_backfill_worker(
            Arc::clone(&self.bars),
            Arc::clone(&self.gaps),
            exchanges,
            self.backfill_cfg,
            stop,
        );
    }

    /// P5 管理面手动补区间：同一时刻仅允许一个手动回补（busy 返回
    /// [`crate::kline_backfill::ManualError::Busy`]，由 handler 映射 40901）。
    /// 自动 worker 不经此锁（它用 GapRegistry 的 per-series inflight 去重）。
    pub async fn manual_backfill(
        self: &Arc<Self>,
        exchanges: &Arc<ExchangeRegistry>,
        venue: &str,
        symbol: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<crate::kline_backfill::BackfillResult, crate::kline_backfill::ManualError> {
        // compare_exchange 抢锁：false→true 成功进入；已在跑则 conflict。
        if self
            .manual_busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(crate::kline_backfill::ManualError::Busy);
        }
        // 无论成功失败/早返回，guard 在 drop 时复位标志。
        let _guard = crate::kline_backfill::ManualBusyGuard::new(Arc::clone(&self.manual_busy));
        crate::kline_backfill::run_range(&self.bars, exchanges, venue, symbol, start_ms, end_ms)
            .await
            .map_err(crate::kline_backfill::ManualError::Backfill)
    }

    /// 独立 OS 线程按扫描周期跑一拍缺口检测（纯内存、不连网）；停机被打断后补扫一次。
    fn spawn_gap_scan(self: &Arc<Self>, stop: Arc<AtomicBool>) {
        let me = Arc::clone(self);
        let interval = Duration::from_millis(self.gap_cfg.scan_interval_ms as u64);
        let handle = thread::Builder::new()
            .name("kline-gap".into())
            .spawn(move || {
                loop {
                    interruptible_sleep(interval, &stop);
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    // 纯内存扫描，异常只记日志、绝不向线程传播 panic（不影响 flush/push）。
                    let added = me.gap_scanner.scan_tick();
                    if added > 0 {
                        tracing::debug!(added, "kline-gap 周期扫描新登记缺失分钟");
                    }
                }
                // 停机前补扫一次（尽力；结果已在 store 收口集合，P4 消费）。
                let _ = me.gap_scanner.scan_tick();
            })
            .expect("启动 kline-gap 线程失败");
        *self.gap_task.lock().unwrap() = Some(handle);
    }

    /// 独立 OS 线程按 200ms 拍轮询：关闭超龄分钟桶并把已关帧发出（同步 `pusher.push`
    /// 只做闸门+spawn，不进 tokio worker）。分钟桶通常由「更晚分钟 bar」的水位线在
    /// 到达后下一拍即发出；停机被打断后 flush 全部在途桶，尽力推出缓冲内剩余 bar。
    fn spawn_push_ticker(self: &Arc<Self>, stop: Arc<AtomicBool>) {
        let me = Arc::clone(self);
        let handle = thread::Builder::new()
            .name("kline-push".into())
            .spawn(move || {
                loop {
                    interruptible_sleep(KLINE_PUSH_TICK_INTERVAL, &stop);
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    me.poll_push();
                }
                // 停机兜底：关闭全部在途分钟桶并尽力推出（断连/在途满的帧按 pusher
                // 返回值计入 dropped，不阻塞停机、不重试）。
                me.flush_remaining_push();
            })
            // 与刷盘线程同口径：启动期 spawn 失败直接 fail-fast，避免静默不推送。
            .expect("启动 kline-push 线程失败");
        *self.push_task.lock().unwrap() = Some(handle);
    }

    /// 独立 OS 线程定时刷盘（同步 IO 不进 tokio worker）；停机被打断后最后再刷一次。
    fn spawn_flush(self: &Arc<Self>, interval: Duration, stop: Arc<AtomicBool>) {
        let me = Arc::clone(self);
        let handle = thread::Builder::new()
            .name("kline-flush".into())
            .spawn(move || {
                loop {
                    interruptible_sleep(interval, &stop);
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let out = me.flush_once();
                    if out.bars.rows > 0 || out.ticks.rows > 0 {
                        tracing::info!(
                            bars = out.bars.rows,
                            bar_files = out.bars.files,
                            ticks = out.ticks.rows,
                            tick_files = out.ticks.files,
                            "K线 CSV 批量刷盘"
                        );
                    }
                    // 慢/断连接推送丢弃计数：有积压才报，便于发现南向推送异常。
                    let dropped = me.dropped.swap(0, Ordering::Relaxed);
                    if dropped > 0 {
                        tracing::warn!(dropped, "kline_1m_batch 推送丢弃（连接不可达/在途积压）");
                    }
                    // bar dirty 积压超阈边沿告警（flush 持续故障时；不丢数据，只提示）。
                    me.check_bar_backlog();
                    // CTP 原始 tick 刷盘窗口背压丢弃（仅 ctp 有写入，非 ctp 计数恒 0）。
                    let tick_dropped = me.ticks.take_dropped();
                    if tick_dropped > 0 {
                        tracing::error!(
                            dropped = tick_dropped,
                            "CTP 原始 tick 因刷盘窗口积压超上限被丢弃（请检查 CSV 落盘/磁盘）"
                        );
                    }
                }
                // 停机兜底：把最后一批定稿 bar/tick 落盘，避免丢窗口内数据。
                let out = me.flush_once();
                if out.bars.rows + out.ticks.rows > 0 {
                    tracing::info!(
                        bars = out.bars.rows,
                        ticks = out.ticks.rows,
                        "K线停机收尾刷盘"
                    );
                }
            })
            // spawn 失败（资源耗尽）必须 fail-fast：否则 K线永不刷盘、内存窗口
            // 无界增长且无任何告警，是静默数据丢失。启动期错误直接 panic 暴露。
            .expect("启动 kline-flush 线程失败");
        // 留存句柄供优雅停机 join（保证最后一刷落盘后进程才退出）。
        *self.flush_task.lock().unwrap() = Some(handle);
    }
}

/// take 出可选线程句柄并 join：先释放锁再等待，句柄为 None（未 start/已 join）安全空转；
/// 线程曾 panic 时向上传播 [`thread::Result`] 错误。
fn join_take(slot: &Mutex<Option<thread::JoinHandle<()>>>) -> thread::Result<()> {
    match slot.lock().unwrap().take() {
        Some(handle) => handle.join(),
        None => Ok(()),
    }
}

/// 按 100ms 分片睡眠，可被停机标志提前打断（与 ctp-md/dominant 同口径）。
fn interruptible_sleep(total: Duration, stop: &AtomicBool) {
    let mut left = total.as_millis() as u64;
    while left > 0 && !stop.load(Ordering::Relaxed) {
        let step = left.min(100);
        thread::sleep(Duration::from_millis(step));
        left = left.saturating_sub(step);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::sync::Mutex;
    use trading_exchange::market::KlineInterval;

    fn kline(venue: &str, symbol: &str, day: u32, start: i64) -> StreamKline {
        // CTP bar 带本所 extras（真实交易所代码 + 周期末持仓量）；其余所 extras 为空。
        let mut extras = BTreeMap::new();
        if venue == "ctp" {
            extras.insert(
                "exchange_id".to_string(),
                trading_models::KlineExtra::Str("SHFE".into()),
            );
            extras.insert(
                "open_interest".to_string(),
                trading_models::KlineExtra::Num(Decimal::from(100)),
            );
        }
        StreamKline {
            venue: venue.into(),
            symbol: symbol.into(),
            interval: KlineInterval::M1,
            trading_day: day,
            start_ms: start,
            end_ms: start + 60_000,
            open: Decimal::from(10),
            high: Decimal::from(12),
            low: Decimal::from(9),
            close: Decimal::from(11),
            volume: 3,
            quote_volume: Decimal::from(33),
            extras,
        }
    }

    fn test_store() -> Arc<TradingStore> {
        let dir = tempfile::tempdir().unwrap();
        let (store, _persist) = TradingStore::open(
            dir.path(),
            std::sync::Arc::new(store_core::SystemClock),
            None,
        )
        .unwrap();
        // 临时目录随进程 tmp 留存即可（测试不校验清理）。
        std::mem::forget(dir);
        Arc::new(store)
    }

    /// 记录推送、可模拟失败的假推送端口。每次 push 记录 (strategy, kind, 帧内 symbols)；
    /// 批量帧 symbols 来自 klines 数组（已排序），非批量帧为空。
    #[derive(Default)]
    struct RecPusher {
        sent: Mutex<Vec<(String, String, Vec<String>)>>,
        fail: Mutex<std::collections::HashSet<String>>,
    }
    impl RecPusher {
        /// 某策略收到的 (帧类型, 帧内标的列表)，按推送顺序。
        fn frames(&self, strategy: &str) -> Vec<(String, Vec<String>)> {
            self.sent
                .lock()
                .unwrap()
                .iter()
                .filter(|(s, _, _)| s == strategy)
                .map(|(_, kind, syms)| (kind.clone(), syms.clone()))
                .collect()
        }
        /// 某策略在所有批量帧里收到的标的（按推送/帧内顺序摊平）。
        fn symbols(&self, strategy: &str) -> Vec<String> {
            self.frames(strategy)
                .into_iter()
                .flat_map(|(_, syms)| syms)
                .collect()
        }
        fn total(&self) -> usize {
            self.sent.lock().unwrap().len()
        }
        fn fail_for(&self, strategy: &str) {
            self.fail.lock().unwrap().insert(strategy.to_string());
        }
    }
    impl KlinePusher for RecPusher {
        fn push(&self, strategy: &str, event: ClientEvent) -> bool {
            if self.fail.lock().unwrap().contains(strategy) {
                return false;
            }
            let syms = event
                .klines
                .unwrap_or_default()
                .iter()
                .map(|k| k.symbol.clone())
                .collect();
            self.sent
                .lock()
                .unwrap()
                .push((strategy.to_string(), event.kind.clone(), syms));
            true
        }
    }

    /// 把测试 StreamKline 转成南向载荷（batcher 收的元素）。
    fn event(venue: &str, symbol: &str, start: i64) -> KlineEvent {
        kline_event(&kline(venue, symbol, 20261027, start))
    }

    /// 取一帧的 (strategy, venue, [symbol])，便于断言。
    fn frame_triple(f: &(String, String, Vec<KlineEvent>)) -> (&str, &str, Vec<&str>) {
        (
            f.0.as_str(),
            f.1.as_str(),
            f.2.iter().map(|k| k.symbol.as_str()).collect(),
        )
    }

    #[test]
    fn kline_event_passes_through_quote_volume_and_venue_extras() {
        // CTP bar：公共 quote_volume 直搬，extras 带真实交易所代码与持仓量。
        let ctp = event("ctp", "cu2512", 60_000);
        assert_eq!(ctp.quote_volume, Decimal::from(33));
        assert_eq!(
            ctp.extras.get("exchange_id"),
            Some(&trading_models::KlineExtra::Str("SHFE".into()))
        );
        assert_eq!(
            ctp.extras.get("open_interest").and_then(|v| v.as_decimal()),
            Some(Decimal::from(100))
        );

        // 非 CTP bar（binance 测试夹具）：extras 为空，不含 CTP 专有键。
        let bin = event("binance", "BTCUSDT", 60_000);
        assert_eq!(bin.quote_volume, Decimal::from(33));
        assert!(bin.extras.is_empty());
        assert!(!bin.extras.contains_key("exchange_id"));
        assert!(!bin.extras.contains_key("open_interest"));
    }

    #[test]
    fn batcher_coalesces_same_minute_symbols_until_next_minute_watermark() {
        // 关键需求：同分钟多标的即便 ~880ms 后才到齐，也必须在关桶前并入同一帧；
        // 直到「同 strategy+venue 的更晚分钟 bar」到达（有序水位线）才关桶。
        let mut core = BatcherCore::new(1_000_000); // 极大超时：本用例只靠水位线关桶
                                                    // 分钟 60000：ETH 先到、BTC 在 880ms 后到（乱序、跨接近 1s）。
        core.push(
            1_000,
            "alpha",
            "binance",
            event("binance", "ETHUSDT", 60_000),
        );
        core.push(
            1_880,
            "alpha",
            "binance",
            event("binance", "BTCUSDT", 60_000),
        );
        // 未到更晚分钟、也未超时 → 不出帧，两标的都还在桶内。
        assert!(core.poll(5_000).is_empty());

        // 分钟 120000 的首根 bar 到达 → 水位线关闭分钟 60000 桶。
        core.push(
            62_000,
            "alpha",
            "binance",
            event("binance", "BTCUSDT", 120_000),
        );
        let frames = core.poll(62_000);
        assert_eq!(frames.len(), 1);
        // 一帧含两标的、按 symbol 排序；新分钟桶仍在途，不被本轮带出。
        assert_eq!(
            frame_triple(&frames[0]),
            ("alpha", "binance", vec!["BTCUSDT", "ETHUSDT"])
        );
        assert!(frames[0].2.iter().all(|k| k.start_time == 60_000));
        // 已取走的帧下轮不重复；新分钟桶未关仍不出。
        assert!(core.poll(62_100).is_empty());
    }

    #[test]
    fn batcher_coalesces_multiple_ctp_products_into_one_batch_frame() {
        // 真机场景：订阅纯品种 cu/rb，分钟边界把两个品种的当前主力
        // （cu2701、rb2701）的定稿 bar 合进同一条 kline_1m_batch。
        let mut core = BatcherCore::new(8_000);
        core.push(0, "alpha", "ctp", event("ctp", "cu2701", 60_000));
        core.push(880, "alpha", "ctp", event("ctp", "rb2701", 60_000));
        // 未到安全超时：两品种都在同一桶内，不出帧。
        assert!(core.poll(7_999).is_empty());

        let frames = core.poll(8_000);
        assert_eq!(frames.len(), 1, "两个品种必须合为一条帧");
        let (strategy, venue, bars) = frame_triple(&frames[0]);
        assert_eq!(strategy, "alpha");
        assert_eq!(venue, "ctp");
        // 同帧含两个品种、按 symbol 排序；分钟桶一致。
        assert_eq!(bars, vec!["cu2701", "rb2701"]);
        assert!(frames[0].2.iter().all(|k| k.start_time == 60_000));
        // 每根都带 CTP 平铺 extras（exchange_id + open_interest），不串味成币安键。
        for k in &frames[0].2 {
            assert_eq!(
                k.extras.get("exchange_id"),
                Some(&trading_models::KlineExtra::Str("SHFE".into())),
                "{} 应带 exchange_id=SHFE",
                k.symbol
            );
            assert!(
                k.extras
                    .get("open_interest")
                    .and_then(|v| v.as_decimal())
                    .is_some(),
                "{} 应带 open_interest",
                k.symbol
            );
            assert!(!k.extras.contains_key("trade_num"));
        }
        assert!(core.poll(8_001).is_empty());
    }

    #[test]
    fn batcher_closes_idle_bucket_by_safety_timeout() {
        // 没有更晚分钟 bar 驱动水位线时（该所这一分钟后无新定稿），桶到安全超时关闭。
        let mut core = BatcherCore::new(8_000);
        core.push(0, "alpha", "binance", event("binance", "BTCUSDT", 60_000));
        assert!(core.poll(7_999).is_empty());
        let frames = core.poll(8_000);
        assert_eq!(frames.len(), 1);
        assert_eq!(
            frame_triple(&frames[0]),
            ("alpha", "binance", vec!["BTCUSDT"])
        );
        // 取空后不重复。
        assert!(core.poll(9_000).is_empty());
    }

    #[test]
    fn batcher_watermark_is_scoped_per_strategy_and_venue() {
        // 别的 strategy 或别的 venue 的更晚分钟 bar，不得关闭本 (strategy,venue) 的桶。
        let mut core = BatcherCore::new(1_000_000);
        core.push(0, "alpha", "binance", event("binance", "BTCUSDT", 60_000));
        // beta/binance 与 alpha/ctp 进到更晚分钟：都不应触发 alpha/binance 的水位线。
        core.push(0, "beta", "binance", event("binance", "BTCUSDT", 120_000));
        core.push(0, "alpha", "ctp", event("ctp", "cu2512", 120_000));
        assert!(core.poll(0).is_empty());

        // 只有 alpha/binance 自己的更晚分钟 bar 才关它的 60000 桶。
        core.push(1, "alpha", "binance", event("binance", "BTCUSDT", 120_000));
        let frames = core.poll(1);
        assert_eq!(frames.len(), 1);
        assert_eq!(
            frame_triple(&frames[0]),
            ("alpha", "binance", vec!["BTCUSDT"])
        );
    }

    #[test]
    fn batcher_separates_frames_by_strategy_venue_and_minute() {
        // strategy × venue × 分钟 三元组各自独立成帧，绝不串帧。
        let mut core = BatcherCore::new(1_000_000);
        core.push(0, "alpha", "binance", event("binance", "BTCUSDT", 60_000));
        core.push(0, "alpha", "binance", event("binance", "BTCUSDT", 120_000));
        core.push(0, "alpha", "ctp", event("ctp", "cu2512", 60_000));
        core.push(0, "beta", "binance", event("binance", "ETHUSDT", 60_000));

        // flush_all 一次关闭全部在途桶；BTreeMap 键序确定。
        let frames = core.flush_all();
        let got: Vec<(&str, &str, i64, Vec<&str>)> = frames
            .iter()
            .map(|(s, v, ks)| {
                (
                    s.as_str(),
                    v.as_str(),
                    ks[0].start_time,
                    ks.iter().map(|k| k.symbol.as_str()).collect(),
                )
            })
            .collect();
        assert_eq!(
            got,
            vec![
                ("alpha", "binance", 60_000, vec!["BTCUSDT"]),
                ("alpha", "binance", 120_000, vec!["BTCUSDT"]),
                ("alpha", "ctp", 60_000, vec!["cu2512"]),
                ("beta", "binance", 60_000, vec!["ETHUSDT"]),
            ]
        );
        // 已全部取空。
        assert!(core.flush_all().is_empty());
    }

    #[test]
    fn batcher_empty_poll_and_flush_yield_no_frames() {
        let mut core = BatcherCore::new(8_000);
        assert!(core.poll(0).is_empty());
        assert!(core.flush_all().is_empty());
    }

    #[test]
    fn kline_batcher_handle_is_shareable_across_clones() {
        // KlineBatcher(Arc<Mutex<_>>) 的 clone 共享同一内核：一个句柄 push，另一个可见；
        // flush_all 关闭在途桶（不依赖墙钟超时），跨 clone 一致取空。
        let batcher = KlineBatcher::default();
        let other = batcher.clone();
        other.push("alpha", "binance", event("binance", "BTCUSDT", 60_000));

        let frames = batcher.flush_all();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].2[0].symbol, "BTCUSDT");
        // flush 后两个句柄看到的都是空。
        assert!(other.flush_all().is_empty());
    }

    #[test]
    fn in_flight_gate_caps_per_strategy_and_isolates_clients() {
        // 每连接上限 2：a 取满 2 个许可后第 3 次被拒；连接 b 互不影响；归还后可再取。
        let gate = InFlightGate::new(2);
        let p1 = gate.try_acquire("a").expect("permit 1");
        let _p2 = gate.try_acquire("a").expect("permit 2");
        // a 在途已满 → 背压拒绝（慢连接丢帧据此计数）。
        assert!(gate.try_acquire("a").is_none());
        // 另一连接 b 独立计数，不受 a 积压牵连。
        let pb = gate.try_acquire("b").expect("b 独立许可");
        // 归还 a 一个许可后恢复可取。
        drop(p1);
        assert!(gate.try_acquire("a").is_some());
        drop(pb);
    }

    #[test]
    fn check_bar_backlog_state_tracks_threshold_transitions() {
        // 阈值取 2：写 3 根未确认 → 超阈置位；mark 清 dirty 后再查 → 复位。
        let store = test_store();
        let (svc, _p) = svc(store.clone());
        svc.bars.upsert(crate_bar("ctp", "cu2512", 60_000));
        svc.bars.upsert(crate_bar("ctp", "cu2512", 120_000));
        svc.bars.upsert(crate_bar("ctp", "rb2611", 60_000));
        svc.check_bar_backlog_at(2);
        assert!(svc.bar_backlog_alarmed.load(Ordering::Relaxed));

        // 仍超阈，状态保持（不重复告警由纯函数保证）。
        svc.check_bar_backlog_at(2);
        assert!(svc.bar_backlog_alarmed.load(Ordering::Relaxed));

        // flush 成功确认后 dirty 归零，再查应复位。
        let snap = svc.bars.snapshot_dirty();
        svc.bars.mark_flushed(
            &snap
                .iter()
                .map(|d| (d.key.clone(), d.start, d.gen))
                .collect::<Vec<_>>(),
        );
        svc.check_bar_backlog_at(2);
        assert!(!svc.bar_backlog_alarmed.load(Ordering::Relaxed));
    }

    /// 测试辅助：造一根最简 bar（pending_dirty/backlog 测试用）。
    fn crate_bar(venue: &str, symbol: &str, start: i64) -> Bar {
        Bar::new(venue, symbol, 20261024, start, start + KLINE_STEP_MS)
    }

    #[test]
    fn backlog_edge_fires_only_on_transitions() {
        const T: usize = 10;
        // 正常→超阈：告警一次。
        assert_eq!(backlog_edge(false, 11, T), (true, true, false));
        // 仍超阈：不重复告警。
        assert_eq!(backlog_edge(true, 12, T), (true, false, false));
        // 回落到阈值内（含恰好等于阈值）：恢复一次。
        assert_eq!(backlog_edge(true, 10, T), (false, false, true));
        // 正常态持续：无事件。
        assert_eq!(backlog_edge(false, 5, T), (false, false, false));
        // 恰好等于阈值不算超阈。
        assert_eq!(backlog_edge(false, T, T), (false, false, false));
    }

    /// 测试用缺省历史取数限额。
    fn default_limits() -> HistoryLimits {
        HistoryLimits::new(
            crate::kline_history::DEFAULT_HISTORY_LIMIT,
            crate::kline_history::MAX_HISTORY_LIMIT,
        )
        .unwrap()
    }

    /// 测试用缺口扫描参数：1m 步长、3min 静默阈值；scan=0（单测默认不起后台线程）。
    fn test_gap_cfg() -> GapScanConfig {
        GapScanConfig::new(KLINE_STEP_MS, 3 * KLINE_STEP_MS, 0).unwrap()
    }

    /// 测试用回补配置：关闭后台 worker（interval=0），避免单测无意起异步任务。
    fn test_backfill_cfg() -> crate::kline_backfill::BackfillRuntimeConfig {
        crate::kline_backfill::BackfillRuntimeConfig::disabled()
    }

    fn svc(store: Arc<TradingStore>) -> (Arc<KlineService>, Arc<RecPusher>) {
        let pusher = Arc::new(RecPusher::default());
        let s = Arc::new(KlineService::new(
            store,
            Arc::clone(&pusher) as Arc<dyn KlinePusher>,
            Vec::new(),
            default_limits(),
            test_gap_cfg(),
            test_backfill_cfg(),
        ));
        (s, pusher)
    }

    /// 带配置基线的服务（基线符号在构造时 trim/去空/去重，保持原大小写）。
    fn svc_baselines(
        store: Arc<TradingStore>,
        baselines: Vec<BaselineKlines>,
    ) -> (Arc<KlineService>, Arc<RecPusher>) {
        let pusher = Arc::new(RecPusher::default());
        let s = Arc::new(KlineService::new(
            store,
            Arc::clone(&pusher) as Arc<dyn KlinePusher>,
            baselines,
            default_limits(),
            test_gap_cfg(),
            test_backfill_cfg(),
        ));
        (s, pusher)
    }

    /// 在保留目录上开一个 store（目录由调用方持有，供冷读 CSV 测试）。
    fn svc_in_dir(dir: &std::path::Path) -> (Arc<TradingStore>, Arc<KlineService>, KlineRepo) {
        let (store, _persist) =
            TradingStore::open(dir, Arc::new(store_core::SystemClock), None).unwrap();
        let store = Arc::new(store);
        let repo = store.kline_repo();
        let pusher = Arc::new(RecPusher::default());
        let svc = Arc::new(KlineService::new(
            Arc::clone(&store),
            pusher as Arc<dyn KlinePusher>,
            Vec::new(),
            default_limits(),
            test_gap_cfg(),
            test_backfill_cfg(),
        ));
        (store, svc, repo)
    }

    /// 真实 UTC 时刻（毫秒）。
    fn utc_ms(y: i32, m: u32, d: u32, h: u32, min: u32) -> i64 {
        use chrono::TimeZone;
        chrono::Utc
            .with_ymd_and_hms(y, m, d, h, min, 0)
            .single()
            .unwrap()
            .timestamp_millis()
    }

    /// 造一根 trading_day 与 start 的 UTC 自然日一致的 bar（WS 所口径，冷读按日枚举目录）。
    fn real_bar(venue: &str, symbol: &str, y: i32, m: u32, d: u32, h: u32, min: u32) -> Bar {
        use chrono::TimeZone;
        let start = utc_ms(y, m, d, h, min);
        let day: u32 = chrono::Utc
            .timestamp_millis_opt(start)
            .single()
            .unwrap()
            .format("%Y%m%d")
            .to_string()
            .parse()
            .unwrap();
        let mut b = Bar::new(venue, symbol, day, start, start + KLINE_STEP_MS);
        b.close = rust_decimal::Decimal::from(100);
        b.volume = min as i64;
        b
    }

    #[tokio::test]
    async fn history_hot_window_served_without_disk_read() {
        let dir = tempfile::tempdir().unwrap();
        let (store, svc, _repo) = svc_in_dir(dir.path());
        let b = real_bar("ctp", "cu2512", 2026, 10, 29, 11, 59);
        let start = b.start_time;
        // 直接写内存表（不落盘）：span 存在、区间在保留窗 → 纯热读返回。
        store.bar_table().upsert(b);

        let page = svc
            .history_at(
                "ctp",
                "cu2512",
                Some(start),
                Some(start + KLINE_STEP_MS),
                None,
                utc_ms(2026, 10, 29, 12, 0),
            )
            .await
            .unwrap();
        assert_eq!(page.bars.len(), 1);
        assert_eq!(page.bars[0].start_time, start);
        assert!(!page.has_more);
    }

    #[tokio::test]
    async fn history_cold_read_merges_archived_csv_with_hot_dedup() {
        let dir = tempfile::tempdir().unwrap();
        let (store, _svc, repo) = svc_in_dir(dir.path());
        // 三根相邻分钟 bar 落 CSV（flush 非破坏，bar 仍在表）。
        for (h, min) in [(11, 58), (11, 59), (12, 0)] {
            store
                .bar_table()
                .upsert(real_bar("ctp", "cu2512", 2026, 10, 29, h, min));
        }
        repo.flush_bars(&store.bar_table()).unwrap();

        // 全新 store（同目录、空内存）：冷启动 restore + 显式旧区间触发冷读，去重合并。
        let (store2, svc2, _) = svc_in_dir(dir.path());
        assert_eq!(
            store2
                .bar_table()
                .coverage(&SeriesKey::new("ctp", "cu2512")),
            0
        );
        let page = svc2
            .history_at(
                "ctp",
                "cu2512",
                Some(0),
                Some(i64::MAX),
                None,
                utc_ms(2026, 10, 29, 12, 0),
            )
            .await
            .unwrap();
        assert_eq!(
            page.bars.iter().map(|b| b.start_time).collect::<Vec<_>>(),
            vec![
                utc_ms(2026, 10, 29, 11, 58),
                utc_ms(2026, 10, 29, 11, 59),
                utc_ms(2026, 10, 29, 12, 0)
            ]
        );
        // restore 预热绝不置 dirty（不会重写盘）。
        assert!(store2.bar_table().snapshot_dirty().is_empty());
    }

    #[tokio::test]
    async fn history_lazy_restore_populates_table_without_dirty() {
        let dir = tempfile::tempdir().unwrap();
        let (store, _svc, repo) = svc_in_dir(dir.path());
        let mut b = real_bar("binance", "BTCUSDT", 2026, 10, 29, 11, 59);
        b.close = rust_decimal::Decimal::from(108);
        let start = b.start_time;
        store.bar_table().upsert(b);
        repo.flush_bars(&store.bar_table()).unwrap();

        // 全新空表 + 近窗 CSV：无区间取最近 → 懒加载 restore 后命中。
        let (store2, svc2, _) = svc_in_dir(dir.path());
        let page = svc2
            .history_at(
                "binance",
                "BTCUSDT",
                None,
                None,
                Some(10),
                utc_ms(2026, 10, 29, 12, 0),
            )
            .await
            .unwrap();
        assert_eq!(page.bars.len(), 1);
        assert_eq!(page.bars[0].close, rust_decimal::Decimal::from(108));
        assert_eq!(page.eff_start, Some(start));
        assert_eq!(page.eff_end, Some(start));
        // 表已预热但不置 dirty。
        assert!(store2.bar_table().snapshot_dirty().is_empty());
    }

    #[tokio::test]
    async fn history_unknown_symbol_returns_empty_not_error() {
        let dir = tempfile::tempdir().unwrap();
        let (_store, svc, _repo) = svc_in_dir(dir.path());
        let page = svc
            .history_at(
                "ctp",
                "ag9999",
                Some(0),
                Some(i64::MAX),
                None,
                utc_ms(2026, 10, 29, 12, 0),
            )
            .await
            .unwrap();
        assert!(page.bars.is_empty());
        assert!(!page.has_more);
    }

    #[test]
    fn scopes_only_cover_active_supported_venues_with_their_itype() {
        let active = vec![
            "binance".to_string(),
            "deribit".to_string(),
            "hyperliquid".to_string(),
            "ctp".to_string(),
            "xili".to_string(), // 本地模拟所：不支持 K线，绝不出现在作用域
        ];
        let scopes = kline_scopes(&active);
        let got: Vec<(&str, InstrumentType)> = scopes.iter().map(|s| (s.venue, s.itype)).collect();
        assert_eq!(
            got,
            vec![
                ("binance", InstrumentType::Futures),
                ("deribit", InstrumentType::Option),
                ("hyperliquid", InstrumentType::Futures),
                ("ctp", InstrumentType::Futures),
            ]
        );
        assert!(kline_scopes(&["xili".to_string()]).is_empty());
        assert_eq!(kline_scopes(&["binance".to_string()])[0].wire(), "futures");
        assert_eq!(kline_scopes(&["deribit".to_string()])[0].wire(), "option");
    }

    #[test]
    fn sink_writes_finalized_bar_into_bar_table() {
        let store = test_store();
        let (service, _pusher) = svc(store.clone());

        service
            .persistence_sink()
            .on_bar(&kline("ctp", "cu2512", 20261024, 60_000));

        let pending = store.bar_table().range(
            &trading_core::kline::SeriesKey::new("ctp", "cu2512"),
            0,
            i64::MAX,
        );
        assert_eq!(pending.len(), 1);
        let b = &pending[0];
        assert_eq!(b.exchange, "ctp");
        assert_eq!(b.symbol, "cu2512");
        assert_eq!(b.trading_day, 20261024);
        assert_eq!(b.start_time, 60_000);
        assert_eq!(b.end_time, 120_000);
        assert_eq!(b.volume, 3);
        // open_interest 现为 CTP extras（Num），公共 Bar 不再有该具名字段。
        assert_eq!(
            b.extras.get("open_interest").and_then(|v| v.as_decimal()),
            Some(Decimal::from(100))
        );
        assert_eq!(b.quote_volume, Decimal::from(33));
    }

    /// 记录 subscribe_klines 调用的假行情流。
    struct FakeStream {
        calls: Mutex<Vec<Vec<String>>>,
        want_err: bool,
    }
    impl FakeStream {
        fn new(want_err: bool) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                want_err,
            }
        }
        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
        /// 最后一次 subscribe_klines 看到的期望符号快照。
        fn last_symbols(&self) -> Vec<String> {
            self.calls
                .lock()
                .unwrap()
                .last()
                .cloned()
                .unwrap_or_default()
        }
    }
    impl MarketDataStream for FakeStream {
        fn subscribe_prices(
            &self,
            _src: std::sync::Arc<dyn DesiredSource>,
        ) -> anyhow::Result<SubscriptionGuard> {
            Ok(SubscriptionGuard::new(Box::new(|| {})))
        }
        fn subscribe_klines(
            &self,
            src: std::sync::Arc<dyn DesiredSource>,
            _sink: std::sync::Arc<dyn BarSink>,
        ) -> anyhow::Result<SubscriptionGuard> {
            self.calls.lock().unwrap().push(src.symbols());
            if self.want_err {
                anyhow::bail!("unsupported");
            }
            Ok(SubscriptionGuard::new(Box::new(|| {})))
        }
    }

    #[test]
    fn attach_registers_one_subscription_per_scope_and_holds_guard() {
        let store = test_store();
        let (service, _pusher) = svc(store.clone());
        let scope = KlineScope::new("ctp", InstrumentType::Futures);
        let stream = std::sync::Arc::new(FakeStream::new(false));

        let guard = service.attach(
            &scope,
            stream.clone() as std::sync::Arc<dyn MarketDataStream>,
        );

        assert!(guard.is_ok());
        assert_eq!(stream.call_count(), 1);
        assert_eq!(service.guard_count(), 1);

        // 同一底层登记表：该作用域新增订阅后，来源快照即时多出该合约。
        store.kline_registry().subscribe(
            "alpha",
            trading_core::kline::KlineKey::new("ctp", InstrumentType::Futures, "cu2512", "1m"),
        );
        assert_eq!(
            service.source_for("ctp", InstrumentType::Futures).symbols(),
            vec!["cu2512".to_string()]
        );
    }

    #[test]
    fn attach_returns_err_and_holds_no_guard_when_stream_unsupported() {
        let store = test_store();
        let (service, _pusher) = svc(store.clone());
        let scope = KlineScope::new("xili", InstrumentType::Futures);
        let stream = std::sync::Arc::new(FakeStream::new(true));

        assert!(service
            .attach(&scope, stream as std::sync::Arc<dyn MarketDataStream>)
            .is_err());
        assert_eq!(service.guard_count(), 0);
    }

    #[test]
    fn attach_source_contains_baseline_symbols_without_any_registry_client() {
        // 配置基线：零南向订阅时 attach 的期望来源也含基线合约（线订阅即建立）。
        let store = test_store();
        let (service, _pusher) = svc_baselines(
            store.clone(),
            vec![BaselineKlines::new(
                "binance",
                InstrumentType::Futures,
                vec!["ETHUSDT".into(), "BTCUSDT".into()],
            )],
        );
        let scope = KlineScope::new("binance", InstrumentType::Futures);
        let stream = std::sync::Arc::new(FakeStream::new(false));

        service
            .attach(
                &scope,
                stream.clone() as std::sync::Arc<dyn MarketDataStream>,
            )
            .unwrap();

        let captured = stream.last_symbols();
        // 去重、字典序，与登记表快照口径一致。
        assert_eq!(captured, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    }

    #[test]
    fn baseline_unions_with_registry_and_dedups_sorted() {
        let store = test_store();
        let (service, _pusher) = svc_baselines(
            store.clone(),
            vec![BaselineKlines::new(
                "binance",
                InstrumentType::Futures,
                vec!["BTCUSDT".into(), " btcusdt ".into(), "".into(), "  ".into()],
            )],
        );
        // 客户端运行时订阅与基线交叠（BTCUSDT）并新增 ETHUSDT。
        service.subscribe("alpha", "binance", &["BTCUSDT".into(), "ETHUSDT".into()]);

        // trim/去空/去重后基线只剩 BTCUSDT 与 btcusdt 两个原值（保持原大小写）；
        // 与登记表取并集后去重排序，大小写不同的符号视为不同频道。
        let got = service
            .source_for("binance", InstrumentType::Futures)
            .symbols();
        assert_eq!(
            got,
            vec![
                "BTCUSDT".to_string(),
                "ETHUSDT".to_string(),
                "btcusdt".to_string()
            ]
        );
        // 别的作用域不受该基线影响。
        assert!(service
            .source_for("ctp", InstrumentType::Futures)
            .symbols()
            .is_empty());
    }

    #[test]
    fn baseline_symbols_never_trigger_south_push_without_subscriber() {
        // 基线只驱动线订阅与落盘，不进 KlineRegistry：基线合约定稿 bar 经推送 sink
        // 时没有任何南向接收方（落盘由独立无条件 sink 负责，不经此路径）。
        let store = test_store();
        let (service, pusher) = svc_baselines(
            store.clone(),
            vec![BaselineKlines::new(
                "binance",
                InstrumentType::Futures,
                vec!["BTCUSDT".into()],
            )],
        );

        service
            .push_sink()
            .on_bar(&kline("binance", "BTCUSDT", 20261027, 60_000));

        assert_eq!(pusher.total(), 0);
        assert_eq!(service.dropped_count(), 0);
    }

    #[test]
    fn flush_once_drains_bar_and_tick_tables_to_csv() {
        let dir = tempfile::tempdir().unwrap();
        let (store, _persist) = TradingStore::open(
            dir.path(),
            std::sync::Arc::new(store_core::SystemClock),
            None,
        )
        .unwrap();
        let store = Arc::new(store);
        let (service, _pusher) = svc(store.clone());
        service
            .persistence_sink()
            .on_bar(&kline("ctp", "cu2512", 20261024, 60_000));
        store.ctp_ticks().append(trading_models::RawTick {
            exchange: "ctp".into(),
            exchange_id: "SHFE".into(),
            symbol: "cu2512".into(),
            timestamp: 60_000,
            trading_day: 20261024,
            price: Decimal::from(11),
            open: Decimal::ZERO,
            high: Decimal::ZERO,
            low: Decimal::ZERO,
            close: Decimal::ZERO,
            volume: 3,
            quote_volume: Decimal::from(33),
            open_interest: Decimal::from(100),
            bid: [Decimal::ZERO; 5],
            ask: [Decimal::ZERO; 5],
            bid_vol: [0; 5],
            ask_vol: [0; 5],
        });

        let report = service.flush_once();

        assert_eq!(report.bars.rows, 1);
        assert_eq!(report.ticks.rows, 1);
        // 非破坏式 flush：bar 本体仍在内存可读，但 dirty 已清（下轮不重写）。
        let bar_key = trading_core::kline::SeriesKey::new("ctp", "cu2512");
        assert_eq!(store.bar_table().coverage(&bar_key), 1);
        assert!(store.bar_table().snapshot_dirty().is_empty());
        // tick 仍是 drain WAL：写成功即清空。
        assert!(store.ctp_ticks().is_empty());
        assert!(dir
            .path()
            .join("kline/ctp/1m/20261024/cu2512.csv")
            .is_file());
        assert!(dir
            .path()
            .join("kline/ctp/tick/20261024/cu2512.csv")
            .is_file());
    }

    #[test]
    fn subscribe_unsubscribe_and_disconnect_drive_registry() {
        let store = test_store();
        let (service, _p) = svc(store.clone());

        service.subscribe("alpha", "ctp", &["cu2512".to_string(), " cu ".to_string()]);
        service.subscribe("beta", "ctp", &["cu2512".to_string()]);
        // 两个客户端订阅同一具体合约：引用计数 2；品种订阅另算。
        assert_eq!(
            service.source_for("ctp", InstrumentType::Futures).symbols(),
            vec!["cu".to_string(), "cu2512".to_string()]
        );

        // alpha 退订具体合约：beta 仍在，合约不消失。
        service.unsubscribe("alpha", "ctp", &["cu2512".to_string()]);
        assert_eq!(
            service.source_for("ctp", InstrumentType::Futures).symbols(),
            vec!["cu".to_string(), "cu2512".to_string()]
        );
        // beta 断连：cu2512 归零消失；alpha 的品种订阅保留。
        service.disconnect("beta");
        assert_eq!(
            service.source_for("ctp", InstrumentType::Futures).symbols(),
            vec!["cu".to_string()]
        );
        // alpha 断连：全部清空 → 交易所下轮退订。
        service.disconnect("alpha");
        assert!(service
            .source_for("ctp", InstrumentType::Futures)
            .symbols()
            .is_empty());
    }

    #[test]
    fn finalized_bar_fans_out_to_exact_and_product_subscribers() {
        let store = test_store();
        let (service, pusher) = svc(store.clone());
        // direct 订具体合约；product 订品种 cu（CTP 适配层已保证只来主力 bar）。
        service.subscribe("direct", "ctp", &["cu2512".to_string()]);
        service.subscribe("product", "ctp", &["cu".to_string()]);
        service.subscribe("other", "ctp", &["rb2611".to_string()]);

        // 推送 sink 只把 bar 入分钟桶，绝不写 BarTable（落盘是另一个无条件 sink 的职责）。
        service
            .push_sink()
            .on_bar(&kline("ctp", "cu2512", 20261024, 60_000));
        // on_bar 不同步发送：同所更晚分钟 bar 形成水位线，下一拍 poll 才关桶出帧。
        service
            .push_sink()
            .on_bar(&kline("ctp", "cu2512", 20261024, 120_000));
        service.poll_push();

        // 两个命中者各收到分钟 60000 的一帧 kline_1m_batch；无关品种不收。
        // 分钟 120000 桶仍在途（未关），本轮不产生第二帧。
        assert_eq!(
            pusher.frames("direct"),
            vec![("kline_1m_batch".to_string(), vec!["cu2512".to_string()])]
        );
        assert_eq!(
            pusher.frames("product"),
            vec![("kline_1m_batch".to_string(), vec!["cu2512".to_string()])]
        );
        assert!(pusher.frames("other").is_empty());
        // 推送 sink 不碰待刷盘窗口。
        assert!(store.bar_table().snapshot_dirty().is_empty());
        assert_eq!(service.dropped_count(), 0);
    }

    #[test]
    fn unreachable_pusher_drops_and_counts_without_blocking() {
        let store = test_store();
        let (service, pusher) = svc(store.clone());
        service.subscribe("dead", "ctp", &["cu2512".to_string()]);
        service.subscribe("alive", "ctp", &["cu2512".to_string()]);
        pusher.fail_for("dead");

        service
            .push_sink()
            .on_bar(&kline("ctp", "cu2512", 20261024, 60_000));
        // 更晚分钟 bar 形成水位线关桶，下一拍 poll 才真正发送：死连接的帧被丢、活连接照常。
        service
            .push_sink()
            .on_bar(&kline("ctp", "cu2512", 20261024, 120_000));
        service.poll_push();

        // 死连接的分钟 60000 帧丢一帧计数 1，但不影响活连接；推送 sink 不落盘。
        // （分钟 120000 桶仍在途，dead 的那一帧尚未发出，故不计入本轮丢弃。）
        assert_eq!(service.dropped_count(), 1);
        assert_eq!(pusher.total(), 1);
        assert_eq!(pusher.symbols("alive"), vec!["cu2512".to_string()]);
        assert!(store.bar_table().snapshot_dirty().is_empty());
    }

    #[test]
    fn push_sink_fans_out_but_never_writes_bar_table() {
        // CTP 路径用 push-only sink：全量落盘由无条件 persistence sink 负责，
        // 这里绝不能再写 BarTable（否则同一根 bar 重复 append）。
        let store = test_store();
        let (service, pusher) = svc(store.clone());
        service.subscribe("direct", "ctp", &["cu2512".to_string()]);

        service
            .push_sink()
            .on_bar(&kline("ctp", "cu2512", 20261024, 60_000));
        // 更晚分钟 bar 关桶后 poll。
        service
            .push_sink()
            .on_bar(&kline("ctp", "cu2512", 20261024, 120_000));
        service.poll_push();

        // 订阅者照常收到分钟 60000 的一个批量帧（帧内含该标的）。
        assert_eq!(
            pusher.frames("direct"),
            vec![("kline_1m_batch".to_string(), vec!["cu2512".to_string()])]
        );
        // 但 BarTable 不被 push sink 写入。
        assert!(store.bar_table().snapshot_dirty().is_empty());
    }

    #[test]
    fn persistence_sink_appends_once_and_does_not_push() {
        let store = test_store();
        let (service, pusher) = svc(store.clone());
        service.subscribe("direct", "ctp", &["cu2512".to_string()]);

        service
            .persistence_sink()
            .on_bar(&kline("ctp", "cu2512", 20261024, 60_000));

        // 仅落待刷盘窗口一次，不触发任何南向推送。
        let k_cu2512 = trading_core::kline::SeriesKey::new("ctp", "cu2512");
        assert_eq!(store.bar_table().coverage(&k_cu2512), 1);
        assert_eq!(store.bar_table().snapshot_dirty().len(), 1);
        assert_eq!(pusher.total(), 0);

        // 另一序列的 bar 再落一条（每根仅 append 一次）。
        service
            .persistence_sink()
            .on_bar(&kline("ctp", "cu2511", 20261024, 60_000));
        assert_eq!(store.bar_table().series_count(), 2);
        assert_eq!(
            store
                .bar_table()
                .coverage(&trading_core::kline::SeriesKey::new("ctp", "cu2511")),
            1
        );

        // 同序列同分钟再来一根：去重覆盖，不增条数，但仍是一根待写 dirty。
        service
            .persistence_sink()
            .on_bar(&kline("ctp", "cu2512", 20261024, 60_000));
        assert_eq!(store.bar_table().coverage(&k_cu2512), 1);
        assert_eq!(store.bar_table().snapshot_dirty().len(), 2);
    }

    #[test]
    fn itype_maps_deribit_to_option_others_futures() {
        let store = test_store();
        let (service, _p) = svc(store.clone());
        assert_eq!(service.itype_for("deribit"), InstrumentType::Option);
        assert_eq!(service.itype_for("binance"), InstrumentType::Futures);
        assert_eq!(service.itype_for("ctp"), InstrumentType::Futures);
        assert_eq!(service.itype_for("nope"), InstrumentType::Futures);
    }

    #[test]
    fn join_flush_without_start_is_safe_noop() {
        // 未起 flush 线程（如无 K线作用域的部署）：join 立即安全返回，不挂死。
        let store = test_store();
        let (service, _p) = svc(store);
        assert!(service.join_flush().is_ok());
        // 重复调用也安全（句柄已 take）。
        assert!(service.join_flush().is_ok());
    }

    #[test]
    fn spawned_flush_thread_is_joined_after_stop() {
        use std::sync::atomic::AtomicBool;
        let store = test_store();
        let (service, _p) = svc(store.clone());
        let stop = Arc::new(AtomicBool::new(false));
        // 直接起 flush 线程（短间隔），句柄应被留存。
        service.spawn_flush(Duration::from_millis(20), Arc::clone(&stop));

        // 预置一根 bar，停机收尾时线程做最后一次 flush_once（join 返回即保证已执行）。
        service
            .persistence_sink()
            .on_bar(&kline("ctp", "cu2512", 20261024, 60_000));
        stop.store(true, Ordering::SeqCst);

        // join 必须正常返回（线程看到 stop 后 break + 最后一刷），不挂死。
        assert!(service.join_flush().is_ok());
        // join 的 happens-before：返回时最后一次 flush_once 已完成，bar dirty 已确认清空
        //（非破坏式：bar 本体仍保留为可读缓存）。
        assert!(store.bar_table().snapshot_dirty().is_empty());
        assert_eq!(
            store
                .bar_table()
                .coverage(&trading_core::kline::SeriesKey::new("ctp", "cu2512")),
            1
        );
        // 再次 join 安全（句柄只 join 一次）。
        assert!(service.join_flush().is_ok());
    }

    #[test]
    fn spawned_gap_scan_thread_registers_internal_gap_and_joins() {
        use std::sync::atomic::AtomicBool;
        // 极短扫描周期；静默阈值保持正常（3min），避免真实墙钟下把“老测试数据”误判为停推。
        let cfg = GapScanConfig::new(KLINE_STEP_MS, 3 * KLINE_STEP_MS, 20).unwrap();
        let store = test_store();
        let pusher = Arc::new(RecPusher::default());
        let service = Arc::new(KlineService::new(
            Arc::clone(&store),
            pusher as Arc<dyn KlinePusher>,
            Vec::new(),
            default_limits(),
            cfg,
            test_backfill_cfg(),
        ));
        // 数据锚定在“当下”：now-2min 与 now（连续无静默），中间缺 now-1min（内部缺口）。
        let now = now_utc_ms();
        let floor = now - now.rem_euclid(KLINE_STEP_MS);
        service
            .bars
            .upsert(crate_bar("binance", "BTCUSDT", floor - 2 * KLINE_STEP_MS));
        service.bars.upsert(crate_bar("binance", "BTCUSDT", floor));

        let stop = Arc::new(AtomicBool::new(false));
        service.spawn_gap_scan(Arc::clone(&stop));
        // 等扫描线程至少拍一次（轮询，避免睡死）。
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while store.gap_registry().is_empty() && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        stop.store(true, Ordering::SeqCst);

        // join 正常收口；内部缺口（floor-1min）被周期扫描登记。
        assert!(service.join_flush().is_ok());
        let got: Vec<i64> = store
            .gap_registry()
            .snapshot()
            .iter()
            .map(|g| g.start)
            .collect();
        assert!(
            got.contains(&(floor - KLINE_STEP_MS)),
            "周期扫描应登记缺失分钟 {got:?}"
        );
        assert!(service.join_flush().is_ok()); // 重复 join 安全
    }

    #[test]
    fn zero_scan_interval_does_not_require_gap_thread_join() {
        // scan=0（默认测试 cfg）：不 spawn gap 线程，join 仍是安全 no-op，不影响 flush/push。
        let store = test_store();
        let (service, _p) = svc(store);
        assert!(service.gap_task.lock().unwrap().is_none());
        assert!(service.join_flush().is_ok());
    }

    #[test]
    fn deribit_option_bar_routes_independently_of_futures_scope() {
        // deribit 是期权作用域：subscribe 入 option key，option bar 命中，
        // 且不与期货作用域串扰（venue+itype 双键隔离）。
        let store = test_store();
        let (service, pusher) = svc(store.clone());
        service.subscribe("opt", "deribit", &["ETH-18SEP26-2550-P".to_string()]);

        // 期望来源：option 作用域有该合约；同所 futures 作用域为空。
        assert_eq!(
            service
                .source_for("deribit", InstrumentType::Option)
                .symbols(),
            vec!["ETH-18SEP26-2550-P".to_string()]
        );
        assert!(service
            .source_for("deribit", InstrumentType::Futures)
            .symbols()
            .is_empty());

        // 交易所定稿一根 deribit option bar（interval 仍为 1m）。
        let mut bar = kline("deribit", "ETH-18SEP26-2550-P", 20260918, 60_000);
        // deribit 走期权类别；构造 helper 默认即可，venue/symbol 已对。
        bar.interval = KlineInterval::M1;
        service.push_sink().on_bar(&bar);
        // 无更晚分钟 bar 驱动水位线：停机 flush 关闭在途桶后出帧。
        service.flush_remaining_push();

        assert_eq!(
            pusher.frames("opt"),
            vec![(
                "kline_1m_batch".to_string(),
                vec!["ETH-18SEP26-2550-P".to_string()]
            )]
        );
    }

    #[test]
    fn push_ticker_emits_on_watermark_and_flushes_remaining_on_shutdown() {
        use std::sync::atomic::AtomicBool;
        let store = test_store();
        let (service, pusher) = svc(store.clone());
        // alpha 订 binance 两标的；beta 订 ctp 一标的（不同所/策略不串帧）。
        service.subscribe(
            "alpha",
            "binance",
            &["BTCUSDT".to_string(), "ETHUSDT".to_string()],
        );
        service.subscribe("beta", "ctp", &["cu2512".to_string()]);
        let stop = Arc::new(AtomicBool::new(false));
        service.spawn_push_ticker(Arc::clone(&stop));

        // 分钟 60000：alpha 同所两标的（乱序、模拟 ~880ms 散布）+ beta 一所一标的。
        service
            .push_sink()
            .on_bar(&kline("binance", "ETHUSDT", 20261027, 60_000));
        service
            .push_sink()
            .on_bar(&kline("binance", "BTCUSDT", 20261027, 60_000));
        service
            .push_sink()
            .on_bar(&kline("ctp", "cu2512", 20261027, 60_000));

        // 各 (strategy,venue) 再来一根更晚分钟 bar → 有序水位线把分钟 60000 桶关闭，
        // 后台线程下一拍（≤200ms）发出：alpha 1 帧含两标的、beta 1 帧含一标的。
        service
            .push_sink()
            .on_bar(&kline("binance", "BTCUSDT", 20261027, 120_000));
        service
            .push_sink()
            .on_bar(&kline("ctp", "cu2512", 20261027, 120_000));

        // 轮询等待后台线程拍出（≤约 1s）。
        let mut deadline = 0u32;
        while pusher.total() < 2 && deadline < 50 {
            thread::sleep(Duration::from_millis(20));
            deadline += 1;
        }
        assert_eq!(
            pusher.frames("alpha"),
            vec![(
                "kline_1m_batch".to_string(),
                vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
            )]
        );
        assert_eq!(
            pusher.frames("beta"),
            vec![("kline_1m_batch".to_string(), vec!["cu2512".to_string()])]
        );

        // 置 stop + join：停机 flush 关闭分钟 120000 在途桶，最后缓冲也必须推出（不漏不重）。
        stop.store(true, Ordering::SeqCst);
        assert!(service.join_flush().is_ok());

        // alpha 现有 2 帧：首帧（分钟60000）两标的，停机收尾帧（分钟120000）单标的。
        assert_eq!(
            pusher.frames("alpha"),
            vec![
                (
                    "kline_1m_batch".to_string(),
                    vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
                ),
                ("kline_1m_batch".to_string(), vec!["BTCUSDT".to_string()]),
            ]
        );
        // beta 分钟60000/120000 各一帧（cu2512），与 binance 帧互不影响。
        assert_eq!(
            pusher.frames("beta"),
            vec![
                ("kline_1m_batch".to_string(), vec!["cu2512".to_string()]),
                ("kline_1m_batch".to_string(), vec!["cu2512".to_string()]),
            ]
        );
        assert_eq!(pusher.total(), 4);
        // 缓冲已空：join 后再 poll/flush 无新增。
        service.poll_push();
        assert_eq!(pusher.total(), 4);
        // 再次 join 安全（push/flush 句柄都只 join 一次）。
        assert!(service.join_flush().is_ok());
    }

    #[test]
    fn multi_symbol_same_minute_collapses_into_one_batch_frame_per_strategy() {
        // 核心需求：同所同分钟多标的即便散布到齐，关桶前也并入**同一帧**；每策略 1 条。
        let store = test_store();
        let (service, pusher) = svc(store.clone());
        // 两个策略都订阅 BTC/ETH（具体合约）。
        for who in ["alpha", "beta"] {
            service.subscribe(
                who,
                "binance",
                &["BTCUSDT".to_string(), "ETHUSDT".to_string()],
            );
        }
        // 无关策略只订别的所，不应收到 binance 帧。
        service.subscribe("gamma", "ctp", &["cu2512".to_string()]);

        // 同分钟（60000）乱序定稿两根 binance bar（模拟相邻 dispatch、~880ms 散布）。
        service
            .push_sink()
            .on_bar(&kline("binance", "ETHUSDT", 20261027, 60_000));
        service
            .push_sink()
            .on_bar(&kline("binance", "BTCUSDT", 20261027, 60_000));
        // 关桶前：on_bar 只入分钟桶，零发送。
        assert_eq!(pusher.total(), 0);

        // 更晚分钟 bar 形成水位线关闭分钟 60000 桶，下一拍 poll 出帧。
        service
            .push_sink()
            .on_bar(&kline("binance", "BTCUSDT", 20261027, 120_000));
        service.poll_push();

        // 每个命中策略恰好 1 帧，帧内两标的按 symbol 排序。
        let expected = vec![(
            "kline_1m_batch".to_string(),
            vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
        )];
        assert_eq!(pusher.frames("alpha"), expected);
        assert_eq!(pusher.frames("beta"), expected);
        assert!(pusher.frames("gamma").is_empty());
        assert_eq!(pusher.total(), 2);
        // 分钟 120000 桶仍在途且远未到 8s 超时：再 poll 无新增（不重复、不早发）。
        service.poll_push();
        assert_eq!(pusher.total(), 2);
        assert_eq!(service.dropped_count(), 0);
    }

    // ---- 缺口检测点 1：持久化 sink 实时尾检（仅三家） --------------------

    #[test]
    fn persistence_sink_tail_detects_gap_for_supported_venue() {
        let store = test_store();
        let (svc, _pusher) = svc(store.clone());
        let sink = svc.persistence_sink();

        // Arrange：先到第 0 分钟。
        sink.on_bar(&kline("binance", "BTCUSDT", 20261029, 0));
        assert!(svc.gaps.is_empty());

        // Act：第 3 分钟才到（中间缺 1、2 分钟）。
        sink.on_bar(&kline("binance", "BTCUSDT", 20261029, 180_000));

        // Assert：缺口逐分钟登记，且 bar 已正常落表。
        let got: Vec<(String, i64)> = svc
            .gaps
            .snapshot()
            .iter()
            .map(|g| (g.symbol.clone(), g.start))
            .collect();
        assert_eq!(
            got,
            vec![
                ("BTCUSDT".to_string(), 60_000),
                ("BTCUSDT".to_string(), 120_000)
            ]
        );
        assert_eq!(
            svc.bars
                .range(&SeriesKey::new("binance", "BTCUSDT"), 0, i64::MAX)
                .len(),
            2
        );
    }

    #[test]
    fn persistence_sink_adjacent_minutes_register_no_gap() {
        let store = test_store();
        let (svc, _pusher) = svc(store.clone());
        let sink = svc.persistence_sink();

        sink.on_bar(&kline("binance", "BTCUSDT", 20261029, 60_000));
        sink.on_bar(&kline("binance", "BTCUSDT", 20261029, 120_000));
        assert!(svc.gaps.is_empty());
    }

    #[test]
    fn persistence_sink_skips_gap_detection_for_ctp_but_still_persists() {
        let store = test_store();
        let (svc, _pusher) = svc(store.clone());
        let sink = svc.persistence_sink();

        // CTP 即便分钟大跳也不登记缺口（午休/夜盘空分钟合法）。
        sink.on_bar(&kline("ctp", "cu2512", 20261029, 0));
        sink.on_bar(&kline("ctp", "cu2512", 20261029, 600_000));

        assert!(svc.gaps.is_empty());
        // 但 bar 仍无条件落表（四家统一持久化）。
        assert_eq!(
            svc.bars
                .range(&SeriesKey::new("ctp", "cu2512"), 0, i64::MAX)
                .len(),
            2
        );
    }

    #[test]
    fn persistence_sink_tail_gap_is_idempotent_on_duplicate_signal() {
        let store = test_store();
        let (svc, _pusher) = svc(store.clone());
        let sink = svc.persistence_sink();

        sink.on_bar(&kline("hyperliquid", "BTC", 20261029, 0));
        sink.on_bar(&kline("hyperliquid", "BTC", 20261029, 120_000));
        assert_eq!(svc.gaps.len(), 1); // 缺第 1 分钟
                                       // 同分钟定稿帧重复/修正：upsert 旧末键为 120_000，不再造缺口。
        sink.on_bar(&kline("hyperliquid", "BTC", 20261029, 120_000));
        assert_eq!(svc.gaps.len(), 1);
    }
}
