//! KlineService：K线订阅的**服务层编排**（venue-neutral）。
//!
//! 对四家交易所走完全一致的一条代码路径——本类型不聚合 tick、不认品种前缀、不选
//! 主力（全部在 CTP 适配层内完成），只负责：
//! 1. 启动时对每个活跃且支持行情流的 (venue, itype) 调一次
//!    [`MarketDataStream::subscribe_klines`]：来源＝客户端订阅登记表的作用域快照，
//!    sink＝把定稿 bar 写入 [`BarTable`]；订阅凭证由本类型持有，与进程同寿。
//! 2. 起独立线程按固定间隔把 [`BarTable`]/CTP [`TickTable`] 经 [`KlineRepo`]
//!    批量 append 到 CSV（行情线程绝不碰文件），停机再刷一次兜底。
//!
//! 客户端运行时订阅（K2）只改 [`KlineRegistry`]，交易所侧下轮 500ms 对账自动
//! 增订/退订，本类型不重复调用 subscribe。

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use trading_core::kline::{BarTable, KlineRegistry, RegistrySource, TickTable};
use trading_core::protocol::{ClientEvent, KlineEvent};
use trading_exchange::market::{
    BarSink, DesiredSource, Kline as StreamKline, MarketDataStream, SubscriptionGuard,
};
use trading_exchange::registry::ExchangeRegistry;
use trading_models::{Bar, InstrumentType};
use trading_store::{KlineFlushReport, KlineRepo, TradingStore};

/// 本期唯一 K线周期标识（南向协议 / 登记表 / CSV 目录同口径）。
pub const KLINE_INTERVAL: &str = "1m";

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
fn bar_from_kline(k: &StreamKline) -> Bar {
    Bar {
        exchange: k.venue.clone(),
        symbol: k.symbol.clone(),
        trading_day: k.trading_day,
        open: k.open,
        high: k.high,
        low: k.low,
        close: k.close,
        volume: k.volume,
        turnover: k.turnover,
        open_interest: k.open_interest,
        start_time: k.start_ms,
        end_time: k.end_ms,
    }
}

/// 把定稿 bar 映射为南向 `kline_1m` 载荷（不产出成交额/持仓的所给 None）。
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
        turnover: Some(k.turnover).filter(|d| *d != rust_decimal::Decimal::ZERO),
        open_interest: Some(k.open_interest).filter(|d| *d != rust_decimal::Decimal::ZERO),
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
                tracing::warn!(strategy = %name, "kline_1m 推送失败（连接不可达），丢弃该帧");
            }
        });
        true
    }
}

/// 定稿 bar → BarTable + 订阅者 fan-out 的 sink（纯内存、同步廉价，满足 BarSink 契约）。
struct TableBarSink {
    bars: Arc<BarTable>,
    registry: Arc<KlineRegistry>,
    pusher: Arc<dyn KlinePusher>,
    /// 慢/不可达连接丢弃计数（service 级共享，观测用）。
    dropped: Arc<AtomicU64>,
}

impl BarSink for TableBarSink {
    fn on_bar(&self, bar: &StreamKline) {
        // 1) 落待刷盘窗口（持久化与推送解耦）。
        self.bars.push(bar_from_kline(bar));
        // 2) 查订阅者（精确/品种前缀），逐连接入既有发送队列，满/断即丢不阻塞。
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
            "定稿 bar fan-out"
        );
        for strategy in targets {
            if !self
                .pusher
                .push(&strategy, ClientEvent::kline_1m(kline_event(bar)))
            {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// 把 core [`RegistrySource`] 薄适配为交易所层 [`DesiredSource`]（避免 core 依赖 exchange）。
struct RegistryDesired {
    source: RegistrySource,
}

impl DesiredSource for RegistryDesired {
    fn symbols(&self) -> Vec<String> {
        self.source.symbols()
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
    /// 慢/不可达连接丢弃的 bar 帧总数（观测）。
    dropped: Arc<AtomicU64>,
    /// 每个作用域一个 subscribe_klines 凭证（与进程同寿；drop 即整体退订）。
    guards: Mutex<Vec<SubscriptionGuard>>,
}

impl KlineService {
    /// 从 TradingStore 取齐五表/repo 句柄构造，注入南向推送端口（内部全是进程单例）。
    pub fn new(store: Arc<TradingStore>, pusher: Arc<dyn KlinePusher>) -> Self {
        Self {
            registry: store.kline_registry(),
            bars: store.bar_table(),
            ticks: store.ctp_ticks(),
            repo: store.kline_repo(),
            pusher,
            dropped: Arc::new(AtomicU64::new(0)),
            guards: Mutex::new(Vec::new()),
        }
    }

    /// 某 venue 的标的类别（deribit=option，其余本期=futures；未知默认 futures）。
    pub fn itype_for(&self, venue: &str) -> InstrumentType {
        venue_itype(venue)
    }

    /// 某作用域的期望来源视图（登记表驱动；attach 与测试共用）。
    pub fn source_for(&self, venue: &str, itype: InstrumentType) -> RegistrySource {
        RegistrySource::new(
            Arc::clone(&self.registry),
            venue.to_string(),
            itype,
            KLINE_INTERVAL.to_string(),
        )
    }

    /// 定稿 bar sink（写 BarTable + 订阅者 fan-out；各 sink 共享丢弃计数）。
    pub fn bar_sink(&self) -> Arc<dyn BarSink> {
        Arc::new(TableBarSink {
            bars: Arc::clone(&self.bars),
            registry: Arc::clone(&self.registry),
            pusher: Arc::clone(&self.pusher),
            dropped: Arc::clone(&self.dropped),
        })
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
        let desired = Arc::new(RegistryDesired {
            source: self.source_for(scope.venue, scope.itype),
        }) as Arc<dyn DesiredSource>;
        let guard = stream.subscribe_klines(desired, self.bar_sink())?;
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
        self.spawn_flush(flush_interval, stop);
    }

    /// 独立 OS 线程定时刷盘（同步 IO 不进 tokio worker）；停机被打断后最后再刷一次。
    fn spawn_flush(self: &Arc<Self>, interval: Duration, stop: Arc<AtomicBool>) {
        let me = Arc::clone(self);
        thread::Builder::new()
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
                        tracing::warn!(dropped, "kline_1m 推送丢弃（连接不可达/在途积压）");
                    }
                    // 刷盘窗口背压丢弃（flush 持续失败致 BarTable 积压超上限）。
                    let bar_dropped = me.bars.take_dropped();
                    if bar_dropped > 0 {
                        tracing::error!(
                            dropped = bar_dropped,
                            "定稿 bar 因刷盘窗口积压超上限被丢弃（请检查 CSV 落盘/磁盘）"
                        );
                    }
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
            turnover: Decimal::from(33),
            open_interest: Decimal::from(100),
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

    /// 记录推送、可模拟失败的假推送端口。
    #[derive(Default)]
    struct RecPusher {
        sent: Mutex<Vec<(String, String, String)>>,
        fail: Mutex<std::collections::HashSet<String>>,
    }
    impl RecPusher {
        fn events(&self, strategy: &str) -> Vec<(String, String)> {
            self.sent
                .lock()
                .unwrap()
                .iter()
                .filter(|(s, _, _)| s == strategy)
                .map(|(_, kind, sym)| (kind.clone(), sym.clone()))
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
            let sym = event
                .kline
                .as_ref()
                .map(|k| k.symbol.clone())
                .unwrap_or_default();
            self.sent
                .lock()
                .unwrap()
                .push((strategy.to_string(), event.kind.clone(), sym));
            true
        }
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

    fn svc(store: Arc<TradingStore>) -> (Arc<KlineService>, Arc<RecPusher>) {
        let pusher = Arc::new(RecPusher::default());
        let s = Arc::new(KlineService::new(
            store,
            Arc::clone(&pusher) as Arc<dyn KlinePusher>,
        ));
        (s, pusher)
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
            .bar_sink()
            .on_bar(&kline("ctp", "cu2512", 20261024, 60_000));

        let pending = store.bar_table().snapshot();
        assert_eq!(pending.len(), 1);
        let b = &pending[0];
        assert_eq!(b.exchange, "ctp");
        assert_eq!(b.symbol, "cu2512");
        assert_eq!(b.trading_day, 20261024);
        assert_eq!(b.start_time, 60_000);
        assert_eq!(b.end_time, 120_000);
        assert_eq!(b.volume, 3);
        assert_eq!(b.open_interest, Decimal::from(100));
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
            .bar_sink()
            .on_bar(&kline("ctp", "cu2512", 20261024, 60_000));
        store.ctp_ticks().append(trading_models::RawTick {
            exchange: "ctp".into(),
            symbol: "cu2512".into(),
            timestamp: 60_000,
            trading_day: 20261024,
            price: Decimal::from(11),
            open: Decimal::ZERO,
            high: Decimal::ZERO,
            low: Decimal::ZERO,
            close: Decimal::ZERO,
            volume: 3,
            turnover: Decimal::from(33),
            open_interest: Decimal::from(100),
            bid: [Decimal::ZERO; 5],
            ask: [Decimal::ZERO; 5],
            bid_vol: [0; 5],
            ask_vol: [0; 5],
        });

        let report = service.flush_once();

        assert_eq!(report.bars.rows, 1);
        assert_eq!(report.ticks.rows, 1);
        assert!(store.bar_table().is_empty());
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

        service
            .bar_sink()
            .on_bar(&kline("ctp", "cu2512", 20261024, 60_000));

        // 两个命中者各收到一帧 kline_1m；无关品种不收。
        assert_eq!(
            pusher.events("direct"),
            vec![("kline_1m".to_string(), "cu2512".to_string())]
        );
        assert_eq!(
            pusher.events("product"),
            vec![("kline_1m".to_string(), "cu2512".to_string())]
        );
        assert!(pusher.events("other").is_empty());
        // bar 仍照常落待刷盘窗口（一条）。
        assert_eq!(store.bar_table().len(), 1);
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
            .bar_sink()
            .on_bar(&kline("ctp", "cu2512", 20261024, 60_000));

        // 死连接丢一帧计数 1，但不影响活连接与落盘。
        assert_eq!(service.dropped_count(), 1);
        assert_eq!(pusher.total(), 1);
        assert_eq!(store.bar_table().len(), 1);
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
        service.bar_sink().on_bar(&bar);

        assert_eq!(
            pusher.events("opt"),
            vec![("kline_1m".to_string(), "ETH-18SEP26-2550-P".to_string())]
        );
    }
}
