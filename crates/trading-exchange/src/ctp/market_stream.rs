//! CTP 单连接统一行情流（[`market::MarketDataStream`] 实现）。
//!
//! 一个进程单例，被业务 `ctp-md` OS 线程与按需 K线消费者共用：
//! - 注册两类期望来源：`subscribe_prices`（系统行情，tick 由 drain 统一写 QuoteTable，
//!   本类型只贡献订阅符号）与 `subscribe_klines`（额外持定稿回调）；
//! - [`Self::price_symbols`] / [`Self::bar_symbols`] 供 drain 计算「业务兴趣 ∪ K线
//!   期望（品种经 [`InstrumentDir`] 展开为全部 is_trading 合约）」并集向柜台订阅；
//! - [`Self::on_kline_tick`]：K线集 tick 喂入 [`BarAggSink`]，分钟翻转定稿后按注册表
//!   路由（catch_unwind+计时摘除）；原始 tick/累计量写三表在同路径完成；
//! - [`Self::elect_cycle`]：收盘选举纯内核（量最大/tiebreak/沿用前日/每日只更新一次），
//!   由 [`Self::spawn_election`] 独立线程在日盘收盘后触发。
//!
//! 全程 `std::sync::Mutex`（OS 线程，非 async）；锁内不调用外部 sink（先收集命中
//! 列表，再在 [`market_hub::safe_notify`] 隔离下回调）。

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rust_decimal::Decimal;
use trading_core::kline::{DominantEntry, DominantTable, LastVolumeTable, TickTable};
use trading_models::RawTick;

use crate::ctp::bar_agg::BarAggSink;
use crate::ctp::candle::CandleTick;
use crate::ctp::dominant::{pick_dominant, product_prefix, ContractVolume};
use crate::ctp::instrument_dir::InstrumentDir;
use crate::market::{
    BarSink, DesiredSource, Kline, KlineInterval, MarketDataStream, SubscriptionGuard,
};
use crate::market_hub::{safe_notify, NotifyOutcome};

/// 落 RawTick 时除聚合三量外的盘口/日 OHLC 字段（CandleTick 保持精简，1m 聚合不用它们）。
///
/// 这些是 CTP **当日累计**日开高低收（盘中 close 常为 0）与五档量价，**不是**
/// 1m bar 的 OHLC；1m OHLC 由 candle 聚合得到。`Default` 全 0（FFI 字段缺失/测试用）。
#[derive(Debug, Clone, Copy, Default)]
pub struct RawTickDepth {
    pub day_open: Decimal,
    pub day_high: Decimal,
    pub day_low: Decimal,
    pub day_close: Decimal,
    pub bid: [Decimal; 5],
    pub ask: [Decimal; 5],
    pub bid_vol: [i32; 5],
    pub ask_vol: [i32; 5],
}

impl From<&crate::ctp::core::quote::TickSnapshot> for RawTickDepth {
    fn from(t: &crate::ctp::core::quote::TickSnapshot) -> Self {
        let dec = |p: f64| Decimal::try_from(p).unwrap_or(Decimal::ZERO);
        Self {
            day_open: dec(t.day_open),
            day_high: dec(t.day_high),
            day_low: dec(t.day_low),
            day_close: dec(t.day_close),
            bid: t.bids.map(|l| dec(l.price)),
            ask: t.asks.map(|l| dec(l.price)),
            bid_vol: t.bids.map(|l| l.volume),
            ask_vol: t.asks.map(|l| l.volume),
        }
    }
}

/// bar sink 单次回调摘除阈值（目标 <1µs，1ms 兜底）。
const BAR_BUDGET: Duration = Duration::from_millis(1);

/// CTP 的 venue 标识（落盘行与南向路由口径一致）。
const CTP_VENUE: &str = "ctp";

struct PriceClient {
    id: u64,
    source: Arc<dyn DesiredSource>,
}

struct BarClient {
    id: u64,
    source: Arc<dyn DesiredSource>,
    sink: Arc<dyn BarSink>,
}

/// bar 定稿路由器：bar 到达时分发给期望集命中的 K线消费者，坏/慢者摘除。
/// 同时作为 [`BarAggSink`] 的下游 [`BarSink`]。
///
/// 主力过滤在此按**每个消费者的订阅意图**判定：精确订阅某合约（如 cu2611）无条件
/// 收它的 bar（即便不是主力）；仅订阅品种（如 cu）时，才要求该 bar 合约是当前主力。
struct BarRouter {
    next_id: Mutex<u64>,
    clients: Mutex<Vec<BarClient>>,
    /// 主力表（品种 → 当前主力），仅对「品种」订阅做过滤；具体合约订阅不查它。
    dominants: Arc<DominantTable>,
}

impl BarRouter {
    fn new(dominants: Arc<DominantTable>) -> Self {
        Self {
            next_id: Mutex::new(0),
            clients: Mutex::new(Vec::new()),
            dominants,
        }
    }

    fn add(&self, source: Arc<dyn DesiredSource>, sink: Arc<dyn BarSink>) -> u64 {
        let mut idg = self.next_id.lock().unwrap();
        *idg += 1;
        let id = *idg;
        drop(idg);
        self.clients
            .lock()
            .unwrap()
            .push(BarClient { id, source, sink });
        id
    }

    fn remove(&self, id: u64) {
        self.clients.lock().unwrap().retain(|c| c.id != id);
    }

    /// 某消费者是否应收该 bar：
    /// 1) 精确订阅该合约（source 含合约号）→ 无条件放行（具体合约订阅直收，不查主力）；
    /// 2) 仅订阅了该合约所属**品种**（source 含 cu 这样的前缀）→ 合约必须是当前主力，
    ///    无主力记录则暂不放行（等收盘选举/重启沿用值）。
    fn client_accepts(
        source_symbols: &HashSet<String>,
        symbol: &str,
        dominants: &DominantTable,
    ) -> bool {
        if source_symbols.contains(symbol) {
            return true;
        }
        if let Some(product) = product_prefix(symbol) {
            if source_symbols.contains(&product) {
                return dominants
                    .dominant_symbol(&product)
                    .is_some_and(|dom| dom == symbol);
            }
        }
        false
    }

    fn dispatch(&self, bar: &Kline) -> usize {
        // 锁内克隆命中 sink 列表，释锁后再回调（不持锁调外部代码）。
        let targets: Vec<(u64, Arc<dyn BarSink>)> = {
            let clients = self.clients.lock().unwrap();
            clients
                .iter()
                .filter(|c| {
                    let syms: HashSet<String> = c.source.symbols().into_iter().collect();
                    Self::client_accepts(&syms, &bar.symbol, &self.dominants)
                })
                .map(|c| (c.id, Arc::clone(&c.sink)))
                .collect()
        };
        let mut evicted = Vec::new();
        for (id, sink) in targets {
            match safe_notify(sink.as_ref(), bar, BAR_BUDGET) {
                NotifyOutcome::Ok => {}
                _ => evicted.push(id),
            }
        }
        if !evicted.is_empty() {
            self.clients
                .lock()
                .unwrap()
                .retain(|c| !evicted.contains(&c.id));
        }
        evicted.len()
    }
}

impl BarSink for BarRouter {
    fn on_bar(&self, bar: &Kline) {
        self.dispatch(bar);
    }
}

/// CTP 统一行情流单例。
pub struct CtpMarketStream {
    next_price_id: Mutex<u64>,
    price_clients: Arc<Mutex<Vec<PriceClient>>>,
    router: Arc<BarRouter>,
    /// 多合约聚合器（router 为定稿下游）。
    bar_agg: Mutex<BarAggSink>,
    /// 三表（drain 写入；聚合只读 dominant，选举读 last_volume、写 dominant）。
    ticks: Arc<TickTable>,
    last_volumes: Arc<LastVolumeTable>,
    dominants: Arc<DominantTable>,
    /// 合约目录（品种展开 / product 分组）。
    instrument_dir: Arc<InstrumentDir>,
    /// 配置基线 K线品种（venue.toml `[ctp].kline_products`，如 cu/rb）：
    /// 与客户端运行时订阅取并集展开为在交易合约，默认就采集，客户端退订不移除。
    kline_products: Vec<String>,
}

impl CtpMarketStream {
    pub fn new(
        ticks: Arc<TickTable>,
        last_volumes: Arc<LastVolumeTable>,
        dominants: Arc<DominantTable>,
        instrument_dir: Arc<InstrumentDir>,
        kline_products: Vec<String>,
    ) -> Arc<Self> {
        let router = Arc::new(BarRouter::new(Arc::clone(&dominants)));
        let bar_agg = BarAggSink::new("ctp", router.clone() as Arc<dyn BarSink>, KlineInterval::M1);
        Arc::new(Self {
            next_price_id: Mutex::new(0),
            price_clients: Arc::new(Mutex::new(Vec::new())),
            router,
            bar_agg: Mutex::new(bar_agg),
            ticks,
            last_volumes,
            dominants,
            instrument_dir,
            kline_products,
        })
    }

    /// 系统行情期望符号（去重有序）。
    pub fn price_symbols(&self) -> BTreeSet<String> {
        self.price_clients
            .lock()
            .unwrap()
            .iter()
            .flat_map(|c| c.source.symbols())
            .collect()
    }

    /// K线期望展开后的具体合约：显式合约 + 品种经目录展开的全部 is_trading 合约。
    /// 品种前缀有两个来源并集：①配置基线 `kline_products`（默认采集）；
    /// ②客户端运行时订阅的符号（具体合约直收，品种前缀展开）。
    ///
    /// 区分约定（与 `product_prefix` 同口径）：**带尾部数字的符号视为具体合约**
    /// （如 cu2512）——无条件直收，不依赖合约目录是否预热（CTP 接受任意合约号，
    /// 订错由订阅应答拒绝并告警）；**纯字母符号视为品种**（如 cu），须查合约目录
    /// 展开为该品种全部在交易合约，目录未就绪时本轮暂不产出（连接保持、下拍补）。
    pub fn bar_symbols(&self) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        let mut products: HashSet<String> = self.kline_products.iter().cloned().collect();
        let clients = self.router.clients.lock().unwrap();
        for c in clients.iter() {
            for raw in c.source.symbols() {
                if is_specific_contract(&raw) {
                    // 具体合约号（带尾部数字）：直收，不阻塞于合约目录预热。
                    out.insert(raw);
                } else {
                    // 纯字母品种：查目录展开。
                    products.insert(raw);
                }
            }
        }
        drop(clients);
        for sym in self.instrument_dir.trading_instruments_for(&products) {
            out.insert(sym);
        }
        out
    }

    /// 是否存在任何 K线需求（配置基线品种非空，或有 bar 客户端）。
    /// 用于 drain 对账时区分「真空闲」与「合约目录尚未预热导致展开暂空」。
    pub fn has_kline_demand(&self) -> bool {
        !self.kline_products.is_empty() || !self.router.clients.lock().unwrap().is_empty()
    }

    /// drain 在 K线集 tick 上调用：原始 tick（含日 OHLC/五档）入 WAL 待刷盘窗口、
    /// 覆盖最新累计量、并喂聚合器。（QuoteTable/observer 的业务路径在 drain 内单独完成。）
    ///
    /// `depth` 为日 OHLC + 五档（FFI 全透传）；K线聚合只依赖 `CandleTick` 的
    /// price/累计量额/持仓，日 OHLC/五档仅落 RawTick。
    pub fn on_kline_tick(&self, symbol: &str, t: &CandleTick, depth: &RawTickDepth) {
        self.ticks.append(RawTick {
            exchange: CTP_VENUE.to_string(),
            symbol: symbol.to_string(),
            timestamp: t.update_ms,
            trading_day: t.trading_day,
            price: t.price,
            open: depth.day_open,
            high: depth.day_high,
            low: depth.day_low,
            close: depth.day_close,
            volume: t.cum_volume,
            turnover: t.cum_turnover,
            open_interest: t.open_interest,
            bid: depth.bid,
            ask: depth.ask,
            bid_vol: depth.bid_vol,
            ask_vol: depth.ask_vol,
        });
        self.last_volumes.put(symbol, t.cum_volume);
        self.bar_agg.lock().unwrap().on_tick(symbol, t);
    }

    /// 注册全量持久化 sink：此后**每根定稿 bar（含非主力、零南向订阅）**都不经
    /// 订阅/主力路由地通知它（用于「有 tick 定稿即落 CSV」）。须在行情喂 tick 前
    /// 注册一次；panic/慢 sink 在聚合层被隔离摘除，不影响行情连接与南向路由。
    pub fn register_persistent_bar_sink(&self, sink: Arc<dyn BarSink>) {
        self.bar_agg.lock().unwrap().add_unconditional_sink(sink);
    }

    /// 强制定稿所有未闭桶并把 bar 同时送全量持久化 sink 与南向路由（优雅停机收口用）。
    /// 重连场景**不得**调用（聚合器随单例跨会话存活，重连 flush 会产生重复 bar）。
    pub fn flush_all(&self) -> Vec<Kline> {
        self.bar_agg.lock().unwrap().flush_all()
    }

    /// 墙钟到点 sweep（md 线程每个 poll 节拍调一次，空闲超时拍也调）：把所有已收到过
    /// tick 且到收盘边界（grace=0：start+interval ≤ now）的合约上一周期桶一次性定稿，先落盘再路由。
    ///
    /// 与 [`Self::on_kline_tick`] 内的事件时间墙钟是同一判定——稀疏成交也由每拍 sweep 到边界定稿；
    /// 两者对同一在途桶 `Option::take`，最多定稿一次。`now_ms` 由调用方按 SystemTime 注入，
    /// 判定本身是纯函数，便于单测；锁内只做内存取桶/聚合（守 `BarSink::on_bar` 同步廉价契约）。
    pub fn sweep_kline_finalized(&self, now_ms: i64) -> Vec<Kline> {
        self.bar_agg.lock().unwrap().sweep_finalized(now_ms)
    }

    /// 收盘选举一轮：按合约所属 product（目录 product_id，缺失则剥尾数字）分组，
    /// 量最大/tiebreak 选主力；该 product 当日已选（dominant.trading_day==day）则跳过
    /// （每日只更新一次，盘中冻结）；当日无 tick 的 product 不产出（沿用旧值）。
    pub fn elect_cycle(&self, trading_day: u32) -> usize {
        let mut by_product: HashMap<String, Vec<ContractVolume>> = HashMap::new();
        for (symbol, vol) in self.last_volumes.snapshot() {
            let product = self
                .instrument_dir
                .get(&symbol)
                .map(|i| i.product_id)
                .filter(|p| !p.is_empty())
                .or_else(|| product_prefix(&symbol));
            if let Some(product) = product {
                by_product.entry(product).or_default().push(ContractVolume {
                    symbol,
                    cum_volume: vol,
                });
            }
        }
        let mut updated = 0;
        for (product, vols) in by_product {
            if let Some(cur) = self.dominants.get(&product) {
                if cur.trading_day == trading_day {
                    continue; // 当日冻结
                }
            }
            if let Some(symbol) = pick_dominant(&vols) {
                let cum_volume = vols
                    .iter()
                    .find(|c| c.symbol == symbol)
                    .map(|c| c.cum_volume)
                    .unwrap_or(0);
                self.dominants.put(
                    &product,
                    DominantEntry {
                        symbol,
                        trading_day,
                        cum_volume,
                    },
                );
                updated += 1;
            }
        }
        updated
    }

    /// 起独立线程做收盘选举：每分钟检查北京 wall-clock 是否进入日盘收盘后窗口
    /// （≥15:00），当天首次进入用当日累计量选一次（elect_cycle 内部按交易日防重）。
    pub fn spawn_election(self: &Arc<Self>, stop: Arc<AtomicBool>) {
        let me = Arc::clone(self);
        std::thread::Builder::new()
            .name("ctp-dominant".into())
            .spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    interruptible_sleep(Duration::from_secs(60), &stop);
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    if let Some(day) = beijing_election_day() {
                        me.elect_cycle(day);
                    }
                }
            })
            .ok();
    }

    /// 冷启动预设主力：把 `["cu:cu2512", ...]` 直接写入主力表（trading_day 取
    /// 北京当日，cum_volume=0）。用于盘中重启/未到 15:00 选举窗口时让品种订阅
    /// 立即只推指定合约；elect_cycle 因该品种当日已选而跳过，下一交易日真实选举接管。
    /// 格式非法/缺冒号的条目忽略并告警，不影响其余条目与行情启动。
    pub fn preset_dominants(&self, pairs: &[String]) -> usize {
        let Some(day) = beijing_trading_day() else {
            return 0;
        };
        let mut applied = 0;
        for raw in pairs {
            let raw = raw.trim();
            let Some((product, symbol)) = raw.split_once(':') else {
                tracing::warn!(entry = %raw, "预设主力格式非法，应为 品种:合约（如 cu:cu2512），忽略");
                continue;
            };
            let (product, symbol) = (product.trim(), symbol.trim());
            if product.is_empty() || symbol.is_empty() {
                tracing::warn!(entry = %raw, "预设主力品种/合约为空，忽略");
                continue;
            }
            // 仅在该品种尚无当日记录时预设，避免覆盖热选举结果。
            if self
                .dominants
                .get(product)
                .is_some_and(|e| e.trading_day == day)
            {
                continue;
            }
            self.dominants.put(
                product,
                DominantEntry {
                    symbol: symbol.to_string(),
                    trading_day: day,
                    cum_volume: 0,
                },
            );
            tracing::info!(product, symbol, trading_day = day, "已预设 CTP 主力合约");
            applied += 1;
        }
        applied
    }
}

/// 60s 睡眠按 1s 分片，可被停机标志提前打断（与 ctp-md 同口径）。
fn interruptible_sleep(total: Duration, stop: &AtomicBool) {
    let mut left = total.as_millis() as u64;
    while left > 0 && !stop.load(Ordering::SeqCst) {
        let step = left.min(1000);
        std::thread::sleep(Duration::from_millis(step));
        left = left.saturating_sub(step);
    }
}

/// 是否为具体合约号（带尾部数字，如 cu2512）；纯字母（cu）视为品种前缀。
fn is_specific_contract(symbol: &str) -> bool {
    symbol.ends_with(|c: char| c.is_ascii_digit())
}

/// 北京时区当前交易日 YYYYMMDD（wall-clock 当日；夜盘归属日由柜台 TradingDay 另管）。
fn beijing_trading_day() -> Option<u32> {
    use chrono::FixedOffset;
    let bj = FixedOffset::east_opt(8 * 3600)?;
    let now = chrono::Utc::now().with_timezone(&bj);
    now.format("%Y%m%d").to_string().parse().ok()
}

/// 北京 wall-clock：仅在 15:00（含）之后返回当日 YYYYMMDD（日盘收盘后选举窗口）。
fn beijing_election_day() -> Option<u32> {
    use chrono::{FixedOffset, Timelike};
    let bj = FixedOffset::east_opt(8 * 3600)?;
    let now = chrono::Utc::now().with_timezone(&bj);
    if now.hour() >= 15 {
        Some(now.format("%Y%m%d").to_string().parse().unwrap_or(0))
    } else {
        None
    }
}

impl MarketDataStream for CtpMarketStream {
    fn subscribe_prices(&self, src: Arc<dyn DesiredSource>) -> anyhow::Result<SubscriptionGuard> {
        let mut idg = self.next_price_id.lock().unwrap();
        *idg += 1;
        let id = *idg;
        drop(idg);
        self.price_clients
            .lock()
            .unwrap()
            .push(PriceClient { id, source: src });
        let clients = Arc::clone(&self.price_clients);
        Ok(SubscriptionGuard::new(Box::new(move || {
            if let Ok(mut v) = clients.lock() {
                v.retain(|c| c.id != id);
            }
        })))
    }

    fn subscribe_klines(
        &self,
        src: Arc<dyn DesiredSource>,
        sink: Arc<dyn BarSink>,
    ) -> anyhow::Result<SubscriptionGuard> {
        let id = self.router.add(src, sink);
        let router = Arc::clone(&self.router);
        Ok(SubscriptionGuard::new(Box::new(move || router.remove(id))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctp::core::event::InstrumentInfo;
    use crate::market::FixedSymbols;
    use rust_decimal::Decimal;

    fn dir_with(infos: Vec<InstrumentInfo>) -> Arc<InstrumentDir> {
        let d = Arc::new(InstrumentDir::new());
        for i in infos {
            d.remember(i);
        }
        d
    }

    fn info(symbol: &str, product: &str, trading: bool) -> InstrumentInfo {
        InstrumentInfo {
            instrument: symbol.into(),
            exchange_id: "SHFE".into(),
            product_id: product.into(),
            is_trading: trading,
            volume_multiple: 5,
            price_tick: 10.0,
            long_margin_ratio: 0.0,
            short_margin_ratio: 0.0,
        }
    }

    fn tables() -> (Arc<TickTable>, Arc<LastVolumeTable>, Arc<DominantTable>) {
        (
            Arc::new(TickTable::new()),
            Arc::new(LastVolumeTable::new()),
            Arc::new(DominantTable::new()),
        )
    }

    fn mk(vol: i64, ms: i64) -> CandleTick {
        CandleTick {
            trading_day: 20261023,
            update_ms: ms,
            price: Decimal::from(100),
            cum_volume: vol,
            cum_turnover: Decimal::ZERO,
            open_interest: Decimal::ZERO,
        }
    }

    #[derive(Default)]
    struct CountSink {
        n: Mutex<usize>,
    }
    impl BarSink for CountSink {
        fn on_bar(&self, _bar: &Kline) {
            *self.n.lock().unwrap() += 1;
        }
    }
    impl CountSink {
        fn count(&self) -> usize {
            *self.n.lock().unwrap()
        }
    }

    struct PanicSink;
    impl BarSink for PanicSink {
        fn on_bar(&self, _bar: &Kline) {
            panic!("boom");
        }
    }

    #[test]
    fn price_subscription_contributes_symbols_and_guard_removes() {
        let (t, v, d) = tables();
        let s = CtpMarketStream::new(t, v, d, dir_with(vec![]), Vec::new());
        assert!(s.price_symbols().is_empty());
        let g = s
            .subscribe_prices(Arc::new(FixedSymbols::new(vec!["rb2510".into()])))
            .unwrap();
        assert_eq!(
            s.price_symbols().into_iter().collect::<Vec<_>>(),
            vec!["rb2510"]
        );
        drop(g);
        assert!(s.price_symbols().is_empty());
    }

    #[test]
    fn product_prefix_expands_to_all_trading_contracts() {
        let (t, v, d) = tables();
        let dir = dir_with(vec![
            info("cu2511", "cu", true),
            info("cu2512", "cu", true),
            info("cu2601", "cu", false), // 摘牌不订
            info("rb2610", "rb", true),
        ]);
        let s = CtpMarketStream::new(t, v, d, dir, Vec::new());
        let _g = s
            .subscribe_klines(
                Arc::new(FixedSymbols::new(vec!["cu".into()])),
                Arc::new(CountSink::default()),
            )
            .unwrap();
        assert_eq!(
            s.bar_symbols().into_iter().collect::<Vec<_>>(),
            vec!["cu2511".to_string(), "cu2512".to_string()]
        );
    }

    #[test]
    fn specific_contract_works_without_instrument_dir_but_product_waits() {
        // 合约目录尚未预热（真机启动竞态）：具体合约 cu2512 必须能直收，
        // 纯品种 cu 因需目录展开而暂不产出（不误订阅，目录就绪后下拍补）。
        let (t, v, d) = tables();
        let empty = Arc::new(InstrumentDir::new());
        let s = CtpMarketStream::new(t, v, d, empty, Vec::new());
        let _g = s
            .subscribe_klines(
                Arc::new(FixedSymbols::new(vec!["cu2512".into(), "cu".into()])),
                Arc::new(CountSink::default()),
            )
            .unwrap();
        assert_eq!(
            s.bar_symbols().into_iter().collect::<Vec<_>>(),
            vec!["cu2512".to_string()]
        );
    }

    #[test]
    fn kline_tick_writes_tables_and_routes_only_dominant() {
        let (ticks, vols, doms) = tables();
        doms.put(
            "cu",
            DominantEntry {
                symbol: "cu2512".into(),
                trading_day: 20261023,
                cum_volume: 0,
            },
        );
        let dir = dir_with(vec![info("cu2511", "cu", true), info("cu2512", "cu", true)]);
        let sink = Arc::new(CountSink::default());
        let s = CtpMarketStream::new(ticks, vols, doms, dir, Vec::new());
        let _g = s
            .subscribe_klines(Arc::new(FixedSymbols::new(vec!["cu".into()])), sink.clone())
            .unwrap();

        // 非主力 cu2511 跨分钟：聚合但不路由。
        s.on_kline_tick("cu2511", &mk(10, 600_000), &RawTickDepth::default());
        s.on_kline_tick("cu2511", &mk(11, 660_000), &RawTickDepth::default());
        // 主力 cu2512 跨分钟：定稿路由 1 根。
        s.on_kline_tick("cu2512", &mk(20, 600_000), &RawTickDepth::default());
        s.on_kline_tick("cu2512", &mk(25, 660_000), &RawTickDepth::default());
        assert_eq!(sink.count(), 1);

        // WAL 每笔留档：cu2511×2（非主力也留）+ cu2512×2，共 4 行待刷盘。
        let pending = s.ticks.snapshot();
        assert_eq!(pending.len(), 4);
        assert_eq!(
            pending
                .iter()
                .map(|r| r.symbol.as_str())
                .collect::<Vec<_>>(),
            vec!["cu2511", "cu2511", "cu2512", "cu2512"]
        );
        assert_eq!(s.last_volumes.get("cu2512"), Some(25));
    }

    #[test]
    fn bar_routing_keeps_exact_but_filters_product_by_dominant() {
        // 纯判定：精确合约订阅不查主力；品种订阅仅放行主力；无主力记录暂不放行。
        let doms = Arc::new(DominantTable::new());
        doms.put(
            "cu",
            DominantEntry {
                symbol: "cu2512".into(),
                trading_day: 20261023,
                cum_volume: 0,
            },
        );
        let want = |syms: &[&str], bar: &str| {
            let set: HashSet<String> = syms.iter().map(|s| s.to_string()).collect();
            BarRouter::client_accepts(&set, bar, &doms)
        };

        // 精确订阅非主力 cu2611：放行。
        assert!(want(&["cu2611"], "cu2611"));
        // 品种订阅 cu：主力 cu2512 放行、非主力 cu2611 拦截。
        assert!(want(&["cu"], "cu2512"));
        assert!(!want(&["cu"], "cu2611"));
        // 完全不相关：拦截。
        assert!(!want(&["rb"], "cu2512"));
        // 未选举品种 rb：品种订阅暂无主力记录，不放行。
        assert!(!want(&["rb"], "rb2611"));
    }

    #[test]
    fn specific_contract_bar_routes_even_when_not_dominant() {
        // cu 的主力是 cu2512；客户端却精确订阅了非主力的具体合约 cu2611。
        // 具体合约订阅应无条件收到它的 bar（主力过滤只针对「品种」订阅）。
        let (ticks, vols, doms) = tables();
        doms.put(
            "cu",
            DominantEntry {
                symbol: "cu2512".into(),
                trading_day: 20261023,
                cum_volume: 0,
            },
        );
        let dir = dir_with(vec![info("cu2512", "cu", true), info("cu2611", "cu", true)]);
        let sink = Arc::new(CountSink::default());
        let s = CtpMarketStream::new(ticks, vols, doms, dir, Vec::new());
        let _g = s
            .subscribe_klines(
                Arc::new(FixedSymbols::new(vec!["cu2611".into()])),
                sink.clone(),
            )
            .unwrap();

        // 非主力 cu2611 跨分钟定稿：具体合约订阅必须收到 1 根。
        s.on_kline_tick("cu2611", &mk(10, 600_000), &RawTickDepth::default());
        s.on_kline_tick("cu2611", &mk(11, 660_000), &RawTickDepth::default());
        assert_eq!(sink.count(), 1);
    }

    #[test]
    fn persistent_sink_receives_non_dominant_bar_without_any_router_client() {
        // 无任何南向订阅（router 客户端为空），合约也非主力：全量持久化 sink 仍须收到定稿 bar。
        let (ticks, vols, doms) = tables();
        let dir = dir_with(vec![info("cu2511", "cu", true), info("cu2512", "cu", true)]);
        doms.put(
            "cu",
            DominantEntry {
                symbol: "cu2512".into(),
                trading_day: 20261023,
                cum_volume: 0,
            },
        );
        let persist = Arc::new(CountSink::default());
        let s = CtpMarketStream::new(ticks, vols, doms, dir, Vec::new());
        s.register_persistent_bar_sink(persist.clone() as Arc<dyn BarSink>);

        // 非主力 cu2511 跨分钟定稿：无 router 客户端，但持久化 sink 收到 1 根。
        s.on_kline_tick("cu2511", &mk(10, 600_000), &RawTickDepth::default());
        s.on_kline_tick("cu2511", &mk(11, 660_000), &RawTickDepth::default());
        assert_eq!(persist.count(), 1);
        // flush 收口 cu2511 在 660_000 开的新桶：持久化 sink 再收 1 根。
        let flushed = s.flush_all();
        assert_eq!(flushed.len(), 1);
        assert_eq!(persist.count(), 2);
    }

    #[test]
    fn wallclock_sweep_finalizes_due_bucket_without_next_tick() {
        // md 线程每拍墙钟 sweep：桶到 start+interval 边界即定稿（grace=0），不等下一周期首笔 tick。
        let (ticks, vols, doms) = tables();
        let sink = Arc::new(CountSink::default());
        let s = CtpMarketStream::new(ticks, vols, doms, dir_with(vec![]), Vec::new());
        let _g = s
            .subscribe_klines(
                Arc::new(FixedSymbols::new(vec!["cu2701".into()])),
                sink.clone(),
            )
            .unwrap();

        // 仅一笔 tick 开 10:00 桶，无跨分钟翻转 → 暂不定稿。
        s.on_kline_tick("cu2701", &mk(10, 600_000), &RawTickDepth::default());
        assert_eq!(sink.count(), 0);
        // 未到收盘边界（660_000）：659_999 不出棒。
        assert!(s.sweep_kline_finalized(659_999).is_empty());
        assert_eq!(sink.count(), 0);
        // 到点 sweep：定稿 10:00 桶并路由，无需任何新 tick。
        let bars = s.sweep_kline_finalized(660_000);
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].symbol, "cu2701");
        assert_eq!(bars[0].start_ms, 600_000);
        assert_eq!(bars[0].end_ms, 660_000);
        assert_eq!(sink.count(), 1);
        // take 语义：重复 sweep 不再出棒。
        assert!(s.sweep_kline_finalized(660_000).is_empty());
        assert_eq!(sink.count(), 1);
    }

    #[test]
    fn raw_tick_depth_is_persisted_into_tick_wal() {
        // FFI 全透传：日 OHLC + 五档量价随 RawTick 落 WAL（不再硬编码 0）。
        let (ticks, vols, doms) = tables();
        let s = CtpMarketStream::new(ticks.clone(), vols, doms, dir_with(vec![]), Vec::new());
        // 盘中 close=0 如实保留（..Default 取 0）。
        let mut bid = [Decimal::ZERO; 5];
        bid[0] = Decimal::from(74_490);
        bid[4] = Decimal::from(74_450);
        let mut ask = [Decimal::ZERO; 5];
        ask[0] = Decimal::from(74_510);
        ask[4] = Decimal::from(74_550);
        let mut bid_vol = [0; 5];
        bid_vol[0] = 11;
        let mut ask_vol = [0; 5];
        ask_vol[0] = 21;
        let depth = RawTickDepth {
            day_open: Decimal::from(74_100),
            day_high: Decimal::from(74_800),
            day_low: Decimal::from(74_000),
            bid,
            ask,
            bid_vol,
            ask_vol,
            ..Default::default()
        };

        s.on_kline_tick("cu2512", &mk(30, 600_000), &depth);

        let pending = ticks.snapshot();
        assert_eq!(pending.len(), 1);
        let r = &pending[0];
        assert_eq!(r.open, Decimal::from(74_100));
        assert_eq!(r.high, Decimal::from(74_800));
        assert_eq!(r.low, Decimal::from(74_000));
        assert_eq!(r.close, Decimal::ZERO); // 盘中 close 0
        assert_eq!(r.bid[0], Decimal::from(74_490));
        assert_eq!(r.bid_vol[0], 11);
        assert_eq!(r.ask[0], Decimal::from(74_510));
        assert_eq!(r.ask_vol[0], 21);
        assert_eq!(r.bid[4], Decimal::from(74_450));
        assert_eq!(r.ask[4], Decimal::from(74_550));
    }

    #[test]
    fn elect_cycle_picks_and_freezes_per_day() {
        let (ticks, vols, doms) = tables();
        let dir = dir_with(vec![info("cu2511", "cu", true), info("cu2512", "cu", true)]);
        vols.put("cu2511", 100);
        vols.put("cu2512", 500);
        let s = CtpMarketStream::new(ticks, vols, doms, dir, Vec::new());

        assert_eq!(s.elect_cycle(20261023), 1);
        assert_eq!(s.dominants.dominant_symbol("cu").as_deref(), Some("cu2512"));
        // 同日冻结。
        s.last_volumes.put("cu2511", 9999);
        assert_eq!(s.elect_cycle(20261023), 0);
        assert_eq!(s.dominants.dominant_symbol("cu").as_deref(), Some("cu2512"));
        // 次日允许重选。
        assert_eq!(s.elect_cycle(20261024), 1);
        assert_eq!(s.dominants.dominant_symbol("cu").as_deref(), Some("cu2511"));
    }

    #[test]
    fn bad_sink_is_evicted_on_route() {
        let (t, v, d) = tables();
        d.put(
            "cu",
            DominantEntry {
                symbol: "cu2512".into(),
                trading_day: 20261023,
                cum_volume: 0,
            },
        );
        let dir = dir_with(vec![info("cu2512", "cu", true)]);
        let s = CtpMarketStream::new(t, v, d, dir, Vec::new());
        let _g = s
            .subscribe_klines(
                Arc::new(FixedSymbols::new(vec!["cu".into()])),
                Arc::new(PanicSink),
            )
            .unwrap();
        // 跨分钟定稿触发 panic sink：被吞住并摘除（不 panic 出 drain）。
        s.on_kline_tick("cu2512", &mk(1, 600_000), &RawTickDepth::default());
        s.on_kline_tick("cu2512", &mk(2, 660_000), &RawTickDepth::default());
        assert_eq!(s.router.clients.lock().unwrap().len(), 0);
    }

    #[test]
    fn preset_dominants_writes_table_and_skips_bad_entries() {
        let (ticks, vols, doms) = tables();
        let s = CtpMarketStream::new(ticks, vols, doms.clone(), dir_with(vec![]), Vec::new());
        // 合法条目写入主力表；非法（缺冒号/空段）忽略。
        let applied = s.preset_dominants(&[
            "cu:cu2512".to_string(),
            "garbage".to_string(),
            " :x".to_string(),
        ]);
        assert_eq!(applied, 1);
        let e = doms.get("cu").expect("cu 主力已预设");
        assert_eq!(e.symbol, "cu2512");
        assert_eq!(e.cum_volume, 0);
        assert!(e.trading_day >= 20261026); // 北京当日
    }
}
