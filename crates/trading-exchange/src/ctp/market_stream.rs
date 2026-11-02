//! CTP 单连接统一行情流（[`market::MarketDataStream`] 实现）。
//!
//! 一个进程单例，被业务 `ctp-md` OS 线程与按需 K线消费者共用：
//! - 注册两类期望来源：`subscribe_prices`（系统行情，tick 由 drain 统一写 QuoteTable，
//!   本类型只贡献订阅符号）与 `subscribe_klines`（额外持定稿回调）；
//! - [`Self::price_symbols`] / [`Self::bar_symbols`] 供 drain 计算「业务兴趣 ∪ K线
//!   期望（品种经 [`InstrumentDir`] 展开为全部 is_trading 合约）」并集向柜台订阅；
//! - [`Self::on_kline_tick`]：K线集 tick 喂入 [`BarAggSink`]，分钟翻转定稿后按注册表
//!   路由（catch_unwind+计时摘除）；原始 tick/累计量写三表在同路径完成；
//! - [`Self::elect_cycle`]：收盘选举纯内核（按配置指标取当日最大/tiebreak、
//!   N 日连击确认、月份单向守卫、每日只处理一次），由 [`Self::spawn_election`]
//!   独立线程在日盘收盘后触发。
//!
//! 全程 `std::sync::Mutex`（OS 线程，非 async）；锁内不调用外部 sink（先收集命中
//! 列表，再在 [`market_hub::safe_notify`] 隔离下回调）。

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rust_decimal::Decimal;
use trading_core::kline::{
    DominantEntry, DominantTable, LastTickStat, LastTickStatTable, PendingCandidate, TickTable,
};
use trading_models::RawTick;

use crate::ctp::bar_agg::BarAggSink;
use crate::ctp::candle::CandleTick;
use crate::ctp::dominant::{
    candidate_month_advances, contract_month, pick_dominant, product_prefix, ContractStat,
    DominantMetric,
};
use crate::ctp::instrument_dir::InstrumentDir;
use crate::market::{
    BarSink, DesiredSource, Kline, KlineInterval, MarketDataStream, SubscriptionGuard,
};
use crate::market_hub::{safe_notify, NotifyOutcome};

/// 落 RawTick 时除聚合三量外的盘口/日 OHLC 字段（CandleTick 保持精简，1m 聚合不用它们）。
///
/// 这些是 CTP **当日累计**日开高低收（盘中 close 常为 0）与五档量价，**不是**
/// 1m bar 的 OHLC；1m OHLC 由 candle 聚合得到。`Default` 全 0（FFI 字段缺失/测试用）。
#[derive(Debug, Clone, Default)]
pub struct RawTickDepth {
    /// 真实交易所代码（SHFE/DCE/…，随原始 tick 落盘 exchange_id 列）。
    pub exchange_id: String,
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
            exchange_id: t.exchange_id.clone(),
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
    /// 三表（drain 写入；聚合只读 dominant，选举读 last_stats、写 dominant）。
    ticks: Arc<TickTable>,
    last_stats: Arc<LastTickStatTable>,
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
        last_stats: Arc<LastTickStatTable>,
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
            last_stats,
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
    ///
    /// `exchange_id` 兜底（CTP 专属，不上提共有层）：优先用 tick 原生 ExchangeID
    /// （生产柜台回填）；部分前置（如 SimNow）行情回调不回填该字段，则回退查进程级
    /// [`InstrumentDir`]（ReqQryInstrument 预热的合约静态信息）；两者皆无则留空，
    /// 不因此丢 tick 或阻塞行情。
    pub fn on_kline_tick(&self, symbol: &str, t: &CandleTick, depth: &RawTickDepth) {
        // exchange_id 解析（CTP 专属，不上提共有层）：原生非空直接用（生产柜台路径，
        // 不查表、零额外分配）；原生为空（如 SimNow 回调不回填）才查 InstrumentDir；
        // 两者皆无留空。
        let resolved = if !t.exchange_id.trim().is_empty() {
            None // 沿用 t 自身的原生 exchange_id
        } else {
            self.instrument_dir
                .exchange(symbol)
                .filter(|e| !e.trim().is_empty())
        };
        // 仅在确实兜底到不同值时构造一个 owned tick，避免生产热路径逐笔整结构 clone。
        let owned;
        let tick = match &resolved {
            Some(eid) => {
                owned = CandleTick {
                    exchange_id: eid.clone(),
                    ..t.clone()
                };
                &owned
            }
            None => t,
        };
        // 聚合器与 WAL 共用同一 exchange_id，保证 bar extras 与 tick 列一致。
        let exchange_id = resolved.unwrap_or_else(|| t.exchange_id.clone());
        self.ticks.append(RawTick {
            exchange: CTP_VENUE.to_string(),
            exchange_id,
            symbol: symbol.to_string(),
            timestamp: tick.update_ms,
            trading_day: tick.trading_day,
            price: tick.price,
            open: depth.day_open,
            high: depth.day_high,
            low: depth.day_low,
            close: depth.day_close,
            volume: tick.cum_volume,
            quote_volume: tick.cum_turnover,
            open_interest: tick.open_interest,
            bid: depth.bid,
            ask: depth.ask,
            bid_vol: depth.bid_vol,
            ask_vol: depth.ask_vol,
        });
        // 选举原料：交易日 + 成交量 + 持仓量同口径覆盖（日过滤在 elect_cycle 内做）。
        self.last_stats.put(
            symbol,
            LastTickStat {
                trading_day: tick.trading_day,
                cum_volume: tick.cum_volume,
                open_interest: tick.open_interest,
            },
        );
        self.bar_agg.lock().unwrap().on_tick(symbol, tick);
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

    /// 收盘选举一轮（每日每品种一次）。
    ///
    /// 1. 只取 `trading_day == 选举日` 的 stats（周末/节假日无数据 → 不产生选举，
    ///    也不打断连击）；按目录 product_id（缺失则剥尾数字）分组，按 `criteria`
    ///    选最大者（固定 tiebreak）；
    /// 2. 该 product 当日已处理（`dominant.trading_day == trading_day`）则跳过，
    ///    防止 60s 线程重复计数；
    /// 3. 无当前主力（冷启动）→ 首次结果立即采纳；
    /// 4. 当前主力继续胜出 → 清空 pending，刷新当日标记/指标；
    /// 5. 换候选：月份必须严格前进；连击达到 `confirm_days` 才切换，否则记录
    ///    `pending`（同一候选 streak+1，换候选重置为 1）。
    ///
    /// 返回值 = 本轮处理过的 product 数（测试/诊断用）。
    pub fn elect_cycle(
        &self,
        trading_day: u32,
        criteria: DominantMetric,
        confirm_days: u32,
    ) -> usize {
        let confirm_days = confirm_days.max(1);
        let reference_year = (trading_day / 10_000) as i32;
        let mut by_product: HashMap<String, Vec<ContractStat>> = HashMap::new();
        for (symbol, stat) in self.last_stats.snapshot() {
            // 只认当日数据：持仓量不日清，陈旧值绝不参与选举。
            if stat.trading_day != trading_day {
                continue;
            }
            let product = self
                .instrument_dir
                .get(&symbol)
                .map(|i| i.product_id)
                .filter(|p| !p.is_empty())
                .or_else(|| product_prefix(&symbol));
            if let Some(product) = product {
                by_product.entry(product).or_default().push(ContractStat {
                    symbol,
                    value: criteria.value(&stat),
                });
            }
        }
        let mut processed = 0;
        for (product, stats) in by_product {
            let current = self.dominants.get(&product);
            if current
                .as_ref()
                .is_some_and(|e| e.trading_day == trading_day)
            {
                continue; // 当日已处理（盘中冻结）
            }
            let Some(winner) = pick_dominant(&stats) else {
                continue; // 当日无正指标：不产出、不改状态
            };
            let winner_value = stats
                .iter()
                .find(|c| c.symbol == winner)
                .map(|c| c.value)
                .unwrap_or_default();
            let Some(cur) = current else {
                // 冷启动：首次选举立即采纳，避免空等 N 天。
                tracing::info!(
                    product, symbol = %winner, trading_day,
                    metric = criteria.as_str(), value = %winner_value,
                    "CTP 主力冷启动：首次选举立即采纳"
                );
                self.put_dominant(&product, winner, trading_day, winner_value, None);
                processed += 1;
                continue;
            };
            if cur.metric_value == Decimal::ZERO {
                // 暂定预设（`preset_dominants` 写入 metric_value=0）：首轮真实选举
                // 无条件覆盖，跳过月份守卫与确认天数——否则错误/过期预设会被
                // 单向月份守卫永久锁死。覆盖后即进入正常语义。
                tracing::info!(
                    product, old = %cur.symbol, new = %winner, trading_day,
                    metric = criteria.as_str(), value = %winner_value,
                    "CTP 主力暂定预设被首轮真实选举覆盖"
                );
                self.put_dominant(&product, winner, trading_day, winner_value, None);
                processed += 1;
                continue;
            }
            if winner == cur.symbol {
                // 当前主力继续胜出：清空连击，刷新当日标记/指标。
                self.put_dominant(&product, cur.symbol, trading_day, winner_value, None);
                processed += 1;
                continue;
            }
            // 月份单向守卫：候选不可解析 → WARN + fail-closed；未前进一律不切换。
            if !candidate_month_advances(&cur.symbol, &winner, reference_year) {
                if contract_month(&winner, reference_year).is_none() {
                    tracing::warn!(
                        product, candidate = %winner, current = %cur.symbol,
                        "CTP 主力候选合约月份不可解析，保留当前主力（fail-closed）"
                    );
                } else {
                    tracing::debug!(
                        product, candidate = %winner, current = %cur.symbol,
                        "CTP 主力候选月份未前进，保留当前主力"
                    );
                }
                self.put_dominant(&product, cur.symbol, trading_day, cur.metric_value, None);
                processed += 1;
                continue;
            }
            // 连击：同一候选累加，换候选从 1 重新开始。
            let streak = match &cur.pending {
                Some(p) if p.symbol == winner => p.streak.saturating_add(1),
                _ => 1,
            };
            if streak >= confirm_days {
                tracing::info!(
                    product, old = %cur.symbol, new = %winner, streak, confirm_days,
                    trading_day, metric = criteria.as_str(),
                    "CTP 主力切换确认"
                );
                self.put_dominant(&product, winner, trading_day, winner_value, None);
            } else {
                tracing::info!(
                    product, candidate = %winner, current = %cur.symbol,
                    streak, confirm_days, trading_day,
                    "CTP 主力候选连击（未达确认天数）"
                );
                self.put_dominant(
                    &product,
                    cur.symbol,
                    trading_day,
                    cur.metric_value,
                    Some(PendingCandidate {
                        symbol: winner,
                        streak,
                    }),
                );
            }
            processed += 1;
        }
        processed
    }

    /// 写主力表：统一构造 [`DominantEntry`]，避免状态机各分支重复结构体字面量。
    fn put_dominant(
        &self,
        product: &str,
        symbol: String,
        trading_day: u32,
        metric_value: Decimal,
        pending: Option<PendingCandidate>,
    ) {
        self.dominants.put(
            product,
            DominantEntry {
                symbol,
                trading_day,
                metric_value,
                pending,
            },
        );
    }

    /// 起独立线程做收盘选举：每分钟检查北京 wall-clock 是否进入日盘收盘后窗口
    /// （≥15:00），当天首次进入按配置指标/确认天数选一次（elect_cycle 内部按交易日防重）。
    pub fn spawn_election(
        self: &Arc<Self>,
        stop: Arc<AtomicBool>,
        criteria: DominantMetric,
        confirm_days: u32,
    ) {
        let me = Arc::clone(self);
        let spawn = std::thread::Builder::new()
            .name("ctp-dominant".into())
            .spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    interruptible_sleep(Duration::from_secs(60), &stop);
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    if let Some(day) = beijing_election_day() {
                        me.elect_cycle(day, criteria, confirm_days);
                    }
                }
            });
        if let Err(e) = spawn {
            // 选举线程缺失 = 收盘后不会自动选主力，必须显式告警（不静默吞掉）。
            tracing::error!(error = %e, "CTP 主力选举线程创建失败，收盘选举将不会运行");
        }
    }

    /// 冷启动预设主力：把 `["cu:cu2512", ...]` 直接写入主力表（trading_day 取
    /// 北京当日，metric_value=0、pending=None）。用于盘中重启/未到 15:00 选举窗口时让品种订阅
    /// 立即只推指定合约；elect_cycle 因该品种当日已选而跳过。
    ///
    /// `metric_value=0` 表示**暂定**：下一交易日首轮真实选举**无条件覆盖**
    /// （跳过月份守卫/确认天数），避免错误或过期预设被单向月份守卫永久锁死；
    /// 覆盖后即进入正常语义。
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
                    metric_value: Decimal::ZERO,
                    pending: None,
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

    fn tables() -> (Arc<TickTable>, Arc<LastTickStatTable>, Arc<DominantTable>) {
        (
            Arc::new(TickTable::new()),
            Arc::new(LastTickStatTable::new()),
            Arc::new(DominantTable::new()),
        )
    }

    fn mk(vol: i64, ms: i64) -> CandleTick {
        CandleTick {
            trading_day: 20261023,
            exchange_id: "SHFE".into(),
            update_ms: ms,
            price: Decimal::from(100),
            cum_volume: vol,
            cum_turnover: Decimal::ZERO,
            open_interest: Decimal::ZERO,
        }
    }

    /// 原生 tick 不带 ExchangeID（SimNow 形态）：exchange_id 为空，靠 InstrumentDir 兜底。
    fn mk_no_eid(vol: i64, ms: i64) -> CandleTick {
        CandleTick {
            exchange_id: String::new(),
            ..mk(vol, ms)
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
                metric_value: Decimal::ZERO,
                pending: None,
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
        let stat = s.last_stats.get("cu2512").expect("统计表已写入");
        assert_eq!(stat.trading_day, 20261023);
        assert_eq!(stat.cum_volume, 25);
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
                metric_value: Decimal::ZERO,
                pending: None,
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
                metric_value: Decimal::ZERO,
                pending: None,
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
                metric_value: Decimal::ZERO,
                pending: None,
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
            exchange_id: "SHFE".into(),
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
        // 真实交易所代码随原始 tick 落 WAL（venue=ctp，exchange_id=SHFE）。
        assert_eq!(r.exchange, "ctp");
        assert_eq!(r.exchange_id, "SHFE");
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
    fn empty_native_exchange_id_falls_back_to_instrument_dir() {
        // SimNow 形态：行情回调 ExchangeID 空，合约目录已由 ReqQryInstrument 预热为 SHFE。
        let (ticks, vols, doms) = tables();
        let dir = dir_with(vec![info("cu2611", "cu", true)]);
        let s = CtpMarketStream::new(ticks.clone(), vols, doms, dir, Vec::new());

        s.on_kline_tick("cu2611", &mk_no_eid(30, 600_000), &RawTickDepth::default());

        // WAL tick 经目录兜底得到 SHFE（venue 仍是 ctp）。
        let pending = ticks.snapshot();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].exchange_id, "SHFE");

        // 聚合定稿 bar 的 extras.exchange_id 同样兜底为 SHFE（墙钟定稿）。
        let bar = s
            .sweep_kline_finalized(660_000)
            .pop()
            .expect("10:00 桶墙钟定稿");
        assert_eq!(
            bar.extras.get("exchange_id"),
            Some(&crate::market::KlineExtra::Str("SHFE".into()))
        );
    }

    #[test]
    fn missing_native_and_dir_exchange_id_stays_blank_without_error() {
        // 原生空、合约目录也未预热：exchange_id 留空，tick 不丢、聚合不报错。
        let (ticks, vols, doms) = tables();
        let s = CtpMarketStream::new(ticks.clone(), vols, doms, dir_with(vec![]), Vec::new());
        s.on_kline_tick("cu9999", &mk_no_eid(1, 600_000), &RawTickDepth::default());
        let pending = ticks.snapshot();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].exchange_id, "");
        assert!(s.sweep_kline_finalized(660_000).len() == 1);
    }

    // ---- 收盘选举：指标 / 当日过滤 / 连击确认 / 月份守卫 ----

    fn stat(day: u32, vol: i64, oi: i64) -> LastTickStat {
        LastTickStat {
            trading_day: day,
            cum_volume: vol,
            open_interest: Decimal::from(oi),
        }
    }

    /// 构造带指定 (合约, 品种) 目录的行情流。
    fn stream_with(pairs: &[(&str, &str)]) -> Arc<CtpMarketStream> {
        let (ticks, stats, doms) = tables();
        let dir = dir_with(
            pairs
                .iter()
                .map(|(sym, prod)| info(sym, prod, true))
                .collect(),
        );
        CtpMarketStream::new(ticks, stats, doms, dir, Vec::new())
    }

    /// 预置**已确认**的当前主力（模拟上一交易日真实选举结果）。
    ///
    /// `metric_value` 必须非 0：0 表示「暂定预设」，首轮真实选举会无条件覆盖。
    fn preset_current(s: &CtpMarketStream, product: &str, symbol: &str, day: u32) {
        s.dominants.put(
            product,
            DominantEntry {
                symbol: symbol.into(),
                trading_day: day,
                metric_value: Decimal::ONE,
                pending: None,
            },
        );
    }

    /// 预置**暂定**主力（等价 `preset_dominants` 写入：metric_value=0）。
    fn preset_provisional(s: &CtpMarketStream, product: &str, symbol: &str, day: u32) {
        s.dominants.put(
            product,
            DominantEntry {
                symbol: symbol.into(),
                trading_day: day,
                metric_value: Decimal::ZERO,
                pending: None,
            },
        );
    }

    #[test]
    fn elect_cycle_cold_start_adopts_open_interest_and_freezes_same_day() {
        let s = stream_with(&[("cu2512", "cu"), ("cu2601", "cu")]);
        // 成交量 cu2601 更大，但默认指标是持仓量：cu2512 (900) 胜 cu2601 (100)。
        s.last_stats.put("cu2512", stat(20261023, 100, 900));
        s.last_stats.put("cu2601", stat(20261023, 500, 100));

        assert_eq!(s.elect_cycle(20261023, DominantMetric::OpenInterest, 3), 1);
        assert_eq!(s.dominants.dominant_symbol("cu").as_deref(), Some("cu2512"));
        // 同日冻结：指标反转也不再重选。
        s.last_stats.put("cu2601", stat(20261023, 9999, 9999));
        assert_eq!(s.elect_cycle(20261023, DominantMetric::OpenInterest, 3), 0);
        assert_eq!(s.dominants.dominant_symbol("cu").as_deref(), Some("cu2512"));
    }

    #[test]
    fn elect_cycle_volume_metric_selects_by_volume() {
        let s = stream_with(&[("cu2512", "cu"), ("cu2601", "cu")]);
        s.last_stats.put("cu2512", stat(20261023, 100, 900));
        s.last_stats.put("cu2601", stat(20261023, 500, 100));
        // 切成交量指标：cu2601 胜出。
        assert_eq!(s.elect_cycle(20261023, DominantMetric::Volume, 3), 1);
        assert_eq!(s.dominants.dominant_symbol("cu").as_deref(), Some("cu2601"));
    }

    #[test]
    fn elect_cycle_ignores_stale_day_stats() {
        // 陈旧交易日（前一天/周末残留）即使指标巨大也不参与选举。
        let s = stream_with(&[("cu2612", "cu"), ("cu2701", "cu")]);
        s.last_stats.put("cu2612", stat(20261022, 9999, 99_999));
        s.last_stats.put("cu2701", stat(20261023, 10, 10));
        assert_eq!(s.elect_cycle(20261023, DominantMetric::OpenInterest, 3), 1);
        assert_eq!(s.dominants.dominant_symbol("cu").as_deref(), Some("cu2701"));
    }

    #[test]
    fn elect_cycle_requires_consecutive_confirm_days_before_switch() {
        let s = stream_with(&[("cu2701", "cu"), ("cu2705", "cu")]);
        preset_current(&s, "cu", "cu2701", 20261101);

        // 第 1/2 天：候选连胜 1/2，仍未切换。
        for (day, streak) in [(20261102u32, 1u32), (20261103, 2)] {
            s.last_stats.put("cu2701", stat(day, 10, 10));
            s.last_stats.put("cu2705", stat(day, 100, 100));
            assert_eq!(s.elect_cycle(day, DominantMetric::OpenInterest, 3), 1);
            let e = s.dominants.get("cu").unwrap();
            assert_eq!(e.symbol, "cu2701", "确认期内保持旧主力");
            assert_eq!(e.pending.as_ref().map(|p| p.streak), Some(streak));
            assert_eq!(
                e.pending.as_ref().map(|p| p.symbol.as_str()),
                Some("cu2705")
            );
            assert_eq!(e.trading_day, day, "每个有数据交易日都推进当日标记");
        }
        // 第 3 天：达到确认天数，切换。
        s.last_stats.put("cu2701", stat(20261104, 10, 10));
        s.last_stats.put("cu2705", stat(20261104, 100, 100));
        assert_eq!(s.elect_cycle(20261104, DominantMetric::OpenInterest, 3), 1);
        let e = s.dominants.get("cu").unwrap();
        assert_eq!(e.symbol, "cu2705");
        assert!(e.pending.is_none(), "确认切换后清空连击");
    }

    #[test]
    fn elect_cycle_confirm_days_one_switches_same_day() {
        let s = stream_with(&[("cu2701", "cu"), ("cu2705", "cu")]);
        preset_current(&s, "cu", "cu2701", 20261101);
        s.last_stats.put("cu2701", stat(20261102, 10, 10));
        s.last_stats.put("cu2705", stat(20261102, 100, 100));
        // confirm_days=1 等价旧行为：当天生效。
        assert_eq!(s.elect_cycle(20261102, DominantMetric::OpenInterest, 1), 1);
        assert_eq!(s.dominants.dominant_symbol("cu").as_deref(), Some("cu2705"));
    }

    #[test]
    fn elect_cycle_candidate_change_resets_streak() {
        let s = stream_with(&[("cu2701", "cu"), ("cu2705", "cu"), ("cu2707", "cu")]);
        preset_current(&s, "cu", "cu2701", 20261101);
        // day1：cu2705 连胜 1。
        s.last_stats.put("cu2701", stat(20261102, 10, 10));
        s.last_stats.put("cu2705", stat(20261102, 100, 100));
        assert_eq!(s.elect_cycle(20261102, DominantMetric::OpenInterest, 3), 1);
        // day2：换成 cu2707 → 连击重置为 1。
        s.last_stats.put("cu2701", stat(20261103, 10, 10));
        s.last_stats.put("cu2705", stat(20261103, 100, 100));
        s.last_stats.put("cu2707", stat(20261103, 200, 200));
        assert_eq!(s.elect_cycle(20261103, DominantMetric::OpenInterest, 3), 1);
        let e = s.dominants.get("cu").unwrap();
        assert_eq!(e.symbol, "cu2701");
        let pending = e.pending.expect("候选连击已记录");
        assert_eq!(pending.symbol, "cu2707");
        assert_eq!(pending.streak, 1);
    }

    #[test]
    fn elect_cycle_current_winner_clears_pending() {
        let s = stream_with(&[("cu2701", "cu"), ("cu2705", "cu")]);
        preset_current(&s, "cu", "cu2701", 20261101);
        // day1：cu2705 连胜 1。
        s.last_stats.put("cu2701", stat(20261102, 10, 10));
        s.last_stats.put("cu2705", stat(20261102, 100, 100));
        s.elect_cycle(20261102, DominantMetric::OpenInterest, 3);
        // day2：当前主力重新胜出 → 连击清零。
        s.last_stats.put("cu2701", stat(20261103, 500, 500));
        s.last_stats.put("cu2705", stat(20261103, 100, 100));
        s.elect_cycle(20261103, DominantMetric::OpenInterest, 3);
        let e = s.dominants.get("cu").unwrap();
        assert_eq!(e.symbol, "cu2701");
        assert!(e.pending.is_none());
    }

    #[test]
    fn elect_cycle_month_guard_never_rolls_back() {
        // 当前 2701；候选 2612（更早月份）即使连续大胜也永不切换。
        let s = stream_with(&[("cu2701", "cu"), ("cu2612", "cu")]);
        preset_current(&s, "cu", "cu2701", 20261101);
        for day in [20261102u32, 20261103, 20261104] {
            s.last_stats.put("cu2701", stat(day, 1, 1));
            s.last_stats.put("cu2612", stat(day, 9999, 9999));
            assert_eq!(s.elect_cycle(day, DominantMetric::OpenInterest, 3), 1);
            let e = s.dominants.get("cu").unwrap();
            assert_eq!(e.symbol, "cu2701");
            assert!(e.pending.is_none(), "回退候选不得累计连击");
        }
    }

    #[test]
    fn elect_cycle_unparseable_candidate_is_fail_closed() {
        let s = stream_with(&[("cu2701", "cu"), ("cuXXXX", "cu")]);
        preset_current(&s, "cu", "cu2701", 20261101);
        s.last_stats.put("cu2701", stat(20261102, 1, 1));
        s.last_stats.put("cuXXXX", stat(20261102, 9999, 9999));
        assert_eq!(s.elect_cycle(20261102, DominantMetric::OpenInterest, 3), 1);
        let e = s.dominants.get("cu").unwrap();
        assert_eq!(e.symbol, "cu2701");
        assert!(e.pending.is_none());
    }

    #[test]
    fn elect_cycle_current_unparseable_accepts_valid_candidate() {
        // 当前合约不可解析、候选合法 → 不做月份守卫拦截（仍受确认天数约束）。
        let s = stream_with(&[("cuXXXX", "cu"), ("cu2705", "cu")]);
        preset_current(&s, "cu", "cuXXXX", 20261101);
        s.last_stats.put("cuXXXX", stat(20261102, 1, 1));
        s.last_stats.put("cu2705", stat(20261102, 100, 100));
        assert_eq!(s.elect_cycle(20261102, DominantMetric::OpenInterest, 1), 1);
        assert_eq!(s.dominants.dominant_symbol("cu").as_deref(), Some("cu2705"));
    }

    #[test]
    fn elect_cycle_no_data_day_does_not_break_streak() {
        // 周末/无 tick 日：不推进当日标记、不打断连击。
        let s = stream_with(&[("cu2701", "cu"), ("cu2705", "cu")]);
        preset_current(&s, "cu", "cu2701", 20261101);
        s.last_stats.put("cu2701", stat(20261102, 10, 10));
        s.last_stats.put("cu2705", stat(20261102, 100, 100));
        s.elect_cycle(20261102, DominantMetric::OpenInterest, 3);
        assert_eq!(s.dominants.get("cu").unwrap().pending.unwrap().streak, 1);

        // day3 无任何当日数据（stats 仍是 day2）：本轮不处理，状态原样保留。
        assert_eq!(s.elect_cycle(20261103, DominantMetric::OpenInterest, 3), 0);
        let e = s.dominants.get("cu").unwrap();
        assert_eq!(e.trading_day, 20261102);
        assert_eq!(e.pending.as_ref().unwrap().streak, 1);

        // day4 恢复数据：同一候选继续累加到 2。
        s.last_stats.put("cu2701", stat(20261104, 10, 10));
        s.last_stats.put("cu2705", stat(20261104, 100, 100));
        s.elect_cycle(20261104, DominantMetric::OpenInterest, 3);
        assert_eq!(s.dominants.get("cu").unwrap().pending.unwrap().streak, 2);
    }

    #[test]
    fn elect_cycle_same_day_repeat_does_not_advance_streak() {
        // 60s 选举线程同一天可能多次触发：trading_day 标记必须保证连击只 +1 一次。
        let s = stream_with(&[("cu2701", "cu"), ("cu2705", "cu")]);
        preset_current(&s, "cu", "cu2701", 20261101);
        s.last_stats.put("cu2701", stat(20261102, 10, 10));
        s.last_stats.put("cu2705", stat(20261102, 100, 100));

        assert_eq!(s.elect_cycle(20261102, DominantMetric::OpenInterest, 3), 1);
        assert_eq!(s.dominants.get("cu").unwrap().pending.unwrap().streak, 1);
        // 同日再次触发：本轮不处理，连击不得变成 2。
        assert_eq!(s.elect_cycle(20261102, DominantMetric::OpenInterest, 3), 0);
        assert_eq!(s.dominants.get("cu").unwrap().pending.unwrap().streak, 1);
    }

    #[test]
    fn elect_cycle_provisional_preset_is_overridden_by_first_real_election() {
        // H1：预设（metric_value=0）只是暂定，首轮真实选举必须无条件覆盖，
        // 即使真实主力的月份「回退」于预设远月（旧逻辑会被月份守卫永久锁死）。
        let s = stream_with(&[("cu2709", "cu"), ("cu2612", "cu")]);
        preset_provisional(&s, "cu", "cu2709", 20261101);
        s.last_stats.put("cu2709", stat(20261102, 10, 500));
        s.last_stats.put("cu2612", stat(20261102, 100, 9000));

        assert_eq!(s.elect_cycle(20261102, DominantMetric::OpenInterest, 3), 1);
        let e = s.dominants.get("cu").unwrap();
        assert_eq!(e.symbol, "cu2612", "暂定预设必须被首轮真实选举覆盖");
        assert_eq!(e.metric_value, Decimal::from(9000));
        assert!(e.pending.is_none());

        // 覆盖后进入正常语义：再次出现更早月份的最大者，月份守卫应拒绝。
        s.last_stats.put("cu2612", stat(20261103, 80, 8000));
        s.last_stats.put("cu2611", stat(20261103, 999, 99_999));
        assert_eq!(s.elect_cycle(20261103, DominantMetric::OpenInterest, 3), 1);
        assert_eq!(s.dominants.dominant_symbol("cu").as_deref(), Some("cu2612"));
    }

    #[test]
    fn elect_cycle_backward_max_masks_forward_runner_up_by_design() {
        // H2 决议：不改为「先过滤再选最大」。全局最大若月份回退，则保留当前并清空
        // pending；次大的前进候选本日不被考虑（这是已确认的语义，不是缺陷）。
        let s = stream_with(&[("cu2701", "cu"), ("cu2612", "cu"), ("cu2705", "cu")]);
        preset_current(&s, "cu", "cu2701", 20261101);

        // day1：cu2705（前进）为最大 → 连击 1。
        s.last_stats.put("cu2701", stat(20261102, 10, 5000));
        s.last_stats.put("cu2612", stat(20261102, 10, 4000));
        s.last_stats.put("cu2705", stat(20261102, 10, 8000));
        s.elect_cycle(20261102, DominantMetric::OpenInterest, 3);
        assert_eq!(s.dominants.get("cu").unwrap().pending.unwrap().streak, 1);

        // day2：近月 cu2612 成为全局最大（回退月份）；cu2705 次大且前进但不被考虑。
        s.last_stats.put("cu2701", stat(20261103, 10, 5000));
        s.last_stats.put("cu2612", stat(20261103, 10, 9000));
        s.last_stats.put("cu2705", stat(20261103, 10, 8500));
        assert_eq!(s.elect_cycle(20261103, DominantMetric::OpenInterest, 3), 1);
        let e = s.dominants.get("cu").unwrap();
        assert_eq!(e.symbol, "cu2701", "全局最大为回退月份：保留当前");
        assert!(e.pending.is_none(), "当前语义：守卫否决时清空 pending");

        // day3：回退合约指标回落，cu2705 再胜 → 连击从 1 重新开始（不是 2）。
        s.last_stats.put("cu2701", stat(20261104, 10, 5000));
        s.last_stats.put("cu2612", stat(20261104, 10, 4000));
        s.last_stats.put("cu2705", stat(20261104, 10, 8000));
        s.elect_cycle(20261104, DominantMetric::OpenInterest, 3);
        assert_eq!(s.dominants.get("cu").unwrap().pending.unwrap().streak, 1);
    }

    #[test]
    fn bad_sink_is_evicted_on_route() {
        let (t, v, d) = tables();
        d.put(
            "cu",
            DominantEntry {
                symbol: "cu2512".into(),
                trading_day: 20261023,
                metric_value: Decimal::ZERO,
                pending: None,
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
        assert_eq!(e.metric_value, Decimal::ZERO);
        assert!(e.pending.is_none());
        assert!(e.trading_day >= 20261026); // 北京当日
    }
}
