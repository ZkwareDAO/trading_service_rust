//! 行情 hub 纯内核：消费者注册表 / 全局并集 / 订阅 diff / 按 symbol 路由 /
//! 无条件 sink 策略（本地慢仅边沿告警、持久化 panic 自愈、南向严格摘除）。
//!
//! 本模块**不含任何 IO、连接、async、channel**：只做登记与纯函数式的集合运算与同步
//! 回调隔离。WS 驱动（[`crate::market_ws`]）与 CTP 单连接（[`crate::ctp::market`]
//! 的 drain 循环）各持有一个 [`HubRegistry`]（`Arc<Mutex<HubCore>>`），周期锁读并集做
//! 订阅增减，收 tick/bar 时按 symbol 查注册表同步回调。
//!
//! 加锁纪律：注册表锁内只取 Arc 列表、不回调、不 IO，释锁后再调用 sink；无条件下游
//! 集合自带独立锁（与注册表锁不嵌套），回调期间仅持该集合锁。`BarSink` 契约仍是
//! 同步纯内存，禁止在 `on_bar` 内回调 `HubRegistry`（否则会自锁）。

use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::Hash;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::market::{BarSink, DesiredSource, Kline, SubscriptionGuard};

/// 订阅增减计划：相对「已订阅集」，达到「期望集」需要增订 / 退订的元素。
///
/// 泛型元素（频道字符串、合约符号等）；两个列表均排序，保证线上订阅报文确定性。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubsPlan<T> {
    pub to_sub: Vec<T>,
    pub to_unsub: Vec<T>,
}

impl<T: Ord + Clone + Hash> SubsPlan<T> {
    /// 计算 desired vs subscribed 差集：新增加订阅、多余退订（各自排序）。
    pub fn diff(desired: &HashSet<T>, subscribed: &HashSet<T>) -> Self {
        let mut to_sub: Vec<T> = desired.difference(subscribed).cloned().collect();
        let mut to_unsub: Vec<T> = subscribed.difference(desired).cloned().collect();
        to_sub.sort();
        to_unsub.sort();
        Self { to_sub, to_unsub }
    }

    pub fn is_empty(&self) -> bool {
        self.to_sub.is_empty() && self.to_unsub.is_empty()
    }
}

/// 一个 K线消费者：期望来源 + 定稿回调。
struct BarClient {
    source: Arc<dyn DesiredSource>,
    sink: Arc<dyn BarSink>,
}

/// hub 注册表内核（仅被 `Arc<Mutex<_>>` 持有，所有方法都是短临界区纯内存操作）。
pub struct HubCore {
    next_id: u64,
    /// 系统行情来源（只贡献订阅并集；价格 tick 由托管 sink 直写 QuoteTable）。
    price_sources: HashMap<u64, Arc<dyn DesiredSource>>,
    /// K线消费者（贡献并集，定稿 bar 按 symbol 路由回调）。
    bar_clients: HashMap<u64, BarClient>,
    /// 无条件 bar 下游（全量持久化）：**绕过 symbol 路由**，每根定稿 bar
    /// （含零订阅标的）都通知；不贡献订阅并集，无注销凭证（进程寿命）。
    /// 独立成锁：派发时释放注册表锁后再回调，避免持注册表锁调用外部 sink。
    unconditional: Arc<Mutex<UnconditionalSinkSet>>,
}

impl HubCore {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            price_sources: HashMap::new(),
            bar_clients: HashMap::new(),
            unconditional: Arc::new(Mutex::new(UnconditionalSinkSet::new())),
        }
    }

    fn alloc_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    fn add_price_source(&mut self, src: Arc<dyn DesiredSource>) -> u64 {
        let id = self.alloc_id();
        self.price_sources.insert(id, src);
        id
    }

    fn add_bar_client(&mut self, src: Arc<dyn DesiredSource>, sink: Arc<dyn BarSink>) -> u64 {
        let id = self.alloc_id();
        self.bar_clients.insert(id, BarClient { source: src, sink });
        id
    }

    fn remove(&mut self, id: u64) {
        // 价格来源与 K线消费者的 id 取自同一自增序列，互不冲突，两处都尝试移除。
        self.price_sources.remove(&id);
        self.bar_clients.remove(&id);
    }

    /// 收集一组来源的期望符号（去重、有序）。
    fn union_of<'a>(it: impl Iterator<Item = &'a Arc<dyn DesiredSource>>) -> BTreeSet<String> {
        let mut union = BTreeSet::new();
        for src in it {
            union.extend(src.symbols());
        }
        union
    }

    /// 所有来源当前期望符号的全局并集（价格 ∪ K线，去重、有序）。
    fn desired_union(&self) -> BTreeSet<String> {
        let mut union = Self::union_of(self.price_sources.values());
        for client in self.bar_clients.values() {
            union.extend(client.source.symbols());
        }
        union
    }

    /// 仅系统行情来源的期望并集（映射 ticker 类频道用）。
    fn desired_price_symbols(&self) -> BTreeSet<String> {
        Self::union_of(self.price_sources.values())
    }

    /// 仅 K线消费者来源的期望并集（映射 K线频道用）。
    fn desired_bar_symbols(&self) -> BTreeSet<String> {
        Self::union_of(self.bar_clients.values().map(|c| &c.source))
    }

    /// 当前期望集中包含 `symbol` 的 K线消费者（id + sink）。
    /// 在锁内克隆 Arc、释锁后再回调，避免持锁调用外部代码。
    fn bar_sinks_for(&self, symbol: &str) -> Vec<(u64, Arc<dyn BarSink>)> {
        self.bar_clients
            .iter()
            .filter(|(_, c)| c.source.symbols().iter().any(|s| s == symbol))
            .map(|(id, c)| (*id, Arc::clone(&c.sink)))
            .collect()
    }
}

impl Default for HubCore {
    fn default() -> Self {
        Self::new()
    }
}

/// 单次 sink 回调的隔离结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotifyOutcome {
    /// 正常返回且耗时在预算内。
    Ok,
    /// 回调 panic（已被 catch_unwind 吞住）。
    Panicked,
    /// 回调耗时达到/超过预算（疑似慢 sink）。
    Slow,
}

/// 在隔离环境中同步通知一个 bar sink：catch_unwind 吞 panic + 单次耗时计时。
/// 纯内存、不起线程；调用方据非 [`NotifyOutcome::Ok`] 摘除该 sink。
///
/// 计时在调用前后各取一次 [`Instant::now`]，达到/超过预算即判慢（即便它正常返回）。
/// `AssertUnwindSafe` 安全：回调只允许纯内存操作，且 panic 后该 sink 即被摘除、
/// 不会再被使用，不存在「观察到被污染状态后继续使用」的路径。
pub fn safe_notify(sink: &dyn BarSink, bar: &Kline, budget: Duration) -> NotifyOutcome {
    let started = Instant::now();
    let result = catch_unwind(AssertUnwindSafe(|| sink.on_bar(bar)));
    let elapsed = started.elapsed();
    match result {
        Err(_) => NotifyOutcome::Panicked,
        Ok(()) if elapsed >= budget => NotifyOutcome::Slow,
        Ok(()) => NotifyOutcome::Ok,
    }
}

/// 本地/无条件 sink 的单次回调预算：超出仅边沿告警、**不摘除**。
///
/// 本地 sink（如持久化写内存表）纯内存、无 IO；偶发调度/锁竞争抖动不应导致
/// 数据通路被永久摘除（2026-11-02 ab 落盘中断事故的根因）。
const LOCAL_SINK_BUDGET: Duration = Duration::from_millis(50);

/// 南向网络路由消费者的单次回调预算：超出即摘除（背压保护，慢客户端不能拖死行情线程）。
pub(crate) const NETWORK_SINK_BUDGET: Duration = Duration::from_millis(1);

/// 持久化 sink 被 panic 摘除后的自愈冷却：防持续 panic 时每根 bar 重注册刷屏/抖动。
const PERSIST_HEAL_COOLDOWN: Duration = Duration::from_secs(60);

/// 无条件 sink 的类型（仅日志/诊断用；策略细节不暴露给宿主）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkKind {
    /// 本地持久化：纯内存、不可失败；只 catch_unwind，不测耗时。
    LocalPersist,
    /// 本地其它 sink：测耗时，超预算仅边沿告警，不摘除。
    LocalBounded,
}

impl SinkKind {
    fn as_str(self) -> &'static str {
        match self {
            SinkKind::LocalPersist => "persist",
            SinkKind::LocalBounded => "bounded",
        }
    }
}

/// 无条件 sink 的隔离/超时策略（私有：宿主只能经 [`UnconditionalSinkSet`] 使用）。
#[derive(Debug, Clone, Copy)]
enum SinkPolicy {
    /// 不计耗时，仅隔离 panic（panic 后由自愈重注册）。
    LocalPersist,
    /// `budget` 内正常返回视为 Ok；超预算判 Slow（保留并边沿告警）。
    LocalBounded { budget: Duration },
}

impl SinkPolicy {
    fn kind(self) -> SinkKind {
        match self {
            SinkPolicy::LocalPersist => SinkKind::LocalPersist,
            SinkPolicy::LocalBounded { .. } => SinkKind::LocalBounded,
        }
    }
}

/// 单次回调的隔离结果（无条件 sink 专用；网络路由仍用 [`NotifyOutcome`]）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkOutcome {
    Ok,
    /// 正常返回但耗时达到/超过预算。
    Slow {
        elapsed: Duration,
        budget: Duration,
    },
    /// panic（已被 catch_unwind 吞住）。
    Panicked,
}

/// 按策略在隔离环境中同步通知一个无条件 sink。
fn notify_with_policy(sink: &dyn BarSink, bar: &Kline, policy: SinkPolicy) -> SinkOutcome {
    match policy {
        // 持久化 sink 纯内存、无 IO：偶发 >1ms 属正常抖动，绝不能因此摘除。
        SinkPolicy::LocalPersist => match catch_unwind(AssertUnwindSafe(|| sink.on_bar(bar))) {
            Ok(()) => SinkOutcome::Ok,
            Err(_) => SinkOutcome::Panicked,
        },
        SinkPolicy::LocalBounded { budget } => {
            let started = Instant::now();
            let result = catch_unwind(AssertUnwindSafe(|| sink.on_bar(bar)));
            let elapsed = started.elapsed();
            match result {
                Err(_) => SinkOutcome::Panicked,
                Ok(()) if elapsed >= budget => SinkOutcome::Slow { elapsed, budget },
                Ok(()) => SinkOutcome::Ok,
            }
        }
    }
}

/// 慢态边沿纯函数：返回 `(新慢态, 是否打超时告警, 是否打恢复日志)`。
///
/// 只在状态跳变时告警/恢复，持续慢态不刷屏；即将摘除的 panic 不误报“恢复”。
fn slow_edge(was_slow: bool, outcome: &SinkOutcome) -> (bool, bool, bool) {
    match outcome {
        SinkOutcome::Slow { .. } => (true, !was_slow, false),
        SinkOutcome::Ok => (false, false, was_slow),
        SinkOutcome::Panicked => (false, false, false),
    }
}

/// 一条已注册的无条件 sink（策略与慢态私有）。
struct SinkEntry {
    sink: Arc<dyn BarSink>,
    policy: SinkPolicy,
    was_slow: bool,
}

/// 无条件 bar 下游集合：策略、边沿告警、panic 摘除与持久化自愈的唯一实现。
///
/// 宿主（[`HubRegistry`] / `ctp::bar_agg::BarAggSink`）只调用注册/派发入口，
/// 看不到预算与策略，避免各自复制一套“慢即摘除”逻辑。
pub(crate) struct UnconditionalSinkSet {
    entries: Vec<SinkEntry>,
    /// 持久化 sink 模板：panic 摘除后据此在冷却结束时自愈重注册。
    persistent: Option<Arc<dyn BarSink>>,
    local_budget: Duration,
    heal_cooldown: Duration,
    /// 上次自愈尝试时刻（仅成功的重注册会刷新，用于冷却判定）。
    last_heal: Option<Instant>,
}

impl UnconditionalSinkSet {
    pub(crate) fn new() -> Self {
        Self {
            entries: Vec::new(),
            persistent: None,
            local_budget: LOCAL_SINK_BUDGET,
            heal_cooldown: PERSIST_HEAL_COOLDOWN,
            last_heal: None,
        }
    }

    /// 注册本地普通无条件 sink：超预算仅边沿告警，不摘除。
    pub(crate) fn register(&mut self, sink: Arc<dyn BarSink>) {
        self.entries.push(SinkEntry {
            sink,
            policy: SinkPolicy::LocalBounded {
                budget: self.local_budget,
            },
            was_slow: false,
        });
    }

    /// 注册本地持久化 sink（不计耗时，panic 后自愈）；已存在**活跃**持久化项时返回
    /// `false` 且不重复挂载，保证每根 bar 只落一次。被 panic 摘除后再次调用会立即
    /// 重注册（绕过自愈冷却；当前装配为 OnceLock 一次性调用，不应依赖此路径）。
    pub(crate) fn register_persistent(&mut self, sink: Arc<dyn BarSink>) -> bool {
        if self.has_persistent() {
            return false;
        }
        self.persistent = Some(Arc::clone(&sink));
        self.entries.push(SinkEntry {
            sink,
            policy: SinkPolicy::LocalPersist,
            was_slow: false,
        });
        true
    }

    /// 当前是否有活跃的持久化 sink（被 panic 摘除后为 `false`，直到自愈）。
    pub(crate) fn has_persistent(&self) -> bool {
        self.entries
            .iter()
            .any(|e| e.policy.kind() == SinkKind::LocalPersist)
    }

    /// 立即尝试恢复被 panic 摘除的持久化 sink（绕过冷却；仅测试用）。
    /// 已有活跃持久化 sink 或从未注册过模板时返回 `false`（幂等无动作）。
    #[cfg(test)]
    pub(crate) fn heal_persistent_now(&mut self) -> bool {
        self.heal_persistent(true)
    }

    /// 通知全部无条件 sink；返回本轮因 panic 被摘除的数量。
    ///
    /// 入口先做持久化自愈（冷却结束后的第一根 bar 不丢），再逐项按策略回调：
    /// Slow 仅边沿告警、保留；Panicked 摘除（持久化模板保留待下次自愈）。
    pub(crate) fn notify_all(&mut self, bar: &Kline) -> usize {
        self.heal_persistent(false);

        let mut survivors = Vec::with_capacity(self.entries.len());
        let mut evicted = 0usize;
        for mut entry in self.entries.drain(..) {
            let kind = entry.policy.kind();
            let outcome = notify_with_policy(entry.sink.as_ref(), bar, entry.policy);
            let (was_slow, alarm, recovered) = slow_edge(entry.was_slow, &outcome);
            entry.was_slow = was_slow;

            match outcome {
                SinkOutcome::Slow { elapsed, budget } => {
                    if alarm {
                        tracing::warn!(
                            kind = kind.as_str(),
                            ?elapsed,
                            ?budget,
                            "无条件 bar sink 回调超时（仅告警不摘除）"
                        );
                    }
                }
                SinkOutcome::Ok => {
                    if recovered {
                        tracing::info!(kind = kind.as_str(), "无条件 bar sink 回调耗时恢复正常");
                    }
                }
                SinkOutcome::Panicked => {
                    evicted += 1;
                    tracing::warn!(
                        kind = kind.as_str(),
                        "无条件 bar sink panic，已摘除（持久化 sink 将由自愈重新注册）"
                    );
                    continue;
                }
            }
            survivors.push(entry);
        }
        self.entries = survivors;
        evicted
    }

    /// 持久化 sink 缺失时按冷却自愈；`force` 绕过冷却（显式诊断/测试）。
    fn heal_persistent(&mut self, force: bool) -> bool {
        let Some(template) = self.persistent.clone() else {
            return false;
        };
        if self.has_persistent() {
            return false;
        }
        let now = Instant::now();
        let due = force
            || match self.last_heal {
                None => true,
                Some(at) => now.duration_since(at) >= self.heal_cooldown,
            };
        if !due {
            return false;
        }
        self.last_heal = Some(now);
        self.entries.push(SinkEntry {
            sink: template,
            policy: SinkPolicy::LocalPersist,
            was_slow: false,
        });
        tracing::error!("持久化 bar sink 缺失（疑似被 panic 摘除），已重新注册");
        true
    }

    /// 仅测试可注入本地预算（生产预算固定在 [`LOCAL_SINK_BUDGET`]）。
    #[cfg(test)]
    fn with_budgets(mut self, local: Duration) -> Self {
        self.local_budget = local;
        self
    }

    /// 仅测试可注入自愈冷却。
    #[cfg(test)]
    fn with_heal_cooldown(mut self, cooldown: Duration) -> Self {
        self.heal_cooldown = cooldown;
        self
    }
}

impl Default for UnconditionalSinkSet {
    fn default() -> Self {
        Self::new()
    }
}

/// hub 注册表句柄（每连接一个）：登记消费者并返回 drop 即退订的 guard。
#[derive(Clone)]
pub struct HubRegistry {
    core: Arc<Mutex<HubCore>>,
}

impl HubRegistry {
    pub fn new() -> Self {
        Self {
            core: Arc::new(Mutex::new(HubCore::new())),
        }
    }

    /// 构造「drop 即按 id 注销」的凭证（闭包只做一次短临界区内存摘除）。
    fn removal_guard(&self, id: u64) -> SubscriptionGuard {
        let core = Arc::clone(&self.core);
        SubscriptionGuard::new(Box::new(move || {
            if let Ok(mut g) = core.lock() {
                g.remove(id);
            }
        }))
    }

    /// 登记一个系统行情来源；guard drop 即移除。
    pub fn register_prices(&self, src: Arc<dyn DesiredSource>) -> SubscriptionGuard {
        let id = self
            .core
            .lock()
            .expect("hub core lock")
            .add_price_source(src);
        self.removal_guard(id)
    }

    /// 登记一个 K线消费者；guard drop 即移除（其独占符号下轮对账退订）。
    pub fn register_klines(
        &self,
        src: Arc<dyn DesiredSource>,
        sink: Arc<dyn BarSink>,
    ) -> SubscriptionGuard {
        let id = self
            .core
            .lock()
            .expect("hub core lock")
            .add_bar_client(src, sink);
        self.removal_guard(id)
    }

    /// 注册本地无条件 bar 下游（全量持久化之外的其他本地 sink）：此后每根定稿 bar
    /// （含零订阅标的）都不经 symbol 路由地通知它；不贡献订阅并集、无注销凭证
    /// （进程寿命，装配期注册一次）。超预算仅边沿告警不摘除，panic 摘除。
    pub fn register_unconditional_bar_sink(&self, sink: Arc<dyn BarSink>) {
        self.unconditional_set()
            .lock()
            .expect("unconditional sinks lock")
            .register(sink);
    }

    /// 注册**持久化**无条件下游（LocalPersist）：纯内存、不计耗时，panic 摘除后自愈。
    /// 重复注册返回 `false`（幂等），保证每根 bar 只落一次。
    pub fn register_persistent_bar_sink(&self, sink: Arc<dyn BarSink>) -> bool {
        self.unconditional_set()
            .lock()
            .expect("unconditional sinks lock")
            .register_persistent(sink)
    }

    /// 取出无条件 sink 集合句柄（短临界区；随后在注册表锁外调用）。
    fn unconditional_set(&self) -> Arc<Mutex<UnconditionalSinkSet>> {
        Arc::clone(&self.core.lock().expect("hub core lock").unconditional)
    }

    /// 当前全局期望并集（价格 ∪ K线；CTP 单连接等「频道即合约」场景用）。
    pub fn desired_symbols(&self) -> BTreeSet<String> {
        self.core.lock().expect("hub core lock").desired_union()
    }

    /// 仅系统行情来源的期望并集（WS 三家映射 ticker 类频道用）。
    pub fn desired_price_symbols(&self) -> BTreeSet<String> {
        self.core
            .lock()
            .expect("hub core lock")
            .desired_price_symbols()
    }

    /// 仅 K线消费者来源的期望并集（WS 三家映射 K线频道用）。
    pub fn desired_bar_symbols(&self) -> BTreeSet<String> {
        self.core
            .lock()
            .expect("hub core lock")
            .desired_bar_symbols()
    }

    /// 定稿 bar 派发：先通知无条件下游（内部按持久化/本地策略隔离），再按 symbol 路由。
    ///
    /// 路由消费者 panic/超 `budget` 当场摘除（南向背压保护）；无条件下游的慢/panic
    /// 策略由 [`UnconditionalSinkSet`] 私有处理。返回被摘除的 sink 数（供计数/日志）。
    ///
    /// 全程「锁内只取 Arc 列表、释锁后回调、再上锁摘除」，绝不在持注册表锁时调用
    /// 外部 sink；无条件集合自带独立锁，回调期间不占用注册表锁。
    pub fn dispatch_bar(&self, bar: &Kline, budget: Duration) -> usize {
        // 短临界区：取无条件集合句柄与本轮命中的路由 sink 句柄，释锁后再回调。
        let (unconditional, targets) = {
            let core = self.core.lock().expect("hub core lock");
            (
                Arc::clone(&core.unconditional),
                core.bar_sinks_for(&bar.symbol),
            )
        };
        let routed_n = targets.len();
        // 只读快照用于日志；持久化注册是装配期一次性的，期间不会变化。
        let persistent_sink = unconditional
            .lock()
            .expect("unconditional sinks lock")
            .has_persistent();
        tracing::debug!(
            venue = %bar.venue,
            symbol = %bar.symbol,
            trading_day = bar.trading_day,
            start_ms = bar.start_ms,
            close = %bar.close,
            volume = bar.volume,
            routed = routed_n,
            persistent_sink,
            "定稿 bar 派发（落盘无条件 + 按订阅路由推送）"
        );

        // 释锁回调。先无条件下游（全量落盘），再路由消费者（南向 fan-out），
        // 顺序与 CTP BarAggSink「先持久化、后路由」一致。
        // notify_all 内部完成策略隔离、边沿告警、panic 摘除与持久化自愈。
        let mut evicted = {
            let mut set = unconditional.lock().expect("unconditional sinks lock");
            set.notify_all(bar)
        };

        // 路由 sink 逐个隔离，记录坏/慢消费者 id。
        let mut evicted_ids = Vec::new();
        for (id, sink) in targets {
            match safe_notify(sink.as_ref(), bar, budget) {
                NotifyOutcome::Ok => {}
                NotifyOutcome::Panicked | NotifyOutcome::Slow => {
                    evicted += 1;
                    evicted_ids.push(id);
                }
            }
        }
        // 再上锁：摘除坏/慢路由消费者（被摘消费者独占的符号在下轮对账自然退订）。
        if !evicted_ids.is_empty() {
            if let Ok(mut core) = self.core.lock() {
                for id in &evicted_ids {
                    core.remove(*id);
                }
            }
        }
        evicted
    }
}

impl Default for HubRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::{FixedSymbols, KlineInterval, PriceBasis, PriceTick};
    use rust_decimal::Decimal;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// 可变来源：背后一个可改写的 Vec，验证并集随时间变化。
    #[derive(Clone)]
    struct MutableSource {
        syms: Arc<Mutex<Vec<String>>>,
    }
    impl MutableSource {
        fn new(syms: &[&str]) -> Self {
            Self {
                syms: Arc::new(Mutex::new(syms.iter().map(|s| s.to_string()).collect())),
            }
        }
        fn set(&self, syms: &[&str]) {
            *self.syms.lock().unwrap() = syms.iter().map(|s| s.to_string()).collect();
        }
    }
    impl DesiredSource for MutableSource {
        fn symbols(&self) -> Vec<String> {
            self.syms.lock().unwrap().clone()
        }
    }

    /// 记录收到的 bar 的正常 sink。
    struct RecSink {
        seen: Mutex<Vec<String>>,
    }
    impl RecSink {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                seen: Mutex::new(Vec::new()),
            })
        }
        fn seen(&self) -> Vec<String> {
            self.seen.lock().unwrap().clone()
        }
    }
    impl BarSink for RecSink {
        fn on_bar(&self, bar: &Kline) {
            self.seen.lock().unwrap().push(bar.symbol.clone());
        }
    }

    struct PanicSink;
    impl BarSink for PanicSink {
        fn on_bar(&self, _bar: &Kline) {
            panic!("sink boom");
        }
    }

    struct SlowSink;
    impl BarSink for SlowSink {
        fn on_bar(&self, _bar: &Kline) {
            std::thread::sleep(Duration::from_millis(40));
        }
    }

    /// 可配置延时 + 记录收到的符号（本地 sink 策略测试用）。
    struct SleepSink {
        delay: Duration,
        seen: Mutex<Vec<String>>,
    }
    impl SleepSink {
        fn new(delay: Duration) -> Arc<Self> {
            Arc::new(Self {
                delay,
                seen: Mutex::new(Vec::new()),
            })
        }
        fn seen(&self) -> Vec<String> {
            self.seen.lock().unwrap().clone()
        }
    }
    impl BarSink for SleepSink {
        fn on_bar(&self, bar: &Kline) {
            std::thread::sleep(self.delay);
            self.seen.lock().unwrap().push(bar.symbol.clone());
        }
    }

    /// 可切换 panic 的持久化 sink（panic 摘除 + 自愈测试用）；正常时记录收到的符号。
    struct FlakySink {
        panic: AtomicBool,
        seen: Mutex<Vec<String>>,
    }
    impl FlakySink {
        fn new(panic: bool) -> Arc<Self> {
            Arc::new(Self {
                panic: AtomicBool::new(panic),
                seen: Mutex::new(Vec::new()),
            })
        }
        fn set_panic(&self, on: bool) {
            self.panic.store(on, Ordering::SeqCst);
        }
        fn seen(&self) -> Vec<String> {
            self.seen.lock().unwrap().clone()
        }
    }
    impl BarSink for FlakySink {
        fn on_bar(&self, bar: &Kline) {
            if self.panic.load(Ordering::SeqCst) {
                panic!("flaky sink boom");
            }
            self.seen.lock().unwrap().push(bar.symbol.clone());
        }
    }

    fn bar(symbol: &str) -> Kline {
        Kline {
            venue: "ctp".into(),
            symbol: symbol.into(),
            interval: KlineInterval::M1,
            trading_day: 20261023,
            start_ms: 0,
            end_ms: 60_000,
            open: Decimal::ONE,
            high: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::ONE,
            volume: 1,
            quote_volume: Decimal::ONE,
            extras: std::collections::BTreeMap::new(),
        }
    }

    fn set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    // ---- SubsPlan ----

    #[test]
    fn plan_diffs_sorted_add_and_remove() {
        let plan = SubsPlan::diff(&set(&["b", "a"]), &set(&["c", "b"]));
        assert_eq!(plan.to_sub, vec!["a".to_string()]);
        assert_eq!(plan.to_unsub, vec!["c".to_string()]);
        assert!(!plan.is_empty());
    }

    #[test]
    fn plan_no_change_is_empty() {
        let s = set(&["a", "b"]);
        assert!(SubsPlan::diff(&s, &s).is_empty());
    }

    #[test]
    fn plan_reconnect_against_empty_replays_all() {
        // 重连后 subscribed 清空：期望集全部进 to_sub（整体重放）。
        let plan = SubsPlan::diff(&set(&["c", "a", "b"]), &set(&[]));
        assert_eq!(plan.to_sub, vec!["a", "b", "c"]);
        assert!(plan.to_unsub.is_empty());
    }

    // ---- 注册表并集 ----

    #[test]
    fn union_combines_price_and_bar_sources_dedup_sorted() {
        let reg = HubRegistry::new();
        let _g1 = reg.register_prices(Arc::new(FixedSymbols::new(vec!["b".into(), "a".into()])));
        let _g2 = reg.register_klines(
            Arc::new(FixedSymbols::new(vec!["c".into(), "b".into()])),
            RecSink::new(),
        );
        assert_eq!(
            reg.desired_symbols().into_iter().collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
    }

    #[test]
    fn union_tracks_dynamic_source_changes() {
        let src = MutableSource::new(&["a"]);
        let reg = HubRegistry::new();
        let _g = reg.register_prices(Arc::new(src.clone()));
        assert_eq!(
            reg.desired_symbols().into_iter().collect::<Vec<_>>(),
            vec!["a"]
        );
        // 来源内容变化（兴趣集周期轮询）：下轮并集随之变化，无需重新登记。
        src.set(&["a", "x"]);
        assert_eq!(
            reg.desired_symbols().into_iter().collect::<Vec<_>>(),
            vec!["a", "x"]
        );
    }

    #[test]
    fn dropping_guard_removes_consumer_and_its_exclusive_symbols() {
        let reg = HubRegistry::new();
        let g_price = reg.register_prices(Arc::new(FixedSymbols::new(vec!["a".into()])));
        let g_bar = reg.register_klines(
            Arc::new(FixedSymbols::new(vec!["b".into()])),
            RecSink::new(),
        );
        assert_eq!(reg.desired_symbols().len(), 2);
        drop(g_bar);
        assert_eq!(
            reg.desired_symbols().into_iter().collect::<Vec<_>>(),
            vec!["a"]
        );
        drop(g_price);
        assert!(reg.desired_symbols().is_empty());
    }

    #[test]
    fn split_unions_separate_price_and_bar_symbols() {
        let reg = HubRegistry::new();
        let _gp = reg.register_prices(Arc::new(FixedSymbols::new(vec!["p".into(), "both".into()])));
        let _gb = reg.register_klines(
            Arc::new(FixedSymbols::new(vec!["k".into(), "both".into()])),
            RecSink::new(),
        );
        // 价格/K线并集各自独立，重叠符号两边都在；总并集去重。
        assert_eq!(
            reg.desired_price_symbols().into_iter().collect::<Vec<_>>(),
            vec!["both", "p"]
        );
        assert_eq!(
            reg.desired_bar_symbols().into_iter().collect::<Vec<_>>(),
            vec!["both", "k"]
        );
        assert_eq!(
            reg.desired_symbols().into_iter().collect::<Vec<_>>(),
            vec!["both", "k", "p"]
        );
    }

    // ---- 路由 ----

    #[test]
    fn bar_routed_only_to_clients_listing_its_symbol() {
        let reg = HubRegistry::new();
        let s1 = RecSink::new();
        let s2 = RecSink::new();
        let _g1 = reg.register_klines(Arc::new(FixedSymbols::new(vec!["x".into()])), s1.clone());
        let _g2 = reg.register_klines(
            Arc::new(FixedSymbols::new(vec!["x".into(), "y".into()])),
            s2.clone(),
        );
        // y 只命中 s2。
        assert_eq!(reg.dispatch_bar(&bar("y"), Duration::from_secs(1)), 0);
        assert_eq!(s1.seen(), Vec::<String>::new());
        assert_eq!(s2.seen(), vec!["y".to_string()]);
        // x 命中两者（重叠符号各收一份）。
        reg.dispatch_bar(&bar("x"), Duration::from_secs(1));
        assert_eq!(s1.seen(), vec!["x".to_string()]);
        assert_eq!(s2.seen(), vec!["y".to_string(), "x".to_string()]);
    }

    // ---- 无条件 sink（全量持久化） ----

    #[test]
    fn unconditional_sink_receives_every_bar_without_any_bar_client() {
        // 没有任何按 symbol 注册的 K线消费者：无条件 sink 仍收到每根定稿 bar
        // （「凡线订阅标的一律全量落盘」零客户端也成立）。
        let reg = HubRegistry::new();
        let persist = RecSink::new();
        reg.register_unconditional_bar_sink(persist.clone() as Arc<dyn BarSink>);

        assert_eq!(reg.desired_bar_symbols().len(), 0);
        assert_eq!(reg.dispatch_bar(&bar("btcusdt"), Duration::from_secs(1)), 0);
        assert_eq!(persist.seen(), vec!["btcusdt".to_string()]);

        // 再来一根不同符号的 bar，无条件 sink 照样收到。
        reg.dispatch_bar(&bar("ethusdt"), Duration::from_secs(1));
        assert_eq!(persist.seen(), vec!["btcusdt", "ethusdt"]);
    }

    #[test]
    fn bar_delivered_to_both_routed_clients_and_unconditional_sink() {
        // 同一路由：按 symbol 命中的消费者与无条件 sink 各收一份（落盘/推送正交）。
        let reg = HubRegistry::new();
        let routed = RecSink::new();
        let persist = RecSink::new();
        let _g = reg.register_klines(
            Arc::new(FixedSymbols::new(vec!["x".into()])),
            routed.clone(),
        );
        reg.register_unconditional_bar_sink(persist.clone() as Arc<dyn BarSink>);

        reg.dispatch_bar(&bar("x"), Duration::from_secs(1));
        assert_eq!(routed.seen(), vec!["x".to_string()]);
        assert_eq!(persist.seen(), vec!["x".to_string()]);

        // 无人按 symbol 订阅的 bar：路由客户不收，无条件 sink 仍收。
        reg.dispatch_bar(&bar("y"), Duration::from_secs(1));
        assert_eq!(routed.seen(), vec!["x".to_string()]);
        assert_eq!(persist.seen(), vec!["x", "y"]);
    }

    #[test]
    fn panicky_unconditional_sink_is_evicted_without_blocking_others() {
        // 无条件 sink panic：被 catch_unwind 吞住并摘除（摘除计数 1）；
        // 另一正常无条件 sink 与按 symbol 路由的消费者全程不受影响。
        let reg = HubRegistry::new();
        let routed = RecSink::new();
        let good = RecSink::new();
        let _g = reg.register_klines(
            Arc::new(FixedSymbols::new(vec!["x".into()])),
            routed.clone(),
        );
        reg.register_unconditional_bar_sink(Arc::new(PanicSink));
        reg.register_unconditional_bar_sink(good.clone() as Arc<dyn BarSink>);

        assert_eq!(reg.dispatch_bar(&bar("x"), Duration::from_secs(1)), 1);
        assert_eq!(routed.seen(), vec!["x".to_string()]);
        assert_eq!(good.seen(), vec!["x".to_string()]);

        // 第二轮：panic sink 已不在表中（摘除计数 0），其余两者继续收 bar。
        assert_eq!(reg.dispatch_bar(&bar("x"), Duration::from_secs(1)), 0);
        assert_eq!(routed.seen(), vec!["x", "x"]);
        assert_eq!(good.seen(), vec!["x", "x"]);
    }

    // ---- 无条件 sink 策略（UnconditionalSinkSet） ----

    fn slow_outcome() -> SinkOutcome {
        SinkOutcome::Slow {
            elapsed: Duration::from_millis(2),
            budget: Duration::from_millis(1),
        }
    }

    #[test]
    fn slow_edge_fires_only_on_transitions() {
        // 进入慢态：保留 sink，打一次告警。
        assert_eq!(slow_edge(false, &slow_outcome()), (true, true, false));
        // 持续慢态：保持状态，不重复告警。
        assert_eq!(slow_edge(true, &slow_outcome()), (true, false, false));
        // 恢复正常：清状态并打一次恢复日志。
        assert_eq!(slow_edge(true, &SinkOutcome::Ok), (false, false, true));
        assert_eq!(slow_edge(false, &SinkOutcome::Ok), (false, false, false));
        // 即将摘除：不把“被摘除”误报成“恢复”。
        assert_eq!(
            slow_edge(true, &SinkOutcome::Panicked),
            (false, false, false)
        );
    }

    #[test]
    fn slow_local_sink_is_kept_and_stays_slow() {
        let sink = SleepSink::new(Duration::from_millis(2));
        let mut set = UnconditionalSinkSet::new().with_budgets(Duration::from_millis(1));
        set.register(sink.clone() as Arc<dyn BarSink>);

        // 超过注入预算：不摘除，仅置慢态（告警由 slow_edge 的上升沿决定）。
        assert_eq!(set.notify_all(&bar("x")), 0);
        assert_eq!(set.entries.len(), 1);
        assert!(set.entries[0].was_slow);
        assert_eq!(sink.seen(), vec!["x".to_string()]);

        // 慢态内再慢：不重复告警、仍保留。
        assert_eq!(set.notify_all(&bar("x")), 0);
        assert!(set.entries[0].was_slow);
        assert_eq!(sink.seen().len(), 2);
    }

    #[test]
    fn persistent_sink_bypasses_timing_check() {
        let sink = SleepSink::new(Duration::from_millis(5));
        let mut set = UnconditionalSinkSet::new().with_budgets(Duration::from_millis(1));
        assert!(set.register_persistent(sink.clone() as Arc<dyn BarSink>));

        // 5ms 远超注入的 1ms 本地预算，但 LocalPersist 不测耗时：不摘除、不置慢态。
        assert_eq!(set.notify_all(&bar("x")), 0);
        assert_eq!(sink.seen(), vec!["x".to_string()]);
        assert!(set.entries.iter().all(|e| !e.was_slow));
        assert!(set.has_persistent());
    }

    #[test]
    fn persistent_registration_is_idempotent() {
        let sink = RecSink::new();
        let mut set = UnconditionalSinkSet::new();
        assert!(set.register_persistent(sink.clone() as Arc<dyn BarSink>));
        // 同一 sink 重复注册被忽略（避免装配多次导致每根 bar 重复落盘）。
        assert!(!set.register_persistent(sink.clone() as Arc<dyn BarSink>));
        assert_eq!(set.notify_all(&bar("x")), 0);
        assert_eq!(sink.seen(), vec!["x".to_string()]);
    }

    #[test]
    fn panic_evicts_persistent_sink_and_heal_restores() {
        let sink = FlakySink::new(true);
        let mut set = UnconditionalSinkSet::new().with_heal_cooldown(Duration::ZERO);
        assert!(set.register_persistent(sink.clone() as Arc<dyn BarSink>));

        // 第一次：panic 被吞住并摘除（模板保留待自愈）。
        assert_eq!(set.notify_all(&bar("x")), 1);
        assert!(!set.has_persistent());

        // 恢复后下一拍入口自愈：冷却为 0，先重注册再派发，首根 bar 不丢。
        sink.set_panic(false);
        assert_eq!(set.notify_all(&bar("y")), 0);
        assert!(set.has_persistent());
        assert_eq!(sink.seen(), vec!["y".to_string()]);
    }

    #[test]
    fn heal_cooldown_bounds_panic_loop() {
        let sink = FlakySink::new(true);
        let mut set = UnconditionalSinkSet::new().with_heal_cooldown(Duration::from_secs(3600));
        assert!(set.register_persistent(sink.clone() as Arc<dyn BarSink>));

        // 首根：原注册项 panic 摘除。
        assert_eq!(set.notify_all(&bar("x")), 1);
        // 次拍：首次自愈（无历史冷却）重注册后再次 panic，摘除 1 个。
        assert_eq!(set.notify_all(&bar("x")), 1);
        // 第三拍：冷却未到，不再重试（避免持续 panic 刷屏/抖动）。
        assert_eq!(set.notify_all(&bar("x")), 0);
        assert!(!set.has_persistent());
    }

    #[test]
    fn heal_persistent_now_bypasses_cooldown() {
        let sink = FlakySink::new(true);
        let mut set = UnconditionalSinkSet::new().with_heal_cooldown(Duration::from_secs(3600));
        assert!(set.register_persistent(sink.clone() as Arc<dyn BarSink>));
        assert_eq!(set.notify_all(&bar("x")), 1);
        // 首次入口自愈后进入冷却；随后再次 panic 摘除。
        assert_eq!(set.notify_all(&bar("x")), 1);
        assert!(!set.has_persistent());

        // 显式自愈绕过冷却；已存在时幂等无动作。
        sink.set_panic(false);
        assert!(set.heal_persistent_now());
        assert!(set.has_persistent());
        assert!(!set.heal_persistent_now());
        assert_eq!(set.notify_all(&bar("y")), 0);
        assert_eq!(sink.seen(), vec!["y".to_string()]);
    }

    #[test]
    fn slow_local_sink_never_evicted_over_many_bars() {
        let sink = SleepSink::new(Duration::from_millis(2));
        let mut set = UnconditionalSinkSet::new().with_budgets(Duration::from_millis(1));
        set.register(sink.clone() as Arc<dyn BarSink>);
        for _ in 0..10 {
            assert_eq!(set.notify_all(&bar("x")), 0);
        }
        assert_eq!(sink.seen().len(), 10);
    }

    #[test]
    fn persistent_registration_reaches_dispatch_without_timing_eviction() {
        // 装配入口 → dispatch 全链路：LocalPersist 超 1ms 预算也不被摘除。
        let reg = HubRegistry::new();
        let sink = SleepSink::new(Duration::from_millis(5));
        assert!(reg.register_persistent_bar_sink(sink.clone() as Arc<dyn BarSink>));
        assert!(!reg.register_persistent_bar_sink(sink.clone() as Arc<dyn BarSink>));

        assert_eq!(reg.dispatch_bar(&bar("x"), Duration::from_millis(1)), 0);
        assert_eq!(reg.dispatch_bar(&bar("y"), Duration::from_millis(1)), 0);
        assert_eq!(sink.seen(), vec!["x".to_string(), "y".to_string()]);
    }

    #[test]
    fn routed_slow_sink_is_still_evicted() {
        // 南向网络路由消费者保持严格：超预算仍摘除（背压保护）。
        let reg = HubRegistry::new();
        let slow = SleepSink::new(Duration::from_millis(5));
        let _g = reg.register_klines(
            Arc::new(FixedSymbols::new(vec!["x".into()])),
            slow.clone() as Arc<dyn BarSink>,
        );

        assert_eq!(reg.dispatch_bar(&bar("x"), Duration::from_millis(1)), 1);
        assert_eq!(slow.seen(), vec!["x".to_string()]);
        // 已摘除：下一次不再回调。
        assert_eq!(reg.dispatch_bar(&bar("x"), Duration::from_millis(1)), 0);
        assert_eq!(slow.seen().len(), 1);
    }

    // ---- 异常 sink 摘除 ----

    #[test]
    fn panicked_sink_is_evicted_others_keep_working() {
        let reg = HubRegistry::new();
        let good = RecSink::new();
        let _gg = reg.register_klines(Arc::new(FixedSymbols::new(vec!["x".into()])), good.clone());
        let _gp = reg.register_klines(
            Arc::new(FixedSymbols::new(vec!["x".into()])),
            Arc::new(PanicSink),
        );
        // 第一次派发：panic sink 被吞住并摘除（摘除计数 1），good 仍收到。
        let evicted = reg.dispatch_bar(&bar("x"), Duration::from_secs(1));
        assert_eq!(evicted, 1);
        assert_eq!(good.seen(), vec!["x".to_string()]);
        // 第二次：panic sink 已不在注册表（其独占符号也消失），只剩 good。
        let evicted2 = reg.dispatch_bar(&bar("x"), Duration::from_secs(1));
        assert_eq!(evicted2, 0);
        assert_eq!(good.seen(), vec!["x".to_string(), "x".to_string()]);
    }

    #[test]
    fn slow_sink_is_evicted_when_over_budget() {
        let outcome = safe_notify(&SlowSink, &bar("x"), Duration::from_millis(10));
        assert_eq!(outcome, NotifyOutcome::Slow);
        let good: Arc<RecSink> = RecSink::new();
        let outcome_ok = safe_notify(good.as_ref(), &bar("x"), Duration::from_millis(10));
        assert_eq!(outcome_ok, NotifyOutcome::Ok);
    }

    #[test]
    fn safe_notify_catches_panic() {
        assert_eq!(
            safe_notify(&PanicSink, &bar("x"), Duration::from_secs(1)),
            NotifyOutcome::Panicked
        );
    }

    // 钉住 PriceTick 携带 basis/volume。
    #[test]
    fn price_tick_carries_basis_and_volume() {
        let t = PriceTick {
            symbol: "rb".into(),
            basis: PriceBasis::Last,
            price: Decimal::new(3800, 0),
            best_bid: None,
            best_ask: None,
            volume: Some(Decimal::new(1000, 0)),
            event_ms: 1,
        };
        assert_eq!(t.basis, PriceBasis::Last);
        assert_eq!(t.volume, Some(Decimal::new(1000, 0)));
    }

    #[test]
    fn dispatch_evicts_all_bad_sinks_in_one_round() {
        // 两个坏 sink 同符号：一次派发摘除 2 个，注册表清空。
        let reg = HubRegistry::new();
        let _g1 = reg.register_klines(
            Arc::new(FixedSymbols::new(vec!["x".into()])),
            Arc::new(PanicSink),
        );
        let _g2 = reg.register_klines(
            Arc::new(FixedSymbols::new(vec!["x".into()])),
            Arc::new(PanicSink),
        );
        assert_eq!(reg.dispatch_bar(&bar("x"), Duration::from_secs(1)), 2);
        assert!(reg.desired_symbols().is_empty());
    }
}
