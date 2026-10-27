//! 行情 hub 纯内核：消费者注册表 / 全局并集 / 订阅 diff / 按 symbol 路由 / 异常 sink 摘除。
//!
//! 本模块**不含任何 IO、连接、async、channel**：只做登记与纯函数式的集合运算与同步
//! 回调隔离。WS 驱动（[`crate::market_ws`]）与 CTP 单连接（[`crate::ctp::market`]
//! 的 drain 循环）各持有一个 [`HubRegistry`]（`Arc<Mutex<HubCore>>`），周期锁读并集做
//! 订阅增减，收 tick/bar 时按 symbol 查注册表同步回调。锁内不回调、不 IO：驱动先在
//! 短临界区内取出当次回调列表，释锁后再调用 sink。

use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::Hash;
use std::panic::AssertUnwindSafe;
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
    unconditional: Vec<Arc<dyn BarSink>>,
}

impl HubCore {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            price_sources: HashMap::new(),
            bar_clients: HashMap::new(),
            unconditional: Vec::new(),
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
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| sink.on_bar(bar)));
    let elapsed = started.elapsed();
    match result {
        Err(_) => NotifyOutcome::Panicked,
        Ok(()) if elapsed >= budget => NotifyOutcome::Slow,
        Ok(()) => NotifyOutcome::Ok,
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

    /// 注册无条件 bar 下游（全量持久化）：此后每根定稿 bar（含零订阅标的）都不经
    /// symbol 路由地通知它；不贡献订阅并集、无注销凭证（进程寿命，装配期注册一次）。
    /// panic/超 budget 的 sink 在 [`Self::dispatch_bar`] 内被隔离摘除，不牵连路由
    /// 消费者与其它无条件 sink。
    pub fn register_unconditional_bar_sink(&self, sink: Arc<dyn BarSink>) {
        self.core
            .lock()
            .expect("hub core lock")
            .unconditional
            .push(sink);
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

    /// 定稿 bar 按 symbol 路由：对每个命中的 K线消费者做隔离回调，
    /// panic/超时的消费者当场摘除；返回被摘除的消费者数（供计数/日志）。
    ///
    /// 全程「锁内只取 Arc 列表、释锁后回调、再上锁摘除」，绝不在持锁时调用外部 sink。
    pub fn dispatch_bar(&self, bar: &Kline, budget: Duration) -> usize {
        // 短临界区：只克隆无条件 sink 列表与本轮命中的路由 sink 句柄（连同计数），
        // 日志移到锁外，与本模块「锁内不做外部动作」的纪律一致。
        let (unconditional, targets, routed_n, persist_n) = {
            let core = self.core.lock().expect("hub core lock");
            let targets = core.bar_sinks_for(&bar.symbol);
            let persist_n = core.unconditional.len();
            let routed_n = targets.len();
            (core.unconditional.clone(), targets, routed_n, persist_n)
        };
        tracing::debug!(
            venue = %bar.venue,
            symbol = %bar.symbol,
            trading_day = bar.trading_day,
            start_ms = bar.start_ms,
            close = %bar.close,
            volume = bar.volume,
            routed = routed_n,
            unconditional = persist_n,
            "定稿 bar 派发（落盘无条件 + 按订阅路由推送）"
        );
        // 释锁回调。先无条件下游（全量落盘），再路由消费者（南向 fan-out），
        // 顺序与 CTP BarAggSink「先持久化、后路由」一致。
        // 无条件 sink 逐个隔离：坏/慢者不进 survivors（摘除），其余留下轮再用。
        let mut survivors: Vec<Arc<dyn BarSink>> = Vec::with_capacity(unconditional.len());
        let mut evicted = 0usize;
        for sink in unconditional {
            match safe_notify(sink.as_ref(), bar, budget) {
                NotifyOutcome::Ok => survivors.push(sink),
                outcome => {
                    evicted += 1;
                    tracing::warn!(
                        ?outcome,
                        "无条件 bar sink 异常，已摘除（不影响路由与其余 sink）"
                    );
                }
            }
        }
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
        // 再上锁：用 survivors 覆盖无条件列表、摘除坏/慢路由消费者
        // （被摘消费者独占的符号在下轮对账自然退订）。
        if evicted > 0 {
            if let Ok(mut core) = self.core.lock() {
                core.unconditional = survivors;
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
            turnover: Decimal::ONE,
            open_interest: Decimal::ONE,
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
