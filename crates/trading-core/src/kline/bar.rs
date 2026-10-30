//! 定稿 bar 的「单一内存事实源」：per-series 有界去重 map。
//!
//! 一份内存同时承担三职：最近 bar 的**可读缓存**（[`BarTable::range`] 等，P2 WS 热读）、
//! 落盘的 **dirty 来源**（[`BarTable::snapshot_dirty`] → append CSV → [`BarTable::mark_flushed`]）、
//! 以及后续 P3 的缺口索引。四家交易所的定稿 bar（WS 原生收盘帧 / CTP 聚合定稿）同一入口
//! [`BarTable::upsert`]；进程启动可经 [`BarTable::restore`] 用当日 CSV 预热（不置 dirty）。
//!
//! flush 为**非破坏式**：snapshot 只克隆 dirty、不移除 bar 本体；落盘成功后按 `(start,gen)`
//! 确认清 dirty。裁剪只淘汰**已确认落盘**（非 dirty）的旧键——未落盘的 dirty 键豁免，
//! 这是不可违背的红线（落盘故障期内存可暂时超窗，由 dropped/告警另计，绝不静默丢数据）。
//!
//! 纯内存、不实现 Model、不做快照恢复；行模型 [`Bar`] 在 trading-models。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dashmap::DashMap;
use trading_models::Bar;

/// 默认每序列保留的 bar 条数（1m × 1440 ＝ 24h）。
pub const DEFAULT_RETAIN_BARS: usize = 1440;
/// 默认保留时长（与条数上限共同约束内存，二者取先到者）。
pub const DEFAULT_RETAIN_AGE: Duration = Duration::from_secs(24 * 60 * 60);

/// 一路序列的身份：交易所 × 合约。本期周期固定 1m，故不入键。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SeriesKey {
    /// 交易所标识（binance / deribit / hyperliquid / ctp）。
    pub venue: String,
    /// 合约/交易对（南向原值）。
    pub symbol: String,
}

impl SeriesKey {
    pub fn new(venue: impl Into<String>, symbol: impl Into<String>) -> Self {
        Self {
            venue: venue.into(),
            symbol: symbol.into(),
        }
    }
}

/// 一根待确认落盘的 dirty bar：携带其被快照时的版本 gen（mark 时做乐观并发校验）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirtyBar {
    /// 所属序列。
    pub key: SeriesKey,
    /// 桶开始时间（UTC 毫秒），即序列内去重主键。
    pub start: i64,
    /// 快照时该键的版本；mark_flushed 仅在版本未再推进时清 dirty。
    pub gen: u64,
    /// bar 本体（克隆）。
    pub bar: Bar,
}

/// 一路序列当前保留 bar 的 start_time 只读快照（缺口周期扫描用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesStarts {
    /// 序列身份（venue × symbol）。
    pub key: SeriesKey,
    /// 保留窗内全部 bar 的 start_time，升序、去重（BTreeMap 键天然如此）；
    /// 末键即 `*starts.last()`（裁剪只淘汰最旧键，最新键永在）。
    pub starts: Vec<i64>,
}

/// 单个序列的可变状态（每序列独立锁，临界区只做 BTreeMap 短操作）。
#[derive(Debug)]
struct SeriesState {
    /// start_time → bar：去重缓存 + 读来源（flush 不清它，裁剪才淘汰）。
    bars: BTreeMap<i64, Bar>,
    /// start_time → gen：未落盘/待确认的脏键及其快照版本。
    dirty: BTreeMap<i64, u64>,
    /// 该序列见过的最大 start（裁剪时间窗与 last_start 用）。
    latest: i64,
    /// 该序列自增版本：每次 upsert 同键覆盖都推进，使旧 gen 的 mark 失效。
    gen: u64,
}

impl SeriesState {
    fn new() -> Self {
        Self {
            bars: BTreeMap::new(),
            dirty: BTreeMap::new(),
            latest: i64::MIN,
            gen: 0,
        }
    }
}

/// 序列缓冲：把状态 Mutex 包成可经 Arc 跨 DashMap 引用共享的一份。
#[derive(Debug)]
struct SeriesBuf {
    state: Mutex<SeriesState>,
}

/// 定稿 bar 单表（多写者＝各行情连接；flush 线程快照 dirty；读路径 range）。
#[derive(Debug)]
pub struct BarTable {
    /// venue/symbol → 序列缓冲（DashMap 分片，避免单锁）。
    series: DashMap<SeriesKey, Arc<SeriesBuf>>,
    /// 每序列保留条数上限（启动装配期可经 [`BarTable::set_retention`] 收紧）。
    retain_bars: AtomicUsize,
    /// 每序列保留时长上限（毫秒；启动装配期可配置）。
    retain_age_ms: AtomicU64,
}

impl Default for BarTable {
    fn default() -> Self {
        Self::with_retention(DEFAULT_RETAIN_BARS, DEFAULT_RETAIN_AGE)
    }
}

impl BarTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// 指定每序列保留条数/时长构造（测试/调参用）。
    pub fn with_retention(retain_bars: usize, retain_age: Duration) -> Self {
        Self {
            series: DashMap::new(),
            retain_bars: AtomicUsize::new(retain_bars),
            retain_age_ms: AtomicU64::new(duration_to_ms(retain_age)),
        }
    }

    /// 启动装配期配置每序列保留条数/时长（在任何 bar 到达前由装配层调用一次）。
    /// 读路径与裁剪按当前值即时生效；下一次 upsert 即据此裁剪。仅设计为启动期设置，
    /// 运行中改窗不回溯重扫已有序列（无新 bar 时内存暂留旧窗，不影响正确性）。
    pub fn set_retention(&self, retain_bars: usize, retain_age: Duration) {
        self.retain_bars.store(retain_bars, Ordering::Relaxed);
        self.retain_age_ms
            .store(duration_to_ms(retain_age), Ordering::Relaxed);
    }

    /// 当前每序列保留条数（诊断/测试用）。
    pub fn retain_bars(&self) -> usize {
        self.retain_bars.load(Ordering::Relaxed)
    }

    /// 当前每序列保留时长（诊断/测试用）。
    pub fn retain_age(&self) -> Duration {
        Duration::from_millis(self.retain_age_ms.load(Ordering::Relaxed))
    }

    /// 读当前保留参数快照（供 upsert/restore 后裁剪）。
    fn retention(&self) -> (usize, Duration) {
        (
            self.retain_bars.load(Ordering::Relaxed),
            Duration::from_millis(self.retain_age_ms.load(Ordering::Relaxed)),
        )
    }

    /// 写入/覆盖一根定稿 bar（按序列内 start 去重）。同 start 覆盖推进该键 gen 并置 dirty；
    /// 随后在本序列上做有界裁剪（dirty 键豁免）。
    ///
    /// 返回**写入前**该序列的末键（`latest`，空序列为 None）：缺口实时尾检
    /// （[`crate::kline::detect_tail_gap`]）在同一把序列锁内取旧末键、无竞态；乱序迟到
    /// （本来棒早于旧末键）时尾检据此自然判 None。
    pub fn upsert(&self, bar: Bar) -> Option<i64> {
        let key = SeriesKey::new(&bar.exchange, &bar.symbol);
        let start = bar.start_time;
        let buf = self.buf_for(&key);
        let mut st = buf.state.lock().unwrap();
        let prev = if st.latest == i64::MIN {
            None
        } else {
            Some(st.latest)
        };
        let gen = st.gen.saturating_add(1);
        st.gen = gen;
        st.bars.insert(start, bar);
        st.dirty.insert(start, gen);
        st.latest = st.latest.max(start);
        let (retain_bars, retain_age) = self.retention();
        prune_locked(&mut st, retain_bars, retain_age);
        prev
    }

    /// CSV 预热：只写 bars/更新 latest，**绝不置 dirty**（禁止把历史再写一遍盘）。
    /// 同样按保留窗裁剪，使预热不撑大内存；预热键非 dirty，可被正常淘汰。
    pub fn restore(&self, bar: Bar) {
        let key = SeriesKey::new(&bar.exchange, &bar.symbol);
        let start = bar.start_time;
        let buf = self.buf_for(&key);
        let mut st = buf.state.lock().unwrap();
        st.bars.insert(start, bar);
        st.latest = st.latest.max(start);
        let (retain_bars, retain_age) = self.retention();
        prune_locked(&mut st, retain_bars, retain_age);
    }

    /// 取某序列的缓冲（不存在则建）。仅在短临界区内持有 DashMap shard 写守卫，
    /// 返回后守卫随临时值释放，后续只锁各序列自己的 Mutex。
    fn buf_for(&self, key: &SeriesKey) -> Arc<SeriesBuf> {
        if let Some(buf) = self.series.get(key) {
            return Arc::clone(&buf);
        }
        self.series
            .entry(key.clone())
            .or_insert_with(|| {
                Arc::new(SeriesBuf {
                    state: Mutex::new(SeriesState::new()),
                })
            })
            .clone()
    }

    /// 快照全部 dirty bar（含 key/start/gen/bar），按 (key, start) 有序；**不移除**
    /// bars/dirty（非破坏式 flush 第一步）。逐序列加锁、克隆后即释放，不在持锁期间 IO。
    pub fn snapshot_dirty(&self) -> Vec<DirtyBar> {
        // DashMap::iter 按 shard 顺序非确定，故收集后按 (key,start) 排序，保证落盘顺序确定。
        let mut out = Vec::new();
        for entry in self.series.iter() {
            let key = entry.key().clone();
            let st = entry.value().state.lock().unwrap();
            for (&start, &gen) in st.dirty.iter() {
                if let Some(bar) = st.bars.get(&start) {
                    out.push(DirtyBar {
                        key: key.clone(),
                        start,
                        gen,
                        bar: bar.clone(),
                    });
                }
            }
        }
        out.sort_by(|a, b| {
            (a.key.venue.as_str(), a.key.symbol.as_str(), a.start).cmp(&(
                b.key.venue.as_str(),
                b.key.symbol.as_str(),
                b.start,
            ))
        });
        out
    }

    /// 落盘成功后确认：仅当该 start 当前 dirty 版本仍等于快照 gen 才清除。
    /// 若快照后同 start 又被 upsert（gen 推进），保留新 dirty，下轮重写新值。
    pub fn mark_flushed(&self, confirms: &[(SeriesKey, i64, u64)]) {
        for (key, start, gen) in confirms {
            if let Some(buf) = self.series.get(key) {
                let mut st = buf.state.lock().unwrap();
                if st.dirty.get(start) == Some(gen) {
                    st.dirty.remove(start);
                }
            }
        }
    }

    /// 某序列最近一根 bar 的开始时间（无序列返回 None）。
    pub fn last_start(&self, key: &SeriesKey) -> Option<i64> {
        let buf = self.series.get(key)?;
        let st = buf.state.lock().unwrap();
        if st.latest == i64::MIN {
            None
        } else {
            Some(st.latest)
        }
    }

    /// 某序列当前保留 bar 的 `(最旧 start, 最新 start)`（空序列返回 None）。
    /// 冷读闸门据此判断查询区间是否完全落在内存保留窗内（不依赖到达顺序，BTreeMap 有序）。
    pub fn span(&self, key: &SeriesKey) -> Option<(i64, i64)> {
        let buf = self.series.get(key)?;
        let st = buf.state.lock().unwrap();
        let oldest = *st.bars.keys().next()?;
        Some((oldest, st.latest))
    }

    /// 读某序列 `[start_ms, end_ms)`（左闭右开）内的 bar，按 start 升序。未知序列为空。
    pub fn range(&self, key: &SeriesKey, start_ms: i64, end_ms: i64) -> Vec<Bar> {
        let Some(buf) = self.series.get(key) else {
            return Vec::new();
        };
        let st = buf.state.lock().unwrap();
        st.bars
            .range(start_ms..end_ms)
            .map(|(_, b)| b.clone())
            .collect()
    }

    /// 某序列当前在内存保留的 bar 条数（含已确认与 dirty）。
    pub fn coverage(&self, key: &SeriesKey) -> usize {
        self.series
            .get(key)
            .map(|buf| buf.state.lock().unwrap().bars.len())
            .unwrap_or(0)
    }

    /// 当前缓存的序列路数（venue × symbol）。
    pub fn series_count(&self) -> usize {
        self.series.len()
    }

    /// 快照全部序列（含 CTP；venue 白名单过滤在调用方）的有序 start 键，供缺口周期扫描。
    ///
    /// 先把 `(key, Arc<SeriesBuf>)` 收集进 Vec 以**立即结束 DashMap 迭代**（解除 shard 占用，
    /// 避免持迭代器做 clone 阻塞扩容/移除），再逐序列加各自短锁 clone 键集合；不在持锁期间 IO。
    /// 成本：每序列 ≤1440 个 i64（约 11KB），5min 一次的低频非热路径。
    pub fn series_starts(&self) -> Vec<SeriesStarts> {
        let bufs: Vec<(SeriesKey, Arc<SeriesBuf>)> = self
            .series
            .iter()
            .map(|entry| (entry.key().clone(), Arc::clone(entry.value())))
            .collect();
        bufs.into_iter()
            .map(|(key, buf)| {
                let st = buf.state.lock().unwrap();
                SeriesStarts {
                    key,
                    starts: st.bars.keys().copied().collect(),
                }
            })
            .collect()
    }

    /// 全表当前未落盘确认的 dirty bar 总数（跨序列求和）。
    ///
    /// flush 持续失败时 dirty 因「裁剪豁免」只增不减；服务侧据此做超阈**告警**（不丢数据），
    /// flush 恢复、mark_flushed 清空后自然回落。遍历各序列短锁求和，仅周期告警调用，非热路径。
    pub fn pending_dirty(&self) -> usize {
        self.series
            .iter()
            .map(|e| e.value().state.lock().unwrap().dirty.len())
            .sum()
    }
}

/// Duration → 毫秒（饱和到 u64；负时长按 0）。
fn duration_to_ms(d: Duration) -> u64 {
    d.as_millis().min(u64::MAX as u128) as u64
}

/// 单序列有界裁剪（调用方持该序列锁）。
///
/// 双上限：从最旧键起，凡 `start < latest - retain_age`（**严格小于**，==cutoff 保留）
/// 或令保留条数超过 `retain_bars`，且该键**非 dirty**（已确认落盘）才淘汰。
///
/// 红线：dirty 键一律豁免——落盘持续故障期内存可暂时超出保留窗，也绝不静默丢未落盘数据。
/// 因豁免而无法裁剪时，超窗的 dirty 键继续保留（由 flush 重试 + dropped/告警另计）。
fn prune_locked(st: &mut SeriesState, retain_bars: usize, retain_age: Duration) {
    if st.bars.is_empty() {
        return;
    }
    // 时间下沿：start 严格小于它才过期。latest 极小/age 极大时 saturate 到 MIN（不过期）。
    let age_ms = retain_age.as_millis().min(i64::MAX as u128) as i64;
    let cutoff = st.latest.saturating_sub(age_ms);
    // 条数下沿：保留最后 retain_bars 个 start（BTreeMap 升序）。不足上限则不过期。
    let count_floor = if st.bars.len() > retain_bars {
        st.bars.keys().nth(st.bars.len() - retain_bars).copied()
    } else {
        None
    };

    let mut victims: Vec<i64> = Vec::new();
    for &start in st.bars.keys() {
        let too_old = start < cutoff;
        let too_many = count_floor.is_some_and(|floor| start < floor);
        if (too_old || too_many) && !st.dirty.contains_key(&start) {
            victims.push(start);
        }
    }
    for start in victims {
        st.bars.remove(&start);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bar(venue: &str, symbol: &str, start: i64) -> Bar {
        let mut b = Bar::new(venue, symbol, 20261024, start, start + 60_000);
        b.close = rust_decimal::Decimal::from(100);
        b.volume = 3;
        b
    }

    fn key(venue: &str, symbol: &str) -> SeriesKey {
        SeriesKey::new(venue, symbol)
    }

    /// 把快照转成 mark_flushed 的确认三元组。
    fn confirm(snap: &[DirtyBar]) -> Vec<(SeriesKey, i64, u64)> {
        snap.iter()
            .map(|d| (d.key.clone(), d.start, d.gen))
            .collect()
    }

    // ---- upsert：去重 / 追加 / 有序 -------------------------------------

    #[test]
    fn upsert_same_start_overwrites_without_growing_count() {
        // Arrange：同序列同分钟先写一根。
        let t = BarTable::new();
        t.upsert(bar("ctp", "cu2512", 60_000));

        // Act：同 start 再来一根（定稿帧迟到/修正）。
        let mut newer = bar("ctp", "cu2512", 60_000);
        newer.volume = 99;
        t.upsert(newer);

        // Assert：条数不增、值被覆盖、latest 仍是该分钟。
        assert_eq!(t.coverage(&key("ctp", "cu2512")), 1);
        let got = t.range(&key("ctp", "cu2512"), 0, i64::MAX);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].volume, 99);
        assert_eq!(t.last_start(&key("ctp", "cu2512")), Some(60_000));
    }

    #[test]
    fn upsert_distinct_starts_appends_and_reads_sorted_by_start() {
        // Arrange：故意乱序到达两根不同分钟。
        let t = BarTable::new();
        t.upsert(bar("ctp", "cu2512", 120_000));
        t.upsert(bar("ctp", "cu2512", 60_000));

        // Act + Assert：读回按 start 升序（与到达顺序无关），跨 trading_day 同序列也按 start。
        let starts: Vec<i64> = t
            .range(&key("ctp", "cu2512"), 0, i64::MAX)
            .iter()
            .map(|b| b.start_time)
            .collect();
        assert_eq!(starts, vec![60_000, 120_000]);
        assert_eq!(t.coverage(&key("ctp", "cu2512")), 2);
        assert_eq!(t.last_start(&key("ctp", "cu2512")), Some(120_000));
    }

    #[test]
    fn upsert_partitions_by_venue_and_symbol() {
        // 不同 venue / symbol 各成序列，互不串扰。
        let t = BarTable::new();
        t.upsert(bar("ctp", "cu2512", 60_000));
        t.upsert(bar("ctp", "rb2611", 60_000));
        t.upsert(bar("binance", "cu2512", 60_000));

        assert_eq!(t.series_count(), 3);
        assert_eq!(t.coverage(&key("ctp", "cu2512")), 1);
        assert_eq!(t.coverage(&key("ctp", "rb2611")), 1);
        assert_eq!(t.coverage(&key("binance", "cu2512")), 1);
        assert_eq!(t.coverage(&key("ctp", "ag9999")), 0);
        assert!(t.last_start(&key("ctp", "ag9999")).is_none());
    }

    // ---- range：左闭右开 -----------------------------------------------

    #[test]
    fn range_is_start_inclusive_end_exclusive() {
        let t = BarTable::new();
        for s in [60_000, 120_000, 180_000] {
            t.upsert(bar("ctp", "cu2512", s));
        }
        // 左闭：含 60_000；右开：不含 180_000。
        let got = t.range(&key("ctp", "cu2512"), 60_000, 180_000);
        assert_eq!(
            got.iter().map(|b| b.start_time).collect::<Vec<_>>(),
            vec![60_000, 120_000]
        );
        // 未知序列给空 Vec（而非错误）。
        assert!(t.range(&key("ctp", "xx"), 0, i64::MAX).is_empty());
    }

    #[test]
    fn span_reports_oldest_and_latest_independent_of_arrival_order() {
        let t = BarTable::new();
        // 空序列无 span。
        assert!(t.span(&key("ctp", "cu2512")).is_none());
        // 乱序到达：oldest 取最小 start，latest 取最大。
        t.upsert(bar("ctp", "cu2512", 180_000));
        t.restore(bar("ctp", "cu2512", 60_000));
        t.upsert(bar("ctp", "cu2512", 120_000));
        assert_eq!(t.span(&key("ctp", "cu2512")), Some((60_000, 180_000)));
        // 同 start 覆盖不改 span；不同序列互不影响。
        t.upsert(bar("ctp", "cu2512", 60_000));
        assert_eq!(t.span(&key("ctp", "cu2512")), Some((60_000, 180_000)));
        assert!(t.span(&key("ctp", "rb2611")).is_none());
    }

    // ---- 条数裁剪：只淘汰已确认的最旧键 --------------------------------

    #[test]
    fn prune_by_count_evicts_only_confirmed_oldest() {
        // Arrange：每序列最多留 3 根。
        let t = BarTable::with_retention(3, Duration::from_secs(10_000_000));
        for s in [60_000, 120_000, 180_000] {
            t.upsert(bar("ctp", "cu2512", s));
        }
        // 全部落盘确认（清 dirty）。
        let snap = t.snapshot_dirty();
        t.mark_flushed(&confirm(&snap));
        assert_eq!(t.coverage(&key("ctp", "cu2512")), 3);

        // Act：再来第 4 根触发裁剪。
        t.upsert(bar("ctp", "cu2512", 240_000));

        // Assert：最旧（60_000，已确认）被淘汰，保留最近 3 根。
        let starts: Vec<i64> = t
            .range(&key("ctp", "cu2512"), 0, i64::MAX)
            .iter()
            .map(|b| b.start_time)
            .collect();
        assert_eq!(starts, vec![120_000, 180_000, 240_000]);
    }

    // ---- 时间裁剪：start < latest-age 淘汰，==cutoff 保留 ---------------

    #[test]
    fn prune_by_age_evicts_strictly_older_than_cutoff_keeps_boundary() {
        // 保留窗 120_000ms。latest=240_000 → cutoff=120_000。
        let t = BarTable::with_retention(1000, Duration::from_millis(120_000));
        for s in [60_000, 120_000, 240_000] {
            t.upsert(bar("ctp", "cu2512", s));
        }
        let snap = t.snapshot_dirty();
        t.mark_flushed(&confirm(&snap));

        // 再 upsert 一次最新键以触发裁剪（latest 仍 240_000）。
        t.upsert(bar("ctp", "cu2512", 240_000));

        // 60_000 < cutoff(120_000) 淘汰；120_000 == cutoff 保留；240_000 保留。
        let starts: Vec<i64> = t
            .range(&key("ctp", "cu2512"), 0, i64::MAX)
            .iter()
            .map(|b| b.start_time)
            .collect();
        assert_eq!(starts, vec![120_000, 240_000]);
    }

    // ---- 红线：dirty 键豁免裁剪 -----------------------------------------

    #[test]
    fn dirty_bars_are_never_pruned_even_when_outside_window() {
        // Arrange：极小保留窗（3 条 / 60s），写入 4 根但**从不 mark_flushed**。
        let t = BarTable::with_retention(3, Duration::from_millis(60_000));
        for s in [60_000, 120_000, 180_000, 240_000] {
            t.upsert(bar("ctp", "cu2512", s));
        }

        // Assert（红线）：所有 dirty 键仍在，全部可读、仍待落盘。
        assert_eq!(t.coverage(&key("ctp", "cu2512")), 4);
        assert_eq!(t.snapshot_dirty().len(), 4);
        let starts: Vec<i64> = t
            .range(&key("ctp", "cu2512"), 0, i64::MAX)
            .iter()
            .map(|b| b.start_time)
            .collect();
        assert_eq!(starts, vec![60_000, 120_000, 180_000, 240_000]);

        // 确认后再插入新键，旧键才允许被裁剪。
        let snap = t.snapshot_dirty();
        t.mark_flushed(&confirm(&snap));
        t.upsert(bar("ctp", "cu2512", 300_000));
        assert!(t.coverage(&key("ctp", "cu2512")) <= 3);
        assert!(!t
            .range(&key("ctp", "cu2512"), 0, i64::MAX)
            .iter()
            .any(|b| b.start_time == 60_000));
    }

    // ---- dirty 快照 / gen 确认 ------------------------------------------

    #[test]
    fn snapshot_dirty_is_ordered_and_non_destructive() {
        let t = BarTable::new();
        t.upsert(bar("ctp", "cu2512", 120_000));
        t.upsert(bar("ctp", "cu2512", 60_000));
        t.upsert(bar("binance", "BTCUSDT", 60_000));

        // 按 (key, start) 有序：binance/BTCUSDT 在 ctp/cu2512 之前；同序列按 start。
        let snap = t.snapshot_dirty();
        let keys: Vec<(&str, &str, i64)> = snap
            .iter()
            .map(|d| (d.key.venue.as_str(), d.key.symbol.as_str(), d.start))
            .collect();
        assert_eq!(
            keys,
            vec![
                ("binance", "BTCUSDT", 60_000),
                ("ctp", "cu2512", 60_000),
                ("ctp", "cu2512", 120_000),
            ]
        );
        // 非破坏：快照后 bar 与 dirty 都还在。
        assert_eq!(t.snapshot_dirty().len(), 3);
        assert_eq!(t.coverage(&key("ctp", "cu2512")), 2);
    }

    #[test]
    fn mark_flushed_clears_only_matching_gen() {
        // Arrange：upsert 一次 → gen=1，快照之。
        let t = BarTable::new();
        t.upsert(bar("ctp", "cu2512", 60_000));
        let snap = t.snapshot_dirty();
        assert_eq!(snap[0].gen, 1);

        // Act：写中被覆盖（gen 推进到 2），随后用旧 gen=1 确认。
        let mut changed = bar("ctp", "cu2512", 60_000);
        changed.volume = 42;
        t.upsert(changed);
        t.mark_flushed(&confirm(&snap));

        // Assert：旧 gen 不得清 dirty（下轮需重写新值），bar 为最新值。
        let pending = t.snapshot_dirty();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].gen, 2);
        assert_eq!(t.range(&key("ctp", "cu2512"), 0, i64::MAX)[0].volume, 42);

        // 用新 gen 确认后 dirty 清空，但 bar 本体保留。
        let snap2 = t.snapshot_dirty();
        t.mark_flushed(&confirm(&snap2));
        assert!(t.snapshot_dirty().is_empty());
        assert_eq!(t.coverage(&key("ctp", "cu2512")), 1);
    }

    // ---- pending_dirty：全局未落盘积压计数（故障期可观测）---------------

    #[test]
    fn pending_dirty_counts_only_unflushed_across_series_and_clears_on_mark() {
        let t = BarTable::new();
        assert_eq!(t.pending_dirty(), 0);

        // 两序列共 3 根 dirty；restore 不计。
        t.upsert(bar("ctp", "cu2512", 60_000));
        t.upsert(bar("ctp", "cu2512", 120_000));
        t.upsert(bar("ctp", "rb2611", 60_000));
        t.restore(bar("ctp", "ag9999", 60_000));
        assert_eq!(t.pending_dirty(), 3);

        // 同 start 覆盖不增加 dirty 数（仍是同一个脏键）。
        t.upsert(bar("ctp", "cu2512", 60_000));
        assert_eq!(t.pending_dirty(), 3);

        // 全部确认后归零（bar 本体保留不影响）。
        let snap = t.snapshot_dirty();
        t.mark_flushed(&confirm(&snap));
        assert_eq!(t.pending_dirty(), 0);
        assert_eq!(t.coverage(&key("ctp", "cu2512")), 2);
    }

    // ---- restore：预热不产生 dirty --------------------------------------

    #[test]
    fn restore_populates_bars_but_never_marks_dirty() {
        let t = BarTable::new();
        t.restore(bar("ctp", "cu2512", 60_000));
        t.restore(bar("ctp", "cu2512", 120_000));

        // 可读、有 latest/coverage，但绝无 dirty（CSV 预热禁止重写盘）。
        assert_eq!(t.coverage(&key("ctp", "cu2512")), 2);
        assert_eq!(t.last_start(&key("ctp", "cu2512")), Some(120_000));
        assert!(t.snapshot_dirty().is_empty());

        // 预热后新到的更晚分钟 upsert 正常去重并置 dirty。
        t.upsert(bar("ctp", "cu2512", 180_000));
        assert_eq!(t.snapshot_dirty().len(), 1);
    }

    // ---- 保留参数可在启动装配期收紧 -------------------------------------

    #[test]
    fn set_retention_shrinks_window_on_next_upsert() {
        // Arrange：大窗写 4 根并全部确认落盘。
        let t = BarTable::with_retention(100, Duration::from_secs(1_000_000));
        for s in [60_000, 120_000, 180_000, 240_000] {
            t.upsert(bar("ctp", "cu2512", s));
        }
        let snap = t.snapshot_dirty();
        t.mark_flushed(&confirm(&snap));
        assert_eq!(t.coverage(&key("ctp", "cu2512")), 4);

        // Act：启动装配期把每序列条数收紧到 2，随后一根新 bar 触发裁剪。
        t.set_retention(2, Duration::from_secs(1_000_000));
        t.upsert(bar("ctp", "cu2512", 300_000));

        // Assert：只留最新两根（240_000、300_000），旧的已确认键被淘汰。
        let starts: Vec<i64> = t
            .range(&key("ctp", "cu2512"), 0, i64::MAX)
            .iter()
            .map(|b| b.start_time)
            .collect();
        assert_eq!(starts, vec![240_000, 300_000]);
    }

    // ---- upsert 返回写入前旧末键（尾检用） ------------------------------

    #[test]
    fn upsert_returns_previous_latest_and_none_for_first() {
        let t = BarTable::new();
        // 首根：写入前无末键。
        assert_eq!(t.upsert(bar("binance", "BTCUSDT", 60_000)), None);
        // 连续追加：返回上一根 start。
        assert_eq!(t.upsert(bar("binance", "BTCUSDT", 120_000)), Some(60_000));
        // 乱序迟到（早于末键）：仍返回旧末键（尾检据此判 None，不在尾部造缺口）。
        assert_eq!(t.upsert(bar("binance", "BTCUSDT", 90_000)), Some(120_000));
        // 不同序列互不影响，仍是首根。
        assert_eq!(t.upsert(bar("ctp", "cu2512", 60_000)), None);
    }

    // ---- series_starts：全序列有序 start 快照 ----------------------------

    #[test]
    fn series_starts_empty_table_is_empty() {
        let t = BarTable::new();
        assert!(t.series_starts().is_empty());
    }

    #[test]
    fn series_starts_reports_sorted_starts_last_and_partitions() {
        let t = BarTable::new();
        // 乱序 upsert（含 CTP）；快照内 starts 必须升序、last 取末键。
        t.upsert(bar("binance", "BTCUSDT", 180_000));
        t.upsert(bar("binance", "BTCUSDT", 60_000));
        t.upsert(bar("binance", "BTCUSDT", 120_000));
        t.upsert(bar("ctp", "cu2512", 60_000));

        let mut got = t.series_starts();
        got.sort_by(|a, b| (&a.key.venue, &a.key.symbol).cmp(&(&b.key.venue, &b.key.symbol)));
        assert_eq!(got.len(), 2);

        let bnc = &got[0];
        assert_eq!(bnc.key.venue, "binance");
        assert_eq!(bnc.key.symbol, "BTCUSDT");
        assert_eq!(bnc.starts, vec![60_000, 120_000, 180_000]);
        assert_eq!(bnc.starts.last().copied(), Some(180_000));

        let ctp = &got[1];
        assert_eq!(ctp.key.venue, "ctp");
        assert_eq!(ctp.starts, vec![60_000]);
        assert_eq!(ctp.starts.last().copied(), Some(60_000));
    }

    #[test]
    fn series_starts_reflects_pruned_window() {
        // 极小条数窗 + 全部确认落盘，再追加触发裁剪。
        let t = BarTable::with_retention(2, Duration::from_secs(10_000_000));
        t.upsert(bar("binance", "BTCUSDT", 60_000));
        t.upsert(bar("binance", "BTCUSDT", 120_000));
        let snap = t.snapshot_dirty();
        t.mark_flushed(&confirm(&snap));
        t.upsert(bar("binance", "BTCUSDT", 180_000));

        let got = t.series_starts();
        assert_eq!(got.len(), 1);
        // 最旧 60_000 已裁剪，只留最近两根。
        assert_eq!(got[0].starts, vec![120_000, 180_000]);
        assert_eq!(got[0].starts.last().copied(), Some(180_000));
    }

    #[test]
    fn series_starts_does_not_deadlock_under_concurrent_upsert() {
        let t = Arc::new(BarTable::new());
        let mut handles = Vec::new();
        for seq in 0..4 {
            let table = Arc::clone(&t);
            handles.push(std::thread::spawn(move || {
                for i in 0..200 {
                    let start = (seq * 200 + i) * 60_000 + 60_000;
                    table.upsert(bar("binance", "BTCUSDT", start));
                }
            }));
        }
        // 主线程并发热读快照，验证不与写者死锁。
        for _ in 0..50 {
            let _ = t.series_starts();
        }
        for h in handles {
            h.join().unwrap();
        }
        // 4 线程各写同一序列不同 start（彼此交错但均递增进同 BTreeMap），最终条数齐。
        let got = t.series_starts();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].starts.len(), 800);
    }
}
