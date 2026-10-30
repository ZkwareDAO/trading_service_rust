//! K线缺口检测的**纯逻辑内核**（P3）：零网络、零文件、零时钟读取。
//!
//! 仅适用 binance/deribit/hyperliquid 三家 7×24 连续撮合所（调用方按 venue 白名单过滤，
//! CTP 不进入）。判据：同序列相邻两根已存在 bar 的 `start_time` 差 `> step`（1m=60_000ms）
//! 即中间缺分钟；步长一律由调用方入参（trading-core 不依赖 trading-exchange，拿不到
//! `KlineInterval`）。
//!
//! 三个纯函数对应三个检测点：
//! * [`detect_tail_gap`]：实时尾部（定稿 upsert 前，旧末键 vs 本来棒 start）；
//! * [`scan_gaps`]：定时内部扫描（一段有序 start 集合，任意到达顺序，内部排序去重）；
//! * [`stall_gap`]：静默停推（`now − 末键 > stall`，登记到「上一已收盘整分钟」）。
//!
//! 检出的缺口经 [`GapRegistry`]（store 收口的 `Mutex<BTreeSet<_>>`，非 channel）逐分钟
//! 去重登记，P4 再消费做 REST 回补。

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

/// 一段连续缺失的 1m bar：用**缺失分钟自身的 start_time 闭区间 `[start,end]`** 表达，
/// 两端均为 `step` 整数倍。相邻已存 bar `prev/next` 时：`start=prev+step`、`end=next-step`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GapRange {
    /// 第一个缺失分钟的 start_time（UTC 毫秒，含）。
    pub start: i64,
    /// 最后一个缺失分钟的 start_time（UTC 毫秒，含）。
    pub end: i64,
}

impl GapRange {
    pub fn new(start: i64, end: i64) -> Self {
        Self { start, end }
    }

    /// 闭区间内的缺失分钟数；`step<=0` 或反向区间返回 0。
    pub fn missing_bars(&self, step_ms: i64) -> usize {
        if step_ms <= 0 || self.end < self.start {
            return 0;
        }
        (self.end - self.start)
            .checked_div(step_ms)
            .map(|n| n as usize + 1)
            .unwrap_or(0)
    }
}

/// 一根「待回补」缺失分钟（[`GapRegistry`] 的 BTreeSet 元素）。
///
/// 去重全序键**只有 `(venue,symbol,start)`**：同一缺失分钟无论被尾检/扫描/静默哪一路、
/// 以多大段范围登记，都视为同一条。段范围（end）与首报时刻仅服务当下日志，**不进键**，
/// 否则同一分钟会因 end 不同而重复入集。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PendingGap {
    /// 交易所标识（binance/deribit/hyperliquid）。
    pub venue: String,
    /// 合约/交易对。
    pub symbol: String,
    /// 缺失分钟的 start_time（UTC 毫秒）。
    pub start: i64,
}

impl PendingGap {
    pub fn new(venue: impl Into<String>, symbol: impl Into<String>, start: i64) -> Self {
        Self {
            venue: venue.into(),
            symbol: symbol.into(),
            start,
        }
    }
}

/// 缺口归属的行情序列强类型键（替代裸 `(String,String)`，语义清晰、便于 claim 返回）。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GapSeries {
    /// 交易所标识（binance/deribit/hyperliquid）。
    pub venue: String,
    /// 合约/交易对。
    pub symbol: String,
}

impl GapSeries {
    pub fn new(venue: impl Into<String>, symbol: impl Into<String>) -> Self {
        Self {
            venue: venue.into(),
            symbol: symbol.into(),
        }
    }
}

/// store 收口的待回补缺口去重集合（进程单例，由 MemoryTables 持有）。
///
/// 两层状态（P4）：
/// * `pending`：已检出、等待回补 worker 认领的缺失分钟；
/// * `inflight`：已被 [`Self::claim_due`] 认领、正在回补的分钟。
///
/// 按序列分组：`GapSeries → 缺失分钟 start 集合`。这样同序列登记一段大缺口时
/// 只分配一次 venue/symbol 字符串，热路径（持久化 sink 尾检）只剩纯 i64 插入，避免
/// 断线很久后逐分钟重复堆分配、超 on_bar 同步预算导致 sink 被摘除。
///
/// 登记只发生在「分钟级定稿」与「5min 扫描」，非逐笔热路径；单把 [`Mutex`] 足够，
/// 锁内仅做 BTreeMap/BTreeSet 短插入，禁止 IO。inflight 期间重复登记同分钟会被忽略，
/// 防止 worker 回补时尾检/扫描把同一分钟再次塞进 pending 造成重复回补。
#[derive(Debug, Default)]
pub struct GapRegistry {
    /// 待认领。
    pending: Mutex<BTreeMap<GapSeries, BTreeSet<i64>>>,
    /// 已认领、回补进行中。
    inflight: Mutex<BTreeMap<GapSeries, BTreeSet<i64>>>,
}

impl GapRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 把一段连续缺口 `[g.start, g.end]` 按 `step_ms` 逐分钟登记进 **pending**，
    /// 返回**本次新登记**的分钟数（已在 pending 或正在 inflight 的分钟不计）。
    /// 调用方据返回值是否 >0 做边沿日志，避免周期刷屏。
    #[must_use = "新增分钟数是边沿日志/计数的依据，忽略它会漏报或重复刷屏"]
    pub fn record(&self, venue: &str, symbol: &str, g: &GapRange, step_ms: i64) -> usize {
        if step_ms <= 0 || g.end < g.start {
            return 0;
        }
        // 固定加锁顺序：先 pending 后 inflight，所有方法一致，避免锁序反转。
        let mut pending = self.pending.lock().unwrap();
        let inflight = self.inflight.lock().unwrap();
        let key = GapSeries::new(venue, symbol);
        let flying = inflight.get(&key);
        // 整段只 entry 一次（一次字符串分配），段内分钟零字符串分配，保持 P3 热路径预算。
        // 借用收在独立作用域内，结束后才能在「全 inflight」时 remove 新建的空 entry。
        let (added, touched_empty) = {
            let set = pending.entry(key).or_default();
            let before = set.len();
            let mut cur = g.start;
            while cur <= g.end {
                // 正在回补的分钟不重复登记（worker 回补期间尾检/扫描的幂等关键）。
                let is_flying = flying.is_some_and(|s| s.contains(&cur));
                if !is_flying {
                    set.insert(cur);
                }
                // checked_add：理论上 i64 分钟时间不会溢出，防御性止步而非死循环。
                cur = match cur.checked_add(step_ms) {
                    Some(next) => next,
                    None => break,
                };
            }
            (set.len() - before, set.is_empty())
        };
        // 整段分钟都在 inflight（纯幂等命中）时会留下新建空集，移除避免悬挂空序列。
        if touched_empty {
            pending.remove(&GapSeries::new(venue, symbol));
        }
        added
    }

    /// 原子认领当前所有 pending 缺口：每序列的离散分钟经 [`coalesce_starts`] 聚成连续
    /// `[start,end]` 段返回，同时把这些分钟从 pending 移入 inflight。返回按
    /// `(venue,symbol)` 升序；无 pending 时为空。认领后 [`Self::len`] 归零（分钟在 inflight）。
    pub fn claim_due(&self, step_ms: i64) -> Vec<(GapSeries, Vec<GapRange>)> {
        let mut pending = self.pending.lock().unwrap();
        let mut inflight = self.inflight.lock().unwrap();
        let taken = std::mem::take(&mut *pending);
        taken
            .into_iter()
            .map(|(series, starts)| {
                let ranges = coalesce_starts(&starts, step_ms);
                inflight.entry(series.clone()).or_default().extend(starts);
                (series, ranges)
            })
            .collect()
    }

    /// 回补成功：把给定分钟从 inflight 永久删除（不回 pending）。分钟/序列不存在即无操作。
    pub fn complete(&self, venue: &str, symbol: &str, starts: &[i64]) {
        let mut inflight = self.inflight.lock().unwrap();
        let key = GapSeries::new(venue, symbol);
        if let Some(set) = inflight.get_mut(&key) {
            for s in starts {
                set.remove(s);
            }
            if set.is_empty() {
                inflight.remove(&key);
            }
        }
    }

    /// 回补失败：把这些段按 `step_ms` 展开覆盖的、**仍在 inflight** 的分钟退回 pending，
    /// 等待下轮重试。已被 [`Self::complete`] 收口的分钟不会被退回（部分成功场景）。
    pub fn fail(&self, venue: &str, symbol: &str, ranges: &[GapRange], step_ms: i64) {
        if step_ms <= 0 {
            return;
        }
        let mut pending = self.pending.lock().unwrap();
        let mut inflight = self.inflight.lock().unwrap();
        let key = GapSeries::new(venue, symbol);
        let Some(flying) = inflight.get_mut(&key) else {
            return;
        };
        let back = pending.entry(key.clone()).or_default();
        for g in ranges {
            let mut cur = g.start;
            while cur <= g.end {
                // 仅退回仍在 inflight 的分钟；已 complete 的不动。
                if flying.remove(&cur) {
                    back.insert(cur);
                }
                cur = match cur.checked_add(step_ms) {
                    Some(next) => next,
                    None => break,
                };
            }
        }
        if flying.is_empty() {
            inflight.remove(&key);
        }
        if back.is_empty() {
            pending.remove(&key);
        }
    }

    /// 快照全部 **pending** 分钟（按 `(venue,symbol,start)` 升序）；inflight 不计。
    pub fn snapshot(&self) -> Vec<PendingGap> {
        self.pending
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(series, starts)| {
                starts
                    .iter()
                    .map(|&start| {
                        PendingGap::new(series.venue.clone(), series.symbol.clone(), start)
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// 当前 **pending**（待认领）分钟总数；inflight 不计。
    pub fn len(&self) -> usize {
        self.pending
            .lock()
            .unwrap()
            .values()
            .map(BTreeSet::len)
            .sum()
    }

    /// 是否没有待认领分钟（inflight 不影响）。
    pub fn is_empty(&self) -> bool {
        self.pending.lock().unwrap().is_empty()
    }
}

/// 把一组升序、已去重的离散分钟 start，聚合成连续 `[start,end]` 段（相邻差恰为
/// `step_ms` 归同段，否则断段）。`step_ms<=0` 或空集返回空。纯函数、零 IO。
#[must_use = "聚合结果用于回补区间规划，忽略等于不回补"]
pub fn coalesce_starts(starts: &BTreeSet<i64>, step_ms: i64) -> Vec<GapRange> {
    if step_ms <= 0 {
        return Vec::new();
    }
    let mut it = starts.iter().copied();
    let first = match it.next() {
        Some(v) => v,
        None => return Vec::new(),
    };
    let mut out = Vec::new();
    let mut seg_start = first;
    let mut prev = first;
    for cur in it {
        if cur == prev.saturating_add(step_ms) {
            // 与上一分钟连续，延续当前段。
            prev = cur;
            continue;
        }
        // 断档：收口当前段，另起一段。
        out.push(GapRange::new(seg_start, prev));
        seg_start = cur;
        prev = cur;
    }
    out.push(GapRange::new(seg_start, prev));
    out
}

/// 检测点 1（实时尾部）：本来棒 `next_start` 与同序列 upsert 前旧末键 `prev_start` 比。
///
/// 首根（无 prev）、相邻（`diff<=step`）、重复/乱序迟到（`next<=prev`）均返回 `None`；
/// `diff>step` 返回缺失分钟闭区间 `[prev+step, next-step]`。
#[must_use = "检出的缺口需登记进 GapRegistry，忽略返回值等于不检测"]
pub fn detect_tail_gap(prev_start: Option<i64>, next_start: i64, step_ms: i64) -> Option<GapRange> {
    if step_ms <= 0 {
        return None;
    }
    let prev = prev_start?;
    let start = prev.saturating_add(step_ms);
    if next_start <= start {
        return None;
    }
    let end = next_start.saturating_sub(step_ms);
    (end >= start).then(|| GapRange::new(start, end))
}

/// 检测点 2（内部扫描）：对任意到达顺序的 start 集合，内部排序去重后逐对比较相邻差。
///
/// 相邻差 `>step` 即一段缺口 `[a+step, b-step]`；`b` 非 step 整数倍的防御情形下，末端
/// 向下对齐到上一个整分钟（宁少报，不为非整分钟造缺口）。
#[must_use = "检出的缺口需登记进 GapRegistry，忽略返回值等于不检测"]
pub fn scan_gaps<I>(starts: I, step_ms: i64) -> Vec<GapRange>
where
    I: IntoIterator<Item = i64>,
{
    if step_ms <= 0 {
        return Vec::new();
    }
    let mut sorted: Vec<i64> = starts.into_iter().collect();
    sorted.sort_unstable();
    sorted.dedup();

    let mut out = Vec::new();
    for pair in sorted.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        if b.saturating_sub(a) <= step_ms {
            continue;
        }
        let start = a.saturating_add(step_ms);
        let end = b
            .saturating_sub(step_ms)
            .div_euclid(step_ms)
            .saturating_mul(step_ms);
        if end >= start {
            out.push(GapRange::new(start, end));
        }
    }
    out
}

/// 检测点 3（静默停推）：末键 `last_start` 距今超过 `stall_ms`（**恰好等于不报**）时，
/// 登记 `[last+step, 上一已收盘整分钟]`。
///
/// 上界 `floor((now-step)/step)*step`：`now` 恰落在整分钟边界 T 时，分钟 [T,T+step)
/// 尚未收盘，上界为 T-step，绝不把「当分钟」误判成缺。
#[must_use = "检出的缺口需登记进 GapRegistry，忽略返回值等于不检测"]
pub fn stall_gap(last_start: i64, now_ms: i64, step_ms: i64, stall_ms: i64) -> Option<GapRange> {
    if step_ms <= 0 || stall_ms <= 0 || now_ms <= last_start {
        return None;
    }
    if now_ms.saturating_sub(last_start) <= stall_ms {
        return None;
    }
    let start = last_start.saturating_add(step_ms);
    let end = now_ms
        .saturating_sub(step_ms)
        .div_euclid(step_ms)
        .saturating_mul(step_ms);
    (end >= start).then(|| GapRange::new(start, end))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    const STEP: i64 = 60_000;
    const STALL: i64 = 3 * STEP;

    /// 测试用：把「第 N 分钟」换算成毫秒时间戳。
    fn m(min: i64) -> i64 {
        min * STEP
    }

    // ---- detect_tail_gap -------------------------------------------------

    #[test]
    fn tail_first_bar_has_no_gap() {
        assert_eq!(detect_tail_gap(None, m(5), STEP), None);
    }

    #[test]
    fn tail_adjacent_minutes_have_no_gap() {
        assert_eq!(detect_tail_gap(Some(m(10)), m(11), STEP), None);
    }

    #[test]
    fn tail_detects_single_missing_minute() {
        // 末键 0 分，本来棒 2 分 → 缺第 1 分。
        assert_eq!(
            detect_tail_gap(Some(m(0)), m(2), STEP),
            Some(GapRange::new(m(1), m(1)))
        );
    }

    #[test]
    fn tail_detects_multiple_missing_minutes_with_count() {
        // 末键 0 分，本来棒 3 分 → 缺第 1、2 分。
        let g = detect_tail_gap(Some(m(0)), m(3), STEP).unwrap();
        assert_eq!(g, GapRange::new(m(1), m(2)));
        assert_eq!(g.missing_bars(STEP), 2);
    }

    #[test]
    fn tail_duplicate_and_late_out_of_order_have_no_gap() {
        // 重复（同分钟）。
        assert_eq!(detect_tail_gap(Some(m(10)), m(10), STEP), None);
        // 迟到旧棒（早于末键）。
        assert_eq!(detect_tail_gap(Some(m(10)), m(8), STEP), None);
    }

    #[test]
    fn tail_works_across_utc_midnight() {
        // 2024-12-31 23:59 → 2025-01-01 00:01（跨 UTC 日，仅毫秒算术，不依赖日历）。
        let prev = 1_735_689_540_000; // 23:59
        let next = 1_735_689_660_000; // 次日 00:01
        let g = detect_tail_gap(Some(prev), next, STEP).unwrap();
        assert_eq!(g.start, 1_735_689_600_000); // 00:00
        assert_eq!(g.end, 1_735_689_600_000);
        assert_eq!(g.missing_bars(STEP), 1);
    }

    #[test]
    fn tail_non_positive_step_returns_none() {
        assert_eq!(detect_tail_gap(Some(m(0)), m(5), 0), None);
        assert_eq!(detect_tail_gap(Some(m(0)), m(5), -STEP), None);
    }

    // ---- scan_gaps -------------------------------------------------------

    #[test]
    fn scan_empty_or_single_key_has_no_gap() {
        assert!(scan_gaps(Vec::<i64>::new(), STEP).is_empty());
        assert!(scan_gaps([m(1)], STEP).is_empty());
    }

    #[test]
    fn scan_contiguous_minutes_have_no_gap() {
        assert!(scan_gaps([m(0), m(1), m(2), m(3)], STEP).is_empty());
    }

    #[test]
    fn scan_detects_single_missing_minute() {
        assert_eq!(
            scan_gaps([m(0), m(2)], STEP),
            vec![GapRange::new(m(1), m(1))]
        );
    }

    #[test]
    fn scan_detects_multiple_segments_and_bars() {
        // 0..1 连续；缺 2、3；4 存在；缺 6（5 之后到 7）。
        let got = scan_gaps([m(0), m(1), m(4), m(5), m(7)], STEP);
        assert_eq!(
            got,
            vec![GapRange::new(m(2), m(3)), GapRange::new(m(6), m(6))]
        );
    }

    #[test]
    fn scan_handles_unsorted_input() {
        // 乱序喂入但实际连续 → 无缺口。
        assert!(scan_gaps([m(3), m(1), m(2), m(0)], STEP).is_empty());
    }

    #[test]
    fn scan_deduplicates_repeated_starts() {
        // 重复的 0 去重后只剩 0 与 3 → 缺 1、2。
        let got = scan_gaps([m(0), m(0), m(3)], STEP);
        assert_eq!(got, vec![GapRange::new(m(1), m(2))]);
    }

    #[test]
    fn scan_floors_end_for_non_aligned_boundary() {
        // b=2.5 分（非整分钟，防御输入）：只报确定缺失的第 1 分，宁少报。
        let got = scan_gaps([0, 150_000], STEP);
        assert_eq!(got, vec![GapRange::new(STEP, STEP)]);
    }

    #[test]
    fn scan_non_positive_step_returns_empty() {
        assert!(scan_gaps([m(0), m(5)], 0).is_empty());
    }

    // ---- stall_gap -------------------------------------------------------

    #[test]
    fn stall_below_or_equal_threshold_reports_nothing() {
        // 差 2 分 < 3 分阈值。
        assert_eq!(stall_gap(m(0), m(2), STEP, STALL), None);
        // 恰好等于阈值（3 分）也不报。
        assert_eq!(stall_gap(m(0), m(3), STEP, STALL), None);
    }

    #[test]
    fn stall_just_over_threshold_reports_only_closed_minutes() {
        // 末键 0 分，now=3 分 +1ms：刚越阈；已收盘的是 1、2 分（3 分桶尚未收盘）。
        let g = stall_gap(m(0), m(3) + 1, STEP, STALL).unwrap();
        assert_eq!(g, GapRange::new(m(1), m(2)));
        assert_eq!(g.missing_bars(STEP), 2);
    }

    #[test]
    fn stall_at_exact_minute_boundary_excludes_current_open_minute() {
        // now 恰为整分钟边界 T=4 分：4 分桶 [4,5) 未收盘，上界=3 分。
        let g = stall_gap(m(0), m(4), STEP, STALL).unwrap();
        assert_eq!(g, GapRange::new(m(1), m(3)));
    }

    #[test]
    fn stall_when_now_at_or_before_last_reports_nothing() {
        assert_eq!(stall_gap(m(10), m(10), STEP, STALL), None);
        assert_eq!(stall_gap(m(10), m(9), STEP, STALL), None);
    }

    #[test]
    fn stall_non_positive_params_return_none() {
        assert_eq!(stall_gap(m(0), m(10), 0, STALL), None);
        assert_eq!(stall_gap(m(0), m(10), STEP, 0), None);
    }

    // ---- GapRegistry -----------------------------------------------------

    #[test]
    fn registry_records_each_missing_minute_and_dedups() {
        let reg = GapRegistry::new();
        // 首登一段 [1,2]：新增 2。
        let added = reg.record("binance", "BTCUSDT", &GapRange::new(m(1), m(2)), STEP);
        assert_eq!(added, 2);
        assert_eq!(reg.len(), 2);
        // 同段再来（另一检测点/下一周期）：全部已存在，新增 0。
        let again = reg.record("binance", "BTCUSDT", &GapRange::new(m(1), m(2)), STEP);
        assert_eq!(again, 0);
        assert_eq!(reg.len(), 2);
    }

    #[test]
    fn registry_expanding_segment_adds_only_new_minute() {
        let reg = GapRegistry::new();
        let _ = reg.record("binance", "BTCUSDT", &GapRange::new(m(1), m(1)), STEP);
        // 段扩成 [1,3]：只有 2、3 是新分钟。
        let added = reg.record("binance", "BTCUSDT", &GapRange::new(m(1), m(3)), STEP);
        assert_eq!(added, 2);
        assert_eq!(reg.len(), 3);
    }

    #[test]
    fn registry_snapshot_is_ordered_and_partitions_by_identity() {
        let reg = GapRegistry::new();
        let _ = reg.record("binance", "BTCUSDT", &GapRange::new(m(2), m(2)), STEP);
        let _ = reg.record("binance", "BTCUSDT", &GapRange::new(m(1), m(1)), STEP);
        let _ = reg.record("ctp", "cu2512", &GapRange::new(m(1), m(1)), STEP); // 不同 venue 不串
        let _ = reg.record("binance", "ETHUSDT", &GapRange::new(m(1), m(1)), STEP); // 不同 symbol 不串

        let snap = reg.snapshot();
        // BTreeSet 全序：venue → symbol → start。
        let keys: Vec<(&str, &str, i64)> = snap
            .iter()
            .map(|p| (p.venue.as_str(), p.symbol.as_str(), p.start))
            .collect();
        assert_eq!(
            keys,
            vec![
                ("binance", "BTCUSDT", m(1)),
                ("binance", "BTCUSDT", m(2)),
                ("binance", "ETHUSDT", m(1)),
                ("ctp", "cu2512", m(1)),
            ]
        );
        assert!(!reg.is_empty());
    }

    #[test]
    fn registry_ignores_invalid_range_and_step() {
        let reg = GapRegistry::new();
        assert_eq!(
            reg.record("binance", "X", &GapRange::new(m(2), m(1)), STEP),
            0
        );
        assert_eq!(reg.record("binance", "X", &GapRange::new(m(1), m(2)), 0), 0);
        assert!(reg.is_empty());
    }

    #[test]
    fn registry_large_range_adds_each_minute_then_dedups() {
        // 断线一整天 = 1440 个缺失分钟：首登全计、再登零增；按序列分组不重复字符串语义不变。
        let reg = GapRegistry::new();
        let day = GapRange::new(m(0), m(1439));
        assert_eq!(reg.record("binance", "BTCUSDT", &day, STEP), 1440);
        assert_eq!(reg.len(), 1440);
        assert_eq!(reg.record("binance", "BTCUSDT", &day, STEP), 0);
        assert_eq!(reg.snapshot().len(), 1440);
        // 扩到次日一分钟：只新增 1。
        assert_eq!(
            reg.record("binance", "BTCUSDT", &GapRange::new(m(0), m(1440)), STEP),
            1
        );
        assert_eq!(reg.len(), 1441);
    }

    #[test]
    fn tail_then_stall_same_minute_dedups_via_registry() {
        // 尾检先报、静默扫描再报同一分钟：集合内只留一条（三路幂等的关键）。
        let reg = GapRegistry::new();
        let tail = detect_tail_gap(Some(m(0)), m(3), STEP).unwrap(); // [1,2]
        let _ = reg.record("binance", "BTCUSDT", &tail, STEP);
        let stall = stall_gap(m(0), m(3) + 1, STEP, STALL).unwrap(); // [1,2]
        let added = reg.record("binance", "BTCUSDT", &stall, STEP);
        assert_eq!(added, 0);
        assert_eq!(reg.len(), 2);
    }

    #[test]
    fn gap_range_missing_bars_handles_invalid() {
        assert_eq!(GapRange::new(m(2), m(1)).missing_bars(STEP), 0);
        assert_eq!(GapRange::new(m(1), m(2)).missing_bars(0), 0);
        assert_eq!(GapRange::new(m(1), m(1)).missing_bars(STEP), 1);
    }

    // 防御：去重键集合语义不依赖具体容器之外的额外状态。
    #[test]
    fn pending_gap_identity_is_venue_symbol_start_only() {
        let mut set = HashSet::new();
        assert!(set.insert(PendingGap::new("binance", "BTCUSDT", m(1))));
        assert!(!set.insert(PendingGap::new("binance", "BTCUSDT", m(1))));
        assert!(set.insert(PendingGap::new("binance", "BTCUSDT", m(2))));
    }

    // ---- coalesce_starts（P4 聚合纯函数） --------------------------------

    #[test]
    fn coalesce_empty_and_non_positive_step() {
        assert!(coalesce_starts(&BTreeSet::new(), STEP).is_empty());
        let mut one = BTreeSet::new();
        one.insert(m(3));
        assert_eq!(coalesce_starts(&one, 0), Vec::<GapRange>::new());
    }

    #[test]
    fn coalesce_single_minute() {
        let mut s = BTreeSet::new();
        s.insert(m(5));
        assert_eq!(coalesce_starts(&s, STEP), vec![GapRange::new(m(5), m(5))]);
    }

    #[test]
    fn coalesce_contiguous_minutes_into_one_segment() {
        let s: BTreeSet<i64> = (1..=4).map(m).collect();
        assert_eq!(coalesce_starts(&s, STEP), vec![GapRange::new(m(1), m(4))]);
    }

    #[test]
    fn coalesce_multiple_segments() {
        // 集合只列「缺失分钟」本身：1,2,4,7 → 段 [1,2]、[4,4]、[7,7]。
        let s: BTreeSet<i64> = [m(1), m(2), m(4), m(7)].into_iter().collect();
        assert_eq!(
            coalesce_starts(&s, STEP),
            vec![
                GapRange::new(m(1), m(2)),
                GapRange::new(m(4), m(4)),
                GapRange::new(m(7), m(7)),
            ]
        );
    }

    // ---- claim_due / complete / fail（pending→inflight→收口） -----------

    #[test]
    fn claim_moves_pending_to_inflight_and_returns_coalesced_ranges() {
        let reg = GapRegistry::new();
        let _ = reg.record("binance", "BTCUSDT", &GapRange::new(m(1), m(2)), STEP);
        let _ = reg.record("binance", "BTCUSDT", &GapRange::new(m(4), m(4)), STEP);

        // 认领：聚合成两段，按 (venue,symbol) 分组返回。
        let due = reg.claim_due(STEP);
        assert_eq!(due.len(), 1);
        let (series, ranges) = &due[0];
        assert_eq!(series.venue, "binance");
        assert_eq!(series.symbol, "BTCUSDT");
        assert_eq!(
            ranges,
            &vec![GapRange::new(m(1), m(2)), GapRange::new(m(4), m(4))]
        );

        // pending 已清空：len/snapshot 为 0，再次认领拿不到东西（分钟在 inflight）。
        assert_eq!(reg.len(), 0);
        assert!(reg.is_empty());
        assert!(reg.claim_due(STEP).is_empty());
    }

    #[test]
    fn claim_partitions_by_series_without_crosstalk() {
        let reg = GapRegistry::new();
        let _ = reg.record("binance", "BTCUSDT", &GapRange::new(m(1), m(1)), STEP);
        let _ = reg.record("deribit", "BTC-PERPETUAL", &GapRange::new(m(2), m(3)), STEP);

        let mut due = reg.claim_due(STEP);
        due.sort_by(|a, b| a.0.venue.cmp(&b.0.venue));
        assert_eq!(due.len(), 2);
        assert_eq!(due[0].0.venue, "binance");
        assert_eq!(due[0].1, vec![GapRange::new(m(1), m(1))]);
        assert_eq!(due[1].0.venue, "deribit");
        assert_eq!(due[1].1, vec![GapRange::new(m(2), m(3))]);
        assert!(reg.is_empty());
    }

    #[test]
    fn complete_removes_inflight_minutes_permanently() {
        let reg = GapRegistry::new();
        let _ = reg.record("binance", "BTCUSDT", &GapRange::new(m(1), m(3)), STEP);
        let _ = reg.claim_due(STEP);

        // 补回 1、2、3 分钟：从 inflight 删除。
        reg.complete("binance", "BTCUSDT", &[m(1), m(2), m(3)]);

        // 再次认领为空（分钟已彻底收口，不在 pending 也不在 inflight）。
        assert!(reg.claim_due(STEP).is_empty());
    }

    #[test]
    fn complete_partial_then_fail_reattaches_remaining() {
        let reg = GapRegistry::new();
        let _ = reg.record("binance", "BTCUSDT", &GapRange::new(m(1), m(3)), STEP);
        let _ = reg.claim_due(STEP);

        // 只完成第 2 分钟：其余 1、3 仍在 inflight。
        reg.complete("binance", "BTCUSDT", &[m(2)]);
        // 失败退回剩余两段（单分钟 1 与 3），应能再次认领。
        reg.fail(
            "binance",
            "BTCUSDT",
            &[GapRange::new(m(1), m(1)), GapRange::new(m(3), m(3))],
            STEP,
        );
        let due = reg.claim_due(STEP);
        assert_eq!(
            due[0].1,
            vec![GapRange::new(m(1), m(1)), GapRange::new(m(3), m(3))]
        );
    }

    #[test]
    fn fail_returns_ranges_back_to_pending_for_retry() {
        let reg = GapRegistry::new();
        let _ = reg.record("binance", "BTCUSDT", &GapRange::new(m(1), m(2)), STEP);
        let _ = reg.claim_due(STEP);
        assert!(reg.is_empty());

        // 拉取失败：整段退回 pending，下轮可再认领。
        reg.fail("binance", "BTCUSDT", &[GapRange::new(m(1), m(2))], STEP);
        assert_eq!(reg.len(), 2);
        let due = reg.claim_due(STEP);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].1, vec![GapRange::new(m(1), m(2))]);
    }

    #[test]
    fn record_while_inflight_is_ignored_idempotently() {
        let reg = GapRegistry::new();
        let _ = reg.record("binance", "BTCUSDT", &GapRange::new(m(1), m(2)), STEP);
        let _ = reg.claim_due(STEP); // 1、2 进入 inflight，pending 空。

        // worker 回补期间，尾检/扫描再次登记同样的 1、2 分钟：必须被忽略，
        // 不重新进 pending（否则会重复回补）。
        let added = reg.record("binance", "BTCUSDT", &GapRange::new(m(1), m(2)), STEP);
        assert_eq!(added, 0);
        assert_eq!(reg.len(), 0);
        assert!(reg.claim_due(STEP).is_empty());

        // 但登记一个「新」分钟（不在 inflight）仍正常入 pending。
        let added_new = reg.record("binance", "BTCUSDT", &GapRange::new(m(3), m(3)), STEP);
        assert_eq!(added_new, 1);
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn complete_and_fail_on_unknown_series_are_noop() {
        let reg = GapRegistry::new();
        // 未认领/不存在的序列上调用 complete/fail 不应 panic、不产生 pending。
        reg.complete("binance", "X", &[m(1)]);
        reg.fail("binance", "X", &[GapRange::new(m(1), m(1))], STEP);
        assert!(reg.is_empty());
    }
}
