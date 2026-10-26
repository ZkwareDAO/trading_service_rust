//! 定稿 bar 待刷盘内存表。
//!
//! 四家交易所的定稿 bar（WS 原生收盘帧 / CTP 聚合定稿）同一入口 [`Self::push`]；
//! KlineRepo 定时 [`Self::drain_all`] 批量 append CSV，成功即清空。
//! 纯内存、不实现 Model、不快照恢复；行模型 [`Bar`] 在 trading-models。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use trading_models::Bar;

/// 待刷盘 bar 窗口的默认最大积压（背压硬上限）。bar 为分钟级、量远小于 tick，
/// 上限取较小值即可；flush 持续失败超限时丢最新定稿 bar 并计 dropped，不 OOM。
pub const DEFAULT_MAX_PENDING_BARS: usize = 100_000;

/// 定稿 bar 待刷盘窗口（多写者＝各行情连接；flush 线程批量取出）。
#[derive(Debug)]
pub struct BarTable {
    rows: Mutex<Vec<Bar>>,
    /// 窗口积压上限：满后新 bar 丢弃。
    capacity: usize,
    /// 因积压达上限而丢弃的 bar 数（诊断/告警用）。
    dropped: AtomicU64,
}

impl Default for BarTable {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_MAX_PENDING_BARS)
    }
}

impl BarTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// 指定积压上限构造（测试/调参用）。
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            rows: Mutex::new(Vec::with_capacity(capacity.min(1024))),
            capacity,
            dropped: AtomicU64::new(0),
        }
    }

    /// 追加一根定稿 bar（不可变行，只追加不覆盖）；积压达上限时丢弃最新 bar
    /// 并累加 dropped（背压，不阻塞多写者、不 OOM）。
    pub fn push(&self, bar: Bar) {
        let mut rows = self.rows.lock().unwrap();
        if rows.len() >= self.capacity {
            drop(rows);
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        rows.push(bar);
    }

    /// 因积压达上限而丢弃的累计 bar 数。
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// 读取并清零丢弃计数（周期告警取增量用）。
    pub fn take_dropped(&self) -> u64 {
        self.dropped.swap(0, Ordering::Relaxed)
    }

    /// 当前窗口快照（不清空；诊断/测试用）。
    pub fn snapshot(&self) -> Vec<Bar> {
        self.rows.lock().unwrap().clone()
    }

    /// 取出全部待写 bar 并清空窗口（KlineRepo 批量写盘成功后调用）。
    pub fn drain_all(&self) -> Vec<Bar> {
        std::mem::take(&mut *self.rows.lock().unwrap())
    }

    /// 写盘失败时把本批 bar 放回窗口**队首**（保留与既有新增行的相对顺序：
    /// 重放的旧批在前、drain 后新到的在后），下轮 flush 重试，不丢数据。
    pub fn requeue_front(&self, mut batch: Vec<Bar>) {
        let mut guard = self.rows.lock().unwrap();
        batch.append(&mut guard);
        *guard = batch;
    }

    pub fn len(&self) -> usize {
        self.rows.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.lock().unwrap().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use trading_models::Bar;

    fn bar(symbol: &str, start: i64) -> Bar {
        let mut b = Bar::new("ctp", symbol, 20261024, start, start + 60_000);
        b.close = Decimal::from(100);
        b.volume = 3;
        b
    }

    #[test]
    fn push_keeps_finalized_bars_in_arrival_order() {
        // 定稿 bar 只追加、不覆盖：同合约不同分钟是两行。
        let t = BarTable::new();
        t.push(bar("cu2512", 60_000));
        t.push(bar("cu2511", 60_000));
        t.push(bar("cu2512", 120_000));
        assert_eq!(t.len(), 3);
        let snap = t.snapshot();
        assert_eq!(
            snap.iter()
                .map(|b| (b.symbol.as_str(), b.start_time))
                .collect::<Vec<_>>(),
            vec![("cu2512", 60_000), ("cu2511", 60_000), ("cu2512", 120_000)]
        );
    }

    #[test]
    fn drain_all_returns_batch_and_clears() {
        // KlineRepo 批量写盘成功后清空；不清空全部历史，只清本批。
        let t = BarTable::new();
        t.push(bar("cu2512", 60_000));
        t.push(bar("cu2511", 60_000));

        let batch = t.drain_all();
        assert_eq!(batch.len(), 2);
        assert!(t.is_empty());
        assert!(t.drain_all().is_empty());

        t.push(bar("rb2611", 180_000));
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn push_drops_newest_and_counts_when_capacity_saturated() {
        // 背压：窗口满后丢最新定稿 bar 并计 dropped，旧 bar 保留、不 OOM。
        let t = BarTable::with_capacity(2);
        t.push(bar("a", 60_000));
        t.push(bar("b", 60_000));
        t.push(bar("c", 60_000)); // 丢弃
        assert_eq!(t.len(), 2);
        assert_eq!(t.dropped_count(), 1);
        let snap = t.snapshot();
        assert_eq!(
            snap.iter().map(|b| b.symbol.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }
}
