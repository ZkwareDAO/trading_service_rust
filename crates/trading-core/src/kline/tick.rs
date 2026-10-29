//! CTP 原始 tick 的 WAL 待刷盘窗口。
//!
//! 与「每合约只留最新价」的 QuoteTable 不同：K线原始数据要求**每笔**留档，
//! 故本表是有界追加窗口——CTP drain 线程对每笔 K线集 tick [`Self::append`]，
//! trading-store 的 KlineRepo 定时 [`Self::drain_all`] 批量写 CSV，写成功即清空。
//! 内存占用＝ flush 间隔 × tick 速率，与全天笔数无关。
//!
//! 纯内存、不实现 Model、不快照恢复；行模型 [`RawTick`] 在 trading-models。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use trading_models::RawTick;

/// 待刷盘窗口的默认最大积压行数（背压硬上限）。flush 持续失败 + 行情持续到达时，
/// 超过此值丢弃最新帧并计入 dropped，宁可丢 K线原始 tick 也不让进程 OOM
/// （K线为旁路数据，不得拖垮订单/仓位等核心内存）。正常 5s flush 远达不到。
pub const DEFAULT_MAX_PENDING_TICKS: usize = 1_000_000;

/// 原始 tick WAL 待刷盘窗口（单写者＝CTP drain；flush 线程批量取出）。
#[derive(Debug)]
pub struct TickTable {
    rows: Mutex<Vec<RawTick>>,
    /// 窗口积压上限：满后新帧丢弃。
    capacity: usize,
    /// 因积压达上限而丢弃的 tick 数（诊断/告警用）。
    dropped: AtomicU64,
}

impl Default for TickTable {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_MAX_PENDING_TICKS)
    }
}

impl TickTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// 指定积压上限构造（测试/按部署调参用）。
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            rows: Mutex::new(Vec::with_capacity(capacity.min(1024))),
            capacity,
            dropped: AtomicU64::new(0),
        }
    }

    /// 追加一笔原始 tick（按到达顺序保留，含非主力合约）；窗口积压达上限时
    /// 丢弃该最新帧并累加 dropped（背压，不阻塞 drain 线程、不 OOM）。
    pub fn append(&self, row: RawTick) {
        let mut rows = self.rows.lock().unwrap();
        if rows.len() >= self.capacity {
            drop(rows);
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        rows.push(row);
    }

    /// 因积压达上限而丢弃的累计 tick 数。
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// 读取并清零丢弃计数（周期告警取增量用）。
    pub fn take_dropped(&self) -> u64 {
        self.dropped.swap(0, Ordering::Relaxed)
    }

    /// 当前窗口快照（不清空；诊断/测试用）。
    pub fn snapshot(&self) -> Vec<RawTick> {
        self.rows.lock().unwrap().clone()
    }

    /// 取出全部待写行并清空窗口（KlineRepo 批量写盘成功后调用）。
    pub fn drain_all(&self) -> Vec<RawTick> {
        std::mem::take(&mut *self.rows.lock().unwrap())
    }

    /// 写盘失败时把本批 tick 放回窗口**队首**（旧批在前、drain 后新到在后），
    /// 下轮 flush 重试，不丢数据。
    pub fn requeue_front(&self, mut batch: Vec<RawTick>) {
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
    use trading_models::RawTick;

    fn tick(symbol: &str, ts: i64, vol: i64) -> RawTick {
        RawTick {
            exchange: "ctp".into(),
            exchange_id: "SHFE".into(),
            symbol: symbol.into(),
            timestamp: ts,
            trading_day: 20261024,
            price: Decimal::from(75_000 + vol),
            open: Decimal::ZERO,
            high: Decimal::ZERO,
            low: Decimal::ZERO,
            close: Decimal::ZERO,
            volume: vol,
            quote_volume: Decimal::ZERO,
            open_interest: Decimal::ZERO,
            bid: [Decimal::ZERO; 5],
            ask: [Decimal::ZERO; 5],
            bid_vol: [0; 5],
            ask_vol: [0; 5],
        }
    }

    #[test]
    fn append_keeps_every_tick_in_arrival_order() {
        // WAL 与「每合约只留最新」的覆盖表不同：每笔 K线集 tick（含非主力合约）
        // 都必须按到达顺序留档，供批量追加 CSV。
        let t = TickTable::new();
        assert!(t.is_empty());
        t.append(tick("cu2511", 100, 10));
        t.append(tick("cu2512", 200, 20));
        t.append(tick("cu2511", 300, 11));
        assert_eq!(t.len(), 3);

        let all = t.snapshot();
        assert_eq!(all.len(), 3);
        assert_eq!(
            all.iter()
                .map(|r| (r.symbol.as_str(), r.timestamp))
                .collect::<Vec<_>>(),
            vec![("cu2511", 100), ("cu2512", 200), ("cu2511", 300)]
        );
    }

    #[test]
    fn drain_all_returns_everything_and_clears_window() {
        // flush 取出全部待写行：成功写盘后窗口必须清空，下批不重复。
        let t = TickTable::new();
        t.append(tick("cu2512", 1, 1));
        t.append(tick("cu2512", 2, 2));

        let batch = t.drain_all();
        assert_eq!(batch.len(), 2);
        assert!(t.is_empty());
        // 再 drain 一次为空（不重复出旧数据）。
        assert!(t.drain_all().is_empty());

        // 清空后仍可继续追加（写盘任务与 drain 线程并发的正常节奏）。
        t.append(tick("rb2611", 3, 3));
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn append_drops_newest_and_counts_when_capacity_saturated() {
        // 背压硬上限：窗口满后丢最新帧（不入队）并累加 dropped，不 OOM、不阻塞。
        let t = TickTable::with_capacity(2);
        t.append(tick("a", 1, 1));
        t.append(tick("b", 2, 2));
        t.append(tick("c", 3, 3)); // 达上限，丢弃
        t.append(tick("d", 4, 4)); // 继续丢弃
        assert_eq!(t.len(), 2);
        assert_eq!(t.dropped_count(), 2);
        // 已入队的前两帧保留（FIFO，旧数据不被新数据顶掉）。
        let snap = t.snapshot();
        assert_eq!(
            snap.iter().map(|r| r.symbol.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }

    #[test]
    fn requeue_front_is_not_subject_to_capacity_drop() {
        // 写盘失败重放的整批必须完整放回（不受容量上限丢弃），下轮重试不丢数据。
        let t = TickTable::with_capacity(1);
        let mut batch = vec![tick("a", 1, 1), tick("b", 2, 2), tick("c", 3, 3)];
        // 模拟 drain 取出后又有新帧入队，失败 requeue 时整批在前。
        t.append(tick("new", 9, 9));
        batch.append(&mut t.drain_all());
        t.requeue_front(batch);
        assert_eq!(t.len(), 4, "重放批 + 新帧全部保留，不被容量裁剪");
        assert_eq!(t.dropped_count(), 0);
    }
}
