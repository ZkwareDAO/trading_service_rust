//! 主力选举相关的两张纯内存表（与 [`crate::quote::QuoteTable`] 同层）。
//!
//! * [`LastVolumeTable`]：合约 → 当日最后累计成交量（每笔 tick 覆盖，选举原料）；
//! * [`DominantTable`]：品种 → 当前主力（symbol + 交易日，收盘选举写入，次日冻结）。
//!
//! 均**不实现 Model、不注册 store-core、不做快照/恢复/日切**——进程重启后为空，
//! 由 CTP 订阅/选举重新填充。

use std::collections::HashMap;

use dashmap::DashMap;

/// 合约 → 当日最后累计成交量（每笔 tick 覆盖）。主力选举按此比较各合约量。
#[derive(Debug, Default)]
pub struct LastVolumeTable {
    inner: DashMap<String, i64>,
}

impl LastVolumeTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&self, symbol: &str, cum_volume: i64) {
        self.inner.insert(symbol.to_string(), cum_volume);
    }

    pub fn get(&self, symbol: &str) -> Option<i64> {
        self.inner.get(symbol).map(|v| *v.value())
    }

    pub fn remove(&self, symbol: &str) {
        self.inner.remove(symbol);
    }

    /// 全表快照：(合约, 当日累计量)，按合约排序保证确定性（主力选举遍历用）。
    pub fn snapshot(&self) -> Vec<(String, i64)> {
        let mut out: Vec<(String, i64)> = self
            .inner
            .iter()
            .map(|e| (e.key().clone(), *e.value()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

/// 主力记录：品种下当前主力合约 + 选举归属交易日。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DominantEntry {
    /// 主力合约（如 cu2512）。
    pub symbol: String,
    /// 写入时的交易日 YYYYMMDD（次日含夜盘整段冻结，不漂移）。
    pub trading_day: u32,
    /// 选举时该主力的当日累计量（诊断用）。
    pub cum_volume: i64,
}

/// 品种（如 cu）→ 当前主力。收盘选举任务写入，行情/推送读。
#[derive(Debug, Default)]
pub struct DominantTable {
    inner: DashMap<String, DominantEntry>,
}

impl DominantTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&self, product: &str, entry: DominantEntry) {
        self.inner.insert(product.to_string(), entry);
    }

    pub fn get(&self, product: &str) -> Option<DominantEntry> {
        self.inner.get(product).map(|e| e.value().clone())
    }

    /// 主力符号快捷查询。
    pub fn dominant_symbol(&self, product: &str) -> Option<String> {
        self.inner.get(product).map(|e| e.symbol.clone())
    }

    pub fn remove(&self, product: &str) {
        self.inner.remove(product);
    }

    /// 全表快照（品种 → 记录）。
    pub fn snapshot(&self) -> HashMap<String, DominantEntry> {
        self.inner
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_volume_snapshot_sorted() {
        let t = LastVolumeTable::new();
        t.put("cu2601", 100);
        t.put("cu2512", 500);
        t.put("rb2611", 7);
        let snap = t.snapshot();
        // 按合约字典序，与插入顺序无关。
        assert_eq!(
            snap,
            vec![
                ("cu2512".to_string(), 500),
                ("cu2601".to_string(), 100),
                ("rb2611".to_string(), 7),
            ]
        );
        assert_eq!(t.get("cu2512"), Some(500));
        assert_eq!(t.get("nope"), None);
    }

    #[test]
    fn dominant_table_put_get_and_symbol() {
        let d = DominantTable::new();
        assert!(d.dominant_symbol("cu").is_none());
        d.put(
            "cu",
            DominantEntry {
                symbol: "cu2512".into(),
                trading_day: 20261023,
                cum_volume: 500,
            },
        );
        assert_eq!(d.dominant_symbol("cu").as_deref(), Some("cu2512"));
        let e = d.get("cu").unwrap();
        assert_eq!(e.trading_day, 20261023);
        assert_eq!(d.len(), 1);
        // 换月选举覆盖。
        d.put(
            "cu",
            DominantEntry {
                symbol: "cu2601".into(),
                trading_day: 20261024,
                cum_volume: 900,
            },
        );
        assert_eq!(d.dominant_symbol("cu").as_deref(), Some("cu2601"));
        d.remove("cu");
        assert!(d.is_empty());
    }
}
