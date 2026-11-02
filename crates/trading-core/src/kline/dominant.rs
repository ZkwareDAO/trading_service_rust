//! 主力选举相关的纯内存表（与 [`crate::quote::QuoteTable`] 同层）。
//!
//! * [`LastTickStatTable`]：合约 → 当日最后 tick 统计
//!   （交易日 / 累计成交量 / 持仓量；每笔 tick 覆盖，选举原料）；
//! * [`DominantTable`]：品种 → 当前主力（symbol + 交易日 + 连击候选，
//!   收盘选举写入，次日冻结）。
//!
//! 均**不实现 Model、不注册 store-core、不做快照/恢复/日切**——进程重启后为空，
//! 由 CTP 订阅/选举重新填充。

use std::collections::HashMap;

use dashmap::DashMap;
use rust_decimal::Decimal;

/// 一个合约当日最后 tick 的选举原料快照。
///
/// 成交量与持仓量**同口径**带 CTP 交易日：选举只认 `trading_day == 选举日`
/// 的记录，避免周末/节假日无数据时把陈旧值（尤其持仓量不日清）带入选举。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastTickStat {
    /// CTP 交易日 YYYYMMDD（夜盘已归次日）。
    pub trading_day: u32,
    /// 当日累计成交量（手）。
    pub cum_volume: i64,
    /// 持仓量（手）。
    pub open_interest: Decimal,
}

/// 合约 → 当日最后 tick 统计（每笔 tick 覆盖）。主力选举按配置指标比较各合约。
#[derive(Debug, Default)]
pub struct LastTickStatTable {
    inner: DashMap<String, LastTickStat>,
}

impl LastTickStatTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&self, symbol: &str, stat: LastTickStat) {
        self.inner.insert(symbol.to_string(), stat);
    }

    pub fn get(&self, symbol: &str) -> Option<LastTickStat> {
        self.inner.get(symbol).map(|v| v.value().clone())
    }

    pub fn remove(&self, symbol: &str) {
        self.inner.remove(symbol);
    }

    /// 全表快照：(合约, 统计)，按合约排序保证确定性（主力选举遍历用）。
    pub fn snapshot(&self) -> Vec<(String, LastTickStat)> {
        let mut out: Vec<(String, LastTickStat)> = self
            .inner
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
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

/// 主力切换的连击候选：同一候选连续多少个**有数据的交易日**胜出。
///
/// 进程内存、不落盘；重启清零（由 `preset_dominants` 冷启动兜底）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingCandidate {
    /// 候选合约（如 cu2705）。
    pub symbol: String,
    /// 已连续胜出的交易日数（含当轮）。
    pub streak: u32,
}

/// 主力记录：品种下当前主力合约 + 选举归属交易日 + 待确认候选。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DominantEntry {
    /// 主力合约（如 cu2612）。
    pub symbol: String,
    /// 本轮选举归属的交易日 YYYYMMDD；同一交易日不再重复计数/切换。
    pub trading_day: u32,
    /// 选举时该主力的当日指标值（成交量或持仓量，诊断用）。
    ///
    /// **0 = 暂定**（`preset_dominants` 启动预设）：首轮真实选举会无条件覆盖，
    /// 之后才启用月份守卫与确认天数。
    pub metric_value: Decimal,
    /// 连击中的候选（`None` = 无待确认换月）。
    pub pending: Option<PendingCandidate>,
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

    fn stat(day: u32, vol: i64, oi: i64) -> LastTickStat {
        LastTickStat {
            trading_day: day,
            cum_volume: vol,
            open_interest: Decimal::from(oi),
        }
    }

    #[test]
    fn last_stat_snapshot_sorted_and_carries_day() {
        let t = LastTickStatTable::new();
        t.put("cu2601", stat(20261023, 100, 900));
        t.put("cu2512", stat(20261022, 500, 700));
        t.put("rb2611", stat(20261023, 7, 70));
        let snap = t.snapshot();
        // 按合约字典序，与插入顺序无关。
        assert_eq!(
            snap.iter().map(|(s, _)| s.as_str()).collect::<Vec<_>>(),
            vec!["cu2512", "cu2601", "rb2611"]
        );
        assert_eq!(snap[1].1, stat(20261023, 100, 900));
        assert_eq!(
            t.get("cu2512"),
            Some(stat(20261022, 500, 700)),
            "get 必须保留交易日/持仓量"
        );
        assert_eq!(t.get("nope"), None);
    }

    #[test]
    fn last_stat_put_overwrites_per_tick() {
        // 每笔 tick 覆盖：同一合约只留最后一份统计。
        let t = LastTickStatTable::new();
        t.put("cu2612", stat(20261023, 10, 100));
        t.put("cu2612", stat(20261023, 25, 130));
        let snap = t.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].1.cum_volume, 25);
        assert_eq!(snap[0].1.open_interest, Decimal::from(130));
    }

    #[test]
    fn dominant_table_put_get_and_symbol() {
        let d = DominantTable::new();
        assert!(d.dominant_symbol("cu").is_none());
        d.put(
            "cu",
            DominantEntry {
                symbol: "cu2612".into(),
                trading_day: 20261023,
                metric_value: Decimal::from(500),
                pending: None,
            },
        );
        assert_eq!(d.dominant_symbol("cu").as_deref(), Some("cu2612"));
        let e = d.get("cu").unwrap();
        assert_eq!(e.trading_day, 20261023);
        assert_eq!(e.metric_value, Decimal::from(500));
        assert_eq!(d.len(), 1);
        // 换月选举覆盖（带连击候选）。
        d.put(
            "cu",
            DominantEntry {
                symbol: "cu2701".into(),
                trading_day: 20261024,
                metric_value: Decimal::from(900),
                pending: Some(PendingCandidate {
                    symbol: "cu2703".into(),
                    streak: 2,
                }),
            },
        );
        let e = d.get("cu").unwrap();
        assert_eq!(e.symbol, "cu2701");
        assert_eq!(e.pending.unwrap().streak, 2);
        d.remove("cu");
        assert!(d.is_empty());
    }
}
