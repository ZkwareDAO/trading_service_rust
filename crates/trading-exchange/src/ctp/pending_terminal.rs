//! 待终态对齐表：全成委托回报（`OnRtnOrder`）先于成交回报（`OnRtnTrade`）到达时，
//! 暂存逻辑单 cid，等成交帧补齐真实成交价（VWAP）后再向上游投影**唯一一次**终态。
//!
//! 背景：CTP 的委托快照管生命周期但**不带成交价**，成交回报才带真实成交价；真实
//! SimNow 上「全成快照」常先于成交回报 1~2ms 到达。若见全成快照即抛终态，晚到的
//! 成交帧会在共享订单状态机被当「终态后帧」丢弃，导致 `filled_price` 恒 0。本表把
//! 这一两路异步的 join 收口在 CTP 层（不改共享状态机）。
//!
//! 生命周期：**仅会话内存**（`Mutex<HashMap>`），不落盘、不跨连接；仅专用会话线程
//! 读写。表项生存期 = 全成快照到达（mark）→ 成交到齐投影终态（remove），或异常丢帧
//! 由 inner_loop 空闲 tick 按宽限兜底取出（take_due）。重连进入 inner_loop 时 clear。

use std::collections::HashMap;
use std::sync::Mutex;

/// 一条「终态已由委托快照确认、但成交回报尚未到齐」的在途对齐项。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PendingTerminal {
    /// 委托快照给出的累计成交量：成交回报累计达到此值即视为到齐、可投影终态。
    pub expected_qty: i64,
    /// 暂存起始时刻（epoch ms），供丢帧兜底宽限判定。
    pub marked_ms: i64,
}

/// cid → 待对齐终态项的会话内存小表。
#[derive(Default)]
pub(crate) struct PendingTerminalTable {
    inner: Mutex<HashMap<String, PendingTerminal>>,
}

impl PendingTerminalTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// 标记 cid 待对齐；重复 mark（快照重放/终态再次到达）以新值覆盖并刷新时刻。
    pub fn mark(&self, cid: &str, expected_qty: i64, now_ms: i64) {
        self.inner.lock().unwrap().insert(
            cid.to_string(),
            PendingTerminal {
                expected_qty: expected_qty.max(0),
                marked_ms: now_ms,
            },
        );
    }

    /// 是否在待对齐集合中。
    pub fn contains(&self, cid: &str) -> bool {
        self.inner.lock().unwrap().contains_key(cid)
    }

    /// 读取待对齐项（应成交量/暂存时刻）。
    #[cfg(test)]
    pub fn get(&self, cid: &str) -> Option<PendingTerminal> {
        self.inner.lock().unwrap().get(cid).copied()
    }

    /// 移除一条（终态已投影 / 撤单回收）；不存在为无害 no-op。
    pub fn remove(&self, cid: &str) {
        self.inner.lock().unwrap().remove(cid);
    }

    /// 清空全部（重连进入新 inner_loop，上一连接的暂存不跨连接残留）。
    pub fn clear(&self) {
        self.inner.lock().unwrap().clear();
    }

    /// 当前暂存条数（测试/诊断用）。
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// 取出并移除所有「暂存时长已达宽限」的到期 cid（异常丢帧兜底），按暂存时刻
    /// 升序返回 `(cid, expected_qty)`，使投影顺序稳定、可测。未到期项保留。
    pub fn take_due(&self, now_ms: i64, grace_ms: i64) -> Vec<(String, i64)> {
        let mut map = self.inner.lock().unwrap();
        let mut due: Vec<(String, PendingTerminal)> = map
            .iter()
            .filter(|(_, p)| now_ms.saturating_sub(p.marked_ms) >= grace_ms)
            .map(|(cid, p)| (cid.clone(), *p))
            .collect();
        // 暂存早的先投影（确定性顺序）。
        due.sort_by_key(|(_, p)| p.marked_ms);
        for (cid, _) in &due {
            map.remove(cid);
        }
        due.into_iter()
            .map(|(cid, p)| (cid, p.expected_qty))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mark_contains_get_remove() {
        let t = PendingTerminalTable::new();
        assert!(!t.contains("a"));
        assert_eq!(t.get("a"), None);

        t.mark("a", 2, 100);
        assert!(t.contains("a"));
        assert_eq!(
            t.get("a"),
            Some(PendingTerminal {
                expected_qty: 2,
                marked_ms: 100
            })
        );

        // 重复 mark 覆盖刷新。
        t.mark("a", 3, 200);
        assert_eq!(
            t.get("a"),
            Some(PendingTerminal {
                expected_qty: 3,
                marked_ms: 200
            })
        );
        assert_eq!(t.len(), 1);

        t.remove("a");
        assert!(!t.contains("a"));
        t.remove("a"); // 幂等 no-op，不 panic。
    }

    #[test]
    fn mark_clamps_negative_expected_qty() {
        let t = PendingTerminalTable::new();
        t.mark("x", -5, 1);
        assert_eq!(t.get("x").unwrap().expected_qty, 0);
    }

    #[test]
    fn take_due_returns_only_expired_and_removes_them() {
        let t = PendingTerminalTable::new();
        t.mark("old", 1, 0); // 已等 1500ms
        t.mark("fresh", 2, 1000); // 仅等 500ms
        t.mark("mid", 3, 500); // 等 1000ms

        let due = t.take_due(1500, 1000);
        // 到期：old（1500≥1000）、mid（1000≥1000）；fresh（500<1000）保留。
        assert_eq!(due, vec![("old".to_string(), 1), ("mid".to_string(), 3)]);
        assert!(!t.contains("old"));
        assert!(!t.contains("mid"));
        assert!(t.contains("fresh"));
        // 再次取：已被移除，不重复返回。
        assert!(t.take_due(1500, 1000).is_empty());
    }

    #[test]
    fn clear_empties_table() {
        let t = PendingTerminalTable::new();
        t.mark("a", 1, 0);
        t.mark("b", 1, 0);
        assert_eq!(t.len(), 2);
        t.clear();
        assert_eq!(t.len(), 0);
    }
}
