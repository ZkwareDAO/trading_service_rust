//! 逻辑单（cid）→ 物理腿委托快照的会话内聚合表。
//!
//! 上期所跨今昨平仓时，一个 cid 对应 1~2 条物理报单（CloseToday/CloseYesterday），
//! 各有自己的 OrderRef/OrderSysID 与独立回报。上游订单状态机只认 cid 这一个逻辑单，
//! 故本模块负责：暂存同 cid 各腿最新委托快照，并把它们**聚合为一条**订单状态——
//! 成交量按 cid 求和、终态等所有腿齐终态、一腿成一腿拒如实呈现部分成交。
//!
//! 生命周期：会话内存（Mutex<HashMap>），某腿终态即移除该腿，cid 下无腿则整组回收；
//! 崩溃恢复不依赖本表（重连 query_orders 重放会重新建腿）。

use std::collections::HashMap;
use std::sync::Mutex;

use crate::ctp::core::event::OrderSnapshot;
use crate::ctp::core::types::OrderStatus;

/// 同 cid 各物理腿聚合后的逻辑单视图。
pub struct AggregatedOrder {
    /// Σ 已成交手数（两腿求和）。
    pub filled: i32,
    /// Σ 委托总手数。
    pub total: i32,
    /// 对外的单一委托状态（见 [`aggregate`] 规则）。
    pub status: OrderStatus,
    /// 是否所有腿都已终态（全成/撤销）。
    pub all_terminal: bool,
    /// 代表腿快照：优先取已取得 OrderSysID 的腿（回执 eid 用），否则首腿。
    pub representative: OrderSnapshot,
}

/// 纯聚合：同 cid 的各腿最新快照 → 单一逻辑视图；空切片返回 None。
///
/// 状态规则：
/// * 所有腿终态：全成（filled==total）→ `AllTraded`；否则 `Cancelled`（零成交）
///   或 `PartTradedNotQueueing`（部分成交，一腿成一腿撤/拒）；
/// * 任一腿在途：filled>0 → `PartTradedQueueing`，否则取在途腿的排队/新状态。
///
/// 成交量/总量恒为各腿求和（部分成交如实呈现，绝不假装全平）。
pub fn aggregate(legs: &[OrderSnapshot]) -> Option<AggregatedOrder> {
    let representative = legs
        .iter()
        .find(|s| !s.order_sys_id.trim().is_empty())
        .or_else(|| legs.first())?
        .clone();
    let filled: i32 = legs
        .iter()
        .fold(0, |acc, s| acc.saturating_add(s.volume_traded.max(0)));
    let total: i32 = legs
        .iter()
        .fold(0, |acc, s| acc.saturating_add(s.volume_total.max(0)));
    let all_terminal = legs.iter().all(|s| s.is_terminal());

    let status = if all_terminal {
        if filled >= total && total > 0 {
            OrderStatus::AllTraded
        } else if filled > 0 {
            // 一腿成一腿撤/拒：终态部分成交（不再排队）。
            OrderStatus::PartTradedNotQueueing
        } else {
            OrderStatus::Cancelled
        }
    } else if filled > 0 {
        OrderStatus::PartTradedQueueing
    } else {
        // 尚无成交：取任一在途腿状态（多腿排队态语义一致）。
        legs.iter()
            .find(|s| !s.is_terminal())
            .map(|s| s.status)
            .unwrap_or(OrderStatus::Unknown)
    };
    Some(AggregatedOrder {
        filled,
        total,
        status,
        all_terminal,
        representative,
    })
}

/// cid → (OrderRef → 该腿最新委托快照)；`Send + Sync`，经 Arc 在线程间共享。
#[derive(Default)]
pub struct LegTable {
    by_cid: Mutex<HashMap<String, HashMap<String, OrderSnapshot>>>,
}

impl LegTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// 写入/覆盖某 cid 下一条腿快照（同 OrderRef 多次回报以后到为准）。
    pub fn upsert(&self, cid: &str, snapshot: OrderSnapshot) {
        let mut all = self.by_cid.lock().unwrap();
        all.entry(cid.to_string())
            .or_default()
            .insert(snapshot.order_ref.clone(), snapshot);
    }

    /// 某 cid 下全部腿快照（顺序无意义，调用方走 [`Self::aggregate_cid`]）。
    pub fn legs_of(&self, cid: &str) -> Vec<OrderSnapshot> {
        let all = self.by_cid.lock().unwrap();
        all.get(cid)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    /// 某 cid 的聚合视图（无腿返回 None）。
    pub fn aggregate_cid(&self, cid: &str) -> Option<AggregatedOrder> {
        let legs = self.legs_of(cid);
        aggregate(&legs)
    }

    /// 某 cid 当前已收到回报的腿数（用于两腿齐终态判定：首腿终态可能先于次腿首报）。
    pub fn leg_count(&self, cid: &str) -> usize {
        self.by_cid.lock().unwrap().get(cid).map_or(0, |m| m.len())
    }

    /// 整组回收某 cid 的全部腿（cid 下所有腿齐终态时调用）；返回是否曾有该 cid。
    pub fn remove_cid(&self, cid: &str) -> bool {
        self.by_cid.lock().unwrap().remove(cid).is_some()
    }

    /// cid 是否仍有在管腿（诊断/测试）。
    pub fn contains(&self, cid: &str) -> bool {
        self.by_cid
            .lock()
            .unwrap()
            .get(cid)
            .is_some_and(|m| !m.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctp::core::types::Direction;

    fn leg(
        order_ref: &str,
        status: OrderStatus,
        total: i32,
        traded: i32,
        sys_id: &str,
    ) -> OrderSnapshot {
        OrderSnapshot {
            order_ref: order_ref.into(),
            instrument: "rb2510".into(),
            exchange_id: "SHFE".into(),
            order_sys_id: sys_id.into(),
            insert_time: "09:30:00".into(),
            direction: Direction::Sell,
            status,
            limit_price: 3300.0,
            volume_total: total,
            volume_traded: traded,
            status_msg: String::new(),
            front_id: 1,
            session_id: 1,
        }
    }

    // ---- 纯聚合 aggregate ----

    #[test]
    fn aggregate_single_leg_maps_its_status_and_volume() {
        let legs = vec![leg("1", OrderStatus::NoTradeQueueing, 3, 0, "")];
        let agg = aggregate(&legs).unwrap();
        assert_eq!(agg.filled, 0);
        assert_eq!(agg.total, 3);
        assert!(!agg.all_terminal);
        assert_eq!(agg.status, OrderStatus::NoTradeQueueing);
    }

    #[test]
    fn aggregate_two_legs_sums_filled_and_total() {
        // 两腿都部分成交：filled=两腿和（1+2=3），总量 5，仍在途 → Partial。
        let legs = vec![
            leg("1", OrderStatus::PartTradedQueueing, 2, 1, "s1"),
            leg("2", OrderStatus::PartTradedQueueing, 3, 2, "s2"),
        ];
        let agg = aggregate(&legs).unwrap();
        assert_eq!((agg.filled, agg.total), (3, 5));
        assert!(!agg.all_terminal);
        assert_eq!(agg.status, OrderStatus::PartTradedQueueing);
    }

    #[test]
    fn aggregate_not_terminal_until_every_leg_terminal() {
        // 一腿全成终态、另一腿仍排队：逻辑单不得提前 Filled。
        let legs = vec![
            leg("1", OrderStatus::AllTraded, 2, 2, "s1"),
            leg("2", OrderStatus::NoTradeQueueing, 3, 0, "s2"),
        ];
        let agg = aggregate(&legs).unwrap();
        assert!(!agg.all_terminal);
        assert_eq!(agg.filled, 2);
        assert_eq!(
            agg.status,
            OrderStatus::PartTradedQueueing,
            "有成交且未齐终态→Partial"
        );
    }

    #[test]
    fn aggregate_all_filled_when_all_legs_traded_fully() {
        let legs = vec![
            leg("1", OrderStatus::AllTraded, 2, 2, "s1"),
            leg("2", OrderStatus::AllTraded, 3, 3, "s2"),
        ];
        let agg = aggregate(&legs).unwrap();
        assert!(agg.all_terminal);
        assert_eq!((agg.filled, agg.total), (5, 5));
        assert_eq!(agg.status, OrderStatus::AllTraded);
    }

    #[test]
    fn aggregate_one_filled_one_cancelled_is_terminal_partial() {
        // 一腿全成（2）、一腿被拒/撤（0），两腿齐终态：逻辑单终态部分成交。
        let legs = vec![
            leg("1", OrderStatus::AllTraded, 2, 2, "s1"),
            leg("2", OrderStatus::Cancelled, 1, 0, "s2"),
        ];
        let agg = aggregate(&legs).unwrap();
        assert!(agg.all_terminal);
        assert_eq!((agg.filled, agg.total), (2, 3));
        assert_eq!(
            agg.status,
            OrderStatus::PartTradedNotQueueing,
            "终态部分成交"
        );
    }

    #[test]
    fn aggregate_both_cancelled_with_no_fill_is_cancelled() {
        let legs = vec![
            leg("1", OrderStatus::Cancelled, 2, 0, "s1"),
            leg("2", OrderStatus::Cancelled, 1, 0, "s2"),
        ];
        let agg = aggregate(&legs).unwrap();
        assert!(agg.all_terminal);
        assert_eq!(agg.status, OrderStatus::Cancelled);
    }

    #[test]
    fn aggregate_prefers_leg_with_sys_id_as_representative() {
        let legs = vec![
            leg("1", OrderStatus::NoTradeQueueing, 2, 0, ""),
            leg("2", OrderStatus::NoTradeQueueing, 3, 0, "sys2"),
        ];
        let agg = aggregate(&legs).unwrap();
        assert_eq!(agg.representative.order_ref, "2");
        assert_eq!(agg.representative.order_sys_id, "sys2");
    }

    #[test]
    fn aggregate_empty_returns_none() {
        assert!(aggregate(&[]).is_none());
    }

    // ---- LegTable 暂存/回收 ----

    #[test]
    fn table_tracks_latest_snapshot_per_leg_and_aggregates_by_cid() {
        let t = LegTable::new();
        let cid = "0xcid";
        // 首腿两次回报（排队→全成）：以最新快照覆盖。
        t.upsert(cid, leg("1", OrderStatus::NoTradeQueueing, 2, 0, "s1"));
        t.upsert(cid, leg("1", OrderStatus::AllTraded, 2, 2, "s1"));
        t.upsert(cid, leg("2", OrderStatus::NoTradeQueueing, 3, 0, "s2"));

        let legs = t.legs_of(cid);
        assert_eq!(legs.len(), 2, "同腿多次回报只保留最新一条");
        let agg = t.aggregate_cid(cid).unwrap();
        assert_eq!((agg.filled, agg.total), (2, 5));
        assert!(!agg.all_terminal);
    }

    #[test]
    fn table_remove_cid_reaps_whole_group() {
        let t = LegTable::new();
        let cid = "0xcid";
        t.upsert(cid, leg("1", OrderStatus::AllTraded, 2, 2, "s1"));
        t.upsert(cid, leg("2", OrderStatus::Cancelled, 3, 0, "s2"));
        assert!(t.contains(cid));

        // 两腿齐终态后整组回收（终态回收的真实路径是 remove_cid，非逐腿删）。
        assert!(t.remove_cid(cid));
        assert!(!t.contains(cid));
        assert!(t.aggregate_cid(cid).is_none());
        // 重复回收为无害 no-op。
        assert!(!t.remove_cid(cid));
    }

    #[test]
    fn table_isolates_cids() {
        let t = LegTable::new();
        t.upsert("a", leg("1", OrderStatus::AllTraded, 1, 1, "s1"));
        t.upsert("b", leg("2", OrderStatus::NoTradeQueueing, 2, 0, "s2"));
        assert_eq!(t.aggregate_cid("a").unwrap().filled, 1);
        assert_eq!(t.aggregate_cid("b").unwrap().filled, 0);
    }
}
