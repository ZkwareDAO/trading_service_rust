//! CTP OrderRef 分配与回报反查（cid ↔ OrderRef 映射）。
//!
//! 背景：`OrderRef` 是客户端自填、柜台原样回传的订单关联字段，上限 13 字节；
//! 本系统 cid 为 `0x`+32hex（34 字节）超限——对长 cid 分配数字短号（epoch
//! 毫秒播种，跨重启单调、不与历史小号空间重叠），回报时反查还原长 cid。
//! ≤13 字节的 cid 直接透传（未来上游若统一短格式，本表自动退化为零开销）。
//!
//! 生命周期：会话态（进程内存）；订单行持久化 `exchange_order_id`（受理时
//! 为 OrderRef，首回报后升级为 OrderSysID）+ 宿主回查 fallback 承担重启恢复，
//! 本表丢得起、可重建。清理有两个钩子：下单同步拒（`api.rs` 受理码非 0）
//! 清脏表；委托到达终态（全部成交/撤销）经 `venue.rs` 终态钩子回收表项，
//! 防长会话无限增长——终态后迟到的回报经宿主 fallback 兜底，无需映射。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// cid ↔ OrderRef 映射表（会话态，进程内存）。
///
/// 内部仅 AtomicU64 + Mutex<HashMap>，自动满足 Send + Sync，可安全跨
/// `Arc` 在 tokio 短会话与专用 OS 线程间共享。
///
/// 撤单（将来 `ReqOrderAction`）可经映射反查 OrderRef；重启后不重建——
/// 历史订单的回报经宿主 `exchange_order_id` 回查 fallback 收敛（见
/// trading-service event 匹配），新订单从 epoch 毫秒量级播种分配。
#[derive(Default)]
pub struct OrderRefRegistry {
    /// 下一个可分配的数字短号（epoch 毫秒播种，进程内单调递增）。
    next: AtomicU64,
    /// OrderRef → 本地长 cid。
    map: Mutex<HashMap<String, String>>,
}

impl OrderRefRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 为长 cid 分配 OrderRef：≤12 字节直接透传（不占表）；长 cid 分配
    /// `max(epoch 毫秒/10, 上次值+1)` 的 **12 位**数字短号——OrderRef 定长
    /// 13 字节须留 1 字节 `\0` 终止符（实测填满 13 位会被柜台截断为 12 位，
    /// 回报反查错配）；重启后计数器不落回历史小号空间，反查天然无碰撞。
    pub fn allocate(&self, cid: &str) -> String {
        if cid.len() <= 13 {
            return cid.to_owned();
        }
        loop {
            let last = self.next.load(Ordering::SeqCst);
            let next = (last + 1).max(epoch_millis_div10());
            if self
                .next
                .compare_exchange(last, next, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                let order_ref = next.to_string();
                self.map
                    .lock()
                    .unwrap()
                    .insert(order_ref.clone(), cid.to_owned());
                return order_ref;
            }
        }
    }

    /// 回报反查：OrderRef → 本地长 cid（未命中返回 None，调用方保留原值）。
    pub fn cid_of(&self, order_ref: &str) -> Option<String> {
        self.map.lock().unwrap().get(order_ref).cloned()
    }

    /// 撤单路由反查：本地长 cid → 本会话分配的 OrderRef。
    ///
    /// 仅长 cid（>13B，分配过数字短号）会入表，故只有它们能命中；≤13B 的
    /// 透传 cid 不占表（其 OrderRef 即 cid 自身），此处返回 None，调用方按
    /// 「非本会话映射」走系统号兜底路径。重启后表为空，恒返回 None。
    pub fn order_ref_of_cid(&self, cid: &str) -> Option<String> {
        // 表为 OrderRef→cid，反查即遍历等值匹配（会话内表项规模=在途单数，量级很小）。
        self.map
            .lock()
            .unwrap()
            .iter()
            .find_map(|(order_ref, mapped_cid)| (mapped_cid == cid).then(|| order_ref.clone()))
    }

    /// 撤单路由反查（全部）：本地长 cid → 本会话为其登记的**全部** OrderRef。
    ///
    /// 跨今昨一单两腿时一个 cid 对应两个 OrderRef（首腿投递前分配、第二腿会话
    /// 线程内再 [`Self::allocate`]），撤单需对两腿分别寻址；单腿单只返回一个。
    /// 表项规模 = 在途物理腿数，遍历成本可忽略。
    pub fn order_refs_of_cid(&self, cid: &str) -> Vec<String> {
        self.map
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, mapped_cid)| *mapped_cid == cid)
            .map(|(order_ref, _)| order_ref.clone())
            .collect()
    }

    /// 移除映射（柜台受理失败时清脏表：未受理的 OrderRef 不会有回报）。
    pub fn remove(&self, order_ref: &str) {
        self.map.lock().unwrap().remove(order_ref);
    }
}

/// 12 位播种基数：epoch 毫秒去掉末位（10ms 精度）。
fn epoch_millis_div10() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64 / 10)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn long_cid(tag: &str) -> String {
        format!("0x{}", tag.repeat(32))
    }

    #[test]
    fn allocate_is_monotonic_and_unique() {
        // Arrange / Act：连续分配两笔。
        let r = OrderRefRegistry::new();
        let a = r.allocate(&long_cid("A"));
        let b = r.allocate(&long_cid("B"));

        // Assert：互不相同、可反查回各自长 cid。
        assert_ne!(a, b);
        assert_eq!(r.cid_of(&a).as_deref(), Some(long_cid("A").as_str()));
        assert_eq!(r.cid_of(&b).as_deref(), Some(long_cid("B").as_str()));
    }

    #[test]
    fn allocate_seeds_from_epoch_millis_across_restart() {
        // Arrange：新进程 = 新 registry（计数器归零），模拟重启。
        let r = OrderRefRegistry::new();

        // Act：重启后第一笔分配。
        let a = r.allocate(&long_cid("A"));

        // Assert：短号 ≥ 当前 epoch 毫秒（13 位量级）——
        // 不会与历史运行的小号空间（1..1000）重叠，回报反查天然无碰撞。
        let floor = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            - 1_000)
            / 10;
        let n: u64 = a.parse().expect("短号应为纯数字（OrderRef 数字口径）");
        assert!(n >= floor, "分配号 {n} 应从 epoch 毫秒/10 量级播种");
        assert!(a.len() <= 12, "OrderRef 须留 1 字节 \0 终止符，实际 {a}");
    }

    #[test]
    fn short_cid_passes_through_without_mapping() {
        // Arrange / Act：≤13 字节的 cid 直接透传。
        let r = OrderRefRegistry::new();
        let a = r.allocate("12345678");

        // Assert：原样返回且不占表（cid_of 本身也是它自己，翻译自然成立）。
        assert_eq!(a, "12345678");
        assert!(r.cid_of("12345678").is_none());
    }

    #[test]
    fn order_ref_of_cid_reverses_allocation_for_long_cid() {
        // Arrange：为长 cid 分配短号。
        let r = OrderRefRegistry::new();
        let cid = long_cid("C");
        let order_ref = r.allocate(&cid);

        // Act / Assert：cid 可反查回本会话分配的 OrderRef。
        assert_eq!(
            r.order_ref_of_cid(&cid).as_deref(),
            Some(order_ref.as_str())
        );
    }

    #[test]
    fn order_ref_of_cid_misses_for_unknown_or_passthrough_cid() {
        // Arrange：未知长 cid；以及 ≤13B 透传 cid（不占表）。
        let r = OrderRefRegistry::new();
        let _ = r.allocate(&long_cid("A"));

        // Act / Assert：重启后表空 / 非本会话 / 透传 cid 均未命中（走系统号兜底）。
        assert!(r.order_ref_of_cid(&long_cid("Z")).is_none());
        assert!(r.order_ref_of_cid("12345678").is_none());
    }

    #[test]
    fn order_refs_of_cid_returns_all_refs_for_two_leg_order() {
        // Arrange：跨今昨一单两腿——同一 cid 分配两个 OrderRef。
        let r = OrderRefRegistry::new();
        let cid = long_cid("L");
        let first = r.allocate(&cid);
        let second = r.allocate(&cid);
        assert_ne!(first, second, "两腿必须是不同 OrderRef");

        // Act
        let mut all = r.order_refs_of_cid(&cid);
        all.sort();

        // Assert：两个 ref 都反查到同一 cid，且都能正向枚举（撤单两腿寻址用）。
        let mut expected = vec![first.clone(), second.clone()];
        expected.sort();
        assert_eq!(all, expected);
        assert_eq!(r.cid_of(&first).as_deref(), Some(cid.as_str()));
        assert_eq!(r.cid_of(&second).as_deref(), Some(cid.as_str()));
    }

    #[test]
    fn order_refs_of_cid_single_and_unknown() {
        // 单腿单只返回一个 ref；未知 cid 为空。
        let r = OrderRefRegistry::new();
        let cid = long_cid("S");
        let only = r.allocate(&cid);
        assert_eq!(r.order_refs_of_cid(&cid), vec![only]);
        assert!(r.order_refs_of_cid(&long_cid("Z")).is_empty());
    }

    #[test]
    fn remove_one_leg_ref_keeps_the_other() {
        // 终态回收是逐腿的：撤掉一腿映射不影响另一腿反查。
        let r = OrderRefRegistry::new();
        let cid = long_cid("R");
        let first = r.allocate(&cid);
        let second = r.allocate(&cid);
        r.remove(&first);
        assert_eq!(r.cid_of(&first), None);
        assert_eq!(r.cid_of(&second).as_deref(), Some(cid.as_str()));
        assert_eq!(r.order_refs_of_cid(&cid), vec![second]);
    }

    #[test]
    fn remove_drops_translation() {
        // Arrange：受理失败的脏映射应可清除。
        let r = OrderRefRegistry::new();
        let a = r.allocate(&long_cid("A"));
        assert!(r.cid_of(&a).is_some());

        // Act
        r.remove(&a);

        // Assert：反查未命中（回报不会带着未受理的 OrderRef 回来）。
        assert!(r.cid_of(&a).is_none());
    }
}
