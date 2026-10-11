//! 进程级「纯内存、不落盘」共享表的统一持有者。
//!
//! 与 [`crate::TradingStore`] 里注册进 store-core、带 CSV 快照/恢复/日切的
//! Account/Order/Position/WatchRule 不同，这里的两张表：
//! * [`OrderRuleTable`]：交易所下单规则，整行不可变、刷新时 Arc 整体替换；
//! * [`QuoteTable`]：行情最新价，高频字段级就地更新。
//!
//! 二者都**不实现 Model、不注册、不持久化**，重启后由各 venue 订阅/规则同步重填。
//! 它们的所有权此前散落在服务 main.rs 各自 `new`，现收口到 store：进程一份、
//! 由 [`crate::TradingStore`] 经 `OnceLock` 惰性单例持有，所有任务共享同一句柄。
//!
//! 类型仍定义在 trading-core（纯内核、不落盘），store 只负责持有与初始化。

use std::sync::Arc;

use trading_core::order_rule::OrderRuleTable;
use trading_core::quote::QuoteTable;

use crate::leverage::LeverageRepo;

/// 进程一份的纯内存共享表集合（廉价克隆，内部全 Arc）。
#[derive(Clone)]
pub struct MemoryTables {
    /// 交易所下单规则表（各 venue 规则同步任务写入，下单校验读取）。
    pub order_rules: Arc<OrderRuleTable>,
    /// 公共行情表（各 venue 公共流写入，规则引擎/仓位重估读取）。
    pub quotes: Arc<QuoteTable>,
    /// 杠杆缓存（账户对账快照写入，下单前据此决定是否需要 set_leverage）。
    pub leverages: LeverageRepo,
}

impl MemoryTables {
    /// 构造一组全新的空内存表（仅由 store 的单例初始化调用）。
    pub fn new() -> Self {
        Self {
            order_rules: Arc::new(OrderRuleTable::new()),
            quotes: Arc::new(QuoteTable::new()),
            leverages: LeverageRepo::new(),
        }
    }
}

impl Default for MemoryTables {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_core::SystemClock;

    /// 用独立临时目录打开一个 TradingStore（空恢复、不开定时落盘）。
    fn open_store() -> crate::TradingStore {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "store-memory-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let (store, _handle) =
            crate::TradingStore::open(&dir, Arc::new(SystemClock), None).unwrap();
        store
    }

    #[test]
    fn new_creates_independent_empty_tables() {
        let a = MemoryTables::new();
        let b = MemoryTables::new();
        assert!(a.order_rules.is_empty());
        assert!(a.quotes.is_empty());
        // 两次构造得到不同实例。
        assert!(!Arc::ptr_eq(&a.order_rules, &b.order_rules));
        assert!(!Arc::ptr_eq(&a.quotes, &b.quotes));
    }

    #[test]
    fn store_returns_same_order_rule_table_every_call() {
        let store = open_store();
        let t1 = store.order_rules();
        let t2 = store.order_rules();
        assert!(Arc::ptr_eq(&t1, &t2), "下单规则表必须是进程内单例");
    }

    #[test]
    fn store_returns_same_quote_table_every_call() {
        let store = open_store();
        let q1 = store.quotes();
        let q2 = store.quotes();
        assert!(Arc::ptr_eq(&q1, &q2), "行情表必须是进程内单例");
    }

    #[test]
    fn store_memory_bundle_matches_individual_accessors() {
        // 集合句柄与两个独立访问器必须指向同一实例（单一事实来源）。
        let store = open_store();
        let mem = store.memory();
        assert!(Arc::ptr_eq(&mem.order_rules, &store.order_rules()));
        assert!(Arc::ptr_eq(&mem.quotes, &store.quotes()));
    }

    #[test]
    fn repos_are_process_singletons() {
        // OrderRepo 的 cid 二级索引可变：每次访问都必须拿到同一仓储实例，
        // 杜绝「A 实例写入、B 实例索引查不到」的分叉。
        let store = open_store();
        let a = store.order_repo().unwrap();
        let b = store.order_repo().unwrap();
        // 仓储内部 Arc 句柄克隆即同表；用插入后跨实例可见证明索引共享。
        use trading_models::{Order, Side};
        let cid = "0x00000000000000000000000000000001".to_string();
        a.insert(Order::new(1, cid.clone(), "xili", "btcusdt", Side::Buy))
            .unwrap();
        assert!(b.get_by_cid(&cid).is_some(), "跨仓储句柄必须共享 cid 索引");

        // position/rule 仓储同样单例（至少不报错且可重复取）。
        assert!(store.position_repo().is_ok());
        assert!(store.position_repo().is_ok());
        assert!(store.rule_repo().is_ok());
        assert!(store.rule_repo().is_ok());
    }
}
