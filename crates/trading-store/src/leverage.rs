//! 杠杆内存仓储与「是否需要设置杠杆」纯决策。
//!
//! 真值来源：账户对账快照（各 venue 的 `PositionInfo.leverage`，如币安
//! positionRisk 已带该字段），由 `PositionEngine` 在对账时写入；**不**为取杠杆
//! 额外发起 REST 查询（查/设杠杆在币安权重同为 5，独立查询不省权重、最差翻倍）。
//!
//! * [`LeverageRepo`]：进程一份的纯内存 KV（不落盘），键
//!   `(account_id, exchange, instrument) -> leverage`，廉价克隆、内部 Arc/DashMap。
//! * [`should_set_leverage`]：建单前的纯决策——平仓单永不设置；开仓单仅当缓存
//!   缺失或与目标不一致时才下发，缓存命中且相等则整次 `/leverage` 跳过（省权重）。
//!
//! 所有权收口到 TradingStore（与其它共享内存一致），服务层不另建全局变量。

use std::sync::Arc;

use dashmap::DashMap;

/// 杠杆缓存键：账户 × 交易所 × 标的。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LeverageKey {
    pub account_id: u64,
    pub exchange: String,
    pub instrument: String,
}

impl LeverageKey {
    pub fn new(
        account_id: u64,
        exchange: impl Into<String>,
        instrument: impl Into<String>,
    ) -> Self {
        Self {
            account_id,
            exchange: exchange.into(),
            instrument: instrument.into(),
        }
    }
}

/// 杠杆内存仓储句柄（廉价克隆，内部 Arc/DashMap，进程内单例）。
#[derive(Clone, Default)]
pub struct LeverageRepo {
    table: Arc<DashMap<LeverageKey, u32>>,
}

impl LeverageRepo {
    pub fn new() -> Self {
        Self::default()
    }

    /// 写入（覆盖）某账户/所/标的的杠杆。
    pub fn put(&self, key: &LeverageKey, leverage: u32) {
        self.table.insert(key.clone(), leverage);
    }

    /// 读取缓存杠杆；未缓存返回 None（首单/缓存未就绪）。
    pub fn get(&self, key: &LeverageKey) -> Option<u32> {
        self.table.get(key).map(|v| *v.value())
    }

    /// 按 `(账户, 交易所, 标的)` 直接读取（免去调用方手工构造 [`LeverageKey`]）。
    pub fn get_for(&self, account_id: u64, exchange: &str, instrument: &str) -> Option<u32> {
        self.get(&LeverageKey::new(account_id, exchange, instrument))
    }
}

/// 是否需要下发 `set_leverage`（**纯决策、零 IO**）：
///
/// * 平仓单（`is_reduce_only`）：**永不设置**——杠杆只影响开仓保证金，对 reduceOnly
///   成交无意义（移除原先平仓前的无效 `/leverage` 步骤）；
/// * 开仓单：缓存命中且等于目标 → 跳过（`false`）；缓存缺失或与目标不一致 → 设置（`true`）。
///
/// 支持目标为 1（从高倍调回 1）：缓存非 1 即需设置。
pub fn should_set_leverage(is_reduce_only: bool, cached: Option<u32>, target: u32) -> bool {
    // 平仓单一律不设置。
    if is_reduce_only {
        return false;
    }
    // 开仓单：仅当缓存不等于目标（含缺失）时才设置。
    cached != Some(target)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> LeverageKey {
        LeverageKey::new(7, "binance", "BTCUSDT")
    }

    // ---- 仓储 put/get ----

    #[test]
    fn put_then_get_returns_value() {
        let repo = LeverageRepo::new();
        assert_eq!(repo.get(&key()), None);
        repo.put(&key(), 3);
        assert_eq!(repo.get(&key()), Some(3));
    }

    #[test]
    fn put_overwrites_previous() {
        let repo = LeverageRepo::new();
        repo.put(&key(), 3);
        repo.put(&key(), 5);
        assert_eq!(repo.get(&key()), Some(5));
    }

    #[test]
    fn keys_are_scoped() {
        // 不同账户/所/标的互不影响。
        let repo = LeverageRepo::new();
        repo.put(&key(), 3);
        assert_eq!(repo.get(&LeverageKey::new(8, "binance", "BTCUSDT")), None);
        assert_eq!(repo.get(&LeverageKey::new(7, "xili", "BTCUSDT")), None);
        assert_eq!(repo.get(&LeverageKey::new(7, "binance", "ETHUSDT")), None);
    }

    // ---- should_set_leverage 纯决策 ----

    #[test]
    fn close_order_never_sets_leverage() {
        // 平仓单：无论缓存缺失/相等/不等，都不设置。
        assert!(!should_set_leverage(true, None, 3));
        assert!(!should_set_leverage(true, Some(3), 3));
        assert!(!should_set_leverage(true, Some(5), 3));
    }

    #[test]
    fn open_order_skips_when_cache_matches() {
        // 开仓：缓存 == 目标 → 跳过（省权重）。
        assert!(!should_set_leverage(false, Some(3), 3));
    }

    #[test]
    fn open_order_sets_when_cache_missing_or_differs() {
        // 缓存缺失（首单）→ 设置。
        assert!(should_set_leverage(false, None, 3));
        // 缓存不等（含调回 1）→ 设置。
        assert!(should_set_leverage(false, Some(5), 3));
        assert!(should_set_leverage(false, Some(10), 1));
    }
}
