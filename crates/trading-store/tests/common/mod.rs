#![allow(dead_code)]

use std::sync::Arc;

use rust_decimal::Decimal;
use store_core::{day_key, MockClock};
use tempfile::TempDir;
use trading_models::{
    Account, Order, OrderType, Position, Side, WatchConditionKind, WatchRule, WatchRuleAction,
};
use trading_store::TradingStore;

/// UTC 2023-11-14 01:00:00 附近固定时间点，便于断言日切行为。
pub const T0: u64 = 1_700_000_000_000 / 86_400_000 * 86_400_000 + 3_600_000;
pub const DAY_MS: u64 = 86_400_000;

pub fn cid(n: u64) -> String {
    format!("0x{n:032x}")
}

pub struct Harness {
    pub tmp: TempDir,
    pub ts: TradingStore,
    pub clock: Arc<MockClock>,
    pub last_day: Option<String>,
}

impl Harness {
    /// 用 MockClock@T0 打开空目录，不启动后台线程，手动 tick。
    pub fn open() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let clock = Arc::new(MockClock::new(T0));
        let (ts, _) = TradingStore::open(
            tmp.path(),
            clock.clone() as Arc<dyn store_core::Clock>,
            None,
        )
        .unwrap();
        Self {
            tmp,
            ts,
            clock,
            last_day: None,
        }
    }

    /// 用当前 clock 在同一目录重开，模拟进程重启。
    pub fn reopen(self) -> Self {
        let path = self.tmp;
        let clock = self.clock;
        let (ts, _) = TradingStore::open(
            path.path(),
            clock.clone() as Arc<dyn store_core::Clock>,
            None,
        )
        .unwrap();
        Self {
            tmp: path,
            ts,
            clock,
            last_day: None,
        }
    }

    pub fn tick(&mut self) {
        store_core::run_tick(self.ts.core(), self.ts.root(), &mut self.last_day).unwrap();
    }

    pub fn day_of_t0() -> String {
        day_key(T0)
    }

    pub fn make_account() -> Account {
        Account::new("alice", "binance")
    }

    pub fn make_order(account_id: u64, n: u64) -> Order {
        let mut o = Order::new(account_id, cid(n), "binance", "BTCUSDT", Side::Buy);
        o.order_quantity = Decimal::new(1, 0);
        o
    }

    pub fn make_position(account_id: u64) -> Position {
        Position::new(account_id, "binance", "BTCUSDT", Side::Buy)
    }

    /// ROI ≥ 12% 全市价平仓的风控规则（挂在仓位 100 上）。
    pub fn make_rule(account_id: u64) -> WatchRule {
        WatchRule::new_risk(
            account_id,
            100,
            WatchConditionKind::Roi,
            None,
            ">=",
            Decimal::new(12, 2),
            WatchRuleAction::UseSelf,
            OrderType::Market,
            Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
            Decimal::ZERO, // 比例平仓：绝对数量为 0
            Decimal::ONE,  // quantity_pct=1 全平
        )
    }
}

// ---- 以下 helper 随原 store-tokio 测试一并迁入（async_rt 模块测试用）----

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use store_core::model::bool_u8;
use store_core::{Model, Store, SystemClock};

/// 测试用全局 model。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub id: u64,
    pub name: String,
    #[serde(with = "bool_u8")]
    pub deleted: bool,
    pub created_at: u64,
    pub updated_at: u64,
}

impl Item {
    pub fn new(name: &str) -> Self {
        Self {
            id: 0,
            name: name.into(),
            deleted: false,
            created_at: 0,
            updated_at: 0,
        }
    }
}

impl Model for Item {
    const MODEL_NAME: &'static str = "item";

    fn id(&self) -> u64 {
        self.id
    }
    fn set_id(&mut self, id: u64) {
        self.id = id;
    }
    fn deleted(&self) -> bool {
        self.deleted
    }
    fn set_deleted(&mut self, deleted: bool) {
        self.deleted = deleted;
    }
    fn created_at(&self) -> u64 {
        self.created_at
    }
    fn updated_at(&self) -> u64 {
        self.updated_at
    }
    fn set_timestamps(&mut self, created_at: u64, updated_at: u64) {
        self.created_at = created_at;
        self.updated_at = updated_at;
    }
}

pub fn today_csv(root: &Path) -> PathBuf {
    root.join(day_key(SystemClock::now_ms()))
        .join("_system")
        .join("item.csv")
}

pub fn any_csv(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk(root, &mut out);
    out.into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "csv"))
        .collect()
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else {
                out.push(p);
            }
        }
    }
}

pub struct Ctx {
    pub tmp: TempDir,
    pub store: Arc<Store>,
    pub rt: trading_store::PersistRuntime,
}

pub async fn setup() -> Ctx {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(SystemClock));
    store.register::<Item>().unwrap();
    let rt = trading_store::PersistRuntime::new(store.clone(), tmp.path());
    rt.recover().await.unwrap();
    Ctx { tmp, store, rt }
}
