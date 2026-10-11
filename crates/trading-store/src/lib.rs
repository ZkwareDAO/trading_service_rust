//! # trading-store
//!
//! 业务装配门面：注册 Account / Order / Position / WatchRule 四张表，启动时从
//! CSV 快照恢复，并可选启动后台定时持久化线程。
//!
//! ```ignore
//! use std::sync::Arc;
//! use std::time::Duration;
//! use store_core::SystemClock;
//! use trading_store::TradingStore;
//!
//! let (store, persist) = TradingStore::open(
//!     "./data",
//!     Arc::new(SystemClock),
//!     Some(Duration::from_secs(60)),
//! )?;
//! let accounts = store.accounts()?;
//! // ... CRUD ...
//! if let Some(handle) = persist { handle.shutdown(); }
//! # Ok::<(), store_core::StoreError>(())
//! ```

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use store_core::{Model, PersistHandle, PersistOptions, Store, StoreError, Table};
use trading_core::order_rule::OrderRuleTable;
use trading_core::quote::QuoteTable;
use trading_models::{Account, Order, Position, WatchRule};

pub mod accounts;
pub mod async_rt;
pub mod history;
pub mod init;
pub mod leverage;
pub mod memory;
pub mod orders;
pub mod positions;
pub mod rules;

pub use accounts::{AccountBrief, AccountManager, ResolvedAccount};
pub use async_rt::{PersistRuntime, PersistTaskHandle};
pub use history::HistoryReader;
pub use leverage::{should_set_leverage, LeverageKey, LeverageRepo};
pub use memory::MemoryTables;
pub use orders::{OrderEventInput, OrderRepo, OrderRepoError};
pub use positions::PositionRepo;
pub use rules::{RuleRepo, RuleRepoError};

/// 业务装配根：持有注册中心、数据根目录与进程级纯内存共享表。
pub struct TradingStore {
    core: Arc<Store>,
    root: PathBuf,
    /// 进程一份、纯内存不落盘的下单规则表/行情表。惰性单例：首次取用才构造，
    /// 之后所有调用返回同一 Arc（见 [`Self::memory`]）。
    memory: OnceLock<MemoryTables>,
    /// 三个业务仓储同样惰性单例化。OrderRepo 持有可变的 cid 二级索引，
    /// 必须全进程共用同一实例，否则各自重建索引会出现「A 写入、B 查不到」。
    order_repo: OnceLock<OrderRepo>,
    position_repo: OnceLock<PositionRepo>,
    rule_repo: OnceLock<RuleRepo>,
}

impl TradingStore {
    /// 打开（或创建）业务存储：
    /// 1. 注册 account/order/position/watch_rule 四张表；
    /// 2. 从 `root` 下历史 CSV 恢复（含停机跨日 purge）；
    /// 3. `interval = Some(..)` 时启动后台定时持久化线程。
    pub fn open(
        root: impl Into<PathBuf>,
        clock: Arc<dyn store_core::Clock>,
        interval: Option<Duration>,
    ) -> Result<(Self, Option<PersistHandle>), StoreError> {
        let root = root.into();
        let core = Store::new(clock);
        core.register_account::<Account>()?;
        core.register::<Order>()?;
        core.register::<Position>()?;
        core.register::<WatchRule>()?;
        core.recover(&root)?;

        let handle = interval
            .map(|d| store_core::spawn(core.clone(), root.clone(), PersistOptions { interval: d }));

        Ok((
            Self {
                core,
                root,
                memory: OnceLock::new(),
                order_repo: OnceLock::new(),
                position_repo: OnceLock::new(),
                rule_repo: OnceLock::new(),
            },
            handle,
        ))
    }

    /// 异步装配（推荐用于服务进程）：
    /// - 注册 account/order/position/watch_rule 四张表；
    /// - `recover` 与落盘全部走 `spawn_blocking`，不阻塞 tokio worker；
    /// - `interval = Some(..)` 时启动 tokio 定时持久化任务，退出前
    ///   对返回句柄调用 `shutdown().await`。
    ///
    /// 业务侧日常 CRUD 仍是同步的 [`Table`] 方法，在 async 代码里直接调用即可。
    pub async fn open_async(
        root: impl Into<PathBuf>,
        clock: Arc<dyn store_core::Clock>,
        interval: Option<Duration>,
    ) -> Result<(Self, Option<PersistTaskHandle>), StoreError> {
        let root = root.into();
        let core = Store::new(clock);
        core.register_account::<Account>()?;
        core.register::<Order>()?;
        core.register::<Position>()?;
        core.register::<WatchRule>()?;

        let runtime = async_rt::PersistRuntime::new(core.clone(), root.clone());
        runtime.recover().await?;
        let handle = interval.map(|d| runtime.spawn(d));

        Ok((
            Self {
                core,
                root,
                memory: OnceLock::new(),
                order_repo: OnceLock::new(),
                position_repo: OnceLock::new(),
                rule_repo: OnceLock::new(),
            },
            handle,
        ))
    }

    /// 手动异步落盘（只写 dirty 表）。
    pub async fn flush_async(&self) -> Result<store_core::FlushReport, StoreError> {
        let runtime = async_rt::PersistRuntime::new(self.core.clone(), self.root.clone());
        runtime.flush().await
    }

    pub fn accounts(&self) -> Result<Arc<Table<Account>>, StoreError> {
        self.core.table::<Account>()
    }

    pub fn orders(&self) -> Result<Arc<Table<Order>>, StoreError> {
        self.core.table::<Order>()
    }

    /// 订单仓储：在订单表上带 client_order_id 索引与状态机。进程内单例缓存：
    /// 其 cid 二级索引可变，必须所有写读共用同一实例。
    /// 务必在 `open`/`open_async` 恢复完成后调用（构造时按当前内存重建索引）。
    pub fn order_repo(&self) -> Result<OrderRepo, StoreError> {
        cached_repo(&self.order_repo, || Ok(OrderRepo::new(self.orders()?)))
    }

    pub fn positions(&self) -> Result<Arc<Table<Position>>, StoreError> {
        self.core.table::<Position>()
    }

    /// 仓位仓储：作用域读取、对账裁决落库、行情重估的统一入口。进程内单例缓存。
    /// 务必在 `open`/`open_async` 恢复完成后调用。
    pub fn position_repo(&self) -> Result<PositionRepo, StoreError> {
        cached_repo(&self.position_repo, || {
            Ok(PositionRepo::new(self.positions()?))
        })
    }

    /// 策略/风控规则表（账户作用域，`watch_rule.csv`）。
    pub fn watch_rules(&self) -> Result<Arc<Table<WatchRule>>, StoreError> {
        self.core.table::<WatchRule>()
    }

    /// 规则仓储：在规则表上承载 active/in_use/pending/retired 状态机。进程内单例缓存。
    /// 务必在 `open`/`open_async` 恢复完成后调用。
    pub fn rule_repo(&self) -> Result<RuleRepo, StoreError> {
        cached_repo(&self.rule_repo, || Ok(RuleRepo::new(self.watch_rules()?)))
    }

    /// 进程级纯内存共享表集合（下单规则表 + 行情表）。
    /// 首次取用惰性构造，之后恒为同一实例（廉价克隆，内部 Arc）。
    pub fn memory(&self) -> MemoryTables {
        self.memory.get_or_init(MemoryTables::new).clone()
    }

    /// 进程一份的交易所下单规则表（纯内存不落盘；各 venue 规则同步任务写入）。
    pub fn order_rules(&self) -> Arc<OrderRuleTable> {
        self.memory
            .get_or_init(MemoryTables::new)
            .order_rules
            .clone()
    }

    /// 进程一份的公共行情表（纯内存不落盘；各 venue 公共流写入）。
    pub fn quotes(&self) -> Arc<QuoteTable> {
        self.memory.get_or_init(MemoryTables::new).quotes.clone()
    }

    /// 进程一份的杠杆内存仓储（纯内存不落盘）：对账快照写入，下单前据此按需设置。
    pub fn leverage_repo(&self) -> LeverageRepo {
        self.memory.get_or_init(MemoryTables::new).leverages.clone()
    }

    /// 底层注册中心（用于手动 flush / run_tick / purge_all）。
    pub fn core(&self) -> &Arc<Store> {
        &self.core
    }

    pub fn root(&self) -> &PathBuf {
        &self.root
    }

    /// 历史归档回查器（廉价构造、无缓存；按需倒序扫历史 CSV）。
    pub fn history(&self) -> HistoryReader {
        HistoryReader::new(self.root.clone())
    }

    /// 立即做一次快照落盘（同步；只写 dirty 表）。
    pub fn flush(&self) -> Result<store_core::FlushReport, StoreError> {
        self.core.flush(&self.root, false)
    }
}

/// 便于上层按泛型直接取表。
pub fn table_of<M: Model>(store: &TradingStore) -> Result<Arc<Table<M>>, StoreError> {
    store.core().table::<M>()
}

/// 进程内单例仓储的惰性获取：已缓存直接克隆；否则用 `build` 构造一次存入。
///
/// 并发 miss 时可能各构造一份（仓储构造仅按当前内存重建只读索引、无副作用），
/// `OnceLock::get_or_init` 保证最终只持久化其中一份，所有调用拿到同一实例。
fn cached_repo<R: Clone>(
    slot: &OnceLock<R>,
    build: impl FnOnce() -> Result<R, StoreError>,
) -> Result<R, StoreError> {
    if let Some(r) = slot.get() {
        return Ok(r.clone());
    }
    // 先在外层 `?` 传播构造错误（如表未注册），再 get_or_init 保证只持久化一份。
    let repo = build()?;
    Ok(slot.get_or_init(|| repo).clone())
}
