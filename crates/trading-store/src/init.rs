//! 进程启动的统一引导（内存相关初始化收口处）。
//!
//! 此前服务 main.rs 要手工分步：开店 + 注册四表 + CSV 恢复 + 起定时落盘 +
//! 账户表首载。本模块把这些**共享内存初始化**收成一次 [`Initialized::bootstrap`]：
//!
//! 1. [`TradingStore::open_async`]：注册 Account/Order/Position/WatchRule、
//!    `spawn_blocking` 恢复最新数据日快照、可选起定时落盘；
//! 2. 在**同一张**账户表上构造 [`AccountManager`] 并首载人工 `account.csv`
//!    （文件不存在为空表、不报错）；
//! 3. 返回 `Arc<TradingStore>`（供上层各任务直接 `store.xxx()` 取句柄）、
//!    落盘句柄与账户管理器。
//!
//! 进程级纯内存表（下单规则/行情）不在此主动构造——它们在首次 `store.order_rules()`
//! / `store.quotes()` 时惰性单例创建，避免无用空表。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use store_core::Clock;

use crate::accounts::AccountManager;
use crate::{PersistTaskHandle, TradingStore};

/// 引导入参（均为进程级配置，由上层命令行解析后传入）。
pub struct InitParams<'a> {
    /// 数据根目录（CSV 快照、account.csv、密钥默认所在）。
    pub data_dir: &'a str,
    /// 时钟（生产 SystemClock，测试 MockClock）。
    pub clock: Arc<dyn Clock>,
    /// 定时落盘间隔；None 仅手动 flush。
    pub flush_interval: Option<Duration>,
    /// 人工账户文件路径（account.csv）。
    pub account_file: PathBuf,
    /// 账户主密钥文件（0600，首启自动生成）。
    pub account_key_file: PathBuf,
}

/// 引导结果：开店后的共享存储、可选落盘句柄、已首载的账户管理器。
pub struct Initialized {
    /// 业务存储根（进程内共享一份；所有 repo/内存表经其单例访问器获取）。
    pub store: Arc<TradingStore>,
    /// 定时落盘句柄；退出前需 `shutdown().await`。None 表示未开定时落盘。
    pub persist: Option<PersistTaskHandle>,
    /// 账户管理器（与 store 共用同一张账户表）。
    pub accounts: AccountManager,
}

impl Initialized {
    /// 一次性完成开店/恢复/定时落盘/账户首载。
    pub async fn bootstrap(params: InitParams<'_>) -> anyhow::Result<Self> {
        let InitParams {
            data_dir,
            clock,
            flush_interval,
            account_file,
            account_key_file,
        } = params;

        let (store, persist) = TradingStore::open_async(data_dir, clock, flush_interval).await?;
        let store = Arc::new(store);

        // 账户表与四表同库：管理器绑定 store 注册的同一张 Table<Account>。
        let account_table = store.accounts()?;
        let accounts =
            AccountManager::bootstrap(account_table, data_dir, account_file, account_key_file)?;

        Ok(Self {
            store,
            persist,
            accounts,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_core::SystemClock;

    const HEADER: &str =
        "id,name,exchange,api_key,api_secret,api_passphrase,position_mode,is_testnet,deleted,created_at,updated_at\n";

    fn tmp_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "store-init-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[tokio::test]
    async fn bootstrap_opens_store_loads_accounts_and_shares_one_table() {
        let dir = tmp_dir();
        let csv = dir.join("account.csv");
        let key = dir.join("account.key");
        // 两个账户：binance + okx（去重连接逻辑在账户管理器单测覆盖）。
        std::fs::write(
            &csv,
            format!("{HEADER}0,a1,binance,K,s,,,0,0,0,0,0\n0,a2,okx,K,s,,,0,0,0,0,0\n"),
        )
        .unwrap();

        let init = Initialized::bootstrap(InitParams {
            data_dir: dir.to_str().unwrap(),
            clock: Arc::new(SystemClock),
            flush_interval: None,
            account_file: csv,
            account_key_file: key,
        })
        .await
        .unwrap();

        // 账户经管理器可见、可解密。
        assert!(init.accounts.resolve("a1", Some("binance")).is_some());
        assert_eq!(
            init.accounts.distinct_exchanges(),
            vec!["binance".to_string(), "okx".to_string()]
        );
        // 账户表与 store 注册的是同一张：store 直读行数与管理器一致。
        assert_eq!(init.store.accounts().unwrap().list().len(), 2);
        // repo/内存表访问器可用且为单例（不 panic、同一实例）。
        assert!(init.store.order_repo().is_ok());
        assert!(std::sync::Arc::ptr_eq(
            &init.store.quotes(),
            &init.store.quotes()
        ));
        // 未开定时落盘。
        assert!(init.persist.is_none());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn bootstrap_without_account_file_is_not_fatal() {
        let dir = tmp_dir();
        let init = Initialized::bootstrap(InitParams {
            data_dir: dir.to_str().unwrap(),
            clock: Arc::new(SystemClock),
            flush_interval: None,
            account_file: dir.join("account.csv"),
            account_key_file: dir.join("account.key"),
        })
        .await
        .unwrap();
        assert!(init.accounts.distinct_exchanges().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }
}
