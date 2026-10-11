//! # async_rt
//!
//! [`store_core`] 的 tokio 异步适配模块（原独立 crate `store-tokio`，唯一消费者
//! 即本包，2026-09 并入 trading-store）。**内存 CRUD 仍然是同步的**——
//! `Table` 上的 std 锁临界区只有纳秒~微秒级纯内存操作，业务模块在任意
//! 线程（含 tokio worker）直接调用即可，不要也不需要 `.await`。
//!
//! 异步只发生在三件慢事上：
//! 1. 启动恢复（扫目录、读 CSV）；
//! 2. 落盘（快照序列化、写文件、fsync、rename）；
//! 3. 定时调度。
//!
//! 所有阻塞工作经 [`tokio::task::spawn_blocking`] 移出 async 线程，
//! 不会卡住 runtime。并发 flush 的安全性由 store-core 保证：临时文件名带
//! 进程内全局唯一序号、rename 原子替换，两个一致快照只有先后覆盖关系。
//! 全程不使用 channel：停止信号为 `AtomicBool`，与项目约束一致。

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use store_core::{run_tick, FlushReport, RecoveryReport, Store, StoreError};
use tokio::task::JoinHandle;

struct Inner {
    store: Arc<Store>,
    root: PathBuf,
    stop: AtomicBool,
}

/// 持久化运行时：恢复、手动落盘、后台定时任务的统一入口（可廉价 clone）。
#[derive(Clone)]
pub struct PersistRuntime {
    inner: Arc<Inner>,
}

impl PersistRuntime {
    pub fn new(store: Arc<Store>, root: impl Into<PathBuf>) -> Self {
        Self {
            inner: Arc::new(Inner {
                store,
                root: root.into(),
                stop: AtomicBool::new(false),
            }),
        }
    }

    /// 从最新数据日快照恢复（阻塞 IO 在 blocking 线程池执行）。
    pub async fn recover(&self) -> Result<RecoveryReport, StoreError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.store.recover(&inner.root))
            .await
            .expect("recover task panicked")
    }

    /// 手动落盘（只写 dirty 表）。
    pub async fn flush(&self) -> Result<FlushReport, StoreError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.store.flush(&inner.root, false))
            .await
            .expect("flush task panicked")
    }

    /// 启动后台定时持久化任务，返回其句柄。必须在 tokio runtime 内调用。
    ///
    /// 任务每 `interval` 执行一次 tick：有变更才落盘；检测到 UTC 日切时
    /// 先 purge tombstone 再强制全写。
    pub fn spawn(&self, interval: Duration) -> PersistTaskHandle {
        let inner = self.inner.clone();
        inner.stop.store(false, Ordering::Relaxed);

        let join = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut last_day: Option<String> = None;

            loop {
                ticker.tick().await;
                if inner.stop.load(Ordering::Relaxed) {
                    break;
                }

                // run_tick 内含 purge + flush，整块进 blocking 线程。
                // last_day 随闭包移入、随结果带回，跨 await 不持有任何 std 锁。
                let store = inner.store.clone();
                let root = inner.root.clone();
                let tick = tokio::task::spawn_blocking(move || {
                    let mut state = last_day;
                    let result = run_tick(&store, &root, &mut state);
                    (result, state)
                })
                .await;

                match tick {
                    Ok((Ok(outcome), state)) => {
                        last_day = state;
                        if outcome.rolled_over {
                            tracing::debug!(
                                purged = outcome.purged,
                                day = %outcome.day,
                                "async persist day rollover"
                            );
                        }
                    }
                    Ok((Err(err), state)) => {
                        last_day = state;
                        tracing::warn!(error = %err, "async persist tick failed");
                    }
                    Err(err) => {
                        tracing::warn!(%err, "async persist task cancelled");
                        break;
                    }
                }
            }
        });

        PersistTaskHandle {
            inner: self.inner.clone(),
            join: Some(join),
        }
    }
}

/// 后台持久化任务句柄。退出前调用 [`shutdown`](PersistTaskHandle::shutdown)。
pub struct PersistTaskHandle {
    inner: Arc<Inner>,
    join: Option<JoinHandle<()>>,
}

impl PersistTaskHandle {
    /// 停止定时任务并做最后一次落盘（只写 dirty 表）。
    pub async fn shutdown(mut self) {
        self.inner.stop.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            join.abort();
            let _ = join.await;
        }
        let inner = self.inner.clone();
        let _ = tokio::task::spawn_blocking(move || inner.store.flush(&inner.root, false)).await;
    }
}
