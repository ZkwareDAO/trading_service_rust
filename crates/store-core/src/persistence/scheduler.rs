use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use tracing::{debug, warn};

use crate::store::Store;
use crate::time::day_key;

#[derive(Debug, Clone, Copy)]
pub struct PersistOptions {
    /// 定时落盘间隔。
    pub interval: Duration,
}

impl Default for PersistOptions {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TickOutcome {
    pub day: String,
    /// 本次 tick 是否发生跨日 rollover（purge + 新日文件）。
    pub rolled_over: bool,
    pub purged: usize,
    pub files: usize,
}

/// 执行一次持久化 tick：检测 UTC 日切 → 跨日先 purge tombstone → flush 全量快照。
///
/// `last_day` 由调用方持有（scheduler 线程或测试），首次调用只 flush 不 purge
/// （启动跨日由 `Store::recover` 处理）。
pub fn run_tick(
    store: &Store,
    root: &Path,
    last_day: &mut Option<String>,
) -> crate::error::Result<TickOutcome> {
    let today = day_key(store.now_ms());
    let mut purged = 0;
    let rolled_over = match last_day {
        Some(prev) if *prev != today => {
            purged = store.purge_all();
            true
        }
        _ => false,
    };
    *last_day = Some(today.clone());

    // 跨日必须强制写全部表：新日期目录需要完整快照（启动恢复只认最新日）。
    let report = store.flush(root, rolled_over)?;
    debug!(day = %today, files = report.files, purged, "persist tick done");
    Ok(TickOutcome {
        day: today,
        rolled_over,
        purged,
        files: report.files,
    })
}

#[derive(Debug)]
struct Signal {
    stop: Mutex<bool>,
    cv: Condvar,
}

/// 后台持久化线程句柄。停止前调用 [`shutdown`](PersistHandle::shutdown)，
/// 它会唤醒线程做最后一次 flush 并 join。
pub struct PersistHandle {
    signal: Arc<Signal>,
    join: Option<JoinHandle<()>>,
}

impl PersistHandle {
    pub fn shutdown(mut self) {
        {
            let mut stop = self.signal.stop.lock().unwrap();
            *stop = true;
        }
        self.signal.cv.notify_one();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// 唤醒粒度：即使 interval 很长，shutdown 也最多延迟这么久被感知。
/// （Condvar 不计数通知：notify 早于首次 wait 时会丢失，不能一次性长睡。）
const WAIT_STEP: Duration = Duration::from_millis(50);

/// 启动后台持久化线程（std 线程 + Condvar，无 channel）。
/// 线程每隔 interval 执行一次 tick；shutdown 时唤醒并执行收尾 tick。
pub fn spawn(store: Arc<Store>, root: PathBuf, options: PersistOptions) -> PersistHandle {
    let signal = Arc::new(Signal {
        stop: Mutex::new(false),
        cv: Condvar::new(),
    });
    let thread_signal = signal.clone();

    let join = thread::Builder::new()
        .name("store-persist".to_string())
        .spawn(move || {
            // 间隔钳到唤醒粒度：interval=0 不能退化成忙等循环。
            let interval = options.interval.max(WAIT_STEP);
            let mut last_day: Option<String> = None;
            let mut guard = thread_signal.stop.lock().unwrap();

            loop {
                // 分段睡到 interval：stop 置位（无论通知是否被错过）都能及时退出。
                let mut waited = Duration::ZERO;
                while waited < interval && !*guard {
                    let step = WAIT_STEP.min(interval - waited);
                    let (next_guard, timeout) = thread_signal.cv.wait_timeout(guard, step).unwrap();
                    guard = next_guard;
                    if timeout.timed_out() {
                        waited += step;
                    }
                }
                let stop = *guard;
                drop(guard);

                // 到点或 shutdown 都执行一次 tick（shutdown 时即收尾落盘）。
                match run_tick(&store, &root, &mut last_day) {
                    Ok(outcome) => {
                        if outcome.rolled_over {
                            // 跨日 purge 是重要生命周期事件（tombstone 被物理清除）：
                            // 升 info，便于核对历史归档边界。
                            tracing::info!(purged = outcome.purged, day = %outcome.day, "day rollover, tombstones purged");
                        }
                    }
                    Err(err) => warn!(error = %err, "persist tick failed"),
                }

                guard = thread_signal.stop.lock().unwrap();
                if stop {
                    break;
                }
            }
        })
        .expect("spawn store-persist thread");

    PersistHandle {
        signal,
        join: Some(join),
    }
}
