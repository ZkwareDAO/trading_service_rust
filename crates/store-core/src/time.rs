use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

/// 毫秒级时间源。生产用 [`SystemClock`]，测试用 [`MockClock`] 驱动跨日。
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

/// 系统墙钟（UTC 由 unix 毫秒时间戳保证，日切见 [`day_key`]）。
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl SystemClock {
    pub fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        Self::now_ms()
    }
}

/// 可控时钟：内部一个原子毫秒值，测试可随意 set/advance。
#[derive(Debug)]
pub struct MockClock(AtomicU64);

impl MockClock {
    pub fn new(ms: u64) -> Self {
        Self(AtomicU64::new(ms))
    }

    pub fn set(&self, ms: u64) {
        self.0.store(ms, Ordering::SeqCst);
    }

    pub fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
}

impl Clock for MockClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// unix 毫秒 -> UTC 日期字符串 `YYYY-MM-DD`，即持久化路径中的 `{day}`。
pub fn day_key(ms: u64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms as i64)
        .unwrap_or_else(|| chrono::DateTime::<chrono::Utc>::from_timestamp_millis(0).unwrap())
        .format("%Y-%m-%d")
        .to_string()
}

/// 便于在多组件间共享的类型别名。
pub type SharedClock = Arc<dyn Clock>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn day_key_is_utc() {
        // 1_700_000_000_000 ms = 2023-11-14 22:13:20 UTC
        assert_eq!(day_key(1_700_000_000_000), "2023-11-14");
        // UTC 23:59:59.999 当天，+1ms 跨到次日
        assert_eq!(day_key(1_700_006_399_999), "2023-11-14");
        assert_eq!(day_key(1_700_006_400_000), "2023-11-15");
    }

    #[test]
    fn mock_clock_advance() {
        let c = MockClock::new(100);
        assert_eq!(c.now_ms(), 100);
        c.advance(50);
        assert_eq!(c.now_ms(), 150);
        c.set(1);
        assert_eq!(c.now_ms(), 1);
    }
}
