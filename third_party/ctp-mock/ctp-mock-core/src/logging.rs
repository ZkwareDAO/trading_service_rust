//! 极简日志：标准 `log` facade + 一个仅在设置 `CTP_MOCK_LOG` 时才安装的 stderr logger。
//!
//! ctp-mock 是被宿主（trading-service，用 tracing）dlopen 的 cdylib，不能假定宿主装了
//! `log` 后端，也不应改变其默认输出。因此：
//! - **未设置 `CTP_MOCK_LOG`（或 =off/none）**：不安装任何 logger，所有 `log::*!` 静默，
//!   现状行为零变化；若宿主自行装了 `log` 后端，则交由宿主按其级别过滤。
//! - **设置 `CTP_MOCK_LOG=debug|trace|info|warn|error`**：安装本 stderr logger 并设全局级别，
//!   联调时 `CTP_MOCK_LOG=debug` 即可看到逐 tick 推送日志。
//!
//! 故意零额外依赖（不用 env_logger）：实现仅约一页，避免给替身引入不必要的重量。

use std::io::Write;
use std::sync::Once;

use log::{LevelFilter, Metadata, Record};

/// 开关环境变量：取值同 `log` 级别名（error/warn/info/debug/trace/off）。
pub const LOG_ENV: &str = "CTP_MOCK_LOG";

struct StderrLogger;

impl log::Log for StderrLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        // 全局级别已由 set_max_level 控制，这里不过滤。
        true
    }

    fn log(&self, record: &Record) {
        // 单行输出，损坏的管道不 panic。
        let ts = chrono::Utc::now().format("%H:%M:%S%.3f");
        let _ = writeln!(
            std::io::stderr().lock(),
            "[ctp-mock {} {}] {}: {}",
            ts,
            record.level(),
            record.target(),
            record.args()
        );
    }

    fn flush(&self) {
        let _ = std::io::stderr().flush();
    }
}

static LOGGER: StderrLogger = StderrLogger;
static INIT: Once = Once::new();

/// 按环境变量安装一次 logger；未设置/关闭或已安装则为空操作（幂等、线程安全）。
pub fn init() {
    INIT.call_once(|| {
        let Some(level) = level_from_env() else {
            return;
        };
        // 宿主可能已装 log 后端：装不上就只抬全局级别，交给既有后端。
        if log::set_logger(&LOGGER).is_ok() {
            log::set_max_level(level);
        }
    });
}

/// 解析 `CTP_MOCK_LOG`：未设置/空/`off`/`none` 返回 None（不安装），其余按级别名解析。
fn level_from_env() -> Option<LevelFilter> {
    let raw = std::env::var(LOG_ENV).ok()?;
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "off" | "none" => None,
        other => other.parse::<LevelFilter>().ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_name_parses() {
        // 级别名解析由 log crate 提供，确认我们依赖的别名都可用。
        assert_eq!("debug".parse::<LevelFilter>().ok(), Some(LevelFilter::Debug));
        assert_eq!("trace".parse::<LevelFilter>().ok(), Some(LevelFilter::Trace));
        assert_eq!("off".parse::<LevelFilter>().ok(), Some(LevelFilter::Off));
        assert_eq!("garbage".parse::<LevelFilter>().ok(), None);
    }

    #[test]
    fn init_is_idempotent_and_safe_without_env() {
        // 重复调用不 panic；无环境变量时为空操作。
        init();
        init();
    }
}
