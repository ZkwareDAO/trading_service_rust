//! 时间与时长工具：RFC3339 时钟、`500ms`/`5s`/`1m` 命令行时长解析。

use std::time::Duration;

use anyhow::{bail, Result};
use chrono::{DateTime, Utc};

/// 当前 UTC 时间的 RFC3339 字符串。
pub fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

/// 解析回报时间字符串；空串视为 None。
pub fn parse_opt_time(raw: &str) -> Option<DateTime<Utc>> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

/// 解析回报时间字符串为 UTC 毫秒时间戳；空串/非法均视为 None
/// （领域模型的时间字段统一是 u64 毫秒）。
pub fn parse_opt_time_ms(raw: &str) -> Option<u64> {
    parse_opt_time(raw).and_then(|dt| u64::try_from(dt.timestamp_millis()).ok())
}

/// 解析带单位的时长：`ms`/`s`/`m`/`h`，如 `500ms`、`5s`、`1m`。
pub fn parse_duration(input: &str) -> Result<Duration> {
    let s = input.trim().to_ascii_lowercase();
    let (num, mul) = if let Some(n) = s.strip_suffix("ms") {
        (n, 1u64)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1_000u64)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60_000u64)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 3_600_000u64)
    } else {
        bail!("invalid duration `{input}`: expected e.g. 500ms/5s/1m/1h")
    };

    let value: u64 = num
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid number in duration `{input}`"))?;
    if value == 0 {
        bail!("duration must be > 0: `{input}`");
    }
    Ok(Duration::from_millis(value * mul))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("5s").unwrap(), Duration::from_secs(5));
        assert_eq!(parse_duration("1m").unwrap(), Duration::from_secs(60));
        assert!(parse_duration("0s").is_err());
        assert!(parse_duration("10x").is_err());
    }

    #[test]
    fn rfc3339_roundtrip() {
        let t = now_rfc3339();
        assert!(parse_opt_time(&t).is_some());
        assert!(parse_opt_time("").is_none());
    }
}
