use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Result, StoreError};

/// 临时文件名自增序号：多个 flush（如 tokio spawn_blocking 工作线程）并发时，
/// 不能共用同一个 `.tmp` 路径。
static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// 账户名作为目录名时的安全清洗：去控制字符/路径分隔符/空白，保留可读主体。
/// 保留目录名（`_system` / `_unassigned`）加 `~` 前缀避免冲突。
pub fn sanitize_dir_name(raw: &str) -> String {
    let cleaned: String = raw
        .trim()
        .chars()
        .map(|c| match c {
            '/' | '\\' | '\0' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_control() || c.is_whitespace() => '_',
            c => c,
        })
        .collect();
    let cleaned = cleaned.trim_matches('.').to_string();
    let cleaned = if cleaned.is_empty() {
        "_".to_string()
    } else {
        cleaned
    };
    match cleaned.as_str() {
        "_system" | "_unassigned" | "." | ".." => format!("~{cleaned}"),
        _ => cleaned,
    }
}

/// 扫描到的一个持久化文件：日期目录、作用域目录（账户名 / `_system`）、model 名、全路径。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DayFile {
    pub day: String,
    pub scope_dir: String,
    pub model: String,
    pub path: PathBuf,
}

/// 形如 `YYYY-MM-DD` 的目录名（仅做格式校验，不强制真实日期）。
fn is_day_dir(name: &str) -> bool {
    let b = name.as_bytes();
    b.len() == 10
        && b[4] == b'-'
        && b[7] == b'-'
        && name.chars().all(|c| c.is_ascii_digit() || c == '-')
}

/// 递归深度固定为 `{day}/{scope}/{model}.csv`，按 (day, scope, model) 排序返回。
pub fn scan(root: &Path) -> Result<Vec<DayFile>> {
    let mut out = Vec::new();
    if !root.exists() {
        return Ok(out);
    }

    let mut days: Vec<String> = fs::read_dir(root)?
        .filter_map(std::result::Result::ok)
        .filter(|e| e.path().is_dir())
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| is_day_dir(n))
        .collect();
    days.sort();

    for day in days {
        let day_dir = root.join(&day);
        let mut scopes: Vec<String> = fs::read_dir(&day_dir)?
            .filter_map(std::result::Result::ok)
            .filter(|e| e.path().is_dir())
            .filter_map(|e| e.file_name().into_string().ok())
            .collect();
        scopes.sort();

        for scope_dir in scopes {
            let scope_path = day_dir.join(&scope_dir);
            let mut models: Vec<String> = fs::read_dir(&scope_path)?
                .filter_map(std::result::Result::ok)
                .filter(|e| e.path().is_file())
                .filter_map(|e| e.file_name().into_string().ok())
                .filter(|n| n.ends_with(".csv"))
                .collect();
            models.sort();

            for file_name in models {
                let model = file_name.trim_end_matches(".csv").to_string();
                out.push(DayFile {
                    day: day.clone(),
                    scope_dir: scope_dir.clone(),
                    model,
                    path: scope_path.join(&file_name),
                });
            }
        }
    }
    Ok(out)
}

/// 原子写：同目录临时文件写完后 rename 覆盖，崩溃不会留下半截 CSV。
pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| StoreError::Decode(format!("bad path: {}", path.display())))?;
    fs::create_dir_all(parent)?;

    let tmp_name = format!(
        ".{}.{}.{}.tmp",
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("snapshot"),
        std::process::id(),
        TMP_SEQ.fetch_add(1, Ordering::Relaxed)
    );
    let tmp = parent.join(tmp_name);
    // sync_all 落盘后再 rename，断电/崩溃也不会留下内容缺失的快照。
    let mut file = fs::File::create(&tmp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_account_names() {
        assert_eq!(sanitize_dir_name("alice"), "alice");
        assert_eq!(sanitize_dir_name(" binance/sub "), "binance_sub");
        assert_eq!(sanitize_dir_name("_system"), "~_system");
        assert_eq!(sanitize_dir_name(".."), "_");
        assert_eq!(sanitize_dir_name(""), "_");
    }

    #[test]
    fn day_dir_matcher() {
        assert!(is_day_dir("2026-09-15"));
        assert!(!is_day_dir("2026-9-15"));
        assert!(!is_day_dir("alice"));
    }
}
