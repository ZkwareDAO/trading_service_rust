//! 敏感文件安全落盘：以 `0600` **原子新建**（`O_CREAT|O_EXCL`）。
//!
//! 放在 store-core（仅依赖 std）：账户主密钥等共享内存相关的密钥文件与服务层
//! south.key 都经此一处落盘，避免同一原语在多个 crate 重复实现。
//!
//! 相比「`fs::write` 先按 umask 建文件（常见 0644）再 `chmod 600`」：
//! * inode 一出生就是 `0600`，没有「短暂 group/other 可读」的时间窗；
//! * 文件已存在（含被他人预放/符号链接占位）时直接报错，绝不覆盖、不跟随，
//!   消除检查与创建之间的 TOCTOU 竞争。
//!
//! 父目录需已存在（由调用方 `create_dir_all`）。非 unix 平台退化为独占新建、
//! 不设置权限位（那些平台另有 ACL 模型）。

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

/// 仅当 `path` 不存在时，以 `0600` 创建并写入 `contents`。
///
/// 已存在（含符号链接占位）则返回 [`std::io::ErrorKind::AlreadyExists`]，
/// 原文件不被改动、链接不被跟随。
#[cfg(unix)]
pub fn write_new_0600(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true) // O_CREAT|O_EXCL：不存在才成功，且不跟随符号链接
        .mode(0o600) // inode 出生即 0600，没有 umask 导致的 0644 窗口
        .open(path)?;
    file.write_all(contents)
}

/// 非 unix 平台等价实现：独占新建（不设置 POSIX 权限位）。
#[cfg(not(unix))]
pub fn write_new_0600(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(contents)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_path(tag: &str) -> std::path::PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "score-fs-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn creates_with_0600_and_exact_contents() {
        let dir = temp_path("create");
        let path = dir.join("secret.key");
        let payload = b"32-bytes-binary-secret-not-utf8-\x00\xff";
        write_new_0600(&path, payload).expect("new file must be created");
        assert_eq!(std::fs::read(&path).unwrap(), payload);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(
                mode, 0o600,
                "secret file must be born 0600 (no 0644 window)"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn never_overwrites_existing_file() {
        let dir = temp_path("exists");
        let path = dir.join("secret.key");
        std::fs::write(&path, b"original").unwrap();

        let err = write_new_0600(&path, b"attacker").unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
        // 原内容必须保持不变。
        assert_eq!(std::fs::read(&path).unwrap(), b"original");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(unix)]
    #[test]
    fn refuses_symlink_placeholder() {
        // 路径上若已被符号链接占位，O_EXCL 创建必须失败，不跟随、不写穿目标。
        use std::os::unix::fs::symlink;
        let dir = temp_path("symlink");
        let target = dir.join("target.bin");
        std::fs::write(&target, b"keep").unwrap();
        let link = dir.join("secret.key");
        symlink(&target, &link).unwrap();

        let err = write_new_0600(&link, b"x").unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&target).unwrap(), b"keep");
        std::fs::remove_dir_all(&dir).ok();
    }
}
