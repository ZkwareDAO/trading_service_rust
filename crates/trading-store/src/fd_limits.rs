//! 进程 fd 上限（RLIMIT_NOFILE）查询/自抬 + K线句柄缓存上限推导。
//!
//! 事故背景（2026-11-18）：新机器 `nofile` soft limit=1024，852 个 CTP 合约的
//! K线 CSV 句柄缓存把进程 fd 顶到 1024/1024，触发 `EMFILE` 持续 19 小时。
//! 加固思路：启动时尽量自抬 soft，并按**实际** soft 推导句柄缓存上限，
//! 保证缓存占用不超过进程 fd 预算的 3/4（余量留给 socket/快照等其它 fd）。
//!
//! 纯函数（[`derive_max_open`]/[`raise_target`]）与系统调用（[`current`]/
//! [`ensure_min`]）分离：前者可无副作用单测，后者只做一次轻量读取/自抬。

/// 启动自抬目标：普通进程即可把 soft 抬到 64k（本机 hard=524288）。
pub const DEFAULT_NOFILE_TARGET: u64 = 65_536;

/// 句柄缓存上限硬顶：soft 再大也不越过该值（远超正常规模，只防失控）。
pub const HANDLE_MAX_OPEN_CEILING: usize = 16_384;

/// 期望的句柄缓存下限；soft 极小时仍会被 `soft-8` 余量进一步压低。
const MIN_MAX_OPEN: usize = 8;

/// 给非缓存 fd（socket / 快照 / 日志等）保留的余量。
const FD_RESERVE: u64 = 8;

/// 进程 `nofile` 限制快照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FdLimit {
    /// 当前生效的 soft limit。
    pub soft: u64,
    /// 允许抬到的 hard limit。
    pub hard: u64,
}

/// 按 soft limit 推导句柄缓存上限：
/// `min(硬顶, max(8, soft*3/4))`，再让出 `soft-8` 的余量，保证永不越 soft。
pub fn derive_max_open(soft: u64) -> usize {
    let budget = (soft / 4) * 3;
    let raw = budget
        .max(MIN_MAX_OPEN as u64)
        .min(HANDLE_MAX_OPEN_CEILING as u64);
    raw.min(soft.saturating_sub(FD_RESERVE)) as usize
}

/// 计算自抬目标：`min(hard, 默认目标)`；soft 已达标返回 `None`（不做任何动作）。
/// 只升不降：soft 大于目标时保持原值。
pub fn raise_target(soft: u64, hard: u64) -> Option<u64> {
    let target = hard.min(DEFAULT_NOFILE_TARGET);
    (soft < target).then_some(target)
}

/// 读取当前进程的 `RLIMIT_NOFILE`；系统调用失败返回 `None`（不 panic）。
#[cfg(unix)]
pub fn current() -> Option<FdLimit> {
    let mut rl = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: 传入有效指针，由内核填充本进程的资源限制。
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) };
    (rc == 0).then_some(FdLimit {
        soft: rl.rlim_cur,
        hard: rl.rlim_max,
    })
}

/// 非 unix 平台无 `RLIMIT_NOFILE`：返回 `None`，上层降级为硬顶/默认值。
#[cfg(not(unix))]
pub fn current() -> Option<FdLimit> {
    None
}

/// 启动时把 soft 自抬到 `min(hard, target)`（若当前更低），返回抬升后的限制。
///
/// 失败只告警并返回原值，**不 panic**：容器里 hard=soft 时无法自抬，上层仍按
/// 实际 soft 推导上限，保证 fd 用量不越限。返回 `None` 表示连读取都失败。
#[cfg(unix)]
pub fn ensure_min(target: u64) -> Option<FdLimit> {
    let original = current()?;
    let Some(desired) = raise_target(original.soft, original.hard) else {
        return Some(original);
    };
    let rl = libc::rlimit {
        rlim_cur: desired as libc::rlim_t,
        rlim_max: original.hard as libc::rlim_t,
    };
    // SAFETY: `rl` 已按 hard limit 约束构造，只提升 soft、不改变 hard。
    let rc = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &rl) };
    if rc != 0 {
        tracing::warn!(
            error = %std::io::Error::last_os_error(),
            target,
            soft = original.soft,
            hard = original.hard,
            "K线句柄 nofile 自抬失败，按现有 soft 推导上限"
        );
        return Some(original);
    }
    // 自抬成功后再读一次；偶发读取失败时按目标值返回（setrlimit 已生效）。
    Some(current().unwrap_or(FdLimit {
        soft: desired,
        hard: original.hard,
    }))
}

/// 非 unix 平台：无自抬能力，降级返回 `None`。
#[cfg(not(unix))]
pub fn ensure_min(_target: u64) -> Option<FdLimit> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- derive_max_open：按 soft 推导句柄上限（硬顶 16384、保留 1/4 给其它 fd） ----

    #[test]
    fn derive_max_open_takes_three_quarters_of_small_soft_limit() {
        // 典型容器默认 1024 → 768，与本次事故场景对齐。
        assert_eq!(derive_max_open(1024), 768);
        assert_eq!(derive_max_open(512), 384);
        assert_eq!(derive_max_open(64), 48);
    }

    #[test]
    fn derive_max_open_is_capped_at_ceiling_for_large_soft_limit() {
        assert_eq!(derive_max_open(65_536), HANDLE_MAX_OPEN_CEILING);
        assert_eq!(derive_max_open(524_288), HANDLE_MAX_OPEN_CEILING);
    }

    #[test]
    fn derive_max_open_never_exceeds_soft_limit_for_tiny_values() {
        for soft in [0u64, 1, 7, 8, 9, 10] {
            assert!(
                derive_max_open(soft) as u64 <= soft,
                "soft={soft} 推导值不得越过 soft"
            );
        }
        // 10 的 3/4 是 7，低于最小可写上限 8，但必须让位给 soft-8 的余量 → 2。
        assert_eq!(derive_max_open(10), 2);
    }

    // ---- raise_target：自抬目标 = min(hard, 65536)，已达标则不动作 ----

    #[test]
    fn raise_target_matches_expected_matrix() {
        assert_eq!(raise_target(1024, 524_288), Some(DEFAULT_NOFILE_TARGET));
        assert_eq!(raise_target(131_072, 524_288), None);
        assert_eq!(raise_target(1024, 4096), Some(4096));
        assert_eq!(raise_target(4096, 4096), None);
        // soft=0 且 hard 低于目标：只能抬到 hard。
        assert_eq!(raise_target(0, 2048), Some(2048));
    }

    // ---- current：真实读取，soft ≤ hard 且均有效 ----

    #[cfg(unix)]
    #[test]
    fn current_reports_sane_limits() {
        let limit = current().expect("getrlimit(RLIMIT_NOFILE) 应成功");
        assert!(limit.soft > 0 && limit.hard > 0);
        assert!(limit.soft <= limit.hard);
    }
}
