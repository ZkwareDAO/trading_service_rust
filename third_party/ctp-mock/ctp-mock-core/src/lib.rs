//! ctp-mock 模拟器内核（rlib）。
//!
//! 纯逻辑、不 dlopen：会话握手、成交脚本、最小自洽台账、字段保真、确定性行情
//! 与控制面都在此处，便于 L1 快速单测。FFI 替身（td/md）只做 ABI 适配并把
//! 请求转交本内核。
//!
//! 任务分期见 `.claude/plans/ctp-mock.plan.md` §12。

pub mod control;
pub mod fields;
pub mod sim;

/// 模拟器语义版本（同时作为 GetApiVersion 的伪装基线）。
pub const MOCK_VERSION: &str = "V6.7.13-MOCK";

/// 带结尾 NUL 的版本字节，供 FFI 直接作为 C 字符串返回（与 [`MOCK_VERSION`] 单源）。
pub const MOCK_VERSION_C: &[u8] = b"V6.7.13-MOCK\0";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_c_is_version_plus_nul() {
        let mut expect = MOCK_VERSION.as_bytes().to_vec();
        expect.push(0);
        assert_eq!(MOCK_VERSION_C, expect.as_slice());
    }
}
