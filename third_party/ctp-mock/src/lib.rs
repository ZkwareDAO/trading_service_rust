//! ctp-mock 嵌套 workspace 的根 harness 包。
//!
//! 本包不产出库逻辑，仅承载 `tests/abi_e2e.rs`（L2 dlopen ABI 冒烟）。
//! 真正的模拟器内核与 ABI 替身位于 members：`ctp-mock-core` / `-td` / `-md` / `-ctl`。
