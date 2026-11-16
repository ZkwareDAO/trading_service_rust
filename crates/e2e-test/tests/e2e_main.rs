//! 跨 crate 业务链 E2E 的唯一集成测试目标。
//!
//! Cargo 只自动发现 `tests/*.rs`；子用例统一挂在 `scenarios` 模块树下，
//! 避免为每个子文件维护 `[[test]]` 条目。harness 在 `e2e_test::harness`。

mod scenarios;
