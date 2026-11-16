//! 跨 crate 自动化测试支撑库（dev-only）。
//!
//! 本 crate 只有一个职责：为「下单 → 成交 → 建仓 → 行情 → 风控 → 平仓」整条
//! 业务链提供可复用的测试 harness。具体场景在 `tests/scenarios/` 下，通过
//! `use e2e_test::harness as common;` 引用本模块，避免每个场景文件各自装配
//! `TradingStore/AppState/RuleEngine`。
//!
//! 刻意保留 `src/lib.rs`：Cargo 对「只有 tests/ 没有 lib/bin」的包不会生成任何
//! 构建目标，clippy/`cargo test -p` 会因此空转。

pub mod harness;
