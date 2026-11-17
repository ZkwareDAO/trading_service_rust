//! E2E 场景模块树入口：共享 harness 在 `crate::harness`，各场景用例在此声明。

mod conditional_open;
mod harness_smoke;
mod order_lifecycle;
mod restart;
mod risk_close;
