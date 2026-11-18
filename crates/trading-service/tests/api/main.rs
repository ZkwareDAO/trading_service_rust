//! REST 接口级测试统一入口（走真实 Router + 真实装配）。
//!
//! 按 `docs/接口文档.md` 逐接口组织：协议层、账户/状态、订单、规则、持仓、
//! 交易所实时查询与 admin 端点各自一个模块；公共装配与 HTTP 工具在 [`support`]。
//!
//! 与 `tests/e2e` 有意分层：这里只钉接口契约（正常路径/参数校验/错误码/
//! 分页过滤/空结果），不复刻业务链断言。

mod accounts;
mod admin;
mod exchange;
mod health_state;
mod orders;
mod positions;
mod protocol;
mod rules;
mod support;
