//! trading-core：trading-service / xili-exchange / strategy-client 三个二进制共享的
//! 纯协议与工具内核。领域模型统一归属 `trading-models`，内存与持久化统一归属
//! `store-core`/`trading-store`，本 crate 不再自带模型表与落盘实现。
//!
//! 模块职责：
//! * [`decimal_wire`]—— 线上 JSON 的 Decimal 编解码（数字/字符串皆可收，发出为数字）
//! * [`id`]          —— 本地自增主键占位 + `0x`+32hex 全局客户单号
//! * [`timeutil`]    —— `5s`/`500ms` 时长解析、RFC3339 时钟
//! * [`protocol`]    —— 南向/北向 WS 消息、交易所 REST 报文（类型来自 trading-models）
//! * [`quote`]       —— 行情内存表（Instrument → 最新 Quote，纯内存、不持久化）
//! * [`order_rule`]  —— 下单前本地校验/精度对齐内核与下单校验规则内存表（纯函数、纯内存）

pub mod decimal_wire;
pub mod id;
pub mod order_rule;
pub mod protocol;
pub mod quote;
pub mod southauth;
pub mod timeutil;
