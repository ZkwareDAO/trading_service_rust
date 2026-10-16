//! ctp-mock 交易前置 ABI 替身。
//!
//! 编译为 `libctpmock_td.so`，导出与官方 v6.7.13（union/评测版）同名的
//! Itanium mangled 构造/版本符号，内部用一份「只覆盖白名单槽、其余为 null」
//! 的 vtable 替换真实柜台。客户端（trading-service）`dlopen` 后零改动即可
//! 把它当 CTP 测试网连接。
//!
//! 任务 1：建实例 / 注册 SPI / 生命周期不崩；握手、报单、查询等行为随任务 2–6 接入。

mod exports;
pub mod ffi;
