//! ctp-mock 行情前置 ABI 替身，编译为 `libctpmock_md.so`。
//!
//! 与 td 替身同构：导出官方 union 版 mangled 构造/版本符号，vtable 只覆盖
//! 白名单槽（§4），其余为 null。任务 7 接入登录/订阅/自动游走 tick。

mod exports;
pub mod ffi;
