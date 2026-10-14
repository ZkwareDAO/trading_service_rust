//! CTP 统一错误类型。

use thiserror::Error;

use crate::ctp::core::order::ValidationError;

/// CTP 网关结果别名。
pub type Result<T> = std::result::Result<T, CtpError>;

/// 与 CTP 交互过程中的错误。
#[derive(Debug, Error)]
pub enum CtpError {
    /// 尚未与前置建立/登录连接即发起请求。
    #[error("CTP 前置未就绪：{0}")]
    NotReady(String),

    /// CTP 应答 RspInfo.ErrorID 非 0。
    #[error("CTP 业务错误[{code}]：{msg}")]
    Rsp { code: i32, msg: String },

    /// 下单要素校验失败。
    #[error(transparent)]
    InvalidOrder(#[from] ValidationError),

    /// 其它运行期错误（动态库加载、字符串转换等）。
    #[error("CTP 运行错误：{0}")]
    Other(String),
}
