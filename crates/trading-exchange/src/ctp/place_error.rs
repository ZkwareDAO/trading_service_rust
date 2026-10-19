//! CTP 下单提交错误（会话线程 → async 门面 → 上层分类的类型化契约）。
//!
//! 会话线程在投递前/受理阶段产生的失败经 oneshot 回传为 `anyhow::Error`，
//! 本类型作为其内层错误，使 [`crate::api::ExchangeApi::classify_place_error`]
//! 能按**错误类型**而非字符串内容区分“确定未受理”与“应答不确定”。

use thiserror::Error;

/// CTP 下单提交的两类失败。
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CtpPlaceError {
    /// 确定性拒绝：下单要素非法、SHFE 平仓超今昨总仓、今昨分布未就绪、
    /// 柜台同步受理码非 0 等——订单确定未送达/未受理，可直接收敛 REJECTED。
    #[error("{0}")]
    Rejected(String),

    /// 应答不确定：信箱等待超时、会话线程已退出等——无法断定订单是否已到
    /// 柜台，落 UNKNOWN，等用户流回报或 REST 查单收敛。
    #[error("{0}")]
    Indeterminate(String),
}

impl CtpPlaceError {
    /// 构造确定性拒绝。
    pub fn rejected(msg: impl Into<String>) -> anyhow::Error {
        anyhow::Error::new(Self::Rejected(msg.into()))
    }

    /// 构造应答不确定错误。
    pub fn indeterminate(msg: impl Into<String>) -> anyhow::Error {
        anyhow::Error::new(Self::Indeterminate(msg.into()))
    }
}
