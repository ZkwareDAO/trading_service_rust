use thiserror::Error;

/// store-core 统一错误类型。库内不 panic（锁中毒除外）、不吞错。
#[derive(Debug, Error)]
pub enum StoreError {
    /// 指定 id 不存在（或已软删除后按"不存在"语义处理）。
    #[error("record not found: id={0}")]
    NotFound(u64),

    /// 显式指定的 id 已存在。
    #[error("duplicate id: {0}")]
    DuplicateId(u64),

    /// 该 model 类型未在 [`Store`](crate::store::Store) 注册。
    #[error("model type not registered: {0}")]
    UnknownModel(&'static str),

    /// 同一类型/名称重复注册。
    #[error("model already registered: {0}")]
    AlreadyRegistered(&'static str),

    /// CSV 行与 model 结构不匹配（缺列、枚举值非法等）。
    #[error("csv decode error: {0}")]
    Decode(String),

    #[error("csv error: {0}")]
    Csv(#[from] csv::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, StoreError>;
