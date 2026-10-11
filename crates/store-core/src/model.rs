use serde::Serialize;
use std::collections::HashMap;

use crate::table::Table;

/// 一条记录的持久化归属：
/// - [`Global`](StorageScope::Global)：全局表，落到 `{day}/_system/{model}.csv`（如账户表）。
/// - [`Account(id)`](StorageScope::Account)：归属某账户，落到 `{day}/{account_name}/{model}.csv`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StorageScope {
    Global,
    Account(u64),
}

/// 所有受管 model 必须实现的 trait。
///
/// `created_at` / `updated_at` / `deleted` 由框架自动维护，业务侧不要手工赋值。
pub trait Model: Serialize + serde::de::DeserializeOwned + Clone + Send + Sync + 'static {
    /// CSV 文件名（不含扩展名），同时作为注册表中的 model 标识。
    const MODEL_NAME: &'static str;

    fn id(&self) -> u64;
    fn set_id(&mut self, id: u64);
    fn deleted(&self) -> bool;
    fn set_deleted(&mut self, deleted: bool);
    fn created_at(&self) -> u64;
    fn updated_at(&self) -> u64;
    fn set_timestamps(&mut self, created_at: u64, updated_at: u64);

    /// 持久化归属，默认全局表；Order/Position 等返回 `Account(account_id)`。
    fn scope(&self) -> StorageScope {
        StorageScope::Global
    }
}

/// 账户类 model 的扩展约束：账户名用于拼账户作用域路径。
pub trait AccountModel: Model {
    fn account_name(&self) -> &str;
}

/// 类型擦除后的账户名索引，flush 时解析 `account_id -> 目录名`。
pub trait AccountIndex: Send + Sync {
    /// 存活账户的 id -> 清洗后的目录名。
    fn name_map(&self) -> HashMap<u64, String>;
}

impl<T: AccountModel> AccountIndex for Table<T> {
    fn name_map(&self) -> HashMap<u64, String> {
        self.snapshot(false)
            .into_iter()
            .map(|a| {
                (
                    a.id(),
                    crate::persistence::sanitize_dir_name(a.account_name()),
                )
            })
            .collect()
    }
}

/// `bool` 在 CSV 中以 `0/1` 存储（文档约定：deleted 默认 0，1 为已删除）。
/// 用法：`#[serde(with = "store_core::model::bool_u8")]`。
pub mod bool_u8 {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(b: &bool, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_u8(if *b { 1 } else { 0 })
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<bool, D::Error> {
        let raw = String::deserialize(d)?;
        match raw.trim() {
            "0" | "false" | "False" | "FALSE" | "" => Ok(false),
            "1" | "true" | "True" | "TRUE" => Ok(true),
            other => Err(serde::de::Error::custom(format!(
                "invalid bool value: {other}"
            ))),
        }
    }
}
