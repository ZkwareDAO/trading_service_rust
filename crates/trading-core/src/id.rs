//! ID 工具：
//! * `client_order_id` 为 `0x` + 32 个十六进制字符（UUIDv4 的 16 字节），
//!   全局唯一，透传给交易所并由回报原样带回，是查单/路由的首选键；
//! * 本地数字主键由 store 框架的 `Table` 自动分配，本模块不再维护生成器；
//! * v0 账户名到账户 id 的映射暂用确定性哈希（与账户表 stable_id 同口径）。

use uuid::Uuid;

/// 生成 `0x` + 32hex 形式的客户订单号（如 `0x9f1c4a2b8d7e4f60a1c3d5e7f90214b6`）。
pub fn new_client_order_id() -> String {
    format!("0x{}", hex32(Uuid::new_v4()))
}

fn hex32(u: Uuid) -> String {
    let bytes = u.as_bytes();
    let mut out = String::with_capacity(32);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// 将账户名稳定映射为 u64 账户 id（与账户热加载的 stable_id 同口径）。
pub fn account_id_from_name(name: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    name.hash(&mut h);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_order_id_shape() {
        let id = new_client_order_id();
        assert_eq!(id.len(), 34);
        assert!(id.starts_with("0x"));
        assert!(id[2..].chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn account_id_is_stable() {
        assert_eq!(account_id_from_name("alice"), account_id_from_name("alice"));
        assert_ne!(account_id_from_name("alice"), account_id_from_name("bob"));
    }
}
