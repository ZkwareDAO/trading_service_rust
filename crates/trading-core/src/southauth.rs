//! 南向连接握手鉴权原语（服务端与 strategy-client 客户端共享同一份字节布局）。
//!
//! 见 `docs/服务端与客户端连接.md`：
//! ```text
//! signature = HMAC-SHA256(secret, strategy_name + nonce)   // hex 小写
//! ```
//! nonce 由客户端每次连接随机生成（128-bit，hex 传输）。服务端重算后用
//! 恒定时间比较（[`subtle::ConstantTimeEq`]）校验，防逐字节时序爆破。
//!
//! 本模块只做纯密码学计算，不负责 nonce 去重/TTL 与密钥文件管理——
//! 那些带状态/IO 的部分在 trading-service 的 `south_auth` 模块。

use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// HMAC-SHA256 输出长度（字节），对应 64 个 hex 字符。
const MAC_LEN: usize = 32;

/// 计算签名：`hex_lower(HMAC-SHA256(secret, strategy_name || nonce))`。
///
/// 注意签名消息里策略名与 nonce 直接拼接、无分隔符；两端必须共用本函数，
/// 不要各自手拼。
pub fn sign(secret: &str, strategy_name: &str, nonce: &str) -> String {
    // new_from_slice 仅在密钥长度超过块长大小时报错（HMAC 接受任意长度），此处不会触发。
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret.as_bytes())
        .expect("HMAC-SHA256 accepts keys of any length");
    mac.update(strategy_name.as_bytes());
    mac.update(nonce.as_bytes());
    hex_lower(&mac.finalize().into_bytes())
}

/// 恒定时间校验签名。签名必须是 64 个十六进制字符；非法 hex / 长度不符 /
/// 密钥不符一律返回 false（不区分原因，避免泄露内部结构）。
pub fn verify(secret: &str, strategy_name: &str, nonce: &str, signature_hex: &str) -> bool {
    let Ok(received) = hex_decode(signature_hex.trim()) else {
        return false;
    };
    if received.len() != MAC_LEN {
        return false;
    }
    let mut mac = match <HmacSha256 as Mac>::new_from_slice(secret.as_bytes()) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(strategy_name.as_bytes());
    mac.update(nonce.as_bytes());
    let expected = mac.finalize().into_bytes();
    // 长度相等的两个 MAC 用恒定时间比较，防逐字节时序爆破；
    // 长度不符已在上方统一返回，不进入比较。
    expected.ct_eq(&received).into()
}

/// 生成一次性 nonce：128-bit 随机数的 32 字符小写 hex（复用 uuid v4 的 CSPRNG）。
pub fn generate_nonce() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// 生成共享密钥：256-bit 随机数的 64 字符小写 hex（两个 128-bit nonce 拼接）。
pub fn generate_secret() -> String {
    generate_nonce() + &generate_nonce()
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

// is_multiple_of 在 Rust 1.87 才稳定，本 crate MSRV=1.80，保留取模写法。
#[allow(clippy::manual_is_multiple_of)]
fn hex_decode(s: &str) -> Result<Vec<u8>, ()> {
    if s.len() % 2 != 0 {
        return Err(());
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_nibble(bytes[i])?;
        let lo = hex_nibble(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn hex_nibble(c: u8) -> Result<u8, ()> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "shared-secret";
    const STRATEGY: &str = "btc_strategy";

    #[test]
    fn sign_is_64_lowercase_hex_and_stable() {
        let nonce = "8f31c2aabbccddeeff00112233445566";
        let a = sign(SECRET, STRATEGY, nonce);
        let b = sign(SECRET, STRATEGY, nonce);
        assert_eq!(a.len(), 64);
        assert!(a
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        assert_eq!(a, b, "same inputs must produce same signature");
    }

    #[test]
    fn verify_accepts_matching_signature() {
        let nonce = "nonce-0001";
        let sig = sign(SECRET, STRATEGY, nonce);
        assert!(verify(SECRET, STRATEGY, nonce, &sig));
    }

    #[test]
    fn verify_rejects_wrong_secret() {
        let nonce = "nonce-0002";
        let sig = sign(SECRET, STRATEGY, nonce);
        assert!(!verify("other-secret", STRATEGY, nonce, &sig));
    }

    #[test]
    fn signature_binds_strategy_name() {
        // 签名与策略名绑定：改用别的策略名验证必须失败。
        let nonce = "nonce-0003";
        let sig = sign(SECRET, STRATEGY, nonce);
        assert!(!verify(SECRET, "eth_strategy", nonce, &sig));
    }

    #[test]
    fn signature_binds_nonce() {
        // 同一签名换 nonce 即失效（防重放的密码学前提）。
        let sig = sign(SECRET, STRATEGY, "nonce-A");
        assert!(!verify(SECRET, STRATEGY, "nonce-B", &sig));
    }

    #[test]
    fn verify_rejects_malformed_signatures() {
        // 长度不符、含非 hex 字符、空串均拒绝。
        assert!(!verify(SECRET, STRATEGY, "nonce", "abc"));
        assert!(!verify(SECRET, STRATEGY, "nonce", &"z".repeat(64)));
        assert!(!verify(SECRET, STRATEGY, "nonce", ""));
    }

    #[test]
    fn verify_is_case_insensitive_on_hex() {
        // 大写 hex 也应能解析（服务端对客户端编码宽容，但自身恒发小写）。
        let nonce = "nonce-0004";
        let sig_upper = sign(SECRET, STRATEGY, nonce).to_ascii_uppercase();
        assert!(verify(SECRET, STRATEGY, nonce, &sig_upper));
    }

    #[test]
    fn generated_nonce_is_unique_and_32_hex() {
        let a = generate_nonce();
        let b = generate_nonce();
        assert_eq!(a.len(), 32);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b, "nonces must not repeat");
    }

    #[test]
    fn generated_secret_is_64_hex() {
        let secret = generate_secret();
        assert_eq!(secret.len(), 64);
        assert!(secret.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(generate_secret(), secret, "secrets must not repeat");
    }
}
