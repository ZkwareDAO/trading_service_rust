use serde::{Deserialize, Serialize};
use store_core::model::bool_u8;
use store_core::{AccountModel, Model, StorageScope};

/// account.csv 表头（恒 7 列：6 公共列 + credentials 私有列），列序固定不再变化。
///
/// 各所凭证 schema 归属各自 venue 模块（见 trading-exchange 的
/// `ApiSecretCreds` / `CtpCreds`），本结构不含任何 venue 专属字段。
pub const ACCOUNT_CSV_HEADER: &str = "id,name,exchange,credentials,deleted,created_at,updated_at";

/// 交易账户。全局表，持久化于 `{day}/_system/account.csv`。
///
/// 自迁移方案 B 起收敛为「公共列 + credentials 密文 JSON」：`credentials`
/// 存整串密文 `enc.v1.<b64>`，解密后为各所 credentials JSON，编解码见
/// [`Account::encode_credentials`] / [`Account::decode_credentials`]。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    /// 本地主键。
    pub id: u64,
    /// 账号名称（同一 exchange 内唯一，同时作为持久化目录名）。
    pub name: String,
    /// 交易所：binance / hyperliquid / deribit / ctp / …
    pub exchange: String,
    /// 凭证密文（`enc.v1.<b64>`，明文为各所 credentials JSON）。
    /// 旧宽表文件缺列时按空串处理（旧格式兼容层在 trading-store）。
    #[serde(default)]
    pub credentials: String,
    /// 软删除标记：0=正常，1=已删除。
    #[serde(with = "bool_u8")]
    pub deleted: bool,
    pub created_at: u64,
    pub updated_at: u64,
}

impl Account {
    pub fn new(name: impl Into<String>, exchange: impl Into<String>) -> Self {
        Self {
            id: 0,
            name: name.into(),
            exchange: exchange.into(),
            credentials: String::new(),
            deleted: false,
            created_at: 0,
            updated_at: 0,
        }
    }

    /// 将 `credentials` 归一化为密文存储形态（返回值由调用方回写，不修改自身）：
    /// 明文告警并加密为 `enc.v1.<b64>`；已是密文或空串原样返回。
    pub fn encode_credentials(&self, cipher: &SecretCipher) -> Result<String> {
        Ok(cipher.encrypt_if_plaintext(&self.credentials)?.0)
    }

    /// 将 `credentials` 解密为 JSON 明文：空串透传；无 `enc.v1.` 前缀的
    /// 历史明文按兼容口径原样返回（导入侧告警见 [`SecretCipher::encrypt_if_plaintext`]）。
    pub fn decode_credentials(&self, cipher: &SecretCipher) -> Result<String> {
        cipher.decrypt(&self.credentials)
    }
}

impl Model for Account {
    const MODEL_NAME: &'static str = "account";

    fn id(&self) -> u64 {
        self.id
    }
    fn set_id(&mut self, id: u64) {
        self.id = id;
    }
    fn deleted(&self) -> bool {
        self.deleted
    }
    fn set_deleted(&mut self, deleted: bool) {
        self.deleted = deleted;
    }
    fn created_at(&self) -> u64 {
        self.created_at
    }
    fn updated_at(&self) -> u64 {
        self.updated_at
    }
    fn set_timestamps(&mut self, created_at: u64, updated_at: u64) {
        self.created_at = created_at;
        self.updated_at = updated_at;
    }
    fn scope(&self) -> StorageScope {
        StorageScope::Global
    }
}

impl AccountModel for Account {
    fn account_name(&self) -> &str {
        &self.name
    }
}

// =====================================================================
// 账户凭证本地加解密（AES-256-GCM）
// =====================================================================
//
// 威胁模型：单机、本地 `data/` 目录，目标是防止 secret 在 CSV 里长期明文留存、
// 防止随手 `cat`/误提交泄露，而不是抵御能读取本机密钥文件的攻击者。
//
// 本类型只负责加解密原语；主密钥文件的生成与 0600 原子落盘是服务层 IO，
// 留在 trading-service（fsutil 同时服务于 south.key）。
// CSV 中的密文形态为 `enc.v1.<base64(nonce||ciphertext)>`。
// 为便于本地联调，未加 `enc.v1.` 前缀的值视为**明文**：可解但会在导入侧告警，
// 并提供 [`SecretCipher::encrypt_if_plaintext`] 供导入归一化。

use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, Key, Nonce};
use anyhow::{anyhow, Context, Result};

/// 密文前缀（版本标记）。
pub const SECRET_PREFIX: &str = "enc.v1.";
/// AES-256 主密钥长度（字节）。
pub const SECRET_KEY_LEN: usize = 32;
/// GCM nonce 长度（96-bit）。
const NONCE_LEN: usize = 12;

/// 持有本地主密钥的加解密器（廉价克隆，内部 Arc）。
#[derive(Clone)]
pub struct SecretCipher {
    inner: std::sync::Arc<Aes256Gcm>,
}

impl SecretCipher {
    /// 生成 32 字节随机主密钥（服务层负责以 0600 落盘）。
    pub fn random_key() -> Vec<u8> {
        Aes256Gcm::generate_key(OsRng).to_vec()
    }

    /// 由 32 字节主密钥构造；长度非法时报错。
    pub fn from_key_bytes(key_bytes: &[u8]) -> Result<Self> {
        if key_bytes.len() != SECRET_KEY_LEN {
            return Err(anyhow!(
                "账户主密钥长度非法：期望 {SECRET_KEY_LEN} 字节，实际 {}",
                key_bytes.len()
            ));
        }
        let key: &Key<Aes256Gcm> = Key::<Aes256Gcm>::from_slice(key_bytes);
        Ok(Self {
            inner: std::sync::Arc::new(Aes256Gcm::new(key)),
        })
    }

    /// 加密明文，输出 `enc.v1.<b64>`；空串原样返回空串。
    pub fn encrypt(&self, plaintext: &str) -> Result<String> {
        if plaintext.is_empty() {
            return Ok(String::new());
        }
        let nonce = Aes256Gcm::generate_nonce(OsRng);
        let ciphertext = self
            .inner
            .encrypt(&nonce, plaintext.as_bytes())
            .map_err(|e| anyhow!("encrypt failed: {e}"))?;
        let mut blob = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        blob.extend_from_slice(nonce.as_slice());
        blob.extend_from_slice(&ciphertext);
        Ok(format!("{SECRET_PREFIX}{}", base64_lite::encode(&blob)))
    }

    /// 解密密文；输入若无 `enc.v1.` 前缀则按明文原样返回（联调兼容）。
    pub fn decrypt(&self, stored: &str) -> Result<String> {
        let stored = stored.trim();
        let Some(b64) = stored.strip_prefix(SECRET_PREFIX) else {
            return Ok(stored.to_string());
        };
        let blob = base64_lite::decode(b64).context("密文 base64 解码失败")?;
        if blob.len() < NONCE_LEN + 1 {
            return Err(anyhow!("密文长度不足"));
        }
        let (nonce_bytes, ciphertext) = blob.split_at(NONCE_LEN);
        let nonce = Nonce::from_slice(nonce_bytes);
        let plain = self
            .inner
            .decrypt(nonce, ciphertext)
            .map_err(|e| anyhow!("decrypt failed（密钥不匹配或密文损坏）: {e}"))?;
        String::from_utf8(plain).map_err(|e| anyhow!("解密结果非 UTF-8: {e}"))
    }

    /// 导入侧归一化：明文则加密，已是密文则原样返回。返回 (值, 是否发生了加密)。
    pub fn encrypt_if_plaintext(&self, value: &str) -> Result<(String, bool)> {
        let value = value.trim();
        if value.is_empty() {
            return Ok((String::new(), false));
        }
        if value.starts_with(SECRET_PREFIX) {
            Ok((value.to_string(), false))
        } else {
            tracing::warn!(
                "检测到明文凭证，已按加密形态处理；建议不要长期在 CSV 中保存明文 secret"
            );
            Ok((self.encrypt(value)?, true))
        }
    }
}

// 极简 base64（标准字母表），避免为单一用途再引一个 crate。
mod base64_lite {
    const TBL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    pub fn encode(input: &[u8]) -> String {
        let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
        for chunk in input.chunks(3) {
            let b0 = chunk[0] as u32;
            let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
            let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
            let triple = (b0 << 16) | (b1 << 8) | b2;
            out.push(TBL[((triple >> 18) & 63) as usize] as char);
            out.push(TBL[((triple >> 12) & 63) as usize] as char);
            if chunk.len() > 1 {
                out.push(TBL[((triple >> 6) & 63) as usize] as char);
            } else {
                out.push('=');
            }
            if chunk.len() > 2 {
                out.push(TBL[(triple & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
        out
    }

    pub fn decode(input: &str) -> anyhow::Result<Vec<u8>> {
        let mut buf: u32 = 0;
        let mut bits = 0u32;
        let mut out = Vec::new();
        for c in input.chars() {
            if c == '=' {
                break;
            }
            let v = match c {
                'A'..='Z' => c as u32 - 'A' as u32,
                'a'..='z' => c as u32 - 'a' as u32 + 26,
                '0'..='9' => c as u32 - '0' as u32 + 52,
                '+' => 62,
                '/' => 63,
                _ => anyhow::bail!("非法 base64 字符: {c}"),
            };
            buf = (buf << 6) | v;
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                out.push((buf >> bits) as u8);
                buf &= (1 << bits) - 1;
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- 新结构：公共列 + credentials JSON（迁移方案 B Phase 1）----

    #[test]
    fn new_account_credentials_defaults_empty() {
        // Arrange / Act：新建账户。
        let a = Account::new("alice", "binance");
        // Assert：credentials 缺省空串（凭证 JSON 由上层导入时填充）。
        assert!(a.credentials.is_empty());
    }

    #[test]
    fn csv_header_is_fixed_public_plus_credentials() {
        // Arrange / Act / Assert：表头恒为「6 公共列 + credentials」，列序固定。
        const EXPECTED: [&str; 7] = [
            "id",
            "name",
            "exchange",
            "credentials",
            "deleted",
            "created_at",
            "updated_at",
        ];
        assert_eq!(ACCOUNT_CSV_HEADER.split(',').collect::<Vec<_>>(), EXPECTED);
    }

    #[test]
    fn new_format_csv_row_parses_credentials_json() {
        // Arrange：新格式行；credentials 为含逗号/引号的 JSON，按 CSV 规范
        // 引号包裹且内嵌双引号双写转义。
        let json = r#"{"api_key":"abc123","api_secret":"s3cret","api_passphrase":null,"position_mode":"one_way","is_testnet":false}"#;
        let csv_text = format!(
            "{ACCOUNT_CSV_HEADER}\n1,main,binance,\"{}\",0,1728600000,1728600000\n",
            json.replace('"', "\"\"")
        );

        // Act：flexible 读取，按表头名匹配字段。
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(csv_text.as_bytes());
        let back: Account = rdr.deserialize().next().unwrap().unwrap();

        // Assert：公共列就位，credentials 保留 JSON 原文。
        assert_eq!(back.id, 1);
        assert_eq!(back.name, "main");
        assert_eq!(back.exchange, "binance");
        assert_eq!(back.credentials, json);
        assert!(!back.deleted);
        assert_eq!(back.created_at, 1728600000);
    }

    #[test]
    fn new_format_csv_roundtrip_preserves_credentials() {
        // Arrange：带完整 JSON 凭证的账户。
        let mut a = Account::new("main", "binance");
        a.credentials = r#"{"api_key":"abc123","api_secret":"s3cret"}"#.into();

        // Act：按框架实际落盘格式（无表头单行）序列化再读回。
        let mut wtr = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(Vec::new());
        wtr.serialize(&a).unwrap();
        let line = String::from_utf8(wtr.into_inner().unwrap()).unwrap();
        // 新结构序列化恒 7 列（JSON 内逗号由 CSV 引号包裹，不可按逗号裸切）。
        let record = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(line.as_bytes())
            .records()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(record.len(), 7);
        let back: Account = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(line.as_bytes())
            .deserialize()
            .next()
            .unwrap()
            .unwrap();

        // Assert：credentials 原文往返保留。
        assert_eq!(back.credentials, a.credentials);
        assert_eq!(back.name, "main");
        assert_eq!(back.exchange, "binance");
    }

    #[test]
    fn credentials_codec_roundtrips_through_cipher() {
        // Arrange：JSON 明文凭证 + 随机主密钥。
        let cipher = SecretCipher::from_key_bytes(&SecretCipher::random_key()).unwrap();
        let mut a = Account::new("main", "binance");
        a.credentials = r#"{"api_key":"k","api_secret":"s"}"#.into();

        // Act：明文归一化为密文存储形态，回写后解密。
        let stored = a.encode_credentials(&cipher).unwrap();
        a.credentials = stored;
        assert!(a.credentials.starts_with(SECRET_PREFIX));
        let plain = a.decode_credentials(&cipher).unwrap();

        // Assert：解密还原 JSON 原文。
        assert_eq!(plain, r#"{"api_key":"k","api_secret":"s"}"#);
    }

    #[test]
    fn credentials_codec_empty_and_plaintext_passthrough() {
        // Arrange：空串与无前缀历史明文的兼容口径。
        let cipher = SecretCipher::from_key_bytes(&SecretCipher::random_key()).unwrap();
        let mut a = Account::new("main", "binance");

        // Act / Assert：空串双向透传（不产生密文）。
        assert_eq!(a.encode_credentials(&cipher).unwrap(), "");
        assert_eq!(a.decode_credentials(&cipher).unwrap(), "");

        // 无 enc.v1. 前缀的历史明文：encode 告警并归一为密文；decode 原样透传。
        a.credentials = "plain-json".into();
        let stored = a.encode_credentials(&cipher).unwrap();
        assert!(stored.starts_with(SECRET_PREFIX));
        assert_eq!(a.decode_credentials(&cipher).unwrap(), "plain-json");
    }

    // ---- SecretCipher（账户凭证本地加解密原语）----

    #[test]
    fn cipher_rejects_bad_key_length() {
        // Arrange / Act / Assert：主密钥必须恰为 32 字节（AES-256）。
        assert!(SecretCipher::from_key_bytes(&[0u8; 31]).is_err());
        assert!(SecretCipher::from_key_bytes(&[0u8; 33]).is_err());
        assert!(SecretCipher::from_key_bytes(&[0u8; 32]).is_ok());
    }

    #[test]
    fn random_key_is_32_bytes_and_builds_usable_cipher() {
        // Arrange：生成两次密钥必须不同（随机）。
        let k1 = SecretCipher::random_key();
        let k2 = SecretCipher::random_key();
        assert_eq!(k1.len(), 32);
        assert_ne!(k1, k2);

        // Act / Assert：随机密钥构造的 cipher 可正常加解密往返。
        let cipher = SecretCipher::from_key_bytes(&k1).unwrap();
        let enc = cipher.encrypt("secret-币").unwrap();
        assert_ne!(enc, "secret-币");
        assert_eq!(cipher.decrypt(&enc).unwrap(), "secret-币");
    }

    #[test]
    fn ciphertext_is_prefixed_and_plaintext_passes_through() {
        // Arrange：空串与明文的兼容口径必须与迁移前一致。
        let cipher = SecretCipher::from_key_bytes(&SecretCipher::random_key()).unwrap();

        // Act / Assert：密文带 enc.v1. 前缀；无前缀输入按明文原样返回；空串安全。
        assert!(cipher.encrypt("").unwrap().is_empty());
        assert_eq!(cipher.decrypt("plain-key-123").unwrap(), "plain-key-123");
        assert_eq!(cipher.decrypt("  trimmed  ").unwrap(), "trimmed");
    }

    #[test]
    fn encrypt_if_plaintext_normalizes_once() {
        let cipher = SecretCipher::from_key_bytes(&SecretCipher::random_key()).unwrap();
        let (once, changed1) = cipher.encrypt_if_plaintext("abc").unwrap();
        assert!(changed1);
        assert!(once.starts_with("enc.v1."));
        let (twice, changed2) = cipher.encrypt_if_plaintext(&once).unwrap();
        assert!(!changed2);
        assert_eq!(twice, once);
        assert_eq!(cipher.decrypt(&twice).unwrap(), "abc");
    }

    // 旧宽表格式（api_key/CTP 逐列）的解析与兼容读取测试随迁移方案 B
    // 移至 trading-store 兼容层（Phase 2），本模块只钉新 7 列结构。
}
