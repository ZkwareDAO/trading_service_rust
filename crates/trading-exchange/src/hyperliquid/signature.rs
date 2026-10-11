//! Hyperliquid L1 action 的 EIP-712 签名（secp256k1 + Keccak256）。
//!
//! 签名链路（与官方 SDK 一致）：
//! 1. action 经 **MessagePack named map**（`rmp_serde::to_vec_named`）序列化；
//! 2. 追加 8 字节大端 nonce 与 1 字节 vault 标志（有 vault 再追加 20 字节地址）；
//! 3. Keccak256 得 connectionId；
//! 4. 作为 EIP-712 `Agent(string source,bytes32 connectionId)` 的 bytes32，
//!    domain 为 name="Exchange"/version="1"/chainId=1337，source 主网="a"/测试网="b"；
//! 5. secp256k1 签名，输出 0x+r+s+(27+recoveryId)。
//!
//! 报文 wire 结构（短字段名 a/b/p/s/r/t/c）同时用于 msgpack 哈希与 JSON 请求体。

use k256::ecdsa::{RecoveryId, Signature as EcdsaSignature, SigningKey};
use serde::Serialize;
use sha3::{Digest, Keccak256};

/// Keccak256 便捷封装，返回 32 字节数组。
fn keccak(data: &[u8]) -> [u8; 32] {
    let out = Keccak256::digest(data);
    let mut buf = [0u8; 32];
    buf.copy_from_slice(&out);
    buf
}

// =====================================================================
// 报文 wire 结构（短字段名；msgpack 哈希与 JSON 请求体共用）
// =====================================================================

/// 一个交易所 action（仅实现当前签名/下单需要的变体，其余按用扩展）。
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub(crate) enum ActionsWire {
    /// 批量下单。
    Order(BulkOrderWire),
    /// 设置杠杆。
    UpdateLeverage(UpdateLeverageWire),
    /// 按 cloid 批量撤单（首选）。
    CancelByCloid(BulkCancelCloidWire),
}

/// updateLeverage action 字段。
#[derive(Debug, Clone, Serialize)]
pub(crate) struct UpdateLeverageWire {
    pub asset: u32,
    // Hyperliquid 官方字段为 camelCase 的 `isCross`；外层 enum 的 rename_all
    // 不会下放到本 struct，必须在此显式 rename，否则测试网返回 422（text/plain）
    // 导致 resp.json() 解析失败、连带整笔开仓被 REJECTED。
    #[serde(rename = "isCross")]
    pub is_cross: bool,
    pub leverage: u32,
}

/// cancelByCloid action。
#[derive(Debug, Clone, Serialize)]
pub(crate) struct BulkCancelCloidWire {
    pub cancels: Vec<CancelCloidItem>,
}

/// cancelByCloid 单项：asset 索引 + cloid。
#[derive(Debug, Clone, Serialize)]
pub(crate) struct CancelCloidItem {
    pub asset: u32,
    pub cloid: String,
}

/// 批量下单 action。
#[derive(Debug, Clone, Serialize)]
pub(crate) struct BulkOrderWire {
    pub orders: Vec<OrderWire>,
    pub grouping: String,
    /// 无 builder 时不序列化。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub builder: Option<BuilderInfoWire>,
}

/// builder 信息（本期不使用，占位保持字段口径）。
#[derive(Debug, Clone, Serialize)]
pub(crate) struct BuilderInfoWire {
    pub address: String,
    pub fee: String,
}

/// 单笔订单 wire。
#[derive(Debug, Clone, Serialize)]
pub(crate) struct OrderWire {
    #[serde(rename = "a")]
    pub asset: u32,
    #[serde(rename = "b")]
    pub is_buy: bool,
    #[serde(rename = "p")]
    pub limit_px: String,
    #[serde(rename = "s")]
    pub sz: String,
    #[serde(rename = "r")]
    pub reduce_only: bool,
    #[serde(rename = "t")]
    pub order_type: OrderTypeWire,
    #[serde(rename = "c", skip_serializing_if = "Option::is_none")]
    pub cloid: Option<String>,
}

/// 订单类型（外部标签，序列化为 `{"limit":{"tif":...}}`）。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum OrderTypeWire {
    /// 限价（tif=Gtc/Ioc/Alo）。
    Limit { tif: String },
}

// =====================================================================
// connectionId：msgpack named map + nonce + vault 标志
// =====================================================================

/// 计算 action 的 connectionId（与官方 SDK `Actions::hash` 逐字节一致）。
///
/// `vault` 为 Some(20 字节地址) 时追加 1+地址，否则追加 0。
pub(crate) fn connection_id(
    action: &ActionsWire,
    nonce: u64,
    vault: Option<[u8; 20]>,
) -> anyhow::Result<[u8; 32]> {
    let mut buf = rmp_serde::to_vec_named(action)
        .map_err(|e| anyhow::anyhow!("msgpack 序列化 action 失败: {e}"))?;
    buf.extend(nonce.to_be_bytes());
    match vault {
        Some(addr) => {
            buf.push(1);
            buf.extend(addr);
        }
        None => buf.push(0),
    }
    Ok(keccak(&buf))
}

// =====================================================================
// EIP-712 Agent 摘要
// =====================================================================

/// `EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)`
fn domain_separator(name: &str, version: &str, chain_id: u64) -> [u8; 32] {
    let type_hash = keccak(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let mut enc = Vec::with_capacity(32 * 5);
    enc.extend(type_hash);
    enc.extend(keccak(name.as_bytes()));
    enc.extend(keccak(version.as_bytes()));
    // uint256 chainId（32 字节大端）。
    let mut chain = [0u8; 32];
    chain[24..].copy_from_slice(&chain_id.to_be_bytes()[..8]);
    enc.extend(chain);
    // address verifyingContract = 0（32 字节）。
    enc.extend([0u8; 32]);
    keccak(&enc)
}

/// 计算 L1 `Agent(string source,bytes32 connectionId)` 的 EIP-712 签名摘要。
fn agent_digest(source: &str, connection_id: [u8; 32]) -> [u8; 32] {
    let domain = domain_separator("Exchange", "1", 1337);
    let agent_type = keccak(b"Agent(string source,bytes32 connectionId)");
    let mut enc = Vec::with_capacity(32 * 3);
    enc.extend(agent_type);
    enc.extend(keccak(source.as_bytes()));
    enc.extend(connection_id);
    let struct_hash = keccak(&enc);
    // digest = keccak(0x1901 ++ domain ++ structHash)。
    let mut digest_input = Vec::with_capacity(2 + 64);
    digest_input.extend([0x19, 0x01]);
    digest_input.extend(domain);
    digest_input.extend(struct_hash);
    keccak(&digest_input)
}

// =====================================================================
// 签名主体
// =====================================================================

/// EVM secp256k1 签名器（持有私钥；地址由公钥推导）。
pub(crate) struct Signer {
    key: SigningKey,
}

impl Signer {
    /// 由十六进制私钥（可带 0x，固定 32 字节）构造。
    pub(crate) fn from_secret_hex(secret: &str) -> anyhow::Result<Self> {
        let s = secret.strip_prefix("0x").unwrap_or(secret);
        if s.len() != 64 {
            anyhow::bail!("私钥须为 32 字节十六进制（64 字符），收到 {} 字符", s.len());
        }
        let bytes = hex::decode(s).map_err(|e| anyhow::anyhow!("私钥十六进制非法: {e}"))?;
        let key = SigningKey::from_slice(&bytes)
            .map_err(|e| anyhow::anyhow!("secp256k1 私钥非法: {e}"))?;
        Ok(Self { key })
    }

    /// 推导 EVM 地址（0x + 20 字节十六进制）。
    // 运行时 user 地址改用配置传入的 api_key（见 venue.rs），本方法仅测试与
    // 诊断使用；保留为 Signer 的固有能力，故允许非测试构建下的 dead_code。
    #[allow(dead_code)]
    pub(crate) fn address_hex(&self) -> String {
        // 未压缩公钥 65 字节，去掉首字节前缀，keccak 后取末 20 字节。
        let point = self.key.verifying_key().to_encoded_point(false);
        let hash = keccak(&point.as_bytes()[1..]);
        format!("0x{}", hex::encode(&hash[12..]))
    }

    /// 对 L1 connectionId 签名，输出 `0x`+r+s+(27+recoveryId)（共 130 字符）。
    /// 生产路径用 sign_l1_parts；本便利方法供完整签名向量的单测使用。
    #[allow(dead_code)]
    pub(crate) fn sign_l1_hex(
        &self,
        connection_id: [u8; 32],
        is_mainnet: bool,
    ) -> anyhow::Result<String> {
        let parts = self.sign_l1_parts(connection_id, is_mainnet)?;
        // 0x + r + s + v（三部分各去掉 0x 前缀）。
        Ok(format!(
            "0x{}{}{:02x}",
            &parts.r[2..],
            &parts.s[2..],
            parts.v
        ))
    }

    /// 对 L1 connectionId 签名，返回分片（exchange body 的 `signature` 对象口径）：
    /// r/s 为 `0x`+32 字节十六进制，v 为 27+recoveryId（27/28）。
    pub(crate) fn sign_l1_parts(
        &self,
        connection_id: [u8; 32],
        is_mainnet: bool,
    ) -> anyhow::Result<SignatureParts> {
        let source = if is_mainnet { "a" } else { "b" };
        let digest = agent_digest(source, connection_id);
        let (sig, recid): (EcdsaSignature, RecoveryId) = self
            .key
            .sign_prehash_recoverable(&digest)
            .map_err(|e| anyhow::anyhow!("签名失败: {e}"))?;
        let (r, s) = sig.split_bytes();
        Ok(SignatureParts {
            r: format!("0x{}", hex::encode(r)),
            s: format!("0x{}", hex::encode(s)),
            v: 27 + recid.to_byte(),
        })
    }
}

/// 签名分片（对应 exchange 请求体 `signature:{r,s,v}`）。
#[derive(Debug, Clone)]
pub(crate) struct SignatureParts {
    pub r: String,
    pub s: String,
    pub v: u8,
}

/// 测试用已知私钥（来自官方 SDK 测试，公开、无资产）。
#[cfg(test)]
const TEST_SECRET: &str = "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e";

#[cfg(test)]
mod tests {
    use super::*;

    /// 构造测试向量中的 IOC 限价单（asset=1，2000.0，3.5，tif=Ioc，无 cloid）。
    fn sample_action() -> ActionsWire {
        ActionsWire::Order(BulkOrderWire {
            orders: vec![OrderWire {
                asset: 1,
                is_buy: true,
                limit_px: "2000.0".to_string(),
                sz: "3.5".to_string(),
                reduce_only: false,
                order_type: OrderTypeWire::Limit {
                    tif: "Ioc".to_string(),
                },
                cloid: None,
            }],
            grouping: "na".to_string(),
            builder: None,
        })
    }

    #[test]
    fn derives_well_formed_evm_address_from_secret() {
        let signer = Signer::from_secret_hex(TEST_SECRET).expect("signer");
        let addr = signer.address_hex();
        assert_eq!(addr.len(), 42, "地址为 0x + 20 字节十六进制");
        assert!(addr.starts_with("0x"));
        // 其余为十六进制字符。
        assert!(addr[2..].chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn signs_order_action_mainnet_and_testnet_exact() {
        let signer = Signer::from_secret_hex(TEST_SECRET).expect("signer");
        let conn = connection_id(&sample_action(), 1_583_838, None).expect("connection id");
        // 主网 / 测试网：均与官方 SDK 测试向量逐字节一致（强校验整条链路）。
        assert_eq!(
            signer.sign_l1_hex(conn, true).expect("sig"),
            "0x77957e58e70f43b6b68581f2dc42011fc384538a2e5b7bf42d5b936f19fbb67360721a8598727230f67080efee48c812a6a4442013fd3b0eed509171bef9f23f1c"
        );
        assert_eq!(
            signer.sign_l1_hex(conn, false).expect("sig"),
            "0xcd0925372ff1ed499e54883e9a6205ecfadec748f80ec463fe2f84f1209648776377961965cb7b12414186b1ea291e95fd512722427efcbcfb3b0b2bcd4d79d01c"
        );
    }

    #[test]
    fn invalid_secret_is_error() {
        assert!(Signer::from_secret_hex("0x123").is_err()); // 长度不对
        assert!(Signer::from_secret_hex(&"g".repeat(64)).is_err()); // 非十六进制
    }

    #[test]
    fn connection_id_matches_official_python_vector() {
        // 官方 Python signing_test 向量：ETH(asset=4) 单，Ioc，无 cloid。
        let action = ActionsWire::Order(BulkOrderWire {
            orders: vec![OrderWire {
                asset: 4,
                is_buy: true,
                limit_px: "1670.1".to_string(),
                sz: "0.0147".to_string(),
                reduce_only: false,
                order_type: OrderTypeWire::Limit {
                    tif: "Ioc".to_string(),
                },
                cloid: None,
            }],
            grouping: "na".to_string(),
            builder: None,
        });
        let conn = connection_id(&action, 1_677_777_606_040, None).expect("conn");
        // msgpack 必须用紧凑编码（fixint/fixmap/fixstr），否则与此官方值不符。
        assert_eq!(
            hex::encode(conn),
            "0fcbeda5ae3c4950a548021552a4fea2226858c4453571bf3f24ba017eac2908"
        );
    }

    #[test]
    fn update_leverage_uses_camel_case_is_cross() {
        // 官方字段必须是 isCross；误写成 is_cross 会被测试网判 422。
        let action = ActionsWire::UpdateLeverage(UpdateLeverageWire {
            asset: 0,
            is_cross: true,
            leverage: 3,
        });
        let json = serde_json::to_value(&action).expect("序列化");
        assert_eq!(json["type"], "updateLeverage");
        assert_eq!(json["isCross"], true, "必须使用 camelCase 的 isCross");
        assert!(
            json.get("is_cross").is_none(),
            "不得残留 snake_case 的 is_cross"
        );
    }
}
