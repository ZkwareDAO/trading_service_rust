//! Deribit JSON-RPC 2.0 over HTTPS：端点选择、鉴权请求、信封解析。
//!
//! Deribit REST 走 JSON-RPC：请求 `{jsonrpc,id,method,params}`，应答
//! `{jsonrpc,id,result}` 或 `{jsonrpc,id,error:{code,message,data}}`。
//! 本模块把**信封解析**做成零 IO 纯函数（TDD）；网络发送为薄 reqwest 封装，
//! token 的缓存/刷新在 venue 层。
//!
//! 见 `docs/扩展交易所.md` 第四节。

use reqwest::Client;
use serde_json::Value;

/// Deribit 主机（testnet：下单/私有走测试网；公共行情恒定主网，见 market）。
pub fn rest_base(testnet: bool) -> &'static str {
    if testnet {
        "https://test.deribit.com"
    } else {
        "https://www.deribit.com"
    }
}

/// JSON-RPC 错误（交易所返回）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<String>,
}

impl RpcError {
    /// 是否为"数量/参数非法"类错误（调用方据此失效缓存的合约规格）。
    /// Deribit 常见：-32602 无效参数、10003 参数缺失等；这里用负的通用码判定。
    pub fn is_invalid_params(&self) -> bool {
        matches!(self.code, -32602 | -32603 | 10003 | 10004)
    }
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "deribit rpc error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for RpcError {}

/// 解析 JSON-RPC 应答信封：成功取 `result`，否则取 `error`。
///
/// 纯函数、零网络。`id` 不强制匹配（调用方如需可自行比对）。
pub fn parse_envelope(raw: &str) -> Result<Value, RpcError> {
    let v: Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(e) => {
            return Err(RpcError {
                code: -32700,
                message: format!("parse error: {e}"),
                data: None,
            })
        }
    };
    if let Some(err) = v.get("error") {
        return Err(RpcError {
            code: err.get("code").and_then(|x| x.as_i64()).unwrap_or(-32603),
            message: err
                .get("message")
                .and_then(|x| x.as_str())
                .unwrap_or("unknown")
                .to_string(),
            data: err.get("data").map(|x| x.to_string()),
        });
    }
    match v.get("result") {
        Some(r) => Ok(r.clone()),
        None => Err(RpcError {
            code: -32603,
            message: "response has neither result nor error".to_string(),
            data: None,
        }),
    }
}

/// 构造 `public/auth`（client_credentials）请求参数。
pub fn auth_params(client_id: &str, client_secret: &str) -> Value {
    serde_json::json!({
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    })
}

/// 无状态 JSON-RPC 发送原语：POST 到 `<base>/api/v2/<method>`，返回已解析的 result。
///
/// 薄封装：信封/错误解析复用 [`parse_envelope`]；token（access_token）由调用方
/// 在需要时放入 `params`，鉴权头对 Deribit 用 `Authorization: Bearer`。
pub async fn call(
    http: Client,
    base: &str,
    method: &str,
    params: Value,
    token: Option<&str>,
) -> anyhow::Result<Value> {
    let url = format!("{}/api/v2/{}", base.trim_end_matches('/'), method);
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    });
    let mut req = http.post(&url);
    if let Some(tok) = token {
        req = req.bearer_auth(tok);
    }
    let resp = req.json(&body).send().await?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    let result = parse_envelope(&text).map_err(|e| anyhow::anyhow!(e))?;
    if !status.is_success() {
        anyhow::bail!("deribit HTTP {status}: {text}");
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_result() {
        let raw = r#"{"jsonrpc":"2.0","id":1,"result":{"order_id":"ETH-331562"}}"#;
        let r = parse_envelope(raw).expect("ok");
        assert_eq!(r["order_id"], "ETH-331562");
    }

    #[test]
    fn parses_error() {
        let raw = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid params","data":"x"}}"#;
        let e = parse_envelope(raw).unwrap_err();
        assert_eq!(
            e,
            RpcError {
                code: -32602,
                message: "invalid params".into(),
                data: Some("\"x\"".into()),
            }
        );
        assert!(e.is_invalid_params());
    }

    #[test]
    fn bad_json_and_missing_result_are_errors() {
        assert_eq!(parse_envelope("nope").unwrap_err().code, -32700);
        assert!(parse_envelope(r#"{"jsonrpc":"2.0","id":1}"#).is_err());
    }

    #[test]
    fn base_picks_testnet_and_prod() {
        assert_eq!(rest_base(true), "https://test.deribit.com");
        assert_eq!(rest_base(false), "https://www.deribit.com");
    }

    #[test]
    fn auth_params_carry_credentials() {
        let p = auth_params("id", "secret");
        assert_eq!(p["grant_type"], "client_credentials");
        assert_eq!(p["client_id"], "id");
        assert_eq!(p["client_secret"], "secret");
    }
}
