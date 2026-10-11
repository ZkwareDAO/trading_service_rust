//! Deribit 私有 WS 的 `subscription` 信封解析与频道归类（纯函数、零网络）。
//!
//! 推送帧形态：
//! ```json
//! {"jsonrpc":"2.0","method":"subscription",
//!  "params":{"channel":"user.orders.option.BTC.raw","data":{ ... }}}
//! ```
//! - `user.orders.option.{BTC,ETH}.raw` → 订单状态（`AccountEvent::Order`）；
//! - `user.changes.option.{BTC,ETH}.raw` → 仓位脏信号（`AccountEvent::Account`），
//!   真值仍由消费方调 REST `get_positions` 获取。
//!
//! 订单/仓位 JSON 的字段映射复用 venue.rs 的 `map_order_state`/`map_position`。

use serde_json::Value;

use crate::api::{AccountDirty, AccountEvent};
use crate::deribit::rpc::RpcError;
use crate::deribit::venue::map_order_state;

/// 订阅帧解析出的频道归属。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionKind {
    Orders,
    Changes,
}

/// 判定频道归属：orders 帧 / changes 帧；其它（含心跳应答）返回 None。
pub fn channel_kind(channel: &str) -> Option<SubscriptionKind> {
    let parts: Vec<&str> = channel.split('.').collect();
    // 形态：user.<orders|changes>.option.<currency>.raw
    match parts.get(1).copied() {
        Some("orders") => Some(SubscriptionKind::Orders),
        Some("changes") => Some(SubscriptionKind::Changes),
        _ => None,
    }
}

/// subscription 信封解析：返回 `(频道名, data)`。
///
/// 非 subscription（如心跳/确认帧）返回 None；JSON 非法/缺字段返回 Err。
pub fn parse_subscription(raw: &str) -> Result<Option<(String, Value)>, RpcError> {
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
    if v.get("method").and_then(|x| x.as_str()) != Some("subscription") {
        return Ok(None);
    }
    let params = v.get("params");
    let channel = params
        .and_then(|p| p.get("channel"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    let data = params
        .and_then(|p| p.get("data"))
        .cloned()
        .unwrap_or(Value::Null);
    Ok(Some((channel, data)))
}

/// 一帧 subscription → 领域 [`AccountEvent`]；非订单/仓位频道或非订阅帧返回 None。
///
/// orders 帧：data 是单个订单对象 → `Order(map_order_state)`。
/// changes 帧：data 可能是数组 → 取其中受影响 instrument 名作为脏信号。
pub fn map_subscription(
    raw: &str,
    fallback_instrument: &str,
) -> Result<Option<AccountEvent>, RpcError> {
    let Some((channel, data)) = parse_subscription(raw)? else {
        return Ok(None);
    };
    match channel_kind(&channel) {
        Some(SubscriptionKind::Orders) => {
            let state = map_order_state(&data, fallback_instrument);
            Ok(Some(AccountEvent::Order(state)))
        }
        Some(SubscriptionKind::Changes) => {
            let instruments = collect_instruments(&data);
            Ok(Some(AccountEvent::Account(AccountDirty { instruments })))
        }
        None => Ok(None),
    }
}

/// changes 帧 data（对象或数组）→ 受影响 instrument 名列表（去重保持顺序）。
fn collect_instruments(data: &Value) -> Vec<String> {
    let rows: Vec<&Value> = match data {
        Value::Array(a) => a.iter().collect(),
        other => vec![other],
    };
    let mut out = Vec::new();
    for row in rows {
        if let Some(name) = row.get("instrument_name").and_then(|x| x.as_str()) {
            if !out.iter().any(|x: &String| x == name) {
                out.push(name.to_string());
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use trading_models::OrderStatus;

    #[test]
    fn classifies_channels() {
        assert_eq!(
            channel_kind("user.orders.option.BTC.raw"),
            Some(SubscriptionKind::Orders)
        );
        assert_eq!(
            channel_kind("user.changes.option.ETH.raw"),
            Some(SubscriptionKind::Changes)
        );
        assert_eq!(channel_kind("heartbeat"), None);
    }

    #[test]
    fn parses_orders_subscription_to_order_event() {
        let raw = r#"{
          "jsonrpc":"2.0","method":"subscription",
          "params":{"channel":"user.orders.option.BTC.raw","data":{
            "order_id":"ETH-331562","label":"0xclient",
            "instrument_name":"BTC-1","order_state":"open",
            "filled_amount":0,"average_price":0
          }}
        }"#;
        let ev = map_subscription(raw, "BTC-1").expect("ok").expect("event");
        match ev {
            AccountEvent::Order(state) => {
                assert_eq!(state.status, OrderStatus::New);
                assert_eq!(state.exchange_order_id, Some("ETH-331562".into()));
            }
            _ => panic!("expected order"),
        }
    }

    #[test]
    fn parses_changes_subscription_to_dirty() {
        let raw = r#"{
          "jsonrpc":"2.0","method":"subscription",
          "params":{"channel":"user.changes.option.BTC.raw","data":[
            {"instrument_name":"BTC-1"},
            {"instrument_name":"BTC-1"},
            {"instrument_name":"BTC-2"}
          ]}
        }"#;
        let ev = map_subscription(raw, "").expect("ok").expect("event");
        match ev {
            AccountEvent::Account(dirty) => {
                // 去重：BTC-1 只一次。
                assert_eq!(
                    dirty.instruments,
                    vec!["BTC-1".to_string(), "BTC-2".to_string()]
                );
            }
            _ => panic!("expected account dirty"),
        }
    }

    #[test]
    fn non_subscription_frames_are_none() {
        // 心跳/确认帧不是 subscription。
        assert!(map_subscription(r#"{"jsonrpc":"2.0","result":{}}"#, "")
            .unwrap()
            .is_none());
        assert!(map_subscription("nope", "").is_err());
    }
}
