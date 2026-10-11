//! Binance U 本位永续用户数据流帧 → 统一 [`AccountEvent`] 的纯函数映射（零网络/SDK）。
//!
//! `ORDER_TRADE_UPDATE` / `ACCOUNT_UPDATE` 由 [`parse_account_event`] 解析；
//! listenKey 过期由 [`is_listen_key_expired`] 识别（连接层据此重连）。

use std::str::FromStr;

use rust_decimal::Decimal;

/// listenKey 过期事件（需要重新申请 listenKey 并重连）。
pub fn is_listen_key_expired(payload: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(payload)
        .map(|v| v.get("e").and_then(|e| e.as_str()) == Some("listenKeyExpired"))
        .unwrap_or(false)
}

// =====================================================================
// 统一账户事件：订单状态 + 账户仓位脏信号（一条连接两类回调）
// =====================================================================

use crate::api::{AccountDirty, AccountEvent, OrderState};
use trading_models::OrderStatus;

/// 解析一帧用户数据流为统一账户事件：
/// * `ORDER_TRADE_UPDATE` → [`AccountEvent::Order`]（含 symbol `o.s`）；
/// * `ACCOUNT_UPDATE` → [`AccountEvent::Account`]，收集 `a.B[].s` 去重后的脏 symbol；
/// * 其它（MARGIN_CALL 等）→ `Ok(None)`。
pub fn parse_account_event(payload: &str) -> anyhow::Result<Option<AccountEvent>> {
    let v: serde_json::Value = serde_json::from_str(payload)?;
    match v.get("e").and_then(|e| e.as_str()) {
        Some("ORDER_TRADE_UPDATE") => Ok(Some(AccountEvent::Order(parse_order_state(&v)?))),
        Some("ACCOUNT_UPDATE") => Ok(Some(AccountEvent::Account(parse_account_dirty(&v)?))),
        _ => Ok(None),
    }
}

fn parse_order_state(v: &serde_json::Value) -> anyhow::Result<OrderState> {
    let o = v
        .get("o")
        .ok_or_else(|| anyhow::anyhow!("ORDER_TRADE_UPDATE missing `o`"))?;
    let client_order_id = o
        .get("c")
        .and_then(|x| x.as_str())
        .ok_or_else(|| anyhow::anyhow!("ORDER_TRADE_UPDATE missing `o.c`"))?
        .to_string();
    let exchange_order_id = o
        .get("i")
        .map(|x| match x {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        })
        .filter(|s| !s.is_empty());
    let status = OrderStatus::from_wire(o.get("X").and_then(|x| x.as_str()).unwrap_or("UNKNOWN"))
        .unwrap_or(OrderStatus::Unknown);
    Ok(OrderState {
        client_order_id: Some(client_order_id),
        exchange_order_id,
        instrument: o
            .get("s")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string(),
        status,
        filled_quantity: decimal_field(o, "z").unwrap_or(Decimal::ZERO),
        average_price: decimal_field(o, "ap"),
        // ps：hedge 持仓方向；缺省/未知按 BOTH（单向）。转换收口在 prep。
        position_side: crate::binance::prep::map_position_side(
            o.get("ps").and_then(|x| x.as_str()),
        ),
        venue_extra: None,
    })
}

fn parse_account_dirty(v: &serde_json::Value) -> anyhow::Result<AccountDirty> {
    // a.B[] 每个元素一个仓位更新，s=symbol；去重保序。
    let mut instruments: Vec<String> = Vec::new();
    if let Some(arr) = v.pointer("/a/B").and_then(|b| b.as_array()) {
        for item in arr {
            if let Some(s) = item.get("s").and_then(|x| x.as_str()) {
                if !s.is_empty() && !instruments.iter().any(|x| x == s) {
                    instruments.push(s.to_string());
                }
            }
        }
    }
    Ok(AccountDirty { instruments })
}

fn decimal_field(o: &serde_json::Value, key: &str) -> Option<Decimal> {
    let d = match o.get(key)? {
        serde_json::Value::String(s) => Decimal::from_str(s.trim()).ok()?,
        serde_json::Value::Number(n) => Decimal::from_str(&n.to_string()).ok()?,
        _ => return None,
    };
    // 0 值视为“尚无成交均价/量”，保持 None 以免覆盖内存中的有效数据。
    if d > Decimal::ZERO {
        Some(d)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trading_models::OrderStatus;

    const FILLED: &str = r#"{
      "e":"ORDER_TRADE_UPDATE","T":1568879465651,"E":1568879465651,
      "o":{
        "s":"BTCUSDT","c":"0x0000000000000000000000000000abcd","S":"SELL","o":"LIMIT",
        "f":"GTC","q":"0.400","p":"11665.19","ap":"11665.18","sp":"0",
        "x":"FILLED","X":"FILLED","i":4283194312,"l":"0.400","z":"0.400",
        "L":"11665.18","T":1568879465650,"t":8,"b":"0","a":"9.33",
        "m":false,"R":false,"wt":"CONTRACT_PRICE","ot":"LIMIT","ps":"BOTH","cp":false,
        "rp":"0","P":""
      }
    }"#;

    #[test]
    fn detects_listen_key_expired() {
        assert!(is_listen_key_expired(r#"{"e":"listenKeyExpired"}"#));
    }

    const HEDGE_CLOSE: &str = r#"{
      "e":"ORDER_TRADE_UPDATE",
      "o":{
        "s":"BTCUSDT","c":"0x000000000000000000000000000000cd",
        "X":"NEW","i":42,"z":"0","ap":"0","ps":"LONG"
      }
    }"#;

    #[test]
    fn order_event_carries_hedge_position_side() {
        // hedge 平仓回报：ps=LONG 必须解析出来（方向不能只靠 side 推断）。
        let ev = parse_account_event(HEDGE_CLOSE).unwrap().unwrap();
        match ev {
            AccountEvent::Order(o) => {
                assert_eq!(o.position_side, crate::api::OrderPositionSide::Long);
            }
            _ => panic!("expected order event"),
        }
    }

    #[test]
    fn private_order_event_has_symbol_and_fills() {
        let ev = parse_account_event(FILLED).unwrap().unwrap();
        match ev {
            AccountEvent::Order(o) => {
                assert_eq!(o.instrument, "BTCUSDT");
                assert_eq!(
                    o.client_order_id.as_deref(),
                    Some("0x0000000000000000000000000000abcd")
                );
                assert_eq!(o.exchange_order_id.as_deref(), Some("4283194312"));
                assert_eq!(o.status, OrderStatus::Filled);
                assert_eq!(o.filled_quantity.to_string(), "0.400");
                assert_eq!(o.average_price.unwrap().to_string(), "11665.18");
            }
            _ => panic!("expected order event"),
        }
    }

    #[test]
    fn account_update_collects_dirty_symbols_dedup() {
        let raw = r#"{"e":"ACCOUNT_UPDATE","a":{"B":[
          {"s":"BTCUSDT","pa":"0.10"},
          {"s":"ETHUSDT","pa":"2"},
          {"s":"BTCUSDT","pa":"0.11"}
        ]}}"#;
        let ev = parse_account_event(raw).unwrap().unwrap();
        match ev {
            AccountEvent::Account(d) => assert_eq!(d.instruments, vec!["BTCUSDT", "ETHUSDT"]),
            _ => panic!("expected account event"),
        }
    }

    #[test]
    fn account_update_without_positions_is_full_resync_signal() {
        let raw = r#"{"e":"ACCOUNT_UPDATE","a":{"B":[]}}"#;
        let ev = parse_account_event(raw).unwrap().unwrap();
        match ev {
            // 空 symbols = 需要全量对账（去抖器据此触发全量 REST）。
            AccountEvent::Account(d) => assert!(d.instruments.is_empty()),
            _ => panic!("expected account event"),
        }
    }

    #[test]
    fn private_parser_ignores_other_events() {
        assert!(parse_account_event(r#"{"e":"MARGIN_CALL"}"#)
            .unwrap()
            .is_none());
    }
}
