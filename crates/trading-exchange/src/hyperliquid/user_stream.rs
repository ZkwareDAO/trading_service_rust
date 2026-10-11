//! Hyperliquid 账户私有 WS（无 token，订阅靠 user 参数）：
//! 一条连接订阅 `orderUpdates` 与 `clearinghouseState`，**读到即推**（坑1），
//! Ping/看门狗保活、断线退避重连，重连后先补一次全量脏信号。
//!
//! - orderUpdates.data[]：每个订单更新 → AccountEvent::Order（推进状态机）；
//! - clearinghouseState 帧 → AccountEvent::Account（仓位脏信号，真值再走 REST）。
//!
//! 见 `docs/扩展交易所.md` 第三节、`docs/扩展交易所.md` 坑1。

use std::time::Duration;

use futures_util::stream::{BoxStream, StreamExt};
use futures_util::SinkExt;
use rust_decimal::Decimal;
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::api::{AccountDirty, AccountEvent, OrderPositionSide, OrderState};
use crate::backoff::ReconnectBackoff;
use crate::heartbeat::{Heartbeat, HeartbeatAction};

/// 主动 Ping / 空闲上限（与 deribit 用户流同口径 15s/45s）。
const PING_INTERVAL: Duration = Duration::from_secs(15);
const IDLE_TIMEOUT: Duration = Duration::from_secs(45);

/// JSON 字段（字符串或数字）→ Decimal。
fn dec_field(v: Option<&Value>) -> Option<Decimal> {
    v.and_then(|x| match x {
        Value::String(s) => s.parse().ok(),
        Value::Number(_) => Decimal::from_str_exact(&x.to_string()).ok(),
        _ => None,
    })
}

/// 单条 orderUpdate（status + order）→ OrderState。
fn map_order_update(v: &Value) -> Option<OrderState> {
    let order = v.get("order")?;
    let status = crate::hyperliquid::venue::map_order_status(
        v.get("status").and_then(|x| x.as_str()).unwrap_or(""),
    );
    let instrument = order.get("coin").and_then(|x| x.as_str())?.to_string();
    // BasicOrder.sz=剩余量、origSz=原始量；成交量 = origSz − sz。
    let orig = dec_field(order.get("origSz")).unwrap_or(Decimal::ZERO);
    let left = dec_field(order.get("sz")).unwrap_or(Decimal::ZERO);
    Some(OrderState {
        client_order_id: order
            .get("cloid")
            .and_then(|x| x.as_str())
            .map(str::to_string),
        exchange_order_id: order.get("oid").map(|x| x.to_string()),
        instrument,
        status,
        filled_quantity: (orig - left).max(Decimal::ZERO),
        average_price: dec_field(order.get("limitPx")),
        position_side: OrderPositionSide::Both,
        venue_extra: None,
    })
}

/// 解析一帧为事件列表：
/// - orderUpdates：data[] 逐条转 Order；
/// - clearinghouseState：一条全量脏信号（真值再走 REST）；
/// - 其他帧（subscriptionResponse）：空列表；坏 JSON：None。
pub(crate) fn parse_message(raw: &str) -> Option<Vec<AccountEvent>> {
    let v: Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(_) => return None,
    };
    match v.get("channel").and_then(|x| x.as_str()) {
        Some("orderUpdates") => {
            let mut out = Vec::new();
            if let Some(items) = v.get("data").and_then(|x| x.as_array()) {
                for item in items {
                    if let Some(state) = map_order_update(item) {
                        out.push(AccountEvent::Order(state));
                    }
                }
            }
            Some(out)
        }
        Some("clearinghouseState") => Some(vec![AccountEvent::Account(AccountDirty::default())]),
        _ => Some(vec![]),
    }
}

/// 建立账户事件流：退避「重连」，每条连接读帧即推。
pub(crate) fn connect(
    url: &'static str,
    address: String,
) -> anyhow::Result<BoxStream<'static, AccountEvent>> {
    let outer = futures_util::stream::unfold(ReconnectBackoff::new(), move |mut backoff| {
        let url = url.to_string();
        let address = address.clone();
        async move {
            if let Some(wait) = backoff.next_wait() {
                tokio::time::sleep(wait).await;
            }
            let conn = match run_conn(&url, &address).await {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::warn!(error = %e, "hyperliquid 账户 WS 失败，将重连");
                    Box::pin(futures_util::stream::empty())
                }
            };
            backoff.note_retry();
            Some((conn, backoff))
        }
    })
    .flatten();
    Ok(Box::pin(outer))
}

/// 单条连接：建连 → 订阅 orderUpdates/clearinghouseState → 首帧脏信号 + 读帧流。
async fn run_conn(url: &str, address: &str) -> anyhow::Result<BoxStream<'static, AccountEvent>> {
    let mut ws = crate::ws::connect(url).await?;
    for subscription_type in ["orderUpdates", "clearinghouseState"] {
        let msg = serde_json::json!({
            "method": "subscribe",
            "subscription": {"type": subscription_type, "user": address},
        });
        ws.send(WsMessage::Text(msg.to_string().into())).await?;
    }

    // 连接建立后先补一次全量脏信号（重连同样补发）。
    let head = futures_util::stream::once(std::future::ready(AccountEvent::Account(
        AccountDirty::default(),
    )));

    let frames = futures_util::stream::unfold(
        (ws, Heartbeat::new(PING_INTERVAL, IDLE_TIMEOUT)),
        |(mut ws, mut hb)| async move {
            let next = match read_frame(&mut ws, &mut hb).await {
                FrameOutcome::End => return None,
                FrameOutcome::Skip => None,
                FrameOutcome::Events(evs) => Some(evs),
            };
            Some((next, (ws, hb)))
        },
    )
    // unfold 产出 Option<Vec<AccountEvent>>：滤掉 None，再把 Vec 拍平为逐条事件。
    .filter_map(std::future::ready)
    .flat_map(futures_util::stream::iter);

    Ok(Box::pin(head.chain(frames)))
}

/// 读帧结果三态：连接结束 / 无业务事件 / 解析出若干事件。
enum FrameOutcome {
    End,
    Skip,
    Events(Vec<AccountEvent>),
}

/// 读并处理下一帧：心跳驱动超时、回 Pong、解析订阅帧。
async fn read_frame(ws: &mut crate::ws::WsStream, hb: &mut Heartbeat) -> FrameOutcome {
    let wait = hb.deadline();
    let frame = match tokio::time::timeout(wait, ws.next()).await {
        Ok(Some(frame)) => {
            hb.on_frame();
            match frame {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!(error = %e, "hyperliquid 账户 WS 帧错误，重连");
                    return FrameOutcome::End;
                }
            }
        }
        Ok(None) => return FrameOutcome::End,
        Err(_) => match hb.advance(wait) {
            HeartbeatAction::Ping => {
                if ws.send(WsMessage::Ping(Default::default())).await.is_err() {
                    return FrameOutcome::End;
                }
                return FrameOutcome::Skip;
            }
            HeartbeatAction::Timeout => return FrameOutcome::End,
            HeartbeatAction::Wait => return FrameOutcome::Skip,
        },
    };

    let text = match frame {
        WsMessage::Text(t) => t,
        WsMessage::Ping(p) => {
            if ws.send(WsMessage::Pong(p)).await.is_err() {
                return FrameOutcome::End;
            }
            return FrameOutcome::Skip;
        }
        WsMessage::Close(_) => return FrameOutcome::End,
        _ => return FrameOutcome::Skip,
    };

    match parse_message(&text) {
        Some(evs) => FrameOutcome::Events(evs),
        None => FrameOutcome::Skip,
    }
}

#[cfg(test)]
mod parse_tests {
    use super::*;
    use crate::api::OrderPositionSide;
    use trading_models::OrderStatus;

    #[test]
    fn parses_order_updates_frame() {
        let raw = r#"{"channel":"orderUpdates","data":[
          {"order":{"coin":"BTC","side":"B","limitPx":"80000","sz":"0","origSz":"0.001","oid":1,"timestamp":5,"cloid":"0xclient"},
           "status":"filled","statusTimestamp":9}
        ]}"#;
        let evs = parse_message(raw).expect("parsed");
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            AccountEvent::Order(o) => {
                assert_eq!(o.client_order_id.as_deref(), Some("0xclient"));
                assert_eq!(o.exchange_order_id.as_deref(), Some("1"));
                assert_eq!(o.instrument, "BTC");
                assert_eq!(o.status, OrderStatus::Filled);
                // 成交量 = origSz − sz = 0.001。
                assert_eq!(o.filled_quantity, Decimal::from_str_exact("0.001").unwrap());
                assert_eq!(o.position_side, OrderPositionSide::Both);
            }
            other => panic!("期望 Order，得到 {other:?}"),
        }
    }

    #[test]
    fn resting_order_update_maps_to_new() {
        let raw = r#"{"channel":"orderUpdates","data":[
          {"order":{"coin":"ETH","side":"A","limitPx":"2000","sz":"1","origSz":"1","oid":7,"timestamp":5},
           "status":"open","statusTimestamp":9}
        ]}"#;
        let evs = parse_message(raw).expect("parsed");
        match &evs[0] {
            AccountEvent::Order(o) => {
                assert_eq!(o.status, OrderStatus::New);
                assert_eq!(o.filled_quantity, Decimal::ZERO);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn clearinghouse_state_frame_is_dirty_signal() {
        let raw = r#"{"channel":"clearinghouseState","data":{"assetPositions":[]}}"#;
        let evs = parse_message(raw).expect("parsed");
        assert_eq!(evs.len(), 1);
        // 全量脏信号：instruments 为空。
        match &evs[0] {
            AccountEvent::Account(d) => assert!(d.instruments.is_empty()),
            other => panic!("期望 Account，得到 {other:?}"),
        }
    }

    #[test]
    fn non_account_frame_or_bad_json_yields_nothing() {
        assert_eq!(
            parse_message(r#"{"channel":"subscriptionResponse"}"#)
                .expect("frame")
                .len(),
            0
        );
        assert!(parse_message("not json").is_none());
    }
}
