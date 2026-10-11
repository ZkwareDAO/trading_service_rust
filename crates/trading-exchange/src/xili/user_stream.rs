//! Xili 账户事件流连接（监督器按账户各起一条，与 binance 用户数据流同生命周期）。
//!
//! 服务作为 WS 客户端主动外连 xili 的 `/ws`；连接结束固定退避重连，直到该账户
//! 被监督器 abort。收到的北向报文解析出订单回报后，统一经 [`OrderEventSink`]
//! 上交宿主。

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use trading_core::protocol::NorthMessage;

use crate::heartbeat::{Heartbeat, HeartbeatAction};
use crate::port::OrderEventSink;
use crate::provider::VenueEndpoint;

/// 连接失败/被对端关闭后的固定重连等待。
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// 启动一个账户到某 xili 端点的外向账户事件流任务（后台 spawn，立即返回句柄）。
pub fn spawn(
    sink: Arc<dyn OrderEventSink>,
    endpoint: VenueEndpoint,
    account: String,
) -> JoinHandle<()> {
    tokio::spawn(run_connection(sink, endpoint, account))
}

/// 主动连接 xili 账户事件 WS，连接结束后固定退避重连，直到任务被 abort。
async fn run_connection(sink: Arc<dyn OrderEventSink>, endpoint: VenueEndpoint, account: String) {
    let VenueEndpoint {
        name,
        rest_base_url: _,
        ws_url,
    } = endpoint;
    // source 用交易所名，回报经它路由、日志区分来源。
    let source = name.clone();
    loop {
        run_session(&sink, &name, &ws_url, &source, &account).await;
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

/// 单次连接会话；函数返回即代表本次连接结束（连接失败/对端关闭/读错误），由外层重连。
async fn run_session(
    sink: &Arc<dyn OrderEventSink>,
    name: &str,
    ws_url: &str,
    source: &str,
    account: &str,
) {
    tracing::info!(exchange = %name, account, url = %ws_url, "connecting xili account-event stream");
    let mut ws = match crate::ws::connect(ws_url).await {
        Ok(ws) => {
            tracing::info!(exchange = %name, account, "xili account-event stream connected");
            ws
        }
        Err(e) => {
            tracing::warn!(exchange = %name, account, error = %e, "xili ws connect failed");
            return;
        }
    };

    // 应用层保活：主动 Ping + 空闲超时，防 TCP 半开挂起（xili 服务端会回 Pong）。
    let mut hb = Heartbeat::new_default();
    loop {
        let wait = hb.deadline();
        match tokio::time::timeout(wait, ws.next()).await {
            Ok(Some(frame)) => {
                hb.on_frame(); // 任意帧（含 Pong）证明存活
                match frame {
                    Ok(Message::Text(t)) => dispatch_text(sink, name, source, &t).await,
                    Ok(Message::Binary(b)) => {
                        dispatch_text(sink, name, source, &String::from_utf8_lossy(&b)).await
                    }
                    Ok(Message::Ping(p)) => {
                        let _ = ws.send(Message::Pong(p)).await;
                    }
                    Ok(Message::Close(_)) => break,
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(exchange = %name, account, error = %e, "xili ws read error");
                        break;
                    }
                }
            }
            Ok(None) => break, // 对端关闭
            Err(_) => match hb.advance(wait) {
                HeartbeatAction::Ping => {
                    if let Err(e) = ws.send(Message::Ping(Default::default())).await {
                        tracing::warn!(exchange = %name, error = %e, "ping send failed, reconnecting");
                        break;
                    }
                }
                HeartbeatAction::Timeout => {
                    tracing::warn!(exchange = %name, "xili account-event stream idle timeout, reconnecting");
                    break;
                }
                HeartbeatAction::Wait => {}
            },
        }
    }
}

/// 解析一帧文本为北向报文，取出订单回报经 sink 推进状态机并回推策略；坏帧仅记录、跳过。
async fn dispatch_text(sink: &Arc<dyn OrderEventSink>, name: &str, source: &str, text: &str) {
    let msg = match serde_json::from_str::<NorthMessage>(text) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(exchange = %name, error = %e, payload = %text, "bad xili message");
            return;
        }
    };
    if let Some(ev) = msg.event {
        sink.on_order_event(&ev, source, name).await;
    }
}
