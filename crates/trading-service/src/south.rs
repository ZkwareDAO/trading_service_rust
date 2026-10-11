//! 南向 WS：对接 strategy 客户端（strategy-client）。
//!
//! * 连接地址（已鉴权）：
//!   `ws://<south-addr>/ws?strategy_name=erp&nonce=<32hex>&signature=<64hex>`，
//!   其中 `signature = HMAC-SHA256(secret, strategy_name || nonce)`。
//! * **验证前置**：strategy_name / nonce / signature 在 WebSocket 升级之前全部
//!   校验（含 nonce 防重放）；非法请求统一返回 401，连握手都走不完，且不创建
//!   channel、不插入 registry、不 spawn 任务。详见 [`crate::south_auth`]。
//! * 握手通过后，连接即绑定到该策略身份并注册到 [`crate::hub::StrategyHub`]；
//!   后续报文不再重复验签，身份一律以握手绑定为准（报文内 strategy_name 不采信）。
//! * 回报推送复用同一个连接写端（[`crate::hub::SharedSink`]），不引入 channel。

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};

use trading_core::protocol::{ClientEvent, SouthCommand};

use crate::hub::ClientSink;
use crate::service;
use crate::state::AppState;

/// 鉴权失败的统一应答：不区分原因、不回显策略名/签名等内部结构。
fn unauthorized() -> Response {
    (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    // —— 升级前完成全部校验（验证前置 / 验证通过才分配资源）——
    let strategy = state.south_auth.authenticate(
        params
            .get("strategy_name")
            .or_else(|| params.get("strategy"))
            .map(String::as_str),
        params.get("nonce").map(String::as_str),
        params.get("signature").map(String::as_str),
    );
    let strategy = match strategy {
        Ok(name) => name,
        Err(_) => return unauthorized(),
    };

    ws.on_upgrade(move |socket| handle_socket(socket, state, strategy))
}

async fn handle_socket(socket: WebSocket, state: AppState, strategy: String) {
    tracing::info!(strategy = %strategy, "strategy connected (south)");

    let (raw_sink, mut stream) = socket.split();
    let sink: crate::hub::SharedSink = Arc::new(futures_util::lock::Mutex::new(raw_sink));

    // 握手通过才注册连接；退出时凭 token 摘除，避免旧连接延迟清理误删重连的新连接。
    let token = state
        .hub
        .register(strategy.clone(), ClientSink::from_shared(Arc::clone(&sink)));

    while let Some(frame) = stream.next().await {
        let frame = match frame {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(strategy = %strategy, error = %e, "south ws read error, closing");
                break;
            }
        };
        match frame {
            Message::Text(text) => {
                let text = text.to_string();
                tracing::info!(strategy = %strategy, payload = %text, "south message received");
                handle_text(&state, &strategy, &sink, text).await;
            }
            Message::Binary(bytes) => {
                if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                    tracing::info!(strategy = %strategy, payload = %text, "south binary received");
                    handle_text(&state, &strategy, &sink, text).await;
                }
            }
            Message::Ping(ping) => {
                let mut g = sink.lock().await;
                let _ = g.send(Message::Pong(ping)).await;
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    state.hub.unregister_if(&strategy, token);
    tracing::info!(strategy = %strategy, "strategy disconnected (south)");
}

async fn handle_text(
    state: &AppState,
    strategy: &str,
    sink: &crate::hub::SharedSink,
    text: String,
) {
    let cmd = match serde_json::from_str::<SouthCommand>(&text) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(strategy = %strategy, error = %e, payload = %text, "bad south command");
            direct_reply(
                sink,
                &ClientEvent::error(format!("报文解析失败: {e}"), Some(strategy.to_string())),
            )
            .await;
            return;
        }
    };

    // 身份以握手绑定为准，直接路由到该策略；命令体里的 strategy_name 不再采信。
    service::dispatch(state.clone(), strategy.to_string(), cmd).await;
}

/// 报文无法解析、尚无业务上下文时，直接往当前连接回一帧错误。
async fn direct_reply(sink: &crate::hub::SharedSink, event: &ClientEvent) {
    if let Ok(json) = serde_json::to_string(event) {
        let mut g = sink.lock().await;
        let _ = g.send(Message::Text(json.into())).await;
        let _ = g.flush().await;
    }
}
