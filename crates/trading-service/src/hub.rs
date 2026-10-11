//! 策略连接注册中心（无 channel）：
//! `strategy_name -> 该策略南向 WS 连接的写端`。
//!
//! 回报到达时，订单里的 strategy_name 在这里查到对应连接，直接向其 TCP 写帧；
//! 同一连接的并发写入用异步互斥串行化。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use dashmap::DashMap;
use futures_util::lock::Mutex;
use futures_util::sink::SinkExt;
use futures_util::stream::SplitSink;

/// 单条南向连接的 WS 写端。
type WsSink = SplitSink<WebSocket, Message>;

/// 多个 [`ClientSink`] 共享同一份连接写端。
pub type SharedSink = Arc<Mutex<WsSink>>;

/// 可廉价克隆共享的连接写端句柄。
#[derive(Clone)]
pub struct ClientSink {
    inner: SharedSink,
}

impl ClientSink {
    pub fn from_shared(inner: SharedSink) -> Self {
        ClientSink { inner }
    }

    /// 发送一帧文本并 flush；连接已关闭时返回错误（调用方据此摘除）。
    pub async fn send_text(&self, text: String) -> anyhow::Result<()> {
        let mut sink = self.inner.lock().await;
        sink.send(Message::Text(text.into())).await?;
        sink.flush().await?;
        Ok(())
    }
}

/// 一次连接注册的令牌：仅在本进程生命周期内有效，用于避免旧连接延迟清理时误删新连接。
pub type ConnToken = u64;

/// 表项：连接写端 + 该连接注册时拿到的唯一令牌。
struct Entry {
    sink: ClientSink,
    token: ConnToken,
}

/// strategy_name -> 当前连接（写端 + 令牌）。
#[derive(Clone)]
pub struct StrategyHub {
    clients: Arc<DashMap<String, Entry>>,
    next_token: Arc<AtomicU64>,
}

impl Default for StrategyHub {
    fn default() -> Self {
        Self::new()
    }
}

impl StrategyHub {
    pub fn new() -> Self {
        StrategyHub {
            clients: Arc::new(DashMap::new()),
            next_token: Arc::new(AtomicU64::new(1)),
        }
    }

    /// 注册/替换某个策略身份的连接，返回本次连接的令牌。
    /// 同名新连接会覆盖旧表项；旧连接应凭自己的令牌调用 [`StrategyHub::unregister_if`]，
    /// 因此它的延迟清理不会把新连接误删。
    pub fn register(&self, strategy_name: impl Into<String>, sink: ClientSink) -> ConnToken {
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        self.clients
            .insert(strategy_name.into(), Entry { sink, token });
        token
    }

    /// 仅当该策略当前表项的令牌与 `token` 一致时才摘除（即只清理“自己”）。
    pub fn unregister_if(&self, strategy_name: &str, token: ConnToken) {
        self.clients
            .remove_if(strategy_name, |_, e| e.token == token);
    }

    /// 向指定策略推送事件；返回 false 表示当前没有该策略的可用连接。
    pub async fn send(&self, strategy_name: &str, event: &serde_json::Value) -> bool {
        let (sink, token) = match self.clients.get(strategy_name) {
            Some(g) => (g.sink.clone(), g.token),
            None => return false,
        };
        // 记录事件类型而非整个 payload：成功推送是高频路径，避免日志噪音/敏感内容。
        let event_kind = event
            .get("type")
            .and_then(|t| t.as_str())
            .unwrap_or("unknown");
        let text = event.to_string();
        match sink.send_text(text).await {
            Ok(()) => {
                tracing::debug!(strategy = %strategy_name, event = event_kind, "event pushed to strategy");
                true
            }
            Err(e) => {
                tracing::warn!(strategy = %strategy_name, error = %e, "send to strategy failed, dropping connection");
                // 同样按令牌条件删除：失败的若是旧连接，不能顺手删掉已注册的新连接。
                self.clients
                    .remove_if(strategy_name, |_, e| e.token == token);
                false
            }
        }
    }
}
