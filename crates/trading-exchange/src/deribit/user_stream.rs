//! Deribit 账户私有 WS：建连 → `private/subscribe`（带 access_token）→
//! **逐条即时**产出 subscription 帧；断线按退避重连。
//!
//! 与旧实现的关键区别：不再把整条连接的事件攒进 `Vec`、等断线才投递（连接被
//! 心跳保活长期不断时，事件会永远滞留、状态只能靠 REST 兜底）。现在每读到一帧
//! 立即经流送出，订单状态实时更新。
//!
//! 每次（重）连接先补发一次全量脏信号，消费方据此立即拉一次 `get_positions`。
//!
//! 鉴权：token 由调用方以 [`TokenProvider`] 供应商注入，**每轮重连现取**
//! （public/auth），避免「一次性 token 过期 → 重连订阅被静默拒 → 连接假死」。

use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use futures_util::stream::{BoxStream, StreamExt};
use futures_util::SinkExt;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::api::{AccountDirty, AccountEvent};
use crate::backoff::ReconnectBackoff;
use crate::deribit::mapping::map_subscription;
use crate::deribit::venue::subscribe_params;
use crate::heartbeat::{Heartbeat, HeartbeatAction};

/// 用户流主动 Ping 间隔：账户空闲时持续有帧，重置服务端/代理/NAT 的空闲计时
/// （此前完全静默，约 60s 被对端无 close_notify 掐断）。
const PING_INTERVAL: Duration = Duration::from_secs(15);
/// 空闲上限：超过该时长未收到任何帧（含 Pong）即判死连，交外层退避重连。
const IDLE_TIMEOUT: Duration = Duration::from_secs(45);

/// token 供应商：**每轮重连调用一次**，现取（public/auth）access_token。
///
/// 不可用（auth 失败）返回 Err，该轮跳过、按退避重试。
pub(crate) type TokenProvider =
    Arc<dyn Fn() -> BoxFuture<'static, anyhow::Result<String>> + Send + Sync>;

/// 建连函数端口：生产用 [`run_conn`]，测试注入 fake（记录 token、不碰网络）。
pub(crate) type ConnFn = Arc<
    dyn Fn(
            String,
            Vec<String>,
            String,
        ) -> BoxFuture<'static, anyhow::Result<BoxStream<'static, AccountEvent>>>
        + Send
        + Sync,
>;

/// `private/subscribe` 应答判定三态。
#[derive(Debug, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)] // 三态名以 SubReply 语义对齐，前缀一致是刻意的
pub(crate) enum SubReply {
    /// 订阅成功，携带确认的频道数。
    Accepted { channels: usize },
    /// 订阅被拒（token 过期/无效等），携带原始 error 文本。
    Rejected { error: String },
    /// 非订阅应答帧（订阅数据帧/心跳/非 JSON）：不判定。
    NotSubReply,
}

/// 宽松判定一帧是否为 `private/subscribe` 应答（JSON-RPC：id=1 且带 result/error）。
pub(crate) fn classify_sub_reply(text: &str) -> SubReply {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return SubReply::NotSubReply;
    };
    // 仅认 id=1 的应答帧（订阅请求固定 id=1）；数据帧无 id、是 method=subscription。
    if v.get("id").and_then(|x| x.as_u64()) != Some(1) {
        return SubReply::NotSubReply;
    }
    if let Some(err) = v.get("error") {
        return SubReply::Rejected {
            error: err.to_string(),
        };
    }
    match v.get("result").and_then(|x| x.as_array()) {
        Some(arr) => SubReply::Accepted {
            channels: arr.len(),
        },
        None => SubReply::NotSubReply,
    }
}

/// 读一帧后的三种结果（显式区分，避免用 None/Some 隐式表达）。
enum FrameOutcome {
    /// 连接结束（对端关闭/出错/超时）：子流终止，外层重连。
    End,
    /// 无业务事件（心跳/服务端 Ping/非订阅帧）：继续读下一帧。
    Skip,
    /// 解析出一条账户事件，立即产出。
    Emit(AccountEvent),
}

/// 建立账户事件流：外层按退避「重连」，每条连接是一个逐条产出的子流。
pub(crate) fn connect(
    url: String,
    channels: Vec<String>,
    token_provider: TokenProvider,
    conn_fn: ConnFn,
) -> anyhow::Result<BoxStream<'static, AccountEvent>> {
    let outer = futures_util::stream::unfold(
        (ReconnectBackoff::new(), token_provider, conn_fn),
        move |(mut backoff, provider, conn_fn)| {
            let url = url.clone();
            let channels = channels.clone();
            async move {
                if let Some(wait) = backoff.next_wait() {
                    tokio::time::sleep(wait).await;
                }
                let conn = dial(&url, channels, &provider, &conn_fn).await;
                backoff.note_retry();
                Some((conn, (backoff, provider, conn_fn)))
            }
        },
    )
    .flatten();
    Ok(Box::pin(outer))
}

/// 一轮连接尝试：现取 token → 建连；任一步失败降级为空子流（外层退避重连）。
async fn dial(
    url: &str,
    channels: Vec<String>,
    provider: &TokenProvider,
    conn_fn: &ConnFn,
) -> BoxStream<'static, AccountEvent> {
    // 每轮重连**现取 token**：旧 token 可能已过期，绝不能复用。
    let token = match (provider)().await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "deribit auth failed; will reconnect");
            return Box::pin(futures_util::stream::empty());
        }
    };
    match (conn_fn)(url.to_string(), channels, token).await {
        Ok(conn) => conn,
        Err(e) => {
            tracing::warn!(error = %e, "deribit account ws failed; will reconnect");
            Box::pin(futures_util::stream::empty())
        }
    }
}

/// 单次连接：握手 → 订阅（读应答帧判定成败）→ unfold 读帧，**每读到一个事件立即产出**。
pub(crate) async fn run_conn(
    url: String,
    channels: Vec<String>,
    token: String,
) -> anyhow::Result<BoxStream<'static, AccountEvent>> {
    let mut ws = crate::ws::connect(&url).await?;

    // private/subscribe（access_token 随 params；Deribit WS 鉴权方式）。
    let sub = subscribe_params(&channels, &token);
    let sub_msg = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "private/subscribe",
        "params": sub,
    });
    ws.send(WsMessage::Text(sub_msg.to_string().into())).await?;

    // 读订阅应答：被拒（如 token 过期）立即报错退出，触发外层重连重新取 token；
    // 期间的服务端 Ping 正常回 Pong，杂帧继续等。
    loop {
        let msg = match tokio::time::timeout(IDLE_TIMEOUT, ws.next()).await {
            Ok(Some(Ok(m))) => m,
            Ok(Some(Err(e))) => return Err(anyhow::anyhow!("订阅应答读取失败: {e}")),
            Ok(None) => return Err(anyhow::anyhow!("连接在订阅应答前关闭")),
            Err(_) => return Err(anyhow::anyhow!("等待订阅应答超时")),
        };
        match msg {
            WsMessage::Text(t) => match classify_sub_reply(&t) {
                SubReply::Accepted { channels } => {
                    tracing::info!(channels, "deribit 账户流订阅成功");
                    break;
                }
                SubReply::Rejected { error } => {
                    tracing::error!(%error, "deribit 订阅被拒（token 可能过期），将重连重新鉴权");
                    anyhow::bail!("subscribe rejected: {error}");
                }
                SubReply::NotSubReply => continue,
            },
            WsMessage::Ping(p) => ws.send(WsMessage::Pong(p)).await?,
            WsMessage::Close(_) => return Err(anyhow::anyhow!("连接在订阅应答前被关闭")),
            _ => continue,
        }
    }

    // 首事件固定为全量脏信号（立即拉一次 get_positions；重连同样补发）。
    let head =
        futures_util::stream::once(std::future::ready(AccountEvent::Account(AccountDirty {
            instruments: Vec::new(),
        })));

    // 读帧即产出：read_frame 返回三态，unfold 只做映射。
    let frames = futures_util::stream::unfold(
        (ws, Heartbeat::new(PING_INTERVAL, IDLE_TIMEOUT)),
        |(mut ws, mut hb)| async move {
            let next = match read_frame(&mut ws, &mut hb).await {
                FrameOutcome::End => return None,
                FrameOutcome::Skip => None,
                FrameOutcome::Emit(ev) => Some(ev),
            };
            Some((next, (ws, hb)))
        },
    )
    // unfold 产出 Option<AccountEvent>，滤空并拍平为 AccountEvent 流。
    .filter_map(std::future::ready);

    let conn: BoxStream<'static, AccountEvent> = Box::pin(head.chain(frames));
    Ok(conn)
}

/// 读并处理下一帧：心跳驱动读超时、回 Pong、解析订阅事件。
///
/// 见 [`FrameOutcome`]：连接结束 → End；无业务事件 → Skip；解析出事件 → Emit。
async fn read_frame(ws: &mut crate::ws::WsStream, hb: &mut Heartbeat) -> FrameOutcome {
    // 等到「下一个心跳决策点」；期间有帧则提前返回。
    let wait = hb.deadline();
    let frame = match tokio::time::timeout(wait, ws.next()).await {
        Ok(Some(frame)) => {
            // 任意入站帧（数据 / Pong / 服务端 Ping）证明存活，复位空闲计时。
            hb.on_frame();
            match frame {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!(error = %e, "account ws frame error, reconnecting");
                    return FrameOutcome::End;
                }
            }
        }
        Ok(None) => return FrameOutcome::End, // 对端关闭
        Err(_) => match hb.advance(wait) {
            HeartbeatAction::Ping => {
                if let Err(e) = ws.send(WsMessage::Ping(Default::default())).await {
                    tracing::warn!(error = %e, "ping send failed, reconnecting");
                    return FrameOutcome::End;
                }
                return FrameOutcome::Skip;
            }
            HeartbeatAction::Timeout => {
                tracing::warn!("account stream idle timeout, reconnecting");
                return FrameOutcome::End;
            }
            HeartbeatAction::Wait => return FrameOutcome::Skip,
        },
    };

    // 服务端主动 Ping：照常回 Pong，不产出业务事件。
    let text = match frame {
        WsMessage::Text(t) => t,
        WsMessage::Ping(p) => {
            if let Err(e) = ws.send(WsMessage::Pong(p)).await {
                tracing::warn!(error = %e, "pong send failed, reconnecting");
                return FrameOutcome::End;
            }
            return FrameOutcome::Skip;
        }
        WsMessage::Close(_) => return FrameOutcome::End,
        _ => return FrameOutcome::Skip,
    };

    // 一帧可能是非订阅类（订阅确认/心跳），map_subscription 返回 None 即跳过。
    match map_subscription(&text, "").transpose() {
        Some(Ok(ev)) => FrameOutcome::Emit(ev),
        Some(Err(e)) => {
            tracing::warn!(error = %e, "deribit frame parse failed");
            FrameOutcome::Skip
        }
        None => FrameOutcome::Skip,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// 记录每轮"连接"实际使用的 token（模拟 venue 侧观测）。
    type UsedLog = Arc<Mutex<Vec<String>>>;

    /// 构造 token 供应商：第 n 次调用签发 "tok-n"（模拟旧 token 失效、每次现取新 token）。
    fn counting_provider(issued: Arc<Mutex<Vec<String>>>) -> TokenProvider {
        Arc::new(move || {
            let issued = issued.clone();
            Box::pin(async move {
                let mut g = issued.lock().unwrap();
                let n = g.len() + 1;
                let tok = format!("tok-{n}");
                g.push(tok.clone());
                Ok(tok)
            })
        })
    }

    /// 注入的假连接函数：记录实际收到的 token，返回「一个脏信号后即结束」的流
    /// （模拟连上收到一帧即断，驱动外层循环进入下一轮重连）。
    ///
    /// 注意不能返回空流：空子流零元素，`take(N)` 按元素计数将永远数不满、
    /// 外层重连循环（含退避 sleep）会无限跑，测试挂死。
    fn fake_conn(used: UsedLog) -> ConnFn {
        Arc::new(move |_url, _ch, token| {
            let used = used.clone();
            Box::pin(async move {
                used.lock().unwrap().push(token.to_string());
                let one = futures_util::stream::once(std::future::ready(AccountEvent::Account(
                    AccountDirty {
                        instruments: Vec::new(),
                    },
                )));
                Ok(Box::pin(one) as BoxStream<'static, AccountEvent>)
            })
        })
    }

    #[tokio::test]
    async fn reconnect_fetches_fresh_token_each_round() {
        // Arrange：供应商逐轮签发新 token；连接函数记录每轮实际使用的 token。
        let issued = Arc::new(Mutex::new(Vec::new()));
        let used: UsedLog = Arc::new(Mutex::new(Vec::new()));
        let provider = counting_provider(Arc::clone(&issued));
        let conn = fake_conn(Arc::clone(&used));

        // Act：跑外层循环 3 轮（take 3 条子流即退出）。
        let stream = connect("ws://x".into(), vec!["ch".into()], provider, conn).unwrap();
        let rounds = stream.take(3).collect::<Vec<_>>().await;

        // Assert：
        assert_eq!(rounds.len(), 3, "应产出 3 轮子流");
        // 1. 每轮使用的是当轮签发的 token（重连重新取，而非复用）。
        let used = used.lock().unwrap().clone();
        let issued = issued.lock().unwrap().clone();
        assert_eq!(used, issued, "每轮连接应使用当轮供应商签发的 token");
        // 2. 三轮 token 互不相同——重连没有复用第一轮的（可能已失效的）token。
        assert_ne!(used[0], used[1]);
        assert_ne!(used[1], used[2]);
    }

    #[tokio::test]
    async fn provider_error_retries_after_backoff() {
        // Arrange：供应商先失败一次再成功——失败轮不应产生连接，也不应终止流。
        let issued = Arc::new(Mutex::new(Vec::new()));
        let calls = Arc::new(Mutex::new(0u32));
        let provider: TokenProvider = {
            let issued = Arc::clone(&issued);
            let calls = Arc::clone(&calls);
            Arc::new(move || {
                let issued = Arc::clone(&issued);
                let calls = Arc::clone(&calls);
                Box::pin(async move {
                    let mut c = calls.lock().unwrap();
                    *c += 1;
                    if *c == 1 {
                        anyhow::bail!("auth failed");
                    }
                    let mut g = issued.lock().unwrap();
                    let tok = format!("tok-{}", g.len() + 1);
                    g.push(tok.clone());
                    Ok(tok)
                })
            })
        };
        let used: UsedLog = Arc::new(Mutex::new(Vec::new()));
        let conn = fake_conn(Arc::clone(&used));

        // Act：take 2（失败轮产出**空子流**不计元素；两个成功轮各产 1 个元素）。
        let stream = connect("ws://x".into(), vec!["ch".into()], provider, conn).unwrap();
        let _ = stream.take(2).collect::<Vec<_>>().await;

        // Assert：仅 2 次连接（失败轮不连接），token 与成功签发一致。
        let used = used.lock().unwrap().clone();
        assert_eq!(used.len(), 2, "auth 失败轮不应建立连接");
        let issued = issued.lock().unwrap().clone();
        assert_eq!(used, issued);
    }

    #[test]
    fn classify_sub_reply_accepts_result_array() {
        // Deribit 订阅成功应答形态。
        let raw = r#"{"jsonrpc":"2.0","id":1,"result":["user.orders.option.BTC.raw"]}"#;
        assert!(matches!(
            classify_sub_reply(raw),
            SubReply::Accepted { channels: 1 }
        ));
    }

    #[test]
    fn classify_sub_reply_rejects_error() {
        // 订阅被拒（如 access_token 过期）的 JSON-RPC error 形态。
        let raw =
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":16009,"message":"access_token expired"}}"#;
        match classify_sub_reply(raw) {
            SubReply::Rejected { error } => assert!(error.contains("16009")),
            other => panic!("应为 Rejected，实际 {other:?}"),
        }
    }

    #[test]
    fn classify_sub_reply_ignores_non_sub_frames() {
        // 订阅数据帧 / 心跳等非订阅应答帧：不判定。
        assert!(matches!(
            classify_sub_reply("not json"),
            SubReply::NotSubReply
        ));
        let data = r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"user.orders.option.BTC.raw","data":{}}}"#;
        assert!(matches!(classify_sub_reply(data), SubReply::NotSubReply));
    }
}
