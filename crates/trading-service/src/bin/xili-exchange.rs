//! xili-exchange（trading-service 包内二进制）：本地模拟交易所，**作为服务端**运行。
//!
//! * 监听一个固定本地端口（`--addr`，默认 127.0.0.1:9200），同时提供：
//!   - REST：下单 `/orders`、撤单 `/orders/cancel`、持仓 `/positions`；
//!   - 账户事件 WS：`/ws`，trading-service 主动连入；
//! * 受理订单后，按 `--order-fill-times/--order-fill-interval` 节奏把 PARTIAL/FILLED
//!   回报以 `{"type":"order_event","name":...,"event":{...}}` 广播给所有 `/ws` 连接。
//!
//! trading-service 侧通过 `--exchange <name>=http://<addr>` 配置本地址并主动外连，
//! 方向与真实交易所一致（REST 下单 + 外向 WS 收回报），不再有反向注册。

use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tracing::{info, warn};
use trading_core::protocol::{
    OrderEvent, PositionSnapshot, RestAck, RestCancelOrder, RestPlaceOrder,
};

#[derive(Debug, Parser, Clone)]
#[command(name = "xili-exchange", version)]
struct Args {
    #[arg(long = "instance", alias = "mock_instance", default_value = "xili")]
    instance: String,
    #[arg(
        long = "order-fill-times",
        alias = "order_fill_times",
        default_value_t = 1
    )]
    order_fill_times: u32,
    #[arg(
        long = "order-fill-interval",
        alias = "order_fill_interval",
        default_value = "1s"
    )]
    order_fill_interval: String,
    #[arg(long = "price", alias = "mock_price", default_value = "80000")]
    price: String,
    /// REST + 账户事件 WS 的统一监听地址（trading-service 用 --exchange 指向它）。
    #[arg(long = "addr", default_value = "127.0.0.1:9200")]
    addr: String,
    /// 模拟交易所下单 REST 处理延迟（在 /orders 受理业务前 sleep）。
    #[arg(long = "rest-delay", alias = "rest_delay", default_value = "200ms")]
    rest_delay: String,
}

#[derive(Debug, Clone)]
struct WorkingOrder {
    client_order_id: String,
    exchange_order_id: String,
    account: String,
    instrument: String,
    side: String,
    price: Decimal,
    quantity: Decimal,
    filled_quantity: Decimal,
    status: String,
}

#[derive(Clone)]
struct AppState {
    instance: String,
    fill_times: u32,
    fill_interval: Duration,
    price: Decimal,
    orders: Arc<DashMap<String, WorkingOrder>>,
    exchange_seq: Arc<AtomicU64>,
    /// 订单回报广播：每个 /ws 连接订阅一份。
    events: broadcast::Sender<String>,
    /// 下单 REST 入口的人为延迟（模拟交易所处理/网络 RTT）。
    rest_delay: Duration,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();
    let fill_times = args.order_fill_times.max(1);
    let fill_interval = trading_core::timeutil::parse_duration(&args.order_fill_interval)?;
    let rest_delay = trading_core::timeutil::parse_duration(&args.rest_delay)?;
    info!(?rest_delay, "xili-exchange REST place_order delay");
    let price = Decimal::from_str(&args.price)?;
    info!(instance = %args.instance, addr = %args.addr, fill_times, ?fill_interval, "starting xili-exchange");

    let (events, _) = broadcast::channel(256);
    let state = AppState {
        instance: args.instance.clone(),
        fill_times,
        fill_interval,
        price,
        orders: Arc::new(DashMap::new()),
        exchange_seq: Arc::new(AtomicU64::new(1)),
        events,
        rest_delay,
    };

    let app = Router::new()
        .route("/orders", post(place_order))
        .route("/orders/cancel", post(cancel_order))
        .route("/positions", get(get_positions))
        .route("/ws", get(ws_handler))
        .with_state(state);

    let listener = TcpListener::bind(&args.addr).await?;
    info!(
        "xili-exchange REST + account-event ws listening on http://{} (ws path /ws)",
        args.addr
    );
    axum::serve(listener, app).await?;
    Ok(())
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| handle_ws(socket, state))
}

/// 一条 trading-service 账户事件连接：把广播的订单回报推下去，同时排空对端帧以感知关闭。
async fn handle_ws(socket: WebSocket, state: AppState) {
    info!("trading-service connected (account-event ws)");
    let (mut sender, mut receiver) = socket.split();
    let mut rx = state.events.subscribe();
    loop {
        tokio::select! {
            biased;
            ev = rx.recv() => {
                match ev {
                    Ok(text) => {
                        if sender.send(Message::Text(text.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "account-event consumer lagged, continuing");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            frame = receiver.next() => match frame {
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(Message::Ping(p))) => {
                    let _ = sender.send(Message::Pong(p)).await;
                }
                Some(Err(e)) => {
                    warn!(error = %e, "account-event ws read error");
                    break;
                }
                _ => {}
            },
        }
    }
    info!("trading-service disconnected (account-event ws)");
}

async fn place_order(
    State(state): State<AppState>,
    Json(req): Json<RestPlaceOrder>,
) -> Json<RestAck> {
    // 模拟交易所处理/网络 RTT：在受理任何业务逻辑前固定延迟。
    if state.rest_delay > Duration::ZERO {
        tokio::time::sleep(state.rest_delay).await;
    }
    info!(instance = %state.instance, ?req, "REST place_order received");
    let seq = state.exchange_seq.fetch_add(1, Ordering::Relaxed);
    let exchange_order_id = format!("{}-{}", state.instance.to_uppercase(), seq);
    let price = if req.price > Decimal::ZERO {
        req.price
    } else {
        state.price
    };
    state.orders.insert(
        req.client_order_id.clone(),
        WorkingOrder {
            client_order_id: req.client_order_id.clone(),
            exchange_order_id: exchange_order_id.clone(),
            account: req.account.clone(),
            instrument: req.instrument.clone(),
            side: req.side.clone(),
            price,
            quantity: req.quantity,
            filled_quantity: Decimal::ZERO,
            status: "NEW".to_string(),
        },
    );
    let ack = RestAck {
        accepted: true,
        client_order_id: req.client_order_id.clone(),
        exchange_order_id: Some(exchange_order_id),
        status: "NEW".to_string(),
        message: Some("accepted by xili-exchange".to_string()),
    };
    info!(?ack, "REST place_order response 200");
    tokio::spawn(simulate_fills(state, req.client_order_id));
    Json(ack)
}

async fn cancel_order(
    State(state): State<AppState>,
    Json(req): Json<RestCancelOrder>,
) -> Json<RestAck> {
    info!(instance = %state.instance, ?req, "REST cancel_order received");
    let ack = match state.orders.get_mut(&req.client_order_id) {
        Some(mut o) => {
            if o.status == "FILLED" || o.status == "CANCELED" {
                RestAck {
                    accepted: false,
                    client_order_id: req.client_order_id.clone(),
                    exchange_order_id: Some(o.exchange_order_id.clone()),
                    status: o.status.clone(),
                    message: Some(format!("order already {}", o.status)),
                }
            } else {
                o.status = "CANCELED".to_string();
                RestAck {
                    accepted: true,
                    client_order_id: req.client_order_id.clone(),
                    exchange_order_id: Some(o.exchange_order_id.clone()),
                    status: "CANCELED".to_string(),
                    message: Some("canceled".to_string()),
                }
            }
        }
        None => RestAck {
            accepted: false,
            client_order_id: req.client_order_id.clone(),
            exchange_order_id: None,
            status: "REJECTED".to_string(),
            message: Some("unknown client_order_id".to_string()),
        },
    };
    info!(?ack, "REST cancel_order response");
    if ack.accepted {
        if let Some(o) = state.orders.get(&req.client_order_id).map(|g| g.clone()) {
            push_event(&state, &o, "CANCELED");
        }
    }
    Json(ack)
}

#[derive(serde::Deserialize)]
struct AccountQuery {
    #[serde(default)]
    account: String,
}

async fn get_positions(
    State(state): State<AppState>,
    Query(q): Query<AccountQuery>,
) -> Json<Vec<PositionSnapshot>> {
    let mut agg: std::collections::BTreeMap<String, (Decimal, Decimal, Decimal)> =
        std::collections::BTreeMap::new();
    for o in state.orders.iter() {
        if !q.account.is_empty() && o.account != q.account {
            continue;
        }
        if o.filled_quantity <= Decimal::ZERO {
            continue;
        }
        let signed = match o.side.to_ascii_lowercase().as_str() {
            "long" | "buy" => o.filled_quantity,
            _ => -o.filled_quantity,
        };
        let entry = agg.entry(o.instrument.clone()).or_default();
        entry.0 += signed;
        entry.1 += o.price * o.filled_quantity;
        entry.2 += o.filled_quantity;
    }
    let positions = agg
        .into_iter()
        .filter(|(_, (net, _, _))| *net != Decimal::ZERO)
        .map(|(instrument, (net, notional, total_qty))| {
            let (side, quantity) = if net > Decimal::ZERO {
                ("long", net)
            } else {
                ("short", -net)
            };
            PositionSnapshot {
                account: q.account.clone(),
                exchange: state.instance.clone(),
                instrument,
                side: side.to_string(),
                quantity,
                pos_price: if total_qty > Decimal::ZERO {
                    (notional / total_qty).round_dp(8)
                } else {
                    Decimal::ZERO
                },
                // 模拟所同样不产生本系统仓位 id，由 trading-service 用内存仓表 join 回填。
                position_id: None,
            }
        })
        .collect::<Vec<_>>();
    info!(account = %q.account, ?positions, "REST positions response");
    Json(positions)
}

async fn simulate_fills(state: AppState, client_order_id: String) {
    let times = state.fill_times.max(1);
    for step in 1..=times {
        tokio::time::sleep(state.fill_interval).await;
        let mut order = match state.orders.get_mut(&client_order_id) {
            Some(o) => o,
            None => return,
        };
        if order.status == "CANCELED" {
            return;
        }
        let total = order.quantity;
        let cum = if step == times {
            total
        } else {
            ((total / Decimal::from(times)) * Decimal::from(step)).round_dp(8)
        };
        order.filled_quantity = cum;
        let status = if step == times { "FILLED" } else { "PARTIAL" };
        order.status = status.to_string();
        let snapshot = order.clone();
        drop(order);
        push_event(&state, &snapshot, status);
    }
}

fn push_event(state: &AppState, order: &WorkingOrder, status: &str) {
    let event = OrderEvent {
        client_order_id: order.client_order_id.clone(),
        status: status.to_string(),
        exchange_order_id: Some(order.exchange_order_id.clone()),
        filled_price: Some(order.price),
        filled_quantity: Some(order.filled_quantity),
        exchange_update_time: Some(trading_core::timeutil::now_rfc3339()),
        message: None,
        instrument: order.instrument.clone(),
        // xili 非 deribit：venue 委托要素留默认，不触发订单事件播报。
        direction: None,
        reduce_only: false,
        amount: None,
        limit_price: None,
    };
    let text =
        serde_json::json!({"type":"order_event","name":state.instance,"event":event}).to_string();
    // 无 /ws 订阅者时 send 报错属正常（trading-service 暂未连上），仅记录。
    if state.events.send(text.clone()).is_err() {
        warn!(%text, "no account-event subscriber, event dropped");
    } else {
        info!(%text, "order event broadcast to account-event ws");
    }
}

fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init()
        .ok();
}
