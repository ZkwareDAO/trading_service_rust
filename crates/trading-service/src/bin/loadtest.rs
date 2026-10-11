//! loadtest（trading-service 包内二进制）：南向高并发压测客户端。
//!
//! 开 `--connections` 条策略 WS 连接（各自独立 strategy_name + HMAC 握手），
//! 按目标 `--rate`（信号/秒）整形发送 `place_order`，每条带唯一且合法的
//! client_order_id；在同一连接上收回调并按 cid 配对，统计端到端（信号发出 ->
//! 收到终态回报 FILLED/CANCELED/REJECTED）的吞吐与延迟分位。
//!
//! 仅为压测工具：允许使用 channel（tokio mpsc）做生产者->连接的派发；
//! 交易服务本体仍坚持无 channel 的共享状态设计。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use trading_core::id::new_client_order_id;
use trading_core::southauth;

#[derive(Debug, Parser, Clone)]
#[command(name = "loadtest", version)]
struct Args {
    #[arg(
        long = "south-addr",
        alias = "south_addr",
        default_value = "127.0.0.1:9100"
    )]
    south_addr: String,
    #[arg(
        long = "secret-file",
        alias = "secret_file",
        default_value = "./data/south.key"
    )]
    secret_file: String,
    /// 跳过签名（服务端需 --south-dev-insecure）。
    #[arg(long = "insecure", default_value_t = false)]
    insecure: bool,
    /// 目标信号速率（信号/秒）。
    #[arg(long = "rate", default_value_t = 1000)]
    rate: u64,
    /// 发送信号总数（>0 时按总数发完即止）。
    #[arg(long = "total", default_value_t = 0)]
    total: u64,
    /// total=0 时的持续时长（如 10s/1m）。
    #[arg(long = "duration", default_value = "30s")]
    duration: String,
    /// 并发策略 WS 连接数。
    #[arg(long = "connections", default_value_t = 64)]
    connections: usize,
    /// 发送结束后等待终态回报的最长排空时间。
    #[arg(long = "drain", default_value = "10s")]
    drain: String,
    #[arg(long = "exchange", alias = "exchange_mock", default_value = "xili")]
    exchange: String,
    #[arg(long = "account", default_value = "loadtest")]
    account: String,
    #[arg(long = "instrument", default_value = "btcusdt")]
    instrument: String,
    #[arg(
        long = "strategy-prefix",
        alias = "strategy_prefix",
        default_value = "lt"
    )]
    strategy_prefix: String,
}

/// 压测过程共享指标。
struct Metrics {
    sent: AtomicU64,
    completed: AtomicU64,
    acked: AtomicU64,
    errors: AtomicU64,
    /// 终态端到端延迟（微秒），回收时排序算分位。
    latencies_us: tokio::sync::Mutex<Vec<u64>>,
}

impl Metrics {
    fn new() -> Self {
        Metrics {
            sent: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            acked: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            latencies_us: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    async fn record_latency(&self, us: u64) {
        self.latencies_us.lock().await.push(us);
    }
}

/// 已发送待配对：cid -> 发出时刻。
type Pending = Arc<DashMap<String, Instant>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let rate = args.rate.max(1);
    let connections = args.connections.max(1);
    let drain = trading_core::timeutil::parse_duration(&args.drain)?;
    let total = if args.total > 0 {
        Some(args.total)
    } else {
        None
    };
    let run_for = match total {
        Some(_) => Duration::from_secs(3600), // 有总数时给很大上限，靠 total 停
        None => trading_core::timeutil::parse_duration(&args.duration)?,
    };

    let secret = if args.insecure {
        String::new()
    } else {
        std::fs::read_to_string(&args.secret_file)?
            .trim()
            .to_string()
    };

    let metrics = Arc::new(Metrics::new());
    let pending: Pending = Arc::new(DashMap::new());

    // 每条连接一个发送通道；生产者按速率整形并 round-robin 派发。
    let mut senders = Vec::with_capacity(connections);
    let mut handles = Vec::with_capacity(connections);
    for i in 0..connections {
        let (tx, rx) = mpsc::channel::<String>(4096);
        senders.push(tx);
        let name = format!("{}-{i}", args.strategy_prefix);
        handles.push(tokio::spawn(connection_task(
            format!("ws://{}/ws", args.south_addr),
            name,
            secret.clone(),
            args.insecure,
            rx,
            metrics.clone(),
            pending.clone(),
        )));
    }

    // —— 生产者：10ms 一个节拍，按 rate 比例突发，channel 提供背压 ——
    let tick = Duration::from_millis(10);
    let per_tick = (rate as f64 * tick.as_secs_f64()).round().max(1.0) as u64;
    let producer = {
        let prod_senders = senders.clone();
        let args_ex = args.clone();
        let metrics = metrics.clone();
        let pending = pending.clone();
        tokio::spawn(async move {
            let start = Instant::now();
            let mut rr: usize = 0;
            let mut produced: u64 = 0;
            let mut timer = tokio::time::interval(tick);
            while start.elapsed() < run_for {
                timer.tick().await;
                for _ in 0..per_tick {
                    if let Some(t) = total {
                        if produced >= t {
                            return produced;
                        }
                    }
                    let cid = new_client_order_id();
                    let cmd = serde_json::json!({
                        "option": "place_order",
                        "strategy_name": format!("{}-{}", args_ex.strategy_prefix, rr % connections),
                        "exchange": args_ex.exchange,
                        "account": args_ex.account,
                        "instrument": args_ex.instrument,
                        "side": "long",
                        "order_type": "market",
                        "price": 80000,
                        "quantity": 1,
                        "instrument_type": "spot",
                        "client_order_id": cid,
                    });
                    let payload = serde_json::to_string(&cmd).expect("serialize command");
                    pending.insert(cid, Instant::now());
                    // 满了就 await 背压（这本身就是系统拐点的表现）。
                    if prod_senders[rr % connections].send(payload).await.is_err() {
                        return produced;
                    }
                    metrics.sent.fetch_add(1, Ordering::Relaxed);
                    produced += 1;
                    rr += 1;
                }
            }
            produced
        })
    };
    drop(senders);

    let produced = producer.await.unwrap_or(0);

    // —— 排空：等待 pending 终态或超时 ——
    let drain_deadline = Instant::now() + drain;
    while !pending.is_empty() && Instant::now() < drain_deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let report = build_report(&args, rate, connections, produced, pending.len(), &metrics).await;
    println!("{report}");

    for h in handles {
        h.abort();
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn connection_task(
    url_base: String,
    name: String,
    secret: String,
    insecure: bool,
    mut rx: mpsc::Receiver<String>,
    metrics: Arc<Metrics>,
    pending: Pending,
) {
    let mut url = format!("{url_base}?strategy_name={name}");
    if !insecure {
        let nonce = southauth::generate_nonce();
        let sig = southauth::sign(&secret, &name, &nonce);
        url.push_str(&format!("&nonce={nonce}&signature={sig}"));
    }
    let (ws, _) = match tokio_tungstenite::connect_async(&url).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[{name}] connect failed: {e}");
            return;
        }
    };
    let (mut sink, mut stream) = ws.split();

    // 写循环：把收到的报文写出。
    let writer = tokio::spawn(async move {
        while let Some(payload) = rx.recv().await {
            if sink.send(Message::Text(payload.into())).await.is_err() {
                break;
            }
        }
    });

    // 读循环：解析回调，按 cid 配对终态。
    while let Some(Ok(Message::Text(text))) = stream.next().await {
        let v: serde_json::Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let kind = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
        let cid = v
            .get("client_order_id")
            .and_then(|c| c.as_str())
            .unwrap_or("");
        if kind == "error" {
            metrics.errors.fetch_add(1, Ordering::Relaxed);
            if !cid.is_empty() {
                if let Some((_, t0)) = pending.remove(cid) {
                    metrics
                        .record_latency(t0.elapsed().as_micros() as u64)
                        .await;
                    metrics.completed.fetch_add(1, Ordering::Relaxed);
                }
            }
            continue;
        }
        if kind == "ack" {
            metrics.acked.fetch_add(1, Ordering::Relaxed);
        }
        let status = v.get("status").and_then(|s| s.as_str()).unwrap_or("");
        let terminal = matches!(status, "FILLED" | "CANCELED" | "REJECTED" | "EXPIRED");
        if terminal && !cid.is_empty() {
            if let Some((_, t0)) = pending.remove(cid) {
                metrics
                    .record_latency(t0.elapsed().as_micros() as u64)
                    .await;
                metrics.completed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    writer.abort();
}

async fn build_report(
    args: &Args,
    rate: u64,
    connections: usize,
    produced: u64,
    pending_left: usize,
    metrics: &Metrics,
) -> String {
    let sent = metrics.sent.load(Ordering::Relaxed);
    let completed = metrics.completed.load(Ordering::Relaxed);
    let acked = metrics.acked.load(Ordering::Relaxed);
    let errors = metrics.errors.load(Ordering::Relaxed);
    let mut lats = metrics.latencies_us.lock().await.clone();
    lats.sort_unstable();
    let n = lats.len() as u64;
    let sum: u64 = lats.iter().sum();
    let avg_ms = if n > 0 {
        sum as f64 / n as f64 / 1000.0
    } else {
        0.0
    };
    let p50 = percentile(&lats, 0.50) as f64 / 1000.0;
    let p95 = percentile(&lats, 0.95) as f64 / 1000.0;
    let p99 = percentile(&lats, 0.99) as f64 / 1000.0;
    let max_ms = lats.last().copied().unwrap_or(0) as f64 / 1000.0;

    serde_json::json!({
        "target_rate_per_s": rate,
        "connections": connections,
        "exchange": args.exchange,
        "produced": produced,
        "sent": sent,
        "acked": acked,
        "completed_terminal": completed,
        "errors": errors,
        "pending_not_completed": pending_left,
        "latency_ms": {"avg": round3(avg_ms), "p50": round3(p50),
                       "p95": round3(p95), "p99": round3(p99), "max": round3(max_ms)},
    })
    .to_string()
}

fn round3(x: f64) -> f64 {
    (x * 1000.0).round() / 1000.0
}

/// 已排序样本上取分位（最近秩法）。空样本返回 0。
fn percentile(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 * q).ceil() as usize).saturating_sub(1);
    sorted[idx.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::percentile;

    #[test]
    fn percentile_nearest_rank() {
        let s: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&s, 0.50), 50);
        assert_eq!(percentile(&s, 0.95), 95);
        assert_eq!(percentile(&s, 0.99), 99);
        assert_eq!(percentile(&s, 1.0), 100);
        assert_eq!(percentile(&[], 0.5), 0);
        assert_eq!(percentile(&[7], 0.99), 7);
    }
}
