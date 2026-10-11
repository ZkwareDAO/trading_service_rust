//! WS 重连退避阶梯（跨交易所/连接复用的纯计时内核）。
//!
//! 节奏（见 `docs/扩展交易所.md` 第五节）：
//! * 进程启动后的**首次连接立即**尝试（不等待）；
//! * 连接失败或断线后：**10s → 30s → 1min → 3min → 5min**，之后固定 5min；
//! * 一旦成功建立连接，阶梯归零——下次断线又从 10s 重新开始。
//!
//! 本类型只决定"等多久"，不真正 sleep（便于确定性单测）；调用方在下次连接前
//! `tokio::time::sleep(backoff.next_wait())` 即可。

use std::time::Duration;

/// 断线/失败后的等待阶梯；最后一档之后封顶重复。
const DELAYS: [Duration; 5] = [
    Duration::from_secs(10),
    Duration::from_secs(30),
    Duration::from_secs(60),
    Duration::from_secs(3 * 60),
    Duration::from_secs(5 * 60),
];

#[derive(Debug, Clone)]
pub struct ReconnectBackoff {
    /// 下一档索引。
    step: usize,
    /// 下次连接前要等待的时长；None=首次启动立即连接。
    pending: Option<Duration>,
}

impl Default for ReconnectBackoff {
    fn default() -> Self {
        Self::new()
    }
}

impl ReconnectBackoff {
    /// 启动态：首次连接立即。
    pub fn new() -> Self {
        Self {
            step: 0,
            pending: None,
        }
    }

    /// 取出下次连接前的等待时长并消费；None 表示立即连接。
    pub fn next_wait(&mut self) -> Option<Duration> {
        self.pending.take()
    }

    /// 记录一次连接失败或连接已断开：排程下一档等待。
    pub fn note_retry(&mut self) {
        let delay = DELAYS[self.step.min(DELAYS.len() - 1)];
        if self.step < DELAYS.len() - 1 {
            self.step += 1;
        }
        self.pending = Some(delay);
    }

    /// 连接成功建立后归零：下次断线重新从 10s 起。
    pub fn reset(&mut self) {
        self.step = 0;
        self.pending = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_connect_is_immediate() {
        let mut b = ReconnectBackoff::new();
        assert_eq!(b.next_wait(), None);
    }

    #[test]
    fn follows_plan_schedule_and_caps_at_five_minutes() {
        let mut b = ReconnectBackoff::new();
        let mut waits = Vec::new();
        for _ in 0..7 {
            b.note_retry();
            waits.push(b.next_wait().unwrap());
        }
        assert_eq!(
            waits,
            vec![
                Duration::from_secs(10),
                Duration::from_secs(30),
                Duration::from_secs(60),
                Duration::from_secs(180),
                Duration::from_secs(300),
                Duration::from_secs(300), // 封顶
                Duration::from_secs(300),
            ]
        );
    }

    #[test]
    fn wait_is_consumed_once() {
        let mut b = ReconnectBackoff::new();
        b.note_retry();
        assert_eq!(b.next_wait(), Some(Duration::from_secs(10)));
        assert_eq!(b.next_wait(), None);
    }

    #[test]
    fn successful_connect_restarts_schedule_at_ten_seconds() {
        let mut b = ReconnectBackoff::new();
        b.note_retry();
        b.note_retry(); // 已爬到 30s 档
        assert_eq!(b.next_wait(), Some(Duration::from_secs(30)));

        b.reset(); // 连接成功
        assert_eq!(b.next_wait(), None);

        // 之后再次断线，从 10s 重新开始。
        b.note_retry();
        assert_eq!(b.next_wait(), Some(Duration::from_secs(10)));
    }

    // ----- 真实链路：交易所长时间不可用→重连持续失败→恢复后续传（跨所通用） -----
    //
    // 复现 go 历史 bug：重连若最终"放弃"，WS 数据永久丢失。这里用真实本地
    // TCP/WS 服务器 + 生产建连原语 `crate::ws::connect` 验证"失败/中断→恢复"。
    // 退避等待在测试中缩短；生产阶梯（10s→…→5min 封顶、永不终止）由上面单测钉死。

    use crate::ws;
    use futures_util::{SinkExt, StreamExt};
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::Message;

    /// 场景一：建连持续失败（维护）→ 恢复后自动重连收到数据。
    #[tokio::test]
    async fn reconnects_resume_after_prolonged_outage() {
        use std::sync::atomic::Ordering;
        let maintenance = Arc::new(AtomicBool::new(true));
        let failures = Arc::new(AtomicUsize::new(0));

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().unwrap().port();
        let url = format!("ws://127.0.0.1:{port}");

        let server = tokio::spawn({
            let maintenance = Arc::clone(&maintenance);
            async move {
                loop {
                    let (tcp, _) = match listener.accept().await {
                        Ok(c) => c,
                        Err(_) => continue,
                    };
                    if maintenance.load(Ordering::SeqCst) {
                        drop(tcp); // 维护：接受即断，WS 握手失败
                        continue;
                    }
                    let mut wsx = match tokio_tungstenite::accept_async(tcp).await {
                        Ok(w) => w,
                        Err(_) => continue,
                    };
                    let _ = wsx.send(Message::Text("market-back".into())).await;
                    while let Some(Ok(_m)) = wsx.next().await {}
                }
            }
        });

        let client = tokio::spawn({
            let url = url.clone();
            let failures = Arc::clone(&failures);
            async move {
                let start = tokio::time::Instant::now();
                loop {
                    match ws::connect(&url).await {
                        Ok(mut stream) => {
                            if let Some(Ok(Message::Text(txt))) = stream.next().await {
                                assert_eq!(txt, "market-back");
                                return true;
                            }
                        }
                        Err(_) => {
                            failures.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                    }
                    if start.elapsed() > Duration::from_secs(5) {
                        return false;
                    }
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            failures.load(Ordering::SeqCst) >= 5,
            "维护期应经历多次连续失败"
        );

        maintenance.store(false, Ordering::SeqCst);
        let resumed = tokio::time::timeout(Duration::from_secs(3), client)
            .await
            .expect("恢复后未及时返回（疑似放弃重连）")
            .expect("join 失败");
        assert!(resumed, "恢复后未能重新收到数据");
        server.abort();
    }

    /// 场景二：已连接收过数据后，运行中途被关闭（维护）→ 重连失败 → 恢复后续传。
    #[tokio::test]
    async fn reconnects_after_mid_session_disconnect() {
        use std::sync::atomic::Ordering;
        // 0=健康(收帧即断) → 1=维护(秒断) → 2=恢复(收帧)
        let phase = Arc::new(AtomicUsize::new(0));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().unwrap().port();
        let url = format!("ws://127.0.0.1:{port}");

        let server = tokio::spawn({
            let phase = Arc::clone(&phase);
            async move {
                loop {
                    let (tcp, _) = match listener.accept().await {
                        Ok(c) => c,
                        Err(_) => continue,
                    };
                    match phase.load(Ordering::SeqCst) {
                        1 => drop(tcp),
                        p => {
                            let mut wsx = match tokio_tungstenite::accept_async(tcp).await {
                                Ok(w) => w,
                                Err(_) => continue,
                            };
                            let frame = if p == 0 { "first" } else { "second" };
                            let _ = wsx.send(Message::Text(frame.into())).await;
                            if p == 0 {
                                tokio::time::sleep(Duration::from_millis(20)).await;
                                let _ = wsx.close(None).await;
                            } else {
                                while let Some(Ok(_m)) = wsx.next().await {}
                            }
                        }
                    }
                }
            }
        });

        let client = tokio::spawn({
            let url = url.clone();
            let phase = Arc::clone(&phase);
            async move {
                let got_first = Arc::new(AtomicBool::new(false));
                let start = tokio::time::Instant::now();
                loop {
                    if let Ok(mut s) = ws::connect(&url).await {
                        if let Some(Ok(Message::Text(t))) = s.next().await {
                            assert_eq!(t, "first");
                            got_first.store(true, Ordering::SeqCst);
                            break;
                        }
                    }
                    if start.elapsed() > Duration::from_secs(3) {
                        return false;
                    }
                }
                phase.store(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(150)).await;
                phase.store(2, Ordering::SeqCst);
                let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
                loop {
                    if let Ok(mut s) = ws::connect(&url).await {
                        if let Some(Ok(Message::Text(t))) = s.next().await {
                            assert_eq!(t, "second");
                            return got_first.load(Ordering::SeqCst);
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    if tokio::time::Instant::now() > deadline {
                        return false;
                    }
                }
            }
        });

        let ok = tokio::time::timeout(Duration::from_secs(5), client)
            .await
            .expect("客户端超时（疑似停止重连）")
            .expect("join 失败");
        assert!(ok, "中断恢复后未能二次收到数据");
        server.abort();
    }
}
