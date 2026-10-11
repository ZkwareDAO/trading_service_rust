//! 外向 WebSocket 应用层保活：主动 Ping + 空闲读超时的**纯计时状态机**。
//!
//! 背景：TCP 半开（NAT 表项超时、对端崩溃无 FIN）时裸 `ws.next().await` 会永久挂起，
//! 重连逻辑永远没机会触发。客户端周期性主动 Ping（Binance 与本地模拟所 xili 都会回 Pong），
//! 配合「距上次收到任何帧」的空闲上限，即可把死连在 idle 上限内判定并交给外层既有的
//! 退避重连。
//!
//! 本类型不含 async/网络：由调用方推进时长、据 [`HeartbeatAction`] 真正发 Ping 或断线，
//! 因而可用 [`Duration`] 确定性单测，不睡眠。

use std::time::Duration;

/// 默认主动 Ping 间隔；各交易所（含本地模拟所 xili）均会回应客户端 Ping。
pub const DEFAULT_PING_INTERVAL: Duration = Duration::from_secs(20);
/// 默认空闲上限：超过该时长未收到任何帧（数据或 Pong）即判定死连。
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// 推进一个时间片后的动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatAction {
    /// 未到任何时点，继续等。
    Wait,
    /// 已连续空闲一个 ping 间隔：发送一帧 Ping（之后等待 Pong，不重复发）。
    Ping,
    /// 已连续空闲超过 idle 上限且 Pong 未归：判定死连，外层应断开重连。
    Timeout,
}

/// 保活状态机：记录「距上次入站帧」的累计空闲与是否在等 Pong。
#[derive(Debug, Clone)]
pub struct Heartbeat {
    ping_interval: Duration,
    idle_timeout: Duration,
    idle: Duration,
    awaiting_pong: bool,
}

impl Heartbeat {
    pub fn new(ping_interval: Duration, idle_timeout: Duration) -> Self {
        Self {
            ping_interval,
            idle_timeout,
            idle: Duration::ZERO,
            awaiting_pong: false,
        }
    }

    /// 默认参数（20s ping / 60s 超时）的新连接保活器。
    pub fn new_default() -> Self {
        Self::new(DEFAULT_PING_INTERVAL, DEFAULT_IDLE_TIMEOUT)
    }

    /// 推进一个时间片（本次睡眠/等待的时长）并返回该做的动作。
    pub fn advance(&mut self, elapsed: Duration) -> HeartbeatAction {
        self.idle = self.idle.saturating_add(elapsed);
        // 空闲已超上限：无论 Pong 是否在途，判死连。
        if self.idle >= self.idle_timeout {
            return HeartbeatAction::Timeout;
        }
        // 未在等 Pong 且到了 ping 间隔：发一帧 Ping，之后只等 Pong/超时。
        if !self.awaiting_pong && self.idle >= self.ping_interval {
            self.awaiting_pong = true;
            return HeartbeatAction::Ping;
        }
        HeartbeatAction::Wait
    }

    /// 调用方推进到下一个决策点前应睡眠的时长（到点后调 [`Self::advance`]）。
    pub fn deadline(&self) -> Duration {
        let target = if self.awaiting_pong {
            self.idle_timeout
        } else {
            self.ping_interval
        };
        target.saturating_sub(self.idle)
    }

    /// 收到任意入站帧（数据 / Pong / 服务端 Ping）：连接确认存活，空闲与等 Pong 复位。
    pub fn on_frame(&mut self) {
        self.idle = Duration::ZERO;
        self.awaiting_pong = false;
    }

    /// 连接重建后复位（新一轮计时）。
    pub fn reset(&mut self) {
        self.idle = Duration::ZERO;
        self.awaiting_pong = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hb() -> Heartbeat {
        Heartbeat::new(Duration::from_secs(20), Duration::from_secs(60))
    }

    #[test]
    fn first_deadline_is_the_ping_interval() {
        let h = hb();
        assert_eq!(h.deadline(), Duration::from_secs(20));
    }

    #[test]
    fn advance_below_ping_interval_waits() {
        let mut h = hb();
        assert_eq!(h.advance(Duration::from_secs(5)), HeartbeatAction::Wait);
        assert_eq!(h.deadline(), Duration::from_secs(15));
    }

    #[test]
    fn reaching_ping_interval_requests_one_ping_then_waits_for_timeout() {
        let mut h = hb();
        assert_eq!(h.advance(Duration::from_secs(20)), HeartbeatAction::Ping);
        // 已发 Ping 等 Pong：下一时点直接是 idle 上限，期间不再重复 Ping。
        assert_eq!(h.deadline(), Duration::from_secs(40));
        assert_eq!(h.advance(Duration::from_secs(30)), HeartbeatAction::Wait);
    }

    #[test]
    fn pong_or_any_frame_after_ping_resets_and_allows_ping_again() {
        let mut h = hb();
        assert_eq!(h.advance(Duration::from_secs(20)), HeartbeatAction::Ping);
        h.on_frame(); // Pong 回来
        assert_eq!(h.deadline(), Duration::from_secs(20));
        assert_eq!(h.advance(Duration::from_secs(20)), HeartbeatAction::Ping);
    }

    #[test]
    fn silence_past_idle_timeout_times_out() {
        let mut h = hb();
        // 20s 发 Ping，之后继续静默，到 60s 仍无任何帧 → 死连。
        assert_eq!(h.advance(Duration::from_secs(20)), HeartbeatAction::Ping);
        assert_eq!(h.advance(Duration::from_secs(40)), HeartbeatAction::Timeout);
    }

    #[test]
    fn frame_before_timeout_keeps_connection_alive() {
        let mut h = hb();
        assert_eq!(h.advance(Duration::from_secs(20)), HeartbeatAction::Ping);
        assert_eq!(h.advance(Duration::from_secs(30)), HeartbeatAction::Wait);
        h.on_frame(); // 第 50s 收到数据，Ping/超时计时全部重新开始
        assert_eq!(h.deadline(), Duration::from_secs(20));
        assert_eq!(h.advance(Duration::from_secs(20)), HeartbeatAction::Ping);
        h.on_frame(); // Pong 回来
                      // 此后再持续静默 60s（Ping 无回应）才判死连。
        assert_eq!(h.advance(Duration::from_secs(60)), HeartbeatAction::Timeout);
    }

    #[test]
    fn busy_connection_with_frequent_frames_never_pings_or_times_out() {
        let mut h = hb();
        for _ in 0..100 {
            assert_eq!(h.advance(Duration::from_secs(10)), HeartbeatAction::Wait);
            h.on_frame(); // 行情流每秒有帧
        }
    }

    #[test]
    fn reset_clears_idle_and_pending_pong() {
        let mut h = hb();
        assert_eq!(h.advance(Duration::from_secs(20)), HeartbeatAction::Ping);
        h.reset();
        assert_eq!(h.deadline(), Duration::from_secs(20));
        assert_eq!(h.advance(Duration::from_secs(20)), HeartbeatAction::Ping);
    }
}
