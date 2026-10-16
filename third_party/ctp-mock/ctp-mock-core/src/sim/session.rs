//! 会话协议纯逻辑：握手三步状态机、MaxOrderRef 延续、单在途查询闸门、分页。
//!
//! 本模块不含线程/SPI，只做可单测的状态归约；异步时序由 `front` 层驱动：
//! 请求槽先查本模块得到同步受理码（0 / -1 未连接 / -3 在途），再在回调线程
//! 产出对应事件并推进状态。
//!
//! 真实 CTP 受理码：`0` 受理、`-1` 网络未连接/未前置、`-2` 未处理请求、
//! `-3` 流控（上一查询未收齐）。

/// 同步受理码：成功。
pub const RET_OK: i32 = 0;
/// 同步受理码：网络连接未建立（在错误的握手阶段发请求同归此类）。
pub const RET_NOT_CONNECTED: i32 = -1;
/// 同步受理码：流控——已有查询在途未收齐。
pub const RET_QUERY_FLOW_CONTROL: i32 = -3;

/// 交易握手阶段（严格按 连接→认证→登录→结算确认 推进）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Phase {
    /// 尚未连上前置。
    #[default]
    Disconnected,
    /// 已 OnFrontConnected，可发起认证。
    Connected,
    /// 认证已通过，可登录。
    Authenticated,
    /// 登录已通过，可做结算确认。
    LoggedIn,
    /// 结算确认完成，交易就绪。
    Ready,
}

/// 查询类别（用于在途闸门的诊断与终态回收）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QryKind {
    Order,
    Trade,
    Position,
    Instrument,
}

/// 登录回报要素（派生自会话，回灌客户端）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginInfo {
    pub trading_day: String,
    pub front_id: i32,
    pub session_id: i32,
    pub max_order_ref: String,
}

/// 握手状态机 + 跨重连延续的 MaxOrderRef。
#[derive(Debug, Clone)]
pub struct Handshake {
    phase: Phase,
    front_id: i32,
    session_id: i32,
    trading_day: String,
    /// 已用最大报单引用；断线不清、重连登录继续回报该值（MaxOrderRef 延续）。
    max_order_ref: i32,
}

impl Handshake {
    /// 以静态前置/会话编号与交易日构造（初始 Disconnected）。
    pub fn new(front_id: i32, session_id: i32, trading_day: impl Into<String>) -> Self {
        Self {
            phase: Phase::Disconnected,
            front_id,
            session_id,
            trading_day: trading_day.into(),
            max_order_ref: 0,
        }
    }

    /// 当前阶段。
    pub fn phase(&self) -> Phase {
        self.phase
    }

    /// 交易是否就绪（结算确认完成）。
    pub fn is_ready(&self) -> bool {
        self.phase == Phase::Ready
    }

    /// OnFrontConnected：仅在断线态生效（重复连接事件幂等忽略）。
    pub fn on_connected(&mut self) {
        if self.phase == Phase::Disconnected {
            self.phase = Phase::Connected;
        }
    }

    /// 受理认证请求的同步闸门：必须已连接且尚未认证。
    pub fn accept_authenticate(&self) -> Result<(), i32> {
        match self.phase {
            Phase::Connected => Ok(()),
            Phase::Disconnected => Err(RET_NOT_CONNECTED),
            _ => Err(RET_NOT_CONNECTED),
        }
    }

    /// 认证成功回报后推进到 Authenticated。
    pub fn mark_authenticated(&mut self) {
        if self.phase == Phase::Connected {
            self.phase = Phase::Authenticated;
        }
    }

    /// 受理登录请求：必须已认证。
    pub fn accept_login(&self) -> Result<(), i32> {
        match self.phase {
            Phase::Authenticated => Ok(()),
            Phase::Disconnected => Err(RET_NOT_CONNECTED),
            _ => Err(RET_NOT_CONNECTED),
        }
    }

    /// 登录成功回报后推进并产出登录要素。
    pub fn mark_logged_in(&mut self) -> LoginInfo {
        if self.phase == Phase::Authenticated {
            self.phase = Phase::LoggedIn;
        }
        LoginInfo {
            trading_day: self.trading_day.clone(),
            front_id: self.front_id,
            session_id: self.session_id,
            max_order_ref: self.max_order_ref.to_string(),
        }
    }

    /// 受理结算确认：必须已登录。
    pub fn accept_settlement_confirm(&self) -> Result<(), i32> {
        match self.phase {
            Phase::LoggedIn => Ok(()),
            Phase::Disconnected => Err(RET_NOT_CONNECTED),
            _ => Err(RET_NOT_CONNECTED),
        }
    }

    /// 结算确认成功回报后推进到 Ready。
    pub fn mark_settlement_confirmed(&mut self) {
        if self.phase == Phase::LoggedIn {
            self.phase = Phase::Ready;
        }
    }

    /// 断线：回到 Disconnected；**不清** MaxOrderRef（重连延续）。
    pub fn on_disconnected(&mut self) {
        self.phase = Phase::Disconnected;
    }

    /// 记录一笔已受理报单的 OrderRef，抬升 MaxOrderRef 水位。
    pub fn note_order_ref(&mut self, order_ref: i32) {
        if order_ref > self.max_order_ref {
            self.max_order_ref = order_ref;
        }
    }

    /// 当前 MaxOrderRef（登录回报用）。
    pub fn max_order_ref(&self) -> i32 {
        self.max_order_ref
    }
}

/// 单在途查询闸门：同一时刻只允许一个查询，未收齐再发返回 -3。
#[derive(Debug, Default, Clone)]
pub struct QueryGate {
    active: Option<ActiveQuery>,
}

/// 在途查询记录。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveQuery {
    pub kind: QryKind,
    pub request_id: i32,
}

impl QueryGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// 是否有在途查询。
    pub fn is_active(&self) -> bool {
        self.active.is_some()
    }

    /// 尝试开始查询；已有在途时返回流控码 -3。
    pub fn begin(&mut self, kind: QryKind, request_id: i32) -> Result<(), i32> {
        if self.active.is_some() {
            return Err(RET_QUERY_FLOW_CONTROL);
        }
        self.active = Some(ActiveQuery { kind, request_id });
        Ok(())
    }

    /// 收到 is_last 后结束在途查询，归还其记录（终态回收）。
    pub fn end(&mut self) -> Option<ActiveQuery> {
        self.active.take()
    }

    /// 当前在途查询。
    pub fn current(&self) -> Option<ActiveQuery> {
        self.active
    }
}

/// 一帧查询应答：是否携带数据行、是否末帧。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QryFrame {
    pub has_row: bool,
    pub is_last: bool,
}
/// 按 CTP 口径把 N 行数据展开为逐帧描述：每行一帧 `is_last=false`，
/// 末尾再补一帧空数据 `is_last=true`；零行时仅有末帧。
///
/// 上层据 `has_row` 决定是否填结构，据 `is_last` 发 QueryEnd/回收闸门。
pub fn paginate(row_count: usize) -> Vec<QryFrame> {
    let mut frames = Vec::with_capacity(row_count + 1);
    for _ in 0..row_count {
        frames.push(QryFrame {
            has_row: true,
            is_last: false,
        });
    }
    frames.push(QryFrame {
        has_row: false,
        is_last: true,
    });
    frames
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handshake() -> Handshake {
        Handshake::new(7, 5002, "20261015")
    }

    #[test]
    fn handshake_runs_three_steps_in_order_then_ready() {
        let mut h = handshake();
        assert_eq!(h.phase(), Phase::Disconnected);
        assert!(!h.is_ready());

        h.on_connected();
        assert_eq!(h.phase(), Phase::Connected);
        assert!(h.accept_authenticate().is_ok());
        h.mark_authenticated();

        assert!(h.accept_login().is_ok());
        let info = h.mark_logged_in();
        assert_eq!(h.phase(), Phase::LoggedIn);
        assert_eq!(info.front_id, 7);
        assert_eq!(info.session_id, 5002);
        assert_eq!(info.trading_day, "20261015");

        assert!(h.accept_settlement_confirm().is_ok());
        h.mark_settlement_confirmed();
        assert!(h.is_ready());
    }

    #[test]
    fn requests_before_connected_are_rejected() {
        let h = handshake();
        assert_eq!(h.accept_authenticate(), Err(RET_NOT_CONNECTED));
    }

    #[test]
    fn login_before_authenticate_is_rejected() {
        let mut h = handshake();
        h.on_connected();
        // 已连接但未认证：登录被拒。
        assert_eq!(h.accept_login(), Err(RET_NOT_CONNECTED));
    }

    #[test]
    fn settlement_before_login_is_rejected() {
        let mut h = handshake();
        h.on_connected();
        h.mark_authenticated();
        assert_eq!(h.accept_settlement_confirm(), Err(RET_NOT_CONNECTED));
    }

    #[test]
    fn duplicate_connected_event_is_idempotent() {
        let mut h = handshake();
        h.on_connected();
        h.mark_authenticated();
        // 已认证后再来连接事件不应回退阶段。
        h.on_connected();
        assert_eq!(h.phase(), Phase::Authenticated);
    }

    #[test]
    fn max_order_ref_advances_and_survives_disconnect_reconnect() {
        let mut h = handshake();
        h.on_connected();
        h.mark_authenticated();
        h.note_order_ref(12);
        let info = h.mark_logged_in();
        assert_eq!(info.max_order_ref, "12");

        // 断线：回 Disconnected 但 MaxOrderRef 不清。
        h.on_disconnected();
        assert_eq!(h.phase(), Phase::Disconnected);
        assert_eq!(h.max_order_ref(), 12);

        // 重连再登录，MaxOrderRef 延续。
        h.on_connected();
        h.mark_authenticated();
        let info2 = h.mark_logged_in();
        assert_eq!(info2.max_order_ref, "12");
        h.note_order_ref(13);
        assert_eq!(h.max_order_ref(), 13);
    }

    #[test]
    fn query_gate_allows_one_and_rejects_second_with_minus_3() {
        let mut g = QueryGate::new();
        assert!(!g.is_active());
        assert!(g.begin(QryKind::Order, 1).is_ok());
        assert!(g.is_active());

        let err = g.begin(QryKind::Position, 2);
        assert_eq!(err, Err(RET_QUERY_FLOW_CONTROL));

        let done = g.end();
        assert_eq!(
            done,
            Some(ActiveQuery {
                kind: QryKind::Order,
                request_id: 1
            })
        );
        assert!(!g.is_active());
        // 回收后可再次查询。
        assert!(g.begin(QryKind::Position, 3).is_ok());
    }

    #[test]
    fn paginate_rows_each_non_last_then_terminal_last() {
        let frames = paginate(2);
        assert_eq!(frames.len(), 3);
        assert_eq!(
            frames[0],
            QryFrame {
                has_row: true,
                is_last: false
            }
        );
        assert_eq!(
            frames[1],
            QryFrame {
                has_row: true,
                is_last: false
            }
        );
        assert_eq!(
            frames[2],
            QryFrame {
                has_row: false,
                is_last: true
            }
        );
    }

    #[test]
    fn paginate_zero_rows_emits_only_terminal_last() {
        let frames = paginate(0);
        assert_eq!(
            frames,
            vec![QryFrame {
                has_row: false,
                is_last: true
            }]
        );
    }
}
