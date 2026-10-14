//! 会话状态：请求编号、报单引用、前置/会话编号。

/// 单个连接会话内的可变状态。
///
/// CTP 要求请求/报单引用在会话内单调递增；登录成功后 OrderRef 应以柜台
/// 返回的 `MaxOrderRef` 为基准继续递增，避免与既有报单引用冲突。
#[derive(Debug, Default)]
pub struct SessionState {
    /// 下一请求编号（从 1 起）。
    next_request_id: i32,
    /// 下一报单引用数值（登录前从 1 起，登录后对齐 MaxOrderRef）。
    next_order_ref: i32,
    /// 前置编号（登录后填充）。
    front_id: i32,
    /// 会话编号（登录后填充）。
    session_id: i32,
    /// 是否已登录。
    logged_in: bool,
}

impl SessionState {
    /// 构造初始会话：请求/报单引用均从 1 起。
    pub fn new() -> Self {
        Self {
            next_request_id: 1,
            next_order_ref: 1,
            ..Default::default()
        }
    }

    /// 取下一请求编号并自增。
    pub fn request_id(&mut self) -> i32 {
        let id = self.next_request_id;
        self.next_request_id += 1;
        id
    }

    /// 取下一报单引用（字符串形态）并自增。
    pub fn order_ref(&mut self) -> String {
        let r#ref = self.next_order_ref;
        self.next_order_ref += 1;
        r#ref.to_string()
    }

    /// 登录成功：记录会话要素，并把下一 OrderRef 对齐到 MaxOrderRef 之后。
    pub fn on_login(&mut self, front_id: i32, session_id: i32, max_order_ref: &str) {
        self.front_id = front_id;
        self.session_id = session_id;
        self.logged_in = true;
        // 已用最大引用之后继续；非数字按 0 处理，从 1 起。
        let max = max_order_ref.trim().parse::<i32>().unwrap_or(0);
        self.next_order_ref = max + 1;
    }

    /// 连接断开：清除登录标志；下一连接重新登录后会再次对齐。
    pub fn on_disconnect(&mut self) {
        self.logged_in = false;
    }

    /// 是否已登录。
    pub fn is_logged_in(&self) -> bool {
        self.logged_in
    }

    /// 前置编号。
    pub fn front_id(&self) -> i32 {
        self.front_id
    }

    /// 会话编号。
    pub fn session_id(&self) -> i32 {
        self.session_id
    }
}

#[cfg(test)]
mod tests {
    use super::SessionState;

    #[test]
    fn request_id_increments_from_one() {
        let mut s = SessionState::new();
        assert_eq!(s.request_id(), 1);
        assert_eq!(s.request_id(), 2);
        assert_eq!(s.request_id(), 3);
    }

    #[test]
    fn order_ref_starts_at_one_before_login() {
        let mut s = SessionState::new();
        assert_eq!(s.order_ref(), "1");
        assert_eq!(s.order_ref(), "2");
    }

    #[test]
    fn order_ref_continues_after_max_order_ref_on_login() {
        let mut s = SessionState::new();
        s.on_login(1, 9001, "12");
        // 后续 OrderRef 从柜台 MaxOrderRef 之后递增。
        assert_eq!(s.order_ref(), "13");
        assert_eq!(s.order_ref(), "14");
    }

    #[test]
    fn login_records_front_and_session_and_flag() {
        let mut s = SessionState::new();
        assert!(!s.is_logged_in());
        s.on_login(7, 5002, "3");
        assert!(s.is_logged_in());
        assert_eq!(s.front_id(), 7);
        assert_eq!(s.session_id(), 5002);
    }

    #[test]
    fn non_numeric_max_order_ref_is_treated_as_zero() {
        let mut s = SessionState::new();
        s.on_login(1, 1, "");
        assert_eq!(s.order_ref(), "1");
    }

    #[test]
    fn reset_on_disconnect_clears_login_flag() {
        let mut s = SessionState::new();
        s.on_login(1, 1, "5");
        s.on_disconnect();
        assert!(!s.is_logged_in());
    }
}
