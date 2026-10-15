//! 高层交易句柄：装配 ctp2rs `TraderApi`、SPI 事件流与会话状态，并提供请求入口。

use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::time::Duration;

use crate::ctp::core::account::CtpAccount;
use crate::ctp::core::encoding::encode_fixed;
use crate::ctp::core::error::{CtpError, Result};
use crate::ctp::core::event::CtpEvent;
use crate::ctp::core::order::OrderRequest;
use ctp2rs::v1alpha1::{TraderApi, THOST_TE_RESUME_TYPE};

use crate::ctp::gateway::convert::order::build_input_order;
use crate::ctp::gateway::session::SessionState;
use crate::ctp::gateway::spi::EventTraderSpi;

/// 把事件应用到会话状态（纯函数式归约，便于单测）。
///
/// 仅登录/断线会改变会话；其余事件（委托、成交等）由上层自行处理，
/// 不污染会话状态。
pub fn apply_event(session: &mut SessionState, event: &CtpEvent) {
    match event {
        CtpEvent::Login(info) => {
            session.on_login(info.front_id, info.session_id, &info.max_order_ref);
        }
        CtpEvent::FrontDisconnected(_) => session.on_disconnect(),
        _ => {}
    }
}

/// 一个连接会话的高层句柄：拥有柜台 API、事件接收端、账户与会话状态。
pub struct TraderHandle {
    /// 柜台交易 API（自带 Drop，关闭时 Release 并卸载动态库）。
    api: TraderApi,
    /// 事件接收端（SPI 在柜台线程推送）。
    events: Receiver<CtpEvent>,
    /// 账户参数（高层请求据此填充）。
    account: CtpAccount,
    /// 会话状态（请求/报单引用、前置/会话编号）。
    session: SessionState,
    /// 自定义 SPI 的裸指针；ctp2rs 不回收内部 `*mut dyn TraderSpi`，需自行回收。
    spi_raw: *mut dyn ctp2rs::v1alpha1::TraderSpi,
}

impl TraderHandle {
    /// 校验账户、打开动态库并完成 Init 前的全部注册，随后初始化连接。
    ///
    /// `is_production_mode`：评测版 API（v6.7.11+）接 SimNow 等仿真环境时必须传
    /// `false`，生产柜台传 `true`（见官方升级说明第 7 条）。
    pub fn connect(
        account: &CtpAccount,
        lib_path: &str,
        flow_path: &str,
        is_production_mode: bool,
    ) -> Result<Self> {
        // 边界校验先于动态库打开。
        account
            .validate()
            .map_err(|e| CtpError::Other(e.to_string()))?;

        let api = TraderApi::create_api(lib_path, flow_path, is_production_mode);

        let (tx, rx): (SyncSender<CtpEvent>, Receiver<CtpEvent>) = sync_channel(1024);
        let spi = EventTraderSpi::new(tx);
        let spi_raw = Box::into_raw(Box::new(spi)) as *mut dyn ctp2rs::v1alpha1::TraderSpi;

        // 注册顺序：SPI → 私有/公共流 → 前置，然后 Init。
        api.register_spi(spi_raw);
        api.subscribe_private_topic(THOST_TE_RESUME_TYPE::THOST_TERT_QUICK, 1);
        api.subscribe_public_topic(THOST_TE_RESUME_TYPE::THOST_TERT_QUICK);
        api.register_front(&account.front_address);
        api.init();

        Ok(Self {
            api,
            events: rx,
            account: account.clone(),
            session: SessionState::new(),
            spi_raw,
        })
    }

    /// 等待下一个事件；超时返回 `None`。同时把登录/断线应用到会话状态。
    pub fn next_event(&mut self, timeout: Duration) -> Option<CtpEvent> {
        match self.events.recv_timeout(timeout) {
            Ok(event) => {
                apply_event(&mut self.session, &event);
                Some(event)
            }
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => None,
        }
    }

    /// 发起客户端认证，返回柜台受理码（0=受理）。
    pub fn authenticate(&mut self) -> i32 {
        let mut req = ctp2rs::v1alpha1::CThostFtdcReqAuthenticateField::default();
        encode_fixed(&mut req.BrokerID, &self.account.broker_id);
        encode_fixed(&mut req.UserID, &self.account.user_id);
        encode_fixed(&mut req.AppID, &self.account.app_id);
        encode_fixed(&mut req.AuthCode, &self.account.auth_code);
        let id = self.session.request_id();
        self.api.req_authenticate(&mut req, id)
    }

    /// 发起用户登录，返回柜台受理码。
    pub fn login(&mut self) -> i32 {
        let mut req = ctp2rs::v1alpha1::CThostFtdcReqUserLoginField::default();
        encode_fixed(&mut req.BrokerID, &self.account.broker_id);
        encode_fixed(&mut req.UserID, &self.account.user_id);
        encode_fixed(&mut req.Password, &self.account.password);
        let id = self.session.request_id();
        self.api.req_user_login(&mut req, id)
    }

    /// 发起结算单确认，返回柜台受理码。
    pub fn confirm_settlement(&mut self) -> i32 {
        let mut req = ctp2rs::v1alpha1::CThostFtdcSettlementInfoConfirmField::default();
        encode_fixed(&mut req.BrokerID, &self.account.broker_id);
        encode_fixed(&mut req.InvestorID, self.account.effective_investor_id());
        let id = self.session.request_id();
        self.api.req_settlement_info_confirm(&mut req, id)
    }

    /// 校验并录入一笔报单，返回柜台受理码。
    pub fn insert_order(&mut self, req: &OrderRequest, order_ref: &str) -> Result<i32> {
        req.validate()?;
        // order_ref 由调用方预分配（cid↔OrderRef 映射的短号，跨会话全局唯一），
        // 柜台原样回传——回报经映射反查还原本地 cid。
        let mut field = build_input_order(req, &self.account, order_ref);
        let id = self.session.request_id();
        Ok(self.api.req_order_insert(&mut field, id))
    }

    /// 发起全量合约查询（保留空字段即查全部），返回柜台受理码。
    ///
    /// 查询流控 1 秒/次；结果经 `CtpEvent::Instrument` 逐条返回。
    pub fn query_instruments(&mut self) -> i32 {
        let mut req = ctp2rs::v1alpha1::CThostFtdcQryInstrumentField::default();
        let id = self.session.request_id();
        self.api.req_qry_instrument(&mut req, id)
    }

    /// 发起持仓查询，返回柜台受理码。
    ///
    /// 结果经 `CtpEvent::Position` 逐条返回（含数量为 0 的行，上层自行过滤）。
    pub fn query_positions(&mut self) -> i32 {
        let mut req = ctp2rs::v1alpha1::CThostFtdcQryInvestorPositionField::default();
        encode_fixed(&mut req.BrokerID, &self.account.broker_id);
        encode_fixed(&mut req.InvestorID, self.account.effective_investor_id());
        let id = self.session.request_id();
        self.api.req_qry_investor_position(&mut req, id)
    }

    /// 发起当日成交查询，返回柜台受理码。
    ///
    /// 结果经 `CtpEvent::Trade` 逐条返回，`CtpEvent::QueryEnd` 表示收齐。
    pub fn query_trades(&mut self) -> i32 {
        let mut req = ctp2rs::v1alpha1::CThostFtdcQryTradeField::default();
        encode_fixed(&mut req.BrokerID, &self.account.broker_id);
        encode_fixed(&mut req.InvestorID, self.account.effective_investor_id());
        let id = self.session.request_id();
        self.api.req_qry_trade(&mut req, id)
    }

    /// 发起当日报单查询，返回柜台受理码。
    ///
    /// `instrument` 非空时只查该合约；结果经 `CtpEvent::Order` 逐条返回，
    /// `CtpEvent::QueryEnd` 表示全部记录已收齐（不再依赖固定超时）。
    pub fn query_orders(&mut self, instrument: &str) -> i32 {
        let mut req = ctp2rs::v1alpha1::CThostFtdcQryOrderField::default();
        encode_fixed(&mut req.BrokerID, &self.account.broker_id);
        encode_fixed(&mut req.InvestorID, self.account.effective_investor_id());
        if !instrument.trim().is_empty() {
            encode_fixed(&mut req.InstrumentID, instrument);
        }
        let id = self.session.request_id();
        self.api.req_qry_order(&mut req, id)
    }

    /// 发起撤单，返回柜台受理码（0=受理；非 0 如 24/25/26 见错误表）。
    ///
    /// 受理码 0 只代表请求被接收，真正撤成以 `on_rtn_order` 撤单终态为准；
    /// 异步拒绝经 `on_err_rtn_order_action`（`CtpEvent::Error`）回灌。
    /// 定位二选一与字段装配见 [`crate::ctp::gateway::convert::order::build_order_action`]。
    pub fn cancel_order(
        &mut self,
        instrument: &str,
        target: &crate::ctp::mailbox::CancelTarget,
    ) -> i32 {
        let action_ref = self.session.order_action_ref();
        let mut field = crate::ctp::gateway::convert::order::build_order_action(
            &self.account,
            instrument,
            target,
            action_ref,
        );
        let id = self.session.request_id();
        self.api.req_order_action(&mut field, id)
    }

    /// 当前会话前置编号（登录后填充；跨会话撤单定位三元组时由上层取用）。
    pub fn front_id(&self) -> i32 {
        self.session.front_id()
    }

    /// 当前会话编号。
    pub fn session_id(&self) -> i32 {
        self.session.session_id()
    }

    /// 显式释放底层连接。
    pub fn release(&self) {
        self.api.release();
    }
}

impl Drop for TraderHandle {
    fn drop(&mut self) {
        // SAFETY: `spi_raw` 由 `connect()` 中 `Box::into_raw` 创建，且仅在本
        // Drop 中回收一次（TraderHandle 不可 Clone/Copy，Drop 至多执行一次）。
        // 回收前先 `api.release()`：CTP 内部线程已退出、不会再触碰 SPI，
        // 避免释放后柜台回调访问悬垂指针（与 ctp2rs 官方示例的释放顺序一致）。
        self.api.release();
        unsafe {
            let _ = Box::from_raw(self.spi_raw);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{apply_event, TraderHandle};
    use crate::ctp::core::account::CtpAccount;
    use crate::ctp::core::event::{CtpEvent, LoginInfo};
    use crate::ctp::gateway::session::SessionState;

    fn invalid_account() -> CtpAccount {
        CtpAccount {
            broker_id: String::new(),
            user_id: "u".into(),
            password: "p".into(),
            app_id: "a".into(),
            auth_code: "c".into(),
            investor_id: String::new(),
            front_address: "tcp://x:1".into(),
            is_production_mode: false,
        }
    }

    #[test]
    fn connect_rejects_invalid_account_before_opening_library() {
        // 账户校验先于动态库打开：即使给假路径也应返回账户错误而非 panic。
        let r = TraderHandle::connect(&invalid_account(), "/nonexistent/x.so", "./td_", true);
        assert!(r.is_err());
    }

    #[test]
    fn login_event_advances_session_and_records_ids() {
        let mut session = SessionState::new();
        let ev = CtpEvent::Login(LoginInfo {
            trading_day: "20261009".into(),
            front_id: 5,
            session_id: 7001,
            max_order_ref: "20".into(),
        });
        apply_event(&mut session, &ev);
        assert!(session.is_logged_in());
        assert_eq!(session.front_id(), 5);
        assert_eq!(session.session_id(), 7001);
        // 下一 OrderRef 对齐到 MaxOrderRef 之后。
        assert_eq!(session.order_ref(), "21");
    }

    #[test]
    fn disconnect_event_clears_login_flag() {
        let mut session = SessionState::new();
        apply_event(
            &mut session,
            &CtpEvent::Login(LoginInfo {
                trading_day: "20261009".into(),
                front_id: 1,
                session_id: 1,
                max_order_ref: "1".into(),
            }),
        );
        apply_event(&mut session, &CtpEvent::FrontDisconnected(0x1001));
        assert!(!session.is_logged_in());
    }

    #[test]
    fn unrelated_events_leave_session_unchanged() {
        let mut session = SessionState::new();
        apply_event(&mut session, &CtpEvent::FrontConnected);
        assert!(!session.is_logged_in());
        assert_eq!(session.order_ref(), "1");
    }
}
