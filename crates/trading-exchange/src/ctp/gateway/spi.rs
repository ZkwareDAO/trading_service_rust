//! ctp2rs `TraderSpi` → `CtpEvent` 适配器。
//!
//! 在柜台回调线程内把原生结构即时拷贝为 owned 事件，经同步通道交给主循环。
//! 主循环拥有 [`crate::session::SessionState`] 并据此驱动流程，本结构只做转发，
//! 不持有任何会话状态（职责单一）。接收方关闭时静默丢弃，避免回调线程 panic。

use std::sync::mpsc::SyncSender;

use crate::ctp::core::encoding::decode_fixed;
use crate::ctp::core::event::{CtpEvent, InstrumentInfo, LoginInfo};
use crate::ctp::core::types::{Direction, Offset};
use ctp2rs::v1alpha1::{CThostFtdcRspInfoField, CThostFtdcRspUserLoginField, TraderSpi};

use crate::ctp::gateway::convert::snapshot::{
    order_snapshot, position_snapshot, trade_snapshot, trading_account_snapshot,
};

/// 把回调映射为 [`CtpEvent`] 并转发的 SPI 适配器。
pub struct EventTraderSpi {
    tx: SyncSender<CtpEvent>,
}

impl EventTraderSpi {
    /// 用事件出口构造。
    pub fn new(tx: SyncSender<CtpEvent>) -> Self {
        Self { tx }
    }

    /// 推送一个事件；接收方已关闭时忽略（仅发生在关停阶段）。
    fn emit(&self, event: CtpEvent) {
        let _ = self.tx.send(event);
    }
}

/// 读取 RspInfo 的错误码与信息；缺省视为无错误。
fn read_rsp(info: Option<&CThostFtdcRspInfoField>) -> (i32, String) {
    info.map(|i| (i.ErrorID, decode_fixed(&i.ErrorMsg)))
        .unwrap_or((0, String::new()))
}

/// 报单录入错误帧 → [`CtpEvent::OrderRejected`]（订单须收敛，不能只记错误）。
///
/// 字段口径与 `order_snapshot` 一致：方向未知回退 Buy、开平未知为 None
/// （宁缺勿错，不臆造开平方向）。
fn rejected_event(
    input: &ctp2rs::v1alpha1::CThostFtdcInputOrderField,
    code: i32,
    msg: String,
) -> CtpEvent {
    CtpEvent::OrderRejected {
        order_ref: decode_fixed(&input.OrderRef),
        instrument: decode_fixed(&input.InstrumentID),
        direction: Direction::from_code(input.Direction as u8 as char).unwrap_or(Direction::Buy),
        offset: Offset::from_code(input.CombOffsetFlag[0] as u8 as char),
        volume: input.VolumeTotalOriginal,
        code,
        msg,
    }
}

impl EventTraderSpi {
    /// 错误码非 0 时上抛错误事件；返回是否上抛。
    fn emit_error_if_any(&self, info: Option<&CThostFtdcRspInfoField>) -> bool {
        let (code, msg) = read_rsp(info);
        if code != 0 {
            self.emit(CtpEvent::Error { code, msg });
            true
        } else {
            false
        }
    }

    /// 报单录入错误帧的统一出口：有 `input` 且 `code != 0` 时发 [`CtpEvent::OrderRejected`]
    /// （订单可收敛）；否则退回 [`CtpEvent::Error`]（无 OrderRef，仅可诊断）。
    fn emit_order_insert_error(
        &self,
        input: Option<&ctp2rs::v1alpha1::CThostFtdcInputOrderField>,
        code: i32,
        msg: String,
    ) {
        match input {
            Some(i) if code != 0 => self.emit(rejected_event(i, code, msg)),
            _ => self.emit(CtpEvent::Error { code, msg }),
        }
    }
}

impl TraderSpi for EventTraderSpi {
    fn on_front_connected(&mut self) {
        self.emit(CtpEvent::FrontConnected);
    }

    fn on_front_disconnected(&mut self, reason: i32) {
        self.emit(CtpEvent::FrontDisconnected(reason));
    }

    fn on_rsp_authenticate(
        &mut self,
        _field: Option<&ctp2rs::v1alpha1::CThostFtdcRspAuthenticateField>,
        info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        _is_last: bool,
    ) {
        let (code, msg) = read_rsp(info);
        self.emit(CtpEvent::Authenticated { code, msg });
    }

    fn on_rsp_user_login(
        &mut self,
        login: Option<&CThostFtdcRspUserLoginField>,
        info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        _is_last: bool,
    ) {
        if let Some(l) = login {
            self.emit(CtpEvent::Login(LoginInfo {
                trading_day: decode_fixed(&l.TradingDay),
                front_id: l.FrontID,
                session_id: l.SessionID,
                max_order_ref: decode_fixed(&l.MaxOrderRef),
            }));
        }
        // 登录被拒（无登录字段且有错误码）时以错误事件上抛，便于主循环中止。
        self.emit_error_if_any(info);
    }

    fn on_rsp_settlement_info_confirm(
        &mut self,
        _field: Option<&ctp2rs::v1alpha1::CThostFtdcSettlementInfoConfirmField>,
        info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        is_last: bool,
    ) {
        if self.emit_error_if_any(info) {
            return;
        }
        if is_last {
            self.emit(CtpEvent::SettlementConfirmed);
        }
    }

    fn on_rsp_order_insert(
        &mut self,
        input: Option<&ctp2rs::v1alpha1::CThostFtdcInputOrderField>,
        info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        _is_last: bool,
    ) {
        // 报单录入被拒；受理成功时委托状态经 on_rtn_order 推送。
        // 插入错误帧后柜台不会再发 OnRtnOrder，只看错误帧会永久卡 NEW——
        // 有 input 时据此收敛终态。
        let (code, msg) = read_rsp(info);
        if code != 0 {
            self.emit_order_insert_error(input, code, msg);
        }
    }

    fn on_rtn_order(&mut self, order: Option<&ctp2rs::v1alpha1::CThostFtdcOrderField>) {
        if let Some(o) = order {
            self.emit(CtpEvent::Order(order_snapshot(o)));
        }
    }

    fn on_rtn_trade(&mut self, trade: Option<&ctp2rs::v1alpha1::CThostFtdcTradeField>) {
        if let Some(t) = trade {
            self.emit(CtpEvent::Trade(trade_snapshot(t)));
        }
    }

    fn on_rsp_qry_instrument(
        &mut self,
        instrument: Option<&ctp2rs::v1alpha1::CThostFtdcInstrumentField>,
        _info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        is_last: bool,
    ) {
        if let Some(i) = instrument {
            self.emit(CtpEvent::Instrument(InstrumentInfo {
                instrument: decode_fixed(&i.InstrumentID),
                exchange_id: decode_fixed(&i.ExchangeID),
                product_id: decode_fixed(&i.ProductID),
                is_trading: i.IsTrading != 0,
                volume_multiple: i.VolumeMultiple,
                // 最小变动价位（SHFE 市价转穿价限价取整用）；柜台缺省 0.0 时预检判未就绪。
                price_tick: i.PriceTick,
                // 单笔报单上限（按合约由柜台下发；0=未知，拆单层不拆）。
                max_limit_volume: i.MaxLimitOrderVolume,
                max_market_volume: i.MaxMarketOrderVolume,
                // 多/空保证金率（合约级静态）：持仓回报 UseMargin=0 时按成本×比率兜底。
                long_margin_ratio: i.LongMarginRatio,
                short_margin_ratio: i.ShortMarginRatio,
            }));
        }
        // 与持仓/委托查询一致：分页末条须发 QueryEnd，否则合约预热闸门永不释放。
        if is_last {
            self.emit(CtpEvent::QueryEnd);
        }
    }

    fn on_rsp_qry_investor_position(
        &mut self,
        position: Option<&ctp2rs::v1alpha1::CThostFtdcInvestorPositionField>,
        _info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        is_last: bool,
    ) {
        if let Some(p) = position {
            self.emit(CtpEvent::Position(position_snapshot(p)));
        }
        if is_last {
            self.emit(CtpEvent::QueryEnd);
        }
    }

    fn on_rsp_qry_trading_account(
        &mut self,
        account: Option<&ctp2rs::v1alpha1::CThostFtdcTradingAccountField>,
        info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        is_last: bool,
    ) {
        // 查询被拒时先保留柜台错误码（CtpEvent::Error 供诊断）；oneshot 仍由随后的
        // QueryEnd 收敛为 Err，二者职责分开、不互相替代。
        self.emit_error_if_any(info);
        // 与持仓/委托查询一致：逐条转发生成数据事件，末条发 QueryEnd 释放闸门。
        if let Some(a) = account {
            self.emit(CtpEvent::TradingAccount(trading_account_snapshot(a)));
        }
        if is_last {
            self.emit(CtpEvent::QueryEnd);
        }
    }

    fn on_rsp_qry_order(
        &mut self,
        order: Option<&ctp2rs::v1alpha1::CThostFtdcOrderField>,
        _info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        is_last: bool,
    ) {
        // 查询应答行（非实时回报）：发 OrderQuery 变体，供前台 oneshot 缓冲或
        // 后台重放走订单桥——与 on_rtn_order 的实时 Order 严格区分，避免互相污染。
        if let Some(o) = order {
            self.emit(CtpEvent::OrderQuery(order_snapshot(o)));
        }
        if is_last {
            self.emit(CtpEvent::QueryEnd);
        }
    }

    fn on_rsp_order_action(
        &mut self,
        _action: Option<&ctp2rs::v1alpha1::CThostFtdcInputOrderActionField>,
        info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        _is_last: bool,
    ) {
        // 撤单请求的同步应答：受理码 0 只代表"请求被受理"，真正撤成以
        // on_rtn_order 的撤单终态为准；非 0（如 24 重复撤单）此处上抛可诊断。
        self.emit_error_if_any(info);
    }

    fn on_err_rtn_order_action(
        &mut self,
        _action: Option<&ctp2rs::v1alpha1::CThostFtdcOrderActionField>,
        info: Option<&CThostFtdcRspInfoField>,
    ) {
        // 撤单被柜台异步拒绝（25 ORDER_NOT_FOUND / 26 状态不适宜等）：
        // 上抛错误事件（中文 msg 已在 read_rsp 解码），终态仍以 on_rtn_order 为准。
        let (code, msg) = read_rsp(info);
        self.emit(CtpEvent::Error { code, msg });
    }

    fn on_err_rtn_order_insert(
        &mut self,
        input: Option<&ctp2rs::v1alpha1::CThostFtdcInputOrderField>,
        info: Option<&CThostFtdcRspInfoField>,
    ) {
        // 异步拒绝报单录入：同样携带 input，收敛路径与同步应答一致。
        let (code, msg) = read_rsp(info);
        self.emit_order_insert_error(input, code, msg);
    }

    fn on_rsp_error(
        &mut self,
        info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        _is_last: bool,
    ) {
        let (code, msg) = read_rsp(info);
        self.emit(CtpEvent::Error { code, msg });
    }
}

#[cfg(test)]
mod tests {
    use super::EventTraderSpi;
    use crate::ctp::core::event::CtpEvent;
    use ctp2rs::v1alpha1::TraderSpi;
    use std::sync::mpsc;

    fn setup() -> (EventTraderSpi, mpsc::Receiver<CtpEvent>) {
        let (tx, rx) = mpsc::sync_channel(16);
        (EventTraderSpi::new(tx), rx)
    }

    #[test]
    fn front_connected_emits_event() {
        let (mut spi, rx) = setup();
        spi.on_front_connected();
        assert!(matches!(rx.try_recv(), Ok(CtpEvent::FrontConnected)));
    }

    #[test]
    fn front_disconnected_emits_reason() {
        let (mut spi, rx) = setup();
        spi.on_front_disconnected(0x1001);
        assert!(matches!(
            rx.try_recv(),
            Ok(CtpEvent::FrontDisconnected(0x1001))
        ));
    }

    #[test]
    fn rtn_order_emits_order_event() {
        let (mut spi, rx) = setup();
        spi.on_rtn_order(Some(&Default::default()));
        assert!(matches!(rx.try_recv(), Ok(CtpEvent::Order(_))));
    }

    #[test]
    fn rsp_qry_instrument_emits_query_end_only_on_last() {
        let (mut spi, rx) = setup();
        // 非末条：只发 Instrument，不发 QueryEnd。
        spi.on_rsp_qry_instrument(Some(&Default::default()), None, 1, false);
        assert!(matches!(rx.try_recv(), Ok(CtpEvent::Instrument(_))));
        assert!(
            rx.try_recv().is_err(),
            "非末条不得发 QueryEnd（否则提前释放闸门）"
        );
        // 末条（末页可能无数据行）：必须发 QueryEnd 释放合约预热闸门。
        spi.on_rsp_qry_instrument(None, None, 1, true);
        assert!(matches!(rx.try_recv(), Ok(CtpEvent::QueryEnd)));
    }

    #[test]
    fn rsp_qry_trading_account_emits_event_and_query_end_on_last() {
        // Arrange：资金账户应答行。
        let mut account = ctp2rs::v1alpha1::CThostFtdcTradingAccountField::default();
        crate::ctp::core::encoding::encode_fixed(&mut account.CurrencyID, "CNY");
        account.Balance = 10.0;

        // Act：非末条只发数据事件，不提前释放查询闸门。
        let (mut spi, rx) = setup();
        spi.on_rsp_qry_trading_account(Some(&account), None, 1, false);
        match rx.try_recv() {
            Ok(CtpEvent::TradingAccount(a)) => {
                assert_eq!(a.currency_id, "CNY");
                assert!((a.balance - 10.0).abs() < 1e-9);
            }
            other => panic!("期望 TradingAccount 事件，实际 {other:?}"),
        }
        assert!(rx.try_recv().is_err(), "非末条不得发 QueryEnd");

        // 末条（末页可能无数据行）：必须发 QueryEnd。
        spi.on_rsp_qry_trading_account(None, None, 1, true);
        assert!(matches!(rx.try_recv(), Ok(CtpEvent::QueryEnd)));
    }

    #[test]
    fn rsp_qry_trading_account_error_keeps_venue_error_code() {
        // Arrange：柜台拒绝查询（非 0 错误码）且无数据行。
        let rsp = ctp2rs::v1alpha1::CThostFtdcRspInfoField {
            ErrorID: 31,
            ..Default::default()
        };

        // Act
        let (mut spi, rx) = setup();
        spi.on_rsp_qry_trading_account(None, Some(&rsp), 1, true);

        // Assert：先错误事件（保留真实错误码），再 QueryEnd 释放闸门。
        assert!(matches!(
            rx.try_recv(),
            Ok(CtpEvent::Error { code: 31, .. })
        ));
        assert!(matches!(rx.try_recv(), Ok(CtpEvent::QueryEnd)));
    }

    #[test]
    fn rsp_qry_instrument_carries_long_short_margin_ratios() {
        // Arrange：合约查询回报带多/空保证金率（持仓 UseMargin=0 时的兜底依据）。
        let mut inst = ctp2rs::v1alpha1::CThostFtdcInstrumentField::default();
        crate::ctp::core::encoding::encode_fixed(&mut inst.InstrumentID, "rb2510");
        inst.LongMarginRatio = 0.10;
        inst.ShortMarginRatio = 0.20;

        // Act
        let (mut spi, rx) = setup();
        spi.on_rsp_qry_instrument(Some(&inst), None, 1, false);

        // Assert：比率随 Instrument 事件原样透传，多/空不可错位。
        match rx.try_recv() {
            Ok(CtpEvent::Instrument(i)) => {
                assert_eq!(i.instrument, "rb2510");
                assert!((i.long_margin_ratio - 0.10).abs() < 1e-12, "多头保证金率");
                assert!((i.short_margin_ratio - 0.20).abs() < 1e-12, "空头保证金率");
            }
            other => panic!("期望 Instrument 事件，实际 {other:?}"),
        }
    }

    #[test]
    fn rtn_trade_emits_trade_event() {
        let (mut spi, rx) = setup();
        spi.on_rtn_trade(Some(&Default::default()));
        assert!(matches!(rx.try_recv(), Ok(CtpEvent::Trade(_))));
    }

    #[test]
    fn err_rtn_order_insert_emits_error_event() {
        let (mut spi, rx) = setup();
        let rsp = ctp2rs::v1alpha1::CThostFtdcRspInfoField::default();
        spi.on_err_rtn_order_insert(None, Some(&rsp));
        assert!(matches!(rx.try_recv(), Ok(CtpEvent::Error { .. })));
    }

    // ---- 报单录入被拒：输入字段携带 OrderRef，须以 OrderRejected 收敛订单 ----

    /// 构造带完整关联字段的报单录入请求（卖开 3 手 rb2510，OrderRef=7）。
    fn input_order() -> ctp2rs::v1alpha1::CThostFtdcInputOrderField {
        use std::os::raw::c_char;
        let mut input = ctp2rs::v1alpha1::CThostFtdcInputOrderField::default();
        crate::ctp::core::encoding::encode_fixed(&mut input.OrderRef, "7");
        crate::ctp::core::encoding::encode_fixed(&mut input.InstrumentID, "rb2510");
        input.Direction = '1' as c_char; // 卖
        input.CombOffsetFlag[0] = '0' as c_char; // 开仓
        input.VolumeTotalOriginal = 3;
        input
    }

    fn rsp_error(code: i32) -> ctp2rs::v1alpha1::CThostFtdcRspInfoField {
        ctp2rs::v1alpha1::CThostFtdcRspInfoField {
            ErrorID: code,
            ..Default::default()
        }
    }

    #[test]
    fn rsp_order_insert_error_with_input_emits_order_rejected() {
        // Arrange：柜台同步拒绝报单录入（code=31 资金不足），input 回显 OrderRef。
        let (mut spi, rx) = setup();

        // Act
        spi.on_rsp_order_insert(Some(&input_order()), Some(&rsp_error(31)), 1, true);

        // Assert：发 OrderRejected 且携带全部关联字段，不再退化为无 Ref 的 Error。
        match rx.try_recv() {
            Ok(CtpEvent::OrderRejected {
                order_ref,
                instrument,
                direction,
                offset,
                volume,
                code,
                msg: _,
            }) => {
                assert_eq!(order_ref, "7");
                assert_eq!(instrument, "rb2510");
                assert_eq!(direction, crate::ctp::core::types::Direction::Sell);
                assert_eq!(offset, Some(crate::ctp::core::types::Offset::Open));
                assert_eq!(volume, 3);
                assert_eq!(code, 31);
            }
            other => panic!("期望 OrderRejected，实际 {other:?}"),
        }
        assert!(rx.try_recv().is_err(), "不应再额外发 Error");
    }

    #[test]
    fn rsp_order_insert_error_without_input_falls_back_to_error() {
        // Arrange：错误帧无 input，无法关联 OrderRef。
        let (mut spi, rx) = setup();

        // Act
        spi.on_rsp_order_insert(None, Some(&rsp_error(31)), 1, true);

        // Assert：退回 Error（保持旧行为，不臆造 OrderRef）。
        assert!(matches!(
            rx.try_recv(),
            Ok(CtpEvent::Error { code: 31, .. })
        ));
    }

    #[test]
    fn rsp_order_insert_accepted_emits_nothing() {
        // Arrange：受理成功（无错误）不制造事件——委托状态经 on_rtn_order 推送。
        let (mut spi, rx) = setup();

        // Act
        spi.on_rsp_order_insert(Some(&input_order()), None, 1, true);

        // Assert
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn err_rtn_order_insert_with_input_emits_order_rejected() {
        // Arrange：异步拒绝报单（OnErrRtnOrderInsert），input 可关联 OrderRef。
        let (mut spi, rx) = setup();

        // Act
        spi.on_err_rtn_order_insert(Some(&input_order()), Some(&rsp_error(31)));

        // Assert
        assert!(matches!(
            rx.try_recv(),
            Ok(CtpEvent::OrderRejected {
                order_ref,
                code: 31,
                ..
            }) if order_ref == "7"
        ));
    }

    #[test]
    fn rsp_qry_order_emits_order_query_then_query_end() {
        // Arrange：查询应答行必须区别于实时回报（OrderQuery，而非 Order）。
        let (mut spi, rx) = setup();

        // Act：一行 + is_last。
        spi.on_rsp_qry_order(Some(&Default::default()), None, 1, false);
        spi.on_rsp_qry_order(None, None, 1, true);

        // Assert
        assert!(matches!(rx.try_recv(), Ok(CtpEvent::OrderQuery(_))));
        assert!(matches!(rx.try_recv(), Ok(CtpEvent::QueryEnd)));
    }

    #[test]
    fn rsp_order_action_nonzero_emits_error_with_code() {
        // Arrange：撤单同步应答带错误码 24（重复撤单）。
        let (mut spi, rx) = setup();
        let rsp = ctp2rs::v1alpha1::CThostFtdcRspInfoField {
            ErrorID: 24,
            ..Default::default()
        };

        // Act
        spi.on_rsp_order_action(None, Some(&rsp), 1, true);

        // Assert：错误码可诊断。
        assert!(matches!(
            rx.try_recv(),
            Ok(CtpEvent::Error { code: 24, .. })
        ));
    }

    #[test]
    fn rsp_order_action_accepted_emits_nothing() {
        // Arrange：受理成功（无错误）不应制造事件——撤成与否以 on_rtn_order 终态为准。
        let (mut spi, rx) = setup();

        // Act
        spi.on_rsp_order_action(None, None, 1, true);

        // Assert：通道空。
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn err_rtn_order_action_emits_error_with_code() {
        // Arrange：异步撤单拒绝 25（ORDER_NOT_FOUND，找不到/跨日）。
        let (mut spi, rx) = setup();
        let rsp = ctp2rs::v1alpha1::CThostFtdcRspInfoField {
            ErrorID: 25,
            ..Default::default()
        };

        // Act
        spi.on_err_rtn_order_action(None, Some(&rsp));

        // Assert
        assert!(matches!(
            rx.try_recv(),
            Ok(CtpEvent::Error { code: 25, .. })
        ));
    }
}
