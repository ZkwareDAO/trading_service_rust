//! ctp2rs `TraderSpi` → `CtpEvent` 适配器。
//!
//! 在柜台回调线程内把原生结构即时拷贝为 owned 事件，经同步通道交给主循环。
//! 主循环拥有 [`crate::session::SessionState`] 并据此驱动流程，本结构只做转发，
//! 不持有任何会话状态（职责单一）。接收方关闭时静默丢弃，避免回调线程 panic。

use std::sync::mpsc::SyncSender;

use crate::ctp::core::encoding::decode_fixed;
use crate::ctp::core::event::{CtpEvent, InstrumentInfo, LoginInfo};
use ctp2rs::v1alpha1::{CThostFtdcRspInfoField, CThostFtdcRspUserLoginField, TraderSpi};

use crate::ctp::gateway::convert::snapshot::{order_snapshot, position_snapshot, trade_snapshot};

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
        _input: Option<&ctp2rs::v1alpha1::CThostFtdcInputOrderField>,
        info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        _is_last: bool,
    ) {
        // 报单录入被拒；受理成功时委托状态经 on_rtn_order 推送。
        self.emit_error_if_any(info);
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
        _is_last: bool,
    ) {
        if let Some(i) = instrument {
            self.emit(CtpEvent::Instrument(InstrumentInfo {
                instrument: decode_fixed(&i.InstrumentID),
                exchange_id: decode_fixed(&i.ExchangeID),
                product_id: decode_fixed(&i.ProductID),
                is_trading: i.IsTrading != 0,
            }));
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

    fn on_rsp_qry_order(
        &mut self,
        order: Option<&ctp2rs::v1alpha1::CThostFtdcOrderField>,
        _info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        is_last: bool,
    ) {
        if let Some(o) = order {
            self.emit(CtpEvent::Order(order_snapshot(o)));
        }
        if is_last {
            self.emit(CtpEvent::QueryEnd);
        }
    }

    fn on_err_rtn_order_insert(
        &mut self,
        _input: Option<&ctp2rs::v1alpha1::CThostFtdcInputOrderField>,
        info: Option<&CThostFtdcRspInfoField>,
    ) {
        let (code, msg) = read_rsp(info);
        self.emit(CtpEvent::Error { code, msg });
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
}
