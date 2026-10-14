//! 原生 Order/Trade 结构 → ctp-core owned 快照。
//!
//! 这些函数在 SPI 回调内被调用：必须只读取、立即拷贝，返回值不含任何
//! 指向柜台内存的指针。未知字符统一回退为 `Unknown`，而不是 panic。

use crate::ctp::core::encoding::decode_fixed;
use crate::ctp::core::event::{OrderSnapshot, PositionSnapshot, TradeSnapshot};
use crate::ctp::core::types::{Direction, OrderStatus};
use ctp2rs::v1alpha1::{
    CThostFtdcInvestorPositionField, CThostFtdcOrderField, CThostFtdcTradeField,
};

/// 把柜台报单结构的关键要素拷贝为 owned 快照。
pub fn order_snapshot(order: &CThostFtdcOrderField) -> OrderSnapshot {
    OrderSnapshot {
        order_ref: decode_fixed(&order.OrderRef),
        instrument: decode_fixed(&order.InstrumentID),
        exchange_id: decode_fixed(&order.ExchangeID),
        order_sys_id: decode_fixed(&order.OrderSysID),
        insert_time: decode_fixed(&order.InsertTime),
        direction: Direction::from_code(order.Direction as u8 as char).unwrap_or(Direction::Buy),
        status: OrderStatus::from_code(order.OrderStatus as u8 as char)
            .unwrap_or(OrderStatus::Unknown),
        limit_price: order.LimitPrice,
        volume_total: order.VolumeTotalOriginal,
        volume_traded: order.VolumeTraded,
        status_msg: decode_fixed(&order.StatusMsg),
        front_id: order.FrontID,
        session_id: order.SessionID,
    }
}

/// 把柜台成交结构的关键要素拷贝为 owned 快照。
pub fn trade_snapshot(trade: &CThostFtdcTradeField) -> TradeSnapshot {
    TradeSnapshot {
        order_ref: decode_fixed(&trade.OrderRef),
        trade_id: decode_fixed(&trade.TradeID),
        instrument: decode_fixed(&trade.InstrumentID),
        exchange_id: decode_fixed(&trade.ExchangeID),
        order_sys_id: decode_fixed(&trade.OrderSysID),
        direction: Direction::from_code(trade.Direction as u8 as char).unwrap_or(Direction::Buy),
        price: trade.Price,
        volume: trade.Volume,
        trade_date: decode_fixed(&trade.TradeDate),
        trade_time: decode_fixed(&trade.TradeTime),
    }
}

/// 把柜台持仓结构的关键要素拷贝为 owned 快照。
pub fn position_snapshot(position: &CThostFtdcInvestorPositionField) -> PositionSnapshot {
    PositionSnapshot {
        instrument: decode_fixed(&position.InstrumentID),
        direction: position.PosiDirection as u8 as char,
        position: position.Position,
        yd_position: position.YdPosition,
        today_position: position.TodayPosition,
        position_cost: position.PositionCost,
        use_margin: position.UseMargin,
        position_profit: position.PositionProfit,
    }
}

#[cfg(test)]
mod tests {
    use super::{order_snapshot, trade_snapshot};
    use crate::ctp::core::encoding::encode_fixed;
    use crate::ctp::core::types::{Direction, OrderStatus};
    use ctp2rs::v1alpha1::{CThostFtdcOrderField, CThostFtdcTradeField};
    use std::os::raw::c_char;

    fn sample_order() -> CThostFtdcOrderField {
        let mut o = CThostFtdcOrderField::default();
        encode_fixed(&mut o.OrderRef, "7");
        encode_fixed(&mut o.InstrumentID, "rb2510");
        encode_fixed(&mut o.ExchangeID, "SHFE");
        encode_fixed(&mut o.OrderSysID, "9001");
        o.Direction = '0' as c_char;
        o.OrderStatus = '1' as c_char; // 部分成交排队中
        o.LimitPrice = 3205.0;
        o.VolumeTotalOriginal = 3;
        o.VolumeTraded = 1;
        o.FrontID = 2;
        o.SessionID = 9001;
        o
    }

    #[test]
    fn maps_identifiers() {
        let s = order_snapshot(&sample_order());
        assert_eq!(s.order_ref, "7");
        assert_eq!(s.instrument, "rb2510");
        assert_eq!(s.exchange_id, "SHFE");
        assert_eq!(s.order_sys_id, "9001");
    }

    #[test]
    fn maps_direction_status_and_price_volume() {
        let s = order_snapshot(&sample_order());
        assert_eq!(s.direction, Direction::Buy);
        assert_eq!(s.status, OrderStatus::PartTradedQueueing);
        assert_eq!(s.limit_price, 3205.0);
        assert_eq!(s.volume_total, 3);
        assert_eq!(s.volume_traded, 1);
    }

    #[test]
    fn maps_front_and_session() {
        let s = order_snapshot(&sample_order());
        assert_eq!(s.front_id, 2);
        assert_eq!(s.session_id, 9001);
    }

    #[test]
    fn unknown_status_char_falls_back_to_unknown() {
        let mut o = sample_order();
        o.OrderStatus = '9' as c_char;
        assert_eq!(order_snapshot(&o).status, OrderStatus::Unknown);
    }

    fn sample_trade() -> CThostFtdcTradeField {
        let mut t = CThostFtdcTradeField::default();
        encode_fixed(&mut t.OrderRef, "1");
        encode_fixed(&mut t.TradeID, "55");
        encode_fixed(&mut t.InstrumentID, "rb2510");
        encode_fixed(&mut t.ExchangeID, "SHFE");
        encode_fixed(&mut t.OrderSysID, "9001");
        t.Direction = '0' as c_char;
        t.Price = 3205.0;
        t.Volume = 1;
        encode_fixed(&mut t.TradeDate, "20261009");
        encode_fixed(&mut t.TradeTime, "10:30:00");
        t
    }

    #[test]
    fn maps_trade_fields() {
        let s = trade_snapshot(&sample_trade());
        assert_eq!(s.order_ref, "1");
        assert_eq!(s.trade_id, "55");
        assert_eq!(s.instrument, "rb2510");
        assert_eq!(s.direction, Direction::Buy);
        assert_eq!(s.price, 3205.0);
        assert_eq!(s.volume, 1);
        assert_eq!(s.trade_date, "20261009");
        assert_eq!(s.trade_time, "10:30:00");
    }
}
