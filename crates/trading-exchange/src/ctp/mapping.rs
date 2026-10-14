//! 统一订单/持仓 → CTP 领域请求/快照的纯映射（无网络、无时钟）。
//!
//! 上游统一领域模型（`Side + reduce_only`）在此折算为 CTP 的
//! `Direction + Offset`；CTP 的多/空分仓、今/昨仓差异不向上游泄漏。

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crate::api::{OrderPositionSide, PositionInfo};
use crate::ctp::core::event::{OrderSnapshot, PositionSnapshot, TradeSnapshot};
use crate::ctp::core::types::Direction;
use trading_models::{InstrumentType, OrderStatus};

/// 统一 `Side`（买/卖）→ CTP `Direction`。
/// reduce_only 只影响开平标志，不改变方向语义。
pub fn direction_from_side(side: trading_models::Side) -> Direction {
    match side {
        trading_models::Side::Buy => Direction::Buy,
        trading_models::Side::Sell => Direction::Sell,
    }
}

/// 手数：统一领域数量（Decimal）→ CTP 整数手数。
///
/// CTP 手数为整数（1 手 = 1 张）；非正数或非整数返回 None，由调用方拒单。
pub fn volume_to_i32(quantity: Decimal) -> Option<i32> {
    if quantity <= Decimal::ZERO || quantity.fract() != Decimal::ZERO {
        return None;
    }
    i32::try_from(quantity.mantissa()).ok()
}

/// 价格：Decimal → f64（CTP 字段口径）；非有限或非正返回 None。
pub fn price_to_f64(price: Decimal) -> Option<f64> {
    let v = price.to_f64()?;
    if v.is_finite() && v > 0.0 {
        Some(v)
    } else {
        None
    }
}

/// CTP 委托快照 → 统一订单状态。
///
/// * `AllTraded` → Filled；`Cancelled` → Canceled；
/// * `NoTradeQueueing`/`NoTradeNotQueueing`/`NotTouched`/`Touched` → New；
/// * `PartTraded*` → Partial；
/// * `Unknown`/未知 → Unknown。
pub fn status_from_snapshot(s: crate::ctp::core::types::OrderStatus) -> OrderStatus {
    use crate::ctp::core::types::OrderStatus as C;
    match s {
        C::AllTraded => OrderStatus::Filled,
        C::Cancelled => OrderStatus::Canceled,
        C::PartTradedQueueing | C::PartTradedNotQueueing => OrderStatus::Partial,
        C::NoTradeQueueing | C::NoTradeNotQueueing | C::NotTouched | C::Touched => OrderStatus::New,
        C::Unknown => OrderStatus::Unknown,
    }
}

/// 委托快照 → 统一订单状态行（供订单流推进内存状态机）。
///
/// `client_order_id` 即 CTP `OrderRef`（本会话内唯一、下单时自产）；
/// `exchange_order_id` 取 `OrderSysID`（受理后才有，可能为空）。
pub fn order_state_from_snapshot(s: &OrderSnapshot) -> crate::api::OrderState {
    crate::api::OrderState {
        client_order_id: Some(s.order_ref.clone()),
        exchange_order_id: if s.order_sys_id.trim().is_empty() {
            None
        } else {
            Some(s.order_sys_id.clone())
        },
        instrument: s.instrument.clone(),
        status: status_from_snapshot(s.status),
        filled_quantity: Decimal::from(s.volume_traded.max(0)),
        average_price: None,
        position_side: OrderPositionSide::Both,
        venue_extra: None,
    }
}

/// 成交快照 → 统一订单状态行。
///
/// CTP 成交回报是**逐笔增量**，`cumulative_filled` 由消费方负责累计；
/// 状态恒为 Partial（终态由委托回报收敛），均价缺省 None 不覆盖内存有效值。
pub fn order_state_from_trade(
    t: &TradeSnapshot,
    cumulative_filled: Decimal,
) -> crate::api::OrderState {
    crate::api::OrderState {
        client_order_id: Some(t.order_ref.clone()),
        exchange_order_id: if t.order_sys_id.trim().is_empty() {
            None
        } else {
            Some(t.order_sys_id.clone())
        },
        instrument: t.instrument.clone(),
        status: OrderStatus::Partial,
        filled_quantity: cumulative_filled,
        average_price: None,
        position_side: OrderPositionSide::Both,
        venue_extra: None,
    }
}

/// 持仓快照 → 统一持仓行。
///
/// CTP 自然多/空分行：`direction='2'`（买/多）→ 正数量的 BOTH 行，
/// `'3'`（卖/空）→ 负数量的 BOTH 行（`signed_quantity` 符号表达净多空，
/// 与 Binance 单向持仓同构；对账 key 靠符号区分）。方向未知或数量为 0 返回 None。
pub fn position_info_from_snapshot(p: &PositionSnapshot) -> Option<PositionInfo> {
    if p.position == 0 {
        return None;
    }
    let signed = match p.direction {
        '2' => Decimal::from(p.position),
        '3' => -Decimal::from(p.position),
        _ => return None,
    };
    // 持仓成本按均价口径折算（CTP 的 PositionCost 已含合约乘数，除以手数
    // 得到每手价格口径的均价）。
    let entry = if p.position > 0 {
        Decimal::try_from(p.position_cost / p.position as f64).unwrap_or_default()
    } else {
        Decimal::ZERO
    };
    Some(PositionInfo {
        instrument: p.instrument.clone(),
        instrument_type: InstrumentType::Futures,
        position_side: OrderPositionSide::Both,
        signed_quantity: signed,
        entry_price: entry,
        mark_price: None,
        unrealized_pnl: Decimal::try_from(p.position_profit).ok(),
        leverage: None,
        notional: None,
        margin: Decimal::try_from(p.use_margin).ok(),
        update_ms: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctp::core::types::OrderStatus as CtpStatus;
    use trading_models::Side;

    #[test]
    fn side_maps_to_direction() {
        assert_eq!(direction_from_side(Side::Buy), Direction::Buy);
        assert_eq!(direction_from_side(Side::Sell), Direction::Sell);
    }

    #[test]
    fn volume_accepts_positive_integers_only() {
        assert_eq!(volume_to_i32(Decimal::from(3)), Some(3));
        assert_eq!(volume_to_i32(Decimal::new(15, 1)), None); // 1.5 → 拒
        assert_eq!(volume_to_i32(Decimal::ZERO), None);
        assert_eq!(volume_to_i32(-Decimal::from(1)), None);
    }

    #[test]
    fn price_rejects_non_positive() {
        assert_eq!(price_to_f64(Decimal::from(3205)), Some(3205.0));
        assert_eq!(price_to_f64(Decimal::ZERO), None);
        assert_eq!(price_to_f64(-Decimal::from(1)), None);
    }

    #[test]
    fn status_mapping_covers_all_ctp_states() {
        assert_eq!(
            status_from_snapshot(CtpStatus::AllTraded),
            OrderStatus::Filled
        );
        assert_eq!(
            status_from_snapshot(CtpStatus::Cancelled),
            OrderStatus::Canceled
        );
        assert_eq!(
            status_from_snapshot(CtpStatus::PartTradedQueueing),
            OrderStatus::Partial
        );
        assert_eq!(
            status_from_snapshot(CtpStatus::PartTradedNotQueueing),
            OrderStatus::Partial
        );
        assert_eq!(
            status_from_snapshot(CtpStatus::NoTradeQueueing),
            OrderStatus::New
        );
        assert_eq!(
            status_from_snapshot(CtpStatus::NotTouched),
            OrderStatus::New
        );
        assert_eq!(
            status_from_snapshot(CtpStatus::Unknown),
            OrderStatus::Unknown
        );
    }

    fn order_snapshot_template() -> OrderSnapshot {
        OrderSnapshot {
            order_ref: "1".into(),
            instrument: "rb2510".into(),
            exchange_id: "SHFE".into(),
            order_sys_id: String::new(),
            insert_time: String::new(),
            direction: Direction::Buy,
            status: CtpStatus::NoTradeQueueing,
            limit_price: 0.0,
            volume_total: 1,
            volume_traded: 0,
            status_msg: String::new(),
            front_id: 0,
            session_id: 0,
        }
    }

    #[test]
    fn order_snapshot_maps_ids_and_quantity() {
        let s = OrderSnapshot {
            order_ref: "7".into(),
            order_sys_id: "9001".into(),
            status: CtpStatus::PartTradedQueueing,
            volume_traded: 1,
            volume_total: 3,
            ..order_snapshot_template()
        };
        let st = order_state_from_snapshot(&s);
        assert_eq!(st.client_order_id.as_deref(), Some("7"));
        assert_eq!(st.exchange_order_id.as_deref(), Some("9001"));
        assert_eq!(st.status, OrderStatus::Partial);
        assert_eq!(st.filled_quantity, Decimal::from(1));
    }

    #[test]
    fn order_snapshot_empty_sys_id_maps_to_none() {
        let s = order_snapshot_template();
        let st = order_state_from_snapshot(&s);
        assert!(st.exchange_order_id.is_none());
        assert_eq!(st.status, OrderStatus::New);
    }

    #[test]
    fn position_long_maps_positive_short_negative() {
        let long = PositionSnapshot {
            instrument: "rb2510".into(),
            direction: '2',
            position: 3,
            yd_position: 1,
            today_position: 2,
            position_cost: 9615.0,
            use_margin: 1000.0,
            position_profit: 55.0,
        };
        let info = position_info_from_snapshot(&long).unwrap();
        assert_eq!(info.signed_quantity, Decimal::from(3));
        assert_eq!(info.position_side, OrderPositionSide::Both);
        assert_eq!(info.entry_price, Decimal::try_from(3205.0f64).unwrap());
        assert_eq!(info.margin, Some(Decimal::try_from(1000.0f64).unwrap()));

        let short = PositionSnapshot {
            direction: '3',
            ..long
        };
        let info = position_info_from_snapshot(&short).unwrap();
        assert_eq!(info.signed_quantity, -Decimal::from(3));
    }

    #[test]
    fn position_zero_or_unknown_direction_filtered() {
        let zero = PositionSnapshot {
            instrument: "rb2510".into(),
            direction: '2',
            position: 0,
            yd_position: 0,
            today_position: 0,
            position_cost: 0.0,
            use_margin: 0.0,
            position_profit: 0.0,
        };
        assert!(position_info_from_snapshot(&zero).is_none());
        let unknown = PositionSnapshot {
            direction: '9',
            ..zero
        };
        assert!(position_info_from_snapshot(&unknown).is_none());
    }

    #[test]
    fn trade_maps_cumulative_filled() {
        let t = TradeSnapshot {
            order_ref: "1".into(),
            trade_id: "T1".into(),
            instrument: "rb2510".into(),
            exchange_id: "SHFE".into(),
            order_sys_id: "9001".into(),
            direction: Direction::Buy,
            price: 3205.0,
            volume: 1,
            trade_date: "20261012".into(),
            trade_time: "09:30:01".into(),
        };
        let st = order_state_from_trade(&t, Decimal::from(2));
        assert_eq!(st.filled_quantity, Decimal::from(2));
        assert_eq!(st.exchange_order_id.as_deref(), Some("9001"));
        // 成交回报同样回传 OrderRef（客户端自填），cid 关联与委托回报同口径。
        assert_eq!(st.client_order_id.as_deref(), Some("1"));
    }
}
