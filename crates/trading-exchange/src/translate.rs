//! 统一 venue 结果 → v0 对策略应答报文的纯映射（跨所通用，零网络）。
//!
//! 装配阶段 service 直接消费新的 [`crate::api::ExchangeApi`]，但回推策略仍用
//! `trading_core::protocol` 里的 `ClientEvent`/`PositionSnapshot`；这些转换集中在此，
//! 便于单测，不散落在编排代码里。

use rust_decimal::Decimal;
use trading_core::protocol::{OrderEvent, PositionSnapshot};
use trading_models::{InstrumentType, OrderStatus, Position, Side};

use crate::api::{OrderPositionSide, OrderState, PositionInfo};

/// 快照方向串（long/short）是否为多。
fn snapshot_side_is_long(side: &str) -> Option<bool> {
    match side.trim().to_ascii_lowercase().as_str() {
        "long" | "buy" | "bid" => Some(true),
        "short" | "sell" | "ask" => Some(false),
        _ => None,
    }
}

/// 用内存活跃仓表给交易所实时快照回填本系统仓位 id（零 IO 纯函数）。
///
/// 入参为 `(快照, 该快照的合约形状)`（合约形状取自 PositionInfo，跨现货/合约批量
/// 查询时各条可能不同）；保持入参顺序返回快照。交易所 REST 不返回本地 id，故按
/// 「交易所 + 合约形状 + 标的 + 方向」在 `active_positions`（调用方已按账户作用域
/// 过滤）中 join：
/// * 命中多行时取 id 最小者（与 PositionRepo::find_active 的确定性口径一致）；
/// * 任一要素不符（含快照方向无法识别）则 position_id 保持 None
///   （如刚开仓尚未对账，调用方提示策略稍后重试）。
pub fn attach_local_position_ids(
    mut items: Vec<(PositionSnapshot, InstrumentType)>,
    active_positions: &[Position],
) -> Vec<PositionSnapshot> {
    for (snap, instrument_type) in items.iter_mut() {
        let Some(is_long) = snapshot_side_is_long(&snap.side) else {
            continue;
        };
        let want_side = if is_long { Side::Buy } else { Side::Sell };
        snap.position_id = active_positions
            .iter()
            .filter(|p| {
                p.instrument_type == *instrument_type
                    && p.instrument == snap.instrument
                    && p.exchange.eq_ignore_ascii_case(&snap.exchange)
                    && p.side == want_side
            })
            .map(|p| p.id)
            .min();
    }
    items.into_iter().map(|(snap, _)| snap).collect()
}

/// signed_quantity（正多负空）→ (side 串, 非负数量)。
pub fn signed_to_side_quantity(signed: Decimal) -> (&'static str, Decimal) {
    if signed.is_sign_negative() {
        ("short", signed.abs())
    } else {
        ("long", signed)
    }
}

/// 持仓方向串：hedge（LONG/SHORT）以交易所方向为准；BOTH 按数量符号拆。
pub fn position_side_wire(p: &PositionInfo) -> &'static str {
    match p.position_side {
        OrderPositionSide::Long => "long",
        OrderPositionSide::Short => "short",
        OrderPositionSide::Both => signed_to_side_quantity(p.signed_quantity).0,
    }
}

/// 一条真实持仓 → 对策略的持仓快照（hedge 用 positionSide，BOTH 用符号拆 side）。
pub fn position_snapshot(account: &str, exchange: &str, p: &PositionInfo) -> PositionSnapshot {
    let side = position_side_wire(p);
    let quantity = p.signed_quantity.abs();
    PositionSnapshot {
        account: account.to_string(),
        exchange: exchange.to_string(),
        instrument: p.instrument.clone(),
        side: side.to_string(),
        quantity,
        pos_price: p.entry_price,
        // 本地仓位 id 不在交易所映射层产生，由服务编排用内存仓表 join 回填。
        position_id: None,
    }
}

/// 私有流订单事件 → 推进订单状态机用的 `OrderEvent`。
/// 零成交均价/累计量映射为 None，避免覆盖内存中的有效数据（与旧帧映射语义一致）。
///
/// venue 原生委托要素（[`OrderState::venue_extra`]）仅 Deribit 填充：透传到
/// direction/reduce_only/amount/limit_price，供订单事件播报使用；其余 venue 为 None。
pub fn order_event(state: &OrderState) -> OrderEvent {
    let extra = state.venue_extra;
    OrderEvent {
        client_order_id: state.client_order_id.clone().unwrap_or_default(),
        status: state.status.wire().to_string(),
        exchange_order_id: state.exchange_order_id.clone(),
        filled_price: state.average_price.filter(|p| *p > Decimal::ZERO),
        filled_quantity: if state.filled_quantity > Decimal::ZERO {
            Some(state.filled_quantity)
        } else {
            None
        },
        exchange_update_time: None,
        message: None,
        // 合约名透传：供通知 A 在本地无 order 时展示。
        instrument: state.instrument.clone(),
        // venue 委托要素：仅 Deribit 透传。
        direction: extra.map(|e| {
            match e.direction {
                Side::Buy => "buy",
                Side::Sell => "sell",
            }
            .to_string()
        }),
        reduce_only: extra.map(|e| e.reduce_only).unwrap_or(false),
        amount: extra.map(|e| e.amount).filter(|a| *a > Decimal::ZERO),
        // 委托价 0（市价单）→ None。
        limit_price: extra.map(|e| e.limit_price).filter(|p| *p > Decimal::ZERO),
    }
}

/// 交易所应答状态串 → 领域订单状态。
/// 先把交易所原生同义词归一为内部 wire 词，再宽松解析；未知归 Unknown。
pub fn status_from_wire(raw: &str) -> OrderStatus {
    let upper = raw.trim().to_ascii_uppercase();
    let normalized = match upper.as_str() {
        "PARTIALLY_FILLED" => "PARTIAL",
        "CANCELLED" => "CANCELED",
        other => other,
    };
    OrderStatus::from_wire(normalized).unwrap_or(OrderStatus::Unknown)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use trading_models::InstrumentType;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn signed_quantity_splits_into_side_and_abs() {
        assert_eq!(signed_to_side_quantity(dec("1.5")), ("long", dec("1.5")));
        assert_eq!(signed_to_side_quantity(dec("-2")), ("short", dec("2")));
        assert_eq!(signed_to_side_quantity(dec("0")), ("long", dec("0")));
    }

    #[test]
    fn position_info_maps_to_snapshot() {
        let info = PositionInfo {
            instrument: "BTCUSDT".into(),
            instrument_type: InstrumentType::Futures,
            position_side: OrderPositionSide::Both,
            signed_quantity: dec("-0.25"),
            entry_price: dec("80000"),
            mark_price: None,
            unrealized_pnl: None,
            leverage: None,
            multiplier: None,
            notional: None,
            margin: None,
            update_ms: None,
        };
        let s = position_snapshot("alice", "binance", &info);
        assert_eq!(s.account, "alice");
        assert_eq!(s.exchange, "binance");
        assert_eq!(s.instrument, "BTCUSDT");
        assert_eq!(s.side, "short");
        assert_eq!(s.quantity, dec("0.25"));
        assert_eq!(s.pos_price, dec("80000"));
    }

    fn local_pos(
        id: u64,
        account: u64,
        inst: &str,
        itype: InstrumentType,
        side: trading_models::Side,
    ) -> Position {
        let mut p = Position::new(account, "binance", inst, side);
        p.id = id;
        p.instrument_type = itype;
        p
    }

    fn snap(inst: &str, side: &str) -> PositionSnapshot {
        PositionSnapshot {
            account: "alice".into(),
            exchange: "binance".into(),
            instrument: inst.into(),
            side: side.into(),
            quantity: dec("1"),
            pos_price: dec("80000"),
            position_id: None,
        }
    }

    /// (快照, 该快照合约形状)。
    fn typed(inst: &str, side: &str, itype: InstrumentType) -> (PositionSnapshot, InstrumentType) {
        (snap(inst, side), itype)
    }

    #[test]
    fn attach_matches_instrument_side_and_type_and_fills_id() {
        let active = vec![local_pos(
            42,
            7,
            "BTCUSDT",
            InstrumentType::Futures,
            trading_models::Side::Buy,
        )];
        let out = attach_local_position_ids(
            vec![typed("BTCUSDT", "long", InstrumentType::Futures)],
            &active,
        );
        assert_eq!(out[0].position_id, Some(42));
    }

    #[test]
    fn attach_leaves_none_on_side_instrument_or_type_mismatch() {
        let active = vec![local_pos(
            42,
            7,
            "BTCUSDT",
            InstrumentType::Futures,
            trading_models::Side::Buy,
        )];
        // 方向不符（本地多，快照空）。
        let out = attach_local_position_ids(
            vec![typed("BTCUSDT", "short", InstrumentType::Futures)],
            &active,
        );
        assert_eq!(out[0].position_id, None);
        // 标的不符。
        let out = attach_local_position_ids(
            vec![typed("ETHUSDT", "long", InstrumentType::Futures)],
            &active,
        );
        assert_eq!(out[0].position_id, None);
        // 合约形状不符（快照为 spot，活跃为 futures）。
        let out = attach_local_position_ids(
            vec![typed("BTCUSDT", "long", InstrumentType::Spot)],
            &active,
        );
        assert_eq!(out[0].position_id, None);
    }

    #[test]
    fn attach_picks_smallest_id_when_multiple_match() {
        let active = vec![
            local_pos(
                55,
                7,
                "BTCUSDT",
                InstrumentType::Futures,
                trading_models::Side::Buy,
            ),
            local_pos(
                42,
                7,
                "BTCUSDT",
                InstrumentType::Futures,
                trading_models::Side::Buy,
            ),
        ];
        let out = attach_local_position_ids(
            vec![typed("BTCUSDT", "long", InstrumentType::Futures)],
            &active,
        );
        assert_eq!(out[0].position_id, Some(42));
    }

    #[test]
    fn hedge_side_uses_position_side_not_sign() {
        // hedge 空头：方向以 positionSide=SHORT 为准，数量取绝对值。
        let short = PositionInfo {
            instrument: "BTCUSDT".into(),
            instrument_type: InstrumentType::Futures,
            position_side: OrderPositionSide::Short,
            signed_quantity: dec("-0.25"),
            entry_price: dec("80000"),
            mark_price: None,
            unrealized_pnl: None,
            leverage: Some(10),
            multiplier: None,
            notional: None,
            margin: None,
            update_ms: None,
        };
        assert_eq!(position_side_wire(&short), "short");
        let s = position_snapshot("alice", "binance", &short);
        assert_eq!(s.side, "short");
        assert_eq!(s.quantity, dec("0.25"));

        // hedge 多头。
        let long = PositionInfo {
            position_side: OrderPositionSide::Long,
            ..short
        };
        assert_eq!(position_side_wire(&long), "long");
        assert_eq!(position_snapshot("alice", "binance", &long).side, "long");
    }

    #[test]
    fn order_state_to_event_maps_fills_and_zeroes_to_none() {
        let state = OrderState {
            client_order_id: Some("0xabc".into()),
            exchange_order_id: Some("123".into()),
            instrument: "BTCUSDT".into(),
            status: OrderStatus::Partial,
            filled_quantity: dec("0.5"),
            average_price: Some(dec("80000")),
            position_side: OrderPositionSide::Both,
            venue_extra: None,
        };
        let ev = order_event(&state);
        assert_eq!(ev.client_order_id, "0xabc");
        assert_eq!(ev.status, "PARTIAL");
        assert_eq!(ev.exchange_order_id.as_deref(), Some("123"));
        assert_eq!(ev.filled_quantity, Some(dec("0.5")));
        assert_eq!(ev.filled_price, Some(dec("80000")));
    }

    #[test]
    fn order_state_zero_fills_become_none() {
        let state = OrderState {
            client_order_id: Some("0xabc".into()),
            exchange_order_id: None,
            instrument: "BTCUSDT".into(),
            status: OrderStatus::New,
            filled_quantity: dec("0"),
            average_price: Some(dec("0")),
            position_side: OrderPositionSide::Both,
            venue_extra: None,
        };
        let ev = order_event(&state);
        assert_eq!(ev.status, "NEW");
        assert_eq!(ev.filled_quantity, None);
        assert_eq!(ev.filled_price, None);
    }

    #[test]
    fn status_wire_is_lenient() {
        assert_eq!(status_from_wire("new"), OrderStatus::New);
        assert_eq!(status_from_wire("PARTIALLY_FILLED"), OrderStatus::Partial);
        assert_eq!(status_from_wire("weird"), OrderStatus::Unknown);
    }
}
