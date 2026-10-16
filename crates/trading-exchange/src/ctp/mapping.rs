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

/// 同一 `(合约, 持仓方向)` 分片聚合结果：整合持仓行 + 今/昨路由明细。
///
/// 上期所等柜台会把同一仓位按今仓/昨仓回**两行**（投机仓）；上游持久层一个
/// (账户,交易所,合约,方向) 只有一条整合 Position，故必须先在 ctp 包内聚合成
/// 一条 [`PositionInfo`]，今/昨明细 (`today`/`yesterday`) 仅喂会话内存
/// `PositionDir` 供平今/平昨路由，不落盘。
pub struct MergedPosition {
    /// 聚合后的统一持仓行（总手数、手数加权合并单价、保证金之和）。
    pub info: PositionInfo,
    /// 今仓手数（Σ TodayPosition）。
    pub today: i32,
    /// 昨仓手数（Σ YdPosition）。
    pub yesterday: i32,
}

/// 同 `(合约, 持仓方向)` 的若干持仓分片 → 一条整合持仓行 + 今/昨明细。
///
/// CTP 自然多/空分行：`direction='2'`（买/多）→ 正数量的 BOTH 行，
/// `'3'`（卖/空）→ 负数量的 BOTH 行（`signed_quantity` 符号表达净多空，
/// 与 Binance 单向持仓同构；对账 key 靠符号区分）。调用方须按
/// `(instrument, direction)` 分桶后逐组调用。
///
/// **量纲**：CTP 的 `PositionCost` 是总持仓成本 = 单价 × 乘数 × 手数（可加）。
/// 今仓行+昨仓行是同一仓位的不相交分片，故：
/// * 总手数 `N = Σ position`（剔除 position=0 空壳行）；
/// * 总成本 `C = Σ PositionCost`；
/// * 合并开仓单价 `entry = C / (N × multiplier)`——**先求和再除**
///   （手数加权；绝不可先算各行单价再平均）；
/// * 占用保证金 `margin = Σ UseMargin`；
/// * `today = Σ TodayPosition`、`yesterday = Σ YdPosition`（喂 PositionDir）。
///
/// 现价/pnl/roi/notional 适配层不计算（`mark_price=None`、`unrealized_pnl=None`），
/// 交引擎用 QuoteTable 现价对合并后的一条统一重估。
///
/// 任一前置不满足返回 None（该组被过滤，绝不产出错误量纲价）：过滤后无有效行
/// （数量皆 0/方向未知）、或乘数缺失/非正（合约目录尚未预热时宁可漏一批，
/// 待下轮对账补齐）。
pub fn merge_position_snapshots(
    group: &[PositionSnapshot],
    volume_multiple: i32,
) -> Option<MergedPosition> {
    // 乘数缺失/非法：整组不产出（避免退化为"每手成本"污染单价口径）。
    if volume_multiple <= 0 {
        return None;
    }
    // 剔除空壳行；同组 (合约,方向) 一致由调用方分桶保证，这里取首个有效行为代表。
    let valid: Vec<&PositionSnapshot> = group
        .iter()
        .filter(|p| p.position != 0 && matches!(p.direction, '2' | '3'))
        .collect();
    let head = *valid.first()?;
    // 手数饱和求和：柜台畸形大值下 debug 构建也不得因 i32 溢出在会话线程 panic
    // （同方向今/昨分片通常仅 1~2 行，饱和上限现实中不可达）。
    let saturating_sum = |f: fn(&PositionSnapshot) -> i32| {
        valid.iter().fold(0_i32, |acc, p| acc.saturating_add(f(p)))
    };
    let total = saturating_sum(|p| p.position);
    if total <= 0 {
        return None;
    }
    let total_cost: f64 = valid.iter().map(|p| p.position_cost).sum();
    let total_margin: f64 = valid.iter().map(|p| p.use_margin).sum();
    let today = saturating_sum(|p| p.today_position.max(0));
    let yesterday = saturating_sum(|p| p.yd_position.max(0));
    let signed = match head.direction {
        '2' => Decimal::from(total),
        '3' => -Decimal::from(total),
        _ => return None,
    };
    // 合并单价 = 总成本之和 / (总手数 × 乘数)；total>0、mult>0 故 denom 必为正。
    let denom = f64::from(total) * f64::from(volume_multiple);
    let entry = Decimal::try_from(total_cost / denom).unwrap_or_default();
    Some(MergedPosition {
        info: PositionInfo {
            instrument: head.instrument.clone(),
            instrument_type: InstrumentType::Futures,
            position_side: OrderPositionSide::Both,
            signed_quantity: signed,
            entry_price: entry,
            mark_price: None,
            // 盈亏交宿主用统一公式（单价 × 乘数）重算，不直接采用柜台汇总值，保证口径单一。
            unrealized_pnl: None,
            leverage: None,
            multiplier: Some(Decimal::from(volume_multiple)),
            notional: None,
            margin: Decimal::try_from(total_margin).ok(),
            update_ms: None,
        },
        today,
        yesterday,
    })
}

/// 单条持仓快照 → 统一持仓行（单价口径）；[`merge_position_snapshots`] 的单行特例。
///
/// 语义与聚合路径完全一致（同一实现），数量为 0/方向未知/乘数缺失非正时返回 None。
pub fn position_info_from_snapshot(
    p: &PositionSnapshot,
    volume_multiple: i32,
) -> Option<PositionInfo> {
    merge_position_snapshots(std::slice::from_ref(p), volume_multiple).map(|m| m.info)
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

    /// rb 螺纹：乘数 10 吨/手，开仓单价 3205，3 手 →
    /// 柜台 PositionCost 是总成本 = 3205 × 10 × 3 = 96150（含乘数）。
    fn rb_long_snapshot() -> PositionSnapshot {
        PositionSnapshot {
            instrument: "rb2510".into(),
            direction: '2',
            position: 3,
            yd_position: 1,
            today_position: 2,
            position_cost: 96_150.0,
            use_margin: 32_050.0,
            position_profit: 55.0,
        }
    }

    #[test]
    fn position_long_divides_cost_by_qty_and_multiple_to_unit_price() {
        let info = position_info_from_snapshot(&rb_long_snapshot(), 10).unwrap();
        assert_eq!(info.signed_quantity, Decimal::from(3));
        assert_eq!(info.position_side, OrderPositionSide::Both);
        // 单价口径：96150 / (3 手 × 10) = 3205，与 tick 现价同量纲。
        assert_eq!(info.entry_price, Decimal::try_from(3205.0f64).unwrap());
        // 真实乘数下传，供宿主 notional/pnl 展开。
        assert_eq!(info.multiplier, Some(Decimal::from(10)));
        assert_eq!(info.margin, Some(Decimal::try_from(32_050.0f64).unwrap()));
        // 盈亏不采用柜台汇总值，交宿主统一公式重算。
        assert_eq!(info.unrealized_pnl, None);
    }

    #[test]
    fn position_short_maps_negative_quantity() {
        let short = PositionSnapshot {
            direction: '3',
            ..rb_long_snapshot()
        };
        let info = position_info_from_snapshot(&short, 10).unwrap();
        assert_eq!(info.signed_quantity, -Decimal::from(3));
        assert_eq!(info.entry_price, Decimal::try_from(3205.0f64).unwrap());
    }

    #[test]
    fn position_zero_or_unknown_direction_filtered() {
        let zero = PositionSnapshot {
            position: 0,
            ..rb_long_snapshot()
        };
        assert!(position_info_from_snapshot(&zero, 10).is_none());
        let unknown = PositionSnapshot {
            direction: '9',
            ..zero
        };
        assert!(position_info_from_snapshot(&unknown, 10).is_none());
    }

    #[test]
    fn position_missing_or_invalid_multiple_filtered() {
        // 合约目录未预热（0）或非法乘数（负）：宁可不产出，也不能退化为错误量纲价。
        assert!(position_info_from_snapshot(&rb_long_snapshot(), 0).is_none());
        assert!(position_info_from_snapshot(&rb_long_snapshot(), -5).is_none());
    }

    // ---- 今/昨分片聚合（SHFE 柜台同方向回两行）----

    /// 构造一条 rb2510 持仓分片（仅聚合相关字段）。
    fn rb_shard(
        direction: char,
        position: i32,
        today: i32,
        yd: i32,
        cost: f64,
        margin: f64,
    ) -> PositionSnapshot {
        PositionSnapshot {
            instrument: "rb2510".into(),
            direction,
            position,
            yd_position: yd,
            today_position: today,
            position_cost: cost,
            use_margin: margin,
            position_profit: 0.0,
        }
    }

    #[test]
    fn merge_today_and_yesterday_shards_into_one_weighted_position() {
        // Arrange：今 2 手@3200（cost=3200×10×2=64000）+ 昨 3 手@3300（99000）。
        let group = vec![
            rb_shard('2', 2, 2, 0, 64_000.0, 20_000.0),
            rb_shard('2', 3, 0, 3, 99_000.0, 30_000.0),
        ];

        // Act
        let merged = merge_position_snapshots(&group, 10).expect("乘数就绪应聚合成功");

        // Assert：一条、qty=5、entry=163000/(5×10)=3260（手数加权，非 3250），
        // margin=两行和，路由明细 today=2/yd=3。
        assert_eq!(merged.info.signed_quantity, Decimal::from(5));
        assert_eq!(merged.info.entry_price, Decimal::try_from(3260.0).unwrap());
        assert_eq!(
            merged.info.margin,
            Some(Decimal::try_from(50_000.0).unwrap())
        );
        assert_eq!(merged.info.multiplier, Some(Decimal::from(10)));
        assert_eq!((merged.today, merged.yesterday), (2, 3));
    }

    #[test]
    fn merge_single_shard_matches_single_row_mapper_field_by_field() {
        // Arrange：单行分片与既有单条映射输入同构。
        let single = rb_long_snapshot();

        // Act
        let merged = merge_position_snapshots(std::slice::from_ref(&single), 10).unwrap();
        let direct = position_info_from_snapshot(&single, 10).unwrap();

        // Assert：聚合路径与单条映射逐字段一致（回归保护），明细取该行今昨。
        assert_eq!(merged.info.signed_quantity, direct.signed_quantity);
        assert_eq!(merged.info.entry_price, direct.entry_price);
        assert_eq!(merged.info.multiplier, direct.multiplier);
        assert_eq!(merged.info.margin, direct.margin);
        assert_eq!(merged.info.instrument, direct.instrument);
        assert_eq!((merged.today, merged.yesterday), (2, 1));
    }

    #[test]
    fn merge_keeps_long_and_short_groups_separate() {
        // Arrange：同合约多/空各一组（每组内部聚合，互不混）。
        let long = vec![
            rb_shard('2', 2, 2, 0, 64_000.0, 20_000.0),
            rb_shard('2', 3, 0, 3, 99_000.0, 30_000.0),
        ];
        let short = vec![rb_shard('3', 4, 1, 3, 132_000.0, 40_000.0)];

        // Act
        let m_long = merge_position_snapshots(&long, 10).unwrap();
        let m_short = merge_position_snapshots(&short, 10).unwrap();

        // Assert：多 +5、空 −4，各自单价独立。
        assert_eq!(m_long.info.signed_quantity, Decimal::from(5));
        assert_eq!(m_short.info.signed_quantity, -Decimal::from(4));
        assert_eq!(m_short.info.entry_price, Decimal::try_from(3300.0).unwrap());
        assert_eq!((m_short.today, m_short.yesterday), (1, 3));
    }

    #[test]
    fn merge_ignores_zero_position_shell_rows() {
        // Arrange：柜台推 position=0 空壳行混入有效今仓行。
        let group = vec![
            rb_shard('2', 0, 0, 0, 0.0, 0.0),
            rb_shard('2', 2, 2, 0, 64_000.0, 20_000.0),
        ];

        // Act / Assert：空壳行不参与手数/成本/明细。
        let merged = merge_position_snapshots(&group, 10).unwrap();
        assert_eq!(merged.info.signed_quantity, Decimal::from(2));
        assert_eq!(merged.info.entry_price, Decimal::try_from(3200.0).unwrap());
        assert_eq!((merged.today, merged.yesterday), (2, 0));
    }

    #[test]
    fn merge_returns_none_when_multiple_missing_or_all_shells() {
        // 乘数 0：整组不映射（等下轮对账）。
        let group = vec![
            rb_shard('2', 2, 2, 0, 64_000.0, 20_000.0),
            rb_shard('2', 3, 0, 3, 99_000.0, 30_000.0),
        ];
        assert!(merge_position_snapshots(&group, 0).is_none());
        // 全是空壳行：无有效持仓。
        let shells = vec![rb_shard('2', 0, 0, 0, 0.0, 0.0)];
        assert!(merge_position_snapshots(&shells, 10).is_none());
        // 空组。
        assert!(merge_position_snapshots(&[], 10).is_none());
    }

    #[test]
    fn merge_short_two_shards_keeps_negative_qty_and_positive_entry() {
        // Arrange：空头今 1@3200 + 昨 2@3300（成本均为正金额）。
        let group = vec![
            rb_shard('3', 1, 1, 0, 32_000.0, 10_000.0),
            rb_shard('3', 2, 0, 2, 66_000.0, 20_000.0),
        ];

        // Act
        let merged = merge_position_snapshots(&group, 10).unwrap();

        // Assert：signed=−3；entry 为正的加权单价 98000/(3×10)≈3266.667。
        assert_eq!(merged.info.signed_quantity, -Decimal::from(3));
        assert!(merged.info.entry_price > Decimal::ZERO, "开仓单价必须为正");
        let expected = Decimal::from(98_000) / Decimal::from(30);
        assert!(
            (merged.info.entry_price - expected).abs() < Decimal::new(1, 6),
            "entry={} 应≈手数加权单价 {expected}",
            merged.info.entry_price
        );
        assert_eq!((merged.today, merged.yesterday), (1, 2));
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
            offset: Some(crate::ctp::core::types::Offset::Open),
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
