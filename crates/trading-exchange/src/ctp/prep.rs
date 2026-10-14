//! CTP 下单纯装配：统一下单请求 → CTP 领域请求（纯内存，无网络/时钟）。
//!
//! 职责（对应方案 §4.3）：
//! * `Side + reduce_only` → `Direction + Offset` 语义映射；
//! * 价格按 `PriceTick` 取整（CTP 价格步长，缺规则时原样放行、最终校验交柜台）；
//! * 手数整型校验（非整数手数拒单）。
//!
//! 上期所平今/平昨（`CloseToday/CloseYesterday`）需要本地今/昨仓分布，
//! 由调用方传入 [`TodayYdSplit`] 决定；非上期所统一传 [`Option::None`]
//! 走柜台自动路由。

use rust_decimal::Decimal;

use crate::api::{PlaceErrorKind, PlaceOrderRequest};
use crate::ctp::core::order::OrderRequest;
use crate::ctp::core::types::{Offset, PriceType, TimeCondition};
use crate::ctp::mapping::{direction_from_side, price_to_f64, volume_to_i32};

/// 本地今/昨仓分布（决定上期所平仓的 Offset 细分）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TodayYdSplit {
    /// 今仓手数。
    pub today: i32,
    /// 昨仓手数。
    pub yesterday: i32,
}

/// 上期所路由结果：一次平仓请求需要的 (Offset, 手数) 序列。
///
/// 上期所平仓必须显式区分今/昨：
/// * 平 `n` 手且今仓足够 → 一笔 `CloseToday`；
/// * 今仓不足 → 先平完今仓（`CloseToday`），剩余平昨仓（`CloseYesterday`）；
/// * 今仓为 0 → 全部 `CloseYesterday`；
/// * 总量不足 → 返回错误（拒单，不下部分仓位的半截单）。
///
/// 非上期所（`split=None`）恒为单笔 `Offset::Close`，由柜台路由今/昨。
pub fn close_offsets(
    volume: i32,
    split: Option<TodayYdSplit>,
) -> Result<Vec<(Offset, i32)>, PlaceErrorKind> {
    use PlaceErrorKind::Rejected;
    let Some(split) = split else {
        return Ok(vec![(Offset::Close, volume)]);
    };
    if volume <= 0 {
        return Err(Rejected);
    }
    if volume > split.today + split.yesterday {
        // 持仓不足以平：拒单。
        return Err(Rejected);
    }
    let mut out = Vec::with_capacity(2);
    if split.today > 0 {
        let n = volume.min(split.today);
        out.push((Offset::CloseToday, n));
    }
    let rest = volume - out.iter().map(|(_, n)| *n).sum::<i32>();
    if rest > 0 {
        out.push((Offset::CloseYesterday, rest));
    }
    Ok(out)
}

/// 价格按 CTP `PriceTick` 取整（向下对齐到合法价位）。
///
/// `tick=None`（规则缺失/非正）时原样放行——元数据故障不挡下单，
/// 最终合法性校验交给柜台（拒单走 Rejected）。
pub fn align_price(price: Decimal, tick: Option<Decimal>) -> Decimal {
    let Some(tick) = tick else {
        return price;
    };
    if tick <= Decimal::ZERO {
        return price;
    }
    (price / tick).floor() * tick
}

/// 统一下单请求 → CTP 限价 GFD 投机单。
///
/// CTP 首版统一限价单（市价 AnyPrice 部分柜台不支持且滑点不可控，
/// 市价转限价由上层 prepare_placement 决定价），故请求必须带价。
/// 校验失败（手数非法/价格缺失）返回 [`PlaceErrorKind::Rejected`]。
pub fn build_open_or_close(
    req: &PlaceOrderRequest,
    price_tick: Option<Decimal>,
) -> Result<OrderRequest, PlaceErrorKind> {
    use PlaceErrorKind::Rejected;
    let volume = volume_to_i32(req.quantity).ok_or(Rejected)?;
    let price_dec = req.price.ok_or(Rejected)?;
    let price_dec = align_price(price_dec, price_tick);
    let price = price_to_f64(price_dec).ok_or(Rejected)?;
    let direction = direction_from_side(req.side);
    let offset = if req.reduce_only {
        Offset::Close
    } else {
        Offset::Open
    };
    Ok(OrderRequest {
        instrument: req.instrument.clone(),
        direction,
        offset,
        hedge_flag: crate::ctp::core::types::HedgeFlag::Speculation,
        price_type: PriceType::LimitPrice,
        price,
        volume,
        time_condition: TimeCondition::Gfd,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::OrderPositionSide;
    use crate::ctp::core::types::Direction;
    use trading_models::{InstrumentType, OrderType, Side, TimeInForce};

    fn place_req() -> PlaceOrderRequest {
        PlaceOrderRequest {
            instrument: "rb2510".into(),
            instrument_type: InstrumentType::Futures,
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(Decimal::from(3205)),
            quantity: Decimal::from(2),
            reduce_only: false,
            position_side: OrderPositionSide::Both,
            time_in_force: Some(TimeInForce::Gtc),
            client_order_id: "0xabc".into(),
        }
    }

    #[test]
    fn open_request_maps_buy_open() {
        let out = build_open_or_close(&place_req(), None).unwrap();
        assert_eq!(out.direction, Direction::Buy);
        assert_eq!(out.offset, Offset::Open);
        assert_eq!(out.volume, 2);
        assert_eq!(out.price, 3205.0);
        assert_eq!(out.price_type, PriceType::LimitPrice);
    }

    #[test]
    fn reduce_only_maps_to_close() {
        let mut req = place_req();
        req.reduce_only = true;
        req.side = Side::Sell;
        let out = build_open_or_close(&req, None).unwrap();
        assert_eq!(out.direction, Direction::Sell);
        assert_eq!(out.offset, Offset::Close);
    }

    #[test]
    fn fractional_volume_is_rejected() {
        let mut req = place_req();
        req.quantity = Decimal::new(15, 1); // 1.5 手
        assert!(matches!(
            build_open_or_close(&req, None),
            Err(PlaceErrorKind::Rejected)
        ));
    }

    #[test]
    fn market_without_price_is_rejected() {
        let mut req = place_req();
        req.price = None;
        assert!(matches!(
            build_open_or_close(&req, None),
            Err(PlaceErrorKind::Rejected)
        ));
    }

    #[test]
    fn price_aligns_down_to_tick() {
        let mut req = place_req();
        req.price = Some(Decimal::new(32053, 1)); // 3205.3
        let out = build_open_or_close(&req, Some(Decimal::from(1))).unwrap();
        assert_eq!(out.price, 3205.0);
    }

    #[test]
    fn price_passthrough_without_tick() {
        let mut req = place_req();
        req.price = Some(Decimal::new(32053, 1));
        let out = build_open_or_close(&req, None).unwrap();
        assert_eq!(out.price, 3205.3);
    }

    #[test]
    fn non_positive_tick_passthrough() {
        let mut req = place_req();
        req.price = Some(Decimal::new(32053, 1));
        let out = build_open_or_close(&req, Some(Decimal::ZERO)).unwrap();
        assert_eq!(out.price, 3205.3);
    }

    #[test]
    fn close_without_split_is_single_close() {
        assert_eq!(close_offsets(3, None).unwrap(), vec![(Offset::Close, 3)]);
    }

    #[test]
    fn shfe_close_today_first_then_yesterday() {
        let split = TodayYdSplit {
            today: 2,
            yesterday: 3,
        };
        assert_eq!(
            close_offsets(3, Some(split)).unwrap(),
            vec![(Offset::CloseToday, 2), (Offset::CloseYesterday, 1)]
        );
    }

    #[test]
    fn shfe_close_all_today() {
        let split = TodayYdSplit {
            today: 2,
            yesterday: 3,
        };
        assert_eq!(
            close_offsets(2, Some(split)).unwrap(),
            vec![(Offset::CloseToday, 2)]
        );
    }

    #[test]
    fn shfe_close_all_yesterday_when_no_today() {
        let split = TodayYdSplit {
            today: 0,
            yesterday: 3,
        };
        assert_eq!(
            close_offsets(2, Some(split)).unwrap(),
            vec![(Offset::CloseYesterday, 2)]
        );
    }

    #[test]
    fn shfe_close_more_than_position_rejected() {
        let split = TodayYdSplit {
            today: 1,
            yesterday: 1,
        };
        assert_eq!(close_offsets(3, Some(split)), Err(PlaceErrorKind::Rejected));
    }

    #[test]
    fn shfe_close_zero_volume_rejected() {
        let split = TodayYdSplit {
            today: 1,
            yesterday: 1,
        };
        assert_eq!(close_offsets(0, Some(split)), Err(PlaceErrorKind::Rejected));
    }
}
