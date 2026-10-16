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

use rust_decimal::prelude::Zero;
use rust_decimal::Decimal;

use crate::api::{PlaceDefer, PlaceErrorKind, PlaceOrderRequest};
use crate::ctp::core::order::OrderRequest;
use crate::ctp::core::types::{Offset, PriceType, TimeCondition};
use crate::ctp::mapping::{direction_from_side, price_to_f64, volume_to_i32};
use trading_models::{OrderType, Side};

/// SHFE 市价转限价所需的盘口（best bid/ask，调用方从 QuoteTable 读出后注入）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MarketRef {
    /// 买一价（平多 sell 取它）。
    pub bid: Decimal,
    /// 卖一价（平空 buy 取它；买开在其上加漂移）。
    pub ask: Decimal,
}

/// 本地今/昨仓分布（决定上期所平仓的 Offset 细分）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TodayYdSplit {
    /// 今仓手数。
    pub today: i32,
    /// 昨仓手数。
    pub yesterday: i32,
}

/// SHFE 平仓价差闸门（预留扩展位）：本期不启用——恒放行，未来需要时在此对
/// best bid/ask 宽度设阈值（参照 deribit `check_spread`），返回 `PlaceDefer`。
#[allow(clippy::unnecessary_wraps)]
fn check_close_spread(_book: MarketRef) -> Result<(), PlaceDefer> {
    Ok(())
}

/// 把价格按 tick 网格**按方向**取整：买单向上（ceil）、卖单向下（floor），
/// 保证穿价限价不被取整削弱成交确定性。注意与限价单的 [`align_price`]（一律 floor，
/// 对齐到不劣于用户所报的合法价位）用途不同，二者不要混用。
fn snap_to_tick(price: Decimal, tick: Decimal, side: Side) -> Decimal {
    let units = price / tick;
    let snapped_units = match side {
        Side::Buy => units.ceil(),
        Side::Sell => units.floor(),
    };
    snapped_units * tick
}

/// SHFE 市价单 → 穿价限价（GFD）纯转换：仅处理**调用方已判定为 SHFE** 的请求。
///
/// * 限价单（开/平）原样返回，不读盘口/tick；
/// * 市价**开仓**：买开 = `ask + drift_ticks×tick`、卖开 = `bid − drift_ticks×tick`
///   （drift_ticks 为 CTP 独立配置，与 deribit 无关），按方向取整到 tick；
/// * 市价**平仓**：平多(sell+reduce_only)=`best bid`、平空(buy+reduce_only)=`best ask`，
///   **不漂移**；本期无价差闸门（[`check_close_spread`] 恒放行，预留扩展）；
/// * 无盘口 / tick 非正 / 算出价 ≤0 → [`PlaceDefer::DataNotReady`]（不 claim、下轮重试）。
///
/// 转换结果仅改 `order_type=Limit` 与 `price`，数量/方向/reduce_only 不变；
/// 有效期沿用限价分支的 GFD（build_open_or_close 产出 LimitPrice+GFD）。
pub fn prepare_shfe_market_limit(
    mut req: PlaceOrderRequest,
    book: Option<MarketRef>,
    tick: Decimal,
    drift_ticks: i64,
) -> Result<PlaceOrderRequest, PlaceDefer> {
    if req.order_type != OrderType::Market {
        return Ok(req);
    }
    let Some(book) = book else {
        return Err(PlaceDefer::DataNotReady);
    };
    if tick <= Decimal::ZERO {
        return Err(PlaceDefer::DataNotReady);
    }
    // 平仓先过（预留的）价差闸门。
    if req.reduce_only {
        check_close_spread(book)?;
    }
    let drift = tick * Decimal::from(drift_ticks.max(0));
    let raw = match (req.side, req.reduce_only) {
        // 市价平仓取对手 best 价、不漂移。
        (Side::Sell, true) => book.bid,
        (Side::Buy, true) => book.ask,
        // 市价开仓按方向穿价漂移。
        (Side::Buy, false) => book.ask + drift,
        (Side::Sell, false) => book.bid - drift,
    };
    if raw <= Decimal::zero() {
        return Err(PlaceDefer::DataNotReady);
    }
    req.price = Some(snap_to_tick(raw, tick, req.side));
    req.order_type = OrderType::Limit;
    Ok(req)
}

/// 上期所路由结果：一次平仓请求需要的 (Offset, 手数) 序列。
///
/// **先平昨、不足再平今**（上期所平今手续费远高于平昨，省费优先）：
/// * 平仓量 ≤ 昨仓 → 仅一笔 `CloseYesterday`，完全不碰今仓；
/// * 平仓量 > 昨仓 → 先平完全部昨仓（`CloseYesterday`），剩余平今仓（`CloseToday`）；
/// * 昨仓为 0 → 全部 `CloseToday`；
/// * 总量不足/非正手数 → 返回错误（拒单，不下部分仓位的半截单）。
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
    // 1) 先平昨（平量与昨仓取小）。
    let yd = volume.min(split.yesterday);
    if yd > 0 {
        out.push((Offset::CloseYesterday, yd));
    }
    // 2) 超出昨仓的部分才平今（昨仓已在上方 min 钳制，rest≥0）。
    let rest = volume - yd;
    if rest > 0 {
        out.push((Offset::CloseToday, rest));
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

/// 统一下单请求 → CTP 投机单（按 `order_type` 分流价格条件）。
///
/// * `Limit`：限价 GFD，价格必填并按 `price_tick` 向下对齐；
/// * `Market`：市价 `AnyPrice + IOC`（柜台撮合定价，`LimitPrice` 按惯例填 0）。
///   注意上期所/能源中心不支持 AnyPrice，市价单会被柜台拒（走 Rejected 回报收敛）；
/// * 其余（`StopMarket`/`TakeMarket`）：条件单需 `ContingentCondition + StopPrice`
///   支持，为独立特性——显式拒绝，不静默降级为限价单。
///
/// 校验失败（手数非法/限价缺价）返回 [`PlaceErrorKind::Rejected`]。
pub fn build_open_or_close(
    req: &PlaceOrderRequest,
    price_tick: Option<Decimal>,
) -> Result<OrderRequest, PlaceErrorKind> {
    use PlaceErrorKind::Rejected;
    let volume = volume_to_i32(req.quantity).ok_or(Rejected)?;
    let (price_type, time_condition, price) = match req.order_type {
        OrderType::Market => (PriceType::AnyPrice, TimeCondition::Ioc, 0.0),
        OrderType::Limit => {
            let price_dec = req.price.ok_or(Rejected)?;
            let price_dec = align_price(price_dec, price_tick);
            let price = price_to_f64(price_dec).ok_or(Rejected)?;
            (PriceType::LimitPrice, TimeCondition::Gfd, price)
        }
        // 条件单待补（ContingentCondition + StopPrice），显式拒绝。
        OrderType::StopMarket | OrderType::TakeMarket => return Err(Rejected),
    };
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
        price_type,
        price,
        volume,
        time_condition,
    })
}

/// 从共享行情表读 CTP 期货某合约的 best bid/ask；无行情行或缺任一侧盘口 → None。
///
/// 涨跌停封板可能只有单边盘口，此时保守判未就绪（调用方拒绝/保活），不在本层用
/// 涨跌停价猜测替代——需要时由后续行情补齐或显式接入上下限价字段。
pub(crate) fn read_ctp_book(
    quotes: &trading_core::quote::QuoteTable,
    symbol: &str,
) -> Option<MarketRef> {
    let key = trading_models::Instrument::new(
        trading_models::Venue::Ctp,
        trading_models::MarketKind::Futures,
        symbol,
    );
    let book = quotes.get(&key)?.book?;
    Some(MarketRef {
        bid: book.bid?,
        ask: book.ask?,
    })
}

/// CTP 市价单的统一预检（**规则引擎与手动 API 共用同一组件**）：
/// 非市价/限价原样透传；合约静态信息未预热 → DataNotReady；**非 SHFE 原样**
/// （DCE 等走原生 AnyPrice+IOC）；SHFE 读盘口+最小变动价位交
/// [`prepare_shfe_market_limit`] 转穿价限价 GFD。
pub(crate) fn prepare_ctp_market(
    dir: &crate::ctp::instrument_dir::InstrumentDir,
    quotes: &trading_core::quote::QuoteTable,
    drift_ticks: i64,
    request: PlaceOrderRequest,
) -> Result<PlaceOrderRequest, PlaceDefer> {
    if request.order_type != OrderType::Market {
        return Ok(request);
    }
    let Some(info) = dir.get(&request.instrument) else {
        return Err(PlaceDefer::DataNotReady);
    };
    if info.exchange_id.trim() != crate::ctp::venue::SHFE_EXCHANGE_ID {
        return Ok(request);
    }
    let book = read_ctp_book(quotes, &request.instrument);
    let tick = Decimal::try_from(info.price_tick).unwrap_or(Decimal::ZERO);
    prepare_shfe_market_limit(request, book, tick, drift_ticks)
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
    fn market_order_maps_to_any_price_ioc_without_price() {
        // Arrange：市价请求不带价（价格由柜台撮合）。
        let mut req = place_req();
        req.order_type = OrderType::Market;
        req.price = None;

        // Act
        let out = build_open_or_close(&req, None).unwrap();

        // Assert：AnyPrice + IOC，价格填 0（字段必填但柜台忽略），
        // 方向/手数复用限价路径。
        assert_eq!(out.price_type, PriceType::AnyPrice);
        assert_eq!(out.time_condition, TimeCondition::Ioc);
        assert_eq!(out.price, 0.0);
        assert_eq!(out.direction, Direction::Buy);
        assert_eq!(out.offset, Offset::Open);
        assert_eq!(out.volume, 2);
    }

    #[test]
    fn market_order_tolerates_price_tick() {
        // Arrange / Act：市价单不做 tick 对齐（不依赖规则表价格），带 tick 不 panic 不改价。
        let mut req = place_req();
        req.order_type = OrderType::Market;
        req.price = None;

        // Act / Assert
        let out = build_open_or_close(&req, Some(Decimal::from(1))).unwrap();
        assert_eq!(out.price, 0.0);
    }

    #[test]
    fn market_order_shares_volume_validation() {
        // Arrange：市价单同样受手数校验约束。
        let mut req = place_req();
        req.order_type = OrderType::Market;
        req.price = None;
        req.quantity = Decimal::new(15, 1); // 1.5 手

        // Act / Assert
        assert!(matches!(
            build_open_or_close(&req, None),
            Err(PlaceErrorKind::Rejected)
        ));
    }

    #[test]
    fn stop_variants_are_explicitly_rejected() {
        // 条件单（ContingentCondition+StopPrice）是独立特性，
        // 不得静默降级为限价单。
        let mut req = place_req();
        req.order_type = OrderType::StopMarket;
        assert!(matches!(
            build_open_or_close(&req, None),
            Err(PlaceErrorKind::Rejected)
        ));

        let mut req = place_req();
        req.order_type = OrderType::TakeMarket;
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
    fn shfe_close_yesterday_first_then_today() {
        // 平今手续费远高于平昨：先平昨 3，不足的 1 手才平今。
        let split = TodayYdSplit {
            today: 2,
            yesterday: 3,
        };
        assert_eq!(
            close_offsets(4, Some(split)).unwrap(),
            vec![(Offset::CloseYesterday, 3), (Offset::CloseToday, 1)]
        );
    }

    #[test]
    fn shfe_close_within_yesterday_never_touches_today() {
        // 平仓量 ≤ 昨仓：只发平昨，完全不碰今仓。
        let split = TodayYdSplit {
            today: 2,
            yesterday: 3,
        };
        assert_eq!(
            close_offsets(2, Some(split)).unwrap(),
            vec![(Offset::CloseYesterday, 2)]
        );
        // 昨仓足够时即使今仓更多，也只平昨。
        let split = TodayYdSplit {
            today: 5,
            yesterday: 3,
        };
        assert_eq!(
            close_offsets(3, Some(split)).unwrap(),
            vec![(Offset::CloseYesterday, 3)]
        );
    }

    #[test]
    fn shfe_close_today_only_when_no_yesterday() {
        // 昨仓为 0：退化为单笔平今。
        let split = TodayYdSplit {
            today: 2,
            yesterday: 0,
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

    // ---- SHFE 市价单 → 穿价限价（GFD）----

    use crate::api::PlaceDefer;

    fn market_req(side: Side, reduce_only: bool) -> PlaceOrderRequest {
        PlaceOrderRequest {
            order_type: OrderType::Market,
            price: None,
            side,
            reduce_only,
            ..place_req()
        }
    }

    #[test]
    fn shfe_market_buy_open_uses_ask_plus_drift_ceiled_to_tick() {
        // 买开：ask 3205 + 1 tick（tick=1）= 3206，向上取整不劣于穿价。
        let req = market_req(Side::Buy, false);
        let out = prepare_shfe_market_limit(
            req,
            Some(MarketRef {
                bid: d(3204.0),
                ask: d(3205.0),
            }),
            d(1.0),
            1,
        )
        .unwrap();
        assert_eq!(out.order_type, OrderType::Limit);
        assert_eq!(out.price, Some(d(3206.0)));
        assert_eq!(out.quantity, Decimal::from(2));
    }

    #[test]
    fn shfe_market_sell_open_uses_bid_minus_drift_floored_to_tick() {
        // 卖开：bid 3204 − 2 tick（tick=0.5）= 3203，向下取整。
        let req = market_req(Side::Sell, false);
        let out = prepare_shfe_market_limit(
            req,
            Some(MarketRef {
                bid: d(3204.0),
                ask: d(3205.0),
            }),
            d(0.5),
            2,
        )
        .unwrap();
        assert_eq!(out.price, Some(d(3203.0)));
    }

    #[test]
    fn shfe_market_close_long_uses_best_bid_no_drift() {
        // 平多 = sell+reduce_only：best bid，不漂移（漂移只作用于开仓）。
        let req = market_req(Side::Sell, true);
        let out = prepare_shfe_market_limit(
            req,
            Some(MarketRef {
                bid: d(3204.0),
                ask: d(3205.0),
            }),
            d(1.0),
            5,
        )
        .unwrap();
        assert_eq!(out.price, Some(d(3204.0)));
    }

    #[test]
    fn shfe_market_close_short_uses_best_ask_no_drift() {
        // 平空 = buy+reduce_only：best ask，不漂移。
        let req = market_req(Side::Buy, true);
        let out = prepare_shfe_market_limit(
            req,
            Some(MarketRef {
                bid: d(3204.0),
                ask: d(3205.0),
            }),
            d(1.0),
            5,
        )
        .unwrap();
        assert_eq!(out.price, Some(d(3205.0)));
    }

    #[test]
    fn shfe_market_price_snaps_to_tick_grid() {
        // ask=3205.3、tick=1、漂移 0：买单向上取整到 3206（保证穿价不被取整削弱）。
        let req = market_req(Side::Buy, false);
        let out = prepare_shfe_market_limit(
            req,
            Some(MarketRef {
                bid: d(3204.7),
                ask: d(3205.3),
            }),
            d(1.0),
            0,
        )
        .unwrap();
        assert_eq!(out.price, Some(d(3206.0)));
    }

    #[test]
    fn shfe_market_without_book_defers() {
        let req = market_req(Side::Buy, false);
        assert_eq!(
            prepare_shfe_market_limit(req, None, d(1.0), 1).unwrap_err(),
            PlaceDefer::DataNotReady
        );
    }

    #[test]
    fn shfe_market_with_nonpositive_tick_defers() {
        let req = market_req(Side::Buy, false);
        assert_eq!(
            prepare_shfe_market_limit(
                req,
                Some(MarketRef {
                    bid: d(1.0),
                    ask: d(1.0)
                }),
                d(0.0),
                1
            )
            .unwrap_err(),
            PlaceDefer::DataNotReady
        );
    }

    #[test]
    fn shfe_limit_request_passes_through() {
        // 限价单（开或平）原样返回，不读盘口/tick。
        let req = place_req();
        let out = prepare_shfe_market_limit(req, None, d(0.0), 9).unwrap();
        assert_eq!(out.order_type, OrderType::Limit);
        assert_eq!(out.price, Some(d(3205.0)));
    }

    fn d(x: f64) -> Decimal {
        Decimal::try_from(x).unwrap()
    }
}
