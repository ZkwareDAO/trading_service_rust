//! Deribit claim 前预检的纯转换：市价开仓档位漂移、市价平仓价差闸门、数量对齐、
//! 撤/查单单号路由。全程 [`Decimal`]、零网络，供 TDD。
//!
//! 行情（mark/bid/ask）由调用方（venue）从 `QuoteTable` 读出后以
//! [`MarketSnapshot`] 注入；合约规格取自已懒加载的 [`OrderRule`]。
//!
//! 见 `docs/deribit开平仓处理.md`、`docs/交易规则.md`。

use rust_decimal::Decimal;
use trading_core::order_rule::align_amount;
use trading_core::quote::QuoteTable;
use trading_models::{Instrument, MarketKind, OrderRule, OrderType, TopOfBook, Venue};

use crate::api::{OrderRef, PlaceDefer, PlaceOrderRequest};
use crate::deribit::pricing::{check_spread, close_price, open_price};

/// 从共享行情表读某 deribit 期权标的 mark/best bid/ask；无行或缺 book → None。
pub(crate) fn read_market(quotes: &QuoteTable, symbol: &str) -> Option<MarketSnapshot> {
    let q = quotes.get(&Instrument::new(Venue::Deribit, MarketKind::Option, symbol))?;
    let book: TopOfBook = q.book?;
    Some(MarketSnapshot {
        mark: q.mark.unwrap_or(Decimal::ZERO),
        bid: book.bid?,
        ask: book.ask?,
    })
}

/// 从内存行情表读出的某标的最新快照（venue 负责取值）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketSnapshot {
    pub mark: Decimal,
    pub bid: Decimal,
    pub ask: Decimal,
}

/// 预检参数：漂移档数与平仓价差阈值（来自 venue 配置）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrepConfig {
    pub drift_ticks: i64,
    pub spread_threshold: Decimal,
}

/// claim 前预检：按「开/平 × 市价/限价」矩阵转换请求。
///
/// - 限价（开或平）：原样返回（仅按规则做数量对齐）；
/// - 市价开仓：缺 mark → [`PlaceDefer::DataNotReady`]；否则 mark±N tick 漂移价；
/// - 市价平仓：缺 bid/ask → DataNotReady；价差超阈值 →
///   [`PlaceDefer::SpreadTooWide`]；否则对手 best bid/ask；
/// - 所有分支把数量对齐到 [`OrderRule::qty_step`]（取不到步长则原样）。
pub fn prepare(
    rule: &OrderRule,
    mut request: PlaceOrderRequest,
    market: Option<MarketSnapshot>,
    cfg: PrepConfig,
) -> Result<PlaceOrderRequest, PlaceDefer> {
    // 市价单必须有行情快照；限价单不需要。
    let needs_market = request.order_type == OrderType::Market;
    let market = if needs_market {
        market.ok_or(PlaceDefer::DataNotReady)?
    } else {
        // 非市价分支不会读 market，给一个占位值即可。
        MarketSnapshot {
            mark: Decimal::ZERO,
            bid: Decimal::ZERO,
            ask: Decimal::ZERO,
        }
    };

    if needs_market {
        let m = market;
        let price = if request.reduce_only {
            // 市价平仓：先价差闸门，再取对手价。
            check_spread(m.bid, m.ask, cfg.spread_threshold).map_err(|g| {
                PlaceDefer::SpreadTooWide {
                    instrument: request.instrument.clone(),
                    bid: m.bid,
                    ask: m.ask,
                    spread: g.spread,
                    threshold: g.threshold,
                }
            })?;
            close_price(m.bid, m.ask, request.side)
        } else {
            // 市价开仓：档位漂移。
            open_price(
                m.mark,
                request.side,
                rule.price_tiers.as_deref().unwrap_or(&[]),
                rule.price_tick.unwrap_or(rule.qty_step),
                cfg.drift_ticks,
            )
        };
        // 仅观测：记录预检时的 mark/bid/ask 与最终委托价（不参与业务判断）。
        tracing::info!(
            instrument = %request.instrument,
            mark = %m.mark,
            bid = %m.bid,
            ask = %m.ask,
            side = ?request.side,
            reduce_only = request.reduce_only,
            prepared_price = %price,
            "市价预检委托价已计算"
        );
        // 市价 → 限价（用算出的价）。
        request.order_type = OrderType::Limit;
        request.price = Some(price);
    }

    // 数量对齐到步长（张数口径，四舍五入；无步长原样）。
    let aligned = align_amount(request.quantity, Some(rule.qty_step)).quantity();
    request.quantity = aligned;
    Ok(request)
}

/// Deribit 撤/查单单号路由：**主用 label（=我们的 cid）**——调用方只传 cid
/// 即可撤单；label 理论上始终存在。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderIdRoute {
    ExchangeOrderId(String),
    Label(String),
}

/// 选择撤/查单使用的单号：优先 cid（label）。
pub fn route_order_ref(order: &OrderRef) -> OrderIdRoute {
    OrderIdRoute::Label(order.client_order_id.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use trading_models::{InstrumentType, PriceTier, QtyUnit, Side};

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    /// 期权规则：分档 tick、qty_step 0.1。
    fn rule() -> OrderRule {
        let mut r = OrderRule::new(
            "deribit",
            "BTC-1",
            InstrumentType::Option,
            QtyUnit::Contract,
            Decimal::ONE,
            "BTC",
            "BTC",
            dec("0.1"),
            "deribit:get_instrument",
            0,
        );
        r.price_tiers = Some(vec![
            PriceTier {
                above_price: dec("0"),
                tick: dec("0.0005"),
            },
            PriceTier {
                above_price: dec("0.005"),
                tick: dec("0.005"),
            },
        ]);
        r
    }

    fn cfg() -> PrepConfig {
        PrepConfig {
            drift_ticks: 5,
            spread_threshold: dec("0.005"),
        }
    }

    fn market_request(reduce_only: bool, side: Side) -> PlaceOrderRequest {
        PlaceOrderRequest {
            instrument: "BTC-1".into(),
            instrument_type: InstrumentType::Option,
            side,
            order_type: OrderType::Market,
            price: None,
            quantity: dec("1"),
            reduce_only,
            position_side: crate::api::OrderPositionSide::Both,
            time_in_force: None,
            client_order_id: "0xclient".into(),
        }
    }

    fn snap_ok() -> MarketSnapshot {
        MarketSnapshot {
            mark: dec("0.5"),
            bid: dec("0.400"),
            ask: dec("0.404"),
        }
    }

    // ---------- 市价开仓：漂移 ----------

    #[test]
    fn market_open_drifts_to_limit() {
        let out = prepare(
            &rule(),
            market_request(false, Side::Buy),
            Some(snap_ok()),
            cfg(),
        )
        .expect("ok");
        assert_eq!(out.order_type, OrderType::Limit);
        // mark 0.5 → tick 0.005；买低 5 tick=0.475。
        assert_eq!(out.price, Some(dec("0.475")));
        assert_eq!(out.quantity, dec("1"));
    }

    #[test]
    fn market_open_without_market_is_data_not_ready() {
        let err = prepare(&rule(), market_request(false, Side::Buy), None, cfg()).unwrap_err();
        assert_eq!(err, PlaceDefer::DataNotReady);
    }

    // ---------- 市价平仓：价差闸门 + 对手价 ----------

    #[test]
    fn market_close_narrow_spread_uses_opposite_price() {
        let out = prepare(
            &rule(),
            market_request(true, Side::Buy),
            Some(snap_ok()),
            cfg(),
        )
        .expect("ok");
        assert_eq!(out.order_type, OrderType::Limit);
        // BUY 平空吃卖一 0.404。
        assert_eq!(out.price, Some(dec("0.404")));
    }

    #[test]
    fn market_close_wide_spread_defers_with_book() {
        let wide = MarketSnapshot {
            mark: dec("0.5"),
            bid: dec("0.400"),
            ask: dec("0.406"),
        };
        let err =
            prepare(&rule(), market_request(true, Side::Sell), Some(wide), cfg()).unwrap_err();
        assert_eq!(
            err,
            PlaceDefer::SpreadTooWide {
                instrument: "BTC-1".into(),
                bid: dec("0.400"),
                ask: dec("0.406"),
                spread: dec("0.006"),
                threshold: dec("0.005"),
            }
        );
    }

    #[test]
    fn market_close_without_book_is_data_not_ready() {
        let err = prepare(&rule(), market_request(true, Side::Buy), None, cfg()).unwrap_err();
        assert_eq!(err, PlaceDefer::DataNotReady);
    }

    // ---------- 限价：原样 ----------

    #[test]
    fn limit_open_and_close_are_unchanged() {
        let mut req = market_request(false, Side::Buy);
        req.order_type = OrderType::Limit;
        req.price = Some(dec("0.42"));
        let out = prepare(&rule(), req.clone(), None, cfg()).expect("ok");
        // 类型/价格未被改写。
        assert_eq!(out.order_type, OrderType::Limit);
        assert_eq!(out.price, Some(dec("0.42")));
    }

    // ---------- 数量对齐 ----------

    #[test]
    fn quantity_is_aligned_to_step() {
        let mut req = market_request(false, Side::Buy);
        req.quantity = dec("1.04");
        let out = prepare(&rule(), req, Some(snap_ok()), cfg()).expect("ok");
        // qty_step 0.1：1.04 四舍五入到 1.0。
        assert_eq!(out.quantity, dec("1.0"));
    }

    // ---------- 单号路由 ----------

    #[test]
    fn route_prefers_label_cid_regardless_of_exchange_id() {
        // 即使带了交易所单号，也优先 label（cid）——传 cid 即可撤单。
        let r = OrderRef::new("0xclient").with_exchange("ETH-331562");
        assert_eq!(route_order_ref(&r), OrderIdRoute::Label("0xclient".into()));
        let only_cid = OrderRef::new("0xclient");
        assert_eq!(
            route_order_ref(&only_cid),
            OrderIdRoute::Label("0xclient".into())
        );
    }
}
