//! REST 直下【市价开仓】单的一次性编排：缺规格/缺 mark 时按需从主网自取写共享表，
//! 再走与规则路径**同一个**纯函数 [`open_price`] 做档位漂移、按步长对齐数量。
//!
//! 与规则路径的区别仅在"数据从哪来"：规则靠 worker 周期订阅，此处下单时一次性拉取；
//! 漂移计算逻辑只有一份（DRY）。开仓只用 **mark**、**不要求盘口 book**，
//! 也**不碰**平仓专用的对手价 / 价差闸门。
//!
//! 网络以注入 `fetcher` 的方式实现（与 [`super::market::warm_missing_specs`] 同风格），
//! 全程零 HTTP 依赖、可单测。

use std::future::Future;

use trading_core::order_rule::{align_amount, OrderRuleTable};
use trading_core::quote::{QuoteTable, QuoteUpdate};
use trading_models::{Instrument, InstrumentType, MarketKind, Venue};

use super::prep::PrepConfig;
use crate::api::PlaceOrderRequest;

/// 期权标的 → QuoteTable 键。
fn quote_instrument(symbol: &str) -> Instrument {
    Instrument::new(Venue::Deribit, MarketKind::Option, symbol)
}

/// 一次性按需拉取的目标：合约规格 或 mark 行情。入参为标的名。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnsureNeed {
    Spec(String),
    Mark(String),
}

/// 编排失败（调用方据此把订单置 REJECTED 并原样展示原因）。
#[derive(Debug)]
pub enum EnsureError {
    /// 规格拉取失败（含主网无此合约）。
    SpecFetch(String),
    /// 规格应答解析失败。
    SpecParse(String),
    /// mark 拉取失败。
    MarkFetch(String),
    /// ticker 应答里没有 mark。
    MarkMissing,
}

impl std::fmt::Display for EnsureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EnsureError::SpecFetch(e) => write!(f, "无法获取合约规格：{e}"),
            EnsureError::SpecParse(e) => write!(f, "合约规格解析失败：{e}"),
            EnsureError::MarkFetch(e) => write!(f, "无法获取行情 mark：{e}"),
            EnsureError::MarkMissing => write!(f, "行情应答缺少 mark"),
        }
    }
}

impl std::error::Error for EnsureError {}

/// 把一笔 REST 市价开仓单准备成可直接挂出的限价请求：
///
/// 1. 规格缺失 → `fetcher(Spec)` → 解析写规则表；
/// 2. mark 缺失 → `fetcher(Mark)` → 写 QuoteTable（只写 mark，不动 book）；
/// 3. [`open_price`] 档位漂移 + 数量按步长对齐，返回 `order_type=Limit` 的请求。
///
/// 前置（调用方保证）：`order_type=Market` 且 `reduce_only=false`。
pub async fn prepare_market_open<F, Fut>(
    mut request: PlaceOrderRequest,
    rules: &OrderRuleTable,
    quotes: &QuoteTable,
    cfg: PrepConfig,
    fetcher: F,
) -> Result<PlaceOrderRequest, EnsureError>
where
    F: Fn(EnsureNeed) -> Fut,
    Fut: Future<Output = anyhow::Result<serde_json::Value>>,
{
    let symbol = request.instrument.clone();
    let spec_key = trading_models::OrderRuleKey::new("deribit", InstrumentType::Option, &symbol);

    // 1) 规格缺失 → 一次性拉取主网 get_instrument，解析写规则表。
    if rules.get(&spec_key).is_none() {
        let value = fetcher(EnsureNeed::Spec(symbol.clone()))
            .await
            .map_err(|e| EnsureError::SpecFetch(e.to_string()))?;
        let fetched_ms = chrono::Utc::now().timestamp_millis();
        let row = super::spec::parse_instrument(&value.to_string(), fetched_ms)
            .map_err(|e| EnsureError::SpecParse(e.to_string()))?;
        rules.put(row);
    }
    // 此刻规格必在表内。
    let rule = rules
        .get(&spec_key)
        .ok_or_else(|| EnsureError::SpecParse("规格写入后读不到".into()))?;

    // 2) mark 缺失 → 一次性拉取主网 ticker，只写 mark（不动 book）。
    let qinst = quote_instrument(&symbol);
    let mut mark = quotes.get(&qinst).and_then(|q| q.mark);
    if mark.is_none() {
        let value = fetcher(EnsureNeed::Mark(symbol.clone()))
            .await
            .map_err(|e| EnsureError::MarkFetch(e.to_string()))?;
        let tick = super::market::parse_ticker(&value).ok_or(EnsureError::MarkMissing)?;
        mark = Some(tick.mark);
        quotes.apply(
            &qinst,
            QuoteUpdate {
                mark: Some(tick.mark),
                ts_recv_ms: chrono::Utc::now().timestamp_millis(),
                ..QuoteUpdate::default()
            },
        );
    }
    let mark = mark.ok_or(EnsureError::MarkMissing)?;

    // 3) 与规则路径同一个纯函数：档位漂移（基础 tick 缺省回退数量步长，同 prep 口径）。
    let base_tick = rule.price_tick.unwrap_or(rule.qty_step);
    let drift_price = super::pricing::open_price(
        mark,
        request.side,
        rule.price_tiers.as_deref().unwrap_or(&[]),
        base_tick,
        cfg.drift_ticks,
    );
    // 仅观测：记录触发下单时的 mark 与漂移价，便于事后核对（不参与任何业务判断）。
    tracing::info!(
        instrument = %symbol,
        %mark,
        tick = %base_tick,
        side = ?request.side,
        drift_price = %drift_price,
        drift_ticks = cfg.drift_ticks,
        "市价开仓漂移价已计算"
    );
    request.order_type = trading_models::OrderType::Limit;
    request.price = Some(drift_price);
    // 数量按步长对齐（张数口径）。
    request.quantity = align_amount(request.quantity, Some(rule.qty_step)).quantity();
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use trading_models::{OrderType, Side};

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    /// 多档规格（精确复刻 0.001 / 0.005 选档回归形状）。
    const INSTRUMENT: &str = r#"{
      "instrument_name":"BTC-25SEP26-86000-C","kind":"option",
      "base_currency":"BTC","contract_size":1,"tick_size":0.0001,
      "tick_size_steps":[
        {"above_price":0.001,"tick_size":0.001},
        {"above_price":0.005,"tick_size":0.005}
      ],
      "min_trade_amount":0.1
    }"#;

    /// 开仓请求（市价、非 reduce_only）。
    fn request() -> PlaceOrderRequest {
        PlaceOrderRequest {
            instrument: "BTC-25SEP26-86000-C".into(),
            instrument_type: InstrumentType::Option,
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            quantity: dec("1"),
            reduce_only: false,
            position_side: crate::api::OrderPositionSide::Both,
            time_in_force: None,
            client_order_id: "0xopen".into(),
        }
    }

    fn cfg() -> PrepConfig {
        PrepConfig {
            drift_ticks: 1,
            spread_threshold: dec("0.005"),
        }
    }

    fn ticker(mark: &str) -> serde_json::Value {
        serde_json::json!({
            "mark_price": mark,
            "best_bid_price": "0.015",
            "best_ask_price": "0.017"
        })
    }

    /// 构造记录调用次数、按需应答的 fetcher。
    fn make_fetcher(
        calls: Arc<AtomicUsize>,
        mark: &'static str,
    ) -> impl Fn(EnsureNeed) -> std::pin::Pin<Box<dyn Future<Output = anyhow::Result<serde_json::Value>>>>
    {
        move |need| {
            let c = Arc::clone(&calls);
            Box::pin(async move {
                c.fetch_add(1, Ordering::SeqCst);
                match need {
                    EnsureNeed::Spec(_) => Ok(serde_json::from_str(INSTRUMENT).unwrap()),
                    EnsureNeed::Mark(_) => Ok(ticker(mark)),
                }
            })
        }
    }

    #[tokio::test]
    async fn empty_tables_fetches_both_and_drifts_with_correct_tier() {
        // Arrange：空规则/行情表；mark=0.016 ≥ 0.005 → 必须选高档 tick 0.005。
        let rules = OrderRuleTable::default();
        let quotes = QuoteTable::default();
        let calls = Arc::new(AtomicUsize::new(0));

        // Act
        let out = prepare_market_open(
            request(),
            &rules,
            &quotes,
            cfg(),
            make_fetcher(Arc::clone(&calls), "0.016"),
        )
        .await
        .expect("prepared");

        // Assert：市价 → 限价；漂移价 = 0.016 − 1×0.005 = 0.011（不是低档的 0.015）。
        assert_eq!(out.order_type, OrderType::Limit);
        // 漂移 0.016−0.005=0.011，再按 tick 0.005 网格向下取整 → 0.010。
        assert_eq!(out.price, Some(dec("0.010")));
        // 规格 + mark 各拉一次。
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn existing_spec_and_mark_does_not_fetch() {
        // Arrange：先拉一遍把规格/mark 写好。
        let rules = OrderRuleTable::default();
        let quotes = QuoteTable::default();
        let seed_calls = Arc::new(AtomicUsize::new(0));
        prepare_market_open(
            request(),
            &rules,
            &quotes,
            cfg(),
            make_fetcher(Arc::clone(&seed_calls), "0.016"),
        )
        .await
        .unwrap();

        // Act：第二遍，表已齐全。
        let calls = Arc::new(AtomicUsize::new(0));
        let out = prepare_market_open(
            request(),
            &rules,
            &quotes,
            cfg(),
            make_fetcher(Arc::clone(&calls), "0.016"),
        )
        .await
        .unwrap();

        // Assert：不再发起任何拉取，漂移价不变。
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        // 漂移 0.016−0.005=0.011，再按 tick 0.005 网格向下取整 → 0.010。
        assert_eq!(out.price, Some(dec("0.010")));
    }

    #[tokio::test]
    async fn open_uses_mark_without_requiring_book() {
        // Arrange：规格就位，但只写 mark、不写盘口 book。
        let rules = OrderRuleTable::default();
        let quotes = QuoteTable::default();
        let pre_calls = Arc::new(AtomicUsize::new(0));
        // 先借一次完整流程把规格写好（同时写了 mark/book），随后清掉行情行、
        // 再以纯 mark 写回，模拟"有 mark 无 book"。
        prepare_market_open(
            request(),
            &rules,
            &quotes,
            cfg(),
            make_fetcher(Arc::clone(&pre_calls), "0.016"),
        )
        .await
        .unwrap();
        let inst = Instrument::new(Venue::Deribit, MarketKind::Option, "BTC-25SEP26-86000-C");
        quotes.remove(&inst);
        quotes.apply(
            &inst,
            QuoteUpdate {
                mark: Some(dec("0.016")),
                ts_recv_ms: 1,
                ..QuoteUpdate::default()
            },
        );

        // Act
        let calls = Arc::new(AtomicUsize::new(0));
        let out = prepare_market_open(
            request(),
            &rules,
            &quotes,
            cfg(),
            make_fetcher(Arc::clone(&calls), "0.016"),
        )
        .await
        .unwrap();

        // Assert：有 mark 无 book 即可漂移，无需再拉（不会因缺 book 失败）。
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        // 漂移 0.016−0.005=0.011，再按 tick 0.005 网格向下取整 → 0.010。
        assert_eq!(out.price, Some(dec("0.010")));
    }

    #[tokio::test]
    async fn spec_fetch_failure_is_error() {
        // Arrange：规格 fetcher 直接失败。
        let rules = OrderRuleTable::default();
        let quotes = QuoteTable::default();
        let out = prepare_market_open(request(), &rules, &quotes, cfg(), |need| async move {
            match need {
                EnsureNeed::Spec(_) => Err(anyhow::anyhow!("404 invalid instrument")),
                EnsureNeed::Mark(_) => unreachable!(),
            }
        })
        .await
        .unwrap_err();

        // Assert
        assert!(matches!(out, EnsureError::SpecFetch(_)));
        assert!(out.to_string().contains("无法获取合约规格"));
    }
}
