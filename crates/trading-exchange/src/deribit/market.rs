//! Deribit 公共行情 **协议层（feed）**：只保留该所协议纯函数 + 一个 [`DeribitMarketFeed`]。
//!
//! 生命周期（建连/退避/Ping/看门狗/500ms 对账/重连整体重放）已收口到
//! [`crate::market_ws`] 的 `WsDriver`；本模块只描述 deribit：
//! - 价格频道 `ticker.<instrument>.100ms`：ticker 帧原子带 mark + best bid/ask，
//!   `write_quote` 字段级写 `QuoteTable{mark, book}` 并回调 observer（与旧 worker 等价）；
//! - K线频道 `chart.trades.<instrument>.1`：仅在分钟桶翻转（收盘）时回调定稿 bar；
//! - `on_desired_async` 对缺规格标的拉 `public/get_instrument` 预热（旧 warm_specs 搬入）。
//!
//! Deribit 行情订阅渠道与符号映射见本模块协议函数。

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::SinkExt;
use rust_decimal::Decimal;
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use trading_core::order_rule::OrderRuleTable;
use trading_core::quote::{QuoteTable, QuoteUpdate};
use trading_models::{Instrument, MarketKind, TopOfBook, Venue};

use crate::market::{Kline, KlineInterval, PriceBasis, PriceTick};
use crate::market_hub::SubsPlan;
use crate::market_ws::{ChannelPlan, FeedMessage, WsMarketFeed};
use crate::port::QuoteObserver;
use crate::ws::WsStream;

/// deribit 交易所标识（写入 Kline.venue）。
pub const VENUE: &str = "deribit";

/// 价格频道名：`ticker.<instrument>.100ms`。
pub(crate) fn ticker_channel(symbol: &str) -> String {
    format!("ticker.{symbol}.100ms")
}

/// 1 分钟 K线频道名：`chart.trades.<instrument>.1`（resolution=1 分钟）。
pub(crate) fn chart_channel(symbol: &str) -> String {
    format!("chart.trades.{symbol}.1")
}

/// 从合约集合得到要订阅的 ticker 频道集合。
fn price_channels(symbols: &BTreeSet<String>) -> HashSet<String> {
    symbols.iter().map(|s| ticker_channel(s)).collect()
}

/// 从合约集合得到要订阅的 chart.trades 频道集合。
fn bar_channels(symbols: &BTreeSet<String>) -> HashSet<String> {
    symbols.iter().map(|s| chart_channel(s)).collect()
}

/// 找出「属于 deribit 期权、但规格表尚无行」的标的（有序去重）。
pub fn missing_spec_symbols(symbols: &BTreeSet<String>, rules: &OrderRuleTable) -> Vec<String> {
    symbols
        .iter()
        .filter(|symbol| {
            let key = trading_models::OrderRuleKey::new(
                VENUE,
                trading_models::InstrumentType::Option,
                symbol.as_str(),
            );
            rules.get(&key).is_none()
        })
        .cloned()
        .collect()
}

/// 对缺规格标的逐个拉 public `get_instrument`、解析后写规格表。
///
/// 单个标的拉取/解析失败仅告警跳过（下轮重试），不影响订阅 diff。返回本次新写入条数。
pub async fn warm_missing_specs(
    symbols: &BTreeSet<String>,
    rules: &OrderRuleTable,
    http: &reqwest::Client,
) -> usize {
    let wanted = missing_spec_symbols(symbols, rules);
    let mut written = 0usize;
    for symbol in wanted {
        let result = crate::deribit::rpc::call(
            http.clone(),
            crate::deribit::rpc::rest_base(false),
            "public/get_instrument",
            serde_json::json!({ "instrument_name": symbol }),
            None,
        )
        .await;
        match result {
            Ok(result) => {
                let fetched = chrono::Utc::now().timestamp_millis();
                match crate::deribit::spec::parse_instrument(&result.to_string(), fetched) {
                    Ok(row) => {
                        tracing::info!(instrument = symbol, "合约规格已预热写表");
                        rules.put(row);
                        written += 1;
                    }
                    Err(e) => {
                        tracing::warn!(instrument = symbol, error = %e, "规格解析失败，下轮重试")
                    }
                }
            }
            Err(e) => tracing::warn!(instrument = symbol, error = %e, "规格拉取失败，下轮重试"),
        }
    }
    written
}

/// ticker 帧解析出的定价要素（mark + book）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TickerTick {
    pub mark: Decimal,
    pub bid: Option<Decimal>,
    pub ask: Option<Decimal>,
}

fn dec_field(v: Option<&Value>) -> Option<Decimal> {
    v.and_then(|x| match x {
        Value::String(s) => s.parse().ok(),
        Value::Number(_) => Decimal::from_str_exact(&x.to_string()).ok(),
        Value::Null => None,
        _ => None,
    })
}

/// 解析 ticker data：mark_price 必填；best_bid_price/best_ask_price 可缺侧。
pub fn parse_ticker(data: &Value) -> Option<TickerTick> {
    let mark = dec_field(data.get("mark_price"))?;
    Some(TickerTick {
        mark,
        bid: dec_field(data.get("best_bid_price")),
        ask: dec_field(data.get("best_ask_price")),
    })
}

/// 一帧 subscription：若是 `ticker.*.100ms`，返回 (symbol, tick)；否则 None。
pub fn parse_ticker_subscription(raw: &str) -> Option<(String, TickerTick)> {
    let (channel, data) = crate::deribit::mapping::parse_subscription(raw).ok()??;
    let symbol = channel
        .strip_prefix("ticker.")
        .and_then(|rest| rest.strip_suffix(".100ms"))?
        .to_string();
    let tick = parse_ticker(&data)?;
    Some((symbol, tick))
}

/// chart.trades 一根蜡烛（deribit v2 字段：t=桶起点 ms，o/h/l/c 字符串价，v 量）。
#[derive(Debug, Clone, Copy)]
struct RawCandle {
    start_ms: i64,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
}

fn parse_candle(v: &Value) -> Option<RawCandle> {
    let get_dec = |k: &str| dec_field(v.get(k));
    Some(RawCandle {
        start_ms: v.get("t")?.as_i64()?,
        open: get_dec("o")?,
        high: get_dec("h")?,
        low: get_dec("l")?,
        close: get_dec("c")?,
        volume: get_dec("v").unwrap_or(Decimal::ZERO),
    })
}

/// 解析一帧 chart.trades：data 为 `{ "<instrument>": [ candles... ] }`（聚合/推送同形），
/// 取出每个 instrument 的蜡烛数组（解析失败的元素跳过）。
fn parse_chart_frame(raw: &str) -> Vec<(String, Vec<RawCandle>)> {
    let Ok(v) = serde_json::from_str::<Value>(raw) else {
        return Vec::new();
    };
    let Some(channel) = v.pointer("/params/channel").and_then(|x| x.as_str()) else {
        return Vec::new();
    };
    let Some(symbol) = channel
        .strip_prefix("chart.trades.")
        .and_then(|rest| rest.strip_suffix(".1"))
    else {
        return Vec::new();
    };
    // data 嵌在 params 下（subscription 信封）。chart.trades.<inst> 频道已按合约
    // 区分，常见为**裸蜡烛数组**；也兼容 { "<instrument>": [...] } 与 {"ticks":[...]}。
    let data = v.pointer("/params/data");
    let arr = data
        .and_then(|d| d.as_array())
        .or_else(|| data.and_then(|d| d.get(symbol).and_then(|x| x.as_array())))
        .or_else(|| data.and_then(|d| d.get("ticks").and_then(|x| x.as_array())));
    let Some(arr) = arr else { return Vec::new() };
    let candles: Vec<RawCandle> = arr.iter().filter_map(parse_candle).collect();
    if candles.is_empty() {
        Vec::new()
    } else {
        vec![(symbol.to_string(), candles)]
    }
}

// =====================================================================
// feed：WsDriver 协议端口实现
// =====================================================================

/// deribit 行情协议（恒定主网、无认证）。进程一个实例，被 `WsDriver` 驱动。
pub struct DeribitMarketFeed {
    http: reqwest::Client,
    rules: Arc<OrderRuleTable>,
    /// chart.trades 收盘判定：每合约最近一根「成形中」蜡烛；观察到下一桶起点时，
    /// 上一根即定稿。
    last_candle: Mutex<HashMap<String, RawCandle>>,
}

impl DeribitMarketFeed {
    pub fn new(http: reqwest::Client, rules: Arc<OrderRuleTable>) -> Self {
        Self {
            http,
            rules,
            last_candle: Mutex::new(HashMap::new()),
        }
    }

    /// 公共行情 WS：恒定主网（即使账户是 testnet），无尾斜杠。
    fn market_ws_url() -> &'static str {
        "wss://www.deribit.com/ws/api/v2"
    }

    /// 组装 subscribe/unsubscribe JSON-RPC 帧。
    fn rpc_frame(method: &str, channels: &[String]) -> String {
        serde_json::json!({
            "jsonrpc": "2.0", "id": 1,
            "method": method,
            "params": { "channels": channels },
        })
        .to_string()
    }

    /// 把一根已确认收盘的蜡烛转为统一 [`Kline`]。
    fn finalize_bar(symbol: &str, c: &RawCandle) -> Kline {
        Kline {
            venue: VENUE.to_string(),
            symbol: symbol.to_string(),
            interval: KlineInterval::M1,
            trading_day: crate::market_ws::trading_day_of_utc_ms(c.start_ms),
            start_ms: c.start_ms,
            end_ms: c.start_ms + 60_000,
            open: c.open,
            high: c.high,
            low: c.low,
            close: c.close,
            // 三家原生 K线量为浮点，统一 Kline.volume 取整（手/张口径由各所定义）。
            volume: c.volume.to_string().parse::<i64>().unwrap_or(0),
            quote_volume: Decimal::ZERO,
            // deribit 当前无 K线扩展指标。
            extras: BTreeMap::new(),
        }
    }
}

#[async_trait]
impl WsMarketFeed for DeribitMarketFeed {
    fn url(&self) -> &str {
        Self::market_ws_url()
    }

    fn reconcile(
        &self,
        price_symbols: &BTreeSet<String>,
        bar_symbols: &BTreeSet<String>,
        subscribed: &HashSet<String>,
    ) -> ChannelPlan {
        // 同一合约可能同时要价格与 K线：ticker 与 chart.trades 是两个不同频道，各自 diff。
        let mut desired = price_channels(price_symbols);
        desired.extend(bar_channels(bar_symbols));
        let plan = SubsPlan::diff(&desired, subscribed);
        ChannelPlan {
            subscribe: plan.to_sub,
            unsubscribe: plan.to_unsub,
        }
    }

    async fn send_subscribe(&self, ws: &mut WsStream, channels: &[String]) -> anyhow::Result<()> {
        ws.send(WsMessage::Text(
            Self::rpc_frame("public/subscribe", channels).into(),
        ))
        .await?;
        Ok(())
    }

    async fn send_unsubscribe(&self, ws: &mut WsStream, channels: &[String]) -> anyhow::Result<()> {
        ws.send(WsMessage::Text(
            Self::rpc_frame("public/unsubscribe", channels).into(),
        ))
        .await?;
        Ok(())
    }

    async fn on_desired_async(
        &self,
        price_symbols: &BTreeSet<String>,
        _bar_symbols: &BTreeSet<String>,
    ) {
        // 旧 worker 行为：先为缺规格的期权标的预热 get_instrument，再订阅。
        let n = warm_missing_specs(price_symbols, &self.rules, &self.http).await;
        if n > 0 {
            tracing::info!(count = n, "reconcile 预热合约规格");
        }
    }

    fn decode(&self, text: &str) -> FeedMessage {
        let mut out = FeedMessage::default();

        // 价格：ticker.*.100ms。
        if let Some((symbol, tick)) = parse_ticker_subscription(text) {
            let now = chrono::Utc::now().timestamp_millis();
            out.ticks.push(PriceTick {
                symbol,
                basis: PriceBasis::Mark,
                price: tick.mark,
                best_bid: tick.bid,
                best_ask: tick.ask,
                volume: None,
                event_ms: now,
            });
        }

        // K线：chart.trades 仅在桶翻转时把上一根定稿。
        for (symbol, mut candles) in parse_chart_frame(text) {
            // 同帧多根按起点升序处理（补推/批量）。
            candles.sort_by_key(|c| c.start_ms);
            let mut last = self.last_candle.lock().expect("deribit candle lock");
            for c in candles {
                let prev = last.get(&symbol).copied();
                match prev {
                    Some(prev) if prev.start_ms < c.start_ms => {
                        // 见到新桶：上一根定稿。
                        out.bars.push(Self::finalize_bar(&symbol, &prev));
                        last.insert(symbol.clone(), c);
                    }
                    Some(_) => {
                        // 同桶更新或乱序回退：覆盖/记录，不定稿（回退不回拨已发布 bar）。
                        last.insert(symbol.clone(), c);
                    }
                    None => {
                        // 首见：只记录，不定稿。
                        last.insert(symbol.clone(), c);
                    }
                }
            }
        }

        out
    }

    fn write_quote(
        &self,
        tick: &PriceTick,
        quotes: &QuoteTable,
        observer: Option<&dyn QuoteObserver>,
    ) {
        let now = chrono::Utc::now().timestamp_millis();
        let book = TopOfBook::new(tick.best_bid, tick.best_ask);
        let update = QuoteUpdate {
            last: None,
            mark: Some(tick.price),
            mid: None,
            book: Some(book),
            ts_exchange_ms: now,
            ts_recv_ms: now,
        };
        let inst = Instrument::new(Venue::Deribit, MarketKind::Option, &tick.symbol);
        quotes.apply(&inst, update);
        if let Some(ob) = observer {
            ob.on_quote(Venue::Deribit, MarketKind::Option, &tick.symbol, tick.price);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn syms(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn ticker_channel_name() {
        assert_eq!(ticker_channel("BTC-1"), "ticker.BTC-1.100ms");
        assert_eq!(chart_channel("BTC-1"), "chart.trades.BTC-1.1");
    }

    #[test]
    fn reconcile_maps_symbols_to_channels_and_diffs() {
        let feed = Arc::new(DeribitMarketFeed::new(
            reqwest::Client::new(),
            Arc::new(OrderRuleTable::default()),
        ));
        // 已订阅 b 的 ticker；期望价格 {a,b}、K线 {b}。
        let mut subscribed = HashSet::new();
        subscribed.insert("ticker.b.100ms".to_string());
        let plan = feed.reconcile(&syms(&["a", "b"]), &syms(&["b"]), &subscribed);
        // 新增：a 的 ticker + b 的 chart.trades；无退订。
        assert_eq!(
            plan.subscribe,
            vec!["chart.trades.b.1".to_string(), "ticker.a.100ms".to_string()]
        );
        assert!(plan.unsubscribe.is_empty());
    }

    #[test]
    fn reconcile_unsubscribes_dropped_channels() {
        let feed = Arc::new(DeribitMarketFeed::new(
            reqwest::Client::new(),
            Arc::new(OrderRuleTable::default()),
        ));
        let subscribed = HashSet::from(["ticker.gone.100ms".to_string()]);
        let plan = feed.reconcile(&syms(&[]), &syms(&[]), &subscribed);
        assert!(plan.subscribe.is_empty());
        assert_eq!(plan.unsubscribe, vec!["ticker.gone.100ms".to_string()]);
    }

    #[test]
    fn missing_spec_symbols_lists_unfetched_options() {
        let rules = OrderRuleTable::default();
        // 预置一个已有规格的标的。
        rules.put(trading_models::OrderRule::new(
            VENUE,
            "BTC-HAVE",
            trading_models::InstrumentType::Option,
            trading_models::QtyUnit::Contract,
            Decimal::ONE,
            "BTC",
            "BTC",
            dec("0.1"),
            "deribit:get_instrument",
            0,
        ));
        let missing = missing_spec_symbols(&syms(&["BTC-HAVE", "BTC-NEED"]), &rules);
        assert_eq!(missing, vec!["BTC-NEED".to_string()]);
    }

    #[test]
    fn parses_ticker_fields_and_missing_sides() {
        let data = serde_json::json!({
            "mark_price": 0.5, "best_bid_price": 0.49, "best_ask_price": 0.51
        });
        let t = parse_ticker(&data).expect("tick");
        assert_eq!(t.mark, dec("0.5"));
        assert_eq!(t.bid, Some(dec("0.49")));
        assert_eq!(t.ask, Some(dec("0.51")));

        let one = serde_json::json!({"mark_price":0.5,"best_bid_price":0.49});
        assert!(parse_ticker(&one).expect("tick").ask.is_none());
        assert!(parse_ticker(&serde_json::json!({"best_bid_price":0.49})).is_none());
    }

    #[test]
    fn parses_ticker_subscription_frame() {
        let raw = r#"{
          "jsonrpc":"2.0","method":"subscription",
          "params":{"channel":"ticker.BTC-1.100ms","data":{
            "mark_price":0.5,"best_bid_price":0.49,"best_ask_price":0.51
          }}
        }"#;
        let (symbol, t) = parse_ticker_subscription(raw).expect("parsed");
        assert_eq!(symbol, "BTC-1");
        assert_eq!(t.mark, dec("0.5"));
    }

    #[test]
    fn non_ticker_frame_is_none() {
        let raw = r#"{"jsonrpc":"2.0","method":"subscription",
            "params":{"channel":"user.orders.option.BTC.raw","data":{}}}"#;
        assert!(parse_ticker_subscription(raw).is_none());
    }

    #[test]
    fn decode_ticker_frame_to_price_tick() {
        let feed =
            DeribitMarketFeed::new(reqwest::Client::new(), Arc::new(OrderRuleTable::default()));
        let raw = r#"{"jsonrpc":"2.0","method":"subscription",
          "params":{"channel":"ticker.BTC-1.100ms","data":{
            "mark_price":"0.5","best_bid_price":"0.49","best_ask_price":"0.51"}}}"#;
        let msg = feed.decode(raw);
        assert_eq!(msg.ticks.len(), 1);
        assert_eq!(msg.ticks[0].symbol, "BTC-1");
        assert_eq!(msg.ticks[0].basis, PriceBasis::Mark);
        assert!(msg.bars.is_empty());
    }

    #[test]
    fn decode_chart_finalizes_previous_candle_on_bucket_flip() {
        let feed =
            DeribitMarketFeed::new(reqwest::Client::new(), Arc::new(OrderRuleTable::default()));
        // 第一帧：桶 60000 成形中 → 不定稿（chart.trades 推送 data 为裸蜡烛数组）。
        let f1 = r#"{"params":{"channel":"chart.trades.BTC-1.1","data":[
          {"t":60000,"o":"100","h":"110","l":"99","c":"105","v":"2"}]}}"#;
        assert!(feed.decode(f1).bars.is_empty());
        // 第二帧：同桶更新 → 仍不定稿。
        let f2 = r#"{"params":{"channel":"chart.trades.BTC-1.1","data":[
          {"t":60000,"o":"100","h":"111","l":"99","c":"108","v":"3"}]}}"#;
        assert!(feed.decode(f2).bars.is_empty());
        // 第三帧：新桶 120000 → 上一根（以最终更新值）定稿。
        let f3 = r#"{"params":{"channel":"chart.trades.BTC-1.1","data":[
          {"t":120000,"o":"108","h":"109","l":"107","c":"108","v":"1"}]}}"#;
        let bars = feed.decode(f3).bars;
        assert_eq!(bars.len(), 1);
        let bar = &bars[0];
        assert_eq!(bar.symbol, "BTC-1");
        assert_eq!(bar.venue, VENUE);
        assert_eq!(bar.start_ms, 60000);
        assert_eq!(bar.end_ms, 120_000);
        assert_eq!(bar.open, dec("100"));
        assert_eq!(bar.high, dec("111"));
        assert_eq!(bar.low, dec("99"));
        assert_eq!(bar.close, dec("108"));
        assert_eq!(bar.volume, 3);
    }

    #[test]
    fn write_quote_marks_table() {
        let feed =
            DeribitMarketFeed::new(reqwest::Client::new(), Arc::new(OrderRuleTable::default()));
        let quotes = Arc::new(QuoteTable::new());
        let tick = PriceTick {
            symbol: "BTC-1".into(),
            basis: PriceBasis::Mark,
            price: dec("0.5"),
            best_bid: Some(dec("0.49")),
            best_ask: Some(dec("0.51")),
            volume: None,
            event_ms: 1,
        };
        feed.write_quote(&tick, &quotes, None);
        let inst = Instrument::new(Venue::Deribit, MarketKind::Option, "BTC-1");
        let q = quotes.get(&inst).expect("quote written");
        assert_eq!(q.mark, Some(dec("0.5")));
        assert_eq!(q.book.unwrap().bid, Some(dec("0.49")));
    }
}
