//! Hyperliquid 公共行情 **协议层（feed）**：生命周期收口到 [`crate::market_ws`]。
//!
//! 订阅口径（与旧 worker 等价）：
//! - `allMids`（全量，建连即常驻）：data.mids 的 coin→mid，写 `Quote.mid`（全市场）；
//! - `activeAssetCtx`（仅价格期望集内的币）：data.ctx.markPx 写 `Quote.mark`；
//! - `candle`（K线期望集）：1m 原生蜡烛，桶翻转时把上一根定稿回调。
//!
//! 写价后回调 `QuoteObserver` 驱动仓位重估。频道/帧解析为纯函数。
//! Hyperliquid 行情订阅与符号映射见本模块。

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Mutex;

use async_trait::async_trait;
use futures_util::SinkExt;
use rust_decimal::Decimal;
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use trading_core::quote::{QuoteTable, QuoteUpdate};
use trading_models::{Instrument, MarketKind, Venue};

use crate::market::{Kline, KlineInterval, PriceBasis, PriceTick};
use crate::market_hub::SubsPlan;
use crate::market_ws::{ChannelPlan, FeedMessage, WsMarketFeed};
use crate::port::QuoteObserver;
use crate::ws::WsStream;

/// hyperliquid 交易所标识。
pub const VENUE: &str = "hyperliquid";

/// 公共行情（恒定正式网）WS 地址。
const MARKET_WS: &str = "wss://api.hyperliquid.xyz/ws";
/// activeAssetCtx 必带 user 字段但服务端不校验身份/持仓，用全零地址作中性占位。
const CTX_ROUTE_USER: &str = "0x0000000000000000000000000000000000000000";

/// activeAssetCtx 频道 id（reconcile 的抽象频道键）。
fn ctx_channel(coin: &str) -> String {
    format!("activeAssetCtx:{coin}")
}
/// candle 1m 频道 id。
fn candle_channel(coin: &str) -> String {
    format!("candle:{coin}")
}

// =====================================================================
// 纯函数解析（零网络，单测覆盖，逻辑与旧 worker 一致）
// =====================================================================

/// JSON 值（字符串/数字）→ Decimal。
fn dec_of(v: &Value) -> Option<Decimal> {
    match v {
        Value::String(s) => s.parse().ok(),
        Value::Number(_) => Decimal::from_str_exact(&v.to_string()).ok(),
        _ => None,
    }
}

/// 解析 allMids 帧为 (coin, mid) 列表（按 coin 排序，保证确定性）。
pub(crate) fn parse_all_mids(raw: &str) -> Option<Vec<(String, Decimal)>> {
    let v: Value = serde_json::from_str(raw).ok()?;
    let mids = v.pointer("/data/mids")?.as_object()?;
    let mut rows = Vec::with_capacity(mids.len());
    for (coin, px) in mids {
        if let Some(price) = dec_of(px) {
            rows.push((coin.clone(), price));
        }
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    Some(rows)
}

/// 解析 activeAssetCtx 帧为 (coin, markPx)；缺字段 → None。
pub(crate) fn parse_active_ctx(raw: &str) -> Option<(String, Decimal)> {
    let v: Value = serde_json::from_str(raw).ok()?;
    let coin = v.pointer("/data/coin")?.as_str()?;
    let mark = dec_of(v.pointer("/data/ctx/markPx")?)?;
    Some((coin.to_string(), mark))
}

/// candle 一根蜡烛：t=桶起点 ms，o/h/l/c 为并行数组的同下标元素。
#[derive(Debug, Clone, Copy)]
struct RawCandle {
    start_ms: i64,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
}

/// 解析 candle 帧 data：t/o/h/l/c 并行数组，取每根蜡烛（缺字段跳过）。
fn parse_candles(raw: &str) -> Option<(String, Vec<RawCandle>)> {
    let v: Value = serde_json::from_str(raw).ok()?;
    if v.get("channel").and_then(|x| x.as_str())? != "candle" {
        return None;
    }
    let data = v.get("data")?;
    let coin = data.get("coin")?.as_str()?.to_string();
    let t = data.get("t")?.as_array()?;
    let get_arr = |k: &str| data.get(k).and_then(|x| x.as_array());
    let (o, h, l, c) = (get_arr("o")?, get_arr("h")?, get_arr("l")?, get_arr("c")?);
    let mut candles = Vec::new();
    for (i, tv) in t.iter().enumerate() {
        let at = |arr: &[Value]| arr.get(i).and_then(dec_of);
        if let (Some(start_ms), Some(open), Some(high), Some(low), Some(close)) =
            (tv.as_i64(), at(o), at(h), at(l), at(c))
        {
            candles.push(RawCandle {
                start_ms,
                open,
                high,
                low,
                close,
            });
        }
    }
    Some((coin, candles))
}

// =====================================================================
// feed
// =====================================================================

/// Hyperliquid 行情协议（恒定正式网、无认证）。
pub struct HlMarketFeed {
    /// candle 收盘判定：每币最近一根成形中蜡烛，见到新桶则上一根定稿。
    last_candle: Mutex<HashMap<String, RawCandle>>,
}

impl HlMarketFeed {
    pub fn new() -> Self {
        Self {
            last_candle: Mutex::new(HashMap::new()),
        }
    }

    /// 组订阅/退订帧（HL 一个 subscription 一帧）。
    fn frame(method: &str, subscription: Value) -> String {
        serde_json::json!({ "method": method, "subscription": subscription }).to_string()
    }

    /// 频道 id → HL subscription 对象。
    fn subscription_of(channel: &str) -> Option<Value> {
        if let Some(coin) = channel.strip_prefix("activeAssetCtx:") {
            Some(serde_json::json!({"type":"activeAssetCtx","coin":coin,"user":CTX_ROUTE_USER}))
        } else {
            channel
                .strip_prefix("candle:")
                .map(|coin| serde_json::json!({"type":"candle","coin":coin,"interval":"1m"}))
        }
    }

    /// 收盘蜡烛 → 统一 Kline。
    fn finalize_bar(coin: &str, c: &RawCandle) -> Kline {
        Kline {
            venue: VENUE.to_string(),
            symbol: coin.to_string(),
            interval: KlineInterval::M1,
            trading_day: crate::market_ws::trading_day_of_utc_ms(c.start_ms),
            start_ms: c.start_ms,
            end_ms: c.start_ms + 60_000,
            open: c.open,
            high: c.high,
            low: c.low,
            close: c.close,
            volume: 0,
            turnover: Decimal::ZERO,
            open_interest: Decimal::ZERO,
        }
    }
}

impl Default for HlMarketFeed {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl WsMarketFeed for HlMarketFeed {
    fn url(&self) -> &str {
        MARKET_WS
    }

    async fn on_connect(&self, ws: &mut WsStream) -> anyhow::Result<()> {
        // allMids 常驻（全市场 mid），与具体期望集无关。
        ws.send(WsMessage::Text(
            Self::frame("subscribe", serde_json::json!({ "type": "allMids" })).into(),
        ))
        .await?;
        Ok(())
    }

    fn reconcile(
        &self,
        price_symbols: &BTreeSet<String>,
        bar_symbols: &BTreeSet<String>,
        subscribed: &HashSet<String>,
    ) -> ChannelPlan {
        // activeAssetCtx 按价格期望集逐币；candle 按 K线期望集逐币；allMids 不在此（常驻）。
        let mut desired: HashSet<String> = price_symbols.iter().map(|c| ctx_channel(c)).collect();
        desired.extend(bar_symbols.iter().map(|c| candle_channel(c)));
        let plan = SubsPlan::diff(&desired, subscribed);
        ChannelPlan {
            subscribe: plan.to_sub,
            unsubscribe: plan.to_unsub,
        }
    }

    async fn send_subscribe(&self, ws: &mut WsStream, channels: &[String]) -> anyhow::Result<()> {
        for ch in channels {
            let Some(sub) = Self::subscription_of(ch) else {
                continue;
            };
            ws.send(WsMessage::Text(Self::frame("subscribe", sub).into()))
                .await?;
        }
        Ok(())
    }

    async fn send_unsubscribe(&self, ws: &mut WsStream, channels: &[String]) -> anyhow::Result<()> {
        for ch in channels {
            let Some(sub) = Self::subscription_of(ch) else {
                continue;
            };
            ws.send(WsMessage::Text(Self::frame("unsubscribe", sub).into()))
                .await?;
        }
        Ok(())
    }

    fn decode(&self, text: &str) -> FeedMessage {
        let mut out = FeedMessage::default();

        // allMids：全市场 mid。
        if let Some(rows) = parse_all_mids(text) {
            for (coin, mid) in rows {
                out.ticks.push(PriceTick {
                    symbol: coin,
                    basis: PriceBasis::Mid,
                    price: mid,
                    best_bid: None,
                    best_ask: None,
                    volume: None,
                    event_ms: 0,
                });
            }
        }

        // activeAssetCtx：兴趣币 mark。
        if let Some((coin, mark)) = parse_active_ctx(text) {
            out.ticks.push(PriceTick {
                symbol: coin,
                basis: PriceBasis::Mark,
                price: mark,
                best_bid: None,
                best_ask: None,
                volume: None,
                event_ms: 0,
            });
        }

        // candle：桶翻转时定稿上一根。
        if let Some((coin, mut candles)) = parse_candles(text) {
            candles.sort_by_key(|c| c.start_ms);
            let mut last = self.last_candle.lock().expect("hl candle lock");
            for c in candles {
                let prev = last.get(&coin).copied();
                match prev {
                    Some(prev) if prev.start_ms < c.start_ms => {
                        out.bars.push(Self::finalize_bar(&coin, &prev));
                        last.insert(coin.clone(), c);
                    }
                    _ => {
                        // 首见/同桶更新/乱序：仅记录，不定稿、不回拨已发 bar。
                        last.insert(coin.clone(), c);
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
        // allMids 写 mid，activeAssetCtx 写 mark，其余字段不动。
        let (mid, mark) = match tick.basis {
            PriceBasis::Mid => (Some(tick.price), None),
            PriceBasis::Mark => (None, Some(tick.price)),
            PriceBasis::Last => (None, None),
        };
        let update = QuoteUpdate {
            last: None,
            mark,
            mid,
            book: None,
            // 与旧 apply_quote 一致：HL 帧不携带事件时间，两列都用本地打点。
            ts_exchange_ms: now,
            ts_recv_ms: now,
        };
        let inst = Instrument::new(Venue::Hyperliquid, MarketKind::Futures, &tick.symbol);
        quotes.apply(&inst, update);
        if let Some(ob) = observer {
            ob.on_quote(
                Venue::Hyperliquid,
                MarketKind::Futures,
                &tick.symbol,
                tick.price,
            );
        }
    }
}

#[cfg(test)]
mod parse_tests {
    use super::*;
    use std::sync::Arc;

    fn syms(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_all_mids_frame() {
        let raw = r##"{"channel":"allMids","data":{"mids":{
          "BTC":"82966.5","ETH":"2650.25","#12090":"0.04887"}}}"##;
        let rows = parse_all_mids(raw).expect("parsed");
        // 预编译键也解析（过滤在写表侧），按 coin 排序保证确定性。
        assert_eq!(
            rows[0],
            (
                "#12090".to_string(),
                Decimal::from_str_exact("0.04887").unwrap()
            )
        );
        assert_eq!(
            rows[1],
            (
                "BTC".to_string(),
                Decimal::from_str_exact("82966.5").unwrap()
            )
        );
        assert_eq!(
            rows[2],
            (
                "ETH".to_string(),
                Decimal::from_str_exact("2650.25").unwrap()
            )
        );
    }

    #[test]
    fn parses_active_asset_ctx_frame() {
        let raw = r#"{"channel":"activeAssetCtx","data":{
          "coin":"BTC","ctx":{"markPx":"82879","oraclePx":"82911.4","midPx":"82873.5"}}}"#;
        let (coin, mark) = parse_active_ctx(raw).expect("parsed");
        assert_eq!(coin, "BTC");
        assert_eq!(mark, Decimal::from(82879));
    }

    #[test]
    fn bad_frame_or_missing_fields_yield_nothing() {
        assert!(parse_all_mids("not json").is_none());
        assert!(parse_all_mids(r#"{"channel":"allMids"}"#).is_none());
        assert!(
            parse_active_ctx(r#"{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{}}}"#)
                .is_none()
        );
    }

    #[test]
    fn reconcile_maps_coins_to_channel_ids() {
        let feed = HlMarketFeed::new();
        // 已订阅 BTC 的 activeAssetCtx；价格期望 {BTC,ETH}、K线 {ETH}。
        let subscribed = HashSet::from([ctx_channel("BTC")]);
        let plan = feed.reconcile(&syms(&["BTC", "ETH"]), &syms(&["ETH"]), &subscribed);
        // 新增：ETH 的 activeAssetCtx + ETH 的 candle。
        assert_eq!(
            plan.subscribe,
            vec!["activeAssetCtx:ETH".to_string(), "candle:ETH".to_string()]
        );
        assert!(plan.unsubscribe.is_empty());
    }

    #[test]
    fn subscription_of_maps_channel_ids() {
        // 帧编码纯逻辑：频道 id → HL subscription 对象。
        let sub = HlMarketFeed::subscription_of("activeAssetCtx:BTC").unwrap();
        assert_eq!(sub["type"], "activeAssetCtx");
        assert_eq!(sub["coin"], "BTC");
        assert_eq!(sub["user"], CTX_ROUTE_USER);
        let candle = HlMarketFeed::subscription_of("candle:ETH").unwrap();
        assert_eq!(candle["type"], "candle");
        assert_eq!(candle["interval"], "1m");
        assert!(HlMarketFeed::subscription_of("unknown:x").is_none());
    }

    #[test]
    fn decode_all_mids_to_mid_ticks_and_ctx_to_mark() {
        let feed = HlMarketFeed::new();
        let mids = r#"{"channel":"allMids","data":{"mids":{"BTC":"100","ETH":"200"}}}"#;
        let msg = feed.decode(mids);
        assert_eq!(msg.ticks.len(), 2);
        assert!(msg.ticks.iter().all(|t| t.basis == PriceBasis::Mid));
        assert!(msg.bars.is_empty());

        let ctx = r#"{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{"markPx":"101"}}}"#;
        let msg = feed.decode(ctx);
        assert_eq!(msg.ticks.len(), 1);
        assert_eq!(msg.ticks[0].basis, PriceBasis::Mark);
        assert_eq!(msg.ticks[0].price, Decimal::from(101));
    }

    #[test]
    fn decode_candle_finalizes_previous_on_bucket_flip() {
        let feed = HlMarketFeed::new();
        // t/o/h/l/c 并行数组。第一帧只给桶 60000（成形中，不定稿）。
        let f1 = r#"{"channel":"candle","data":{"coin":"BTC","interval":"1m",
          "t":[60000],"o":["100"],"h":["110"],"l":["99"],"c":["105"]}}"#;
        assert!(feed.decode(f1).bars.is_empty());
        // 同桶更新。
        let f2 = r#"{"channel":"candle","data":{"coin":"BTC","interval":"1m",
          "t":[60000],"o":["100"],"h":["111"],"l":["99"],"c":["108"]}}"#;
        assert!(feed.decode(f2).bars.is_empty());
        // 新桶 120000：上一根定稿（取最终更新值）。
        let f3 = r#"{"channel":"candle","data":{"coin":"BTC","interval":"1m",
          "t":[120000],"o":["108"],"h":["109"],"l":["107"],"c":["108"]}}"#;
        let bars = feed.decode(f3).bars;
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].symbol, "BTC");
        assert_eq!(bars[0].venue, VENUE);
        assert_eq!(bars[0].start_ms, 60000);
        assert_eq!(bars[0].close, Decimal::from(108));
        assert_eq!(bars[0].high, Decimal::from(111));
    }

    #[test]
    fn write_quote_routes_mid_and_mark_to_distinct_fields() {
        let feed = HlMarketFeed::new();
        let quotes = Arc::new(QuoteTable::new());
        let inst = Instrument::new(Venue::Hyperliquid, MarketKind::Futures, "BTC");
        feed.write_quote(
            &PriceTick {
                symbol: "BTC".into(),
                basis: PriceBasis::Mid,
                price: Decimal::from(100),
                best_bid: None,
                best_ask: None,
                volume: None,
                event_ms: 1,
            },
            &quotes,
            None,
        );
        feed.write_quote(
            &PriceTick {
                symbol: "BTC".into(),
                basis: PriceBasis::Mark,
                price: Decimal::from(101),
                best_bid: None,
                best_ask: None,
                volume: None,
                event_ms: 2,
            },
            &quotes,
            None,
        );
        let q = quotes.get(&inst).expect("quote");
        assert_eq!(q.mid, Some(Decimal::from(100)));
        assert_eq!(q.mark, Some(Decimal::from(101)));
    }
}
