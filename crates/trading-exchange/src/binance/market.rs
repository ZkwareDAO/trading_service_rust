//! Binance 公共行情 **协议层（feed）**：生命周期收口到 [`crate::market_ws`]。
//!
//! 两个全市场、无认证、所有账户共享的 feed（各一条 WS，URL 即订阅，空集=全市场）：
//! * 现货 `!miniTicker@arr`（1s 裸数组）→ 最新成交价 last（不挂仓位重估观察口）；
//! * U 本位合约 `!markPrice@arr@1s`（裸数组）→ 标记价 mark（写价后回调 observer）。
//!
//! K线（同一连接动态 SUBSCRIBE/UNSUBSCRIBE）：`<symbol>@kline_1m`，仅在 `k.x=true`
//! （分钟收盘定稿）时产出一根 [`Kline`]。
//!
//! 解析裸数组的纯函数与旧实现一致（2026 WS 迁移：合约走 `/market/ws`，返回裸数组）。

use std::collections::{BTreeSet, HashSet};
use std::str::FromStr;

use async_trait::async_trait;
use futures_util::SinkExt;
use rust_decimal::Decimal;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use trading_core::quote::{QuoteTable, QuoteUpdate};
use trading_models::{Instrument, MarketKind, Venue};

use crate::market::{Kline, KlineInterval, PriceBasis, PriceTick};
use crate::market_hub::SubsPlan;
use crate::market_ws::{ChannelPlan, FeedMessage, WsMarketFeed};
use crate::port::QuoteObserver;
use crate::ws::WsStream;

/// binance 交易所标识。
pub const VENUE: &str = "binance";

const SPOT_URL: &str = "wss://stream.binance.com:9443/ws/!miniTicker@arr";
/// 合约公共行情恒定正式网（2026 WS 迁移：`/market/ws` 返回裸数组）。
const FUTURES_URL: &str = "wss://fstream.binance.com/market/ws/!markPrice@arr@1s";

/// 解析出的一行：（symbol, 价格, 交易所事件时间毫秒）。
type PriceRow = (String, Decimal, i64);

/// K线频道：`<symbol(lowercase)>@kline_1m`。
fn kline_channel(symbol: &str) -> String {
    format!("{}@kline_1m", symbol.to_ascii_lowercase())
}

// =====================================================================
// feed
// =====================================================================

/// Binance 全市场行情协议（kind 区分现货 last / 合约 mark）。
pub struct BinanceMarketFeed {
    kind: MarketKind,
}

impl BinanceMarketFeed {
    /// 现货全市场（miniTicker → last，无 observer）。
    pub fn spot() -> Self {
        Self {
            kind: MarketKind::Spot,
        }
    }
    /// U 本位合约全市场（markPrice → mark，写价驱动仓位重估）。
    pub fn futures() -> Self {
        Self {
            kind: MarketKind::Futures,
        }
    }

    fn is_futures(&self) -> bool {
        self.kind == MarketKind::Futures
    }

    fn venue(&self) -> Venue {
        if self.is_futures() {
            Venue::BinanceFutures
        } else {
            Venue::BinanceSpot
        }
    }

    /// 解析全市场裸数组帧（现货 miniTicker / 合约 markPrice）。
    fn parse_price_array(&self, text: &str) -> Vec<PriceRow> {
        let parsed = if self.is_futures() {
            parse_futures_mark_price(text)
        } else {
            parse_spot_mini_ticker(text)
        };
        parsed.unwrap_or_default()
    }

    /// 组 SUBSCRIBE/UNSUBSCRIBE 帧（K线频道）。
    fn method_frame(method: &str, channels: &[String]) -> String {
        serde_json::json!({"method": method, "params": channels, "id": 1}).to_string()
    }
}

#[async_trait]
impl WsMarketFeed for BinanceMarketFeed {
    fn url(&self) -> &str {
        if self.is_futures() {
            FUTURES_URL
        } else {
            SPOT_URL
        }
    }

    fn reconcile(
        &self,
        _price_symbols: &BTreeSet<String>,
        bar_symbols: &BTreeSet<String>,
        subscribed: &HashSet<String>,
    ) -> ChannelPlan {
        // 价格为全市场：URL 即订阅，不产生动态频道；只有 K线按 symbol 增删。
        let desired: HashSet<String> = bar_symbols.iter().map(|s| kline_channel(s)).collect();
        let plan = SubsPlan::diff(&desired, subscribed);
        ChannelPlan {
            subscribe: plan.to_sub,
            unsubscribe: plan.to_unsub,
        }
    }

    async fn send_subscribe(&self, ws: &mut WsStream, channels: &[String]) -> anyhow::Result<()> {
        ws.send(WsMessage::Text(
            Self::method_frame("SUBSCRIBE", channels).into(),
        ))
        .await?;
        Ok(())
    }

    async fn send_unsubscribe(&self, ws: &mut WsStream, channels: &[String]) -> anyhow::Result<()> {
        ws.send(WsMessage::Text(
            Self::method_frame("UNSUBSCRIBE", channels).into(),
        ))
        .await?;
        Ok(())
    }

    fn decode(&self, text: &str) -> FeedMessage {
        let mut out = FeedMessage::default();

        // 全市场价格裸数组（非数组即非该流帧，返回空不报错）。
        for (symbol, price, event_ms) in self.parse_price_array(text) {
            out.ticks.push(PriceTick {
                symbol,
                basis: if self.is_futures() {
                    PriceBasis::Mark
                } else {
                    PriceBasis::Last
                },
                price,
                best_bid: None,
                best_ask: None,
                volume: None,
                event_ms,
            });
        }

        // K线单对象帧：仅收盘定稿。
        if let Some(bar) = parse_kline_frame(text) {
            out.bars.push(bar);
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
        let (last, mark) = if self.is_futures() {
            (None, Some(tick.price))
        } else {
            (Some(tick.price), None)
        };
        let update = QuoteUpdate {
            last,
            mark,
            mid: None,
            book: None,
            // 与旧 forward_quotes 一致：交易所时间用帧事件时间 E，接收时间本地打点。
            ts_exchange_ms: tick.event_ms,
            ts_recv_ms: now,
        };
        let inst = Instrument::new(self.venue(), self.kind, &tick.symbol);
        quotes.apply(&inst, update);
        // 现货不挂观察口（provider 传 None）；合约写价后驱动仓位重估。
        if let Some(ob) = observer {
            ob.on_quote(self.venue(), self.kind, &tick.symbol, tick.price);
        }
    }
}

/// 解析 kline 事件帧：仅 `k.x=true`（分钟收盘）产出定稿 [`Kline`]。
fn parse_kline_frame(text: &str) -> Option<Kline> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;
    if v.get("e")?.as_str()? != "kline" {
        return None;
    }
    let symbol = v.get("s")?.as_str()?.to_string();
    let k = v.get("k")?;
    if !k.get("x")?.as_bool()? {
        return None; // 成形中，不定稿
    }
    let dec = |key: &str| parse_decimal(k.get(key)).unwrap_or(Decimal::ZERO);
    let start_ms = k.get("t")?.as_i64()?;
    let end_ms = k.get("T")?.as_i64()?;
    // 成交量/额为字符串小数；Kline.volume 为 i64，截断取整（计划二可扩展为小数量）。
    use rust_decimal::prelude::ToPrimitive;
    let volume = dec("v").trunc().to_i64().unwrap_or(0);
    Some(Kline {
        venue: VENUE.to_string(),
        symbol,
        interval: KlineInterval::M1,
        trading_day: crate::market_ws::trading_day_of_utc_ms(start_ms),
        start_ms,
        end_ms,
        open: dec("o"),
        high: dec("h"),
        low: dec("l"),
        close: dec("c"),
        volume,
        turnover: dec("q"),
        open_interest: Decimal::ZERO,
    })
}

// =====================================================================
// 纯函数解析（零网络，单测覆盖，逻辑与旧 worker 一致）
// =====================================================================

fn parse_decimal(v: Option<&serde_json::Value>) -> Option<Decimal> {
    match v? {
        serde_json::Value::String(s) => Decimal::from_str(s.trim()).ok(),
        serde_json::Value::Number(n) => Decimal::from_str(&n.to_string()).ok(),
        _ => None,
    }
}

/// 现货 `!miniTicker@arr`：逐元素取 `s`(symbol)、`c`(最新成交价 close)、`E`(事件时间)。
pub fn parse_spot_mini_ticker(payload: &str) -> anyhow::Result<Vec<PriceRow>> {
    let v: serde_json::Value = serde_json::from_str(payload)?;
    let arr = v
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("miniTicker payload is not an array"))?;
    Ok(parse_rows(arr, "c"))
}

/// 合约 `!markPrice@arr@1s`：逐元素取 `s`、`p`(标记价 markPrice)、`E`。
pub fn parse_futures_mark_price(payload: &str) -> anyhow::Result<Vec<PriceRow>> {
    let v: serde_json::Value = serde_json::from_str(payload)?;
    let arr = v
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("markPrice payload is not an array"))?;
    Ok(parse_rows(arr, "p"))
}

fn parse_rows(arr: &[serde_json::Value], price_key: &str) -> Vec<PriceRow> {
    let mut out = Vec::with_capacity(arr.len());
    for it in arr {
        let Some(symbol) = it.get("s").and_then(|x| x.as_str()) else {
            continue;
        };
        let Some(price) = parse_decimal(it.get(price_key)) else {
            continue;
        };
        let event_ms = it.get("E").and_then(|x| x.as_i64()).unwrap_or(0);
        out.push((symbol.to_string(), price, event_ms));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn syms(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_spot_mini_ticker_array() {
        let payload = r#"[
            {"e":"24hrMiniTicker","E":1700000001000,"s":"BTCUSDT","c":"99999.50"},
            {"e":"24hrMiniTicker","E":1700000001000,"s":"ETHUSDT","c":"2000.25"}
        ]"#;
        assert_eq!(
            parse_spot_mini_ticker(payload).unwrap(),
            vec![
                ("BTCUSDT".to_string(), dec("99999.50"), 1700000001000),
                ("ETHUSDT".to_string(), dec("2000.25"), 1700000001000),
            ]
        );
    }

    #[test]
    fn parses_futures_mark_price_array() {
        let payload = r#"[
            {"e":"markPriceUpdate","E":1700000002000,"s":"BTCUSDT","p":"100000.25","r":"0.0001","T":1700000003000},
            {"e":"markPriceUpdate","E":1700000002000,"s":"ETHUSDT","p":2001.5}
        ]"#;
        assert_eq!(
            parse_futures_mark_price(payload).unwrap(),
            vec![
                ("BTCUSDT".to_string(), dec("100000.25"), 1700000002000),
                ("ETHUSDT".to_string(), dec("2001.5"), 1700000002000),
            ]
        );
    }

    #[test]
    fn skips_rows_with_missing_symbol_or_price() {
        let payload = r#"[{"E":1,"s":"BAD"},{"E":2,"c":"10"},{"s":"OK","c":"11","E":3}]"#;
        assert_eq!(
            parse_spot_mini_ticker(payload).unwrap(),
            vec![("OK".to_string(), dec("11"), 3)]
        );
    }

    #[test]
    fn non_array_or_bad_json_is_error_for_price_parsers() {
        assert!(parse_spot_mini_ticker(r#"{"s":"BTCUSDT"}"#).is_err());
        assert!(parse_futures_mark_price("not json").is_err());
    }

    #[test]
    fn urls_are_whole_market_and_futures_production() {
        assert_eq!(BinanceMarketFeed::spot().url(), SPOT_URL);
        assert_eq!(
            BinanceMarketFeed::futures().url(),
            "wss://fstream.binance.com/market/ws/!markPrice@arr@1s"
        );
        assert!(!BinanceMarketFeed::futures().url().contains("binancefuture"));
    }

    #[test]
    fn reconcile_only_emits_kline_channels_price_is_implicit() {
        let feed = BinanceMarketFeed::futures();
        // 即使价格期望非空，也不产生价格频道（URL 已订阅全市场）。
        let plan = feed.reconcile(
            &syms(&["BTCUSDT", "ETHUSDT"]),
            &syms(&["BTCUSDT"]),
            &HashSet::new(),
        );
        assert_eq!(plan.subscribe, vec!["btcusdt@kline_1m".to_string()]);
        assert!(plan.unsubscribe.is_empty());

        // 退订：已订的 kline 不在期望集。
        let subscribed = HashSet::from(["ethusdt@kline_1m".to_string()]);
        let plan = feed.reconcile(&syms(&[]), &syms(&["BTCUSDT"]), &subscribed);
        assert_eq!(plan.subscribe, vec!["btcusdt@kline_1m".to_string()]);
        assert_eq!(plan.unsubscribe, vec!["ethusdt@kline_1m".to_string()]);
    }

    #[test]
    fn decode_futures_array_to_mark_ticks() {
        let feed = BinanceMarketFeed::futures();
        let msg = feed.decode(r#"[{"e":"markPriceUpdate","E":7,"s":"BTCUSDT","p":"100"}]"#);
        assert_eq!(msg.ticks.len(), 1);
        assert_eq!(msg.ticks[0].symbol, "BTCUSDT");
        assert_eq!(msg.ticks[0].basis, PriceBasis::Mark);
        assert!(msg.bars.is_empty());
    }

    #[test]
    fn decode_spot_array_to_last_ticks() {
        let feed = BinanceMarketFeed::spot();
        let msg = feed.decode(r#"[{"e":"24hrMiniTicker","E":7,"s":"BTCUSDT","c":"99"}]"#);
        assert_eq!(msg.ticks[0].basis, PriceBasis::Last);
        assert!(msg.bars.is_empty());
    }

    #[test]
    fn decode_kline_only_emits_final_bar() {
        let feed = BinanceMarketFeed::futures();
        let forming = r#"{"e":"kline","E":1,"s":"BTCUSDT","k":{
          "t":60000,"T":120000,"i":"1m","o":"100","h":"110","l":"99","c":"105",
          "v":"3.5","q":"360.0","x":false}}"#;
        assert!(feed.decode(forming).bars.is_empty());
        let final_frame = r#"{"e":"kline","E":1,"s":"BTCUSDT","k":{
          "t":60000,"T":120000,"i":"1m","o":"100","h":"111","l":"99","c":"108",
          "v":"4","q":"432.0","x":true}}"#;
        let bars = feed.decode(final_frame).bars;
        assert_eq!(bars.len(), 1);
        let b = &bars[0];
        assert_eq!(b.symbol, "BTCUSDT");
        assert_eq!(b.venue, VENUE);
        assert_eq!(b.start_ms, 60000);
        assert_eq!(b.end_ms, 120_000);
        assert_eq!(b.close, dec("108"));
        assert_eq!(b.high, dec("111"));
        assert_eq!(b.volume, 4);
        assert_eq!(b.turnover, dec("432.0"));
    }

    #[test]
    fn write_quote_spot_uses_last_futures_uses_mark() {
        let quotes = Arc::new(QuoteTable::new());
        let spot = BinanceMarketFeed::spot();
        let fut = BinanceMarketFeed::futures();

        spot.write_quote(
            &PriceTick {
                symbol: "BTCUSDT".into(),
                basis: PriceBasis::Last,
                price: dec("99"),
                best_bid: None,
                best_ask: None,
                volume: None,
                event_ms: 0,
            },
            &quotes,
            None,
        );
        fut.write_quote(
            &PriceTick {
                symbol: "BTCUSDT".into(),
                basis: PriceBasis::Mark,
                price: dec("100"),
                best_bid: None,
                best_ask: None,
                volume: None,
                event_ms: 5,
            },
            &quotes,
            None,
        );
        let spot_inst = Instrument::new(Venue::BinanceSpot, MarketKind::Spot, "BTCUSDT");
        let fut_inst = Instrument::new(Venue::BinanceFutures, MarketKind::Futures, "BTCUSDT");
        assert_eq!(quotes.get(&spot_inst).unwrap().last, Some(dec("99")));
        assert_eq!(quotes.get(&fut_inst).unwrap().mark, Some(dec("100")));
    }
}
