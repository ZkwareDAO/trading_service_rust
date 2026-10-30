//! Binance U 本位永续历史 K线 REST 回补（`GET /fapi/v1/klines`）。
//!
//! 协议差异（数组下标、字符串小数、专有字段）全部下沉在本模块：解析函数
//! [`parse_klines_json`] 零 IO、可用文档实测响应做 fixture 单测；网络分页/节流复用
//! [`crate::market::collect_pages`]。回补 bar 只回写内存 BarTable，不进实时 fanout。

use std::collections::BTreeMap;
use std::time::Duration;

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde_json::Value;

use crate::market::{collect_pages, HistoricalKlines, Kline, KlineInterval};

/// Binance U 本位永续公开 REST 基址（历史回补恒定主网，与公共行情口径一致）。
const FAPI_BASE: &str = "https://fapi.binance.com";

/// 解析 `/fapi/v1/klines` 数组响应。每行为数组：
/// `[openTime, open, high, low, close, volume, closeTime, quoteVolume,
///   trades, takerBuyBase, takerBuyQuote, ignore]`。
///
/// 仅取收盘所需字段；成交量为字符串小数，[`Kline::volume`] 截断取整（与 WS 定稿同口径）。
/// 任一必需下标缺失/类型不符即返回 `Err`（不静默丢根）。
pub(crate) fn parse_klines_json(symbol: &str, text: &str) -> anyhow::Result<Vec<Kline>> {
    let rows: Vec<Value> = serde_json::from_str(text)?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let a = row
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("binance klines 行不是数组：{row}"))?;
        let at = |i: usize| a.get(i);
        let start_ms = at(0)
            .and_then(Value::as_i64)
            .ok_or_else(|| anyhow::anyhow!("binance klines 缺少 openTime：{row}"))?;
        let open = req_dec(at(1), "open", row)?;
        let high = req_dec(at(2), "high", row)?;
        let low = req_dec(at(3), "low", row)?;
        let close = req_dec(at(4), "close", row)?;
        let volume = dec_or_zero(at(5)).trunc().to_i64().unwrap_or(0);
        let quote_volume = dec_or_zero(at(7));
        // 与 WS 定稿帧一致的币安专有扩展指标。
        let mut extras: BTreeMap<String, crate::market::KlineExtra> = BTreeMap::new();
        extras.insert(
            "trade_num".to_string(),
            crate::market::KlineExtra::Int(at(8).and_then(Value::as_i64).unwrap_or(0)),
        );
        extras.insert(
            "active_buy_volume".to_string(),
            crate::market::KlineExtra::Num(dec_or_zero(at(9))),
        );
        extras.insert(
            "active_buy_quote_volume".to_string(),
            crate::market::KlineExtra::Num(dec_or_zero(at(10))),
        );
        out.push(Kline {
            venue: crate::binance::market::VENUE.to_string(),
            symbol: symbol.to_string(),
            interval: KlineInterval::M1,
            trading_day: crate::market_ws::trading_day_of_utc_ms(start_ms),
            start_ms,
            end_ms: start_ms + KlineInterval::M1.duration_ms(),
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            extras,
        });
    }
    Ok(out)
}

/// 必需价格字段：缺失/非法即报错。
fn req_dec(v: Option<&Value>, field: &str, row: &Value) -> anyhow::Result<Decimal> {
    parse_decimal(v).ok_or_else(|| anyhow::anyhow!("binance klines 字段 {field} 非法：{row}"))
}

/// 量额字段：缺失/非法按 0 处理（坏值不致丢整根 bar，与 WS 同口径）。
fn dec_or_zero(v: Option<&Value>) -> Decimal {
    parse_decimal(v).unwrap_or(Decimal::ZERO)
}

/// 字符串或数字标量 → Decimal（字符串小数 trim 后解析）。
fn parse_decimal(v: Option<&Value>) -> Option<Decimal> {
    match v? {
        Value::String(s) => Decimal::from_str_exact(s.trim()).ok(),
        Value::Number(n) => Decimal::from_str_exact(&n.to_string()).ok(),
        _ => None,
    }
}

/// 单窗取数（自由函数，便于分页闭包 clone 廉价 client 复用）：`endTime` 传窗口上界，
/// 该边界所属的下一根会被 `collect_pages` 的区间过滤剔除；`limit` 不小于本窗根数。
/// 查询参数经 reqwest `.query()` 自动 percent-encode（symbol 来自 admin/订阅，按系统边界处理）。
async fn fetch_window(
    http: &reqwest::Client,
    symbol: &str,
    win_start: i64,
    win_end: i64,
    page_size: usize,
) -> anyhow::Result<Vec<Kline>> {
    let url = format!("{FAPI_BASE}/fapi/v1/klines");
    let resp = http
        .get(url)
        .query(&[
            ("symbol", symbol),
            ("interval", KlineInterval::M1.as_str()),
            ("startTime", &win_start.to_string()),
            ("endTime", &win_end.to_string()),
            ("limit", &page_size.to_string()),
        ])
        .send()
        .await?
        .error_for_status()?;
    parse_klines_json(symbol, &resp.text().await?)
}

/// Binance 历史 K线回补实现（持有共享 HTTP client 与分页/节流参数）。
pub(crate) struct BinanceHistorical {
    http: reqwest::Client,
    page_size: usize,
    page_delay: Duration,
}

impl BinanceHistorical {
    pub(crate) fn new(http: reqwest::Client, page_size: usize, page_delay: Duration) -> Self {
        Self {
            http,
            page_size,
            page_delay,
        }
    }
}

#[async_trait::async_trait]
impl HistoricalKlines for BinanceHistorical {
    async fn fetch_klines(
        &self,
        symbol: &str,
        interval: KlineInterval,
        start_ms: i64,
        end_ms: i64,
    ) -> anyhow::Result<Vec<Kline>> {
        let step = interval.duration_ms();
        let page_size = self.page_size;
        collect_pages(
            start_ms,
            end_ms,
            step,
            self.page_size,
            self.page_delay,
            |ws, we| {
                // FnMut 分页闭包：collect_pages 在调用下一页前已 await 完当前 future，
                // 故每窗克隆廉价 client（Arc 连接池）即可，不跨窗持有借用。
                let http = self.http.clone();
                let sym = symbol.to_string();
                async move { fetch_window(&http, &sym, ws, we, page_size).await }
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RESP: &str = r#"[
      [1700000000000,"100.5","101.0","99.5","100.8","12.5",1700000059999,"1260.0",42,"7.0","705.6","0"],
      [1700000060000,"100.8","102.0","100.0","101.5","8.0",1700000119999,"812.0",17,"3.0","304.5","0"]
    ]"#;

    #[test]
    fn parse_maps_ohlcv_quote_and_extras() {
        let got = parse_klines_json("BTCUSDT", RESP).unwrap();
        assert_eq!(got.len(), 2);
        let b = &got[0];
        assert_eq!(b.venue, "binance");
        assert_eq!(b.symbol, "BTCUSDT");
        assert_eq!(b.interval, KlineInterval::M1);
        assert_eq!(b.start_ms, 1_700_000_000_000);
        assert_eq!(b.end_ms, 1_700_000_060_000);
        assert_eq!(b.open, Decimal::new(1005, 1));
        assert_eq!(b.high, Decimal::new(1010, 1));
        assert_eq!(b.low, Decimal::new(995, 1));
        assert_eq!(b.close, Decimal::new(1008, 1));
        assert_eq!(b.volume, 12); // 12.5 截断
        assert_eq!(b.quote_volume, Decimal::new(12600, 1));
        assert_eq!(
            b.extras.get("trade_num"),
            Some(&crate::market::KlineExtra::Int(42))
        );
        assert_eq!(
            b.extras.get("active_buy_volume"),
            Some(&crate::market::KlineExtra::Num(Decimal::new(70, 1)))
        );
        // 升序、trading_day 已填。
        assert!(got[0].start_ms < got[1].start_ms);
        assert_ne!(got[0].trading_day, 0);
    }

    #[test]
    fn parse_rejects_non_array_envelope() {
        assert!(parse_klines_json("BTCUSDT", r#"{"msg":"bad"}"#).is_err());
    }

    #[test]
    fn parse_rejects_row_missing_required_price() {
        // close（下标 4）为空对象，无法解析为 decimal → 整批报错。
        let bad = r#"[[1700000000000,"100","101","99",{},"1","2","3"]]"#;
        assert!(parse_klines_json("BTCUSDT", bad).is_err());
    }

    #[test]
    fn parse_empty_array_is_ok() {
        assert!(parse_klines_json("BTCUSDT", "[]").unwrap().is_empty());
    }
}
