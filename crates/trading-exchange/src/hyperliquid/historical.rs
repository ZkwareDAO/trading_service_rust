//! Hyperliquid 历史 K线 REST 回补（POST `/info`，`type=candleSnapshot`，body 嵌套 `req`）。
//!
//! 请求体：
//! ```json
//! {"type":"candleSnapshot","req":{"coin":"BTC","interval":"1m",
//!   "startTime":1700000000000,"endTime":1700000600000}}
//! ```
//! 响应为对象数组，元素字段 `t/T/s/i/o/h/l/c/v/n`。解析 [`parse_candle_snapshot`] 零 IO；
//! 网络分页/节流复用 [`crate::market::collect_pages`]。
//!
//! 字段口径与 hyperliquid WS candle 定稿 `finalize_bar` **严格一致**（volume=0、
//! quote_volume=0、无 extras）：WS candle 频道只给 OHLC，为保证同序列回补 bar 与实时 bar
//! 语义一致，这里即便响应含 v/n 也不写入量/扩展字段（待 WS 侧统一放量后一并放开）。

use std::time::Duration;

use rust_decimal::Decimal;
use serde_json::{json, Value};

use crate::market::{collect_pages, HistoricalKlines, Kline, KlineInterval};

/// 解析 candleSnapshot 数组响应：元素 `{t,o,h,l,c,...}`，必需 t/o/h/l/c。
/// 任一必需字段缺失/非法即返回 `Err`（不静默吞成空成功）。
pub(crate) fn parse_candle_snapshot(coin: &str, text: &str) -> anyhow::Result<Vec<Kline>> {
    let rows: Vec<Value> = serde_json::from_str(text)?;
    let step = KlineInterval::M1.duration_ms();
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let Some(start_ms) = row.get("t").and_then(Value::as_i64) else {
            anyhow::bail!("hyperliquid candle 缺少 t：{row}");
        };
        let dec = |k: &str| -> anyhow::Result<Decimal> {
            match row.get(k) {
                Some(Value::String(s)) => Decimal::from_str_exact(s.trim())
                    .map_err(|_| anyhow::anyhow!("hyperliquid candle {k} 非法：{row}")),
                Some(Value::Number(_)) => Decimal::from_str_exact(&row[k].to_string())
                    .map_err(|_| anyhow::anyhow!("hyperliquid candle {k} 非法：{row}")),
                _ => anyhow::bail!("hyperliquid candle 缺少 {k}：{row}"),
            }
        };
        let (open, high, low, close) = (dec("o")?, dec("h")?, dec("l")?, dec("c")?);
        out.push(Kline {
            venue: crate::hyperliquid::market::VENUE.to_string(),
            symbol: coin.to_string(),
            interval: KlineInterval::M1,
            trading_day: crate::market_ws::trading_day_of_utc_ms(start_ms),
            start_ms,
            end_ms: start_ms + step,
            open,
            high,
            low,
            close,
            // 与 WS finalize_bar 同口径：candle 频道无量，统一落 0。
            volume: 0,
            quote_volume: Decimal::ZERO,
            extras: std::collections::BTreeMap::new(),
        });
    }
    Ok(out)
}

/// 构造 candleSnapshot 请求体：必须嵌套 `req`（Hyperliquid 特有信封）。纯函数便于单测。
pub(crate) fn candle_snapshot_body(coin: &str, win_start: i64, win_end: i64) -> serde_json::Value {
    json!({
        "type": "candleSnapshot",
        "req": {
            "coin": coin,
            "interval": KlineInterval::M1.as_str(),
            "startTime": win_start,
            "endTime": win_end,
        }
    })
}

/// 单窗取数：POST /info，body 必须嵌套 `req`。
async fn fetch_window(
    http: &reqwest::Client,
    url: &str,
    coin: &str,
    win_start: i64,
    win_end: i64,
) -> anyhow::Result<Vec<Kline>> {
    let body = candle_snapshot_body(coin, win_start, win_end);
    let resp = http
        .post(url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    parse_candle_snapshot(coin, &resp.text().await?)
}

/// Hyperliquid 历史 K线回补实现（公共市场数据恒定主网 /info）。
pub(crate) struct HlHistorical {
    http: reqwest::Client,
    info_url: &'static str,
    page_size: usize,
    page_delay: Duration,
}

impl HlHistorical {
    pub(crate) fn new(http: reqwest::Client, page_size: usize, page_delay: Duration) -> Self {
        Self {
            http,
            info_url: crate::hyperliquid::endpoint::MAINNET_REST_INFO,
            page_size,
            page_delay,
        }
    }
}

#[async_trait::async_trait]
impl HistoricalKlines for HlHistorical {
    async fn fetch_klines(
        &self,
        symbol: &str,
        interval: KlineInterval,
        start_ms: i64,
        end_ms: i64,
    ) -> anyhow::Result<Vec<Kline>> {
        let step = interval.duration_ms();
        let url = self.info_url;
        collect_pages(
            start_ms,
            end_ms,
            step,
            self.page_size,
            self.page_delay,
            |ws, we| {
                let http = self.http.clone();
                let coin = symbol.to_string();
                async move { fetch_window(&http, url, &coin, ws, we).await }
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RESP: &str = r#"[
      {"t":1700000000000,"T":1700000060000,"s":"BTC","i":"1m",
       "o":"35000.0","h":"35100.0","l":"34900.0","c":"35050.0","v":"12.5","n":42},
      {"t":1700000060000,"T":1700000120000,"s":"BTC","i":"1m",
       "o":"35050.0","h":"35200.0","l":"35000.0","c":"35150.0","v":"8.0","n":17}
    ]"#;

    #[test]
    fn parse_maps_ohlc_and_mirrors_ws_zero_volume() {
        let got = parse_candle_snapshot("BTC", RESP).unwrap();
        assert_eq!(got.len(), 2);
        let b = &got[0];
        assert_eq!(b.venue, "hyperliquid");
        assert_eq!(b.symbol, "BTC");
        assert_eq!(b.start_ms, 1_700_000_000_000);
        assert_eq!(b.end_ms, 1_700_000_060_000);
        assert_eq!(b.open, Decimal::new(350000, 1));
        assert_eq!(b.high, Decimal::new(351000, 1));
        assert_eq!(b.low, Decimal::new(349000, 1));
        assert_eq!(b.close, Decimal::new(350500, 1));
        // 与 WS finalize 一致：量为 0、extras 空（v/n 不写入）。
        assert_eq!(b.volume, 0);
        assert_eq!(b.quote_volume, Decimal::ZERO);
        assert!(b.extras.is_empty());
    }

    #[test]
    fn parse_rejects_non_array_envelope() {
        assert!(parse_candle_snapshot("BTC", r#"{"error":"bad"}"#).is_err());
    }

    #[test]
    fn parse_rejects_element_missing_close() {
        let bad = r#"[{"t":1700000000000,"o":"1","h":"2","l":"0.5"}]"#;
        assert!(parse_candle_snapshot("BTC", bad).is_err());
    }

    #[test]
    fn parse_empty_array_is_ok() {
        assert!(parse_candle_snapshot("BTC", "[]").unwrap().is_empty());
    }

    #[test]
    fn request_body_nests_params_under_req() {
        // Hyperliquid 要求参数嵌在 req 内（顶层只有 type + req）。
        let body = candle_snapshot_body("BTC", 1_700_000_000_000, 1_700_000_060_000);
        assert_eq!(body["type"], "candleSnapshot");
        assert_eq!(body["req"]["coin"], "BTC");
        assert_eq!(body["req"]["interval"], "1m");
        assert_eq!(body["req"]["startTime"], 1_700_000_000_000_i64);
        assert_eq!(body["req"]["endTime"], 1_700_000_060_000_i64);
        // 顶层不得平铺参数。
        assert!(body.get("coin").is_none());
        assert!(body.get("startTime").is_none());
    }
}
