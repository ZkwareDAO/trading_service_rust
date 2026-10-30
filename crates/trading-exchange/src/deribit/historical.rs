//! Deribit 历史 K线 REST 回补（v2 `GET /api/v2/public/get_tradingview_chart_data`）。
//!
//! 必须用 **v2**；`result` 内为等长并行数组 `ticks/open/high/low/close/volume/cost`，
//! zip 成一根根 bar。解析函数 [`parse_chart_data`] 零 IO、可 fixture 单测；网络分页/节流
//! 复用 [`crate::market::collect_pages`]。字段口径与 deribit WS 定稿 `finalize_bar` 一致
//! （volume 取整、quote_volume=0、无 extras），保证同序列回补 bar 与实时 bar 同语义。

use std::time::Duration;

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde_json::Value;

use crate::market::{collect_pages, HistoricalKlines, Kline, KlineInterval};

/// Deribit 公共行情恒定主网（与 WS 口径一致）。
const REST_BASE: &str = "https://www.deribit.com";

/// 解析 get_tradingview_chart_data 响应：`result` 下并行数组按 `ticks` zip。
/// 必需 `ticks/open/high/low/close`；`volume` 缺省 0。任一价格数组缺失/非等长即 `Err`。
pub(crate) fn parse_chart_data(symbol: &str, text: &str) -> anyhow::Result<Vec<Kline>> {
    let v: Value = serde_json::from_str(text)?;
    let result = v
        .get("result")
        .ok_or_else(|| anyhow::anyhow!("deribit chart_data 缺少 result：{text}"))?;
    let ticks = num_arr(result, "ticks")?;
    let opens = dec_arr(result, "open")?;
    let highs = dec_arr(result, "high")?;
    let lows = dec_arr(result, "low")?;
    let closes = dec_arr(result, "close")?;
    let volumes = dec_arr(result, "volume").unwrap_or_default();

    let n = ticks.len();
    if opens.len() != n || highs.len() != n || lows.len() != n || closes.len() != n {
        anyhow::bail!("deribit chart_data 数组长度不一致（ticks={n}）");
    }
    let step = KlineInterval::M1.duration_ms();
    let mut out = Vec::with_capacity(n);
    for (i, &start_ms) in ticks.iter().enumerate() {
        // volume 为浮点张/手数，截断取整（与 WS finalize_bar 同口径）。
        let volume = volumes
            .get(i)
            .copied()
            .unwrap_or(Decimal::ZERO)
            .trunc()
            .to_i64()
            .unwrap_or(0);
        out.push(Kline {
            venue: crate::deribit::market::VENUE.to_string(),
            symbol: symbol.to_string(),
            interval: KlineInterval::M1,
            trading_day: crate::market_ws::trading_day_of_utc_ms(start_ms),
            start_ms,
            end_ms: start_ms + step,
            open: opens[i],
            high: highs[i],
            low: lows[i],
            close: closes[i],
            volume,
            quote_volume: Decimal::ZERO,
            extras: std::collections::BTreeMap::new(),
        });
    }
    Ok(out)
}

/// 取 i64 数值数组（ticks：毫秒）。
fn num_arr(v: &Value, key: &str) -> anyhow::Result<Vec<i64>> {
    let arr = v
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("deribit chart_data 缺少数组 {key}"))?;
    arr.iter()
        .map(|x| {
            x.as_i64()
                .or_else(|| x.as_f64().map(|f| f as i64))
                .ok_or_else(|| anyhow::anyhow!("deribit {key} 含非数值：{x}"))
        })
        .collect()
}

/// 取 Decimal 数组（价格/量：数字或字符串）。
fn dec_arr(v: &Value, key: &str) -> anyhow::Result<Vec<Decimal>> {
    let arr = v
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("deribit chart_data 缺少数组 {key}"))?;
    arr.iter()
        .map(|x| match x {
            Value::String(s) => Decimal::from_str_exact(s.trim())
                .map_err(|_| anyhow::anyhow!("deribit {key} 非法小数：{x}")),
            Value::Number(_) => Decimal::from_str_exact(&x.to_string())
                .map_err(|_| anyhow::anyhow!("deribit {key} 非法小数：{x}")),
            _ => anyhow::bail!("deribit {key} 非数值：{x}"),
        })
        .collect()
}

/// 单窗取数：v2 GET，`resolution=1`（1 分钟），起止均毫秒。
async fn fetch_window(
    http: &reqwest::Client,
    symbol: &str,
    win_start: i64,
    win_end: i64,
) -> anyhow::Result<Vec<Kline>> {
    let url = format!("{REST_BASE}/api/v2/public/get_tradingview_chart_data");
    // 查询参数经 reqwest 自动 percent-encode（instrument_name 含 -/.，且来自 admin/订阅）。
    let resp = http
        .get(url)
        .query(&[
            ("instrument_name", symbol),
            ("start_timestamp", &win_start.to_string()),
            ("end_timestamp", &win_end.to_string()),
            ("resolution", "1"),
        ])
        .send()
        .await?
        .error_for_status()?;
    parse_chart_data(symbol, &resp.text().await?)
}

/// Deribit 历史 K线回补实现。
pub(crate) struct DeribitHistorical {
    http: reqwest::Client,
    /// 本地分页根数：v2 端点本身由时间窗界定，分页窗口在 collect_pages 内按此切分。
    page_size: usize,
    page_delay: Duration,
}

impl DeribitHistorical {
    pub(crate) fn new(http: reqwest::Client, page_size: usize, page_delay: Duration) -> Self {
        Self {
            http,
            page_size,
            page_delay,
        }
    }
}

#[async_trait::async_trait]
impl HistoricalKlines for DeribitHistorical {
    async fn fetch_klines(
        &self,
        symbol: &str,
        interval: KlineInterval,
        start_ms: i64,
        end_ms: i64,
    ) -> anyhow::Result<Vec<Kline>> {
        let step = interval.duration_ms();
        collect_pages(
            start_ms,
            end_ms,
            step,
            self.page_size,
            self.page_delay,
            |ws, we| {
                let http = self.http.clone();
                let sym = symbol.to_string();
                async move { fetch_window(&http, &sym, ws, we).await }
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RESP: &str = r#"{"jsonrpc":"2.0","result":{
      "instrument_name":"BTC-PERPETUAL","resolution":1,
      "ticks":[1700000000000,1700000060000],
      "open":["35000.0","35010.0"],"high":["35100.0","35120.0"],
      "low":["34900.0","34950.0"],"close":["35010.0","35050.0"],
      "volume":[12.7,8.0],"cost":[444000.0,280000.0],"status":"ok"
    },"usIn":1,"usOut":2}"#;

    #[test]
    fn parse_zips_parallel_arrays() {
        let got = parse_chart_data("BTC-PERPETUAL", RESP).unwrap();
        assert_eq!(got.len(), 2);
        let b = &got[0];
        assert_eq!(b.venue, "deribit");
        assert_eq!(b.symbol, "BTC-PERPETUAL");
        assert_eq!(b.start_ms, 1_700_000_000_000);
        assert_eq!(b.end_ms, 1_700_000_060_000);
        assert_eq!(b.open, Decimal::new(350000, 1));
        assert_eq!(b.high, Decimal::new(351000, 1));
        assert_eq!(b.close, Decimal::new(350100, 1));
        assert_eq!(b.volume, 12); // 12.7 截断
        assert_eq!(b.quote_volume, Decimal::ZERO); // 与 WS finalize 一致
        assert!(b.extras.is_empty());
    }

    #[test]
    fn parse_rejects_missing_result() {
        assert!(parse_chart_data("X", r#"{"error":{}}"#).is_err());
    }

    #[test]
    fn parse_rejects_length_mismatch() {
        let bad = r#"{"result":{"ticks":[1,2],"open":["1"],"high":["1","2"],
                    "low":["1","2"],"close":["1","2"]}}"#;
        assert!(parse_chart_data("X", bad).is_err());
    }

    #[test]
    fn parse_rejects_non_numeric_price() {
        let bad = r#"{"result":{"ticks":[1],"open":["x"],"high":["1"],
                    "low":["1"],"close":["1"]}}"#;
        assert!(parse_chart_data("X", bad).is_err());
    }
}
