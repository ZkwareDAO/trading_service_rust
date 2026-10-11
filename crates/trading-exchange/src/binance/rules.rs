//! Binance `exchangeInfo` → [`OrderRule`] 归一化（RuleFetcher 的纯内核）。
//!
//! 不碰网络/SDK：只把交易所原始 JSON 解析成统一规则行，HTTP 拉取由薄封装负责。
//! 见 `docs/交易规则.md` 第二节与 6.4 字段映射。
//!
//! 归一化内核已由单测覆盖；消费它的无认证 `RuleFetcher`（HTTP GET exchangeInfo）
//! 与启动/30 天定时接线在下一步落地，此前非测试构建暂无调用方，允许 dead_code。
#![allow(dead_code)]

use std::str::FromStr;

use anyhow::Context;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;
use trading_models::{InstrumentType, OrderRule, QtyUnit};

const EXCHANGE: &str = "binance";

/// Binance 市场：决定端点、合约形状与过滤器差异。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinanceMarket {
    /// U 本位永续：GET /fapi/v1/exchangeInfo，只收 contractType=PERPETUAL 且 TRADING。
    UsdmFutures,
    /// 现货：GET /api/v3/exchangeInfo，只收 status=TRADING；有 MARKET_LOT_SIZE / NOTIONAL。
    Spot,
}

impl BinanceMarket {
    fn instrument_type(self) -> InstrumentType {
        match self {
            // 当前 U 本位永续在本系统统一记为 Futures（Perpetual 为预留扩展，暂不使用）。
            BinanceMarket::UsdmFutures => InstrumentType::Futures,
            BinanceMarket::Spot => InstrumentType::Spot,
        }
    }

    fn source(self) -> &'static str {
        match self {
            BinanceMarket::UsdmFutures => "binance:fapi:exchangeInfo",
            BinanceMarket::Spot => "binance:api:exchangeInfo",
        }
    }
}

// ---------- exchangeInfo 原始 JSON（仅取需要的字段，其余忽略） ----------

#[derive(Debug, Deserialize)]
struct ExchangeInfo {
    #[serde(default)]
    symbols: Vec<RawSymbol>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawSymbol {
    symbol: String,
    status: String,
    /// 仅合约存在（contractType）。
    #[serde(default)]
    contract_type: Option<String>,
    /// 现货报价资产（quoteAsset）。
    #[serde(default)]
    quote_asset: Option<String>,
    /// 合约保证金资产（marginAsset，U 本位即结算币）。
    #[serde(default)]
    margin_asset: Option<String>,
    #[serde(default)]
    filters: Vec<RawFilter>,
}

/// 过滤器用内部 tag + 各字段可选的宽松结构承接，再按 filterType 取字段。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawFilter {
    filter_type: String,
    // PRICE_FILTER
    #[serde(default)]
    min_price: Option<String>,
    #[serde(default)]
    max_price: Option<String>,
    #[serde(default)]
    tick_size: Option<String>,
    // LOT_SIZE / MARKET_LOT_SIZE
    #[serde(default)]
    min_qty: Option<String>,
    #[serde(default)]
    max_qty: Option<String>,
    #[serde(default)]
    step_size: Option<String>,
    // 合约 MIN_NOTIONAL
    #[serde(default)]
    notional: Option<String>,
    // 现货 NOTIONAL
    #[serde(default)]
    min_notional: Option<String>,
    #[serde(default)]
    apply_min_to_market: Option<bool>,
    #[serde(default)]
    apply_max_to_market: Option<bool>,
}

/// 解析整份 exchangeInfo，归一化为规则行。不满足条件（合约非 PERPETUAL、状态非
/// TRADING、缺必备过滤器）的标的跳过；单标的数值解析失败返回错误以便发现映射问题。
pub fn parse_exchange_info(market: BinanceMarket, raw: &str) -> anyhow::Result<Vec<OrderRule>> {
    let info: ExchangeInfo =
        serde_json::from_str(raw).context("parse binance exchangeInfo json")?;
    let fetched_at_ms = Utc::now().timestamp_millis();
    let mut rows = Vec::new();

    for s in info.symbols {
        if !s.status.eq_ignore_ascii_case("TRADING") {
            continue;
        }
        if market == BinanceMarket::UsdmFutures
            && !s
                .contract_type
                .as_deref()
                .is_some_and(|c| c.eq_ignore_ascii_case("PERPETUAL"))
        {
            continue;
        }

        if let Some(row) = map_symbol(market, &s, fetched_at_ms)
            .with_context(|| format!("map binance symbol {}", s.symbol))?
        {
            rows.push(row);
        }
    }
    Ok(rows)
}

/// 把单个标的映射为规则行；缺必备过滤器（PRICE_FILTER / LOT_SIZE）返回 Ok(None) 跳过，
/// 数值字段非法返回 Err（由调用方附 symbol 上下文）。
fn map_symbol(
    market: BinanceMarket,
    s: &RawSymbol,
    fetched_at_ms: i64,
) -> anyhow::Result<Option<OrderRule>> {
    // 必备过滤器/精度字段缺失 → 该标的不可下单，跳过。
    let Some(price) = s.filters.iter().find(|f| f.filter_type == "PRICE_FILTER") else {
        return Ok(None);
    };
    let Some(lot) = s.filters.iter().find(|f| f.filter_type == "LOT_SIZE") else {
        return Ok(None);
    };
    let Some(tick) = price.tick_size.as_deref() else {
        return Ok(None);
    };
    let Some(step) = lot.step_size.as_deref() else {
        return Ok(None);
    };
    let settle = match market {
        BinanceMarket::UsdmFutures => s.margin_asset.clone(),
        BinanceMarket::Spot => s.quote_asset.clone(),
    }
    .unwrap_or_default();

    let mut row = OrderRule::new(
        EXCHANGE,
        s.symbol.clone(),
        market.instrument_type(),
        QtyUnit::Base,
        Decimal::ONE, // 现货/永续：1 下单单位 = 1 标的
        settle.clone(),
        settle,
        parse_dec(step)?,
        market.source(),
        fetched_at_ms,
    );
    row.price_tick = Some(parse_dec(tick)?);
    row.price_min = opt_dec(&price.min_price)?;
    row.price_max = opt_dec(&price.max_price)?;
    row.qty_min = opt_dec(&lot.min_qty)?;
    row.qty_max = opt_dec(&lot.max_qty)?;

    // 名义额：合约 MIN_NOTIONAL.notional；现货 NOTIONAL.minNotional。
    if let Some(n) = s.filters.iter().find(|f| f.filter_type == "MIN_NOTIONAL") {
        row.notional_min = opt_dec(&n.notional)?;
    }
    if let Some(n) = s.filters.iter().find(|f| f.filter_type == "NOTIONAL") {
        row.notional_min = opt_dec(&n.min_notional)?;
        row.notional_min_to_market = n.apply_min_to_market.unwrap_or(false);
        row.notional_max_to_market = n.apply_max_to_market.unwrap_or(false);
    }

    // 现货独有：市价数量档。
    if market == BinanceMarket::Spot {
        if let Some(mlot) = s
            .filters
            .iter()
            .find(|f| f.filter_type == "MARKET_LOT_SIZE")
        {
            row.market_qty_step = opt_dec(&mlot.step_size)?;
            row.market_qty_min = opt_dec(&mlot.min_qty)?;
            row.market_qty_max = opt_dec(&mlot.max_qty)?;
        }
    }
    Ok(Some(row))
}

fn parse_dec(raw: &str) -> anyhow::Result<Decimal> {
    Decimal::from_str(raw.trim()).map_err(|e| anyhow::anyhow!("invalid decimal `{raw}`: {e}"))
}

/// 解析可选 Decimal 字段；`None` 透传，`Some(非法)` 报错。
fn opt_dec(raw: &Option<String>) -> anyhow::Result<Option<Decimal>> {
    raw.as_deref().map(parse_dec).transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    // 一份最小但字段齐全的 U 本位永续 exchangeInfo（BTCUSDT）。
    const FAPI_BTC: &str = r#"{
      "timezone": "UTC",
      "symbols": [
        {
          "symbol": "BTCUSDT",
          "status": "TRADING",
          "contractType": "PERPETUAL",
          "quoteAsset": "USDT",
          "marginAsset": "USDT",
          "filters": [
            {"filterType":"PRICE_FILTER","minPrice":"1000.0","maxPrice":"10000000.0","tickSize":"0.10"},
            {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"1000","stepSize":"0.001"},
            {"filterType":"MIN_NOTIONAL","notional":"5"}
          ]
        }
      ]
    }"#;

    #[test]
    fn parses_usdm_perpetual_filters() {
        let rows = parse_exchange_info(BinanceMarket::UsdmFutures, FAPI_BTC).unwrap();
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.exchange, "binance");
        assert_eq!(r.symbol, "BTCUSDT");
        assert_eq!(r.instrument_type, InstrumentType::Futures);
        assert_eq!(r.qty_unit, QtyUnit::Base);
        assert_eq!(r.multiplier, Decimal::ONE);
        assert_eq!(r.quote_currency, "USDT");
        assert_eq!(r.settle_currency, "USDT");
        assert_eq!(r.price_tick, Some(dec("0.10")));
        assert_eq!(r.price_min, Some(dec("1000.0")));
        assert_eq!(r.price_max, Some(dec("10000000.0")));
        assert_eq!(r.qty_step, dec("0.001"));
        assert_eq!(r.qty_min, Some(dec("0.001")));
        assert_eq!(r.qty_max, Some(dec("1000")));
        assert_eq!(r.notional_min, Some(dec("5")));
        // 永续不使用市价档/到期日。
        assert!(r.market_qty_step.is_none());
        assert!(r.expires_at_ms.is_none());
        assert_eq!(r.source, "binance:fapi:exchangeInfo");
        assert!(r.fetched_at_ms > 0);
    }

    #[test]
    fn skips_non_perpetual_and_non_trading_contracts() {
        let raw = r#"{"symbols":[
          {"symbol":"ETHUSDT","status":"TRADING","contractType":"CURRENT_QUARTER","quoteAsset":"USDT","marginAsset":"USDT","filters":[
            {"filterType":"PRICE_FILTER","tickSize":"0.01","minPrice":"1","maxPrice":"9"},
            {"filterType":"LOT_SIZE","stepSize":"0.001","minQty":"0.001","maxQty":"100"}]},
          {"symbol":"SOLUSDT","status":"PENDING_TRADING","contractType":"PERPETUAL","quoteAsset":"USDT","marginAsset":"USDT","filters":[
            {"filterType":"PRICE_FILTER","tickSize":"0.01","minPrice":"1","maxPrice":"9"},
            {"filterType":"LOT_SIZE","stepSize":"1","minQty":"1","maxQty":"100"}]}
        ]}"#;
        let rows = parse_exchange_info(BinanceMarket::UsdmFutures, raw).unwrap();
        assert!(rows.is_empty(), "交割合约与非 TRADING 均应跳过");
    }

    #[test]
    fn parses_spot_market_lot_and_notional_flags() {
        let raw = r#"{
          "symbols": [
            {
              "symbol": "BTCUSDT",
              "status": "TRADING",
              "quoteAsset": "USDT",
              "filters": [
                {"filterType":"PRICE_FILTER","minPrice":"0.01","maxPrice":"1000000","tickSize":"0.0100"},
                {"filterType":"LOT_SIZE","minQty":"0.00001","maxQty":"9000","stepSize":"0.00001"},
                {"filterType":"MARKET_LOT_SIZE","minQty":"0.0001","maxQty":"100","stepSize":"0.000100"},
                {"filterType":"NOTIONAL","minNotional":"5.0","applyMinToMarket":true,"applyMaxToMarket":false}
              ]
            }
          ]
        }"#;
        let rows = parse_exchange_info(BinanceMarket::Spot, raw).unwrap();
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.instrument_type, InstrumentType::Spot);
        assert_eq!(r.settle_currency, "USDT");
        assert_eq!(r.qty_step, dec("0.00001"));
        assert_eq!(r.market_qty_step, Some(dec("0.000100")));
        assert_eq!(r.market_qty_min, Some(dec("0.0001")));
        assert_eq!(r.market_qty_max, Some(dec("100")));
        assert_eq!(r.notional_min, Some(dec("5.0")));
        assert!(r.notional_min_to_market);
        assert!(!r.notional_max_to_market);
    }

    #[test]
    fn skips_symbol_missing_price_or_lot_filter() {
        let raw = r#"{"symbols":[
          {"symbol":"BADUSDT","status":"TRADING","contractType":"PERPETUAL","quoteAsset":"USDT","marginAsset":"USDT","filters":[
            {"filterType":"PRICE_FILTER","tickSize":"0.1","minPrice":"1","maxPrice":"9"}]}
        ]}"#;
        // 缺 LOT_SIZE：跳过该行而不是中断整批解析。
        let rows = parse_exchange_info(BinanceMarket::UsdmFutures, raw).unwrap();
        assert!(rows.is_empty());
    }
}
