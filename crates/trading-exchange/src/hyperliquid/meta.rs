//! Hyperliquid `meta` 响应纯解析 → 统一 [`OrderRule`]（零网络、零 IO）。
//!
//! `meta.universe[]` 每个币一项：`name`、`szDecimals`（数量精度）、`maxLeverage`，
//! 可选 `isDelisted`。归一化口径：
//! - 数量步长 `qty_step = 10^(-szDecimals)`；
//! - 价格无固定 tick，标记 `price_max_sig_digits=5`；
//! - 报价 / 结算币恒为 USDC，乘数 1、按币（QtyUnit::Base）计；
//! - `maxLeverage` 暂存入 `extra`；下架币过滤。

use rust_decimal::Decimal;
use trading_models::{InstrumentType, OrderRule, QtyUnit};

/// 交易所规范名与来源标识、报价 / 结算币种。
const EXCHANGE: &str = "hyperliquid";
const SOURCE: &str = "hyperliquid:meta";
const USDC: &str = "USDC";
/// Hyperliquid 价格精度：5 位有效数字。
const PRICE_SIG_DIGITS: u8 = 5;
/// extra 中存放最大杠杆的键名。
const MAX_LEVERAGE_KEY: &str = "max_leverage";
/// extra 中存放 asset 整数索引（universe 顺序）的键名，下单/撤单 wire 需要。
const ASSET_INDEX_KEY: &str = "asset_index";

/// meta 响应原始结构（仅取需要字段，其余忽略）。
#[derive(Debug, serde::Deserialize)]
struct RawMeta {
    #[serde(default)]
    universe: Vec<RawAsset>,
}

/// universe 单个币的原始字段。
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct RawAsset {
    name: String,
    sz_decimals: u32,
    max_leverage: u64,
    /// 存在且为 true 表示已下架。
    #[serde(default)]
    is_delisted: bool,
}

/// 解析 meta 响应为规则行；下架币过滤。
///
/// `fetched_at_ms` 为拉取时间（毫秒）；`rule_is_testnet` 标注本批规则的
/// 来源网络（true=测试网 meta），使两网同名币在规则表中各自独立。
pub(crate) fn parse_meta(
    raw: &str,
    fetched_at_ms: i64,
    rule_is_testnet: bool,
) -> anyhow::Result<Vec<OrderRule>> {
    let meta: RawMeta =
        serde_json::from_str(raw).map_err(|e| anyhow::anyhow!("parse hyperliquid meta: {e}"))?;
    let mut rows = Vec::with_capacity(meta.universe.len());
    // asset 索引按 universe 原始顺序计数（下架币也占位，保证索引与交易所一致）。
    for (asset_index, asset) in meta.universe.into_iter().enumerate() {
        if asset.is_delisted {
            continue;
        }
        rows.push(map_asset(
            asset,
            fetched_at_ms,
            asset_index as u32,
            rule_is_testnet,
        ));
    }
    Ok(rows)
}

/// 单个币 → 规则行：填步长 / 价格有效数字 / USDC / 最大杠杆 / asset 索引。
fn map_asset(
    asset: RawAsset,
    fetched_at_ms: i64,
    asset_index: u32,
    rule_is_testnet: bool,
) -> OrderRule {
    let mut row = OrderRule::new(
        EXCHANGE,
        asset.name.clone(),
        InstrumentType::Futures,
        QtyUnit::Base,
        Decimal::ONE,
        USDC,
        USDC,
        qty_step(asset.sz_decimals),
        SOURCE,
        fetched_at_ms,
    );
    // Hyperliquid 无固定 tick，价格按 5 位有效数字口径。
    row.price_max_sig_digits = Some(PRICE_SIG_DIGITS);
    // 最大杠杆暂存逃生舱，供后续杠杆守卫使用。
    row.extra
        .insert(MAX_LEVERAGE_KEY.to_string(), asset.max_leverage.to_string());
    // asset 整数索引：下单/撤单 wire 的 a 字段。
    row.extra
        .insert(ASSET_INDEX_KEY.to_string(), asset_index.to_string());
    // 标注本批规则的来源网络。
    row.with_testnet(rule_is_testnet)
}

/// 数量步长 = 10^(-sz_decimals)；sz_decimals=0 时为 1。
fn qty_step(sz_decimals: u32) -> Decimal {
    // 循环累乘 10^sz_decimals，避免依赖 rust_decimal 的幂方法。
    let mut scale = Decimal::ONE;
    for _ in 0..sz_decimals {
        scale *= Decimal::from(10u32);
    }
    Decimal::ONE / scale
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    /// 一份精简但字段完整的 meta（BTC/ETH/SOL）。
    const META: &str = r#"{
      "universe": [
        {"szDecimals": 5, "name": "BTC", "maxLeverage": 40, "marginTableId": 56},
        {"szDecimals": 4, "name": "ETH", "maxLeverage": 25, "marginTableId": 55},
        {"szDecimals": 2, "name": "SOL", "maxLeverage": 20, "marginTableId": 54}
      ]
    }"#;

    #[test]
    fn parses_universe_into_order_rules() {
        let rows = parse_meta(META, 1_000, false).expect("parse ok");
        assert_eq!(rows.len(), 3);
        let btc = &rows[0];
        assert_eq!(btc.exchange, "hyperliquid");
        assert_eq!(btc.symbol, "BTC");
        assert_eq!(btc.instrument_type, InstrumentType::Futures);
        assert_eq!(btc.qty_unit, QtyUnit::Base);
        assert_eq!(btc.multiplier, Decimal::ONE);
        assert_eq!(btc.quote_currency, "USDC");
        assert_eq!(btc.settle_currency, "USDC");
        // 数量步长 10^-5；价格有效数字 5。
        assert_eq!(btc.qty_step, dec("0.00001"));
        assert_eq!(btc.price_max_sig_digits, Some(5));
        // maxLeverage / asset 索引入 extra。
        assert_eq!(btc.extra.get("max_leverage"), Some(&"40".to_string()));
        assert_eq!(btc.extra.get("asset_index"), Some(&"0".to_string()));
        assert_eq!(rows[1].extra.get("asset_index"), Some(&"1".to_string()));
        assert_eq!(btc.source, "hyperliquid:meta");
        assert_eq!(btc.fetched_at_ms, 1_000);
        // ETH / SOL 步长。
        assert_eq!(rows[1].qty_step, dec("0.0001"));
        assert_eq!(rows[2].qty_step, dec("0.01"));
    }

    #[test]
    fn filters_delisted_assets() {
        let raw = r#"{"universe":[
          {"szDecimals":5,"name":"BTC","maxLeverage":40},
          {"szDecimals":1,"name":"MATIC","maxLeverage":20,"isDelisted":true}
        ]}"#;
        let rows = parse_meta(raw, 0, false).expect("parse ok");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].symbol, "BTC");
    }

    #[test]
    fn empty_universe_yields_no_rules() {
        let rows = parse_meta(r#"{"universe":[]}"#, 7, false).expect("parse ok");
        assert!(rows.is_empty());
    }

    #[test]
    fn invalid_json_is_error() {
        assert!(parse_meta("not json", 0, false).is_err());
        assert!(parse_meta(r#"{"universe":42}"#, 0, false).is_err());
    }

    #[test]
    fn rows_are_tagged_with_source_network() {
        // 同一 meta 按网络打标：行与派生 key 都携带对应网络位。
        let mainnet = parse_meta(META, 1_000, false).expect("parse");
        let testnet = parse_meta(META, 1_000, true).expect("parse");
        assert!(!mainnet[0].rule_is_testnet);
        assert!(testnet[0].rule_is_testnet);
        // asset_index 仍按各自 universe 顺序（此精简 meta 下 BTC 同为 0）。
        assert_eq!(mainnet[0].extra.get("asset_index"), Some(&"0".to_string()));
        assert_eq!(testnet[0].extra.get("asset_index"), Some(&"0".to_string()));
        // 两网同名行的 key 不同，可在表中共存。
        assert_ne!(mainnet[0].key(), testnet[0].key());
    }
}
