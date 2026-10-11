//! Hyperliquid 币名与 asset 索引的纯转换（零网络、零 IO）。
//!
//! 系统内部标的统一带 `USDC` 后缀（`BTCUSDC`），交易所接口用裸币名（`BTC`），
//! 二者在本模块互转；下单/撤单还需把 coin 换成 `meta.universe` 顺序对应的整数
//! asset 索引。差异不泄漏到行情 key 与编排层。
//!
//! 部分 codec 能力（to_canonical / CoinIndex）当前生产路径未全部调用、由单测覆盖，
//! 作为完整币名/索引转换 API 保留。
#![allow(dead_code)]

use std::collections::HashMap;

/// 系统内部统一名追加的报价币后缀。
const USDC_SUFFIX: &str = "USDC";

/// 系统内部统一名（BTCUSDC）→ 交易所裸币名（BTC）。
///
/// 去掉结尾 `USDC`；若去掉后为空（如字面 "USDC"）则不剥离，原样返回，避免空币名。
pub(crate) fn to_native_coin(symbol: &str) -> String {
    let s = symbol.trim();
    match s.strip_suffix(USDC_SUFFIX) {
        Some(rest) if !rest.is_empty() => rest.to_string(),
        _ => s.to_string(),
    }
}

/// 交易所裸币名（BTC）→ 系统内部统一名（BTCUSDC）；已带后缀不重复追加。
pub(crate) fn to_canonical(coin: &str) -> String {
    let s = coin.trim();
    if s.ends_with(USDC_SUFFIX) {
        s.to_string()
    } else {
        format!("{s}{USDC_SUFFIX}")
    }
}

/// coin ↔ asset 整数索引表（索引来自 `meta.universe` 数组顺序）。
#[derive(Debug, Clone, Default)]
pub(crate) struct CoinIndex {
    coin_to_asset: HashMap<String, u32>,
    asset_to_coin: HashMap<u32, String>,
}

impl CoinIndex {
    /// 按 universe 顺序（迭代顺序即 asset 索引）构造。
    pub(crate) fn new<I, S>(coins: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut idx = CoinIndex::default();
        for (asset, coin) in coins.into_iter().enumerate() {
            let coin = coin.into();
            idx.coin_to_asset.insert(coin.clone(), asset as u32);
            idx.asset_to_coin.insert(asset as u32, coin);
        }
        idx
    }

    /// coin → asset 索引；未知币返回 None。
    pub(crate) fn asset_of(&self, coin: &str) -> Option<u32> {
        self.coin_to_asset.get(coin.trim()).copied()
    }

    /// asset 索引 → coin；越界返回 None。
    pub(crate) fn coin_of(&self, asset: u32) -> Option<&str> {
        self.asset_to_coin.get(&asset).map(String::as_str)
    }

    /// 已收录币数量。
    pub(crate) fn len(&self) -> usize {
        self.asset_to_coin.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_usdc_suffix_to_native_coin() {
        assert_eq!(to_native_coin("BTCUSDC"), "BTC");
        assert_eq!(to_native_coin("SOLUSDC"), "SOL");
        // 已是裸币名 / 仅 trim。
        assert_eq!(to_native_coin("  BTC  "), "BTC");
        assert_eq!(to_native_coin("ETH"), "ETH");
    }

    #[test]
    fn appends_usdc_suffix_to_canonical() {
        assert_eq!(to_canonical("BTC"), "BTCUSDC");
        assert_eq!(to_canonical(" SOL "), "SOLUSDC");
        // 已带后缀不重复追加。
        assert_eq!(to_canonical("BTCUSDC"), "BTCUSDC");
    }

    #[test]
    fn lone_or_blank_usdc_is_not_mangled() {
        // 只剩后缀会得到空名时，不剥离，原样返回（避免空币名）。
        assert_eq!(to_native_coin("USDC"), "USDC");
        assert_eq!(to_native_coin(""), "");
    }

    #[test]
    fn maps_coin_to_asset_index_by_universe_order() {
        let idx = CoinIndex::new(["BTC", "ETH", "SOL"]);
        assert_eq!(idx.asset_of("BTC"), Some(0));
        assert_eq!(idx.asset_of("ETH"), Some(1));
        assert_eq!(idx.asset_of("SOL"), Some(2));
        // 未知币。
        assert_eq!(idx.asset_of("DOGE"), None);
        // 反查。
        assert_eq!(idx.coin_of(0), Some("BTC"));
        assert_eq!(idx.coin_of(2), Some("SOL"));
        assert_eq!(idx.coin_of(9), None);
        assert_eq!(idx.len(), 3);
    }
}
