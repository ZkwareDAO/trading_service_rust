//! CTP 主力合约选举（纯函数 + 收盘选举表）。
//!
//! 规则（见 plan / 时序图）：
//! - 某品种各合约「当日最后 tick 的累计成交量」最大者为主力；
//! - 并列时用**固定、与时间无关**的次序打破平局（合约字典序），保证确定性、可重放；
//! - 当日无任何 tick（重启/数据缺失）→ 沿用上一交易日主力；连首日都没有则 None。
//!
//! 选出后写入 [`trading_core::kline::DominantTable`]（product → symbol + trading_day），
//! 次日（含夜盘整段）冻结，盘中不漂移。

use std::collections::HashMap;

/// 一个合约的当日累计成交量快照。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractVolume {
    pub symbol: String,
    pub cum_volume: i64,
}

/// 从一组同品种合约的当日累计量中选主力。
///
/// - 忽略非正量（集合竞价前 0 量、停牌）；
/// - 全为非正/空集 → 返回 None（调用方沿用上一交易日主力）；
/// - 最大量并列时取合约字典序最小者（固定 tiebreak，不依赖遍历顺序/时间）。
pub fn pick_dominant(volumes: &[ContractVolume]) -> Option<String> {
    volumes
        .iter()
        .filter(|c| c.cum_volume > 0)
        .max_by(|a, b| {
            // 先比量（大者胜），量同再比合约符号（字典序小者胜 → 用 Reverse 语义）。
            a.cum_volume
                .cmp(&b.cum_volume)
                .then_with(|| b.symbol.cmp(&a.symbol))
        })
        .map(|c| c.symbol.clone())
}

/// 按品种聚合选举：输入 (合约, 量) 与「合约 → 品种前缀」解析结果，产出每品种主力。
///
/// `product_of`：从合约代码取品种前缀（如 cu2512→cu），纯函数（典型实现是剥末尾
/// 的数字/年份月份）。无法解析品种的合约跳过。
pub fn elect_dominants<F>(volumes: &[ContractVolume], product_of: F) -> HashMap<String, String>
where
    F: Fn(&str) -> Option<String>,
{
    let mut by_product: HashMap<String, Vec<ContractVolume>> = HashMap::new();
    for c in volumes {
        if let Some(product) = product_of(&c.symbol) {
            by_product.entry(product).or_default().push(c.clone());
        }
    }
    by_product
        .into_iter()
        .filter_map(|(product, vols)| pick_dominant(&vols).map(|sym| (product, sym)))
        .collect()
}

/// 从合约代码剥出品种前缀：去掉末尾的数字，剩余字母即品种（cu2512→cu、rb2601→rb、
/// ag2512→ag）。无字母前缀或纯数字返回 None。
pub fn product_prefix(symbol: &str) -> Option<String> {
    let prefix: &str = symbol.trim_end_matches(|c: char| c.is_ascii_digit());
    if prefix.is_empty() {
        None
    } else {
        Some(prefix.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cv(symbol: &str, vol: i64) -> ContractVolume {
        ContractVolume {
            symbol: symbol.into(),
            cum_volume: vol,
        }
    }

    #[test]
    fn picks_largest_volume() {
        let vols = vec![cv("cu2511", 100), cv("cu2512", 500), cv("cu2601", 300)];
        assert_eq!(pick_dominant(&vols).as_deref(), Some("cu2512"));
    }

    #[test]
    fn tie_breaks_by_smallest_symbol_deterministically() {
        // 量相同：固定取字典序最小者，与输入顺序无关。
        let vols = vec![cv("cu2601", 500), cv("cu2512", 500)];
        assert_eq!(pick_dominant(&vols).as_deref(), Some("cu2512"));
        let vols_rev = vec![cv("cu2512", 500), cv("cu2601", 500)];
        assert_eq!(pick_dominant(&vols_rev).as_deref(), Some("cu2512"));
    }

    #[test]
    fn non_positive_and_empty_yield_none() {
        assert_eq!(pick_dominant(&[]), None);
        assert_eq!(pick_dominant(&[cv("cu2512", 0), cv("cu2601", -1)]), None);
        // 混合：仍选唯一正量者。
        assert_eq!(
            pick_dominant(&[cv("cu2512", 0), cv("cu2601", 3)]).as_deref(),
            Some("cu2601")
        );
    }

    #[test]
    fn product_prefix_strips_trailing_digits() {
        assert_eq!(product_prefix("cu2512").as_deref(), Some("cu"));
        assert_eq!(product_prefix("rb2601").as_deref(), Some("rb"));
        assert_eq!(product_prefix("ag2512").as_deref(), Some("ag"));
        assert_eq!(product_prefix("123"), None);
    }

    #[test]
    fn elect_groups_by_product() {
        let vols = vec![
            cv("cu2511", 100),
            cv("cu2512", 500),
            cv("rb2611", 70),
            cv("rb2601", 900),
        ];
        let got = elect_dominants(&vols, product_prefix);
        assert_eq!(got.get("cu").map(String::as_str), Some("cu2512"));
        assert_eq!(got.get("rb").map(String::as_str), Some("rb2601"));
        assert_eq!(got.len(), 2);
    }
}
