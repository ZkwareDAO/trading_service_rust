//! CTP 主力合约选举（纯函数 + 收盘选举内核）。
//!
//! 选举规则（指标/确认天数由 `venue.toml [ctp]` 配置）：
//! - 指标 `main_contract_criteria`：当日累计成交量（volume）或持仓量
//!   （open_interest，默认），最大者为主力；
//! - 并列时用**固定、与时间无关**的次序打破平局（合约字典序），保证确定性、可重放；
//! - 当日无任何 tick（重启/数据缺失）→ 沿用当前主力；连首日都没有则 None；
//! - 换月需连续 `main_contract_confirm_days` 个**有数据的交易日**胜出，且候选
//!   合约月份必须**严格前进**（[`candidate_month_advances`]）。
//!
//! 选出后写入 [`trading_core::kline::DominantTable`]（product → symbol + trading_day），
//! 当日（含夜盘整段）冻结，盘中不漂移。

use std::collections::HashMap;

use rust_decimal::Decimal;
use trading_core::kline::LastTickStat;

/// 主力选举指标（`[ctp].main_contract_criteria`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DominantMetric {
    /// 当日累计成交量。
    Volume,
    /// 当日持仓量（默认）。
    #[default]
    OpenInterest,
}

impl DominantMetric {
    /// 解析配置字符串；未知值返回 `None`（由配置层 fail-fast）。
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "volume" => Some(Self::Volume),
            "open_interest" => Some(Self::OpenInterest),
            _ => None,
        }
    }

    /// 配置口径字符串（日志/文档/错误信息展示用）。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Volume => "volume",
            Self::OpenInterest => "open_interest",
        }
    }

    /// 从当日 tick 统计取出参与比较的指标值（两者同口径，均只认当日记录）。
    pub fn value(self, stat: &LastTickStat) -> Decimal {
        match self {
            Self::Volume => Decimal::from(stat.cum_volume),
            Self::OpenInterest => stat.open_interest,
        }
    }
}

/// 一个合约的选举指标快照（品种内比较用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractStat {
    pub symbol: String,
    pub value: Decimal,
}

/// 从一组同品种合约的当日指标中选主力。
///
/// - 忽略非正指标（集合竞价前 0 量、停牌、持仓未就绪）；
/// - 全为非正/空集 → 返回 None（调用方沿用当前主力）；
/// - 最大指标并列时取合约字典序最小者（固定 tiebreak，不依赖遍历顺序/时间）。
pub fn pick_dominant(stats: &[ContractStat]) -> Option<String> {
    stats
        .iter()
        .filter(|c| c.value > Decimal::ZERO)
        .max_by(|a, b| {
            // 先比指标（大者胜），同值再比合约符号（字典序小者胜 → 用 Reverse 语义）。
            a.value.cmp(&b.value).then_with(|| b.symbol.cmp(&a.symbol))
        })
        .map(|c| c.symbol.clone())
}

/// 按品种聚合选举：输入合约指标与「合约 → 品种前缀」解析结果，产出每品种主力。
///
/// `product_of`：从合约代码取品种前缀（如 cu2512→cu），纯函数（典型实现是剥末尾
/// 的数字/年份月份）。无法解析品种的合约跳过。
pub fn elect_dominants<F>(stats: &[ContractStat], product_of: F) -> HashMap<String, String>
where
    F: Fn(&str) -> Option<String>,
{
    let mut by_product: HashMap<String, Vec<ContractStat>> = HashMap::new();
    for c in stats {
        if let Some(product) = product_of(&c.symbol) {
            by_product.entry(product).or_default().push(c.clone());
        }
    }
    by_product
        .into_iter()
        .filter_map(|(product, stats)| pick_dominant(&stats).map(|sym| (product, sym)))
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

/// 解析合约月份 `(year, month)`。
///
/// * 4 位尾数（多数交易所）：`YYMM`，如 `cu2612` → (2026, 12)；
/// * 3 位尾数（郑商所）：`YMM`，如 `TA701`；用 `reference_year` 做十年消歧——
///   取 **≥ reference_year-1** 的最小同余年份（ref=2026、Y=7 → 2027-01，
///   Y=5 → 2025-12）；
/// * 月份越界（0 或 >12）/ 无可解析尾数 → `None`（调用方 fail-closed）。
pub fn contract_month(symbol: &str, reference_year: i32) -> Option<(i32, u32)> {
    let prefix = symbol.trim_end_matches(|c: char| c.is_ascii_digit());
    // 必须有非空品种前缀（纯数字/空串不是合法合约代码）。
    if prefix.is_empty() {
        return None;
    }
    let digits = &symbol[prefix.len()..];
    match digits.len() {
        // YYMM：2000 年后的两位年份（CTP 国内期货口径）。
        4 => {
            let year = 2000 + digits[..2].parse::<i32>().ok()?;
            let month = digits[2..].parse::<u32>().ok()?;
            valid_month(month).then_some((year, month))
        }
        // YMM（郑商所）：单个年位 + 两位月份，十年消歧。
        3 => {
            let year_digit = digits[..1].parse::<i32>().ok()?;
            let month = digits[1..].parse::<u32>().ok()?;
            if !valid_month(month) {
                return None;
            }
            // 最小同余年份：满足 year % 10 == year_digit 且 year >= reference_year-1。
            let base = reference_year - 1;
            let diff = (year_digit - base.rem_euclid(10)).rem_euclid(10);
            Some((base + diff, month))
        }
        _ => None,
    }
}

/// 月份是否合法（1..=12）。
fn valid_month(month: u32) -> bool {
    (1..=12).contains(&month)
}

/// 月份单向守卫：候选月份是否**严格晚于**当前月份（允许进入连击/切换）。
///
/// * 候选不可解析 → `false`（fail-closed，保留当前并告警）；
/// * 当前不可解析、候选合法 → `true`（接受候选）；
/// * 其余按 `(year, month)` 字典序比较（同月/回退月份一律拒绝）。
pub fn candidate_month_advances(current: &str, candidate: &str, reference_year: i32) -> bool {
    let Some(candidate_month) = contract_month(candidate, reference_year) else {
        return false;
    };
    match contract_month(current, reference_year) {
        Some(current_month) => candidate_month > current_month,
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cs(symbol: &str, value: i64) -> ContractStat {
        ContractStat {
            symbol: symbol.into(),
            value: Decimal::from(value),
        }
    }

    #[test]
    fn dominant_metric_parses_and_defaults_to_open_interest() {
        assert_eq!(
            DominantMetric::parse("volume"),
            Some(DominantMetric::Volume)
        );
        assert_eq!(
            DominantMetric::parse(" Open_Interest "),
            Some(DominantMetric::OpenInterest)
        );
        assert_eq!(DominantMetric::parse("oi"), None);
        assert_eq!(DominantMetric::default(), DominantMetric::OpenInterest);
        assert_eq!(DominantMetric::Volume.as_str(), "volume");
        assert_eq!(DominantMetric::OpenInterest.as_str(), "open_interest");
    }

    #[test]
    fn dominant_metric_reads_configured_field() {
        let stat = LastTickStat {
            trading_day: 20261102,
            cum_volume: 123,
            open_interest: Decimal::from(456),
        };
        assert_eq!(DominantMetric::Volume.value(&stat), Decimal::from(123));
        assert_eq!(
            DominantMetric::OpenInterest.value(&stat),
            Decimal::from(456)
        );
    }

    #[test]
    fn picks_largest_value() {
        let stats = vec![cs("cu2511", 100), cs("cu2512", 500), cs("cu2601", 300)];
        assert_eq!(pick_dominant(&stats).as_deref(), Some("cu2512"));
    }

    #[test]
    fn tie_breaks_by_smallest_symbol_deterministically() {
        // 指标相同：固定取字典序最小者，与输入顺序无关。
        let stats = vec![cs("cu2601", 500), cs("cu2512", 500)];
        assert_eq!(pick_dominant(&stats).as_deref(), Some("cu2512"));
        let stats_rev = vec![cs("cu2512", 500), cs("cu2601", 500)];
        assert_eq!(pick_dominant(&stats_rev).as_deref(), Some("cu2512"));
    }

    #[test]
    fn non_positive_and_empty_yield_none() {
        assert_eq!(pick_dominant(&[]), None);
        assert_eq!(pick_dominant(&[cs("cu2512", 0), cs("cu2601", -1)]), None);
        // 混合：仍选唯一正指标者。
        assert_eq!(
            pick_dominant(&[cs("cu2512", 0), cs("cu2601", 3)]).as_deref(),
            Some("cu2601")
        );
    }

    #[test]
    fn product_prefix_strips_trailing_digits() {
        assert_eq!(product_prefix("cu2512").as_deref(), Some("cu"));
        assert_eq!(product_prefix("rb2601").as_deref(), Some("rb"));
        assert_eq!(product_prefix("ag2512").as_deref(), Some("ag"));
        assert_eq!(product_prefix("i2701").as_deref(), Some("i"));
        assert_eq!(product_prefix("TA701").as_deref(), Some("TA"));
        assert_eq!(product_prefix("123"), None);
    }

    #[test]
    fn elect_groups_by_product() {
        let stats = vec![
            cs("cu2511", 100),
            cs("cu2512", 500),
            cs("rb2611", 70),
            cs("rb2601", 900),
        ];
        let got = elect_dominants(&stats, product_prefix);
        assert_eq!(got.get("cu").map(String::as_str), Some("cu2512"));
        assert_eq!(got.get("rb").map(String::as_str), Some("rb2601"));
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn contract_month_parses_four_digit_yymm() {
        assert_eq!(contract_month("cu2612", 2026), Some((2026, 12)));
        assert_eq!(contract_month("rb2701", 2026), Some((2027, 1)));
        assert_eq!(contract_month("i2701", 2026), Some((2027, 1)));
        assert_eq!(contract_month("ag2612", 2026), Some((2026, 12)));
    }

    #[test]
    fn contract_month_parses_three_digit_ymm_with_decade_disambiguation() {
        // ref=2026：Y=7 → 2027（不取 2017），Y=6 → 2026，Y=5 → 2025（≥ ref-1）。
        assert_eq!(contract_month("TA701", 2026), Some((2027, 1)));
        assert_eq!(contract_month("TA603", 2026), Some((2026, 3)));
        assert_eq!(contract_month("TA512", 2026), Some((2025, 12)));
        // 跨十年边界：ref=2030、Y=1 → 2031（≥2029 的最小同余年份）。
        assert_eq!(contract_month("SR105", 2030), Some((2031, 5)));
    }

    #[test]
    fn contract_month_rejects_invalid_shapes_and_months() {
        assert_eq!(contract_month("cu2613", 2026), None); // 月份 13
        assert_eq!(contract_month("cu2600", 2026), None); // 月份 0
        assert_eq!(contract_month("TA700", 2026), None); // 月份 00
        assert_eq!(contract_month("cu26", 2026), None); // 2 位尾数
        assert_eq!(contract_month("cu", 2026), None); // 无尾数
        assert_eq!(contract_month("2612", 2026), None); // 无字母前缀
    }

    #[test]
    fn month_guard_only_advances_forward() {
        // 当前 2701：候选 2612（更早）永不通过；2705 前进通过。
        assert!(!candidate_month_advances("cu2701", "cu2612", 2026));
        assert!(candidate_month_advances("cu2701", "cu2705", 2026));
        // 同月不算前进。
        assert!(!candidate_month_advances("cu2701", "cu2701", 2026));
        // 候选不可解析 → fail-closed 保留当前。
        assert!(!candidate_month_advances("cu2701", "cuXXXX", 2026));
        // 当前不可解析、候选合法 → 接受候选。
        assert!(candidate_month_advances("cuXXXX", "cu2705", 2026));
        // 3 位合约同样生效：TA701(2027-01) vs TA703(2027-03)。
        assert!(candidate_month_advances("TA701", "TA703", 2026));
        assert!(!candidate_month_advances("TA703", "TA701", 2026));
    }
}
