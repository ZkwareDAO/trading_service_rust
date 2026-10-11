//! 条件比较操作符的纯函数解释：`metric + operator + value`。
//!
//! 零 IO、全程 [`Decimal`]（禁 f64）；操作符串来自 `WatchRule.operator`，
//! 由风控引擎（本模块）解释，model 只保证字符串原样落盘。

use rust_decimal::Decimal;

/// 规则条件支持的五种比较操作符。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    /// `>=`
    Gte,
    /// `<=`
    Lte,
    /// `>`
    Gt,
    /// `<`
    Lt,
    /// `==`（定点严格相等）
    Eq,
}

impl CompareOp {
    /// 解析规则行上的操作符串；空白容忍，无法识别返回 None（调用方按配置错误处理）。
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim() {
            ">=" => Some(Self::Gte),
            "<=" => Some(Self::Lte),
            ">" => Some(Self::Gt),
            "<" => Some(Self::Lt),
            "==" => Some(Self::Eq),
            _ => None,
        }
    }

    /// 比较指标值与阈值。
    pub fn matches(self, metric: Decimal, threshold: Decimal) -> bool {
        match self {
            Self::Gte => metric >= threshold,
            Self::Lte => metric <= threshold,
            Self::Gt => metric > threshold,
            Self::Lt => metric < threshold,
            Self::Eq => metric == threshold,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn parses_all_supported_operators_and_rejects_unknown() {
        assert_eq!(CompareOp::parse(">="), Some(CompareOp::Gte));
        assert_eq!(CompareOp::parse("<="), Some(CompareOp::Lte));
        assert_eq!(CompareOp::parse(">"), Some(CompareOp::Gt));
        assert_eq!(CompareOp::parse("<"), Some(CompareOp::Lt));
        assert_eq!(CompareOp::parse("=="), Some(CompareOp::Eq));
        // 容忍首尾空白；不支持 != / =< => 等。
        assert_eq!(CompareOp::parse("  >= "), Some(CompareOp::Gte));
        assert_eq!(CompareOp::parse("!="), None);
        assert_eq!(CompareOp::parse("=<"), None);
        assert_eq!(CompareOp::parse(""), None);
    }

    #[test]
    fn compares_inclusive_and_exclusive_bounds() {
        assert!(CompareOp::Gte.matches(dec("0.12"), dec("0.12"))); // 边界含等号
        assert!(CompareOp::Lte.matches(dec("90000"), dec("90000")));
        assert!(!CompareOp::Gt.matches(dec("1"), dec("1")));
        assert!(!CompareOp::Lt.matches(dec("1"), dec("1")));
        assert!(CompareOp::Gt.matches(dec("1.0001"), dec("1")));
        assert!(CompareOp::Lt.matches(dec("0.9999"), dec("1")));
    }

    #[test]
    fn equality_is_exact_fixed_point() {
        // ROI 0.12 与阈值 0.12 严格相等；浮点不可表达的 0.1 在 Decimal 下精确。
        assert!(CompareOp::Eq.matches(dec("0.1"), dec("0.1")));
        assert!(!CompareOp::Eq.matches(dec("0.12"), dec("0.1201")));
        // 负数方向（亏损 ROI）也成立。
        assert!(CompareOp::Lte.matches(dec("-0.05"), dec("-0.01")));
    }
}
