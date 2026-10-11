//! Hyperliquid 市价 IOC 定价的纯内核（零网络、零 IO，全程 Decimal）。
//!
//! Hyperliquid 无真市价单，市价转 IOC 限价：以中间价 mid 为基准，买入向上、
//! 卖出向下各放 slippage（默认 5%），再按 5 位有效数字取整；下单数量对齐
//! 到数量步长。价格精度口径见 `docs/扩展交易所.md` 6.1/6.3。

use rust_decimal::Decimal;
use rust_decimal::RoundingStrategy;

/// Hyperliquid 价格有效数字位数。
const SIG_FIGS: u32 = 5;

/// 以 mid 为基准按方向放滑点：买单向上（保证吃到卖单）、卖单向下。
pub(crate) fn apply_slippage(mid: Decimal, is_buy: bool, slippage: Decimal) -> Decimal {
    if is_buy {
        mid * (Decimal::ONE + slippage)
    } else {
        mid * (Decimal::ONE - slippage)
    }
}

/// 计算 floor(log10(abs(v)))（v≠0），纯 Decimal 实现，避免引入 f64。
///
/// 把绝对值归一到 [1,10) 期间以 10 为底缩放，统计指数。
fn log10_floor(v: Decimal) -> i32 {
    let mut x = v.abs();
    let mut exp = 0i32;
    if x >= Decimal::ONE {
        while x >= Decimal::from(10u32) {
            x /= Decimal::from(10u32);
            exp += 1;
        }
    } else {
        while x < Decimal::ONE {
            x *= Decimal::from(10u32);
            exp -= 1;
        }
    }
    exp
}

/// 把价格四舍五入到 `decimal_places` 位小数；支持负位数（向十/百位取整）。
///
/// 半数远离零（与 JS `Math.round` 对正数的行为一致）。
fn round_to_places(value: Decimal, decimal_places: i32) -> Decimal {
    if decimal_places >= 0 {
        value.round_dp_with_strategy(
            decimal_places as u32,
            RoundingStrategy::MidpointAwayFromZero,
        )
    } else {
        // 负位数：按 10^(-decimal_places) 的整数倍取整。
        let mut factor = Decimal::ONE;
        for _ in 0..(-decimal_places) {
            factor *= Decimal::from(10u32);
        }
        let scaled = value / factor;
        scaled.round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero) * factor
    }
}

/// 按 `sig_figs` 位有效数字四舍五入；0 保持 0。
pub(crate) fn round_sig_figs(value: Decimal, sig_figs: u32) -> Decimal {
    if value == Decimal::ZERO {
        return Decimal::ZERO;
    }
    // 取整的小数位 = 有效数字位数 − 1 − 量级指数。
    let decimal_places = sig_figs as i32 - 1 - log10_floor(value);
    round_to_places(value, decimal_places)
}

/// 市价 IOC 限价：mid 放滑点后按 5 位有效数字取整。
pub(crate) fn market_limit_price(mid: Decimal, is_buy: bool, slippage: Decimal) -> Decimal {
    round_sig_figs(apply_slippage(mid, is_buy, slippage), SIG_FIGS)
}

/// 数量向下（向零）对齐到步长整数倍，绝不超量；不足一步返回 0。
pub(crate) fn align_size(size: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return size;
    }
    let multiples = (size / step).round_dp_with_strategy(0, RoundingStrategy::ToZero);
    multiples * step
}

/// 生成 Hyperliquid wire 字符串：去掉小数部分无意义的尾随零。
///
/// 对齐步长后数量常带尾随零（如 BTC 0.002 对齐 0.00001 → 0.00200）。
/// Hyperliquid 服务端按归一化后的值重构 action 再校验 L1 哈希，若我们把
/// 带尾随零的字符串送进 msgpack 哈希，两端哈希即分叉、ecrecover 出错误
/// 地址（官方文档将 "trailing zeroes on numbers" 列为签名坑，SDK 用
/// `Decimal.normalize()` 规避）。价格、数量在哈希与 body 中都必须经此归一化。
pub(crate) fn wire_normalize(value: Decimal) -> String {
    value.normalize().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn applies_slippage_in_favorable_direction() {
        // 买单：mid ×(1+slippage) 向上，保证能吃到卖单。
        assert_eq!(apply_slippage(dec("100"), true, dec("0.05")), dec("105"));
        // 卖单：mid ×(1−slippage) 向下。
        assert_eq!(apply_slippage(dec("100"), false, dec("0.05")), dec("95"));
        assert_eq!(
            apply_slippage(dec("80000"), true, dec("0.05")),
            dec("84000")
        );
    }

    #[test]
    fn rounds_high_priced_asset_to_five_sig_figs() {
        // 82966.5 → 5 位有效数字 = 82966（无小数）。
        assert_eq!(round_sig_figs(dec("82966.5"), SIG_FIGS), dec("82967"));
        // 82966 已是 5 位，保持。
        assert_eq!(round_sig_figs(dec("82966"), SIG_FIGS), dec("82966"));
        // 8296.6 → 8296.7（5 位：4 整数 + 1 小数）。
        assert_eq!(round_sig_figs(dec("8296.66"), SIG_FIGS), dec("8296.7"));
    }

    #[test]
    fn rounds_low_priced_asset_to_five_sig_figs() {
        // 0.0488725 → 0.048873（前导 0 不计有效数字，5 位在小数上）。
        assert_eq!(round_sig_figs(dec("0.0488725"), SIG_FIGS), dec("0.048873"));
        // 0.5 → 0.50000（1 位有效数字补到 5 位）。
        assert_eq!(round_sig_figs(dec("0.5"), SIG_FIGS), dec("0.50000"));
    }

    #[test]
    fn market_limit_price_combines_slippage_and_sig_figs() {
        // 买：100 →105 →5 位有效数字 105。
        assert_eq!(
            market_limit_price(dec("100"), true, dec("0.05")),
            dec("105")
        );
        // 买：82966.5 ×1.05 = 87114.825 → 87115。
        assert_eq!(
            market_limit_price(dec("82966.5"), true, dec("0.05")),
            dec("87115")
        );
        // 卖：82966.5 ×0.95 = 78818.175 → 78818。
        assert_eq!(
            market_limit_price(dec("82966.5"), false, dec("0.05")),
            dec("78818")
        );
    }

    #[test]
    fn aligns_size_down_to_step() {
        // step 0.1：1.234 → 1.2（向下对齐，绝不超量）。
        assert_eq!(align_size(dec("1.234"), dec("0.1")), dec("1.2"));
        // 已是步长整数倍：保持。
        assert_eq!(align_size(dec("1.5"), dec("0.1")), dec("1.5"));
        // BTC step 0.00001。
        assert_eq!(align_size(dec("0.001005"), dec("0.00001")), dec("0.00100"));
        // 不足一个步长 → 0（调用方应据此拒单）。
        assert_eq!(align_size(dec("0.000005"), dec("0.00001")), dec("0"));
    }
}
