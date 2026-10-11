//! Deribit 期权定价纯内核：分档 tick、市价转限价（开仓漂移 / 平仓对手价）、
//! 平仓价差闸门。全程 [`Decimal`]、零网络，供 TDD。
//!
//! 见 `docs/deribit开平仓处理.md`。

use rust_decimal::Decimal;
use trading_models::{PriceTier, Side};

/// 价差闸门结果：过闸返回绝对价差。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpreadGate {
    pub spread: Decimal,
    pub threshold: Decimal,
}

impl std::fmt::Display for SpreadGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "spread {} exceeds threshold {}",
            self.spread, self.threshold
        )
    }
}

/// 从分档表选 tick：取所有 `above_price <= price` 中门槛最高的一档；
/// 无适用档（空表或价低于首档）回退基础 tick。
pub fn tick_for_price(price: Decimal, tiers: &[PriceTier], base_tick: Decimal) -> Decimal {
    tiers
        .iter()
        .filter(|t| price >= t.above_price)
        .max_by_key(|t| t.above_price)
        .map(|t| t.tick)
        .unwrap_or(base_tick)
}

/// 把价格对齐到 tick 整数倍：`ceil=true` 向上、否则向下（朝零方向，正数即 floor）。
pub fn truncate_to_tick(price: Decimal, tick: Decimal, ceil: bool) -> Decimal {
    let scaled = price / tick;
    let rounded = if ceil { scaled.ceil() } else { scaled.floor() };
    rounded * tick
}

/// 市价开仓的「档位漂移」被动价：
/// BUY 低于 mark N tick（向下取整）；SELL 高于 mark N tick（向上取整）。
/// 取整方向保证挂单价不越过目标价。
pub fn open_price(
    mark: Decimal,
    side: Side,
    tiers: &[PriceTier],
    base_tick: Decimal,
    drift_ticks: i64,
) -> Decimal {
    let tick = tick_for_price(mark, tiers, base_tick);
    let drift = tick * Decimal::from(drift_ticks);
    let price = match side {
        Side::Buy => truncate_to_tick(mark - drift, tick, false),
        Side::Sell => truncate_to_tick(mark + drift, tick, true),
    };
    // 下界保护：便宜期权（mark 极小）漂移档数可能把挂单价压到 ≤0，交易所会拒绝。
    // BUY 被动价兜底为最小正价（1 tick）；SELL 方向加 drift，恒正不受影响。
    //
    // 注：兜底到 1 tick 等价于把实际漂移档数从 drift_ticks 收敛为 1——极便宜期权
    // 的 mark 本身放不下 N 档（如 mark=0.0002、tick=0.0001 时最多漂 1 档），
    // "N 档被动漂移"在这种合约上数学上不成立。挂单价仍 < mark、不会更贵成交；
    // 正常期权（空间充足）此分支不触发，照常漂满 drift_ticks。
    if price <= Decimal::ZERO {
        tick
    } else {
        price
    }
}

/// 平仓可成交对手价：BUY（平空）吃卖一；SELL（平多）吃买一。**不做 tick 截断**。
pub fn close_price(bid: Decimal, ask: Decimal, side: Side) -> Decimal {
    match side {
        Side::Buy => ask,
        Side::Sell => bid,
    }
}

/// 平仓价差闸门：`|ask − bid|` ≤ threshold 过闸（等号也算），否则返回 Err。
pub fn check_spread(bid: Decimal, ask: Decimal, threshold: Decimal) -> Result<Decimal, SpreadGate> {
    let spread = (ask - bid).abs();
    if spread <= threshold {
        Ok(spread)
    } else {
        Err(SpreadGate { spread, threshold })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    /// 期权风格分档（价越低 tick 越小）。
    fn tiers() -> Vec<PriceTier> {
        vec![
            PriceTier {
                above_price: dec("0"),
                tick: dec("0.0005"),
            },
            PriceTier {
                above_price: dec("0.001"),
                tick: dec("0.001"),
            },
            PriceTier {
                above_price: dec("0.005"),
                tick: dec("0.005"),
            },
        ]
    }
    const BASE_TICK: &str = "0.0005";

    // ---------- 分档 tick ----------

    #[test]
    fn tick_picks_highest_applicable_tier() {
        assert_eq!(
            tick_for_price(dec("0.0008"), &tiers(), dec(BASE_TICK)),
            dec("0.0005")
        );
        // 门槛边界：等于 above_price 即适用该档。
        assert_eq!(
            tick_for_price(dec("0.001"), &tiers(), dec(BASE_TICK)),
            dec("0.001")
        );
        assert_eq!(
            tick_for_price(dec("0.004"), &tiers(), dec(BASE_TICK)),
            dec("0.001")
        );
        assert_eq!(
            tick_for_price(dec("0.02"), &tiers(), dec(BASE_TICK)),
            dec("0.005")
        );
    }

    #[test]
    fn tick_falls_back_to_base_tick() {
        // 空档位 → 基础 tick。
        assert_eq!(tick_for_price(dec("1"), &[], dec(BASE_TICK)), dec("0.0005"));
        // 低于首档门槛也回退（首档 above 通常为 0，此处用更高首档构造）。
        let t = vec![PriceTier {
            above_price: dec("0.1"),
            tick: dec("0.01"),
        }];
        assert_eq!(
            tick_for_price(dec("0.05"), &t, dec(BASE_TICK)),
            dec("0.0005")
        );
    }

    // ---------- floor / ceil 取整 ----------

    #[test]
    fn truncate_aligns_to_tick_multiple() {
        assert_eq!(
            truncate_to_tick(dec("0.0021"), dec("0.0005"), false),
            dec("0.0020")
        );
        assert_eq!(
            truncate_to_tick(dec("0.0021"), dec("0.0005"), true),
            dec("0.0025")
        );
        // 已在网格上：不变。
        assert_eq!(
            truncate_to_tick(dec("0.0020"), dec("0.0005"), false),
            dec("0.0020")
        );
    }

    // ---------- 开仓档位漂移 ----------

    #[test]
    fn open_price_buy_floors_below_mark_sell_ceils_above() {
        // mark=0.5 → 适用 tick 0.005；漂移 5 tick=0.025。
        assert_eq!(
            open_price(dec("0.5"), Side::Buy, &tiers(), dec(BASE_TICK), 5),
            dec("0.475")
        );
        assert_eq!(
            open_price(dec("0.5"), Side::Sell, &tiers(), dec(BASE_TICK), 5),
            dec("0.525")
        );
    }

    #[test]
    fn cheap_option_uses_base_tick_and_floor_clamps_to_one_tick() {
        // 真实单档形态：base 0.0001，分档 {above 0.005 → 0.0005}。
        let tiers = vec![PriceTier {
            above_price: dec("0.005"),
            tick: dec("0.0005"),
        }];
        // mark=0.0002 < 0.005 → 必须用 base tick 0.0001（不是分档 0.0005）。
        // 5 档漂移 = 0.0005 > mark 本身；挂单价不能为负/零，兜底到最小正价 1 tick。
        let price = open_price(dec("0.0002"), Side::Buy, &tiers, dec("0.0001"), 5);
        assert_eq!(price, dec("0.0001"));
    }

    // ---------- 平仓对手价 ----------

    #[test]
    fn close_price_uses_opposite_side_of_book() {
        // BUY（平空）吃卖一；SELL（平多）吃买一。
        assert_eq!(
            close_price(dec("0.40"), dec("0.42"), Side::Buy),
            dec("0.42")
        );
        assert_eq!(
            close_price(dec("0.40"), dec("0.42"), Side::Sell),
            dec("0.40")
        );
    }

    // ---------- 平仓价差闸门 ----------

    #[test]
    fn spread_gate_passes_when_at_or_below_threshold() {
        // spread 0.004 < 0.005。
        assert_eq!(
            check_spread(dec("0.400"), dec("0.404"), dec("0.005")),
            Ok(dec("0.004"))
        );
        // 边界：恰好等于阈值也算过闸。
        assert_eq!(
            check_spread(dec("0.400"), dec("0.405"), dec("0.005")),
            Ok(dec("0.005"))
        );
    }

    #[test]
    fn spread_gate_blocks_when_above_threshold() {
        assert_eq!(
            check_spread(dec("0.400"), dec("0.406"), dec("0.005")),
            Err(SpreadGate {
                spread: dec("0.006"),
                threshold: dec("0.005")
            })
        );
    }
}
