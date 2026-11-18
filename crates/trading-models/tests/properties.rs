//! L1 属性测试：trading-models 纯计算不变量。
//!
//! 纪律（计划 §B4）：
//! * PR 门禁固定 seed、`cases = 10_000`；
//! * 需要随机 seed 的大规模夜间跑：
//!   `PROPTEST_RNG_SEED=<随机数> PROPTEST_CASES=1000000 cargo test -p trading-models --test properties`；
//! * 失败反例写入 crate 根 `proptest-regressions/models.txt`（首次失败时自动建目录），
//!   一旦产生必须随代码提交。

use proptest::prelude::*;
use proptest::test_runner::{Config, FileFailurePersistence, RngSeed};
use rust_decimal::Decimal;
use trading_models::{Position, Side};

/// 门禁默认 seed：固定以保证 PR 可复现；可由 `PROPTEST_RNG_SEED` 覆盖。
const DEFAULT_SEED: u64 = 0x5EED_2026;
/// 门禁快速档用例数；夜间可由 `PROPTEST_CASES` 放大到 1_000_000。
const DEFAULT_CASES: u32 = 10_000;

fn config() -> Config {
    let seed = std::env::var("PROPTEST_RNG_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_SEED);
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(DEFAULT_CASES);
    Config {
        cases,
        rng_seed: RngSeed::Fixed(seed),
        // 反例语料库落在 crate 根 `proptest-regressions/`（必须随代码提交）。
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
            "proptest-regressions/models.txt",
        ))),
        ..Config::default()
    }
}

// ---- 生成器：只用整数区间，保证 Decimal 定点运算无精度丢失去打扰不变量 ----

fn price() -> impl Strategy<Value = Decimal> {
    (1u32..1_000_000).prop_map(Decimal::from)
}

fn quantity() -> impl Strategy<Value = Decimal> {
    (1u32..100_000).prop_map(Decimal::from)
}

fn multiplier() -> impl Strategy<Value = Decimal> {
    prop_oneof![
        Just(Decimal::ONE),
        Just(Decimal::from(10u32)),
        Just(Decimal::from(100u32))
    ]
}

fn side() -> impl Strategy<Value = Side> {
    prop_oneof![Just(Side::Buy), Just(Side::Sell)]
}

/// 构造一个保证金已就位（cost/leverage 兜底口径）的仓位。
fn position(side: Side, pos_price: Decimal, qty: Decimal, mult: Decimal) -> Position {
    let mut p = Position::new(1, "test", "BTCUSDT", side);
    p.quantity = qty;
    p.pos_price = pos_price;
    p.multiplier = mult;
    p.leverage = 10;
    p.margin = pos_price * qty * mult / Decimal::from(10u32);
    p
}

proptest! {
    #![proptest_config(config())]

    /// ① 盈亏分解：按标记价重估的未实现盈亏 == 按该价全额平仓的已实现盈亏；
    /// 且「累计已实现 + 未实现」等于全额平仓总盈亏。
    #[test]
    fn pnl_decomposition_equals_full_close_at_mark(
        side in side(),
        pos_price in price(),
        mark in price(),
        qty in quantity(),
        mult in multiplier(),
    ) {
        let p = position(side, pos_price, qty, mult);
        let unrealized = p.revalued(mark).pnl_value;
        let (close_all, _) = p.realized_close_pnl(mark, qty);

        prop_assert_eq!(unrealized, close_all);
        prop_assert_eq!(p.realized_pnl + unrealized, close_all);
    }

    /// ② 盈亏按平仓量线性可加：分两笔平 vs 一次全平，总盈亏相同（成本口径）。
    #[test]
    fn pnl_is_additive_over_closed_quantity(
        pos_price in price(),
        mark in price(),
        q1 in quantity(),
        q2 in quantity(),
        mult in multiplier(),
    ) {
        let p = position(Side::Buy, pos_price, q1 + q2, mult);
        let (a, _) = p.realized_close_pnl(mark, q1);
        let (b, _) = p.realized_close_pnl(mark, q2);
        let (all, _) = p.realized_close_pnl(mark, q1 + q2);

        prop_assert_eq!(a + b, all);
    }

    /// ③ 缩放性：价格整体 ×k → 盈亏 ×k，保证金 ×k，ROI 不变（保证金口径标度无关）。
    #[test]
    fn roi_is_scale_invariant(
        pos_price in price(),
        mark in price(),
        qty in quantity(),
        k in 2u32..10u32,
    ) {
        let k = Decimal::from(k);
        let base = position(Side::Buy, pos_price, qty, Decimal::ONE);
        let scaled = position(Side::Buy, pos_price * k, qty, Decimal::ONE);

        let r1 = base.revalued(mark);
        let r2 = scaled.revalued(mark * k);

        prop_assert_eq!(r2.margin, r1.margin * k);
        prop_assert_eq!(r2.pnl_value, r1.pnl_value * k);
        prop_assert_eq!(r2.roi, r1.roi);
    }

    /// ④ 平仓归零：数量为 0 时名义额/盈亏/ROI 全部归零，且零量平仓不产生盈亏。
    #[test]
    fn zero_quantity_zeroes_derivatives(
        pos_price in price(),
        mark in price(),
        mult in multiplier(),
    ) {
        let p = position(Side::Buy, pos_price, Decimal::ZERO, mult);
        let r = p.revalued(mark);

        prop_assert_eq!(r.notional, Decimal::ZERO);
        prop_assert_eq!(r.pnl_value, Decimal::ZERO);
        prop_assert_eq!(r.roi, Decimal::ZERO);
        prop_assert_eq!(
            p.realized_close_pnl(mark, Decimal::ZERO),
            (Decimal::ZERO, Decimal::ZERO)
        );
    }
}
