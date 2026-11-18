//! L1 属性测试：trading-service 编排层的纯逻辑不变量。
//!
//! 覆盖：仓位对账投影幂等/重复投递无副作用/快照乱序无关、风控阈值单调性、
//! 数量精度只减不增、余额守恒（成本口径平仓 vs 标记价重估的交叉 oracle）。
//!
//! 纪律（计划 §B4）：PR 门禁固定 seed、`cases = 10_000`；
//! 夜间随机 seed：`PROPTEST_RNG_SEED=<随机数> PROPTEST_CASES=1000000 cargo test -p trading-service --test properties`。

use proptest::prelude::*;
use proptest::test_runner::{Config, FileFailurePersistence, RngSeed};
use rust_decimal::Decimal;
use trading_core::order_rule::align_quantity;
use trading_models::position_sync::{diff_positions, merge_remote_into_local};
use trading_models::{InstrumentType, OrderRule, OrderType, Position, QtyUnit, Side};
use trading_service::testkit::CompareOp;

/// 门禁默认 seed：固定以保证 PR 可复现；可由 `PROPTEST_RNG_SEED` 覆盖。
const DEFAULT_SEED: u64 = 0x5EED_2026;
/// 门禁快速档用例数；夜间可由 `PROPTEST_CASES` 放大到 1_000_000。
const DEFAULT_CASES: u32 = 10_000;
/// 余额守恒的初始权益（合成值）。
const INITIAL_EQUITY: u32 = 100_000;

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
            "proptest-regressions/service.txt",
        ))),
        ..Config::default()
    }
}

// ---- 生成器 ----

fn price() -> impl Strategy<Value = Decimal> {
    (1u32..1_000_000).prop_map(Decimal::from)
}

fn quantity() -> impl Strategy<Value = Decimal> {
    (1u32..100_000).prop_map(Decimal::from)
}

fn leverage() -> impl Strategy<Value = u64> {
    1u64..50
}

fn side() -> impl Strategy<Value = Side> {
    prop_oneof![Just(Side::Buy), Just(Side::Sell)]
}

fn signed_decimal() -> impl Strategy<Value = Decimal> {
    (-1_000_000i64..1_000_000).prop_map(Decimal::from)
}

// ---- 辅助构造 ----

/// 交易所回查快照仓位（mapper 产物：id=0、当前价已给）。
fn remote_position(
    instrument: &str,
    side: Side,
    qty: Decimal,
    pos_price: Decimal,
    mark: Decimal,
    leverage: u64,
) -> Position {
    let mut p = Position::new(1, "test", instrument, side);
    p.instrument_type = InstrumentType::Futures;
    p.quantity = qty;
    p.pos_price = pos_price;
    p.current_price = mark;
    p.leverage = leverage;
    // 柜台未给保证金时的兜底口径：成本 / 杠杆。
    p.margin = pos_price * qty / Decimal::from(leverage);
    p.exchange_update_time = 9_000;
    p
}

/// 本地活跃仓位（带 id/账簿字段），用于对账投影的起点。
fn local_position() -> Position {
    let mut p = Position::new(1, "test", "BTCUSDT", Side::Buy);
    p.id = 7;
    p.rule_id = 3;
    p.realized_pnl = Decimal::from(7u32);
    p.max_roi = Decimal::from_str_exact("0.5").unwrap();
    p.created_at = 111;
    p.updated_at = 222;
    p
}

/// 把一组远端快照投影到本地活跃仓位集合（纯函数，无 IO）。
fn apply_snapshots(remote: Vec<Position>, local: Vec<Position>, now_ms: u64) -> Vec<Position> {
    diff_positions(remote, local, now_ms)
        .into_iter()
        .map(|d| d.position().clone())
        .collect()
}

proptest! {
    #![proptest_config(config())]

    /// ① 余额守恒（交叉 oracle，手续费为 0）：成本口径逐笔平仓的盈亏合计，
    /// 必须等于按标记价重估的未实现盈亏合计——两条独立计算路径给出同一权益变化。
    #[test]
    fn balance_conservation_cross_oracle(
        side1 in side(),
        side2 in side(),
        pos1 in price(),
        pos2 in price(),
        mark1 in price(),
        mark2 in price(),
        q1 in quantity(),
        q2 in quantity(),
        lev1 in leverage(),
        lev2 in leverage(),
    ) {
        let p1 = remote_position("AAAUSDT", side1, q1, pos1, mark1, lev1);
        let p2 = remote_position("BBBUSDT", side2, q2, pos2, mark2, lev2);

        // 路径 A：按成本价全额平仓的已实现盈亏。
        let realized = p1.realized_close_pnl(mark1, q1).0 + p2.realized_close_pnl(mark2, q2).0;
        // 路径 B：按标记价重估的未实现盈亏。
        let unrealized = p1.revalued(mark1).pnl_value + p2.revalued(mark2).pnl_value;

        prop_assert_eq!(realized, unrealized);
        let equity_from_fills = Decimal::from(INITIAL_EQUITY) + realized;
        let equity_from_marks = Decimal::from(INITIAL_EQUITY) + unrealized;
        prop_assert_eq!(equity_from_fills, equity_from_marks);
    }

    /// ② 同步幂等：对账是「投影」不是「累加」，同一快照投影两次 == 投影一次。
    #[test]
    fn sync_projection_is_idempotent(
        s in side(),
        qty in quantity(),
        pos_price in price(),
        mark in price(),
        lev in leverage(),
    ) {
        let remote = remote_position("BTCUSDT", s, qty, pos_price, mark, lev);

        let once = apply_snapshots(vec![remote.clone()], vec![], 1_000);
        prop_assert_eq!(once.len(), 1);
        let twice = apply_snapshots(vec![remote], once.clone(), 2_000);

        prop_assert_eq!(twice.len(), 1);
        // `Position` 未派生 PartialEq；Debug 串含全部字段，用作严格全字段相等断言。
        prop_assert_eq!(format!("{:?}", twice[0]), format!("{:?}", once[0]));
    }

    /// ③ 重复投递无副作用：同一快照合并 N 次 == 合并 1 次。
    #[test]
    fn repeated_remote_merge_is_noop(
        s in side(),
        qty in quantity(),
        pos_price in price(),
        mark in price(),
        lev in leverage(),
        rounds in 1u32..20u32,
    ) {
        let remote = remote_position("BTCUSDT", s, qty, pos_price, mark, lev);
        let base = local_position();

        let once = merge_remote_into_local(&base, &remote).revalued(remote.current_price);
        let mut acc = once.clone();
        for _ in 0..rounds {
            acc = merge_remote_into_local(&acc, &remote).revalued(remote.current_price);
        }

        // 同上：Debug 串逐字段比较（Position 无 PartialEq）。
        prop_assert_eq!(format!("{acc:?}"), format!("{once:?}"));
    }

    /// ④ 快照乱序无关：对同一组相互独立的标的快照，任意处理顺序终态一致。
    #[test]
    fn snapshot_order_is_irrelevant(
        qty in quantity(),
        pos_price in price(),
        mark in price(),
        lev in leverage(),
    ) {
        let remotes = [
            remote_position("AAAUSDT", Side::Buy, qty, pos_price, mark, lev),
            remote_position("BBBUSDT", Side::Sell, qty, pos_price, mark, lev),
            remote_position("CCCUSDT", Side::Buy, qty, pos_price, mark, lev),
        ];
        let permutations = [
            [0usize, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0],
        ];
        let mut renderings = Vec::new();
        for perm in permutations {
            let ordered: Vec<Position> = perm.iter().map(|i| remotes[*i].clone()).collect();
            let out = apply_snapshots(ordered, vec![], 1_000);
            renderings.push(format!("{out:?}"));
        }

        prop_assert!(renderings.windows(2).all(|w| w[0] == w[1]));
    }

    /// ⑤ 风控阈值单调性：放宽阈值不得使原本放行的订单被拒。
    ///
    /// `>=` 阈值越小越宽松；`<=` 阈值越大越宽松。
    #[test]
    fn risk_threshold_relaxation_never_rejects(
        metric in signed_decimal(),
        a in signed_decimal(),
        b in signed_decimal(),
    ) {
        let (lo, hi) = if a <= b { (a, b) } else { (b, a) };

        // 高阈值通过 ⇒ 低阈值必然通过（>= 方向放宽）。
        if CompareOp::Gte.matches(metric, hi) {
            prop_assert!(CompareOp::Gte.matches(metric, lo));
        }
        // 低阈值通过 ⇒ 高阈值必然通过（<= 方向放宽）。
        if CompareOp::Lte.matches(metric, lo) {
            prop_assert!(CompareOp::Lte.matches(metric, hi));
        }
    }

    /// ⑥ 精度只减不增：任意数量经步长对齐后 `<= 原始量` 且为步长整数倍（限价/市价两档）。
    #[test]
    fn precision_alignment_never_inflates(
        qty in 1u64..1_000_000_000u64,
        // 交易所 stepSize 恒为 10^-n（有效小数位口径），故只在该域内验证「步长整数倍」。
        decimals in 1u32..7u32,
        market in any::<bool>(),
    ) {
        let qty = Decimal::from(qty);
        let limit_step = Decimal::new(1, decimals);
        let market_step = limit_step * Decimal::from(10u32);
        let mut row = OrderRule::new(
            "test",
            "BTCUSDT",
            InstrumentType::Futures,
            QtyUnit::Base,
            Decimal::ONE,
            "USDT",
            "USDT",
            limit_step,
            "property-test",
            0,
        );
        row.market_qty_step = Some(market_step);

        let (order_type, effective_step) = if market {
            (OrderType::Market, market_step)
        } else {
            (OrderType::Limit, limit_step)
        };
        let aligned = align_quantity(&row, order_type, qty).expect("无区间约束时正数量必可对齐");

        prop_assert!(aligned <= qty, "对齐不得放大数量：{aligned} > {qty}");
        prop_assert!(aligned >= Decimal::ZERO);
        prop_assert_eq!(aligned % effective_step, Decimal::ZERO);
    }
}
