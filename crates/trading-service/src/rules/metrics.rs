//! 规则条件指标的纯计算层：`Position + Market → EvaluationContext → 指标`。
//!
//! 本层不涉及 IO / 网络 / 存储 / 下单，也不修改规则：仓位自身的指标
//! （roi / 盈利回落百分点 / 最新价 / 持仓量）取自 [`Position`] 已重估字段；
//! `condition_instrument` 指向其它标的时，价格与持仓量经注入闭包查询
//! （生产环境闭包查 `QuoteTable` 与仓位表）。价格查不到返回 `None` 本轮不触发；
//! 跨标的**持仓量**闭包由生产引擎对"无活跃仓位"统一补 0（配合建规则时的存在性
//! 校验，让 `position_X == 0` 在 X 被平掉后可命中），本内核只透传闭包结果。
//!
//! 全程 [`Decimal`]，禁 f64。

use rust_decimal::Decimal;
use trading_models::{Position, WatchConditionKind, WatchRule};

use super::eval::CompareOp;

/// 一次条件判定可依赖的、已脱离存储/网络的指标集合。
///
/// 仓位类指标为 `None` 表示当前没有可判定的仓位（如 `new` 规则 position_id=0）；
/// `own_price` 为 `None` 表示仓位标的尚无有效价（`current_price <= 0`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationContext {
    /// 仓位未实现盈亏率（比率口径，+12% 为 0.12）。
    pub roi: Option<Decimal>,
    /// 盈利自最高盈利回落的百分点（保证金口径：max_roi − roi）。
    pub profit_drawdown: Option<Decimal>,
    /// 本仓位标的最新价；无价为 None。
    pub own_price: Option<Decimal>,
    /// 本仓位持仓数量。
    pub own_quantity: Option<Decimal>,
    /// 是否存在对应仓位。
    pub position_exists: bool,
}

impl EvaluationContext {
    /// 无仓位上下文（开仓规则/仓位已平）。
    pub fn empty() -> Self {
        Self {
            roi: None,
            profit_drawdown: None,
            own_price: None,
            own_quantity: None,
            position_exists: false,
        }
    }

    /// 从一条已重估的活跃仓位提取自身指标。
    pub fn from_position(position: &Position) -> Self {
        // 规则 ROI 直接采用仓位的保证金口径收益率（pnl/占用保证金），各所统一：
        // deribit/现货 lev=1 与旧口径一致；binance/HL 由「价格涨跌幅」改为
        // 「保证金收益率」（用户设阈值时直接按真实 ROI 表达）。
        Self {
            roi: Some(position.roi),
            profit_drawdown: Some(position.profit_drawdown),
            // 0 价视为尚无有效行情，不参与价格类条件。
            own_price: if position.current_price > Decimal::ZERO {
                Some(position.current_price)
            } else {
                None
            },
            own_quantity: Some(position.quantity),
            position_exists: true,
        }
    }
}

/// 指标构建器：跨标的的行情价、持仓量、仓位上下文以闭包注入，保持内核零 IO。
///
/// * `price_of(symbol)`：其它标的最新估值价（生产查 `QuoteTable.valuation_price`）；
/// * `position_of(symbol)`：其它标的在本账户的持仓数量（生产查仓位表）；
/// * `context_of(symbol)`：其它标的在本账户的仓位指标上下文（生产用本 rule 的
///   account 查仓位表后 `EvaluationContext::from_position`），供跨标的 ROI/盈利。
pub struct MetricsBuilder<PriceOf, PositionOf, ContextOf>
where
    PriceOf: Fn(&str) -> Option<Decimal>,
    PositionOf: Fn(&str) -> Option<Decimal>,
    ContextOf: Fn(&str) -> Option<EvaluationContext>,
{
    price_of: PriceOf,
    position_of: PositionOf,
    context_of: ContextOf,
}

impl<PriceOf, PositionOf, ContextOf> MetricsBuilder<PriceOf, PositionOf, ContextOf>
where
    PriceOf: Fn(&str) -> Option<Decimal>,
    PositionOf: Fn(&str) -> Option<Decimal>,
    ContextOf: Fn(&str) -> Option<EvaluationContext>,
{
    pub fn new(price_of: PriceOf, position_of: PositionOf, context_of: ContextOf) -> Self {
        Self {
            price_of,
            position_of,
            context_of,
        }
    }

    /// 解析规则条件指向的标的后缀：空/None 表示本仓位标的。
    fn target_symbol<'a>(&self, rule: &'a WatchRule) -> Option<&'a str> {
        rule.condition_instrument
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }

    /// 取出规则条件所需的指标值；数据不可得（无仓位/无行情/无持仓）返回 None。
    pub fn metric(&self, rule: &WatchRule, ctx: &EvaluationContext) -> Option<Decimal> {
        match rule.condition_kind {
            // ROI/盈利：空后缀读绑定仓位自身；跨标的经 context_of 取目标仓位上下文。
            WatchConditionKind::Roi => match self.target_symbol(rule) {
                None => ctx.roi,
                Some(symbol) => (self.context_of)(symbol).and_then(|c| c.roi),
            },
            WatchConditionKind::ProfitDrawdown => match self.target_symbol(rule) {
                None => ctx.profit_drawdown,
                Some(symbol) => (self.context_of)(symbol).and_then(|c| c.profit_drawdown),
            },
            WatchConditionKind::Price => match self.target_symbol(rule) {
                // 空后缀：本仓位标的最新价。
                None => ctx.own_price,
                // 跨标的：查注入的行情闭包；取不到即 None（本轮不触发）。
                Some(symbol) => (self.price_of)(symbol),
            },
            WatchConditionKind::Position => match self.target_symbol(rule) {
                None => ctx.own_quantity,
                Some(symbol) => (self.position_of)(symbol),
            },
        }
    }

    /// 判定规则条件是否命中。
    ///
    /// 指标不可得（None）或操作符非法时一律 false——缺数据绝不触发，也不报错：
    /// 行情/仓位在下一轮 WS 推送后补齐再判。
    pub fn evaluate(&self, rule: &WatchRule, ctx: &EvaluationContext) -> bool {
        let Some(metric) = self.metric(rule, ctx) else {
            return false;
        };
        let Some(op) = CompareOp::parse(&rule.operator) else {
            return false;
        };
        op.matches(metric, rule.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use trading_models::{OrderType, Side, WatchConditionKind, WatchRule, WatchRuleAction};

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    /// 构造一条指定条件/操作符/阈值的风控规则（动作 self，比例全平）。
    fn rule(kind: WatchConditionKind, suffix: Option<&str>, op: &str, value: &str) -> WatchRule {
        let mut r = WatchRule::new_risk(
            7,
            100,
            kind,
            suffix.map(str::to_string),
            op,
            dec(value),
            WatchRuleAction::UseSelf,
            OrderType::Market,
            Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
            Decimal::ZERO,
            Decimal::ONE,
        );
        r.id = 1;
        r
    }

    fn position() -> Position {
        let mut p = Position::new(7, "binance", "BTCUSDT", Side::Buy);
        p.quantity = dec("2");
        p.current_price = dec("91000");
        p.roi = dec("0.12");
        p.profit_drawdown = dec("0.3");
        p
    }

    /// 闭包：只认识 BTCUSDT=90000 / ETHUSDT=3000，其余无价。
    fn price_of(symbol: &str) -> Option<Decimal> {
        match symbol {
            "BTCUSDT" => Some(dec("90000")),
            "ETHUSDT" => Some(dec("3000")),
            _ => None,
        }
    }

    /// 闭包：只认识 ETHUSDT 持仓 5，其余无。
    fn position_of(symbol: &str) -> Option<Decimal> {
        (symbol == "ETHUSDT").then(|| dec("5"))
    }

    /// 默认 builder：跨标的仓位上下文恒 None（单仓指标测试不依赖它）。
    #[allow(clippy::type_complexity)]
    fn builder() -> MetricsBuilder<
        impl Fn(&str) -> Option<Decimal>,
        impl Fn(&str) -> Option<Decimal>,
        impl Fn(&str) -> Option<EvaluationContext>,
    > {
        MetricsBuilder::new(price_of, position_of, |_: &str| None)
    }

    #[test]
    fn roi_condition_uses_position_metric() {
        let b = builder();
        let ctx = EvaluationContext::from_position(&position());
        assert!(b.evaluate(&rule(WatchConditionKind::Roi, None, ">=", "0.12"), &ctx));
        assert!(!b.evaluate(&rule(WatchConditionKind::Roi, None, ">", "0.12"), &ctx));
        // 当前 roi=0.12 不满足 <= -0.01。
        assert!(!b.evaluate(&rule(WatchConditionKind::Roi, None, "<=", "-0.01"), &ctx));
    }

    #[test]
    fn roi_metric_uses_position_roi_directly_without_leverage_division() {
        let b = builder();
        // 规则 ROI 直接用仓位保证金口径收益率，不再除以杠杆。
        let mut p = position();
        p.roi = dec("0.003");
        p.leverage = 3;
        p.current_price = dec("91000");
        let ctx = EvaluationContext::from_position(&p);
        assert_eq!(ctx.roi, Some(dec("0.003")));
        // 阈值按保证金收益率判定：0.003 命中 > 0.002，不命中 > 0.005。
        assert!(b.evaluate(&rule(WatchConditionKind::Roi, None, ">", "0.002"), &ctx));
        assert!(!b.evaluate(&rule(WatchConditionKind::Roi, None, ">", "0.005"), &ctx));
    }

    #[test]
    fn drawdown_condition_uses_position_metric() {
        let b = builder();
        let ctx = EvaluationContext::from_position(&position());
        assert!(b.evaluate(
            &rule(WatchConditionKind::ProfitDrawdown, None, ">=", "0.3"),
            &ctx
        ));
        assert!(!b.evaluate(
            &rule(WatchConditionKind::ProfitDrawdown, None, ">", "0.3"),
            &ctx
        ));
    }

    #[test]
    fn price_condition_empty_suffix_uses_own_position_price() {
        let b = builder();
        let ctx = EvaluationContext::from_position(&position());
        // 空后缀取仓位 current_price=91000，不查闭包。
        assert!(b.evaluate(&rule(WatchConditionKind::Price, None, "<=", "91000"), &ctx));
        assert!(!b.evaluate(&rule(WatchConditionKind::Price, None, "<", "91000"), &ctx));
    }

    #[test]
    fn price_condition_with_suffix_quotes_injected_market() {
        let b = builder();
        let ctx = EvaluationContext::from_position(&position());
        assert!(b.evaluate(
            &rule(WatchConditionKind::Price, Some("ETHUSDT"), "<=", "3000"),
            &ctx
        ));
        assert!(!b.evaluate(
            &rule(WatchConditionKind::Price, Some("ETHUSDT"), "<", "3000"),
            &ctx
        ));
    }

    #[test]
    fn missing_cross_market_price_does_not_trigger() {
        let b = builder();
        let ctx = EvaluationContext::from_position(&position());
        // Q3：取不到价就判不了，本轮不触发（即便 <= 阈值对 0 会成立）。
        assert!(!b.evaluate(
            &rule(WatchConditionKind::Price, Some("DOGEUSDT"), "<=", "999999"),
            &ctx
        ));
    }

    #[test]
    fn position_condition_reads_quantity_own_and_cross() {
        let b = builder();
        let ctx = EvaluationContext::from_position(&position());
        // 空后缀：本仓位持仓量 2。
        assert!(b.evaluate(&rule(WatchConditionKind::Position, None, ">=", "2"), &ctx));
        // 跨标的：ETHUSDT 持仓 5。
        assert!(b.evaluate(
            &rule(WatchConditionKind::Position, Some("ETHUSDT"), "==", "5"),
            &ctx
        ));
        // 跨标的无持仓：不触发（不按 0 误判）。
        assert!(!b.evaluate(
            &rule(WatchConditionKind::Position, Some("DOGEUSDT"), "==", "0"),
            &ctx
        ));
    }

    /// 跨标的仓位上下文闭包：认识 ETHUSDT（roi=0.2、盈利回撤=0.1），其余无。
    fn eth_context() -> EvaluationContext {
        let mut c = EvaluationContext::empty();
        c.roi = Some(dec("0.2"));
        c.profit_drawdown = Some(dec("0.1"));
        c
    }

    #[test]
    fn cross_symbol_roi_reads_context_of_target() {
        let eth = eth_context();
        let b = MetricsBuilder::new(price_of, position_of, |s: &str| {
            (s == "ETHUSDT").then(|| eth.clone())
        });
        let ctx = EvaluationContext::empty();
        // 跨标的 ROI 取 ETHUSDT 仓位的 roi=0.2。
        assert!(b.evaluate(
            &rule(WatchConditionKind::Roi, Some("ETHUSDT"), ">=", "0.2"),
            &ctx
        ));
        assert!(!b.evaluate(
            &rule(WatchConditionKind::Roi, Some("ETHUSDT"), ">", "0.2"),
            &ctx
        ));
        // 目标标的无活跃仓位 → 取不到，不触发。
        assert!(!b.evaluate(
            &rule(WatchConditionKind::Roi, Some("DOGEUSDT"), ">=", "0"),
            &ctx
        ));
    }

    #[test]
    fn cross_symbol_profit_drawdown_reads_context_of_target() {
        let eth = eth_context();
        let b = MetricsBuilder::new(price_of, position_of, |s: &str| {
            (s == "ETHUSDT").then(|| eth.clone())
        });
        let ctx = EvaluationContext::empty();
        assert!(b.evaluate(
            &rule(
                WatchConditionKind::ProfitDrawdown,
                Some("ETHUSDT"),
                ">=",
                "0.1"
            ),
            &ctx
        ));
        assert!(!b.evaluate(
            &rule(
                WatchConditionKind::ProfitDrawdown,
                Some("ETHUSDT"),
                ">",
                "0.1"
            ),
            &ctx
        ));
    }

    #[test]
    fn no_position_context_skips_position_bound_conditions() {
        let b = builder();
        let ctx = EvaluationContext::empty();
        // new 规则（无仓位）：roi/本仓价/本仓量都不可判。
        assert!(!b.evaluate(&rule(WatchConditionKind::Roi, None, ">=", "0"), &ctx));
        assert!(!b.evaluate(&rule(WatchConditionKind::Price, None, "<=", "999999"), &ctx));
        assert!(!b.evaluate(&rule(WatchConditionKind::Position, None, ">=", "0"), &ctx));
        // 但跨标的价格条件仍可凭注入行情判定。
        assert!(b.evaluate(
            &rule(WatchConditionKind::Price, Some("BTCUSDT"), ">=", "90000"),
            &ctx
        ));
        assert!(!ctx.position_exists);
    }

    #[test]
    fn zero_current_price_is_treated_as_no_price() {
        let b = builder();
        let mut p = position();
        p.current_price = Decimal::ZERO;
        let ctx = EvaluationContext::from_position(&p);
        assert_eq!(ctx.own_price, None);
        assert!(!b.evaluate(&rule(WatchConditionKind::Price, None, "<=", "999999"), &ctx));
    }

    #[test]
    fn invalid_operator_does_not_trigger() {
        let b = builder();
        let ctx = EvaluationContext::from_position(&position());
        let bad = rule(WatchConditionKind::Roi, None, "!=", "0");
        assert!(!b.evaluate(&bad, &ctx));
    }

    #[test]
    fn evaluation_does_not_mutate_inputs() {
        let b = builder();
        let p = position();
        let r = rule(WatchConditionKind::Roi, None, ">=", "0.1");
        let p_before = p.clone();
        let r_before = r.clone();
        let ctx = EvaluationContext::from_position(&p);
        let _ = b.evaluate(&r, &ctx);
        assert_eq!(p.roi, p_before.roi);
        assert_eq!(r.value, r_before.value);
        assert_eq!(r.status, r_before.status);
    }
}
