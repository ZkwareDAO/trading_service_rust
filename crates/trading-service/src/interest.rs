//! 宿主侧行情兴趣供给：读 TradingStore（活跃仓位 ∪ 规则涉及标的），
//! 给出当前需要订阅的标的键。市场 worker 只取属于自身 venue/kind 的部分。
//!
//! 规则涉及标的同时计入**条件标的**与**动作（开/平仓）标的**：
//! new rule 的两者可能是不同标的。

use std::collections::BTreeSet;

use trading_exchange::port::{InterestKey, MarketInterest};
use trading_models::{InstrumentType, MarketKind, Position, Venue, WatchRule, WatchRuleType};
use trading_store::{PositionRepo, RuleRepo};

/// 宿主兴趣供给（持有仓位/规则仓储句柄）。
pub struct MarketInterests {
    positions: PositionRepo,
    rules: RuleRepo,
}

impl MarketInterests {
    pub fn new(positions: PositionRepo, rules: RuleRepo) -> Self {
        Self { positions, rules }
    }
}

/// (exchange, instrument_type) → 行情 (venue, kind)；无法映射 → None。
fn venue_of(exchange: &str, itype: InstrumentType) -> Option<(Venue, MarketKind)> {
    match (exchange.trim(), itype) {
        ("deribit", InstrumentType::Option) => Some((Venue::Deribit, MarketKind::Option)),
        ("binance", InstrumentType::Futures) => Some((Venue::BinanceFutures, MarketKind::Futures)),
        ("binance", InstrumentType::Spot) => Some((Venue::BinanceSpot, MarketKind::Spot)),
        ("hyperliquid", InstrumentType::Futures) => Some((Venue::Hyperliquid, MarketKind::Futures)),
        ("ctp", InstrumentType::Futures) => Some((Venue::Ctp, MarketKind::Futures)),
        _ => None,
    }
}

type PushFn<'a> =
    &'a mut dyn FnMut(&mut BTreeSet<String>, &mut Vec<InterestKey>, Venue, MarketKind, &str);

impl MarketInterest for MarketInterests {
    fn interests(&self) -> Vec<InterestKey> {
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut out = Vec::new();

        let mut push = |seen: &mut BTreeSet<String>,
                        out: &mut Vec<InterestKey>,
                        venue: Venue,
                        kind: MarketKind,
                        symbol: &str| {
            let key = format!("{venue:?}|{kind:?}|{symbol}");
            if seen.insert(key) {
                out.push(InterestKey::new(venue, kind, symbol));
            }
        };

        // 1) 活跃仓位。
        for p in self.positions.list_active() {
            if let Some((v, k)) = venue_of(&p.exchange, p.instrument_type) {
                push(&mut seen, &mut out, v, k, &p.instrument);
            }
        }

        // 2) 未终止（active/pending/in_use）规则的条件标的 ∪ 动作标的。
        for r in self.rules.list_non_retired() {
            collect_rule(&r, &mut seen, &mut out, &mut push);
        }

        // 取完并集后做一道「按本所形状」的过滤：剔除被错挂到本所的其它所符号
        // （如 deribit 规则 price_BTCUSDT 条件会被错标成 deribit 标的），
        // 避免触发该所的无效订阅/规格预热；只删形状异常者，本所合法标的原样保留。
        sanitize_interests(out)
    }
}

/// 对并集按 `(venue, kind)` 分组做形状过滤，再拼回结果（顺序按组首次出现）。
fn sanitize_interests(keys: Vec<InterestKey>) -> Vec<InterestKey> {
    // 收集出现过的 (venue, kind)（Venue/Kind 未实现 Ord，手动去重）。
    let mut groups: Vec<(Venue, MarketKind)> = Vec::new();
    for k in &keys {
        let group = (k.venue, k.kind);
        if !groups.contains(&group) {
            groups.push(group);
        }
    }
    let mut out = Vec::with_capacity(keys.len());
    for (venue, kind) in groups {
        out.extend(filter_native(venue, kind, &keys));
    }
    out
}

/// 从并集中筛出「属于指定 venue/kind 且形状为本所原生」的标的（交易所无关，可复用）。
///
/// deribit/HL 现在用；未来新交易所只要在 [`is_native_symbol`] 登记一条形状规则
/// 即可启用，过滤机制本身无需改动。
fn filter_native(venue: Venue, kind: MarketKind, keys: &[InterestKey]) -> Vec<InterestKey> {
    let mut out = Vec::new();
    for k in keys {
        if k.venue != venue || k.kind != kind {
            continue;
        }
        if is_native_symbol(venue, kind, &k.symbol) {
            out.push(k.clone());
        } else {
            // 形状非本所原生（如跨所条件符号被错挂）：剔除并留痕，便于排查「为何没订阅」。
            tracing::debug!(
                ?venue,
                ?kind,
                symbol = %k.symbol,
                "行情兴趣过滤：符号形状非本所原生，已剔除（不订阅）"
            );
        }
    }
    out
}

/// symbol 是否为该 venue/kind 的「本所原生形状」（纯函数，收口各所符号形状）。
///
/// 仅对形状明确、且靠兴趣集订阅的所定义；binance（全量流、不读兴趣集）与
/// **未知新交易所默认放行**——不了解新所就自动判有误删风险，须显式登记才启用过滤。
fn is_native_symbol(venue: Venue, kind: MarketKind, symbol: &str) -> bool {
    let s = symbol.trim();
    match (venue, kind) {
        // Deribit 期权：完整 instrument 名必含 '-'（如 BTC-24SEP26-87000-P）。
        (Venue::Deribit, MarketKind::Option) => s.contains('-'),
        // Hyperliquid：原生为裸币名（BTC）。USDT 后缀属币安符号→异常；
        // USDC 后缀（内部统一名）与裸币放行，宁可保留也不误删；空符号异常。
        (Venue::Hyperliquid, MarketKind::Futures) => !s.is_empty() && !s.ends_with("USDT"),
        // CTP：合约号字母数字组合（rb2510 / IF2412），无分隔符约束，放行非空即可。
        (Venue::Ctp, MarketKind::Futures) => !s.is_empty(),
        // binance / 未知所：默认放行。
        _ => true,
    }
}

/// 收集一条规则的动作标的与条件标的。
fn collect_rule(
    r: &WatchRule,
    seen: &mut BTreeSet<String>,
    out: &mut Vec<InterestKey>,
    push: PushFn,
) {
    // 动作标的口径：risk/new 均取规则行落库的 exchange/itype/instrument。
    let act_exchange = r.exchange.as_str();
    let act_itype = r.instrument_type;
    let act_symbol = r.instrument.as_str();

    if let Some((v, k)) = venue_of(act_exchange, act_itype) {
        // 动作（开/平仓）标的。
        push(seen, out, v, k, act_symbol);

        // 条件标的：显式 condition_instrument，否则=动作标的。
        let cond_symbol = r
            .condition_instrument
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or(act_symbol);
        push(seen, out, v, k, cond_symbol);
    }
}

#[allow(dead_code)]
fn _types(_t: WatchRuleType, _p: Position) {}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::sync::Arc;
    use trading_models::{OrderType, Side, WatchConditionKind};

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    /// 内存 store 中注册仓位/规则表，返回兴趣供给与两个 repo。
    fn harness() -> (MarketInterests, PositionRepo, RuleRepo) {
        let store = store_core::Store::new(Arc::new(store_core::SystemClock));
        let pos_table = store.register::<Position>().unwrap();
        let rule_table = store.register::<WatchRule>().unwrap();
        let positions = PositionRepo::new(pos_table);
        let rules = RuleRepo::new(rule_table);
        (
            MarketInterests::new(positions.clone(), rules.clone()),
            positions,
            rules,
        )
    }

    /// 一条 deribit 开仓规则：动作标的 inst，条件标的 cond。
    fn deribit_rule(inst: &str, cond: Option<&str>) -> WatchRule {
        let mut r = WatchRule::new_open(
            7,
            0,
            "deribit",
            inst,
            InstrumentType::Option,
            Side::Buy,
            OrderType::Limit,
            Decimal::ZERO,
            Decimal::ONE,
            1,
        );
        r.condition_kind = WatchConditionKind::Price;
        r.condition_instrument = cond.map(str::to_string);
        r
    }

    /// 一条 hyperliquid 开仓规则：动作标的 inst，条件标的 cond。
    fn hl_rule(inst: &str, cond: Option<&str>) -> WatchRule {
        let mut r = WatchRule::new_open(
            9,
            0,
            "hyperliquid",
            inst,
            InstrumentType::Futures,
            Side::Buy,
            OrderType::Limit,
            Decimal::ZERO,
            Decimal::ONE,
            1,
        );
        r.condition_kind = WatchConditionKind::Price;
        r.condition_instrument = cond.map(str::to_string);
        r
    }

    /// 兴趣集中是否存在指定三元组。
    fn has(keys: &[InterestKey], venue: Venue, kind: MarketKind, symbol: &str) -> bool {
        keys.iter()
            .any(|k| k.venue == venue && k.kind == kind && k.symbol == symbol)
    }

    // ---- 1) 符号形状谓词 ----

    #[test]
    fn native_symbol_recognizes_each_known_venue_shape() {
        // deribit option：必须含 '-'。
        assert!(is_native_symbol(
            Venue::Deribit,
            MarketKind::Option,
            "BTC-24SEP26-87000-P"
        ));
        assert!(!is_native_symbol(
            Venue::Deribit,
            MarketKind::Option,
            "BTCUSDT"
        ));
        // hyperliquid futures：裸币通过；USDT 形状异常；USDC 宁留勿删。
        assert!(is_native_symbol(
            Venue::Hyperliquid,
            MarketKind::Futures,
            "BTC"
        ));
        assert!(!is_native_symbol(
            Venue::Hyperliquid,
            MarketKind::Futures,
            "BTCUSDT"
        ));
        assert!(is_native_symbol(
            Venue::Hyperliquid,
            MarketKind::Futures,
            "BTCUSDC"
        ));
        // binance / 未知新所：默认放行（绝不误删）。
        assert!(is_native_symbol(
            Venue::BinanceFutures,
            MarketKind::Futures,
            "BTCUSDT"
        ));
        assert!(is_native_symbol(
            Venue::BinanceSpot,
            MarketKind::Spot,
            "anything"
        ));
    }

    // ---- 2) 通用过滤方法 ----

    #[test]
    fn filter_native_keeps_legitimate_and_drops_foreign_symbols() {
        let keys = vec![
            InterestKey::new(Venue::Deribit, MarketKind::Option, "ETH-18SEP26-2500-P"),
            InterestKey::new(Venue::Deribit, MarketKind::Option, "BTCUSDT"),
            InterestKey::new(Venue::Hyperliquid, MarketKind::Futures, "BTC"),
            InterestKey::new(Venue::Hyperliquid, MarketKind::Futures, "BTCUSDT"),
        ];
        let deribit = filter_native(Venue::Deribit, MarketKind::Option, &keys);
        assert!(has(
            &deribit,
            Venue::Deribit,
            MarketKind::Option,
            "ETH-18SEP26-2500-P"
        ));
        assert!(!has(
            &deribit,
            Venue::Deribit,
            MarketKind::Option,
            "BTCUSDT"
        ));

        let hl = filter_native(Venue::Hyperliquid, MarketKind::Futures, &keys);
        assert!(has(&hl, Venue::Hyperliquid, MarketKind::Futures, "BTC"));
        assert!(!has(
            &hl,
            Venue::Hyperliquid,
            MarketKind::Futures,
            "BTCUSDT"
        ));
    }

    // ---- 3) 端到端：deribit 只剔异常、动作/合法条件一个不少 ----

    #[test]
    fn deribit_interests_drop_foreign_but_keep_action_and_native_condition() {
        let (supply, _pos, rules) = harness();
        // 动作标的 + 合法跨标的条件。
        rules
            .insert(deribit_rule(
                "BTC-24SEP26-87000-P",
                Some("ETH-18SEP26-2500-P"),
            ))
            .unwrap();
        // 跨所异常条件（价格源在币安）。
        rules
            .insert(deribit_rule("BTC-24SEP26-87000-P", Some("BTCUSDT")))
            .unwrap();

        let keys = supply.interests();
        // 异常 key 被移除 → 不再触发 get_instrument -32602。
        assert!(!has(&keys, Venue::Deribit, MarketKind::Option, "BTCUSDT"));
        // 铁约束：动作标的与合法条件标的仍在（否则开/平仓失效）。
        assert!(has(
            &keys,
            Venue::Deribit,
            MarketKind::Option,
            "BTC-24SEP26-87000-P"
        ));
        assert!(has(
            &keys,
            Venue::Deribit,
            MarketKind::Option,
            "ETH-18SEP26-2500-P"
        ));
    }

    // ---- 4) 端到端：HL 剔异常 + binance 正常订阅不受影响 ----

    #[test]
    fn hl_interests_drop_foreign_and_binance_subscriptions_remain() {
        let (supply, _pos, rules) = harness();
        rules.insert(hl_rule("BTC", Some("BTCUSDT"))).unwrap();

        // binance 同所条件回归：动作/条件均应保留。
        let mut bn = WatchRule::new_open(
            1,
            0,
            "binance",
            "BTCUSDT",
            InstrumentType::Futures,
            Side::Buy,
            OrderType::Limit,
            Decimal::ZERO,
            dec("1"),
            1,
        );
        bn.condition_kind = WatchConditionKind::Price;
        rules.insert(bn).unwrap();

        let keys = supply.interests();
        // HL：异常剔除、裸币动作保留。
        assert!(!has(
            &keys,
            Venue::Hyperliquid,
            MarketKind::Futures,
            "BTCUSDT"
        ));
        assert!(has(&keys, Venue::Hyperliquid, MarketKind::Futures, "BTC"));
        // binance 同所订阅完全保留。
        assert!(has(
            &keys,
            Venue::BinanceFutures,
            MarketKind::Futures,
            "BTCUSDT"
        ));
    }
}
