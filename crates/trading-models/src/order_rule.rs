//! 交易规则（下单校验所依赖的交易所公开规格）的内存数据模型。
//!
//! 与 [`crate::Order`] / [`crate::Position`] 不同，规则是交易所发布的公开静态数据：
//! 没有 `id` / `deleted` / `created_at`，**不落 CSV、不参与日切恢复**，进程重启后
//! 重新同步即可。因此这里只定义纯数据行（可 serde，供抓取/调试），运行时的内存表
//! 与校验内核位于 `trading-core`。
//!
//! 一行一标的，键为 `(exchange, instrument_type, symbol)`，整行不可变、刷新时整体替换。
//! 金额/精度字段一律定点 [`rust_decimal::Decimal`]，禁止 f64 参与步长/名义额运算。

use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::types::InstrumentType;
use crate::QtyUnit;

/// 规则表键：（交易所, 合约形状, 交易所原生符号）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderRuleKey {
    /// "binance" / "hyperliquid" / "deribit"。
    pub exchange: String,
    pub instrument_type: InstrumentType,
    /// 交易所原生符号：BTCUSDT / BTC / BTC-27DEC26-100000-C。
    pub symbol: String,
    /// 本行规则的**来源网络**（非下单账户网络）：true=测试网 meta。
    /// 币安/Deribit 规则恒定只取主网 → 恒 false；仅 Hyperliquid 按账户网络区分。
    #[serde(default)]
    pub rule_is_testnet: bool,
}

impl OrderRuleKey {
    /// 旧构造器：规则来源按主网（`rule_is_testnet=false`）。币安/Deribit 沿用。
    pub fn new(
        exchange: impl Into<String>,
        instrument_type: InstrumentType,
        symbol: impl Into<String>,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            instrument_type,
            symbol: symbol.into(),
            rule_is_testnet: false,
        }
    }

    /// 带来源网络的构造器，供 Hyperliquid 区分主网/测试网规则。
    pub fn with_network(
        exchange: impl Into<String>,
        instrument_type: InstrumentType,
        symbol: impl Into<String>,
        rule_is_testnet: bool,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            instrument_type,
            symbol: symbol.into(),
            rule_is_testnet,
        }
    }
}

/// Deribit 分价位 tick 档；顺序即交易所返回顺序。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceTier {
    /// 委托价 ≥ 该值时适用本档（对应 Deribit `above_price`，首档通常为 0）。
    pub above_price: Decimal,
    /// 该档价格步长。
    pub tick: Decimal,
}

/// 一个交易标的的下单规则行；整行不可变，刷新时整体替换。
///
/// 三种价格精度口径按交易所择一：固定 [`Self::price_tick`]（Binance）、
/// 分档 [`Self::price_tiers`]（Deribit）、有效数字 [`Self::price_max_sig_digits`]
/// （Hyperliquid，当前只同步不消费）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderRule {
    // —— 身份与单位 ——
    pub exchange: String,
    pub symbol: String,
    /// 本行规则的**来源网络**（非账户网络）：true=测试网 meta。
    /// 币安/Deribit 恒 false；仅 Hyperliquid 按网络区分。
    #[serde(default)]
    pub rule_is_testnet: bool,
    pub instrument_type: InstrumentType,
    /// 标的本体，期权必填（Deribit=BTC）；现货/永续留空。
    #[serde(default)]
    pub underlying: Option<String>,
    /// 数量口径：Binance/HL=base（币），Deribit=contract（张）。
    pub qty_unit: QtyUnit,
    /// 1 个下单单位对应的标的量：现货/永续=1，期权=contract_size。
    pub multiplier: Decimal,
    /// 报价币种：USDT / USD / BTC（期权费以 BTC 计）。
    pub quote_currency: String,
    /// 结算/盈亏币种。
    pub settle_currency: String,

    // —— 价格精度：固定 tick / 分档 tiers / 有效数字，三口径择一 ——
    #[serde(default)]
    pub price_tick: Option<Decimal>,
    #[serde(default)]
    pub price_tiers: Option<Vec<PriceTier>>,
    #[serde(default)]
    pub price_min: Option<Decimal>,
    #[serde(default)]
    pub price_max: Option<Decimal>,
    #[serde(default)]
    pub price_max_sig_digits: Option<u8>,

    // —— 数量精度 ——
    /// 限价数量步长：Binance stepSize、HL 10^-sz_decimals、Deribit qty_tick_size。
    pub qty_step: Decimal,
    #[serde(default)]
    pub qty_min: Option<Decimal>,
    #[serde(default)]
    pub qty_max: Option<Decimal>,
    /// 市价数量档（Binance 现货 MARKET_LOT_SIZE）；为空表示与限价档相同。
    #[serde(default)]
    pub market_qty_step: Option<Decimal>,
    #[serde(default)]
    pub market_qty_min: Option<Decimal>,
    #[serde(default)]
    pub market_qty_max: Option<Decimal>,

    // —— 名义额 ——
    #[serde(default)]
    pub notional_min: Option<Decimal>,
    #[serde(default)]
    pub notional_max: Option<Decimal>,
    #[serde(default)]
    pub notional_min_to_market: bool,
    #[serde(default)]
    pub notional_max_to_market: bool,

    // —— 生命周期（仅期权）——
    #[serde(default)]
    pub expires_at_ms: Option<i64>,

    // —— 溯源：只服务刷新/过期决策，不参与校验 ——
    pub source: String,
    pub fetched_at_ms: i64,
    #[serde(default)]
    pub spec_version: Option<String>,

    // —— 逃生舱：未升格为正式字段的交易所怪约束，仅透传，校验器不消费 ——
    #[serde(default)]
    pub extra: BTreeMap<String, String>,
}

impl OrderRule {
    /// 该标的的键（携带本行规则的来源网络）。
    pub fn key(&self) -> OrderRuleKey {
        OrderRuleKey::with_network(
            self.exchange.clone(),
            self.instrument_type,
            self.symbol.clone(),
            self.rule_is_testnet,
        )
    }

    /// 链式标注本行规则的来源网络；返回新行，不修改原行。供 HL fetcher 使用。
    pub fn with_testnet(mut self, rule_is_testnet: bool) -> Self {
        self.rule_is_testnet = rule_is_testnet;
        self
    }

    /// 按委托价选价格 tick：固定 tick 直接返回；分档取所有 `above_price <= price`
    /// 中门槛最高的一档。有效数字口径（HL）当前不参与，返回 None。
    pub fn price_tick_at(&self, price: Decimal) -> Option<Decimal> {
        if let Some(tick) = self.price_tick {
            return Some(tick);
        }
        let tiers = self.price_tiers.as_ref()?;
        tiers
            .iter()
            .filter(|t| price >= t.above_price)
            .max_by_key(|t| t.above_price)
            .map(|t| t.tick)
    }
}

impl OrderRule {
    /// 以必填字段构造一行，可选精度/名义额字段默认留空，由各交易所 fetcher 映射后填充。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        exchange: impl Into<String>,
        symbol: impl Into<String>,
        instrument_type: InstrumentType,
        qty_unit: QtyUnit,
        multiplier: Decimal,
        quote_currency: impl Into<String>,
        settle_currency: impl Into<String>,
        qty_step: Decimal,
        source: impl Into<String>,
        fetched_at_ms: i64,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            symbol: symbol.into(),
            rule_is_testnet: false,
            instrument_type,
            underlying: None,
            qty_unit,
            multiplier,
            quote_currency: quote_currency.into(),
            settle_currency: settle_currency.into(),
            price_tick: None,
            price_tiers: None,
            price_min: None,
            price_max: None,
            price_max_sig_digits: None,
            qty_step,
            qty_min: None,
            qty_max: None,
            market_qty_step: None,
            market_qty_min: None,
            market_qty_max: None,
            notional_min: None,
            notional_max: None,
            notional_min_to_market: false,
            notional_max_to_market: false,
            expires_at_ms: None,
            source: source.into(),
            fetched_at_ms,
            spec_version: None,
            extra: BTreeMap::new(),
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

    fn tier(above: &str, tick: &str) -> PriceTier {
        PriceTier {
            above_price: dec(above),
            tick: dec(tick),
        }
    }

    fn row() -> OrderRule {
        OrderRule::new(
            "deribit",
            "BTC-27DEC26-100000-C",
            InstrumentType::Option,
            QtyUnit::Contract,
            Decimal::ONE,
            "BTC",
            "BTC",
            dec("1"),
            "deribit:get_instrument",
            0,
        )
    }

    #[test]
    fn fixed_tick_wins_over_tiers() {
        let mut r = row();
        r.price_tick = Some(dec("0.1"));
        r.price_tiers = Some(vec![tier("0", "0.0005")]);
        assert_eq!(r.price_tick_at(Decimal::ZERO), Some(dec("0.1")));
    }

    #[test]
    fn tier_picks_highest_applicable_threshold() {
        // Deribit 期权：价越低 tick 越小。
        let mut r = row();
        r.price_tiers = Some(vec![
            tier("0", "0.0005"),
            tier("0.1", "0.001"),
            tier("0.5", "0.005"),
            tier("1", "0.01"),
        ]);
        assert_eq!(r.price_tick_at(dec("0.05")), Some(dec("0.0005")));
        // 门槛边界：等于门槛即适用该档。
        assert_eq!(r.price_tick_at(dec("0.1")), Some(dec("0.001")));
        assert_eq!(r.price_tick_at(dec("0.49")), Some(dec("0.001")));
        assert_eq!(r.price_tick_at(dec("7.5")), Some(dec("0.01")));
    }

    #[test]
    fn legacy_key_constructor_defaults_to_mainnet() {
        // 币安/Deribit 继续用旧 new()：规则来源恒主网，字段必须为 false。
        let key = OrderRuleKey::new("binance", InstrumentType::Futures, "BTCUSDT");
        assert!(!key.rule_is_testnet);
        assert_eq!(
            key,
            OrderRuleKey::with_network("binance", InstrumentType::Futures, "BTCUSDT", false)
        );
    }

    #[test]
    fn same_coin_mainnet_and_testnet_keys_are_distinct() {
        // HL 同名币两网 key 不同，可在同一规则表共存。
        let mainnet =
            OrderRuleKey::with_network("hyperliquid", InstrumentType::Futures, "BTC", false);
        let testnet =
            OrderRuleKey::with_network("hyperliquid", InstrumentType::Futures, "BTC", true);
        assert_ne!(mainnet, testnet);
        assert!(!mainnet.rule_is_testnet);
        assert!(testnet.rule_is_testnet);
    }

    #[test]
    fn rule_with_testnet_is_reflected_in_key() {
        // 行上打网络标后，由行派生出对应网络的 key。
        let testnet_row = row().with_testnet(true);
        assert!(testnet_row.rule_is_testnet);
        assert!(testnet_row.key().rule_is_testnet);
        // 链式打标不改变其它身份字段，只改网络位。
        let mainnet_row = testnet_row.with_testnet(false);
        assert!(!mainnet_row.key().rule_is_testnet);
    }
}
