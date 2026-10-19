//! 策略/风控规则（挂在仓位上的触发条件）的持久化业务 model。
//!
//! 与本 crate [`crate::OrderRule`]（交易所发布的公开下单规格，纯内存、不落盘）是
//! 两种东西：这里的 [`WatchRule`] 由策略端创建、随仓位生命周期推进，是
//! `Order.rule_id` / `Position.rule_id` 外键指向的权威行，账户作用域，
//! 持久化于 `{day}/{account_name}/watch_rule.csv`。
//!
//! 命中后做什么由两个正交的字段决定：
//! * [`WatchRule::rule_type`] 决定「用自己」时执行的业务动作，也决定 `quantity` 的语义——
//!   `new` 按本行下单要素开仓（quantity=开仓量）；`risk` 减/平仓，数量取
//!   `quantity`（绝对数量）或 `quantity_pct`（按持仓比例）二者之一；
//! * [`WatchRule::action`]（[`WatchRuleAction`]）决定命中后是执行本规则自身，
//!   还是转而激活另一条链式规则。
//!
//! 文档来源：`docs/rule设计.md`。

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use store_core::model::bool_u8;
use store_core::{Model, StorageScope};

use crate::order::OrderType;
use crate::types::{InstrumentType, Side};

/// 规则类型。CSV 中存小写字符串。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum WatchRuleType {
    /// 开仓规则：携带交易所/账号/标的/价/量等下单要素，命中后按本行下单。
    #[default]
    New,
    /// 风控规则：条件命中后减仓/平仓，平仓单类型取本行 `order_type`，数量取
    /// `quantity`（绝对数量）或 `quantity_pct`（持仓比例）。
    Risk,
}

/// 规则生命周期状态。CSV 中存小写字符串。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum WatchRuleStatus {
    /// 生效中：每轮判定都会检查。
    #[default]
    Active,
    /// 等待激活（链式规则等待前一条命中后转入 active）。
    Pending,
    /// 已命中、执行中（平仓单已下发尚未终结）。
    InUse,
    /// 已终止（终态）。
    Retired,
}

impl WatchRuleStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, WatchRuleStatus::Retired)
    }
}

/// 条件类型（对应文档 `condition_name` 的主类；标的后缀拆到
/// [`WatchRule::condition_instrument`]，磁盘上的 `condition_name` =
/// `roi` / `price_{instrument}` / `position_{instrument}` / `profit_drawdown`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WatchConditionKind {
    /// 持仓盈利百分比（仓位 roi 比率）。
    #[default]
    Roi,
    /// 某代币价格；condition_instrument 为空时默认本仓位标的。
    Price,
    /// 某标的持仓数量；condition_instrument 为空时默认本仓位标的。
    Position,
    /// 盈利自最高盈利回落的百分点（保证金口径）。
    ProfitDrawdown,
}

impl WatchConditionKind {
    /// 组合文档形式的 condition_name。
    pub fn wire(self, instrument: Option<&str>) -> String {
        match self {
            WatchConditionKind::Roi => "roi".to_string(),
            WatchConditionKind::Price => format!("price_{}", instrument.unwrap_or("")),
            WatchConditionKind::Position => format!("position_{}", instrument.unwrap_or("")),
            WatchConditionKind::ProfitDrawdown => "profit_drawdown".to_string(),
        }
    }

    /// 解析文档形式的 condition_name：`price_BTCUSDT` / `position_BTCUSDT`
    /// / `roi` / `profit_drawdown`。无法识别返回 None。
    pub fn parse_wire(raw: &str) -> Option<(Self, Option<String>)> {
        let raw = raw.trim();
        match raw {
            "roi" => return Some((WatchConditionKind::Roi, None)),
            "profit_drawdown" => return Some((WatchConditionKind::ProfitDrawdown, None)),
            _ => {}
        }
        if let Some(rest) = raw.strip_prefix("price_") {
            return Some((WatchConditionKind::Price, non_empty(rest)));
        }
        if let Some(rest) = raw.strip_prefix("position_") {
            return Some((WatchConditionKind::Position, non_empty(rest)));
        }
        None
    }
}

fn non_empty(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

/// 命中后的动作（磁盘 `action` 列的语义化形态；只经 [`Self::wire`] /
/// [`Self::parse_wire`] 与字符串列互转，自身不参与 serde）。
///
/// 与 [`WatchRuleType`] 正交：本枚举只表达「执行本规则自身」还是「改去激活另一条」，
/// 「执行自身」具体是开仓还是减仓由 rule_type 决定。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchRuleAction {
    /// 执行本规则自身：new 按本行下单要素开仓；risk 按 `order_type`+`quantity_pct` 减/平仓。
    UseSelf,
    /// 不立即下单，转而激活另一条链式规则。
    ActivateRule {
        /// 目标规则 id。
        rule_id: u64,
    },
}

impl WatchRuleAction {
    pub fn wire(&self) -> String {
        match self {
            WatchRuleAction::UseSelf => "self".to_string(),
            WatchRuleAction::ActivateRule { rule_id } => format!("rule:{rule_id}"),
        }
    }

    /// 解析 action：`self` 或 `rule:<id>`。
    pub fn parse_wire(raw: &str) -> Option<Self> {
        let raw = raw.trim();
        if raw == "self" {
            return Some(WatchRuleAction::UseSelf);
        }
        if let Some(id) = raw.strip_prefix("rule:") {
            return id
                .parse()
                .ok()
                .map(|rule_id| WatchRuleAction::ActivateRule { rule_id });
        }
        None
    }
}

/// `quantity_pct` 缺省值：全平（比率 1）。
fn default_quantity_pct() -> Decimal {
    Decimal::ONE
}

/// `leverage` 缺省值：1（risk 规则不消费；new 开仓规则命中后先按此倍数
/// `set_leverage` 再下单）。
fn default_leverage() -> u64 {
    1
}

/// 策略/风控规则行。账户作用域表，持久化于 `{day}/{account_name}/watch_rule.csv`。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchRule {
    /// 本地主键。
    pub id: u64,
    /// 对应仓位 id（开仓规则可为 0）。
    pub position_id: u64,
    pub rule_type: WatchRuleType,
    /// 条件主类。
    pub condition_kind: WatchConditionKind,
    /// price_/position_ 条件的标的后缀；为空表示本仓位标的（roi 等忽略）。
    #[serde(default)]
    pub condition_instrument: Option<String>,
    /// 比较操作符（`>=` / `<=` / `>` / `<` / `==`），由风控引擎解释。
    pub operator: String,
    /// 条件阈值，定点。
    pub value: Decimal,
    /// 命中动作的磁盘编码："self"（执行本规则）/ "rule:{id}"（激活另一条）。
    pub action: String,
    pub status: WatchRuleStatus,

    // —— 下单要素 ——
    // new 规则用 exchange/instrument/instrument_type/price/quantity/order_type 开仓；
    // risk 规则用 order_type + (quantity 绝对数量 | quantity_pct 比例) 减/平仓。
    pub exchange: String,
    pub account_id: u64,
    /// 目标代币。
    pub instrument: String,
    /// 合约形状。Binance U 本位永续在本系统统一记为 `futures`；`perpetual` 为
    /// 预留扩展、当前不产出。
    #[serde(default)]
    pub instrument_type: InstrumentType,
    /// 开仓方向：仅 `new` 规则消费（多=Buy/空=Sell）；risk 平仓方向由持仓反向
    /// 推出，不读本字段。CSV 存大写串；新增列，旧快照缺列按 Buy 恢复。
    #[serde(default)]
    pub side: Side,
    /// 下单价格，定点（市价单为 0）。
    pub price: Decimal,
    /// 下单数量，语义随 [`WatchRule::rule_type`] 区分：
    /// * `new`：**开仓数量**（币/张/股）；
    /// * `risk`：**平仓绝对数量**；若按持仓比例平仓则本字段为 0、数量取
    ///   [`WatchRule::quantity_pct`]。
    ///
    /// 与 `quantity_pct` 互斥：risk 规则二者至多一个非零。
    pub quantity: Decimal,
    /// 订单类型：new=开仓单类型；risk=平仓单类型。
    pub order_type: OrderType,
    /// risk 按当前持仓数量的比例减/平仓（比率：全平=1，半仓=0.5）；
    /// 仅 risk 的比例平仓使用，此时 `quantity=0`；绝对数量平仓与 new 规则不消费，缺省 1。
    #[serde(default = "default_quantity_pct")]
    pub quantity_pct: Decimal,
    /// 开仓杠杆倍数：仅 `new` 规则消费（命中下单前先 `set_leverage`）；
    /// risk 规则不读本字段。新增列，旧快照缺列按 1 恢复。
    #[serde(default = "default_leverage")]
    pub leverage: u64,
    /// 下单金额（报价币种计）：仅 `new` 规则消费，与 `quantity` 二选一。
    /// 规则命中走下单路径时按 `order_cash ÷ 参考价` 换算数量（复用 cashqty，
    /// 口径同 place_order）；为 0 表示走 quantity。新增列，旧快照缺列按 0 恢复。
    #[serde(default)]
    pub order_cash: Decimal,

    /// 软删除：0=活跃，1=终止态。
    #[serde(with = "bool_u8")]
    pub deleted: bool,
    pub created_at: u64,
    pub updated_at: u64,
}

/// 规则「身份键」：界定两条规则是否为同一条（用于"重设"时 find-or-update，
/// 避免口语"重新设置"被当成追加、旧规则仍 active 造成重复）。
///
/// 只含**身份字段**；其余可变参数（`action`/`quantity*`/`price`/`order_type`/
/// `leverage`/`order_cash`/`status`/时间戳）不参与，命中已有规则时这些字段被
/// 覆盖更新。`value` **参与身份**：阈值不同即不同规则（如 roi<=0.1 与 roi<=0.2
/// 应并存），value 也相同时才走更新。
///
/// 身份构成：
/// * 通用：`account_id`、`rule_type`、`condition_kind`、`condition_instrument`、
///   `operator`（`>=` 与 `<=` 语义相反，必须区分，否则反向阈值的规则会被误并）、
///   `value`（阈值不同即不同规则）；
/// * risk：目标标的取 `position_id`（规则语义绑定具体仓位）；
/// * new：目标标的取 `instrument`+`instrument_type`，并按 `side` 区分多/空。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleIdentity {
    pub account_id: u64,
    pub rule_type: WatchRuleType,
    pub condition_kind: WatchConditionKind,
    /// 条件标的；None = 本标的。
    pub condition_instrument: Option<String>,
    /// 比较方向（`>=`/`<=`/`>`/`<`/`==`）：条件的一部分，参与身份。
    pub operator: String,
    /// 条件阈值：参与身份，仅 value 不同的两条规则各自独立。
    pub value: Decimal,
    /// risk：绑定的仓位 id；new：恒为 0。
    pub position_id: u64,
    /// new：开仓标的 + 合约形状 + 方向；risk：均为默认值。
    pub instrument: String,
    pub instrument_type: InstrumentType,
    pub side: Side,
}

impl WatchRule {
    /// 计算该规则的[`RuleIdentity`]（纯函数）。
    pub fn identity(&self) -> RuleIdentity {
        // 两类规则共有身份字段。
        let common = RuleIdentity {
            account_id: self.account_id,
            rule_type: self.rule_type,
            condition_kind: self.condition_kind,
            condition_instrument: self.condition_instrument.clone(),
            operator: self.operator.clone(),
            value: self.value,
            position_id: 0,
            instrument: String::new(),
            instrument_type: InstrumentType::default(),
            side: Side::default(),
        };
        // risk 只额外绑定仓位 id；new 携带开仓标的/合约形状/方向。
        match self.rule_type {
            WatchRuleType::Risk => RuleIdentity {
                position_id: self.position_id,
                ..common
            },
            WatchRuleType::New => RuleIdentity {
                instrument: self.instrument.clone(),
                instrument_type: self.instrument_type,
                side: self.side,
                ..common
            },
        }
    }
}

impl WatchRule {
    /// 构造一条风控规则（[`WatchRuleType::Risk`]），开仓下单要素留默认值，
    /// id/时间戳由框架填。
    ///
    /// 命中且动作为 [`WatchRuleAction::UseSelf`] 时的平仓数量两种口径二选一：
    /// * 按当前持仓比例：`quantity=0`、`quantity_pct` 给比率（全平 1、半仓 0.5）；
    /// * 按绝对数量：`quantity` 给数量、`quantity_pct=0`。
    ///
    /// `order_type` 为平仓单类型；动作为激活其他规则时数量/类型不被消费，传默认即可。
    ///
    /// `price` 为平仓限价：限价单（非 [`OrderType::Market`]）必须给正数，命中下单
    /// 直接作为委托价；市价单调 [`Decimal::ZERO`]（正价校验由调用方负责）。
    #[allow(clippy::too_many_arguments)]
    pub fn new_risk(
        account_id: u64,
        position_id: u64,
        condition: WatchConditionKind,
        condition_instrument: Option<String>,
        operator: impl Into<String>,
        value: Decimal,
        action: WatchRuleAction,
        order_type: OrderType,
        price: Decimal,
        quantity: Decimal,
        quantity_pct: Decimal,
    ) -> Self {
        Self {
            id: 0,
            position_id,
            rule_type: WatchRuleType::Risk,
            condition_kind: condition,
            condition_instrument,
            operator: operator.into(),
            value,
            action: action.wire(),
            status: WatchRuleStatus::Active,
            exchange: String::new(),
            account_id,
            instrument: String::new(),
            // Binance U 本位永续统一记 futures；risk 规则默认挂在合约仓位上。
            instrument_type: InstrumentType::Futures,
            // risk 平仓方向由持仓反向推出，不消费 side；给默认值即可。
            side: Side::Buy,
            // 平仓限价：market 由调用方传 ZERO；limit 为校验后的正数。
            price,
            quantity,
            order_type,
            quantity_pct,
            leverage: 1,
            order_cash: Decimal::ZERO,
            deleted: false,
            created_at: 0,
            updated_at: 0,
        }
    }

    /// 构造一条开仓规则（[`WatchRuleType::New`]），命中动作为执行本规则自身。
    ///
    /// `side` 为开仓方向（多=Buy/空=Sell），`leverage` 为开仓杠杆（1 表示现货式
    /// 无杠杆）；开仓条件与下单要素解耦，构造时条件字段给默认（roi/无操作符），
    /// 由调用方按需再覆盖 condition_*。
    #[allow(clippy::too_many_arguments)]
    pub fn new_open(
        account_id: u64,
        position_id: u64,
        exchange: impl Into<String>,
        instrument: impl Into<String>,
        instrument_type: InstrumentType,
        side: Side,
        order_type: OrderType,
        price: Decimal,
        quantity: Decimal,
        leverage: u64,
    ) -> Self {
        Self {
            id: 0,
            position_id,
            rule_type: WatchRuleType::New,
            condition_kind: WatchConditionKind::Roi,
            condition_instrument: None,
            operator: String::new(),
            value: Decimal::ZERO,
            action: WatchRuleAction::UseSelf.wire(),
            status: WatchRuleStatus::Active,
            exchange: exchange.into(),
            account_id,
            instrument: instrument.into(),
            instrument_type,
            side,
            price,
            quantity,
            order_type,
            quantity_pct: Decimal::ONE,
            leverage: leverage.max(1),
            order_cash: Decimal::ZERO,
            deleted: false,
            created_at: 0,
            updated_at: 0,
        }
    }

    /// 文档形式的 condition_name（`roi` / `price_{instrument}` / …）。
    pub fn condition_name(&self) -> String {
        self.condition_kind
            .wire(self.condition_instrument.as_deref())
    }

    /// 命中后动作的语义化形态；action 串非法时返回 None。
    pub fn parsed_action(&self) -> Option<WatchRuleAction> {
        WatchRuleAction::parse_wire(&self.action)
    }
}

impl Model for WatchRule {
    const MODEL_NAME: &'static str = "watch_rule";

    fn id(&self) -> u64 {
        self.id
    }
    fn set_id(&mut self, id: u64) {
        self.id = id;
    }
    fn deleted(&self) -> bool {
        self.deleted
    }
    fn set_deleted(&mut self, deleted: bool) {
        self.deleted = deleted;
    }
    fn created_at(&self) -> u64 {
        self.created_at
    }
    fn updated_at(&self) -> u64 {
        self.updated_at
    }
    fn set_timestamps(&mut self, created_at: u64, updated_at: u64) {
        self.created_at = created_at;
        self.updated_at = updated_at;
    }
    fn scope(&self) -> StorageScope {
        StorageScope::Account(self.account_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn open_rule_order_cash_defaults_zero() {
        // new_open 不传 order_cash：默认 0（保持既有 11 个调用点不变）。
        let r = WatchRule::new_open(
            7,
            0,
            "binance",
            "BTCUSDT",
            InstrumentType::Futures,
            Side::Buy,
            OrderType::Market,
            Decimal::ZERO,
            Decimal::ONE,
            1,
        );
        assert_eq!(r.order_cash, Decimal::ZERO);
    }

    // risk 身份键：携带 position_id；目标标的/类型/side 不参与（默认空）。
    #[test]
    fn risk_identity_carries_position_id_and_condition() {
        let r = WatchRule::new_risk(
            7,
            100,
            WatchConditionKind::Roi,
            None,
            ">=",
            Decimal::new(1, 2),
            WatchRuleAction::UseSelf,
            OrderType::Market,
            Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
            Decimal::ZERO,
            Decimal::ONE,
        );
        let id = r.identity();
        assert_eq!(id.account_id, 7);
        assert_eq!(id.rule_type, WatchRuleType::Risk);
        assert_eq!(id.condition_kind, WatchConditionKind::Roi);
        assert_eq!(id.condition_instrument, None);
        assert_eq!(id.position_id, 100);
        // risk 不读开仓标的/类型/方向。
        assert_eq!(id.instrument, "");
        assert_eq!(id.side, Side::Buy);
    }

    // risk：条件标的（跨标的）不同 → 身份不同。
    #[test]
    fn risk_identity_differs_by_position_id_and_condition_instrument() {
        let make = |pos: u64, cond: Option<&str>| {
            let mut r = WatchRule::new_risk(
                7,
                pos,
                WatchConditionKind::Price,
                None,
                ">=",
                Decimal::ZERO,
                WatchRuleAction::UseSelf,
                OrderType::Market,
                Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
                Decimal::ZERO,
                Decimal::ONE,
            );
            r.condition_instrument = cond.map(str::to_string);
            r.identity()
        };
        assert_ne!(make(100, None), make(200, None));
        assert_ne!(make(100, None), make(100, Some("ETHUSDT")));
        // 仅 quantity 等（身份外）可变字段不同 → 身份仍相同。
        let mut a = WatchRule::new_risk(
            7,
            100,
            WatchConditionKind::Roi,
            None,
            ">=",
            Decimal::new(1, 2),
            WatchRuleAction::UseSelf,
            OrderType::Market,
            Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
            Decimal::ZERO,
            Decimal::ONE,
        );
        let b = WatchRule::new_risk(
            7,
            100,
            WatchConditionKind::Roi,
            None,
            ">=",
            Decimal::new(1, 2),
            WatchRuleAction::UseSelf,
            OrderType::Market,
            Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
            Decimal::ZERO,
            Decimal::ONE,
        );
        a.quantity = Decimal::new(5, 0); // 可变字段改动（value 相同）
        assert_eq!(a.identity(), b.identity());
    }

    // value 参与身份：同身份字段、仅 value 不同 → 身份不同（两条独立规则并存）。
    #[test]
    fn risk_identity_differs_by_value() {
        let make = |value: Decimal| {
            WatchRule::new_risk(
                7,
                100,
                WatchConditionKind::Roi,
                None,
                "<=",
                value,
                WatchRuleAction::UseSelf,
                OrderType::Market,
                Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
                Decimal::ZERO,
                Decimal::ONE,
            )
            .identity()
        };
        // 同为 roi + <=，仅 value 0.1 vs 0.2 → 不同身份（不得被去重合并）。
        assert_ne!(make(Decimal::new(1, 1)), make(Decimal::new(2, 1)));
        // value 完全相同 → 同一身份（重设仍走更新）。
        assert_eq!(make(Decimal::new(1, 1)), make(Decimal::new(1, 1)));
    }

    // new 规则同样以 value 参与身份（两类规则共有口径）。
    #[test]
    fn new_identity_differs_by_value() {
        let make = |value: Decimal| {
            let mut r = WatchRule::new_open(
                7,
                0,
                "binance",
                "BTCUSDT",
                InstrumentType::Futures,
                Side::Buy,
                OrderType::Market,
                Decimal::ZERO,
                Decimal::ONE,
                1,
            );
            r.condition_kind = WatchConditionKind::Roi;
            r.condition_instrument = Some("BTCUSDT".to_string());
            r.operator = "<=".to_string();
            r.value = value;
            r.identity()
        };
        assert_ne!(make(Decimal::new(1, 1)), make(Decimal::new(2, 1)));
        assert_eq!(make(Decimal::new(1, 1)), make(Decimal::new(1, 1)));
    }

    // operator 方向相反（如 roi>=0.55 武装 vs roi<=-0.50 止损）→ 身份不同，
    // 不得被 find-or-update 合并。
    #[test]
    fn risk_identity_differs_by_operator() {
        let make = |op: &str| {
            WatchRule::new_risk(
                7,
                100,
                WatchConditionKind::Roi,
                None,
                op,
                Decimal::ZERO,
                WatchRuleAction::UseSelf,
                OrderType::Market,
                Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
                Decimal::ZERO,
                Decimal::ONE,
            )
            .identity()
        };
        assert_ne!(make(">="), make("<="));
        assert_ne!(make(">"), make("<"));
        assert_ne!(make(">="), make("=="));
    }

    // new 身份键：开仓标的 + 合约形状 + side 区分多空。
    #[test]
    fn new_identity_uses_instrument_type_and_side() {
        let make = |inst: &str, t: InstrumentType, side: Side| {
            WatchRule::new_open(
                7,
                0,
                "binance",
                inst,
                t,
                side,
                OrderType::Market,
                Decimal::ZERO,
                Decimal::ONE,
                1,
            )
            .identity()
        };
        // 同标的、不同 side → 不同身份（多空各一条）。
        assert_ne!(
            make("BTCUSDT", InstrumentType::Futures, Side::Buy),
            make("BTCUSDT", InstrumentType::Futures, Side::Sell)
        );
        // 不同标的 / 形状 → 不同身份。
        assert_ne!(
            make("BTCUSDT", InstrumentType::Futures, Side::Buy),
            make("ETHUSDT", InstrumentType::Futures, Side::Buy)
        );
        // new 的 position_id 恒为 0。
        assert_eq!(
            make("BTCUSDT", InstrumentType::Futures, Side::Buy).position_id,
            0
        );
        // 同身份：仅 value/operator 等可变参数不同时身份相等。
        let a = make("BTCUSDT", InstrumentType::Futures, Side::Buy);
        let b = make("BTCUSDT", InstrumentType::Futures, Side::Buy);
        assert_eq!(a, b);
    }

    #[test]
    fn order_cash_serde_backfills_old_rows() {
        // 旧持久化行没有 order_cash 列：反序列化按 0 恢复，不报错。
        let raw = r#"{
            "id":5,"position_id":0,"rule_type":"new","condition_kind":"price",
            "operator":">=","value":"70000","action":"self","status":"active",
            "exchange":"binance","account_id":7,"instrument":"BTCUSDT",
            "instrument_type":"futures","side":"BUY","price":"0","quantity":"0",
            "order_type":"MARKET","quantity_pct":"1","leverage":1,
            "deleted":"0","created_at":1,"updated_at":1
        }"#;
        let r: WatchRule = serde_json::from_str(raw).unwrap();
        assert_eq!(r.order_cash, Decimal::ZERO);

        // 带 order_cash 的行：序列化输出必须包含该字段（bool_u8 为 CSV 非对称
        // codec，不能整体 JSON 往返，故只验证序列化侧）。
        let mut r = r;
        r.order_cash = Decimal::from_str("100").unwrap();
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("order_cash"), "序列化缺少 order_cash: {json}");
    }

    #[test]
    fn condition_name_wire_roundtrip() {
        assert_eq!(WatchConditionKind::Roi.wire(None), "roi");
        assert_eq!(
            WatchConditionKind::Price.wire(Some("BTCUSDT")),
            "price_BTCUSDT"
        );
        assert_eq!(
            WatchConditionKind::Position.wire(Some("ETHUSDT")),
            "position_ETHUSDT"
        );
        assert_eq!(
            WatchConditionKind::ProfitDrawdown.wire(None),
            "profit_drawdown"
        );

        assert_eq!(
            WatchConditionKind::parse_wire("roi"),
            Some((WatchConditionKind::Roi, None))
        );
        assert_eq!(
            WatchConditionKind::parse_wire("price_BTCUSDT"),
            Some((WatchConditionKind::Price, Some("BTCUSDT".to_string())))
        );
        assert_eq!(
            WatchConditionKind::parse_wire("position_ETHUSDT"),
            Some((WatchConditionKind::Position, Some("ETHUSDT".to_string())))
        );
        assert_eq!(
            WatchConditionKind::parse_wire("profit_drawdown"),
            Some((WatchConditionKind::ProfitDrawdown, None))
        );
        // 空标的后缀按 None 归一；未知前缀拒绝。
        assert_eq!(
            WatchConditionKind::parse_wire("price_"),
            Some((WatchConditionKind::Price, None))
        );
        assert_eq!(WatchConditionKind::parse_wire("unknown"), None);
    }

    #[test]
    fn action_wire_roundtrip() {
        assert_eq!(WatchRuleAction::UseSelf.wire(), "self");
        assert_eq!(
            WatchRuleAction::parse_wire("self"),
            Some(WatchRuleAction::UseSelf)
        );
        assert_eq!(
            WatchRuleAction::parse_wire("rule:42"),
            Some(WatchRuleAction::ActivateRule { rule_id: 42 })
        );
        assert_eq!(WatchRuleAction::parse_wire("rule:x"), None);
        assert_eq!(WatchRuleAction::parse_wire("reduce"), None); // 旧名不再接受
        assert_eq!(WatchRuleAction::parse_wire("unknown"), None);
    }

    #[test]
    fn new_risk_use_self_carries_close_type_and_pct() {
        let r = WatchRule::new_risk(
            7,
            100,
            WatchConditionKind::Roi,
            None,
            ">=",
            Decimal::new(12, 2), // 0.12 = +12%
            WatchRuleAction::UseSelf,
            OrderType::Market,
            Decimal::ZERO,      // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
            Decimal::ZERO,      // 比例平仓：绝对数量为 0
            Decimal::new(5, 1), // 半仓 0.5
        );
        assert_eq!(r.rule_type, WatchRuleType::Risk);
        assert_eq!(r.status, WatchRuleStatus::Active);
        assert!(!r.deleted); // 0=活跃
        assert_eq!(r.position_id, 100);
        assert_eq!(r.account_id, 7);
        assert_eq!(r.condition_name(), "roi");
        assert_eq!(r.parsed_action(), Some(WatchRuleAction::UseSelf));
        assert_eq!(r.order_type, OrderType::Market);
        assert_eq!(r.quantity_pct, Decimal::new(5, 1));
        // risk 规则默认挂合约仓位：Binance U 本位永续统一记 futures。
        assert_eq!(r.instrument_type, InstrumentType::Futures);
    }

    #[test]
    fn risk_rule_can_close_by_absolute_quantity() {
        // risk 也可按绝对数量平仓：quantity>0、quantity_pct=0（与比例口径互斥）。
        let r = WatchRule::new_risk(
            7,
            100,
            WatchConditionKind::Price,
            None,
            "<=",
            Decimal::new(90000, 0),
            WatchRuleAction::UseSelf,
            OrderType::Limit,
            Decimal::ZERO,      // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
            Decimal::new(3, 0), // 平仓 3（币/张）
            Decimal::ZERO,
        );
        assert_eq!(r.rule_type, WatchRuleType::Risk);
        assert_eq!(r.quantity, Decimal::new(3, 0));
        assert_eq!(r.quantity_pct, Decimal::ZERO);
    }

    #[test]
    fn new_open_uses_self_action_and_order_fields() {
        let r = WatchRule::new_open(
            7,
            0,
            "binance",
            "BTCUSDT",
            InstrumentType::Futures,
            Side::Sell,
            OrderType::Limit,
            Decimal::new(50000, 0),
            Decimal::new(1, 0),
            3,
        );
        assert_eq!(r.rule_type, WatchRuleType::New);
        assert_eq!(r.leverage, 3);
        assert_eq!(r.parsed_action(), Some(WatchRuleAction::UseSelf));
        // 开仓方向随规则显式给出（risk 方向由持仓反向推出，不用本字段）。
        assert_eq!(r.side, Side::Sell);
        assert_eq!(r.exchange, "binance");
        assert_eq!(r.instrument, "BTCUSDT");
        assert_eq!(r.instrument_type, InstrumentType::Futures);
        assert_eq!(r.price, Decimal::from(50000));
        assert_eq!(r.quantity, Decimal::ONE);
        assert_eq!(r.order_type, OrderType::Limit);
        // 开仓不消费平仓比例，缺省 1。
        assert_eq!(r.quantity_pct, Decimal::ONE);
    }

    /// deleted/status/枚举/Decimal/quantity_pct 经框架实际落盘格式（CSV）往返。
    #[test]
    fn csv_roundtrip_preserves_fields() {
        let mut r = WatchRule::new_risk(
            7,
            100,
            WatchConditionKind::Price,
            Some("BTCUSDT".to_string()),
            "<=",
            Decimal::new(90000, 0),
            WatchRuleAction::ActivateRule { rule_id: 55 },
            OrderType::Limit,
            Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
            Decimal::ZERO,
            Decimal::new(5, 1),
        );
        r.id = 3;
        r.status = WatchRuleStatus::InUse;
        r.deleted = true; // 1=终止态
        r.created_at = 1000;
        r.updated_at = 2000;

        let mut wtr = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(Vec::new());
        wtr.serialize(&r).unwrap();
        let line = String::from_utf8(wtr.into_inner().unwrap()).unwrap();

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(line.as_bytes());
        let back: WatchRule = rdr.deserialize().next().unwrap().unwrap();

        assert_eq!(back.id, 3);
        assert_eq!(back.position_id, 100);
        assert_eq!(back.rule_type, WatchRuleType::Risk);
        assert_eq!(back.condition_kind, WatchConditionKind::Price);
        assert_eq!(back.condition_instrument.as_deref(), Some("BTCUSDT"));
        assert_eq!(back.condition_name(), "price_BTCUSDT");
        assert_eq!(back.operator, "<=");
        assert_eq!(back.value, Decimal::from(90000));
        assert_eq!(
            back.parsed_action(),
            Some(WatchRuleAction::ActivateRule { rule_id: 55 })
        );
        assert_eq!(back.order_type, OrderType::Limit);
        assert_eq!(back.quantity_pct, Decimal::new(5, 1));
        assert_eq!(back.status, WatchRuleStatus::InUse);
        assert_eq!(back.account_id, 7);
        // risk 规则不消费杠杆，缺省恢复为 1。
        assert_eq!(back.leverage, 1);
        assert!(back.deleted); // 磁盘 1 读回为 true
        assert_eq!(back.created_at, 1000);
        assert_eq!(back.updated_at, 2000);
    }

    /// 缺少 quantity_pct 字段的行反序列化按全平 1 处理（serde 默认）。
    #[test]
    fn missing_quantity_pct_defaults_to_full_close() {
        // bool_u8 的 deleted 按字符串 "0" 收；故意不给 quantity_pct 字段。
        let json = r#"{
            "id":0,"position_id":0,"rule_type":"risk","condition_kind":"roi",
            "operator":">=","value":0,"action":"self","status":"active",
            "exchange":"","account_id":1,"instrument":"","instrument_type":"futures",
            "price":0,"quantity":0,"order_type":"MARKET","deleted":"0",
            "created_at":0,"updated_at":0
        }"#;
        let r: WatchRule = serde_json::from_str(json).unwrap();
        assert_eq!(r.quantity_pct, Decimal::ONE);
        assert_eq!(r.order_type, OrderType::Market);
        // 旧快照无 side 列：serde default 恢复为 Buy，不阻断恢复。
        assert_eq!(r.side, Side::Buy);
        // 旧快照无 leverage 列：serde default 恢复为 1。
        assert_eq!(r.leverage, 1);
    }

    #[test]
    fn side_roundtrips_as_uppercase() {
        // side 是新增列：CSV 落大写串，且开多/开空都能往返。
        let mut r = WatchRule::new_open(
            1,
            0,
            "binance",
            "BTCUSDT",
            InstrumentType::Futures,
            Side::Sell,
            OrderType::Market,
            Decimal::ZERO,
            Decimal::new(1, 0),
            2,
        );
        r.id = 9;

        let mut wtr = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(Vec::new());
        wtr.serialize(&r).unwrap();
        let line = String::from_utf8(wtr.into_inner().unwrap()).unwrap();

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(line.as_bytes());
        let back: WatchRule = rdr.deserialize().next().unwrap().unwrap();
        assert_eq!(back.side, Side::Sell);
        // 磁盘形态为大写 SELL（与 order/position 的 side 列一致）。
        assert!(line.split(',').any(|col| col == "SELL"));
    }
}
