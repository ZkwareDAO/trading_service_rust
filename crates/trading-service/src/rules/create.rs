//! 远程建规则（create_rule）的纯构造/校验层。
//!
//! 零 IO：仓位是否存在经 [`RulePositionLookup`] 端口查询（生产由 trading-store 的
//! `PositionRepo` 实现），网络/落盘都不在这里。策略端不接触任何 id：
//! * risk 规则按「账户 + 交易所 + 合约形状 + 标的 + 方向」定位 deleted=0 仓位，
//!   命中后内部以该仓位 id 填充 `position_id`；查不到直接拒绝创建；
//! * 跨标的持仓条件（`position_FILUSDT == 0`）还要求条件标的当前有活跃仓位，
//!   否则不予创建（运行期该标的被平掉后指标按 0，规则才会命中）；
//! * 链式动作 `rule:<id>` 需要先知道目标规则 id，本期不开放远程创建。

use rust_decimal::Decimal;
use trading_models::{
    InstrumentType, OrderType, Position, Side, WatchConditionKind, WatchRule, WatchRuleAction,
    WatchRuleStatus, WatchRuleType,
};

use super::eval::CompareOp;

/// 建规则所需的活跃仓位查询端口（生产为 `PositionRepo`，测试可给假实现）。
/// 纯同步读，调用方不跨线程持有 trait 对象。
pub trait RulePositionLookup {
    /// 按本地仓位主键取活跃仓位（deleted=0）；跨账户/不存在/已平仓返回 None。
    fn find_active_by_id(&self, account_id: u64, position_id: u64) -> Option<Position>;

    /// 条件标的是否存在任意方向的活跃仓位（跨标的 position 条件创建期校验）。
    fn exists_active_instrument(
        &self,
        account_id: u64,
        exchange: &str,
        instrument_type: InstrumentType,
        instrument: &str,
    ) -> bool;
}

/// 建规则失败原因（面向策略端的中文提示）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateRuleError {
    /// 缺少必填字段。
    Missing(String),
    /// 字段值非法。
    Invalid(String),
    /// 绑定/条件标的在内存中没有活跃仓位。
    NoActivePosition(String),
}

impl std::fmt::Display for CreateRuleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CreateRuleError::Missing(s) => write!(f, "缺少必填字段：{s}"),
            CreateRuleError::Invalid(s) => write!(f, "字段非法：{s}"),
            CreateRuleError::NoActivePosition(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for CreateRuleError {}

/// 已解析的建规则请求（wire 字符串由服务层解析后填入）。
#[derive(Debug, Clone)]
pub struct RuleCreateInput {
    pub account_id: u64,
    pub rule_type: WatchRuleType,
    /// risk 绑定的本地仓位 id（get_position 回填值）；new 固定 0。
    pub position_id: u64,
    /// 绑定交易所（已归一阵列名/大小写）；new 消费，risk 仅用于账户/凭证解析，
    /// 规则行的下单要素以绑定仓位为准。
    pub exchange: String,
    /// 绑定标的（已按归属所归一大小写）；new 消费。
    pub instrument: String,
    pub instrument_type: InstrumentType,
    /// 开仓方向：仅 `new` 使用；risk 传 None（平仓方向命中时从持仓反向推出）。
    pub open_side: Option<Side>,
    pub condition_kind: WatchConditionKind,
    pub condition_instrument: Option<String>,
    pub operator: String,
    pub value: Decimal,
    /// 仅接受 self；链式 rule:<id> 报错。
    pub action: WatchRuleAction,
    pub order_type: OrderType,
    /// risk=平仓绝对量（0 表示走 quantity_pct）；new=开仓量。
    pub quantity: Decimal,
    /// risk 平仓比例（1 全平、0.5 半仓）。
    pub quantity_pct: Decimal,
    /// new 开仓杠杆（>=1）。
    pub leverage: u64,
    /// new 下单金额（报价币种）：与 quantity 二选一；>0 时规则命中按 cash 换算数量。
    pub order_cash: Decimal,
    /// 限价单价格（市价为 0）。
    pub price: Decimal,
    /// 创建为 pending（链式后继等待激活）。
    pub pending: bool,
}

/// 校验并构造一条规则（未分配 id/时间戳）；仓位绑定经 lookup 端口完成。
pub fn build_rule(
    input: &RuleCreateInput,
    lookup: &dyn RulePositionLookup,
) -> Result<WatchRule, CreateRuleError> {
    // 操作符合法性对两类规则都要求（条件必须可解释）。
    if CompareOp::parse(&input.operator).is_none() {
        return Err(CreateRuleError::Invalid(format!(
            "operator `{}` 不支持（允许 >= <= > < ==）",
            input.operator
        )));
    }
    // 链式动作 rule:<id> 的「目标存在且 pending」校验由服务层用 RuleRepo 完成
    // （本纯内核只有仓位查询端口）；此处原样透传动作。

    let mut rule = match input.rule_type {
        WatchRuleType::Risk => build_risk(input, lookup)?,
        WatchRuleType::New => build_new(input, lookup)?,
    };
    if input.pending {
        rule.status = WatchRuleStatus::Pending;
    }
    Ok(rule)
}

/// risk 规则：不使用 side，按 position_id 绑定的活跃仓位必须存在且归属同一账户；
/// 跨标的持仓条件的条件标的也必须存在。
fn build_risk(
    input: &RuleCreateInput,
    lookup: &dyn RulePositionLookup,
) -> Result<WatchRule, CreateRuleError> {
    // risk 必须显式给 position_id（值来自 get_position 回填），不再按方向反查。
    let position_id = if input.position_id > 0 {
        input.position_id
    } else {
        return Err(CreateRuleError::Missing(
            "position_id（risk 规则必填，来自 get_position 回填的 position_id）".to_string(),
        ));
    };
    // 按 id 取活跃仓位；lookup 实现负责校验账户归属，跨账户/已平仓/不存在均拒绝。
    let position = lookup
        .find_active_by_id(input.account_id, position_id)
        .ok_or_else(|| {
            CreateRuleError::NoActivePosition(format!(
                "position_id={position_id} 不存在、已平仓或不属于该账户，不能创建风控规则"
            ))
        })?;

    // 跨标的持仓条件：交易所/合约形状以绑定仓位为准（FILUSDT 联动例）。
    if input.condition_kind == WatchConditionKind::Position {
        if let Some(other) = input
            .condition_instrument
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            if !lookup.exists_active_instrument(
                input.account_id,
                &position.exchange,
                position.instrument_type,
                other,
            ) {
                return Err(CreateRuleError::NoActivePosition(format!(
                    "条件标的 {other} 无 deleted=0 活跃仓位，不能创建 position 联动规则"
                )));
            }
        }
    }

    // fail-fast：显式绝对平仓量不得超过当前持仓量（笔误当场报错）。
    // 比例/全平口径天然不超量，予以豁免。这只是创建期反馈，触发期仍会按实时
    // 持仓再次 clamp（建规则后仓位可能已变化）。
    if input.quantity > Decimal::ZERO && input.quantity > position.quantity {
        return Err(CreateRuleError::Invalid(format!(
            "平仓绝对量 {} 超过当前持仓量 {}",
            input.quantity, position.quantity
        )));
    }

    // 平仓量口径：绝对量优先；否则取比例（缺省全平）。
    let (quantity, quantity_pct) = if input.quantity > Decimal::ZERO {
        (input.quantity, Decimal::ZERO)
    } else {
        let pct = if input.quantity_pct > Decimal::ZERO {
            input.quantity_pct
        } else {
            Decimal::ONE
        };
        (Decimal::ZERO, pct)
    };

    // 限价平仓单必须给正价（与 new 开仓限价比齐）：否则命中下单委托价恒 0，
    // 必被柜台拒绝（真机 CTP 2115 LIMIT px=0 REJECTED）。market 不消费价格。
    if !matches!(input.order_type, OrderType::Market) && input.price <= Decimal::ZERO {
        return Err(CreateRuleError::Invalid(
            "limit 平仓规则 price 必须为正数".to_string(),
        ));
    }
    let price = if matches!(input.order_type, OrderType::Market) {
        Decimal::ZERO
    } else {
        input.price
    };

    Ok(WatchRule::new_risk(
        input.account_id,
        position.id,
        input.condition_kind,
        input.condition_instrument.clone(),
        input.operator.clone(),
        input.value,
        input.action,
        input.order_type,
        price,
        quantity,
        quantity_pct,
    ))
}

/// new 规则：携带下单要素，position_id=0；限价必须有价、数量必须为正。
fn build_new(
    input: &RuleCreateInput,
    lookup: &dyn RulePositionLookup,
) -> Result<WatchRule, CreateRuleError> {
    // new 开仓必须显式给方向。
    let side = input
        .open_side
        .ok_or_else(|| CreateRuleError::Missing("side（new 开仓规则必填）".to_string()))?;
    if input.instrument.trim().is_empty() {
        return Err(CreateRuleError::Missing("instrument".to_string()));
    }
    // roi 条件必须满足两点，否则规则永不触发，创建期直接拒绝：
    // ① new 开仓前无仓位上下文，roi 必须显式指定 condition_instrument（必填）；
    // ② 该标的在本账户下必须有活跃仓位（与 risk 的 position 联动同口径）。
    if input.condition_kind == WatchConditionKind::Roi {
        let target = input
            .condition_instrument
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                CreateRuleError::Missing(
                    "roi 条件必须指定 condition_instrument（new 规则开仓前无仓位上下文）"
                        .to_string(),
                )
            })?;
        if !lookup.exists_active_instrument(
            input.account_id,
            &input.exchange,
            input.instrument_type,
            target,
        ) {
            return Err(CreateRuleError::NoActivePosition(format!(
                "条件标的 {target} 无 deleted=0 活跃仓位，不能创建 roi 条件开仓规则"
            )));
        }
    }
    // 取量二选一：quantity 优先；否则 order_cash（命中走下单路径时再换算数量）。
    // 二者都不给才拒绝。
    if input.quantity <= Decimal::ZERO && input.order_cash <= Decimal::ZERO {
        return Err(CreateRuleError::Invalid(
            "new 规则必须给正数 quantity 或正数 order_cash".to_string(),
        ));
    }
    if !matches!(input.order_type, OrderType::Market) && input.price <= Decimal::ZERO {
        return Err(CreateRuleError::Invalid(
            "limit 开仓规则 price 必须为正数".to_string(),
        ));
    }
    let price = if matches!(input.order_type, OrderType::Market) {
        Decimal::ZERO
    } else {
        input.price
    };

    let mut rule = WatchRule::new_open(
        input.account_id,
        0,
        input.exchange.clone(),
        input.instrument.clone(),
        input.instrument_type,
        side,
        input.order_type,
        price,
        input.quantity,
        input.leverage,
    );
    rule.condition_kind = input.condition_kind;
    rule.condition_instrument = input.condition_instrument.clone();
    rule.operator = input.operator.clone();
    rule.value = input.value;
    rule.action = input.action.wire();
    rule.order_cash = input.order_cash;
    Ok(rule)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::str::FromStr;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    /// 假仓位查询：预置带 id 的活跃仓位，按 (账户, id) 定位。
    #[derive(Default)]
    struct FakeLookup {
        positions: RefCell<Vec<Position>>,
    }
    impl FakeLookup {
        /// 在账户 7 下放一条 binance 合约活跃仓位。
        fn with_position(&self, instrument: &str, side: Side, id: u64) -> &Self {
            self.with_position_for(7, instrument, side, id)
        }
        /// 指定账户放仓（用于跨账户归属校验）。
        fn with_position_for(&self, account: u64, instrument: &str, side: Side, id: u64) -> &Self {
            let mut p = Position::new(account, "binance", instrument, side);
            p.id = id;
            p.instrument_type = InstrumentType::Futures;
            self.positions.borrow_mut().push(p);
            self
        }
    }
    impl RulePositionLookup for FakeLookup {
        fn find_active_by_id(&self, account_id: u64, position_id: u64) -> Option<Position> {
            self.positions
                .borrow()
                .iter()
                .find(|p| p.id == position_id && p.account_id == account_id)
                .cloned()
        }
        fn exists_active_instrument(
            &self,
            account_id: u64,
            _exchange: &str,
            _instrument_type: InstrumentType,
            instrument: &str,
        ) -> bool {
            self.positions
                .borrow()
                .iter()
                .any(|p| p.account_id == account_id && p.instrument == instrument)
        }
    }

    fn base_input(rule_type: WatchRuleType) -> RuleCreateInput {
        RuleCreateInput {
            account_id: 7,
            rule_type,
            // risk 绑定预置仓位 id=42；new 不绑仓（0）。
            position_id: match rule_type {
                WatchRuleType::Risk => 42,
                WatchRuleType::New => 0,
            },
            exchange: "binance".into(),
            instrument: "BTCUSDT".into(),
            instrument_type: InstrumentType::Futures,
            // 仅 new 给开仓方向；risk 不使用 side。
            open_side: match rule_type {
                WatchRuleType::New => Some(Side::Buy),
                WatchRuleType::Risk => None,
            },
            condition_kind: WatchConditionKind::Price,
            condition_instrument: None,
            operator: ">".to_string(),
            value: dec("70000"),
            action: WatchRuleAction::UseSelf,
            order_type: OrderType::Market,
            quantity: match rule_type {
                WatchRuleType::New => dec("0.1"),
                WatchRuleType::Risk => Decimal::ZERO,
            },
            quantity_pct: Decimal::ONE,
            leverage: 3,
            order_cash: Decimal::ZERO,
            price: Decimal::ZERO,
            pending: false,
        }
    }

    #[test]
    fn risk_rule_binds_to_active_position_id() {
        let lookup = FakeLookup::default();
        lookup.with_position("BTCUSDT", Side::Buy, 42);
        let rule = build_rule(&base_input(WatchRuleType::Risk), &lookup).unwrap();
        assert_eq!(rule.rule_type, WatchRuleType::Risk);
        assert_eq!(rule.position_id, 42);
        assert_eq!(rule.status, WatchRuleStatus::Active);
        // risk 不消费杠杆。
        assert_eq!(rule.leverage, 1);
        // 缺省比例口径：全平。
        assert_eq!(rule.quantity, Decimal::ZERO);
        assert_eq!(rule.quantity_pct, Decimal::ONE);
    }

    #[test]
    fn risk_rule_rejected_without_active_position() {
        let lookup = FakeLookup::default();
        let err = build_rule(&base_input(WatchRuleType::Risk), &lookup).unwrap_err();
        assert!(matches!(err, CreateRuleError::NoActivePosition(_)));
    }

    #[test]
    fn risk_absolute_quantity_exceeding_position_is_invalid() {
        // 持仓量 0.001，显式绝对平仓量 5 → 创建期拒绝（fail-fast）。
        let lookup = FakeLookup::default();
        let mut p = Position::new(7, "binance", "BTCUSDT", Side::Buy);
        p.id = 42;
        p.instrument_type = InstrumentType::Futures;
        p.quantity = dec("0.001");
        lookup.positions.borrow_mut().push(p);

        let mut input = base_input(WatchRuleType::Risk);
        input.quantity = dec("5"); // 绝对平仓超量
        let err = build_rule(&input, &lookup).unwrap_err();
        assert!(matches!(err, CreateRuleError::Invalid(_)));
    }

    #[test]
    fn risk_absolute_quantity_within_position_is_accepted() {
        // 持仓量 2，绝对平仓量 0.7 → 通过。
        let lookup = FakeLookup::default();
        let mut p = Position::new(7, "binance", "BTCUSDT", Side::Buy);
        p.id = 42;
        p.instrument_type = InstrumentType::Futures;
        p.quantity = dec("2");
        lookup.positions.borrow_mut().push(p);

        let mut input = base_input(WatchRuleType::Risk);
        input.quantity = dec("0.7");
        let rule = build_rule(&input, &lookup).unwrap();
        assert_eq!(rule.quantity, dec("0.7"));
    }

    #[test]
    fn risk_rule_requires_position_id() {
        let lookup = FakeLookup::default();
        lookup.with_position("BTCUSDT", Side::Buy, 42);
        let mut input = base_input(WatchRuleType::Risk);
        input.position_id = 0; // 未回传 position_id
        let err = build_rule(&input, &lookup).unwrap_err();
        assert!(matches!(err, CreateRuleError::Missing(_)));
    }

    #[test]
    fn risk_rule_rejects_position_owned_by_other_account() {
        // 仓位 42 属于账户 8，请求账户是 7：跨账户绑定必须拒绝。
        let lookup = FakeLookup::default();
        lookup.with_position_for(8, "BTCUSDT", Side::Buy, 42);
        let err = build_rule(&base_input(WatchRuleType::Risk), &lookup).unwrap_err();
        assert!(matches!(err, CreateRuleError::NoActivePosition(_)));
    }

    #[test]
    fn new_rule_requires_open_side() {
        let lookup = FakeLookup::default();
        let mut input = base_input(WatchRuleType::New);
        input.open_side = None;
        let err = build_rule(&input, &lookup).unwrap_err();
        assert!(matches!(err, CreateRuleError::Missing(_)));
    }

    #[test]
    fn risk_position_link_requires_condition_instrument_position() {
        // BTC 绑定仓存在、FIL 条件仓不存在：拒绝（用例 6-1）。
        let lookup = FakeLookup::default();
        lookup.with_position("BTCUSDT", Side::Buy, 42);
        let mut input = base_input(WatchRuleType::Risk);
        input.condition_kind = WatchConditionKind::Position;
        input.condition_instrument = Some("FILUSDT".into());
        input.operator = "==".into();
        input.value = Decimal::ZERO;
        assert!(matches!(
            build_rule(&input, &lookup).unwrap_err(),
            CreateRuleError::NoActivePosition(_)
        ));

        // 两个仓位都存在：创建成功并绑定 BTC 仓。
        lookup.with_position("FILUSDT", Side::Buy, 77);
        let rule = build_rule(&input, &lookup).unwrap();
        assert_eq!(rule.position_id, 42);
        assert_eq!(rule.condition_instrument.as_deref(), Some("FILUSDT"));
    }

    #[test]
    fn risk_absolute_quantity_takes_precedence() {
        let lookup = FakeLookup::default();
        // 预置足量持仓（with_position 默认 quantity=0，需显式给量）。
        let mut seeded = Position::new(7, "binance", "BTCUSDT", Side::Buy);
        seeded.id = 42;
        seeded.instrument_type = InstrumentType::Futures;
        seeded.quantity = dec("2");
        lookup.positions.borrow_mut().push(seeded);

        let mut input = base_input(WatchRuleType::Risk);
        input.quantity = dec("0.005");
        let rule = build_rule(&input, &lookup).unwrap();
        assert_eq!(rule.quantity, dec("0.005"));
        assert_eq!(rule.quantity_pct, Decimal::ZERO);
    }

    #[test]
    fn invalid_operator_is_rejected() {
        let lookup = FakeLookup::default();
        lookup.with_position("BTCUSDT", Side::Buy, 42);
        let mut input = base_input(WatchRuleType::Risk);
        input.operator = "!=".into();
        assert!(matches!(
            build_rule(&input, &lookup).unwrap_err(),
            CreateRuleError::Invalid(_)
        ));
    }

    #[test]
    fn chain_action_is_passed_through_on_risk_rule() {
        // 链式动作的目标存在性由服务层（RuleRepo）校验；纯构造层只透传动作串。
        let lookup = FakeLookup::default();
        lookup.with_position("BTCUSDT", Side::Buy, 42);
        let mut input = base_input(WatchRuleType::Risk);
        input.action = WatchRuleAction::ActivateRule { rule_id: 9 };
        let rule = build_rule(&input, &lookup).unwrap();
        assert_eq!(
            rule.parsed_action(),
            Some(WatchRuleAction::ActivateRule { rule_id: 9 })
        );
    }

    #[test]
    fn new_rule_carries_open_fields_and_leverage() {
        let lookup = FakeLookup::default();
        // roi 条件现在必须带标的且该标的有活跃仓位。
        lookup.with_position("BTCUSDT", Side::Buy, 99);
        let mut input = base_input(WatchRuleType::New);
        input.condition_kind = WatchConditionKind::Roi;
        input.condition_instrument = Some("BTCUSDT".into());
        input.operator = ">".into();
        input.value = dec("0.01");
        let rule = build_rule(&input, &lookup).unwrap();
        assert_eq!(rule.position_id, 0);
        assert_eq!(rule.leverage, 3);
        assert_eq!(rule.side, Side::Buy);
        assert_eq!(rule.quantity, dec("0.1"));
        assert_eq!(rule.condition_kind, WatchConditionKind::Roi);
    }

    #[test]
    fn new_rule_accepts_order_cash_without_quantity() {
        // 仅给 order_cash、quantity=0：允许创建，规则行记录 cash。
        let lookup = FakeLookup::default();
        let mut input = base_input(WatchRuleType::New);
        input.quantity = Decimal::ZERO;
        input.order_cash = dec("100");
        let rule = build_rule(&input, &lookup).unwrap();
        assert_eq!(rule.order_cash, dec("100"));
        assert_eq!(rule.quantity, Decimal::ZERO); // 数量留到命中时换算
    }

    #[test]
    fn new_rule_rejects_when_quantity_and_cash_both_absent() {
        // quantity 与 order_cash 都为 0：拒绝。
        let lookup = FakeLookup::default();
        let mut input = base_input(WatchRuleType::New);
        input.quantity = Decimal::ZERO;
        input.order_cash = Decimal::ZERO;
        let err = build_rule(&input, &lookup).unwrap_err();
        assert!(matches!(err, CreateRuleError::Invalid(_)));
    }

    #[test]
    fn new_limit_rule_requires_positive_price() {
        let lookup = FakeLookup::default();
        let mut input = base_input(WatchRuleType::New);
        input.order_type = OrderType::Limit;
        input.price = Decimal::ZERO;
        assert!(matches!(
            build_rule(&input, &lookup).unwrap_err(),
            CreateRuleError::Invalid(_)
        ));
    }

    #[test]
    fn risk_limit_rule_carries_positive_price() {
        // risk 限价平仓规则：正价必须透传进规则（真机缺陷：price 恒 0，
        // 命中下单 LIMIT px=0 被柜台拒单 2115）。
        let lookup = FakeLookup::default();
        lookup.with_position("BTCUSDT", Side::Buy, 42);
        let mut input = base_input(WatchRuleType::Risk);
        input.order_type = OrderType::Limit;
        input.price = dec("4050");
        let rule = build_rule(&input, &lookup).unwrap();
        assert_eq!(rule.order_type, OrderType::Limit);
        assert_eq!(rule.price, dec("4050"));
    }

    #[test]
    fn risk_limit_rule_without_positive_price_is_invalid() {
        // risk 限价却不给正价 → 创建期 Invalid（与 new 限价比齐，消息区分平仓规则）。
        let lookup = FakeLookup::default();
        lookup.with_position("BTCUSDT", Side::Buy, 42);
        let mut input = base_input(WatchRuleType::Risk);
        input.order_type = OrderType::Limit;
        input.price = Decimal::ZERO;
        let err = build_rule(&input, &lookup).unwrap_err();
        match err {
            CreateRuleError::Invalid(msg) => assert!(
                msg.contains("平仓") && msg.contains("price"),
                "错误消息应区分 risk 平仓限价，实际：{msg}"
            ),
            other => panic!("期望 Invalid，实际 {other:?}"),
        }
    }

    #[test]
    fn risk_market_rule_keeps_zero_price() {
        // risk 市价平仓：price 保持 0（现状不变）。
        let lookup = FakeLookup::default();
        lookup.with_position("BTCUSDT", Side::Buy, 42);
        let input = base_input(WatchRuleType::Risk);
        let rule = build_rule(&input, &lookup).unwrap();
        assert_eq!(rule.order_type, OrderType::Market);
        assert_eq!(rule.price, Decimal::ZERO);
    }

    #[test]
    fn pending_flag_creates_pending_rule() {
        let lookup = FakeLookup::default();
        lookup.with_position("BTCUSDT", Side::Buy, 42);
        let mut input = base_input(WatchRuleType::Risk);
        input.pending = true;
        let rule = build_rule(&input, &lookup).unwrap();
        assert_eq!(rule.status, WatchRuleStatus::Pending);
    }

    #[test]
    fn new_roi_condition_rejected_without_target_position() {
        // new 规则 roi 条件指向 BTCUSDT，但本账户无该活跃仓位 → 拒绝创建。
        let lookup = FakeLookup::default();
        let mut input = base_input(WatchRuleType::New);
        input.condition_kind = WatchConditionKind::Roi;
        input.condition_instrument = Some("BTCUSDT".into());
        input.operator = ">=".to_string();
        input.value = dec("0.2");
        let err = build_rule(&input, &lookup).unwrap_err();
        assert!(matches!(err, CreateRuleError::NoActivePosition(_)));
    }

    #[test]
    fn new_roi_condition_allowed_when_target_position_exists() {
        // 本账户已有 BTCUSDT 仓位 → roi 条件合法，规则可创建。
        let lookup = FakeLookup::default();
        lookup.with_position("BTCUSDT", Side::Buy, 99);
        let mut input = base_input(WatchRuleType::New);
        input.condition_kind = WatchConditionKind::Roi;
        input.condition_instrument = Some("BTCUSDT".into());
        input.operator = ">=".to_string();
        input.value = dec("0.2");
        let rule = build_rule(&input, &lookup).unwrap();
        assert_eq!(rule.rule_type, WatchRuleType::New);
        assert_eq!(rule.condition_instrument.as_deref(), Some("BTCUSDT"));
    }

    #[test]
    fn new_roi_condition_rejected_without_condition_instrument() {
        // new 规则开仓前无仓位：roi 条件不指定 condition_instrument 则无法计算、
        // 永不触发 → 创建期要求必填并拒绝。
        let lookup = FakeLookup::default();
        let mut input = base_input(WatchRuleType::New);
        input.condition_kind = WatchConditionKind::Roi;
        input.condition_instrument = None;
        input.operator = ">=".to_string();
        input.value = dec("0.2");
        let err = build_rule(&input, &lookup).unwrap_err();
        assert!(matches!(err, CreateRuleError::Missing(_)));
    }
}
