//! 列表查询的零 IO 谓词：把已解析的查询条件作用于内存 model 行。
//!
//! 分两层：
//! * `XxxQuery`：handler 从 querystring 取到的原始字符串/数字（宽松、未解析枚举）；
//! * `XxxFilter`：强类型谓词，`matches(&model)` 做 AND 组合。
//!
//! 不做账户名解析（属 IO，由 handler 经账户管理器先解析成 account_id 再注入），
//! 不做排序/分页（在 envelope::paginate 统一处理）。

use rust_decimal::Decimal;
use serde::Deserialize;
use trading_models::{
    InstrumentType, Order, OrderStatus, Position, Side, WatchConditionKind, WatchRule,
    WatchRuleStatus, WatchRuleType,
};

use super::envelope::{ApiError, BizError};

/// 软删三态：不传=不筛选；0=仅未删；1=仅已软删。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeletedFilter {
    /// 不做软删筛选（未删 + 已软删都返回）。
    #[default]
    Any,
    /// 仅未删行。
    Active,
    /// 仅已软删行。
    Deleted,
}

impl DeletedFilter {
    /// query 的 deleted（0/1，缺省 None=不筛选）转三态；非 0/1 回 40001。
    pub fn parse(raw: Option<i64>) -> Result<Self, ApiError> {
        Ok(match raw {
            None => DeletedFilter::Any,
            Some(0) => DeletedFilter::Active,
            Some(1) => DeletedFilter::Deleted,
            Some(other) => {
                return Err(ApiError::new(
                    BizError::BadRequest,
                    format!("deleted 只接受 0/1，收到 {other}"),
                ))
            }
        })
    }

    fn matches(self, deleted: bool) -> bool {
        match self {
            DeletedFilter::Any => true,
            DeletedFilter::Active => !deleted,
            DeletedFilter::Deleted => deleted,
        }
    }
}

/// 宽松解析方向（BUY/long/多 等）；无法识别回 40001。
fn parse_side(raw: &str) -> Result<Side, ApiError> {
    Side::from_wire(raw).ok_or_else(|| {
        ApiError::bad_request(format!("side 非法：`{raw}`（允许 BUY/SELL/long/short）"))
    })
}

/// 标的形状：入参 perpetual/perp/swap/usdm 归一为 futures；缺省 spot。
fn parse_instrument_type(raw: &str) -> Result<InstrumentType, ApiError> {
    Ok(match raw.trim().to_ascii_lowercase().as_str() {
        "futures" | "perpetual" | "perp" | "swap" | "usdm" | "futures_usdm" => {
            InstrumentType::Futures
        }
        "spot" => InstrumentType::Spot,
        "option" => InstrumentType::Option,
        "equity" => InstrumentType::Equity,
        "fund" => InstrumentType::Fund,
        other => {
            return Err(ApiError::bad_request(format!(
                "instrument_type 非法：`{other}`"
            )))
        }
    })
}

// =====================================================================
// 订单
// =====================================================================

/// 订单查询的原始 query（未解析枚举/多值）。字段全部可选，缺失走默认。
#[derive(Debug, Clone, Default, Deserialize)]
pub struct OrderQuery {
    /// 账户名（与 exchange 搭配，handler 解析成 account_id；不直接进 filter）。
    #[serde(default)]
    pub account: Option<String>,
    #[serde(default)]
    pub account_id: Option<u64>,
    #[serde(default)]
    pub exchange: Option<String>,
    #[serde(default)]
    pub instrument: Option<String>,
    #[serde(default)]
    pub instrument_type: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    /// 可重复传参（status=NEW&status=PARTIAL），空=不限制状态。
    #[serde(default)]
    pub status: Vec<String>,
    #[serde(default)]
    pub strategy_name: Option<String>,
    #[serde(default)]
    pub signal_id: Option<String>,
    #[serde(default)]
    pub rule_id: Option<u64>,
    #[serde(default)]
    pub client_order_id: Option<String>,
    #[serde(default)]
    pub exchange_order_id: Option<String>,
    #[serde(default)]
    pub start_time: Option<u64>,
    #[serde(default)]
    pub end_time: Option<u64>,
    /// 0/1，缺省=不做软删筛选。
    #[serde(default)]
    pub deleted: Option<i64>,
    #[serde(default)]
    pub page: Option<usize>,
    #[serde(default)]
    pub page_size: Option<usize>,
}

impl OrderQuery {
    /// 解析为强类型谓词。`resolved_account_id` 是 handler 按 account 名解析出的
    /// account_id；显式 account_id 与之同传时以显式值为准。
    #[allow(clippy::wrong_self_convention)]
    pub fn to_filter(self, resolved_account_id: Option<u64>) -> Result<OrderFilter, ApiError> {
        let statuses = self
            .status
            .iter()
            .map(|s| {
                OrderStatus::from_wire(s)
                    .ok_or_else(|| ApiError::bad_request(format!("status 非法：`{s}`")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(OrderFilter {
            account_id: self.account_id.or(resolved_account_id),
            exchange: self.exchange,
            instrument: self.instrument,
            instrument_type: match self.instrument_type {
                Some(s) => Some(parse_instrument_type(&s)?),
                None => None,
            },
            side: match self.side {
                Some(s) => Some(parse_side(&s)?),
                None => None,
            },
            statuses,
            strategy_name: self.strategy_name,
            signal_id: self.signal_id,
            rule_id: self.rule_id,
            client_order_id: self.client_order_id,
            exchange_order_id: self.exchange_order_id,
            start_time: self.start_time,
            end_time: self.end_time,
            deleted: DeletedFilter::parse(self.deleted)?,
        })
    }
}

/// 订单强类型过滤谓词（全部条件 AND；空 Vec/None 表示不限制该维）。
#[derive(Debug, Clone, Default)]
pub struct OrderFilter {
    pub account_id: Option<u64>,
    pub exchange: Option<String>,
    pub instrument: Option<String>,
    pub instrument_type: Option<InstrumentType>,
    pub side: Option<Side>,
    pub statuses: Vec<OrderStatus>,
    pub strategy_name: Option<String>,
    pub signal_id: Option<String>,
    /// 0=只看手工/信号单。
    pub rule_id: Option<u64>,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub deleted: DeletedFilter,
}

impl OrderFilter {
    /// 所有条件 AND；字符串精确匹配（exchange/instrument 均为已归一口径）。
    pub fn matches(&self, o: &Order) -> bool {
        self.account_id.is_none_or(|v| v == o.account_id)
            && self.exchange.as_deref().is_none_or(|v| v == o.exchange)
            && self.instrument.as_deref().is_none_or(|v| v == o.instrument)
            && self.instrument_type.is_none_or(|v| v == o.instrument_type)
            && self.side.is_none_or(|v| v == o.side)
            && (self.statuses.is_empty() || self.statuses.contains(&o.status))
            && self
                .strategy_name
                .as_deref()
                .is_none_or(|v| o.strategy_name.as_deref() == Some(v))
            && self
                .signal_id
                .as_deref()
                .is_none_or(|v| o.signal_id.as_deref() == Some(v))
            && self.rule_id.is_none_or(|v| v == o.rule_id)
            && self
                .client_order_id
                .as_deref()
                .is_none_or(|v| v == o.client_order_id)
            && self
                .exchange_order_id
                .as_deref()
                .is_none_or(|v| o.exchange_order_id.as_deref() == Some(v))
            && self.start_time.is_none_or(|v| o.created_at >= v)
            && self.end_time.is_none_or(|v| o.created_at <= v)
            && self.deleted.matches(o.deleted)
    }
}

// =====================================================================
// 规则
// =====================================================================

/// 规则查询的原始 query。
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RuleQuery {
    /// 本地主键精确查询（命中至多 1 条、不分页；内存 miss 自动回源历史归档）。
    #[serde(default)]
    pub id: Option<u64>,
    /// 账户名（handler 解析成 account_id）。
    #[serde(default)]
    pub account: Option<String>,
    #[serde(default)]
    pub account_id: Option<u64>,
    #[serde(default)]
    pub exchange: Option<String>,
    #[serde(default)]
    pub instrument: Option<String>,
    #[serde(default)]
    pub instrument_type: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub position_id: Option<u64>,
    #[serde(default)]
    pub rule_type: Option<String>,
    #[serde(default)]
    pub condition_kind: Option<String>,
    #[serde(default)]
    pub condition_instrument: Option<String>,
    /// 可重复传参（IN）。
    #[serde(default)]
    pub status: Vec<String>,
    #[serde(default)]
    pub include_retired: Option<bool>,
    #[serde(default)]
    pub page: Option<usize>,
    #[serde(default)]
    pub page_size: Option<usize>,
}

impl RuleQuery {
    #[allow(clippy::wrong_self_convention)]
    pub fn to_filter(self, resolved_account_id: Option<u64>) -> Result<RuleFilter, ApiError> {
        let rule_type = match self.rule_type {
            Some(s) => Some(match s.trim().to_ascii_lowercase().as_str() {
                "new" => WatchRuleType::New,
                "risk" => WatchRuleType::Risk,
                other => {
                    return Err(ApiError::bad_request(format!(
                        "rule_type 非法：`{other}`（允许 new/risk）"
                    )))
                }
            }),
            None => None,
        };
        let condition_kind = match self.condition_kind {
            Some(s) => Some(match s.trim().to_ascii_lowercase().as_str() {
                "roi" => WatchConditionKind::Roi,
                "price" => WatchConditionKind::Price,
                "position" => WatchConditionKind::Position,
                "profit_drawdown" | "drawdown" => WatchConditionKind::ProfitDrawdown,
                other => {
                    return Err(ApiError::bad_request(format!(
                        "condition_kind 非法：`{other}`"
                    )))
                }
            }),
            None => None,
        };
        let statuses = self
            .status
            .iter()
            .map(|s| parse_rule_status(s))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RuleFilter {
            account_id: self.account_id.or(resolved_account_id),
            exchange: self.exchange,
            instrument: self.instrument,
            instrument_type: match self.instrument_type {
                Some(s) => Some(parse_instrument_type(&s)?),
                None => None,
            },
            side: match self.side {
                Some(s) => Some(parse_side(&s)?),
                None => None,
            },
            position_id: self.position_id,
            rule_type,
            condition_kind,
            condition_instrument: self.condition_instrument,
            statuses,
            // id 精确查询：默认放开 retired tombstone（id 本身已唯一定位）。
            include_retired: self.include_retired.unwrap_or_else(|| self.id.is_some()),
            id: self.id,
        })
    }
}

fn parse_rule_status(raw: &str) -> Result<WatchRuleStatus, ApiError> {
    Ok(match raw.trim().to_ascii_lowercase().as_str() {
        "active" => WatchRuleStatus::Active,
        "pending" => WatchRuleStatus::Pending,
        "in_use" => WatchRuleStatus::InUse,
        "retired" => WatchRuleStatus::Retired,
        other => {
            return Err(ApiError::bad_request(format!(
                "status 非法：`{other}`（允许 active/pending/in_use/retired）"
            )))
        }
    })
}

/// 规则强类型过滤谓词。
#[derive(Debug, Clone, Default)]
pub struct RuleFilter {
    pub id: Option<u64>,
    pub account_id: Option<u64>,
    pub exchange: Option<String>,
    pub instrument: Option<String>,
    pub instrument_type: Option<InstrumentType>,
    pub side: Option<Side>,
    pub position_id: Option<u64>,
    pub rule_type: Option<WatchRuleType>,
    pub condition_kind: Option<WatchConditionKind>,
    pub condition_instrument: Option<String>,
    pub statuses: Vec<WatchRuleStatus>,
    /// true 时含 retired tombstone；默认 false。
    pub include_retired: bool,
}

impl RuleFilter {
    pub fn matches(&self, r: &WatchRule) -> bool {
        // 默认排除终态 tombstone；显式 include_retired 或显式 status 命中才放行。
        if r.deleted && !self.include_retired && !self.statuses.contains(&r.status) {
            return false;
        }
        self.account_id.is_none_or(|v| v == r.account_id)
            // risk 行 exchange/instrument 为空串：这两维只对 new 规则有意义，
            // risk 不因空串被过滤掉（与 side 同口径）。
            && self
                .exchange
                .as_deref()
                .is_none_or(|v| matches!(r.rule_type, WatchRuleType::Risk) || v == r.exchange)
            && self
                .instrument
                .as_deref()
                .is_none_or(|v| matches!(r.rule_type, WatchRuleType::Risk) || v == r.instrument)
            && self.instrument_type.is_none_or(|v| v == r.instrument_type)
            // side 仅对 new 规则有效；risk 行 side 为无意义默认值，不据此过滤。
            && self.side.is_none_or(|v| {
                matches!(r.rule_type, WatchRuleType::Risk) || v == r.side
            })
            && self.position_id.is_none_or(|v| v == r.position_id)
            && self.rule_type.is_none_or(|v| v == r.rule_type)
            && self.condition_kind.is_none_or(|v| v == r.condition_kind)
            && self
                .condition_instrument
                .as_deref()
                .is_none_or(|v| r.condition_instrument.as_deref() == Some(v))
            && (self.statuses.is_empty() || self.statuses.contains(&r.status))
            && self.id.is_none_or(|v| v == r.id)
    }
}

// =====================================================================
// 持仓
// =====================================================================

/// 持仓查询的原始 query（roi 等比率直接收 Decimal：数字或字符串）。
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PositionQuery {
    /// 账户名（handler 解析成 account_id）。
    #[serde(default)]
    pub account: Option<String>,
    #[serde(default)]
    pub account_id: Option<u64>,
    #[serde(default)]
    pub exchange: Option<String>,
    #[serde(default)]
    pub instrument: Option<String>,
    #[serde(default)]
    pub instrument_type: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub min_roi: Option<Decimal>,
    #[serde(default)]
    pub max_roi: Option<Decimal>,
    #[serde(default)]
    pub deleted: Option<i64>,
    #[serde(default)]
    pub page: Option<usize>,
    #[serde(default)]
    pub page_size: Option<usize>,
}

impl PositionQuery {
    #[allow(clippy::wrong_self_convention)]
    pub fn to_filter(self, resolved_account_id: Option<u64>) -> Result<PositionFilter, ApiError> {
        Ok(PositionFilter {
            account_id: self.account_id.or(resolved_account_id),
            exchange: self.exchange,
            instrument: self.instrument,
            instrument_type: match self.instrument_type {
                Some(s) => Some(parse_instrument_type(&s)?),
                None => None,
            },
            side: match self.side {
                Some(s) => Some(parse_side(&s)?),
                None => None,
            },
            min_roi: self.min_roi,
            max_roi: self.max_roi,
            deleted: DeletedFilter::parse(self.deleted)?,
        })
    }
}

/// 持仓强类型过滤谓词。
#[derive(Debug, Clone, Default)]
pub struct PositionFilter {
    pub account_id: Option<u64>,
    pub exchange: Option<String>,
    pub instrument: Option<String>,
    pub instrument_type: Option<InstrumentType>,
    pub side: Option<Side>,
    pub min_roi: Option<Decimal>,
    pub max_roi: Option<Decimal>,
    /// 文档默认不传=不筛选（与订单一致）。
    pub deleted: DeletedFilter,
}

impl PositionFilter {
    pub fn matches(&self, p: &Position) -> bool {
        self.account_id.is_none_or(|v| v == p.account_id)
            // 仓储的仓位交易所匹配按忽略大小写口径（文档 §4.8）。
            && self
                .exchange
                .as_deref()
                .is_none_or(|v| v.eq_ignore_ascii_case(&p.exchange))
            && self.instrument.as_deref().is_none_or(|v| v == p.instrument)
            && self.instrument_type.is_none_or(|v| v == p.instrument_type)
            && self.side.is_none_or(|v| v == p.side)
            && self.min_roi.is_none_or(|v| p.roi >= v)
            && self.max_roi.is_none_or(|v| p.roi <= v)
            && self.deleted.matches(p.deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use trading_models::{
        InstrumentType, Order, OrderStatus, OrderType, Position, Side, WatchConditionKind,
        WatchRule, WatchRuleAction, WatchRuleStatus, WatchRuleType,
    };

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    // ---------- OrderFilter ----------

    #[test]
    fn order_filter_ands_all_conditions() {
        // Arrange
        let mut hit = Order::new(
            7,
            "0x0123456789abcdef0123456789abcdef",
            "binance",
            "BTCUSDT",
            Side::Buy,
        );
        hit.id = 2;
        hit.instrument_type = InstrumentType::Futures;
        hit.status = OrderStatus::New;
        hit.strategy_name = Some("s1".into());
        hit.signal_id = Some("sig".into());
        hit.created_at = 100;
        let mut other = hit.clone();
        other.id = 3;
        other.exchange = "xili".into();

        let f = OrderFilter {
            account_id: Some(7),
            exchange: Some("binance".into()),
            instrument: Some("BTCUSDT".into()),
            instrument_type: Some(InstrumentType::Futures),
            side: Some(Side::Buy),
            statuses: vec![OrderStatus::New, OrderStatus::Partial],
            strategy_name: Some("s1".into()),
            signal_id: Some("sig".into()),
            rule_id: None,
            client_order_id: None,
            exchange_order_id: None,
            start_time: Some(50),
            end_time: Some(150),
            deleted: DeletedFilter::Active,
        };
        // Act / Assert
        assert!(f.matches(&hit));
        assert!(!f.matches(&other)); // 交易所不符
    }

    #[test]
    fn order_filter_rule_id_zero_means_manual_only() {
        // Arrange
        let mut manual = Order::new(1, "0x0123456789abcdef0123456789abcdef", "b", "X", Side::Buy);
        manual.rule_id = 0;
        let mut rule = manual.clone();
        rule.rule_id = 5;
        // Act
        let only_manual = OrderFilter {
            rule_id: Some(0),
            ..Default::default()
        };
        // Assert
        assert!(only_manual.matches(&manual));
        assert!(!only_manual.matches(&rule));
        // 具体规则 id 过滤。
        let rule5 = OrderFilter {
            rule_id: Some(5),
            ..Default::default()
        };
        assert!(!rule5.matches(&manual));
        assert!(rule5.matches(&rule));
    }

    #[test]
    fn order_filter_created_at_time_range_inclusive() {
        // Arrange
        let mut o = Order::new(1, "0x0123456789abcdef0123456789abcdef", "b", "X", Side::Buy);
        o.created_at = 100;
        let f = |start, end| OrderFilter {
            start_time: start,
            end_time: end,
            ..Default::default()
        };
        // Act / Assert：闭区间。
        assert!(f(Some(100), Some(100)).matches(&o));
        assert!(f(Some(99), None).matches(&o));
        assert!(!f(Some(101), None).matches(&o));
        assert!(!f(None, Some(99)).matches(&o));
    }

    #[test]
    fn order_filter_deleted_tri_state() {
        // Arrange
        let mut alive = Order::new(1, "0x0123456789abcdef0123456789abcdef", "b", "X", Side::Buy);
        alive.deleted = false;
        let mut gone = alive.clone();
        gone.deleted = true;
        // Act / Assert：缺省 Any 两类都收；Active/Deleted 各自单边。
        assert!(OrderFilter::default().matches(&alive));
        assert!(OrderFilter::default().matches(&gone));
        assert!(OrderFilter {
            deleted: DeletedFilter::Active,
            ..Default::default()
        }
        .matches(&alive));
        assert!(!OrderFilter {
            deleted: DeletedFilter::Active,
            ..Default::default()
        }
        .matches(&gone));
        assert!(OrderFilter {
            deleted: DeletedFilter::Deleted,
            ..Default::default()
        }
        .matches(&gone));
    }

    #[test]
    fn order_query_parses_repeated_status_and_deleted() {
        // Act：status 可重复传参（IN），deleted 三态。
        let f = OrderQuery {
            account_id: Some(7),
            status: vec!["NEW".into(), "partial".into()],
            deleted: Some(1),
            ..Default::default()
        }
        .to_filter(None)
        .unwrap();
        // Assert
        assert_eq!(f.account_id, Some(7));
        assert_eq!(f.statuses, vec![OrderStatus::New, OrderStatus::Partial]);
        assert_eq!(f.deleted, DeletedFilter::Deleted);

        // 未知状态串回 40001。
        let err = OrderQuery {
            status: vec!["NOPE".into()],
            ..Default::default()
        }
        .to_filter(None)
        .unwrap_err();
        assert_eq!(err.code, BizError::BadRequest);
        // deleted 非 0/1 回 40001。
        assert!(OrderQuery {
            deleted: Some(2),
            ..Default::default()
        }
        .to_filter(None)
        .is_err());
    }

    #[test]
    fn order_query_account_id_wins_when_both_given() {
        // 账户名解析出的 id 与显式 account_id 同传时，以显式 account_id 为准。
        let f = OrderQuery {
            account_id: Some(3),
            ..Default::default()
        }
        .to_filter(Some(99))
        .unwrap();
        assert_eq!(f.account_id, Some(3));
        // 未显式给 account_id 时用解析结果。
        let f = OrderQuery::default().to_filter(Some(99)).unwrap();
        assert_eq!(f.account_id, Some(99));
    }

    // ---------- RuleFilter ----------

    fn risk_rule(id: u64, side: Side) -> WatchRule {
        let mut r = WatchRule::new_risk(
            7,
            42,
            WatchConditionKind::Roi,
            None,
            ">=",
            Decimal::ONE,
            WatchRuleAction::UseSelf,
            OrderType::Market,
            Decimal::ZERO,
            Decimal::ONE,
        );
        r.id = id;
        // new_risk 固定 Buy；测试需要 SELL 时覆盖。
        r.side = side;
        r
    }

    fn new_rule(id: u64, side: Side) -> WatchRule {
        let mut r = WatchRule::new_open(
            7,
            0,
            "binance",
            "BTCUSDT",
            InstrumentType::Futures,
            side,
            OrderType::Limit,
            Decimal::from(80_000),
            Decimal::ONE,
            1,
        );
        r.id = id;
        r
    }

    #[test]
    fn rule_side_filter_only_applies_to_new_rules() {
        // risk 行的 side 无业务意义，side 过滤不得排除 risk。
        let risk = risk_rule(1, Side::Buy);
        let open_buy = new_rule(2, Side::Buy);
        let open_sell = new_rule(3, Side::Sell);
        let f = RuleFilter {
            side: Some(Side::Sell),
            ..Default::default()
        };
        assert!(f.matches(&risk), "risk 行忽略 side 过滤");
        assert!(!f.matches(&open_buy));
        assert!(f.matches(&open_sell));
    }

    #[test]
    fn rule_filter_hides_retired_unless_included() {
        // Arrange
        let active = risk_rule(1, Side::Buy);
        let mut retired = risk_rule(2, Side::Buy);
        retired.status = WatchRuleStatus::Retired;
        retired.deleted = true;
        // Act / Assert：默认只返回未终止行。
        assert!(RuleFilter::default().matches(&active));
        assert!(!RuleFilter::default().matches(&retired));
        assert!(RuleFilter {
            include_retired: true,
            ..Default::default()
        }
        .matches(&retired));
    }

    #[test]
    fn rule_filter_status_in_and_identity_fields() {
        let mut pending = new_rule(1, Side::Buy);
        pending.status = WatchRuleStatus::Pending;
        let f = RuleFilter {
            account_id: Some(7),
            rule_type: Some(WatchRuleType::New),
            condition_kind: Some(WatchConditionKind::Roi),
            position_id: Some(0),
            statuses: vec![WatchRuleStatus::Pending],
            ..Default::default()
        };
        assert!(f.matches(&pending));
        // 状态不在 IN 集合。
        let active = new_rule(2, Side::Buy);
        assert!(!f.matches(&active));
        // rule_type 不符。
        assert!(!RuleFilter {
            rule_type: Some(WatchRuleType::Risk),
            ..Default::default()
        }
        .matches(&active));
    }

    #[test]
    fn rule_query_parses_lowercase_enums() {
        let q = RuleQuery {
            rule_type: Some("new".into()),
            condition_kind: Some("profit_drawdown".into()),
            side: Some("short".into()),
            status: vec!["active".into(), "pending".into()],
            include_retired: Some(true),
            ..Default::default()
        };
        let f = q.to_filter(None).unwrap();
        assert_eq!(f.rule_type, Some(WatchRuleType::New));
        assert_eq!(f.condition_kind, Some(WatchConditionKind::ProfitDrawdown));
        assert_eq!(f.side, Some(Side::Sell));
        assert_eq!(f.statuses.len(), 2);
        assert!(f.include_retired);
        // drawdown 是 profit_drawdown 的别名。
        let f = RuleQuery {
            condition_kind: Some("drawdown".into()),
            ..Default::default()
        }
        .to_filter(None)
        .unwrap();
        assert_eq!(f.condition_kind, Some(WatchConditionKind::ProfitDrawdown));
        // 非法枚举回 40001。
        assert!(RuleQuery {
            rule_type: Some("nope".into()),
            ..Default::default()
        }
        .to_filter(None)
        .is_err());
    }

    // ---------- PositionFilter ----------

    fn position(id: u64, side: Side, roi: &str) -> Position {
        let mut p = Position::new(7, "binance", "BTCUSDT", side);
        p.id = id;
        p.roi = dec(roi);
        p
    }

    #[test]
    fn position_filter_roi_range() {
        // Arrange
        let win = position(1, Side::Buy, "0.12");
        let lose = position(2, Side::Sell, "-0.05");
        // Act
        let f = PositionFilter {
            min_roi: Some(dec("0.1")),
            max_roi: None,
            ..Default::default()
        };
        // Assert
        assert!(f.matches(&win));
        assert!(!f.matches(&lose));
        // 区间。
        let band = PositionFilter {
            min_roi: Some(dec("-0.1")),
            max_roi: Some(dec("0")),
            ..Default::default()
        };
        assert!(!band.matches(&win));
        assert!(band.matches(&lose));
    }

    #[test]
    fn position_filter_exchange_is_case_insensitive() {
        // 仓储的仓位交易所匹配按忽略大小写口径（文档 §4.8）。
        let p = position(1, Side::Buy, "0");
        let f = PositionFilter {
            exchange: Some("BINANCE".into()),
            ..Default::default()
        };
        assert!(f.matches(&p));
    }

    #[test]
    fn position_filter_deleted_defaults_to_any_explicit_active_hides_closed() {
        // §4.8：deleted 不传=不筛选（持仓中+已平仓都返回）；显式 0 才仅持仓中。
        let mut closed = position(1, Side::Buy, "0");
        closed.deleted = true;
        // 默认 Any：tombstone 也可见。
        assert!(PositionFilter::default().matches(&closed));
        // 显式 Active：隐藏已平仓。
        assert!(!PositionFilter {
            deleted: DeletedFilter::Active,
            ..Default::default()
        }
        .matches(&closed));
        // 显式 Deleted：只看 tombstone。
        assert!(PositionFilter {
            deleted: DeletedFilter::Deleted,
            ..Default::default()
        }
        .matches(&closed));
    }

    #[test]
    fn position_query_parses_decimal_roi_and_deleted() {
        // 入参比率接受数字或字符串（decimal_wire 口径在 handler 层；这里给已解析 Decimal）。
        let f = PositionQuery {
            min_roi: Some(dec("0.1")),
            deleted: Some(0),
            ..Default::default()
        }
        .to_filter(None)
        .unwrap();
        assert_eq!(f.min_roi, Some(dec("0.1")));
        assert_eq!(f.deleted, DeletedFilter::Active);
    }
}
