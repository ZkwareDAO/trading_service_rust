//! 线上报文协议（三个进程共享同一份定义，避免字符串各自漂移）：
//!
//! * 南向（strategy -> trading-service）：[`SouthCommand`]，`option` 取值
//!   `place_order` / `cancel_order` / `get_order` / `get_positions` / `create_rule`
//!   （create_rule 的规则字段见结构体尾部；place_order/new 规则可选 `leverage`；
//!   get_order 必填 account/exchange/client_order_id，返回订单与下单回报同形；
//!   get_positions 只读本地仓位、参数全部可选，与 REST `GET /positions` 同逻辑）；
//! * 北向（交易所 -> trading-service）：[`NorthMessage`]，
//!   `type=register` 注册 REST 基址，`type=order_event` 推送订单回报；
//! * 交易所 REST（trading-service -> 交易所）：[`RestPlaceOrder`] /
//!   [`RestCancelOrder`] / [`RestAck`]；
//! * 回报/应答（trading-service -> strategy）：[`ClientEvent`]。

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use trading_models::{Order, OrderStatus, OrderType, Side};

use crate::decimal_wire;

/// 交易所 REST：下单路径。
pub const REST_PLACE_PATH: &str = "/orders";
/// 交易所 REST：撤单路径。
pub const REST_CANCEL_PATH: &str = "/orders/cancel";
/// 交易所 REST：持仓查询路径。
pub const REST_POSITIONS_PATH: &str = "/positions";

// =============================================================================
// 南向：strategy -> trading-service
// =============================================================================

/// 策略端命令。字段全部宽松可选，由服务层按 `option` 校验必填项。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SouthCommand {
    /// place_order / cancel_order / get_position。
    pub option: String,
    /// 策略身份；同时是回报路由键。
    #[serde(default)]
    pub strategy_name: Option<String>,
    /// 早期示例里的身份字段，作为 strategy_name 的回退（线兼容旧名）。
    #[serde(default, alias = "strategy_mock")]
    pub strategy: Option<String>,
    /// 目标交易所名（xili / binance…）。线兼容旧字段名 exchange_mock。
    #[serde(default, alias = "exchange_mock")]
    pub exchange: Option<String>,
    /// 账户名（v0 由服务映射为 account_id）。
    #[serde(default)]
    pub account: Option<String>,
    /// 交易所原生标的；兼容旧示例的 symbol。
    #[serde(default)]
    pub instrument: Option<String>,
    #[serde(default)]
    pub symbol: Option<String>,
    /// long/short（兼容 buy/sell）。
    #[serde(default)]
    pub side: Option<String>,
    /// limit/market…，缺省 limit。
    #[serde(default)]
    pub order_type: Option<String>,
    /// 委托价；市价单可省略。
    #[serde(default)]
    #[serde(with = "decimal_wire::opt")]
    pub price: Option<Decimal>,
    /// 委托数量。
    #[serde(default)]
    #[serde(with = "decimal_wire::opt")]
    pub quantity: Option<Decimal>,
    /// 撤单目标：客户订单号（查单首选键）。
    #[serde(default)]
    pub client_order_id: Option<String>,
    /// 标的类型：spot / futures（perpetual/swap 同义）。缺省按 spot。
    #[serde(default, alias = "instrumentType")]
    pub instrument_type: Option<String>,
    /// 杠杆倍数：place_order 期货开仓消费。三态语义：缺省 None=不下发
    /// set_leverage（维持账户当前杠杆）；显式给值（含 1）= 下单前总是下发该倍数。
    /// create_rule 的 new 规则同样消费；平仓单（reduce_only=true）不消费。
    #[serde(default)]
    pub leverage: Option<u64>,
    /// 只减仓不反手：true=立即平仓信号（配合反向下单，全平/减仓由 quantity 决定）；
    /// 缺省 false=普通开仓。平仓单不触发 set_leverage。仅单向(BOTH)持仓生效。
    #[serde(default)]
    pub reduce_only: bool,
    /// risk 风控规则绑定的本地仓位 id（值来自 get_position 回填的 position_id）；
    /// place_order 不消费。create_rule 的 risk 规则必填，服务端按此取活跃仓位。
    #[serde(default)]
    pub position_id: Option<u64>,
    /// 来源信号 id：place_order 必填（WS 下单必带）；一个 signal_id 仅对应一笔订单，
    /// 重复 signal_id 的下单命令直接忽略（不重复下单）。规则单不带。
    #[serde(default)]
    pub signal_id: Option<String>,
    /// 下单金额（报价币种计，如 USDT）。与 quantity 二选一：
    /// 传了 quantity 用 quantity；否则用 order_cash ÷ 参考价换算出 quantity
    /// （limit 用委托价；market 用盘口可成交价、回退估值价 mark>mid>last）。
    #[serde(default)]
    #[serde(with = "decimal_wire::opt")]
    pub order_cash: Option<Decimal>,
    /// get_positions 过滤：最小 ROI（比率，如 0.1）。仅查询消费，其余命令忽略。
    #[serde(default)]
    #[serde(with = "decimal_wire::opt")]
    pub min_roi: Option<Decimal>,
    /// get_positions 过滤：最大 ROI。
    #[serde(default)]
    #[serde(with = "decimal_wire::opt")]
    pub max_roi: Option<Decimal>,
    /// get_positions 过滤：0=仅活跃、1=已平仓、不传=全部（与 REST 一致）。
    #[serde(default)]
    pub deleted: Option<i64>,

    // —— create_rule 专用（place_order 等命令忽略；全部可选，服务层按 rule_type 校验）——
    /// 规则类型：new（开仓）/ risk（风控）。缺省 risk。
    #[serde(default)]
    pub rule_type: Option<String>,
    /// 条件主类：roi / price / position / profit_drawdown。
    #[serde(default, alias = "condition")]
    pub condition_kind: Option<String>,
    /// price_/position_ 条件的标的后缀；空=绑定仓位自身标的。
    #[serde(default, alias = "conditionSymbol")]
    pub condition_instrument: Option<String>,
    /// 条件比较操作符：>= <= > < ==。
    #[serde(default)]
    pub operator: Option<String>,
    /// 条件阈值（比率/价格/数量）。
    #[serde(default)]
    #[serde(with = "decimal_wire::opt")]
    pub value: Option<Decimal>,
    /// 命中动作：self（执行本规则）。链式 rule:<id> 本期不开放远程创建。
    #[serde(default)]
    pub action: Option<String>,
    /// risk 按持仓比例减/平仓（1=全平、0.5=半仓）；与 quantity 绝对量互斥。
    #[serde(default)]
    #[serde(with = "decimal_wire::opt")]
    pub quantity_pct: Option<Decimal>,
    /// 创建为 pending（链式后继，等待激活）；缺省 active。
    #[serde(default)]
    pub pending: bool,
}

/// 标的类型归一化：futures/perpetual/swap/futures_usdm -> futures，option -> option，
/// 其余视为 spot。
pub fn normalize_instrument_type(raw: Option<&str>) -> String {
    match raw.unwrap_or("spot").trim().to_ascii_lowercase().as_str() {
        "futures" | "perpetual" | "perp" | "swap" | "usdm" | "futures_usdm" => {
            "futures".to_string()
        }
        "option" | "options" => "option".to_string(),
        _ => "spot".to_string(),
    }
}

impl SouthCommand {
    pub fn option_kind(&self) -> String {
        self.option.trim().to_ascii_lowercase()
    }

    pub fn strategy(&self) -> Option<&str> {
        self.strategy_name
            .as_deref()
            .or(self.strategy.as_deref())
            .filter(|s| !s.is_empty())
    }

    pub fn exchange(&self) -> Option<&str> {
        self.exchange.as_deref().filter(|s| !s.is_empty())
    }

    pub fn instrument(&self) -> Option<&str> {
        self.instrument
            .as_deref()
            .or(self.symbol.as_deref())
            .filter(|s| !s.is_empty())
    }

    pub fn side(&self) -> Option<Side> {
        self.side.as_deref().and_then(Side::from_wire)
    }

    pub fn order_type(&self) -> OrderType {
        OrderType::from_wire(self.order_type.as_deref().unwrap_or("limit"))
    }
}

// =============================================================================
// 北向：交易所 -> trading-service
// =============================================================================

/// 交易所在北向 WS 上发送的消息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NorthMessage {
    /// register / order_event。
    #[serde(rename = "type")]
    pub kind: String,
    /// 交易所实例名（register 与回报均携带）。
    #[serde(default)]
    pub name: Option<String>,
    /// register：交易所 REST 基址，如 http://127.0.0.1:40513。
    #[serde(default)]
    pub rest_base_url: Option<String>,
    /// order_event：订单回报体。
    #[serde(default)]
    pub event: Option<OrderEvent>,
}

impl NorthMessage {
    pub fn is_register(&self) -> bool {
        self.kind.eq_ignore_ascii_case("register")
    }
}

/// 交易所订单回报。`client_order_id` 原样带回，是匹配与路由的首选键。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderEvent {
    pub client_order_id: String,
    /// NEW / PARTIAL / FILLED / CANCELED / REJECTED / EXPIRED / UNKNOWN。
    pub status: String,
    #[serde(default)]
    pub exchange_order_id: Option<String>,
    /// 成交均价（兼容旧字段名 avg_price）。
    #[serde(default, alias = "avg_price")]
    #[serde(with = "decimal_wire::opt")]
    pub filled_price: Option<Decimal>,
    /// 累计成交量（兼容旧字段名 cum_qty）。
    #[serde(default, alias = "cum_qty")]
    #[serde(with = "decimal_wire::opt")]
    pub filled_quantity: Option<Decimal>,
    #[serde(default)]
    pub exchange_update_time: Option<String>,
    #[serde(default)]
    pub message: Option<String>,

    /// 合约 / 标的名（如 BTC-31JUL26-66000-P）。通知 A 在本地无 order 时也需展示，
    /// 故随回报透传；`#[serde(default)]` 兼容旧 south 客户端。
    #[serde(default)]
    pub instrument: String,

    // —— venue 原生委托要素（仅 Deribit 订单事件播报使用；其余 venue 留默认）——
    // 全部 #[serde(default)]：旧 south 客户端缺字段也能正常反序列化。
    /// 订单原生方向（"buy"/"sell"）；reduce_only 时播报需取反得仓位方向。
    #[serde(default)]
    pub direction: Option<String>,
    /// 是否 reduce_only 平仓/减仓单。
    #[serde(default)]
    pub reduce_only: bool,
    /// 委托总量（挂单阶段数量；部分成交判定基准）。
    #[serde(default)]
    #[serde(with = "decimal_wire::opt")]
    pub amount: Option<Decimal>,
    /// 委托价（挂单阶段价格；市价单缺省）。
    #[serde(default)]
    #[serde(with = "decimal_wire::opt")]
    pub limit_price: Option<Decimal>,
}

impl OrderEvent {
    /// 把交易所状态串解析为领域状态。
    pub fn resolved_status(&self) -> Option<OrderStatus> {
        parse_status(&self.status)
    }
}

/// 宽松解析交易所/内部状态串（大小写、连字符均容忍）。
/// 权威实现位于 [`OrderStatus::from_wire`]，此处保留协议层入口便于调用方引用。
pub fn parse_status(raw: &str) -> Option<OrderStatus> {
    OrderStatus::from_wire(raw)
}

// =============================================================================
// 交易所 REST 报文（trading-service <-> 交易所）
// =============================================================================

/// 下单请求体。client_order_id 由 trading-service 生成并透传。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestPlaceOrder {
    pub client_order_id: String,
    pub account: String,
    pub instrument: String,
    pub side: String,
    pub order_type: String,
    pub time_in_force: String,
    #[serde(with = "decimal_wire")]
    pub price: Decimal,
    #[serde(with = "decimal_wire")]
    pub quantity: Decimal,
    pub reduce_only: bool,
    pub leverage: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestCancelOrder {
    pub client_order_id: String,
    pub instrument: String,
}

/// 交易所 REST 应答。HTTP 2xx 且 accepted=true 视为受理。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestAck {
    pub accepted: bool,
    pub client_order_id: String,
    #[serde(default)]
    pub exchange_order_id: Option<String>,
    pub status: String,
    #[serde(default)]
    pub message: Option<String>,
}

// =============================================================================
// trading-service -> strategy 的事件/应答
// =============================================================================

/// 推送给策略端的统一事件体（ack、订单回报、错误、持仓查询结果）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientEvent {
    /// ack / order_event / error / positions。
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub client_order_id: Option<String>,
    /// 来源信号 id（原样回传下单时的 signal_id），供策略侧据此识别回报归属；
    /// 规则单（无 signal_id）为 null。
    #[serde(default)]
    pub signal_id: Option<String>,
    #[serde(default)]
    pub exchange_order_id: Option<String>,
    #[serde(default)]
    pub exchange: Option<String>,
    #[serde(default)]
    pub strategy_name: Option<String>,
    #[serde(default)]
    pub instrument: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub order_type: Option<String>,
    #[serde(default)]
    #[serde(with = "decimal_wire::opt")]
    pub order_price: Option<Decimal>,
    #[serde(default)]
    #[serde(with = "decimal_wire::opt")]
    pub order_quantity: Option<Decimal>,
    /// 平仓单标记（reduce_only=true）；仅 ack/order_event 随订单快照带出。
    #[serde(default)]
    pub reduce_only: Option<bool>,
    #[serde(default)]
    #[serde(with = "decimal_wire::opt")]
    pub filled_price: Option<Decimal>,
    #[serde(default)]
    #[serde(with = "decimal_wire::opt")]
    pub filled_quantity: Option<Decimal>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub positions: Option<Vec<PositionSnapshot>>,
}

/// 持仓快照（get_position 应答元素，来自交易所实时查询）。
/// `position_id` 不是交易所字段：由服务端用内存活跃仓表按
/// 账户+所+合约形状+标的+方向 join 回填；匹配不上（如刚开仓尚未对账）为 null。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionSnapshot {
    pub account: String,
    pub exchange: String,
    pub instrument: String,
    pub side: String,
    #[serde(with = "decimal_wire")]
    pub quantity: Decimal,
    #[serde(with = "decimal_wire")]
    pub pos_price: Decimal,
    /// 本系统内存仓位主键，供 create_rule(risk) 绑定；交易所侧无此 id。
    #[serde(default)]
    pub position_id: Option<u64>,
}

impl ClientEvent {
    /// 依据订单当前内存快照构造事件（ack / order_event 共用）。
    pub fn order_snapshot(kind: &str, status: &str, order: &Order) -> Self {
        ClientEvent {
            kind: kind.to_string(),
            status: Some(status.to_string()),
            client_order_id: Some(order.client_order_id.clone()),
            signal_id: order.signal_id.clone(),
            exchange_order_id: order.exchange_order_id.clone(),
            exchange: Some(order.exchange.clone()),
            strategy_name: order.strategy_name.clone(),
            instrument: Some(order.instrument.clone()),
            side: Some(order.side.to_string()),
            order_type: Some(order.order_type.wire().to_string()),
            order_price: Some(order.order_price),
            order_quantity: Some(order.order_quantity),
            reduce_only: Some(order.reduce_only),
            filled_price: Some(order.filled_price),
            filled_quantity: Some(order.filled_quantity),
            message: None,
            positions: None,
        }
    }

    pub fn error(message: impl Into<String>, strategy_name: Option<String>) -> Self {
        ClientEvent {
            kind: "error".to_string(),
            status: Some("REJECTED".to_string()),
            client_order_id: None,
            signal_id: None,
            exchange_order_id: None,
            exchange: None,
            strategy_name,
            instrument: None,
            side: None,
            order_type: None,
            order_price: None,
            order_quantity: None,
            reduce_only: None,
            filled_price: None,
            filled_quantity: None,
            message: Some(message.into()),
            positions: None,
        }
    }

    /// 无订单/无 id 的纯文本成功应答（如 create_rule 创建成功；本期不回 rule_id/position_id）。
    pub fn ack(message: impl Into<String>, strategy_name: Option<String>) -> Self {
        ClientEvent {
            kind: "ack".to_string(),
            status: Some("OK".to_string()),
            client_order_id: None,
            signal_id: None,
            exchange_order_id: None,
            exchange: None,
            strategy_name,
            instrument: None,
            side: None,
            order_type: None,
            order_price: None,
            order_quantity: None,
            reduce_only: None,
            filled_price: None,
            filled_quantity: None,
            message: Some(message.into()),
            positions: None,
        }
    }

    pub fn positions(strategy_name: Option<String>, positions: Vec<PositionSnapshot>) -> Self {
        ClientEvent {
            kind: "positions".to_string(),
            status: Some("OK".to_string()),
            client_order_id: None,
            signal_id: None,
            exchange_order_id: None,
            exchange: None,
            strategy_name,
            instrument: None,
            side: None,
            order_type: None,
            order_price: None,
            order_quantity: None,
            reduce_only: None,
            filled_price: None,
            filled_quantity: None,
            message: Some(
                "持仓来自交易所实时查询，position_id 由服务端按内存活跃仓 join 回填".to_string(),
            ),
            positions: Some(positions),
        }
    }
}

#[cfg(test)]
#[cfg(test)]
mod tests {
    use super::normalize_instrument_type;

    #[test]
    fn instrument_type_normalizes() {
        assert_eq!(normalize_instrument_type(Some("futures")), "futures");
        assert_eq!(normalize_instrument_type(Some("PERPETUAL")), "futures");
        assert_eq!(normalize_instrument_type(Some("swap")), "futures");
        assert_eq!(normalize_instrument_type(Some("FUTURES")), "futures");
        assert_eq!(normalize_instrument_type(Some("spot")), "spot");
        assert_eq!(normalize_instrument_type(None), "spot");
        assert_eq!(normalize_instrument_type(Some("weird")), "spot");
        // 期权（Deribit）独立归一为 option，大小写不敏感。
        assert_eq!(normalize_instrument_type(Some("option")), "option");
        assert_eq!(normalize_instrument_type(Some("OPTION")), "option");
    }
    use super::*;
    use std::str::FromStr;

    #[test]
    fn parses_v1plan_place_command() {
        let raw = r#"{"option":"place_order","strategy_name":"test_binance","instrument":"btcusdt","side":"long","price":80000,"quantity":20,"order_type":"market"}"#;
        let cmd: SouthCommand = serde_json::from_str(raw).unwrap();
        assert_eq!(cmd.option_kind(), "place_order");
        assert_eq!(cmd.strategy(), Some("test_binance"));
        assert_eq!(cmd.instrument(), Some("btcusdt"));
        assert_eq!(cmd.side(), Some(Side::Buy));
        assert!(matches!(cmd.order_type(), OrderType::Market));
        assert_eq!(cmd.quantity, Some(Decimal::from(20)));
    }

    #[test]
    fn place_command_parses_signal_id_and_order_cash() {
        // 现金口径下单：带 signal_id 与 order_cash、不带 quantity。
        let raw = r#"{"option":"place_order","exchange":"binance","instrument":"btcusdt","side":"long","order_type":"market","signal_id":"sig-001","order_cash":100}"#;
        let cmd: SouthCommand = serde_json::from_str(raw).unwrap();
        assert_eq!(cmd.signal_id.as_deref(), Some("sig-001"));
        assert_eq!(cmd.order_cash, Some(Decimal::from(100)));
        assert_eq!(cmd.quantity, None);
    }

    #[test]
    fn place_command_defaults_signal_id_and_cash_absent() {
        let raw = r#"{"option":"place_order","exchange":"binance","instrument":"btcusdt","side":"long","quantity":1}"#;
        let cmd: SouthCommand = serde_json::from_str(raw).unwrap();
        assert_eq!(cmd.signal_id, None);
        assert_eq!(cmd.order_cash, None);
    }

    #[test]
    fn status_parsing_is_lenient() {
        assert_eq!(parse_status("new"), Some(OrderStatus::New));
        assert_eq!(parse_status("PARTIAL"), Some(OrderStatus::Partial));
        assert_eq!(parse_status("filled"), Some(OrderStatus::Filled));
        assert_eq!(parse_status("rejected"), Some(OrderStatus::Rejected));
        assert_eq!(parse_status("nope"), None);
    }

    #[test]
    fn south_command_defaults_reduce_only_and_position_id() {
        // 缺省：reduce_only=false、position_id=None（旧客户端报文向后兼容）。
        let raw = r#"{"option":"place_order","exchange":"binance","instrument":"btcusdt","side":"long","quantity":1}"#;
        let cmd: SouthCommand = serde_json::from_str(raw).unwrap();
        assert!(!cmd.reduce_only);
        assert_eq!(cmd.position_id, None);
    }

    #[test]
    fn south_command_parses_reduce_only_and_position_id() {
        // 立即平仓信号：reduce_only=true + 本地仓位 id。
        let raw = r#"{"option":"place_order","exchange":"binance","instrument":"btcusdt","side":"short","quantity":1,"reduce_only":true,"position_id":42}"#;
        let cmd: SouthCommand = serde_json::from_str(raw).unwrap();
        assert!(cmd.reduce_only);
        assert_eq!(cmd.position_id, Some(42));
    }

    #[test]
    fn order_snapshot_carries_reduce_only_flag() {
        use trading_models::Side;
        let mut order = Order::new(
            1,
            "0x0123456789abcdef0123456789abcdef",
            "binance",
            "BTCUSDT",
            Side::Sell,
        );
        order.reduce_only = true;
        let ev = ClientEvent::order_snapshot("ack", "NEW", &order);
        assert_eq!(
            ev.reduce_only,
            Some(true),
            "平仓单快照必须带出 reduce_only=true"
        );
    }

    #[test]
    fn order_snapshot_echoes_signal_id() {
        use trading_models::Side;
        let mut order = Order::new(
            1,
            "0x0123456789abcdef0123456789abcdef",
            "binance",
            "BTCUSDT",
            Side::Buy,
        );
        order.signal_id = Some("sig-0001".to_string());
        // ack 与 order_event 复用同一构造，都必须原样回传 signal_id。
        let ack = ClientEvent::order_snapshot("ack", "NEW", &order);
        let ev = ClientEvent::order_snapshot("order_event", "FILLED", &order);
        assert_eq!(ack.signal_id.as_deref(), Some("sig-0001"));
        assert_eq!(ev.signal_id.as_deref(), Some("sig-0001"));
    }

    #[test]
    fn position_snapshot_carries_optional_local_id() {
        // 构造层缺省 position_id=None；get_position 回填后给 Some。
        let snap = PositionSnapshot {
            account: "a".into(),
            exchange: "binance".into(),
            instrument: "BTCUSDT".into(),
            side: "long".into(),
            quantity: Decimal::ONE,
            pos_price: Decimal::ZERO,
            position_id: Some(7),
        };
        let json = serde_json::to_string(&snap).unwrap();
        let back: PositionSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back.position_id, Some(7));
    }

    #[test]
    fn order_event_accepts_legacy_field_names() {
        let ev: OrderEvent = serde_json::from_str(
            r#"{"client_order_id":"0xabc","status":"partial","avg_price":"100.5","cum_qty":2}"#,
        )
        .unwrap();
        assert_eq!(ev.filled_price, Some(Decimal::from_str("100.5").unwrap()));
        assert_eq!(ev.filled_quantity, Some(Decimal::from(2)));
        assert_eq!(ev.resolved_status(), Some(OrderStatus::Partial));
    }
}
