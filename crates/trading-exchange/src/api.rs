//! 真实交易所适配器统一契约（见 `docs/扩展交易所.md`）。
//!
//! REST 与 WebSocket 拆成两组能力；账户事件 WS 是**一条已认证连接、两类回调**
//! （订单状态 + 账户仓位），因此它们同属 [`AccountEventStream`] 一个 trait，不能再拆，
//! 否则要为同一账户开两条连接。
//!
//! ## 两类主体
//! * **用户主体**：一个「用户 × 交易所」一个对象、一套凭据，同时实现
//!   [`ExchangeApi`] + [`AccountEventStream`]。
//! * **交易所主体**：进程内每交易所一个无认证对象，实现 [`MarketDataStream`] 与
//!   [`RuleFetcher`]（公开行情 + 公开交易规则），所有用户共享。规则不随用户重复拉取。
//!
//! ## 两套订单号
//! * `client_order_id`：本系统 `0x`+32hex，下单即有、全局唯一、始终携带；
//! * `exchange_order_id`：交易所受理 ACK 后才有，三家形态各异，**一律视为不透明
//!   字符串原样存、原样发**，不解析/不截断/不剥前缀（Deribit 可能是 `ETH-331562`
//!   这类带前缀形态）。撤单/查单统一传 [`OrderRef`]，各适配器内部决定用哪个号路由。
//!
//! 本模块只定义接口与报文类型；各交易所实现在 [`crate::binance`] / [`crate::xili`]，
//! 宿主服务经 `trading_exchange::api` 消费这些契约。
#![allow(dead_code)]

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::stream::BoxStream;
use rust_decimal::Decimal;
use trading_core::order_rule::OrderRuleTable;
use trading_models::{InstrumentType, OrderStatus, OrderType, Side, TimeInForce};

// =====================================================================
// 订单引用与请求/应答
// =====================================================================

/// 撤单/查单的统一订单引用，替代裸 order_id。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderRef {
    /// 本系统 0x+32hex，下单即有，始终携带。
    pub client_order_id: String,
    /// 受理 ACK / WS 回报后补全；不透明字符串（可能缺）。
    pub exchange_order_id: Option<String>,
}

impl OrderRef {
    pub fn new(client_order_id: impl Into<String>) -> Self {
        Self {
            client_order_id: client_order_id.into(),
            exchange_order_id: None,
        }
    }

    pub fn with_exchange(mut self, exchange_order_id: impl Into<String>) -> Self {
        self.exchange_order_id = Some(exchange_order_id.into());
        self
    }

    /// 从内存订单行统一构造撤单/查单引用：cid 必带；若订单已有受理后的
    /// 交易所单号（重启后定位的真相源）则一并装入。
    ///
    /// 宿主只把已知的号都放进 [`OrderRef`]，**用哪个号路由由各适配器内部
    /// 决定**（CTP 同会话走 OrderRef 三元组、重启后走 OrderSysID+ExchangeID；
    /// Binance 首选 cid、eid 兜底），宿主不写任何 `if exchange == ...` 分支。
    pub fn from_order(order: &trading_models::Order) -> Self {
        let mut r#ref = Self::new(order.client_order_id.clone());
        if let Some(eid) = order.exchange_order_id.clone() {
            r#ref = r#ref.with_exchange(eid);
        }
        r#ref
    }
}

/// 逐单的持仓方向（仅 hedge 模式下显式发送；单向持仓用 [`OrderPositionSide::Both`]）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderPositionSide {
    Long,
    Short,
    Both,
}

/// 下单请求（领域口径；instrument 传交易所原生符号）。
#[derive(Debug, Clone)]
pub struct PlaceOrderRequest {
    pub instrument: String,
    pub instrument_type: InstrumentType,
    pub side: Side,
    pub order_type: OrderType,
    /// 限价必填；市价由各适配器内部转换（HL=IOC 滑点价、Deribit=档位限价），
    /// 转换后的限价仍在适配器内产生。
    pub price: Option<Decimal>,
    pub quantity: Decimal,
    /// 风控平仓单。
    pub reduce_only: bool,
    pub position_side: OrderPositionSide,
    pub time_in_force: Option<TimeInForce>,
    /// 本系统客户单号，下单时同时透传给交易所（newClientOrderId / cloid / label）。
    pub client_order_id: String,
}

/// 下单/撤单受理应答。三家挂单初始状态统一归一化为 [`OrderStatus::New`] 后存储。
#[derive(Debug, Clone)]
pub struct OrderAck {
    pub accepted: bool,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub status: OrderStatus,
    pub message: Option<String>,
    /// 实际挂出的委托类型（如 deribit 把市价开仓转成限价）；原生市价/未改写为 None。
    pub placed_type: Option<OrderType>,
    /// 实际挂出的委托价（deribit 漂移/对手限价）；无价为 None。
    pub placed_price: Option<Decimal>,
    /// 实际委托数量（经步长对齐）；未改写为 None。
    pub placed_quantity: Option<Decimal>,
}

/// venue 私有订单流的「原生委托要素」（仅特定 venue 的订单播报需要）。
///
/// 现有 [`OrderState`] 通用字段不含订单方向 / reduce_only / 委托总量 / 委托价，
/// 而 Deribit 订单事件播报（通知 A）要据此拼「开/减买仓/卖仓、委托价量」。
/// 故把这些要素收口为本结构：**仅 Deribit 映射层填充**，其余 venue 恒为 [`None`]，
/// 不污染 binance / hyperliquid 的通用映射。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VenueOrderExtra {
    /// 订单原生方向（buy/sell）；播报时 reduce_only 需再取反得仓位方向。
    pub direction: Side,
    /// 是否为 reduce_only 平仓/减仓单。
    pub reduce_only: bool,
    /// 委托总量（挂单阶段数量；部分成交判定基准）。
    pub amount: Decimal,
    /// 委托价（挂单阶段价格；市价单为 0）。
    pub limit_price: Decimal,
}

/// 查单结果（REST 查单或订单回报的统一形态）。
#[derive(Debug, Clone)]
pub struct OrderState {
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub instrument: String,
    pub status: OrderStatus,
    /// 累计成交量。
    pub filled_quantity: Decimal,
    /// 成交均价。
    pub average_price: Option<Decimal>,
    /// 持仓方向（Binance hedge 回报 ps=LONG/SHORT；单向缺省 Both）。
    /// 仅 Binance 用户流解析；其余 venue 恒 Both。
    pub position_side: OrderPositionSide,
    /// venue 原生委托要素：仅 Deribit 订单播报填充，其余 venue/REST 查单为 None。
    pub venue_extra: Option<VenueOrderExtra>,
}

// =====================================================================
// 持仓 / 行情
// =====================================================================

/// 持仓 REST 快照（一行一个持仓方向）。
///
/// `signed_quantity` 带符号（正多负空），与交易所原始口径一致；hedge 模式下同标的
/// 会有 LONG/SHORT 两行，方向以 [`Self::position_side`] 为准、符号仅表达净多空，
/// 对账 key 必须带上 position_side，不能只看符号。
#[derive(Debug, Clone)]
pub struct PositionInfo {
    pub instrument: String,
    pub instrument_type: InstrumentType,
    /// 交易所持仓方向：单向=BOTH，hedge=LONG/SHORT。
    pub position_side: OrderPositionSide,
    /// 带符号持仓量（正=多，负=空）；为 0 的仓位由适配器过滤。
    pub signed_quantity: Decimal,
    /// 开仓均价。
    pub entry_price: Decimal,
    /// 标记价（若该所提供）。
    pub mark_price: Option<Decimal>,
    /// 未实现盈亏（结算币种计）。
    pub unrealized_pnl: Option<Decimal>,
    pub leverage: Option<u64>,
    /// 名义价值（交易所口径；部分所带符号，宿主侧取绝对值参与估值）。
    pub notional: Option<Decimal>,
    /// 占用保证金（仅交易所直接给出时填充；交叉保证金由宿主按 notional/leverage 自算）。
    pub margin: Option<Decimal>,
    /// 交易所侧最近更新时间（UTC 毫秒）。
    pub update_ms: Option<i64>,
}

/// 参考价口径：三家不一致（币安=最新成交、Hyperliquid=中间价、Deribit=标记价）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriceBasis {
    Last,
    Mid,
    Mark,
}

/// 单标的参考价。
#[derive(Debug, Clone)]
pub struct ReferencePrice {
    pub instrument: String,
    pub basis: PriceBasis,
    pub price: Decimal,
}

// =====================================================================
// 私有 / 公开数据流事件
// =====================================================================

/// 账户级"脏信号"：只通知仓位可能变了，**真值一律再走 REST `get_positions` 拉取**，
/// 以覆盖外部导入仓位、强平、资金费挪动等不产生本地订单事件的变动，并避免
/// WS/REST 两条解析路径漂移。
#[derive(Debug, Clone, Default)]
pub struct AccountDirty {
    /// 受影响标的；为空表示需要全量对账。
    pub instruments: Vec<String>,
}

/// 账户事件连接上的事件：一条连接产出两类。
#[derive(Debug, Clone)]
pub enum AccountEvent {
    /// 订单状态变化（ORDER_TRADE_UPDATE / orderUpdates / user.orders.*）。
    Order(OrderState),
    /// 账户/仓位变化脏信号（ACCOUNT_UPDATE / clearinghouseState / user.changes.*）。
    Account(AccountDirty),
}

/// 公开行情逐档/聚合价。
#[derive(Debug, Clone)]
pub struct PriceTick {
    pub instrument: String,
    /// 最新/中间/标记价（按订阅口径）。
    pub price: Decimal,
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub event_ms: i64,
}

// =====================================================================
// 查单三态（区分「明确查无」与「网络失败」）
// =====================================================================

/// REST 查单结果（三态）：
///
/// * [`OrderLookup::Found`]：查到订单，携带其最新状态；
/// * [`OrderLookup::NotExists`]：交易所**明确应答订单不存在**（如币安 `-2013
///   Order does not exist`）——可据此把"下单超时 + UNKNOWN"的订单在足够时延后
///   收敛为 REJECTED（交易所确未受理）；
/// * [`OrderLookup::Unavailable`]：网络/超时/签名等**查询本身失败**——不代表
///   订单不存在，本轮不收敛、下轮再试。
///
/// 三态分离避免把"查无"与"查不了"混为一谈（原先二者都映射为 None）。
#[derive(Debug, Clone)]
pub enum OrderLookup {
    Found(OrderState),
    NotExists,
    Unavailable,
}

// =====================================================================
// claim/建单前的预检（仅新增接缝：默认透传，Binance/xili 零行为变化）
// =====================================================================

/// 预检通过后的下单要素：可能被 venue 改写（市价转限价）后的订单与请求。
#[derive(Debug, Clone)]
pub struct PreparedPlacement {
    /// 可能被更新（类型/价格/数量）后的内存订单。
    pub order: trading_models::Order,
    pub request: PlaceOrderRequest,
}

/// 预检未通过的原因（此时**不 claim、不建单**，规则保持 active、下轮重试）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlaceDefer {
    /// Deribit 平仓价差过宽：附带盘口与阈值供告警渲染。
    SpreadTooWide {
        instrument: String,
        bid: Decimal,
        ask: Decimal,
        spread: Decimal,
        threshold: Decimal,
    },
    /// 行情/合约规格尚未预热到内存：本轮无法定价，下轮再来。
    DataNotReady,
}

/// 一次下单错误的性质（把「交易所明确拒绝」与「应答不确定」区分开）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaceErrorKind {
    /// 交易所明确应答拒绝（如 HTTP 400 业务错误，币安 -2010 ReduceOnly 被拒）：
    /// 订单确定未受理，可直接收敛为 REJECTED（终态）。
    Rejected,
    /// 应答不确定（超时 / 断网 / 5xx / 429 / 签名或解析异常）：无法断定订单是否
    /// 已到交易所，落 UNKNOWN，等用户流回报或 REST 查单收敛。
    Indeterminate,
}

// =====================================================================
// Trait
// =====================================================================

/// 交易/查询能力（REST）。一个「用户 × 交易所」对象实现一份，自带凭据。
#[async_trait]
pub trait ExchangeApi: Send + Sync {
    /// 交易所标识：binance / hyperliquid / deribit。
    fn exchange(&self) -> &str;

    /// claim/建单前的**同步、纯内存**预检（无网络）。
    ///
    /// 默认原样透传，故 Binance/xili 不覆盖即行为逐字不变；需要市价转限价或
    /// 平仓价差闸门的交易所（Deribit）覆盖之：
    /// - 返回 [`PreparedPlacement`]：调用方继续 claim → 建单 → 下单；
    /// - 返回 [`PlaceDefer`]：**不 claim、不建单**，规则保持 active，下一轮扫描
    ///   重新调用本方法（持续判断，直到条件满足）。
    fn prepare_to_place(
        &self,
        order: trading_models::Order,
        request: PlaceOrderRequest,
    ) -> Result<PreparedPlacement, PlaceDefer> {
        Ok(PreparedPlacement { order, request })
    }

    /// 下单。适配器内部完成精度校验/对齐、市价转限价、客户单号透传。
    async fn place_order(&self, req: &PlaceOrderRequest) -> anyhow::Result<OrderAck>;

    /// 对一次 [`Self::place_order`] 的错误做性质分类（默认实现）：
    ///
    /// 默认一律 [`PlaceErrorKind::Indeterminate`]——不掌握交易所具体错误结构时，
    /// 保守按「应答不确定」处理，落 UNKNOWN。能识别交易所明确业务拒绝的适配器
    /// （如 Binance 识别 HTTP 400）覆盖本方法返回 [`PlaceErrorKind::Rejected`]。
    ///
    /// 与 `place_order` 的 `anyhow` 返回类型解耦：对各所 SDK 错误类型的依赖
    /// 收口在适配器内，调用方只读本枚举。后续接入新所只需覆盖本方法。
    fn classify_place_error(
        &self,
        #[allow(unused_variables)] err: &anyhow::Error,
    ) -> PlaceErrorKind {
        PlaceErrorKind::Indeterminate
    }

    /// 撤单；用哪个单号由各适配器按单号路由决定。
    async fn cancel_order(&self, instrument: &str, order: &OrderRef) -> anyhow::Result<OrderAck>;

    /// 查单；单号路由同撤单。返回 [`OrderLookup`] 三态：查到（Found）/
    /// 交易所明确应答不存在（NotExists）/ 查询本身失败（Unavailable）。
    async fn query_order(&self, instrument: &str, order: &OrderRef) -> OrderLookup;

    /// 查询账户持仓（仅返回数量非零者）。
    async fn get_positions(&self) -> anyhow::Result<Vec<PositionInfo>>;

    /// 设置杠杆；Deribit（仅期权）应返回 `不支持` 错误而非成功。
    async fn set_leverage(&self, instrument: &str, leverage: u32) -> anyhow::Result<()>;

    /// 取参考价（口径见各所：last / mid / mark）。
    async fn get_reference_price(&self, instrument: &str) -> anyhow::Result<ReferencePrice>;

    /// 查询交易所实际持仓模式（**只读校验**）：
    ///
    /// 返回 `Ok(mode)` 供与本地配置比对；默认 [`None`] 表示该交易所不做此校验
    /// （Deribit/xili 零影响）。仅 Binance 覆盖，调用方发现不一致只记日志，
    /// 绝不自动修改用户交易所设置。
    async fn query_position_mode(&self) -> anyhow::Result<Option<trading_models::PositionMode>> {
        Ok(None)
    }
}

/// 交易规则抓取能力（REST、**公开数据、无认证**）。
///
/// 规则不属于某个用户：进程内每个交易所一个无认证对象实现本 trait（可与
/// [`MarketDataStream`] 同对象），在所有用户间共享，**全所只同步一次**，不随账户重复。
#[async_trait]
pub trait RuleFetcher: Send + Sync {
    fn exchange(&self) -> &str;

    /// 全量同步规则到共享内存表（Binance/HL：启动 + 30 天定时整表替换），返回写入行数。
    /// Deribit 期权标的过多，全量无意义，可返回 0 并改走 [`RuleFetcher::ensure_rule`] 懒加载。
    async fn sync_rules(&self, table: &OrderRuleTable) -> anyhow::Result<usize>;

    /// 确保某标的规则在表内并返回该行（Deribit：未命中懒加载 `get_instrument`；
    /// Binance/HL：直接读全量表，缺失则触发一次 [`RuleFetcher::sync_rules`]）。
    async fn ensure_rule(
        &self,
        table: &OrderRuleTable,
        instrument_type: InstrumentType,
        instrument: &str,
    ) -> anyhow::Result<Arc<trading_models::OrderRule>>;
}

/// 账户事件流：**一条已认证连接**同时产出订单与账户两类事件（不可拆成两个 trait）。
#[async_trait]
pub trait AccountEventStream: Send + Sync {
    /// 建立（或复用）已认证账户事件连接，返回订单 + 账户事件流。
    /// 实现需自行保活，并在断线后按退避重连、重连后补发一次账户快照信号。
    async fn subscribe_account_events(&self) -> anyhow::Result<BoxStream<'static, AccountEvent>>;
}

/// 公开行情流：交易所主体、无认证、所有用户共享。
#[async_trait]
pub trait MarketDataStream: Send + Sync {
    /// 订阅给定标的的价格/盘口；空列表表示订阅全市场（如币安 !miniTicker、HL allMids）。
    async fn subscribe_prices(
        &self,
        instruments: &[String],
    ) -> anyhow::Result<BoxStream<'static, PriceTick>>;
}

/// 需要完整能力（交易 + 账户事件回报）处使用的组合约束。
pub trait UserVenue: ExchangeApi + AccountEventStream {}
impl<T: ExchangeApi + AccountEventStream> UserVenue for T {}

#[cfg(test)]
mod order_ref_tests {
    use super::OrderRef;
    use trading_models::{Order, Side};

    fn order_with(eid: Option<String>) -> Order {
        let mut o = Order::new(1, "0xabc", "ctp", "rb2510", Side::Buy);
        o.exchange_order_id = eid;
        o
    }

    #[test]
    fn from_order_carries_cid_always() {
        // Arrange / Act：无交易所单号。
        let r = OrderRef::from_order(&order_with(None));
        // Assert：cid 必带，eid 缺省为 None。
        assert_eq!(r.client_order_id, "0xabc");
        assert_eq!(r.exchange_order_id, None);
    }

    #[test]
    fn from_order_includes_exchange_id_when_present() {
        // Arrange / Act：受理后已有交易所单号。
        let r = OrderRef::from_order(&order_with(Some("20260101-7".into())));
        // Assert：cid + eid 一并装入，供适配器内部单号路由。
        assert_eq!(r.client_order_id, "0xabc");
        assert_eq!(r.exchange_order_id.as_deref(), Some("20260101-7"));
    }
}

#[cfg(test)]
mod preflight_tests {
    use super::*;
    use trading_models::Order;

    /// 最小 ExchangeApi 实现：所有异步方法 bail/默认，仅用于验证默认预检行为。
    struct PassThroughVenue;

    #[async_trait]
    impl ExchangeApi for PassThroughVenue {
        fn exchange(&self) -> &str {
            "pass-through"
        }
        async fn place_order(&self, _: &PlaceOrderRequest) -> anyhow::Result<OrderAck> {
            anyhow::bail!("unused")
        }
        async fn cancel_order(&self, _: &str, _: &OrderRef) -> anyhow::Result<OrderAck> {
            anyhow::bail!("unused")
        }
        async fn query_order(&self, _: &str, _: &OrderRef) -> OrderLookup {
            OrderLookup::Unavailable
        }
        async fn get_positions(&self) -> anyhow::Result<Vec<PositionInfo>> {
            Ok(vec![])
        }
        async fn set_leverage(&self, _: &str, _: u32) -> anyhow::Result<()> {
            anyhow::bail!("unused")
        }
        async fn get_reference_price(&self, _: &str) -> anyhow::Result<ReferencePrice> {
            anyhow::bail!("unused")
        }
    }

    fn sample() -> (Order, PlaceOrderRequest) {
        let order = Order::new(1, "0xclient", "deribit", "BTC-1", Side::Buy);
        let request = PlaceOrderRequest {
            instrument: "BTC-1".into(),
            instrument_type: InstrumentType::Option,
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            quantity: Decimal::ONE,
            reduce_only: false,
            position_side: OrderPositionSide::Both,
            time_in_force: None,
            client_order_id: "0xclient".into(),
        };
        (order, request)
    }

    #[test]
    fn default_prepare_passes_through_unchanged() {
        // 默认预检：关键字段原样返回，Binance/xili 不覆盖即零行为变化。
        let (order, request) = sample();
        let out = PassThroughVenue
            .prepare_to_place(order.clone(), request.clone())
            .expect("default prepare passes through");
        // 订单要素未被改写。
        assert_eq!(out.order.client_order_id, order.client_order_id);
        assert_eq!(out.order.instrument, order.instrument);
        assert_eq!(out.order.order_type, order.order_type);
        assert_eq!(out.order.order_quantity, order.order_quantity);
        // 请求未被改写（市价仍是 None 价）。
        assert_eq!(out.request.instrument, request.instrument);
        assert_eq!(out.request.order_type, request.order_type);
        assert_eq!(out.request.price, request.price);
        assert_eq!(out.request.quantity, request.quantity);
    }
}
