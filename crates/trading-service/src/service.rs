//! 南向业务编排：解析命令 -> 更新内存订单 -> spawn 任务调用交易所 REST ->
//! 依据应答更新内存并把 ack/事件路由回对应策略连接。
//!
//! 主路径只做内存操作与任务派发，所有阻塞性 IO（交易所 HTTP）都在独立 task 中；
//! 落盘由独立 write-to-disk 线程负责（见 main）。任务之间只共享 [`AppState`]，
//! 不使用 channel。

use rust_decimal::Decimal;
use tracing::{info, warn};
use trading_core::id::new_client_order_id;
use trading_core::protocol::{normalize_instrument_type, ClientEvent, SouthCommand};
use trading_models::{
    InstrumentType, Order, OrderStatus, OrderType, Side, TimeInForce, WatchConditionKind,
    WatchRuleAction, WatchRuleType,
};

use crate::api::filters::OrderQuery;
use crate::rules::{build_rule, RuleCreateInput};

use crate::state::AppState;
use trading_exchange::api::{OrderRef, PlaceErrorKind, PlaceOrderRequest};

/// 选定客户单号：客户端提供且合法（`0x`+32hex）则沿用（压测/幂等配对用），否则服务端生成。
fn choose_client_order_id(provided: Option<&str>) -> String {
    match provided {
        Some(cid) if Order::is_valid_client_order_id(cid) => cid.to_string(),
        _ => new_client_order_id(),
    }
}

// =====================================================================
// 下单/撤单/建规则共享内核参数（WS 与 REST 共用；纯新增，不改 dispatch 契约）
// =====================================================================

/// 内核前置拒绝原因（携带可选 cid，供 WS 拼 REJECTED 帧、REST 转错误码）。
#[derive(Debug, Clone)]
pub(crate) struct RejectReason {
    pub client_order_id: Option<String>,
    pub message: String,
}

impl RejectReason {
    fn new(message: impl Into<String>) -> Self {
        Self {
            client_order_id: None,
            message: message.into(),
        }
    }
    fn with_cid(mut self, cid: &str) -> Self {
        self.client_order_id = Some(cid.to_string());
        self
    }
}

/// 下单内核入参：字段完整，WS 与 REST 各自从报文映射后填入。
#[derive(Debug, Clone)]
pub(crate) struct PlaceOrderInput {
    pub exchange: String,
    pub account: String,
    pub instrument: String,
    pub instrument_type: InstrumentType,
    pub side: Side,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub price: Decimal,
    pub quantity: Decimal,
    pub order_cash: Decimal,
    pub reduce_only: bool,
    /// 是否显式下发 set_leverage（WS 三态、REST 给值即 true）。
    pub set_leverage_explicit: bool,
    pub leverage: u64,
    pub strategy_name: Option<String>,
    pub signal_id: Option<String>,
    pub client_order_id: String,
}

/// 解析有效期策略（缺省 GTC）。未知值按 GTC（本期不做校验）。
pub(crate) fn parse_time_in_force(raw: Option<&str>) -> TimeInForce {
    match raw
        .map(str::trim)
        .unwrap_or("GTC")
        .to_ascii_uppercase()
        .as_str()
    {
        "GTC" => TimeInForce::Gtc,
        "IOC" => TimeInForce::Ioc,
        "FOK" => TimeInForce::Fok,
        "DAY" => TimeInForce::Day,
        _ => TimeInForce::Gtc,
    }
}

/// 撤单内核入参。
#[derive(Debug, Clone)]
pub(crate) struct CancelOrderInput {
    pub client_order_id: String,
    /// 账户/所：撤单需要凭证；WS/REST 可给（或从内存订单回查）。
    pub account: Option<String>,
    pub exchange: Option<String>,
}

/// 内核事件回调出口：决定后台受理/失败事件发往何处。
///
/// * WS 路径：`ToStrategy`——沿用原 send_event 推给策略连接；
/// * REST 路径：`Ignore`——后台事件不推送（文档规定靠轮询/WS 路由）。
#[derive(Debug, Clone)]
pub(crate) enum Callback {
    /// 发往该策略身份的南向连接（含原日志）。
    ToStrategy(String),
    /// 不发送后台事件。
    Ignore,
}

impl Callback {
    /// 需要发送时返回目标策略名。
    fn strategy(&self) -> Option<&str> {
        match self {
            Callback::ToStrategy(s) => Some(s),
            Callback::Ignore => None,
        }
    }
}

/// 下单内核失败：普通前置拒绝，或在途信号撞车（WS 静默忽略）。
#[derive(Debug)]
pub(crate) enum OrderCoreError {
    /// 前置/调用阶段拒绝（拼 REJECTED 帧或 REST 错误码）。
    Reject(RejectReason),
    /// 插入时同 (策略,signal) 被并发抢占：WS 静默，REST 不会触发。
    DuplicateSignal(String),
}

impl From<RejectReason> for OrderCoreError {
    fn from(r: RejectReason) -> Self {
        OrderCoreError::Reject(r)
    }
}

/// 下单共享内核：解析账户/标的 → 取量 → 内存建 PLACING 单并立即返回 →
/// spawn 任务调交易所 REST，受理/失败事件按 [`Callback`] 决定去向。
///
/// WS 与 REST 同走此处，逻辑逐字唯一。WS 专属的 signal 必填/在途判重由
/// 调用方在进入内核前完成。
pub(crate) async fn submit_order_core(
    state: AppState,
    input: PlaceOrderInput,
    callback: Callback,
) -> Result<Order, OrderCoreError> {
    let PlaceOrderInput {
        exchange,
        account,
        instrument: raw_instrument,
        instrument_type,
        side,
        order_type,
        time_in_force,
        price,
        quantity: raw_quantity,
        order_cash,
        reduce_only,
        set_leverage_explicit,
        leverage,
        strategy_name,
        signal_id,
        client_order_id,
    } = input;

    // 交易所名只 trim，保持精确名（模拟所名 --exchange 同理）。
    let venue = resolve_venue(&exchange);
    // 任一所都必须已注册且有精确匹配账户，否则前置拒绝。
    let trading_account = state
        .resolve_trading_account(&venue, &account)
        .map_err(RejectReason::new)?;

    // 入站符号按归属所归一（binance→大写）。
    let instrument = state
        .exchanges
        .normalize_instrument(&venue, &raw_instrument);
    if !matches!(order_type, OrderType::Market) && price <= Decimal::ZERO {
        return Err(RejectReason::new("limit 单 price 必须为正数").into());
    }

    let orders = state.store.order_repo().expect("order repo after recover");

    // 取量二选一：quantity 优先；否则 order_cash ÷ 参考价换算。
    let quantity = if raw_quantity > Decimal::ZERO {
        raw_quantity
    } else if order_cash > Decimal::ZERO {
        let limit_price = (!matches!(order_type, OrderType::Market)).then_some(price);
        let Some(ref_price) = crate::cashqty::reference_price(
            &state.store.quotes(),
            &venue,
            instrument_type,
            &instrument,
            side,
            order_type,
            limit_price,
        ) else {
            return Err(RejectReason::new(
                "order_cash 换算失败：缺少可用参考价（行情未就绪或未接入该交易所）",
            )
            .into());
        };
        let Some(q) = crate::cashqty::cash_to_quantity(order_cash, ref_price) else {
            return Err(RejectReason::new("order_cash 换算出的数量非正").into());
        };
        q
    } else {
        return Err(RejectReason::new("必须传正数 quantity 或正数 order_cash").into());
    };

    // 建单前按交易规则步长对齐数量（领域层收口），使订单行数量 = 实际委托量
    // = 通知数量。规则未就绪（启动同步窗口/测试未 seed）时**容错透传**：不改
    // 同步建单契约，最终对齐/校验仍由 venue 在实际下单时兜底。
    let quantity = {
        let rule_key = trading_models::OrderRuleKey::new(&venue, instrument_type, &instrument);
        match state.store.order_rules().get(&rule_key) {
            Some(qrow) => trading_core::order_rule::align_quantity(&qrow, order_type, quantity)
                .map_err(|e| RejectReason::new(format!("数量对齐失败 {instrument}: {e:?}")))?,
            // 规则缺失：透传原量，交给 venue 兜底（保持原同步 PLACING 行为）。
            None => quantity,
        }
    };

    // —— 内存建单（PLACING；id/时间戳由框架表分配）——
    let mut order = Order::new(
        trading_account.id,
        client_order_id.clone(),
        venue.clone(),
        instrument.clone(),
        side,
    );
    order.strategy_name = strategy_name;
    order.signal_id = signal_id;
    order.order_cash = order_cash;
    order.instrument_type = instrument_type;
    // 测试网是账户属性：随该下单账户。
    order.test_net = trading_account.is_testnet;
    order.order_type = order_type;
    order.time_in_force = time_in_force;
    order.order_price = if matches!(order_type, OrderType::Market) {
        Decimal::ZERO
    } else {
        price
    };
    order.order_quantity = quantity;
    order.reduce_only = reduce_only;
    order.leverage = leverage;
    order.status = OrderStatus::Placing;
    let order = match orders.insert(order) {
        Ok(stored) => stored,
        Err(trading_store::OrderRepoError::DuplicateSignal(sig)) => {
            return Err(OrderCoreError::DuplicateSignal(sig))
        }
        Err(e) => {
            return Err(RejectReason::new(format!("建单失败: {e}"))
                .with_cid(&client_order_id)
                .into())
        }
    };
    info!(%client_order_id, strategy = ?callback.strategy(), venue = %venue, instrument = %instrument, instrument_type = ?order.instrument_type, test_net = order.test_net, "order created in memory (PLACING)");
    // 返回给调用方的同步结果（PLACING）；spawn 任务另持一份。
    let returned = order.clone();

    // 统一下单请求。
    let req = PlaceOrderRequest {
        instrument: instrument.clone(),
        instrument_type: order.instrument_type,
        side,
        order_type,
        price: if matches!(order_type, OrderType::Market) {
            None
        } else {
            Some(price)
        },
        quantity,
        reduce_only: order.reduce_only,
        // positionSide 统一收口：权威实现在 binance::prep（单向=BOTH，hedge
        // 平仓锁定被平持仓而非反向开仓）。
        position_side: trading_exchange::binance::prep::order_position_side(
            trading_account.position_mode,
            side,
            order.reduce_only,
        ),
        time_in_force: Some(order.time_in_force),
        client_order_id: client_order_id.clone(),
    };
    // connect 用的标的类型提示（归一小写 wire）。
    let connect_type = connect_hint(instrument_type);

    // —— spawn 任务：REST 下单，受理后按 callback 决定是否回调 ——
    tokio::spawn(async move {
        let adapter = match state
            .exchanges
            .connect(&venue, Some(connect_type), &account)
        {
            Ok(a) => a,
            Err(e) => {
                warn!(%client_order_id, error = %e, "cannot reach exchange");
                if let Err(re) = orders.apply_rejection(&client_order_id) {
                    warn!(%client_order_id, error = %re, "apply_rejection failed");
                }
                emit(&state, &callback, |strategy| {
                    let ev = ClientEvent::error(
                        format!("无法连接交易所: {e}"),
                        Some(strategy.to_string()),
                    );
                    with_cid(ev, &client_order_id)
                })
                .await;
                return;
            }
        };

        // 期货开仓、且显式带杠杆时：读对账缓存的杠杆，命中且相等则**整次跳过**
        // （省权重）；缓存缺失/不一致才 set_leverage。平仓单不碰杠杆。
        if matches!(order.instrument_type, InstrumentType::Futures) && set_leverage_explicit {
            let cached = state
                .store
                .leverage_repo()
                .get_for(order.account_id, &venue, &instrument);
            let need_set = trading_store::should_set_leverage(reduce_only, cached, leverage as u32);
            if need_set {
                if let Err(e) = adapter.set_leverage(&instrument, leverage as u32).await {
                    warn!(%client_order_id, leverage, error = %e, "set_leverage failed");
                    if let Err(re) = orders.apply_rejection(&client_order_id) {
                        warn!(%client_order_id, error = %re, "apply_rejection failed");
                    }
                    emit(&state, &callback, |strategy| {
                        let ev = ClientEvent::error(
                            format!("设置杠杆失败: {e}"),
                            Some(strategy.to_string()),
                        );
                        with_cid(ev, &client_order_id)
                    })
                    .await;
                    return;
                }
            }
        }

        match adapter.place_order(&req).await {
            Ok(ack) => {
                let acknowledged = orders
                    .apply_ack(&client_order_id, ack.exchange_order_id, OrderStatus::New)
                    .unwrap_or_else(|_| order.clone());
                // 适配器把市价开仓转成限价（如 deribit 漂移）时，回写真实委托类型/价/量；
                // 无回显（binance 原生市价）则原样保留。
                let updated = orders
                    .apply_placed(
                        &client_order_id,
                        ack.placed_type,
                        ack.placed_price,
                        ack.placed_quantity,
                    )
                    .unwrap_or(acknowledged);
                info!(%client_order_id, exchange_order_id = ?updated.exchange_order_id, "exchange accepted (NEW)");
                emit(&state, &callback, |_| {
                    ClientEvent::order_snapshot("ack", OrderStatus::New.wire(), &updated)
                })
                .await;
            }
            Err(e) => {
                // 按错误性质分流：交易所明确业务拒绝（HTTP400）→ REJECTED 终态；
                // 超时/网络等应答不确定 → UNKNOWN，等回报或 REST 查单收敛。
                match adapter.classify_place_error(&e) {
                    PlaceErrorKind::Rejected => {
                        warn!(%client_order_id, error = %e, "place_order rejected by exchange");
                        // 终态软删由 OrderRepo 内部收口，调用方不碰 deleted。
                        let _ = orders.apply_rejection(&client_order_id);
                        emit(&state, &callback, |strategy| {
                            let ev = ClientEvent::error(
                                format!("交易所拒绝下单: {e}"),
                                Some(strategy.to_string()),
                            );
                            with_cid(ev, &client_order_id)
                        })
                        .await;
                    }
                    PlaceErrorKind::Indeterminate => {
                        warn!(%client_order_id, error = %e, "place_order indeterminate");
                        let updated = orders.mark_unknown(&client_order_id).unwrap_or(order);
                        emit(&state, &callback, |_| {
                            let mut ev = ClientEvent::order_snapshot(
                                "ack",
                                OrderStatus::Unknown.wire(),
                                &updated,
                            );
                            ev.message = Some(format!("交易所调用失败: {e}"));
                            ev
                        })
                        .await;
                    }
                }
            }
        }
    });

    Ok(returned)
}

/// connect 的标的类型提示（归一小写 wire 串）。
/// 生成服务端 client_order_id（REST 一律服务端生成）。
pub(crate) fn new_client_id() -> String {
    new_client_order_id()
}

fn connect_hint(t: InstrumentType) -> &'static str {
    match t {
        InstrumentType::Futures | InstrumentType::Perpetual => "futures",
        InstrumentType::Spot => "spot",
        InstrumentType::Option => "option",
        InstrumentType::Equity => "equity",
        InstrumentType::Fund => "fund",
    }
}

/// 仅当 callback 需要发送时，构造 [`ClientEvent`] 并按策略发出；Ignore 时无操作。
async fn emit<F>(state: &AppState, callback: &Callback, build: F)
where
    F: FnOnce(&str) -> ClientEvent,
{
    let Some(strategy) = callback.strategy() else {
        return;
    };
    send_event(state, strategy, &build(strategy)).await;
}

// =====================================================================
// 撤单共享内核
// =====================================================================

/// 撤单内核失败：缺 cid、或内存查无此单（REST 转 40004）。
#[derive(Debug)]
pub(crate) enum CancelCoreError {
    /// 缺 cid 等请求问题。
    Reject(RejectReason),
    /// 内存中无此 cid 订单。
    NotFound(String),
}

impl From<RejectReason> for CancelCoreError {
    fn from(r: RejectReason) -> Self {
        CancelCoreError::Reject(r)
    }
}

/// 撤单共享内核：解析内存订单（账户/所未给则回查）→ spawn 调交易所撤单，
/// 受理/失败事件按 [`Callback`] 决定去向。同步返回当前订单。
pub(crate) async fn submit_cancel_core(
    state: AppState,
    input: CancelOrderInput,
    callback: Callback,
) -> Result<Order, CancelCoreError> {
    if input.client_order_id.trim().is_empty() {
        return Err(RejectReason::new("cancel_order 缺少 client_order_id").into());
    }
    let orders = state.store.order_repo().expect("order repo after recover");
    let client_order_id = input.client_order_id.trim().to_string();

    // 内存订单：账户/所/标的以内存行为权威（入参未给则回查）。
    let order = orders
        .get_by_cid(&client_order_id)
        .ok_or_else(|| CancelCoreError::NotFound(client_order_id.clone()))?;
    let exchange = input.exchange.unwrap_or(order.exchange.clone());
    let instrument = order.instrument.clone();
    let connect_type = connect_hint(order.instrument_type);
    let account = input.account.unwrap_or_else(|| "default".to_string());
    // 撤单统一以客户单号 cid 为准（调用方只传 cid 即可撤）；cid 必然存在。
    // 交易所单号仅作适配器内部首选失败时的兜底（各 venue 路由自己决定）。
    let mut order_ref = OrderRef::new(client_order_id.clone());
    if let Some(exch_id) = order.exchange_order_id.clone() {
        order_ref = order_ref.with_exchange(exch_id);
    }
    let returned = order.clone();

    tokio::spawn(async move {
        match state
            .exchanges
            .connect(&exchange, Some(connect_type), &account)
        {
            Ok(adapter) => match adapter.cancel_order(&instrument, &order_ref).await {
                Ok(ack) => {
                    let updated = orders.apply_canceled(&client_order_id).unwrap_or(order);
                    info!(%client_order_id, exchange_order_id = ?ack.exchange_order_id, "cancel accepted");
                    emit(&state, &callback, |_| {
                        ClientEvent::order_snapshot("ack", OrderStatus::Canceled.wire(), &updated)
                    })
                    .await;
                }
                Err(e) => {
                    warn!(%client_order_id, error = %e, "cancel_order failed");
                    emit(&state, &callback, |strategy| {
                        let mut ev = ClientEvent::error(
                            format!("撤单失败: {e}"),
                            Some(strategy.to_string()),
                        );
                        ev.client_order_id = Some(client_order_id.clone());
                        ev
                    })
                    .await;
                }
            },
            Err(e) => {
                emit(&state, &callback, |strategy| {
                    let ev = ClientEvent::error(
                        format!("无法连接交易所: {e}"),
                        Some(strategy.to_string()),
                    );
                    with_cid(ev, &client_order_id)
                })
                .await;
            }
        }
    });

    Ok(returned)
}

/// 策略显式指定 venue：交易所名全局唯一，直接使用其传入的精确名，不做任何
/// 别名/大小写归一（仅 trim 入站空白）。合约/现货由 instrument_type 区分，不靠
/// 交易所名；配置的本地模拟所名（xili，见 --exchange）同样原样使用。
fn resolve_venue(exchange: &str) -> String {
    exchange.trim().to_string()
}

/// 标的类型落库：futures→合约、option→期权，其余（含缺省）→现货。
fn instrument_type_of(raw: Option<&str>) -> InstrumentType {
    match normalize_instrument_type(raw).as_str() {
        "futures" => InstrumentType::Futures,
        "option" => InstrumentType::Option,
        _ => InstrumentType::Spot,
    }
}

/// 分发一条已通过连接层解析的南向命令。`strategy` 为该连接绑定的策略身份。
pub async fn dispatch(state: AppState, strategy: String, cmd: SouthCommand) {
    match cmd.option_kind().as_str() {
        "place_order" | "place" => place_order(state, strategy, cmd).await,
        "cancel_order" | "cancel" => cancel_order(state, strategy, cmd).await,
        "get_positions" => get_positions(state, strategy, cmd).await,
        "get_order" => get_order(state, strategy, cmd).await,
        "create_rule" | "create_watch_rule" => create_rule(state, strategy, cmd).await,
        other => {
            warn!(option = %other, "unknown south command");
            let ev = ClientEvent::error(format!("未知命令 `{other}`"), Some(strategy.clone()));
            send_event(&state, &strategy, &ev).await;
        }
    }
}

async fn place_order(state: AppState, strategy: String, cmd: SouthCommand) {
    // —— WS 专属前置（dispatch 契约不变）：缺字段直接 REJECTED 回调 ——
    let exchange = match cmd.exchange() {
        Some(e) => e.to_string(),
        None => return reject(&state, &strategy, None, None, "缺少 exchange").await,
    };
    let account = cmd.account.clone().unwrap_or_else(|| "default".to_string());
    let instrument = match cmd.instrument() {
        Some(s) => s.to_string(),
        None => return reject(&state, &strategy, None, None, "缺少 instrument/symbol").await,
    };
    let side: Side = match cmd.side() {
        Some(s) => s,
        None => return reject(&state, &strategy, None, None, "side 非法（long/short）").await,
    };
    let order_type: OrderType = cmd.order_type();
    let instrument_type = instrument_type_of(cmd.instrument_type.as_deref());
    let price = cmd.price.unwrap_or(Decimal::ZERO);

    // WS 下单必带 signal_id；同策略在途信号重复直接忽略（不下单、不回推）。
    let orders = state.store.order_repo().expect("order repo after recover");
    let signal_id = match cmd
        .signal_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(s) => s.to_string(),
        None => return reject(&state, &strategy, None, None, "缺少 signal_id").await,
    };
    if orders.get_active_by_signal(&strategy, &signal_id).is_some() {
        warn!(%signal_id, strategy = %strategy, "duplicate in-flight signal_id, command ignored");
        return;
    }

    // cid：WS 沿用合法自带值（压测），否则服务端生成。
    let client_order_id = choose_client_order_id(cmd.client_order_id.as_deref());
    let input = PlaceOrderInput {
        exchange,
        account,
        instrument,
        instrument_type,
        side,
        order_type,
        // WS SouthCommand 无 tif 字段：保持原行为默认 GTC。
        time_in_force: TimeInForce::Gtc,
        price,
        quantity: cmd.quantity.unwrap_or(Decimal::ZERO),
        order_cash: cmd.order_cash.unwrap_or(Decimal::ZERO),
        reduce_only: cmd.reduce_only,
        set_leverage_explicit: cmd.leverage.is_some(),
        leverage: cmd.leverage.unwrap_or(1),
        strategy_name: Some(strategy.clone()),
        signal_id: Some(signal_id.clone()),
        client_order_id,
    };

    // 共享内核；后台事件发回本策略连接。
    match submit_order_core(state.clone(), input, Callback::ToStrategy(strategy.clone())).await {
        Ok(_) => {}
        Err(OrderCoreError::Reject(reason)) => {
            reject(
                &state,
                &strategy,
                reason.client_order_id.as_deref(),
                Some(&signal_id),
                &reason.message,
            )
            .await;
        }
        Err(OrderCoreError::DuplicateSignal(sig)) => {
            warn!(signal_id = %sig, strategy = %strategy, "duplicate signal lost race, command ignored");
        }
    }
}

/// 解析条件主类 wire 串。
fn parse_condition_kind(raw: &str) -> Option<WatchConditionKind> {
    Some(match raw.trim().to_ascii_lowercase().as_str() {
        "roi" => WatchConditionKind::Roi,
        "price" => WatchConditionKind::Price,
        "position" => WatchConditionKind::Position,
        "profit_drawdown" | "drawdown" => WatchConditionKind::ProfitDrawdown,
        _ => return None,
    })
}

/// 解析规则类型 wire 串（缺省 risk）。
fn parse_rule_type(raw: Option<&str>) -> Result<WatchRuleType, String> {
    Ok(
        match raw.unwrap_or("risk").trim().to_ascii_lowercase().as_str() {
            "risk" => WatchRuleType::Risk,
            "new" => WatchRuleType::New,
            other => return Err(format!("rule_type `{other}` 非法（允许 new/risk）")),
        },
    )
}

/// create_rule：策略端按「账户 + 交易所 + 标的 + 方向」创建盯盘规则，全程不接触
/// position_id/rule_id（内部按内存活跃仓位解析绑定；链式规则暂不开放远程创建）。
async fn create_rule(state: AppState, strategy: String, cmd: SouthCommand) {
    // `Result<T,String>` 失败统一回 error 帧，消除重复的 match+return 样板。
    macro_rules! try_field {
        ($e:expr) => {
            match $e {
                Ok(v) => v,
                Err(reason) => return rule_error(&state, &strategy, &reason).await,
            }
        };
        ($e:expr, $msg:expr) => {
            match $e {
                Some(v) => v,
                None => return rule_error(&state, &strategy, $msg).await,
            }
        };
    }

    // —— 账户解析（与 place_order 同口径，账户必须在 account.csv 且交易所匹配）——
    let exchange = try_field!(cmd.exchange().map(str::to_string), "缺少 exchange");
    let account = cmd.account.clone().unwrap_or_else(|| "default".to_string());
    let venue = resolve_venue(&exchange);
    let trading_account = try_field!(state.resolve_trading_account(&venue, &account));
    let rule_type = try_field!(parse_rule_type(cmd.rule_type.as_deref()));

    // 标的/方向随规则类型区分：
    // * new 开仓：必须给 instrument + side（开仓方向），position_id 固定 0；
    // * risk 平仓：不使用 instrument/side，按 cmd.position_id 绑定活跃仓位，
    //   命中时交易所/标的/合约形状/平仓方向全部从该仓位派生。
    let (instrument, open_side, position_id) = match rule_type {
        WatchRuleType::New => {
            let instrument_raw = try_field!(cmd.instrument(), "缺少 instrument/symbol");
            let instrument = state.exchanges.normalize_instrument(&venue, instrument_raw);
            let side = try_field!(cmd.side(), "side 非法（long/short）");
            (instrument, Some(side), 0u64)
        }
        WatchRuleType::Risk => {
            // risk 规则行的下单要素以绑定仓位为准，instrument 占位不消费。
            let instrument = cmd
                .instrument()
                .map(|s| state.exchanges.normalize_instrument(&venue, s))
                .unwrap_or_default();
            (instrument, None, cmd.position_id.unwrap_or(0))
        }
    };
    let condition_kind = try_field!(
        parse_condition_kind(cmd.condition_kind.as_deref().unwrap_or("roi")),
        "condition_kind 非法（允许 roi/price/position/profit_drawdown）"
    );
    let condition_instrument = cmd
        .condition_instrument
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        // 跨标的条件（如 FILUSDT）同样按归属所归一大小写。
        .map(|s| state.exchanges.normalize_instrument(&venue, s));
    let operator = try_field!(
        cmd.operator
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        "缺少 operator（>= <= > < ==）"
    );
    let value = try_field!(cmd.value, "缺少 value 阈值");
    let action = match cmd
        .action
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        None | Some("self") => WatchRuleAction::UseSelf,
        Some(other) => try_field!(
            WatchRuleAction::parse_wire(other),
            "action 非法（支持 self 或 rule:<目标规则id>）"
        ),
    };

    // 仓位/规则仓储：进程内单例，从统一存储取（建规则的仓位存在性校验与落库）。
    let positions = state
        .store
        .position_repo()
        .expect("position repo after recover");
    let watch_rules = state.store.rule_repo().expect("rule repo after recover");

    let input = RuleCreateInput {
        account_id: trading_account.id,
        rule_type,
        position_id,
        // venue 已是全局精确名，直接作为规则存储口径（无别名归一）。
        exchange: venue,
        instrument,
        instrument_type: instrument_type_of(cmd.instrument_type.as_deref()),
        open_side,
        condition_kind,
        condition_instrument,
        operator,
        value,
        action,
        order_type: cmd.order_type(),
        quantity: cmd.quantity.unwrap_or(Decimal::ZERO),
        // new 规则现金口径：与 quantity 二选一，命中时换算数量。
        order_cash: cmd.order_cash.unwrap_or(Decimal::ZERO),
        // 缺省 0：纯构造层把 0 解释为「全平 1」，避免协议层预设业务默认。
        quantity_pct: cmd.quantity_pct.unwrap_or(Decimal::ZERO),
        leverage: cmd.leverage.unwrap_or(1),
        price: cmd.price.unwrap_or(Decimal::ZERO),
        pending: cmd.pending,
    };

    // —— 纯校验/构造（仓位存在性在此判定）→ 落规则表（状态机外的普通插入）——
    let rule = match build_rule(&input, &positions) {
        Ok(r) => r,
        Err(reason) => return rule_error(&state, &strategy, &reason.to_string()).await,
    };
    // 链式动作：目标规则必须存在（未软删）且为 pending，等待本规则激活。
    if let Some(WatchRuleAction::ActivateRule { rule_id: target }) = rule.parsed_action() {
        let target_error = match watch_rules.get(target) {
            Some(t) if t.status == trading_models::WatchRuleStatus::Pending => None,
            Some(t) => Some(format!(
                "链式目标规则 {target} 不是 pending（当前 {:?}）",
                t.status
            )),
            None => Some(format!("链式目标规则 {target} 不存在")),
        };
        if let Some(reason) = target_error {
            return rule_error(&state, &strategy, &reason).await;
        }
    }
    match watch_rules.insert(rule) {
        Ok(stored) => {
            info!(
                rule_id = stored.id,
                rule_type = ?stored.rule_type,
                position_id = stored.position_id,
                account = %account,
                strategy = %strategy,
                condition = %stored.condition_name(),
                "watch rule created"
            );
            let type_wire = if matches!(stored.rule_type, WatchRuleType::New) {
                "new"
            } else {
                "risk"
            };
            let ev = ClientEvent::ack(
                format!("规则创建成功（{} {}）", type_wire, stored.condition_name()),
                Some(strategy.clone()),
            );
            send_event(&state, &strategy, &ev).await;
        }
        Err(e) => rule_error(&state, &strategy, &format!("规则落库失败: {e}")).await,
    }
}

/// create_rule 失败统一回 error 帧（不带任何 id）。
async fn rule_error(state: &AppState, strategy: &str, msg: &str) {
    warn!(strategy = %strategy, msg, "create_rule rejected");
    let ev = ClientEvent::error(msg.to_string(), Some(strategy.to_string()));
    send_event(state, strategy, &ev).await;
}

/// get_order：查询单笔订单，返回与下单回报同形的快照。
///
/// 支持两个等价的键：`client_order_id` 或 `signal_id`（后者配合握手策略身份）。
/// 业务查询（活动内存 → 当日 tombstone → 历史归档 CSV）统一走
/// `crate::order_query::query_orders`，与 REST `GET /orders` 共用；本函数只做
/// WS 入参校验与应答封装。
async fn get_order(state: AppState, strategy: String, cmd: SouthCommand) {
    let exchange = match cmd.exchange() {
        Some(e) => e.to_string(),
        None => return reject(&state, &strategy, None, None, "缺少 exchange").await,
    };
    let account = match cmd
        .account
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(a) => a.to_string(),
        None => return reject(&state, &strategy, None, None, "缺少 account").await,
    };
    let cid = cmd
        .client_order_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let signal_id = cmd
        .signal_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    // cid 与 signal_id 至少给一个。
    if cid.is_none() && signal_id.is_none() {
        return reject(
            &state,
            &strategy,
            cid.as_deref(),
            None,
            "缺少 client_order_id 或 signal_id",
        )
        .await;
    }

    // 映射为统一查询参数；strategy_name 填握手身份，core 据此限定只能查到本策略的单
    // （越权/查无结果一致，不泄露订单存在性）。账户/所解析也由 core 统一负责。
    let q = OrderQuery {
        account: Some(account),
        exchange: Some(exchange),
        client_order_id: cid,
        signal_id,
        strategy_name: Some(strategy.clone()),
        ..Default::default()
    };
    let order = match crate::order_query::query_orders(&state, q).await {
        // core 报错（账户/所无法解析）→ 带回错误原因。
        Err(e) => return reject(&state, &strategy, None, None, &e.0).await,
        Ok(rows) => rows.into_iter().next(),
    };
    let Some(order) = order else {
        return reject(&state, &strategy, None, None, "订单不存在").await;
    };

    let ev = ClientEvent::order_snapshot("order", order.status.wire(), &order);
    send_event(&state, &strategy, &ev).await;
}

async fn cancel_order(state: AppState, strategy: String, cmd: SouthCommand) {
    let input = CancelOrderInput {
        client_order_id: cmd
            .client_order_id
            .as_deref()
            .map(str::trim)
            .unwrap_or("")
            .to_string(),
        account: cmd.account.clone(),
        exchange: None,
    };
    // 共享内核；后台撤单事件发回本策略连接。
    match submit_cancel_core(state.clone(), input, Callback::ToStrategy(strategy.clone())).await {
        Ok(_) => {}
        Err(CancelCoreError::Reject(reason)) => {
            reject(
                &state,
                &strategy,
                reason.client_order_id.as_deref(),
                None,
                &reason.message,
            )
            .await;
        }
        Err(CancelCoreError::NotFound(cid)) => {
            reject(&state, &strategy, Some(&cid), None, "订单不存在").await;
        }
    }
}

/// WS 获取仓位：**只读本地仓位表**（不调交易所 API），与 REST `GET /positions`
/// 走同一个 [`crate::position_query::query_positions`]。参数全部可选——传了筛选、
/// 不传不处理；返回元素字段与 REST `PositionDTO` 完全一致，仅外层帧封装不同。
async fn get_positions(state: AppState, strategy: String, cmd: SouthCommand) {
    // SouthCommand -> 中性 PositionQuery（不传的字段保持 None=不筛选）。
    // account 是账户名；先取借用/拷贝字段、最后再 move 它，避免部分移动后再借用。
    let query = crate::api::filters::PositionQuery {
        account_id: None,
        exchange: cmd.exchange().map(str::to_string),
        instrument: cmd.instrument().map(str::to_string),
        instrument_type: cmd.instrument_type,
        side: cmd.side,
        min_roi: cmd.min_roi,
        max_roi: cmd.max_roi,
        deleted: cmd.deleted,
        account: cmd.account,
        page: None,
        page_size: None,
    };

    let positions = match crate::position_query::query_positions(&state, query).await {
        Ok(rows) => rows,
        // 账户/所无法解析、参数非法：统一回 error 帧（WS 不细分错误码）。
        Err(e) => {
            let ev = ClientEvent::error(e.message().to_string(), Some(strategy.clone()));
            return send_event(&state, &strategy, &ev).await;
        }
    };

    // 业务载荷复用 REST 投影（完整字段），外层包成 WS positions 帧。
    let dtos: Vec<serde_json::Value> = positions
        .iter()
        .map(crate::api::dto::position::PositionDTO::from)
        .map(serde_json::to_value)
        .map(|r| r.unwrap_or(serde_json::Value::Null))
        .collect();
    let frame = serde_json::json!({
        "type": "positions",
        "status": "OK",
        "positions": dtos,
    });
    send_value(&state, &strategy, &frame).await;
}

async fn reject(
    state: &AppState,
    strategy: &str,
    client_order_id: Option<&str>,
    signal_id: Option<&str>,
    msg: &str,
) {
    warn!(strategy = %strategy, client_order_id, signal_id, msg, "order rejected before placement");
    let mut ev = ClientEvent::error(msg.to_string(), Some(strategy.to_string()));
    ev.client_order_id = client_order_id.map(str::to_string);
    // 回传 signal_id，便于策略侧把被拒回报关联回原信号。
    ev.signal_id = signal_id.map(str::to_string);
    send_event(state, strategy, &ev).await;
}

fn with_cid(mut ev: ClientEvent, cid: &str) -> ClientEvent {
    ev.client_order_id = Some(cid.to_string());
    ev
}

/// 序列化事件并路由到策略连接；无连接时仅记录（策略可能已断线）。
async fn send_event(state: &AppState, strategy: &str, ev: &ClientEvent) {
    let value = match serde_json::to_value(ev) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "serialize client event failed");
            return;
        }
    };
    send_value(state, strategy, &value).await
}

/// 把已构造好的 JSON 帧路由到策略连接（positions 等非 ClientEvent 强类型帧复用）。
async fn send_value(state: &AppState, strategy: &str, value: &serde_json::Value) {
    if !state.hub.send(strategy, value).await {
        warn!(strategy = %strategy, event = ?value, "no live strategy connection, event dropped");
    } else {
        info!(strategy = %strategy, event = ?value, "callback sent to strategy");
    }
}

#[cfg(test)]
mod tests {
    use super::{
        choose_client_order_id, new_client_id, submit_order_core, Callback, PlaceOrderInput,
    };
    use rust_decimal::Decimal;
    use std::str::FromStr;
    use trading_models::{InstrumentType, Order, OrderStatus, OrderType, Side, TimeInForce};

    #[test]
    fn reuses_valid_client_supplied_cid() {
        let cid = format!("0x{}", "a".repeat(32));
        assert!(Order::is_valid_client_order_id(&cid));
        assert_eq!(choose_client_order_id(Some(&cid)), cid);
    }

    /// 南向标的类型映射：futures/option 各自命中，别名归一，其余（含缺省）为现货。
    #[test]
    fn instrument_type_maps_futures_option_and_default_spot() {
        use super::instrument_type_of;
        // 合约：本名与常见别名。
        assert_eq!(instrument_type_of(Some("futures")), InstrumentType::Futures);
        assert_eq!(
            instrument_type_of(Some("PERPETUAL")),
            InstrumentType::Futures
        );
        // 期权：本名与复数别名（此前漏配，会被错误归为现货）。
        assert_eq!(instrument_type_of(Some("option")), InstrumentType::Option);
        assert_eq!(instrument_type_of(Some("OPTIONS")), InstrumentType::Option);
        // 现货：未知值与缺省。
        assert_eq!(instrument_type_of(Some("spot")), InstrumentType::Spot);
        assert_eq!(instrument_type_of(None), InstrumentType::Spot);
    }

    /// 构造一条币安 USDM 永续规则行（step=0.001，tick=0.1，名义额下限 5）。
    fn binance_rule() -> trading_models::OrderRule {
        use rust_decimal::Decimal;
        use trading_models::{OrderRule, QtyUnit};
        let d = |s: &str| Decimal::from_str(s).unwrap();
        let mut r = OrderRule::new(
            "binance",
            "BTCUSDT",
            InstrumentType::Futures,
            QtyUnit::Base,
            Decimal::ONE,
            "USDT",
            "USDT",
            d("0.001"),
            "test",
            0,
        );
        r.price_tick = Some(d("0.1"));
        r.qty_min = Some(d("0.001"));
        r.qty_max = Some(d("1000"));
        r.notional_min = Some(d("5"));
        r
    }

    /// 阶段A：建单前数量须按步长对齐——市价单 0.00245534 → 0.002，
    /// 使订单行数量（=通知数量）等于实际可委托量。
    #[tokio::test]
    async fn submit_core_aligns_quantity_to_lot_step_before_insert() {
        // Arrange：起带 binance 账户的应用，并在规则表放入合成交易规则。
        let app =
            crate::api::test_support::app_with_accounts("0,tb,binance,K,s,,one_way,0,0,0,0\n")
                .await;
        app.state.store.order_rules().put(binance_rule());

        let d = |s: &str| rust_decimal::Decimal::from_str(s).unwrap();
        let input = PlaceOrderInput {
            exchange: "binance".into(),
            account: "tb".into(),
            instrument: "BTCUSDT".into(),
            instrument_type: InstrumentType::Futures,
            side: Side::Buy,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::Gtc,
            price: Decimal::ZERO,
            // 现金换算出的未对齐量。
            quantity: d("0.00245534"),
            order_cash: Decimal::ZERO,
            reduce_only: false,
            set_leverage_explicit: false,
            leverage: 1,
            strategy_name: None,
            signal_id: None,
            client_order_id: new_client_id(),
        };

        // Act
        let order = submit_order_core(app.state.clone(), input, Callback::Ignore)
            .await
            .unwrap();

        // Assert：订单行数量已对齐到 0.002（与实际仓位/通知一致）。
        assert_eq!(order.order_quantity, d("0.002"));
    }

    // ---- get_order：南向 WS 查单（活动/tombstone/历史/缺参/越权）----

    /// 连上策略身份 s1 的南向 WS（dev 旁路：只带 strategy_name 即可）。
    async fn connect_south(
        url: &str,
        strategy: &str,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        let ws_url = format!("{url}?strategy_name={strategy}");
        tokio_tungstenite::connect_async(&ws_url).await.unwrap().0
    }

    /// 发一条 get_order 命令并读回一帧应答。
    async fn send_get_order(
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        body: &str,
    ) -> serde_json::Value {
        use futures_util::{SinkExt, StreamExt};
        let cmd = format!(r#"{{"option":"get_order","exchange":"binance",{body}}}"#);
        ws.send(tokio_tungstenite::tungstenite::Message::Text(cmd.into()))
            .await
            .unwrap();
        let msg = ws.next().await.unwrap().unwrap();
        serde_json::from_str(msg.to_text().unwrap()).unwrap()
    }

    /// 装配一条挂在指定账户/策略下的订单（可指定终态/删除）。
    /// account_id 必须传该测试 app 解析出的真实账户 id（core 按账户归属过滤）。
    fn strategy_order(account_id: u64, cid: &str, strategy: &str, terminal: bool) -> Order {
        let mut o = Order::new(account_id, cid, "binance", "BTCUSDT", Side::Buy);
        o.strategy_name = Some(strategy.to_string());
        if terminal {
            o.status = OrderStatus::Filled;
            o.deleted = true;
        }
        o
    }

    #[tokio::test]
    async fn get_order_returns_active_order() {
        let app =
            crate::api::test_support::app_with_accounts("1,alice,binance,K,s,,one_way,0,0,0,0\n")
                .await;
        let url = crate::api::test_support::serve_south_ws(&app.state).await;
        let cid = "0x000000000000000000000000000000a1";
        let acct_id = app
            .state
            .resolve_trading_account("binance", "alice")
            .unwrap()
            .id;
        app.state
            .store
            .order_repo()
            .unwrap()
            .insert(strategy_order(acct_id, cid, "s1", false))
            .unwrap();
        let mut ws = connect_south(&url, "s1").await;

        let v = send_get_order(
            &mut ws,
            &(r#""account":"alice","client_order_id":""#.to_owned() + cid + r#"""#),
        )
        .await;

        assert_eq!(v["type"], "order");
        assert_eq!(v["client_order_id"], cid);
        assert_eq!(v["status"], "PLACING");
        assert_eq!(v["exchange"], "binance");
    }

    #[tokio::test]
    async fn get_order_returns_terminal_tombstone() {
        // 当日终态单已软删：peek tombstone 仍须可读。
        let app =
            crate::api::test_support::app_with_accounts("1,alice,binance,K,s,,one_way,0,0,0,0\n")
                .await;
        let url = crate::api::test_support::serve_south_ws(&app.state).await;
        let cid = "0x000000000000000000000000000000a2";
        let acct_id = app
            .state
            .resolve_trading_account("binance", "alice")
            .unwrap()
            .id;
        app.state
            .store
            .order_repo()
            .unwrap()
            .insert(strategy_order(acct_id, cid, "s1", true))
            .unwrap();
        let mut ws = connect_south(&url, "s1").await;

        let v = send_get_order(
            &mut ws,
            &(r#""account":"alice","client_order_id":""#.to_owned() + cid + r#"""#),
        )
        .await;

        assert_eq!(v["client_order_id"], cid);
        assert_eq!(v["status"], "FILLED");
    }

    #[tokio::test]
    async fn get_order_falls_back_to_history_file() {
        // 跨日已 purge：内存无此单，从历史 CSV 回读。
        let app =
            crate::api::test_support::app_with_accounts("1,alice,binance,K,s,,one_way,0,0,0,0\n")
                .await;
        let url = crate::api::test_support::serve_south_ws(&app.state).await;
        let cid = "0x000000000000000000000000000000a3";
        let acct_id = app
            .state
            .resolve_trading_account("binance", "alice")
            .unwrap()
            .id;
        // 在历史日目录写订单（账户目录名 alice，account_id 取当前解析 id）。
        let dir = app.state.store.root().join("2026-09-19").join("alice");
        std::fs::create_dir_all(&dir).unwrap();
        let mut wtr = csv::Writer::from_path(dir.join("order.csv")).unwrap();
        wtr.serialize(strategy_order(acct_id, cid, "s1", true))
            .unwrap();
        wtr.flush().unwrap();
        let mut ws = connect_south(&url, "s1").await;

        let v = send_get_order(
            &mut ws,
            &(r#""account":"alice","client_order_id":""#.to_owned() + cid + r#"""#),
        )
        .await;

        assert_eq!(v["client_order_id"], cid);
        assert_eq!(v["status"], "FILLED");
    }

    #[tokio::test]
    async fn get_order_errors_on_missing_cid() {
        let app =
            crate::api::test_support::app_with_accounts("1,alice,binance,K,s,,one_way,0,0,0,0\n")
                .await;
        let url = crate::api::test_support::serve_south_ws(&app.state).await;
        let mut ws = connect_south(&url, "s1").await;

        // 缺 client_order_id：error 帧。
        let v = send_get_order(&mut ws, r#""account":"alice""#).await;
        assert_eq!(v["type"], "error");
    }

    #[tokio::test]
    async fn get_order_rejects_other_strategy_order() {
        // 订单属于 s2：s1 不得查到（归属校验失败按查无处理）。
        let app =
            crate::api::test_support::app_with_accounts("1,alice,binance,K,s,,one_way,0,0,0,0\n")
                .await;
        let url = crate::api::test_support::serve_south_ws(&app.state).await;
        let cid = "0x000000000000000000000000000000a4";
        let acct_id = app
            .state
            .resolve_trading_account("binance", "alice")
            .unwrap()
            .id;
        app.state
            .store
            .order_repo()
            .unwrap()
            .insert(strategy_order(acct_id, cid, "s2", false))
            .unwrap();
        let mut ws = connect_south(&url, "s1").await;

        let v = send_get_order(
            &mut ws,
            &(r#""account":"alice","client_order_id":""#.to_owned() + cid + r#"""#),
        )
        .await;
        assert_eq!(v["type"], "error");
    }

    #[tokio::test]
    async fn get_order_supports_signal_id_key() {
        // WS get_order 也应支持按 signal_id（配合握手策略身份）查询，
        // 与 REST GET /orders 等价。
        let app =
            crate::api::test_support::app_with_accounts("1,alice,binance,K,s,,one_way,0,0,0,0\n")
                .await;
        let url = crate::api::test_support::serve_south_ws(&app.state).await;
        let cid = "0x000000000000000000000000000000a5";
        let acct_id = app
            .state
            .resolve_trading_account("binance", "alice")
            .unwrap()
            .id;
        let mut o = strategy_order(acct_id, cid, "s1", true);
        o.signal_id = Some("sig-abc".into());
        app.state.store.order_repo().unwrap().insert(o).unwrap();
        let mut ws = connect_south(&url, "s1").await;

        // 只带 signal_id，不带 client_order_id。
        let v = send_get_order(&mut ws, r#""account":"alice","signal_id":"sig-abc""#).await;

        assert_eq!(v["type"], "order");
        assert_eq!(v["client_order_id"], cid);
        assert_eq!(v["signal_id"], "sig-abc");
    }

    #[test]
    fn generates_when_missing_or_invalid() {
        let g1 = choose_client_order_id(None);
        assert!(Order::is_valid_client_order_id(&g1));
        assert!(choose_client_order_id(Some("not-a-cid")).starts_with("0x"));
        assert!(choose_client_order_id(Some("0x123")).starts_with("0x"));
        // 两次生成不应相同。
        assert_ne!(choose_client_order_id(None), choose_client_order_id(None));
    }

    // ---- get_positions：南向 WS 查本地仓位（不调交易所 API / 过滤 / 完整字段 / 空）----

    /// 发一条 get_positions 命令并读回一帧应答（body 为 JSON 片段，可空）。
    async fn send_get_positions(
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        body: &str,
    ) -> serde_json::Value {
        use futures_util::{SinkExt, StreamExt};
        // body 为空时不补逗号，避免 {"option":"get_positions",} 这种非法 JSON。
        let cmd = if body.trim().is_empty() {
            r#"{"option":"get_positions"}"#.to_string()
        } else {
            format!(r#"{{"option":"get_positions",{body}}}"#)
        };
        ws.send(tokio_tungstenite::tungstenite::Message::Text(cmd.into()))
            .await
            .unwrap();
        let msg = ws.next().await.unwrap().unwrap();
        serde_json::from_str(msg.to_text().unwrap()).unwrap()
    }

    /// 在本地仓位表插入一条指定账户/标的/方向/合约形状的仓位，返回其本地 id。
    fn insert_position(
        state: &crate::state::AppState,
        account_id: u64,
        exchange: &str,
        instrument: &str,
        side: Side,
        itype: InstrumentType,
    ) -> u64 {
        let mut p = trading_models::Position::new(account_id, exchange, instrument, side);
        p.instrument_type = itype;
        state.store.positions().unwrap().insert(p).unwrap().id
    }

    #[tokio::test]
    async fn get_positions_reads_local_table_with_full_fields() {
        // Arrange：本地仓位表有一条活跃仓（不接入任何交易所适配器）。
        let app =
            crate::api::test_support::app_with_accounts("1,alice,binance,K,s,,one_way,0,0,0,0\n")
                .await;
        let url = crate::api::test_support::serve_south_ws(&app.state).await;
        let acct_id = app
            .state
            .resolve_trading_account("binance", "alice")
            .unwrap()
            .id;
        let local_id = insert_position(
            &app.state,
            acct_id,
            "binance",
            "BTCUSDT",
            Side::Buy,
            InstrumentType::Futures,
        );
        let mut ws = connect_south(&url, "s1").await;

        // Act：不带任何过滤参数。
        let v = send_get_positions(&mut ws, "").await;

        // Assert：帧类型 positions，命中本地行，元素字段与 REST PositionDTO 同集（26）。
        assert_eq!(v["type"], "positions", "整帧: {v}");
        let list = v["positions"].as_array().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0]["id"], local_id);
        assert_eq!(list[0]["account_id"], acct_id);
        assert_eq!(list[0]["exchange"], "binance");
        assert_eq!(list[0]["instrument"], "BTCUSDT");
        assert_eq!(list[0]["side"], "BUY");
        assert_eq!(list[0]["deleted"], false);
        // 与 REST PositionDTO 完全一致的 26 个业务键。
        assert_eq!(
            list[0].as_object().unwrap().len(),
            26,
            "WS 仓位元素应含完整 26 字段: {}",
            list[0]
        );
    }

    #[tokio::test]
    async fn get_positions_no_args_returns_all_rows() {
        // Arrange：两个账户各一条仓位。
        let app = crate::api::test_support::app_with_accounts(
            "1,alice,binance,K,s,,one_way,0,0,0,0\n2,bob,binance,K,s,,one_way,0,0,0,0\n",
        )
        .await;
        let url = crate::api::test_support::serve_south_ws(&app.state).await;
        let alice_id = app
            .state
            .resolve_trading_account("binance", "alice")
            .unwrap()
            .id;
        let bob_id = app
            .state
            .resolve_trading_account("binance", "bob")
            .unwrap()
            .id;
        insert_position(
            &app.state,
            alice_id,
            "binance",
            "BTCUSDT",
            Side::Buy,
            InstrumentType::Futures,
        );
        insert_position(
            &app.state,
            bob_id,
            "binance",
            "ETHUSDT",
            Side::Sell,
            InstrumentType::Futures,
        );
        let mut ws = connect_south(&url, "s1").await;

        // Act + Assert：不传参=不筛选，返回两条（跨账户）。
        let v = send_get_positions(&mut ws, "").await;
        assert_eq!(v["positions"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn get_positions_filters_by_account_exchange_type_instrument() {
        // Arrange：同账户两条不同标的仓位。
        let app =
            crate::api::test_support::app_with_accounts("1,alice,binance,K,s,,one_way,0,0,0,0\n")
                .await;
        let url = crate::api::test_support::serve_south_ws(&app.state).await;
        let acct_id = app
            .state
            .resolve_trading_account("binance", "alice")
            .unwrap()
            .id;
        insert_position(
            &app.state,
            acct_id,
            "binance",
            "BTCUSDT",
            Side::Buy,
            InstrumentType::Futures,
        );
        insert_position(
            &app.state,
            acct_id,
            "binance",
            "ETHUSDT",
            Side::Buy,
            InstrumentType::Futures,
        );
        let mut ws = connect_south(&url, "s1").await;

        // Act：account+exchange+instrument_type+instrument 精确过滤。
        let v = send_get_positions(
            &mut ws,
            r#""account":"alice","exchange":"binance","instrument_type":"futures","instrument":"BTCUSDT""#,
        )
        .await;

        // Assert：仅命中 BTCUSDT 一条。
        let list = v["positions"].as_array().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0]["instrument"], "BTCUSDT");
    }

    #[tokio::test]
    async fn get_positions_no_match_returns_empty_list_not_error() {
        // Arrange：本地有一条 BTCUSDT 仓位。
        let app =
            crate::api::test_support::app_with_accounts("1,alice,binance,K,s,,one_way,0,0,0,0\n")
                .await;
        let url = crate::api::test_support::serve_south_ws(&app.state).await;
        let acct_id = app
            .state
            .resolve_trading_account("binance", "alice")
            .unwrap()
            .id;
        insert_position(
            &app.state,
            acct_id,
            "binance",
            "BTCUSDT",
            Side::Buy,
            InstrumentType::Futures,
        );
        let mut ws = connect_south(&url, "s1").await;

        // Act：查一个不存在的标的（账户合法）。
        let v = send_get_positions(
            &mut ws,
            r#""account":"alice","exchange":"binance","instrument":"DOGEUSDT""#,
        )
        .await;

        // Assert：正常空列表，而非错误帧。
        assert_eq!(v["type"], "positions");
        assert_eq!(v["positions"].as_array().unwrap().len(), 0);
    }
}
