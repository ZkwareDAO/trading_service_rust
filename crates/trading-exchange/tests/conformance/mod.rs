//! 交易所接口契约（后端无关）。
//!
//! 目录约定：
//! * 本文件是 `[[test]] path` 直接引用的测试目标入口，同时定义与后端无关的
//!   `assert_*` 契约断言；
//! * `sim.rs` 是当前唯一后端（模拟所），每个 `#[tokio::test]` 调用本模块契约；
//! * 新后端（binance / replay）只需实现 `Backend` 句柄并复用同一套 `assert_*`，
//!   无需复制断言。CTP 的多腿/平今平昨语义由 `third_party/ctp-mock` 覆盖，不走本套件。

mod sim;

use std::sync::Arc;

use rust_decimal::Decimal;
use trading_exchange::api::{
    ExchangeApi, OrderLookup, OrderPositionSide, OrderRef, PlaceOrderRequest,
};
use trading_models::{InstrumentType, OrderStatus, OrderType, Side};

/// 契约测试统一标的。
pub const INSTRUMENT: &str = "BTCUSDT";

/// 契约测试的后端句柄：一个全新后端实例 + 该校验基准。
///
/// 每个契约用例都必须构造全新实例，避免订单/仓位状态在用例间串扰。
pub struct Backend {
    /// 后端精确名（失败信息定位用）。
    pub name: &'static str,
    pub api: Arc<dyn ExchangeApi>,
    /// 该后端实例的标记价（契约用固定值，不依赖真实行情）。
    pub mark: Decimal,
    /// 初始权益（余额恒等式的起点）。
    pub initial_equity: Decimal,
}

/// 查单故障注入口径：后端各自映射到自己的查询模式（模拟所是 `QueryMode`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupFault {
    NotExists,
    Unavailable,
}

/// 契约统一的数量/价格字面量解析。
pub fn dec(s: &str) -> Decimal {
    use std::str::FromStr;
    Decimal::from_str(s).expect("契约字面量必须是合法十进制")
}

/// 构造一个契约下单请求。
pub fn request(
    cid: &str,
    order_type: OrderType,
    price: Option<Decimal>,
    quantity: Decimal,
) -> PlaceOrderRequest {
    PlaceOrderRequest {
        instrument: INSTRUMENT.into(),
        instrument_type: InstrumentType::Futures,
        side: Side::Buy,
        order_type,
        price,
        quantity,
        reduce_only: false,
        position_side: OrderPositionSide::Both,
        time_in_force: None,
        client_order_id: cid.into(),
    }
}

/// 契约①：受理 ACK 归一为 NEW，带交易所单号并回显 cid。
pub async fn assert_ack_is_normalized(b: &Backend) {
    let ack = b
        .api
        .place_order(&request("c-ack", OrderType::Limit, Some(b.mark), dec("1")))
        .await
        .unwrap_or_else(|e| panic!("[{}] 受理应成功：{e}", b.name));

    assert!(ack.accepted, "[{}] 挂单应被受理", b.name);
    assert_eq!(
        ack.status,
        OrderStatus::New,
        "[{}] 受理态应归一为 NEW",
        b.name
    );
    assert!(
        ack.exchange_order_id.is_some(),
        "[{}] 受理后必须带交易所单号",
        b.name
    );
    assert_eq!(ack.client_order_id, "c-ack", "[{}] cid 必须回显", b.name);
}

/// 契约②：重复 client_order_id 必须确定性拒单（accepted=false），不得抛传输错。
pub async fn assert_duplicate_cid_is_rejected(b: &Backend) {
    let req = request("c-dup", OrderType::Limit, Some(b.mark), dec("1"));
    assert!(b.api.place_order(&req).await.expect("首次受理").accepted);
    let ack = b
        .api
        .place_order(&req)
        .await
        .unwrap_or_else(|e| panic!("[{}] 重复 cid 应为业务拒单而非传输错：{e}", b.name));
    assert!(!ack.accepted, "[{}] 重复 cid 必须拒绝", b.name);
    assert!(
        ack.message.is_some(),
        "[{}] 拒单必须带原因（可观测）",
        b.name
    );
}

/// 契约③：市价成交后持仓带 signed qty / 成本价 / 保证金 / 方向。
pub async fn assert_market_position_fields(b: &Backend) {
    let ack = b
        .api
        .place_order(&request("c-mkt", OrderType::Market, None, dec("2")))
        .await
        .expect("市价受理");
    assert!(ack.accepted, "[{}] 市价应被受理", b.name);

    let positions = b.api.get_positions().await.expect("查仓");
    assert_eq!(positions.len(), 1, "[{}] 应恰有一条持仓", b.name);
    let p = &positions[0];
    assert_eq!(
        p.signed_quantity,
        dec("2"),
        "[{}] 多头 signed qty 为正",
        b.name
    );
    assert_eq!(p.entry_price, b.mark, "[{}] 成本价应为成交价", b.name);
    assert_eq!(p.mark_price, Some(b.mark), "[{}] 标记价应回填", b.name);
    assert!(
        p.margin.expect("保证金") > Decimal::ZERO,
        "[{}] 成交后必须占用保证金",
        b.name
    );
}

/// 契约④：余额三字段满足 `available == equity - margin_used`（资金守恒的所级口径）。
pub async fn assert_balance_identity(b: &Backend) {
    b.api
        .place_order(&request("c-bal", OrderType::Market, None, dec("2")))
        .await
        .expect("市价受理");

    let balances = b.api.get_balance().await.expect("查余额");
    assert_eq!(balances.len(), 1, "[{}] 应恰有一个结算币种", b.name);
    let bal = &balances[0];
    assert!(!bal.currency.is_empty(), "[{}] 币种不得为空", b.name);
    assert_eq!(
        bal.equity, b.initial_equity,
        "[{}] 无浮盈时权益不变",
        b.name
    );
    let margin = bal.margin_used.expect("余额须带占用保证金");
    assert!(margin > Decimal::ZERO, "[{}] 成交后占用保证金为正", b.name);
    assert_eq!(
        bal.available,
        bal.equity - margin,
        "[{}] 余额守恒：available == equity - margin_used",
        b.name
    );
}

/// 契约⑤：撤单幂等——未知单不 panic（accepted=false）；已撤单再撤仍为 Canceled。
pub async fn assert_cancel_is_idempotent(b: &Backend) {
    // 未知单：业务性 false，而非传输错。
    let unknown = OrderRef::new("c-cancel-unknown");
    let ack = b
        .api
        .cancel_order(INSTRUMENT, &unknown)
        .await
        .unwrap_or_else(|e| panic!("[{}] 撤未知单不应是传输错：{e}", b.name));
    assert!(!ack.accepted, "[{}] 撤未知单应 accepted=false", b.name);

    // NEW 单 → Canceled；重复撤销保持 Canceled（幂等终态）。
    assert!(
        b.api
            .place_order(&request(
                "c-cancel",
                OrderType::Limit,
                Some(b.mark),
                dec("1")
            ))
            .await
            .expect("挂单受理")
            .accepted
    );
    let r = OrderRef::new("c-cancel");
    let first = b.api.cancel_order(INSTRUMENT, &r).await.expect("首次撤单");
    assert!(first.accepted, "[{}] 撤 NEW 单应受理", b.name);
    assert_eq!(first.status, OrderStatus::Canceled, "[{}]", b.name);
    let second = b.api.cancel_order(INSTRUMENT, &r).await.expect("重复撤单");
    assert!(second.accepted, "[{}] 重复撤单应幂等受理", b.name);
    assert_eq!(second.status, OrderStatus::Canceled, "[{}]", b.name);
}

/// 契约⑥：查单三态可区分（Found / NotExists / Unavailable）。
///
/// `inject` 由后端提供故障注入；契约只认三态语义，不认后端实现细节。
pub async fn assert_query_three_states(b: &Backend, inject: impl Fn(LookupFault)) {
    assert!(
        b.api
            .place_order(&request("c-q", OrderType::Limit, Some(b.mark), dec("1")))
            .await
            .expect("挂单受理")
            .accepted
    );
    let r = OrderRef::new("c-q");

    assert!(
        matches!(
            b.api.query_order(INSTRUMENT, &r).await,
            OrderLookup::Found(_)
        ),
        "[{}] 已存在订单应查到",
        b.name
    );

    inject(LookupFault::NotExists);
    assert!(
        matches!(
            b.api.query_order(INSTRUMENT, &r).await,
            OrderLookup::NotExists
        ),
        "[{}] 应能表达「确定不存在」",
        b.name
    );

    inject(LookupFault::Unavailable);
    assert!(
        matches!(
            b.api.query_order(INSTRUMENT, &r).await,
            OrderLookup::Unavailable
        ),
        "[{}] 应能表达「查询不可用」",
        b.name
    );
}
