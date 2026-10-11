//! Deribit 用户主体（一套 client_id/secret）：实现 [`ExchangeApi`]。
//!
//! 职责接线（计算均在纯模块）：
//! - 合约规格懒加载：表内缺失 → `public/get_instrument` → `parse_instrument` → 写表；
//! - claim 前预检：从 `QuoteTable` 读 mark/book → `prep::prepare`（漂移/价差闸门）；
//! - 下单/撤/查：JSON-RPC；不透明 order_id 原样；
//! - 期权无杠杆：`set_leverage` 返回不支持。
//!
//! 账户事件流（`AccountEventStream`）在 P4 于本类型上实现。
//!
//! 见 `docs/扩展交易所.md` 第四节。

use std::sync::Arc;

use async_trait::async_trait;
use reqwest::Client;
use rust_decimal::Decimal;
use trading_core::order_rule::OrderRuleTable;
use trading_models::{InstrumentType, OrderStatus, OrderType, Side};

use crate::api::{
    AccountEventStream, ExchangeApi, OrderAck, OrderPositionSide, OrderRef, OrderState,
    PlaceErrorKind, PlaceOrderRequest, PreparedPlacement, PriceBasis, ReferencePrice,
};
use crate::deribit::instant;
use crate::deribit::prep::{self, PrepConfig};
use crate::deribit::rpc;
use crate::deribit::spec;

const EXCHANGE: &str = "deribit";

/// Deribit 用户主体。
pub struct DeribitUser {
    http: Client,
    rules: Arc<OrderRuleTable>,
    quotes: Arc<trading_core::quote::QuoteTable>,
    client_id: String,
    client_secret: String,
    testnet: bool,
    cfg: PrepConfig,
}

impl DeribitUser {
    pub fn new(
        http: Client,
        rules: Arc<OrderRuleTable>,
        quotes: Arc<trading_core::quote::QuoteTable>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        testnet: bool,
        cfg: PrepConfig,
    ) -> Self {
        Self {
            http,
            rules,
            quotes,
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            testnet,
            cfg,
        }
    }

    fn base(&self) -> &'static str {
        rpc::rest_base(self.testnet)
    }

    /// 主网公共端点取数（`public/get_instrument` / `public/ticker`）：无需鉴权，
    /// 与行情 worker 同主机口径。两个方法仅 method 不同，收口为一处。
    async fn fetch_mainnet_public(
        &self,
        method: &str,
        symbol: &str,
    ) -> anyhow::Result<serde_json::Value> {
        rpc::call(
            self.http.clone(),
            rpc::rest_base(false),
            method,
            serde_json::json!({ "instrument_name": symbol }),
            None,
        )
        .await
    }

    /// 取 access_token（client_credentials）。本期每次私有调用前现取（无缓存）。
    async fn token(&self) -> anyhow::Result<String> {
        fetch_auth_token(
            &self.http,
            self.base(),
            &self.client_id,
            &self.client_secret,
        )
        .await
    }

    /// 确保合约规格在规则表内并返回行；缺失则拉取 get_instrument 后写入。
    #[allow(dead_code)]
    async fn ensure_rule(
        &self,
        instrument: &str,
    ) -> anyhow::Result<Arc<trading_models::OrderRule>> {
        let key = trading_models::OrderRuleKey::new(EXCHANGE, InstrumentType::Option, instrument);
        if let Some(row) = self.rules.get(&key) {
            return Ok(row);
        }
        let token = self.token().await?;
        let params = serde_json::json!({ "instrument_name": instrument });
        let result = rpc::call(
            self.http.clone(),
            self.base(),
            "public/get_instrument",
            params,
            Some(&token),
        )
        .await?;
        let fetched = chrono::Utc::now().timestamp_millis();
        let row = spec::parse_instrument(&result.to_string(), fetched)?;
        self.rules.put(row);
        self.rules
            .get(&key)
            .ok_or_else(|| anyhow::anyhow!("deribit 规则写入后读不到：{instrument}"))
    }
}

#[async_trait]
impl ExchangeApi for DeribitUser {
    fn exchange(&self) -> &str {
        EXCHANGE
    }

    /// claim 前预检（**同步、纯内存**）：规格须已在表内（缺失→DataNotReady），
    /// 读内存行情后做纯转换（漂移/价差闸门/对齐）。不在此做网络 IO。
    fn prepare_to_place(
        &self,
        order: trading_models::Order,
        request: PlaceOrderRequest,
    ) -> Result<PreparedPlacement, crate::api::PlaceDefer> {
        let key = trading_models::OrderRuleKey::new(
            EXCHANGE,
            InstrumentType::Option,
            &request.instrument,
        );
        let row = match self.rules.get(&key) {
            Some(r) => r,
            None => return Err(crate::api::PlaceDefer::DataNotReady),
        };
        let market = prep::read_market(&self.quotes, &request.instrument);
        let prepared = prep::prepare(&row, request, market, self.cfg)?;
        Ok(PreparedPlacement {
            order,
            request: prepared,
        })
    }

    async fn place_order(&self, req: &PlaceOrderRequest) -> anyhow::Result<OrderAck> {
        // —— 市价【开仓】单：先一次性 ensure（缺规格/mark 走主网自取）+ 档位漂移 ——
        // 其余（限价单、reduce_only 平仓单）原样进入下面的挂单逻辑。
        let is_market_open = matches!(req.order_type, OrderType::Market) && !req.reduce_only;
        let (prepared, converted) = if is_market_open {
            // 主网公共取数（get_instrument/ticker）无需鉴权；映射 EnsureNeed→方法。
            let user: &DeribitUser = self;
            let fetcher = move |need: instant::EnsureNeed| async move {
                let (method, symbol) = match need {
                    instant::EnsureNeed::Spec(symbol) => ("public/get_instrument", symbol),
                    instant::EnsureNeed::Mark(symbol) => ("public/ticker", symbol),
                };
                user.fetch_mainnet_public(method, &symbol).await
            };
            let prepared = instant::prepare_market_open(
                req.clone(),
                &self.rules,
                &self.quotes,
                self.cfg,
                fetcher,
            )
            .await?;
            (prepared, true)
        } else {
            (req.clone(), false)
        };

        let token = self.token().await?;
        // reduce_only：方向由 side（预检已取反）；开仓按 rule.side。
        let method = match prepared.side {
            Side::Buy => "private/buy",
            Side::Sell => "private/sell",
        };
        let params = serde_json::json!({
            "instrument_name": prepared.instrument,
            "amount": prepared.quantity,
            "type": "limit",
            "price": prepared.price.unwrap_or(Decimal::ZERO),
            "label": prepared.client_order_id,
            // 平仓单显式标记 reduce_only，确保仅减仓、不被当作反向开仓。
            "reduce_only": prepared.reduce_only,
        });
        let result =
            rpc::call(self.http.clone(), self.base(), method, params, Some(&token)).await?;
        let order_id = result
            .get("order")
            .and_then(|o| o.get("order_id"))
            .and_then(|x| x.as_str())
            .or_else(|| result.get("order_id").and_then(|x| x.as_str()))
            .map(str::to_string);
        Ok(OrderAck {
            accepted: true,
            client_order_id: prepared.client_order_id.clone(),
            exchange_order_id: order_id,
            status: OrderStatus::New,
            message: None,
            // 仅市价开仓被转换时回显真实限价委托；其余 None（调用方维持原存储）。
            placed_type: converted.then_some(OrderType::Limit),
            placed_price: converted.then_some(prepared.price).flatten(),
            placed_quantity: converted.then_some(prepared.quantity),
        })
    }

    async fn cancel_order(&self, instrument: &str, order: &OrderRef) -> anyhow::Result<OrderAck> {
        let token = self.token().await?;
        let currency = instrument.split('-').next().unwrap_or("BTC");
        // 撤单接口按可用单号分流：
        // - exchange_order_id → private/cancel（按 order_id 撤，返回单条）；
        // - label → private/cancel_by_label（**cancel 接口不支持 label**，
        //   传 label 会 -32602；cancel_by_label 才是按 label 撤单的端点）。
        let (method, params) = match prep::route_order_ref(order) {
            prep::OrderIdRoute::ExchangeOrderId(id) => {
                ("private/cancel", serde_json::json!({ "order_id": id }))
            }
            prep::OrderIdRoute::Label(label) => (
                "private/cancel_by_label",
                serde_json::json!({ "label": label, "currency": currency }),
            ),
        };
        let result =
            rpc::call(self.http.clone(), self.base(), method, params, Some(&token)).await?;
        // cancel 返回单条对象；cancel_by_label 返回 {cancelled: N}，无 order_id。
        let order_id = result
            .get("order_id")
            .and_then(|x| x.as_str())
            .map(str::to_string);
        Ok(OrderAck {
            accepted: true,
            client_order_id: order.client_order_id.clone(),
            exchange_order_id: order_id,
            status: OrderStatus::Canceled,
            message: None,
            placed_type: None,
            placed_price: None,
            placed_quantity: None,
        })
    }

    async fn query_order(&self, instrument: &str, order: &OrderRef) -> crate::api::OrderLookup {
        use crate::api::OrderLookup;
        let token = match self.token().await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(error = %e, "deribit query: auth failed");
                return OrderLookup::Unavailable;
            }
        };
        // 主 get_order_state；仅 exchange_id 缺失时用 by_label。
        let (method, params) = match prep::route_order_ref(order) {
            prep::OrderIdRoute::ExchangeOrderId(id) => (
                "private/get_order_state",
                serde_json::json!({ "order_id": id }),
            ),
            prep::OrderIdRoute::Label(label) => (
                "private/get_order_state_by_label",
                serde_json::json!({
                    "label": label,
                    "currency": instrument.split('-').next().unwrap_or("BTC"),
                }),
            ),
        };
        let result =
            match rpc::call(self.http.clone(), self.base(), method, params, Some(&token)).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(error = %e, "deribit query failed");
                    return OrderLookup::Unavailable;
                }
            };
        // by_label 返回数组：取 label 精确匹配一条；查无→NotExists。
        let order_json = match &result {
            serde_json::Value::Array(arr) => {
                match arr.iter().find(|o| {
                    o.get("label").and_then(|x| x.as_str()) == Some(order.client_order_id.as_str())
                }) {
                    Some(o) => o.clone(),
                    None => return OrderLookup::NotExists,
                }
            }
            other => other.clone(),
        };
        OrderLookup::Found(map_order_state(&order_json, instrument))
    }

    async fn get_positions(&self) -> anyhow::Result<Vec<crate::api::PositionInfo>> {
        let token = self.token().await?;
        let mut out = Vec::new();
        for currency in ["BTC", "ETH"] {
            let params = serde_json::json!({ "currency": currency, "kind": "option" });
            let result = rpc::call(
                self.http.clone(),
                self.base(),
                "private/get_positions",
                params,
                Some(&token),
            )
            .await?;
            if let Some(arr) = result.as_array() {
                for row in arr {
                    if let Some(info) = map_position(row, currency) {
                        out.push(info);
                    }
                }
            }
        }
        Ok(out)
    }

    /// 错误分类委托纯函数 [`classify_deribit_error`]。
    fn classify_place_error(&self, err: &anyhow::Error) -> PlaceErrorKind {
        classify_deribit_error(err)
    }

    async fn set_leverage(&self, _instrument: &str, _leverage: u32) -> anyhow::Result<()> {
        anyhow::bail!("deribit 期权不支持设置杠杆")
    }

    async fn get_reference_price(&self, instrument: &str) -> anyhow::Result<ReferencePrice> {
        // public/ticker 无需鉴权；口径 = mark。
        let params = serde_json::json!({ "instrument_name": instrument });
        let result = rpc::call(
            self.http.clone(),
            self.base(),
            "public/ticker",
            params,
            None,
        )
        .await?;
        let mark = result
            .get("mark_price")
            .and_then(|v| match v {
                serde_json::Value::String(s) => s.parse().ok(),
                serde_json::Value::Number(_) => Decimal::from_str_exact(&v.to_string()).ok(),
                _ => None,
            })
            .ok_or_else(|| anyhow::anyhow!("ticker missing mark_price"))?;
        Ok(ReferencePrice {
            instrument: instrument.to_string(),
            basis: PriceBasis::Mark,
            price: mark,
        })
    }
}

/// Deribit 下单错误分类（纯函数）：
/// 错误链中含 [`instant::EnsureError`]（下单**前**规格/mark 自取的确定性失败）→ Rejected；
/// 其余（真正挂单 RPC 的超时/不确定）→ Indeterminate。
pub(crate) fn classify_deribit_error(err: &anyhow::Error) -> PlaceErrorKind {
    if err.chain().any(|e| e.is::<instant::EnsureError>()) {
        PlaceErrorKind::Rejected
    } else {
        PlaceErrorKind::Indeterminate
    }
}

/// 映射订单状态 JSON 为统一 [`OrderState`]。
pub(crate) fn map_order_state(v: &serde_json::Value, instrument: &str) -> OrderState {
    let label = v.get("label").and_then(|x| x.as_str()).map(str::to_string);
    // venue 原生委托要素：供订单事件播报（通知 A）使用。
    let direction = match v.get("direction").and_then(|x| x.as_str()) {
        Some("sell") => Side::Sell,
        _ => Side::Buy,
    };
    let reduce_only = v
        .get("reduce_only")
        .and_then(|x| x.as_bool())
        .unwrap_or(false);
    let extra = crate::api::VenueOrderExtra {
        direction,
        reduce_only,
        amount: json_dec(v.get("amount")).unwrap_or(Decimal::ZERO),
        limit_price: json_dec(v.get("price")).unwrap_or(Decimal::ZERO),
    };
    OrderState {
        client_order_id: label,
        exchange_order_id: v
            .get("order_id")
            .and_then(|x| x.as_str())
            .map(str::to_string),
        instrument: v
            .get("instrument_name")
            .and_then(|x| x.as_str())
            .unwrap_or(instrument)
            .to_string(),
        status: map_status(v.get("order_state").and_then(|x| x.as_str()).unwrap_or("")),
        filled_quantity: json_dec(v.get("filled_amount")).unwrap_or(Decimal::ZERO),
        average_price: json_dec(v.get("average_price")),
        // Deribit 不使用持仓方向模式（开/平仓语义与 Binance 不同）：恒 Both。
        position_side: OrderPositionSide::Both,
        venue_extra: Some(extra),
    }
}

/// 映射 get_positions 一行为 [`PositionInfo`]；非 option/0 量返回 None（过滤）。
fn map_position(v: &serde_json::Value, _currency: &str) -> Option<crate::api::PositionInfo> {
    if v.get("kind").and_then(|x| x.as_str()).unwrap_or("option") != "option" {
        return None;
    }
    let size = json_dec(v.get("size"))?;
    if size == Decimal::ZERO {
        return None;
    }
    let instrument = v
        .get("instrument_name")
        .and_then(|x| x.as_str())?
        .to_string();
    let signed = if v.get("direction").and_then(|x| x.as_str()) == Some("short") {
        -size
    } else {
        size
    };
    Some(crate::api::PositionInfo {
        instrument,
        instrument_type: InstrumentType::Option,
        position_side: OrderPositionSide::Both,
        signed_quantity: signed,
        entry_price: json_dec(v.get("average_price")).unwrap_or(Decimal::ZERO),
        mark_price: json_dec(v.get("mark_price")),
        unrealized_pnl: json_dec(v.get("total_profit_loss")),
        leverage: None,
        notional: None,
        margin: None,
        update_ms: None,
    })
}

/// Deribit order_state 串 → 领域状态；初始挂单统一 NEW。
fn map_status(raw: &str) -> OrderStatus {
    match raw.trim().to_ascii_lowercase().as_str() {
        "open" => OrderStatus::New,
        "filled" => OrderStatus::Filled,
        "rejected" => OrderStatus::Rejected,
        "cancelled" | "canceled" => OrderStatus::Canceled,
        _ => OrderStatus::Unknown,
    }
}

fn json_dec(v: Option<&serde_json::Value>) -> Option<Decimal> {
    v.and_then(|x| match x {
        serde_json::Value::String(s) => s.parse().ok(),
        serde_json::Value::Number(_) => Decimal::from_str_exact(&x.to_string()).ok(),
        _ => None,
    })
}

// =====================================================================
// 账户事件流：私有 WS 订阅参数（纯函数）+ AccountEventStream 委托
// =====================================================================

/// 账户私有 WS 地址（随账户 testnet；与公共行情主网不同）。
///
/// 协议必须是 `wss://`（不能沿用 REST 的 `https://`，否则 tungstenite 直接拒绝）；
/// 路径与 Go 服务 / 官方文档一致，**不带结尾斜杠**：
/// 测试网 `wss://test.deribit.com/ws/api/v2`，正式网 `wss://www.deribit.com/ws/api/v2`。
///
/// 注：迁移期代理出口偶发对该路径返回 404（实测与是否带尾斜杠无关、会随时间翻转），
/// 由上层重连退避兜住，不在此处改路径迁就。
pub(crate) fn ws_url(testnet: bool) -> String {
    let host = rpc::rest_base(testnet).trim_start_matches("https://");
    format!("wss://{host}/ws/api/v2")
}

/// 四个私有频道：orders/changes × BTC/ETH（原始订阅形态）。
pub(crate) fn private_channels() -> Vec<String> {
    let mut out = Vec::new();
    for currency in ["BTC", "ETH"] {
        out.push(format!("user.orders.option.{currency}.raw"));
        out.push(format!("user.changes.option.{currency}.raw"));
    }
    out
}

/// `private/subscribe` 的请求参数：携带频道列表与 access_token。
pub(crate) fn subscribe_params(channels: &[String], access_token: &str) -> serde_json::Value {
    serde_json::json!({
        "channels": channels,
        "access_token": access_token,
    })
}

#[async_trait]
impl AccountEventStream for DeribitUser {
    async fn subscribe_account_events(
        &self,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, crate::api::AccountEvent>> {
        use futures_util::future::BoxFuture;

        // 建连 + 订阅的运行循环在 user_stream；token 以供应商注入，
        // **每轮重连现取**（public/auth），避免旧 token 过期后订阅被静默拒。
        let http = self.http.clone();
        let base = self.base().to_string();
        let client_id = self.client_id.clone();
        let client_secret = self.client_secret.clone();
        let provider: crate::deribit::user_stream::TokenProvider =
            Arc::new(move || -> BoxFuture<'static, anyhow::Result<String>> {
                let (http, base) = (http.clone(), base.clone());
                let (client_id, client_secret) = (client_id.clone(), client_secret.clone());
                Box::pin(
                    async move { fetch_auth_token(&http, &base, &client_id, &client_secret).await },
                )
            });
        let conn_fn: crate::deribit::user_stream::ConnFn = Arc::new(
            |url,
             channels,
             token|
             -> BoxFuture<
                'static,
                anyhow::Result<futures_util::stream::BoxStream<'static, crate::api::AccountEvent>>,
            > { Box::pin(crate::deribit::user_stream::run_conn(url, channels, token)) },
        );
        let url = ws_url(self.testnet);
        let channels = private_channels();
        crate::deribit::user_stream::connect(url, channels, provider, conn_fn)
    }
}

/// 现取 access_token（client_credentials）：`DeribitUser::token` 与订阅 token
/// 供应商共用的唯一实现（消除重复；供应商闭包需要 'static 故拆为自由函数）。
async fn fetch_auth_token(
    http: &Client,
    base: &str,
    client_id: &str,
    client_secret: &str,
) -> anyhow::Result<String> {
    let result = rpc::call(
        http.clone(),
        base,
        "public/auth",
        rpc::auth_params(client_id, client_secret),
        None,
    )
    .await?;
    result
        .get("access_token")
        .and_then(|x| x.as_str())
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("public/auth missing access_token"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[test]
    fn ensure_error_is_classified_rejected() {
        // 下单前规格/mark 自取的确定性失败 → Rejected。
        let err = anyhow::Error::new(instant::EnsureError::MarkMissing);
        assert_eq!(classify_deribit_error(&err), PlaceErrorKind::Rejected);
    }

    #[test]
    fn non_ensure_error_is_classified_indeterminate() {
        // 真正挂单 RPC 的不确定错误 → Indeterminate。
        let err = anyhow::anyhow!("rpc timeout");
        assert_eq!(classify_deribit_error(&err), PlaceErrorKind::Indeterminate);
    }

    #[test]
    fn order_state_open_maps_to_new() {
        let v = serde_json::json!({
            "order_id": "ETH-331562", "label": "0xclient",
            "instrument_name": "BTC-1", "order_state": "open",
            "filled_amount": 0, "average_price": 0
        });
        let s = map_order_state(&v, "BTC-1");
        assert_eq!(s.status, OrderStatus::New);
        assert_eq!(s.exchange_order_id, Some("ETH-331562".into()));
        assert_eq!(s.client_order_id, Some("0xclient".into()));
    }

    #[test]
    fn ws_url_uses_wss_and_api_v2() {
        // 私有 WS 不能用 REST 的 https://，且入口必须是 /ws/api/v2。
        assert_eq!(ws_url(true), "wss://test.deribit.com/ws/api/v2");
        assert_eq!(ws_url(false), "wss://www.deribit.com/ws/api/v2");
    }

    #[test]
    fn position_short_maps_signed_and_filters_zero() {
        let v = serde_json::json!({
            "instrument_name": "BTC-1", "kind": "option",
            "size": 1, "direction": "short",
            "average_price": 0.4, "mark_price": 0.5, "total_profit_loss": -0.1
        });
        let p = map_position(&v, "BTC").expect("mapped");
        assert_eq!(p.signed_quantity, dec("-1"));
        assert_eq!(p.entry_price, dec("0.4"));
        assert_eq!(p.mark_price, Some(dec("0.5")));
        assert_eq!(p.unrealized_pnl, Some(dec("-0.1")));

        let zero = serde_json::json!({"instrument_name":"X","kind":"option","size":0});
        assert!(map_position(&zero, "BTC").is_none());
        let not_option = serde_json::json!({"instrument_name":"X","kind":"future","size":1});
        assert!(map_position(&not_option, "BTC").is_none());
    }
}
