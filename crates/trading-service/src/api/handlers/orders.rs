//! `POST /api/v1/orders`（下单）与 `DELETE /api/v1/orders`（撤单）：
//! 复用 service 下单/撤单内核，后台事件不推送（靠轮询）。

use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::api::dto::order::OrderDTO;
use crate::api::envelope::{parse_json, ApiError, ApiResponse, BizError};
use crate::service::{
    submit_cancel_core, submit_order_core, Callback, CancelCoreError, CancelOrderInput,
    OrderCoreError, PlaceOrderInput,
};
use crate::state::AppState;

/// `POST /api/v1/orders` 请求体（文档 §4.4）。
///
/// Decimal 字段直接收数字或字符串（serde 经 rust_decimal 字符串/数字反序列化）；
/// REST 无 client_order_id/signal_id 字段——cid 服务端生成。
#[derive(Debug, Clone, Deserialize)]
pub struct PlaceOrderRequestDto {
    pub exchange: String,
    #[serde(default = "default_account")]
    pub account: String,
    pub instrument: String,
    #[serde(default = "default_instrument_type")]
    pub instrument_type: Option<String>,
    pub side: String,
    #[serde(default = "default_order_type")]
    pub order_type: Option<String>,
    #[serde(default)]
    pub time_in_force: Option<String>,
    #[serde(default)]
    pub price: Option<Decimal>,
    #[serde(default)]
    pub quantity: Option<Decimal>,
    #[serde(default)]
    pub order_cash: Option<Decimal>,
    #[serde(default)]
    pub reduce_only: bool,
    #[serde(default)]
    pub leverage: Option<u64>,
    #[serde(default)]
    pub strategy_name: Option<String>,
}

fn default_account() -> String {
    "default".to_string()
}
/// 供规则写入复用的默认账户名。
pub(crate) fn default_account_name() -> String {
    default_account()
}
fn default_order_type() -> Option<String> {
    None
}
fn default_instrument_type() -> Option<String> {
    None
}

/// 宽松解析方向（buy/long/多）。
fn parse_side(raw: &str) -> Result<trading_models::Side, ApiError> {
    trading_models::Side::from_wire(raw)
        .ok_or_else(|| ApiError::bad_request(format!("side 非法：`{raw}`")))
}

/// 标的形状（规则写入复用入口）。
pub(crate) fn parse_itype_pub(
    raw: Option<&str>,
) -> Result<trading_models::InstrumentType, ApiError> {
    parse_itype(raw)
}

/// 标的形状：perpetual/swap 等归一 futures，缺省 spot。
fn parse_itype(raw: Option<&str>) -> Result<trading_models::InstrumentType, ApiError> {
    let raw = raw.unwrap_or("spot");
    Ok(match raw.trim().to_ascii_lowercase().as_str() {
        "futures" | "perpetual" | "perp" | "swap" | "usdm" | "futures_usdm" => {
            trading_models::InstrumentType::Futures
        }
        "spot" => trading_models::InstrumentType::Spot,
        "option" => trading_models::InstrumentType::Option,
        other => {
            return Err(ApiError::bad_request(format!(
                "instrument_type 非法：`{other}`"
            )))
        }
    })
}

/// 宽松解析下单类型（stop/take 别名；缺省 limit）。
fn parse_order_type(raw: Option<&str>) -> trading_models::OrderType {
    match raw {
        Some(s) => trading_models::OrderType::from_wire(s),
        None => trading_models::OrderType::Limit,
    }
}

/// `POST /api/v1/orders`。
///
/// 手动读取并解析 body：不依赖 axum `Json` 提取器（其对坏 JSON 返回 422），
/// 统一按文档把解析失败收敛为 40001/400。
pub async fn place_order(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Result<Response, ApiError> {
    let req: PlaceOrderRequestDto = parse_json(&body)?;
    // 边界入口日志：只记下单要素（不含任何凭证），便于与内核/回调日志串起全链路。
    tracing::info!(
        exchange = %req.exchange,
        account = %req.account,
        instrument = %req.instrument,
        side = %req.side,
        order_type = ?req.order_type,
        "POST /orders received"
    );
    let side = parse_side(&req.side)?;
    let itype = parse_itype(req.instrument_type.as_deref())?;
    let order_type = parse_order_type(req.order_type.as_deref());
    let price = req.price.unwrap_or(Decimal::ZERO);
    // cid 一律服务端生成，请求即夹带也忽略。
    let client_order_id = crate::service::new_client_id();

    let input = PlaceOrderInput {
        exchange: req.exchange,
        account: req.account,
        instrument: req.instrument,
        instrument_type: itype,
        side,
        order_type,
        time_in_force: crate::service::parse_time_in_force(req.time_in_force.as_deref()),
        price,
        quantity: req.quantity.unwrap_or(Decimal::ZERO),
        order_cash: req.order_cash.unwrap_or(Decimal::ZERO),
        reduce_only: req.reduce_only,
        // REST 显式给杠杆（含 1）→ 下发；未给 → 不下发（维持账户当前）。
        set_leverage_explicit: req.leverage.is_some(),
        leverage: req.leverage.unwrap_or(1),
        strategy_name: req.strategy_name,
        signal_id: None,
        client_order_id,
    };

    match submit_order_core(state, input, Callback::Ignore).await {
        Ok(order) => {
            tracing::info!(
                client_order_id = %order.client_order_id,
                status = %order.status.wire(),
                "POST /orders accepted in memory (PLACING)"
            );
            Ok(Json(ApiResponse::ok(OrderDTO::from(&order))).into_response())
        }
        Err(OrderCoreError::Reject(reason)) => Err(map_reject(reason)),
        Err(OrderCoreError::DuplicateSignal(_)) => Err(ApiError::conflict("signal_id 撞车")),
    }
}

/// 前置拒绝映射：账户/所解析类 → 40002；其余 → 40001。
fn map_reject(reason: crate::service::RejectReason) -> ApiError {
    let msg = reason.message;
    // 账户未配置/交易所未注册的文案归 40002；缺字段/枚举/取量类归 40001。
    if msg.contains("未知交易所") || msg.contains("精确匹配") {
        ApiError::new(BizError::AccountNotConfigured, msg)
    } else {
        ApiError::bad_request(msg)
    }
}

/// `DELETE /api/v1/orders` query 参数。
#[derive(Debug, Clone, Deserialize)]
pub struct CancelOrderQuery {
    pub client_order_id: Option<String>,
    pub account: Option<String>,
    pub exchange: Option<String>,
}

/// `DELETE /api/v1/orders`。
pub async fn cancel_order(
    State(state): State<AppState>,
    Query(query): Query<CancelOrderQuery>,
) -> Result<Response, ApiError> {
    let client_order_id = query
        .client_order_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ApiError::bad_request("缺少 client_order_id"))?
        .to_string();

    let input = CancelOrderInput {
        client_order_id,
        account: query.account,
        exchange: query.exchange,
    };
    match submit_cancel_core(state, input, Callback::Ignore).await {
        Ok(order) => Ok(Json(ApiResponse::ok(OrderDTO::from(&order))).into_response()),
        // 内存查无此单：本期 code:0,data=null（非错误）。
        Err(CancelCoreError::NotFound(_)) => {
            Ok(Json(ApiResponse::<OrderDTO>::ok_optional(None)).into_response())
        }
        Err(CancelCoreError::Reject(reason)) => Err(map_reject(reason)),
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{Request, StatusCode};
    use axum::Router;
    use tower::ServiceExt;

    use crate::api::test_support::app_with_accounts;

    /// 起带一个 binance 账户的应用，返回 router。
    async fn app() -> Router {
        let app = app_with_accounts("0,test_binance,binance,K,s,,one_way,0,0,0,0\n").await;
        crate::api::router().with_state(app.state.clone())
    }

    /// 发请求，返回 (状态码, JSON)。
    async fn call(
        router: &Router,
        method: &str,
        uri: &str,
        body: &str,
    ) -> (StatusCode, serde_json::Value) {
        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        if bytes.is_empty() {
            return (status, serde_json::Value::Null);
        }
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn post_order_returns_placing_dto() {
        // Arrange
        let router = app().await;
        // Act
        let (status, v) = call(
            &router,
            "POST",
            "/api/v1/orders",
            r#"{"exchange":"binance","account":"test_binance","instrument":"btcusdt",
                "side":"buy","order_type":"market","quantity":0.1}"#,
        )
        .await;
        // Assert：200，data 为 PLACING OrderDTO。
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["code"], 0);
        assert_eq!(v["data"]["status"], "PLACING");
        assert_eq!(v["data"]["exchange"], "binance");
        assert_eq!(v["data"]["instrument"], "BTCUSDT");
        assert_eq!(v["data"]["side"], "BUY");
        assert_eq!(v["data"]["test_net"], false);
        // cid 服务端生成（0x + 32hex），signal_id 为 null。
        let cid = v["data"]["client_order_id"].as_str().unwrap();
        assert!(cid.starts_with("0x") && cid.len() == 34);
        assert!(v["data"]["signal_id"].is_null());
        assert!(v["data"]["strategy_name"].is_null());
    }

    #[tokio::test]
    async fn post_order_accepts_order_cash_and_validates_reference_price() {
        // order_cash 放开：缺行情时换算失败 → 40001。
        let router = app().await;
        let (status, v) = call(
            &router,
            "POST",
            "/api/v1/orders",
            r#"{"exchange":"binance","account":"test_binance","instrument":"btcusdt",
                "side":"buy","order_type":"market","order_cash":100}"#,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(v["code"], 40001);
    }

    #[tokio::test]
    async fn post_order_missing_account_returns_40002() {
        // 账户/所解析失败 → 40002。
        let app = app_with_accounts("").await;
        let router = crate::api::router().with_state(app.state.clone());
        let (status, v) = call(
            &router,
            "POST",
            "/api/v1/orders",
            r#"{"exchange":"binance","account":"ghost","instrument":"btcusdt",
                "side":"buy","order_type":"market","quantity":1}"#,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(v["code"], 40002);
        assert!(v["data"].is_null());
    }

    #[tokio::test]
    async fn post_order_bad_json_returns_40001() {
        let router = app().await;
        let (status, v) = call(&router, "POST", "/api/v1/orders", "{not-json}").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(v["code"], 40001);
    }

    #[tokio::test]
    async fn delete_order_cancels_existing() {
        // Arrange：先下一笔，拿回 cid。
        let router = app().await;
        let (_, created) = call(
            &router,
            "POST",
            "/api/v1/orders",
            r#"{"exchange":"binance","account":"test_binance","instrument":"btcusdt",
                "side":"buy","order_type":"market","quantity":1}"#,
        )
        .await;
        let cid = created["data"]["client_order_id"].as_str().unwrap();

        // Act：撤单。
        let (status, v) = call(
            &router,
            "DELETE",
            &format!("/api/v1/orders?client_order_id={cid}"),
            "",
        )
        .await;
        // Assert：200，当前订单 DTO。
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["code"], 0);
        assert_eq!(v["data"]["client_order_id"], cid);
    }

    #[tokio::test]
    async fn delete_unknown_cid_returns_null_data() {
        // 内存查无此单：本期 code:0,data=null（非错误）。
        let router = app().await;
        let (status, v) = call(
            &router,
            "DELETE",
            "/api/v1/orders?client_order_id=0x0123456789abcdef0123456789abcdef",
            "",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(v["data"].is_null());
    }

    #[tokio::test]
    async fn list_orders_filters_by_status_bracket() {
        // Arrange：下一笔，停在 PLACING（未成交）。
        let router = app().await;
        let (_, created) = call(
            &router,
            "POST",
            "/api/v1/orders",
            r#"{"exchange":"binance","account":"test_binance","instrument":"btcusdt",
                "side":"buy","order_type":"market","quantity":1}"#,
        )
        .await;
        assert_eq!(created["data"]["status"], "PLACING");

        // Act：只查 FILLED（status[]=FILLED）。
        let (status, v) = call(
            &router,
            "GET",
            "/api/v1/orders?account=test_binance&exchange=binance&status[]=FILLED",
            "",
        )
        .await;

        // Assert：PLACING 单不应混入 FILLED 结果 → 返回空列表。
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["code"], 0);
        assert_eq!(v["data"]["total"], 0);
        assert!(v["data"]["list"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_missing_cid_param_returns_40001() {
        let router = app().await;
        let (status, v) = call(&router, "DELETE", "/api/v1/orders", "").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(v["code"], 40001);
    }
}
