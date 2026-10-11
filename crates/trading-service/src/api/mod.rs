//! REST API（`/health` 与 `/api/v1/*`）：与南向 WS 同进程同端口挂载。
//!
//! 分层：
//! * [`envelope`]：标准信封/错误码/分页（零 IO 纯类型纯函数）；
//! * `dto`：REST 报文对象，与 CSV 持久化 model 分离（bool 出 true/false、
//!   枚举出线上字符串、Decimal 出精确数字、不暴露 qty_unit/密钥等内部字段）；
//! * `handlers`：薄 axum handler，只做解析 → 调 store/service → DTO 信封；
//! * `filters`：列表查询的零 IO 谓词；
//! * `rules_batch`：连锁规则批量创建的零 IO 纯模块（§9 两遍落库）。

pub mod dto;
pub mod envelope;
pub mod filters;
pub mod handlers;
pub mod rules_batch;

#[cfg(test)]
pub mod test_support;

use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::state::AppState;

// 只读 handler 在模块树声明（见 handlers）。
use crate::api::handlers::accounts::list_accounts;
use crate::api::handlers::exchange_positions::query_exchange_positions;
use crate::api::handlers::lists::{
    get_position, list_orders as query_orders, list_positions, list_rules,
};
use crate::api::handlers::orders::{cancel_order as http_cancel, place_order as http_place};
use crate::api::handlers::rule_writes::{batch_create_rules, rule_status_batch, upsert_rule};
use crate::api::handlers::state::service_state;

/// 存活探针应答（不走业务信封）。
#[derive(Debug, Serialize)]
struct HealthReply {
    status: &'static str,
}

/// `GET /health`：存活探针，恒 200，裸 `{status:"healthy"}`。
pub async fn health() -> Response {
    Json(HealthReply { status: "healthy" }).into_response()
}

/// 构造 REST 路由（与南向 WS 同端口，由 main merge 后统一提供 AppState）。
///
/// 目前只挂 `/health`；后续阶段在本函数内追加 `/api/v1/*` 路由。
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        // 账户安全投影查询。
        .route("/api/v1/accounts", get(list_accounts))
        // 服务状态摘要。
        .route("/api/v1/state", get(service_state))
        // 订单/规则/持仓列表与单条持仓查询。
        .route("/api/v1/orders", get(query_orders))
        .route("/api/v1/rules", get(list_rules))
        .route("/api/v1/positions", get(list_positions))
        .route("/api/v1/positions/{id}", get(get_position))
        // 下单/撤单（REST 复用 service 内核）。
        .route(
            "/api/v1/orders",
            axum::routing::post(http_place).delete(http_cancel),
        )
        // 规则创建/更新与批量启停。
        .route("/api/v1/rules", axum::routing::post(upsert_rule))
        .route(
            "/api/v1/rules/status",
            axum::routing::post(rule_status_batch),
        )
        // 连锁规则批量创建（两遍落库）。
        .route(
            "/api/v1/rules/batch",
            axum::routing::post(batch_create_rules),
        )
        // 实时查询交易所侧持仓。
        .route(
            "/api/v1/exchange/positions",
            axum::routing::post(query_exchange_positions),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[tokio::test]
    async fn health_returns_bare_healthy_without_envelope() {
        // Act：直接调用无状态 handler。
        let resp = health().await;
        // Assert：200 且裸报文（无 code/message/data 信封）。
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v, serde_json::json!({"status": "healthy"}));
        assert!(v.get("code").is_none());
    }
}
