//! `POST /api/v1/exchange/positions`：实时查询交易所侧持仓（不落库）。

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use crate::api::dto::position::PositionDTO;
use crate::api::envelope::{now_ms, parse_json, ApiError, ApiResponse, BizError};
use crate::state::AppState;

/// 请求体（文档 §4.10）。
#[derive(Debug, Clone, Deserialize)]
pub struct ExchangePositionsRequest {
    pub exchange: String,
    pub account: String,
    #[serde(default)]
    pub instrument_type: Option<String>,
}

/// `POST /api/v1/exchange/positions`。
///
/// 手动解析 body（坏 JSON 40001）。账户解析（所已注册+精确匹配）失败 40002；
/// connect 或 get_positions 失败属交易所调用问题 → 50001。返回映射后的
/// ExchangePosition 列表（不落库）。
pub async fn query_exchange_positions(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Result<Response, ApiError> {
    let req: ExchangePositionsRequest = parse_json(&body)?;

    // 账户解析：所已注册 + 账户精确匹配（同时拿 account_id 与 test_net）。
    let resolved = state
        .resolve_trading_account(&req.exchange, &req.account)
        .map_err(ApiError::account_not_configured)?;

    // 查仓专用接口缺省按合约（futures）：当前实际持仓查询几乎都是合约；
    // 仅本入口如此，不改全局 normalize_instrument_type 的 spot 默认。
    // 显式传 spot/option 等仍照常生效。
    let instrument_type = req.instrument_type.as_deref().or(Some("futures"));

    // connect（失败 → 50001）。
    let adapter = state
        .exchanges
        .connect(&req.exchange, instrument_type, &req.account)
        .map_err(|e| ApiError::new(BizError::ExchangeFailure, format!("连接交易所失败: {e}")))?;

    // 实时拉取（失败 → 50001）。
    let infos = adapter
        .get_positions()
        .await
        .map_err(|e| ApiError::new(BizError::ExchangeFailure, format!("查询持仓失败: {e}")))?;

    // 经 PositionEngine 同一套 mapper 映射（不落库）。
    let server_time_ms = now_ms();
    let positions = state.position_engine.map_exchange_positions(
        resolved.id,
        &req.exchange,
        &infos,
        server_time_ms,
    );
    let list: Vec<PositionDTO> = positions.iter().map(PositionDTO::from).collect();
    let data = serde_json::json!({
        "account": req.account,
        "exchange": req.exchange,
        "server_time_ms": server_time_ms,
        "list": list,
    });
    Ok(axum::Json(ApiResponse::ok(data)).into_response())
}

#[cfg(test)]
mod tests {
    use axum::http::{Request, StatusCode};
    use axum::Router;
    use tower::ServiceExt;

    use crate::api::test_support::app_with_accounts;

    async fn post(router: &Router, uri: &str, body: &str) -> (StatusCode, serde_json::Value) {
        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
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
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
        )
    }

    #[tokio::test]
    async fn query_returns_50001_when_exchange_unreachable() {
        // Arrange：binance 账户（测试无真实端点：connect/调用失败 → 50001）。
        let app = app_with_accounts("0,test_binance,binance,K,s,,one_way,0,0,0,0\n").await;
        let router: Router = crate::api::router().with_state(app.state.clone());

        // Act
        let (status, v) = post(
            &router,
            "/api/v1/exchange/positions",
            r#"{"exchange":"binance","account":"test_binance"}"#,
        )
        .await;

        // Assert：连不上/调用失败 → 50001。
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(v["code"], 50001);
        assert!(v["data"].is_null());
    }

    #[tokio::test]
    async fn missing_account_returns_40002() {
        let app = app_with_accounts("").await;
        let router = crate::api::router().with_state(app.state.clone());
        let (status, v) = post(
            &router,
            "/api/v1/exchange/positions",
            r#"{"exchange":"binance","account":"ghost"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(v["code"], 40002);
    }

    #[tokio::test]
    async fn bad_json_returns_40001() {
        let app = app_with_accounts("0,test_binance,binance,K,s,,one_way,0,0,0,0\n").await;
        let router = crate::api::router().with_state(app.state.clone());
        let (status, v) = post(&router, "/api/v1/exchange/positions", "{x").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(v["code"], 40001);
    }
}
