//! `POST /api/v1/exchange/balance`：实时查询交易所账户余额（不落库）。

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use trading_exchange::api::BalanceInfo;

use crate::api::dto::balance::BalanceDTO;
use crate::api::envelope::{now_ms, parse_json, ApiError, ApiResponse, BizError};
use crate::state::AppState;

/// 请求体。
#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeBalanceRequest {
    pub exchange: String,
    pub account: String,
    /// 适配器产品选择；缺省 futures，与 `POST /api/v1/exchange/positions` 同口径。
    #[serde(default)]
    pub instrument_type: Option<String>,
}

/// `POST /api/v1/exchange/balance`。
///
/// 手动解析 body（坏 JSON 40001）。账户解析（所已注册+精确匹配）失败 40002；
/// connect 或 get_balance 失败属交易所调用问题 → 50001。返回该账户当前余额，
/// 按结算币种一行（Deribit 多币种多行），不落库。
pub async fn query_exchange_balance(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Result<Response, ApiError> {
    let req: ExchangeBalanceRequest = parse_json(&body)?;

    // 账户解析：所已注册 + 账户精确匹配。只做合法性校验，响应不回显 account_id。
    state
        .resolve_trading_account(&req.exchange, &req.account)
        .map_err(ApiError::account_not_configured)?;

    // 与实时查仓同口径：产品缺省 futures；Deribit 等需显式传 option。
    let instrument_type = req.instrument_type.as_deref().or(Some("futures"));
    let adapter = state
        .exchanges
        .connect(&req.exchange, instrument_type, &req.account)
        .map_err(|e| ApiError::new(BizError::ExchangeFailure, format!("连接交易所失败: {e}")))?;
    let balances = adapter
        .get_balance()
        .await
        .map_err(|e| ApiError::new(BizError::ExchangeFailure, format!("查询余额失败: {e}")))?;

    let data = build_balance_data(&req.exchange, &req.account, now_ms(), &balances);
    Ok(axum::Json(ApiResponse::ok(data)).into_response())
}

/// 组装成功应答 data：回显 exchange/account/server_time_ms，集合键固定 `balances`。
fn build_balance_data(
    exchange: &str,
    account: &str,
    server_time_ms: u64,
    balances: &[BalanceInfo],
) -> serde_json::Value {
    let list: Vec<BalanceDTO> = balances.iter().map(BalanceDTO::from).collect();
    serde_json::json!({
        "exchange": exchange,
        "account": account,
        "server_time_ms": server_time_ms,
        "balances": list,
    })
}

#[cfg(test)]
mod tests {
    use axum::http::{Request, StatusCode};
    use axum::Router;
    use rust_decimal::Decimal;
    use tower::ServiceExt;
    use trading_exchange::api::BalanceInfo;

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
    async fn bad_json_returns_40001() {
        let app = app_with_accounts("").await;
        let router: Router = crate::api::router().with_state(app.state.clone());
        let (status, v) = post(&router, "/api/v1/exchange/balance", "{x").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(v["code"], 40001);
    }

    #[tokio::test]
    async fn missing_account_returns_40002() {
        let app = app_with_accounts("").await;
        let router = crate::api::router().with_state(app.state.clone());
        let (status, v) = post(
            &router,
            "/api/v1/exchange/balance",
            r#"{"exchange":"binance","account":"ghost"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(v["code"], 40002);
    }

    #[tokio::test]
    async fn omitted_instrument_type_defaults_to_futures_before_connect() {
        // Arrange：Deribit 账户，不传 instrument_type。
        // Deribit build_api 只接受 option，缺省 futures 会在**连接阶段**同步失败，
        // 不产生任何外网请求——正好用来验证缺省值确实是 futures。
        // 合成凭证（非真实密钥）：ApiSecretCreds 要求 api_key/api_secret 非空，
        // 否则账户被拒载、解析先行 40002，测不到产品路由。
        let app = app_with_accounts(
            "0,test_deribit,deribit,\"{\"\"api_key\"\":\"\"k\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n",
        )
        .await;
        let router = crate::api::router().with_state(app.state.clone());

        // Act
        let (status, v) = post(
            &router,
            "/api/v1/exchange/balance",
            r#"{"exchange":"deribit","account":"test_deribit"}"#,
        )
        .await;

        // Assert：50001，且错误来自产品路由（futures 不被 Deribit 接受）。
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(v["code"], 50001);
        assert!(
            v["message"]
                .as_str()
                .unwrap_or_default()
                .contains("仅支持期权"),
            "缺省 instrument_type 应为 futures：{}",
            v["message"]
        );
    }

    #[test]
    fn reply_data_echoes_identifiers_and_uses_balances_key() {
        // Arrange：两行多币种余额（Deribit 形态）。
        let balances = vec![
            BalanceInfo {
                currency: "BTC".into(),
                equity: Decimal::from(1),
                available: Decimal::from(1),
                margin_used: None,
                unrealized_pnl: None,
                update_ms: None,
            },
            BalanceInfo {
                currency: "ETH".into(),
                equity: Decimal::from(2),
                available: Decimal::from(2),
                margin_used: None,
                unrealized_pnl: None,
                update_ms: None,
            },
        ];

        // Act
        let data = super::build_balance_data("deribit", "main", 1_731_081_600_000, &balances);

        // Assert：回显 exchange/account/server_time_ms；集合键为 balances；全部币种原样返回。
        assert_eq!(data["exchange"], "deribit");
        assert_eq!(data["account"], "main");
        assert_eq!(data["server_time_ms"], 1_731_081_600_000u64);
        let rows = data["balances"].as_array().expect("balances 应为数组");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["currency"], "BTC");
        assert_eq!(rows[1]["currency"], "ETH");
        // 不回显 account_id。
        assert!(data.get("account_id").is_none());
    }
}
