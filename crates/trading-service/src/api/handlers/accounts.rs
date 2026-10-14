//! `GET /api/v1/accounts`：账户安全投影查询（不分页）。

use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use crate::api::dto::account::AccountDTO;
use crate::api::envelope::ApiResponse;
use crate::state::AppState;

/// 账户查询参数（全部可选、AND 组合）。
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AccountQueryParams {
    pub exchange: Option<String>,
    pub name: Option<String>,
    /// query 直接收 bool（true/false）。
    pub is_testnet: Option<bool>,
    pub position_mode: Option<trading_models::PositionMode>,
}

/// `GET /api/v1/accounts`：读内存活动账户简报，过滤后做安全投影。
pub async fn list_accounts(
    State(state): State<AppState>,
    Query(params): Query<AccountQueryParams>,
) -> Response {
    // 账户数量少，全量取活动简报后内存过滤（不触盘、不分页）。
    let list: Vec<AccountDTO> = state
        .accounts
        .list_active_briefs()
        .into_iter()
        .filter(|a| params.exchange.as_deref().is_none_or(|v| v == a.exchange))
        .filter(|a| params.name.as_deref().is_none_or(|v| v == a.name))
        .filter(|a| params.is_testnet.is_none_or(|v| v == a.is_testnet))
        .filter(|a| params.position_mode.is_none_or(|v| v == a.position_mode))
        // 简报不含密钥，统一走 AccountDTO 安全投影。
        .map(|brief| AccountDTO::from(&brief))
        .collect();
    axum::Json(ApiResponse::ok(serde_json::json!({ "list": list }))).into_response()
}

#[cfg(test)]
mod tests {
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use crate::api::test_support::{app_with_accounts, ACCOUNT_HEADER};

    #[tokio::test]
    async fn lists_active_accounts_safe_fields_only() {
        // Arrange：两个活动账户（hedge 测试网、one_way 正式网）。
        let app = app_with_accounts(
            "0,hedger,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\",\"\"position_mode\"\":\"\"hedge\"\",\"\"is_testnet\"\":true}\",0,0,0\n\
             0,plain,binance,K2,s2,,one_way,0,0,0,0\n",
        )
        .await;

        // Act
        let resp = app
            .router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/accounts")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Assert：200 + 标准信封。
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["code"], 0);
        assert_eq!(v["message"], "success");
        let list = v["data"]["list"].as_array().unwrap();
        assert_eq!(list.len(), 2);
        // 每条恰好 4 个非敏感字段。
        for item in list {
            let mut keys: Vec<_> = item.as_object().unwrap().keys().cloned().collect();
            keys.sort();
            assert_eq!(
                keys,
                vec!["exchange", "is_testnet", "name", "position_mode"]
            );
        }
        // 全报文不含密钥/内部字段。
        let raw = String::from_utf8(bytes.to_vec()).unwrap();
        for leaked in [
            "\"K\"",
            "\"s\"",
            "\"K2\"",
            "\"s2\"",
            "api_key",
            "api_secret",
            "\"id\"",
            "deleted",
            "created_at",
        ] {
            assert!(!raw.contains(leaked), "疑似泄漏 {leaked}: {raw}");
        }
    }

    #[tokio::test]
    async fn filters_by_exchange_name_testnet_and_position_mode() {
        // Arrange
        let app = app_with_accounts(
            "0,a1,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n\
             0,a2,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\",\"\"position_mode\"\":\"\"hedge\"\",\"\"is_testnet\"\":true}\",0,0,0\n\
             0,a3,okx,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n",
        )
        .await;

        // helper：发 GET 请求，返回 data.list 长度。
        let count = |state: crate::state::AppState, uri: String| async move {
            let r = crate::api::router()
                .with_state(state)
                .oneshot(
                    Request::builder()
                        .uri(uri)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(r.status(), StatusCode::OK);
            let b = axum::body::to_bytes(r.into_body(), usize::MAX)
                .await
                .unwrap();
            let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
            v["data"]["list"].as_array().unwrap().len()
        };

        // Act / Assert
        assert_eq!(
            count(app.state.clone(), "/api/v1/accounts?exchange=okx".into()).await,
            1
        );
        assert_eq!(
            count(app.state.clone(), "/api/v1/accounts?is_testnet=true".into()).await,
            1
        );
        assert_eq!(
            count(
                app.state.clone(),
                "/api/v1/accounts?position_mode=hedge".into()
            )
            .await,
            1
        );
        assert_eq!(
            count(app.state.clone(), "/api/v1/accounts?name=a1".into()).await,
            1
        );
        // 组合：binance + one_way = 1（a1）。
        assert_eq!(
            count(
                app.state.clone(),
                "/api/v1/accounts?exchange=binance&position_mode=one_way".into()
            )
            .await,
            1
        );
    }

    #[tokio::test]
    async fn empty_account_table_returns_empty_list_envelope() {
        // Arrange：无账户文件。
        let app = app_with_accounts("").await;
        // Act
        let resp = app
            .router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/accounts")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // Assert
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["data"]["list"], serde_json::json!([]));
    }

    // 钉住表头常量与生产列一致（防测试 CSV 与真实格式漂移）。
    #[test]
    fn account_header_constant_shape() {
        assert_eq!(
            ACCOUNT_HEADER,
            "id,name,exchange,credentials,deleted,created_at,updated_at\n"
        );
    }
}
