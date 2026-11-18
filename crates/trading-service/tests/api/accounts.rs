//! `GET /api/v1/accounts`：账户安全投影（仅非敏感四字段）与过滤。

use axum::http::StatusCode;

use crate::support::{app_with_csv, get_json};

const TWO_ACCOUNTS: &str = "0,a1,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n\
     0,a2,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\",\"\"position_mode\"\":\"\"hedge\"\",\"\"is_testnet\"\":true}\",0,0,0\n\
     0,a3,okx,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n";

/// 列表返回安全投影：每条恰好四个非敏感字段，报文不含凭证/内部列。
#[tokio::test]
async fn accounts_list_returns_safe_projection() {
    // Arrange
    let app = app_with_csv(TWO_ACCOUNTS).await;

    // Act
    let (status, v) = get_json(&app.router, "/api/v1/accounts").await;

    // Assert
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["code"], 0);
    let list = v["data"]["list"].as_array().unwrap();
    assert_eq!(list.len(), 3);
    for item in list {
        let mut keys: Vec<_> = item.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        assert_eq!(
            keys,
            vec!["exchange", "is_testnet", "name", "position_mode"]
        );
    }
    let raw = v.to_string();
    for leaked in ["api_key", "api_secret", "\"K\"", "\"s\"", "deleted"] {
        assert!(!raw.contains(leaked), "账户投影疑似泄漏 {leaked}: {raw}");
    }
}

/// 过滤参数按 AND 组合：exchange / name / is_testnet / position_mode。
#[tokio::test]
async fn accounts_filter_by_exchange_name_and_testnet() {
    let app = app_with_csv(TWO_ACCOUNTS).await;

    async fn count(app: &crate::support::TestApi, uri: &str) -> usize {
        let (status, v) = get_json(&app.router, uri).await;
        assert_eq!(status, StatusCode::OK);
        v["data"]["list"].as_array().unwrap().len()
    }

    assert_eq!(count(&app, "/api/v1/accounts?exchange=okx").await, 1);
    assert_eq!(count(&app, "/api/v1/accounts?exchange=binance").await, 2);
    assert_eq!(count(&app, "/api/v1/accounts?is_testnet=true").await, 1);
    assert_eq!(count(&app, "/api/v1/accounts?position_mode=hedge").await, 1);
    assert_eq!(count(&app, "/api/v1/accounts?name=a1").await, 1);
    // 组合：binance + one_way = 1（a1）。
    assert_eq!(
        count(
            &app,
            "/api/v1/accounts?exchange=binance&position_mode=one_way"
        )
        .await,
        1
    );
}
