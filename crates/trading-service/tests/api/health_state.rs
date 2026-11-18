//! `GET /api/v1/state`：服务状态摘要（计数 + 交易所聚合，不泄露凭证）。

use axum::http::StatusCode;

use crate::support::{app_with_csv, get_json};

/// 两个账户分属 binance/okx；空业务表计数归零，报文不含任何凭证。
#[tokio::test]
async fn state_reports_counts_and_exchanges_without_secrets() {
    // Arrange
    let app = app_with_csv(
        "0,a1,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n\
         0,a2,okx,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n",
    )
    .await;

    // Act
    let (status, v) = get_json(&app.router, "/api/v1/state").await;

    // Assert：200 + 信封 + 计数。
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["code"], 0);
    let d = &v["data"];
    assert!(d["server_time_ms"].as_u64().unwrap() > 0);
    assert_eq!(d["accounts"]["total"], 2);
    assert_eq!(d["accounts"]["active"], 2);
    assert_eq!(d["orders"]["active"], 0);
    assert_eq!(d["positions"]["active"], 0);
    assert_eq!(d["rules"]["active"], 0);

    // 交易所聚合：每个所 1 个账户。
    let mut names: Vec<_> = d["exchanges"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, vec!["binance", "okx"]);

    // 安全红线：报文不含凭证。
    let raw = v.to_string();
    for leaked in ["api_key", "api_secret"] {
        assert!(!raw.contains(leaked), "状态报文疑似泄漏 {leaked}");
    }
}
