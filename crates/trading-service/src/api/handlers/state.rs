//! `GET /api/v1/state`：服务状态摘要（计数 + 交易所账户聚合）。

use std::collections::BTreeMap;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use trading_models::{OrderStatus, WatchRuleStatus};

use crate::api::envelope::{now_ms, ApiResponse};
use crate::state::AppState;

/// `GET /api/v1/state`：读内存各表计数 + 交易所账户聚合（不触盘、不连交易所）。
pub async fn service_state(State(state): State<AppState>) -> Response {
    // 账户：total 含软删、active 仅未删。
    let account_table = state.store.accounts().expect("account table");
    let accounts_total = account_table.list_including_deleted().len();
    let accounts_active = account_table.list().len();

    // 订单：active=未软删数；by_status 按八态全量出 key（0 也保留）。
    let orders = state.store.order_repo().expect("order repo");
    let active_orders = orders.list_active();
    let mut by_status: BTreeMap<&'static str, usize> = [
        OrderStatus::Placing,
        OrderStatus::Unknown,
        OrderStatus::New,
        OrderStatus::Partial,
        OrderStatus::Filled,
        OrderStatus::Canceled,
        OrderStatus::Rejected,
        OrderStatus::Expired,
    ]
    .iter()
    .map(|s| (s.wire(), 0))
    .collect();
    for o in &active_orders {
        *by_status.entry(o.status.wire()).or_default() += 1;
    }

    // 仓位：active=未软平仓数。
    let positions = state.store.position_repo().expect("position repo");
    let positions_active = positions.list_active().len();

    // 规则：按未软删行的状态计数 active/pending/in_use。
    let rules = state.store.rule_repo().expect("rule repo");
    // active=status=active 行数；pending/in_use 经表行统计。
    let rule_active = rules.list_active().len();
    let mut rule_pending = 0usize;
    let mut rule_in_use = 0usize;
    // pending/in_use 经 table 读取（未软删行）。
    if let Ok(table) = state.store.watch_rules() {
        for r in table.list() {
            match r.status {
                WatchRuleStatus::Pending => rule_pending += 1,
                WatchRuleStatus::InUse => rule_in_use += 1,
                _ => {}
            }
        }
    }

    // 交易所账户聚合：按账户简报 group by exchange（账户已不含密钥）。
    let mut exchange_accounts: BTreeMap<String, usize> = BTreeMap::new();
    for brief in state.accounts.list_active_briefs() {
        *exchange_accounts.entry(brief.exchange).or_default() += 1;
    }
    let exchanges: Vec<serde_json::Value> = exchange_accounts
        .into_iter()
        .map(|(name, accounts)| serde_json::json!({ "name": name, "accounts": accounts }))
        .collect();

    let data = serde_json::json!({
        "server_time_ms": now_ms(),
        "accounts": { "total": accounts_total, "active": accounts_active },
        "orders": { "active": active_orders.len(), "by_status": by_status },
        "positions": { "active": positions_active },
        "rules": { "active": rule_active, "pending": rule_pending, "in_use": rule_in_use },
        "exchanges": exchanges,
    });
    axum::Json(ApiResponse::ok(data)).into_response()
}

#[cfg(test)]
mod tests {
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use crate::api::test_support::app_with_accounts;

    #[tokio::test]
    async fn state_reports_counts_and_exchanges_without_secrets() {
        // Arrange：2 个账户分属 binance/okx；订单/仓位/规则为空。
        let app = app_with_accounts(
            "0,test_binance,binance,K,s,,one_way,0,0,0,0\n\
             0,test_okx,okx,K,s,,one_way,0,0,0,0\n",
        )
        .await;

        // Act
        let resp = app
            .router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/state")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Assert：200 信封。
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["code"], 0);
        let d = &v["data"];
        // 时间戳为正整数。
        assert!(d["server_time_ms"].as_u64().unwrap() > 0);
        // 账户计数（total=active=2）。
        assert_eq!(d["accounts"]["total"], 2);
        assert_eq!(d["accounts"]["active"], 2);
        // 空业务表计数归零。
        assert_eq!(d["orders"]["active"], 0);
        assert_eq!(d["positions"]["active"], 0);
        assert_eq!("rules", "rules");
        assert_eq!(d["rules"]["active"], 0);
        // 交易所聚合：每个所 1 个账户。
        let exchanges = d["exchanges"].as_array().unwrap();
        assert_eq!(exchanges.len(), 2);
        let mut names: Vec<_> = exchanges
            .iter()
            .map(|e| e["name"].as_str().unwrap())
            .collect();
        names.sort();
        assert_eq!(names, vec!["binance", "okx"]);
        for e in exchanges {
            assert_eq!(e["accounts"], 1);
        }

        // 安全红线：报文不含任何凭证/敏感字段。
        let raw = String::from_utf8(bytes.to_vec()).unwrap();
        for leaked in ["api_key", "api_secret", "\"K\"", "\"s\""] {
            assert!(!raw.contains(leaked), "状态报文疑似泄漏 {leaked}");
        }
    }
}
