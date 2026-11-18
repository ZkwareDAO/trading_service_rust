//! 规则接口：列表、单条 upsert、批量启停与连锁批量创建。

use axum::http::StatusCode;
use serde_json::json;

use trading_models::{OrderType, Position, Side, WatchConditionKind, WatchRule, WatchRuleAction};

use crate::support::{
    app_with_csv, dec, get_json, post_value, sim_app, TestApi, DEFAULT_INSTRUMENT, SIM_ACCOUNT,
    SIM_EXCHANGE,
};

/// 一条合法的 risk 规则夹具（account_id=1）。
fn risk_rule() -> WatchRule {
    WatchRule::new_risk(
        1,
        0,
        WatchConditionKind::Roi,
        None,
        ">=",
        dec("1"),
        WatchRuleAction::UseSelf,
        OrderType::Market,
        dec("0"),
        dec("0"),
        dec("1"),
    )
}

/// 空账户应用 + 2 条 active 规则。
async fn seeded_rules() -> TestApi {
    let app = app_with_csv("").await;
    let rules = app.state.store.rule_repo().expect("rule repo");
    rules.insert(risk_rule()).expect("insert rule 1");
    rules.insert(risk_rule()).expect("insert rule 2");
    app
}

/// 在 sim 应用里种一条活跃仓位，返回 (app, position_id, account_id)。
async fn sim_with_position() -> (crate::support::SimApi, u64, u64) {
    let sim = sim_app().await;
    let account_id = sim
        .api
        .state
        .resolve_trading_account(SIM_EXCHANGE, SIM_ACCOUNT)
        .expect("sim 账户可解析")
        .id;
    let repo = sim.api.state.store.position_repo().expect("position repo");
    repo.upsert_matching(vec![Position::new(
        account_id,
        SIM_EXCHANGE,
        DEFAULT_INSTRUMENT,
        Side::Buy,
    )])
    .expect("seed position");
    let position = repo
        .list_active()
        .into_iter()
        .find(|p| p.instrument == DEFAULT_INSTRUMENT)
        .expect("种子仓位可见");
    (sim, position.id, account_id)
}

/// 列表：默认只回 active，按 id 倒序，带只读 condition_name。
#[tokio::test]
async fn rules_list_returns_active_rules() {
    let app = seeded_rules().await;

    let (status, v) = get_json(&app.router, "/api/v1/rules?account_id=1").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["code"], 0);
    assert_eq!(v["data"]["total"], 2);
    let list = v["data"]["list"].as_array().unwrap();
    assert!(list[0]["id"].as_u64().unwrap() > list[1]["id"].as_u64().unwrap());
    assert_eq!(list[0]["condition_name"], "roi");
    assert_eq!(list[0]["status"], "active");
}

/// risk 规则创建：只认 position_id，回显绑定仓位与规则类型。
#[tokio::test]
async fn rules_create_risk_binds_position() {
    let (sim, position_id, _) = sim_with_position().await;

    let (status, v) = post_value(
        &sim.api.router,
        "/api/v1/rules",
        &json!({
            "account": SIM_ACCOUNT,
            "exchange": SIM_EXCHANGE,
            "rule_type": "risk",
            "position_id": position_id,
            "condition_kind": "roi",
            "operator": ">=",
            "value": "0.12",
            "order_type": "market",
            "quantity_pct": "1",
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["code"], 0);
    assert!(v["data"]["id"].as_u64().unwrap() > 0);
    assert_eq!(v["data"]["rule_type"], "risk");
    assert_eq!(v["data"]["position_id"], position_id);
    assert_eq!(v["data"]["condition_kind"], "roi");
}

/// new 规则创建后带 id 重提 → 同一 id 更新（upsert 语义）。
#[tokio::test]
async fn rules_create_new_and_resubmit_updates() {
    let (sim, _, _) = sim_with_position().await;
    let body = |id: Option<u64>, value: &str| {
        json!({
            "id": id,
            "account": SIM_ACCOUNT,
            "exchange": SIM_EXCHANGE,
            "rule_type": "new",
            "instrument": DEFAULT_INSTRUMENT,
            "instrument_type": "futures",
            "side": "buy",
            "condition_kind": "price",
            "operator": "<=",
            "value": value,
            "order_type": "limit",
            "price": "80000",
            "quantity": "0.1",
            "leverage": 3,
        })
    };

    // 创建（id 缺省 = 新建）
    let (status, created) =
        post_value(&sim.api.router, "/api/v1/rules", &body(None, "80000")).await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let id = created["data"]["id"].as_u64().unwrap();
    assert!(id > 0);

    // 带 id 重提不同阈值 → 同一 id、阈值被更新。
    let (status, updated) =
        post_value(&sim.api.router, "/api/v1/rules", &body(Some(id), "79000")).await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["data"]["id"], id);
    assert_eq!(updated["data"]["value"].as_f64(), Some(79000.0));
    // 仍只有一条规则。
    let (_, list) = get_json(&sim.api.router, "/api/v1/rules").await;
    assert_eq!(list["data"]["total"], 1);
}

/// 批量启停：retired 后默认列表不可见，active 后恢复可见。
#[tokio::test]
async fn rules_status_batch_retire_and_reactivate() {
    let app = seeded_rules().await;
    let id = app.state.store.rule_repo().unwrap().list_active()[0].id;

    // 停用
    let (status, v) = post_value(
        &app.router,
        "/api/v1/rules/status",
        &json!({ "ids": [id], "status": "retired" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["data"]["updated"], 1);
    assert_eq!(v["data"]["items"][0]["id"], id);
    assert_eq!(v["data"]["items"][0]["status"], "retired");
    assert_eq!(v["data"]["items"][0]["ok"], true);
    let (_, list) = get_json(&app.router, "/api/v1/rules?account_id=1").await;
    assert_eq!(list["data"]["total"], 1);

    // 重新启用（items 形态）
    let (status, v) = post_value(
        &app.router,
        "/api/v1/rules/status",
        &json!({ "items": [{ "id": id, "status": "active" }] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["data"]["updated"], 1);
    let (_, list) = get_json(&app.router, "/api/v1/rules?account_id=1").await;
    assert_eq!(list["data"]["total"], 2);
}

/// 连锁批量创建：前向 ref 在第二遍回填为真实 rule:<id>。
#[tokio::test]
async fn rules_batch_create_two_pass_links_actions() {
    let (sim, position_id, _) = sim_with_position().await;

    let (status, v) = post_value(
        &sim.api.router,
        "/api/v1/rules/batch",
        &json!({
            "rules": [
                {
                    "ref": "take_profit",
                    "account": SIM_ACCOUNT,
                    "exchange": SIM_EXCHANGE,
                    "rule_type": "risk",
                    "position_id": position_id,
                    "condition_kind": "roi",
                    "operator": ">=",
                    "value": "0.12",
                    "action": "rule:breakeven",
                    "order_type": "market",
                    "quantity_pct": "1",
                },
                {
                    "ref": "breakeven",
                    "account": SIM_ACCOUNT,
                    "exchange": SIM_EXCHANGE,
                    "rule_type": "risk",
                    "position_id": position_id,
                    "condition_kind": "price",
                    "operator": "<=",
                    "value": "80000",
                    "action": "self",
                    "order_type": "market",
                    "quantity_pct": "1",
                    "pending": true,
                },
            ]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["code"], 0);
    let list = v["data"]["list"].as_array().unwrap();
    assert_eq!(list.len(), 2);
    let successor_id = list[1]["id"].as_u64().unwrap();
    assert_eq!(list[0]["action"], format!("rule:{successor_id}"));
    assert_eq!(list[1]["action"], "self");
}
