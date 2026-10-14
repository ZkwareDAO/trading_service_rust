//! 规则写入：`POST /api/v1/rules`（创建/更新）与
//! `POST /api/v1/rules/status`（批量启停）。

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::Json;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::api::dto::rule::RuleDTO;
use crate::api::envelope::{parse_json, ApiError, ApiResponse, BizError};
use crate::rules::{build_rule, RuleCreateInput};
use crate::state::AppState;
use trading_models::WatchRule;

/// `POST /api/v1/rules` 请求体（文档 §4.6.1）。
#[derive(Debug, Clone, Deserialize)]
pub struct UpsertRuleRequestDto {
    /// 带 id=更新；缺省/0=创建。
    #[serde(default)]
    pub id: Option<u64>,
    /// 批量创建时本条的客户端 ref 名（供其它条目 `rule:<ref>` 引用）。
    #[serde(default, rename = "ref")]
    pub client_ref: Option<String>,
    #[serde(default = "crate::api::handlers::orders::default_account_name")]
    pub account: String,
    pub rule_type: Option<String>,
    #[serde(default)]
    pub exchange: Option<String>,
    #[serde(default)]
    pub instrument: Option<String>,
    #[serde(default)]
    pub instrument_type: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub position_id: Option<u64>,
    #[serde(default)]
    pub condition_kind: Option<String>,
    #[serde(default)]
    pub condition_instrument: Option<String>,
    #[serde(default)]
    pub operator: Option<String>,
    #[serde(default)]
    pub value: Option<Decimal>,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub order_type: Option<String>,
    #[serde(default)]
    pub quantity: Option<Decimal>,
    /// new 规则现金口径：与 quantity 二选一。
    #[serde(default)]
    pub order_cash: Option<Decimal>,
    #[serde(default)]
    pub quantity_pct: Option<Decimal>,
    #[serde(default)]
    pub leverage: Option<u64>,
    #[serde(default)]
    pub price: Option<Decimal>,
    #[serde(default)]
    pub pending: bool,
}

/// 解析规则类型（缺省 risk）。
fn parse_rule_type(raw: Option<&str>) -> Result<trading_models::WatchRuleType, ApiError> {
    Ok(
        match raw.unwrap_or("risk").trim().to_ascii_lowercase().as_str() {
            "risk" => trading_models::WatchRuleType::Risk,
            "new" => trading_models::WatchRuleType::New,
            other => return Err(ApiError::bad_request(format!("rule_type 非法：`{other}`"))),
        },
    )
}

/// 解析条件主类（缺省 roi；drawdown 为别名）。
fn parse_condition_kind(raw: Option<&str>) -> Result<trading_models::WatchConditionKind, ApiError> {
    Ok(
        match raw.unwrap_or("roi").trim().to_ascii_lowercase().as_str() {
            "roi" => trading_models::WatchConditionKind::Roi,
            "price" => trading_models::WatchConditionKind::Price,
            "position" => trading_models::WatchConditionKind::Position,
            "profit_drawdown" | "drawdown" => trading_models::WatchConditionKind::ProfitDrawdown,
            other => {
                return Err(ApiError::bad_request(format!(
                    "condition_kind 非法：`{other}`"
                )))
            }
        },
    )
}

/// 构造 [`RuleCreateInput`]（创建字段解析）。`venue` 用于标的归一化。
fn build_create_input(
    state: &AppState,
    resolved_id: u64,
    req: &UpsertRuleRequestDto,
) -> Result<RuleCreateInput, ApiError> {
    let rule_type = parse_rule_type(req.rule_type.as_deref())?;
    let condition_kind = parse_condition_kind(req.condition_kind.as_deref())?;
    let itype = crate::api::handlers::orders::parse_itype_pub(req.instrument_type.as_deref())?;
    let venue = req
        .exchange
        .clone()
        .unwrap_or_else(|| "binance".to_string());
    let order_type = match req.order_type.as_deref() {
        Some(s) => trading_models::OrderType::from_wire(s),
        None => trading_models::OrderType::Market,
    };

    // 标的/方向随类型区分（与 WS create_rule 同口径）：
    // new：instrument + side 必填；risk：只认 position_id。
    let (instrument, open_side, position_id) = match rule_type {
        trading_models::WatchRuleType::New => {
            let raw_inst = req
                .instrument
                .as_deref()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| ApiError::bad_request("new 规则缺少 instrument"))?;
            // 按归属所归一化标的（binance→大写）。
            let raw_inst = state.exchanges.normalize_instrument(&venue, raw_inst);
            let side = req
                .side
                .as_deref()
                .ok_or_else(|| ApiError::bad_request("new 规则缺少 side"))?;
            let side = trading_models::Side::from_wire(side)
                .ok_or_else(|| ApiError::bad_request("side 非法"))?;
            (raw_inst.to_string(), Some(side), 0u64)
        }
        trading_models::WatchRuleType::Risk => (
            req.instrument.clone().unwrap_or_default(),
            None,
            req.position_id.unwrap_or(0),
        ),
    };

    let exchange = req.exchange.clone().unwrap_or_default();
    let condition_instrument = req.condition_instrument.clone();
    let operator = req.operator.clone().unwrap_or_default();
    let value = req.value.unwrap_or(Decimal::ZERO);
    let action = match req.action.as_deref() {
        None | Some("self") => trading_models::WatchRuleAction::UseSelf,
        Some(other) => trading_models::WatchRuleAction::parse_wire(other)
            // 批量占位 `rule:<ref>` 此刻无法用强类型表示：先落 UseSelf 占位，
            // 由批量第二遍用真实字符串回填（单条路径不会出现 Ref，已在入口校验拒绝）。
            .unwrap_or(trading_models::WatchRuleAction::UseSelf),
    };

    Ok(RuleCreateInput {
        account_id: resolved_id,
        rule_type,
        position_id,
        exchange,
        instrument,
        instrument_type: itype,
        open_side,
        condition_kind,
        condition_instrument,
        operator,
        value,
        action,
        order_type,
        quantity: req.quantity.unwrap_or(Decimal::ZERO),
        order_cash: req.order_cash.unwrap_or(Decimal::ZERO),
        quantity_pct: req.quantity_pct.unwrap_or(Decimal::ZERO),
        leverage: req.leverage.unwrap_or(1),
        price: req.price.unwrap_or(Decimal::ZERO),
        pending: req.pending,
    })
}

/// `POST /api/v1/rules`：创建或按 id 更新。
pub async fn upsert_rule(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Result<Response, ApiError> {
    let req: UpsertRuleRequestDto = parse_json(&body)?;

    // 账户解析（与下单同口径：所已注册 + 账户精确匹配）。
    let venue = req.exchange.as_deref();
    let resolved = state
        .resolve_trading_account(venue.unwrap_or("binance"), &req.account)
        .map_err(ApiError::account_not_configured)?;

    // 更新路径：身份/绑定字段保持，仅替换可变业务字段。
    if let Some(id) = req.id.filter(|v| *v > 0) {
        return update_existing(&state, id, &req);
    }

    // 单条创建：action 必须可强类型解析（不接受批量占位 rule:<ref>）。
    if let Some(a) = req.action.as_deref() {
        if a != "self" && trading_models::WatchRuleAction::parse_wire(a).is_none() {
            return Err(ApiError::bad_request(
                "action 非法（支持 self 或 rule:<id>）",
            ));
        }
    }

    // 创建路径：复用零 IO build_rule（risk 的仓位存在性校验在此）。
    let positions = state.store.position_repo().expect("position repo");
    let rules = state.store.rule_repo().expect("rule repo");
    let input = build_create_input(&state, resolved.id, &req)?;
    let rule = build_rule(&input, &positions).map_err(|e| ApiError::bad_request(e.to_string()))?;

    // 重设语义（find-or-update）：客户端未必知道已有同身份规则。若存在
    // 未删除且身份相同的规则（同账户/类型/条件/目标标的；risk 含 position_id、
    // new 含 side），复用其 id 走更新，而非追加——避免旧规则仍 active 造成重复。
    if let Some(existing) = rules.find_active_by_identity(&rule.identity()) {
        return update_existing(&state, existing.id, &req);
    }

    let stored = rules
        .insert(rule)
        .map_err(|e| ApiError::internal(e.to_string()))?;
    Ok(Json(ApiResponse::ok(RuleDTO::from(&stored))).into_response())
}

/// 按 id 更新：Table::update 只在闭包内覆盖可变字段，身份/绑定/status/deleted 不动。
fn update_existing(
    state: &AppState,
    id: u64,
    req: &UpsertRuleRequestDto,
) -> Result<Response, ApiError> {
    // 先解析新字段值（身份/绑定不来自此处）。
    let condition_kind = parse_condition_kind(req.condition_kind.as_deref())?;
    let value = req.value;
    let operator = req.operator.clone();
    let order_type = req
        .order_type
        .as_deref()
        .map(trading_models::OrderType::from_wire);
    let price = req.price;
    let quantity = req.quantity;
    let quantity_pct = req.quantity_pct;
    let condition_instrument = req.condition_instrument.clone();
    let action = match req.action.as_deref() {
        Some(other) => trading_models::WatchRuleAction::parse_wire(other)
            .ok_or_else(|| ApiError::bad_request("action 非法"))?,
        None => trading_models::WatchRuleAction::UseSelf,
    };
    let action_string = action_str(&action);

    let table = state
        .store
        .watch_rules()
        .map_err(|e| ApiError::internal(e.to_string()))?;
    let updated = table
        .update(id, move |r| {
            // 仅可变业务字段；id/account_id/rule_type/position_id/status/deleted/时间戳保持。
            r.condition_kind = condition_kind;
            if let Some(v) = value {
                r.value = v;
            }
            if let Some(v) = operator {
                r.operator = v;
            }
            if let Some(v) = order_type {
                r.order_type = v;
            }
            if let Some(v) = price {
                r.price = v;
            }
            if let Some(v) = quantity {
                r.quantity = v;
            }
            if let Some(v) = quantity_pct {
                r.quantity_pct = v;
            }
            if let Some(v) = condition_instrument {
                r.condition_instrument = Some(v);
            }
            r.action = action_string.clone();
        })
        .map_err(|e| ApiError::new(BizError::NotFound, e.to_string()))?;
    Ok(Json(ApiResponse::ok(RuleDTO::from(&updated))).into_response())
}

fn action_str(a: &trading_models::WatchRuleAction) -> String {
    match a {
        trading_models::WatchRuleAction::UseSelf => "self".to_string(),
        trading_models::WatchRuleAction::ActivateRule { rule_id } => format!("rule:{rule_id}"),
    }
}

// =====================================================================
// 批量启停
// =====================================================================

/// `POST /api/v1/rules/status` 请求体（ids 或 items 形态）。
#[derive(Debug, Clone, Deserialize)]
pub struct StatusBatchRequest {
    #[serde(default)]
    pub ids: Vec<u64>,
    #[serde(default)]
    pub items: Vec<StatusItem>,
    #[serde(default)]
    pub status: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StatusItem {
    pub id: u64,
    pub status: String,
}

/// 单条处理结果。
#[derive(Debug, Clone, serde::Serialize)]
struct StatusResultItem {
    id: u64,
    status: String,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// `POST /api/v1/rules/status`。
pub async fn rule_status_batch(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Result<Response, ApiError> {
    let req: StatusBatchRequest = parse_json(&body)?;

    // 归一为 (id,目标状态) 列表：ids 用顶层 status，items 各自带。
    let mut targets: Vec<(u64, String)> =
        req.items.iter().map(|i| (i.id, i.status.clone())).collect();
    if targets.is_empty() {
        let status = req
            .status
            .clone()
            .ok_or_else(|| ApiError::bad_request("缺少 status"))?;
        targets = req.ids.iter().map(|id| (*id, status.clone())).collect();
    }
    if targets.is_empty() {
        return Err(ApiError::bad_request("ids/items 不能为空"));
    }

    let rules = state.store.rule_repo().expect("rule repo");
    let mut results = Vec::new();
    let mut updated = 0usize;
    for (id, target) in targets {
        let outcome = apply_status(&rules, id, &target);
        match outcome {
            Ok(final_status) => {
                updated += 1;
                results.push(StatusResultItem {
                    id,
                    status: final_status,
                    ok: true,
                    error: None,
                });
            }
            Err(e) => results.push(StatusResultItem {
                id,
                status: target,
                ok: false,
                error: Some(e),
            }),
        }
    }

    let data = serde_json::json!({
        "updated": updated,
        "items": results,
    });
    Ok(Json(ApiResponse::ok(data)).into_response())
}

// =====================================================================
// 连锁规则批量创建（§9 两遍落库）
// =====================================================================

/// `POST /api/v1/rules/batch` 请求体。
#[derive(Debug, Clone, Deserialize)]
pub struct BatchRulesRequest {
    pub rules: Vec<UpsertRuleRequestDto>,
}

/// `POST /api/v1/rules/batch`。
///
/// 手动解析 body（坏 JSON 统一 40001）。先零 IO 校验 ref 集合（零落库），
/// 再第一遍逐条 insert（action 占位原样存）、第二遍按 ref→id 映射 update 回填。
pub async fn batch_create_rules(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Result<Response, ApiError> {
    let req: BatchRulesRequest = parse_json(&body)?;
    if req.rules.is_empty() {
        return Err(ApiError::bad_request("rules 不能为空"));
    }

    // —— 零 IO 预校验（§9.5）：ref 名唯一、action 引用目标存在（前向合法）——
    // 未携带 ref 的条目给内部唯一名（不会也不应被引用）。
    let entries: Vec<(String, String)> = req
        .rules
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let declared = r
                .client_ref
                .clone()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| format!("__row{i}"));
            (
                declared,
                r.action.clone().unwrap_or_else(|| "self".to_string()),
            )
        })
        .collect();
    let ref_entries: Vec<(&str, &str)> = entries
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    crate::api::rules_batch::validate_entries(ref_entries).map_err(ApiError::bad_request)?;

    // —— 第一遍：逐条构造并 insert（占位 action 原样存）——
    let positions = state.store.position_repo().expect("position repo");
    let rules = state.store.rule_repo().expect("rule repo");
    let mut inserted: Vec<WatchRule> = Vec::new();
    for rule_req in req.rules.iter() {
        let venue = rule_req
            .exchange
            .clone()
            .unwrap_or_else(|| "binance".to_string());
        let resolved = state
            .resolve_trading_account(&venue, &rule_req.account)
            .map_err(ApiError::account_not_configured)?;
        let input = build_create_input(&state, resolved.id, rule_req)?;
        let rule =
            build_rule(&input, &positions).map_err(|e| ApiError::bad_request(e.to_string()))?;
        // find-or-update：库存中已有同身份（未删）规则时复用，不新增，避免重发翻倍。
        let stored = match rules.find_active_by_identity(&rule.identity()) {
            Some(existing) => existing,
            None => rules
                .insert(rule)
                .map_err(|e| ApiError::internal(e.to_string()))?,
        };
        inserted.push(stored);
    }

    // —— 第二遍：客户端 ref→真实 id 映射，update 回填 action ——
    let mut id_map = crate::api::rules_batch::RefIdMap::new();
    for (i, rule) in inserted.iter().enumerate() {
        // 显式 client_ref 与内部行名都登记（后者不会出现在 action 中）。
        if let Some(cr) = req.rules[i].client_ref.as_deref() {
            id_map.insert(cr, rule.id);
        }
        id_map.insert(&format!("__row{i}"), rule.id);
    }
    let table = state
        .store
        .watch_rules()
        .map_err(|e| ApiError::internal(e.to_string()))?;
    let mut final_rows: Vec<WatchRule> = Vec::new();
    for (i, rule) in inserted.into_iter().enumerate() {
        let rule_req = &req.rules[i];

        // 解析可变字段（与单条 update_existing 同一口径）；身份/绑定字段不动。
        let new_condition_kind = parse_condition_kind(rule_req.condition_kind.as_deref())?;

        // action 允许前向 ref（rule:tp），不在此处强校验；由 backfill 统一解析。
        let raw_action = rule_req.action.as_deref().unwrap_or("self");
        let final_action = id_map.backfill_action(raw_action);

        let updated = table
            .update(rule.id, move |r| {
                // 命中已存在规则：覆盖全部可变业务字段，使重发改参数生效，
                // 行为与单条接口一致。id/account_id/rule_type/position_id/
                // status/deleted/时间戳保持。
                r.condition_kind = new_condition_kind;
                if let Some(v) = rule_req.value {
                    r.value = v;
                }
                if let Some(v) = rule_req.operator.clone() {
                    r.operator = v;
                }
                if let Some(s) = rule_req.order_type.as_deref() {
                    r.order_type = trading_models::OrderType::from_wire(s);
                }
                if let Some(v) = rule_req.price {
                    r.price = v;
                }
                if let Some(v) = rule_req.quantity {
                    r.quantity = v;
                }
                if let Some(v) = rule_req.quantity_pct {
                    r.quantity_pct = v;
                }
                if let Some(v) = rule_req.condition_instrument.clone() {
                    r.condition_instrument = Some(v);
                }
                r.action = final_action.clone();
            })
            .map_err(|e| ApiError::internal(e.to_string()))?;
        final_rows.push(updated);
    }

    // 按入参顺序返回 RuleDTO（非倒序）。
    let list: Vec<RuleDTO> = final_rows.iter().map(RuleDTO::from).collect();
    Ok(Json(ApiResponse::ok(serde_json::json!({ "list": list }))).into_response())
}

/// 应用单条启停：retired→retire（幂等）；active→reactivate；其它状态不支持。
fn apply_status(rules: &trading_store::RuleRepo, id: u64, target: &str) -> Result<String, String> {
    match target.trim().to_ascii_lowercase().as_str() {
        "retired" => {
            rules.retire(id).map_err(|e| e.to_string())?;
            Ok("retired".to_string())
        }
        "active" => {
            rules.reactivate(id).map_err(|e| e.to_string())?;
            Ok("active".to_string())
        }
        other => Err(format!("status 目标不支持：`{other}`")),
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{Request, StatusCode};
    use axum::Router;
    use tower::ServiceExt;

    use crate::api::test_support::app_with_accounts;

    /// 起带一个 binance 账户的应用。
    async fn app() -> Router {
        let app = app_with_accounts("0,test_binance,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n").await;
        crate::api::router().with_state(app.state.clone())
    }

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
    async fn create_risk_rule_binds_position_id() {
        // Arrange：先解析账户真实 id，再建一条该账户持仓拿到 position_id。
        let app = app_with_accounts("0,test_binance,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n").await;
        let account_id = app
            .state
            .accounts
            .resolve("test_binance", Some("binance"))
            .unwrap()
            .id;
        let pos_id = {
            let table = app.state.store.positions().unwrap();
            table
                .insert(trading_models::Position::new(
                    account_id,
                    "binance",
                    "BTCUSDT",
                    trading_models::Side::Buy,
                ))
                .unwrap()
                .id
        };
        let router = crate::api::router().with_state(app.state.clone());

        // Act：创建 risk 规则（只认 position_id，不给 side）。
        let body = format!(
            r#"{{"account":"test_binance","rule_type":"risk","position_id":{pos_id},
                "condition_kind":"roi","operator":">=","value":0.1}}"#
        );
        let (status, v) = post(&router, "/api/v1/rules", &body).await;

        // Assert
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["code"], 0);
        assert_eq!(v["data"]["rule_type"], "risk");
        assert_eq!(v["data"]["position_id"], pos_id);
        assert_eq!(v["data"]["condition_name"], "roi");
        assert_eq!(v["data"]["status"], "active");
    }

    #[tokio::test]
    async fn create_new_rule_requires_side_and_open_fields() {
        let router = app().await;
        let body = r#"{"account":"test_binance","rule_type":"new",
            "exchange":"binance","instrument":"btcusdt","side":"buy",
            "order_type":"limit","price":80000,"quantity":0.1,
            "condition_kind":"price","condition_instrument":"BTCUSDT",
            "operator":">=","value":90000}"#;
        let (status, v) = post(&router, "/api/v1/rules", body).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["data"]["rule_type"], "new");
        assert_eq!(v["data"]["side"], "BUY");
        assert_eq!(v["data"]["exchange"], "binance");
        assert_eq!(v["data"]["instrument"], "BTCUSDT");
    }

    #[tokio::test]
    async fn update_rule_replaces_mutable_fields_only() {
        // Arrange：先创建。
        let router = app().await;
        let created = post(
            &router,
            "/api/v1/rules",
            r#"{"account":"test_binance","rule_type":"new","exchange":"binance",
                "instrument":"btcusdt","side":"buy","order_type":"limit",
                "price":80000,"quantity":0.1,"condition_kind":"price",
                "condition_instrument":"BTCUSDT","operator":">=","value":90000}"#,
        )
        .await
        .1;
        let id = created["data"]["id"].as_u64().unwrap();
        let orig_account = created["data"]["account_id"].clone();

        // Act：带 id 更新（改 value/operator/price）。
        let body = format!(
            r#"{{"id":{id},"account":"test_binance","rule_type":"new",
                "exchange":"binance","instrument":"btcusdt","side":"buy",
                "order_type":"limit","price":79000,"quantity":0.2,
                "operator":"<=","value":0.2}}"#
        );
        let (status, v) = post(&router, "/api/v1/rules", &body).await;

        // Assert：可变字段更新，身份/绑定字段保持。
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["data"]["id"], id);
        assert_eq!(v["data"]["value"], serde_json::json!(0.2));
        assert_eq!(v["data"]["operator"], "<=");
        assert_eq!(v["data"]["price"], serde_json::json!(79000));
        assert_eq!(v["data"]["account_id"], orig_account);
        assert_eq!(v["data"]["rule_type"], "new");
    }

    #[tokio::test]
    async fn resubmit_profit_drawdown_updates_instead_of_appending() {
        // 复现生产：profit_drawdown >=0.05 连发两次相同请求，应更新而非新增。
        let app = app_with_accounts("0,test_binance,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n").await;
        let account_id = app
            .state
            .accounts
            .resolve("test_binance", Some("binance"))
            .unwrap()
            .id;
        let mut pos = trading_models::Position::new(
            account_id,
            "binance",
            "BTCUSDT",
            trading_models::Side::Buy,
        );
        pos.quantity = "10".parse().unwrap();
        let pos_id = app.state.store.positions().unwrap().insert(pos).unwrap().id;
        let router = crate::api::router().with_state(app.state.clone());

        let body = format!(
            r#"{{"account":"test_binance","rule_type":"risk","position_id":{pos_id},
                "condition_kind":"profit_drawdown","operator":">=","value":0.05,
                "quantity":10}}"#
        );
        let (s1, v1) = post(&router, "/api/v1/rules", &body).await;
        assert_eq!(s1, StatusCode::OK);
        let rule_id = v1["data"]["id"].as_u64().unwrap();

        // 第二次完全相同请求
        let (s2, v2) = post(&router, "/api/v1/rules", &body).await;
        assert_eq!(s2, StatusCode::OK);
        assert_eq!(v2["data"]["id"].as_u64().unwrap(), rule_id);
        assert_eq!(app.state.store.rule_repo().unwrap().list_active().len(), 1);
    }

    #[tokio::test]
    async fn different_condition_kind_is_still_a_new_rule() {
        // Arrange
        let app = app_with_accounts("0,test_binance,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n").await;
        let account_id = app
            .state
            .accounts
            .resolve("test_binance", Some("binance"))
            .unwrap()
            .id;
        let pos_id = app
            .state
            .store
            .positions()
            .unwrap()
            .insert(trading_models::Position::new(
                account_id,
                "binance",
                "BTCUSDT",
                trading_models::Side::Buy,
            ))
            .unwrap()
            .id;
        let router = crate::api::router().with_state(app.state.clone());

        // 第一条：roi 条件。
        let b1 = format!(
            r#"{{"account":"test_binance","rule_type":"risk","position_id":{pos_id},"condition_kind":"roi","operator":">=","value":0.1}}"#
        );
        post(&router, "/api/v1/rules", &b1).await;
        // 第二条：同仓位但 condition_kind=price → 身份不同，应新增。
        let b2 = format!(
            r#"{{"account":"test_binance","rule_type":"risk","position_id":{pos_id},"condition_kind":"price","operator":">=","value":90000}}"#
        );
        let (s2, _) = post(&router, "/api/v1/rules", &b2).await;
        assert_eq!(s2, StatusCode::OK);
        assert_eq!(app.state.store.rule_repo().unwrap().list_active().len(), 2);
    }

    #[tokio::test]
    async fn create_rule_bad_json_returns_40001() {
        let router = app().await;
        let (status, v) = post(&router, "/api/v1/rules", "{bad}").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(v["code"], 40001);
    }

    #[tokio::test]
    async fn status_batch_retire_and_reactivate() {
        // Arrange：建一条规则。
        let router = app().await;
        let created = post(
            &router,
            "/api/v1/rules",
            r#"{"account":"test_binance","rule_type":"new","exchange":"binance",
                "instrument":"btcusdt","side":"buy","order_type":"limit",
                "price":80000,"quantity":0.1,"condition_kind":"price",
                "condition_instrument":"BTCUSDT","operator":">=","value":90000}"#,
        )
        .await
        .1;
        let id = created["data"]["id"].as_u64().unwrap();

        // Act：retire（ids 形态）。
        let (status, v) = post(
            &router,
            "/api/v1/rules/status",
            &format!(r#"{{"ids":[{id}],"status":"retired"}}"#),
        )
        .await;
        // Assert
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["data"]["updated"], 1);
        let item = &v["data"]["items"][0];
        assert_eq!(item["id"], id);
        assert_eq!(item["status"], "retired");
        assert!(item["ok"].as_bool().unwrap());

        // 规则已 retired：GET rules 默认查不到。
        let q = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/rules?account_id=1")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let qv: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(q.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(qv["data"]["total"], 0);

        // Act：active（items 形态）→ reactivate。
        let (_, v) = post(
            &router,
            "/api/v1/rules/status",
            &format!(r#"{{"items":[{{"id":{id},"status":"active"}}]}}"#),
        )
        .await;
        assert_eq!(v["data"]["updated"], 1);
        assert_eq!(v["data"]["items"][0]["status"], "active");
    }

    #[tokio::test]
    async fn batch_create_two_pass_inserts_and_backfills_actions() {
        // Arrange：账户 + 一条持仓。
        let app = app_with_accounts("0,test_binance,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n").await;
        let account_id = app
            .state
            .accounts
            .resolve("test_binance", Some("binance"))
            .unwrap()
            .id;
        let pos_id = app
            .state
            .store
            .positions()
            .unwrap()
            .insert(trading_models::Position::new(
                account_id,
                "binance",
                "BTCUSDT",
                trading_models::Side::Buy,
            ))
            .unwrap()
            .id;
        let router = crate::api::router().with_state(app.state.clone());

        // Act：批量 2 条，第 1 条 rule:tp 前向引用第 2 条（ref=tp）。
        let body = format!(
            r#"{{"rules":[
                {{"account":"test_binance","rule_type":"risk","position_id":{pos_id},
                 "operator":">=","value":0.1,"action":"rule:tp"}},
                {{"ref":"tp","account":"test_binance","rule_type":"risk",
                 "position_id":{pos_id},"operator":"<=","value":0.2,"action":"self"}}
            ]}}"#
        );
        let (status, v) = post(&router, "/api/v1/rules/batch", &body).await;

        // Assert：200，2 条结果，顺序与入参一致。
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["code"], 0);
        let list = v["data"]["list"].as_array().unwrap();
        assert_eq!(list.len(), 2);
        let second_id = list[1]["id"].as_u64().unwrap();
        // 第 1 条 action 已回填为真实 rule:<第2条id>。
        assert_eq!(list[0]["action"], format!("rule:{second_id}"));
        assert_eq!(list[1]["action"], "self");
        // 两条均 active，id 倒序不影响本接口（按入参顺序返回）。
        assert_eq!(list[0]["status"], "active");

        // 落库核对：规则表共 2 行，第 1 行 action 指向第 2 行。
        let stored = app.state.store.rule_repo().unwrap();
        // 经 GET rules?include_retired 可查两条。
        assert!(stored.list_active().len() >= 2);
    }

    #[tokio::test]
    async fn batch_resubmit_same_rules_updates_instead_of_duplicating() {
        // Arrange：账户 + 一条持仓。
        let app = app_with_accounts("0,test_binance,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n").await;
        let account_id = app
            .state
            .accounts
            .resolve("test_binance", Some("binance"))
            .unwrap()
            .id;
        let pos_id = app
            .state
            .store
            .positions()
            .unwrap()
            .insert(trading_models::Position::new(
                account_id,
                "binance",
                "BTCUSDT",
                trading_models::Side::Buy,
            ))
            .unwrap()
            .id;
        let router = crate::api::router().with_state(app.state.clone());

        // 同一组 2 条（第1条链式引用第2条 ref=tp）。
        let body = format!(
            r#"{{"rules":[
                {{"account":"test_binance","rule_type":"risk","position_id":{pos_id},
                 "operator":">=","value":0.1,"action":"rule:tp"}},
                {{"ref":"tp","account":"test_binance","rule_type":"risk",
                 "position_id":{pos_id},"operator":"<=","value":0.2,"action":"self"}}
            ]}}"#
        );

        // 第一次提交。
        let (s1, v1) = post(&router, "/api/v1/rules/batch", &body).await;
        assert_eq!(s1, StatusCode::OK);
        let first: Vec<u64> = v1["data"]["list"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["id"].as_u64().unwrap())
            .collect();
        assert_eq!(first.len(), 2);

        // 原样重发一次。
        let (s2, v2) = post(&router, "/api/v1/rules/batch", &body).await;
        assert_eq!(s2, StatusCode::OK);
        let second: Vec<u64> = v2["data"]["list"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["id"].as_u64().unwrap())
            .collect();

        // 断言：id 不变（更新而非新增）。
        assert_eq!(second, first, "重发应复用已有规则 id");

        // 落库核对：规则表仍只有 2 行，没有翻倍。
        assert_eq!(
            app.state.store.rule_repo().unwrap().list_active().len(),
            2,
            "重发同组规则不得产生重复行"
        );
    }

    #[tokio::test]
    async fn resubmit_same_value_updates_instead_of_appending() {
        // 身份完全一致（含 value）时绝不 append：两次相同请求复用同 id 走更新。
        let app = app_with_accounts("0,test_binance,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n").await;
        let account_id = app
            .state
            .accounts
            .resolve("test_binance", Some("binance"))
            .unwrap()
            .id;
        let pos_id = app
            .state
            .store
            .positions()
            .unwrap()
            .insert(trading_models::Position::new(
                account_id,
                "binance",
                "BTCUSDT",
                trading_models::Side::Buy,
            ))
            .unwrap()
            .id;
        let router = crate::api::router().with_state(app.state.clone());

        let body = format!(
            r#"{{"account":"test_binance","rule_type":"risk","position_id":{pos_id},
                "condition_kind":"roi","operator":">=","value":0.1}}"#
        );
        let (s1, v1) = post(&router, "/api/v1/rules", &body).await;
        assert_eq!(s1, StatusCode::OK);
        let rule_id = v1["data"]["id"].as_u64().unwrap();

        let (s2, v2) = post(&router, "/api/v1/rules", &body).await;
        assert_eq!(s2, StatusCode::OK);
        assert_eq!(
            v2["data"]["id"].as_u64().unwrap(),
            rule_id,
            "同身份必须复用同 id"
        );
        assert_eq!(app.state.store.rule_repo().unwrap().list_active().len(), 1);
    }

    #[tokio::test]
    async fn resubmit_changed_value_creates_new_rule() {
        // 身份其余字段一致、仅 value 不同 → 不复用，创建第二条独立规则。
        let app = app_with_accounts("0,test_binance,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n").await;
        let account_id = app
            .state
            .accounts
            .resolve("test_binance", Some("binance"))
            .unwrap()
            .id;
        let pos_id = app
            .state
            .store
            .positions()
            .unwrap()
            .insert(trading_models::Position::new(
                account_id,
                "binance",
                "BTCUSDT",
                trading_models::Side::Buy,
            ))
            .unwrap()
            .id;
        let router = crate::api::router().with_state(app.state.clone());

        let body = format!(
            r#"{{"account":"test_binance","rule_type":"risk","position_id":{pos_id},
                "condition_kind":"roi","operator":">=","value":0.1}}"#
        );
        let (s1, v1) = post(&router, "/api/v1/rules", &body).await;
        assert_eq!(s1, StatusCode::OK);
        let rule_id1 = v1["data"]["id"].as_u64().unwrap();

        // Act：仅 value 0.1 → 0.2，其余条件全部相同。
        let body2 = format!(
            r#"{{"account":"test_binance","rule_type":"risk","position_id":{pos_id},
                "condition_kind":"roi","operator":">=","value":0.2}}"#
        );
        let (s2, v2) = post(&router, "/api/v1/rules", &body2).await;

        // Assert：新 id（append 而非覆盖），两条规则并存且 value 各自保留。
        assert_eq!(s2, StatusCode::OK);
        let rule_id2 = v2["data"]["id"].as_u64().unwrap();
        assert_ne!(rule_id2, rule_id1, "value 不同必须创建新规则");
        assert_eq!(v2["data"]["value"], serde_json::json!(0.2));
        let active = app.state.store.rule_repo().unwrap().list_active();
        assert_eq!(active.len(), 2);
        let values: Vec<_> = active.iter().map(|r| r.value.to_string()).collect();
        assert!(values.contains(&"0.1".to_string()) && values.contains(&"0.2".to_string()));
    }

    #[tokio::test]
    async fn batch_resubmit_changed_value_appends_new_rule() {
        // 批量接口同口径：value 不同 → append 新行；value 相同 → 复用更新。
        let app = app_with_accounts("0,test_binance,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n").await;
        let account_id = app
            .state
            .accounts
            .resolve("test_binance", Some("binance"))
            .unwrap()
            .id;
        let mut pos = trading_models::Position::new(
            account_id,
            "binance",
            "BTCUSDT",
            trading_models::Side::Buy,
        );
        pos.quantity = "10".parse().unwrap();
        let pos_id = app.state.store.positions().unwrap().insert(pos).unwrap().id;
        let router = crate::api::router().with_state(app.state.clone());

        let mk_body = |value: &str| {
            format!(
                r#"{{"rules":[
                    {{"account":"test_binance","rule_type":"risk","position_id":{pos_id},
                     "condition_kind":"roi","operator":">=","value":{value},
                     "quantity":10,"action":"self"}}
                ]}}"#
            )
        };

        // 第一次：value=0.1。
        let (s1, v1) = post(&router, "/api/v1/rules/batch", &mk_body("0.1")).await;
        assert_eq!(s1, StatusCode::OK);
        let id1 = v1["data"]["list"][0]["id"].as_u64().unwrap();

        // 重发：同身份但 value=0.2 → append。
        let (s2, v2) = post(&router, "/api/v1/rules/batch", &mk_body("0.2")).await;
        assert_eq!(s2, StatusCode::OK);
        let id2 = v2["data"]["list"][0]["id"].as_u64().unwrap();
        assert_ne!(id2, id1, "批量下 value 不同必须 append 新规则");
        assert_eq!(app.state.store.rule_repo().unwrap().list_active().len(), 2);

        // 再发：value 回到 0.1 → 与第一条身份一致，复用 id1，不 append。
        let (s3, v3) = post(&router, "/api/v1/rules/batch", &mk_body("0.1")).await;
        assert_eq!(s3, StatusCode::OK);
        assert_eq!(v3["data"]["list"][0]["id"].as_u64().unwrap(), id1);
        assert_eq!(app.state.store.rule_repo().unwrap().list_active().len(), 2);
    }

    #[tokio::test]
    async fn batch_resubmit_same_value_updates_field_with_same_id() {
        // 批量接口：身份完全一致（含 value）→ 绝不 append，复用同 id 更新。
        let app = app_with_accounts("0,test_binance,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n").await;
        let account_id = app
            .state
            .accounts
            .resolve("test_binance", Some("binance"))
            .unwrap()
            .id;
        let mut pos = trading_models::Position::new(
            account_id,
            "binance",
            "BTCUSDT",
            trading_models::Side::Buy,
        );
        pos.quantity = "10".parse().unwrap();
        let pos_id = app.state.store.positions().unwrap().insert(pos).unwrap().id;
        let router = crate::api::router().with_state(app.state.clone());

        let body = format!(
            r#"{{"rules":[
                {{"account":"test_binance","rule_type":"risk","position_id":{pos_id},
                 "condition_kind":"profit_drawdown","operator":">=","value":0.05,
                 "quantity":10,"action":"self"}}
            ]}}"#
        );
        let (s1, v1) = post(&router, "/api/v1/rules/batch", &body).await;
        assert_eq!(s1, StatusCode::OK);
        let id1 = v1["data"]["list"][0]["id"].as_u64().unwrap();

        let (s2, v2) = post(&router, "/api/v1/rules/batch", &body).await;
        assert_eq!(s2, StatusCode::OK);
        assert_eq!(
            v2["data"]["list"][0]["id"].as_u64().unwrap(),
            id1,
            "value 相同且身份一致必须复用同 id"
        );
        assert_eq!(app.state.store.rule_repo().unwrap().list_active().len(), 1);
    }

    #[tokio::test]
    async fn batch_create_bad_ref_returns_40001_before_insert() {
        let router = app().await;
        // 引用不存在的 ref：整批拒绝（零落库）。
        let body = r#"{"rules":[
            {"account":"test_binance","rule_type":"risk","position_id":1,
             "operator":">=","value":0.1,"action":"rule:ghost"}]}"#;
        let (status, v) = post(&router, "/api/v1/rules/batch", body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(v["code"], 40001);
    }

    #[tokio::test]
    async fn batch_create_bad_json_returns_40001() {
        let router = app().await;
        let (status, v) = post(&router, "/api/v1/rules/batch", "{nope}").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(v["code"], 40001);
    }
}
