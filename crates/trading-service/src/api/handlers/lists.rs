//! 订单/规则/持仓列表查询与单条持仓查询 handler。
//!
//! handler 只做：账户名解析（→account_id）→ Query 转 Filter → 取全量表行 →
//! 零 IO 谓词过滤 → id 倒序分页 → DTO 信封。业务逻辑不在此层。

use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};

use crate::api::dto::order::OrderDTO;
use crate::api::dto::position::PositionDTO;
use crate::api::dto::rule::RuleDTO;
use crate::api::envelope::{paginate, ApiError, ApiResponse, Page};
use crate::api::filters::{OrderQuery, PositionQuery, RuleQuery};
use crate::state::AppState;

/// 解析账户名为 account_id：
/// * 未给 account → None（不按账户过滤）；
/// * 给了 exchange → 走账户解析（所已注册 + 账户精确匹配），失败回 40002；
/// * 未给 exchange → 只按账户名取 id。
fn resolve_account_id(
    state: &AppState,
    account: Option<&str>,
    exchange: Option<&str>,
) -> Result<Option<u64>, ApiError> {
    let Some(name) = account.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    match exchange {
        Some(venue) => state
            .resolve_trading_account(venue, name)
            .map(|a| Some(a.id))
            .map_err(ApiError::account_not_configured),
        None => Ok(state.accounts.account_id_by_name(name)),
    }
}

/// 统一分页信封。
fn page_reply<T: serde::Serialize + Clone>(
    rows: Vec<T>,
    total: usize,
    params: crate::api::envelope::PageParams,
) -> Response {
    axum::Json(ApiResponse::ok(Page {
        list: rows,
        total,
        page: params.page,
        page_size: params.page_size,
    }))
    .into_response()
}

/// `GET /api/v1/orders`：订单条件查询（cid/signal 精确查单时不做分页）。
///
/// HTTP 层只负责：解析 query 参数 → 调不依赖 HTTP 的查询核心
/// （`crate::order_query::query_orders`）→ 分页封装。过滤与历史回源在 core，
/// 与南向 WS `get_order` 共用。
pub async fn list_orders(
    State(state): State<AppState>,
    // 用 serde_qs 解析：支持 status[]=NEW / 重复 key 填 Vec（axum 默认 Query
    // 基于 serde_urlencoded，无法反序列化 Vec，会报错或静默丢弃 status 参数）。
    serde_qs::axum::QsQuery(query): serde_qs::axum::QsQuery<OrderQuery>,
) -> Result<Response, ApiError> {
    // 精确查询（cid 或 signal_id+strategy）命中至多 1 条、不分页。
    let exact = query.client_order_id.is_some()
        || (query.signal_id.is_some() && query.strategy_name.is_some());
    let params = crate::api::envelope::page_params(query.page, query.page_size)?;

    // core 报错（账户/所无法解析）→ 40002；查无匹配 → 正常空列表。
    let orders = crate::order_query::query_orders(&state, query)
        .await
        .map_err(|e| ApiError::account_not_configured(e.0))?;
    let mut matched: Vec<OrderDTO> = orders.iter().map(OrderDTO::from).collect();

    // 精确查询忽略分页，直接返回（至多 1 条）。
    if exact {
        let total = matched.len();
        return Ok(page_reply(matched, total, params));
    }

    // 普通条件查询：id 倒序分页。
    let (rows, total) = paginate(matched, params, |dto| dto.id);
    matched = rows;
    Ok(page_reply(matched, total, params))
}

/// `GET /api/v1/rules`：规则条件查询（默认不含 retired）。
pub async fn list_rules(
    State(state): State<AppState>,
    Query(query): Query<RuleQuery>,
) -> Result<Response, ApiError> {
    let (page, page_size) = (query.page, query.page_size);
    // id 精确查询不做分页（与 orders 的 cid 查询同口径）。
    let by_id = query.id;
    // account 提前取出：to_filter 会消费 query，但回源还要用账户名。
    let account = query.account.clone();
    let resolved = resolve_account_id(&state, query.account.as_deref(), query.exchange.as_deref())?;
    let filter = query.to_filter(resolved)?;
    let params = crate::api::envelope::page_params(page, page_size)?;

    let table = state
        .store
        .watch_rules()
        .map_err(|e| ApiError::internal(e.to_string()))?;
    let matched: Vec<_> = table
        .list_including_deleted()
        .into_iter()
        .filter(|r| filter.matches(r))
        .map(|r| RuleDTO::from(&r))
        .collect();

    // id 精确查询：内存 miss 时回源历史归档，命中至多 1 条、忽略分页。
    if let Some(id) = by_id {
        let matched = if matched.is_empty() {
            state
                .store
                .history()
                .find_rule(account.as_deref(), id)
                .map(|r| RuleDTO::from(&r))
                .into_iter()
                .collect()
        } else {
            matched
        };
        let total = matched.len();
        return Ok(page_reply(matched, total, params));
    }
    let (rows, total) = paginate(matched, params, |dto| dto.id);
    Ok(page_reply(rows, total, params))
}

/// `GET /api/v1/positions`：内存持仓条件查询（deleted 不传=不筛选）。
pub async fn list_positions(
    State(state): State<AppState>,
    Query(query): Query<PositionQuery>,
) -> Result<Response, ApiError> {
    let (page, page_size) = (query.page, query.page_size);
    // 走与 WS get_positions 共用的本地查询（不调交易所 API）；错误映射回与原
    // REST 路线一致的错误码（40002/40001/50000）。
    let positions = crate::position_query::query_positions(&state, query)
        .await
        .map_err(|e| match e {
            crate::position_query::QueryError::Account(m) => ApiError::account_not_configured(m),
            crate::position_query::QueryError::BadRequest(m) => ApiError::bad_request(m),
            crate::position_query::QueryError::Internal(m) => ApiError::internal(m),
        })?;
    let params = crate::api::envelope::page_params(page, page_size)?;

    let matched: Vec<_> = positions.iter().map(PositionDTO::from).collect();
    let (rows, total) = paginate(matched, params, |dto| dto.id);
    Ok(page_reply(rows, total, params))
}

/// `GET /api/v1/positions/{id}`：单条持仓；活跃内存 miss 时回源历史归档
/// （当日软平行、跨日已 purge 行均可读回），查无才 data=null。
pub async fn get_position(State(state): State<AppState>, Path(id): Path<u64>) -> Response {
    let repo = state.store.position_repo().ok();
    let dto = repo
        .as_ref()
        .and_then(|r| r.list_active().into_iter().find(|p| p.id == id))
        // 活跃内存 miss：历史归档按 id 回查（id 全局唯一，不限制账户目录）。
        .or_else(|| state.store.history().find_position(None, id))
        .map(|p| PositionDTO::from(&p));
    axum::Json(ApiResponse::ok_optional(dto)).into_response()
}

#[cfg(test)]
mod tests {
    use axum::http::{Request, StatusCode};
    use axum::Router;
    use rust_decimal::Decimal;
    use std::fs;
    use tower::ServiceExt;
    use trading_models::{
        Order, OrderType, Position, Side, WatchConditionKind, WatchRule, WatchRuleAction,
        WatchRuleStatus,
    };

    use crate::api::test_support::app_with_accounts;

    /// 起空账户应用并往各表插合成行，返回 router + state。
    async fn seeded_app() -> (Router, crate::state::AppState) {
        let app = app_with_accounts("").await;
        let state = app.state.clone();

        // 3 条订单（account_id=1，cid 合法且互异）。
        let orders = state.store.orders().unwrap();
        for (i, cid) in [
            "0x0123456789abcdef0123456789abcdef",
            "0x1123456789abcdef0123456789abcdef",
            "0x2123456789abcdef0123456789abcdef",
        ]
        .iter()
        .enumerate()
        {
            let mut o = Order::new(1, *cid, "binance", "BTCUSDT", Side::Buy);
            o.instrument_type = trading_models::InstrumentType::Futures;
            // 第 3 条标记不同标的，用于过滤断言。
            if i == 2 {
                o.instrument = "ETHUSDT".into();
            }
            orders.insert(o).unwrap();
        }

        // 2 条活跃持仓（BTC 多、ETH 空）。
        let positions = state.store.positions().unwrap();
        positions
            .insert(Position::new(1, "binance", "BTCUSDT", Side::Buy))
            .unwrap();
        positions
            .insert(Position::new(1, "binance", "ETHUSDT", Side::Sell))
            .unwrap();

        // 2 条 active 风控规则。
        let rules = state.store.rule_repo().unwrap();
        for _ in 0..2 {
            rules
                .insert(WatchRule::new_risk(
                    1,
                    0,
                    WatchConditionKind::Roi,
                    None,
                    ">=",
                    Decimal::ONE,
                    WatchRuleAction::UseSelf,
                    OrderType::Market,
                    Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
                    Decimal::ZERO,
                    Decimal::ONE,
                ))
                .unwrap();
        }

        (crate::api::router().with_state(state.clone()), state)
    }

    /// 发 GET，返回 (状态码, JSON)。
    async fn get_json(router: &Router, uri: &str) -> (StatusCode, serde_json::Value) {
        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn orders_list_paginates_by_id_desc() {
        let (router, _) = seeded_app().await;

        // Act：第 1 页、2 条。
        let (status, v) = get_json(&router, "/api/v1/orders?account_id=1&page_size=2").await;

        // Assert：200 信封 + 分页体。
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["code"], 0);
        let d = &v["data"];
        assert_eq!(d["total"], 3);
        assert_eq!(d["page"], 1);
        assert_eq!(d["page_size"], 2);
        let list = d["list"].as_array().unwrap();
        assert_eq!(list.len(), 2);
        // id 倒序：第一条 id 大于第二条。
        let first = list[0]["id"].as_u64().unwrap();
        let second = list[1]["id"].as_u64().unwrap();
        assert!(first > second);
    }

    #[tokio::test]
    async fn orders_list_filters_by_instrument() {
        let (router, _) = seeded_app().await;
        let (status, v) = get_json(&router, "/api/v1/orders?account_id=1&instrument=ETHUSDT").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["data"]["total"], 1);
        assert_eq!(v["data"]["list"][0]["instrument"], "ETHUSDT");
    }

    #[tokio::test]
    async fn rules_list_returns_active_rules_id_desc() {
        let (router, _) = seeded_app().await;
        let (status, v) = get_json(&router, "/api/v1/rules?account_id=1").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["data"]["total"], 2);
        let list = v["data"]["list"].as_array().unwrap();
        assert!(list[0]["id"].as_u64().unwrap() > list[1]["id"].as_u64().unwrap());
        // 默认不含 retired；condition_name 只读串存在。
        assert_eq!(list[0]["condition_name"], "roi");
    }

    #[tokio::test]
    async fn positions_list_returns_active_rows() {
        let (router, _) = seeded_app().await;
        let (status, v) = get_json(&router, "/api/v1/positions?account_id=1").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["data"]["total"], 2);
        let instruments: Vec<_> = v["data"]["list"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["instrument"].as_str().unwrap())
            .collect();
        // 多空两仓各自独立成行。
        assert!(instruments.contains(&"BTCUSDT"));
        assert!(instruments.contains(&"ETHUSDT"));
        // 不暴露 qty_unit。
        assert!(v["data"]["list"][0].get("qty_unit").is_none());
    }

    #[tokio::test]
    async fn position_get_by_id_returns_dto_or_null() {
        let (router, state) = seeded_app().await;
        let id = state.store.position_repo().unwrap().list_active()[0].id;

        // 命中：200 data 为 PositionDTO。
        let (status, v) = get_json(&router, &format!("/api/v1/positions/{id}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["data"]["id"], id);

        // 不存在：200 data=null。
        let (_, v) = get_json(&router, "/api/v1/positions/999999").await;
        assert!(v["data"].is_null());
    }

    #[tokio::test]
    async fn account_name_resolution_returns_40002_when_missing() {
        // 账户名 + 所解析：本应用无账户，解析失败回 40002。
        let app = app_with_accounts("").await;
        let router = crate::api::router().with_state(app.state.clone());
        let (status, v) =
            get_json(&router, "/api/v1/orders?account=missing&exchange=binance").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(v["code"], 40002);
        assert!(v["data"].is_null());
    }

    // ---- 历史回查：内存 miss 时自动读历史归档 CSV ----

    /// 在 store 根目录写一份历史日 CSV（账户目录名 test_binance，与账户名一致）。
    fn write_history_csv<T: serde::Serialize>(
        state: &crate::state::AppState,
        day: &str,
        model: &str,
        rows: &[T],
    ) {
        let dir = state.store.root().join(day).join("test_binance");
        fs::create_dir_all(&dir).unwrap();
        let mut wtr = csv::Writer::from_path(dir.join(format!("{model}.csv"))).unwrap();
        for row in rows {
            wtr.serialize(row).unwrap();
        }
        wtr.flush().unwrap();
    }

    fn historical_order(cid: &str, id: u64) -> Order {
        let mut o = Order::new(1, cid, "binance", "BTCUSDT", Side::Buy);
        o.id = id;
        o.status = trading_models::OrderStatus::Filled;
        o
    }

    #[tokio::test]
    async fn order_query_by_cid_falls_back_to_history() {
        // Arrange：内存无此单，历史日 CSV 里有。
        let app = app_with_accounts("1,test_binance,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n").await;
        let router = crate::api::router().with_state(app.state.clone());
        let cid = "0x000000000000000000000000000000aa";
        write_history_csv(
            &app.state,
            "2026-09-20",
            "order",
            &[historical_order(cid, 9)],
        );

        // Act：按 cid 查询自动回源历史。
        let (status, v) = get_json(&router, &format!("/api/v1/orders?client_order_id={cid}")).await;

        // Assert。
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["data"]["total"], 1);
        assert_eq!(v["data"]["list"][0]["id"], 9);
        assert_eq!(v["data"]["list"][0]["status"], "FILLED");
    }

    #[tokio::test]
    async fn order_query_by_signal_falls_back_to_history() {
        // Arrange：内存无此单，历史日 CSV 里按 (strategy, signal) 可定位。
        let app = app_with_accounts("1,test_binance,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n").await;
        let router = crate::api::router().with_state(app.state.clone());
        let cid = "0x000000000000000000000000000000ab";
        let mut o = historical_order(cid, 9);
        o.strategy_name = Some("gridA".into());
        o.signal_id = Some("sig-1".into());
        write_history_csv(&app.state, "2026-09-20", "order", &[o]);

        // Act：signal_id + strategy_name 查询自动回源。
        let (status, v) = get_json(
            &router,
            "/api/v1/orders?signal_id=sig-1&strategy_name=gridA",
        )
        .await;

        // Assert。
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["data"]["total"], 1);
        assert_eq!(v["data"]["list"][0]["id"], 9);
    }

    #[tokio::test]
    async fn position_get_by_id_falls_back_to_history() {
        // Arrange：持仓只存在历史日、已软平。
        let app = app_with_accounts("1,test_binance,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n").await;
        let router = crate::api::router().with_state(app.state.clone());
        let mut p = Position::new(1, "binance", "BTCUSDT", Side::Buy);
        p.id = 13;
        p.deleted = true;
        write_history_csv(&app.state, "2026-09-20", "position", &[p]);

        // Act + Assert：按 id 命中历史持仓。
        let (status, v) = get_json(&router, "/api/v1/positions/13").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["data"]["id"], 13);
        assert_eq!(v["data"]["deleted"], true);
    }

    #[tokio::test]
    async fn rule_query_by_id_falls_back_to_history() {
        // Arrange：规则只存历史日、已 retired。
        let app = app_with_accounts("1,test_binance,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n").await;
        let router = crate::api::router().with_state(app.state.clone());
        let mut r = WatchRule::new_risk(
            1,
            0,
            WatchConditionKind::Roi,
            None,
            ">=",
            Decimal::ONE,
            WatchRuleAction::UseSelf,
            OrderType::Market,
            Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
            Decimal::ZERO,
            Decimal::ONE,
        );
        r.id = 21;
        r.status = WatchRuleStatus::Retired;
        write_history_csv(&app.state, "2026-09-20", "watch_rule", &[r]);

        // Act：按 id 精确查询自动回源。
        let (status, v) = get_json(&router, "/api/v1/rules?id=21").await;

        // Assert。
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["data"]["total"], 1);
        assert_eq!(v["data"]["list"][0]["id"], 21);
        assert_eq!(v["data"]["list"][0]["status"], "retired");
    }
}
