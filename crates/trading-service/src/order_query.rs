//! 订单查询核心（不依赖 axum / HTTP 层），REST `GET /orders` 与南向 WS
//! `get_order` 共用，避免两条入口各写一套"过滤 + 历史回源"。
//!
//! 入参 [`OrderQuery`] 是纯参数结构（定义在 `api::filters`，本身不依赖 HTTP）。
//! 返回匹配的订单（含已软删 tombstone），精确查询（cid 或 signal_id）在内存
//! miss 时自动回源历史归档 CSV。

use crate::api::filters::OrderQuery;
use crate::state::AppState;
use trading_models::Order;

/// 查询失败：账户/交易所无法解析（调用方各自映射为 REST 40002 或 WS error 帧）。
/// 与"查无匹配"（Ok(空)）区分开，避免把无法解析吞成空结果。
#[derive(Debug)]
pub(crate) struct QueryError(pub String);

/// 按查询条件取订单。
///
/// - 普通条件查询：返回内存（含当日 tombstone）中全部谓词匹配行；
/// - `client_order_id` 精确查询：内存 miss → 历史归档按 cid 回查；
/// - `signal_id`（配合 `strategy_name`）精确查询：内存 miss → 历史归档按
///   (strategy, signal) 回查。
///
/// 给了 `account` 时必须能解析（连同 `exchange`），解析失败返回
/// [`Err(QueryError)`]；未给账户则不解析、不按账户过滤。
pub(crate) async fn query_orders(
    state: &AppState,
    q: OrderQuery,
) -> Result<Vec<Order>, QueryError> {
    let account = q.account.clone();
    let exchange = q.exchange.clone();
    let by_cid = q.client_order_id.clone();
    let by_signal = q.signal_id.clone();
    let by_strategy = q.strategy_name.clone();

    // 按 account 名解析 account_id（与 REST handler 同口径）。
    let resolved_id = match (exchange.as_deref(), account.as_deref()) {
        (Some(exch), Some(name)) => Some(
            state
                .resolve_trading_account(exch, name)
                .map_err(QueryError)?
                .id,
        ),
        _ => None,
    };

    let filter = q
        .to_filter(resolved_id)
        .map_err(|e| QueryError(e.message))?;

    let table = state
        .store
        .orders()
        .map_err(|e| QueryError(e.to_string()))?;
    let matched: Vec<Order> = table
        .list_including_deleted()
        .into_iter()
        .filter(|o| filter.matches(o))
        .collect();

    // 精确查询：内存（含当日 tombstone）命中直接返回；miss 才回源历史归档。
    if let Some(cid) = by_cid {
        return Ok(or_else_empty(matched, || {
            state.store.history().find_order(account.as_deref(), &cid)
        }));
    }
    if let (Some(signal_id), Some(strategy)) = (by_signal, by_strategy) {
        return Ok(or_else_empty(matched, || {
            state
                .store
                .history()
                .find_order_by_signal(account.as_deref(), &strategy, &signal_id)
        }));
    }

    Ok(matched)
}

/// `rows` 非空原样返回；为空则用 `fallback` 取（历史回源）。
fn or_else_empty(rows: Vec<Order>, fallback: impl FnOnce() -> Option<Order>) -> Vec<Order> {
    if !rows.is_empty() {
        rows
    } else {
        fallback().into_iter().collect()
    }
}
