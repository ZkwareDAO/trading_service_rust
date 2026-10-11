//! 仓位查询核心（不依赖 axum / HTTP 层），REST `GET /positions` 与南向 WS
//! `get_positions` 共用，避免两条入口各写一套"账户解析 + 本地过滤"。
//!
//! 入参 [`PositionQuery`] 是纯参数结构（定义在 `api::filters`，本身不依赖 HTTP）。
//! 数据**只来自本地仓位表**（含当日已软平行），不调用交易所 API、不做跨日历史
//! 归档回源；返回匹配的仓位，由 REST / WS 各自做分页或事件帧封装。

use crate::api::filters::PositionQuery;
use crate::state::AppState;
use trading_models::Position;

/// 查询失败。区分类别，使 REST 能映射为与原来一致的错误码、WS 只取消息：
#[derive(Debug)]
pub(crate) enum QueryError {
    /// 账户/所无法解析（REST 40002 account_not_configured）。
    Account(String),
    /// 过滤参数非法（side/instrument_type/deleted，REST 40001 bad_request）。
    BadRequest(String),
    /// 本地仓位表读取失败（REST 50000 internal）。
    Internal(String),
}

impl QueryError {
    /// 错误原文（WS 不区分类别，统一进 error 帧的 message）。
    pub(crate) fn message(&self) -> &str {
        match self {
            QueryError::Account(m) | QueryError::BadRequest(m) | QueryError::Internal(m) => m,
        }
    }
}

/// 按查询条件取本地仓位。
///
/// - 给了 `account` 时：连同 `exchange` 解析成 account_id（有 exchange 走
///   账户+所精确解析，无 exchange 按账户名全局解析）；
/// - 未给账户则不解析、不按账户过滤；
/// - 其余字段（exchange/instrument/instrument_type/side/min_roi/max_roi/deleted）
///   传了才筛选，不传不处理。
///
/// 无匹配返回空 `Vec`。
pub(crate) async fn query_positions(
    state: &AppState,
    q: PositionQuery,
) -> Result<Vec<Position>, QueryError> {
    // 解析 account_id（口径与 REST list_positions 的 resolve_account_id 一致）。
    let resolved_id = resolve_account_id(state, q.account.as_deref(), q.exchange.as_deref())?;
    // to_filter 的解析失败恒为 40001（side/instrument_type/deleted 非法）。
    let filter = q
        .to_filter(resolved_id)
        .map_err(|e| QueryError::BadRequest(e.message))?;

    let table = state
        .store
        .positions()
        .map_err(|e| QueryError::Internal(e.to_string()))?;
    let matched: Vec<Position> = table
        .list_including_deleted()
        .into_iter()
        .filter(|p| filter.matches(p))
        .collect();
    Ok(matched)
}

/// 账户名 → account_id：有 exchange 走账户+所精确解析；无 exchange 按名全局解析；
/// 未给账户名返回 None（=不按账户过滤）。
fn resolve_account_id(
    state: &AppState,
    account: Option<&str>,
    exchange: Option<&str>,
) -> Result<Option<u64>, QueryError> {
    let Some(name) = account.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    match exchange {
        Some(venue) => Ok(Some(
            state
                .resolve_trading_account(venue, name)
                .map_err(|e| QueryError::Account(e.to_string()))?
                .id,
        )),
        None => Ok(state.accounts.account_id_by_name(name)),
    }
}
