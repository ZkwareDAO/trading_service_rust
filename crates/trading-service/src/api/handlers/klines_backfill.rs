//! `POST /admin/klines/backfill`：管理面手动补一段历史 1m K线。
//!
//! 仅 binance/deribit/hyperliquid；回补 bar 只写内存 BarTable（随后续 flush 落 CSV），
//! **不实时推送**。响应走统一信封 `code/message/data`；忙（已有一个手动补在跑）= 40901。
//!
//! 纯逻辑（请求校验 [`validate_request`]、错误映射 [`map_manual_error`]）与 IO 分离，
//! 便于不连真实交易所即可钉死 40001/40002/40901/50001；成功路径为简单信封封装，
//! 端到端取数依赖真实交易所，与其它联网功能一样不在单测内联。

use axum::body::Bytes;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;

use crate::api::envelope::{parse_json, ApiError, ApiResponse};
use crate::kline_backfill::{BackfillError, BackfillResult, ManualError};
use crate::state::AppState;

/// 请求体：交易所 + 原生标的 + 半开区间（UTC 毫秒，`[start_ms, end_ms)`）。
#[derive(Debug, Deserialize)]
pub struct BackfillRequestBody {
    /// 交易所规范名（binance/deribit/hyperliquid）。
    pub exchange: String,
    /// 交易所原生标的（如 BTCUSDT / BTC-PERPETUAL / BTC）。
    pub symbol: String,
    /// 起点（含，UTC 毫秒）。
    pub start_ms: i64,
    /// 终点（不含，UTC 毫秒）。
    pub end_ms: i64,
}

/// 纯校验：trim、非空、非负、start<end。非法统一回 40001。
fn validate_request(req: &BackfillRequestBody) -> Result<(String, String, i64, i64), ApiError> {
    let exchange = req.exchange.trim();
    let symbol = req.symbol.trim();
    if exchange.is_empty() {
        return Err(ApiError::bad_request("exchange 不能为空"));
    }
    if symbol.is_empty() {
        return Err(ApiError::bad_request("symbol 不能为空"));
    }
    if req.start_ms < 0 || req.end_ms < 0 {
        return Err(ApiError::bad_request("start_ms/end_ms 不能为负"));
    }
    if req.end_ms <= req.start_ms {
        return Err(ApiError::bad_request(format!(
            "end_ms({}) 必须大于 start_ms({})",
            req.end_ms, req.start_ms
        )));
    }
    Ok((
        exchange.to_string(),
        symbol.to_string(),
        req.start_ms,
        req.end_ms,
    ))
}

/// 把回补层错误映射到统一业务码：
/// 忙→40901；不支持所→40002；交易所取数失败→50001；坏区间→40001。
fn map_manual_error(err: ManualError) -> ApiError {
    match err {
        ManualError::Busy => ApiError::conflict("已有一个手动回补任务在执行，请稍后再试"),
        ManualError::Backfill(BackfillError::UnsupportedVenue { venue }) => {
            ApiError::account_not_configured(format!("交易所 `{venue}` 不支持历史 K线回补"))
        }
        ManualError::Backfill(BackfillError::BadRange(m)) => ApiError::bad_request(m),
        ManualError::Backfill(BackfillError::Fetch(m)) => ApiError::exchange_failure(m),
    }
}

/// `POST /admin/klines/backfill`。
pub async fn backfill(State(state): State<AppState>, body: Bytes) -> Result<Response, ApiError> {
    let req: BackfillRequestBody = parse_json(&body)?;
    let (exchange, symbol, start_ms, end_ms) = validate_request(&req)?;

    let result: BackfillResult = state
        .kline
        .manual_backfill(&state.exchanges, &exchange, &symbol, start_ms, end_ms)
        .await
        .map_err(map_manual_error)?;

    Ok(Json(ApiResponse::ok(result)).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::envelope::BizError;

    fn req(exchange: &str, symbol: &str, start: i64, end: i64) -> BackfillRequestBody {
        BackfillRequestBody {
            exchange: exchange.to_string(),
            symbol: symbol.to_string(),
            start_ms: start,
            end_ms: end,
        }
    }

    // ---- validate_request：40001 ----

    #[test]
    fn valid_request_trims_and_passes() {
        let (ex, sym, s, e) = validate_request(&req(" binance ", " BTCUSDT ", 100, 200)).unwrap();
        assert_eq!(ex, "binance");
        assert_eq!(sym, "BTCUSDT");
        assert_eq!((s, e), (100, 200));
    }

    #[test]
    fn empty_exchange_or_symbol_is_bad_request() {
        assert_eq!(
            validate_request(&req("  ", "BTCUSDT", 0, 60_000))
                .unwrap_err()
                .code,
            BizError::BadRequest
        );
        assert_eq!(
            validate_request(&req("binance", " ", 0, 60_000))
                .unwrap_err()
                .code,
            BizError::BadRequest
        );
    }

    #[test]
    fn start_ge_end_is_bad_request() {
        assert_eq!(
            validate_request(&req("binance", "X", 100, 100))
                .unwrap_err()
                .code,
            BizError::BadRequest
        );
        assert_eq!(
            validate_request(&req("binance", "X", 200, 100))
                .unwrap_err()
                .code,
            BizError::BadRequest
        );
    }

    #[test]
    fn negative_bounds_are_bad_request() {
        assert_eq!(
            validate_request(&req("binance", "X", -1, 100))
                .unwrap_err()
                .code,
            BizError::BadRequest
        );
    }

    // ---- map_manual_error ----

    #[test]
    fn busy_maps_to_conflict_40901() {
        assert_eq!(map_manual_error(ManualError::Busy).code, BizError::Conflict);
    }

    #[test]
    fn unsupported_venue_maps_to_40002() {
        assert_eq!(
            map_manual_error(ManualError::Backfill(BackfillError::UnsupportedVenue {
                venue: "ctp".into()
            }))
            .code,
            BizError::AccountNotConfigured
        );
    }

    #[test]
    fn fetch_failure_maps_to_50001() {
        assert_eq!(
            map_manual_error(ManualError::Backfill(BackfillError::Fetch("500".into()))).code,
            BizError::ExchangeFailure
        );
    }

    #[test]
    fn bad_range_maps_to_40001() {
        assert_eq!(
            map_manual_error(ManualError::Backfill(BackfillError::BadRange("x".into()))).code,
            BizError::BadRequest
        );
    }
}
