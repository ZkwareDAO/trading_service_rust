//! 标准响应信封：除 `/health` 外所有 REST 接口统一
//! `{ "code": 0, "message": "success", "data": ... }`。
//!
//! 本模块只含零 IO 的纯类型与纯函数：信封/错误码/分页参数解析/倒序切片，
//! 不接触 store、不连网络，便于独立单测。

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

/// 当前毫秒级 Unix 时间戳。
pub fn now_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}

// =====================================================================
// 标准信封
// =====================================================================

/// 统一应答信封：`code=0` 成功；非 0 业务错误码时 data 为 null。
#[derive(Debug, Clone, Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub code: i32,
    pub message: String,
    pub data: Option<T>,
}

impl<T: Serialize> ApiResponse<T> {
    /// 成功信封（200，code=0）。
    pub fn ok(data: T) -> Self {
        Self {
            code: 0,
            message: "success".to_string(),
            data: Some(data),
        }
    }

    /// data 可为 None 的成功信封（本期"查无此行回 data=null"场景使用）。
    pub fn ok_optional(data: Option<T>) -> Self {
        Self {
            code: 0,
            message: "success".to_string(),
            data,
        }
    }
}

/// 列表分页数据体：`{list,total,page,page_size}`。
#[derive(Debug, Clone, Serialize)]
pub struct Page<T: Serialize> {
    pub list: Vec<T>,
    pub total: usize,
    pub page: usize,
    pub page_size: usize,
}

// =====================================================================
// 业务错误码（草案，随校验迭代落地）
// =====================================================================

/// 业务错误码枚举：使用处只能在字典内取值，避免魔法数字。
///
/// 部分变体（如 StoreFailure）是接口契约登记的预留错误码，即使当前无构造点
/// 也须保留以维持错误码字典完整，故在此允许 dead_code。
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BizError {
    /// JSON 解析失败、必填缺失、枚举无法识别。
    BadRequest = 40001,
    /// 未知交易所、无精确匹配账户。
    AccountNotConfigured = 40002,
    /// cid/id 查不到活动行。
    NotFound = 40004,
    /// 重复 cid、规则状态机非法迁移。
    Conflict = 40901,
    /// 交易所连接/调用失败。
    ExchangeFailure = 50001,
    /// 内存表/Repo 写入失败。
    StoreFailure = 50002,
    /// 未分类内部错误。
    Internal = 50000,
}

impl BizError {
    /// 数字错误码。
    pub fn code(self) -> i32 {
        self as i32
    }

    /// 错误码首位推 HTTP：`5` 开头 → 500；其余（4xxxx）→ 400
    /// （本期不存在/冲突等语义也统一 400，后续再细分 404/409）。
    pub fn http_status(self) -> StatusCode {
        if self.code() / 10000 == 5 {
            StatusCode::INTERNAL_SERVER_ERROR
        } else {
            StatusCode::BAD_REQUEST
        }
    }
}

/// 错误应答体（data 恒为 null）。
#[derive(Debug, Clone, Serialize)]
struct ErrorBody {
    code: i32,
    message: String,
    data: Option<serde_json::Value>,
}

/// 统一业务错误：携带字典内错误码 + 中文原因。
#[derive(Debug, Clone)]
pub struct ApiError {
    pub code: BizError,
    pub message: String,
}

/// 部分构造器对应预留错误码（store_failure）或当前统一走 new 的路径，
/// 属接口契约的一部分，允许 dead_code。
#[allow(dead_code)]
impl ApiError {
    pub fn new(code: BizError, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self::new(BizError::BadRequest, msg)
    }

    pub fn account_not_configured(msg: impl Into<String>) -> Self {
        Self::new(BizError::AccountNotConfigured, msg)
    }

    pub fn conflict(msg: impl Into<String>) -> Self {
        Self::new(BizError::Conflict, msg)
    }

    pub fn exchange_failure(msg: impl Into<String>) -> Self {
        Self::new(BizError::ExchangeFailure, msg)
    }

    pub fn store_failure(msg: impl Into<String>) -> Self {
        Self::new(BizError::StoreFailure, msg)
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self::new(BizError::Internal, msg)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let code = self.code.code();
        let message = self.message;
        // 错误出口统一分级：客户端 4xx 属可预期坏请求（warn）；5xx 为服务故障（error）。
        // 一处收口，调用点无需各自打日志。
        if code / 10000 == 5 {
            tracing::error!(code, error = %message, "REST request failed (5xx)");
        } else {
            tracing::warn!(code, error = %message, "REST request rejected (4xx)");
        }
        let body = ErrorBody {
            code,
            message,
            data: None,
        };
        (self.code.http_status(), Json(body)).into_response()
    }
}

/// 解析请求体 JSON：失败统一收敛为 40001（不依赖 axum `Json` 提取器的 422）。
pub fn parse_json<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, ApiError> {
    serde_json::from_slice(body)
        .map_err(|e| ApiError::bad_request(format!("请求体 JSON 解析失败: {e}")))
}

// =====================================================================
// 分页参数
// =====================================================================

/// 默认每页 50 条。
pub const DEFAULT_PAGE_SIZE: usize = 50;
/// 每页上限 500 条。
pub const MAX_PAGE_SIZE: usize = 500;

/// 分页参数（page 从 1 起）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageParams {
    pub page: usize,
    pub page_size: usize,
}

impl Default for PageParams {
    fn default() -> Self {
        Self {
            page: 1,
            page_size: DEFAULT_PAGE_SIZE,
        }
    }
}

/// 从已解析的 `Option<usize>` 构造分页参数（handler 经 serde 拿到数字时用，
/// 无需再转字符串重解析）。缺省 1 / DEFAULT_PAGE_SIZE；0 非法回 40001；
/// page_size 超上限钳到 MAX_PAGE_SIZE。
pub fn page_params(page: Option<usize>, page_size: Option<usize>) -> Result<PageParams, ApiError> {
    if let Some(0) = page {
        return Err(ApiError::bad_request("page 必须为 >=1 的整数"));
    }
    if let Some(0) = page_size {
        return Err(ApiError::bad_request("page_size 必须为 >=1 的整数"));
    }
    Ok(PageParams {
        page: page.unwrap_or(1),
        page_size: page_size.unwrap_or(DEFAULT_PAGE_SIZE).min(MAX_PAGE_SIZE),
    })
}

/// 按主键提取函数做 id 倒序，再按分页切片。返回 (当前页行, total)。
///
/// 调用方先完成 model→DTO 映射，再用本函数排序分页，DTO 不需要实现任何 trait。
pub fn paginate<T: Clone, F>(rows: Vec<T>, params: PageParams, id_of: F) -> (Vec<T>, usize)
where
    F: Fn(&T) -> u64,
{
    let total = rows.len();
    let mut ordered = rows;
    // id 倒序（最新创建在前）；sort_by_key 稳定，行为确定。
    ordered.sort_by_key(|r| std::cmp::Reverse(id_of(r)));
    let start = (params.page - 1) * params.page_size;
    if start >= total {
        return (Vec::new(), total);
    }
    let end = (start + params.page_size).min(total);
    (ordered[start..end].to_vec(), total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_envelope_shape() {
        // Arrange
        let env = ApiResponse::ok(serde_json::json!({"a": 1}));
        // Act
        let v = serde_json::to_value(&env).unwrap();
        // Assert
        assert_eq!(v["code"], 0);
        assert_eq!(v["message"], "success");
        assert_eq!(v["data"]["a"], 1);
    }

    #[test]
    fn error_envelope_has_code_message_null_data() {
        // Arrange
        let err = ApiError::new(BizError::AccountNotConfigured, "无此账户");
        // Act
        let resp = err.into_response();
        // Assert：HTTP 400，错误体三段式。
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = ErrorBody {
            code: BizError::AccountNotConfigured.code(),
            message: "无此账户".to_string(),
            data: None,
        };
        let v = serde_json::to_value(body).unwrap();
        assert_eq!(v["code"], 40002);
        assert_eq!(v["message"], "无此账户");
        assert!(v["data"].is_null());
    }

    #[test]
    fn http_status_derivation_five_prefix_500_else_400() {
        assert_eq!(
            BizError::ExchangeFailure.http_status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            BizError::Internal.http_status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            BizError::StoreFailure.http_status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(BizError::BadRequest.http_status(), StatusCode::BAD_REQUEST);
        assert_eq!(BizError::Conflict.http_status(), StatusCode::BAD_REQUEST);
        assert_eq!(BizError::NotFound.http_status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            BizError::AccountNotConfigured.http_status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn page_params_defaults_and_clamps_oversize() {
        // 缺省 1 / 50。
        assert_eq!(
            super::page_params(None, None).unwrap(),
            PageParams {
                page: 1,
                page_size: 50
            }
        );
        // 超过 500 钳到 500。
        assert_eq!(
            super::page_params(Some(3), Some(9999)).unwrap(),
            PageParams {
                page: 3,
                page_size: 500
            }
        );
        // 0 非法回 40001。
        assert!(matches!(
            super::page_params(Some(0), None).unwrap_err().code,
            BizError::BadRequest
        ));
        assert!(matches!(
            super::page_params(None, Some(0)).unwrap_err().code,
            BizError::BadRequest
        ));
    }

    #[test]
    fn paginate_orders_by_id_desc_and_slices() {
        // Arrange：id 乱序的简单行。
        let make = |id: u64| (id, format!("row{id}"));
        // Act：每页 2 条第 1 页。
        let (page1, total) = paginate(
            vec![make(2), make(5), make(1)],
            PageParams {
                page: 1,
                page_size: 2,
            },
            |r| r.0,
        );
        // Assert：倒序 [5,2]，total=3。
        assert_eq!(total, 3);
        assert_eq!(page1.iter().map(|r| r.0).collect::<Vec<_>>(), vec![5, 2]);

        // 第 2 页只剩 id=1。
        let (page2, _) = paginate(
            vec![make(2), make(5), make(1)],
            PageParams {
                page: 2,
                page_size: 2,
            },
            |r| r.0,
        );
        assert_eq!(page2.iter().map(|r| r.0).collect::<Vec<_>>(), vec![1]);

        // 超出范围返回空列表（非错误）。
        let (page9, total9) = paginate(
            vec![make(1)],
            PageParams {
                page: 9,
                page_size: 2,
            },
            |r| r.0,
        );
        assert!(page9.is_empty());
        assert_eq!(total9, 1);
    }
}
