//! 管理 API（预留）：运行期运维触发入口，挂在南向同一个本地 HTTP 服务上。
//!
//! 当前仅提供账户重载：
//! * `POST /admin/accounts/reload` —— 重新读取并 reconcile account.csv；
//!   成功后账户管理器递增重载体，交易所接入层的周期对账循环据此把变更账户的
//!   用户数据流连接停旧起新（改密/切测试网/新增/禁用均无需重启进程）。
//!
//! 设计说明：本服务不做文件轮询热加载，重载只由显式触发；连接的实际
//! 增删改由交易所接入层的共享状态对账完成，handler 不直接操作连接。
//! 默认只绑定本地回环地址，鉴权（管理令牌）在后续迭代补上。

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

use crate::state::AppState;

/// 统一应答信封：success + data(accounts) + error。
#[derive(Debug, Serialize)]
struct ReloadReply {
    success: bool,
    /// 本次 reconcile 写入的账户行数。
    accounts: usize,
    error: Option<String>,
}

/// `POST /admin/accounts/reload`：触发账户配置重载。
pub async fn reload_accounts(State(state): State<AppState>) -> Response {
    match state.accounts.reload() {
        Ok(n) => {
            tracing::info!(
                accounts = n,
                "账户重载成功（管理 API 触发，连接将在对账周期重建）"
            );
            (
                StatusCode::OK,
                Json(ReloadReply {
                    success: true,
                    accounts: n,
                    error: None,
                }),
            )
                .into_response()
        }
        Err(e) => {
            // 解析失败时账户管理器保留旧表（不半更新），返回 5xx 与原因。
            tracing::error!(error = %e, "账户重载失败，保留当前内存账户表");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ReloadReply {
                    success: false,
                    accounts: 0,
                    error: Some(e.to_string()),
                }),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 信封序列化字段钉死：success/accounts/error 三段式。
    #[test]
    fn reply_envelope_shape() {
        let ok = serde_json::to_value(ReloadReply {
            success: true,
            accounts: 2,
            error: None,
        })
        .unwrap();
        assert_eq!(ok["success"], true);
        assert_eq!(ok["accounts"], 2);
        assert!(ok["error"].is_null());
    }
}
