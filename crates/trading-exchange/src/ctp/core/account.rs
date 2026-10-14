//! CTP 账户/凭证与连接参数。

/// 账户字段校验错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AccountError {
    /// 经纪公司代码为空。
    #[error("经纪公司代码为空")]
    EmptyBrokerId,
    /// 用户代码为空。
    #[error("用户代码为空")]
    EmptyUserId,
    /// 密码为空。
    #[error("密码为空")]
    EmptyPassword,
    /// AppID 为空。
    #[error("AppID 为空")]
    EmptyAppId,
    /// 认证码为空。
    #[error("认证码为空")]
    EmptyAuthCode,
    /// 前置地址为空。
    #[error("前置地址为空")]
    EmptyFront,
}

/// CTP 账户凭证与交易前置参数。
///
/// 包含看穿式认证所需的 AppID/AuthCode。`Debug` 刻意脱敏密码与授权码，
/// 避免日志或 panic 信息泄漏。
#[derive(Clone)]
pub struct CtpAccount {
    /// 经纪公司代码，SimNow 为 "9999"。
    pub broker_id: String,
    /// 用户代码（登录账号）。
    pub user_id: String,
    /// 登录密码。
    pub password: String,
    /// 客户端认证 AppID。
    pub app_id: String,
    /// 客户端认证授权码。
    pub auth_code: String,
    /// 投资者代码；多数账户与 user_id 相同，空则回退 user_id。
    pub investor_id: String,
    /// 交易前置地址，如 "tcp://182.254.243.31:40001"。
    pub front_address: String,
    /// 前置环境：评测版合并 API（v6.7.11+）的**实例级**开关——false=SimNow
    /// 等仿真，true=生产柜台。归账户所有（随 credentials JSON），创建 API
    /// 对象时决定且此后不可更改；环境切换经停旧会话起新会话（重建对象）实现。
    pub is_production_mode: bool,
}

impl std::fmt::Debug for CtpAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CtpAccount")
            .field("broker_id", &self.broker_id)
            .field("user_id", &self.user_id)
            .field("password", &"<redacted>")
            .field("app_id", &self.app_id)
            .field("auth_code", &"<redacted>")
            .field("investor_id", &self.investor_id)
            .field("front_address", &self.front_address)
            .field("is_production_mode", &self.is_production_mode)
            .finish()
    }
}

impl CtpAccount {
    /// 边界校验：必填字段不得为空白。
    pub fn validate(&self) -> std::result::Result<(), AccountError> {
        if self.broker_id.trim().is_empty() {
            return Err(AccountError::EmptyBrokerId);
        }
        if self.user_id.trim().is_empty() {
            return Err(AccountError::EmptyUserId);
        }
        if self.password.is_empty() {
            return Err(AccountError::EmptyPassword);
        }
        if self.app_id.trim().is_empty() {
            return Err(AccountError::EmptyAppId);
        }
        if self.auth_code.trim().is_empty() {
            return Err(AccountError::EmptyAuthCode);
        }
        if self.front_address.trim().is_empty() {
            return Err(AccountError::EmptyFront);
        }
        Ok(())
    }

    /// 生效的投资者代码：investor_id 非空用之，否则回退 user_id。
    pub fn effective_investor_id(&self) -> &str {
        if self.investor_id.trim().is_empty() {
            &self.user_id
        } else {
            &self.investor_id
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account() -> CtpAccount {
        CtpAccount {
            broker_id: "9999".into(),
            user_id: "123456".into(),
            password: "secret".into(),
            app_id: "simnow_client_test".into(),
            auth_code: "0000000000000000".into(),
            investor_id: "123456".into(),
            front_address: "tcp://182.254.243.31:40001".into(),
            is_production_mode: false,
        }
    }

    #[test]
    fn fully_populated_account_is_valid() {
        assert!(account().validate().is_ok());
    }

    #[test]
    fn empty_broker_id_is_rejected() {
        let mut a = account();
        a.broker_id = " ".into();
        assert_eq!(a.validate().unwrap_err(), AccountError::EmptyBrokerId);
    }

    #[test]
    fn empty_user_id_is_rejected() {
        let mut a = account();
        a.user_id = String::new();
        assert_eq!(a.validate().unwrap_err(), AccountError::EmptyUserId);
    }

    #[test]
    fn empty_password_is_rejected() {
        let mut a = account();
        a.password = String::new();
        assert_eq!(a.validate().unwrap_err(), AccountError::EmptyPassword);
    }

    #[test]
    fn empty_app_id_is_rejected() {
        let mut a = account();
        a.app_id = String::new();
        assert_eq!(a.validate().unwrap_err(), AccountError::EmptyAppId);
    }

    #[test]
    fn empty_auth_code_is_rejected() {
        let mut a = account();
        a.auth_code = String::new();
        assert_eq!(a.validate().unwrap_err(), AccountError::EmptyAuthCode);
    }

    #[test]
    fn empty_front_address_is_rejected() {
        let mut a = account();
        a.front_address = String::new();
        assert_eq!(a.validate().unwrap_err(), AccountError::EmptyFront);
    }

    #[test]
    fn investor_id_defaults_to_user_id_when_blank() {
        assert_eq!(account().effective_investor_id(), "123456");
    }

    #[test]
    fn debug_does_not_leak_password_or_auth_code() {
        let dbg = format!("{:?}", account());
        assert!(!dbg.contains("secret"));
        assert!(!dbg.contains("0000000000000000"));
    }
}
