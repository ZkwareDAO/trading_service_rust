//! 账户端口收口（服务层适配器）。
//!
//! [`trading_store::AccountManager`] 是纯账户域（已下沉到 trading-store），不能在
//! store 内为「交易所接入层 / 规则引擎 / 仓位引擎」的 trait 写实现——那会让数据层
//! 反向依赖网络层，也会撞孤儿规则。本 newtype 在服务层集中实现这些出站/反查端口，
//! 全部薄委托给内部管理器：
//!
//! * [`CredentialResolver`]：下单/建连按名取解密凭证；
//! * [`AccountTargets`]：账户连接监督器的期望 (交易所,账户) 目标集；
//! * [`RuleAccountLookup`]：规则引擎按 account_id 反查账户名/测试网；
//! * [`AccountLookup`]：仓位对账按账户名取表主键。
//!
//! 经 [`Deref`] 直接暴露管理器的其余方法（resolve/reload/distinct_exchanges…），
//! 调用点无需逐层转发。

use std::ops::Deref;

use trading_exchange::port::{AccountTargets, CredentialResolver};
use trading_store::AccountManager;

use crate::positions::AccountLookup;
use crate::rules::{RuleAccount, RuleAccountLookup};

/// 服务层账户句柄：廉价克隆（内部管理器全 Arc），既是账户域门面又是各端口实现。
#[derive(Clone)]
pub struct AccountHub(AccountManager);

impl AccountHub {
    pub fn new(manager: AccountManager) -> Self {
        Self(manager)
    }

    /// 借用内部纯账户域管理器。
    pub fn inner(&self) -> &AccountManager {
        &self.0
    }
}

impl Deref for AccountHub {
    type Target = AccountManager;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// 交易所接入层凭证解析端口：精确名匹配 + 现取现解密，不长期缓存 secret。
///
/// 薄透传：解密与按 exchange 的 schema 路由已收口在 store 兼容层
/// （`ResolvedAccount.credential` 直接携带强类型枚举），本 impl 无任何
/// exchange 分支（迁移方案 B Phase 3）。
impl CredentialResolver for AccountHub {
    fn resolve(
        &self,
        account: &str,
        exchange: &str,
    ) -> Option<trading_exchange::port::ExchangeCredential> {
        self.0
            .resolve(account, Some(exchange))
            .map(|acc| acc.credential)
    }
}

/// 账户连接监督目标供给：version 取重载体，交易所侧据此停旧起新。
impl AccountTargets for AccountHub {
    fn active_targets(&self) -> Vec<trading_exchange::runtime::ConnTarget> {
        let version = self.reload_version();
        self.0
            .list_active_briefs()
            .into_iter()
            .map(|a| trading_exchange::runtime::ConnTarget {
                exchange: a.exchange,
                account: a.name,
                version,
            })
            .collect()
    }
}

/// 规则引擎账户反查端口（只读、不解密）。
impl RuleAccountLookup for AccountHub {
    fn account_by_id(&self, id: u64) -> Option<RuleAccount> {
        self.0.account_by_id(id).map(|a| RuleAccount {
            name: a.name,
            is_testnet: a.is_testnet,
            position_mode: a.position_mode,
        })
    }
}

/// 仓位对账账户解析端口：账户名全局唯一、入站已按交易所精确校验，只按名取 id。
impl AccountLookup for AccountHub {
    fn account_id(&self, _exchange: &str, account: &str) -> Option<u64> {
        self.0.account_id_by_name(account)
    }
}
