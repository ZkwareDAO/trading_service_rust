//! 规则仓储：在通用 [`Table<WatchRule>`] 之上承载盯盘规则的业务状态机。
//!
//! * 所有状态变更都经 [`Table::update`] 在行写锁内完成，落盘仍由 TradingStore
//!   统一负责，本仓储不另建内存结构/落盘线程；
//! * `active → in_use` 用行锁闭包内的 CAS 完成：WS 连续推送并发触发时，
//!   只有一个流程能把 active 改成 in_use，其余拿到 false 直接跳过，杜绝重复下单；
//! * 链式激活（`rule:{id}`）要求「当前规则 retired + 目标规则 active」成对生效，
//!   仓储以一把短临界区互斥锁串行化两次内存 update（绝不含网络/await）；
//! * retired 是唯一业务终态，落状态的同时置 deleted=1，且对终态行幂等。

use std::sync::{Arc, Mutex};

use store_core::{StoreError, Table};
use trading_models::{RuleIdentity, WatchRule, WatchRuleStatus};

/// 规则状态机应用失败。
#[derive(Debug, thiserror::Error)]
pub enum RuleRepoError {
    /// 规则不存在或已软删。
    #[error("rule `{0}` not found")]
    NotFound(u64),
    /// 当前规则不是 active（不能触发/链式终结）。
    #[error("rule `{0}` is not active (status={1:?})")]
    NotActive(u64, WatchRuleStatus),
    /// 链式目标规则不是 pending。
    #[error("chain target rule `{0}` is not pending (status={1:?})")]
    TargetNotPending(u64, WatchRuleStatus),
    /// 链式目标规则不存在或已软删。
    #[error("chain target rule `{0}` not found")]
    TargetMissing(u64),
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// 规则仓储句柄，廉价克隆（内部为 Arc 表 + Arc 互斥锁）。
#[derive(Clone)]
pub struct RuleRepo {
    table: Arc<Table<WatchRule>>,
    /// 串行化链式双改；不保护其它路径（claim/retire 只依赖行锁 CAS）。
    chain_lock: Arc<Mutex<()>>,
}

impl RuleRepo {
    /// 绑定一张已注册并完成恢复的规则表。
    pub fn new(table: Arc<Table<WatchRule>>) -> Self {
        Self {
            table,
            chain_lock: Arc::new(Mutex::new(())),
        }
    }

    /// 建规则（id=0 由表分配，时间戳自动注入）。
    pub fn insert(&self, rule: WatchRule) -> Result<WatchRule, RuleRepoError> {
        Ok(self.table.insert(rule)?)
    }

    /// 取一条未删除规则快照。
    pub fn get(&self, id: u64) -> Option<WatchRule> {
        self.table.get(id).ok().flatten()
    }

    /// 取一条规则快照（含 retired 后的 tombstone）；诊断/终态确认用。
    pub fn get_including_deleted(&self, id: u64) -> Option<WatchRule> {
        self.table.get_including_deleted(id).ok().flatten()
    }

    /// 触发「执行自己」：仅当规则当前为 active 时原子置为 in_use。
    ///
    /// 返回 true 表示本调用抢到触发权（调用方随后下单）；false 表示规则已不是
    /// active（被并发流程抢先/已终结），调用方必须跳过。任何 IO 都发生在本调用
    /// 返回之后，锁不跨网络。
    pub fn claim_self(&self, id: u64) -> Result<bool, RuleRepoError> {
        // 含 tombstone 判定：彻底没有此行才算 NotFound；已终结（deleted）的规则
        // 再触发属于正常竞争，返回 false 由调用方跳过，而不是报错。
        let Some(raw) = self.table.get_including_deleted(id)? else {
            return Err(RuleRepoError::NotFound(id));
        };
        if raw.deleted {
            return Ok(false);
        }

        // 行写锁闭包内 CAS：只有 active 才能转 in_use。WS 并发触发时，多个调用
        // 串行进入同一行锁，仅第一个观察到 active，其余拿到 false——无需外层加锁，
        // 临界区也只覆盖内存读改写，不含任何 IO。
        let mut won = false;
        let updated = self.table.update(id, |r| {
            if r.status == WatchRuleStatus::Active {
                r.status = WatchRuleStatus::InUse;
                won = true;
            }
        });
        match updated {
            Ok(_) => Ok(won),
            // 检查与更新之间被并发 retire（行转 tombstone）：按未抢到处理。
            Err(StoreError::NotFound(_)) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// 链式命中：当前规则 active → retired(+deleted=1)，目标规则 pending → active。
    ///
    /// 两个转换在同一临界区内完成；前置条件（current=active、target=pending）
    /// 不满足时原样返回错误且不改动任何一行。
    pub fn chain_fire(&self, current_id: u64, target_id: u64) -> Result<(), RuleRepoError> {
        // 仅串行化链式双改，临界区内只有两次内存 update，绝不跨网络/await。
        let _guard = self.chain_lock.lock().unwrap();

        // —— 前置校验（任何一行不满足都原样返回，不动数据）——
        let current = self
            .table
            .get(current_id)?
            .ok_or(RuleRepoError::NotFound(current_id))?;
        if current.status != WatchRuleStatus::Active {
            return Err(RuleRepoError::NotActive(current_id, current.status));
        }
        let target = self
            .table
            .get(target_id)?
            .ok_or(RuleRepoError::TargetMissing(target_id))?;
        if target.status != WatchRuleStatus::Pending {
            return Err(RuleRepoError::TargetNotPending(target_id, target.status));
        }

        // —— 成对落库：闭包内再判一次源状态，防 chain_lock 之外的 claim 抢先 ——
        let mut current_retired = false;
        self.table.update(current_id, |r| {
            if r.status == WatchRuleStatus::Active {
                r.status = WatchRuleStatus::Retired;
                r.deleted = true;
                current_retired = true;
            }
        })?;
        if !current_retired {
            // 被并发 claim_self 抢先转走：target 尚未触碰，按前置不符返回。
            let status = self
                .table
                .get_including_deleted(current_id)?
                .map(|r| r.status)
                .unwrap_or(WatchRuleStatus::Retired);
            return Err(RuleRepoError::NotActive(current_id, status));
        }
        // target 的 pending→active 只可能由本方法（持 chain_lock）完成，校验后必成功。
        self.table.update(target_id, |r| {
            r.status = WatchRuleStatus::Active;
        })?;
        Ok(())
    }

    /// 规则终态：置 retired 并 deleted=1。已是终态（tombstone）时幂等成功。
    pub fn retire(&self, id: u64) -> Result<(), RuleRepoError> {
        let Some(raw) = self.table.get_including_deleted(id)? else {
            return Err(RuleRepoError::NotFound(id));
        };
        // 幂等：迟到的重复终态回报再次到达时，tombstone 已代表终态，直接成功。
        if raw.deleted {
            return Ok(());
        }
        self.table.update(id, |r| {
            r.status = WatchRuleStatus::Retired;
            r.deleted = true;
        })?;
        Ok(())
    }

    /// 重新启用：retired tombstone → active（deleted=false）。
    ///
    /// 仅终态行可 reactivate；active/pending/in_use 行返回 [`RuleRepoError::NotActive`]，
    /// 不存在返回 NotFound。不改变身份/绑定字段。
    pub fn reactivate(&self, id: u64) -> Result<WatchRule, RuleRepoError> {
        let raw = self
            .table
            .get_including_deleted(id)?
            .ok_or(RuleRepoError::NotFound(id))?;
        if !raw.deleted || raw.status != WatchRuleStatus::Retired {
            return Err(RuleRepoError::NotActive(id, raw.status));
        }
        // Table::update 不作用于已删行：复活 tombstone 用 upsert 整体放回
        // （保留原 id/created_at），只改状态与软删标记。
        let mut revived = raw;
        revived.status = WatchRuleStatus::Active;
        revived.deleted = false;
        Ok(self.table.upsert(revived)?)
    }

    /// 全部 active 规则（deleted=0 且 status=active），id 升序。
    pub fn list_active(&self) -> Vec<WatchRule> {
        self.active_rows()
    }

    /// 全部**未终止**（active/pending/in_use）规则，id 升序。
    ///
    /// 供行情兴趣供给：pending 链式后继与 in_use 在途单的标的也需提前订阅；
    /// 纯读取，不触碰状态机。
    pub fn list_non_retired(&self) -> Vec<WatchRule> {
        self.table
            .list()
            .into_iter()
            .filter(|r| r.status != WatchRuleStatus::Retired)
            .collect()
    }

    /// 指定仓位上的全部 active 规则，id 升序。
    pub fn list_active_for_position(&self, position_id: u64) -> Vec<WatchRule> {
        self.active_rows()
            .into_iter()
            .filter(|r| r.position_id == position_id)
            .collect()
    }

    /// 按[`RuleIdentity`]查找一条未删除（deleted=0）的规则；不存在返回 None。
    ///
    /// 供"重设"场景 find-or-update：身份命中即复用该规则（更新可变字段），
    /// 不再追加新规则。仅扫描活跃行，tombstone 不参与。
    pub fn find_active_by_identity(&self, identity: &RuleIdentity) -> Option<WatchRule> {
        self.table
            .list()
            .into_iter()
            .find(|r| &r.identity() == identity)
    }

    /// 未删除且 status=active 的规则（`table.list` 已按 id 升序）。
    fn active_rows(&self) -> Vec<WatchRule> {
        self.table
            .list()
            .into_iter()
            .filter(|r| r.status == WatchRuleStatus::Active)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use store_core::MockClock;
    use trading_models::{
        OrderType, Side, WatchConditionKind, WatchRule, WatchRuleAction, WatchRuleStatus,
        WatchRuleType,
    };

    fn repo() -> RuleRepo {
        let clock = Arc::new(MockClock::new(1_700_000_000_000));
        RuleRepo::new(Table::new(clock))
    }

    /// 一条挂在仓位 100 上、动作 self 的 active 风控规则。
    fn risk_rule(repo: &RuleRepo, position_id: u64) -> WatchRule {
        repo.insert(WatchRule::new_risk(
            7,
            position_id,
            WatchConditionKind::Roi,
            None,
            ">=",
            Decimal::new(12, 2),
            WatchRuleAction::UseSelf,
            OrderType::Market,
            Decimal::ZERO,
            Decimal::ONE,
        ))
        .unwrap()
    }

    /// 插入一条动作 rule:{target} 的 pending 规则，返回插入后的行。
    fn pending_rule(repo: &RuleRepo, position_id: u64, target: u64) -> WatchRule {
        let r = repo
            .insert(WatchRule::new_risk(
                7,
                position_id,
                WatchConditionKind::Price,
                None,
                "<=",
                Decimal::from(90_000),
                WatchRuleAction::ActivateRule { rule_id: target },
                OrderType::Limit,
                Decimal::ZERO,
                Decimal::ONE,
            ))
            .unwrap();
        // new_* 默认 active；链式后继需经状态机外的装配转为 pending。
        repo.table
            .update(r.id, |row| row.status = WatchRuleStatus::Pending)
            .unwrap()
    }

    #[test]
    fn claim_moves_active_to_in_use_once() {
        let repo = repo();
        let r = risk_rule(&repo, 100);

        assert!(repo.claim_self(r.id).unwrap(), "首次抢占应成功");
        let now = repo.get(r.id).unwrap();
        assert_eq!(now.status, WatchRuleStatus::InUse);

        // in_use 再抢：幂等失败，不回退状态。
        assert!(!repo.claim_self(r.id).unwrap());
        assert_eq!(repo.get(r.id).unwrap().status, WatchRuleStatus::InUse);
    }

    #[test]
    fn concurrent_claims_have_single_winner() {
        let repo = std::sync::Arc::new(repo());
        let r = risk_rule(&repo, 100);
        let mut handles = Vec::new();
        for _ in 0..16 {
            let repo = repo.clone();
            handles.push(std::thread::spawn(move || repo.claim_self(r.id).unwrap()));
        }
        let winners = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .filter(|won| *won)
            .count();
        assert_eq!(winners, 1, "并发触发只能有一个 active→in_use 赢家");
        assert_eq!(repo.get(r.id).unwrap().status, WatchRuleStatus::InUse);
        // active 列表里不应再有它。
        assert!(repo.list_active().is_empty());
    }

    #[test]
    fn claim_unknown_rule_errors() {
        let err = repo().claim_self(404).unwrap_err();
        assert!(matches!(err, RuleRepoError::NotFound(404)));
    }

    #[test]
    fn chain_fire_retires_current_and_activates_target_atomically() {
        let repo = repo();
        let cur = risk_rule(&repo, 100);
        let mut next = WatchRule::new_risk(
            7,
            100,
            WatchConditionKind::Price,
            None,
            "<=",
            Decimal::from(90_000),
            WatchRuleAction::UseSelf,
            OrderType::Market,
            Decimal::ZERO,
            Decimal::ONE,
        );
        next.status = WatchRuleStatus::Pending;
        let next = repo.insert(next).unwrap();

        repo.chain_fire(cur.id, next.id).unwrap();

        // 当前规则终态且软删：get 不可见，tombstone 可见且 status=retired。
        assert!(repo.get(cur.id).is_none());
        let tomb = repo.table.get_including_deleted(cur.id).unwrap().unwrap();
        assert_eq!(tomb.status, WatchRuleStatus::Retired);
        assert!(tomb.deleted);

        // 目标规则转 active。
        assert_eq!(repo.get(next.id).unwrap().status, WatchRuleStatus::Active);
        let active = repo.list_active_for_position(100);
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].id, next.id);
    }

    #[test]
    fn chain_fire_rejects_when_current_not_active_and_changes_nothing() {
        let repo = repo();
        let cur = risk_rule(&repo, 100);
        let next = pending_rule(&repo, 100, u64::MAX);
        repo.claim_self(cur.id).unwrap(); // cur → in_use

        let err = repo.chain_fire(cur.id, next.id).unwrap_err();
        assert!(matches!(
            err,
            RuleRepoError::NotActive(id, WatchRuleStatus::InUse) if id == cur.id
        ));
        // 目标仍 pending，当前仍 in_use，未软删。
        assert_eq!(repo.get(next.id).unwrap().status, WatchRuleStatus::Pending);
        assert_eq!(repo.get(cur.id).unwrap().status, WatchRuleStatus::InUse);
    }

    #[test]
    fn chain_fire_rejects_when_target_not_pending() {
        let repo = repo();
        let cur = risk_rule(&repo, 100);
        let target = risk_rule(&repo, 100); // target 也是 active

        let err = repo.chain_fire(cur.id, target.id).unwrap_err();
        assert!(matches!(
            err,
            RuleRepoError::TargetNotPending(id, WatchRuleStatus::Active) if id == target.id
        ));
        // 两规则都维持 active。
        assert_eq!(repo.get(cur.id).unwrap().status, WatchRuleStatus::Active);
        assert_eq!(repo.get(target.id).unwrap().status, WatchRuleStatus::Active);
    }

    #[test]
    fn chain_fire_reports_missing_rules() {
        let repo = repo();
        let cur = risk_rule(&repo, 100);
        assert!(matches!(
            repo.chain_fire(cur.id, 404).unwrap_err(),
            RuleRepoError::TargetMissing(404)
        ));
        assert!(matches!(
            repo.chain_fire(404, cur.id).unwrap_err(),
            RuleRepoError::NotFound(404)
        ));
    }

    #[test]
    fn retire_sets_terminal_and_is_idempotent() {
        let repo = repo();
        let r = risk_rule(&repo, 100);
        repo.claim_self(r.id).unwrap();

        repo.retire(r.id).unwrap();
        assert!(repo.get(r.id).is_none());
        let tomb = repo.table.get_including_deleted(r.id).unwrap().unwrap();
        assert_eq!(tomb.status, WatchRuleStatus::Retired);
        assert!(tomb.deleted);

        // 终态后再 retire（如迟到的重复终态回报）：幂等成功，不报错。
        repo.retire(r.id).unwrap();
    }

    #[test]
    fn retire_unknown_rule_errors() {
        assert!(matches!(
            repo().retire(404).unwrap_err(),
            RuleRepoError::NotFound(404)
        ));
    }

    #[test]
    fn list_active_filters_status_and_position() {
        let repo = repo();
        let a1 = risk_rule(&repo, 100);
        let a2 = risk_rule(&repo, 200);
        let _pend = pending_rule(&repo, 100, u64::MAX);

        assert_eq!(repo.list_active().len(), 2);
        let for_100 = repo.list_active_for_position(100);
        assert_eq!(for_100.len(), 1);
        assert_eq!(for_100[0].id, a1.id);
        assert_eq!(repo.list_active_for_position(200)[0].id, a2.id);
        assert!(repo.list_active_for_position(300).is_empty());

        // 终结后从 active 列表消失。
        repo.claim_self(a1.id).unwrap();
        repo.retire(a1.id).unwrap();
        assert_eq!(repo.list_active_for_position(100).len(), 0);
        assert_eq!(repo.list_active().len(), 1);
    }

    #[test]
    fn new_open_rule_carries_side_and_can_claim() {
        // new 规则同走 active→in_use 状态机；方向存在规则行上。
        let repo = repo();
        let r = repo
            .insert(WatchRule::new_open(
                7,
                0,
                "binance",
                "BTCUSDT",
                trading_models::InstrumentType::Futures,
                Side::Sell,
                OrderType::Limit,
                Decimal::from(50_000),
                Decimal::new(1, 0),
                1,
            ))
            .unwrap();
        assert_eq!(r.rule_type, WatchRuleType::New);
        assert_eq!(r.side, Side::Sell);
        assert!(repo.claim_self(r.id).unwrap());
        assert_eq!(repo.get(r.id).unwrap().status, WatchRuleStatus::InUse);
    }

    #[test]
    fn reactivate_moves_retired_back_to_active() {
        // Arrange：规则先 retire（status=retired, deleted=true）。
        let repo = repo();
        let r = risk_rule(&repo, 100);
        repo.retire(r.id).unwrap();
        assert!(repo.get(r.id).is_none());

        // Act：reactivate。
        let updated = repo.reactivate(r.id).unwrap();

        // Assert：status=active、deleted=false，重新可见。
        assert_eq!(updated.status, WatchRuleStatus::Active);
        assert!(!updated.deleted);
        assert_eq!(repo.get(r.id).unwrap().status, WatchRuleStatus::Active);
    }

    #[test]
    fn reactivate_unknown_rule_errors() {
        assert!(matches!(
            repo().reactivate(404).unwrap_err(),
            RuleRepoError::NotFound(404)
        ));
    }

    #[test]
    fn reactivate_active_rule_is_conflict() {
        // active 行不能 reactivate：返回 NotActive 类冲突。
        let repo = repo();
        let r = risk_rule(&repo, 100);
        let err = repo.reactivate(r.id).unwrap_err();
        assert!(matches!(err, RuleRepoError::NotActive(rr, _) if rr == r.id));
    }

    #[test]
    fn find_active_by_identity_matches_and_skips_other() {
        let repo = repo();
        // 两条绑定不同仓位的 risk 规则。
        let r1 = risk_rule(&repo, 100);
        let _r2 = risk_rule(&repo, 200);

        // 用 r1 身份查询：命中 r1。
        let found = repo.find_active_by_identity(&r1.identity()).unwrap();
        assert_eq!(found.id, r1.id);

        // 终态（retire）后：tombstone 不再被活跃身份查询命中。
        repo.retire(r1.id).unwrap();
        assert!(repo.find_active_by_identity(&r1.identity()).is_none());
    }
}
