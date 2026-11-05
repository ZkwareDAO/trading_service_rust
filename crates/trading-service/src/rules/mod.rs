//! 盯盘规则引擎（trading-service 侧）。
//!
//! 分层与 [`crate::positions`] 一致：纯决策内核与 IO 编排分离。
//! * [`eval`]：条件比较操作符（`>=` `<=` `>` `<` `==`）的纯函数解释；
//! * [`metrics`]：`Position + Market → EvaluationContext` 的纯计算层
//!   （零 IO/网络/存储，行情与跨标的持仓以闭包注入），Rule 只认指标做阈值判定；
//! * [`order`]：命中规则 → 内存订单/下单请求的纯构造（risk 反向减仓、new 按 side 开仓）；
//! * [`engine`]：一轮扫描编排——评估 active 规则、行锁 CAS 抢占、链式激活、建单；
//! * [`placer`]：生产下单口（经交易所注册表提交并回写受理/失败）。
//!
//! 状态转换（active/in_use/pending/retired）一律走 trading-store 的 `RuleRepo`，
//! 本模块不自建共享内存；定时扫描任务在 `main` 装配，终态回收在 `event`。

pub mod create;
pub mod engine;
pub mod eval;
pub mod metrics;
pub mod order;
pub mod placer;

pub use create::{build_rule, RuleCreateInput, RulePositionLookup};
pub use engine::{RuleAccount, RuleAccountLookup, RuleEngine, RuleOrderPlacer, RuleOrderQuerier};
pub use placer::{ExchangeRuleOrderQuerier, ExchangeRulePlacer};

use crate::rules::order::terminal_rule_id;
use trading_models::Order;
use trading_store::RuleRepo;

/// 订单终态后按 `rule_id` 退休规则，但先校验归属。
///
/// 规则 id 由 `Table::next_id` 分配、跨日 purge 后会复用；订单里的 `rule_id`
/// 是裸数字且订单不随日切清理。若不校验，旧订单的迟到终态会把复用同一 id 的
/// 新规则误置终态（2026-11-05 HL 订单 2096 / CTP rule 51 事故）。
///
/// 校验顺序：账户 → 交易所（规则 exchange 非空时）→ 规则创建时间不晚于订单。
/// 任一不匹配只 warn 跳过，绝不动规则状态；规则不存在/已终态按幂等处理。
pub fn retire_rule_for_order(rules: &RuleRepo, order: &Order, reason: &str) {
    // 统一判定：仅"订单已终态且由规则单产生"才需要 retire。
    let Some(rule_id) = terminal_rule_id(order) else {
        return;
    };
    let Some(rule) = rules.get_including_deleted(rule_id) else {
        tracing::debug!(
            rule_id,
            client_order_id = %order.client_order_id,
            reason,
            "订单终态：规则不存在，跳过 retire"
        );
        return;
    };
    if rule.account_id != order.account_id {
        tracing::warn!(
            rule_id,
            client_order_id = %order.client_order_id,
            rule_account = rule.account_id,
            order_account = order.account_id,
            reason,
            "订单与规则归属账户不匹配，拒绝 retire（防 rule_id 复用误杀）"
        );
        return;
    }
    if !rule.exchange.is_empty() && !rule.exchange.eq_ignore_ascii_case(&order.exchange) {
        tracing::warn!(
            rule_id,
            client_order_id = %order.client_order_id,
            rule_exchange = %rule.exchange,
            order_exchange = %order.exchange,
            reason,
            "订单与规则归属交易所不匹配，拒绝 retire"
        );
        return;
    }
    if rule.created_at > order.created_at {
        tracing::warn!(
            rule_id,
            client_order_id = %order.client_order_id,
            rule_created_at = rule.created_at,
            order_created_at = order.created_at,
            reason,
            "规则创建时间晚于订单，判定为 rule_id 复用，拒绝 retire"
        );
        return;
    }
    match rules.retire(rule_id) {
        Ok(()) => tracing::debug!(
            rule_id,
            client_order_id = %order.client_order_id,
            reason,
            "订单终态：规则已 retire"
        ),
        Err(e) => tracing::warn!(
            rule_id,
            client_order_id = %order.client_order_id,
            error = %e,
            reason,
            "订单终态 retire 规则失败"
        ),
    }
}

/// 生产仓位查询端口：trading-store 的 PositionRepo 直接适配建规则校验。
impl RulePositionLookup for trading_store::PositionRepo {
    fn find_active_by_id(
        &self,
        account_id: u64,
        position_id: u64,
    ) -> Option<trading_models::Position> {
        // 按主键取活跃仓，并在此校验账户归属（跨账户视为不存在）。
        self.get_active(position_id)
            .filter(|p| p.account_id == account_id)
    }

    fn exists_active_instrument(
        &self,
        account_id: u64,
        exchange: &str,
        instrument_type: trading_models::InstrumentType,
        instrument: &str,
    ) -> bool {
        self.exists_active_instrument(account_id, exchange, instrument_type, instrument)
    }
}
