//! 订单回报统一入口：交易所回报 -> 订单内存状态机 -> 按 strategy_name 经南向连接回推。
//!
//! 各 venue 的外向「账户事件流」（真实交易所、本地模拟所 xili）都把解析出的
//! [`OrderEvent`] 交给 [`apply_and_route`]，处理逻辑只有一份，避免重复实现。

use trading_core::{
    protocol,
    protocol::{ClientEvent, OrderEvent},
    timeutil,
};
use trading_store::OrderEventInput;

use crate::rules::order::terminal_rule_id;
use crate::state::AppState;

/// 订单被推进到终态（WS 回报或 REST 拉单补偿，任一途径）后，若它由规则单产生，
/// 把关联规则 in_use → retired（+deleted=1）。retire 幂等，重复/迟到终态安全。
///
/// 规则单没有 strategy_name 也要执行，故放在路由策略连接之前、对所有终态统一调用。
pub fn retire_rule_for_terminal_order(state: &AppState, order: &trading_models::Order) {
    if let Some(rule_id) = terminal_rule_id(order) {
        let watch_rules = state.store.rule_repo().expect("rule repo after recover");
        if let Err(e) = watch_rules.retire(rule_id) {
            tracing::warn!(
                rule_id,
                client_order_id = %order.client_order_id,
                error = %e,
                "cannot retire rule after terminal order"
            );
        }
    }
}

/// 协议回报 -> 仓储输入：状态串宽松解析、RFC3339 时间转毫秒。
fn to_repo_input(ev: &OrderEvent) -> OrderEventInput {
    OrderEventInput {
        client_order_id: ev.client_order_id.clone(),
        status: protocol::parse_status(&ev.status),
        exchange_order_id: ev.exchange_order_id.clone(),
        filled_price: ev.filled_price,
        filled_quantity: ev.filled_quantity,
        exchange_update_ms: ev
            .exchange_update_time
            .as_deref()
            .and_then(timeutil::parse_opt_time_ms),
    }
}

/// 应用一条订单回报到内存并按 strategy_name 路由回南向连接。
/// `source` 仅用于日志区分来源（如 "binance" / "xili"）。
pub async fn apply_and_route(state: &AppState, ev: &OrderEvent, source: &str) {
    let orders = state.store.order_repo().expect("order repo after recover");
    let mut input = to_repo_input(ev);
    // cid miss 时的回查 fallback（各所通用）：柜台回报可能只带交易所侧标识
    // （CTP 重启后内存映射丢失，事件 cid=OrderRef；订单行受理时已落
    // exchange_order_id）。命中唯一行则改写 cid；其余维持丢弃语义。
    //
    // 关键：这里必须**只读探测**（peek_by_cid），不能用会改状态的 apply_event
    // 当“探测”——否则若事件本身就是终态（CTP 即时成交同毫秒推 NEW→满量
    // PARTIAL→FILLED，cid 又恰好命中），第一次“探测性 apply”就把订单推进终态，
    // 第二次 apply 必报 already terminal 走 Err，导致挂在 Ok 分支的规则退休 /
    // 成交通知 / 策略回推全部被吞。改为先 peek 决定目标 cid，全程只 apply 一次。
    if orders.peek_by_cid(&ev.client_order_id).is_none() {
        if let Some(cid) =
            orders.resolve_cid_by_exchange_id(&ev.client_order_id, ev.exchange_order_id.as_deref())
        {
            tracing::info!(
                source,
                event_cid = %ev.client_order_id,
                resolved_cid = %cid,
                "order event cid miss, resolved by exchange_order_id"
            );
            input.client_order_id = cid;
        }
    }
    match orders.apply_event(&input) {
        Ok(updated) => {
            tracing::info!(
                source = source,
                client_order_id = %updated.client_order_id,
                status = %updated.status.wire(),
                filled_quantity = %updated.filled_quantity,
                filled_price = %updated.filled_price,
                "order memory updated from event"
            );
            // 规则单到达终态：经 rule_id 回溯，把对应规则 in_use → retired（+deleted=1）。
            // retire 幂等，重复/迟到终态回报安全；无 strategy_name 的规则单也必须执行。
            // WS 回报与未来的 REST 拉单补偿共用本函数同一收口。
            retire_rule_for_terminal_order(state, &updated);
            // 成交通知（纯通知、fire-and-forget）：内部仅在完全成交时触发，
            // 失败只记日志，不影响下面的策略路由。
            state.notify.on_order_filled(&updated);
            if let Some(strategy) = &updated.strategy_name {
                let out =
                    ClientEvent::order_snapshot("order_event", updated.status.wire(), &updated);
                if let Ok(value) = serde_json::to_value(&out) {
                    state.hub.send(strategy, &value).await;
                }
            } else {
                tracing::debug!(client_order_id = %updated.client_order_id, "order has no strategy_name, skip strategy routing");
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, source = source, client_order_id = %input.client_order_id, "cannot apply order event")
        }
    }
}

/// AppState 作为交易所接入层的订单回报出口：账户事件流只认 `OrderEventSink`，
/// 不反向依赖 AppState 具体类型。
///
/// 顺序：**先触发通知 A（仅 deribit）、再做本地订单处理**。通知为 fire-and-forget
/// （内部自行 spawn），此处不 await，故通知超时/失败绝不影响下面的订单状态机与路由。
#[async_trait::async_trait]
impl trading_exchange::port::OrderEventSink for crate::state::AppState {
    async fn on_order_event(&self, ev: &OrderEvent, source: &str, account: &str) {
        // 通知 A：仅 deribit 订单流；在 apply_and_route 之前旁路触发。
        if source == "deribit" {
            // 按账户名反查 id（供任务内查仓位 ROI）；查不到传 None，任务照常播报。
            let account_id = self.accounts.account_id_by_name(account);
            self.notify
                .announce_deribit_position(ev, account, account_id);
        }
        apply_and_route(self, ev, source).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use trading_core::protocol::OrderEvent;
    use trading_models::{
        Order, OrderType, Side, WatchConditionKind, WatchRule, WatchRuleAction, WatchRuleStatus,
    };

    /// 造一条规则平仓单 + 关联的 in_use 风控规则（active→claim→in_use）。
    /// 返回 (state, 本地 cid, 规则 id)。
    async fn seed_inuse_rule_order(seed_exchange_order_id: bool) -> (AppState, String, u64) {
        let app = crate::api::test_support::app_with_accounts("").await;
        let state = app.state;
        let rules = state.store.rule_repo().unwrap();
        let orders = state.store.order_repo().unwrap();

        let cid = "0xapply_route_terminal_0000000000000001".to_string();
        let rule = rules
            .insert(WatchRule::new_risk(
                1,
                1,
                WatchConditionKind::Price,
                Some("rb2612".into()),
                ">=",
                Decimal::from(1),
                WatchRuleAction::UseSelf,
                OrderType::Market,
                Decimal::ZERO,
                Decimal::ONE,
            ))
            .unwrap();
        // 规则已触发：active → in_use（模拟已 claim、等待平仓终态退休）。
        assert!(rules.claim_self(rule.id).unwrap());

        let mut order = Order::new(1, cid.clone(), "ctp", "rb2612", Side::Sell);
        order.rule_id = rule.id;
        order.order_type = OrderType::Limit;
        order.order_quantity = Decimal::from(2);
        if seed_exchange_order_id {
            order.exchange_order_id = Some("179223760958".to_string());
        }
        orders.insert(order).unwrap();
        (state, cid, rule.id)
    }

    fn ev(cid: &str, status: &str, exchange_order_id: Option<&str>) -> OrderEvent {
        OrderEvent {
            client_order_id: cid.to_string(),
            status: status.to_string(),
            exchange_order_id: exchange_order_id.map(str::to_string),
            filled_price: None,
            filled_quantity: Some(Decimal::from(2)),
            exchange_update_time: None,
            message: None,
            instrument: "rb2612".to_string(),
            direction: Some("sell".to_string()),
            reduce_only: true,
            amount: None,
            limit_price: None,
        }
    }

    /// CTP 路径：回报 cid 即本地长 cid（venue 已反查），NEW/FILLED 都直接命中。
    /// 若 apply_and_route 的双 apply 在「第一次就置终态」时吞掉退休副作用，
    /// 规则会卡在 in_use——本用例据此判定。
    #[tokio::test]
    async fn terminal_event_with_matching_cid_retires_inuse_rule() {
        let (state, cid, rule_id) = seed_inuse_rule_order(false).await;

        // 受理：NEW（两次 apply 均非终态，幂等）。
        apply_and_route(&state, &ev(&cid, "NEW", None), "ctp").await;
        // 满量成交：单帧 FILLED。
        apply_and_route(&state, &ev(&cid, "FILLED", Some("179223760958")), "ctp").await;

        let rule = state
            .store
            .rule_repo()
            .unwrap()
            .get_including_deleted(rule_id)
            .expect("rule row exists");
        assert_eq!(
            rule.status,
            WatchRuleStatus::Retired,
            "单帧 FILLED（cid 直接命中）必须把 in_use 规则退休；卡在 {:?} 即双 apply 吞了退休副作用",
            rule.status
        );
        // FILLED 单已被状态机软删（tombstone），须用 peek 读取终态。
        let order = state.store.order_repo().unwrap().peek_by_cid(&cid).unwrap();
        assert_eq!(order.status, trading_models::OrderStatus::Filled);
    }

    /// 健康对照：回报 cid 是交易所单号（本地未命中），靠 exchange_order_id fallback
    /// 解析后再置终态——第一次 apply 探测失败（不改状态），第二次唯一生效，规则应退休。
    #[tokio::test]
    async fn terminal_event_resolved_by_exchange_id_retires_rule() {
        let (state, _cid, rule_id) = seed_inuse_rule_order(true).await;
        apply_and_route(
            &state,
            &ev("179223760958", "FILLED", Some("179223760958")),
            "ctp",
        )
        .await;

        let rule = state
            .store
            .rule_repo()
            .unwrap()
            .get_including_deleted(rule_id)
            .expect("rule row exists");
        assert_eq!(rule.status, WatchRuleStatus::Retired);
    }
}
