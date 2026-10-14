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
    let input = to_repo_input(ev);
    // cid miss 时的回查 fallback（各所通用）：柜台回报可能只带交易所侧标识
    // （CTP 重启后内存映射丢失，事件 cid=OrderRef；订单行受理时已落
    // exchange_order_id）。命中唯一行则改写 cid 重试一次；其余维持丢弃语义。
    let input = match orders.apply_event(&input) {
        Ok(_) => input,
        Err(_) => {
            let fallback = orders
                .resolve_cid_by_exchange_id(&ev.client_order_id, ev.exchange_order_id.as_deref());
            match fallback {
                Some(cid) => {
                    tracing::info!(
                        source,
                        event_cid = %ev.client_order_id,
                        resolved_cid = %cid,
                        "order event cid miss, resolved by exchange_order_id"
                    );
                    OrderEventInput {
                        client_order_id: cid,
                        ..input
                    }
                }
                None => input,
            }
        }
    };
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
