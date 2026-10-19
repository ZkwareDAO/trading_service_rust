//! 规则单的生产下单口：经交易所注册表提交，并把受理/失败回写订单仓储。
//!
//! 与策略命令路径（`service.rs::place_order`）同一套口径：
//! * 适配器连接失败 / 下单被前置拒绝 → REJECTED（终态），规则随即 retire；
//! * REST 发出但应答不确定 → UNKNOWN（非终态），等用户数据流回报收敛；
//! * 受理成功 → NEW（非终态），终态由账户事件流经 `event.rs` 推进并 retire 规则。

use std::sync::Arc;

use async_trait::async_trait;
use trading_exchange::api::{OrderLookup, OrderRef, PlaceErrorKind, PlaceOrderRequest};
use trading_exchange::registry::ExchangeRegistry;
use trading_models::{InstrumentType, Order, OrderStatus};
use trading_store::{LeverageRepo, OrderRepo};

use super::engine::{RuleOrderPlacer, RuleOrderQuerier};

/// 经 [`ExchangeRegistry`] 提交规则单的生产实现（廉价克隆，内部 Arc）。
#[derive(Clone)]
pub struct ExchangeRulePlacer {
    orders: OrderRepo,
    exchanges: Arc<ExchangeRegistry>,
    /// 杠杆缓存：下单前据此决定开仓是否需要 set_leverage（平仓不设）。
    leverages: LeverageRepo,
}

impl ExchangeRulePlacer {
    pub fn new(
        orders: OrderRepo,
        exchanges: Arc<ExchangeRegistry>,
        leverages: LeverageRepo,
    ) -> Self {
        Self {
            orders,
            exchanges,
            leverages,
        }
    }

    /// 订单合约形状 → connect 探测用的产品类型串（与策略命令入参同口径）。
    fn instrument_type_wire(itype: InstrumentType) -> Option<&'static str> {
        match itype {
            InstrumentType::Spot => Some("spot"),
            InstrumentType::Futures | InstrumentType::Perpetual => Some("futures"),
            InstrumentType::Option => Some("option"),
            // 股票/基金等暂未接入真实所：交给注册表按缺省路由。
            InstrumentType::Equity | InstrumentType::Fund => None,
        }
    }
}

#[async_trait]
impl RuleOrderPlacer for ExchangeRulePlacer {
    async fn place(&self, order: &Order, request: PlaceOrderRequest, account_name: &str) {
        let cid = &order.client_order_id;
        let adapter = match self.exchanges.connect(
            &order.exchange,
            Self::instrument_type_wire(order.instrument_type),
            account_name,
        ) {
            Ok(adapter) => adapter,
            Err(e) => {
                tracing::warn!(%cid, error = %e, "rule order: cannot reach exchange");
                if let Err(re) = self.orders.apply_rejection(cid) {
                    tracing::warn!(%cid, error = %re, "apply_rejection failed");
                }
                return;
            }
        };

        // 期货杠杆按需设置（仅开仓）：读对账缓存的杠杆，命中且相等则**整次跳过**
        // （省权重）；缓存缺失/不一致才 set_leverage。**平仓单（reduce_only）不碰
        // 杠杆**——杠杆对平仓成交无意义，移除原先的无效前置。开仓设置失败按拒单
        // 处理，规则随之 retire。
        // 该所不支持程序化杠杆（如 CTP，保证金率由柜台定）：开仓也整段跳过，
        // 绝不能因 set_leverage 返回“不支持”而把开仓单误判为拒单。
        if matches!(order.instrument_type, InstrumentType::Futures)
            && adapter.supports_programmatic_leverage()
        {
            let cached =
                self.leverages
                    .get_for(order.account_id, &order.exchange, &order.instrument);
            let need_set = trading_store::should_set_leverage(
                order.reduce_only,
                cached,
                order.leverage as u32,
            );
            if need_set {
                if let Err(e) = adapter
                    .set_leverage(&order.instrument, order.leverage as u32)
                    .await
                {
                    tracing::warn!(%cid, leverage = order.leverage, error = %e, "rule order: set_leverage rejected");
                    if let Err(re) = self.orders.apply_rejection(cid) {
                        tracing::warn!(%cid, error = %re, "apply_rejection failed");
                    }
                    return;
                }
            }
        }

        match adapter.place_order(&request).await {
            Ok(ack) if !ack.accepted => {
                // 应答成功但适配器明确未受理（确定性拒绝，如 CTP 小数手/超量）：
                // 收敛 REJECTED（规则随终态退休），不能误标 NEW。
                let reason = ack
                    .message
                    .clone()
                    .unwrap_or_else(|| "交易所拒绝下单".into());
                tracing::warn!(%cid, error = %reason, "rule order rejected by exchange (ack)");
                if let Err(re) = self.orders.apply_rejection(cid) {
                    tracing::warn!(%cid, error = %re, "apply_rejection failed");
                }
            }
            Ok(ack) => {
                let updated = self
                    .orders
                    .apply_ack(cid, ack.exchange_order_id, OrderStatus::New)
                    .unwrap_or_else(|_| order.clone());
                tracing::info!(
                    %cid,
                    exchange_order_id = ?updated.exchange_order_id,
                    rule_id = order.rule_id,
                    "rule order accepted (NEW)"
                );
            }
            Err(e) => {
                // 按错误性质分流（与策略命令路径一致）：
                // 交易所明确业务拒绝（HTTP400）→ REJECTED 终态；
                // 超时/网络等应答不确定 → UNKNOWN，等用户数据流回报收敛。
                match adapter.classify_place_error(&e) {
                    PlaceErrorKind::Rejected => {
                        tracing::warn!(%cid, error = %e, "rule order rejected by exchange");
                        if let Err(re) = self.orders.apply_rejection(cid) {
                            tracing::warn!(%cid, error = %re, "apply_rejection failed");
                        }
                    }
                    PlaceErrorKind::Indeterminate => {
                        tracing::warn!(%cid, error = %e, "rule order placement indeterminate");
                        if let Err(re) = self.orders.mark_unknown(cid) {
                            tracing::warn!(%cid, error = %re, "mark_unknown failed");
                        }
                    }
                }
            }
        }
    }
}

/// 经 [`ExchangeRegistry`] REST 查单的生产实现（规则单终态兜底）。
#[derive(Clone)]
pub struct ExchangeRuleOrderQuerier {
    exchanges: Arc<ExchangeRegistry>,
}

impl ExchangeRuleOrderQuerier {
    pub fn new(exchanges: Arc<ExchangeRegistry>) -> Self {
        Self { exchanges }
    }
}

#[async_trait]
impl RuleOrderQuerier for ExchangeRuleOrderQuerier {
    async fn query(&self, order: &Order, account_name: &str) -> OrderLookup {
        let adapter = match self.exchanges.connect(
            &order.exchange,
            ExchangeRulePlacer::instrument_type_wire(order.instrument_type),
            account_name,
        ) {
            Ok(a) => a,
            // 连不上交易所：查询本身失败，不代表订单不存在。
            Err(e) => {
                tracing::warn!(cid = %order.client_order_id, error = %e, "rule order query: cannot reach exchange");
                return OrderLookup::Unavailable;
            }
        };
        // 单号路由是适配器内部职责：cid 必带，已知 eid 一并装入（重启后 CTP
        // registry 为空时，兜底查单可凭 eid=OrderSysID 收敛），与撤单同范式。
        let order_ref = OrderRef::from_order(order);
        let outcome = adapter.query_order(&order.instrument, &order_ref).await;
        if matches!(outcome, OrderLookup::Unavailable) {
            tracing::warn!(cid = %order.client_order_id, "rule order REST query unavailable");
        }
        outcome
    }
}
