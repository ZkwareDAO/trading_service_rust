//! CTP 订单 API 主体：实现 [`ExchangeApi`]。
//!
//! 柜台调用经专用会话执行（`spawn_blocking` + 短会话），async 侧不阻塞
//! tokio 调度器；本结构是 tokio 侧的薄门面。

use async_trait::async_trait;

use crate::api::{
    ExchangeApi, OrderAck, OrderLookup, OrderRef, PlaceErrorKind, PlaceOrderRequest, PositionInfo,
    ReferencePrice,
};
use crate::ctp::mailbox::{InsertMailbox, PendingInsert};
use crate::ctp::order_ref::OrderRefRegistry;
use crate::ctp::prep;

/// 组装下单应答（三处构造共用的字段骨架：placed_* 随请求透传）。
fn make_ack(
    req: &PlaceOrderRequest,
    accepted: bool,
    exchange_order_id: Option<String>,
    message: Option<String>,
) -> OrderAck {
    OrderAck {
        accepted,
        client_order_id: req.client_order_id.clone(),
        exchange_order_id,
        status: if accepted {
            trading_models::OrderStatus::New
        } else {
            trading_models::OrderStatus::Rejected
        },
        message,
        placed_type: None,
        placed_price: req.price,
        placed_quantity: Some(req.quantity),
    }
}

/// 下单等待长会话线程回执的超时（会话断开/线程退出时以此兜底）。
const SUBMIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// CTP 下单/查询门面（tokio 侧持有；实际执行在阻塞会话内）。
pub struct CtpApi {
    /// 账户名（日志标识；当前实现未消费，保留供回调扩展）。
    #[allow(dead_code)]
    account: String,
    /// cid ↔ OrderRef 映射（与账户流长会话共享；回报反查同一份表）。
    order_refs: std::sync::Arc<OrderRefRegistry>,
    /// 下单请求信箱（与账户流长会话线程共享同一份）。
    inserts: std::sync::Arc<InsertMailbox>,
}

impl CtpApi {
    pub fn new(
        account: String,
        order_refs: std::sync::Arc<OrderRefRegistry>,
        inserts: std::sync::Arc<InsertMailbox>,
    ) -> Self {
        Self {
            account,
            order_refs,
            inserts,
        }
    }
}

#[async_trait]
impl ExchangeApi for CtpApi {
    fn exchange(&self) -> &str {
        "ctp"
    }

    /// 提交一笔限价单；返回受理 ACK。
    ///
    /// 纯内存装配失败（手数非法/价格缺失）即拒单；柜台受理码 != 0
    /// 视为同步拒单；受理成功返回 New，后续状态由账户流回报推进（P3）。
    async fn place_order(&self, req: &PlaceOrderRequest) -> anyhow::Result<OrderAck> {
        let order_req = match prep::build_open_or_close(req, None) {
            Ok(r) => r,
            Err(PlaceErrorKind::Rejected) => {
                return Ok(make_ack(
                    req,
                    false,
                    None,
                    Some("下单要素非法（手数须为正整数/价格缺失）".into()),
                ));
            }
            Err(_) => anyhow::bail!("CTP 下单预检失败"),
        };
        let client_order_id = req.client_order_id.clone();
        // 提交锁**跨回执持有**：同一账户同一时刻至多一笔在飞——单槽信箱下
        // 并发提交会互相覆盖（后写覆盖先取，首笔请求永久丢失）。
        let _guard = self.inserts.lock_submission().await;
        // OrderRef 客户端自填、柜台原样回传：锁内分配（分配顺序=提交顺序）
        // 并登记映射；受理成功即回填 OrderAck.exchange_order_id（宿主落订单行，
        // 重启恢复的真相源）。投递到长会话线程：复用已登录 API 实例，毫秒级提交。
        let order_ref = self.order_refs.allocate(&client_order_id);
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.inserts.submit(PendingInsert {
            req: order_req,
            order_ref: order_ref.clone(),
            reply: reply_tx,
        });
        let code = match tokio::time::timeout(SUBMIT_TIMEOUT, reply_rx).await {
            Ok(Ok(ret)) => ret.map_err(|e| anyhow::anyhow!("CTP 报单校验失败: {e}"))?,
            Ok(Err(_)) => anyhow::bail!("CTP 会话线程已退出，下单结果不确定"),
            Err(_) => anyhow::bail!("CTP 下单等待会话线程超时（5s），结果不确定"),
        };
        // 受理码 0=受理；非 0 为柜台拒绝（同步拒）。
        let accepted = code == 0;
        if !accepted {
            // 同步拒：未受理的 OrderRef 不会有回报，清脏映射。
            self.order_refs.remove(&order_ref);
        }
        Ok(make_ack(
            req,
            accepted,
            accepted.then_some(order_ref),
            (!accepted).then(|| format!("CTP 报单受理码 {code}")),
        ))
    }

    /// 撤单未封装（方案 R1：ReqOrderAction 待补），显式报不支持。
    async fn cancel_order(&self, _instrument: &str, _order: &OrderRef) -> anyhow::Result<OrderAck> {
        anyhow::bail!("CTP 撤单暂未接入（ReqOrderAction 待封装）")
    }

    /// 查单经账户流回报收敛，本口径返回不可用。
    async fn query_order(&self, _instrument: &str, _order: &OrderRef) -> OrderLookup {
        OrderLookup::Unavailable
    }

    /// 持仓真值经账户流对账拉取（CTP 无 REST 语义），本口径不适用。
    async fn get_positions(&self) -> anyhow::Result<Vec<PositionInfo>> {
        anyhow::bail!("CTP 持仓经账户流对账拉取（无 REST 语义）")
    }

    /// CTP 无程序化杠杆设置语义（保证金率由柜台/交易所规定）。
    async fn set_leverage(&self, _instrument: &str, _leverage: u32) -> anyhow::Result<()> {
        anyhow::bail!("CTP 不支持程序化设置杠杆")
    }

    /// 参考价来自公共行情流（market.rs 写 QuoteTable），不直连柜台。
    async fn get_reference_price(&self, _instrument: &str) -> anyhow::Result<ReferencePrice> {
        anyhow::bail!("CTP 参考价经公共行情流获取（market.rs）")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::OrderPositionSide;
    use rust_decimal::Decimal;
    use trading_models::{InstrumentType, OrderType, Side, TimeInForce};
    fn place_req() -> PlaceOrderRequest {
        PlaceOrderRequest {
            instrument: "rb2510".into(),
            instrument_type: InstrumentType::Futures,
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(Decimal::from(3205)),
            quantity: Decimal::from(2),
            reduce_only: false,
            position_side: OrderPositionSide::Both,
            time_in_force: Some(TimeInForce::Gtc),
            client_order_id: "0xabc".into(),
        }
    }

    #[tokio::test]
    async fn place_order_submits_via_mailbox_and_stamps_order_ref() {
        // Arrange：模拟长会话线程——从信箱取单并回执受理码 0（无 FFI）。
        let registry = std::sync::Arc::new(crate::ctp::order_ref::OrderRefRegistry::new());
        let inserts = std::sync::Arc::new(crate::ctp::mailbox::InsertMailbox::new());
        let api = CtpApi::new(
            "acct".into(),
            std::sync::Arc::clone(&registry),
            std::sync::Arc::clone(&inserts),
        );
        let responder = tokio::spawn(async move {
            loop {
                if let Some(p) = inserts.take() {
                    let _ = p.reply.send(Ok(0));
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        });

        // Act：长 cid（34 字节）走映射分配；place_req 夹具的 0xabc 会透传。
        let mut req = place_req();
        req.client_order_id = format!("0x{}", "a".repeat(32));
        let ack = api.place_order(&req).await.unwrap();
        responder.await.unwrap();

        // Assert：受理 + OrderAck 回填 OrderRef；registry 登记 cid↔OrderRef。
        assert!(ack.accepted);
        let order_ref = ack.exchange_order_id.expect("应回填 OrderRef");
        assert!(order_ref.len() <= 13, "OrderRef 上限 13 字节");
        assert_eq!(
            registry.cid_of(&order_ref).as_deref(),
            Some(req.client_order_id.as_str())
        );
    }

    #[test]
    fn exchange_name_is_ctp() {
        let api = CtpApi::new(
            "acct".into(),
            std::sync::Arc::new(crate::ctp::order_ref::OrderRefRegistry::new()),
            std::sync::Arc::new(crate::ctp::mailbox::InsertMailbox::new()),
        );
        assert_eq!(api.exchange(), "ctp");
    }

    #[test]
    fn place_req_template_is_valid() {
        // 防御性：模板请求始终可被 prep 装配（保证测试模板不腐化）。
        assert!(prep::build_open_or_close(&place_req(), None).is_ok());
    }
}
