//! CTP 订单 API 主体：实现 [`ExchangeApi`]。
//!
//! 柜台调用经专用会话执行（`spawn_blocking` + 短会话），async 侧不阻塞
//! tokio 调度器；本结构是 tokio 侧的薄门面。

use async_trait::async_trait;

use crate::api::{
    ExchangeApi, OrderAck, OrderLookup, OrderRef, PlaceErrorKind, PlaceOrderRequest, PositionInfo,
    ReferencePrice,
};
use crate::ctp::mailbox::{
    CancelJob, PendingInsert, QueryOrderJob, QueryPositionsJob, SessionMailbox, SessionRequest,
};
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
/// 撤单/查单等待回执超时（受 2s 事件轮询粒度 + 柜台查询流控影响，给 5s）。
const CANCEL_QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
/// 查仓等待回执超时（按需直调方可能赶上对账在途，放宽到 10s）。
const POSITIONS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// CTP 下单/查询门面（tokio 侧持有；实际执行在阻塞会话内）。
pub struct CtpApi {
    /// 账户名（日志标识；当前实现未消费，保留供回调扩展）。
    #[allow(dead_code)]
    account: String,
    /// cid ↔ OrderRef 映射（与账户流长会话共享；回报反查同一份表）。
    order_refs: std::sync::Arc<OrderRefRegistry>,
    /// 会话请求信箱（下单/撤单/查询；与账户流长会话线程共享同一份）。
    mailbox: std::sync::Arc<SessionMailbox>,
    /// 进程级共享合约静态目录（SHFE 市价转限价判交易所/取 tick；与规则引擎同源）。
    instrument_dir: std::sync::Arc<crate::ctp::instrument_dir::InstrumentDir>,
    /// 共享行情表（SHFE 市价转限价读 best bid/ask）。
    quotes: std::sync::Arc<trading_core::quote::QuoteTable>,
    /// SHFE 市价开仓穿价漂移档数（CTP 独立配置）。
    shfe_drift_ticks: i64,
}

impl CtpApi {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        account: String,
        order_refs: std::sync::Arc<OrderRefRegistry>,
        mailbox: std::sync::Arc<SessionMailbox>,
        instrument_dir: std::sync::Arc<crate::ctp::instrument_dir::InstrumentDir>,
        quotes: std::sync::Arc<trading_core::quote::QuoteTable>,
        shfe_drift_ticks: i64,
    ) -> Self {
        Self {
            account,
            order_refs,
            mailbox,
            instrument_dir,
            quotes,
            shfe_drift_ticks,
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
        // 手动下单与规则引擎走同一套 SHFE 市价→穿价限价转换（单一事实源）：
        // 非市价/非 SHFE 原样；SHFE 合约或盘口未就绪（DataNotReady）时不挂起重试
        // （同步请求无保活语义），直接回 rejected ACK，由调用方稍后重试。
        let req = match prep::prepare_ctp_market(
            &self.instrument_dir,
            &self.quotes,
            self.shfe_drift_ticks,
            req.clone(),
        ) {
            Ok(prepared) => prepared,
            Err(crate::api::PlaceDefer::DataNotReady) => {
                return Ok(make_ack(
                    req,
                    false,
                    None,
                    Some("CTP 合约/行情尚未就绪，SHFE 市价无法定价，请稍后重试".into()),
                ));
            }
            // 当前 check_close_spread 恒放行（价差闸门为预留位），此臂暂不可达；
            // 保留是为将来启用闸门时手动路径有与规则引擎一致的兜底，勿当死代码删除。
            Err(crate::api::PlaceDefer::SpreadTooWide { .. }) => {
                return Ok(make_ack(
                    req,
                    false,
                    None,
                    Some("CTP 盘口价差过宽，暂不接受市价转限价".into()),
                ));
            }
        };
        let order_req = match prep::build_open_or_close(&req, None) {
            Ok(r) => r,
            Err(PlaceErrorKind::Rejected) => {
                return Ok(make_ack(
                    &req,
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
        let _guard = self.mailbox.lock_submission().await;
        // OrderRef 客户端自填、柜台原样回传：锁内分配（分配顺序=提交顺序）
        // 并登记映射；受理成功即回填 OrderAck.exchange_order_id（宿主落订单行，
        // 重启恢复的真相源）。投递到长会话线程：复用已登录 API 实例，毫秒级提交。
        let order_ref = self.order_refs.allocate(&client_order_id);
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.mailbox.submit(SessionRequest::Insert(PendingInsert {
            req: order_req,
            order_ref: order_ref.clone(),
            reply: reply_tx,
        }));
        // 会话线程回传的错误已带类型（CtpPlaceError，见 venue 提交链路），原样
        // 透传以便 classify_place_error 精确判别；超时/线程退出是应答不确定。
        let code = match tokio::time::timeout(SUBMIT_TIMEOUT, reply_rx).await {
            Ok(Ok(ret)) => ret?,
            Ok(Err(_)) => {
                return Err(crate::ctp::place_error::CtpPlaceError::indeterminate(
                    "CTP 会话线程已退出，下单结果不确定",
                ))
            }
            Err(_) => {
                return Err(crate::ctp::place_error::CtpPlaceError::indeterminate(
                    "CTP 下单等待会话线程超时（5s），结果不确定",
                ))
            }
        };
        // 受理码 0=受理；非 0 为柜台拒绝（同步拒）。
        let accepted = code == 0;
        if !accepted {
            // 同步拒：未受理的 OrderRef 不会有回报，清脏映射。
            self.order_refs.remove(&order_ref);
        }
        Ok(make_ack(
            &req,
            accepted,
            accepted.then_some(order_ref),
            (!accepted).then(|| format!("CTP 报单受理码 {code}")),
        ))
    }

    /// 撤单：投递信箱，定位（三元组/系统号）在会话线程内完成。
    ///
    /// 受理码 0 仅代表柜台受理撤单请求，真正撤成以后续 `on_rtn_order` 撤单
    /// 终态为准（宿主乐观 apply_canceled 幂等）；25/26 等异步拒绝经错误回报可诊断。
    async fn cancel_order(&self, instrument: &str, order: &OrderRef) -> anyhow::Result<OrderAck> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        // 撤单不占下单提交锁（与下单是不同的柜台动作，串行由会话线程保证）。
        self.mailbox.submit(SessionRequest::Cancel(CancelJob {
            instrument: instrument.to_string(),
            order: order.clone(),
            reply: reply_tx,
        }));
        let code = match tokio::time::timeout(CANCEL_QUERY_TIMEOUT, reply_rx).await {
            Ok(Ok(ret)) => ret?,
            Ok(Err(_)) => anyhow::bail!("CTP 会话线程已退出，撤单结果不确定"),
            Err(_) => anyhow::bail!("CTP 撤单等待会话线程超时（5s）"),
        };
        let accepted = code == 0;
        Ok(OrderAck {
            accepted,
            client_order_id: order.client_order_id.clone(),
            exchange_order_id: None,
            status: if accepted {
                trading_models::OrderStatus::Canceled
            } else {
                trading_models::OrderStatus::Rejected
            },
            message: (!accepted).then(|| format!("CTP 撤单受理码 {code}")),
            placed_type: None,
            placed_price: None,
            placed_quantity: None,
        })
    }

    /// 查单：柜台只支持按 InstrumentID 拉当日全部委托，会话线程拉回后客户端匹配。
    ///
    /// 重启后以 exchange_order_id（OrderSysID）为准；同进程附 cid→registry
    /// 反查出的 OrderRef 兜底。查无分流（CTP 无历史委托查询，均不判 `NotExists`）：
    /// * 当日报单 / eid 无日期 / 登录交易日缺失 / 断线超时 → `Unavailable`（下轮再试，
    ///   绝不把在途 UNKNOWN 单误收敛）；
    /// * OrderSysID 交易日前缀早于当前登录交易日（跨日历史单）且当日簿查无 →
    ///   `Found(Canceled, 0 成交)`，交上层 `Found(终态)` 分支收口为已取消。
    async fn query_order(&self, instrument: &str, order: &OrderRef) -> OrderLookup {
        // 同进程：cid 反查本会话 OrderRef 作兜底匹配值（重启后 registry 空 → None）。
        let hint = self.order_refs.order_ref_of_cid(&order.client_order_id);
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.mailbox
            .submit(SessionRequest::QueryOrder(QueryOrderJob {
                instrument: instrument.to_string(),
                exchange_order_id: order.exchange_order_id.clone().unwrap_or_default(),
                order_ref_hint: hint,
                reply: reply_tx,
            }));
        match tokio::time::timeout(CANCEL_QUERY_TIMEOUT, reply_rx).await {
            // 会话线程已在 QueryEnd 给出三态结论。
            Ok(Ok(lookup)) => lookup,
            // 断线（Sender drop）或超时：查询本身失败 ≠ 订单不存在，一律 Unavailable。
            _ => OrderLookup::Unavailable,
        }
    }

    /// 查仓：复用长会话线程的 ReqQryInvestorPosition + 同一 mapper，经信箱
    /// oneshot 回真实快照（CTP 无 REST；与后台去抖对账共用同一查询路径）。
    async fn get_positions(&self) -> anyhow::Result<Vec<PositionInfo>> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.mailbox
            .submit(SessionRequest::QueryPositions(QueryPositionsJob {
                reply: reply_tx,
            }));
        match tokio::time::timeout(POSITIONS_TIMEOUT, reply_rx).await {
            Ok(Ok(ret)) => ret,
            Ok(Err(_)) => anyhow::bail!("CTP 会话线程已退出，持仓查询失败（会话断开）"),
            Err(_) => anyhow::bail!("CTP 持仓查询等待会话线程超时（10s）"),
        }
    }

    /// CTP 无程序化杠杆设置语义（保证金率由柜台/交易所规定）。
    async fn set_leverage(&self, _instrument: &str, _leverage: u32) -> anyhow::Result<()> {
        anyhow::bail!("CTP 不支持程序化设置杠杆")
    }

    /// CTP 保证金率由柜台规定，无程序化杠杆接口；开仓前一律跳过 set_leverage。
    fn supports_programmatic_leverage(&self) -> bool {
        false
    }

    /// CTP 下单错误分类：
    ///
    /// * 信箱超时 / 会话线程退出 / 5s 无回执——应答不确定，订单是否已到柜台
    ///   不可知，落 [`PlaceErrorKind::Indeterminate`]（UNKNOWN，等回报/查单收敛）；
    /// * 其余均为投递前的**确定性**失败：下单要素非法、SHFE 平仓超今昨总仓、
    ///   柜台同步受理码非 0——订单确定未受理，落 [`PlaceErrorKind::Rejected`]。
    fn classify_place_error(&self, err: &anyhow::Error) -> PlaceErrorKind {
        // 按类型化会话错误判别，不依赖错误文案：会话线程投递前/受理阶段的
        // 确定性失败包 CtpPlaceError。其余（未类型化错误，如今昨未就绪等可
        // 重试情形、未知错误）保守判 Indeterminate，避免把可重试误终态拒绝。
        match err.downcast_ref::<crate::ctp::place_error::CtpPlaceError>() {
            Some(crate::ctp::place_error::CtpPlaceError::Rejected(_)) => PlaceErrorKind::Rejected,
            Some(crate::ctp::place_error::CtpPlaceError::Indeterminate(_)) => {
                PlaceErrorKind::Indeterminate
            }
            None => PlaceErrorKind::Indeterminate,
        }
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

    /// 测试用 CtpApi：可注入合约目录/行情表（漂移 1 档）。
    fn test_api(
        order_refs: std::sync::Arc<crate::ctp::order_ref::OrderRefRegistry>,
        mailbox: std::sync::Arc<SessionMailbox>,
        instrument_dir: std::sync::Arc<crate::ctp::instrument_dir::InstrumentDir>,
        quotes: std::sync::Arc<trading_core::quote::QuoteTable>,
    ) -> CtpApi {
        CtpApi::new(
            "acct".into(),
            order_refs,
            mailbox,
            instrument_dir,
            quotes,
            1,
        )
    }

    /// 构造带 SHFE 合约（tick）与双边盘口的手动下单 API。
    fn shfe_api_with_book(
        bid: f64,
        ask: f64,
    ) -> (
        CtpApi,
        std::sync::Arc<SessionMailbox>,
        std::sync::Arc<crate::ctp::instrument_dir::InstrumentDir>,
    ) {
        use crate::ctp::core::event::InstrumentInfo;
        let registry = std::sync::Arc::new(crate::ctp::order_ref::OrderRefRegistry::new());
        let mailbox = std::sync::Arc::new(SessionMailbox::new());
        let dir = std::sync::Arc::new(crate::ctp::instrument_dir::InstrumentDir::new());
        dir.remember(InstrumentInfo {
            instrument: "rb2510".into(),
            exchange_id: "SHFE".into(),
            product_id: "rb".into(),
            is_trading: true,
            volume_multiple: 10,
            price_tick: 1.0,
            long_margin_ratio: 0.0,
            short_margin_ratio: 0.0,
        });
        let quotes = std::sync::Arc::new(trading_core::quote::QuoteTable::new());
        use trading_models::{Instrument, MarketKind, TopOfBook, Venue};
        let mut u = trading_core::quote::QuoteUpdate::new(1);
        u.book = Some(TopOfBook::new(
            Some(Decimal::try_from(bid).unwrap()),
            Some(Decimal::try_from(ask).unwrap()),
        ));
        quotes.apply(
            &Instrument::new(Venue::Ctp, MarketKind::Futures, "rb2510"),
            u,
        );
        let api = test_api(registry, mailbox.clone(), dir.clone(), quotes);
        (api, mailbox, dir)
    }

    fn market_req(side: Side, reduce_only: bool) -> PlaceOrderRequest {
        PlaceOrderRequest {
            instrument: "rb2510".into(),
            instrument_type: InstrumentType::Futures,
            side,
            order_type: OrderType::Market,
            price: None,
            quantity: Decimal::from(2),
            reduce_only,
            position_side: OrderPositionSide::Both,
            time_in_force: Some(TimeInForce::Ioc),
            client_order_id: format!("0x{}", "b".repeat(32)),
        }
    }

    #[tokio::test]
    async fn manual_shfe_market_buy_open_converts_to_cross_limit_like_rule_engine() {
        // Arrange：SHFE 市价买开，bid3204/ask3205，drift=1 → 3206 穿价限价。
        let (api, mailbox, _dir) = shfe_api_with_book(3204.0, 3205.0);
        let captured = std::sync::Arc::new(tokio::sync::Mutex::new(None));
        let cap2 = captured.clone();
        let responder = tokio::spawn(async move {
            loop {
                if let Some(SessionRequest::Insert(p)) = mailbox.take_next() {
                    *cap2.lock().await =
                        Some((p.req.price_type, p.req.time_condition, p.req.price));
                    let _ = p.reply.send(Ok(0));
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        });
        // Act
        let ack = api
            .place_order(&market_req(Side::Buy, false))
            .await
            .unwrap();
        responder.await.unwrap();
        // Assert：受理成功；柜台收到的是限价 GFD、3206（与规则引擎同口径）。
        assert!(ack.accepted, "SHFE 市价应转限价后受理");
        let (pt, tc, price) = captured.lock().await.unwrap();
        use crate::ctp::core::types::{PriceType, TimeCondition};
        assert_eq!(pt, PriceType::LimitPrice);
        assert_eq!(tc, TimeCondition::Gfd);
        assert_eq!(price, 3206.0);
    }

    #[tokio::test]
    async fn manual_shfe_market_close_long_uses_best_bid_no_drift() {
        // 平多（sell+reduce_only）：best bid 3204，不漂移。
        let (api, mailbox, _dir) = shfe_api_with_book(3204.0, 3205.0);
        let captured = std::sync::Arc::new(tokio::sync::Mutex::new(None));
        let cap2 = captured.clone();
        let responder = tokio::spawn(async move {
            loop {
                if let Some(SessionRequest::Insert(p)) = mailbox.take_next() {
                    *cap2.lock().await = Some(p.req.price);
                    let _ = p.reply.send(Ok(0));
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        });
        let _ = api
            .place_order(&market_req(Side::Sell, true))
            .await
            .unwrap();
        responder.await.unwrap();
        assert_eq!(*captured.lock().await, Some(3204.0));
    }

    #[tokio::test]
    async fn manual_non_shfe_market_stays_native_anyprice_ioc() {
        // DCE 市价：不转限价，柜台收到 AnyPrice+IOC。
        use crate::ctp::core::event::InstrumentInfo;
        let registry = std::sync::Arc::new(crate::ctp::order_ref::OrderRefRegistry::new());
        let mailbox = std::sync::Arc::new(SessionMailbox::new());
        let dir = std::sync::Arc::new(crate::ctp::instrument_dir::InstrumentDir::new());
        dir.remember(InstrumentInfo {
            instrument: "rb2510".into(),
            exchange_id: "DCE".into(),
            product_id: "i".into(),
            is_trading: true,
            volume_multiple: 100,
            price_tick: 0.5,
            long_margin_ratio: 0.0,
            short_margin_ratio: 0.0,
        });
        let req = market_req(Side::Buy, false);
        let api = test_api(
            registry,
            mailbox.clone(),
            dir,
            std::sync::Arc::new(trading_core::quote::QuoteTable::new()),
        );
        let captured = std::sync::Arc::new(tokio::sync::Mutex::new(None));
        let cap2 = captured.clone();
        let responder = tokio::spawn(async move {
            loop {
                if let Some(SessionRequest::Insert(p)) = mailbox.take_next() {
                    *cap2.lock().await = Some((p.req.price_type, p.req.time_condition));
                    let _ = p.reply.send(Ok(0));
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        });
        let _ = api.place_order(&req).await.unwrap();
        responder.await.unwrap();
        use crate::ctp::core::types::{PriceType, TimeCondition};
        assert_eq!(
            *captured.lock().await,
            Some((PriceType::AnyPrice, TimeCondition::Ioc))
        );
    }

    #[tokio::test]
    async fn manual_shfe_market_without_contract_returns_rejected_ack_not_pending() {
        // 合约目录未预热（空）：手动同步请求不挂起，直接 rejected ACK，且不投信箱。
        let registry = std::sync::Arc::new(crate::ctp::order_ref::OrderRefRegistry::new());
        let mailbox = std::sync::Arc::new(SessionMailbox::new());
        let api = test_api(
            registry,
            mailbox.clone(),
            std::sync::Arc::new(crate::ctp::instrument_dir::InstrumentDir::new()),
            std::sync::Arc::new(trading_core::quote::QuoteTable::new()),
        );
        let ack = api
            .place_order(&market_req(Side::Buy, false))
            .await
            .unwrap();
        assert!(!ack.accepted, "未就绪应 rejected");
        assert!(ack.message.unwrap().contains("尚未就绪"));
        // 不应有任何请求进入信箱（take_next 应保持空）。
        assert!(mailbox.take_next().is_none());
    }

    #[tokio::test]
    async fn place_order_submits_via_mailbox_and_stamps_order_ref() {
        // Arrange：模拟长会话线程——从信箱取单并回执受理码 0（无 FFI）。
        let registry = std::sync::Arc::new(crate::ctp::order_ref::OrderRefRegistry::new());
        let mailbox = std::sync::Arc::new(crate::ctp::mailbox::SessionMailbox::new());
        let api = test_api(
            std::sync::Arc::clone(&registry),
            std::sync::Arc::clone(&mailbox),
            std::sync::Arc::new(crate::ctp::instrument_dir::InstrumentDir::new()),
            std::sync::Arc::new(trading_core::quote::QuoteTable::new()),
        );
        let responder = tokio::spawn(async move {
            use crate::ctp::mailbox::SessionRequest;
            loop {
                // 模拟长会话线程：取到 Insert 即回执受理码 0（其余变体本期不出现）。
                if let Some(SessionRequest::Insert(p)) = mailbox.take_next() {
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
        let api = test_api(
            std::sync::Arc::new(crate::ctp::order_ref::OrderRefRegistry::new()),
            std::sync::Arc::new(crate::ctp::mailbox::SessionMailbox::new()),
            std::sync::Arc::new(crate::ctp::instrument_dir::InstrumentDir::new()),
            std::sync::Arc::new(trading_core::quote::QuoteTable::new()),
        );
        assert_eq!(api.exchange(), "ctp");
    }

    #[test]
    fn ctp_does_not_support_programmatic_leverage() {
        // CTP 无程序化杠杆接口：开仓规则/手动开仓据此跳过 set_leverage，
        // 不会因“CTP 不支持程序化设置杠杆”而把开仓单误判拒单。
        let api = test_api(
            std::sync::Arc::new(crate::ctp::order_ref::OrderRefRegistry::new()),
            std::sync::Arc::new(crate::ctp::mailbox::SessionMailbox::new()),
            std::sync::Arc::new(crate::ctp::instrument_dir::InstrumentDir::new()),
            std::sync::Arc::new(trading_core::quote::QuoteTable::new()),
        );
        assert!(!api.supports_programmatic_leverage());
    }

    #[test]
    fn classifies_deterministic_reject_vs_indeterminate() {
        let api = test_api(
            std::sync::Arc::new(crate::ctp::order_ref::OrderRefRegistry::new()),
            std::sync::Arc::new(crate::ctp::mailbox::SessionMailbox::new()),
            std::sync::Arc::new(crate::ctp::instrument_dir::InstrumentDir::new()),
            std::sync::Arc::new(trading_core::quote::QuoteTable::new()),
        );
        use crate::ctp::place_error::CtpPlaceError;
        // 类型化：确定性拒绝（超今昨总仓等）→ Rejected。
        let rejected = CtpPlaceError::rejected("CTP SHFE 平仓被拒：rb2612 平仓量 20 超过今+昨总仓");
        assert!(matches!(
            api.classify_place_error(&rejected),
            crate::api::PlaceErrorKind::Rejected
        ));
        // 类型化：信箱超时 / 线程退出 → Indeterminate（落 UNKNOWN，等回报/查单）。
        let unknown = CtpPlaceError::indeterminate("CTP 下单等待会话线程超时（5s），结果不确定");
        assert!(matches!(
            api.classify_place_error(&unknown),
            crate::api::PlaceErrorKind::Indeterminate
        ));
        // 未类型化的“可重试/推迟”错误（如今昨分布未就绪）：保守判 Indeterminate，
        // 绝不能因文案含“被拒”字样就误终态 REJECTED（对账完成后仍可下单）。
        let deferrable = anyhow::anyhow!(
            "CTP SHFE 平仓推迟：rb2612 的今/昨仓分布尚未就绪（重连/首轮持仓对账未完成）"
        );
        assert!(matches!(
            api.classify_place_error(&deferrable),
            crate::api::PlaceErrorKind::Indeterminate
        ));
    }

    #[test]
    fn place_req_template_is_valid() {
        // 防御性：模板请求始终可被 prep 装配（保证测试模板不腐化）。
        assert!(prep::build_open_or_close(&place_req(), None).is_ok());
    }

    // ---- 撤单 / 查单 / 查仓（假线程应答信箱，不触 FFI）----

    use crate::api::OrderState;
    use crate::ctp::mailbox::{CancelJob, QueryPositionsJob, SessionMailbox, SessionRequest};

    fn api_with_mailbox() -> (CtpApi, std::sync::Arc<SessionMailbox>) {
        let mailbox = std::sync::Arc::new(SessionMailbox::new());
        let api = test_api(
            std::sync::Arc::new(crate::ctp::order_ref::OrderRefRegistry::new()),
            std::sync::Arc::clone(&mailbox),
            std::sync::Arc::new(crate::ctp::instrument_dir::InstrumentDir::new()),
            std::sync::Arc::new(trading_core::quote::QuoteTable::new()),
        );
        (api, mailbox)
    }

    fn found_state() -> OrderState {
        OrderState {
            client_order_id: Some("101".into()),
            exchange_order_id: Some("20260101-2".into()),
            instrument: "rb2510".into(),
            status: trading_models::OrderStatus::New,
            filled_quantity: Decimal::ZERO,
            average_price: None,
            position_side: OrderPositionSide::Both,
            venue_extra: None,
        }
    }

    /// 假线程：取到第一笔请求后按变体应答（模拟专用 OS 线程）。
    fn spawn_worker(
        mailbox: std::sync::Arc<SessionMailbox>,
        reply: impl FnOnce(SessionRequest) + Send + 'static,
    ) {
        tokio::spawn(async move {
            loop {
                if let Some(req) = mailbox.take_next() {
                    reply(req);
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });
    }

    #[tokio::test]
    async fn cancel_accepted_returns_canceled_ack() {
        // Arrange：假线程对撤单回执受理码 0。
        let (api, mailbox) = api_with_mailbox();
        spawn_worker(mailbox, |req| match req {
            SessionRequest::Cancel(CancelJob { reply, .. }) => {
                let _ = reply.send(Ok(0));
            }
            _ => panic!("期望撤单请求"),
        });

        // Act
        let ack = api
            .cancel_order(
                "rb2510",
                &OrderRef::new("0xcid").with_exchange("20260101-2"),
            )
            .await
            .unwrap();

        // Assert：受理 + 乐观 Canceled（终态以后续 on_rtn_order 为准）。
        assert!(ack.accepted);
        assert_eq!(ack.status, trading_models::OrderStatus::Canceled);
        assert_eq!(ack.client_order_id, "0xcid");
    }

    #[tokio::test]
    async fn cancel_thread_reject_or_drop_returns_error() {
        // Arrange：假线程回执无法定位错误（Err）——门面必须上抛而非误标撤销。
        let (api, mailbox) = api_with_mailbox();
        spawn_worker(mailbox, |req| match req {
            SessionRequest::Cancel(CancelJob { reply, .. }) => {
                let _ = reply.send(Err(anyhow::anyhow!("交易所目录未预热")));
            }
            _ => panic!("期望撤单请求"),
        });
        // Act / Assert
        assert!(api
            .cancel_order("rb2510", &OrderRef::new("0xcid"))
            .await
            .is_err());

        // Arrange：线程退出（reply 直接 drop）——立即 RecvError，不等 5s。
        let (api2, mailbox2) = api_with_mailbox();
        spawn_worker(mailbox2, |req| match req {
            SessionRequest::Cancel(CancelJob { reply, .. }) => drop(reply),
            _ => panic!("期望撤单请求"),
        });
        let start = std::time::Instant::now();
        // Act / Assert：快速失败（远小于 5s 超时）。
        assert!(api2
            .cancel_order("rb2510", &OrderRef::new("0xcid"))
            .await
            .is_err());
        assert!(start.elapsed() < std::time::Duration::from_secs(2));
    }

    #[tokio::test]
    async fn query_order_hit_returns_found() {
        // Arrange：假线程对查单回 Found。
        let (api, mailbox) = api_with_mailbox();
        spawn_worker(mailbox, |req| match req {
            SessionRequest::QueryOrder(q) => {
                let _ = q.reply.send(OrderLookup::Found(found_state()));
            }
            _ => panic!("期望查单请求"),
        });

        // Act
        let out = api
            .query_order(
                "rb2510",
                &OrderRef::new("0xcid").with_exchange("20260101-2"),
            )
            .await;

        // Assert
        match out {
            OrderLookup::Found(st) => {
                assert_eq!(st.exchange_order_id.as_deref(), Some("20260101-2"))
            }
            other => panic!("期望 Found，实际 {other:?}"),
        }
    }

    #[tokio::test]
    async fn query_order_empty_or_disconnected_returns_unavailable() {
        // Arrange：空结果 → Unavailable（绝不 NotExists）。
        let (api, mailbox) = api_with_mailbox();
        spawn_worker(mailbox, |req| match req {
            SessionRequest::QueryOrder(q) => {
                let _ = q.reply.send(OrderLookup::Unavailable);
            }
            _ => panic!("期望查单请求"),
        });
        // Act / Assert
        assert!(matches!(
            api.query_order("rb2510", &OrderRef::new("0xcid")).await,
            OrderLookup::Unavailable
        ));

        // Arrange：线程退出（reply drop）——同样 Unavailable，不代表订单不存在。
        let (api2, mailbox2) = api_with_mailbox();
        spawn_worker(mailbox2, |req| match req {
            SessionRequest::QueryOrder(q) => drop(q.reply),
            _ => panic!("期望查单请求"),
        });
        // Act / Assert
        assert!(matches!(
            api2.query_order("rb2510", &OrderRef::new("0xcid")).await,
            OrderLookup::Unavailable
        ));
    }

    #[tokio::test]
    async fn query_order_passes_eid_and_registry_hint() {
        // Arrange：长 cid 经 registry 分配过 OrderRef；假线程断言收到 eid + hint。
        let registry = std::sync::Arc::new(crate::ctp::order_ref::OrderRefRegistry::new());
        let mailbox = std::sync::Arc::new(SessionMailbox::new());
        let cid = format!("0x{}", "a".repeat(32));
        let hint = registry.allocate(&cid);
        let api = test_api(
            std::sync::Arc::clone(&registry),
            std::sync::Arc::clone(&mailbox),
            std::sync::Arc::new(crate::ctp::instrument_dir::InstrumentDir::new()),
            std::sync::Arc::new(trading_core::quote::QuoteTable::new()),
        );
        let (tx, rx) = tokio::sync::oneshot::channel();
        spawn_worker(mailbox, move |req| match req {
            SessionRequest::QueryOrder(q) => {
                let _ = tx.send((q.exchange_order_id, q.order_ref_hint));
                let _ = q.reply.send(OrderLookup::Unavailable);
            }
            _ => panic!("期望查单请求"),
        });

        // Act
        let _ = api
            .query_order("rb2510", &OrderRef::new(cid).with_exchange("20260101-2"))
            .await;

        // Assert：线程收到首选 eid 与 registry 反查 hint。
        let (eid, got_hint) = rx.await.unwrap();
        assert_eq!(eid, "20260101-2");
        assert_eq!(got_hint.as_deref(), Some(hint.as_str()));
    }

    fn one_position() -> crate::api::PositionInfo {
        crate::api::PositionInfo {
            instrument: "rb2510".into(),
            instrument_type: trading_models::InstrumentType::Futures,
            position_side: OrderPositionSide::Both,
            signed_quantity: Decimal::from(3),
            entry_price: Decimal::from(3205),
            mark_price: None,
            unrealized_pnl: None,
            leverage: None,
            multiplier: None,
            notional: None,
            margin: None,
            update_ms: None,
        }
    }

    #[tokio::test]
    async fn get_positions_returns_batch() {
        // Arrange：假线程聚合后回一批持仓（含 1 条多头）。
        let (api, mailbox) = api_with_mailbox();
        spawn_worker(mailbox, |req| match req {
            SessionRequest::QueryPositions(QueryPositionsJob { reply }) => {
                let _ = reply.send(Ok(vec![one_position()]));
            }
            _ => panic!("期望查仓请求"),
        });
        // Act
        let ps = api.get_positions().await.unwrap();
        // Assert：整批透传。
        assert_eq!(ps.len(), 1);
        assert_eq!(ps[0].signed_quantity, Decimal::from(3));
    }

    #[tokio::test]
    async fn get_positions_flow_control_or_disconnect_errors() {
        // Arrange：受理失败（流控）→ Err（区别于合法空仓）。
        let (api, mailbox) = api_with_mailbox();
        spawn_worker(mailbox, |req| match req {
            SessionRequest::QueryPositions(QueryPositionsJob { reply }) => {
                let _ = reply.send(Err(anyhow::anyhow!("受理失败")));
            }
            _ => panic!("期望查仓请求"),
        });
        assert!(api.get_positions().await.is_err());

        // Arrange：线程退出（reply drop）→ Err（RecvError 映射会话断开）。
        let (api2, mailbox2) = api_with_mailbox();
        spawn_worker(mailbox2, |req| match req {
            SessionRequest::QueryPositions(QueryPositionsJob { reply }) => drop(reply),
            _ => panic!("期望查仓请求"),
        });
        let start = std::time::Instant::now();
        assert!(api2.get_positions().await.is_err());
        assert!(start.elapsed() < std::time::Duration::from_secs(2));
    }
}
