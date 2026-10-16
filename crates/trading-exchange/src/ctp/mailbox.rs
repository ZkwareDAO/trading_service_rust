//! 会话请求信箱：async 侧投递 → 长会话线程消费（复用已登录 API 实例）。
//!
//! 背景：C++ API 实例只能被拥有它的线程调用（请求须串行），而长会话的
//! `TraderHandle` 被 move 进专用 OS 线程——async 侧无法直接触达。信箱把请求
//! 送进拥有者的线程：提交（async）→ FIFO 队列 → `inner_loop` 每轮取单执行 →
//! oneshot 回执。
//!
//! 四类请求共用一条 FIFO（方案①泛化，不另造同构信箱）：
//! * [`SessionRequest::Insert`]：下单（保留跨回执提交锁，同账户同时刻至多一笔在飞）；
//! * [`SessionRequest::Cancel`]：撤单；
//! * [`SessionRequest::QueryOrder`] / [`SessionRequest::QueryPositions`]：按需查询，
//!   受线程内核 `ActiveQuery` 单在途闸门串行（柜台查询 1s/次流控，Task D 接线）。
//!
//! 语义：会话断开时请求无人取 → 提交方超时（下单 5s / 撤查 5s / 查仓 10s）；
//! 线程中途退出 → 未回执 Sender drop → RecvError，调用方映射"会话断开"，无泄漏。
//! 调用方超时 drop 接收端后，取单以 [`SessionRequest::is_closed`] 跳过过期请求，
//! 不浪费柜台流控。

use std::collections::VecDeque;
use std::sync::Mutex;

use tokio::sync::Mutex as AsyncMutex;

use crate::api::PositionInfo;
use crate::ctp::core::order::OrderRequest;

/// 撤单定位方式（适配器内部单号路由的产物；宿主不感知 CTP 单号形态）。
///
/// 柜台 `ReqOrderAction` 二选一定位一笔活跃委托：
/// * 本会话（registry 命中自产 OrderRef）→ 三元组 `FrontID+SessionID+OrderRef`；
/// * 跨会话（重启后 registry 为空）→ `ExchangeID+OrderSysID`（交易所系统号）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelTarget {
    /// 本会话三元组定位。
    ByOrderRef {
        /// 前置编号（取线程当前 SessionState）。
        front_id: i32,
        /// 会话编号。
        session_id: i32,
        /// 本会话自产 OrderRef。
        order_ref: String,
    },
    /// 跨会话系统号定位。
    ByOrderSysId {
        /// 交易所代码（取自共享 InstrumentDir）。
        exchange_id: String,
        /// 交易所系统号 OrderSysID。
        order_sys_id: String,
    },
}

/// 一笔待提交的下单请求（async 侧投递 → 长会话线程消费）。
pub struct PendingInsert {
    /// 已预装配的柜台订单请求。
    pub req: OrderRequest,
    /// 已分配的 OrderRef（registry 登记在投递前完成）。
    pub order_ref: String,
    /// 受理码回执（长会话线程插单后回传；Sender drop = 结果不确定）。
    pub reply: tokio::sync::oneshot::Sender<anyhow::Result<i32>>,
}

/// 一笔撤单请求（携带未解析单号；定位在拥有 TraderHandle 的会话线程内完成，
/// 因为三元组所需的当前 FrontID/SessionID 只有线程内会话状态掌握）。
pub struct CancelJob {
    /// 合约代码（柜台撤单结构的 InstrumentID，两路定位都填）。
    pub instrument: String,
    /// 统一订单引用（cid 必带、eid 可选）；线程内 resolve 成 [`CancelTarget`]。
    pub order: crate::api::OrderRef,
    /// 受理码回执（0=受理；撤成与否以 on_rtn_order 终态为准；无法定位回 Err）。
    pub reply: tokio::sync::oneshot::Sender<anyhow::Result<i32>>,
}

/// 一笔当日委托查询请求（柜台按 InstrumentID 拉当日全部，会话线程客户端匹配）。
pub struct QueryOrderJob {
    /// 合约代码（空串查全部）。
    pub instrument: String,
    /// 首选匹配值：受理后升级的 OrderSysID（OrderRef.exchange_order_id，可空）。
    pub exchange_order_id: String,
    /// 兜底匹配值：同进程 cid→registry 反查出的本会话 OrderRef（重启后为 None）。
    pub order_ref_hint: Option<String>,
    /// 三态查单结果回执（命中 Found / 查无 Unavailable；断线 Sender drop→RecvError）。
    pub reply: tokio::sync::oneshot::Sender<crate::api::OrderLookup>,
}

/// 一笔持仓查询请求（整批聚合后回执）。
pub struct QueryPositionsJob {
    /// 聚合后的持仓回执；查仓流控/无法发起为 Err（区别于合法的空仓 Ok(vec![])）。
    /// 断线时 Sender 随 ActiveQuery drop → 接收端 RecvError，门面映射"会话断开"。
    pub reply: tokio::sync::oneshot::Sender<anyhow::Result<Vec<PositionInfo>>>,
}

/// 投递到长会话线程的请求（四类共用一条 FIFO）。
pub enum SessionRequest {
    /// 下单。
    Insert(PendingInsert),
    /// 撤单。
    Cancel(CancelJob),
    /// 当日委托查询。
    QueryOrder(QueryOrderJob),
    /// 持仓查询。
    QueryPositions(QueryPositionsJob),
}

impl SessionRequest {
    /// 调用方是否已放弃（接收端 drop：超时/断连）。过期请求取单时跳过，
    /// 不占用柜台串行/流控配额。
    pub fn is_closed(&self) -> bool {
        match self {
            SessionRequest::Insert(p) => p.reply.is_closed(),
            SessionRequest::Cancel(c) => c.reply.is_closed(),
            SessionRequest::QueryOrder(q) => q.reply.is_closed(),
            SessionRequest::QueryPositions(q) => q.reply.is_closed(),
        }
    }

    /// 变体名（日志/诊断；oneshot::Sender 非 Debug，不能整体 {:?}）。
    pub fn kind(&self) -> &'static str {
        match self {
            SessionRequest::Insert(_) => "Insert",
            SessionRequest::Cancel(_) => "Cancel",
            SessionRequest::QueryOrder(_) => "QueryOrder",
            SessionRequest::QueryPositions(_) => "QueryPositions",
        }
    }
}

/// 会话请求信箱（provider 级共享：async 侧投递，长会话线程消费）。
///
/// FIFO 队列 + 下单提交锁：提交锁**跨回执持有**（同一账户同一时刻至多一笔
/// 下单在飞，并发下单在锁上排队，提交顺序=OrderRef 分配顺序）。撤单/查询
/// 不占下单锁，直接入队，由线程内串行（查询另受 ActiveQuery 闸门约束）。
///
/// 内部 tokio Mutex + std Mutex，自动满足 Send + Sync，经 `Arc` 在 tokio
/// async 侧与专用 OS 线程间共享。
#[derive(Default)]
pub struct SessionMailbox {
    /// 下单提交串行化锁（place_order 跨 await 持有）。
    submit_lock: AsyncMutex<()>,
    /// FIFO 待处理请求。
    queue: Mutex<VecDeque<SessionRequest>>,
}

impl SessionMailbox {
    pub fn new() -> Self {
        Self::default()
    }

    /// 下单提交串行化锁（place_order 跨 await 持有，保证并发下单排队有序）。
    pub async fn lock_submission(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.submit_lock.lock().await
    }

    /// 投递一笔请求到队尾（下单调用方已持提交锁；撤单/查询直接投递）。
    pub fn submit(&self, req: SessionRequest) {
        self.queue.lock().unwrap().push_back(req);
    }

    /// 长会话线程取单：弹出队首第一个**未放弃**的请求；途中丢弃 is_closed
    /// 的过期请求（调用方已超时/放弃，不浪费柜台串行/流控配额）；队列空返回 None。
    pub fn take_next(&self) -> Option<SessionRequest> {
        let mut q = self.queue.lock().unwrap();
        while let Some(req) = q.pop_front() {
            if req.is_closed() {
                continue;
            }
            return Some(req);
        }
        None
    }

    /// 把暂不处理的请求塞回队首（仅线程内核过渡用：保持 FIFO 且不丢请求）。
    pub fn requeue_front(&self, req: SessionRequest) {
        self.queue.lock().unwrap().push_front(req);
    }
}

/// 一轮信箱扫描的处理决策（纯函数 [`plan_mailbox_drain`] 的产物，venue 据此执行）。
/// 不派生 Debug：载荷含 oneshot::Sender（非 Debug）。
pub(crate) enum MailboxAction {
    /// 立即执行：下单/撤单（非查询，不受 ActiveQuery 闸门约束）。
    Run(SessionRequest),
    /// 发起查询并占位（携带要占位的查询请求）。
    Issue(SessionRequest),
    /// 队首是查询但已有查询在途：把请求塞回队首，本轮不发新查询（保序、保串行）。
    Defer(SessionRequest),
    /// 信箱空（或仅有已放弃请求，均已丢弃）。
    Empty,
}

/// 队首请求是否为查询类（受 ActiveQuery 单在途闸门约束）。
pub(crate) fn is_query(req: &SessionRequest) -> bool {
    matches!(
        req,
        SessionRequest::QueryOrder(_) | SessionRequest::QueryPositions(_)
    )
}

/// 纯调度：给定队首请求与"当前是否有查询在途"，决定本轮动作。
///
/// 不变量：
/// * 下单/撤单永远 [`MailboxAction::Run`]——不被查询闸门阻塞；
/// * 查询仅在无在途查询时 [`MailboxAction::Issue`]，否则 [`MailboxAction::Defer`]
///   （塞回队首，严格串行，柜台查询 1s/次流控下不叠加在途查询）；
/// * 空队列 [`MailboxAction::Empty`]。
///
/// 已放弃（is_closed）请求由 [`SessionMailbox::take_next`] 在出队时丢弃，
/// 本函数只见到未过期请求。
pub(crate) fn plan_mailbox_drain(
    front: Option<SessionRequest>,
    query_in_flight: bool,
) -> MailboxAction {
    match front {
        None => MailboxAction::Empty,
        Some(req) if is_query(&req) && query_in_flight => MailboxAction::Defer(req),
        Some(req) if is_query(&req) => MailboxAction::Issue(req),
        Some(req) => MailboxAction::Run(req),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctp::core::types::{Direction, Offset};

    // 夹具回执端用 forget 保活：模拟调用方仍在等待（否则接收端随函数返回即
    // drop，take_next 会把请求当过期请求跳过；真实调用方在 await oneshot）。
    fn insert(cid: &str) -> SessionRequest {
        let (tx, rx) = tokio::sync::oneshot::channel();
        std::mem::forget(rx); // 模拟调用方仍在等待回执（接收端保活）。
        SessionRequest::Insert(PendingInsert {
            req: OrderRequest::limit("rb2510", Direction::Buy, Offset::Open, 3205.0, 1),
            order_ref: format!("17{cid}"),
            reply: tx,
        })
    }

    fn cancel() -> SessionRequest {
        let (tx, rx) = tokio::sync::oneshot::channel();
        std::mem::forget(rx); // 模拟调用方仍在等待回执（接收端保活）。
        SessionRequest::Cancel(CancelJob {
            instrument: "rb2510".into(),
            order: crate::api::OrderRef::new("0xcid"),
            reply: tx,
        })
    }

    fn query_order() -> SessionRequest {
        let (tx, rx) = tokio::sync::oneshot::channel();
        std::mem::forget(rx); // 模拟调用方仍在等待回执（接收端保活）。
        SessionRequest::QueryOrder(QueryOrderJob {
            instrument: "rb2510".into(),
            exchange_order_id: "20260101-7".into(),
            order_ref_hint: None,
            reply: tx,
        })
    }

    fn query_positions() -> SessionRequest {
        let (tx, rx) = tokio::sync::oneshot::channel();
        std::mem::forget(rx); // 模拟调用方仍在等待回执（接收端保活）。
        SessionRequest::QueryPositions(QueryPositionsJob { reply: tx })
    }

    #[tokio::test]
    async fn submit_then_take_returns_insert_payload() {
        // Arrange / Act
        let m = SessionMailbox::new();
        m.submit(insert("A"));
        let req = m.take_next();

        // Assert：载荷完整取回。
        match req {
            Some(SessionRequest::Insert(p)) => assert_eq!(p.order_ref, "17A"),
            Some(other) => panic!("期望 Insert，实际 {}", other.kind()),
            None => panic!("期望 Insert，实际 None"),
        }
    }

    #[tokio::test]
    async fn take_without_submit_returns_none() {
        // Arrange / Act / Assert：空队列无单可取。
        let m = SessionMailbox::new();
        assert!(m.take_next().is_none());
    }

    #[tokio::test]
    async fn requests_are_served_in_fifo_across_all_kinds() {
        // Arrange：四类请求混排。
        let m = SessionMailbox::new();
        m.submit(insert("A"));
        m.submit(cancel());
        m.submit(query_order());
        m.submit(query_positions());

        // Act / Assert：严格按入队顺序、类型正确地取回。
        assert!(matches!(m.take_next(), Some(SessionRequest::Insert(_))));
        assert!(matches!(m.take_next(), Some(SessionRequest::Cancel(_))));
        assert!(matches!(m.take_next(), Some(SessionRequest::QueryOrder(_))));
        assert!(matches!(
            m.take_next(),
            Some(SessionRequest::QueryPositions(_))
        ));
        assert!(m.take_next().is_none());
    }

    #[tokio::test]
    async fn take_skips_closed_requests() {
        // Arrange：队首是一笔调用方已放弃（接收端 drop）的过期撤单。
        let m = SessionMailbox::new();
        let (tx, rx) = tokio::sync::oneshot::channel::<anyhow::Result<i32>>();
        m.submit(SessionRequest::Cancel(CancelJob {
            instrument: "rb2510".into(),
            order: crate::api::OrderRef::new("0xcid"),
            reply: tx,
        }));
        m.submit(insert("B"));
        drop(rx); // 调用方超时/放弃 → reply 关闭。

        // Act / Assert：过期请求被跳过，不浪费流控，直接取到后面的有效请求。
        match m.take_next() {
            Some(SessionRequest::Insert(p)) => assert_eq!(p.order_ref, "17B"),
            Some(other) => panic!("期望跳过 closed 撤单后取到 Insert，实际 {}", other.kind()),
            None => panic!("期望取到 Insert，实际 None"),
        }
    }

    #[tokio::test]
    async fn requeue_front_keeps_request_for_next_round() {
        // Arrange：投一笔查询，取出后暂不处理塞回队首。
        let m = SessionMailbox::new();
        m.submit(query_order());
        let req = m.take_next().expect("已投递");
        m.requeue_front(req);

        // Act / Assert：下一轮仍能取到同一笔（未丢失、未重复）。
        assert!(matches!(m.take_next(), Some(SessionRequest::QueryOrder(_))));
    }

    #[tokio::test]
    async fn submission_lock_serializes_concurrent_submitters() {
        // Arrange：并发两个下单方争锁（撤单/查询不走此锁）。
        let m = std::sync::Arc::new(SessionMailbox::new());
        let g1 = m.lock_submission().await;

        // Act：另一方在锁释放前无法进入提交段。
        let m2 = std::sync::Arc::clone(&m);
        let task = tokio::spawn(async move {
            let _g2 = m2.lock_submission().await;
            m2.submit(insert("B"));
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(m.take_next().is_none(), "锁未释放时另一方不得入队");

        // Assert：释放后对方完成提交并可取回。
        drop(g1);
        task.await.unwrap();
        assert!(matches!(m.take_next(), Some(SessionRequest::Insert(_))));
    }

    // ---- 纯调度 plan_mailbox_drain（ActiveQuery 闸门的可测内核）----

    #[test]
    fn drain_empty_queue_returns_empty() {
        // Arrange / Act / Assert：无论是否有查询在途，空队列都是 Empty。
        assert!(matches!(
            plan_mailbox_drain(None, false),
            MailboxAction::Empty
        ));
        assert!(matches!(
            plan_mailbox_drain(None, true),
            MailboxAction::Empty
        ));
    }

    #[test]
    fn drain_insert_and_cancel_always_run_even_when_query_in_flight() {
        // Arrange：已有查询在途。
        // Act / Assert：下单、撤单仍立即执行（不被闸门阻塞）。
        assert!(matches!(
            plan_mailbox_drain(Some(insert("A")), true),
            MailboxAction::Run(_)
        ));
        assert!(matches!(
            plan_mailbox_drain(Some(cancel()), true),
            MailboxAction::Run(_)
        ));
    }

    #[test]
    fn drain_query_issues_when_idle_but_defers_when_in_flight() {
        // Arrange / Act / Assert：无在途查询 → Issue（本轮发出并占位）。
        assert!(matches!(
            plan_mailbox_drain(Some(query_order()), false),
            MailboxAction::Issue(_)
        ));
        assert!(matches!(
            plan_mailbox_drain(Some(query_positions()), false),
            MailboxAction::Issue(_)
        ));
        // 已有查询在途 → Defer（携带原请求塞回队首，本轮不叠加第二个查询）。
        assert!(matches!(
            plan_mailbox_drain(Some(query_order()), true),
            MailboxAction::Defer(_)
        ));
    }
}
