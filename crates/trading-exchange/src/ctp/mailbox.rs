//! 下单请求信箱：async 侧投递 → 长会话线程消费（复用已登录 API 实例）。
//!
//! 背景：C++ API 实例只能被拥有它的线程调用（请求须串行），而长会话的
//! `TraderHandle` 被 move 进专用 OS 线程——async 侧的 `place_order` 无法直接
//! 触达。信箱把请求送进拥有者的线程：提交（async）→ 标志置位 → `inner_loop`
//! 空闲分支取单执行 → oneshot 回执受理码。
//!
//! 语义：会话断开时请求无人取 → 提交方超时 → `Indeterminate`（断线时本就
//! 不可受理）；线程中途退出 → Sender drop → 同样不确定，无泄漏路径。

use std::sync::Mutex;

use tokio::sync::Mutex as AsyncMutex;

use crate::ctp::core::order::OrderRequest;

/// 一笔待提交的下单请求（async 侧投递 → 长会话线程消费）。
pub struct PendingInsert {
    /// 已预装配的柜台订单请求。
    pub req: OrderRequest,
    /// 已分配的 OrderRef（registry 登记在投递前完成）。
    pub order_ref: String,
    /// 受理码回执（长会话线程插单后回传；Sender drop = 结果不确定）。
    pub reply: tokio::sync::oneshot::Sender<anyhow::Result<i32>>,
}

/// 下单请求信箱（provider 级共享：async 侧投递，长会话线程消费）。
///
/// 单槽 + 提交锁：提交锁**跨回执持有**（同一账户同一时刻至多一笔在飞，
/// 并发下单在锁上排队，提交顺序=OrderRef 分配顺序）——CTP 本就要求柜台
/// 请求串行。会话断开时请求无人取 → 提交方 5s 超时返回 Indeterminate
/// （正确语义：断线时不可受理）。
///
/// 内部 AtomicBool + Mutex + tokio Mutex，自动满足 Send + Sync，经 `Arc`
/// 在 tokio async 侧与专用 OS 线程间共享。
#[derive(Default)]
pub struct InsertMailbox {
    /// 提交串行化锁（place_order 跨 await 持有）。
    submit_lock: AsyncMutex<()>,
    /// 待提交请求（Some = 有单待取；槽自身即有无语义，无需额外标志）。
    pending: Mutex<Option<PendingInsert>>,
}

impl InsertMailbox {
    pub fn new() -> Self {
        Self::default()
    }

    /// 提交串行化锁（place_order 跨 await 持有，保证并发下单排队有序）。
    pub async fn lock_submission(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.submit_lock.lock().await
    }

    /// 投递一笔待提交请求（调用方已持提交锁）。
    pub fn submit(&self, pending: PendingInsert) {
        *self.pending.lock().unwrap() = Some(pending);
    }

    /// 长会话线程取单：槽空返回 None；取走即清空。
    pub fn take(&self) -> Option<PendingInsert> {
        self.pending.lock().unwrap().take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctp::core::order::OrderRequest;
    use crate::ctp::core::types::{Direction, Offset};

    fn pending(cid: &str) -> PendingInsert {
        let (tx, _rx) = tokio::sync::oneshot::channel();
        PendingInsert {
            req: OrderRequest::limit("rb2510", Direction::Buy, Offset::Open, 3205.0, 1),
            order_ref: format!("17{cid}"),
            reply: tx,
        }
    }

    #[tokio::test]
    async fn submit_then_take_returns_pending() {
        // Arrange / Act
        let m = InsertMailbox::new();
        m.submit(pending("A"));
        let p = m.take();

        // Assert：载荷完整取回。
        assert!(p.is_some());
        assert_eq!(p.unwrap().order_ref, "17A");
    }

    #[tokio::test]
    async fn take_without_submit_returns_none() {
        // Arrange / Act / Assert：未置位标志 → 无单可取。
        let m = InsertMailbox::new();
        assert!(m.take().is_none());
    }

    #[tokio::test]
    async fn take_clears_slot_and_flag() {
        // Arrange：提交一笔并取走。
        let m = InsertMailbox::new();
        m.submit(pending("A"));
        assert!(m.take().is_some());

        // Act / Assert：槽与标志均已清空，二次 take 返回 None。
        assert!(m.take().is_none());
    }

    #[tokio::test]
    async fn submission_lock_serializes_concurrent_submitters() {
        // Arrange：并发两个提交方争锁。
        let m = std::sync::Arc::new(InsertMailbox::new());
        let g1 = m.lock_submission().await;

        // Act：另一提交方在锁释放前无法进入提交段。
        let m2 = std::sync::Arc::clone(&m);
        let task = tokio::spawn(async move {
            let _g2 = m2.lock_submission().await;
            m2.submit(pending("B"));
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(m.take().is_none(), "锁未释放时另一提交方不得写入槽位");

        // Assert：释放后对方完成提交并可取回。
        drop(g1);
        task.await.unwrap();
        let p = m.take();
        assert!(p.is_some());
        assert_eq!(p.unwrap().order_ref, "17B");
    }
}
