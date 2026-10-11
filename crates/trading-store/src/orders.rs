//! 订单仓储：在通用 [`Table<Order>`] 之上维护 `client_order_id -> id` 索引，
//! 并承载订单状态机（建单 / REST 受理 / 北向回报）。
//!
//! * 通用表只按数字主键取数，业务查单首选键是 `client_order_id`（透传交易所、
//!   回报原样带回），故仓储内额外维护一张 [`DashMap`] 索引，insert/recover 时同步；
//! * 所有状态变更都经 [`Table::update`]，行锁内完成，`updated_at` 由框架维护，
//!   业务侧不直接碰 CSV，也不另建内存结构；
//! * 记账字段（成交量/价）一律定点 [`Decimal`]，禁止浮点；
//! * 软删行的索引保留，但 [`Table::get`] 对 tombstone 返回 None，查单即不可见。

use std::sync::Arc;

use dashmap::DashMap;
use rust_decimal::Decimal;
use store_core::{StoreError, Table};
use trading_models::{Order, OrderStatus, OrderType};

/// 一条订单回报的仓储输入（与线上协议解耦）：
/// 协议层负责把交易所报文解析成此结构（状态串、RFC3339 时间在入仓储前转换好）。
#[derive(Debug, Clone)]
pub struct OrderEventInput {
    pub client_order_id: String,
    /// 回报状态；None 时维持当前状态。
    pub status: Option<OrderStatus>,
    pub exchange_order_id: Option<String>,
    /// 累计成交均价。
    pub filled_price: Option<Decimal>,
    /// 累计成交量（仓储内再做单调不减、不超委托量的守卫）。
    pub filled_quantity: Option<Decimal>,
    /// 交易所回报时间（毫秒）；None 时仓储填当前墙钟。
    pub exchange_update_ms: Option<u64>,
}

/// 订单状态机应用失败。
#[derive(Debug, thiserror::Error)]
pub enum OrderRepoError {
    /// client_order_id 在活动订单中找不到（不存在或已软删）。
    #[error("unknown client_order_id `{0}`")]
    UnknownOrder(String),
    /// client_order_id 已存在（建单幂等冲突）。
    #[error("client_order_id `{0}` already exists")]
    DuplicateClientOrderId(String),
    /// 同策略已有在途（非终态）订单占用该 signal_id。
    #[error("signal_id `{0}` already has an in-flight order")]
    DuplicateSignal(String),
    /// 终结态订单收到回报。
    #[error("order `{0}` already terminal ({1:?}); ignoring event `{2}`")]
    Terminal(String, OrderStatus, String),
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// 订单仓储句柄，廉价克隆（内部均为 Arc/DashMap）。
#[derive(Clone)]
pub struct OrderRepo {
    table: Arc<Table<Order>>,
    /// client_order_id -> 本地数字主键。
    by_cid: Arc<DashMap<String, u64>>,
    /// (策略名, signal_id) 在途占位：只登记**非终态**信号单；
    /// REJECTED 等终态时释放，允许策略用同一 signal_id 修正重发。
    by_signal: Arc<DashMap<(String, String), ()>>,
}

impl OrderRepo {
    /// 绑定一张已注册的订单表，并按表内当前数据（含 tombstone）重建 cid 索引。
    /// 恢复流程完成后调用。
    pub fn new(table: Arc<Table<Order>>) -> Self {
        let repo = Self {
            table,
            by_cid: Arc::new(DashMap::new()),
            by_signal: Arc::new(DashMap::new()),
        };
        repo.rebuild_index();
        repo
    }

    /// 按表内全量数据重建 cid -> id 索引（启动恢复后使用）。
    fn rebuild_index(&self) {
        for o in self.table.list_including_deleted() {
            self.by_cid.insert(o.client_order_id.clone(), o.id);
        }
        // 在途（非终态、未软删）信号单重新占用 (策略, signal)；终态单不占，
        // 重启后允许策略用同一 signal_id 修正重发。
        for o in self.table.list() {
            if !o.status.is_terminal() {
                if let Some(key) = Self::signal_key_of(&o) {
                    self.by_signal.insert(key, ());
                }
            }
        }
    }

    /// 订单的在途占位键 (策略名, signal_id)；规则单/无策略/无信号均不占。
    fn signal_key_of(o: &Order) -> Option<(String, String)> {
        Some((o.strategy_name.clone()?, o.signal_id.clone()?))
    }

    /// 建单：id 置 0 由表分配数字主键并自动注入 created_at/updated_at，
    /// 随后登记 cid 索引。重复 cid 直接拒绝。
    ///
    /// 用 DashMap 的 `entry` 在分片锁内原子占位，避免“检查 contains_key 后
    /// 再 insert”期间的并发竞态（两笔同 cid 单同时通过检查、各拿一个 id）。
    pub fn insert(&self, mut order: Order) -> Result<Order, OrderRepoError> {
        use dashmap::mapref::entry::Entry;

        let cid = order.client_order_id.clone();
        let cid_vacant = match self.by_cid.entry(cid.clone()) {
            Entry::Occupied(_) => return Err(OrderRepoError::DuplicateClientOrderId(cid)),
            Entry::Vacant(v) => v,
        };
        // 信号幂等：(策略, signal) 在途占位与 cid 同处原子判定，杜绝
        // 「先查重再插入」在断线重连双连接重叠时的 check-then-act 竞态。
        let sig_key = Self::signal_key_of(&order);
        let sig_vacant = match sig_key {
            Some(key) => match self.by_signal.entry(key) {
                Entry::Occupied(k) => {
                    return Err(OrderRepoError::DuplicateSignal(k.key().1.clone()))
                }
                Entry::Vacant(v) => Some(v),
            },
            None => None,
        };
        order.id = 0;
        match self.table.insert(order) {
            Ok(stored) => {
                // 落表成功后才把占位写实；此前 VacantEntry 一直占着分片锁。
                cid_vacant.insert(stored.id);
                if let Some(v) = sig_vacant {
                    v.insert(());
                }
                Ok(stored)
            }
            // 表插入失败（理论上 id=0 不会 DuplicateId）：丢弃两个 VacantEntry，
            // 占位随之释放，不会留下指向空行的索引。
            Err(e) => Err(e.into()),
        }
    }

    /// 订单进入终态时释放其 (策略, signal) 在途占位（幂等：无键则空操作）。
    /// 终态生命周期收口（**全项目唯一软删规则点**）：订单一旦进入终态即软删。
    ///
    /// 调用方一律不直接碰 `deleted`；将来要改成「不软删 / 按订单类型区分 /
    /// 延迟软删」，只改这一个函数，所有落态写方法（apply_ack/set_nonterminal_
    /// status/apply_event）随之生效，调用方零改动。
    fn apply_terminal_lifecycle(o: &mut Order) {
        if o.status.is_terminal() {
            o.deleted = true;
        }
    }

    /// 释放后策略可用同一 signal_id 修正重发；FILLED 等正常终态的重发通常不会发生。
    fn release_signal_if_terminal(&self, order: &Order) {
        if order.status.is_terminal() {
            if let Some(key) = Self::signal_key_of(order) {
                self.by_signal.remove(&key);
            }
        }
    }

    /// 按 client_order_id 取活动订单快照（克隆，不持锁返回）。
    pub fn get_by_cid(&self, client_order_id: &str) -> Option<Order> {
        let id = *self.by_cid.get(client_order_id)?.value();
        // Table::get 对已软删行返回 None。
        self.table.get(id).ok().flatten()
    }

    /// 按 (策略, 信号 id) 取**在途**订单（未软删且非终态）；查无返回 None。
    ///
    /// 信号幂等作用域是「同一策略」：不同策略用同名 signal_id 不互相挡。
    /// 终态（REJECTED/FILLED/…）不参与，允许策略修正后用同一 signal_id 重发。
    /// 权威原子判重在 [`Self::insert`]（信号占位 + cid 同一临界区），本方法仅
    /// 供下单前快速短路，避免重复构造/校验。
    pub fn get_active_by_signal(&self, strategy: &str, signal_id: &str) -> Option<Order> {
        self.table.list().into_iter().find(|o| {
            o.strategy_name.as_deref() == Some(strategy)
                && o.signal_id.as_deref() == Some(signal_id)
                && !o.status.is_terminal()
        })
    }

    /// 全部活动订单，按 id 升序（调试/对账用）。
    pub fn list_active(&self) -> Vec<Order> {
        self.table.list()
    }

    /// 按 client_order_id 取订单，**包含已软删（终态）行**。
    ///
    /// 终态软删后普通 [`Self::get_by_cid`] 返回 None；需要在订单终态后仍能回查
    /// （如规则引擎按 rule_id retire、对账补收敛）时用本方法。cid 索引覆盖 tombstone。
    pub fn peek_by_cid(&self, client_order_id: &str) -> Option<Order> {
        let id = *self.by_cid.get(client_order_id)?.value();
        self.table.get_including_deleted(id).ok().flatten()
    }

    /// REST 受理：记录交易所订单号并推进到给定状态（通常 NEW）。
    /// 终结态订单不被 ack 回退，原样返回。
    pub fn apply_ack(
        &self,
        client_order_id: &str,
        exchange_order_id: Option<String>,
        status: OrderStatus,
    ) -> Result<Order, OrderRepoError> {
        let current = self.get_required(client_order_id)?;
        // 终结态订单不被 ack 回退，原样返回。
        if current.status.is_terminal() {
            return Ok(current);
        }
        self.table.update(current.id, |o| {
            o.status = status;
            if let Some(eoid) = exchange_order_id {
                o.exchange_order_id = Some(eoid);
            }
            o.exchange_update_time = store_core::SystemClock::now_ms();
            // 与状态变更同锁收口：若进入终态即软删。
            Self::apply_terminal_lifecycle(o);
        })?;
        self.get_required(client_order_id)
    }

    /// 回写"实际挂出的委托要素"（如 deribit 把市价开仓转成限价漂移单）：
    /// 仅在非终结态（PLACING/NEW）单上更新类型/价/量；终态单不回退、原样返回。
    /// 任一字段为 None 表示该要素未被改写、保留现值。
    pub fn apply_placed(
        &self,
        client_order_id: &str,
        placed_type: Option<OrderType>,
        placed_price: Option<Decimal>,
        placed_quantity: Option<Decimal>,
    ) -> Result<Order, OrderRepoError> {
        let current = self.get_required(client_order_id)?;
        // 终态（tombstone）不回退。
        if current.status.is_terminal() {
            return Ok(current);
        }
        let id = current.id;
        self.table.update(id, move |o| {
            if o.status.is_terminal() {
                return;
            }
            if let Some(t) = placed_type {
                o.order_type = t;
            }
            if let Some(p) = placed_price {
                o.order_price = p;
            }
            if let Some(q) = placed_quantity {
                o.order_quantity = q;
            }
        })?;
        self.get_required(client_order_id)
    }

    /// REST 失败：非终结态订单进入 UNKNOWN。
    pub fn mark_unknown(&self, client_order_id: &str) -> Result<Order, OrderRepoError> {
        self.set_nonterminal_status(client_order_id, OrderStatus::Unknown)
    }

    /// 下单被拒（交易所未受理或前置校验失败）。
    pub fn apply_rejection(&self, client_order_id: &str) -> Result<Order, OrderRepoError> {
        self.set_nonterminal_status(client_order_id, OrderStatus::Rejected)
    }

    /// 撤单成功。
    pub fn apply_canceled(&self, client_order_id: &str) -> Result<Order, OrderRepoError> {
        self.set_nonterminal_status(client_order_id, OrderStatus::Canceled)
    }

    fn set_nonterminal_status(
        &self,
        client_order_id: &str,
        status: OrderStatus,
    ) -> Result<Order, OrderRepoError> {
        let current = self.get_required(client_order_id)?;
        // 幂等：已到终态（tombstone）不再回退、也无需再写，原样返回。
        // （table.update 对软删行会报 NotFound，故终态在此提前返回。）
        if current.status.is_terminal() {
            return Ok(current);
        }
        let id = current.id;
        // 终结态不再回退：闭包内再判一次，防止与并发回报竞争。
        self.table.update(id, |o| {
            if !o.status.is_terminal() {
                o.status = status;
                o.exchange_update_time = store_core::SystemClock::now_ms();
                // 与状态变更同锁收口：进入终态即软删。
                Self::apply_terminal_lifecycle(o);
            }
        })?;
        let updated = self.get_required(client_order_id)?;
        // 进入终态（REJECTED/CANCELED/…）→ 释放信号在途占位，允许同 signal_id 修正重发。
        self.release_signal_if_terminal(&updated);
        Ok(updated)
    }

    /// 应用北向订单回报（定点推进，禁浮点）。终结态订单收到回报报错。
    pub fn apply_event(&self, ev: &OrderEventInput) -> Result<Order, OrderRepoError> {
        let current = self.get_required(&ev.client_order_id)?;
        if current.status.is_terminal() {
            return Err(OrderRepoError::Terminal(
                ev.client_order_id.clone(),
                current.status,
                ev.status.map(|s| s.wire().to_string()).unwrap_or_default(),
            ));
        }
        let id = current.id;
        let now = store_core::SystemClock::now_ms();
        self.table.update(id, |o| {
            // 行锁内再判一次终结态，杜绝与并发回报的竞争。
            if o.status.is_terminal() {
                return;
            }
            let target = ev.status.unwrap_or(o.status);
            let order_qty = o.order_quantity;

            // 累计成交量单调不减，且不超过委托量（定点守卫）。
            let mut cum = ev.filled_quantity.unwrap_or(o.filled_quantity);
            if cum < o.filled_quantity {
                cum = o.filled_quantity;
            }
            if cum > order_qty {
                cum = order_qty;
            }
            if let Some(price) = ev.filled_price {
                o.filled_price = price;
            }
            o.filled_quantity = cum;
            if let Some(eoid) = ev.exchange_order_id.clone() {
                o.exchange_order_id = Some(eoid);
            }
            o.exchange_update_time = ev.exchange_update_ms.unwrap_or(now);

            // 状态归一化：成交量只信交易所回报，绝不因状态是 FILLED 就把量补成
            // 委托量（reduce_only 超仓时交易所只成交实际持仓、却回报 FILLED，
            // 若补成委托量会让上层误判成交量）。
            o.status = match target {
                // 显式终态原样采用；成交量保持回报累计值。
                OrderStatus::Rejected | OrderStatus::Canceled | OrderStatus::Expired => target,
                // 其余（含交易所报 FILLED）：以实际成交量为准——达到委托量才
                // FILLED，否则回退为 PARTIAL。
                _ if order_qty > Decimal::ZERO && o.filled_quantity >= order_qty => {
                    OrderStatus::Filled
                }
                OrderStatus::Partial => OrderStatus::Partial,
                // 回报 FILLED 但成交量不足（超仓等）：如实标 PARTIAL。
                OrderStatus::Filled => OrderStatus::Partial,
                other => other,
            };
            // 与状态变更同锁收口：归一化后若为终态即软删（唯一软删规则点）。
            Self::apply_terminal_lifecycle(o);
        })?;
        let updated = self.get_required(&ev.client_order_id)?;
        // FILLED/REJECTED/CANCELED/EXPIRED 终态 → 释放信号在途占位，允许同 signal 重发。
        self.release_signal_if_terminal(&updated);
        Ok(updated)
    }

    /// 按 cid 取行（**含已软删 tombstone**）：
    ///
    /// 落态写方法需要对终态幂等——终态行虽已软删，但仍要读得到以便「不回退 /
    /// 报 Terminal」，故走 peek 而非只看活动行。cid 真不存在才报 UnknownOrder。
    fn get_required(&self, client_order_id: &str) -> Result<Order, OrderRepoError> {
        self.peek_by_cid(client_order_id)
            .ok_or_else(|| OrderRepoError::UnknownOrder(client_order_id.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_core::MockClock;
    use trading_models::{OrderType, Side, TimeInForce};

    fn repo() -> OrderRepo {
        let clock = Arc::new(MockClock::new(1_700_000_000_000));
        OrderRepo::new(Table::new(clock))
    }

    fn seed(repo: &OrderRepo) -> String {
        let cid = "0x00000000000000000000000000000001".to_string();
        let mut order = Order::new(1, cid.clone(), "xili", "btcusdt", Side::Buy);
        order.order_type = OrderType::Limit;
        order.time_in_force = TimeInForce::Gtc;
        order.order_price = Decimal::from(80_000);
        order.order_quantity = Decimal::from(2);
        repo.insert(order).unwrap();
        cid
    }

    fn event(
        cid: &str,
        status: Option<OrderStatus>,
        price: Option<i64>,
        qty: Option<i64>,
    ) -> OrderEventInput {
        OrderEventInput {
            client_order_id: cid.to_string(),
            status,
            exchange_order_id: None,
            filled_price: price.map(Decimal::from),
            filled_quantity: qty.map(Decimal::from),
            exchange_update_ms: Some(1_700_000_001_000),
        }
    }

    #[test]
    fn insert_assigns_id_and_indexes_cid() {
        let repo = repo();
        let cid = seed(&repo);
        let o = repo.get_by_cid(&cid).expect("indexed");
        assert_ne!(o.id, 0);
        assert!(o.created_at > 0 && o.updated_at > 0);
        assert!(repo.get_by_cid("0xdead").is_none());
    }

    #[test]
    fn duplicate_cid_is_rejected() {
        let repo = repo();
        let cid = seed(&repo);
        let err = repo
            .insert(Order::new(1, cid, "xili", "ethusdt", Side::Sell))
            .unwrap_err();
        assert!(matches!(err, OrderRepoError::DuplicateClientOrderId(_)));
    }

    fn signal_order(cid: &str, strategy: &str, sig: &str) -> Order {
        let mut o = Order::new(1, cid.to_string(), "binance", "BTCUSDT", Side::Buy);
        o.strategy_name = Some(strategy.to_string());
        o.signal_id = Some(sig.to_string());
        o
    }

    #[test]
    fn signal_dedup_is_scoped_by_strategy_and_lifecycle() {
        let repo = repo();
        let cid1 = "0x00000000000000000000000000000001";
        repo.insert(signal_order(cid1, "strat-a", "sig-1")).unwrap();

        // 同策略同 signal：命中在途单。
        assert_eq!(
            repo.get_active_by_signal("strat-a", "sig-1")
                .unwrap()
                .client_order_id,
            cid1
        );
        // 不同策略同名 signal 互不影响（M2 作用域）。
        assert!(repo.get_active_by_signal("strat-b", "sig-1").is_none());
        // 不同 signal 查不到。
        assert!(repo.get_active_by_signal("strat-a", "sig-x").is_none());
    }

    #[test]
    fn duplicate_signal_insert_rejected_then_allowed_after_rejection() {
        let repo = repo();
        let cid1 = "0x00000000000000000000000000000001";
        let cid2 = "0x00000000000000000000000000000002";
        let cid3 = "0x00000000000000000000000000000003";
        repo.insert(signal_order(cid1, "strat-a", "sig-1")).unwrap();

        // 在途：同 (策略, signal) 再插（不同 cid）被原子判重拒绝（H1）。
        let err = repo
            .insert(signal_order(cid2, "strat-a", "sig-1"))
            .unwrap_err();
        assert!(matches!(err, OrderRepoError::DuplicateSignal(_)));

        // 首发被 REJECTED → 释放占位；同 signal 修正重发允许（H2）。
        repo.apply_rejection(cid1).unwrap();
        assert!(repo.get_active_by_signal("strat-a", "sig-1").is_none());
        repo.insert(signal_order(cid3, "strat-a", "sig-1")).unwrap();
        assert_eq!(
            repo.get_active_by_signal("strat-a", "sig-1")
                .unwrap()
                .client_order_id,
            cid3
        );
    }

    #[test]
    fn terminal_order_releases_signal_lookup() {
        let repo = repo();
        let cid1 = "0x00000000000000000000000000000001";
        repo.insert(signal_order(cid1, "strat-a", "sig-1")).unwrap();
        // 推到终态（REJECTED）即不可按在途信号查到（无需软删）。
        repo.apply_rejection(cid1).unwrap();
        assert!(repo.get_active_by_signal("strat-a", "sig-1").is_none());
    }

    #[test]
    fn ack_then_partial_then_fill() {
        let repo = repo();
        let cid = seed(&repo);

        let acked = repo
            .apply_ack(&cid, Some("EX-1".into()), OrderStatus::New)
            .unwrap();
        assert_eq!(acked.status, OrderStatus::New);
        assert_eq!(acked.exchange_order_id.as_deref(), Some("EX-1"));

        let partial = repo
            .apply_event(&event(
                &cid,
                Some(OrderStatus::Partial),
                Some(80_000),
                Some(1),
            ))
            .unwrap();
        assert_eq!(partial.status, OrderStatus::Partial);

        // 成交量达到委托量时即使回报写 PARTIAL 也归一化为 FILLED。
        let filled = repo
            .apply_event(&event(
                &cid,
                Some(OrderStatus::Partial),
                Some(80_000),
                Some(2),
            ))
            .unwrap();
        assert_eq!(filled.status, OrderStatus::Filled);
        assert_eq!(filled.filled_quantity, Decimal::from(2));

        // 终结态后再来的回报必须被拒绝。
        let err = repo
            .apply_event(&event(
                &cid,
                Some(OrderStatus::Partial),
                Some(80_000),
                Some(1),
            ))
            .unwrap_err();
        assert!(matches!(err, OrderRepoError::Terminal(..)));
    }

    #[test]
    fn cumulative_qty_is_capped_to_order_qty() {
        let repo = repo();
        let cid = seed(&repo);
        // 回报量超过委托量时封顶到 2（状态随之归一化为 FILLED）。
        let o = repo
            .apply_event(&event(&cid, Some(OrderStatus::Partial), None, Some(9)))
            .unwrap();
        assert_eq!(o.filled_quantity, Decimal::from(2));
        assert_eq!(o.status, OrderStatus::Filled);
    }

    #[test]
    fn cumulative_qty_is_monotonic() {
        let repo = repo();
        let cid = seed(&repo);
        repo.apply_event(&event(&cid, Some(OrderStatus::Partial), None, Some(1)))
            .unwrap();
        // 更小的回报量不得让累计量回退。
        let o = repo
            .apply_event(&event(&cid, Some(OrderStatus::New), None, Some(0)))
            .unwrap();
        assert_eq!(o.filled_quantity, Decimal::from(1));
        assert_eq!(o.status, OrderStatus::New);
    }

    #[test]
    fn filled_event_with_less_than_order_qty_keeps_actual_qty_and_partial() {
        // reduce_only 超仓场景：交易所只成交 1（实际持仓），却回报 FILLED。
        // 成交量必须保持 1，状态如实为 PARTIAL，不得抬到委托量 2 / FILLED。
        let repo = repo();
        let cid = seed(&repo); // 委托量 2
        let o = repo
            .apply_event(&event(&cid, Some(OrderStatus::Filled), None, Some(1)))
            .unwrap();
        assert_eq!(o.filled_quantity, Decimal::from(1));
        assert_eq!(o.status, OrderStatus::Partial);
    }

    #[test]
    fn ack_does_not_roll_back_terminal() {
        let repo = repo();
        let cid = seed(&repo);
        repo.apply_rejection(&cid).unwrap();
        let o = repo.apply_ack(&cid, None, OrderStatus::New).unwrap();
        assert_eq!(o.status, OrderStatus::Rejected);
    }

    #[test]
    fn rejection_marks_row_deleted() {
        // 同步终态（REJECTED）：行即软删，活动查询不可见、peek 仍见 tombstone。
        let repo = repo();
        let cid = seed(&repo);
        let updated = repo.apply_rejection(&cid).unwrap();
        assert!(updated.status.is_terminal());
        assert!(updated.deleted, "进入终态即 deleted=1");
        assert!(repo.get_by_cid(&cid).is_none());
        let tomb = repo.peek_by_cid(&cid).expect("tombstone 仍可 peek");
        assert!(tomb.deleted);
    }

    #[test]
    fn filled_event_marks_row_deleted() {
        // WS/REST 回报推进到终态 FILLED：行同样软删。
        let repo = repo();
        let cid = seed(&repo);
        let o = repo
            .apply_event(&event(
                &cid,
                Some(OrderStatus::Filled),
                Some(80_000),
                Some(2),
            ))
            .unwrap();
        assert_eq!(o.status, OrderStatus::Filled);
        assert!(o.deleted);
        assert!(repo.get_by_cid(&cid).is_none());
    }

    #[test]
    fn canceled_marks_row_deleted() {
        let repo = repo();
        let cid = seed(&repo);
        let o = repo.apply_canceled(&cid).unwrap();
        assert!(o.deleted);
        assert!(repo.get_by_cid(&cid).is_none());
    }

    #[test]
    fn nonterminal_status_keeps_row_visible() {
        // 非终态（UNKNOWN/NEW）：不软删，活动查询仍可见。
        let repo = repo();
        let cid = seed(&repo);
        let unknown = repo.mark_unknown(&cid).unwrap();
        assert!(!unknown.deleted);
        assert!(repo.get_by_cid(&cid).is_some());

        let acked = repo
            .apply_ack(&cid, Some("EX-1".into()), OrderStatus::New)
            .unwrap();
        assert!(!acked.deleted);
        assert!(repo.get_by_cid(&cid).is_some());
    }

    #[test]
    fn terminal_soft_delete_is_idempotent() {
        // 已终态软删的行再次落终态（幂等）：不报错、仍为 tombstone。
        let repo = repo();
        let cid = seed(&repo);
        repo.apply_rejection(&cid).unwrap();
        // apply_rejection 第二次：行已软删，update 作用不到，应稳定返回而非崩溃。
        let again = repo.apply_rejection(&cid).unwrap();
        assert!(again.deleted);
        assert_eq!(again.status, OrderStatus::Rejected);
    }

    #[test]
    fn unknown_cid_errors() {
        let repo = repo();
        assert!(matches!(
            repo.mark_unknown("0xnope").unwrap_err(),
            OrderRepoError::UnknownOrder(_)
        ));
    }

    #[test]
    fn concurrent_inserts_of_same_cid_leave_single_row() {
        let repo = std::sync::Arc::new(repo());
        let cid = "0x000000000000000000000000000000aa".to_string();
        let mut handles = Vec::new();
        for _ in 0..16 {
            let repo = repo.clone();
            let cid = cid.clone();
            handles.push(std::thread::spawn(move || {
                repo.insert(Order::new(1, cid, "xili", "btcusdt", Side::Buy))
                    .is_ok()
            }));
        }
        let wins = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .filter(|ok| *ok)
            .count();
        assert_eq!(wins, 1, "exactly one concurrent insert may win");
        assert_eq!(repo.list_active().len(), 1);
        assert!(repo.get_by_cid(&cid).is_some());
    }

    #[test]
    fn late_event_after_soft_delete_reports_terminal_not_unknown() {
        // 终态软删后，迟到的同 cid 回报：peek 仍读到 tombstone，报语义准确的
        // Terminal（而非 UnknownOrder）；活动查询不可见，但 peek 可回查。
        let repo = repo();
        let cid = seed(&repo);
        repo.apply_rejection(&cid).unwrap();
        assert!(repo.get_by_cid(&cid).is_none());
        let tomb = repo.peek_by_cid(&cid).expect("tombstone 可 peek");
        assert!(tomb.deleted);
        assert!(tomb.status.is_terminal());

        // 再来一条同 cid 回报：必须报 Terminal。
        let err = repo
            .apply_event(&event(&cid, Some(OrderStatus::New), None, Some(0)))
            .unwrap_err();
        assert!(
            matches!(err, OrderRepoError::Terminal(..)),
            "迟到回报应报 Terminal，得到 {err:?}"
        );
    }

    #[test]
    fn event_for_unknown_cid_reports_unknown_order() {
        // cid 真不存在（非软删）：报 UnknownOrder。
        let repo = repo();
        let err = repo
            .apply_event(&event("0xnope", Some(OrderStatus::New), None, Some(0)))
            .unwrap_err();
        assert!(matches!(err, OrderRepoError::UnknownOrder(_)));
    }

    #[test]
    fn rebuilt_index_covers_recovered_rows() {
        let clock = Arc::new(MockClock::new(1_700_000_000_000));
        let table: Arc<Table<Order>> = Table::new(clock);
        let cid = "0x0000000000000000000000000000abcd".to_string();
        table
            .insert(Order::new(3, cid.clone(), "xili", "btcusdt", Side::Buy))
            .unwrap();
        // 模拟恢复后重新构造仓储：索引应能覆盖已有行。
        let repo = OrderRepo::new(table);
        assert_eq!(repo.get_by_cid(&cid).unwrap().account_id, 3);
        assert_eq!(repo.list_active().len(), 1);
    }
}
