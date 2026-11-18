//! 交易所侧独立台账：订单 / 成交 / 持仓 / 保证金 / 余额 + 账户事件队列。
//!
//! 刻意**不复用宿主仓储与仓位计算**，作为 E2E 的独立 Oracle：系统仓位/ROI 与
//! 本台账逐字段比对，能独立暴露宿主映射/对账缺陷。
//!
//! 并发模型：内部 `Mutex` 保护可变台账；事件经共享队列 + [`Notify`] 唤醒
//! 账户事件流（不使用 channel）。所有方法同步、无 `.await`。

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use rust_decimal::Decimal;
use tokio::sync::Notify;
use trading_exchange::api::{
    AccountDirty, AccountEvent, BalanceInfo, OrderAck, OrderLookup, OrderPositionSide, OrderRef,
    OrderState, PlaceOrderRequest, PositionInfo, PriceBasis, ReferencePrice,
};
use trading_models::{OrderStatus, OrderType, Side};

use crate::{now_ms, LimitFillMode, QueryMode, SimConfig};

/// 一条模拟所订单。
#[derive(Debug, Clone)]
pub struct SimOrder {
    pub client_order_id: String,
    pub exchange_order_id: String,
    pub account: String,
    pub instrument: String,
    pub side: Side,
    pub order_type: OrderType,
    pub price: Option<Decimal>,
    pub quantity: Decimal,
    pub filled: Decimal,
    pub avg_price: Option<Decimal>,
    pub reduce_only: bool,
    pub status: OrderStatus,
}

impl SimOrder {
    fn state(&self) -> OrderState {
        OrderState {
            client_order_id: Some(self.client_order_id.clone()),
            exchange_order_id: Some(self.exchange_order_id.clone()),
            instrument: self.instrument.clone(),
            status: self.status,
            filled_quantity: self.filled,
            average_price: self.avg_price,
            position_side: OrderPositionSide::Both,
            venue_extra: None,
        }
    }

    fn remaining(&self) -> Decimal {
        (self.quantity - self.filled).max(Decimal::ZERO)
    }
}

/// 单向持仓（带符号数量 + 开仓均价）。
#[derive(Debug, Clone, Default)]
struct SimPosition {
    pub quantity: Decimal,
    pub entry_price: Decimal,
}

struct Inner {
    orders: BTreeMap<String, SimOrder>,
    /// 持仓按 `(账户, 标的)` 归集：多账户互不串仓（差分 oracle 的前提）。
    positions: BTreeMap<(String, String), SimPosition>,
    marks: BTreeMap<String, Decimal>,
    events: VecDeque<(String, AccountEvent)>,
    reject_next: Option<String>,
    /// 下一次下单模拟「应答不确定」（网络/超时）：返回 `Err` 而非 ack。
    indeterminate_next: Option<String>,
    query_mode: QueryMode,
    limit_fill: LimitFillMode,
    leverage: u32,
    /// 已实现盈亏按账户归集。
    realized: BTreeMap<String, Decimal>,
    next_seq: u64,
}

impl Inner {
    fn new(cfg: &SimConfig) -> Self {
        Self {
            orders: BTreeMap::new(),
            positions: BTreeMap::new(),
            marks: BTreeMap::new(),
            events: VecDeque::new(),
            reject_next: None,
            indeterminate_next: None,
            query_mode: QueryMode::Found,
            limit_fill: cfg.limit_fill,
            leverage: cfg.leverage,
            realized: BTreeMap::new(),
            next_seq: 0,
        }
    }

    fn push(&mut self, account: &str, ev: AccountEvent) {
        self.events.push_back((account.to_string(), ev));
    }

    fn find(&self, r: &OrderRef) -> Option<&SimOrder> {
        if let Some(o) = self.orders.get(&r.client_order_id) {
            return Some(o);
        }
        let eid = r.exchange_order_id.as_deref()?;
        self.orders.values().find(|o| o.exchange_order_id == eid)
    }

    /// 应用一笔成交：推进订单成交累计 + 更新持仓/已实现盈亏 + 产事件。
    ///
    /// `reduce_only` 订单只允许减少反向持仓：可减量为 0 时按「无可平仓位」撤单，
    /// 绝不反手开仓（与真实所语义一致，保证风控平仓的差分断言不被放大）。
    fn apply_fill(&mut self, cid: &str, qty: Decimal, price: Decimal) {
        let Some(mut order) = self.orders.get(cid).cloned() else {
            return;
        };
        let mut clamped_reduce = false;
        let qty = if order.reduce_only {
            let closable = self.closable(&order.account, &order.instrument, order.side);
            let clamped = qty.min(closable);
            if clamped <= Decimal::ZERO {
                order.status = OrderStatus::Canceled;
                self.orders.insert(cid.to_string(), order.clone());
                self.push(&order.account, AccountEvent::Order(order.state()));
                return;
            }
            clamped_reduce = clamped < qty;
            clamped
        } else {
            qty
        };
        let prev_notional = order.avg_price.unwrap_or(Decimal::ZERO) * order.filled;
        order.filled += qty;
        order.avg_price = if order.filled > Decimal::ZERO {
            Some((prev_notional + price * qty) / order.filled)
        } else {
            None
        };
        order.status = if clamped_reduce {
            // 平仓量被持仓上限截断：剩余委托已无对应仓位，按撤单收口（不得反手）。
            OrderStatus::Canceled
        } else if order.filled >= order.quantity {
            OrderStatus::Filled
        } else {
            OrderStatus::Partial
        };
        self.orders.insert(cid.to_string(), order.clone());
        self.apply_position(&order.account, &order.instrument, order.side, qty, price);
        self.push(&order.account, AccountEvent::Order(order.state()));
        self.push(
            &order.account,
            AccountEvent::Account(AccountDirty {
                instruments: vec![order.instrument.clone()],
            }),
        );
    }

    /// reduce_only 可平量：与成交方向相反的现有持仓绝对值。
    fn closable(&self, account: &str, instrument: &str, side: Side) -> Decimal {
        let qty = self
            .positions
            .get(&(account.to_string(), instrument.to_string()))
            .map(|p| p.quantity)
            .unwrap_or(Decimal::ZERO);
        match side {
            Side::Buy if qty.is_sign_negative() => qty.abs(),
            Side::Sell if qty.is_sign_positive() => qty,
            _ => Decimal::ZERO,
        }
    }

    /// 带符号持仓演进：同向加权、减仓成本不变、穿零反向按成交价重置。
    fn apply_position(
        &mut self,
        account: &str,
        instrument: &str,
        side: Side,
        qty: Decimal,
        price: Decimal,
    ) {
        let delta = match side {
            Side::Buy => qty,
            Side::Sell => -qty,
        };
        let pos = self
            .positions
            .entry((account.to_string(), instrument.to_string()))
            .or_default();
        if pos.quantity.is_zero() {
            pos.quantity = delta;
            pos.entry_price = price;
            return;
        }
        let old_abs = pos.quantity.abs();
        let trade_abs = delta.abs();
        if pos.quantity.is_sign_positive() == delta.is_sign_positive() {
            // 同向加仓：按数量加权平均成本。
            pos.entry_price =
                (pos.entry_price * old_abs + price * trade_abs) / (old_abs + trade_abs);
            pos.quantity += delta;
            return;
        }
        // 反向：先按原成本结算被平掉的部分。
        let closed = old_abs.min(trade_abs);
        let realized = if pos.quantity.is_sign_positive() {
            (price - pos.entry_price) * closed
        } else {
            (pos.entry_price - price) * closed
        };
        *self.realized.entry(account.to_string()).or_default() += realized;
        let new_qty = pos.quantity + delta;
        if new_qty.is_zero() {
            pos.quantity = Decimal::ZERO;
            pos.entry_price = Decimal::ZERO;
        } else if new_qty.is_sign_positive() == pos.quantity.is_sign_positive() {
            // 部分减仓：成本价不变。
            pos.quantity = new_qty;
        } else {
            // 穿零反向：剩余仓按本次成交价建仓。
            pos.quantity = new_qty;
            pos.entry_price = price;
        }
    }
}

/// 模拟交易所台账。
pub struct SimLedger {
    cfg: SimConfig,
    inner: Mutex<Inner>,
    /// 每账户一个唤醒器：多账户流共享同一 Notify 会互相错唤醒/漏唤醒。
    notifiers: Mutex<BTreeMap<String, Arc<Notify>>>,
}

impl SimLedger {
    pub fn new(cfg: SimConfig) -> Self {
        Self {
            inner: Mutex::new(Inner::new(&cfg)),
            cfg,
            notifiers: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn config(&self) -> &SimConfig {
        &self.cfg
    }

    /// 某账户的事件流唤醒器（首次取用即创建）。
    pub(crate) fn notifier_for(&self, account: &str) -> Arc<Notify> {
        self.notifiers
            .lock()
            .unwrap()
            .entry(account.to_string())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    /// 显式写 mark（不触碰宿主行情表；SimVenue::set_mark 负责宿主侧）。
    pub fn set_mark(&self, instrument: &str, price: Decimal) {
        let mut affected = BTreeSet::new();
        let mut inner = self.inner.lock().unwrap();
        inner.marks.insert(instrument.to_string(), price);
        if inner.limit_fill == LimitFillMode::FillWhenCrosses {
            let candidates: Vec<(String, String, Decimal)> = inner
                .orders
                .values()
                .filter(|o| !o.status.is_terminal() && o.instrument == instrument)
                .filter(|o| match (o.side, o.price) {
                    (Side::Buy, Some(p)) => price <= p,
                    (Side::Sell, Some(p)) => price >= p,
                    _ => false,
                })
                .map(|o| {
                    (
                        o.account.clone(),
                        o.client_order_id.clone(),
                        o.price.unwrap_or(price),
                    )
                })
                .collect();
            for (account, cid, px) in candidates {
                let remaining = inner
                    .orders
                    .get(&cid)
                    .map(SimOrder::remaining)
                    .unwrap_or(Decimal::ZERO);
                if remaining > Decimal::ZERO {
                    affected.insert(account);
                    inner.apply_fill(&cid, remaining, px);
                }
            }
        }
        drop(inner);
        for account in affected {
            self.notifier_for(&account).notify_one();
        }
    }

    pub fn mark(&self, instrument: &str) -> Option<Decimal> {
        self.inner.lock().unwrap().marks.get(instrument).copied()
    }

    /// 模拟所受理下单。市价单要求已有 mark，并即时成交；限价单保持 NEW。
    pub fn place(&self, account: &str, req: &PlaceOrderRequest) -> anyhow::Result<OrderAck> {
        if req.quantity <= Decimal::ZERO {
            return Ok(reject_ack(req, "数量必须为正"));
        }
        let mut inner = self.inner.lock().unwrap();
        if let Some(reason) = inner.reject_next.take() {
            return Ok(reject_ack(req, &reason));
        }
        if let Some(reason) = inner.indeterminate_next.take() {
            return Err(anyhow::anyhow!(reason));
        }
        if inner.orders.contains_key(&req.client_order_id) {
            return Ok(reject_ack(req, "重复 client_order_id"));
        }
        let is_market = matches!(
            req.order_type,
            OrderType::Market | OrderType::StopMarket | OrderType::TakeMarket
        );
        let fill_price = if is_market {
            match inner.marks.get(&req.instrument).copied().or(req.price) {
                Some(p) if p > Decimal::ZERO => Some(p),
                _ => None,
            }
        } else {
            None
        };
        if is_market && fill_price.is_none() {
            return Ok(reject_ack(req, "无可用行情价"));
        }

        inner.next_seq += 1;
        let exchange_order_id = format!("SIM-{:08}", inner.next_seq);
        let order = SimOrder {
            client_order_id: req.client_order_id.clone(),
            exchange_order_id: exchange_order_id.clone(),
            account: account.to_string(),
            instrument: req.instrument.clone(),
            side: req.side,
            order_type: req.order_type,
            price: req.price,
            quantity: req.quantity,
            filled: Decimal::ZERO,
            avg_price: None,
            reduce_only: req.reduce_only,
            status: OrderStatus::New,
        };
        inner
            .orders
            .insert(req.client_order_id.clone(), order.clone());
        inner.push(account, AccountEvent::Order(order.state()));
        if let Some(price) = fill_price {
            inner.apply_fill(&req.client_order_id, req.quantity, price);
        }
        drop(inner);
        self.notifier_for(account).notify_one();
        Ok(OrderAck {
            accepted: true,
            client_order_id: req.client_order_id.clone(),
            exchange_order_id: Some(exchange_order_id),
            status: OrderStatus::New,
            message: None,
            placed_type: None,
            placed_price: None,
            placed_quantity: None,
        })
    }

    /// 显式成交整笔未成交量（限价单默认不自动成交时由测试驱动）。
    pub fn fill_order(&self, client_order_id: &str) -> bool {
        self.fill_quantity(client_order_id, None)
    }

    /// 显式成交指定数量（`None` = 全部剩余量）。
    pub fn fill_quantity(&self, client_order_id: &str, quantity: Option<Decimal>) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let Some(order) = inner.orders.get(client_order_id).cloned() else {
            return false;
        };
        if order.status.is_terminal() {
            return false;
        }
        let remaining = order.remaining();
        let qty = quantity.unwrap_or(remaining).min(remaining);
        if qty <= Decimal::ZERO {
            return false;
        }
        let price = match order
            .price
            .or_else(|| inner.marks.get(&order.instrument).copied())
        {
            Some(p) => p,
            None => return false,
        };
        let account = order.account.clone();
        inner.apply_fill(client_order_id, qty, price);
        drop(inner);
        self.notifier_for(&account).notify_one();
        true
    }

    pub fn cancel(&self, r: &OrderRef) -> anyhow::Result<OrderAck> {
        let mut inner = self.inner.lock().unwrap();
        let Some(mut order) = inner.find(r).cloned() else {
            return Ok(OrderAck {
                accepted: false,
                client_order_id: r.client_order_id.clone(),
                exchange_order_id: r.exchange_order_id.clone(),
                status: OrderStatus::Canceled,
                message: Some("订单不存在".to_string()),
                placed_type: None,
                placed_price: None,
                placed_quantity: None,
            });
        };
        if !order.status.is_terminal() {
            order.status = OrderStatus::Canceled;
            inner
                .orders
                .insert(order.client_order_id.clone(), order.clone());
            inner.push(&order.account, AccountEvent::Order(order.state()));
        }
        let account = order.account.clone();
        drop(inner);
        self.notifier_for(&account).notify_one();
        Ok(OrderAck {
            accepted: true,
            client_order_id: order.client_order_id,
            exchange_order_id: Some(order.exchange_order_id),
            status: order.status,
            message: None,
            placed_type: None,
            placed_price: None,
            placed_quantity: None,
        })
    }

    /// 查单三态：`QueryMode` 可强制 NotExists / Unavailable（异常矩阵用）。
    pub fn query(&self, r: &OrderRef) -> OrderLookup {
        let inner = self.inner.lock().unwrap();
        match inner.query_mode {
            QueryMode::NotExists => OrderLookup::NotExists,
            QueryMode::Unavailable => OrderLookup::Unavailable,
            QueryMode::Found => inner
                .find(r)
                .map(|o| OrderLookup::Found(o.state()))
                .unwrap_or(OrderLookup::NotExists),
        }
    }

    /// 某账户的持仓快照（仅返回数量非零者）。
    pub fn positions(&self, account: &str) -> Vec<PositionInfo> {
        let inner = self.inner.lock().unwrap();
        let now = now_ms();
        let leverage = inner.leverage.max(1);
        inner
            .positions
            .iter()
            .filter(|((acct, _), p)| acct == account && !p.quantity.is_zero())
            .map(|((_, instrument), p)| {
                let mark = inner
                    .marks
                    .get(instrument)
                    .copied()
                    .unwrap_or(p.entry_price);
                let qty_abs = p.quantity.abs();
                let unrealized = if p.quantity.is_sign_positive() {
                    (mark - p.entry_price) * p.quantity
                } else {
                    (p.entry_price - mark) * qty_abs
                };
                PositionInfo {
                    instrument: instrument.clone(),
                    instrument_type: self.cfg.instrument_type,
                    position_side: OrderPositionSide::Both,
                    signed_quantity: p.quantity,
                    entry_price: p.entry_price,
                    mark_price: Some(mark),
                    unrealized_pnl: Some(unrealized),
                    leverage: Some(u64::from(leverage)),
                    multiplier: None,
                    notional: Some(mark * qty_abs),
                    margin: Some(p.entry_price * qty_abs / Decimal::from(leverage)),
                    update_ms: Some(now),
                }
            })
            .collect()
    }

    /// 某账户某标的的完整持仓快照（逐字段差分用；无仓返回 None）。
    pub fn position_info(&self, account: &str, instrument: &str) -> Option<PositionInfo> {
        self.positions(account)
            .into_iter()
            .find(|p| p.instrument == instrument)
    }

    /// 某账户的余额快照（权益 = 初始资金 + 该账户已实现盈亏 + 浮盈亏）。
    pub fn balance(&self, account: &str) -> Vec<BalanceInfo> {
        let inner = self.inner.lock().unwrap();
        let leverage = inner.leverage.max(1);
        let mut unrealized = Decimal::ZERO;
        let mut margin_used = Decimal::ZERO;
        for ((acct, instrument), p) in &inner.positions {
            if acct != account || p.quantity.is_zero() {
                continue;
            }
            let mark = inner
                .marks
                .get(instrument)
                .copied()
                .unwrap_or(p.entry_price);
            let qty_abs = p.quantity.abs();
            unrealized += if p.quantity.is_sign_positive() {
                (mark - p.entry_price) * p.quantity
            } else {
                (p.entry_price - mark) * qty_abs
            };
            margin_used += p.entry_price * qty_abs / Decimal::from(leverage);
        }
        let realized = inner
            .realized
            .get(account)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let equity = self.cfg.initial_balance + realized + unrealized;
        vec![BalanceInfo {
            currency: self.cfg.settle_currency.clone(),
            equity,
            available: equity - margin_used,
            margin_used: Some(margin_used),
            unrealized_pnl: Some(unrealized),
            update_ms: Some(now_ms()),
        }]
    }

    pub fn reference_price(&self, instrument: &str) -> anyhow::Result<ReferencePrice> {
        let price = self
            .inner
            .lock()
            .unwrap()
            .marks
            .get(instrument)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("模拟所无 `{instrument}` 行情"))?;
        Ok(ReferencePrice {
            instrument: instrument.to_string(),
            basis: PriceBasis::Mark,
            price,
        })
    }

    pub fn set_leverage(&self, leverage: u32) {
        self.inner.lock().unwrap().leverage = leverage.max(1);
    }

    pub fn leverage(&self) -> u32 {
        self.inner.lock().unwrap().leverage
    }

    /// 下一次下单强制拒单（确定性拒绝，`accepted=false`）。
    pub fn reject_next(&self, reason: impl Into<String>) {
        self.inner.lock().unwrap().reject_next = Some(reason.into());
    }

    /// 下一次下单模拟「应答不确定」（超时/断网）：返回 `Err`，宿主应落 UNKNOWN。
    pub fn fail_next_place(&self, reason: impl Into<String>) {
        self.inner.lock().unwrap().indeterminate_next = Some(reason.into());
    }

    pub fn set_query_mode(&self, mode: QueryMode) {
        self.inner.lock().unwrap().query_mode = mode;
    }

    /// 从共享队列弹出属于该账户的下一条事件（无则 None）。
    pub fn pop_event(&self, account: &str) -> Option<AccountEvent> {
        let mut inner = self.inner.lock().unwrap();
        let idx = inner.events.iter().position(|(acct, _)| acct == account)?;
        inner.events.remove(idx).map(|(_, ev)| ev)
    }

    /// 全部订单快照（按 cid 排序；测试断言/差分用）。
    pub fn orders(&self) -> Vec<SimOrder> {
        self.inner
            .lock()
            .unwrap()
            .orders
            .values()
            .cloned()
            .collect()
    }

    /// 订单快照（测试断言用，含终态）。
    pub fn order(&self, client_order_id: &str) -> Option<SimOrder> {
        self.inner
            .lock()
            .unwrap()
            .orders
            .get(client_order_id)
            .cloned()
    }

    /// 单账户便捷读取：任取该标的的非零持仓（多账户请用 [`Self::positions`]）。
    pub fn position(&self, instrument: &str) -> Option<(Decimal, Decimal)> {
        let inner = self.inner.lock().unwrap();
        inner
            .positions
            .iter()
            .find(|((_, inst), p)| inst == instrument && !p.quantity.is_zero())
            .map(|(_, p)| (p.quantity, p.entry_price))
    }

    /// 台账事件排队数（泄漏检测/断言用）。
    pub fn queued_events(&self) -> usize {
        self.inner.lock().unwrap().events.len()
    }
}

fn reject_ack(req: &PlaceOrderRequest, message: &str) -> OrderAck {
    OrderAck {
        accepted: false,
        client_order_id: req.client_order_id.clone(),
        exchange_order_id: None,
        status: OrderStatus::Rejected,
        message: Some(message.to_string()),
        placed_type: None,
        placed_price: None,
        placed_quantity: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    use trading_models::InstrumentType;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn req(
        cid: &str,
        side: Side,
        order_type: OrderType,
        price: Option<&str>,
        quantity: &str,
    ) -> PlaceOrderRequest {
        PlaceOrderRequest {
            instrument: "BTCUSDT".to_string(),
            instrument_type: InstrumentType::Futures,
            side,
            order_type,
            price: price.map(dec),
            quantity: dec(quantity),
            reduce_only: false,
            position_side: OrderPositionSide::Both,
            time_in_force: None,
            client_order_id: cid.to_string(),
        }
    }

    fn ledger_at(price: &str) -> SimLedger {
        let ledger = SimLedger::new(SimConfig::default());
        ledger.set_mark("BTCUSDT", dec(price));
        ledger
    }

    /// 测试辅助：从事件中取订单状态（非订单事件返回 None）。
    fn order_status(ev: AccountEvent) -> Option<OrderStatus> {
        match ev {
            AccountEvent::Order(o) => Some(o.status),
            AccountEvent::Account(_) => None,
        }
    }

    #[test]
    fn market_order_fills_immediately_and_creates_long_position() {
        let ledger = ledger_at("100");
        let ack = ledger
            .place("acct", &req("c1", Side::Buy, OrderType::Market, None, "2"))
            .unwrap();

        assert!(ack.accepted);
        assert_eq!(ack.status, OrderStatus::New);
        let order = ledger.order("c1").unwrap();
        assert_eq!(order.status, OrderStatus::Filled);
        assert_eq!(order.filled, dec("2"));
        assert_eq!(ledger.position("BTCUSDT"), Some((dec("2"), dec("100"))));

        // NEW → FILLED → Account(dirty) 三类事件都已入队。
        assert!(ledger.queued_events() >= 3);
        assert_eq!(
            order_status(ledger.pop_event("acct").unwrap()),
            Some(OrderStatus::New)
        );
    }

    #[test]
    fn add_position_uses_weighted_average_cost() {
        let ledger = ledger_at("100");
        ledger
            .place("acct", &req("c1", Side::Buy, OrderType::Market, None, "2"))
            .unwrap();
        ledger.set_mark("BTCUSDT", dec("110"));
        ledger
            .place("acct", &req("c2", Side::Buy, OrderType::Market, None, "2"))
            .unwrap();

        // (100×2 + 110×2) / 4 = 105
        assert_eq!(ledger.position("BTCUSDT"), Some((dec("4"), dec("105"))));
    }

    #[test]
    fn reduce_position_keeps_cost_and_realizes_pnl() {
        let ledger = ledger_at("100");
        ledger
            .place("acct", &req("c1", Side::Buy, OrderType::Market, None, "2"))
            .unwrap();
        ledger.set_mark("BTCUSDT", dec("110"));
        ledger
            .place("acct", &req("c2", Side::Sell, OrderType::Market, None, "1"))
            .unwrap();

        // 减仓后成本价仍为 100；已实现 +10、剩余 1 手浮盈 +10。
        assert_eq!(ledger.position("BTCUSDT"), Some((dec("1"), dec("100"))));
        let bal = &ledger.balance("acct")[0];
        assert_eq!(bal.equity, dec("100020"));
        assert_eq!(bal.unrealized_pnl, Some(dec("10")));
    }

    #[test]
    fn cross_zero_flip_resets_entry_price() {
        let ledger = ledger_at("100");
        ledger
            .place("acct", &req("c1", Side::Buy, OrderType::Market, None, "1"))
            .unwrap();
        ledger.set_mark("BTCUSDT", dec("120"));
        ledger
            .place("acct", &req("c2", Side::Sell, OrderType::Market, None, "2"))
            .unwrap();

        // 平多 + 反手 1 张空，成本按本次成交价 120。
        assert_eq!(ledger.position("BTCUSDT"), Some((dec("-1"), dec("120"))));
    }

    #[test]
    fn limit_order_stays_new_until_explicit_fill() {
        let ledger = ledger_at("100");
        let ack = ledger
            .place(
                "acct",
                &req("c1", Side::Buy, OrderType::Limit, Some("90"), "1"),
            )
            .unwrap();

        assert!(ack.accepted);
        assert_eq!(ledger.order("c1").unwrap().status, OrderStatus::New);
        assert_eq!(ledger.position("BTCUSDT"), None);

        assert!(ledger.fill_order("c1"));
        assert_eq!(ledger.order("c1").unwrap().status, OrderStatus::Filled);
        assert_eq!(ledger.position("BTCUSDT"), Some((dec("1"), dec("90"))));
    }

    #[test]
    fn set_mark_crossing_fills_limit_when_mode_enabled() {
        let cfg = SimConfig {
            limit_fill: LimitFillMode::FillWhenCrosses,
            ..SimConfig::default()
        };
        let ledger = SimLedger::new(cfg);
        ledger.set_mark("BTCUSDT", dec("100"));
        ledger
            .place(
                "acct",
                &req("c1", Side::Buy, OrderType::Limit, Some("101"), "1"),
            )
            .unwrap();
        assert_eq!(ledger.order("c1").unwrap().status, OrderStatus::New);

        // 买单委托价 101 已被 mark=100 穿过 → 自动成交。
        ledger.set_mark("BTCUSDT", dec("100"));
        assert_eq!(ledger.order("c1").unwrap().status, OrderStatus::Filled);
    }

    #[test]
    fn reject_next_yields_unaccepted_ack() {
        let ledger = ledger_at("100");
        ledger.reject_next("余额不足");
        let ack = ledger
            .place("acct", &req("c1", Side::Buy, OrderType::Market, None, "1"))
            .unwrap();
        assert!(!ack.accepted);
        assert_eq!(ack.message.as_deref(), Some("余额不足"));
        assert!(ledger.position("BTCUSDT").is_none());
    }

    #[test]
    fn query_order_reports_three_states() {
        let ledger = ledger_at("100");
        ledger
            .place(
                "acct",
                &req("c1", Side::Buy, OrderType::Limit, Some("90"), "1"),
            )
            .unwrap();
        let r = OrderRef::new("c1");
        assert!(matches!(ledger.query(&r), OrderLookup::Found(_)));

        ledger.set_query_mode(QueryMode::NotExists);
        assert!(matches!(ledger.query(&r), OrderLookup::NotExists));

        ledger.set_query_mode(QueryMode::Unavailable);
        assert!(matches!(ledger.query(&r), OrderLookup::Unavailable));
    }

    #[test]
    fn fail_next_place_returns_err_and_does_not_create_order() {
        let ledger = ledger_at("100");
        ledger.fail_next_place("模拟超时");
        let err = ledger
            .place("acct", &req("c1", Side::Buy, OrderType::Market, None, "1"))
            .unwrap_err();
        assert!(err.to_string().contains("模拟超时"));
        assert!(ledger.order("c1").is_none());
        assert!(ledger.position("BTCUSDT").is_none());

        // 只影响一次：下一次下单恢复正常。
        let ack = ledger
            .place("acct", &req("c2", Side::Buy, OrderType::Market, None, "1"))
            .unwrap();
        assert!(ack.accepted);
    }

    #[test]
    fn positions_and_balance_are_account_scoped() {
        let ledger = ledger_at("100");
        ledger
            .place("alice", &req("a1", Side::Buy, OrderType::Market, None, "1"))
            .unwrap();
        ledger
            .place("bob", &req("b1", Side::Buy, OrderType::Market, None, "2"))
            .unwrap();

        assert_eq!(ledger.positions("alice")[0].signed_quantity, dec("1"));
        assert_eq!(ledger.positions("bob")[0].signed_quantity, dec("2"));
        // 保证金 = 成本 / 杠杆（10x）：alice 10、bob 20，互不串户。
        assert_eq!(ledger.balance("alice")[0].margin_used, Some(dec("10")));
        assert_eq!(ledger.balance("bob")[0].margin_used, Some(dec("20")));
        assert_eq!(ledger.balance("alice")[0].equity, dec("100000"));
    }

    #[test]
    fn reduce_only_clamps_to_position_and_never_flips() {
        let ledger = ledger_at("100");
        ledger
            .place(
                "acct",
                &req("open", Side::Buy, OrderType::Market, None, "1"),
            )
            .unwrap();

        let mut close = req("close", Side::Sell, OrderType::Market, None, "5");
        close.reduce_only = true;
        let ack = ledger.place("acct", &close).unwrap();
        assert!(ack.accepted);

        // 只能平掉现有 1 手；剩余委托按撤单收口，绝不反手做空。
        assert_eq!(ledger.position("BTCUSDT"), None);
        let order = ledger.order("close").unwrap();
        assert_eq!(order.filled, dec("1"));
        assert_eq!(order.status, OrderStatus::Canceled);
    }

    #[test]
    fn reduce_only_without_position_is_canceled_not_flipped() {
        let ledger = ledger_at("100");
        let mut close = req("close", Side::Sell, OrderType::Market, None, "1");
        close.reduce_only = true;
        ledger.place("acct", &close).unwrap();

        assert_eq!(ledger.position("BTCUSDT"), None);
        assert_eq!(ledger.order("close").unwrap().status, OrderStatus::Canceled);
    }

    #[test]
    fn duplicate_client_order_id_is_rejected() {
        let ledger = ledger_at("100");
        ledger
            .place(
                "acct",
                &req("c1", Side::Buy, OrderType::Limit, Some("90"), "1"),
            )
            .unwrap();
        let ack = ledger
            .place(
                "acct",
                &req("c1", Side::Buy, OrderType::Limit, Some("91"), "2"),
            )
            .unwrap();

        assert!(!ack.accepted);
        assert_eq!(ledger.order("c1").unwrap().quantity, dec("1"));
    }

    #[test]
    fn cancel_marks_order_canceled_and_is_idempotent() {
        let ledger = ledger_at("100");
        ledger
            .place(
                "acct",
                &req("c1", Side::Buy, OrderType::Limit, Some("90"), "1"),
            )
            .unwrap();
        let ack = ledger.cancel(&OrderRef::new("c1")).unwrap();
        assert!(ack.accepted);
        assert_eq!(ledger.order("c1").unwrap().status, OrderStatus::Canceled);
        // 再次撤单：已终态，幂等受理、状态不回退。
        let again = ledger.cancel(&OrderRef::new("c1")).unwrap();
        assert!(again.accepted);
        assert_eq!(ledger.order("c1").unwrap().status, OrderStatus::Canceled);
    }
}
