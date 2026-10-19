//! 盯盘规则编排引擎：一轮扫描的唯一收口点。
//!
//! 数据流（与 `docs/rule设计.md` 一致）：
//! ```text
//! Position 表（已被行情/对账流水线刷新）+ QuoteTable
//!        → MetricsBuilder/EvaluationContext（纯内核）
//!        → 仅评估 active 规则
//!        → chain：RuleRepo.chain_fire 原子双改（不下单）
//!        → self ：预检(账户/仓位/建单，纯内存) → claim_self 原子抢占
//!                  → 落内存订单 → 下单端口（网络在此之后）
//! ```
//!
//! 关键并发口径：**状态检查与转换都在 [`RuleRepo`] 的行锁内**；账户解析与
//! 订单构造（无副作用）提前到 claim 之前完成，claim 只在确有单可下时发生，
//! 因此缺账户/缺仓位不会把规则卡死在 in_use。网络只发生在 [`RuleOrderPlacer`]
//! 端口之后，绝不持锁等待交易所。

use std::sync::Arc;

use async_trait::async_trait;
use rust_decimal::Decimal;
use trading_core::id::new_client_order_id;
use trading_core::quote::QuoteTable;
use trading_exchange::api::{OrderLookup, PlaceOrderRequest};
use trading_models::{
    InstrumentType, MarketKind, Order, OrderStatus, OrderType, Position, Venue, WatchRule,
    WatchRuleAction, WatchRuleStatus, WatchRuleType,
};
use trading_store::{OrderEventInput, OrderRepo, PositionRepo, RuleRepo};

use super::metrics::{EvaluationContext, MetricsBuilder};
use super::order::{build_place_request, build_rule_order};

/// 规则下单所需的账户信息（不含密钥；下单交易所取订单自身，凭证在适配器内按名现取）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleAccount {
    pub name: String,
    pub is_testnet: bool,
    /// 账户持仓模式：下单构造 positionSide 用（仅 Binance 消费）。
    pub position_mode: trading_models::PositionMode,
}

/// 账户 id → 账户信息反查端口（规则行只存 account_id）。抽成小端口便于单测。
pub trait RuleAccountLookup: Send + Sync {
    fn account_by_id(&self, id: u64) -> Option<RuleAccount>;
}

/// 规则单的交易所提交端口：实现负责连适配器、REST 下单，并把受理/失败回写订单仓储。
#[async_trait]
pub trait RuleOrderPlacer: Send + Sync {
    async fn place(&self, order: &Order, request: PlaceOrderRequest, account_name: &str);
}

/// 规则单的 REST 查单端口：用户流 WS 不可用/漏推时，用交易所 REST 查询兜底，
/// 把已到终态的规则单收敛（订单推进终态、关联规则 retire）。
#[async_trait]
pub trait RuleOrderQuerier: Send + Sync {
    /// REST 查单，返回 [`OrderLookup`] 三态：
    /// Found（携带状态）/ NotExists（交易所明确应答不存在）/ Unavailable（查询失败）。
    async fn query(&self, order: &Order, account_name: &str) -> OrderLookup;
}

/// 一条已 claim、已落内存表、待提交的命中意图。
struct FireIntent {
    order: Order,
    request: PlaceOrderRequest,
    account_name: String,
}

/// 「订单查无」收敛 REJECTED 前的宽限期（毫秒）：
///
/// 必须同时满足订单为 UNKNOWN 且 `now - created_at ≥ 60s` 才据币安 `-2013
/// Order does not exist` 收敛。60s 足以排除交易所主从同步延迟（秒级）造成的
/// 短暂"查无"，避免把实际已到所的订单误判为 REJECTED。
const NOT_EXISTS_GRACE_MS: u64 = 60_000;

/// 是否可据「交易所明确查无」把订单收敛为 REJECTED（**纯决策、零 IO**）：
///
/// 必须同时满足：
/// * `status == Unknown`：仅"下单超时、应答不确定"的订单才可能被收敛；已在途
///   （NEW）或其它状态不据此处理；
/// * `age_ms ≥ NOT_EXISTS_GRACE_MS`：留出 60s 排除交易所主从同步延迟造成的
///   短暂"查无"，避免误判实际已到所的订单。
pub fn should_reject_on_not_exists(status: OrderStatus, age_ms: u64) -> bool {
    status == OrderStatus::Unknown && age_ms >= NOT_EXISTS_GRACE_MS
}

/// 一轮扫描结果计数（日志/测试用）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanReport {
    /// 链式激活成对落库的当前规则数。
    pub chained: usize,
    /// self 命中、已提交下单端口的数量。
    pub fired: usize,
    /// 命中但因缺账户/缺仓位/建单失败被跳过（未 claim、未下单）的数量。
    pub skipped: usize,
    /// 命中但交易所预检未过（如 deribit 平仓价差过宽）：未 claim、规则保持 active。
    pub deferred: usize,
}

/// 规则引擎（廉价克隆，内部全 Arc 句柄，不自建共享内存）。
#[derive(Clone)]
pub struct RuleEngine {
    rules: RuleRepo,
    orders: OrderRepo,
    positions: PositionRepo,
    quotes: Arc<QuoteTable>,
    accounts: Arc<dyn RuleAccountLookup>,
    placer: Arc<dyn RuleOrderPlacer>,
    /// 交易所注册表：claim 前经此做 venue 预检（市价转限价/价差闸门）。
    registry: Arc<trading_exchange::registry::ExchangeRegistry>,
    /// 可选 REST 查单兜底（用户流 WS 不可用/漏推时收敛终态）。
    order_querier: Option<Arc<dyn RuleOrderQuerier>>,
    /// 可选企业微信成交通知（纯通知；测试缺省即不发）。
    notify: Option<crate::notify::NotifyHub>,
}

impl RuleEngine {
    pub fn new(
        rules: RuleRepo,
        orders: OrderRepo,
        positions: PositionRepo,
        quotes: Arc<QuoteTable>,
        accounts: Arc<dyn RuleAccountLookup>,
        placer: Arc<dyn RuleOrderPlacer>,
        registry: Arc<trading_exchange::registry::ExchangeRegistry>,
    ) -> Self {
        Self {
            rules,
            orders,
            positions,
            quotes,
            accounts,
            placer,
            registry,
            order_querier: None,
            notify: None,
        }
    }

    /// 注入 REST 查单兜底口（生产装配用；测试缺省即不补偿）。
    pub fn with_order_querier(mut self, querier: Arc<dyn RuleOrderQuerier>) -> Self {
        self.order_querier = Some(querier);
        self
    }

    /// 注入企业微信成交通知（生产装配用；测试缺省即不通知）。
    pub fn with_notify_hub(mut self, notify: crate::notify::NotifyHub) -> Self {
        self.notify = Some(notify);
        self
    }

    /// 跨标的行情估值价：QuoteTable（1s mark WS）优先；WS 行情退化（无帧）时
    /// 回退到本账户同标的活跃仓位的 `current_price`——它由仓位 REST 对账/重估以
    /// 同一 fapi 标记价口径周期刷新，保证行情 WS 不可用时价格类条件仍可判定。
    fn market_price(
        &self,
        preferred: Option<(Venue, MarketKind)>,
        account_id: u64,
        symbol: &str,
    ) -> Option<Decimal> {
        // 本所优先、全局按 symbol 回退（不再硬编码 BinanceFutures）。
        if let Some(price) = self.quotes.price_for_symbol(preferred, symbol) {
            return Some(price);
        }
        // 行情退化时回退本账户同标的活跃仓位的 current_price。
        self.positions
            .list_active()
            .into_iter()
            .find(|p| {
                p.account_id == account_id
                    && p.instrument == symbol
                    && p.current_price > Decimal::ZERO
            })
            .map(|p| p.current_price)
    }

    /// 本账户某标的的跨标的持仓数量（单向持仓一行；防御性对多方向求和）。
    ///
    /// 与创建期存在性校验配套：**无活跃仓位返回 Some(0)** 而非 None——
    /// 这样 `position_FILUSDT == 0` 联动规则在条件标的被平掉后才能命中；
    /// 该规则创建时已要求条件标的存在活跃仓位，故不会对从未持仓的标的误触发。
    fn position_quantity(
        &self,
        account_id: u64,
        scope_exchange: &str,
        scope_itype: InstrumentType,
        symbol: &str,
    ) -> Option<Decimal> {
        let mut total = Decimal::ZERO;
        for p in self.positions.list_active() {
            // 按规则上下文的交易所/合约形状统计（期权仓位也是合法持仓），
            // 不能写死 Futures，否则期权持仓量被漏成 0 导致误触发。
            if p.account_id == account_id
                && p.exchange == scope_exchange
                && p.instrument_type == scope_itype
                && p.instrument == symbol
            {
                total += p.quantity;
            }
        }
        Some(total)
    }

    /// 按 position_id 取活跃持仓快照。
    fn find_position(&self, position_id: u64) -> Option<Position> {
        // 按主键直取（O(1)）；get 自动过滤已软删行，语义与“list_active 再找 id”一致。
        self.positions.get_active(position_id)
    }

    /// new 规则现金口径：未给 quantity、给了 order_cash 时，按命中瞬间的
    /// 参考价换算数量（复用 cashqty，口径同 place_order）。
    ///
    /// 返回填入数量后的规则副本；行情未就绪/换算非正返回 Err（调用方本轮
    /// 跳过、不 claim，下一轮扫描自动重试）。
    fn resolve_open_rule(&self, rule: &WatchRule) -> Result<WatchRule, String> {
        if rule.quantity > Decimal::ZERO || rule.order_cash <= Decimal::ZERO {
            return Ok(rule.clone());
        }
        // 限价用委托价；市价的参考价由 cashqty 按盘口/估值价取。
        let limit_price = (!matches!(rule.order_type, OrderType::Market)).then_some(rule.price);
        let ref_price = crate::cashqty::reference_price(
            self.quotes.as_ref(),
            &rule.exchange,
            rule.instrument_type,
            &rule.instrument,
            rule.side,
            rule.order_type,
            limit_price,
        )
        .ok_or_else(|| "order_cash 换算失败：缺少可用参考价（行情未就绪）".to_string())?;
        let quantity = crate::cashqty::cash_to_quantity(rule.order_cash, ref_price)
            .ok_or_else(|| "order_cash 换算出的数量非正".to_string())?;
        let mut resolved = rule.clone();
        resolved.quantity = quantity;
        Ok(resolved)
    }

    /// self 命中的预检 + 建单（纯内存、无副作用）：解析账户、构造订单。
    /// `position` 由调用方在评估阶段一次性取好（risk 必有，new 为 None）。
    /// 成功返回待落库订单与请求；失败返回原因（调用方跳过、不 claim）。
    fn prepare_self(
        &self,
        rule: &WatchRule,
        position: Option<&Position>,
    ) -> Result<(RuleAccount, Order, PlaceOrderRequest), String> {
        let account = self
            .accounts
            .account_by_id(rule.account_id)
            .ok_or_else(|| format!("账户 {} 不可解析", rule.account_id))?;
        // new + order_cash：命中时先把 cash 换算成数量，再走未改动的规则单构造。
        let resolved = self.resolve_open_rule(rule)?;
        let order = build_rule_order(
            &resolved,
            position,
            account.is_testnet,
            new_client_order_id(),
        )
        .map_err(|e| format!("规则单构造失败: {e:?}"))?;
        let request = build_place_request(&order, account.position_mode);
        Ok((account, order, request))
    }

    /// 处理预检未过：DataNotReady 静默下轮回；SpreadTooWide 记日志并触发
    /// 人工平仓告警（NotifyHub 内部按 symbol 60s 冷却）。
    fn handle_deferred(&self, rule: &WatchRule, deferred: &trading_exchange::api::PlaceDefer) {
        match deferred {
            trading_exchange::api::PlaceDefer::DataNotReady => {
                tracing::debug!(rule_id = rule.id, "预检：行情/规格未就绪，下轮重试");
            }
            trading_exchange::api::PlaceDefer::SpreadTooWide {
                instrument,
                bid,
                ask,
                spread,
                threshold,
            } => {
                tracing::warn!(
                    rule_id = rule.id,
                    instrument,
                    bid = %bid,
                    ask = %ask,
                    spread = %spread,
                    threshold = %threshold,
                    "平仓价差过宽，暂不下单（规则保持 active，持续判断）"
                );
                if let Some(notify) = &self.notify {
                    notify.on_spread_too_wide(instrument, *bid, *ask, *spread, rule.account_id);
                }
            }
        }
    }

    /// 评估所有 active 规则并推进状态（纯内存、同步、无网络）。
    fn collect_fires(&self) -> (Vec<FireIntent>, ScanReport) {
        let mut intents = Vec::new();
        let mut report = ScanReport::default();

        // 先回收 pending 的 risk 孤儿规则：主循环只扫 active（见下方），pending
        // 后继若绑定仓位已被平掉，将永远不会再被 arm。这里仅对 pending risk 做
        // 「仓位存在性」检查，命中即 retired，并立刻跳过——绝不进入条件评估，
        // 否则 pending 条件恰好为真时会在"武装前"被提前触发。
        for rule in self.rules.list_non_retired() {
            if rule.status != WatchRuleStatus::Pending {
                continue;
            }
            if rule.rule_type != WatchRuleType::Risk || rule.position_id == 0 {
                // new 的 pending 规则 position_id 可合法为 0，不能误杀。
                continue;
            }
            if self.find_position(rule.position_id).is_none() {
                tracing::info!(
                    rule_id = rule.id,
                    position_id = rule.position_id,
                    "pending risk 规则绑定仓位已不存在，规则置 retired"
                );
                if let Err(e) = self.rules.retire(rule.id) {
                    tracing::warn!(rule_id = rule.id, error = %e, "pending 规则绑定仓位消失后 retire 失败");
                }
            }
        }

        for rule in self.rules.list_active() {
            let Some(action) = rule.parsed_action() else {
                report.skipped += 1;
                continue;
            };

            // 指标上下文：
            // * risk 必须有活跃持仓；本轮活跃仓位中已无该 position_id（仓位已平/被
            //   对账软删）→ 规则直接 retired 终态，不再每轮空判；
            // * new 默认 position_id=0 用空上下文（条件须是跨标的 price_/position_）；
            //   链式 new 规则可携带「信号仓位」id（如被激活后按该仓位 roi 决定是否开仓），
            //   该仓位只提供指标上下文，new 命中后的下单要素仍全部取规则行。
            let position = match rule.rule_type {
                WatchRuleType::Risk => match self.find_position(rule.position_id) {
                    Some(p) => Some(p),
                    None => {
                        tracing::info!(
                            rule_id = rule.id,
                            position_id = rule.position_id,
                            "risk 规则绑定仓位已不存在，规则置 retired"
                        );
                        if let Err(e) = self.rules.retire(rule.id) {
                            tracing::warn!(rule_id = rule.id, error = %e, "绑定仓位消失后 retire 失败");
                        }
                        continue;
                    }
                },
                WatchRuleType::New => self.find_position(rule.position_id),
            };
            let account_id = rule.account_id;
            // 指标上下文与跨标的查询作用域一并按 position 取：
            // 有仓（risk 绑定/new 信号仓）→ 仓位自身；无仓（new 空上下文）→ 规则行。
            let (ctx, scope_exchange, scope_itype) = match position.as_ref() {
                Some(p) => (
                    EvaluationContext::from_position(p),
                    p.exchange.clone(),
                    p.instrument_type,
                ),
                None => (
                    EvaluationContext::empty(),
                    rule.exchange.clone(),
                    rule.instrument_type,
                ),
            };
            // 本所优先的 preferred 键（未接入所/形状为 None，直接走全局按名回退）。
            let preferred = crate::cashqty::market_key(&scope_exchange, scope_itype);
            let builder = MetricsBuilder::new(
                |symbol: &str| self.market_price(preferred, account_id, symbol),
                |symbol: &str| {
                    self.position_quantity(account_id, &scope_exchange, scope_itype, symbol)
                },
                // 用本 rule 的账户取目标标的活跃仓位 → 指标上下文（跨标的 ROI/盈利）。
                |symbol: &str| {
                    self.positions
                        .find_active_instrument(account_id, &scope_exchange, scope_itype, symbol)
                        .map(|p| EvaluationContext::from_position(&p))
                },
            );
            let matched = builder.evaluate(&rule, &ctx);
            // 逐条判定结果 debug（高频轮询：只记 rule_id 与命中与否，不记全量指标）。
            tracing::debug!(rule_id = rule.id, matched, "rule evaluated");
            if !matched {
                continue;
            }

            match action {
                WatchRuleAction::ActivateRule { rule_id: target } => {
                    match self.rules.chain_fire(rule.id, target) {
                        Ok(()) => report.chained += 1,
                        Err(e) => {
                            tracing::warn!(rule_id = rule.id, target, error = %e, "链式激活失败，规则保持原状");
                            report.skipped += 1;
                        }
                    }
                }
                WatchRuleAction::UseSelf => {
                    // 预检在前（无副作用）：不满足不下单也不推进状态。
                    let (account, mut order, request) =
                        match self.prepare_self(&rule, position.as_ref()) {
                            Ok(v) => v,
                            Err(reason) => {
                                tracing::warn!(rule_id = rule.id, reason, "规则命中但跳过下单");
                                report.skipped += 1;
                                continue;
                            }
                        };

                    // 交易所 claim 前预检（同步、纯内存）：市价转限价 / 平仓价差闸门。
                    // PlaceDefer（价差过宽等）→ **不 claim、不建单**，规则保持 active，
                    // 下轮重新预检（持续判断，价差收敛后才成交）。
                    // 预检按「有效交易所」路由：risk 的 rule.exchange 设计上留空、
                    // 交易所来自绑定仓位；new 来自规则行。scope_exchange 即此口径，
                    // 否则 risk 永远用空串匹配不到 deribit provider，漂移/价差门失效。
                    let request = match self.registry.prepare_placement(&scope_exchange, request) {
                        Ok(prepared) => prepared,
                        Err(deferred) => {
                            self.handle_deferred(&rule, &deferred);
                            report.deferred += 1;
                            continue;
                        }
                    };
                    // 预检可能改类型/价格/数量：同步到待落库订单。
                    order.order_type = request.order_type;
                    order.order_price = request.price.unwrap_or(Decimal::ZERO);
                    order.order_quantity = request.quantity;

                    // 原子抢占：并发/连续两轮只有一个赢家，赢了才落内存订单。
                    if !matches!(self.rules.claim_self(rule.id), Ok(true)) {
                        continue;
                    }
                    match self.orders.insert(order) {
                        Ok(stored) => {
                            tracing::info!(
                                client_order_id = %stored.client_order_id,
                                rule_id = rule.id,
                                venue = %stored.exchange,
                                instrument = %stored.instrument,
                                side = ?stored.side,
                                qty = %stored.order_quantity,
                                "rule triggered, order created (PLACING)"
                            );
                            intents.push(FireIntent {
                                order: stored,
                                request,
                                account_name: account.name,
                            });
                        }
                        Err(e) => {
                            // 极小窗口：cid 冲突等导致落单失败，规则留 in_use 等待核对。
                            tracing::error!(rule_id = rule.id, error = %e, "规则命中后内存建单失败");
                            report.skipped += 1;
                        }
                    }
                }
            }
        }
        (intents, report)
    }

    /// 提交意图给交易所；同步终态（拒单等）立即 retire 规则，非终态等用户流回报。
    async fn dispatch(&self, intents: Vec<FireIntent>, report: &mut ScanReport) {
        for intent in intents {
            self.placer
                .place(&intent.order, intent.request, &intent.account_name)
                .await;
            let rule_id = intent.order.rule_id;
            // 终态单已被仓储内部软删：普通 get_by_cid 会读 None，必须用
            // peek_by_cid（含 tombstone）才能正确探测终态、不遗漏 retire。
            let terminal = self
                .orders
                .peek_by_cid(&intent.order.client_order_id)
                .map(|o| o.status.is_terminal())
                .unwrap_or(false);
            if terminal {
                if let Err(e) = self.rules.retire(rule_id) {
                    tracing::warn!(rule_id, error = %e, "终态后 retire 规则失败");
                }
            }
            report.fired += 1;
        }
    }

    /// 订单终态已收敛后的统一收尾：规则单（rule_id != 0）retire 其关联规则，
    /// 策略单无规则可退。retire 失败仅 warn，不影响收敛（订单终态含软删已由
    /// 仓储收口）。`reason` 仅用于日志说明收敛途径（如"查无且过宽限"/"FILLED"）。
    fn retire_after_converged(&self, order: &Order, reason: &str) {
        if order.rule_id == 0 {
            tracing::info!(cid = %order.client_order_id, reason, "REST 兜底：策略单终态已收敛");
            return;
        }
        match self.rules.retire(order.rule_id) {
            Ok(()) => tracing::info!(
                cid = %order.client_order_id, rule_id = order.rule_id, reason,
                "REST 兜底：终态已收敛、规则 retired"
            ),
            Err(e) => tracing::warn!(
                rule_id = order.rule_id, cid = %order.client_order_id, error = %e, reason,
                "REST 兜底收敛后 retire 规则失败"
            ),
        }
    }

    /// REST 终态兜底：对所有「本地未终结」订单主动查单，把交易所侧已到终态
    /// （FILLED/CANCELED/REJECTED/EXPIRED）但用户流 WS 漏推/断连的订单收敛，
    /// 规则单并 retire 其关联规则。返回本轮收敛条数。无 querier 或无待收敛时为 0。
    pub async fn reconcile_terminal_orders(&self) -> usize {
        let Some(querier) = &self.order_querier else {
            return 0;
        };
        // 候选集：**所有本地未终结订单**——不区分规则单/策略单。
        // retire 这一副作用才需要 rule_id != 0，在下面按条件执行，不能据此
        // 把策略单（rule_id=0）整个排除，否则 UNKNOWN 策略单永远得不到兜底。
        let pending: Vec<Order> = self
            .orders
            .list_active()
            .into_iter()
            .filter(|o| !o.status.is_terminal())
            .collect();
        let mut converged = 0;
        for order in pending {
            let Some(account) = self.accounts.account_by_id(order.account_id) else {
                continue;
            };
            match querier.query(&order, &account.name).await {
                // 查询本身失败：不代表订单不存在，本轮跳过、下轮再试。
                OrderLookup::Unavailable => continue,
                // 交易所明确应答订单不存在：仅当订单为 UNKNOWN（下单超时、应答
                // 不确定）且已过宽限期，才收敛为 REJECTED——此时可确定交易所未
                // 受理。其余状态（如 NEW）不据此收敛，避免误判主从延迟。
                OrderLookup::NotExists => {
                    let now = store_core::SystemClock::now_ms();
                    let age = now.saturating_sub(order.created_at);
                    if !should_reject_on_not_exists(order.status, age) {
                        continue;
                    }
                    if let Err(e) = self.orders.apply_rejection(&order.client_order_id) {
                        tracing::warn!(cid = %order.client_order_id, error = %e, "REST 兜底：查无收敛 REJECTED 失败");
                        continue;
                    }
                    self.retire_after_converged(
                        &order,
                        &format!("查无且过宽限({age}ms)，收敛 REJECTED"),
                    );
                    converged += 1;
                }
                // 查到订单：仅在为明确终态时收敛。
                OrderLookup::Found(state) => {
                    if !state.status.is_terminal() {
                        continue;
                    }
                    let input = OrderEventInput {
                        client_order_id: order.client_order_id.clone(),
                        status: Some(state.status),
                        exchange_order_id: state
                            .exchange_order_id
                            .or(order.exchange_order_id.clone()),
                        filled_price: state.average_price,
                        filled_quantity: (state.filled_quantity > Decimal::ZERO)
                            .then_some(state.filled_quantity),
                        exchange_update_ms: None,
                    };
                    match self.orders.apply_event(&input) {
                        Ok(updated) => {
                            // 成交通知（纯通知、fire-and-forget），与 WS 回报同一收口。
                            if let Some(notify) = &self.notify {
                                notify.on_order_filled(&updated);
                            }
                            self.retire_after_converged(
                                &order,
                                &format!("查到终态 {:?}", state.status),
                            );
                            converged += 1;
                        }
                        Err(e) => {
                            tracing::warn!(cid = %order.client_order_id, error = %e, "REST 兜底应用订单终态失败")
                        }
                    }
                }
            }
        }
        converged
    }

    /// 执行一轮完整扫描（定时轮调用）。
    pub async fn run_scan(&self) -> ScanReport {
        let (intents, mut report) = self.collect_fires();
        self.dispatch(intents, &mut report).await;
        // 仅在确有状态推进时打汇总；跳过项（缺仓位/账户）逐条 warn，
        // 避免高频轮询对长期不满足前置的规则反复刷 info。
        if report.chained > 0 || report.fired > 0 {
            tracing::info!(?report, "规则扫描完成");
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use std::sync::Mutex;
    use trading_exchange::api::OrderState;
    use trading_models::{
        Instrument, OrderStatus, OrderType, Side, WatchConditionKind, WatchRule, WatchRuleAction,
        WatchRuleStatus,
    };

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    /// 固定账户反查：7=alice/binance/正式网。
    struct FixedAccounts;
    impl RuleAccountLookup for FixedAccounts {
        fn account_by_id(&self, id: u64) -> Option<RuleAccount> {
            (id == 7).then(|| RuleAccount {
                name: "alice".to_string(),
                is_testnet: false,
                position_mode: trading_models::PositionMode::OneWay,
            })
        }
    }

    /// 记录提交请求的假下单口；reject=true 时把订单推进到 REJECTED 终态。
    struct FakePlacer {
        // cid, reduce_only, account, leverage
        sent: Mutex<Vec<(String, bool, String, u64)>>,
        orders: OrderRepo,
        reject: bool,
    }
    #[async_trait]
    impl RuleOrderPlacer for FakePlacer {
        async fn place(&self, order: &Order, _request: PlaceOrderRequest, account_name: &str) {
            self.sent.lock().unwrap().push((
                order.client_order_id.clone(),
                order.reduce_only,
                account_name.to_string(),
                order.leverage,
            ));
            if self.reject {
                self.orders.apply_rejection(&order.client_order_id).unwrap();
            }
        }
    }

    struct Harness {
        engine: RuleEngine,
        rules: RuleRepo,
        orders: OrderRepo,
        positions: PositionRepo,
        quotes: Arc<QuoteTable>,
        placer: Arc<FakePlacer>,
    }

    fn standalone() -> std::sync::Arc<store_core::Store> {
        let store = store_core::Store::new(std::sync::Arc::new(store_core::SystemClock));
        store.register::<trading_models::Order>().unwrap();
        store.register::<trading_models::Position>().unwrap();
        store.register::<WatchRule>().unwrap();
        store
    }

    /// 测试用空注册表：无 provider → prepare_placement 对任意交易所透传（预检不变）。
    fn empty_registry() -> Arc<trading_exchange::registry::ExchangeRegistry> {
        Arc::new(trading_exchange::registry::ExchangeRegistry::from_providers(vec![]))
    }

    impl Harness {
        fn new(reject: bool) -> Self {
            let store = standalone();
            let orders = OrderRepo::new(store.table::<trading_models::Order>().unwrap());
            let positions = PositionRepo::new(store.table::<trading_models::Position>().unwrap());
            let rules = RuleRepo::new(store.table::<WatchRule>().unwrap());
            let quotes = Arc::new(QuoteTable::new());
            let placer = Arc::new(FakePlacer {
                sent: Mutex::new(Vec::new()),
                orders: orders.clone(),
                reject,
            });
            let engine = RuleEngine::new(
                rules.clone(),
                orders.clone(),
                positions.clone(),
                Arc::clone(&quotes),
                Arc::new(FixedAccounts),
                placer.clone(),
                empty_registry(),
            );
            Self {
                engine,
                rules,
                orders,
                positions,
                quotes,
                placer,
            }
        }

        fn sent(&self) -> Vec<(String, bool, String, u64)> {
            self.placer.sent.lock().unwrap().clone()
        }
    }

    /// 插入 roi=0.2 的 BTCUSDT 多仓，返回带 id 的行。
    fn seed_long_position(h: &Harness) -> Position {
        let mut p = Position::new(7, "binance", "BTCUSDT", Side::Buy);
        p.quantity = dec("2");
        p.current_price = dec("120");
        p.pos_price = dec("100");
        p.roi = dec("0.2");
        h.positions
            .apply_diffs(trading_models::diff_positions(vec![p], vec![], 1))
            .unwrap();
        h.positions.list_active()[0].clone()
    }

    /// 挂在指定仓位上的 roi 风控规则。
    fn roi_rule(h: &Harness, position_id: u64, op: &str, value: &str) -> WatchRule {
        h.rules
            .insert(WatchRule::new_risk(
                7,
                position_id,
                WatchConditionKind::Roi,
                None,
                op,
                dec(value),
                WatchRuleAction::UseSelf,
                OrderType::Market,
                Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
                Decimal::ZERO,
                Decimal::ONE,
            ))
            .unwrap()
    }

    #[tokio::test]
    async fn risk_hit_claims_creates_order_and_places_once() {
        let h = Harness::new(false);
        let pos = seed_long_position(&h);
        let rule = roi_rule(&h, pos.id, ">=", "0.12");

        let report = h.engine.run_scan().await;
        assert_eq!(
            report,
            ScanReport {
                chained: 0,
                fired: 1,
                skipped: 0,
                deferred: 0
            }
        );
        assert_eq!(h.rules.get(rule.id).unwrap().status, WatchRuleStatus::InUse);

        // 平多卖单：reduce_only、rule_id 关联、全平数量 2、账户名 alice。
        assert_eq!(h.sent().len(), 1);
        let (cid, reduce_only, account, leverage) = h.sent()[0].clone();
        assert!(reduce_only);
        assert_eq!(account, "alice");
        assert_eq!(leverage, 1); // 风控平仓单随持仓杠杆（seed_long_position 为 1x）
        let order = h.orders.get_by_cid(&cid).unwrap();
        assert_eq!(order.rule_id, rule.id);
        assert_eq!(order.side, Side::Sell);
        assert_eq!(order.order_quantity, dec("2"));

        // 第二轮：规则已 in_use，不得重复下单。
        assert_eq!(h.engine.run_scan().await.fired, 0);
        assert_eq!(h.sent().len(), 1);
    }

    #[tokio::test]
    async fn condition_not_met_does_nothing() {
        let h = Harness::new(false);
        let pos = seed_long_position(&h); // roi=0.2
        let rule = roi_rule(&h, pos.id, ">=", "0.5");

        let report = h.engine.run_scan().await;
        assert_eq!(report.fired, 0);
        assert!(h.sent().is_empty());
        assert_eq!(
            h.rules.get(rule.id).unwrap().status,
            WatchRuleStatus::Active
        );
    }

    #[tokio::test]
    async fn chain_action_retires_current_and_activates_target() {
        let h = Harness::new(false);
        let pos = seed_long_position(&h);
        // 先建 pending 目标。
        let target = h
            .rules
            .insert({
                let mut t = WatchRule::new_risk(
                    7,
                    pos.id,
                    WatchConditionKind::Price,
                    None,
                    "<=",
                    dec("90000"),
                    WatchRuleAction::UseSelf,
                    OrderType::Market,
                    Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
                    Decimal::ZERO,
                    Decimal::ONE,
                );
                t.status = WatchRuleStatus::Pending;
                t
            })
            .unwrap();
        // 当前规则命中后激活 target。
        let cur = h
            .rules
            .insert(WatchRule::new_risk(
                7,
                pos.id,
                WatchConditionKind::Roi,
                None,
                ">=",
                dec("0.12"),
                WatchRuleAction::ActivateRule { rule_id: target.id },
                OrderType::Market,
                Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
                Decimal::ZERO,
                Decimal::ONE,
            ))
            .unwrap();

        let report = h.engine.run_scan().await;
        assert_eq!(report.chained, 1);
        assert_eq!(report.fired, 0);
        assert!(h.sent().is_empty());
        assert!(h.rules.get(cur.id).is_none()); // 当前规则终结软删
        assert_eq!(
            h.rules.get(target.id).unwrap().status,
            WatchRuleStatus::Active
        );
    }

    #[tokio::test]
    async fn new_rule_with_cross_market_price_opens_position() {
        let h = Harness::new(false);
        let rule = h
            .rules
            .insert({
                let mut r = WatchRule::new_open(
                    7,
                    0,
                    "binance",
                    "BTCUSDT",
                    InstrumentType::Futures,
                    Side::Buy,
                    OrderType::Limit,
                    dec("50000"),
                    dec("0.1"),
                    1,
                );
                r.condition_kind = WatchConditionKind::Price;
                r.condition_instrument = Some("BTCUSDT".to_string());
                r.operator = ">=".to_string();
                r.value = dec("50000");
                r
            })
            .unwrap();

        // 尚无行情：跨标的条件缺价，本轮不触发。
        assert_eq!(h.engine.run_scan().await.fired, 0);
        assert_eq!(
            h.rules.get(rule.id).unwrap().status,
            WatchRuleStatus::Active
        );

        // 行情到达后命中开仓。
        h.quotes.apply(
            &Instrument::new(Venue::BinanceFutures, MarketKind::Futures, "BTCUSDT"),
            trading_core::quote::QuoteUpdate {
                mark: Some(dec("50100")),
                ts_recv_ms: 1,
                ..Default::default()
            },
        );
        assert_eq!(h.engine.run_scan().await.fired, 1);
        let (cid, reduce_only, _, leverage) = h.sent()[0].clone();
        assert!(!reduce_only);
        assert_eq!(leverage, 1); // 该 new 规则未设杠杆
        let order = h.orders.get_by_cid(&cid).unwrap();
        assert_eq!(order.side, Side::Buy);
        assert_eq!(order.order_quantity, dec("0.1"));
        assert_eq!(order.rule_id, rule.id);
    }

    #[tokio::test]
    async fn new_rule_with_order_cash_converts_quantity_on_trigger() {
        let h = Harness::new(false);
        let rule = h
            .rules
            .insert({
                // new 规则：quantity=0、order_cash=100、市价买。
                let mut r = WatchRule::new_open(
                    7,
                    0,
                    "binance",
                    "BTCUSDT",
                    InstrumentType::Futures,
                    Side::Buy,
                    OrderType::Market,
                    Decimal::ZERO,
                    Decimal::ZERO,
                    1,
                );
                r.order_cash = dec("100");
                // 条件：BTCUSDT 价格 >= 0（行情到达即命中）。
                r.condition_kind = WatchConditionKind::Price;
                r.condition_instrument = Some("BTCUSDT".to_string());
                r.operator = ">=".to_string();
                r.value = Decimal::ZERO;
                r
            })
            .unwrap();

        // 行情未到达：cash 无参考价，不触发、规则保持 active（不 claim）。
        assert_eq!(h.engine.run_scan().await.fired, 0);
        assert_eq!(
            h.rules.get(rule.id).unwrap().status,
            WatchRuleStatus::Active
        );

        // 行情到达：100 ÷ mark 50000 → quantity=0.002。
        h.quotes.apply(
            &Instrument::new(Venue::BinanceFutures, MarketKind::Futures, "BTCUSDT"),
            trading_core::quote::QuoteUpdate {
                mark: Some(dec("50000")),
                ts_recv_ms: 1,
                ..Default::default()
            },
        );
        assert_eq!(h.engine.run_scan().await.fired, 1);
        let cid = h.sent()[0].0.clone();
        let order = h.orders.get_by_cid(&cid).unwrap();
        assert_eq!(order.order_quantity, dec("0.002")); // cash 换算
        assert!(!order.reduce_only);
        assert_eq!(order.rule_id, rule.id);
    }

    #[tokio::test]
    async fn risk_rule_whose_position_gone_is_retired() {
        let h = Harness::new(false);
        let rule = roi_rule(&h, 100, ">=", "0"); // 仓位 100 在本轮活跃仓位中不存在

        let report = h.engine.run_scan().await;
        assert_eq!(report.fired, 0);
        assert!(h.sent().is_empty());
        assert_eq!(h.orders.list_active().len(), 0);
        // 绑定仓位消失：规则直接 retired（+deleted=1），不再每轮空判。
        assert!(h.rules.list_active().is_empty());
        let tomb = h.rules.get_including_deleted(rule.id).unwrap();
        assert_eq!(tomb.status, WatchRuleStatus::Retired);
        assert!(tomb.deleted);
    }

    #[tokio::test]
    async fn pending_risk_rule_whose_position_gone_is_retired() {
        let h = Harness::new(false);
        // pending risk 后继，绑定仓位 100（本轮活跃仓位不存在）。
        let mut pending = WatchRule::new_risk(
            7,
            100,
            WatchConditionKind::ProfitDrawdown,
            None,
            ">=",
            dec("0.05"),
            WatchRuleAction::UseSelf,
            OrderType::Market,
            Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
            Decimal::ZERO,
            Decimal::ONE,
        );
        pending.status = WatchRuleStatus::Pending;
        let rule = h.rules.insert(pending).unwrap();

        let report = h.engine.run_scan().await;
        assert_eq!(report.fired, 0);
        // 仓位已平，pending 后继不会再被 arm：连带 retired（+deleted=1）。
        let tomb = h.rules.get_including_deleted(rule.id).unwrap();
        assert_eq!(tomb.status, WatchRuleStatus::Retired);
        assert!(tomb.deleted);
    }

    #[tokio::test]
    async fn pending_risk_rule_with_live_position_is_kept() {
        let h = Harness::new(false);
        let pos = seed_long_position(&h);
        // 绑定仓位仍在 → pending 规则保留，不被误回收、也不提前触发。
        let mut pending = WatchRule::new_risk(
            7,
            pos.id,
            WatchConditionKind::ProfitDrawdown,
            None,
            ">=",
            dec("0.05"),
            WatchRuleAction::UseSelf,
            OrderType::Market,
            Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
            Decimal::ZERO,
            Decimal::ONE,
        );
        pending.status = WatchRuleStatus::Pending;
        let rule = h.rules.insert(pending).unwrap();

        let report = h.engine.run_scan().await;
        assert_eq!(report.fired, 0);
        let kept = h.rules.get_including_deleted(rule.id).unwrap();
        assert_eq!(kept.status, WatchRuleStatus::Pending);
        assert!(!kept.deleted);
    }

    #[tokio::test]
    async fn unknown_account_is_skipped_without_state_change() {
        let h = Harness::new(false);
        let pos = seed_long_position(&h);
        let rule = h
            .rules
            .insert(WatchRule::new_risk(
                999, // FixedAccounts 只认识 7
                pos.id,
                WatchConditionKind::Roi,
                None,
                ">=",
                dec("0.1"),
                WatchRuleAction::UseSelf,
                OrderType::Market,
                Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
                Decimal::ZERO,
                Decimal::ONE,
            ))
            .unwrap();

        let report = h.engine.run_scan().await;
        assert_eq!(report.skipped, 1);
        assert!(h.sent().is_empty());
        // 预检缺账户：claim 未发生，规则保持 active，不会卡死。
        assert_eq!(
            h.rules.get(rule.id).unwrap().status,
            WatchRuleStatus::Active
        );
    }

    /// 价格条件 risk 规则（绑定仓位，动作 self）。
    fn price_rule(
        h: &Harness,
        position_id: u64,
        op: &str,
        value: &str,
        quantity: &str,
        pct: &str,
    ) -> WatchRule {
        h.rules
            .insert(WatchRule::new_risk(
                7,
                position_id,
                WatchConditionKind::Price,
                None,
                op,
                dec(value),
                WatchRuleAction::UseSelf,
                OrderType::Market,
                Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
                if quantity.is_empty() {
                    Decimal::ZERO
                } else {
                    dec(quantity)
                },
                if pct.is_empty() {
                    Decimal::ZERO
                } else {
                    dec(pct)
                },
            ))
            .unwrap()
    }

    /// 插入指定杠杆的 BTC 多仓，并用真实重估（revalued）设置入场价/标记价，
    /// 使 Position.roi 为保证金口径（价格涨幅×杠杆），回读落库行。
    fn seed_lev_position(
        h: &Harness,
        instrument: &str,
        qty: &str,
        entry: &str,
        mark: &str,
        leverage: u64,
    ) -> Position {
        let mut p = Position::new(7, "binance", instrument, Side::Buy);
        p.quantity = dec(qty);
        p.pos_price = dec(entry);
        p.leverage = leverage;
        p = p.revalued(dec(mark));
        h.positions
            .apply_diffs(trading_models::diff_positions(vec![p], vec![], 1))
            .unwrap();
        h.positions
            .list_active()
            .into_iter()
            .find(|p| p.instrument == instrument)
            .unwrap()
    }

    /// 对已落库仓位重新以 mark 重估并回写（模拟行情推进；id 保留）。
    fn revalue(h: &Harness, mark: &str, now: u64) {
        let existing = h.positions.list_active();
        let updated = existing
            .into_iter()
            .map(|p| p.revalued(dec(mark)))
            .collect::<Vec<_>>();
        let before = h.positions.list_active();
        h.positions
            .apply_diffs(trading_models::diff_positions(updated, before, now))
            .unwrap();
    }

    /// 软平掉指定仓位（对账口径：远端无该行 → Closed）。
    fn close_positions(h: &Harness, targets: Vec<Position>, now: u64) {
        let before = h.positions.list_active();
        h.positions
            .apply_diffs(trading_models::diff_positions(vec![], targets, now))
            .unwrap();
        let _ = before;
    }

    // —— 用例②：btc 价格 > 70000 全部平仓 ——
    #[tokio::test]
    async fn case2_price_above_threshold_closes_full() {
        let h = Harness::new(false);
        // 3x、0.01 BTC 多仓，标记 70100（高于阈值 70000）。
        let pos = seed_lev_position(&h, "BTCUSDT", "0.01", "60000", "70100", 3);
        let _rule = price_rule(&h, pos.id, ">", "70000", "", "1");

        let report = h.engine.run_scan().await;
        assert_eq!(report.fired, 1);
        let (cid, reduce_only, _, leverage) = h.sent()[0].clone();
        assert!(reduce_only);
        let order = h.orders.get_by_cid(&cid).unwrap();
        assert_eq!(order.side, Side::Sell);
        assert_eq!(order.order_quantity, dec("0.01")); // 全平数量=持仓量
        assert_eq!(order.leverage, leverage);
        assert_eq!(leverage, 3); // 平仓单随持仓 3x
    }

    // —— 用例②：btc 价格 > 70000 平仓 50%（核对数量）——
    #[tokio::test]
    async fn case2_price_above_threshold_closes_half() {
        let h = Harness::new(false);
        let pos = seed_lev_position(&h, "BTCUSDT", "0.01", "60000", "70100", 3);
        let _rule = price_rule(&h, pos.id, ">", "70000", "", "0.5");

        let report = h.engine.run_scan().await;
        assert_eq!(report.fired, 1);
        let (cid, ..) = h.sent()[0].clone();
        // 半仓：0.01 × 0.5 = 0.005。
        assert_eq!(
            h.orders.get_by_cid(&cid).unwrap().order_quantity,
            dec("0.005")
        );
    }

    // —— 用例③：roi > 0.001 按「价格涨跌幅」口径（Position.roi 已乘杠杆，判定再除杠杆）——
    #[tokio::test]
    async fn case3_roi_threshold_uses_price_movement_not_margin_roi() {
        // 场景 A（关键反例）：价格涨 0.05%（60000→60030），价格 roi=0.0005（≤0.001），
        // 但 3x 保证金 roi=0.0015（>0.001）——若误用保证金口径会误触发，正确口径不触发。
        let h = Harness::new(false);
        let pos = seed_lev_position(&h, "BTCUSDT", "0.01", "60000", "60030", 3);
        let rule = roi_rule(&h, pos.id, ">", "0.001");
        assert_eq!(h.engine.run_scan().await.fired, 0);
        assert_eq!(
            h.rules.get(rule.id).unwrap().status,
            WatchRuleStatus::Active
        );

        // 场景 B：价格涨 0.1667%（>0.001）→ 触发全平。
        let h = Harness::new(false);
        let pos = seed_lev_position(&h, "BTCUSDT", "0.01", "60000", "60100", 3);
        let _rule = roi_rule(&h, pos.id, ">", "0.001");
        assert_eq!(h.engine.run_scan().await.fired, 1);
    }

    // —— 用例④：roi < -0.003 全部平仓（亏损方向，同样是价格口径）——
    #[tokio::test]
    async fn case4_negative_roi_uses_price_movement() {
        // 关键反例：价格跌 0.15%（60000→59910），价格 roi=-0.0015（>-0.003），
        // 但 3x 保证金 roi=-0.0045（<-0.003）——保证金口径会误平，价格口径不平。
        let h = Harness::new(false);
        let pos = seed_lev_position(&h, "BTCUSDT", "0.01", "60000", "59910", 3);
        let rule = roi_rule(&h, pos.id, "<", "-0.003");
        assert_eq!(h.engine.run_scan().await.fired, 0);
        assert_eq!(
            h.rules.get(rule.id).unwrap().status,
            WatchRuleStatus::Active
        );

        // 价格跌 0.4%（<-0.003）→ 触发。
        let h = Harness::new(false);
        let pos = seed_lev_position(&h, "BTCUSDT", "0.01", "60000", "59760", 3);
        let _rule = roi_rule(&h, pos.id, "<", "-0.003");
        assert_eq!(h.engine.run_scan().await.fired, 1);
    }

    // —— 用例⑤：roi > 0.01 武装 profit_drawdown > 0.05；回落未达不平、到达全平 ——
    #[tokio::test]
    async fn case5_drawdown_arms_then_fires_only_after_threshold() {
        let h = Harness::new(false);
        // 1x 多仓，先把价格推到涨幅 10%（60000→66000，保证金 roi=0.10）。
        let pos = seed_lev_position(&h, "BTCUSDT", "1", "60000", "66000", 1);

        // 目标：盈利回落 >5% 全平（pending，等待武装）。
        let dd = h
            .rules
            .insert({
                let mut t = WatchRule::new_risk(
                    7,
                    pos.id,
                    WatchConditionKind::ProfitDrawdown,
                    None,
                    ">",
                    dec("0.05"),
                    WatchRuleAction::UseSelf,
                    OrderType::Market,
                    Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
                    Decimal::ZERO,
                    Decimal::ONE,
                );
                t.status = WatchRuleStatus::Pending;
                t
            })
            .unwrap();
        // 武装规则：价格口径 roi > 0.01 → 激活 dd（链式，自身不下单）。
        let arm = roi_rule_action(
            &h,
            pos.id,
            ">",
            "0.01",
            WatchRuleAction::ActivateRule { rule_id: dd.id },
        );

        // 第 1 轮：在峰值，arm 命中、dd 被激活；峰值处回落为 0，本轮无下单。
        let report = h.engine.run_scan().await;
        assert_eq!(report.chained, 1);
        assert_eq!(report.fired, 0);
        assert!(h.sent().is_empty());
        assert!(h.rules.get(arm.id).is_none()); // arm 终结
        assert_eq!(h.rules.get(dd.id).unwrap().status, WatchRuleStatus::Active);

        // 第 2 轮：小幅回落（66000→65400，保证金 roi 0.10→0.09，回落 0.01 < 0.05）→ 不平。
        revalue(&h, "65400", 2);
        assert_eq!(h.engine.run_scan().await.fired, 0);
        assert!(h.sent().is_empty());
        assert_eq!(h.rules.get(dd.id).unwrap().status, WatchRuleStatus::Active);

        // 第 3 轮：继续回落至 62400（保证金 roi 0.04，自峰值回落 0.06 > 0.05）→ 全平。
        revalue(&h, "62400", 3);
        assert_eq!(h.engine.run_scan().await.fired, 1);
        let (cid, reduce_only, _, _) = h.sent()[0].clone();
        assert!(reduce_only);
        assert_eq!(h.orders.get_by_cid(&cid).unwrap().order_quantity, dec("1"));
    }

    /// 挂在指定仓位上的 roi 规则（自定义动作）。
    fn roi_rule_action(
        h: &Harness,
        position_id: u64,
        op: &str,
        value: &str,
        action: WatchRuleAction,
    ) -> WatchRule {
        h.rules
            .insert(WatchRule::new_risk(
                7,
                position_id,
                WatchConditionKind::Roi,
                None,
                op,
                dec(value),
                action,
                OrderType::Market,
                Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
                Decimal::ZERO,
                Decimal::ONE,
            ))
            .unwrap()
    }

    // —— 用例⑥：position_FILUSDT == 0 绑定 BTC 仓；FIL 被平后平 BTC ——
    #[tokio::test]
    async fn case6_fil_close_triggers_linked_btc_close() {
        let h = Harness::new(false);
        let btc = seed_lev_position(&h, "BTCUSDT", "0.01", "60000", "60100", 3);
        let fil = seed_lev_position(&h, "FILUSDT", "10", "5", "5", 1);

        // 绑定 BTC 仓的联动规则：FILUSDT 持仓量 == 0 即全市价平 BTC。
        let rule = h
            .rules
            .insert(WatchRule::new_risk(
                7,
                btc.id,
                WatchConditionKind::Position,
                Some("FILUSDT".to_string()),
                "==",
                Decimal::ZERO,
                WatchRuleAction::UseSelf,
                OrderType::Market,
                Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
                Decimal::ZERO,
                Decimal::ONE,
            ))
            .unwrap();

        // FIL 仍有 10 个：不触发（指标=10，非 0）。
        assert_eq!(h.engine.run_scan().await.fired, 0);
        assert_eq!(
            h.rules.get(rule.id).unwrap().status,
            WatchRuleStatus::Active
        );

        // FIL 被平掉（对账软删）：跨标的持仓量补 0，规则命中平 BTC。
        close_positions(&h, vec![fil], 2);
        assert_eq!(h.engine.run_scan().await.fired, 1);
        let (cid, reduce_only, _, _) = h.sent()[0].clone();
        let order = h.orders.get_by_cid(&cid).unwrap();
        assert!(reduce_only);
        assert_eq!(order.instrument, "BTCUSDT");
        assert_eq!(order.side, Side::Sell);
        assert_eq!(order.order_quantity, dec("0.01"));
    }

    // —— 复现：条件标的是「期权」仓位且一直持有，position == 0 不得误触发 ——
    #[tokio::test]
    async fn option_position_nonzero_does_not_trigger_position_rule() {
        let h = Harness::new(false);

        // 两个期权仓：被保护的 BTC 仓 + 条件标的 ETH 仓（2 张，持续持有）。
        let mut btc = Position::new(7, "deribit", "BTC-25SEP26-86000-C", Side::Buy);
        btc.instrument_type = InstrumentType::Option;
        btc.quantity = dec("2");
        let mut eth = Position::new(7, "deribit", "ETH-25SEP26-2200-C", Side::Sell);
        eth.instrument_type = InstrumentType::Option;
        eth.quantity = dec("2");
        h.positions
            .apply_diffs(trading_models::diff_positions(vec![btc, eth], vec![], 1))
            .unwrap();
        let btc_id = h.positions.list_active()[0].id;

        // 绑定 BTC 期权仓的联动规则：ETH 期权持仓量 == 0 才平 BTC。
        let rule = h
            .rules
            .insert(WatchRule::new_risk(
                7,
                btc_id,
                WatchConditionKind::Position,
                Some("ETH-25SEP26-2200-C".to_string()),
                "==",
                Decimal::ZERO,
                WatchRuleAction::UseSelf,
                OrderType::Market,
                Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
                Decimal::ZERO,
                Decimal::ONE,
            ))
            .unwrap();

        // ETH 期权仓有 2 张（≠0）：规则不得触发、必须保持 active。
        assert_eq!(h.engine.run_scan().await.fired, 0);
        assert_eq!(
            h.rules.get(rule.id).unwrap().status,
            WatchRuleStatus::Active
        );
    }

    // —— 用例⑦：链式激活 new 规则 → 为 binance 账户市价开 ETHUSDT 2x 多仓 ——
    #[tokio::test]
    async fn case7_chain_activates_new_rule_opening_eth_2x_long() {
        let h = Harness::new(false);
        // 信号仓位：3x BTC 多仓。
        let pos = seed_lev_position(&h, "BTCUSDT", "0.01", "60000", "60700", 3);

        // 目标 new 规则：pending；激活后按信号仓位 roi（阈值 -1 恒成立）开 ETH 2x 市价多。
        let eth_new = h
            .rules
            .insert({
                let mut r = WatchRule::new_open(
                    7,
                    pos.id,
                    "binance",
                    "ETHUSDT",
                    InstrumentType::Futures,
                    Side::Buy,
                    OrderType::Market,
                    Decimal::ZERO,
                    dec("0.5"),
                    2,
                );
                r.status = WatchRuleStatus::Pending;
                r.condition_kind = WatchConditionKind::Roi;
                r.operator = ">".to_string();
                r.value = dec("-1");
                r
            })
            .unwrap();
        // 武装规则：价格口径 roi > 0.01 → 激活 new 规则（不下单）。
        let arm = roi_rule_action(
            &h,
            pos.id,
            ">",
            "0.01",
            WatchRuleAction::ActivateRule {
                rule_id: eth_new.id,
            },
        );

        // 第 1 轮：链式激活，无下单。
        let report = h.engine.run_scan().await;
        assert_eq!(report.chained, 1);
        assert_eq!(report.fired, 0);
        assert!(h.sent().is_empty());
        assert!(h.rules.get(arm.id).is_none());
        assert_eq!(
            h.rules.get(eth_new.id).unwrap().status,
            WatchRuleStatus::Active
        );

        // 第 2 轮：new 规则命中 → 开 ETHUSDT 市价多、2x、非 reduce_only。
        assert_eq!(h.engine.run_scan().await.fired, 1);
        let (cid, reduce_only, _, leverage) = h.sent()[0].clone();
        let order = h.orders.get_by_cid(&cid).unwrap();
        assert_eq!(order.instrument, "ETHUSDT");
        assert_eq!(order.side, Side::Buy);
        assert_eq!(order.order_type, OrderType::Market);
        assert_eq!(order.order_quantity, dec("0.5"));
        assert!(!reduce_only);
        assert_eq!(leverage, 2);
    }

    // 跨标的价格在 QuoteTable 缺失（行情 WS 退化）时，回退到同账户活跃仓位的
    // current_price（REST 对账刷新的 fapi 标记价），new 规则仍可触发开仓。
    #[tokio::test]
    async fn cross_price_falls_back_to_active_position_mark() {
        let h = Harness::new(false);
        // 有 BTC 多仓且 current_price>0（由对账/重估设定）；不向 QuoteTable 写价。
        seed_lev_position(&h, "BTCUSDT", "0.002", "80000", "80900", 3);
        // new 规则：跨标的 BTCUSDT 价 > 70000 → 市价开 ETH 0.08、2x。
        h.rules
            .insert({
                let mut r = WatchRule::new_open(
                    7,
                    0,
                    "binance",
                    "ETHUSDT",
                    InstrumentType::Futures,
                    Side::Buy,
                    OrderType::Market,
                    Decimal::ZERO,
                    dec("0.08"),
                    2,
                );
                r.condition_kind = WatchConditionKind::Price;
                r.condition_instrument = Some("BTCUSDT".to_string());
                r.operator = ">".to_string();
                r.value = dec("70000");
                r
            })
            .unwrap();

        assert_eq!(h.engine.run_scan().await.fired, 1);
        let (cid, reduce_only, _, leverage) = h.sent()[0].clone();
        let order = h.orders.get_by_cid(&cid).unwrap();
        assert_eq!(order.instrument, "ETHUSDT");
        assert!(!reduce_only);
        assert_eq!(order.order_quantity, dec("0.08"));
        assert_eq!(leverage, 2);
    }

    /// REST 查单口：对指定 cid 返回 FILLED 终态。
    struct FilledQuerier {
        cid: String,
        exchange_order_id: String,
    }
    #[async_trait]
    impl RuleOrderQuerier for FilledQuerier {
        async fn query(&self, order: &Order, _account_name: &str) -> OrderLookup {
            if order.client_order_id != self.cid {
                return OrderLookup::Unavailable;
            }
            OrderLookup::Found(OrderState {
                client_order_id: Some(order.client_order_id.clone()),
                exchange_order_id: Some(self.exchange_order_id.clone()),
                instrument: order.instrument.clone(),
                status: OrderStatus::Filled,
                filled_quantity: order.order_quantity,
                average_price: Some(dec("81000")),
                position_side: trading_exchange::api::OrderPositionSide::Both,
                venue_extra: None,
            })
        }
    }

    /// REST 查单口：恒返回「交易所明确查无（NotExists）」（模拟币安 -2013）。
    struct NotExistsQuerier;
    #[async_trait]
    impl RuleOrderQuerier for NotExistsQuerier {
        async fn query(&self, _order: &Order, _account_name: &str) -> OrderLookup {
            OrderLookup::NotExists
        }
    }

    // 阶段D纯决策：仅 UNKNOWN 且 age≥60s 才可据"查无"收敛 REJECTED。
    #[test]
    fn should_reject_on_not_exists_rules() {
        // UNKNOWN + 刚好/超过宽限 → true。
        assert!(should_reject_on_not_exists(
            OrderStatus::Unknown,
            NOT_EXISTS_GRACE_MS
        ));
        assert!(should_reject_on_not_exists(
            OrderStatus::Unknown,
            NOT_EXISTS_GRACE_MS + 1
        ));
        // UNKNOWN 但未过宽限 → false。
        assert!(!should_reject_on_not_exists(
            OrderStatus::Unknown,
            NOT_EXISTS_GRACE_MS - 1
        ));
        // 非 UNKNOWN（NEW/…）即便过宽限也不收敛。
        assert!(!should_reject_on_not_exists(
            OrderStatus::New,
            NOT_EXISTS_GRACE_MS + 1
        ));
        assert!(!should_reject_on_not_exists(
            OrderStatus::Filled,
            NOT_EXISTS_GRACE_MS + 1
        ));
    }

    // 阶段D安全口径：未过宽限（或非 UNKNOWN）时，查无不得收敛。
    #[tokio::test]
    async fn rest_reconcile_not_exists_before_grace_is_skipped() {
        let h = Harness::new(false);
        let pos = seed_lev_position(&h, "BTCUSDT", "1", "60000", "60700", 3);
        let rule = roi_rule(&h, pos.id, ">=", "0");
        h.engine.run_scan().await;
        let cid = h.sent()[0].0.clone();
        // created_at 保持当前（age≈0 < 60s），即使置 UNKNOWN 也不得收敛。
        h.orders.mark_unknown(&cid).unwrap();

        let engine = RuleEngine::new(
            h.rules.clone(),
            h.orders.clone(),
            h.positions.clone(),
            Arc::clone(&h.quotes),
            Arc::new(FixedAccounts),
            h.placer.clone(),
            empty_registry(),
        )
        .with_order_querier(Arc::new(NotExistsQuerier));

        assert_eq!(engine.reconcile_terminal_orders().await, 0);
        let order = h.orders.get_by_cid(&cid).unwrap();
        assert_eq!(order.status, OrderStatus::Unknown); // 维持 UNKNOWN
        assert_eq!(h.rules.get(rule.id).unwrap().status, WatchRuleStatus::InUse);
    }

    // REST 兜底：规则单 WS 漏推，主动查单为 FILLED → 订单终态 + 规则 retired。
    #[tokio::test]
    async fn rest_reconcile_converges_terminal_order_and_retires_rule() {
        let h = Harness::new(false);
        let pos = seed_lev_position(&h, "BTCUSDT", "1", "60000", "60700", 3);
        let rule = roi_rule(&h, pos.id, ">=", "0"); // 必然命中
        h.engine.run_scan().await; // 命中 → in_use，FakePlacer 不推进终态
        assert_eq!(h.rules.get(rule.id).unwrap().status, WatchRuleStatus::InUse);
        let cid = h.sent()[0].0.clone();

        // 无 querier：不收敛。
        assert_eq!(h.engine.reconcile_terminal_orders().await, 0);
        assert_eq!(h.rules.get(rule.id).unwrap().status, WatchRuleStatus::InUse);

        // 装上 REST 查单口：查到 FILLED → 订单终态、规则 retired。
        let engine = RuleEngine::new(
            h.rules.clone(),
            h.orders.clone(),
            h.positions.clone(),
            Arc::clone(&h.quotes),
            Arc::new(FixedAccounts),
            h.placer.clone(),
            empty_registry(),
        )
        .with_order_querier(Arc::new(FilledQuerier {
            cid: cid.clone(),
            exchange_order_id: "EX-1".into(),
        }));
        assert_eq!(engine.reconcile_terminal_orders().await, 1);
        assert!(h.rules.get(rule.id).is_none());
        assert_eq!(
            h.rules.get_including_deleted(rule.id).unwrap().status,
            WatchRuleStatus::Retired
        );
        // 终态单已软删：用 peek 读 tombstone。
        let order = h.orders.peek_by_cid(&cid).unwrap();
        assert_eq!(order.status, OrderStatus::Filled);
        assert!(order.deleted);
        // 幂等：再来一轮无未终结规则单，收敛 0。
        assert_eq!(engine.reconcile_terminal_orders().await, 0);
    }

    // REST 兜底：rule_id=0 的策略 UNKNOWN 单也必须被收敛；终态软删，但**不** retire。
    #[tokio::test]
    async fn rest_reconcile_converges_strategy_order_without_rule() {
        let h = Harness::new(false);
        // 直接落一笔 rule_id=0 的策略单并置 UNKNOWN（下单超时场景）。
        let cid = "0x".to_string() + &"b".repeat(32);
        let mut order = Order::new(7, cid.clone(), "binance", "BTCUSDT", Side::Buy);
        order.order_quantity = dec("1");
        order.status = OrderStatus::Unknown;
        h.orders.insert(order).unwrap();

        let engine = RuleEngine::new(
            h.rules.clone(),
            h.orders.clone(),
            h.positions.clone(),
            Arc::clone(&h.quotes),
            Arc::new(FixedAccounts),
            h.placer.clone(),
            empty_registry(),
        )
        .with_order_querier(Arc::new(FilledQuerier {
            cid: cid.clone(),
            exchange_order_id: "EX-9".into(),
        }));

        // 策略单被收敛计数为 1（旧实现因 rule_id=0 过滤返回 0）。
        assert_eq!(engine.reconcile_terminal_orders().await, 1);
        // 终态已软删：活动查询不可见，peek 见 tombstone。
        assert!(h.orders.get_by_cid(&cid).is_none());
        let tomb = h.orders.peek_by_cid(&cid).expect("tombstone 可 peek");
        assert_eq!(tomb.status, OrderStatus::Filled);
        assert!(tomb.deleted);
        // 规则仓仍为空（没有规则被 retire，也没有误建）。
        assert_eq!(h.rules.list_active().len(), 0);
        // 幂等：下一轮无未终结单。
        assert_eq!(engine.reconcile_terminal_orders().await, 0);
    }

    #[tokio::test]
    async fn synchronous_rejection_retires_rule() {
        let h = Harness::new(true); // 下单口模拟拒单
        let pos = seed_long_position(&h);
        let rule = roi_rule(&h, pos.id, ">=", "0.12");

        let report = h.engine.run_scan().await;
        assert_eq!(report.fired, 1);
        // 拒单终态：规则 retired + deleted=1，active 列表为空。
        assert!(h.rules.list_active().is_empty());
        let tomb = h.rules.get_including_deleted(rule.id).unwrap();
        assert_eq!(tomb.status, WatchRuleStatus::Retired);
        assert!(tomb.deleted);
        // 终态拒单已软删：按 sent 的 cid 用 peek 读 tombstone。
        let cid = &h.sent()[0].0;
        let order = h.orders.peek_by_cid(cid).unwrap();
        assert_eq!(order.status, OrderStatus::Rejected);
        assert!(order.deleted);
    }
}
