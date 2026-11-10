//! 企业微信通知（开仓成交 / 风控平仓成交）。
//!
//! **纯通知、无业务操作**：触发点 fire-and-forget；发送在独立任务内进行，
//! HTTP 失败或机器人返回错误一律只记日志，绝不向交易路径传播。
//! WS 回报与 REST 终态兜底共用同一触发收口，订单终结态守卫保证只触发一次。

mod channel;
mod message;
pub mod profile;
pub mod report;

pub use channel::{ChannelUrls, NotifyChannel};

use std::sync::Arc;

use reqwest::Client;
use rust_decimal::Decimal;
use serde::Serialize;
use trading_models::{Order, Position, Side, WatchConditionKind};
use trading_store::{PositionRepo, RuleRepo};

use crate::rules::{RuleAccount, RuleAccountLookup};

/// 企业微信群机器人统一请求体（markdown 类型）。
#[derive(Debug, Serialize)]
struct WeComMessage<'a> {
    msgtype: &'static str,
    markdown: WeComMarkdown<'a>,
}

#[derive(Debug, Serialize)]
struct WeComMarkdown<'a> {
    content: &'a str,
}

/// 企业微信通知句柄（廉价克隆，内部全 Arc）。
///
/// 通道 URL 统一收口在 [`ChannelUrls`]：开仓成交走 Open、风控平仓成交走 Close。
/// 任一通道未配置则对应消息静默跳过。**只通知、不做业务**：所有失败只记日志。
#[derive(Clone)]
pub struct NotifyHub {
    http: Client,
    positions: PositionRepo,
    /// 规则仓储：平仓通知据此判定 condition_kind 是否为 profit_drawdown。
    rules: RuleRepo,
    accounts: Arc<dyn RuleAccountLookup>,
    channels: ChannelUrls,
    /// 各交易所通知差异档案（唯一扩展点，启动期构造后只读）。
    profiles: Arc<profile::NotifyProfiles>,
    /// 价差告警每 symbol 上次发送时间（连接期临时态，非业务数据）。
    spread_last_sent: Arc<tokio::sync::Mutex<std::collections::HashMap<String, i64>>>,
    /// 订单事件播报去重表：`account:cid` → (status, filled)（通知临时态，仅 CTP 档案启用）。
    order_event_dedup: Arc<tokio::sync::Mutex<std::collections::HashMap<String, (String, String)>>>,
}

/// 价差告警冷却：持续超阈值时每 symbol 每 60s 最多发一次。
pub const SPREAD_ALERT_COOLDOWN_MS: i64 = 60_000;

/// 订单事件去重表容量上限：超限整体清空（通知临时态，不做精确 LRU）。
const ORDER_EVENT_DEDUP_CAP: usize = 10_000;

/// 冷却判定（纯函数）：首次（last=None）必发；距上次 ≥ cooldown 才再发。
pub fn should_send_spread_alert(now_ms: i64, last_sent: Option<i64>, cooldown_ms: i64) -> bool {
    match last_sent {
        None => true,
        Some(last) => now_ms.saturating_sub(last) >= cooldown_ms,
    }
}

/// 平仓通知 ROI（纯函数）：盈利值始终按实际成交价重算，分母由档案决定。
///
/// * [`profile::CloseRoiBasis::Cost`]：名义成本口径（`profit / cost`，旧行为）；
/// * [`profile::CloseRoiBasis::Margin`]：占用保证金口径，按平仓量/持仓量摊分保证金。
///
/// 保证金缺失（`margin <= 0`）、持仓量非正或摊分后非正时**回退成本口径**，
/// 绝不除零；平仓量超过持仓量时按持仓量摊分，避免保证金分母被放大。
fn close_pnl_ratio(
    pos: &Position,
    close_price: Decimal,
    close_qty: Decimal,
    basis: profile::CloseRoiBasis,
) -> (Decimal, Decimal) {
    let (profit, cost_ratio) = pos.realized_close_pnl(close_price, close_qty);
    if basis == profile::CloseRoiBasis::Cost {
        return (profit, cost_ratio);
    }
    if pos.margin <= Decimal::ZERO || pos.quantity <= Decimal::ZERO {
        return (profit, cost_ratio);
    }
    // 平仓量按持仓量封顶；摊分保证金 = margin × (平仓量 / 持仓量)。
    let qty = std::cmp::min(close_qty, pos.quantity);
    let margin_part = qty
        .checked_div(pos.quantity)
        .and_then(|ratio| pos.margin.checked_mul(ratio))
        .unwrap_or(Decimal::ZERO);
    if margin_part <= Decimal::ZERO {
        return (profit, cost_ratio);
    }
    let ratio = profit.checked_div(margin_part).unwrap_or(cost_ratio);
    (profit, ratio)
}

impl NotifyHub {
    pub fn new(
        http: Client,
        positions: PositionRepo,
        rules: RuleRepo,
        accounts: Arc<dyn RuleAccountLookup>,
        channels: ChannelUrls,
        profiles: Arc<profile::NotifyProfiles>,
    ) -> Self {
        Self {
            http,
            positions,
            rules,
            accounts,
            channels,
            profiles,
            spread_last_sent: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            order_event_dedup: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// 取交易所通知档案；未登记交易所返回默认档案（与改造前行为一致）。
    pub fn profile(&self, exchange: &str) -> profile::VenueNotifyProfile {
        self.profiles.get(exchange)
    }

    /// Deribit 平仓价差过宽时调用：按每 symbol 60s 冷却决定是否发人工平仓告警。
    ///
    /// 同步判冷却；命中才 fire-and-forget 渲染/发送（manual_close 通道，URL 未配
    /// 则静默）。冷却只作用于通知，不影响引擎每轮重新判价差。
    pub fn on_spread_too_wide(
        &self,
        instrument: &str,
        bid: Decimal,
        ask: Decimal,
        spread: Decimal,
        account_id: u64,
    ) {
        let now = chrono::Utc::now().timestamp_millis();
        let Ok(mut guard) = self.spread_last_sent.try_lock() else {
            return; // 极短锁竞争：本轮跳过
        };
        let last = guard.get(instrument).copied();
        if !should_send_spread_alert(now, last, SPREAD_ALERT_COOLDOWN_MS) {
            tracing::debug!(instrument, "价差告警冷却中，本次不发送");
            return;
        }
        // 先记下发送时间，防并发重复。
        guard.insert(instrument.to_string(), now);
        drop(guard);

        let this = self.clone();
        let instrument = instrument.to_string();
        tokio::spawn(async move {
            this.deliver_spread(&instrument, bid, ask, spread, account_id)
                .await;
        });
    }

    /// 订单回报收口点调用：仅完全成交触发通知。
    ///
    /// fire-and-forget：渲染与发送全部移入独立任务，调用点不等待；
    /// 订单终结态守卫已保证 WS / REST 两条路径只可能有一条到达这里。
    pub fn on_order_filled(&self, order: &Order) {
        if order.status != trading_models::OrderStatus::Filled {
            return;
        }
        let this = self.clone();
        let order = order.clone();
        tokio::spawn(async move {
            this.deliver(&order).await;
        });
    }

    /// 通知 A 触发点：档案启用了订单事件播报的交易所，**任何状态变化**
    /// （挂单/成交/部分成交/撤单/失败）都播报，且不依赖本地是否有 order
    /// （柜台/其他终端手工单同样播）。
    ///
    /// **业务隔离**：本方法只做一次 `spawn`、立即返回，调用点不 await；查询、渲染、
    /// HTTP 全部在异步任务内，超时/失败只 `warn!`，绝不影响订单状态机与策略路由。
    ///
    /// 去重仅在 `profile.dedup_order_events` 时生效（CTP 同一状态会重复推送），
    /// 键为 `(account, cid, status, filled)`；deribit 不去重，行为与改造前一致。
    pub fn announce_order_event(
        &self,
        profile: profile::VenueNotifyProfile,
        ev: &trading_core::protocol::OrderEvent,
        account: &str,
        account_id: Option<u64>,
    ) {
        if profile.order_events.is_none() {
            return;
        }
        // CTP（dedup 开）专用：不可播报的帧（缺方向/委托量、成交类无真实价）不播，
        // 也不占用去重键——避免无价/无方向帧吞掉随后带真实成交价的帧（价格 0 的成因）。
        if profile.dedup_order_events && !Self::order_event_announceable(ev) {
            tracing::debug!(
                cid = %ev.client_order_id,
                status = %ev.status,
                "订单事件不可播报，跳过"
            );
            return;
        }
        if profile.dedup_order_events && self.is_duplicate_order_event(account, ev) {
            tracing::debug!(
                cid = %ev.client_order_id,
                status = %ev.status,
                "订单事件重复，跳过播报"
            );
            return;
        }
        let this = self.clone();
        let ev = ev.clone();
        let account = account.to_string();
        tokio::spawn(async move {
            this.deliver_order_event(profile, &ev, &account, account_id)
                .await;
        });
    }

    /// 订单事件去重判定：同一 `(account, cid, status, filled)` 只播一次。
    ///
    /// 键必须带账户：柜台手工单的 cid 是 OrderRef，按账户/会话独立递增，
    /// 进程级去重表若只按 cid 会误吞另一账户的手工单播报。
    ///
    /// 锁竞争极短时放弃去重（宁可多播不丢播）；容量超限整体清空。
    fn is_duplicate_order_event(
        &self,
        account: &str,
        ev: &trading_core::protocol::OrderEvent,
    ) -> bool {
        let Ok(mut guard) = self.order_event_dedup.try_lock() else {
            return false;
        };
        let key = format!("{account}:{}", ev.client_order_id);
        let status = Self::dedupe_status(ev);
        let filled = ev
            .filled_quantity
            .map(|f| f.to_string())
            .unwrap_or_default();
        if let Some((s, f)) = guard.get(&key) {
            if *s == status && *f == filled {
                return true;
            }
        }
        if guard.len() >= ORDER_EVENT_DEDUP_CAP {
            guard.clear();
        }
        guard.insert(key, (status, filled));
        false
    }

    /// 去重键用的状态：满量成交（filled>=amount）的 PARTIAL 与 FILLED 归一为同一终态，
    /// 避免"成交先到的 PARTIAL 满量"与随后"FILLED"各播一条（trade-first 全成交重复）。
    fn dedupe_status(ev: &trading_core::protocol::OrderEvent) -> String {
        let full = matches!(
            (ev.filled_quantity, ev.amount),
            (Some(f), Some(a)) if a > Decimal::ZERO && f >= a
        );
        match ev.status.as_str() {
            "PARTIAL" | "FILLED" if full => "FILLED".to_string(),
            _ => ev.status.clone(),
        }
    }

    /// CTP 订单事件是否可播报（纯函数）：状态可识别 + 有方向 + 有委托量 + 成交类
    /// （PARTIAL/FILLED，经 [`message::deribit_state_from_wire`] 归一）必须带正的
    /// 真实成交价。在去重之前拦截，既避免渲染 `价格: 0.0000`，也避免不可用帧占去重键。
    fn order_event_announceable(ev: &trading_core::protocol::OrderEvent) -> bool {
        let Some(state) = message::deribit_state_from_wire(&ev.status) else {
            return false;
        };
        if ev.direction.is_none() || ev.amount.is_none() {
            return false;
        }
        if matches!(state, message::DeribitState::Filled)
            && !matches!(ev.filled_price, Some(p) if p > Decimal::ZERO)
        {
            return false;
        }
        true
    }

    /// 投递一条成交通知（同步等待 HTTP，仅供 spawn 任务/测试调用）。
    /// 任何异常路径都只 `warn!`，不向上传播。
    async fn deliver(&self, order: &Order) {
        use trading_models::OrderStatus;
        if order.status != OrderStatus::Filled {
            return;
        }
        // 账户名是标题 userName 的来源；查不到账户时仍发通知（名称降级为空）。
        let user_name = self
            .accounts
            .account_by_id(order.account_id)
            .map(|RuleAccount { name, .. }| name)
            .unwrap_or_default();

        let (channel, content) = if order.reduce_only {
            let (profit, ratio, position) = self.close_pnl(order);
            // 仅 profit_drawdown 规则才追加两行；其余（含缺仓位/规则）为 None。
            let drawdown = self.drawdown_context(order, position);
            (
                NotifyChannel::Close,
                message::render_risk_close(order, &user_name, profit, ratio, drawdown),
            )
        } else {
            (NotifyChannel::Open, message::render_open(order, &user_name))
        };

        // 通道未配置：静默跳过（manual_close 的 close 回退不涉及本路径）。
        let Some(url) = self.channels.target_url(channel) else {
            tracing::debug!(
                client_order_id = %order.client_order_id,
                "通知 Webhook 未配置，静默跳过"
            );
            return;
        };
        self.post_text(url, &content, &order.client_order_id).await;
    }

    /// 平仓盈亏：按订单成交要素精确定位活跃仓位，用**实际成交均价**重算；
    /// 仓位已不可见（被对账软删等）时降级返回 0，消息其余字段照常。
    ///
    /// 同时返回匹配到的仓位（[`Option<Position>`]），供 [`Self::drawdown_context`]
    /// 判定 profit_drawdown，避免重复查仓。
    fn close_pnl(&self, order: &Order) -> (Decimal, Decimal, Option<Position>) {
        // 平仓单方向与仓位相反：Sell 单平多仓、Buy 单平空仓，故取反再定位。
        let position_side = order.side.opposite();
        // 含已软删仓位：避免仓位先一步 deleted=1 时通知取不到、盈亏与回落丢失；
        // 多条同键历史仓位时取 created_at 最晚者。
        let position = self.positions.find_latest_including_deleted(
            order.account_id,
            &order.exchange,
            order.instrument_type,
            &order.instrument,
            position_side,
        );
        match position {
            Some(pos) => {
                // 分母口径由交易所档案决定：deribit/binance 成本、CTP 保证金。
                let basis = self.profile(&order.exchange).close_roi;
                let (profit, ratio) =
                    close_pnl_ratio(&pos, order.filled_price, order.filled_quantity, basis);
                (profit, ratio, Some(pos))
            }
            None => {
                tracing::warn!(
                    client_order_id = %order.client_order_id,
                    instrument = %order.instrument,
                    "平仓通知：查不到匹配活跃仓位，盈亏按 0 降级"
                );
                (Decimal::ZERO, Decimal::ZERO, None)
            }
        }
    }

    /// 平仓通知 drawdown 上下文：**触发本次平仓的订单**所关联规则 condition_kind
    /// 为 profit_drawdown 时，返回 `Some(position.max_roi)`；规则缺失 / 非该类型 /
    /// 无仓位时返回 None。
    ///
    /// 注意：必须用**订单的 `order.rule_id`**（真正触发平仓的规则，链式回落里即
    /// trail 规则），而非 `position.rule_id`——后者记录的是最初开仓规则，链式场景
    /// 指向 arm 规则、对账合并时也不更新，用它会漏判回落止盈。
    /// 只读、不改任何状态。
    fn drawdown_context(&self, order: &Order, position: Option<Position>) -> Option<Decimal> {
        let pos = position?;
        // 通知触发时触发规则往往已 retired（成交即收敛），故须读含已删的快照；
        // 普通 `get` 只读未删行，会把已 retire 的 profit_drawdown 规则漏判。
        let rule = self.rules.get_including_deleted(order.rule_id)?;
        if rule.condition_kind == WatchConditionKind::ProfitDrawdown {
            tracing::debug!(
                position_id = pos.id,
                rule_id = order.rule_id,
                "平仓通知命中 profit_drawdown，追加回落止盈两行"
            );
            Some(pos.max_roi)
        } else {
            tracing::debug!(
                rule_id = order.rule_id,
                "平仓规则非 profit_drawdown，不追加"
            );
            None
        }
    }

    /// 通知 A 投递：解析订单事件 → 拼动作 → 查活跃仓位取 ROI → 渲染 → POST Position 通道。
    ///
    /// 动作矩阵与文案对所有档案完全一致，仅标题（`profile.order_events.title`）与
    /// 查 ROI 的标的类别不同。任何异常（未知状态、缺方向/委托量/URL、HTTP 失败）
    /// 都只记日志、不外传。
    async fn deliver_order_event(
        &self,
        profile: profile::VenueNotifyProfile,
        ev: &trading_core::protocol::OrderEvent,
        account: &str,
        account_id: Option<u64>,
    ) {
        let Some(broadcast) = profile.order_events else {
            return;
        };
        // 前置守卫：通道未配置 / 状态不播报，都在解析渲染之前直接跳过。
        let Some(url) = self.channels.target_url(NotifyChannel::Position) else {
            tracing::debug!(account, "position Webhook 未配置，通知A静默跳过");
            return;
        };
        let Some(state) = message::deribit_state_from_wire(&ev.status) else {
            tracing::debug!(account, status = %ev.status, "通知A：状态不播报，跳过");
            return;
        };
        // 手工单/重放行也必须有方向与委托量，缺失宁可不播（不臆造开/平与数量）。
        let direction = match ev.direction.as_deref() {
            Some("sell") => Side::Sell,
            Some("buy") => Side::Buy,
            _ => {
                tracing::debug!(account, cid = %ev.client_order_id, "通知A：缺订单方向，跳过");
                return;
            }
        };
        let Some(amount) = ev.amount else {
            tracing::debug!(account, cid = %ev.client_order_id, "通知A：缺委托量，跳过");
            return;
        };
        let reduce_only = ev.reduce_only;
        let filled = ev.filled_quantity.unwrap_or(Decimal::ZERO);
        let is_filled = matches!(state, message::DeribitState::Filled);

        // 数量/价格：成交切换为成交量/成交均价，挂单用委托量/委托价。
        let (quantity, price) = if is_filled {
            (filled, ev.filled_price.unwrap_or(Decimal::ZERO))
        } else {
            (amount, ev.limit_price.unwrap_or(Decimal::ZERO))
        };
        // 未成交数量：仅成交减仓且 filled<amount。
        let remaining = if is_filled && reduce_only && filled < amount {
            amount - filled
        } else {
            Decimal::ZERO
        };
        let action = message::build_deribit_action(direction, reduce_only, state, filled, amount);

        // ROI/max_roi：按档案的交易所 + 标的类别查活跃仓位（缺省/查不到则 0，隐藏 ROI 段）。
        let (roi, max_roi) = account_id
            .and_then(|id| {
                self.positions.find_active_instrument(
                    id,
                    profile.exchange,
                    broadcast.instrument_type,
                    &ev.instrument,
                )
            })
            .map(|p| (p.roi, p.max_roi))
            .unwrap_or((Decimal::ZERO, Decimal::ZERO));

        let input = message::DeribitOrderStatusInput {
            // 标题用户名即账户名。
            user_name: account,
            instrument: &ev.instrument,
            action: &action,
            quantity,
            remaining,
            roi,
            max_roi,
            price,
        };
        let content = message::render_order_status(broadcast.title, &input);
        self.post_text(url, &content, account).await;
    }

    /// 投递价差告警：渲染后发往 manual_close 通道（未配置→静默；URL 空则跳过）。
    ///
    /// ROI 尽力从活跃仓位取（找不到按 0，省略该行）。
    async fn deliver_spread(
        &self,
        instrument: &str,
        bid: Decimal,
        ask: Decimal,
        spread: Decimal,
        account_id: u64,
    ) {
        let (strategy_name, roi) = self.spread_context(account_id, instrument);
        let input = message::SpreadAlertInput {
            instrument,
            strategy_name: &strategy_name,
            best_bid: bid,
            best_ask: ask,
            spread,
            roi,
        };
        let content = message::render_deribit_spread(&input);
        let Some(url) = self.channels.target_url(NotifyChannel::ManualClose) else {
            tracing::debug!(instrument, "manual_close Webhook 未配置，静默跳过");
            return;
        };
        self.post_text(url, &content, instrument).await;
    }

    /// 价差告警上下文：策略名与仓位 ROI。
    ///
    /// 当前由规则引擎的平仓价差闸门触发，`WatchRule` 不带策略名——**不得用账户名
    /// 顶替**（账户名显示为"策略名称"是 bug）。故策略名恒为空（渲染时保留标签、
    /// 值留空）。ROI 需方向、告警帧未带方向，按 0 省略。未来若有策略路径触发，
    /// 在此透传真实策略名。
    fn spread_context(&self, account_id: u64, instrument: &str) -> (String, Decimal) {
        let _ = (account_id, instrument);
        (String::new(), Decimal::ZERO)
    }

    /// 发送 markdown 文本并按企业微信约定判定；失败只记日志。
    async fn post_text(&self, url: &str, content: &str, label: &str) {
        let body = WeComMessage {
            msgtype: "markdown",
            markdown: WeComMarkdown { content },
        };
        match self.http.post(url).json(&body).send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let text = resp.text().await.unwrap_or_default();
                if message::wecom_ok(status, &text) {
                    tracing::info!(label, "企业微信通知发送成功");
                } else {
                    tracing::warn!(label, status, reply = %text, "企业微信通知发送失败");
                }
            }
            Err(e) => tracing::warn!(label, error = %e, "企业微信通知 HTTP 失败"),
        }
    }

    /// 分段发送 markdown：内容超过企微 4096 字节限制时，按仓间空行切分为
    /// ≤[`message::CHUNK_MAX_BYTES`] 的段逐段 POST（单仓超限时按字符边界硬切）。
    ///
    /// 每段独立判定成败；失败只 `warn!`，不影响后续段与调用方。
    pub(crate) async fn post_markdown_chunked(&self, url: &str, content: &str, label: &str) {
        let chunks = message::split_chunks(content, message::CHUNK_MAX_BYTES);
        let total = chunks.len();
        for (i, chunk) in chunks.into_iter().enumerate() {
            if i > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            // 多段时标签带序号，便于日志区分。
            let tag = if total > 1 {
                format!("{label}({}/{})", i + 1, total)
            } else {
                label.to_string()
            };
            self.post_text(url, &chunk, &tag).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;
    use trading_models::{InstrumentType, Order, OrderStatus, OrderType, Position, Side};
    use trading_store::PositionRepo;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    /// 固定账户反查：1=follow_prod。
    struct FixedAccounts;
    impl crate::rules::RuleAccountLookup for FixedAccounts {
        fn account_by_id(&self, id: u64) -> Option<crate::rules::RuleAccount> {
            (id == 1).then(|| crate::rules::RuleAccount {
                name: "follow_prod".to_string(),
                is_testnet: false,
                position_mode: trading_models::PositionMode::OneWay,
            })
        }
    }

    /// 内存仓位仓储。
    fn position_repo() -> PositionRepo {
        let store = store_core::Store::new(std::sync::Arc::new(store_core::SystemClock));
        let table = store.register::<Position>().unwrap();
        PositionRepo::new(table)
    }

    /// 已成交订单。
    fn filled_order(side: Side, reduce_only: bool, price: &str, qty: &str) -> Order {
        let mut o = Order::new(1, "0xclient", "binance", "BTCUSDT", side);
        o.instrument_type = InstrumentType::Futures;
        o.reduce_only = reduce_only;
        o.strategy_name = Some("网格策略A".to_string());
        o.filled_price = dec(price);
        o.filled_quantity = dec(qty);
        o.order_quantity = dec(qty);
        o.status = OrderStatus::Filled;
        o
    }

    /// 极简 mock 企业微信服务器：返回 (URL, 收到的请求体集合)。
    fn mock_wecom() -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        mock_wecom_n(1)
    }

    /// 接收最多 `n` 条请求的 mock（多消息播报测试用）。
    fn mock_wecom_n(n: usize) -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let bodies = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let bodies2 = std::sync::Arc::clone(&bodies);
        std::thread::spawn(move || {
            for stream in listener.incoming().take(n) {
                let mut stream = stream.unwrap();
                let mut buf = Vec::new();
                let mut chunk = [0u8; 1024];
                // 读到头，解析 Content-Length 后收齐 body。
                let body = loop {
                    let n = stream.read(&mut chunk).unwrap();
                    buf.extend_from_slice(&chunk[..n]);
                    let text = String::from_utf8_lossy(&buf);
                    if let Some((headers, rest)) = text.split_once("\r\n\r\n") {
                        let len: usize = headers
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|v| v.trim().parse().ok())
                            })
                            .unwrap_or(0);
                        let mut body = rest.as_bytes().to_vec();
                        while body.len() < len {
                            let n = stream.read(&mut chunk).unwrap();
                            body.extend_from_slice(&chunk[..n]);
                        }
                        break String::from_utf8(body).unwrap();
                    }
                };
                bodies2.lock().unwrap().push(body);
                let reply = b"HTTP/1.1 200 OK\r\nContent-Length: 14\r\n\r\n{\"errcode\":0}";
                stream.write_all(reply).unwrap();
            }
        });
        (url, bodies)
    }

    fn hub(positions: PositionRepo, open_url: &str, close_url: &str) -> NotifyHub {
        let channels = ChannelUrls::new(
            open_url.to_string(),
            close_url.to_string(),
            String::new(),
            String::new(),
            String::new(),
        );
        // 空规则仓储：默认无 profit_drawdown 规则，平仓通知保持原样。
        let store = store_core::Store::new(std::sync::Arc::new(store_core::SystemClock));
        let rule_table = store.register::<trading_models::WatchRule>().unwrap();
        let rule_repo = trading_store::RuleRepo::new(rule_table);
        NotifyHub::new(
            reqwest::Client::new(),
            positions,
            rule_repo,
            std::sync::Arc::new(FixedAccounts),
            channels,
            default_profiles(),
        )
    }

    /// 默认档案注册表（-20% / -20%），与生产默认阈值一致。
    fn default_profiles() -> Arc<profile::NotifyProfiles> {
        Arc::new(profile::NotifyProfiles::from_config(
            dec("-0.20"),
            dec("-0.20"),
        ))
    }

    #[tokio::test]
    async fn deliver_open_posts_markdown_to_open_webhook() {
        // Arrange：mock 服务器接收开仓通知。
        let (url, bodies) = mock_wecom();
        let h = hub(position_repo(), &url, "");

        // Act：直接执行投递（不经 spawn，便于断言）。
        h.deliver(&filled_order(Side::Buy, false, "63250.5", "0.125"))
            .await;

        // Assert：请求体为 markdown 机器人消息，内容与文档示例一致。
        let sent = bodies.lock().unwrap();
        assert_eq!(sent.len(), 1);
        let v: serde_json::Value = serde_json::from_str(&sent[0]).unwrap();
        assert_eq!(v["msgtype"], "markdown");
        assert_eq!(
            v["markdown"]["content"],
            "**> follow_prod(FutureOrder)**\n\
             - 类型：合约\n\
             - 合约代币：BTCUSDT\n\
             - 策略名称：网格策略A\n\
             - 方向：开多挂单\n\
             - 下单价格：63250.500000\n\
             - 数量：0.125000"
        );
    }

    #[tokio::test]
    async fn deliver_risk_close_uses_position_pnl_at_fill_price() {
        // Arrange：本地活跃多仓（成本 63250.5），平仓成交 64100.2。
        let repo = position_repo();
        let mut pos = Position::new(1, "binance", "BTCUSDT", Side::Buy);
        pos.quantity = dec("0.125");
        pos.pos_price = dec("63250.5");
        repo.upsert_matching(vec![pos]).unwrap();

        let (url, bodies) = mock_wecom();
        let h = hub(repo, "", &url);

        // Act：
        h.deliver(&filled_order(Side::Sell, true, "64100.2", "0.125"))
            .await;

        // Assert：平仓消息发往 close 通道，盈亏按成交均价重算（=106.2125）。
        let sent = bodies.lock().unwrap();
        assert_eq!(sent.len(), 1);
        let v: serde_json::Value = serde_json::from_str(&sent[0]).unwrap();
        let content = v["markdown"]["content"].as_str().unwrap();
        assert!(content.contains("**> follow_prod(新风控下单)**"));
        assert!(content.contains("- 方向：平多挂单"));
        // 盈亏 =(64100.2-63250.5)×0.125=106.2125；比率 =106.2125/(63250.5×0.125)=1.3434%。
        assert!(content.contains("- 盈利值：106.212500"));
        assert!(content.contains("- 盈利百分比：1.3434%"));
    }

    // ---- Task 2：平仓 ROI 口径（Cost / Margin；纯函数 + 消息级）----

    /// 构造一个合约仓位（成本价/数量/保证金/乘数可调）。
    fn position(
        exchange: &str,
        instrument: &str,
        side: Side,
        pos_price: &str,
        quantity: &str,
        margin: &str,
    ) -> Position {
        let mut p = Position::new(1, exchange, instrument, side);
        p.instrument_type = InstrumentType::Futures;
        p.pos_price = dec(pos_price);
        p.quantity = dec(quantity);
        p.multiplier = dec("10");
        p.margin = dec(margin);
        p
    }

    #[test]
    fn close_pnl_ratio_cost_basis_matches_legacy_cost_ratio() {
        use crate::notify::profile::CloseRoiBasis;
        // 多仓：成本 100×1×10=1000，平仓 110 → profit=100，Cost 口径 ratio=100/1000=0.1。
        let pos = position("binance", "BTCUSDT", Side::Buy, "100", "1", "50");
        let (profit, ratio) = close_pnl_ratio(&pos, dec("110"), dec("1"), CloseRoiBasis::Cost);
        assert_eq!(profit, dec("100"));
        assert_eq!(ratio, dec("0.1"));
    }

    #[test]
    fn close_pnl_ratio_margin_basis_full_close_uses_margin() {
        use crate::notify::profile::CloseRoiBasis;
        // CTP：全平按占用保证金 50 作分母 → ratio=100/50=2（200%），而非成本口径 0.1。
        let pos = position("ctp", "rb2612", Side::Buy, "100", "1", "50");
        let (profit, ratio) = close_pnl_ratio(&pos, dec("110"), dec("1"), CloseRoiBasis::Margin);
        assert_eq!(profit, dec("100"));
        assert_eq!(ratio, dec("2"));
    }

    #[test]
    fn close_pnl_ratio_margin_basis_partial_close_prorates_margin() {
        use crate::notify::profile::CloseRoiBasis;
        // 4 手持仓保证金 1000，平 2 手 → 摊分保证金 500；profit=200 → ratio=0.4。
        let pos = position("ctp", "rb2612", Side::Buy, "100", "4", "1000");
        let (profit, ratio) = close_pnl_ratio(&pos, dec("110"), dec("2"), CloseRoiBasis::Margin);
        assert_eq!(profit, dec("200"));
        assert_eq!(ratio, dec("0.4"));
    }

    #[test]
    fn close_pnl_ratio_margin_basis_falls_back_when_margin_or_quantity_missing() {
        use crate::notify::profile::CloseRoiBasis;
        // margin=0 / quantity=0：回退成本口径，绝不除零。
        let no_margin = position("ctp", "rb2612", Side::Buy, "100", "1", "0");
        assert_eq!(
            close_pnl_ratio(&no_margin, dec("110"), dec("1"), CloseRoiBasis::Margin),
            (dec("100"), dec("0.1"))
        );
        let no_qty = position("ctp", "rb2612", Side::Buy, "100", "0", "50");
        assert_eq!(
            close_pnl_ratio(&no_qty, dec("110"), dec("1"), CloseRoiBasis::Margin),
            (dec("100"), dec("0.1"))
        );
    }

    #[test]
    fn close_pnl_ratio_margin_basis_caps_close_qty_at_position_qty() {
        use crate::notify::profile::CloseRoiBasis;
        // 平仓量大于持仓量：保证金按持仓量封顶（50，不随平仓量放大），
        // 盈利仍按传入平仓量计算（10×2×10=200）→ 200/50=4。
        let pos = position("ctp", "rb2612", Side::Buy, "100", "1", "50");
        let (profit, ratio) = close_pnl_ratio(&pos, dec("110"), dec("2"), CloseRoiBasis::Margin);
        assert_eq!(profit, dec("200"));
        assert_eq!(ratio, dec("4"));
    }

    #[tokio::test]
    async fn deliver_ctp_close_uses_margin_roi_percentage_e2e() {
        // Arrange：CTP 多仓（保证金 50、乘数 10），平仓成交 110。
        let repo = position_repo();
        repo.upsert_matching(vec![position("ctp", "rb2612", Side::Buy, "100", "1", "50")])
            .unwrap();

        let (url, bodies) = mock_wecom();
        let h = hub(repo, "", &url);
        let mut order = filled_order(Side::Sell, true, "110", "1");
        order.exchange = "ctp".to_string();
        order.instrument = "rb2612".to_string();
        h.deliver(&order).await;

        // Assert：盈利值按成交价重算 100；百分比按保证金口径 100/50=200%。
        let sent = bodies.lock().unwrap();
        let v: serde_json::Value = serde_json::from_str(&sent[0]).unwrap();
        let content = v["markdown"]["content"].as_str().unwrap();
        assert!(content.contains("- 盈利值：100.000000"), "实际:\n{content}");
        assert!(
            content.contains("- 盈利百分比：200.0000%"),
            "实际:\n{content}"
        );
    }

    // ---- Task 6：CTP 订单事件播报（手工单 / 标题 / 去重 / replay）----

    /// 构造订单事件（方向/开平/委托量/成交可调）。
    fn order_event(
        cid: &str,
        status: &str,
        direction: &str,
        reduce_only: bool,
        amount: &str,
        filled: Option<(&str, &str)>,
        limit_price: &str,
    ) -> trading_core::protocol::OrderEvent {
        trading_core::protocol::OrderEvent {
            client_order_id: cid.to_string(),
            status: status.to_string(),
            exchange_order_id: Some("sys-1".to_string()),
            filled_price: filled.map(|(_, p)| dec(p)),
            filled_quantity: filled.map(|(q, _)| dec(q)),
            exchange_update_time: None,
            message: None,
            instrument: "rb2612".to_string(),
            direction: Some(direction.to_string()),
            reduce_only,
            amount: Some(dec(amount)),
            limit_price: Some(dec(limit_price)),
        }
    }

    /// Position 通道指向 mock 的 hub（订单事件播报走该通道）。
    fn hub_with_position_url(url: &str) -> NotifyHub {
        hub_with_position_url_and(position_repo(), url)
    }

    /// 同上，但可注入仓位仓储（ROI 片段测试用）。
    fn hub_with_position_url_and(positions: PositionRepo, url: &str) -> NotifyHub {
        let mut h = hub(positions, "", "");
        h.channels = ChannelUrls::new(
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            url.to_string(),
        );
        h
    }

    /// 等待 mock 收到至少 `n` 条请求（announce 内部 spawn，异步到达）。
    async fn wait_for_bodies(bodies: &std::sync::Arc<std::sync::Mutex<Vec<String>>>, n: usize) {
        for _ in 0..200 {
            if bodies.lock().unwrap().len() >= n {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!(
            "等待 {n} 条通知超时，实际 {} 条",
            bodies.lock().unwrap().len()
        );
    }

    #[tokio::test]
    async fn deliver_ctp_order_event_manual_order_uses_profile_title_and_copy() {
        // 本地无任何 Order/Position（柜台手工单）：仍按事件字段播报。
        let (url, bodies) = mock_wecom();
        let h = hub_with_position_url(&url);
        h.deliver_order_event(
            h.profile("ctp"),
            &order_event("manual-1", "NEW", "sell", true, "5", None, "3500"),
            "ctp_acct",
            None,
        )
        .await;

        let sent = bodies.lock().unwrap();
        assert_eq!(sent.len(), 1);
        let v: serde_json::Value = serde_json::from_str(&sent[0]).unwrap();
        assert_eq!(
            v["markdown"]["content"],
            "> **CtpPositions(ctp_acct)**\n\
             代币：rb2612，减买仓挂单成功，数量：5.0000 ，价格: 3500.0000"
        );
    }

    #[tokio::test]
    async fn deliver_ctp_filled_event_uses_filled_qty_and_vwap() {
        let (url, bodies) = mock_wecom();
        let h = hub_with_position_url(&url);
        h.deliver_order_event(
            h.profile("ctp"),
            &order_event(
                "c-1",
                "FILLED",
                "buy",
                false,
                "5",
                Some(("5", "3520.5")),
                "3500",
            ),
            "ctp_acct",
            None,
        )
        .await;

        let sent = bodies.lock().unwrap();
        let v: serde_json::Value = serde_json::from_str(&sent[0]).unwrap();
        assert_eq!(
            v["markdown"]["content"],
            "> **CtpPositions(ctp_acct)**\n\
             代币：rb2612，开买仓成功，数量：5.0000 ，价格: 3520.5000"
        );
    }

    #[tokio::test]
    async fn deliver_ctp_order_event_includes_roi_from_local_position() {
        // 档案按 (exchange=ctp, instrument_type=Futures) 查本地活跃仓位取 ROI。
        let repo = position_repo();
        let mut pos = Position::new(1, "ctp", "rb2612", Side::Buy);
        pos.instrument_type = InstrumentType::Futures;
        pos.quantity = dec("1");
        pos.pos_price = dec("3500");
        pos.multiplier = dec("10");
        pos.roi = dec("-0.25");
        pos.max_roi = dec("0.12");
        repo.upsert_matching(vec![pos]).unwrap();

        let (url, bodies) = mock_wecom();
        let h = hub_with_position_url_and(repo, &url);
        h.deliver_order_event(
            h.profile("ctp"),
            &order_event(
                "r1",
                "FILLED",
                "sell",
                true,
                "5",
                Some(("5", "3505")),
                "3500",
            ),
            "ctp_acct",
            Some(1),
        )
        .await;

        let sent = bodies.lock().unwrap();
        let v: serde_json::Value = serde_json::from_str(&sent[0]).unwrap();
        let content = v["markdown"]["content"].as_str().unwrap();
        assert!(
            content.contains(" (ROI: -25.00%, Max ROI: 12.00%)"),
            "实际:\n{content}"
        );
    }

    #[tokio::test]
    async fn ctp_order_events_dedup_repeated_state_to_three_messages() {
        // 10 手 5+5：挂单 → 第一次成交（部分成交）→ 终态成功。
        // 同一 (cid,status,filled) 的重复帧（OnRtnTrade 与 OnRtnOrder 同状态）只播一次。
        // mock 接受 4 条：若去重失效，第 4 条会被记录下来使断言失败。
        let (url, bodies) = mock_wecom_n(4);
        let h = hub_with_position_url(&url);
        let profile = h.profile("ctp");
        let events = [
            order_event("c1", "NEW", "sell", true, "10", None, "3500"),
            order_event(
                "c1",
                "PARTIAL",
                "sell",
                true,
                "10",
                Some(("5", "3500")),
                "3500",
            ),
            // 重复帧：与上一条状态/成交量完全相同 → 去重。
            order_event(
                "c1",
                "PARTIAL",
                "sell",
                true,
                "10",
                Some(("5", "3500")),
                "3500",
            ),
            order_event(
                "c1",
                "FILLED",
                "sell",
                true,
                "10",
                Some(("10", "3505")),
                "3500",
            ),
        ];
        for ev in &events {
            h.announce_order_event(profile, ev, "ctp_acct", None);
        }

        wait_for_bodies(&bodies, 3).await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let sent = bodies.lock().unwrap();
        assert_eq!(sent.len(), 3, "重复状态不应重复播报（挂单/部分成交/成功）");
        // 内容断言：三种状态各恰好一条，防止「去重了错误的帧」这类回归。
        let contents: Vec<String> = sent
            .iter()
            .map(|b| {
                let v: serde_json::Value = serde_json::from_str(b).unwrap();
                v["markdown"]["content"].as_str().unwrap().to_string()
            })
            .collect();
        assert_eq!(
            contents.iter().filter(|c| c.contains("挂单成功")).count(),
            1,
            "实际: {contents:?}"
        );
        assert_eq!(
            contents
                .iter()
                .filter(|c| c.contains("部分成交成功"))
                .count(),
            1,
            "实际: {contents:?}"
        );
        assert_eq!(
            contents.iter().filter(|c| c.contains("减买仓成功")).count(),
            1,
            "实际: {contents:?}"
        );
    }

    #[tokio::test]
    async fn ctp_dedup_is_per_account_not_global() {
        // 手工单 cid 是柜台 OrderRef（每账户/会话递增）：两账户同 cid 同状态都必须播。
        let (url, bodies) = mock_wecom_n(2);
        let h = hub_with_position_url(&url);
        let profile = h.profile("ctp");
        let ev = order_event("101", "NEW", "buy", false, "5", None, "3500");
        h.announce_order_event(profile, &ev, "acct-a", None);
        h.announce_order_event(profile, &ev, "acct-b", None);

        wait_for_bodies(&bodies, 2).await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            bodies.lock().unwrap().len(),
            2,
            "去重必须按账户隔离，不能跨账户吞掉手工单播报"
        );
    }

    #[tokio::test]
    async fn ctp_leg_events_are_deduped_per_physical_order() {
        // 同一逻辑 cid 的两张物理腿（cid#OrderRef）各播一条；重复的同腿状态仍被去重。
        let (url, bodies) = mock_wecom_n(3);
        let h = hub_with_position_url(&url);
        let profile = h.profile("ctp-leg");
        let leg_a = order_event("c1#101", "NEW", "buy", false, "5", None, "3500");
        let leg_b = order_event("c1#102", "NEW", "buy", false, "5", None, "3500");

        h.announce_order_event(profile, &leg_a, "ctp_acct", None);
        h.announce_order_event(profile, &leg_b, "ctp_acct", None);
        wait_for_bodies(&bodies, 2).await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(bodies.lock().unwrap().len(), 2, "两张物理单各一条");

        // 重复的同腿状态：若未去重会发第 3 条（mock 接受 3 条，会被记录）。
        h.announce_order_event(profile, &leg_a, "ctp_acct", None);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            bodies.lock().unwrap().len(),
            2,
            "同一物理腿的重复状态应被去重"
        );
    }

    /// 无价成交帧：不得播 0 价、也不得占用去重位；随后带价帧必须正常播（真实价）。
    #[tokio::test]
    async fn ctp_unpriced_partial_skipped_and_does_not_block_priced_frame() {
        let (url, bodies) = mock_wecom_n(1);
        let h = hub_with_position_url(&url);
        let profile = h.profile("ctp");
        let mut unpriced = order_event(
            "c-p",
            "PARTIAL",
            "sell",
            true,
            "10",
            Some(("4", "0")),
            "3500",
        );
        unpriced.filled_price = None; // 快照推导帧：无价
        h.announce_order_event(profile, &unpriced, "ctp_acct", None);
        let priced = order_event(
            "c-p",
            "PARTIAL",
            "sell",
            true,
            "10",
            Some(("4", "3500")),
            "3500",
        );
        h.announce_order_event(profile, &priced, "ctp_acct", None);

        wait_for_bodies(&bodies, 1).await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let sent = bodies.lock().unwrap();
        assert_eq!(sent.len(), 1, "只应播带价那条");
        let v: serde_json::Value = serde_json::from_str(&sent[0]).unwrap();
        let content = v["markdown"]["content"].as_str().unwrap();
        assert!(content.contains("部分成交成功"), "实际: {content}");
        assert!(
            content.contains("价格: 3500.0000"),
            "带价帧不得被无价帧吞掉: {content}"
        );
    }

    /// 成交先到：PARTIAL(满量) 与随后 FILLED 视为同一终态，只播一条。
    #[tokio::test]
    async fn ctp_trade_first_full_fill_notifies_once() {
        let (url, bodies) = mock_wecom_n(2);
        let h = hub_with_position_url(&url);
        let profile = h.profile("ctp");
        h.announce_order_event(
            profile,
            &order_event(
                "c-f",
                "PARTIAL",
                "sell",
                true,
                "10",
                Some(("10", "3500")),
                "3500",
            ),
            "ctp_acct",
            None,
        );
        h.announce_order_event(
            profile,
            &order_event(
                "c-f",
                "FILLED",
                "sell",
                true,
                "10",
                Some(("10", "3500")),
                "3500",
            ),
            "ctp_acct",
            None,
        );
        wait_for_bodies(&bodies, 1).await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(bodies.lock().unwrap().len(), 1, "满量成交不得重复播报");
    }

    /// deribit（dedup=false）不得受 CTP 可播报守卫影响：无价成交仍按旧行为播（隔离回归）。
    #[tokio::test]
    async fn deribit_unpriced_fill_still_notifies_unchanged() {
        let (url, bodies) = mock_wecom_n(1);
        let h = hub_with_position_url(&url);
        let profile = h.profile("deribit");
        let mut ev = order_event("d-1", "FILLED", "buy", false, "2", Some(("2", "0")), "0");
        ev.filled_price = None;
        h.announce_order_event(profile, &ev, "deribit_acct", None);
        wait_for_bodies(&bodies, 1).await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            bodies.lock().unwrap().len(),
            1,
            "deribit 通知行为不得被 CTP 守卫改变"
        );
    }

    #[tokio::test]
    async fn deribit_order_events_are_not_deduped() {
        // deribit 档案 dedup=false：同一状态重复推送仍逐条播报（现状不变）。
        let (url, bodies) = mock_wecom_n(2);
        let h = hub_with_position_url(&url);
        let profile = h.profile("deribit");
        let ev = order_event("d1", "NEW", "buy", false, "1", None, "0.01");
        h.announce_order_event(profile, &ev, "deribit_acct", None);
        h.announce_order_event(profile, &ev, "deribit_acct", None);

        wait_for_bodies(&bodies, 2).await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(bodies.lock().unwrap().len(), 2);
    }

    #[test]
    fn ctp_replay_source_has_no_order_events() {
        // 重连/重启的重放行 source="ctp-replay"：未登记 → 默认档案 → 不播。
        let h = hub(position_repo(), "", "");
        assert!(h.profile("ctp").order_events.is_some());
        assert!(h.profile("ctp-replay").order_events.is_none());
    }

    #[tokio::test]
    async fn deliver_trail_close_appends_drawdown_lines_e2e() {
        // Arrange：hub 内插入一条 profit_drawdown(trail) 规则；upsert 一个对应活跃空仓。
        let (mut h, pos_repo, trail_id) = hub_with_rule(trail_rule());
        // 让 mock webhook 生效（hub_with_rule 里 channel 指向 unused，这里换通道）。
        let (url, bodies) = mock_wecom();
        h.channels = ChannelUrls::new(
            String::new(),
            url,
            String::new(),
            String::new(),
            String::new(),
        );
        let mut pos = Position::new(1, "deribit", "BTC-1OCT26-82000-P", Side::Sell);
        pos.instrument_type = InstrumentType::Option;
        pos.quantity = dec("2");
        pos.pos_price = dec("0.0008");
        pos.rule_id = 73; // 仓位 rule_id 指向 arm（模拟链式），不能用它判定。
        pos.max_roi = dec("0.5");
        pos_repo.upsert_matching(vec![pos]).unwrap();

        // 平仓 BUY 单（平空），rule_id 指向真正触发的 trail 规则。
        let mut order = filled_order(Side::Buy, true, "0.0009", "2");
        order.exchange = "deribit".to_string();
        order.instrument = "BTC-1OCT26-82000-P".to_string();
        order.instrument_type = InstrumentType::Option;
        order.rule_id = trail_id;
        h.deliver(&order).await;

        // Assert：消息含「类型：回落止盈」「最大盈利百分比」。
        let sent = bodies.lock().unwrap();
        assert_eq!(sent.len(), 1, "应发出一条 close 通知");
        let v: serde_json::Value = serde_json::from_str(&sent[0]).unwrap();
        let content = v["markdown"]["content"].as_str().unwrap();
        assert!(content.contains("- 类型：回落止盈"), "实际内容:\n{content}");
        assert!(content.contains("- 最大盈利百分比：50.0000%"));
    }

    #[tokio::test]
    async fn deliver_missing_position_degrades_pnl_to_zero() {
        // 查不到活跃仓位（已被对账软删等）：消息照发，盈亏降级为 0。
        let (url, bodies) = mock_wecom();
        let h = hub(position_repo(), "", &url);
        h.deliver(&filled_order(Side::Sell, true, "64100.2", "0.125"))
            .await;

        let sent = bodies.lock().unwrap();
        let v: serde_json::Value = serde_json::from_str(&sent[0]).unwrap();
        let content = v["markdown"]["content"].as_str().unwrap();
        assert!(content.contains("- 盈利值：0.000000"));
        assert!(content.contains("- 盈利百分比：0.0000%"));
    }

    #[tokio::test]
    async fn deliver_empty_webhook_skips_silently() {
        // 对应通道 URL 为空：静默跳过，不发请求（mock 服务器不被调用，无 panic 即通过）。
        let h = hub(position_repo(), "", "");
        h.deliver(&filled_order(Side::Buy, false, "100", "1")).await;
        h.deliver(&filled_order(Side::Sell, true, "100", "1")).await;
    }

    #[tokio::test]
    async fn deliver_non_filled_order_is_ignored() {
        // 只有完全成交才通知：NEW/PARTIAL 一律忽略。
        let (url, bodies) = mock_wecom();
        let h = hub(position_repo(), &url, "");
        let mut o = filled_order(Side::Buy, false, "100", "1");
        o.status = OrderStatus::New;
        h.deliver(&o).await;
        assert!(bodies.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn deliver_bad_webhook_failure_does_not_propagate() {
        // 指向不存在的端口：发送失败只内部消化，deliver 不 panic/不返回 Err。
        let h = hub(position_repo(), "http://127.0.0.1:1/", "");
        h.deliver(&filled_order(Side::Buy, false, "100", "1")).await;
    }

    /// 构造一个带规则仓储的 hub，并插入一条规则；返回 (hub, 规则的真实 id)。
    fn hub_with_rule(rule: trading_models::WatchRule) -> (NotifyHub, PositionRepo, u64) {
        let positions = position_repo();
        let store = store_core::Store::new(std::sync::Arc::new(store_core::SystemClock));
        let rule_table = store.register::<trading_models::WatchRule>().unwrap();
        let rule_repo = trading_store::RuleRepo::new(rule_table);
        let saved = rule_repo.insert(rule).unwrap();
        let channels = ChannelUrls::new(
            "http://unused".to_string(),
            "http://unused".to_string(),
            String::new(),
            String::new(),
            String::new(),
        );
        let h = NotifyHub::new(
            reqwest::Client::new(),
            positions.clone(),
            rule_repo,
            std::sync::Arc::new(FixedAccounts),
            channels,
            default_profiles(),
        );
        (h, positions, saved.id)
    }

    /// 一条 profit_drawdown(trail) 规则。
    fn trail_rule() -> trading_models::WatchRule {
        use trading_models::{WatchConditionKind, WatchRuleAction};
        let mut r = trading_models::WatchRule::new_risk(
            1,
            1270,
            WatchConditionKind::ProfitDrawdown,
            None,
            ">=",
            dec("0.002"),
            WatchRuleAction::UseSelf,
            OrderType::Market,
            Decimal::ZERO, // risk 平仓限价：测试夹具沿用旧硬编码 ZERO
            Decimal::ZERO,
            dec("1"),
        );
        r.id = 0; // 插入前清零，由仓储分配。
        r
    }

    #[test]
    fn hub_profile_uses_registry_for_known_and_unknown_exchanges() {
        // 通知层统一从 NotifyHub 取档案：已登记所命中专属档案，未知所回落默认。
        let h = hub(position_repo(), "", "");
        assert_eq!(
            h.profile("deribit").close_roi,
            crate::notify::profile::CloseRoiBasis::Cost
        );
        assert_eq!(
            h.profile("ctp").report_source,
            crate::notify::profile::ReportSource::LocalPositions
        );
        assert!(h.profile("binance").order_events.is_none());
    }

    #[test]
    fn drawdown_context_uses_order_rule_id_not_position_rule_id() {
        // Arrange：插入 profit_drawdown(trail) 规则。
        let (h, _positions, trail_id) = hub_with_rule(trail_rule());

        // 仓位的 rule_id 故意指向别处（模拟链式：仓位 rule_id=arm/开仓规则）。
        let mut pos = Position::new(1, "deribit", "BTC-1OCT26-82000-P", Side::Sell);
        pos.rule_id = 999; // 非 trail 规则；不能用它判定。
        pos.max_roi = dec("0.05");

        // 平仓订单的 rule_id 才指向真正触发平仓的 trail 规则。
        let mut order = filled_order(Side::Buy, true, "0.0008", "2");
        order.rule_id = trail_id;

        // Act：用订单 rule_id 判定 → 识别为回落止盈，返回 max_roi。
        assert_eq!(h.drawdown_context(&order, Some(pos)), Some(dec("0.05")));
    }

    #[test]
    fn drawdown_context_non_drawdown_trigger_returns_none() {
        // 订单由非 profit_drawdown 规则触发（rule_id 指向普通规则或为 0）→ None。
        let (h, _p, _id) = hub_with_rule(trail_rule());
        let pos = Position::new(1, "deribit", "BTC-1OCT26-82000-P", Side::Sell);
        let mut order = filled_order(Side::Buy, true, "0.0008", "2");
        order.rule_id = 0; // 非规则平仓。
        assert_eq!(h.drawdown_context(&order, Some(pos)), None);
    }
}
