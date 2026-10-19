//! 企业微信通知（开仓成交 / 风控平仓成交）。
//!
//! **纯通知、无业务操作**：触发点 fire-and-forget；发送在独立任务内进行，
//! HTTP 失败或机器人返回错误一律只记日志，绝不向交易路径传播。
//! WS 回报与 REST 终态兜底共用同一触发收口，订单终结态守卫保证只触发一次。

mod channel;
mod message;
pub mod report;

pub use channel::{ChannelUrls, NotifyChannel};

use std::sync::Arc;

use reqwest::Client;
use rust_decimal::Decimal;
use serde::Serialize;
use trading_models::{Order, Position, WatchConditionKind};
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
    /// 价差告警每 symbol 上次发送时间（连接期临时态，非业务数据）。
    spread_last_sent: Arc<tokio::sync::Mutex<std::collections::HashMap<String, i64>>>,
}

/// 价差告警冷却：持续超阈值时每 symbol 每 60s 最多发一次。
pub const SPREAD_ALERT_COOLDOWN_MS: i64 = 60_000;

/// 冷却判定（纯函数）：首次（last=None）必发；距上次 ≥ cooldown 才再发。
pub fn should_send_spread_alert(now_ms: i64, last_sent: Option<i64>, cooldown_ms: i64) -> bool {
    match last_sent {
        None => true,
        Some(last) => now_ms.saturating_sub(last) >= cooldown_ms,
    }
}

impl NotifyHub {
    pub fn new(
        http: Client,
        positions: PositionRepo,
        rules: RuleRepo,
        accounts: Arc<dyn RuleAccountLookup>,
        channels: ChannelUrls,
    ) -> Self {
        Self {
            http,
            positions,
            rules,
            accounts,
            channels,
            spread_last_sent: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        }
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

    /// 通知 A 触发点：deribit 订单流**任何状态变化**（挂单/成交/部分成交/撤单/失败）
    /// 都播报，且不依赖本地是否有 order。
    ///
    /// **业务隔离**：本方法只做一次 `spawn`、立即返回，调用点不 await；查询、渲染、
    /// HTTP 全部在异步任务内，超时/失败只 `warn!`，绝不影响订单状态机与策略路由。
    pub fn announce_deribit_position(
        &self,
        ev: &trading_core::protocol::OrderEvent,
        account: &str,
        account_id: Option<u64>,
    ) {
        let this = self.clone();
        let ev = ev.clone();
        let account = account.to_string();
        tokio::spawn(async move {
            this.deliver_deribit_position(&ev, &account, account_id)
                .await;
        });
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
                let (profit, ratio) =
                    pos.realized_close_pnl(order.filled_price, order.filled_quantity);
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
    /// 任何异常（未知状态、缺 account_id/仓位/URL、HTTP 失败）都只记日志、不外传。
    async fn deliver_deribit_position(
        &self,
        ev: &trading_core::protocol::OrderEvent,
        account: &str,
        account_id: Option<u64>,
    ) {
        use trading_models::InstrumentType;
        // 前置守卫：通道未配置 / 状态不播报，都在解析渲染之前直接跳过。
        let Some(url) = self.channels.target_url(NotifyChannel::Position) else {
            tracing::debug!(account, "position Webhook 未配置，通知A静默跳过");
            return;
        };
        let Some(state) = message::deribit_state_from_wire(&ev.status) else {
            tracing::debug!(account, status = %ev.status, "通知A：状态不播报，跳过");
            return;
        };
        // 2) 订单方向 / reduce_only / 委托量 / 累计成交量。
        let direction = if ev.direction.as_deref() == Some("sell") {
            trading_models::Side::Sell
        } else {
            trading_models::Side::Buy
        };
        let reduce_only = ev.reduce_only;
        let amount = ev.amount.unwrap_or(Decimal::ZERO);
        let filled = ev.filled_quantity.unwrap_or(Decimal::ZERO);
        let is_filled = matches!(state, message::DeribitState::Filled);

        // 3) 数量/价格：成交切换为成交量/成交均价，挂单用委托量/委托价。
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

        // 4) ROI/max_roi：按 instrument 查活跃仓位（account_id 缺省或查不到则 0，隐藏 ROI 段）。
        let (roi, max_roi) = account_id
            .and_then(|id| {
                self.positions.find_active_instrument(
                    id,
                    "deribit",
                    InstrumentType::Option,
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
        let content = message::render_deribit_order_status(&input);
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
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let bodies = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let bodies2 = std::sync::Arc::clone(&bodies);
        std::thread::spawn(move || {
            for stream in listener.incoming().take(1) {
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
        )
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
