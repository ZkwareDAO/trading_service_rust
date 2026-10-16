//! 仓位定时播报与告警（**纯通知、无业务操作**）。
//!
//! 每 1h 三个动作，全部发 [`NotifyChannel::Position`] 通道：
//! 1. **播报 C**：遍历账户 → 经注册表构建 [`ExchangeApi`] 拉取归一化持仓
//!    （`get_positions`，只读、与业务链共用同一口径）→ 按账户渲染一条 markdown；
//! 2. **告警 1**：本地活跃仓位过滤 deribit 且 `roi <= 阈值`（默认 -0.20）；
//! 3. **告警 2**：本地活跃仓位过滤 deribit，按「账户 × 标的」查是否存在 active 的
//!    risk 类型规则，未覆盖者播报。
//!
//! 与 [`super::NotifyHub`] 同风格：fire-and-forget，任何失败只 `warn!`，
//! 绝不影响交易路径；URL 未配置则整轮静默（零外联）。

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use rust_decimal::Decimal;
use trading_exchange::api::{OrderPositionSide, PositionInfo};
use trading_exchange::registry::ExchangeRegistry;
use trading_models::Side;
use trading_store::positions::PositionRepo;
use trading_store::rules::RuleRepo;
use trading_store::AccountManager;

use super::channel::NotifyChannel;
use super::message::{
    render_no_rule_positions, render_position_report, render_roi_alert, NoRuleItem,
    PositionReportItem,
};
use super::NotifyHub;

/// 默认 ROI 告警阈值：-20%（比率口径）。
pub fn default_roi_alert_threshold() -> Decimal {
    Decimal::new(-20, 2)
}

/// 参与本地告警（ROI 阈值 / 未设规则仓位）的交易所集合；播报 C 不受此限（全所播报）。
/// 扩充只需在此登记交易所名。
/// 注意：`position.roi` 为保证金口径（含杠杆），同一阈值在不同所语义不同
/// （合约含杠杆放大），扩充时需按所重新定标。
const ALERT_EXCHANGES: &[&str] = &["deribit"];

/// 该交易所是否参与本地告警。
fn is_alert_exchange(exchange: &str) -> bool {
    ALERT_EXCHANGES.contains(&exchange)
}

/// 距下一个整点的毫秒数（整点对齐用）：如 10:30:00 → 30 分钟。
/// 恰在整点（余 0）时按「下一整点」处理，返回整 1h，避免 sleep(0) 与
/// interval 首 tick 重叠成连发两轮。
fn millis_until_next_hour(now_ms: i64) -> u64 {
    const HOUR_MS: i64 = 3_600_000;
    let into_hour = now_ms.rem_euclid(HOUR_MS);
    let delta = if into_hour == 0 {
        HOUR_MS
    } else {
        HOUR_MS - into_hour
    };
    delta as u64
}

/// 仓位播报 / 告警依赖（均为廉价 clone 句柄）。
#[derive(Clone)]
pub struct PositionReporter {
    hub: NotifyHub,
    registry: Arc<ExchangeRegistry>,
    accounts: AccountManager,
    positions: PositionRepo,
    rules: RuleRepo,
    /// deribit ROI 告警阈值（比率口径，如 -0.20）。
    roi_threshold: Decimal,
}

impl PositionReporter {
    pub fn new(
        hub: NotifyHub,
        registry: Arc<ExchangeRegistry>,
        accounts: AccountManager,
        positions: PositionRepo,
        rules: RuleRepo,
        roi_threshold: Decimal,
    ) -> Self {
        Self {
            hub,
            registry,
            accounts,
            positions,
            rules,
            roi_threshold,
        }
    }

    /// 启动 1h 定时任务；**整点对齐**（对齐 Go 版 crontab 语义）：
    /// 先睡到下一个整点再跑首轮，此后每小时整点一轮（重启不漂移）。
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let wait = millis_until_next_hour(chrono::Utc::now().timestamp_millis());
            tokio::time::sleep(Duration::from_millis(wait)).await;
            let mut ticker = tokio::time::interval(Duration::from_secs(3600));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                self.run_once().await;
            }
        })
    }

    /// 单轮：播报 C（逐账户拉交易所）+ 告警 1/2（本地内存过滤）。
    async fn run_once(&self) {
        self.report_all_accounts().await;
        self.alert_roi_below_threshold();
        self.alert_positions_without_rule();
    }

    // ---- 播报 C：逐账户拉取交易所真实持仓 ----

    /// 遍历活动账户，逐账户拉取并播报；单账户失败只 warn，不中断其余账户。
    async fn report_all_accounts(&self) {
        let Some(url) = self.hub.channels.target_url(NotifyChannel::Position) else {
            return;
        };
        for brief in self.accounts.list_active_briefs() {
            // 各所 build_api 按产品路由：binance/HL 仅 futures、deribit 仅 option。
            // 播报要覆盖多产品账户，逐产品尝试（不支持的所/产品报错即跳过）。
            let products: &[&str] = match brief.exchange.as_str() {
                "deribit" => &["option"],
                // binance/HL 当前仅永续；后续接入现货在此追加。
                _ => &["futures"],
            };
            for product in products {
                // 单账户/单产品失败只 warn，不中断其余。
                let Ok(api) = self
                    .registry
                    .connect(&brief.exchange, Some(product), &brief.name)
                else {
                    tracing::warn!(account = %brief.name, exchange = %brief.exchange, product, "仓位播报：构建交易所主体失败，跳过该产品");
                    continue;
                };
                match api.get_positions().await {
                    Ok(list) if !list.is_empty() => {
                        let items: Vec<PositionReportItem> =
                            list.iter().filter_map(report_item_from_info).collect();
                        if items.is_empty() {
                            continue;
                        }
                        let content = render_position_report(&brief.name, &brief.exchange, &items);
                        self.send(
                            url,
                            content,
                            &format!("{} - {}", brief.name, brief.exchange),
                        );
                    }
                    Ok(_) => tracing::debug!(account = %brief.name, "仓位播报：无持仓，跳过"),
                    Err(e) => {
                        tracing::warn!(account = %brief.name, exchange = %brief.exchange, error = %e, "仓位播报：拉取持仓失败，跳过该账户");
                    }
                }
            }
        }
    }

    // ---- 告警 1：deribit ROI 低于阈值 ----

    /// 本地活跃仓位过滤告警交易所 + roi 阈值，按账户聚合渲染告警。
    fn alert_roi_below_threshold(&self) {
        let Some(url) = self.hub.channels.target_url(NotifyChannel::Position) else {
            return;
        };
        let threshold = self.roi_threshold;
        // BTreeMap 按账户稳定排序，便于阅读与断言。
        let mut by_account: BTreeMap<u64, Vec<PositionReportItem>> = BTreeMap::new();
        let active = self.positions.list_active();
        for p in &active {
            if !is_alert_exchange(&p.exchange) || p.roi > threshold {
                continue;
            }
            let item = report_item_from_local(p);
            by_account.entry(p.account_id).or_default().push(item);
        }
        for (account_id, items) in by_account {
            let account = self.account_name(account_id);
            let content = render_roi_alert(&account, "deribit", &items);
            self.send(url, content, &format!("告警仓位({account} - deribit)"));
        }
    }

    // ---- 告警 2：deribit 未设置 active risk 规则的仓位 ----

    /// 按「账户 × 标的」维度：活跃 deribit 仓位若不存在任何 active 的 risk 类型
    /// 规则，则播报（同标的多仓位只报一次）。
    fn alert_positions_without_rule(&self) {
        let Some(url) = self.hub.channels.target_url(NotifyChannel::Position) else {
            return;
        };
        // active risk 规则索引：(account_id, instrument)。
        let covered: HashSet<(u64, String)> = self
            .rules
            .list_active()
            .into_iter()
            .filter(|r| r.rule_type == trading_models::WatchRuleType::Risk)
            .map(|r| (r.account_id, r.instrument))
            .collect();

        // 按账户聚合渲染条目（借用本轮 active 快照）：过滤与「同标的去重」
        // 判定均只依据合约名，不改变筛选/聚合口径。
        let active = self.positions.list_active();
        let mut by_account: BTreeMap<u64, Vec<NoRuleItem>> = BTreeMap::new();
        for p in &active {
            if !is_alert_exchange(&p.exchange)
                || covered.contains(&(p.account_id, p.instrument.clone()))
            {
                continue;
            }
            // 同标的多仓位只报一次。
            let items = by_account.entry(p.account_id).or_default();
            if !items.iter().any(|i| i.instrument == p.instrument) {
                items.push(NoRuleItem {
                    instrument: &p.instrument,
                    roi: p.roi,
                });
            }
        }
        for (account_id, items) in by_account {
            let account = self.account_name(account_id);
            let content = render_no_rule_positions(&account, "deribit", &items);
            self.send(
                url,
                content,
                &format!("未设置盯盘规则仓位({account} - deribit)"),
            );
        }
    }

    /// 账户 id → 名称（查不到降级为 id 字符串）。
    fn account_name(&self, account_id: u64) -> String {
        self.accounts
            .account_by_id(account_id)
            .map(|b| b.name)
            .unwrap_or_else(|| account_id.to_string())
    }

    /// fire-and-forget 分段发送；失败只 `warn!`（NotifyHub 内部消化）。
    fn send(&self, url: &str, content: String, label: &str) {
        let hub = self.hub.clone();
        let url = url.to_string();
        let label = label.to_string();
        tokio::spawn(async move {
            hub.post_markdown_chunked(&url, &content, &label).await;
        });
    }
}

// =====================================================================
// 归一化：交易所快照 / 本地仓位 → 播报条目
// =====================================================================

/// ROI 现算：`(mark-entry)/entry × 杠杆(0按1)`，空单反向；entry=0 安全返回 0。
fn calc_roi(entry: Decimal, mark: Decimal, leverage: u64, side: Side) -> Decimal {
    if entry.is_zero() || mark.is_zero() {
        return Decimal::ZERO;
    }
    let leverage = if leverage == 0 { 1 } else { leverage };
    let price_ratio = if side == Side::Sell {
        (entry - mark) / entry
    } else {
        (mark - entry) / entry
    };
    price_ratio * Decimal::from(leverage)
}

/// 带符号数量 + 持仓侧 → 仓位方向（数量恒正；单向模式负数量=空单）。
fn side_of(signed: Decimal, position_side: OrderPositionSide) -> Side {
    let is_short = matches!(position_side, OrderPositionSide::Short) || signed < Decimal::ZERO;
    if is_short {
        Side::Sell
    } else {
        Side::Buy
    }
}

/// 交易所快照 → 播报条目：数量恒正、ROI 现算（Go reporter 口径）。
fn report_item_from_info(p: &PositionInfo) -> Option<PositionReportItem<'_>> {
    let side = side_of(p.signed_quantity, p.position_side);
    let market_price = p.mark_price?;
    let leverage = p.leverage.unwrap_or(0);
    Some(PositionReportItem {
        instrument: &p.instrument,
        side,
        market_price,
        entry_price: p.entry_price,
        quantity: p.signed_quantity.abs(),
        leverage,
        pnl: p.unrealized_pnl.unwrap_or(Decimal::ZERO),
        roi: calc_roi(p.entry_price, market_price, leverage, side),
    })
}

/// 本地活跃仓位 → 播报条目：ROI 直接取本地行情驱动重估值（与告警同源）。
fn report_item_from_local(p: &trading_models::Position) -> PositionReportItem<'_> {
    PositionReportItem {
        instrument: &p.instrument,
        side: p.side,
        market_price: p.current_price,
        entry_price: p.pos_price,
        quantity: p.quantity,
        leverage: p.leverage,
        pnl: p.pnl_value,
        roi: p.roi,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    // ---- 纯函数：side_of / calc_roi / 整点对齐 ----

    #[test]
    fn millis_until_next_hour_aligns_to_wall_clock_hour() {
        // 10:30:00.000 → 30 分钟。
        assert_eq!(
            millis_until_next_hour(3_600_000 * 10 + 1_800_000),
            1_800_000
        );
        // 10:59:59.999 → 1ms。
        assert_eq!(millis_until_next_hour(3_600_000 * 11 - 1), 1);
        // 恰在整点：返回整 1h（避免 sleep(0) 与首 tick 连发两轮）。
        assert_eq!(millis_until_next_hour(3_600_000 * 10), 3_600_000);
        // 跨天/毫秒带小数（timestamp_millis 含余数）。
        assert_eq!(
            millis_until_next_hour(3_600_000 * 25 + 1_234),
            3_600_000 - 1_234
        );
    }

    #[test]
    fn side_of_maps_signed_qty_and_position_side() {
        // 单向负数量 = 空单；hedge SHORT = 空单；正数量 = 多单。
        assert_eq!(side_of(dec("-1"), OrderPositionSide::Both), Side::Sell);
        assert_eq!(side_of(dec("1"), OrderPositionSide::Both), Side::Buy);
        assert_eq!(side_of(dec("1"), OrderPositionSide::Short), Side::Sell);
        assert_eq!(side_of(dec("1"), OrderPositionSide::Long), Side::Buy);
    }

    #[test]
    fn calc_roi_long_short_and_leverage_one_fallback() {
        // 多单：(mark-entry)/entry × 杠杆。
        assert_eq!(calc_roi(dec("100"), dec("110"), 10, Side::Buy), dec("1"));
        // 空单反向：(entry-mark)/entry。
        assert_eq!(calc_roi(dec("100"), dec("90"), 10, Side::Sell), dec("1"));
        // 期权杠杆 0 按 1 倍。
        assert_eq!(calc_roi(dec("0.1"), dec("0.11"), 0, Side::Buy), dec("0.1"));
        // entry=0 安全 0。
        assert_eq!(calc_roi(dec("0"), dec("1"), 1, Side::Buy), Decimal::ZERO);
    }

    #[test]
    fn report_item_from_info_abs_quantity_and_computed_roi() {
        // Arrange：binance 单向空仓（负数量、无杠杆字段缺省）。
        let info = PositionInfo {
            instrument: "ETHUSDT".to_string(),
            instrument_type: trading_models::InstrumentType::Futures,
            position_side: OrderPositionSide::Both,
            signed_quantity: dec("-0.5"),
            entry_price: dec("3000"),
            mark_price: Some(dec("2970")),
            unrealized_pnl: Some(dec("15")),
            leverage: Some(5),
            multiplier: None,
            notional: None,
            margin: None,
            update_ms: None,
        };
        // Act & Assert：数量取绝对值、方向空单、ROI=(3000-2970)/3000×5=0.05。
        let item = report_item_from_info(&info).unwrap();
        assert_eq!(item.side, Side::Sell);
        assert_eq!(item.quantity, dec("0.5"));
        assert_eq!(item.roi, dec("0.05"));
    }
}
