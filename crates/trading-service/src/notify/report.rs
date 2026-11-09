//! 仓位定时播报与告警（**纯通知、无业务操作**）。
//!
//! 每 1h 三个动作，全部发 [`NotifyChannel::Position`] 通道：
//! 1. **播报 C**：按交易所档案取数——交易所快照账户经注册表拉 `get_positions`，
//!    本地仓位账户（CTP，持仓查询无实时 mark）直接读已重估的活跃仓位——
//!    按账户渲染一条 markdown；
//! 2. **告警 1**：档案登记阈值的交易所，活跃仓位 `roi <= 阈值`（默认 -0.20）；
//! 3. **告警 2**：档案登记阈值的交易所，按「账户 × 标的」查是否存在 active 的
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
use trading_store::{AccountBrief, AccountManager};

use super::channel::NotifyChannel;
use super::message::{
    render_no_rule_positions, render_position_report, render_roi_alert, LeverageDisplay,
    NoRuleItem, PositionReportItem,
};
use super::profile::{NotifyProfiles, ReportLeverageLine, ReportSource};
use super::NotifyHub;

/// 默认 ROI 告警阈值：-20%（比率口径）。
pub fn default_roi_alert_threshold() -> Decimal {
    Decimal::new(-20, 2)
}

/// 告警分组：`(account_id, exchange)` → 活跃仓位引用（BTreeMap 稳定排序）。
type AlertGroups<'a> = BTreeMap<(u64, String), Vec<&'a trading_models::Position>>;

/// 告警 1 过滤 + 分组（纯函数）：仅档案带阈值的交易所参与，`roi <= threshold`
/// 进入；按 `(account_id, exchange)` 分组，BTreeMap 保证稳定顺序。
///
/// 注意：`position.roi` 统一为保证金口径（pnl/占用保证金），各所语义一致。
fn roi_alert_groups<'a>(
    active: &'a [trading_models::Position],
    profiles: &NotifyProfiles,
) -> AlertGroups<'a> {
    let mut groups = AlertGroups::new();
    for p in active {
        let Some(threshold) = profiles.get(&p.exchange).alert_threshold else {
            continue;
        };
        // 严格大于才跳过：恰好等于阈值也要告警。
        if p.roi > threshold {
            continue;
        }
        groups
            .entry((p.account_id, p.exchange.clone()))
            .or_default()
            .push(p);
    }
    groups
}

/// 告警 2 过滤 + 分组（纯函数）：仅档案参与告警的交易所（有阈值）；
/// 已由 active risk 规则覆盖的 `(account_id, instrument)` 跳过；
/// 同账户同交易所同标的只保留一条。
fn no_rule_alert_groups<'a>(
    active: &'a [trading_models::Position],
    covered: &HashSet<(u64, String)>,
    profiles: &NotifyProfiles,
) -> AlertGroups<'a> {
    let mut groups = AlertGroups::new();
    for p in active {
        // 是否参与告警统一由档案决定（当前以「登记阈值」为参与标志）。
        if profiles.get(&p.exchange).alert_threshold.is_none() {
            continue;
        }
        if covered.contains(&(p.account_id, p.instrument.clone())) {
            continue;
        }
        let items = groups
            .entry((p.account_id, p.exchange.clone()))
            .or_default();
        if !items.iter().any(|i| i.instrument == p.instrument) {
            items.push(p);
        }
    }
    groups
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
}

impl PositionReporter {
    pub fn new(
        hub: NotifyHub,
        registry: Arc<ExchangeRegistry>,
        accounts: AccountManager,
        positions: PositionRepo,
        rules: RuleRepo,
    ) -> Self {
        Self {
            hub,
            registry,
            accounts,
            positions,
            rules,
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

    /// 遍历活动账户，按档案选择取数来源并播报；单账户失败只 warn，不中断其余。
    async fn report_all_accounts(&self) {
        let Some(url) = self.hub.channels.target_url(NotifyChannel::Position) else {
            return;
        };
        // 本轮本地仓位快照：多 CTP 账户共用，避免逐账户整表克隆，也保证同一轮口径一致。
        let active = self.positions.list_active();
        for brief in self.accounts.list_active_briefs() {
            let profile = self.hub.profile(&brief.exchange);
            match profile.report_source {
                ReportSource::ExchangeSnapshot => self.report_exchange_snapshot(url, &brief).await,
                ReportSource::LocalPositions => {
                    self.report_local_positions(url, &brief, profile.report_leverage, &active)
                }
            }
        }
    }

    /// 交易所快照播报：逐产品拉取只读持仓（deribit/binance 现状）。
    async fn report_exchange_snapshot(&self, url: &str, brief: &AccountBrief) {
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

    /// 本地仓位播报：CTP 持仓查询无实时 mark，走已由行情驱动重估的活跃仓位。
    fn report_local_positions(
        &self,
        url: &str,
        brief: &AccountBrief,
        leverage_line: ReportLeverageLine,
        active: &[trading_models::Position],
    ) {
        let Some(account_id) = self.accounts.account_id_by_name(&brief.name) else {
            tracing::warn!(account = %brief.name, "仓位播报：账户名查不到 id，跳过");
            return;
        };
        let items = local_report_items(account_id, &brief.exchange, leverage_line, active);
        if items.is_empty() {
            tracing::debug!(account = %brief.name, "仓位播报：无本地持仓，跳过");
            return;
        }
        let content = render_position_report(&brief.name, &brief.exchange, &items);
        self.send(
            url,
            content,
            &format!("{} - {}", brief.name, brief.exchange),
        );
    }

    // ---- 告警 1：档案登记的交易所 ROI 低于阈值 ----

    /// 本地活跃仓位按档案阈值过滤，按 `(账户, 交易所)` 聚合渲染告警。
    fn alert_roi_below_threshold(&self) {
        let Some(url) = self.hub.channels.target_url(NotifyChannel::Position) else {
            return;
        };
        let active = self.positions.list_active();
        for ((account_id, exchange), positions) in roi_alert_groups(&active, &self.hub.profiles) {
            let account = self.account_name(account_id);
            let leverage_line = self.hub.profile(&exchange).report_leverage;
            let items: Vec<PositionReportItem> = positions
                .iter()
                .map(|p| report_item_from_local(p, leverage_line))
                .collect();
            let content = render_roi_alert(&account, &exchange, &items);
            self.send(url, content, &format!("告警仓位({account} - {exchange})"));
        }
    }

    // ---- 告警 2：档案登记的交易所未设置 active risk 规则的仓位 ----

    /// 按「账户 × 标的」维度：活跃仓位若不存在任何 active 的 risk 类型规则，
    /// 则播报（同标的多仓位只报一次）；交易所范围由档案决定。
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

        // 过滤与「同标的去重」均只依据合约名，不改变筛选/聚合口径。
        let active = self.positions.list_active();
        for ((account_id, exchange), positions) in
            no_rule_alert_groups(&active, &covered, &self.hub.profiles)
        {
            let account = self.account_name(account_id);
            let items: Vec<NoRuleItem> = positions
                .iter()
                .map(|p| NoRuleItem {
                    instrument: &p.instrument,
                    roi: p.roi,
                })
                .collect();
            let content = render_no_rule_positions(&account, &exchange, &items);
            self.send(
                url,
                content,
                &format!("未设置盯盘规则仓位({account} - {exchange})"),
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
        // 交易所快照口径与改造前一致：仅 >0 显示「杠杆倍数」。
        leverage: (leverage > 0).then_some(LeverageDisplay::Multiple(leverage)),
        pnl: p.unrealized_pnl.unwrap_or(Decimal::ZERO),
        roi: calc_roi(p.entry_price, market_price, leverage, side),
    })
}

/// 本地活跃仓位 → 播报条目：ROI 直接取本地行情驱动重估值（与告警同源）。
fn report_item_from_local(
    p: &trading_models::Position,
    leverage_line: ReportLeverageLine,
) -> PositionReportItem<'_> {
    PositionReportItem {
        instrument: &p.instrument,
        side: p.side,
        market_price: p.current_price,
        entry_price: p.pos_price,
        quantity: p.quantity,
        leverage: leverage_display(p, leverage_line),
        pnl: p.pnl_value,
        roi: p.roi,
    }
}

/// 按档案选择杠杆行：交易所倍数 / 保证金比例（比率 = margin / 名义价值）。
///
/// 分母（`pos_price × quantity × multiplier`）≤0 或 margin ≤0 时返回 None，
/// 渲染层省略该行，绝不除零。
fn leverage_display(
    p: &trading_models::Position,
    line: ReportLeverageLine,
) -> Option<LeverageDisplay> {
    match line {
        ReportLeverageLine::ExchangeMultiple => {
            (p.leverage > 0).then_some(LeverageDisplay::Multiple(p.leverage))
        }
        ReportLeverageLine::MarginRatio => {
            if p.margin <= Decimal::ZERO {
                return None;
            }
            let notional = p
                .pos_price
                .checked_mul(p.quantity)
                .and_then(|v| v.checked_mul(p.multiplier))
                .unwrap_or(Decimal::ZERO);
            if notional <= Decimal::ZERO {
                return None;
            }
            p.margin
                .checked_div(notional)
                .map(LeverageDisplay::MarginRatio)
        }
    }
}

/// 本地活跃仓位 → 指定账户 + 交易所的播报条目（CTP 等无实时 mark 的所）。
fn local_report_items<'a>(
    account_id: u64,
    exchange: &str,
    leverage_line: ReportLeverageLine,
    active: &'a [trading_models::Position],
) -> Vec<PositionReportItem<'a>> {
    active
        .iter()
        .filter(|p| p.account_id == account_id && p.exchange == exchange)
        .map(|p| report_item_from_local(p, leverage_line))
        .collect()
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

    // ---- Task 3：本地仓位播报（CTP）----

    /// 构造一个本地合约仓位（成本/现价/保证金/杠杆可调）。
    fn position(exchange: &str, instrument: &str, account_id: u64) -> trading_models::Position {
        let mut p = trading_models::Position::new(account_id, exchange, instrument, Side::Buy);
        p.instrument_type = trading_models::InstrumentType::Futures;
        p.pos_price = dec("4000");
        p.current_price = dec("4100");
        p.quantity = dec("1");
        p.multiplier = dec("10");
        p.margin = dec("4000");
        p.leverage = 10;
        p.pnl_value = dec("100");
        p.roi = dec("0.025");
        p
    }

    #[test]
    fn local_report_items_filters_by_account_and_exchange() {
        // 只取目标账户 + 目标交易所的活跃仓位。
        let active = vec![
            position("ctp", "rb2612", 1),
            position("ctp", "rb2612", 2),
            position("binance", "BTCUSDT", 1),
        ];
        let items = local_report_items(1, "ctp", ReportLeverageLine::MarginRatio, &active);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].instrument, "rb2612");
    }

    #[test]
    fn local_report_items_margin_ratio_uses_margin_over_notional() {
        // 保证金 4000 / 名义 4000×1×10=40000 = 10%。
        let active = vec![position("ctp", "rb2612", 1)];
        let items = local_report_items(1, "ctp", ReportLeverageLine::MarginRatio, &active);
        assert_eq!(
            items[0].leverage,
            Some(crate::notify::message::LeverageDisplay::MarginRatio(dec(
                "0.1"
            )))
        );
    }

    #[test]
    fn local_report_items_margin_ratio_omitted_when_denominator_or_margin_zero() {
        // 分母 ≤0 或 margin ≤0：省略该行（None），不产生除零/荒谬比例。
        let mut no_margin = position("ctp", "rb2612", 1);
        no_margin.margin = Decimal::ZERO;
        let active = [no_margin];
        let items = local_report_items(1, "ctp", ReportLeverageLine::MarginRatio, &active);
        assert_eq!(items[0].leverage, None);

        let mut no_price = position("ctp", "rb2612", 1);
        no_price.pos_price = Decimal::ZERO;
        let active = [no_price];
        let items = local_report_items(1, "ctp", ReportLeverageLine::MarginRatio, &active);
        assert_eq!(items[0].leverage, None);
    }

    #[test]
    fn local_report_items_exchange_multiple_preserves_leverage_line() {
        let mut p = position("ctp", "rb2612", 1);
        p.leverage = 7;
        let active = [p];
        let items = local_report_items(1, "ctp", ReportLeverageLine::ExchangeMultiple, &active);
        assert_eq!(
            items[0].leverage,
            Some(crate::notify::message::LeverageDisplay::Multiple(7))
        );

        // 杠杆 0（无杠杆）：省略行。
        let mut zero = position("ctp", "rb2612", 1);
        zero.leverage = 0;
        let active = [zero];
        let items = local_report_items(1, "ctp", ReportLeverageLine::ExchangeMultiple, &active);
        assert_eq!(items[0].leverage, None);
    }

    // ---- Task 4：档案驱动告警（ROI 阈值 / 未设规则）----

    fn profiles(deribit: &str, ctp: &str) -> super::super::profile::NotifyProfiles {
        super::super::profile::NotifyProfiles::from_config(dec(deribit), dec(ctp))
    }

    fn pos_with_roi(exchange: &str, instrument: &str, roi: &str) -> trading_models::Position {
        let mut p = position(exchange, instrument, 1);
        p.roi = dec(roi);
        p
    }

    #[test]
    fn roi_alert_groups_uses_per_venue_threshold_and_groups_by_exchange() {
        // deribit/ctp 各 1 条越阈值；ctp 未越阈值与未登记交易所（binance）均不出现。
        let active = vec![
            pos_with_roi("deribit", "BTC-P", "-0.25"),
            pos_with_roi("ctp", "rb2612", "-0.25"),
            pos_with_roi("ctp", "rb2701", "-0.05"),
            pos_with_roi("binance", "BTCUSDT", "-0.90"),
        ];
        let groups = roi_alert_groups(&active, &profiles("-0.20", "-0.20"));

        assert_eq!(groups.len(), 2, "实际: {groups:?}");
        assert_eq!(groups[&(1u64, "deribit".to_string())].len(), 1);
        let ctp = &groups[&(1u64, "ctp".to_string())];
        assert_eq!(ctp.len(), 1);
        assert_eq!(ctp[0].instrument, "rb2612");
    }

    #[test]
    fn roi_alert_groups_thresholds_are_independent() {
        // CTP roi=-0.30：阈值 -0.20 时告警，阈值 -0.50 时不告警（deribit 阈值不影响）。
        let active = vec![pos_with_roi("ctp", "rb2612", "-0.30")];
        assert!(roi_alert_groups(&active, &profiles("-0.20", "-0.50")).is_empty());
        assert_eq!(
            roi_alert_groups(&active, &profiles("-0.20", "-0.20")).len(),
            1
        );

        // deribit 同理：roi=-0.15 在阈值 -0.10 时告警、-0.20 时不告警。
        let active = vec![pos_with_roi("deribit", "BTC-P", "-0.15")];
        assert_eq!(
            roi_alert_groups(&active, &profiles("-0.10", "-0.20")).len(),
            1
        );
        assert!(roi_alert_groups(&active, &profiles("-0.20", "-0.20")).is_empty());
    }

    #[test]
    fn roi_alert_groups_boundary_equal_threshold_alerts() {
        // 语义为 roi <= 阈值（严格大于才跳过）：恰好等于阈值也告警。
        let active = vec![pos_with_roi("ctp", "rb2612", "-0.20")];
        let groups = roi_alert_groups(&active, &profiles("-0.20", "-0.20"));
        assert_eq!(groups[&(1u64, "ctp".to_string())].len(), 1);
    }

    #[test]
    fn no_rule_alert_groups_filters_by_alert_profile_and_dedups_instrument() {
        // deribit 已被规则覆盖 → 跳过；binance 未登记 → 跳过；
        // ctp 同账户同标的两条仓位 → 只报一次。
        let mut covered = HashSet::new();
        covered.insert((1u64, "BTC-P".to_string()));
        let active = vec![
            position("deribit", "BTC-P", 1),
            position("binance", "BTCUSDT", 1),
            position("ctp", "rb2612", 1),
            position("ctp", "rb2612", 1),
        ];
        let groups = no_rule_alert_groups(&active, &covered, &profiles("-0.20", "-0.20"));

        assert_eq!(groups.len(), 1, "实际: {groups:?}");
        let ctp = &groups[&(1u64, "ctp".to_string())];
        assert_eq!(ctp.len(), 1);
        assert_eq!(ctp[0].instrument, "rb2612");
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
