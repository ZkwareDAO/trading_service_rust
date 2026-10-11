//! 企业微信通知消息渲染（**纯函数、零 IO**）。
//!
//! 消息均为企业微信群机器人 markdown；本模块只负责把成交订单渲染成 markdown 文本，
//! 发送与成功判定（[`wecom_ok`]）的 HTTP 细节在上层 [`super::NotifyHub`]。

use rust_decimal::Decimal;
use serde::Deserialize;
use trading_models::{InstrumentType, Order, Side};

/// 价格 / 数量 / 盈亏值的固定小数位。
const MONEY_SCALE: u32 = 6;
/// 盈亏百分比的固定小数位（百分比已 ×100）。
const PCT_SCALE: u32 = 4;

/// 开仓事件名（**下游契约，禁止修改**）：现货 SpotOrder、期权 OptionOrder，
/// 其余（含未知品类）一律默认 FutureOrder。
fn open_event_name(t: InstrumentType) -> &'static str {
    match t {
        InstrumentType::Spot => "SpotOrder",
        InstrumentType::Option => "OptionOrder",
        _ => "FutureOrder",
    }
}

/// 市场类型中文展示：现货 / 合约 / 期权；股票等不可展示品类返回 None（类型行省略）。
fn market_type_label(t: InstrumentType) -> Option<&'static str> {
    match t {
        InstrumentType::Spot => Some("现货"),
        InstrumentType::Futures | InstrumentType::Perpetual => Some("合约"),
        InstrumentType::Option => Some("期权"),
        InstrumentType::Equity | InstrumentType::Fund => None,
    }
}

/// 开仓方向文案：买入开多、卖出开空。
fn open_direction(side: Side) -> &'static str {
    match side {
        Side::Buy => "开多挂单",
        Side::Sell => "开空挂单",
    }
}

/// 平仓方向文案（按订单方向反推仓位）：Sell 平多、Buy 平空。
fn close_direction(side: Side) -> &'static str {
    match side {
        Side::Sell => "平多挂单",
        Side::Buy => "平空挂单",
    }
}

/// 定点格式化：四舍五入到 `scale` 位并补齐尾随零（不经过 f64）。
///
/// 注意：Decimal 的 `format!("{:.n}")` 是**截断**而非四舍五入，故必须先
/// [`Decimal::round_dp`] 再格式化补零，否则会出现 1.34338→1.3433 这类少一的误差。
fn fixed(value: Decimal, scale: u32) -> String {
    let rounded = value.round_dp(scale);
    let places = scale as usize;
    format!("{rounded:.places$}")
}

/// 渲染开仓成交通知 markdown。`user_name` 为按账户反查的账户名。
pub fn render_open(order: &Order, user_name: &str) -> String {
    let mut lines = vec![format!(
        "**> {}({})**",
        user_name,
        open_event_name(order.instrument_type)
    )];
    if let Some(label) = market_type_label(order.instrument_type) {
        lines.push(format!("- 类型：{label}"));
    }
    lines.push(format!("- 合约代币：{}", order.instrument));
    lines.push(format!(
        "- 策略名称：{}",
        order.strategy_name.as_deref().unwrap_or_default()
    ));
    lines.push(format!("- 方向：{}", open_direction(order.side)));
    lines.push(format!(
        "- 下单价格：{}",
        fixed(order.filled_price, MONEY_SCALE)
    ));
    lines.push(format!(
        "- 数量：{}",
        fixed(order.filled_quantity, MONEY_SCALE)
    ));
    lines.join("\n")
}

/// 渲染风控平仓成交通知 markdown。
/// `profit`/`profit_ratio` 为按成交均价重算的已实现盈亏值与成本口径比率。
///
/// `drawdown`：仅当该仓位关联规则的 condition_kind 为 profit_drawdown 时传入
/// `Some(position.max_roi)`，此时在盈利百分比下方追加「类型：回落止盈」与
/// 「最大盈利百分比」两行；传入 None（其余规则 / 缺仓位或缺规则）时与原通知一致。
pub fn render_risk_close(
    order: &Order,
    user_name: &str,
    profit: Decimal,
    profit_ratio: Decimal,
    drawdown: Option<Decimal>,
) -> String {
    let mut lines = vec![format!("**> {}(新风控下单)**", user_name)];
    if let Some(label) = market_type_label(order.instrument_type) {
        lines.push(format!("- 类型：{label}"));
    }
    lines.push(format!("- 合约代币：{}", order.instrument));
    lines.push(format!(
        "- 策略名称：{}",
        order.strategy_name.as_deref().unwrap_or_default()
    ));
    lines.push(format!("- 方向：{}", close_direction(order.side)));
    lines.push(format!(
        "- 平仓价格：{}",
        fixed(order.filled_price, MONEY_SCALE)
    ));
    lines.push(format!(
        "- 数量：{}",
        fixed(order.filled_quantity, MONEY_SCALE)
    ));
    lines.push(format!("- 盈利值：{}", fixed(profit, MONEY_SCALE)));
    lines.push(format!(
        "- 盈利百分比：{}%",
        fixed(profit_ratio * Decimal::ONE_HUNDRED, PCT_SCALE)
    ));
    // 回落止盈：在盈利百分比下方追加两行（方向行保持原样）。
    if let Some(max_roi) = drawdown {
        lines.push("- 类型：回落止盈".to_string());
        lines.push(format!(
            "- 最大盈利百分比：{}%",
            fixed(max_roi * Decimal::ONE_HUNDRED, PCT_SCALE)
        ));
    }
    lines.join("\n")
}

/// 企业微信机器人响应体。
#[derive(Debug, Deserialize)]
struct WeComReply {
    errcode: i64,
}

/// 发送成功判定：HTTP 状态码 200 且响应体 `errcode == 0`；
/// 状态码不符或响应体无法解析均视为失败。
pub fn wecom_ok(status: u16, body: &str) -> bool {
    if status != 200 {
        return false;
    }
    serde_json::from_str::<WeComReply>(body)
        .map(|r| r.errcode == 0)
        .unwrap_or(false)
}

// =====================================================================
// 预留骨架（待 Deribit 适配器 / 定时任务接入后填实现）
// =====================================================================
//
// 下列三类消息的**函数签名与输入结构**先行定稿，函数体为 `todo!`；
// 对应的格式测试已写好但以 #[ignore] 挂起，实现时去掉 ignore 即驱动开发。
// 通道语义见 `crates/trading-service/src/notify/channel.rs`（Open/Close/Test/ManualClose/Position 五通道）。

/// Deribit 期权价差告警（需人工平仓）渲染输入。
// 预留脚手架：仅 #[ignore] 测试引用，接入前允许死代码。
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct SpreadAlertInput<'a> {
    /// 合约名（BTC-31JUL26-66000-P）。
    pub instrument: &'a str,
    /// 策略名（当前调用点可能为空）。
    pub strategy_name: &'a str,
    /// 买一价。
    pub best_bid: Decimal,
    /// 卖一价。
    pub best_ask: Decimal,
    /// 买一卖一绝对价差。
    pub spread: Decimal,
    /// 当前 ROI（比率口径；为 0 时消息中省略该行）。
    pub roi: Decimal,
}

/// 渲染 Deribit 期权价差告警（需人工平仓）markdown（docs §四）。
#[allow(dead_code)]
pub fn render_deribit_spread(input: &SpreadAlertInput) -> String {
    let mut lines = vec!["**> 需要手动平仓**".to_string()];
    lines.push(format!("- 合约：{}", input.instrument));
    // 策略名称：传入显示值；未传（空）仍保留标签、值留空（不省略整行）。
    lines.push(format!("- 策略名称：{}", input.strategy_name));
    lines.push(format!("- 买一价：{}", fixed(input.best_bid, MONEY_SCALE)));
    lines.push(format!("- 卖一价：{}", fixed(input.best_ask, MONEY_SCALE)));
    lines.push(format!("- 价差：{}", fixed(input.spread, MONEY_SCALE)));
    if input.roi != Decimal::ZERO {
        lines.push(format!(
            "- ROI：{}%",
            fixed(input.roi * Decimal::ONE_HUNDRED, PCT_SCALE)
        ));
    }
    lines.push(format!(
        "- 说明：买一卖一差距悬殊（{}），需要手动操作",
        fixed(input.spread, MONEY_SCALE)
    ));
    lines.join("\n")
}

/// Deribit 订单状态播报渲染输入（docs §五，单行消息）。
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct DeribitOrderStatusInput<'a> {
    /// 标题中的用户名。
    pub user_name: &'a str,
    /// 合约名。
    pub instrument: &'a str,
    /// 由 [`build_deribit_action`] 按订单要素拼好的动作文案。
    pub action: &'a str,
    /// 数量（挂单=订单量，成交=成交量）。
    pub quantity: Decimal,
    /// 未成交数量（>0 时才出现该行片段）。
    pub remaining: Decimal,
    /// ROI（比率；与 max_roi 任一非 0 才出现该片段）。
    pub roi: Decimal,
    /// 历史最高 ROI（比率）。
    pub max_roi: Decimal,
    /// 价格（挂单=订单价，成交=成交均价）。
    pub price: Decimal,
}

/// Deribit 订单归一状态（播报专用；其余状态不播报）。
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeribitState {
    /// 挂单中（order_state=open）。
    New,
    /// 已成交（含部分成交；部分成交由 filled<amount 区分）。
    Filled,
    /// 已撤单。
    Cancelled,
    /// 失败 / 被拒 / 过期。
    Failed,
}

/// wire 状态串 → 归一状态；无法识别（UNKNOWN 等）返回 None（不播报）。
#[allow(dead_code)]
pub fn deribit_state_from_wire(raw: &str) -> Option<DeribitState> {
    use DeribitState::*;
    match raw.trim().to_ascii_uppercase().as_str() {
        "NEW" => Some(New),
        // 部分成交并入 Filled：是否显示「部分成交」由 filled<amount 判定。
        "FILLED" | "PARTIAL" => Some(Filled),
        "CANCELED" | "CANCELLED" => Some(Cancelled),
        "REJECTED" | "EXPIRED" | "FAILED" => Some(Failed),
        _ => None,
    }
}

/// 按订单要素拼「动作」文案（docs §2.2，纯函数）。
///
/// * 仓位方向：reduce_only 时订单方向**取反**（平买仓下 sell、平卖仓下 buy）；
/// * 前缀：reduce_only→「减」、否则「开」；
/// * 后缀：New→挂单成功；Filled 开仓→成功，减仓全成→成功、filled<amount→部分成交成功；
///   Cancelled→挂单已取消；Failed→挂单失败。
#[allow(dead_code)]
pub fn build_deribit_action(
    direction: Side,
    reduce_only: bool,
    state: DeribitState,
    filled: Decimal,
    amount: Decimal,
) -> String {
    use DeribitState::*;
    // reduce_only 取反得真实仓位方向（XOR）。
    let is_buy_position = matches!(direction, Side::Buy) != reduce_only;
    let position_label = if is_buy_position { "买仓" } else { "卖仓" };
    let open_label = if reduce_only { "减" } else { "开" };
    let suffix = match state {
        New => "挂单成功".to_string(),
        Filled => {
            // 仅减仓且 filled<amount 才是部分成交；开仓无部分成交概念。
            if reduce_only && filled < amount {
                "部分成交成功".to_string()
            } else {
                "成功".to_string()
            }
        }
        Cancelled => "挂单已取消".to_string(),
        Failed => "挂单失败".to_string(),
    };
    format!("{open_label}{position_label}{suffix}")
}

/// 通知 A 数量 / 价格的固定小数位（4 位；期权权利金很小）。
const DERIBIT_QTY_SCALE: u32 = 4;
/// 通知 A ROI 百分比的固定小数位（2 位）。
const DERIBIT_ROI_PCT_SCALE: u32 = 2;

/// Deribit 订单状态播报（docs §五，单行 markdown）。
#[allow(dead_code)]
pub fn render_deribit_order_status(input: &DeribitOrderStatusInput) -> String {
    // 头部 + 「代币 / 动作 / 数量」。
    let mut line = format!(
        "> **DeribitPositions({})**\n代币：{}，{}，数量：{}",
        input.user_name,
        input.instrument,
        input.action,
        fixed(input.quantity, DERIBIT_QTY_SCALE)
    );
    // 未成交数量：仅 >0 显示，片段带全角逗号。
    if input.remaining > Decimal::ZERO {
        line.push_str(&format!(
            "，未成交数量：{}",
            fixed(input.remaining, DERIBIT_QTY_SCALE)
        ));
    }
    // ROI 片段：roi 或 max_roi 任一非 0 才显示，2 位小数。
    if input.roi != Decimal::ZERO || input.max_roi != Decimal::ZERO {
        line.push_str(&format!(
            " (ROI: {}%, Max ROI: {}%)",
            fixed(input.roi * Decimal::ONE_HUNDRED, DERIBIT_ROI_PCT_SCALE),
            fixed(input.max_roi * Decimal::ONE_HUNDRED, DERIBIT_ROI_PCT_SCALE)
        ));
    }
    // 价格：4 位小数；「价格:」前为历史格式的全角逗号 + 空格。
    line.push_str(&format!(
        " ，价格: {}",
        fixed(input.price, DERIBIT_QTY_SCALE)
    ));
    line
}

/// ROI 告警渲染时的小数位（百分比 2 位）。
const REPORT_ROI_PCT_SCALE: u32 = 2;
/// 数量固定小数位（docs §六：代币数量 4 位）。
const REPORT_QTY_SCALE: u32 = 4;
/// 分段发送默认上限（字节）：企微 markdown content 上限 4096，预留安全余量。
pub const CHUNK_MAX_BYTES: usize = 4000;

/// 仓位定时播报中的单条持仓渲染输入（docs §六）。
#[derive(Debug, Clone)]
pub struct PositionReportItem<'a> {
    /// 交易对。
    pub instrument: &'a str,
    /// 持仓方向（多单/空单，决定颜色与文案）。
    pub side: Side,
    /// 市场价（mark）。
    pub market_price: Decimal,
    /// 成本价（entry）。
    pub entry_price: Decimal,
    /// 代币数量。
    pub quantity: Decimal,
    /// 杠杆（0 = 期权无杠杆，省略该行）。
    pub leverage: u64,
    /// 盈亏金额。
    pub pnl: Decimal,
    /// 盈亏比率（比率口径；渲染 ×100，盈利带 +）。
    pub roi: Decimal,
}

/// 企微 `<font>` 着色：盈利/多头 info（绿），亏损/空头 warning（红）。
fn color_font(value: &str, positive: bool) -> String {
    if positive {
        format!("<font color=\"info\">{value}</font>")
    } else {
        format!("<font color=\"warning\">{value}</font>")
    }
}

/// 盈亏比率 → 百分比展示文本：×100 后 2 位小数，正值显式加 `+`（负值自带 `-`）。
/// 仓位播报段落与「未设置盯盘规则仓位」行共用同一口径，避免两处格式漂移。
fn roi_pct_text(roi: Decimal) -> String {
    let positive = roi >= Decimal::ZERO;
    let pct = fixed(roi * Decimal::ONE_HUNDRED, REPORT_ROI_PCT_SCALE);
    if positive {
        format!("+{pct}")
    } else {
        pct
    }
}

/// 渲染单条持仓段落（docs §六）：方向/盈亏着色，杠杆 0 省略行。
///
/// 段落首行只输出合约名（不再附带账户后缀）：账户/交易所信息已由外层
/// [`render_position_report`] / [`render_roi_alert`] 的标题统一承载，避免重复。
fn render_report_item(p: &PositionReportItem) -> String {
    let is_long = p.side == Side::Buy;
    let positive = p.roi >= Decimal::ZERO;
    let direction = color_font(if is_long { "多单" } else { "空单" }, is_long);
    let mut lines = vec![
        format!("**{}**", p.instrument),
        format!("方向: {direction}"),
        format!("市场价: {}", fixed(p.market_price, MONEY_SCALE)),
        format!("成本价: {}", fixed(p.entry_price, MONEY_SCALE)),
        format!("代币数量: {}", fixed(p.quantity, REPORT_QTY_SCALE)),
    ];
    // 杠杆行：仅 >0 输出（期权无杠杆）。
    if p.leverage > 0 {
        lines.push(format!("杠杆倍数: {}", p.leverage));
    }
    // ROI 文本：×100 后 2 位小数，正值显式加 `+`（负值自带 -）。
    let roi_text = roi_pct_text(p.roi);
    lines.push(format!(
        "盈亏金额: {}",
        color_font(&fixed(p.pnl, MONEY_SCALE), positive)
    ));
    lines.push(format!(
        "盈亏百分比: {}",
        color_font(&format!("{roi_text}%"), positive)
    ));
    lines.join("\n")
}

/// 段落列表 → 仓间空一行连接。
fn join_paragraphs(items: impl Iterator<Item = String>) -> String {
    items.collect::<Vec<_>>().join("\n\n")
}

/// 仓位定时播报（docs §六，含 `<font>` 颜色）；仓间空一行。无持仓返回空串。
pub fn render_position_report(
    user_name: &str,
    exchange: &str,
    positions: &[PositionReportItem],
) -> String {
    if positions.is_empty() {
        return String::new();
    }
    let body = join_paragraphs(positions.iter().map(render_report_item));
    format!("## 仓位播报 ({user_name} - {exchange})\n\n{body}")
}

/// ROI 告警播报。
pub fn render_roi_alert(
    user_name: &str,
    exchange: &str,
    positions: &[PositionReportItem],
) -> String {
    if positions.is_empty() {
        return String::new();
    }
    let body = join_paragraphs(positions.iter().map(render_report_item));
    format!("## 告警仓位({user_name} - {exchange})\n\n{body}")
}

/// 未设置盯盘规则仓位的单行渲染输入。
#[derive(Debug, Clone)]
pub struct NoRuleItem<'a> {
    /// 合约名（BTC-25DEC26-85000-C）。
    pub instrument: &'a str,
    /// 盈亏比率（比率口径）：渲染 ×100 显示百分比，并决定整行颜色与 ± 前缀；
    /// 0 与盈利同口径（归绿、带 +）。
    pub roi: Decimal,
}

/// 渲染未设规则仓位单行 `标的(盈亏百分比)`：口径与播报段落
/// [`render_report_item`] 完全一致——百分比 2 位小数、盈利（含 roi==0）
/// 补 `+` 并整行 info 绿、亏损自带 `-` 并整行 warning 红。
fn render_no_rule_line(i: &NoRuleItem) -> String {
    let positive = i.roi >= Decimal::ZERO;
    let line = format!("{}({}%)", i.instrument, roi_pct_text(i.roi));
    color_font(&line, positive)
}

/// 未设置盯盘规则仓位播报：标题与播报/ROI 告警同款（`##` 加粗）并携带
/// `(账户 - 交易所)`；每行 `标的(盈亏百分比)`，口径与播报段落一致——
/// 盈利（含 roi==0）整行 info 绿、亏损整行 warning 红。
pub fn render_no_rule_positions(account: &str, exchange: &str, items: &[NoRuleItem]) -> String {
    if items.is_empty() {
        return String::new();
    }
    let body = items
        .iter()
        .map(render_no_rule_line)
        .collect::<Vec<_>>()
        .join("\n");
    format!("## 未设置盯盘规则仓位({account} - {exchange})\n\n{body}")
}

/// 按 UTF-8 安全边界把内容切成不超过 `max_bytes` 的段：
/// 优先在仓间空行（`\n\n`）处切，段落本身超限时按字符边界硬切（保证 UTF-8 合法）。
pub fn split_chunks(content: &str, max_bytes: usize) -> Vec<String> {
    if content.len() <= max_bytes {
        return vec![content.to_string()];
    }
    let mut chunks = Vec::new();
    let mut current = String::new();
    for part in content.split("\n\n") {
        // 已有内容 + 分隔符 + 新段放不下：先落盘当前段。
        if !current.is_empty() && current.len() + 2 + part.len() > max_bytes {
            chunks.push(std::mem::take(&mut current));
        }
        if !current.is_empty() {
            current.push_str("\n\n");
        }
        if part.len() > max_bytes {
            // 单段超限：按字符边界硬切成多块，每块独立成段。
            for piece in hard_split(part, max_bytes) {
                if !current.is_empty() {
                    chunks.push(std::mem::take(&mut current));
                }
                chunks.push(piece);
            }
        } else {
            current.push_str(part);
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

/// 按字符（非字节）边界硬切，保证 UTF-8 合法。
fn hard_split(part: &str, max_bytes: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut current_bytes = 0;
    for ch in part.chars() {
        let ch_len = ch.len_utf8();
        if current_bytes + ch_len > max_bytes && !current.is_empty() {
            out.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current.push(ch);
        current_bytes += ch_len;
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;
    use trading_models::{InstrumentType, Order, Side};

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    /// 构造一个已成交订单（reduce_only/类型/策略/成交字段可调）。
    #[allow(clippy::too_many_arguments)]
    fn filled_order(
        side: Side,
        reduce_only: bool,
        itype: InstrumentType,
        instrument: &str,
        strategy: Option<&str>,
        price: &str,
        qty: &str,
    ) -> Order {
        let mut o = Order::new(1, "0xclient", "binance", instrument, side);
        o.instrument_type = itype;
        o.reduce_only = reduce_only;
        o.strategy_name = strategy.map(str::to_string);
        o.filled_price = dec(price);
        o.filled_quantity = dec(qty);
        o.order_quantity = dec(qty);
        o
    }

    // ---- 开仓成交通知 ----

    #[test]
    fn render_open_matches_doc_example_verbatim() {
        // 文档 §二 示例逐字断言（合约买入开多，价格/数量 6 位小数尾随零）。
        let order = filled_order(
            Side::Buy,
            false,
            InstrumentType::Futures,
            "BTCUSDT",
            Some("网格策略A"),
            "63250.5",
            "0.125",
        );
        assert_eq!(
            render_open(&order, "follow_prod"),
            "**> follow_prod(FutureOrder)**\n\
             - 类型：合约\n\
             - 合约代币：BTCUSDT\n\
             - 策略名称：网格策略A\n\
             - 方向：开多挂单\n\
             - 下单价格：63250.500000\n\
             - 数量：0.125000"
        );
    }

    #[test]
    fn render_open_spot_uses_spot_event_and_type() {
        // 现货：事件名 SpotOrder、类型 现货。
        let order = filled_order(
            Side::Buy,
            false,
            InstrumentType::Spot,
            "BTCUSDT",
            None,
            "100",
            "1",
        );
        let msg = render_open(&order, "bob");
        assert!(msg.starts_with("**> bob(SpotOrder)**"));
        assert!(msg.contains("- 类型：现货"));
    }

    #[test]
    fn render_open_option_uses_option_event_and_type() {
        let order = filled_order(
            Side::Buy,
            false,
            InstrumentType::Option,
            "BTC-31JUL26-66000-P",
            None,
            "0.01",
            "1",
        );
        let msg = render_open(&order, "bob");
        assert!(msg.starts_with("**> bob(OptionOrder)**"));
        assert!(msg.contains("- 类型：期权"));
    }

    #[test]
    fn render_open_unknown_type_omits_type_line_and_defaults_future_event() {
        // 股票等未知品类：类型行省略，事件名默认 FutureOrder。
        let order = filled_order(
            Side::Buy,
            false,
            InstrumentType::Equity,
            "AAPL",
            None,
            "100",
            "1",
        );
        let msg = render_open(&order, "bob");
        assert!(msg.starts_with("**> bob(FutureOrder)**"));
        assert!(!msg.contains("类型"));
    }

    #[test]
    fn render_open_sell_is_open_short() {
        let order = filled_order(
            Side::Sell,
            false,
            InstrumentType::Futures,
            "BTCUSDT",
            None,
            "100",
            "1",
        );
        assert!(render_open(&order, "bob").contains("- 方向：开空挂单"));
    }

    #[test]
    fn render_open_missing_strategy_renders_empty_name() {
        let order = filled_order(
            Side::Buy,
            false,
            InstrumentType::Futures,
            "BTCUSDT",
            None,
            "100",
            "1",
        );
        assert!(render_open(&order, "bob").contains("- 策略名称："));
    }

    // ---- 风控平仓成交通知 ----

    #[test]
    fn render_risk_close_matches_doc_example_verbatim() {
        // 文档 §三 示例逐字断言（Sell 平多，盈亏值 6 位、百分比×100 后 4 位带 %）。
        let order = filled_order(
            Side::Sell,
            true,
            InstrumentType::Futures,
            "BTCUSDT",
            Some("网格策略A"),
            "64100.2",
            "0.125",
        );
        assert_eq!(
            render_risk_close(
                &order,
                "follow_prod",
                dec("106.2125"),
                dec("0.013432"),
                None
            ),
            "**> follow_prod(新风控下单)**\n\
             - 类型：合约\n\
             - 合约代币：BTCUSDT\n\
             - 策略名称：网格策略A\n\
             - 方向：平多挂单\n\
             - 平仓价格：64100.200000\n\
             - 数量：0.125000\n\
             - 盈利值：106.212500\n\
             - 盈利百分比：1.3432%"
        );
    }

    #[test]
    fn render_risk_close_buy_order_closes_short() {
        // 平空仓下的是 Buy 单 → 方向文案 平空挂单。
        let order = filled_order(
            Side::Buy,
            true,
            InstrumentType::Futures,
            "BTCUSDT",
            None,
            "100",
            "1",
        );
        assert!(
            render_risk_close(&order, "bob", dec("-5"), dec("-0.05"), None)
                .contains("- 方向：平空挂单")
        );
    }

    #[test]
    fn render_risk_close_with_drawdown_appends_two_lines() {
        // condition_kind=profit_drawdown：方向行保持原样，盈利百分比下方追加两行。
        let order = filled_order(
            Side::Sell,
            true,
            InstrumentType::Futures,
            "BNBUSDT",
            Some("sar_supertrend"),
            "763.28",
            "0.26",
        );
        // max_roi=0.1556 → 15.5600%。
        let msg = render_risk_close(
            &order,
            "test_qpw",
            dec("-1.4801"),
            dec("-0.007514"),
            Some(dec("0.1556")),
        );
        assert_eq!(
            msg,
            "**> test_qpw(新风控下单)**\n\
             - 类型：合约\n\
             - 合约代币：BNBUSDT\n\
             - 策略名称：sar_supertrend\n\
             - 方向：平多挂单\n\
             - 平仓价格：763.280000\n\
             - 数量：0.260000\n\
             - 盈利值：-1.480100\n\
             - 盈利百分比：-0.7514%\n\
             - 类型：回落止盈\n\
             - 最大盈利百分比：15.5600%"
        );
    }

    #[test]
    fn render_risk_close_without_drawdown_matches_original() {
        // 非 profit_drawdown：输出与现状逐字一致，不追加任何行。
        let order = filled_order(
            Side::Sell,
            true,
            InstrumentType::Futures,
            "BTCUSDT",
            Some("网格策略A"),
            "64100.2",
            "0.125",
        );
        assert_eq!(
            render_risk_close(
                &order,
                "follow_prod",
                dec("106.2125"),
                dec("0.013432"),
                None
            ),
            "**> follow_prod(新风控下单)**\n\
             - 类型：合约\n\
             - 合约代币：BTCUSDT\n\
             - 策略名称：网格策略A\n\
             - 方向：平多挂单\n\
             - 平仓价格：64100.200000\n\
             - 数量：0.125000\n\
             - 盈利值：106.212500\n\
             - 盈利百分比：1.3432%"
        );
    }

    // ---- 成功判定 ----

    #[test]
    fn wecom_ok_requires_200_and_errcode_zero() {
        assert!(wecom_ok(200, r#"{"errcode":0,"errmsg":""}"#));
        // HTTP 非 200：失败。
        assert!(!wecom_ok(500, r#"{"errcode":0}"#));
        // errcode 非 0：失败。
        assert!(!wecom_ok(
            200,
            r#"{"errcode":93000,"errmsg":"invalid webhook"}"#
        ));
        // 响应体无法解析：失败。
        assert!(!wecom_ok(200, "not json"));
    }

    // 空策略名：标签仍显示、值留空（修复：不再省略整行）。
    #[test]
    fn render_spread_keeps_label_when_strategy_empty() {
        let input = SpreadAlertInput {
            instrument: "BTC-23SEP26-72000-C",
            strategy_name: "",
            best_bid: dec("0.162"),
            best_ask: dec("0.1655"),
            spread: dec("0.0035"),
            roi: Decimal::ZERO,
        };
        let msg = render_deribit_spread(&input);
        assert!(
            msg.lines().any(|l| l == "- 策略名称："),
            "空策略名必须保留标签行，得到:\n{msg}"
        );
        // 标签恰好一次，不出现账户名被误填。
        assert_eq!(msg.matches("- 策略名称：").count(), 1);
    }

    // 传入策略名：显示「- 策略名称：<名>」。
    #[test]
    fn render_spread_shows_strategy_when_provided() {
        let input = SpreadAlertInput {
            instrument: "BTC-23SEP26-72000-C",
            strategy_name: "test_deribit",
            best_bid: dec("0.162"),
            best_ask: dec("0.1655"),
            spread: dec("0.0035"),
            roi: Decimal::ZERO,
        };
        let msg = render_deribit_spread(&input);
        assert!(msg.lines().any(|l| l == "- 策略名称：test_deribit"));
    }

    // ---- 预留 TODO 测试（当前 #[ignore]，实现对应骨架后去掉 ignore 启用）----

    /// docs §四 示例：价差告警含默认标题、买一/卖一/价差、ROI 行与说明。
    #[test]
    #[ignore = "TODO: 待 render_deribit_spread 实现后启用"]
    fn render_deribit_spread_matches_doc_example() {
        let input = SpreadAlertInput {
            instrument: "BTC-31JUL26-66000-P",
            strategy_name: "",
            best_bid: dec("0.0125"),
            best_ask: dec("0.0189"),
            spread: dec("0.0064"),
            // ROI 为比率，12.5% 存 0.125。
            roi: dec("0.125"),
        };
        assert_eq!(
            render_deribit_spread(&input),
            "**> 需要手动平仓**\n\
             - 合约代币：BTC-31JUL26-66000-P\n\
             - 策略名称：\n\
             - 买一价：0.012500\n\
             - 卖一价：0.018900\n\
             - 价差：0.006400\n\
             - 当前roi：12.50%\n\
             - 说明：买一卖一差距悬殊（0.006400），需要手动操作"
        );
    }

    /// docs §四：ROI 为 0 时省略「当前roi」行。
    #[test]
    #[ignore = "TODO: 待 render_deribit_spread 实现后启用"]
    fn render_deribit_spread_omits_roi_line_when_zero() {
        let input = SpreadAlertInput {
            instrument: "BTC-31JUL26-66000-P",
            strategy_name: "",
            best_bid: dec("0.0125"),
            best_ask: dec("0.0189"),
            spread: dec("0.0064"),
            roi: Decimal::ZERO,
        };
        let msg = render_deribit_spread(&input);
        assert!(msg.starts_with("**> 需要手动平仓**"));
        assert!(!msg.contains("当前roi"));
    }

    // ---- 通知 A：DeribitPositions 订单事件播报 ----

    /// 减仓挂单（reduce_only、方向取反）含 ROI 片段；无未成交数量。
    #[test]
    fn render_deribit_order_status_reduce_new_with_roi() {
        let input = DeribitOrderStatusInput {
            user_name: "test_acct",
            instrument: "BTC-2OCT26-84500-P",
            // sell + reduce_only → 取反得卖仓（减卖仓）。
            action: "减卖仓挂单成功",
            quantity: dec("2"),
            remaining: Decimal::ZERO,
            roi: dec("-0.5793"),
            max_roi: dec("0.1556"),
            price: dec("0.0100"),
        };
        assert_eq!(
            render_deribit_order_status(&input),
            "> **DeribitPositions(test_acct)**\n\
             代币：BTC-2OCT26-84500-P，减卖仓挂单成功，数量：2.0000 (ROI: -57.93%, Max ROI: 15.56%) ，价格: 0.0100"
        );
    }

    /// 减仓部分成交：含未成交数量片段。
    #[test]
    fn render_deribit_order_status_partial_with_remaining() {
        let input = DeribitOrderStatusInput {
            user_name: "demo",
            instrument: "BTC-31JUL26-66000-P",
            action: "减买仓部分成交成功",
            quantity: dec("59"),
            remaining: dec("21"),
            roi: dec("0.123"),
            max_roi: dec("0.3"),
            price: dec("0.0013"),
        };
        assert_eq!(
            render_deribit_order_status(&input),
            "> **DeribitPositions(demo)**\n\
             代币：BTC-31JUL26-66000-P，减买仓部分成交成功，数量：59.0000，未成交数量：21.0000 (ROI: 12.30%, Max ROI: 30.00%) ，价格: 0.0013"
        );
    }

    /// 开仓挂单、无未成交、ROI 全 0：对应片段均省略。
    #[test]
    fn render_deribit_order_status_open_omits_fragments() {
        let input = DeribitOrderStatusInput {
            user_name: "demo",
            instrument: "BTC-31JUL26-66000-P",
            action: "开卖仓挂单成功",
            quantity: dec("2"),
            remaining: Decimal::ZERO,
            roi: Decimal::ZERO,
            max_roi: Decimal::ZERO,
            price: dec("0.02"),
        };
        assert_eq!(
            render_deribit_order_status(&input),
            "> **DeribitPositions(demo)**\n\
             代币：BTC-31JUL26-66000-P，开卖仓挂单成功，数量：2.0000 ，价格: 0.0200"
        );
    }

    /// build_deribit_action 完整矩阵（docs §2.2）。
    #[test]
    fn build_deribit_action_matrix() {
        use DeribitState::*;
        // 开仓挂单
        assert_eq!(
            build_deribit_action(Side::Buy, false, New, dec("0"), dec("2")),
            "开买仓挂单成功"
        );
        assert_eq!(
            build_deribit_action(Side::Sell, false, New, dec("0"), dec("2")),
            "开卖仓挂单成功"
        );
        // reduce_only 挂单：方向取反（buy→减卖仓，sell→减买仓）
        assert_eq!(
            build_deribit_action(Side::Buy, true, New, dec("0"), dec("2")),
            "减卖仓挂单成功"
        );
        assert_eq!(
            build_deribit_action(Side::Sell, true, New, dec("0"), dec("2")),
            "减买仓挂单成功"
        );
        // 开仓成交（无部分成交概念）
        assert_eq!(
            build_deribit_action(Side::Buy, false, Filled, dec("2"), dec("2")),
            "开买仓成功"
        );
        assert_eq!(
            build_deribit_action(Side::Sell, false, Filled, dec("2"), dec("2")),
            "开卖仓成功"
        );
        // 减仓全成 / 部分成交
        assert_eq!(
            build_deribit_action(Side::Buy, true, Filled, dec("2"), dec("2")),
            "减卖仓成功"
        );
        assert_eq!(
            build_deribit_action(Side::Sell, true, Filled, dec("2"), dec("2")),
            "减买仓成功"
        );
        assert_eq!(
            build_deribit_action(Side::Buy, true, Filled, dec("59"), dec("80")),
            "减卖仓部分成交成功"
        );
        assert_eq!(
            build_deribit_action(Side::Sell, true, Filled, dec("1"), dec("2")),
            "减买仓部分成交成功"
        );
        // 撤单 / 失败
        assert_eq!(
            build_deribit_action(Side::Buy, false, Cancelled, dec("0"), dec("2")),
            "开买仓挂单已取消"
        );
        assert_eq!(
            build_deribit_action(Side::Sell, true, Cancelled, dec("0"), dec("2")),
            "减买仓挂单已取消"
        );
        assert_eq!(
            build_deribit_action(Side::Buy, false, Failed, dec("0"), dec("2")),
            "开买仓挂单失败"
        );
        assert_eq!(
            build_deribit_action(Side::Sell, true, Failed, dec("0"), dec("2")),
            "减买仓挂单失败"
        );
    }

    /// wire 状态串 → DeribitState 映射；未知/None 跳过。
    #[test]
    fn deribit_state_from_wire_maps_and_rejects() {
        use DeribitState::*;
        assert_eq!(deribit_state_from_wire("NEW"), Some(New));
        assert_eq!(deribit_state_from_wire("FILLED"), Some(Filled));
        assert_eq!(deribit_state_from_wire("CANCELED"), Some(Cancelled));
        assert_eq!(deribit_state_from_wire("REJECTED"), Some(Failed));
        assert_eq!(deribit_state_from_wire("EXPIRED"), Some(Failed));
        assert_eq!(deribit_state_from_wire("PARTIAL"), Some(Filled));
        // 未知 / 空：不播报。
        assert_eq!(deribit_state_from_wire("UNKNOWN"), None);
        assert_eq!(deribit_state_from_wire(""), None);
    }

    /// docs §六 示例：仓位播报标题、颜色标签、杠杆与盈亏格式（多仓）。
    #[test]
    fn render_position_report_matches_doc_example_long() {
        let item = PositionReportItem {
            instrument: "BTCUSDT",
            side: Side::Buy,
            market_price: dec("63980.5"),
            entry_price: dec("63250"),
            quantity: dec("0.125"),
            leverage: 10,
            pnl: dec("91.3125"),
            roi: dec("0.0115"),
        };
        assert_eq!(
            render_position_report("bob", "binance", &[item]),
            "## 仓位播报 (bob - binance)\n\n\
             **BTCUSDT**\n\
             方向: <font color=\"info\">多单</font>\n\
             市场价: 63980.500000\n\
             成本价: 63250.000000\n\
             代币数量: 0.1250\n\
             杠杆倍数: 10\n\
             盈亏金额: <font color=\"info\">91.312500</font>\n\
             盈亏百分比: <font color=\"info\">+1.15%</font>"
        );
    }

    /// docs §六：空仓用 warning 色；杠杆为 0（期权）省略杠杆行。
    #[test]
    fn render_position_report_short_uses_warning_color_and_hides_leverage() {
        let item = PositionReportItem {
            instrument: "ETHUSDT",
            side: Side::Sell,
            market_price: dec("3100.2"),
            entry_price: dec("3120"),
            quantity: dec("1.5"),
            leverage: 0,
            pnl: dec("29.7"),
            roi: dec("0.0095"),
        };
        let msg = render_position_report("bob", "binance", &[item]);
        assert!(msg.contains("<font color=\"warning\">空单</font>"));
        assert!(!msg.contains("杠杆倍数"));
        // 负 ROI：百分比带 -、不重复加 +。
        let neg = PositionReportItem {
            instrument: "ETHUSDT",
            side: Side::Sell,
            market_price: dec("0.058267"),
            entry_price: dec("0.065"),
            quantity: dec("40"),
            leverage: 0,
            pnl: dec("0.269335"),
            roi: dec("-0.1036"),
        };
        let msg = render_position_report("m", "deribit", &[neg]);
        assert!(
            msg.contains("<font color=\"warning\">-10.36%</font>"),
            "实际:\n{msg}"
        );
    }

    /// 无持仓：返回空串（调用方跳过发送）。
    #[test]
    fn render_position_report_empty_returns_empty() {
        assert_eq!(render_position_report("bob", "binance", &[]), "");
        assert_eq!(render_roi_alert("bob", "deribit", &[]), "");
        assert_eq!(render_no_rule_positions("bob", "deribit", &[]), "");
    }

    /// ROI 告警：标题为 `告警仓位(账户 - 交易所)`，段落与播报一致。
    #[test]
    fn render_roi_alert_title_and_body() {
        let item = PositionReportItem {
            instrument: "ETH-25DEC26-2800-C",
            side: Side::Buy,
            market_price: dec("0.083009"),
            entry_price: dec("0.11"),
            quantity: dec("40"),
            leverage: 0,
            pnl: dec("-1.079623"),
            roi: dec("-0.2454"),
        };
        let msg = render_roi_alert("test_acct", "deribit", &[item]);
        assert!(msg.starts_with("## 告警仓位(test_acct - deribit)\n\n"));
        assert!(msg.contains("**ETH-25DEC26-2800-C**"));
        assert!(msg.contains("<font color=\"warning\">-24.54%</font>"));
    }

    /// 未设规则仓位：标题带账户与交易所；每行 `标的(盈亏百分比)`，
    /// 盈利（含 roi==0）整体 info 绿、亏损整体 warning 红；行内不再出现账户后缀。
    #[test]
    fn render_no_rule_positions_lists_instruments_with_roi_color() {
        let items = vec![
            // 亏损：-10.50%，warning 红。
            NoRuleItem {
                instrument: "BTC-25DEC26-85000-C",
                roi: dec("-0.105"),
            },
            // 盈利：+2.08%，info 绿。
            NoRuleItem {
                instrument: "BTC-25DEC26-90000-C",
                roi: dec("0.0208"),
            },
            // 恰好 0：与播报段落统一，归绿，显示 (+0.00%)。
            NoRuleItem {
                instrument: "BTC-30OCT26-87000-P",
                roi: Decimal::ZERO,
            },
        ];
        assert_eq!(
            render_no_rule_positions("monthopt", "deribit", &items),
            "## 未设置盯盘规则仓位(monthopt - deribit)\n\n\
             <font color=\"warning\">BTC-25DEC26-85000-C(-10.50%)</font>\n\
             <font color=\"info\">BTC-25DEC26-90000-C(+2.08%)</font>\n\
             <font color=\"info\">BTC-30OCT26-87000-P(+0.00%)</font>"
        );
    }

    // ---- split_chunks 分段 ----

    #[test]
    fn split_chunks_single_when_under_limit() {
        assert_eq!(split_chunks("hello", 100), vec!["hello".to_string()]);
    }

    #[test]
    fn split_chunks_splits_on_paragraph_boundary() {
        // 两段共超限：在 \n\n 处切，不留空段、不破坏段内。
        let a = "a".repeat(60);
        let b = "b".repeat(60);
        let chunks = split_chunks(&format!("{a}\n\n{b}"), 100);
        assert_eq!(chunks, vec![a, b]);
    }

    #[test]
    fn split_chunks_hard_splits_oversized_paragraph_on_char_boundary() {
        // 单段（含多字节中文字符）超限：硬切且各块 UTF-8 合法。
        let para = "汉".repeat(3000); // 每字 3 字节，共 9000 字节
        let chunks = split_chunks(&para, 4000);
        assert!(chunks.len() >= 3);
        for c in &chunks {
            assert!(c.len() <= 4000);
            assert!(std::str::from_utf8(c.as_bytes()).is_ok());
        }
        assert_eq!(chunks.concat().chars().count(), 3000);
    }

    #[test]
    fn split_chunks_mixed_boundaries_and_oversized() {
        // 多段混合：短段合并、超长段独立硬切。
        let small = "s".repeat(50);
        let big = "b".repeat(5000);
        let chunks = split_chunks(&format!("{small}\n\n{big}"), 1000);
        assert_eq!(chunks.first().unwrap(), &small);
        assert!(chunks[1..].iter().all(|c| c.len() <= 1000));
        assert_eq!(chunks[1..].concat().len(), 5000);
    }
}
