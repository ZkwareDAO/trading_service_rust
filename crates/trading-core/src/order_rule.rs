//! 下单前的本地校验 / 精度对齐内核。
//!
//! 纯函数、全程 [`Decimal`]、无网络：交易所差异已在写入侧被归一化为
//! [`trading_models::OrderRule`]，本模块只认这一种行结构。
//!
//! * Binance：严格校验——限价按 tick **截断（floor，非四舍五入）** 后重检价格区间，
//!   数量按 step 截断后重检区间，限价比对最小名义额；市价只校数量。规则缺失即失败，
//!   不把明显非法的单子下发给交易所。
//! * Deribit：数量对齐到步长整数倍（**四舍五入**到最近整数倍）；取不到步长时
//!   **原样放行**，把最终校验交给交易所（元数据故障不得挡住风控平仓）。
//! * Hyperliquid：当前不在本地消费规则（市价走滑点价、数量透传），故此处无 HL 入口。
//!
//! 见 `docs/交易规则.md` 第二、三节。

use std::sync::Arc;

use dashmap::DashMap;
use rust_decimal::Decimal;
use trading_models::{OrderRule, OrderRuleKey, OrderType};

/// 数量对齐后超过该绝对偏差即视为「非步长整数倍、对齐会留下残余仓位」，调用方应告警。
/// 取 1e-12，仅用于吸收 Decimal 运算尾差，不代表业务容差。
pub const ALIGN_RESIDUAL_NOISE: Decimal = Decimal::from_parts(1, 0, 0, false, 12);

/// 数量区间校验的容差，吸收 0.009999999… 这类表示误差。
const RANGE_EPS: Decimal = Decimal::from_parts(1, 0, 0, false, 9);

/// Binance 下单校验失败原因。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderRuleError {
    /// 限价单缺少价格 tick（PRICE_FILTER），无法对齐——不下发。
    PriceTickMissing,
    /// 限价单价格必须为正。
    PriceNotPositive,
    /// 价格超出 [price_min, price_max]（截断前或截断后）。
    PriceOutOfRange,
    /// 数量超出 [qty_min, qty_max]（截断前或截断后，含容差）。
    QuantityOutOfRange,
    /// 限价名义额（对齐后 price × qty）低于 notional_min。
    NotionalBelowMin,
}

/// 校验通过后可直接发送的字符串价格 / 数量（Binance 以字符串提交）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlignedOrder {
    pub price: String,
    pub quantity: String,
}

/// Deribit 数量对齐结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlignedAmount {
    /// 已对齐到步长整数倍；`residual` 为对齐造成的残余（原量 − 对齐量）。
    Aligned { value: Decimal, residual: Decimal },
    /// 无可用步长（规则缺失/拉取失败）：原样发出，交给交易所兜底。
    Passthrough(Decimal),
}

impl AlignedAmount {
    /// 实际要发出的数量。
    pub fn quantity(&self) -> Decimal {
        match self {
            AlignedAmount::Aligned { value, .. } => *value,
            AlignedAmount::Passthrough(q) => *q,
        }
    }

    /// 对齐残余是否超出浮点噪声（超出则调用方应告警残留仓位）。
    pub fn has_significant_residual(&self) -> bool {
        match self {
            AlignedAmount::Aligned { residual, .. } => residual.abs() > ALIGN_RESIDUAL_NOISE,
            AlignedAmount::Passthrough(_) => false,
        }
    }
}

/// 数量按交易规则步长对齐（**领域层、建单前使用**）。
///
/// 与 [`verify_binance`] 同一套 lot 口径、复用 [`align_qty_lot`] 内核：
/// * 限价单用 `LOT_SIZE` 档（step/min/max）；
/// * 市价 / 止损市价 / 止盈市价等（非限价）用 `MARKET_LOT_SIZE` 档，缺失回退 `LOT_SIZE`；
/// * 先带容差校验数量区间，再按步长朝零截断，截断后重检区间。
///
/// 返回对齐后的数量（Decimal），供领域层在写内存订单前把 `order_quantity` 规整为
/// 实际可委托量，使「订单行数量 = 实际委托量 = 通知数量」一致。
pub fn align_quantity(
    row: &OrderRule,
    order_type: OrderType,
    quantity: Decimal,
) -> Result<Decimal, OrderRuleError> {
    // 与 verify_binance 一致：仅限价走 LOT_SIZE，其余走市价档（缺省回退 LOT）。
    let is_limit = matches!(order_type, OrderType::Limit);
    let (step, min, max) = if is_limit {
        (row.qty_step, row.qty_min, row.qty_max)
    } else {
        (
            row.market_qty_step.unwrap_or(row.qty_step),
            row.market_qty_min.or(row.qty_min),
            row.market_qty_max.or(row.qty_max),
        )
    };
    align_qty_lot(quantity, step, min, max)
}

/// Binance 下单前校验/对齐。
///
/// 返回可直接发送的字符串价格、数量。规则与口径见 `docs/交易规则.md` 2.2 / 6.6。
pub fn verify_binance(
    row: &OrderRule,
    order_type: OrderType,
    price: Decimal,
    quantity: Decimal,
) -> Result<AlignedOrder, OrderRuleError> {
    match order_type {
        OrderType::Limit => verify_limit(row, price, quantity),
        // 真市价：价格由市场决定，只校数量（止损/止盈市价单同此口径）。
        _ => verify_market(row, quantity),
    }
}

/// 限价：tick 截断 + 区间重检 + 数量步长截断 + 名义额下限。
fn verify_limit(
    row: &OrderRule,
    price: Decimal,
    quantity: Decimal,
) -> Result<AlignedOrder, OrderRuleError> {
    // ---- 价格：必须有 tick，否则不下发 ----
    let tick = row.price_tick.ok_or(OrderRuleError::PriceTickMissing)?;
    if price <= Decimal::ZERO {
        return Err(OrderRuleError::PriceNotPositive);
    }
    if !within(price, row.price_min, row.price_max, Decimal::ZERO) {
        return Err(OrderRuleError::PriceOutOfRange);
    }
    let legal_price = truncate_to_step(price, tick);
    // 截断后重新做区间校验（floor 可能把价格压到下限之下）。
    if !within(legal_price, row.price_min, row.price_max, Decimal::ZERO) {
        return Err(OrderRuleError::PriceOutOfRange);
    }

    // ---- 数量：限价档 ----
    let legal_qty = align_qty_lot(quantity, row.qty_step, row.qty_min, row.qty_max)?;

    // ---- 最小名义额（限价比对对齐后的价×量）----
    if let Some(min_notional) = row.notional_min {
        if min_notional > Decimal::ZERO && legal_price * legal_qty < min_notional {
            return Err(OrderRuleError::NotionalBelowMin);
        }
    }

    Ok(AlignedOrder {
        price: format_fixed(legal_price, effective_decimal_places(tick)),
        quantity: format_fixed(legal_qty, effective_decimal_places(row.qty_step)),
    })
}

/// 市价：价格发 "0"，只校数量；优先 MARKET_LOT_SIZE，缺省回退限价档。
fn verify_market(row: &OrderRule, quantity: Decimal) -> Result<AlignedOrder, OrderRuleError> {
    let step = row.market_qty_step.unwrap_or(row.qty_step);
    let min = row.market_qty_min.or(row.qty_min);
    let max = row.market_qty_max.or(row.qty_max);
    let legal_qty = align_qty_lot(quantity, step, min, max)?;
    // 名义额无法预知价格：Binance 现货由 notional_*_to_market 决定是否对 QuoteOrderQty
    // 生效，本口径提交的是 base 数量，故市价不做名义额校验。
    Ok(AlignedOrder {
        price: "0".to_string(),
        quantity: format_fixed(legal_qty, effective_decimal_places(step)),
    })
}

/// 数量区间校验（带 epsilon 容差）→ 按步长截断 → 截断后重检区间。
fn align_qty_lot(
    quantity: Decimal,
    step: Decimal,
    min: Option<Decimal>,
    max: Option<Decimal>,
) -> Result<Decimal, OrderRuleError> {
    if !within(quantity, min, max, RANGE_EPS) {
        return Err(OrderRuleError::QuantityOutOfRange);
    }
    let legal = truncate_to_step(quantity, step);
    if !within(legal, min, max, RANGE_EPS) {
        return Err(OrderRuleError::QuantityOutOfRange);
    }
    Ok(legal)
}

/// `min - eps <= value <= max + eps`；缺省的边界视为无界。
fn within(value: Decimal, min: Option<Decimal>, max: Option<Decimal>, eps: Decimal) -> bool {
    if let Some(min) = min {
        if value < min - eps {
            return false;
        }
    }
    if let Some(max) = max {
        if value > max + eps {
            return false;
        }
    }
    true
}

/// Deribit 数量对齐。
///
/// `step` 由调用方从（已懒加载的）规则行解析：`qty_tick_size`，≤0 时回退
/// `min_trade_amount`；规则缺失传 `None`。`None`（或非正步长）时原样放行，
/// 元数据故障不挡风控平仓，最终校验交给交易所。
pub fn align_amount(quantity: Decimal, step: Option<Decimal>) -> AlignedAmount {
    let Some(step) = step else {
        return AlignedAmount::Passthrough(quantity);
    };
    if step <= Decimal::ZERO {
        return AlignedAmount::Passthrough(quantity);
    }
    // 对齐到最近的步长整数倍（四舍五入；与 Binance 的 floor 不同，张数口径无名义额方向）。
    let value = (quantity / step).round() * step;
    AlignedAmount::Aligned {
        value,
        residual: quantity - value,
    }
}

/// 运行时规则表：键 → 不可变规则行（`Arc`）。读端克隆 `Arc` 后无锁使用，刷新端整行
/// 替换，不会读到「新 tick + 旧 step」的撕裂组合。纯内存、不落盘。
#[derive(Debug, Default)]
pub struct OrderRuleTable {
    inner: DashMap<OrderRuleKey, Arc<OrderRule>>,
}

impl OrderRuleTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// 整行放入（刷新端构造好新行后调用）。
    pub fn put(&self, row: OrderRule) {
        let key = row.key();
        self.inner.insert(key, Arc::new(row));
    }

    /// 以共享行放入（批量同步时复用已构造的 `Arc`）。
    pub fn put_arc(&self, row: Arc<OrderRule>) {
        self.inner.insert(row.key(), row);
    }

    /// 读端：克隆 `Arc`，离开 map 后无锁。
    pub fn get(&self, key: &OrderRuleKey) -> Option<Arc<OrderRule>> {
        self.inner.get(key).map(|r| Arc::clone(r.value()))
    }

    /// 删除一行（Deribit 被「数量非法」拒单后失效重读）。
    pub fn invalidate(&self, key: &OrderRuleKey) {
        self.inner.remove(key);
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

// =====================================================================
// 定点精度小工具
// =====================================================================

/// 一个正步长的**有效**小数位数（剥掉尾随 0）：`0.00100 -> 3`、`1 -> 0`。
/// 尾随 0 若计入会把值截断到比真实网格更细的位数、反而偏离网格，故必须 normalize。
fn effective_decimal_places(step: Decimal) -> u32 {
    step.normalize().scale()
}

/// 10^n（n 为步长的有效小数位，实际不超过几十）。
fn pow10(n: u32) -> Decimal {
    let mut f = 1u64;
    for _ in 0..n {
        f = f.saturating_mul(10);
    }
    Decimal::from(f)
}

/// 按 `tick/step` 的有效小数位**截断（朝零，正数即 floor）**。
fn truncate_to_step(value: Decimal, step: Decimal) -> Decimal {
    let dp = effective_decimal_places(step);
    let factor = pow10(dp);
    (value * factor).trunc() / factor
}

/// 格式化为恰好 `dp` 位小数（不足补 0），供以字符串提交数量/价格的交易所使用。
fn format_fixed(value: Decimal, dp: u32) -> String {
    let s = value.round_dp(dp).to_string();
    if dp == 0 {
        return s;
    }
    match s.find('.') {
        None => format!("{s}.{zeros}", zeros = "0".repeat(dp as usize)),
        Some(i) => {
            let have = (s.len() - i - 1) as u32;
            if have >= dp {
                s
            } else {
                format!("{s}{zeros}", zeros = "0".repeat((dp - have) as usize))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use trading_models::{InstrumentType, OrderType, QtyUnit};

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    /// Binance USDⓈ-M 永续风格行：tick 0.1、step 0.001、名义额下限 5。
    fn binance_row() -> OrderRule {
        let mut r = OrderRule::new(
            "binance",
            "BTCUSDT",
            InstrumentType::Futures,
            QtyUnit::Base,
            Decimal::ONE,
            "USDT",
            "USDT",
            dec("0.001"),
            "binance:exchangeInfo",
            0,
        );
        r.price_tick = Some(dec("0.1"));
        r.price_min = Some(dec("1000"));
        r.price_max = Some(dec("10000000"));
        r.qty_min = Some(dec("0.001"));
        r.qty_max = Some(dec("1000"));
        r.notional_min = Some(dec("5"));
        r
    }

    // ---------- 精度小工具 ----------

    #[test]
    fn effective_places_strips_trailing_zeros() {
        assert_eq!(effective_decimal_places(dec("0.00100")), 3);
        assert_eq!(effective_decimal_places(dec("0.1")), 1);
        assert_eq!(effective_decimal_places(dec("1")), 0);
    }

    #[test]
    fn truncate_floors_to_step_precision() {
        assert_eq!(
            truncate_to_step(dec("65000.19"), dec("0.1")),
            dec("65000.1")
        );
        assert_eq!(truncate_to_step(dec("0.01239"), dec("0.001")), dec("0.012"));
        // 步长带尾随 0 也按有效位截断。
        assert_eq!(truncate_to_step(dec("1.2399"), dec("0.0100")), dec("1.23"));
    }

    #[test]
    fn format_fixed_pads_trailing_zeros() {
        assert_eq!(format_fixed(dec("65000.1"), 1), "65000.1");
        assert_eq!(format_fixed(dec("65000"), 1), "65000.0");
        assert_eq!(format_fixed(dec("0.012"), 3), "0.012");
        assert_eq!(format_fixed(dec("12"), 0), "12");
    }

    // ---------- align_quantity（领域层数量对齐）----------

    #[test]
    fn align_quantity_limit_truncates_to_lot_step() {
        // 限价：现金换算出 0.00245534，按 LOT_SIZE step=0.001 截断为 0.002。
        assert_eq!(
            align_quantity(&binance_row(), OrderType::Limit, dec("0.00245534")).unwrap(),
            dec("0.002")
        );
    }

    #[test]
    fn align_quantity_market_uses_market_lot_when_present() {
        // 市价档 step=0.01：0.025 → 0.02。
        let mut r = binance_row();
        r.market_qty_step = Some(dec("0.01"));
        r.market_qty_min = Some(dec("0.01"));
        r.market_qty_max = Some(dec("100"));
        assert_eq!(
            align_quantity(&r, OrderType::Market, dec("0.025")).unwrap(),
            dec("0.02")
        );
    }

    #[test]
    fn align_quantity_market_falls_back_to_lot_step() {
        // 无市价档：市价回退 LOT_SIZE step=0.001。
        assert_eq!(
            align_quantity(&binance_row(), OrderType::Market, dec("0.00245534")).unwrap(),
            dec("0.002")
        );
    }

    #[test]
    fn align_quantity_rejects_below_min() {
        // 低于 qty_min（截断后）→ QuantityOutOfRange。
        assert!(matches!(
            align_quantity(&binance_row(), OrderType::Limit, dec("0.0005")).unwrap_err(),
            OrderRuleError::QuantityOutOfRange
        ));
    }

    // ---------- Binance 限价 ----------

    #[test]
    fn binance_limit_truncates_price_and_qty_floor() {
        let out = verify_binance(
            &binance_row(),
            OrderType::Limit,
            dec("65000.19"),
            dec("0.01239"),
        )
        .unwrap();
        assert_eq!(out.price, "65000.1");
        assert_eq!(out.quantity, "0.012");
    }

    #[test]
    fn binance_limit_requires_positive_price() {
        let err =
            verify_binance(&binance_row(), OrderType::Limit, dec("0"), dec("0.012")).unwrap_err();
        assert_eq!(err, OrderRuleError::PriceNotPositive);
    }

    #[test]
    fn binance_limit_rejects_price_out_of_range() {
        let err =
            verify_binance(&binance_row(), OrderType::Limit, dec("500"), dec("0.012")).unwrap_err();
        assert_eq!(err, OrderRuleError::PriceOutOfRange);
    }

    #[test]
    fn binance_limit_price_that_truncates_below_min_is_rejected() {
        // 下限不在 tick 网格上：价格 1000.09 落在区间内，但按 0.1 截断成 1000.0，
        // 跌破下限 1000.05——截断后必须重检。
        let mut r = binance_row();
        r.price_min = Some(dec("1000.05"));
        let err = verify_binance(&r, OrderType::Limit, dec("1000.09"), dec("0.01")).unwrap_err();
        assert_eq!(err, OrderRuleError::PriceOutOfRange);
    }

    #[test]
    fn binance_limit_rejects_qty_below_min_after_truncation() {
        let err = verify_binance(
            &binance_row(),
            OrderType::Limit,
            dec("65000"),
            dec("0.0005"),
        )
        .unwrap_err();
        assert_eq!(err, OrderRuleError::QuantityOutOfRange);
    }

    #[test]
    fn binance_limit_rejects_notional_below_min() {
        // 价 1000、量 0.001 → 名义额 1 < 5。
        let err = verify_binance(&binance_row(), OrderType::Limit, dec("1000"), dec("0.001"))
            .unwrap_err();
        assert_eq!(err, OrderRuleError::NotionalBelowMin);
    }

    #[test]
    fn binance_limit_without_price_tick_fails_closed() {
        let mut r = binance_row();
        r.price_tick = None;
        let err = verify_binance(&r, OrderType::Limit, dec("65000"), dec("0.012")).unwrap_err();
        assert_eq!(err, OrderRuleError::PriceTickMissing);
    }

    #[test]
    fn binance_limit_tolerates_qty_on_min_with_noise() {
        // 0.0010000000001 这类表示误差在容差内，截断到 0.001 仍合法。
        let out = verify_binance(
            &binance_row(),
            OrderType::Limit,
            dec("65000"),
            dec("0.0010000000001"),
        )
        .unwrap();
        assert_eq!(out.quantity, "0.001");
    }

    // ---------- Binance 市价 ----------

    #[test]
    fn binance_market_skips_price_and_uses_market_lot() {
        let mut r = binance_row();
        r.market_qty_step = Some(dec("0.01"));
        r.market_qty_min = Some(dec("0.01"));
        r.market_qty_max = Some(dec("100"));
        let out = verify_binance(&r, OrderType::Market, Decimal::ZERO, dec("0.025")).unwrap();
        assert_eq!(out.price, "0");
        assert_eq!(out.quantity, "0.02");
    }

    #[test]
    fn binance_market_falls_back_to_limit_lot_when_no_market_lot() {
        let out = verify_binance(
            &binance_row(),
            OrderType::Market,
            Decimal::ZERO,
            dec("0.0129"),
        )
        .unwrap();
        assert_eq!(out.price, "0");
        assert_eq!(out.quantity, "0.012");
    }

    // ---------- Deribit 数量对齐 ----------

    #[test]
    fn deribit_aligns_to_nearest_step_multiple() {
        let out = align_amount(dec("1.26"), Some(dec("0.1")));
        assert_eq!(
            out,
            AlignedAmount::Aligned {
                value: dec("1.3"),
                residual: dec("-0.04"),
            }
        );
        assert!(out.has_significant_residual());
    }

    #[test]
    fn deribit_already_multiple_has_zero_residual() {
        let out = align_amount(dec("1.3"), Some(dec("0.1")));
        assert_eq!(
            out,
            AlignedAmount::Aligned {
                value: dec("1.3"),
                residual: Decimal::ZERO,
            }
        );
        assert!(!out.has_significant_residual());
    }

    #[test]
    fn deribit_passes_through_when_step_unavailable() {
        // 元数据故障不挡单：原样发出，交给交易所兜底。
        let out = align_amount(dec("1.2345"), None);
        assert_eq!(out, AlignedAmount::Passthrough(dec("1.2345")));
        assert_eq!(out.quantity(), dec("1.2345"));
        assert!(!out.has_significant_residual());
    }

    // ---------- 内存表 ----------

    #[test]
    fn rule_table_put_get_replace_invalidate() {
        let table = OrderRuleTable::new();
        assert!(table.is_empty());
        let key = OrderRuleKey::new("binance", InstrumentType::Futures, "BTCUSDT");

        table.put(binance_row());
        let first = table.get(&key).expect("row present");
        assert_eq!(first.price_tick, Some(dec("0.1")));
        assert_eq!(table.len(), 1);

        // 整行替换：读到新 tick，且为不同 Arc。
        let mut updated = binance_row();
        updated.price_tick = Some(dec("0.01"));
        let updated_arc = Arc::new(updated);
        table.put_arc(Arc::clone(&updated_arc));
        assert_eq!(table.get(&key).unwrap().price_tick, Some(dec("0.01")));
        assert!(Arc::ptr_eq(&table.get(&key).unwrap(), &updated_arc));
        assert!(!Arc::ptr_eq(&first, &updated_arc));

        table.invalidate(&key);
        assert!(table.get(&key).is_none());
    }
}
