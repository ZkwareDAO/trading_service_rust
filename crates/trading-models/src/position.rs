use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use store_core::model::bool_u8;
use store_core::{Model, StorageScope};

use crate::types::{InstrumentType, Side};

/// 数量单位，冗余自 InstrumentSpec，便于直接读懂。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum QtyUnit {
    #[default]
    Base,
    Contract,
    Share,
}

/// 持仓。账户作用域表，持久化于 `{day}/{account_name}/position.csv`。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub id: u64,
    pub account_id: u64,
    /// 最近一次风控平仓的触发规则，默认为 0。
    pub rule_id: u64,
    pub exchange: String,
    pub instrument_type: InstrumentType,
    pub instrument: String,
    pub side: Side,
    /// 持仓数量（按 qty_unit：币/张/股），定点。
    pub quantity: Decimal,
    pub qty_unit: QtyUnit,
    /// 合约乘数（冗余自 spec）；真实敞口 = quantity × multiplier。
    pub multiplier: Decimal,
    /// 持仓成本价（加仓按加权平均），定点。
    pub pos_price: Decimal,
    /// 当前行情价，定点。
    pub current_price: Decimal,
    /// 名义价值 = current_price × quantity × multiplier，定点。
    pub notional: Decimal,
    /// 杠杆（现货/股票=1）。
    pub leverage: u64,
    /// 占用保证金/本金 = notional / leverage，定点。
    pub margin: Decimal,
    /// 结算/盈亏币种（USDT/USD/BTC…），冗余自 spec。
    pub settle_currency: String,
    /// 未实现盈亏（以 settle_currency 计），定点。
    pub pnl_value: Decimal,
    /// 已实现盈亏（减仓/平仓累计），定点。
    pub realized_pnl: Decimal,
    /// 未实现盈亏率（比率而非百分数：+12.34% 存 0.1234，固定标度），定点。
    /// margin=0 等不可定义时存 0；供风控 rule 做阈值判定。
    #[serde(default)]
    pub roi: Decimal,
    /// 持仓期内出现过的最高 roi（盈利峰值，只增不减），固定标度，定点。
    /// 从未盈利时为 0；与 roi 同为比率口径。
    #[serde(default)]
    pub max_roi: Decimal,
    /// 盈利回落百分点：曾有盈利峰值（max_roi>0）后，roi 自峰值回落的差值
    /// `max_roi - roi`（与 roi 同为保证金口径）；未盈利或恰处峰值时为 0。
    #[serde(default)]
    pub profit_drawdown: Decimal,
    /// 到期/交割日毫秒时间戳；现货/永续/股票为空。
    pub expires_at: Option<u64>,
    /// 软删除：0=持仓中，1=已平仓。
    #[serde(with = "bool_u8")]
    pub deleted: bool,
    /// 平仓时间戳。
    pub close_time: Option<u64>,
    /// 最近一次回报时间戳（毫秒）。
    pub exchange_update_time: u64,
    pub created_at: u64,
    pub updated_at: u64,
}

impl Position {
    /// 测试/装配用最小构造。
    pub fn new(
        account_id: u64,
        exchange: impl Into<String>,
        instrument: impl Into<String>,
        side: Side,
    ) -> Self {
        Self {
            id: 0,
            account_id,
            rule_id: 0,
            exchange: exchange.into(),
            // Binance U 本位永续在本系统统一记为 Futures（Perpetual 为预留扩展）。
            instrument_type: InstrumentType::Futures,
            instrument: instrument.into(),
            side,
            quantity: Decimal::ZERO,
            qty_unit: QtyUnit::Base,
            multiplier: Decimal::ONE,
            pos_price: Decimal::ZERO,
            current_price: Decimal::ZERO,
            notional: Decimal::ZERO,
            leverage: 1,
            margin: Decimal::ZERO,
            settle_currency: "USDT".to_string(),
            pnl_value: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            roi: Decimal::ZERO,
            max_roi: Decimal::ZERO,
            profit_drawdown: Decimal::ZERO,
            expires_at: None,
            deleted: false,
            close_time: None,
            exchange_update_time: 0,
            created_at: 0,
            updated_at: 0,
        }
    }
}

/// 比率字段（`roi` / `max_roi`）与差值字段（`profit_drawdown`，百分点）
/// 统一固定标度：1e-6（= 0.0001%）。既保留足够风控分辨率，又避免无限循环小数。
pub const ROI_SCALE: u32 = 6;

/// 行情驱动的仓位重估输入：一条标的的最新价及其所属交易所/合约类型。
///
/// 批量重估 [`revalue_positions`] 只更新 `exchange + instrument_type + instrument`
/// 三者全部匹配的仓位，从输入侧保证「某交易所行情只更新该交易所仓位」，
/// 不会误刷新其它交易所/市场（现货 vs 合约）的同名标的仓位。
#[derive(Debug, Clone)]
pub struct PositionPrice {
    /// 交易所规范名（Position.exchange 口径，如 "binance"）。
    pub exchange: String,
    pub instrument_type: InstrumentType,
    pub instrument: String,
    /// 最新估值价（合约为标记价口径）。
    pub price: Decimal,
}

impl PositionPrice {
    pub fn new(
        exchange: impl Into<String>,
        instrument_type: InstrumentType,
        instrument: impl Into<String>,
        price: Decimal,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            instrument_type,
            instrument: instrument.into(),
            price,
        }
    }

    /// 该价格是否作用于给定仓位（交易所 + 合约类型 + 标的三重匹配）。
    fn applies_to(&self, p: &Position) -> bool {
        p.exchange == self.exchange
            && p.instrument_type == self.instrument_type
            && p.instrument == self.instrument
    }
}

impl Position {
    /// 用最新价重估单个仓位，返回更新后的副本（**仓位计算方法**，不可变）。
    ///
    /// 仅刷新行情衍生字段：`current_price` / `notional` / `pnl_value` / `roi` /
    /// `max_roi` / `profit_drawdown`；身份、数量、成本价、杠杆、保证金、已实现盈亏与
    /// 时间戳均保持不变（`margin` 以交易所回查为权威，不由行情臆造）。
    ///
    /// 口径（见 `docs/状态机设计.md`）：
    /// * `notional = current_price × quantity × multiplier`
    /// * 多：`pnl = (current_price − pos_price) × qty × mult`；
    ///   空：`pnl = (pos_price − current_price) × qty × mult`
    /// * 成本基准 `cost = pos_price × quantity × multiplier`（建仓投入，不随现价变）
    /// * `roi = pnl × leverage / cost`（保留 [`ROI_SCALE`] 位小数；=pnl/实际占用保证金）
    /// * `max_roi = max(历史 max_roi, roi)`（盈利峰值只增不减）
    /// * `profit_drawdown`：仅在 `max_roi > 0` 时为 roi 自峰值回落的百分点
    ///   `max_roi − roi`，否则 0
    ///
    /// 边界：负价视为无效、原样返回；成本 ≤ 0（零成本价/零量/零乘数）时不定义 roi，
    /// 盈亏与比率归零但保留历史峰值，绝不触发除零。
    pub fn revalued(&self, current_price: Decimal) -> Position {
        let mut next = self.clone();

        // 负价非法：不重估，避免算出符号错误的盈亏。
        if current_price.is_sign_negative() {
            return next;
        }
        next.current_price = current_price;

        let notional = multiply_exposure(current_price, self);
        next.notional = notional;
        // 成本基准（建仓时投入，不随现价变）= 成本价 × 量 × 乘数。
        let cost = multiply_exposure(self.pos_price, self);

        // 成本不可定义（零成本价/零量）：盈亏/比率归零，峰值保留。
        if cost <= Decimal::ZERO {
            next.pnl_value = Decimal::ZERO;
            next.roi = Decimal::ZERO;
            next.max_roi = self.max_roi;
            next.profit_drawdown = profit_drawdown(self.max_roi, Decimal::ZERO);
            return next;
        }

        // 方向性价差：多赚低买高卖，空赚高卖低买。
        let price_edge = match self.side {
            Side::Buy => current_price - self.pos_price,
            Side::Sell => self.pos_price - current_price,
        };
        let pnl = price_edge
            .checked_mul(self.quantity)
            .and_then(|v| v.checked_mul(self.multiplier))
            .unwrap_or(Decimal::ZERO);
        next.pnl_value = pnl;

        // ROI = pnl/实际投入：杠杆为 0 不应出现（现货=1），下限钳到 1 以免除零。
        // 分母用成本基准 cost（=pos_price×量×乘数），不随现价变。
        let leverage = Decimal::from(self.leverage.max(1));
        let roi = pnl
            .checked_mul(leverage)
            .and_then(|v| v.checked_div(cost))
            .map(|v| v.round_dp(ROI_SCALE))
            .unwrap_or(Decimal::ZERO);
        next.roi = roi;
        next.max_roi = self.max_roi.max(roi);
        next.profit_drawdown = profit_drawdown(next.max_roi, roi);

        next
    }

    /// 平仓已实现盈亏（**实际成交均价、成本口径**），返回 `(盈亏值, 盈亏比率)`。
    ///
    /// 风控平仓成交通知专用：不用成交前 mark 价聚合的浮动盈亏。口径：
    /// * 成本基数 `cost = pos_price × qty × multiplier`；
    /// * 多：`profit = (close_price − pos_price) × qty × multiplier`；
    ///   空：`profit = (pos_price − close_price) × qty × multiplier`；
    /// * 比率 = `profit / cost`（**成本口径，与杠杆无关**）；`cost ≤ 0` 时归 0，不除零。
    ///
    /// 边界：平仓负价或数量非正均视为非法输入，返回 `(0, 0)`。
    pub fn realized_close_pnl(
        &self,
        close_price: Decimal,
        close_quantity: Decimal,
    ) -> (Decimal, Decimal) {
        if close_price.is_sign_negative() || close_quantity <= Decimal::ZERO {
            return (Decimal::ZERO, Decimal::ZERO);
        }
        // 成本基数 = 成本价 × 平仓量 × 乘数。
        let cost = self
            .pos_price
            .checked_mul(close_quantity)
            .and_then(|v| v.checked_mul(self.multiplier))
            .unwrap_or(Decimal::ZERO);
        let price_edge = match self.side {
            Side::Buy => close_price - self.pos_price,
            Side::Sell => self.pos_price - close_price,
        };
        let profit = price_edge
            .checked_mul(close_quantity)
            .and_then(|v| v.checked_mul(self.multiplier))
            .unwrap_or(Decimal::ZERO);
        // cost=0 时 checked_div 返回 None，比率归 0，不除零。
        let ratio = profit.checked_div(cost).unwrap_or(Decimal::ZERO);
        (profit, ratio)
    }
}

/// 批量仓位更新方法：用一条最新价重估**当前全部活跃仓位**中与之匹配的行。
///
/// 入参为所有 `deleted=0` 的仓位数组；内部逐行调用单仓计算 [`Position::revalued`]，
/// 仅返回 `exchange + instrument_type + instrument` 三重匹配的**新副本**
/// （未匹配行不在结果中，入参数组不被修改）。调用方持返回结果经仓储逐行落库。
/// 交易所无关：不同交易所推送频率不同，各自的价格只刷新各自仓位。
pub fn revalue_positions(price: &PositionPrice, active: Vec<Position>) -> Vec<Position> {
    active
        .into_iter()
        .filter(|p| price.applies_to(p))
        .map(|p| p.revalued(price.price))
        .collect()
}

/// 敞口 = 价格 × 数量 × 合约乘数；任一步溢出归 0（Decimal 正常输入不会溢出）。
fn multiply_exposure(price: Decimal, p: &Position) -> Decimal {
    price
        .checked_mul(p.quantity)
        .and_then(|v| v.checked_mul(p.multiplier))
        .unwrap_or(Decimal::ZERO)
}

/// 盈利回落百分点：仅在历史盈利峰值 `max_roi > 0` 时有定义。
/// `roi == max_roi`（在峰值）为 0；回落越多值越大；从未盈利为 0。
fn profit_drawdown(max_roi: Decimal, roi: Decimal) -> Decimal {
    if max_roi <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    max_roi
        .checked_sub(roi)
        .map(|v| v.round_dp(ROI_SCALE))
        .unwrap_or(Decimal::ZERO)
}

impl Model for Position {
    const MODEL_NAME: &'static str = "position";

    fn id(&self) -> u64 {
        self.id
    }
    fn set_id(&mut self, id: u64) {
        self.id = id;
    }
    fn deleted(&self) -> bool {
        self.deleted
    }
    fn set_deleted(&mut self, deleted: bool) {
        self.deleted = deleted;
    }
    fn created_at(&self) -> u64 {
        self.created_at
    }
    fn updated_at(&self) -> u64 {
        self.updated_at
    }
    fn set_timestamps(&mut self, created_at: u64, updated_at: u64) {
        self.created_at = created_at;
        self.updated_at = updated_at;
    }
    fn scope(&self) -> StorageScope {
        StorageScope::Account(self.account_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    /// 金额/数量类字段全部为定点 Decimal：CSV 往返后标度与数值严格一致，
    /// 任何回退成 f64 的改动都会让这些精确相等断言失败。
    #[test]
    fn csv_roundtrip_preserves_decimal_scale_exactly() {
        let mut p = Position::new(7, "binance", "BTCUSDT", Side::Buy);
        // 不显式覆盖 instrument_type：默认即 U 本位永续（Futures）。
        p.qty_unit = QtyUnit::Contract;
        p.quantity = dec("1.25");
        p.multiplier = dec("10");
        p.pos_price = dec("80000.5");
        p.current_price = dec("80123.75");
        p.notional = dec("1001546.875");
        p.margin = dec("0.00001");
        p.pnl_value = dec("154.125");
        p.realized_pnl = dec("-12.34");
        p.roi = dec("0.1234");
        p.max_roi = dec("0.2");
        p.profit_drawdown = dec("0.383");
        p.settle_currency = "USDT".into();
        p.exchange_update_time = 1_726_400_000_123;
        p.created_at = 1_726_400_000_000;
        p.updated_at = 1_726_400_000_123;

        let mut wtr = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(Vec::new());
        wtr.serialize(&p).unwrap();
        let bytes = wtr.into_inner().unwrap();

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(bytes.as_slice());
        let back: Position = rdr.deserialize().next().unwrap().unwrap();

        // 定点逐字段严格相等（含标度），证明全程未经过 f64。
        assert_eq!(back.quantity.to_string(), "1.25");
        assert_eq!(back.multiplier.to_string(), "10");
        assert_eq!(back.pos_price.to_string(), "80000.5");
        assert_eq!(back.current_price.to_string(), "80123.75");
        assert_eq!(back.notional.to_string(), "1001546.875");
        assert_eq!(back.margin.to_string(), "0.00001");
        assert_eq!(back.pnl_value.to_string(), "154.125");
        assert_eq!(back.realized_pnl.to_string(), "-12.34");
        assert_eq!(back.roi.to_string(), "0.1234");
        assert_eq!(back.max_roi.to_string(), "0.2");
        assert_eq!(back.profit_drawdown.to_string(), "0.383");
        // 枚举 / 时间戳 / 软删标记。
        assert_eq!(back.instrument_type, InstrumentType::Futures);
        assert_eq!(back.qty_unit, QtyUnit::Contract);
        assert_eq!(back.side, Side::Buy);
        assert_eq!(back.exchange_update_time, 1_726_400_000_123);
        assert_eq!(back.created_at, 1_726_400_000_000);
        assert_eq!(back.updated_at, 1_726_400_000_123);
        assert!(!back.deleted);
    }

    /// 构造一个可调数量/成本/杠杆/交易所的仓位（mult=1）。
    fn position(
        account: u64,
        exchange: &str,
        itype: InstrumentType,
        side: Side,
        quantity: &str,
        pos_price: &str,
        leverage: u64,
    ) -> Position {
        let mut p = Position::new(account, exchange, "BTCUSDT", side);
        p.instrument_type = itype;
        p.quantity = dec(quantity);
        p.pos_price = dec(pos_price);
        p.leverage = leverage;
        p
    }

    #[test]
    fn roi_uses_cost_basis_not_current_notional() {
        // 10x，成本名义价值=100，保证金=10，现价 125，pnl=25 → roi=25/10=2.5。
        // 旧「当前价」口径给 25*10/125=2，是错的。
        let p = position(
            1,
            "binance",
            InstrumentType::Futures,
            Side::Buy,
            "1",
            "100",
            10,
        );
        let r = p.revalued(dec("125"));
        assert_eq!(r.pnl_value, dec("25"));
        assert_eq!(r.roi, dec("2.5"));
    }

    #[test]
    fn spot_roi_is_pnl_over_cost_regardless_of_current_price() {
        // 现货/期权 lev=1：roi = pnl/(pos_price×量)。100→125 → 0.25。
        let p = position(1, "binance", InstrumentType::Spot, Side::Buy, "1", "100", 1);
        let r = p.revalued(dec("125"));
        assert_eq!(r.roi, dec("0.25"));
    }

    #[test]
    fn long_profit_computes_notional_pnl_roi_and_peak() {
        // 10x，100 买入，125 估值：pnl=25，notional=125，roi=25*10/125=2。
        let p = position(
            1,
            "binance",
            InstrumentType::Futures,
            Side::Buy,
            "1",
            "100",
            10,
        );
        let r = p.revalued(dec("125"));
        assert_eq!(r.current_price, dec("125"));
        assert_eq!(r.notional, dec("125"));
        assert_eq!(r.pnl_value, dec("25"));
        // 成本口径：roi=25×10/100=2.5（=pnl/保证金10）。
        assert_eq!(r.roi, dec("2.5"));
        assert_eq!(r.max_roi, dec("2.5"));
        assert_eq!(r.profit_drawdown, dec("0"));
    }

    #[test]
    fn short_profit_reverses_price_edge() {
        let p = position(
            1,
            "binance",
            InstrumentType::Futures,
            Side::Sell,
            "1",
            "100",
            10,
        );
        let r = p.revalued(dec("80"));
        assert_eq!(r.notional, dec("80"));
        assert_eq!(r.pnl_value, dec("20"));
        // 成本口径：roi=20×10/100=2.0。
        assert_eq!(r.roi, dec("2"));
        assert_eq!(r.max_roi, dec("2"));
        assert_eq!(r.profit_drawdown, dec("0"));
    }

    #[test]
    fn peak_is_monotonic_and_drawdown_tracks_fall_from_peak() {
        let base = position(
            1,
            "binance",
            InstrumentType::Futures,
            Side::Buy,
            "1",
            "100",
            10,
        );
        let at_125 = base.revalued(dec("125"));
        assert_eq!(at_125.max_roi, dec("2.5"));
        let at_200 = at_125.revalued(dec("200"));
        // 成本口径：分母锚定成本100，roi 随现价线性：200→pnl100、roi=10。
        assert_eq!(at_200.roi, dec("10"));
        assert_eq!(at_200.max_roi, dec("10"));
        // 回到 150：roi=5，峰值 10，回落=10-5=5（保证金口径百分点）。
        let at_150 = at_200.revalued(dec("150"));
        assert_eq!(at_150.roi, dec("5"));
        assert_eq!(at_150.max_roi, dec("10"));
        assert_eq!(at_150.profit_drawdown, dec("5"));
    }

    #[test]
    fn zero_quantity_zeroes_derivatives_but_keeps_peak() {
        let mut p = position(
            1,
            "binance",
            InstrumentType::Futures,
            Side::Buy,
            "0",
            "100",
            10,
        );
        p.max_roi = dec("0.3");
        let r = p.revalued(dec("100"));
        assert_eq!(r.notional, dec("0"));
        assert_eq!(r.roi, dec("0"));
        assert_eq!(r.max_roi, dec("0.3"));
        assert_eq!(r.profit_drawdown, dec("0.3")); // 0.3 - 0
    }

    #[test]
    fn negative_price_is_ignored() {
        let mut p = position(
            1,
            "binance",
            InstrumentType::Futures,
            Side::Buy,
            "1",
            "100",
            10,
        );
        p.current_price = dec("50");
        p.roi = dec("0.7");
        let r = p.revalued(dec("-1"));
        assert_eq!(r.current_price, dec("50"));
        assert_eq!(r.roi, dec("0.7"));
    }

    #[test]
    fn batch_revalues_only_matching_exchange_type_and_symbol() {
        // Arrange：binance 合约 BTC（多）、binance 合约 ETH、xili 合约 BTC、binance 现货 BTC。
        let b_btc = position(
            1,
            "binance",
            InstrumentType::Futures,
            Side::Buy,
            "1",
            "100",
            10,
        );
        let b_eth = {
            let mut p = position(
                1,
                "binance",
                InstrumentType::Futures,
                Side::Buy,
                "1",
                "100",
                10,
            );
            p.instrument = "ETHUSDT".into();
            p
        };
        let x_btc = position(
            1,
            "xili",
            InstrumentType::Futures,
            Side::Buy,
            "1",
            "100",
            10,
        );
        let b_spot = {
            let mut p = position(1, "binance", InstrumentType::Spot, Side::Buy, "1", "100", 1);
            p.instrument = "BTCUSDT".into();
            p
        };
        // 同标的对冲空仓也应一起重估。
        let b_btc_short = position(
            1,
            "binance",
            InstrumentType::Futures,
            Side::Sell,
            "1",
            "100",
            10,
        );

        // Act：binance 合约 BTCUSDT 标记价 125。
        let price = PositionPrice::new("binance", InstrumentType::Futures, "BTCUSDT", dec("125"));
        let out = revalue_positions(&price, vec![b_btc, b_eth, x_btc, b_spot, b_btc_short]);

        // Assert：仅 binance/Futures/BTCUSDT 的多空两行返回，且按各自方向重估。
        assert_eq!(out.len(), 2, "跨交易所/跨市场/不同标的的仓位不得被刷新");
        assert!(out.iter().all(|p| p.exchange == "binance"
            && p.instrument_type == InstrumentType::Futures
            && p.instrument == "BTCUSDT"));
        let long = out.iter().find(|p| p.side == Side::Buy).unwrap();
        let short = out.iter().find(|p| p.side == Side::Sell).unwrap();
        assert_eq!(long.current_price, dec("125"));
        // 成本口径：long roi=25*10/100=2.5。
        assert_eq!(long.roi, dec("2.5"));
        // 空仓 125 亏损：pnl=(100-125)=-25，roi=-25*10/100=-2.5。
        assert_eq!(short.current_price, dec("125"));
        assert_eq!(short.roi, dec("-2.5"));
    }

    #[test]
    fn batch_revalue_empty_input_yields_empty() {
        let price = PositionPrice::new("binance", InstrumentType::Futures, "BTCUSDT", dec("1"));
        assert!(revalue_positions(&price, vec![]).is_empty());
    }

    // ---- 平仓已实现盈亏（成交均价、成本口径）----

    /// 构造带乘数的仓位。
    fn close_position(side: Side, pos_price: &str, mult: &str) -> Position {
        let mut p = Position::new(1, "binance", "BTCUSDT", side);
        p.instrument_type = InstrumentType::Futures;
        p.pos_price = dec(pos_price);
        p.multiplier = dec(mult);
        p
    }

    #[test]
    fn realized_close_long_profit_uses_cost_basis() {
        // 多仓：成本 100，平仓 125，量 1，乘数 1 → 盈亏 25，比率 25/100=0.25（与杠杆无关）。
        let p = close_position(Side::Buy, "100", "1");
        assert_eq!(
            p.realized_close_pnl(dec("125"), dec("1")),
            (dec("25"), dec("0.25"))
        );
    }

    #[test]
    fn realized_close_short_profit_reverses_edge() {
        // 空仓：成本 100，平仓 80 → 盈亏 20，比率 0.2。
        let p = close_position(Side::Sell, "100", "1");
        assert_eq!(
            p.realized_close_pnl(dec("80"), dec("1")),
            (dec("20"), dec("0.2"))
        );
    }

    #[test]
    fn realized_close_loss_is_negative() {
        // 多仓亏损：平仓 90 → 盈亏 -10，比率 -0.1。
        let p = close_position(Side::Buy, "100", "1");
        assert_eq!(
            p.realized_close_pnl(dec("90"), dec("1")),
            (dec("-10"), dec("-0.1"))
        );
    }

    #[test]
    fn realized_close_applies_qty_and_multiplier() {
        // 乘数 10、量 2：盈亏 =(125-100)×2×10=500；成本=100×2×10=2000 → 0.25。
        let p = close_position(Side::Buy, "100", "10");
        assert_eq!(
            p.realized_close_pnl(dec("125"), dec("2")),
            (dec("500"), dec("0.25"))
        );
    }

    #[test]
    fn realized_close_zero_cost_yields_zero_pct() {
        // 成本价为 0：盈亏仍按价差计算，比率不可定义归 0，绝不除零。
        let p = close_position(Side::Buy, "0", "1");
        assert_eq!(
            p.realized_close_pnl(dec("125"), dec("1")),
            (dec("125"), dec("0"))
        );
    }

    #[test]
    fn realized_close_rejects_bad_inputs() {
        // 负价 / 非正量：不产生盈亏。
        let p = close_position(Side::Buy, "100", "1");
        assert_eq!(
            p.realized_close_pnl(dec("-1"), dec("1")),
            (dec("0"), dec("0"))
        );
        assert_eq!(
            p.realized_close_pnl(dec("125"), dec("0")),
            (dec("0"), dec("0"))
        );
    }

    #[test]
    fn new_position_is_zeroed_and_active() {
        let p = Position::new(1, "xili", "btcusdt", Side::Sell);
        assert_eq!(p.roi, Decimal::ZERO);
        assert_eq!(p.quantity, Decimal::ZERO);
        assert_eq!(p.multiplier, Decimal::ONE);
        assert!(!p.deleted);
        // Binance U 本位永续在本系统统一记为 Futures（Perpetual 为预留扩展）。
        assert_eq!(p.instrument_type, InstrumentType::Futures);
    }
}
