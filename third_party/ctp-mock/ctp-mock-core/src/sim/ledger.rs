//! 最小自洽台账（不做风控）：成交 → 持仓累计；查仓 → 多/空行派生。
//!
//! 唯一保留的「账」：
//! * 每个合约一条净仓 [`InstrumentPos`]（带符号净仓、今/昨、开仓均价、乘数等静态夹具）；
//! * 委托账本 [`OrderLedger`] 记录本会话委托，供查单/撤单两路定位；
//! * 查仓时由净仓**派生** CTP 形态的多/空、今/昨行（含数量为 0 的行）。
//!
//! 成交口径（带符号简单加减，无保证金/限仓校验）：
//! * 买开(+vol, today)、卖开(-vol, today)；
//! * 平仓优先减昨仓再减今仓（符号与开仓相反）；SHFE/INE 区分今昨由 offset 决定，
//!   其他所净额（平=Close 时按昨→今）。
//!
//! 本模块纯逻辑、可单测；FFI 层据此填充 `CThostFtdcInvestorPositionField`。

use std::collections::BTreeMap;

/// 开平字符（对齐 ctp-core types）：开 '0' / 平 '1' / 平今 '3' / 平昨 '4'。
pub const OFFSET_OPEN: char = '0';
pub const OFFSET_CLOSE: char = '1';
pub const OFFSET_CLOSE_TODAY: char = '3';
pub const OFFSET_CLOSE_YESTERDAY: char = '4';
/// 买 '0' / 卖 '1'；持仓方向 多 '2' / 空 '3'。
pub const DIR_BUY: char = '0';
pub const DIR_SELL: char = '1';
pub const POSI_LONG: char = '2';
pub const POSI_SHORT: char = '3';
/// 持仓日期：今 '1' / 昨 '2'（CTP `THOST_FTDC_PSD_Today/History`）。
pub const POS_DATE_TODAY: char = '1';
pub const POS_DATE_YESTERDAY: char = '2';

/// 合约静态属性（夹具；只保证字段非空与自洽，不代表真实合约参数）。
#[derive(Debug, Clone, PartialEq)]
pub struct InstrumentSpec {
    pub instrument_id: String,
    pub exchange_id: String,
    pub product_id: String,
    /// 合约乘数（手 → 量）。
    pub volume_multiple: i32,
    pub long_margin_ratio: f64,
    pub short_margin_ratio: f64,
    /// 是否区分今昨（SHFE/INE 为 true；其他所净额平仓）。
    pub today_yesterday_split: bool,
}

impl InstrumentSpec {
    /// 上期所/能源中心口径：区分今昨。
    pub fn shfe(instrument_id: &str) -> Self {
        Self {
            instrument_id: instrument_id.into(),
            exchange_id: "SHFE".into(),
            product_id: instrument_id
                .chars()
                .take_while(|c| c.is_alphabetic())
                .collect(),
            volume_multiple: 10,
            long_margin_ratio: 0.1,
            short_margin_ratio: 0.1,
            today_yesterday_split: true,
        }
    }

    /// 静态夹具：按合约前缀推断交易所（仅用于 mock，不代表真实合约参数）。
    ///
    /// 上期所系（黑色/有色/贵金属等前缀）区分今昨；其余按净额处理。
    pub fn fixture(instrument_id: &str) -> Self {
        const SHFE_PREFIXES: [&str; 16] = [
            "rb", "hc", "ss", "cu", "al", "zn", "pb", "ni", "sn", "au", "ag", "fu", "bu", "ru",
            "sp", "ao",
        ];
        let alpha: String = instrument_id
            .chars()
            .take_while(|c| c.is_alphabetic())
            .collect();
        if SHFE_PREFIXES.contains(&alpha.as_str()) {
            Self::shfe(instrument_id)
        } else {
            Self::generic(instrument_id, "")
        }
    }

    /// 其他所口径：净额、不区分今昨（平今/平昨都落到净仓）。
    pub fn generic(instrument_id: &str, exchange_id: &str) -> Self {
        Self {
            instrument_id: instrument_id.into(),
            exchange_id: exchange_id.into(),
            product_id: instrument_id
                .chars()
                .take_while(|c| c.is_alphabetic())
                .collect(),
            volume_multiple: 10,
            long_margin_ratio: 0.1,
            short_margin_ratio: 0.1,
            today_yesterday_split: false,
        }
    }
}

/// 单个合约的净仓账（带符号：正=净多，负=净空）。
#[derive(Debug, Clone, PartialEq)]
pub struct InstrumentPos {
    pub instrument_id: String,
    pub exchange_id: String,
    pub volume_multiple: i32,
    pub long_margin_ratio: f64,
    pub short_margin_ratio: f64,
    pub today_yesterday_split: bool,
    /// 带符号今仓（开仓累计/平今减少）。
    pub today: i32,
    /// 昨仓（播种而来；平昨减少）。带符号。
    pub yd: i32,
    /// 开仓均价（按成交量加权；简化口径，仅用于非空字段）。
    pub avg_open_price: f64,
}

impl InstrumentPos {
    fn empty(spec: &InstrumentSpec) -> Self {
        Self {
            instrument_id: spec.instrument_id.clone(),
            exchange_id: spec.exchange_id.clone(),
            volume_multiple: spec.volume_multiple,
            long_margin_ratio: spec.long_margin_ratio,
            short_margin_ratio: spec.short_margin_ratio,
            today_yesterday_split: spec.today_yesterday_split,
            today: 0,
            yd: 0,
            avg_open_price: 0.0,
        }
    }

    /// 带符号总净仓（今 + 昨）。
    pub fn net(&self) -> i32 {
        self.today + self.yd
    }
}

/// 一笔成交（驱动持仓账）。
#[derive(Debug, Clone, PartialEq)]
pub struct Fill {
    pub instrument_id: String,
    pub direction: char,
    pub offset: char,
    pub price: f64,
    pub volume: i32,
}

/// 查仓派生行（FFI 据此填 `CThostFtdcInvestorPositionField`）。
#[derive(Debug, Clone, PartialEq)]
pub struct PositionRow {
    pub instrument_id: String,
    pub exchange_id: String,
    /// 多 '2' / 空 '3'。
    pub posi_direction: char,
    /// 今 '1' / 昨 '2'（非今昨区分所一律 Today）。
    pub position_date: char,
    pub position: i32,
    pub today_position: i32,
    pub yd_position: i32,
    pub position_cost: f64,
    pub use_margin: f64,
    pub position_profit: f64,
    pub volume_multiple: i32,
}

/// 成交 → 持仓台账。
#[derive(Debug, Default)]
pub struct PositionLedger {
    specs: BTreeMap<String, InstrumentSpec>,
    pos: BTreeMap<String, InstrumentPos>,
}

impl PositionLedger {
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册/覆盖合约静态属性。
    pub fn register_spec(&mut self, spec: InstrumentSpec) {
        self.specs.insert(spec.instrument_id.clone(), spec);
    }

    /// 已注册合约静态属性（确定性顺序，供合约查询逐行回放）。
    pub fn known_specs(&self) -> Vec<&InstrumentSpec> {
        self.specs.values().collect()
    }

    /// 按合约代码取已注册规格；未注册返回 None。
    pub fn spec(&self, instrument: &str) -> Option<&InstrumentSpec> {
        self.specs.get(instrument)
    }

    /// 确保某合约规格存在：未注册时按代码前缀给静态夹具（SHFE 系区分今昨）。
    pub fn ensure_spec(&mut self, instrument: &str) {
        if self.specs.contains_key(instrument) {
            return;
        }
        self.specs
            .insert(instrument.to_string(), InstrumentSpec::fixture(instrument));
    }

    fn spec_of(&self, instrument: &str) -> InstrumentSpec {
        self.specs
            .get(instrument)
            .cloned()
            .unwrap_or_else(|| InstrumentSpec::generic(instrument, ""))
    }

    fn entry(&mut self, instrument: &str) -> &mut InstrumentPos {
        if !self.pos.contains_key(instrument) {
            let spec = self.spec_of(instrument);
            self.pos
                .insert(instrument.into(), InstrumentPos::empty(&spec));
        }
        self.pos.get_mut(instrument).unwrap()
    }

    /// 播种/覆盖持仓（控制面 seed-position）。非今昨所把全部量记为 today。
    pub fn seed(
        &mut self,
        instrument: &str,
        long: bool,
        total: i32,
        today: i32,
        yd: i32,
        avg_open_price: f64,
    ) {
        let spec = self.spec_of(instrument);
        let signed_total = if long { total } else { -total };
        let (t, y) = if spec.today_yesterday_split {
            let st = if long { today } else { -today };
            let sy = if long { yd } else { -yd };
            (st, sy)
        } else {
            (signed_total, 0)
        };
        let p = self.entry(instrument);
        p.today = t;
        p.yd = y;
        p.avg_open_price = avg_open_price;
    }

    /// 应用一笔成交，更新净仓（无风控、简单加减）。
    ///
    /// 带符号量：买 `+vol`、卖 `-vol`。开仓与平仓都是「净仓 += 带符号量」
    /// （卖平多时 -vol 使正净仓下降，买平空时 +vol 使负净仓回升）；区别仅在
    /// 今/昨分配：平昨优先减昨仓，普通平仓先昨后今，平今只减今仓。
    pub fn apply_fill(&mut self, fill: &Fill) {
        let is_split = self.spec_of(&fill.instrument_id).today_yesterday_split;
        let signed: i32 = if fill.direction == DIR_BUY {
            fill.volume
        } else {
            -fill.volume
        };
        let p = self.entry(&fill.instrument_id);

        if fill.offset == OFFSET_OPEN {
            let prev = p.today.unsigned_abs();
            p.today += signed;
            // 同向加仓按成交量加权更新开仓均价。
            let new_total = prev + fill.volume as u32;
            if new_total > 0 {
                p.avg_open_price = (p.avg_open_price * prev as f64
                    + fill.price * fill.volume as f64)
                    / new_total as f64;
            }
            return;
        }

        if is_split && fill.offset == OFFSET_CLOSE_YESTERDAY {
            p.yd += signed;
            return;
        }
        if is_split && fill.offset == OFFSET_CLOSE_TODAY {
            p.today += signed;
            return;
        }

        // 普通平仓 / 非今昨所：先减昨仓（同向可平量为限），余量减今仓。
        let vol = fill.volume;
        let yd_avail = if signed < 0 {
            p.yd.max(0)
        } else {
            (-p.yd).max(0)
        };
        let consume_yd = vol.min(yd_avail);
        let delta_yd = if signed < 0 { -consume_yd } else { consume_yd };
        p.yd += delta_yd;
        let remain = vol - consume_yd;
        p.today += if signed < 0 { -remain } else { remain };
    }

    /// 派生某合约的查仓行（含 0 量行）。
    ///
    /// * 今昨区分所：净多→多行（今/昨），净空→空行（今/昨），对侧给 0 行；
    /// * 非今昨所：每个净仓合约给多、空各一行（一侧为净量绝对值，另一侧 0）；
    /// * 含 0 量行（CTP 查仓会回所有方向/日期组合），上层自行过滤。
    pub fn derive_rows(&self, instrument: &str) -> Vec<PositionRow> {
        let Some(p) = self.pos.get(instrument) else {
            // 无持仓也回 0 量行（多/空各一），供上层建仓前对账。
            let spec = self.spec_of(instrument);
            return vec![
                Self::zero_row(&spec, POSI_LONG),
                Self::zero_row(&spec, POSI_SHORT),
            ];
        };

        let mk = |dir: char, date: char, pos: i32, yd: i32| PositionRow {
            instrument_id: p.instrument_id.clone(),
            exchange_id: p.exchange_id.clone(),
            posi_direction: dir,
            position_date: date,
            position: pos,
            today_position: if date == POS_DATE_TODAY { pos } else { 0 },
            yd_position: if date == POS_DATE_YESTERDAY { yd } else { 0 },
            position_cost: p.avg_open_price * pos as f64 * p.volume_multiple as f64,
            use_margin: {
                let ratio = if dir == POSI_LONG {
                    p.long_margin_ratio
                } else {
                    p.short_margin_ratio
                };
                p.avg_open_price * pos as f64 * p.volume_multiple as f64 * ratio
            },
            // 持仓盈亏需要行情重估；台账无最新价，先给 0（行情驱动重估在服务侧）。
            position_profit: 0.0,
            volume_multiple: p.volume_multiple,
        };

        let mut rows = Vec::new();
        if p.today_yesterday_split {
            let net = p.net();
            if net >= 0 {
                // 净多：多行给今/昨；空行给 0。
                rows.push(mk(POSI_LONG, POS_DATE_TODAY, p.today.max(0), 0));
                rows.push(mk(POSI_LONG, POS_DATE_YESTERDAY, p.yd.max(0), p.yd.max(0)));
                rows.push(Self::zero_row_from(p, POSI_SHORT));
            } else {
                rows.push(Self::zero_row_from(p, POSI_LONG));
                rows.push(mk(POSI_SHORT, POS_DATE_TODAY, (-p.today).max(0), 0));
                rows.push(mk(
                    POSI_SHORT,
                    POS_DATE_YESTERDAY,
                    (-p.yd).max(0),
                    (-p.yd).max(0),
                ));
            }
        } else {
            let net = p.net();
            if net >= 0 {
                rows.push(mk(POSI_LONG, POS_DATE_TODAY, net, 0));
                rows.push(Self::zero_row_from(p, POSI_SHORT));
            } else {
                rows.push(Self::zero_row_from(p, POSI_LONG));
                rows.push(mk(POSI_SHORT, POS_DATE_TODAY, -net, 0));
            }
        }
        rows
    }

    fn zero_row(spec: &InstrumentSpec, dir: char) -> PositionRow {
        PositionRow {
            instrument_id: spec.instrument_id.clone(),
            exchange_id: spec.exchange_id.clone(),
            posi_direction: dir,
            position_date: POS_DATE_TODAY,
            position: 0,
            today_position: 0,
            yd_position: 0,
            position_cost: 0.0,
            use_margin: 0.0,
            position_profit: 0.0,
            volume_multiple: spec.volume_multiple,
        }
    }

    fn zero_row_from(p: &InstrumentPos, dir: char) -> PositionRow {
        PositionRow {
            instrument_id: p.instrument_id.clone(),
            exchange_id: p.exchange_id.clone(),
            posi_direction: dir,
            position_date: POS_DATE_TODAY,
            position: 0,
            today_position: 0,
            yd_position: 0,
            position_cost: 0.0,
            use_margin: 0.0,
            position_profit: 0.0,
            volume_multiple: p.volume_multiple,
        }
    }

    /// 全部有账合约（确定性顺序）。
    pub fn instruments(&self) -> Vec<String> {
        self.pos.keys().cloned().collect()
    }

    /// 当前净仓（测试/控制面 ledger 用）。
    pub fn net_of(&self, instrument: &str) -> i32 {
        self.pos.get(instrument).map(|p| p.net()).unwrap_or(0)
    }

    /// 派生全部有账合约的持仓行（确定性顺序，控制面 ledger 快照用）。
    pub fn all_rows(&self) -> Vec<PositionRow> {
        self.instruments()
            .iter()
            .flat_map(|i| self.derive_rows(i))
            .collect()
    }
}

// ---------------- 委托账本（查单/撤单两路定位） ----------------

/// 成交流水记录（当日成交查询用）。
#[derive(Debug, Clone, PartialEq)]
pub struct TradeRecord {
    pub order_ref: String,
    pub trade_id: String,
    pub instrument_id: String,
    pub exchange_id: String,
    pub direction: char,
    pub offset: char,
    pub price: f64,
    pub volume: i32,
}

/// 委托生命周期状态（仅保留 mock 需要的字段）。
#[derive(Debug, Clone, PartialEq)]
pub struct OrderRecord {
    pub order_ref: String,
    pub order_sys_id: String,
    pub instrument_id: String,
    pub exchange_id: String,
    pub front_id: i32,
    pub session_id: i32,
    pub direction: char,
    pub offset: char,
    /// 价格条件 '1' 市价 / '2' 限价（回报 OrderPriceType 回填）。
    pub price_type: char,
    pub limit_price: f64,
    pub volume_total: i32,
    pub volume_traded: i32,
    /// CTP 状态字符 '0'/'1'/'3'/'5'。
    pub status: char,
}

impl OrderRecord {
    /// 是否处于可撤状态（未成交排队/部分成交排队）。
    pub fn is_cancellable(&self) -> bool {
        matches!(self.status, '3' | '1')
    }
}

/// 委托账本：按 OrderRef 与 OrderSysID 双索引。
#[derive(Debug, Default)]
pub struct OrderLedger {
    by_ref: BTreeMap<String, OrderRecord>,
}

impl OrderLedger {
    pub fn new() -> Self {
        Self::default()
    }

    /// 录入一笔委托（初始状态由调用方给，通常 '3'）。
    pub fn insert(&mut self, rec: OrderRecord) {
        self.by_ref.insert(rec.order_ref.clone(), rec);
    }

    pub fn get_by_ref(&self, order_ref: &str) -> Option<&OrderRecord> {
        self.by_ref.get(order_ref)
    }

    /// 三元组定位（Front+Session+OrderRef）。
    pub fn find_by_triple(
        &self,
        front_id: i32,
        session_id: i32,
        order_ref: &str,
    ) -> Option<&OrderRecord> {
        self.by_ref
            .get(order_ref)
            .filter(|o| o.front_id == front_id && o.session_id == session_id)
    }

    /// 系统号定位（Exchange+OrderSysID）：线性匹配（账本量级小）。
    pub fn find_by_sys_id(&self, order_sys_id: &str) -> Option<&OrderRecord> {
        self.by_ref
            .values()
            .find(|o| o.order_sys_id == order_sys_id)
    }

    /// 可变借用（成交累计/撤单终态）。
    pub fn record_mut(&mut self, order_ref: &str) -> Option<&mut OrderRecord> {
        self.by_ref.get_mut(order_ref)
    }

    /// 本会话全部委托（确定性顺序，供查单逐行回放）。
    pub fn all(&self) -> Vec<&OrderRecord> {
        self.by_ref.values().collect()
    }

    /// 按合约过滤（空串=全部）。
    pub fn query(&self, instrument: &str) -> Vec<&OrderRecord> {
        self.by_ref
            .values()
            .filter(|o| instrument.trim().is_empty() || o.instrument_id == instrument)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shfe_ledger() -> PositionLedger {
        let mut l = PositionLedger::new();
        l.register_spec(InstrumentSpec::shfe("rb2510"));
        l
    }

    fn generic_ledger() -> PositionLedger {
        let mut l = PositionLedger::new();
        l.register_spec(InstrumentSpec::generic("IF2510", "CFFEX"));
        l
    }

    #[test]
    fn buy_open_accumulates_long_today() {
        let mut l = shfe_ledger();
        l.apply_fill(&Fill {
            instrument_id: "rb2510".into(),
            direction: DIR_BUY,
            offset: OFFSET_OPEN,
            price: 3200.0,
            volume: 2,
        });
        assert_eq!(l.net_of("rb2510"), 2);
        let p = l.instruments();
        assert_eq!(p, vec!["rb2510".to_string()]);
    }

    #[test]
    fn sell_open_accumulates_short_today() {
        let mut l = shfe_ledger();
        l.apply_fill(&Fill {
            instrument_id: "rb2510".into(),
            direction: DIR_SELL,
            offset: OFFSET_OPEN,
            price: 3200.0,
            volume: 3,
        });
        assert_eq!(l.net_of("rb2510"), -3);
    }

    #[test]
    fn weighted_avg_open_price() {
        let mut l = shfe_ledger();
        l.apply_fill(&Fill {
            instrument_id: "rb2510".into(),
            direction: DIR_BUY,
            offset: OFFSET_OPEN,
            price: 3200.0,
            volume: 2,
        });
        l.apply_fill(&Fill {
            instrument_id: "rb2510".into(),
            direction: DIR_BUY,
            offset: OFFSET_OPEN,
            price: 3300.0,
            volume: 2,
        });
        // (3200*2 + 3300*2)/4 = 3250
        let rows = l.derive_rows("rb2510");
        let long_today = rows
            .iter()
            .find(|r| r.posi_direction == POSI_LONG && r.position_date == POS_DATE_TODAY)
            .unwrap();
        assert!((long_today.position as f64 - 4.0).abs() < 1e-9);
    }

    #[test]
    fn shfe_close_today_reduces_then_close_yesterday_reduces_yd() {
        let mut l = shfe_ledger();
        l.seed("rb2510", true, 3, 1, 2, 3200.0); // 今1 昨2
                                                 // 平今 1
        l.apply_fill(&Fill {
            instrument_id: "rb2510".into(),
            direction: DIR_SELL,
            offset: OFFSET_CLOSE_TODAY,
            price: 3210.0,
            volume: 1,
        });
        // 平昨 1（买/卖方向与多头相反 → sell）
        l.apply_fill(&Fill {
            instrument_id: "rb2510".into(),
            direction: DIR_SELL,
            offset: OFFSET_CLOSE_YESTERDAY,
            price: 3210.0,
            volume: 1,
        });
        assert_eq!(l.net_of("rb2510"), 1);
        let rows = l.derive_rows("rb2510");
        let long_today = rows
            .iter()
            .find(|r| r.posi_direction == POSI_LONG && r.position_date == POS_DATE_TODAY)
            .unwrap();
        let long_yd = rows
            .iter()
            .find(|r| r.posi_direction == POSI_LONG && r.position_date == POS_DATE_YESTERDAY)
            .unwrap();
        assert_eq!(long_today.position, 0);
        assert_eq!(long_yd.position, 1);
    }

    #[test]
    fn generic_exchange_close_is_net_and_single_date() {
        let mut l = generic_ledger();
        l.apply_fill(&Fill {
            instrument_id: "IF2510".into(),
            direction: DIR_BUY,
            offset: OFFSET_OPEN,
            price: 4000.0,
            volume: 3,
        });
        // 非今昨所平今/平昨都减净仓。
        l.apply_fill(&Fill {
            instrument_id: "IF2510".into(),
            direction: DIR_SELL,
            offset: OFFSET_CLOSE_TODAY,
            price: 4010.0,
            volume: 1,
        });
        assert_eq!(l.net_of("IF2510"), 2);
        let rows = l.derive_rows("IF2510");
        // 净多：多行有量（Today），空行为 0；不产生 YESTERDAY 行。
        assert!(rows.iter().all(|r| r.position_date == POS_DATE_TODAY));
        let long = rows.iter().find(|r| r.posi_direction == POSI_LONG).unwrap();
        assert_eq!(long.position, 2);
        let short = rows
            .iter()
            .find(|r| r.posi_direction == POSI_SHORT)
            .unwrap();
        assert_eq!(short.position, 0);
    }

    #[test]
    fn derive_rows_includes_zero_and_long_short_sides() {
        let mut l = shfe_ledger();
        l.apply_fill(&Fill {
            instrument_id: "rb2510".into(),
            direction: DIR_BUY,
            offset: OFFSET_OPEN,
            price: 3200.0,
            volume: 2,
        });
        let rows = l.derive_rows("rb2510");
        // 净多 SHFE：多今、多昨(0)、空(0) 三行；含 0 量行。
        assert_eq!(rows.len(), 3);
        assert!(rows
            .iter()
            .any(|r| r.posi_direction == POSI_SHORT && r.position == 0));
        assert!(rows.iter().any(|r| r.posi_direction == POSI_LONG
            && r.position_date == POS_DATE_YESTERDAY
            && r.position == 0));
    }

    #[test]
    fn no_position_yields_zero_rows_for_both_sides() {
        let l = shfe_ledger();
        let rows = l.derive_rows("rb2510");
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.position == 0));
    }

    #[test]
    fn margin_and_cost_fields_are_nonzero_and_self_consistent() {
        let mut l = shfe_ledger();
        l.apply_fill(&Fill {
            instrument_id: "rb2510".into(),
            direction: DIR_BUY,
            offset: OFFSET_OPEN,
            price: 3200.0,
            volume: 2,
        });
        let rows = l.derive_rows("rb2510");
        let long = rows
            .iter()
            .find(|r| r.posi_direction == POSI_LONG && r.position == 2)
            .unwrap();
        // cost = 3200 * 2 * 10 = 64000；margin = cost * 0.1 = 6400。
        assert!((long.position_cost - 64_000.0).abs() < 1e-6);
        assert!((long.use_margin - 6_400.0).abs() < 1e-6);
    }

    #[test]
    fn order_ledger_triple_and_sysid_lookup_and_cancellable() {
        let mut l = OrderLedger::new();
        l.insert(OrderRecord {
            order_ref: "7".into(),
            order_sys_id: "    20261015-1".into(),
            instrument_id: "rb2510".into(),
            exchange_id: "SHFE".into(),
            front_id: 1,
            session_id: 7001,
            direction: DIR_BUY,
            offset: OFFSET_OPEN,
            price_type: '2',
            limit_price: 3200.0,
            volume_total: 2,
            volume_traded: 0,
            status: '3',
        });
        assert!(l.find_by_triple(1, 7001, "7").is_some());
        assert!(l.find_by_triple(9, 7001, "7").is_none());
        assert!(l.find_by_sys_id("    20261015-1").is_some());
        assert!(l.find_by_sys_id("nope").is_none());
        assert!(l.get_by_ref("7").unwrap().is_cancellable());
    }

    #[test]
    fn terminal_orders_are_not_cancellable() {
        let mut l = OrderLedger::new();
        l.insert(OrderRecord {
            order_ref: "8".into(),
            order_sys_id: "s".into(),
            instrument_id: "rb2510".into(),
            exchange_id: "SHFE".into(),
            front_id: 1,
            session_id: 1,
            direction: DIR_BUY,
            offset: OFFSET_OPEN,
            price_type: '2',
            limit_price: 1.0,
            volume_total: 1,
            volume_traded: 1,
            status: '0',
        });
        assert!(!l.get_by_ref("8").unwrap().is_cancellable());
    }

    #[test]
    fn order_query_filters_by_instrument() {
        let mut l = OrderLedger::new();
        for (r#ref, inst) in [("1", "rb2510"), ("2", "IF2510")] {
            l.insert(OrderRecord {
                order_ref: r#ref.into(),
                order_sys_id: "s".into(),
                instrument_id: inst.into(),
                exchange_id: "".into(),
                front_id: 1,
                session_id: 1,
                direction: DIR_BUY,
                offset: OFFSET_OPEN,
                price_type: '2',
                limit_price: 1.0,
                volume_total: 1,
                volume_traded: 0,
                status: '3',
            });
        }
        assert_eq!(l.query("rb2510").len(), 1);
        assert_eq!(l.query("").len(), 2);
    }
}
