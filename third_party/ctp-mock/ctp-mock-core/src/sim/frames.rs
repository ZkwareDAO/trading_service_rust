//! bindgen 回报帧构造：领域台账记录 → `CThostFtdc*Field`（FFI 回调直接借用）。
//!
//! 集中字段填充，保证 13/21/9/81B 位宽与字符码与真实柜台一致；纯构造、可 L1 单测。
//! 所有帧在回调线程上临时构造、即造即用，不跨回调持有指针。

use ctp2rs::v1alpha1::{
    CThostFtdcInstrumentField, CThostFtdcInvestorPositionField, CThostFtdcOrderField,
    CThostFtdcRspInfoField, CThostFtdcTradeField, TThostFtdcErrorMsgType,
};

use crate::fields::put_fixed;
use crate::fields::put_gb18030;
use crate::sim::ledger::{InstrumentSpec, OrderRecord, PositionRow};

/// 投机套保标志字符。
const HEDGE_SPECULATION: char = '1';
/// 期货产品类字符（`THOST_FTDC_PC_Futures`）。
const PRODUCT_FUTURES: char = '1';

/// 构造错误应答帧（中文按 GB18030 写入 81B ErrorMsg）。
pub fn rsp_info(error_id: i32, msg: &str) -> CThostFtdcRspInfoField {
    // 只初始化 81B 报文数组（GB18030 中文），ErrorID 与之一并在字面量给出。
    let mut error_msg: TThostFtdcErrorMsgType = [0; 81];
    put_gb18030(&mut error_msg, msg);
    CThostFtdcRspInfoField {
        ErrorID: error_id,
        ErrorMsg: error_msg,
    }
}

/// 账户三方标识（回报回填 BrokerID/InvestorID/UserID）。
#[derive(Debug, Clone, Default)]
pub struct Party {
    pub broker: String,
    pub investor: String,
    pub user: String,
}

/// 由委托账本记录 + 当前状态/累计成交/信息构造报单回报帧。
#[allow(clippy::too_many_arguments)]
pub fn order_frame(
    rec: &OrderRecord,
    party: &Party,
    status: char,
    volume_traded: i32,
    status_msg: &str,
    trading_day: &str,
    hms: &str,
) -> CThostFtdcOrderField {
    let mut o = CThostFtdcOrderField::default();
    put_fixed(&mut o.BrokerID, &party.broker);
    put_fixed(&mut o.InvestorID, &party.investor);
    put_fixed(&mut o.UserID, &party.user);
    put_fixed(&mut o.OrderRef, &rec.order_ref);
    put_fixed(&mut o.InstrumentID, &rec.instrument_id);
    put_fixed(&mut o.ExchangeID, &rec.exchange_id);
    put_fixed(&mut o.OrderSysID, &rec.order_sys_id);
    o.OrderPriceType = rec.price_type as i8;
    o.Direction = rec.direction as i8;
    o.CombOffsetFlag[0] = rec.offset as i8;
    o.CombHedgeFlag[0] = HEDGE_SPECULATION as i8;
    o.LimitPrice = rec.limit_price;
    o.VolumeTotalOriginal = rec.volume_total;
    o.VolumeTotal = rec.volume_total;
    o.VolumeTraded = volume_traded;
    o.OrderStatus = status as i8;
    o.OrderSubmitStatus = '0' as i8; // 已提交
    o.OrderSource = '0' as i8; // 来自终端
    o.FrontID = rec.front_id;
    o.SessionID = rec.session_id;
    put_fixed(&mut o.TradingDay, trading_day);
    put_fixed(&mut o.InsertDate, trading_day);
    put_fixed(&mut o.InsertTime, hms);
    put_fixed(&mut o.ActiveTime, hms);
    put_fixed(&mut o.UpdateTime, hms);
    put_gb18030(&mut o.StatusMsg, status_msg);
    o
}

/// 由委托记录 + 本笔成交构造成交回报帧。
#[allow(clippy::too_many_arguments)]
pub fn trade_frame(
    rec: &OrderRecord,
    party: &Party,
    trade_id: &str,
    price: f64,
    volume: i32,
    trading_day: &str,
    hms: &str,
) -> CThostFtdcTradeField {
    let mut t = CThostFtdcTradeField::default();
    put_fixed(&mut t.BrokerID, &party.broker);
    put_fixed(&mut t.InvestorID, &party.investor);
    put_fixed(&mut t.OrderRef, &rec.order_ref);
    put_fixed(&mut t.UserID, &party.user);
    put_fixed(&mut t.InstrumentID, &rec.instrument_id);
    put_fixed(&mut t.ExchangeID, &rec.exchange_id);
    put_fixed(&mut t.OrderSysID, &rec.order_sys_id);
    put_fixed(&mut t.TradeID, trade_id);
    t.Direction = rec.direction as i8;
    t.OffsetFlag = rec.offset as i8;
    t.HedgeFlag = HEDGE_SPECULATION as i8;
    t.Price = price;
    t.Volume = volume;
    put_fixed(&mut t.TradingDay, trading_day);
    put_fixed(&mut t.TradeDate, trading_day);
    put_fixed(&mut t.TradeTime, hms);
    t
}

/// 由派生持仓行构造投资者持仓回报帧。
pub fn position_frame(
    row: &PositionRow,
    party: &Party,
    trading_day: &str,
) -> CThostFtdcInvestorPositionField {
    let mut p = CThostFtdcInvestorPositionField::default();
    put_fixed(&mut p.BrokerID, &party.broker);
    put_fixed(&mut p.InvestorID, &party.investor);
    put_fixed(&mut p.InstrumentID, &row.instrument_id);
    put_fixed(&mut p.ExchangeID, &row.exchange_id);
    p.PosiDirection = row.posi_direction as i8;
    p.HedgeFlag = HEDGE_SPECULATION as i8;
    p.PositionDate = row.position_date as i8;
    p.Position = row.position;
    p.TodayPosition = row.today_position;
    p.YdPosition = row.yd_position;
    p.PositionCost = row.position_cost;
    p.UseMargin = row.use_margin;
    p.PositionProfit = row.position_profit;
    put_fixed(&mut p.TradingDay, trading_day);
    p
}

/// 由合约静态属性构造合约查询回报帧。
pub fn instrument_frame(spec: &InstrumentSpec) -> CThostFtdcInstrumentField {
    let mut i = CThostFtdcInstrumentField::default();
    put_fixed(&mut i.InstrumentID, &spec.instrument_id);
    put_fixed(&mut i.ExchangeID, &spec.exchange_id);
    put_fixed(&mut i.ProductID, &spec.product_id);
    put_fixed(&mut i.InstrumentName, &spec.instrument_id);
    i.ProductClass = PRODUCT_FUTURES as i8;
    i.VolumeMultiple = spec.volume_multiple;
    i.PriceTick = 1.0;
    i.IsTrading = 1;
    i.LongMarginRatio = spec.long_margin_ratio;
    i.ShortMarginRatio = spec.short_margin_ratio;
    // 持仓日期类型：'2' 区分今昨（SHFE/INE），'1' 仅今仓（其他所净额）。
    i.PositionDateType = if spec.today_yesterday_split { '2' } else { '1' } as i8;
    i.MaxMarginSideAlgorithm = '0' as i8;
    i
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fields::get_fixed;
    use crate::sim::ledger::{InstrumentSpec, OrderRecord, PositionRow, POSI_LONG, POS_DATE_TODAY};

    fn party() -> Party {
        Party {
            broker: "9999".into(),
            investor: "123456".into(),
            user: "123456".into(),
        }
    }

    fn rec() -> OrderRecord {
        OrderRecord {
            order_ref: "7".into(),
            order_sys_id: "    20261015-1".into(),
            instrument_id: "rb2510".into(),
            exchange_id: "SHFE".into(),
            front_id: 1,
            session_id: 7001,
            direction: '0',
            offset: '0',
            price_type: '2',
            limit_price: 3205.0,
            volume_total: 2,
            volume_traded: 0,
            status: '3',
        }
    }

    #[test]
    fn rsp_info_sets_id_and_gbk_message() {
        let f = rsp_info(25, "撤单找不到相应报单");
        assert_eq!(f.ErrorID, 25);
        // GB18030 中文可经 get_fixed 解回。
        assert_eq!(get_fixed(&f.ErrorMsg), "撤单找不到相应报单");
    }

    #[test]
    fn order_frame_carries_ids_status_and_cumulative_volume() {
        let f = order_frame(&rec(), &party(), '1', 1, "部分成交", "20261015", "09:30:00");
        assert_eq!(get_fixed(&f.OrderRef), "7");
        assert_eq!(get_fixed(&f.OrderSysID), "    20261015-1");
        assert_eq!(f.OrderStatus as u8 as char, '1');
        assert_eq!(f.VolumeTraded, 1);
        assert_eq!(f.VolumeTotalOriginal, 2);
        assert_eq!(f.FrontID, 1);
        assert_eq!(f.SessionID, 7001);
        assert_eq!(get_fixed(&f.TradingDay), "20261015");
        assert_eq!(get_fixed(&f.StatusMsg), "部分成交");
    }

    #[test]
    fn trade_frame_carries_fill_and_21b_ids() {
        let f = trade_frame(
            &rec(),
            &party(),
            "    20261015-1",
            3205.0,
            2,
            "20261015",
            "09:30:00",
        );
        assert_eq!(get_fixed(&f.TradeID), "    20261015-1");
        assert_eq!(f.Price, 3205.0);
        assert_eq!(f.Volume, 2);
        assert_eq!(get_fixed(&f.TradeDate), "20261015");
    }

    #[test]
    fn position_frame_maps_row_fields() {
        let row = PositionRow {
            instrument_id: "rb2510".into(),
            exchange_id: "SHFE".into(),
            posi_direction: POSI_LONG,
            position_date: POS_DATE_TODAY,
            position: 2,
            today_position: 2,
            yd_position: 0,
            position_cost: 64_000.0,
            use_margin: 6_400.0,
            position_profit: 0.0,
            volume_multiple: 10,
        };
        let f = position_frame(&row, &party(), "20261015");
        assert_eq!(get_fixed(&f.InstrumentID), "rb2510");
        assert_eq!(f.PosiDirection as u8 as char, POSI_LONG);
        assert_eq!(f.Position, 2);
        assert_eq!(f.TodayPosition, 2);
        assert!((f.UseMargin - 6_400.0).abs() < 1e-9);
    }

    #[test]
    fn instrument_frame_carries_spec_attributes() {
        let f = instrument_frame(&InstrumentSpec::shfe("rb2510"));
        assert_eq!(get_fixed(&f.InstrumentID), "rb2510");
        assert_eq!(get_fixed(&f.ExchangeID), "SHFE");
        assert_eq!(f.VolumeMultiple, 10);
        assert_eq!(f.IsTrading, 1);
        assert_eq!(f.PositionDateType as u8 as char, '2');
    }
}
