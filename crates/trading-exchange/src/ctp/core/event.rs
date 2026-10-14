//! 网关对上层产出的事件，以及委托/成交的 owned 快照。
//!
//! 这些类型在 SPI 回调内即时拷贝生成，不含任何指向 C++ 内存的指针，
//! 可安全地跨线程/在回调返回后使用。

use crate::ctp::core::types::{Direction, OrderStatus};

/// 委托快照（柜台 `CThostFtdcOrderField` 的安全副本）。
#[derive(Debug, Clone)]
pub struct OrderSnapshot {
    /// 报单引用（本会话内唯一）。
    pub order_ref: String,
    /// 合约代码。
    pub instrument: String,
    /// 交易所代码。
    pub exchange_id: String,
    /// 报单编号（交易所系统号）。
    pub order_sys_id: String,
    /// 委托时间（`HH:MM:SS`）。
    pub insert_time: String,
    /// 买卖方向。
    pub direction: Direction,
    /// 委托状态。
    pub status: OrderStatus,
    /// 委托价。
    pub limit_price: f64,
    /// 委托总量。
    pub volume_total: i32,
    /// 已成交数量。
    pub volume_traded: i32,
    /// 状态信息（GB18030 解码后的中文文本）。
    pub status_msg: String,
    /// 前置编号。
    pub front_id: i32,
    /// 会话编号。
    pub session_id: i32,
}

impl OrderSnapshot {
    /// 剩余未成交手数。
    pub fn remaining(&self) -> i32 {
        (self.volume_total - self.volume_traded).max(0)
    }

    /// 委托是否已进入终态。
    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }
}

/// 成交快照（柜台 `CThostFtdcTradeField` 的安全副本）。
#[derive(Debug, Clone)]
pub struct TradeSnapshot {
    /// 报单引用（客户端自填，柜台原样回传；cid 关联键）。
    pub order_ref: String,
    /// 成交编号。
    pub trade_id: String,
    /// 合约代码。
    pub instrument: String,
    /// 交易所代码。
    pub exchange_id: String,
    /// 报单系统号。
    pub order_sys_id: String,
    /// 买卖方向。
    pub direction: Direction,
    /// 成交价。
    pub price: f64,
    /// 成交手数。
    pub volume: i32,
    /// 成交日期（`YYYYMMDD`）。
    pub trade_date: String,
    /// 成交时间（`HH:MM:SS`）。
    pub trade_time: String,
}

/// 网关向上层推送的事件。
#[derive(Debug, Clone)]
pub enum CtpEvent {
    /// 前置连接建立（未登录）。
    FrontConnected,
    /// 前置连接断开，附带原因码。
    FrontDisconnected(i32),
    /// 客户端认证应答；错误码为 0 表示通过。
    Authenticated { code: i32, msg: String },
    /// 登录应答成功，附带交易日与系统会话信息。
    Login(LoginInfo),
    /// 结算单确认完成。
    SettlementConfirmed,
    /// 报单回报（委托状态变化）。
    Order(OrderSnapshot),
    /// 成交回报。
    Trade(TradeSnapshot),
    /// 报单被拒/错误回报。
    Error { code: i32, msg: String },
    /// 合约信息（查询应答逐条返回）。
    Instrument(InstrumentInfo),
    /// 持仓信息（查询应答逐条返回；数量为 0 的行柜台也会推送，由上层过滤）。
    Position(PositionSnapshot),
    /// 查询应答结束（`is_last=true`），标志一次分页查询的所有记录已收齐。
    QueryEnd,
}

/// 持仓快照（柜台 `CThostFtdcInvestorPositionField` 的安全副本）。
#[derive(Debug, Clone)]
pub struct PositionSnapshot {
    /// 合约代码。
    pub instrument: String,
    /// 持仓方向（柜台字符：'2'=买/多头，'3'=卖/空头）。
    pub direction: char,
    /// 总持仓（手）。
    pub position: i32,
    /// 昨仓（手）。
    pub yd_position: i32,
    /// 今仓（手）。
    pub today_position: i32,
    /// 持仓成本。
    pub position_cost: f64,
    /// 占用保证金。
    pub use_margin: f64,
    /// 持仓盈亏。
    pub position_profit: f64,
}

/// 合约基础信息（查询应答快照）。
#[derive(Debug, Clone)]
pub struct InstrumentInfo {
    /// 合约代码。
    pub instrument: String,
    /// 交易所代码。
    pub exchange_id: String,
    /// 产品代码。
    pub product_id: String,
    /// 是否交易中。
    pub is_trading: bool,
}

/// 登录成功后从柜台取得的会话信息。
#[derive(Debug, Clone)]
pub struct LoginInfo {
    /// 交易日（`YYYYMMDD`）。
    pub trading_day: String,
    /// 前置编号。
    pub front_id: i32,
    /// 会话编号。
    pub session_id: i32,
    /// 当前最大报单引用（后续 OrderRef 从其递增）。
    pub max_order_ref: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctp::core::types::*;

    fn order(status: OrderStatus, total: i32, traded: i32) -> OrderSnapshot {
        OrderSnapshot {
            order_ref: "1".into(),
            instrument: "rb2510".into(),
            exchange_id: "SHFE".into(),
            order_sys_id: "    1".into(),
            insert_time: "13:30:00".into(),
            direction: Direction::Buy,
            status,
            limit_price: 3205.0,
            volume_total: total,
            volume_traded: traded,
            status_msg: String::new(),
            front_id: 1,
            session_id: 9001,
        }
    }

    #[test]
    fn remaining_volume_is_total_minus_traded() {
        let s = order(OrderStatus::PartTradedQueueing, 3, 1);
        assert_eq!(s.remaining(), 2);
    }

    #[test]
    fn fully_traded_order_has_zero_remaining() {
        let s = order(OrderStatus::AllTraded, 2, 2);
        assert_eq!(s.remaining(), 0);
    }

    #[test]
    fn snapshot_is_terminal_only_when_order_status_is_terminal() {
        assert!(order(OrderStatus::AllTraded, 1, 1).is_terminal());
        assert!(order(OrderStatus::Cancelled, 1, 0).is_terminal());
        assert!(!order(OrderStatus::NoTradeQueueing, 1, 0).is_terminal());
    }

    #[test]
    fn trade_snapshot_carries_fill_fields() {
        let t = TradeSnapshot {
            order_ref: "1".into(),
            trade_id: "42".into(),
            instrument: "rb2510".into(),
            exchange_id: "SHFE".into(),
            order_sys_id: "1".into(),
            direction: Direction::Buy,
            price: 3205.0,
            volume: 1,
            trade_date: "20261009".into(),
            trade_time: "10:30:00".into(),
        };
        assert_eq!(t.trade_id, "42");
        assert_eq!(t.price, 3205.0);
    }
}
