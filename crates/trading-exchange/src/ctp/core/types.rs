//! CTP 领域枚举及其与柜台单字符常量的双向映射。
//!
//! CTP 大量字段是 `char`（C++ 单引号常量）。这里为 MVP 涉及的字段建立
//! 类型安全枚举：`code()` 编码为柜台字符，`from_code()` 反向解析。

/// 为「单字符编码」枚举生成类型定义与 `code/from_code` 双向映射，消除重复 match。
macro_rules! coded_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $($variant:ident = $code:expr),* $(,)?
        }
    ) => {
        $(#[$meta])*
        $vis enum $name {
            $($variant),*
        }

        impl $name {
            /// 编码为 CTP 单字符常量。
            pub fn code(self) -> char {
                match self {
                    $($name::$variant => $code),*
                }
            }

            /// 由 CTP 字符解析；未知字符返回 `None`。
            pub fn from_code(code: char) -> Option<Self> {
                match code {
                    $($code => Some($name::$variant)),*,
                    _ => None,
                }
            }
        }
    };
}

coded_enum!(
    /// 买卖方向。
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Direction {
        Buy = '0',
        Sell = '1',
    }
);

coded_enum!(
    /// 开平标志（平今/平昨主要用于上期所/能源中心）。
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Offset {
        Open = '0',
        Close = '1',
        CloseToday = '3',
        CloseYesterday = '4',
    }
);

coded_enum!(
    /// 投机套保标志。
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum HedgeFlag {
        Speculation = '1',
        Arbitrage = '2',
        Hedge = '3',
    }
);

coded_enum!(
    /// 报单价格条件（MVP 仅用市价/限价）。
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum PriceType {
        AnyPrice = '1',
        LimitPrice = '2',
    }
);

coded_enum!(
    /// 有效期类型。
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum TimeCondition {
        Ioc = '1',
        Gfs = '2',
        Gfd = '3',
        Gtd = '4',
    }
);

coded_enum!(
    /// 成交量类型。
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum VolumeCondition {
        Av = '1',
        Cv = '3',
    }
);

coded_enum!(
    /// 触发条件（普通单恒为「立即」）。
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ContingentCondition {
        Immediately = '1',
    }
);

coded_enum!(
    /// 强平原因（普通单为「非强平」）。
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ForceCloseReason {
        NotForceClose = '0',
    }
);

coded_enum!(
    /// 委托状态（回报侧；v6.7.13：数字 0-5 + 字母 a/b/c）。
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum OrderStatus {
        AllTraded = '0',
        PartTradedQueueing = '1',
        PartTradedNotQueueing = '2',
        NoTradeQueueing = '3',
        NoTradeNotQueueing = '4',
        Cancelled = '5',
        Unknown = 'a',
        NotTouched = 'b',
        Touched = 'c',
    }
);

impl OrderStatus {
    /// 是否为终态（全部成交/撤销）。非排队的部分成交等仍可能变化，不算终态。
    pub fn is_terminal(self) -> bool {
        matches!(self, OrderStatus::AllTraded | OrderStatus::Cancelled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Direction 买卖方向 ----

    #[test]
    fn direction_buy_maps_to_char_0() {
        assert_eq!(Direction::Buy.code(), '0');
    }

    #[test]
    fn direction_sell_maps_to_char_1() {
        assert_eq!(Direction::Sell.code(), '1');
    }

    #[test]
    fn direction_parses_known_codes() {
        assert_eq!(Direction::from_code('0'), Some(Direction::Buy));
        assert_eq!(Direction::from_code('1'), Some(Direction::Sell));
    }

    #[test]
    fn direction_returns_none_for_unknown_code() {
        assert_eq!(Direction::from_code('9'), None);
    }

    // ---- Offset 开平标志 ----

    #[test]
    fn offset_maps_to_ctp_codes() {
        assert_eq!(Offset::Open.code(), '0');
        assert_eq!(Offset::Close.code(), '1');
        assert_eq!(Offset::CloseToday.code(), '3');
        assert_eq!(Offset::CloseYesterday.code(), '4');
    }

    #[test]
    fn offset_parses_known_codes() {
        assert_eq!(Offset::from_code('0'), Some(Offset::Open));
        assert_eq!(Offset::from_code('3'), Some(Offset::CloseToday));
        assert_eq!(Offset::from_code('4'), Some(Offset::CloseYesterday));
    }

    #[test]
    fn offset_returns_none_for_unknown_code() {
        assert_eq!(Offset::from_code('z'), None);
    }

    // ---- HedgeFlag 投机套保标志 ----

    #[test]
    fn hedge_flag_maps_to_ctp_codes() {
        assert_eq!(HedgeFlag::Speculation.code(), '1');
        assert_eq!(HedgeFlag::Arbitrage.code(), '2');
        assert_eq!(HedgeFlag::Hedge.code(), '3');
    }

    #[test]
    fn hedge_flag_parses_known_codes() {
        assert_eq!(HedgeFlag::from_code('1'), Some(HedgeFlag::Speculation));
        assert_eq!(HedgeFlag::from_code('3'), Some(HedgeFlag::Hedge));
    }

    #[test]
    fn hedge_flag_returns_none_for_unknown_code() {
        assert_eq!(HedgeFlag::from_code('0'), None);
    }

    // ---- PriceType 报单价格条件 ----

    #[test]
    fn price_type_maps_to_ctp_codes() {
        assert_eq!(PriceType::AnyPrice.code(), '1');
        assert_eq!(PriceType::LimitPrice.code(), '2');
    }

    #[test]
    fn price_type_parses_known_codes() {
        assert_eq!(PriceType::from_code('2'), Some(PriceType::LimitPrice));
        assert_eq!(PriceType::from_code('1'), Some(PriceType::AnyPrice));
    }

    // ---- TimeCondition 有效期类型 ----

    #[test]
    fn time_condition_maps_to_ctp_codes() {
        assert_eq!(TimeCondition::Ioc.code(), '1');
        assert_eq!(TimeCondition::Gfs.code(), '2');
        assert_eq!(TimeCondition::Gfd.code(), '3');
        assert_eq!(TimeCondition::Gtd.code(), '4');
    }

    #[test]
    fn time_condition_parses_known_codes() {
        assert_eq!(TimeCondition::from_code('3'), Some(TimeCondition::Gfd));
        assert_eq!(TimeCondition::from_code('1'), Some(TimeCondition::Ioc));
    }

    // ---- VolumeCondition 成交量类型 ----

    #[test]
    fn volume_condition_maps_to_ctp_codes() {
        assert_eq!(VolumeCondition::Av.code(), '1');
        assert_eq!(VolumeCondition::Cv.code(), '3');
    }

    #[test]
    fn volume_condition_parses_known_codes() {
        assert_eq!(VolumeCondition::from_code('1'), Some(VolumeCondition::Av));
        assert_eq!(VolumeCondition::from_code('3'), Some(VolumeCondition::Cv));
    }

    // ---- ContingentCondition 触发条件（只关心"立即"）----

    #[test]
    fn contingent_condition_immediately_maps_to_char_1() {
        assert_eq!(ContingentCondition::Immediately.code(), '1');
    }

    #[test]
    fn contingent_condition_parses_immediately() {
        assert_eq!(
            ContingentCondition::from_code('1'),
            Some(ContingentCondition::Immediately)
        );
    }

    // ---- ForceCloseReason 强平原因 ----

    #[test]
    fn force_close_reason_not_force_close_maps_to_char_0() {
        assert_eq!(ForceCloseReason::NotForceClose.code(), '0');
    }

    #[test]
    fn force_close_reason_parses_not_force_close() {
        assert_eq!(
            ForceCloseReason::from_code('0'),
            Some(ForceCloseReason::NotForceClose)
        );
    }

    // ---- OrderStatus 委托状态（回报侧；v6.7.13：数字 0-5 + 字母 a/b/c）----

    #[test]
    fn order_status_parses_all_ctp_status_chars() {
        assert_eq!(OrderStatus::from_code('0'), Some(OrderStatus::AllTraded));
        assert_eq!(
            OrderStatus::from_code('1'),
            Some(OrderStatus::PartTradedQueueing)
        );
        assert_eq!(
            OrderStatus::from_code('2'),
            Some(OrderStatus::PartTradedNotQueueing)
        );
        assert_eq!(
            OrderStatus::from_code('3'),
            Some(OrderStatus::NoTradeQueueing)
        );
        assert_eq!(
            OrderStatus::from_code('4'),
            Some(OrderStatus::NoTradeNotQueueing)
        );
        assert_eq!(OrderStatus::from_code('5'), Some(OrderStatus::Cancelled));
        assert_eq!(OrderStatus::from_code('a'), Some(OrderStatus::Unknown));
        assert_eq!(OrderStatus::from_code('b'), Some(OrderStatus::NotTouched));
        assert_eq!(OrderStatus::from_code('c'), Some(OrderStatus::Touched));
    }

    #[test]
    fn order_status_is_terminal_only_for_final_states() {
        assert!(OrderStatus::AllTraded.is_terminal());
        assert!(OrderStatus::Cancelled.is_terminal());
        assert!(!OrderStatus::NoTradeQueueing.is_terminal());
        assert!(!OrderStatus::PartTradedQueueing.is_terminal());
    }

    #[test]
    fn order_status_returns_none_for_unknown_code() {
        assert_eq!(OrderStatus::from_code('9'), None);
    }
}
