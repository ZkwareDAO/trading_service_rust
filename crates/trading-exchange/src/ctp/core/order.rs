//! 下单请求领域模型与系统边界校验。

use crate::ctp::core::types::{Direction, HedgeFlag, Offset, PriceType, TimeCondition};

/// 下单要素校验错误（在送入柜台前于边界处快速失败）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ValidationError {
    /// 合约代码为空。
    #[error("合约代码为空")]
    EmptyInstrument,
    /// 限价非法（非有限值或 ≤ 0）。
    #[error("限价非法：须为有限正数")]
    InvalidPrice,
    /// 委托手数非法（≤ 0）。
    #[error("委托手数非法：须为正整数")]
    InvalidVolume,
}

/// 一笔下单请求（领域口径；instrument 为交易所原生合约号）。
#[derive(Debug, Clone)]
pub struct OrderRequest {
    /// 合约代码，如 "rb2510"。
    pub instrument: String,
    /// 买卖方向。
    pub direction: Direction,
    /// 开平标志。
    pub offset: Offset,
    /// 投机套保标志。
    pub hedge_flag: HedgeFlag,
    /// 价格条件（限价/市价）。
    pub price_type: PriceType,
    /// 限价单委托价；市价单不使用。
    pub price: f64,
    /// 委托手数（整数）。
    pub volume: i32,
    /// 有效期类型。
    pub time_condition: TimeCondition,
}

impl OrderRequest {
    /// 构造一笔投机限价 GFD 单（最常见挂单形态）。
    pub fn limit(
        instrument: impl Into<String>,
        direction: Direction,
        offset: Offset,
        price: f64,
        volume: i32,
    ) -> Self {
        Self {
            instrument: instrument.into(),
            direction,
            offset,
            hedge_flag: HedgeFlag::Speculation,
            price_type: PriceType::LimitPrice,
            price,
            volume,
            time_condition: TimeCondition::Gfd,
        }
    }

    /// 系统边界校验：合约非空、限价有效、手数为正。
    pub fn validate(&self) -> std::result::Result<(), ValidationError> {
        if self.instrument.trim().is_empty() {
            return Err(ValidationError::EmptyInstrument);
        }
        // 仅限价单校验价格；市价单价格由柜台撮合，不做约束。
        if self.price_type == PriceType::LimitPrice
            && (!self.price.is_finite() || self.price <= 0.0)
        {
            return Err(ValidationError::InvalidPrice);
        }
        if self.volume <= 0 {
            return Err(ValidationError::InvalidVolume);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctp::core::types::*;

    fn valid_request() -> OrderRequest {
        OrderRequest::limit("rb2510", Direction::Buy, Offset::Open, 3205.0, 1)
    }

    #[test]
    fn limit_constructor_sets_limit_price_type_and_speculation() {
        let req = valid_request();
        assert_eq!(req.instrument, "rb2510");
        assert_eq!(req.price_type, PriceType::LimitPrice);
        assert_eq!(req.hedge_flag, HedgeFlag::Speculation);
        assert_eq!(req.time_condition, TimeCondition::Gfd);
    }

    #[test]
    fn valid_request_passes_validation() {
        assert!(valid_request().validate().is_ok());
    }

    #[test]
    fn empty_instrument_is_rejected() {
        let mut req = valid_request();
        req.instrument = "   ".to_string();
        let err = req.validate().unwrap_err();
        assert_eq!(err, ValidationError::EmptyInstrument);
    }

    #[test]
    fn non_positive_price_is_rejected() {
        let mut req = valid_request();
        req.price = 0.0;
        assert_eq!(req.validate().unwrap_err(), ValidationError::InvalidPrice);
    }

    #[test]
    fn negative_price_is_rejected() {
        let mut req = valid_request();
        req.price = -1.0;
        assert_eq!(req.validate().unwrap_err(), ValidationError::InvalidPrice);
    }

    #[test]
    fn non_finite_price_is_rejected() {
        let mut req = valid_request();
        req.price = f64::NAN;
        assert_eq!(req.validate().unwrap_err(), ValidationError::InvalidPrice);
    }

    #[test]
    fn zero_volume_is_rejected() {
        let mut req = valid_request();
        req.volume = 0;
        assert_eq!(req.validate().unwrap_err(), ValidationError::InvalidVolume);
    }

    #[test]
    fn negative_volume_is_rejected() {
        let mut req = valid_request();
        req.volume = -2;
        assert_eq!(req.validate().unwrap_err(), ValidationError::InvalidVolume);
    }

    #[test]
    fn market_order_without_price_is_allowed() {
        let req = OrderRequest {
            instrument: "rb2510".to_string(),
            direction: Direction::Sell,
            offset: Offset::Close,
            hedge_flag: HedgeFlag::Speculation,
            price_type: PriceType::AnyPrice,
            price: 0.0,
            volume: 1,
            time_condition: TimeCondition::Ioc,
        };
        assert!(req.validate().is_ok());
    }
}
