//! `OrderRequest` → `CThostFtdcInputOrderField` 字段映射。

use crate::ctp::core::account::CtpAccount;
use crate::ctp::core::encoding::encode_fixed;
use crate::ctp::core::order::OrderRequest;
use ctp2rs::v1alpha1::CThostFtdcInputOrderField;

/// 依据下单请求、账户与本会话 OrderRef，构造柜台报单录入结构。
///
/// 调用方应已通过 `OrderRequest::validate()`；本函数只负责字段搬运，
/// 未在 `OrderRequest` 中暴露的柜台字段（自动挂起、强平等）一律取安全默认 0。
pub fn build_input_order(
    req: &OrderRequest,
    account: &CtpAccount,
    order_ref: &str,
) -> CThostFtdcInputOrderField {
    let mut field = CThostFtdcInputOrderField::default();

    // 账户/会话标识
    encode_fixed(&mut field.BrokerID, &account.broker_id);
    encode_fixed(&mut field.InvestorID, account.effective_investor_id());
    encode_fixed(&mut field.UserID, &account.user_id);
    encode_fixed(&mut field.OrderRef, order_ref);
    encode_fixed(&mut field.InstrumentID, &req.instrument);

    // 报单要素（char 字段）
    field.OrderPriceType = req.price_type.code() as i8;
    field.Direction = req.direction.code() as i8;
    // 单腿委托：只设置组合标志首字符，其余保留 NUL。
    field.CombOffsetFlag[0] = req.offset.code() as i8;
    field.CombHedgeFlag[0] = req.hedge_flag.code() as i8;

    // 价格/数量
    field.LimitPrice = req.price;
    field.VolumeTotalOriginal = req.volume;

    // 有效期 / 成交量条件
    field.TimeCondition = req.time_condition.code() as i8;
    field.VolumeCondition = crate::ctp::core::types::VolumeCondition::Av.code() as i8;
    field.MinVolume = 1;

    // 普通单：立即触发、非强平、无止损价
    field.ContingentCondition =
        crate::ctp::core::types::ContingentCondition::Immediately.code() as i8;
    field.StopPrice = 0.0;
    field.ForceCloseReason = crate::ctp::core::types::ForceCloseReason::NotForceClose.code() as i8;

    // IsAutoSuspend / UserForceClose / IsSwapOrder 由 default() 置 0。
    field
}

#[cfg(test)]
mod tests {
    use super::build_input_order;
    use crate::ctp::core::account::CtpAccount;
    use crate::ctp::core::encoding::decode_fixed;
    use crate::ctp::core::order::OrderRequest;
    use crate::ctp::core::types::{Direction, Offset};
    use std::os::raw::c_char;

    fn account() -> CtpAccount {
        CtpAccount {
            broker_id: "9999".into(),
            user_id: "123456".into(),
            password: "secret".into(),
            app_id: "simnow_client_test".into(),
            auth_code: "0000000000000000".into(),
            investor_id: "123456".into(),
            front_address: "tcp://182.254.243.31:40001".into(),
            is_production_mode: false,
        }
    }

    fn built() -> ctp2rs::v1alpha1::CThostFtdcInputOrderField {
        let req = OrderRequest::limit("rb2510", Direction::Buy, Offset::Open, 3205.0, 2);
        build_input_order(&req, &account(), "12")
    }

    #[test]
    fn copies_account_identifiers() {
        let f = built();
        assert_eq!(decode_fixed(&f.BrokerID), "9999");
        assert_eq!(decode_fixed(&f.InvestorID), "123456");
        assert_eq!(decode_fixed(&f.UserID), "123456");
    }

    #[test]
    fn sets_order_ref() {
        assert_eq!(decode_fixed(&built().OrderRef), "12");
    }

    #[test]
    fn sets_instrument() {
        assert_eq!(decode_fixed(&built().InstrumentID), "rb2510");
    }

    #[test]
    fn sets_limit_price_type_and_direction() {
        let f = built();
        assert_eq!(f.OrderPriceType, '2' as c_char);
        assert_eq!(f.Direction, '0' as c_char);
    }

    #[test]
    fn sets_open_offset_and_speculation_hedge_flags() {
        let f = built();
        assert_eq!(f.CombOffsetFlag[0], '0' as c_char);
        assert_eq!(f.CombHedgeFlag[0], '1' as c_char);
    }

    #[test]
    fn sets_price_and_volume() {
        let f = built();
        assert_eq!(f.LimitPrice, 3205.0);
        assert_eq!(f.VolumeTotalOriginal, 2);
    }

    #[test]
    fn sets_gfd_time_and_av_volume_condition() {
        let f = built();
        assert_eq!(f.TimeCondition, '3' as c_char);
        assert_eq!(f.VolumeCondition, '1' as c_char);
        assert_eq!(f.MinVolume, 1);
    }

    #[test]
    fn sets_immediate_contingent_and_not_force_close() {
        let f = built();
        assert_eq!(f.ContingentCondition, '1' as c_char);
        assert_eq!(f.StopPrice, 0.0);
        assert_eq!(f.ForceCloseReason, '0' as c_char);
    }

    #[test]
    fn boolean_flags_default_zero() {
        let f = built();
        assert_eq!(f.IsAutoSuspend, 0);
        assert_eq!(f.UserForceClose, 0);
        assert_eq!(f.IsSwapOrder, 0);
    }
}
