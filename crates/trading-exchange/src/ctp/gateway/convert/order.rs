//! `OrderRequest` → `CThostFtdcInputOrderField` 字段映射。

use crate::ctp::core::account::CtpAccount;
use crate::ctp::core::encoding::encode_fixed;
use crate::ctp::core::order::OrderRequest;
use crate::ctp::mailbox::CancelTarget;
use ctp2rs::v1alpha1::{CThostFtdcInputOrderActionField, CThostFtdcInputOrderField};

/// 撤单操作标志：删除（CTP 字符常量 `THOST_FTDC_AF_Delete` = `'0'`）。
const ACTION_FLAG_DELETE: std::os::raw::c_char = b'0' as std::os::raw::c_char;

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

/// 依据账户、合约、撤单定位与操作引用，构造柜台撤单结构。
///
/// 只负责字段搬运（与 [`build_input_order`] 同范式）：
/// * 账户段 BrokerID/InvestorID/UserID 恒填；
/// * `ActionFlag='0'`（Delete 删除）；`OrderActionRef` 由会话状态分配；
/// * 定位二选一：[`CancelTarget::ByOrderRef`] 填 FrontID/SessionID/OrderRef，
///   [`CancelTarget::ByOrderSysId`] 填 ExchangeID/OrderSysID；
/// * InstrumentID 两路都填（柜台校验/回报关联需要）。
pub fn build_order_action(
    account: &CtpAccount,
    instrument: &str,
    target: &CancelTarget,
    action_ref: i32,
) -> CThostFtdcInputOrderActionField {
    let mut field = CThostFtdcInputOrderActionField::default();

    // 账户/合约标识（两路共用）。
    encode_fixed(&mut field.BrokerID, &account.broker_id);
    encode_fixed(&mut field.InvestorID, account.effective_investor_id());
    encode_fixed(&mut field.UserID, &account.user_id);
    encode_fixed(&mut field.InstrumentID, instrument);

    // 撤单语义：删除 + 本次操作引用。
    field.ActionFlag = ACTION_FLAG_DELETE;
    field.OrderActionRef = action_ref;

    // 定位二选一（另一路字段保持 default 空值，绝不两路混填）。
    match target {
        CancelTarget::ByOrderRef {
            front_id,
            session_id,
            order_ref,
        } => {
            field.FrontID = *front_id;
            field.SessionID = *session_id;
            encode_fixed(&mut field.OrderRef, order_ref);
        }
        CancelTarget::ByOrderSysId {
            exchange_id,
            order_sys_id,
        } => {
            encode_fixed(&mut field.ExchangeID, exchange_id);
            encode_fixed(&mut field.OrderSysID, order_sys_id);
        }
    }
    field
}

#[cfg(test)]
mod tests {
    use super::{build_input_order, build_order_action};
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

    // ---- 撤单结构装配 ----

    use crate::ctp::mailbox::CancelTarget;

    #[test]
    fn action_sets_account_ids_instrument_and_delete_flag() {
        // Arrange
        let target = CancelTarget::ByOrderRef {
            front_id: 1,
            session_id: 2,
            order_ref: "12".into(),
        };

        // Act
        let f = build_order_action(&account(), "rb2510", &target, 7);

        // Assert：账户段、合约、删除标志、操作引用。
        assert_eq!(decode_fixed(&f.BrokerID), "9999");
        assert_eq!(decode_fixed(&f.InvestorID), "123456");
        assert_eq!(decode_fixed(&f.UserID), "123456");
        assert_eq!(decode_fixed(&f.InstrumentID), "rb2510");
        assert_eq!(f.ActionFlag, b'0' as c_char, "ActionFlag 必须是 Delete='0'");
        assert_eq!(f.OrderActionRef, 7);
    }

    #[test]
    fn action_triple_route_fills_front_session_order_ref_only() {
        // Arrange：本会话三元组定位。
        let target = CancelTarget::ByOrderRef {
            front_id: 5,
            session_id: 9001,
            order_ref: "42".into(),
        };

        // Act
        let f = build_order_action(&account(), "rb2510", &target, 3);

        // Assert：三元组填入；跨会话字段留空。
        assert_eq!(f.FrontID, 5);
        assert_eq!(f.SessionID, 9001);
        assert_eq!(decode_fixed(&f.OrderRef), "42");
        assert_eq!(decode_fixed(&f.ExchangeID), "");
        assert_eq!(decode_fixed(&f.OrderSysID), "");
    }

    #[test]
    fn action_sysid_route_fills_exchange_and_sys_id_only() {
        // Arrange：重启后跨会话系统号定位。
        let target = CancelTarget::ByOrderSysId {
            exchange_id: "SHFE".into(),
            order_sys_id: "    20260101-7".into(),
        };

        // Act
        let f = build_order_action(&account(), "rb2510", &target, 9);

        // Assert：交易所/系统号填入；三元组留空（Front/Session=0、OrderRef 空）。
        assert_eq!(decode_fixed(&f.ExchangeID), "SHFE");
        assert_eq!(decode_fixed(&f.OrderSysID), "    20260101-7");
        assert_eq!(f.FrontID, 0);
        assert_eq!(f.SessionID, 0);
        assert_eq!(decode_fixed(&f.OrderRef), "");
        assert_eq!(decode_fixed(&f.InstrumentID), "rb2510");
    }
}
