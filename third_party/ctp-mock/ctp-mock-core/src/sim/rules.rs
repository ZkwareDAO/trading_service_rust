//! 柜台内置报单规则（纯内核，不触 FFI / 不持台账锁）。
//!
//! 与「成交脚本」不同，这里模拟的是**真实柜台对字段级合法性的固有拒绝**——
//! 不由场景注入、默认常开，且与具体成交策略无关：
//! * **SHFE 不支持市价**（`AnyPrice`）：上期所只收限价，市价单录入即拒
//!   `80 PRICETYPE_NOTSUPPORT_BYEXCHANGE`（真实柜台经 `OnRspOrderInsert` 异步回）；
//!   其他所可发 `AnyPrice+IOC`，原样受理。
//! * **SHFE 不接受笼统平仓 `Close`**：上期所必须显式发平今 `'3'` / 平昨 `'4'`，
//!   跨今昨由客户端拆成两笔调用；笼统 `Close` 录入即拒 `15 BAD_FIELD`。
//! * **SHFE 显式平今/平昨超可平量**：平今量 > 今仓拒 `50`，平昨量 > 昨仓拒 `51`
//!   （可平量由调用方从台账按被平方向读出后注入，本函数保持纯逻辑）。
//!
//! 错误码与文案逐字取自 vendored v6.7.13 `error.xml`（去 `CTP:` 前缀）。
//! 能源中心 INE 规则类似但首版不纳入（与服务端口径一致，见 venue.rs 风险注记）。

use super::ledger::{OFFSET_CLOSE, OFFSET_CLOSE_TODAY, OFFSET_CLOSE_YESTERDAY};
use super::script::{OrderReq, PRICE_ANY};

/// 上期所交易所代码：只有 SHFE 强制显式平今/平昨、不支持市价。
pub const SHFE_EXCHANGE_ID: &str = "SHFE";

/// `BAD_FIELD`：报单字段有误（SHFE 笼统平仓的实测拒绝码）。
pub const ERR_BAD_FIELD: i32 = 15;
/// `OVER_CLOSETODAY_POSITION`：平今仓位不足。
pub const ERR_OVER_CLOSETODAY: i32 = 50;
/// `OVER_CLOSEYESTERDAY_POSITION`：平昨仓位不足。
pub const ERR_OVER_CLOSEYESTERDAY: i32 = 51;
/// `PRICETYPE_NOTSUPPORT_BYEXCHANGE`：交易所不支持的价格类型（SHFE 市价）。
pub const ERR_PRICETYPE_UNSUPPORTED: i32 = 80;

/// 交易所 / 价格类型 / 开平标志层面的**字段级拒单**（不依赖持仓台账）。
///
/// 仅对 SHFE 生效，返回真实 ErrorID；其余交易所（含目录未预热到的空交易所代码，
/// mock 夹具对非 SHFE 前缀给 `exchange_id=""`）恒放行。
pub fn reject_insert_code(req: &OrderReq) -> Option<i32> {
    if req.exchange_id != SHFE_EXCHANGE_ID {
        return None;
    }
    // 价格条件优先：SHFE 不支持 AnyPrice（无论开平）。
    if req.price_type == PRICE_ANY {
        return Some(ERR_PRICETYPE_UNSUPPORTED);
    }
    // SHFE 平仓必须显式平今/平昨，笼统 Close 字段非法。
    if req.offset == OFFSET_CLOSE {
        return Some(ERR_BAD_FIELD);
    }
    None
}

/// 是否需要查台账做可平量校验：仅 SHFE 显式平今/平昨。
///
/// 供前置层决定是否上持仓锁——开仓/非 SHFE/笼统 Close（已被
/// [`reject_insert_code`] 拦）都无需读台账。
pub fn needs_close_avail_check(req: &OrderReq) -> bool {
    req.exchange_id == SHFE_EXCHANGE_ID
        && matches!(req.offset, OFFSET_CLOSE_TODAY | OFFSET_CLOSE_YESTERDAY)
}

/// SHFE 显式平今/平昨的**可平量拒单**（依赖台账今/昨分布）。
///
/// `today_avail` / `yd_avail` 为调用方按**被平方向**（SELL 平多 / BUY 平空）
/// 从台账读出的可平今/昨仓手数（非负）。非 SHFE、开仓、或数量充足时放行。
pub fn reject_close_avail_code(req: &OrderReq, today_avail: i32, yd_avail: i32) -> Option<i32> {
    if req.exchange_id != SHFE_EXCHANGE_ID {
        return None;
    }
    match req.offset {
        OFFSET_CLOSE_TODAY if req.volume > today_avail => Some(ERR_OVER_CLOSETODAY),
        OFFSET_CLOSE_YESTERDAY if req.volume > yd_avail => Some(ERR_OVER_CLOSEYESTERDAY),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::ledger::OFFSET_OPEN;

    const LIMIT: char = '2';

    fn order_req(exchange: &str, price_type: char, offset: char, volume: i32) -> OrderReq {
        OrderReq {
            order_ref: "1".into(),
            instrument: "rb2510".into(),
            exchange_id: exchange.into(),
            direction: '0',
            offset,
            hedge_flag: '1',
            price_type,
            limit_price: if price_type == PRICE_ANY { 0.0 } else { 3200.0 },
            volume,
            time_condition: if price_type == PRICE_ANY { '1' } else { '3' },
        }
    }

    // ---- reject_insert_code：SHFE 市价 → 80 ----

    #[test]
    fn shfe_market_open_is_rejected_with_80() {
        let req = order_req(SHFE_EXCHANGE_ID, PRICE_ANY, OFFSET_OPEN, 2);
        assert_eq!(reject_insert_code(&req), Some(ERR_PRICETYPE_UNSUPPORTED));
    }

    #[test]
    fn shfe_market_close_today_is_still_rejected_with_80_not_close_rule() {
        // 价格条件先判：即使带了合法的显式平今，SHFE 市价仍回 80。
        let req = order_req(SHFE_EXCHANGE_ID, PRICE_ANY, OFFSET_CLOSE_TODAY, 2);
        assert_eq!(reject_insert_code(&req), Some(ERR_PRICETYPE_UNSUPPORTED));
    }

    #[test]
    fn non_shfe_market_is_accepted() {
        // 大商所/中金所等：AnyPrice+IOC 直接受理。
        assert_eq!(
            reject_insert_code(&order_req("DCE", PRICE_ANY, OFFSET_OPEN, 2)),
            None
        );
        assert_eq!(
            reject_insert_code(&order_req("CFFEX", PRICE_ANY, OFFSET_CLOSE, 2)),
            None
        );
        // 目录未预热（空交易所代码）也按非 SHFE 放行，不误伤。
        assert_eq!(
            reject_insert_code(&order_req("", PRICE_ANY, OFFSET_OPEN, 2)),
            None
        );
    }

    // ---- reject_insert_code：SHFE 笼统平仓 → 15 ----

    #[test]
    fn shfe_generic_close_is_rejected_with_15() {
        let req = order_req(SHFE_EXCHANGE_ID, LIMIT, OFFSET_CLOSE, 2);
        assert_eq!(reject_insert_code(&req), Some(ERR_BAD_FIELD));
    }

    #[test]
    fn shfe_limit_open_and_explicit_close_today_yesterday_pass() {
        assert_eq!(
            reject_insert_code(&order_req(SHFE_EXCHANGE_ID, LIMIT, OFFSET_OPEN, 2)),
            None
        );
        assert_eq!(
            reject_insert_code(&order_req(SHFE_EXCHANGE_ID, LIMIT, OFFSET_CLOSE_TODAY, 2)),
            None
        );
        assert_eq!(
            reject_insert_code(&order_req(
                SHFE_EXCHANGE_ID,
                LIMIT,
                OFFSET_CLOSE_YESTERDAY,
                2
            )),
            None
        );
    }

    #[test]
    fn non_shfe_generic_close_passes() {
        assert_eq!(
            reject_insert_code(&order_req("DCE", LIMIT, OFFSET_CLOSE, 2)),
            None
        );
    }

    // ---- reject_close_avail_code：50/51 ----

    #[test]
    fn shfe_close_today_over_today_position_is_50() {
        let req = order_req(SHFE_EXCHANGE_ID, LIMIT, OFFSET_CLOSE_TODAY, 2);
        // 今仓仅 1：平 2 拒 50。
        assert_eq!(
            reject_close_avail_code(&req, 1, 3),
            Some(ERR_OVER_CLOSETODAY)
        );
        // 今仓充足：放行（不看昨仓）。
        assert_eq!(reject_close_avail_code(&req, 2, 0), None);
    }

    #[test]
    fn shfe_close_yesterday_over_yd_position_is_51() {
        let req = order_req(SHFE_EXCHANGE_ID, LIMIT, OFFSET_CLOSE_YESTERDAY, 3);
        assert_eq!(
            reject_close_avail_code(&req, 5, 2),
            Some(ERR_OVER_CLOSEYESTERDAY)
        );
        assert_eq!(reject_close_avail_code(&req, 0, 3), None);
    }

    #[test]
    fn close_avail_rule_silent_for_non_shfe_and_open() {
        // 非 SHFE 即使超量也不在本模拟器校验（净额平仓路径零风控，保持现状）。
        let req = order_req("DCE", LIMIT, OFFSET_CLOSE, 5);
        assert_eq!(reject_close_avail_code(&req, 0, 0), None);
        let open = order_req(SHFE_EXCHANGE_ID, LIMIT, OFFSET_OPEN, 5);
        assert_eq!(reject_close_avail_code(&open, 0, 0), None);
    }

    #[test]
    fn needs_avail_check_only_for_shfe_explicit_today_yesterday() {
        use crate::sim::ledger::OFFSET_CLOSE;
        assert!(needs_close_avail_check(&order_req(
            SHFE_EXCHANGE_ID,
            LIMIT,
            OFFSET_CLOSE_TODAY,
            1
        )));
        assert!(needs_close_avail_check(&order_req(
            SHFE_EXCHANGE_ID,
            LIMIT,
            OFFSET_CLOSE_YESTERDAY,
            1
        )));
        // SHFE 开仓/笼统平仓、非 SHFE 显式平今都不需要查台账。
        assert!(!needs_close_avail_check(&order_req(
            SHFE_EXCHANGE_ID,
            LIMIT,
            OFFSET_OPEN,
            1
        )));
        assert!(!needs_close_avail_check(&order_req(
            SHFE_EXCHANGE_ID,
            LIMIT,
            OFFSET_CLOSE,
            1
        )));
        assert!(!needs_close_avail_check(&order_req(
            "DCE",
            LIMIT,
            OFFSET_CLOSE_TODAY,
            1
        )));
    }

    #[test]
    fn shfe_zero_volume_close_does_not_fire_avail_rule() {
        // 边界：可平量 0 但下单量也是 0 时不触发 50/51（规则只拒绝「超过」可平量的正向超量；
        // 0 量单的字段合法性由其他环节把关）。
        let today = order_req(SHFE_EXCHANGE_ID, LIMIT, OFFSET_CLOSE_TODAY, 0);
        assert_eq!(reject_close_avail_code(&today, 0, 0), None);
        let yd = order_req(SHFE_EXCHANGE_ID, LIMIT, OFFSET_CLOSE_YESTERDAY, 0);
        assert_eq!(reject_close_avail_code(&yd, 0, 0), None);
    }
}
