//! 成交脚本（纯函数）：报单请求 → 回调事件序列；撤单 → 结果。
//!
//! 不撮合、不看盘口：成交与否、分几笔、用什么价，完全由 [`FillScript`] 决定。
//! 输出是与 bindgen 解耦的领域事件，FFI 层只负责搬运成 `on_rtn_order/trade`；
//! 因而可在 L1 快速、确定地断言各 FillMode 与错误注入。
//!
//! CTP 委托状态字符：`'3'` 未成交排队、`'1'` 部分成交排队、`'0'` 全部成交、
//! `'5'` 已撤销；价格条件 `'1'` 市价（AnyPrice）/`'2'` 限价。

/// 市价单的价格条件字符（CTP `THOST_FTDC_OPT_AnyPrice`）。
pub const PRICE_ANY: char = '1';

/// 报单请求要素（FFI 层从 `CThostFtdcInputOrderField` 拷贝而来）。
#[derive(Debug, Clone, PartialEq)]
pub struct OrderReq {
    pub order_ref: String,
    pub instrument: String,
    pub exchange_id: String,
    /// 买卖：'0' 买 / '1' 卖。
    pub direction: char,
    /// 开平：'0' 开 / '1' 平 / '3' 平今 / '4' 平昨。
    pub offset: char,
    /// 投机套保：'1' 投机。
    pub hedge_flag: char,
    /// 价格条件：'1' 市价 / '2' 限价。
    pub price_type: char,
    pub limit_price: f64,
    pub volume: i32,
}

/// 成交方式（脚本，不撮合）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillMode {
    /// 仅受理，挂在 '3'（用于挂单→查询→撤单链路）。
    AcceptedOnly,
    /// 受理 '3' → 一笔成交 → 全成 '0'（默认）。
    InstantFill,
    /// 受理 '3' → 按脚本逐笔成交（'1' 累计）→ 全成 '0'。
    PartialThenFill,
    /// 录入即拒（`on_rsp/on_err_rtn_order_insert` 带真实 ErrorID）。
    Rejected,
}

/// 一笔报单的成交脚本。
#[derive(Debug, Clone, PartialEq)]
pub struct FillScript {
    pub mode: FillMode,
    /// PartialThenFill 的逐笔成交量；其余模式忽略。
    /// 各笔之和不须恰为委托量：累计不足时末笔自动补齐，超出时末笔截断，保证自洽。
    pub partial_steps: Vec<i32>,
    /// Rejected 时的真实 CTP ErrorID（如 30 平仓量超持仓）。
    pub reject_code: i32,
}

impl FillScript {
    pub fn accepted_only() -> Self {
        Self {
            mode: FillMode::AcceptedOnly,
            partial_steps: Vec::new(),
            reject_code: 0,
        }
    }
    pub fn instant_fill() -> Self {
        Self {
            mode: FillMode::InstantFill,
            partial_steps: Vec::new(),
            reject_code: 0,
        }
    }
    pub fn partial_then_fill(steps: Vec<i32>) -> Self {
        Self {
            mode: FillMode::PartialThenFill,
            partial_steps: steps,
            reject_code: 0,
        }
    }
    pub fn rejected(code: i32) -> Self {
        Self {
            mode: FillMode::Rejected,
            partial_steps: Vec::new(),
            reject_code: code,
        }
    }
}

/// 录单/成交回调事件（与 bindgen 解耦）。
#[derive(Debug, Clone, PartialEq)]
pub enum OrderEvent {
    /// 录入被拒：对应 `on_rsp_order_insert` / `on_err_rtn_order_insert`。
    Rejected { code: i32, msg: String },
    /// 委托状态回报：对应 `on_rtn_order`（携带累计已成量）。
    Status {
        status: char,
        volume_traded: i32,
        status_msg: String,
    },
    /// 成交回报：对应 `on_rtn_trade`。`seq` 为本单内成交序号（从 1 起），
    /// 21B TradeID 的具体拼装见 fields（任务 5）。
    Trade { seq: u64, price: f64, volume: i32 },
}

/// 一笔报单的完整应答：顶层标识 + 有序事件。
#[derive(Debug, Clone, PartialEq)]
pub struct OrderAck {
    pub order_ref: String,
    pub order_sys_id: String,
    pub events: Vec<OrderEvent>,
}

/// 撤单结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelOutcome {
    /// 撤成：异步 `on_rtn_order` '5'。
    Cancelled,
    /// 异步拒绝：`on_err_rtn_order_action` 带真实 ErrorID（24/25/26）。
    Rejected { code: i32, msg: String },
}

/// 真实 CTP 错误文案（摘自 vendored v6.7.13 `error.xml`，去掉 `CTP:` 前缀）。
/// 未收录码返回通用兜底，避免空信息。
pub fn ctp_error_text(code: i32) -> &'static str {
    match code {
        24 => "撤单已报送，不允许重复撤单",
        25 => "撤单找不到相应报单",
        26 => "报单已全成交或已撤销，不能再撤",
        30 => "平仓量超过持仓量",
        64 => "客户端未认证",
        _ => "未知错误",
    }
}

/// 计算该报单的成交价：限价用委托价；市价用调用方给的脚本价（不看盘口）。
fn fill_price(req: &OrderReq, fallback_market_price: f64) -> f64 {
    if req.price_type == PRICE_ANY {
        fallback_market_price
    } else {
        req.limit_price
    }
}

/// 由成交脚本生成回调事件序列（纯函数）。
///
/// `order_sys_id` 由台账分配后传入（21B 风格）；`market_price` 仅在市价单时使用。
pub fn plan_insert(
    req: &OrderReq,
    order_sys_id: &str,
    script: &FillScript,
    market_price: f64,
) -> OrderAck {
    let make = |events: Vec<OrderEvent>| OrderAck {
        order_ref: req.order_ref.clone(),
        order_sys_id: order_sys_id.to_string(),
        events,
    };

    match script.mode {
        FillMode::Rejected => make(vec![OrderEvent::Rejected {
            code: script.reject_code,
            msg: ctp_error_text(script.reject_code).to_string(),
        }]),
        FillMode::AcceptedOnly => make(vec![OrderEvent::Status {
            status: '3',
            volume_traded: 0,
            status_msg: String::new(),
        }]),
        FillMode::InstantFill => {
            let price = fill_price(req, market_price);
            make(vec![
                OrderEvent::Status {
                    status: '3',
                    volume_traded: 0,
                    status_msg: String::new(),
                },
                OrderEvent::Trade {
                    seq: 1,
                    price,
                    volume: req.volume,
                },
                OrderEvent::Status {
                    status: '0',
                    volume_traded: req.volume,
                    status_msg: "全部成交".to_string(),
                },
            ])
        }
        FillMode::PartialThenFill => {
            let price = fill_price(req, market_price);
            let mut events = vec![OrderEvent::Status {
                status: '3',
                volume_traded: 0,
                status_msg: String::new(),
            }];
            let mut cum = 0i32;
            let mut seq = 0u64;
            for &step in &script.partial_steps {
                // 累计不得超过委托量：超出部分截断（自洽兜底）。
                let vol = step.clamp(0, (req.volume - cum).max(0));
                if vol <= 0 {
                    continue;
                }
                cum += vol;
                seq += 1;
                events.push(OrderEvent::Trade {
                    seq,
                    price,
                    volume: vol,
                });
                // 尚未全成 → 部分成交排队中；恰好全成则末帧统一发 '0'。
                if cum < req.volume {
                    events.push(OrderEvent::Status {
                        status: '1',
                        volume_traded: cum,
                        status_msg: "部分成交".to_string(),
                    });
                }
            }
            // 脚本量不足：补一笔到全成。
            if cum < req.volume {
                seq += 1;
                events.push(OrderEvent::Trade {
                    seq,
                    price,
                    volume: req.volume - cum,
                });
                cum = req.volume;
            }
            events.push(OrderEvent::Status {
                status: '0',
                volume_traded: cum,
                status_msg: "全部成交".to_string(),
            });
            make(events)
        }
    }
}

/// 由撤单错误注入生成撤单结果（纯函数）。
///
/// `injected`：`None` 撤成；`Some(24|25|26)` 异步拒绝（其余码原样透传文案）。
pub fn plan_cancel(injected: Option<i32>) -> CancelOutcome {
    match injected {
        None => CancelOutcome::Cancelled,
        Some(code) => CancelOutcome::Rejected {
            code,
            msg: ctp_error_text(code).to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit_buy(vol: i32) -> OrderReq {
        OrderReq {
            order_ref: "7".into(),
            instrument: "rb2510".into(),
            exchange_id: "SHFE".into(),
            direction: '0',
            offset: '0',
            hedge_flag: '1',
            price_type: '2',
            limit_price: 3205.0,
            volume: vol,
        }
    }

    fn market_buy(vol: i32) -> OrderReq {
        let mut r = limit_buy(vol);
        r.price_type = PRICE_ANY;
        r.limit_price = 0.0;
        r
    }

    #[test]
    fn accepted_only_emits_single_queued_status() {
        let ack = plan_insert(
            &limit_buy(2),
            "    20261015-1",
            &FillScript::accepted_only(),
            0.0,
        );
        assert_eq!(ack.order_ref, "7");
        assert_eq!(ack.order_sys_id, "    20261015-1");
        assert_eq!(
            ack.events,
            vec![OrderEvent::Status {
                status: '3',
                volume_traded: 0,
                status_msg: String::new()
            }]
        );
    }

    #[test]
    fn instant_fill_limit_uses_order_price_and_full_volume() {
        let ack = plan_insert(&limit_buy(3), "sys", &FillScript::instant_fill(), 9999.0);
        // 受理 → 一笔成交（限价用委托价，不用脚本价）→ 全成。
        assert!(matches!(
            ack.events[0],
            OrderEvent::Status {
                status: '3',
                volume_traded: 0,
                ..
            }
        ));
        assert_eq!(
            ack.events[1],
            OrderEvent::Trade {
                seq: 1,
                price: 3205.0,
                volume: 3
            }
        );
        assert!(matches!(
            ack.events[2],
            OrderEvent::Status {
                status: '0',
                volume_traded: 3,
                ..
            }
        ));
        assert_eq!(ack.events.len(), 3);
    }

    #[test]
    fn instant_fill_market_uses_script_price() {
        let ack = plan_insert(&market_buy(2), "sys", &FillScript::instant_fill(), 3300.0);
        assert_eq!(
            ack.events[1],
            OrderEvent::Trade {
                seq: 1,
                price: 3300.0,
                volume: 2
            }
        );
    }

    #[test]
    fn partial_then_fill_emits_trades_with_cumulative_statuses_then_all_traded() {
        // 委托 5 手，脚本分 2、2（不足 5）→ 自动补最后一笔 1。
        let ack = plan_insert(
            &limit_buy(5),
            "sys",
            &FillScript::partial_then_fill(vec![2, 2]),
            0.0,
        );
        // 3 受理 + (Trade2,Status'1'2) + (Trade2,Status'1'4) + Trade1 补 + Status'0'5
        assert_eq!(
            ack.events[0],
            OrderEvent::Status {
                status: '3',
                volume_traded: 0,
                status_msg: String::new()
            }
        );
        assert_eq!(
            ack.events[1],
            OrderEvent::Trade {
                seq: 1,
                price: 3205.0,
                volume: 2
            }
        );
        assert_eq!(
            ack.events[2],
            OrderEvent::Status {
                status: '1',
                volume_traded: 2,
                status_msg: "部分成交".into()
            }
        );
        assert_eq!(
            ack.events[3],
            OrderEvent::Trade {
                seq: 2,
                price: 3205.0,
                volume: 2
            }
        );
        assert_eq!(
            ack.events[4],
            OrderEvent::Status {
                status: '1',
                volume_traded: 4,
                status_msg: "部分成交".into()
            }
        );
        assert_eq!(
            ack.events[5],
            OrderEvent::Trade {
                seq: 3,
                price: 3205.0,
                volume: 1
            }
        );
        assert_eq!(
            ack.events[6],
            OrderEvent::Status {
                status: '0',
                volume_traded: 5,
                status_msg: "全部成交".into()
            }
        );
        assert_eq!(ack.events.len(), 7);
    }

    #[test]
    fn partial_steps_exceeding_volume_are_truncated() {
        // 委托 3 手，脚本给 2、5：第二笔只能再成 1，不超量。
        let ack = plan_insert(
            &limit_buy(3),
            "sys",
            &FillScript::partial_then_fill(vec![2, 5]),
            0.0,
        );
        let trades: Vec<(u64, i32)> = ack
            .events
            .iter()
            .filter_map(|e| match e {
                OrderEvent::Trade { seq, volume, .. } => Some((*seq, *volume)),
                _ => None,
            })
            .collect();
        assert_eq!(trades, vec![(1, 2), (2, 1)]);
        assert!(matches!(
            ack.events.last().unwrap(),
            OrderEvent::Status {
                status: '0',
                volume_traded: 3,
                ..
            }
        ));
    }

    #[test]
    fn rejected_emits_error_with_real_ctp_code_and_text() {
        let ack = plan_insert(&limit_buy(1), "sys", &FillScript::rejected(30), 0.0);
        assert_eq!(ack.events.len(), 1);
        assert_eq!(
            ack.events[0],
            OrderEvent::Rejected {
                code: 30,
                msg: "平仓量超过持仓量".into()
            }
        );
        // 被拒不分配成交/状态。
    }

    #[test]
    fn cancel_default_is_cancelled() {
        assert_eq!(plan_cancel(None), CancelOutcome::Cancelled);
    }

    #[test]
    fn cancel_error_24_25_26_map_to_real_text() {
        assert_eq!(
            plan_cancel(Some(24)),
            CancelOutcome::Rejected {
                code: 24,
                msg: "撤单已报送，不允许重复撤单".into()
            }
        );
        assert_eq!(
            plan_cancel(Some(25)),
            CancelOutcome::Rejected {
                code: 25,
                msg: "撤单找不到相应报单".into()
            }
        );
        assert_eq!(
            plan_cancel(Some(26)),
            CancelOutcome::Rejected {
                code: 26,
                msg: "报单已全成交或已撤销，不能再撤".into()
            }
        );
    }
}
