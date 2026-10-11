//! 盯盘规则引擎（trading-service 侧）。
//!
//! 分层与 [`crate::positions`] 一致：纯决策内核与 IO 编排分离。
//! * [`eval`]：条件比较操作符（`>=` `<=` `>` `<` `==`）的纯函数解释；
//! * [`metrics`]：`Position + Market → EvaluationContext` 的纯计算层
//!   （零 IO/网络/存储，行情与跨标的持仓以闭包注入），Rule 只认指标做阈值判定；
//! * [`order`]：命中规则 → 内存订单/下单请求的纯构造（risk 反向减仓、new 按 side 开仓）；
//! * [`engine`]：一轮扫描编排——评估 active 规则、行锁 CAS 抢占、链式激活、建单；
//! * [`placer`]：生产下单口（经交易所注册表提交并回写受理/失败）。
//!
//! 状态转换（active/in_use/pending/retired）一律走 trading-store 的 `RuleRepo`，
//! 本模块不自建共享内存；定时扫描任务在 `main` 装配，终态回收在 `event`。

pub mod create;
pub mod engine;
pub mod eval;
pub mod metrics;
pub mod order;
pub mod placer;

pub use create::{build_rule, RuleCreateInput, RulePositionLookup};
pub use engine::{RuleAccount, RuleAccountLookup, RuleEngine, RuleOrderPlacer, RuleOrderQuerier};
pub use placer::{ExchangeRuleOrderQuerier, ExchangeRulePlacer};

/// 生产仓位查询端口：trading-store 的 PositionRepo 直接适配建规则校验。
impl RulePositionLookup for trading_store::PositionRepo {
    fn find_active_by_id(
        &self,
        account_id: u64,
        position_id: u64,
    ) -> Option<trading_models::Position> {
        // 按主键取活跃仓，并在此校验账户归属（跨账户视为不存在）。
        self.get_active(position_id)
            .filter(|p| p.account_id == account_id)
    }

    fn exists_active_instrument(
        &self,
        account_id: u64,
        exchange: &str,
        instrument_type: trading_models::InstrumentType,
        instrument: &str,
    ) -> bool {
        self.exists_active_instrument(account_id, exchange, instrument_type, instrument)
    }
}
