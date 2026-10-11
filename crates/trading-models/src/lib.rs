//! 交易服务业务 model：Account / Order / Position。
//!
//! 三个 model 均实现 [`store_core::Model`]：
//! - `deleted` 在 CSV 中存 0/1；
//! - `created_at` / `updated_at` 为 UTC 毫秒时间戳，由框架自动维护；
//! - 价格/数量/金额统一使用 [`rust_decimal::Decimal`] 定点数（文档中 Position
//!   标注的 float64 经评审改为 Decimal，禁止浮点累加与相等判定）。

pub mod account;
pub mod order;
pub mod order_rule;
pub mod position;
pub mod position_sync;
pub mod quote;
pub mod types;
pub mod watch_rule;

pub use account::{Account, SecretCipher};
pub use order::{Order, OrderStatus, OrderType, TimeInForce};
pub use order_rule::{OrderRule, OrderRuleKey, PriceTier};
pub use position::{revalue_positions, Position, PositionPrice, QtyUnit, ROI_SCALE};
pub use position_sync::{diff_positions, merge_remote_into_local, PositionDiff, ReconcileReport};
pub use quote::{Instrument, MarketKind, Quote, TopOfBook, Venue};
pub use types::{InstrumentType, PositionMode, Side};
pub use watch_rule::{
    RuleIdentity, WatchConditionKind, WatchRule, WatchRuleAction, WatchRuleStatus, WatchRuleType,
};
