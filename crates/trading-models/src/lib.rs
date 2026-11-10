//! 交易服务业务 model：Account / Order / Position。
//!
//! 三个 model 均实现 [`store_core::Model`]：
//! - `deleted` 在 CSV 中存 0/1；
//! - `created_at` / `updated_at` 为 UTC 毫秒时间戳，由框架自动维护；
//! - 价格/数量/金额统一使用 [`rust_decimal::Decimal`] 定点数（Position 早期
//!   设计的 float64 经评审改为 Decimal，禁止浮点累加与相等判定）。

pub mod account;
pub mod bar;
pub mod order;
pub mod order_rule;
pub mod position;
pub mod position_sync;
pub mod quote;
pub mod types;
pub mod watch_rule;

pub use account::{Account, SecretCipher, ACCOUNT_CSV_HEADER, SECRET_PREFIX};
pub use bar::{
    bar_csv_header, bar_extra_columns, Bar, BarParseError, ExtraKind, KlineExtra, RawTick,
    RawTickParseError, BAR_CSV_COMMON_COLUMNS, BAR_CSV_COMMON_HEADER, RAW_TICK_COLUMNS,
    RAW_TICK_CSV_HEADER,
};
pub use order::{Order, OrderStatus, OrderType, TimeInForce};
pub use order_rule::{OrderRule, OrderRuleKey, PriceTier};
pub use position::{revalue_positions, Position, PositionPrice, QtyUnit, ROI_SCALE};
pub use position_sync::{
    diff_positions, find_duplicate_active_groups, merge_remote_into_local, DuplicateActiveGroup,
    PositionDiff, ReconcileReport,
};
pub use quote::{Instrument, MarketKind, Quote, TopOfBook, Venue};
pub use types::{InstrumentType, PositionMode, Side};
pub use watch_rule::{
    RuleIdentity, WatchConditionKind, WatchRule, WatchRuleAction, WatchRuleStatus, WatchRuleType,
};
