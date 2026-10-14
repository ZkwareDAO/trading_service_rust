//! 仓位流水线档案：一家交易所接入「仓位对账 / 行情重估」流水线的**静态自描述元数据**。
//!
//! 设计动机见 `.claude/plans/position-profile-abstraction.plan.md`：
//! 此前服务侧的 PositionEngine 自己硬编码了三处「按交易所分派」
//! （对账作用域产品集 / on_quote 入站键 / 快照无 mark 时的兜底估值标的），
//! 每新增一家交易所都要回头改 engine 这一共有路径，且漏配不会被编译器发现。
//!
//! 收口方式：每家交易所的 [`crate::provider::VenueProvider`] 自行返回一份
//! [`PositionProfile`]，engine 启动时统一汇总并自检（fail-fast）。此后新增交易所
//! 只需在该所 provider 声明档案 + 注册表 push 一行，**不再改 engine 共有路径**。
//!
//! 档案为 `Copy` 的纯静态数据（`exchange` 取 `'static` 字面量），装配在启动前
//! 一次性完成，运行期只读。

use trading_models::{InstrumentType, MarketKind, Venue};

/// 一家交易所接入「仓位对账 / 重估流水线」的静态元数据，由该所 VenueProvider 自描述。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PositionProfile {
    /// 交易所规范名（"binance" / "deribit" / "hyperliquid"），与 `Position.exchange` 同口径。
    pub exchange: &'static str,
    /// 对账作用域的产品形状（收口原 engine 的 `snapshot_types`）。
    pub instrument_type: InstrumentType,
    /// `on_quote` 入站行情键（收口原 engine 的 `quote_target`）：
    /// 公共行情以 `(quote_venue, quote_kind)` 写入，命中即反查到本档案。
    pub quote_venue: Venue,
    pub quote_kind: MarketKind,
    /// 快照本身无 mark 价时的兜底估值标的（收口原 engine 的 `quote_price`）；
    /// `None` = 不兜底。**Deribit 必须为 None**（历史上兜底对它恒为 None，不得改变）。
    pub fallback_valuation: Option<(Venue, MarketKind)>,
}

impl PositionProfile {
    /// Binance U 本位永续档案：合约对账；入站/兜底均为 BinanceFutures 合约行情。
    pub const fn binance() -> Self {
        Self {
            exchange: "binance",
            instrument_type: InstrumentType::Futures,
            quote_venue: Venue::BinanceFutures,
            quote_kind: MarketKind::Futures,
            fallback_valuation: Some((Venue::BinanceFutures, MarketKind::Futures)),
        }
    }

    /// Deribit 期权档案：期权对账；入站 Deribit 期权行情；**无兜底估值**
    /// （保持历史行为：缺 mark 即无价，不得改用自身 venue 造出新估值）。
    pub const fn deribit() -> Self {
        Self {
            exchange: "deribit",
            instrument_type: InstrumentType::Option,
            quote_venue: Venue::Deribit,
            quote_kind: MarketKind::Option,
            fallback_valuation: None,
        }
    }

    /// Hyperliquid 永续档案：合约对账；入站/兜底均为 Hyperliquid 合约行情。
    pub const fn hyperliquid() -> Self {
        Self {
            exchange: "hyperliquid",
            instrument_type: InstrumentType::Futures,
            quote_venue: Venue::Hyperliquid,
            quote_kind: MarketKind::Futures,
            fallback_valuation: Some((Venue::Hyperliquid, MarketKind::Futures)),
        }
    }

    /// CTP 国内期货档案：合约对账；入站/兜底均为 CTP 合约行情
    /// （行情前置不可用时 market.rs 降级：QuoteTable 无该标的行时兜底
    /// 自然取不到价，由宿主 fallback_valuation 语义处理，不阻断交易）。
    #[cfg(feature = "ctp")]
    pub const fn ctp() -> Self {
        Self {
            exchange: "ctp",
            instrument_type: InstrumentType::Futures,
            quote_venue: Venue::Ctp,
            quote_kind: MarketKind::Futures,
            fallback_valuation: Some((Venue::Ctp, MarketKind::Futures)),
        }
    }
}
