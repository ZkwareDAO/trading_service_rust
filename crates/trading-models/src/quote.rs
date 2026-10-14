//! 行情数据模型：强类型标的 [`Instrument`] 与每个标的一份的最新行情 [`Quote`]。
//!
//! 规范来源：`docs/行情订阅模型.md`。与 [`crate::OrderRule`] 同类——行情是交易所
//! 发布的瞬时公开数据：**只存在于内存，不实现 [`store_core::Model`]、不落 CSV、
//! 不参与日切与恢复**，进程重启后由各 venue 订阅重新填充。运行时的内存表
//! （`DashMap<Instrument, Quote>`）位于 `trading-core`。
//!
//! 价格一律定点 [`rust_decimal::Decimal`]（规范中的 `decimal`），禁止 f64 参与
//! 估值/风控运算；时间统一 UTC 毫秒时间戳。

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// 交易场所。行情订阅的物理连接以 venue 区分（Binance 现货/合约还是不同主机）。
///
/// 与 `Order`/`OrderRule` 上的 `exchange: String`（"binance" 等）在 venue codec
/// 边界互转；行情 key 用强类型枚举，避免同名跨市场混淆。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Venue {
    /// Binance 现货。
    BinanceSpot,
    /// Binance USDⓈ-M U 本位永续。
    BinanceFutures,
    /// Hyperliquid 永续。
    Hyperliquid,
    /// Deribit 期权。
    Deribit,
    /// 国内期货 CTP。
    Ctp,
}

/// 市场形态（现货 / 期货类 / 期权），行情订阅的三分类。
///
/// 与 [`crate::InstrumentType`] 刻意不同：后者是订单/规则口径，含 `Perpetual`、
/// `Equity`、`Fund` 等下单语义；行情侧只按订阅拓扑分三类（Binance 永续与交割
/// 同走合约主机，归 `Futures`）。二者在 venue codec 边界互转，不强行合并。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketKind {
    Spot,
    Futures,
    Option,
}

/// 全局唯一标的：强类型 key，从根上区分「同名但不同市场/交易所」。
///
/// `symbol` 存**规范化符号**：Binance 原样（BTCUSDT）、Hyperliquid 裸币名（NEAR）、
/// Deribit 完整 instrument 名（ETH-18SEP26-2550-P）。交易所原始 wire 符号与本结构
/// 的互转由各 venue 的 codec 负责（如 Hyperliquid 的 `NEAR <-> NEARUSDC` 去/补后缀），
/// 差异不泄漏到行情存储 key。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Instrument {
    pub venue: Venue,
    pub kind: MarketKind,
    /// 规范化符号（canonical），见结构文档。
    pub symbol: String,
}

impl Instrument {
    pub fn new(venue: Venue, kind: MarketKind, symbol: impl Into<String>) -> Self {
        Self {
            venue,
            kind,
            symbol: symbol.into(),
        }
    }
}

/// 买一/卖一盘口。任一侧为 `None` 表示该侧无报价或未订阅。
///
/// 当前仅 Deribit ticker 填充；Binance/Hyperliquid 结构保留、暂不订阅盘口。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TopOfBook {
    pub bid: Option<Decimal>,
    pub ask: Option<Decimal>,
}

impl TopOfBook {
    pub fn new(bid: Option<Decimal>, ask: Option<Decimal>) -> Self {
        Self { bid, ask }
    }

    /// 买卖盘中点；任一侧缺失则不可计算。
    pub fn mid(&self) -> Option<Decimal> {
        match (self.bid, self.ask) {
            (Some(bid), Some(ask)) => Some((bid + ask) / Decimal::TWO),
            _ => None,
        }
    }
}

/// 每个 [`Instrument`] 对应一份的最新行情。字段全部按需填充，未取到/未订阅即为 `None`。
///
/// 不设单调 `version`：单连接内消息保序，新鲜度由 [`Quote::ts_recv_ms`] 判定；
/// 重连乱序若将来需要处理，用「连接代际（epoch）丢弃旧连接消息」解决，
/// 而不是给每个标的维护计数器。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Quote {
    /// 最新成交价（Binance 现货 miniTicker；其余可选）。
    pub last: Option<Decimal>,
    /// 标记价，估值/风控主口径（B 合约 markPrice 流、H activeAssetCtx.markPx、D ticker）。
    pub mark: Option<Decimal>,
    /// 买卖盘中点（H allMids；D 可由 bid/ask 推导）。
    pub mid: Option<Decimal>,
    /// 最佳买/卖价（可成交价口径）。当前仅 D ticker 填充；B/H 结构保留、暂不订阅。
    pub book: Option<TopOfBook>,
    /// 交易所消息时间戳（无则 0）。
    pub ts_exchange_ms: i64,
    /// 本地接收时间戳（新鲜度判定主依据）。
    pub ts_recv_ms: i64,
}

impl Quote {
    /// 估值/风控价：`mark > mid > last`；全 `None` 即无价。
    pub fn valuation_price(&self) -> Option<Decimal> {
        self.mark.or(self.mid).or(self.last)
    }

    /// 可成交价：平多（`is_short=false`）吃 bid，平空（`is_short=true`）吃 ask。
    ///
    /// B/H 当前 `book` 为 `None`（走交易所原生市价单），仅 D 用本地对手价转限价。
    pub fn executable_price(&self, is_short: bool) -> Option<Decimal> {
        self.book.and_then(|b| if is_short { b.ask } else { b.bid })
    }

    /// 新鲜度：按本地接收时间判定，过期数据不得参与风控。
    ///
    /// 从未写入（`ts_recv_ms == 0`）恒为不新鲜；时钟回拨导致的负年龄也视为过期。
    pub fn is_fresh(&self, now_ms: i64, max_age_ms: i64) -> bool {
        let age = now_ms - self.ts_recv_ms;
        self.ts_recv_ms != 0 && (0..=max_age_ms).contains(&age)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn instrument_is_hashable_key_and_distinct_by_venue() {
        use std::collections::HashSet;
        let spot = Instrument::new(Venue::BinanceSpot, MarketKind::Spot, "BTCUSDT");
        let fut = Instrument::new(Venue::BinanceFutures, MarketKind::Futures, "BTCUSDT");
        assert_ne!(spot, fut);
        let set: HashSet<_> = [&spot, &fut].into_iter().collect();
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn valuation_price_prefers_mark_then_mid_then_last() {
        let mut q = Quote::default();
        assert_eq!(q.valuation_price(), None);

        q.last = Some(dec("100"));
        assert_eq!(q.valuation_price(), Some(dec("100")));

        q.mid = Some(dec("101"));
        assert_eq!(q.valuation_price(), Some(dec("101")));

        q.mark = Some(dec("102"));
        assert_eq!(q.valuation_price(), Some(dec("102")));
    }

    #[test]
    fn executable_price_hits_bid_to_close_long_ask_to_close_short() {
        let mut q = Quote::default();
        // 未订阅盘口：B/H 市价单路径，本地不取对手价。
        assert_eq!(q.executable_price(false), None);
        assert_eq!(q.executable_price(true), None);

        q.book = Some(TopOfBook::new(Some(dec("100")), Some(dec("101"))));
        assert_eq!(q.executable_price(false), Some(dec("100")));
        assert_eq!(q.executable_price(true), Some(dec("101")));
    }

    #[test]
    fn one_sided_book_has_no_executable_price_on_missing_side() {
        let q = Quote {
            book: Some(TopOfBook::new(Some(dec("100")), None)),
            ..Quote::default()
        };
        assert_eq!(q.executable_price(false), Some(dec("100")));
        assert_eq!(q.executable_price(true), None);
    }

    #[test]
    fn book_mid_requires_both_sides() {
        assert_eq!(TopOfBook::default().mid(), None);
        assert_eq!(TopOfBook::new(Some(dec("100")), None).mid(), None);
        assert_eq!(
            TopOfBook::new(Some(dec("100")), Some(dec("101"))).mid(),
            Some(dec("100.5"))
        );
    }

    #[test]
    fn freshness_uses_local_recv_timestamp() {
        let q = Quote {
            ts_recv_ms: 1_000,
            ..Quote::default()
        };
        assert!(q.is_fresh(1_500, 600));
        // 边界：年龄恰好等于 max_age 仍新鲜。
        assert!(q.is_fresh(1_600, 600));
        assert!(!q.is_fresh(1_601, 600));
        // 从未写入。
        assert!(!Quote::default().is_fresh(1_000, 600));
        // 时钟回拨：不应当作新鲜。
        assert!(!q.is_fresh(999, 600));
    }

    #[test]
    fn venue_serde_is_stable_snake_case() {
        // 行情不持久化，但经 WS/REST JSON 在线传输，枚举串需稳定可读。
        let v = serde_json::to_value(Venue::BinanceFutures).unwrap();
        assert_eq!(v, serde_json::json!("binance_futures"));
        let back: Venue = serde_json::from_value(v).unwrap();
        assert_eq!(back, Venue::BinanceFutures);
    }

    #[test]
    fn quote_serde_roundtrips_empty_and_partial() {
        let empty = serde_json::to_value(Quote::default()).unwrap();
        let back: Quote = serde_json::from_value(empty).unwrap();
        assert_eq!(back, Quote::default());

        let q = Quote {
            mark: Some(dec("3000.5")),
            ts_exchange_ms: 11,
            ts_recv_ms: 22,
            ..Quote::default()
        };
        let v = serde_json::to_value(q).unwrap();
        let back: Quote = serde_json::from_value(v).unwrap();
        assert_eq!(back, q);
    }
}
