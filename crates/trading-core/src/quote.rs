//! 行情内存表：每个 [`Instrument`] 一份最新 [`Quote`]。
//!
//! 与 [`crate::order_rule::OrderRuleTable`] 同属「纯内存、不落盘」一层，但并发形态不同：
//! 规则行是整行不可变、刷新时 `Arc` 整体替换；行情是**高频字段级更新**
//! （Deribit 一条 ticker 原子带 mark+book、Hyperliquid 按字段 merge），
//! 故这里直接在 [`DashMap`] 中就地持有 `Quote`（小的 `Copy` 值，无 `String`），
//! 更新方法只覆盖消息实际携带的字段（`None` 表示「本频道不提供」，不参与 merge）。
//!
//! 行情**不注册进 `TradingStore`、不实现 `Model`、没有快照/恢复/日切**：
//! 进程重启后表为空，由各 venue 订阅重新填充。读端拿到的是 [`Quote`] 快照拷贝，
//! 不持有任何锁，可安全跨 `.await` 使用。
//!
//! 规范来源：`docs/行情订阅模型.md`。并发口径：句柄 `Arc` 可自由复制到任意
//! 线程 / tokio 任务，业务热路径同步调用、无 `.await`。

use dashmap::DashMap;
use rust_decimal::Decimal;
use trading_models::{Instrument, MarketKind, Quote, TopOfBook, Venue};

/// 字段级行情更新：`None` 字段表示该消息未携带，merge 时保留现值不动。
///
/// 对应规范里的频道形态：Binance 全市场快照只给 `last`/`mark`；
/// Hyperliquid `activeAssetCtx` 同时给 mark/mid；Deribit ticker 给 mark+book。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QuoteUpdate {
    pub last: Option<Decimal>,
    pub mark: Option<Decimal>,
    pub mid: Option<Decimal>,
    /// `None` = 本消息不含盘口，保留原 book；`Some(_)` = 消息携带盘口（可含单侧空值）。
    pub book: Option<TopOfBook>,
    /// 交易所消息时间戳；消息没有时给 0（写入时 0 不覆盖已有值）。
    pub ts_exchange_ms: i64,
    /// 本地接收时间戳，调用方在写入前打点；0 视为无效不覆盖。
    pub ts_recv_ms: i64,
}

impl QuoteUpdate {
    pub fn new(ts_recv_ms: i64) -> Self {
        Self {
            ts_recv_ms,
            ..Self::default()
        }
    }

    /// 就地 merge 到目标 [`Quote`]：`None` 字段保留现值，时间戳按「更大者」推进，
    /// 杜绝乱序/旧消息把较新的时间戳改回 0。
    fn apply_to(&self, q: &mut Quote) {
        // 更新携带（Some）则覆盖，否则保留现值；时间戳只增不退，杜绝旧消息回拨。
        q.last = self.last.or(q.last);
        q.mark = self.mark.or(q.mark);
        q.mid = self.mid.or(q.mid);
        q.book = self.book.or(q.book);
        q.ts_exchange_ms = q.ts_exchange_ms.max(self.ts_exchange_ms);
        q.ts_recv_ms = q.ts_recv_ms.max(self.ts_recv_ms);
    }
}

/// 进程内一份的行情表。`Send + Sync`，以 `Arc<QuoteTable>` 在任意线程 / tokio
/// 任务间共享；所有方法同步、无 `.await`（纯内存纳秒~微秒级）。
#[derive(Debug, Default)]
pub struct QuoteTable {
    inner: DashMap<Instrument, Quote>,
}

impl QuoteTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// 字段级写入：标的不存在则插入；存在则只覆盖消息携带的字段。
    ///
    /// 经 `entry` 在单次 shard 写锁内完成「缺行即建 + merge」：多线程对**同一新标的**
    /// 并发首写不会出现「各自从默认值合并、后者整体覆盖前者」的丢字段窗口。
    pub fn apply(&self, instrument: &Instrument, update: QuoteUpdate) {
        let mut row = self.inner.entry(instrument.clone()).or_default();
        update.apply_to(&mut row);
    }

    /// 整份替换（如全市场快照后，以快照为该标的的唯一事实来源）。
    pub fn put(&self, instrument: &Instrument, quote: Quote) {
        self.inner.insert(instrument.clone(), quote);
    }

    /// 读端：拷贝一份 [`Quote`]（`Copy`），离开 map 后无锁。
    pub fn get(&self, instrument: &Instrument) -> Option<Quote> {
        self.inner.get(instrument).map(|q| *q.value())
    }

    /// 估值/风控价（`mark > mid > last`），无价返回 `None`。
    pub fn valuation_price(&self, instrument: &Instrument) -> Option<Decimal> {
        self.inner.get(instrument).and_then(|q| q.valuation_price())
    }

    /// 跨标的条件取价：按 symbol 反查估值价（`mark > mid > last`）。
    ///
    /// 顺序固定、与时间/随机无关，保证同名多报价时多轮结果一致：
    /// 1. **本所优先**：`preferred` 给出 (venue, kind) 时先精确查；
    /// 2. **全局回退**：全表按 symbol 匹配，按 market kind 优先级
    ///    （futures > option > spot）、同 kind 再按 venue 固定次序取第一条。
    ///
    /// 命中行若无任何估值价则跳过；最终无价返回 `None`。
    pub fn price_for_symbol(
        &self,
        preferred: Option<(Venue, MarketKind)>,
        symbol: &str,
    ) -> Option<Decimal> {
        // 本所精确命中。
        if let Some((venue, kind)) = preferred {
            let inst = Instrument::new(venue, kind, symbol);
            if let Some(price) = self.valuation_price(&inst) {
                return Some(price);
            }
        }
        // 全局按名回退：在所有同名行中选优先级最高者。
        self.snapshot()
            .into_iter()
            .filter(|(inst, _)| inst.symbol == symbol)
            .filter_map(|(inst, q)| q.valuation_price().map(|p| (inst, p)))
            .max_by_key(|(inst, _)| (kind_rank(inst.kind), venue_rank(inst.venue)))
            .map(|(_, price)| price)
    }

    /// 某标的的行情是否仍新鲜（按本地接收时间）。不存在即不新鲜。
    pub fn is_fresh(&self, instrument: &Instrument, now_ms: i64, max_age_ms: i64) -> bool {
        self.inner
            .get(instrument)
            .is_some_and(|q| q.is_fresh(now_ms, max_age_ms))
    }

    /// 全表快照（订阅重放/诊断用）；每次 key 都 clone 一个 String。
    pub fn snapshot(&self) -> Vec<(Instrument, Quote)> {
        self.inner
            .iter()
            .map(|e| (e.key().clone(), *e.value()))
            .collect()
    }

    /// 删除一个标的（取消订阅 / 期权到期下架）。
    pub fn remove(&self, instrument: &Instrument) {
        self.inner.remove(instrument);
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

/// market kind 的全局优先级：期货类 > 期权 > 现货。
fn kind_rank(kind: MarketKind) -> u8 {
    match kind {
        MarketKind::Futures => 2,
        MarketKind::Option => 1,
        MarketKind::Spot => 0,
    }
}

/// venue 的固定次序（仅用于同 kind 多报价时打破平局，保持确定性）。
fn venue_rank(venue: Venue) -> u8 {
    match venue {
        Venue::BinanceFutures => 3,
        Venue::BinanceSpot => 2,
        Venue::Deribit => 1,
        Venue::Hyperliquid => 0,
        Venue::Ctp => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use trading_models::{MarketKind, Venue};

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn btc_perp() -> Instrument {
        Instrument::new(Venue::Hyperliquid, MarketKind::Futures, "BTC")
    }

    fn deribit_opt() -> Instrument {
        Instrument::new(Venue::Deribit, MarketKind::Option, "ETH-18SEP26-2550-P")
    }

    #[test]
    fn apply_inserts_then_merges_field_level() {
        let table = QuoteTable::new();
        let inst = btc_perp();

        // 首条：allMids 只给 mid。
        table.apply(
            &inst,
            QuoteUpdate {
                mid: Some(dec("100")),
                ts_recv_ms: 10,
                ..QuoteUpdate::default()
            },
        );
        let q = table.get(&inst).unwrap();
        assert_eq!(q.mid, Some(dec("100")));
        assert_eq!(q.last, None);
        assert_eq!(q.ts_recv_ms, 10);

        // 第二条：activeAssetCtx 给 mark，不带 mid —— mid 必须保留。
        table.apply(
            &inst,
            QuoteUpdate {
                mark: Some(dec("101")),
                ts_recv_ms: 20,
                ..QuoteUpdate::default()
            },
        );
        let q = table.get(&inst).unwrap();
        assert_eq!(q.mark, Some(dec("101")));
        assert_eq!(q.mid, Some(dec("100")));
        assert_eq!(q.ts_recv_ms, 20);
        assert_eq!(q.valuation_price(), Some(dec("101")));
    }

    #[test]
    fn apply_does_not_rewind_timestamps() {
        let table = QuoteTable::new();
        let inst = btc_perp();
        table.apply(
            &inst,
            QuoteUpdate {
                mark: Some(dec("100")),
                ts_exchange_ms: 500,
                ts_recv_ms: 600,
                ..QuoteUpdate::default()
            },
        );
        // 乱序/无时间戳消息不得把时间改回 0。
        table.apply(
            &inst,
            QuoteUpdate {
                mid: Some(dec("99")),
                ..QuoteUpdate::default()
            },
        );
        let q = table.get(&inst).unwrap();
        assert_eq!(q.ts_exchange_ms, 500);
        assert_eq!(q.ts_recv_ms, 600);
        assert_eq!(q.mid, Some(dec("99")));
    }

    #[test]
    fn book_merges_and_clears_per_side() {
        let table = QuoteTable::new();
        let inst = deribit_opt();
        table.apply(
            &inst,
            QuoteUpdate {
                mark: Some(dec("2500")),
                book: Some(TopOfBook::new(Some(dec("2499")), Some(dec("2501")))),
                ts_exchange_ms: 1,
                ts_recv_ms: 2,
                ..QuoteUpdate::default()
            },
        );
        let q = table.get(&inst).unwrap();
        // 平多吃 bid、平空吃 ask。
        assert_eq!(q.executable_price(false), Some(dec("2499")));
        assert_eq!(q.executable_price(true), Some(dec("2501")));

        // 新 ticker 卖侧暂无报价：ask 覆盖为空，bid 更新。
        table.apply(
            &inst,
            QuoteUpdate {
                book: Some(TopOfBook::new(Some(dec("2498")), None)),
                ts_recv_ms: 3,
                ..QuoteUpdate::default()
            },
        );
        let q = table.get(&inst).unwrap();
        assert_eq!(q.book.unwrap().bid, Some(dec("2498")));
        assert_eq!(q.executable_price(true), None);
    }

    #[test]
    fn put_replaces_whole_quote() {
        let table = QuoteTable::new();
        let inst = btc_perp();
        table.apply(
            &inst,
            QuoteUpdate {
                mid: Some(dec("100")),
                ts_recv_ms: 1,
                ..QuoteUpdate::default()
            },
        );
        table.put(
            &inst,
            Quote {
                last: Some(dec("99")),
                ts_recv_ms: 5,
                ..Quote::default()
            },
        );
        let q = table.get(&inst).unwrap();
        assert_eq!(q.mid, None);
        assert_eq!(q.last, Some(dec("99")));
    }

    #[test]
    fn freshness_and_remove() {
        let table = QuoteTable::new();
        let inst = btc_perp();
        assert!(!table.is_fresh(&inst, 1_000, 600));
        assert_eq!(table.valuation_price(&inst), None);

        table.apply(
            &inst,
            QuoteUpdate {
                mark: Some(dec("1")),
                ts_recv_ms: 500,
                ..QuoteUpdate::default()
            },
        );
        assert!(table.is_fresh(&inst, 1_000, 600));
        assert!(!table.is_fresh(&inst, 1_200, 600));

        table.remove(&inst);
        assert!(table.is_empty());
        assert!(!table.is_fresh(&inst, 1_000, 600));
    }

    #[test]
    fn snapshot_returns_all_instruments() {
        let table = QuoteTable::new();
        table.apply(
            &btc_perp(),
            QuoteUpdate {
                mark: Some(dec("1")),
                ts_recv_ms: 1,
                ..QuoteUpdate::default()
            },
        );
        table.apply(
            &deribit_opt(),
            QuoteUpdate {
                mark: Some(dec("2")),
                ts_recv_ms: 1,
                ..QuoteUpdate::default()
            },
        );
        assert_eq!(table.len(), 2);
        assert_eq!(table.snapshot().len(), 2);
    }

    fn quote_with_mark(mark: &str) -> Quote {
        Quote {
            mark: Some(dec(mark)),
            ts_exchange_ms: 1,
            ts_recv_ms: 1,
            ..Quote::default()
        }
    }

    #[test]
    fn price_for_symbol_prefers_exact_venue_kind() {
        // 本所优先：preferred 命中即返回，即使全表还有同名其它报价。
        let t = QuoteTable::new();
        t.put(
            &Instrument::new(Venue::BinanceFutures, MarketKind::Futures, "BTCUSDT"),
            quote_with_mark("90000"),
        );
        t.put(
            &Instrument::new(Venue::BinanceSpot, MarketKind::Spot, "BTCUSDT"),
            quote_with_mark("88000"),
        );
        let p = t.price_for_symbol(Some((Venue::BinanceSpot, MarketKind::Spot)), "BTCUSDT");
        assert_eq!(p, Some(dec("88000")));
    }

    #[test]
    fn price_for_symbol_falls_back_globally_by_name() {
        // deribit 账户查 BTCUSDT：本所无该 symbol，全局按名回退到币安合约。
        let t = QuoteTable::new();
        t.put(
            &Instrument::new(Venue::BinanceFutures, MarketKind::Futures, "BTCUSDT"),
            quote_with_mark("90000"),
        );
        // 同时放一个同名现货：合约优先于现货。
        t.put(
            &Instrument::new(Venue::BinanceSpot, MarketKind::Spot, "BTCUSDT"),
            quote_with_mark("88000"),
        );
        let p = t.price_for_symbol(Some((Venue::Deribit, MarketKind::Option)), "BTCUSDT");
        assert_eq!(p, Some(dec("90000")));
    }

    #[test]
    fn price_for_symbol_no_preference_still_matches() {
        let t = QuoteTable::new();
        t.put(
            &Instrument::new(Venue::BinanceFutures, MarketKind::Futures, "ETHUSDT"),
            quote_with_mark("3000"),
        );
        assert_eq!(t.price_for_symbol(None, "ETHUSDT"), Some(dec("3000")));
    }

    #[test]
    fn price_for_symbol_skips_quotes_without_valuation() {
        // 命中 symbol 但该 Quote 无任何估值价（无 mark/mid/last）→ 视为无价。
        let t = QuoteTable::new();
        t.put(
            &Instrument::new(Venue::BinanceFutures, MarketKind::Futures, "DOGEUSDT"),
            Quote::default(),
        );
        assert_eq!(t.price_for_symbol(None, "DOGEUSDT"), None);
    }

    #[test]
    fn price_for_symbol_unknown_symbol_is_none() {
        let t = QuoteTable::new();
        assert_eq!(t.price_for_symbol(None, "NOPEUSDT"), None);
    }

    #[test]
    fn concurrent_updates_to_distinct_instruments_never_lose_rows() {
        use std::thread;

        let table = std::sync::Arc::new(QuoteTable::new());
        let threads = 8;
        let per = 500;
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let table = std::sync::Arc::clone(&table);
                thread::spawn(move || {
                    for i in 0..per {
                        let inst = Instrument::new(
                            Venue::BinanceFutures,
                            MarketKind::Futures,
                            format!("COIN{t:X}{i:04}"),
                        );
                        table.apply(
                            &inst,
                            QuoteUpdate {
                                mark: Some(dec("1")),
                                ts_recv_ms: (t * per + i) as i64 + 1,
                                ..QuoteUpdate::default()
                            },
                        );
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(table.len(), threads * per);
        assert!(table.snapshot().iter().all(|(_, q)| q.mark.is_some()));
    }

    #[test]
    fn concurrent_first_writes_to_same_instrument_lose_no_fields() {
        // 同一新标的的并发首写回归：四个频道（last/mark/mid/book）各自由独立线程、
        // 经 Barrier 在空表上同时发起首写。旧的「get_mut miss 后再 insert」实现会让
        // 后写者基于默认值整体覆盖、丢掉先写者字段；entry().or_default() 必须四字段全保。
        use std::sync::Arc;
        use std::thread;

        let updates = [
            QuoteUpdate {
                last: Some(dec("10")),
                ts_recv_ms: 1,
                ..QuoteUpdate::default()
            },
            QuoteUpdate {
                mark: Some(dec("11")),
                ts_recv_ms: 1,
                ..QuoteUpdate::default()
            },
            QuoteUpdate {
                mid: Some(dec("12")),
                ts_recv_ms: 1,
                ..QuoteUpdate::default()
            },
            QuoteUpdate {
                book: Some(TopOfBook::new(Some(dec("9")), Some(dec("13")))),
                ts_recv_ms: 1,
                ..QuoteUpdate::default()
            },
        ];

        for _ in 0..200 {
            let table = Arc::new(QuoteTable::new());
            let barrier = Arc::new(std::sync::Barrier::new(4));
            let handles: Vec<_> = updates
                .iter()
                .map(|up| {
                    let table = Arc::clone(&table);
                    let barrier = Arc::clone(&barrier);
                    let inst = btc_perp();
                    let up = *up;
                    thread::spawn(move || {
                        barrier.wait(); // 同时放行，最大化首写竞争窗口
                        table.apply(&inst, up);
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }

            let q = table.get(&btc_perp()).unwrap();
            assert_eq!(q.last, Some(dec("10")));
            assert_eq!(q.mark, Some(dec("11")));
            assert_eq!(q.mid, Some(dec("12")));
            assert_eq!(
                q.book,
                Some(TopOfBook::new(Some(dec("9")), Some(dec("13"))))
            );
        }
    }
}
