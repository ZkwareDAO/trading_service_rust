//! 南向 `get_klines` 历史取数的**纯逻辑层**（零 IO）。
//!
//! 读盘/读内存由 [`crate::kline::KlineService`] 完成，本模块只负责把「热（内存保留窗）」
//! 与「冷（归档 CSV）」两份 [`Bar`] 合并去重、按查询开窗与截断，并把行模型映射成南向
//! `KlineEvent`。不连网、不读文件，全部行为可离线单测。
//!
//! 合并主键＝序列内 `start_time`；同一 start 热数据覆盖冷数据（内存为较新事实）。

use std::collections::BTreeMap;

use trading_core::protocol::KlineEvent;
use trading_models::Bar;

/// 不传 limit 时的缺省返回根数（≈最近 5 小时 1m）。
pub const DEFAULT_HISTORY_LIMIT: usize = 300;
/// 单次返回的硬上限（防一次取全量拖垮连接/内存）。
pub const MAX_HISTORY_LIMIT: usize = 1500;

/// 历史查询的根数限制（缺省/硬上限），由配置注入；启动期校验 `1 ≤ default ≤ max`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryLimits {
    /// 不传 limit 时取最近/最早的根数。
    pub default_limit: usize,
    /// 单次返回的硬上限（请求超限截断到此值）。
    pub max_limit: usize,
}

impl HistoryLimits {
    /// 校验并构造：default、max 都须 ≥1，且 default 不超过 max；否则 None（启动 fail-fast）。
    pub fn new(default_limit: usize, max_limit: usize) -> Option<Self> {
        if default_limit >= 1 && max_limit >= 1 && default_limit <= max_limit {
            Some(Self {
                default_limit,
                max_limit,
            })
        } else {
            None
        }
    }

    /// 归一客户端 limit：None/0 → default；超过 max → 截断为 max。
    pub fn normalize(&self, requested: Option<usize>) -> usize {
        match requested {
            Some(n) if n >= 1 => n.min(self.max_limit),
            _ => self.default_limit,
        }
    }
}

/// 一次历史取数请求（limit 已用 [`HistoryLimits::normalize`] 归一）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryQuery {
    /// 交易所标识。
    pub venue: String,
    /// 合约/交易对。
    pub symbol: String,
    /// 起点（UTC 毫秒，含）；None＝取最近 N 根。
    pub start: Option<i64>,
    /// 终点（UTC 毫秒，不含）；None＝开放。
    pub end: Option<i64>,
    /// 已归一的返回根数上限（≥1）。
    pub limit: usize,
}

/// 开窗/截断后的一页结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryPage {
    /// 命中的 bar，按 start_time 升序、按 start 去重。
    pub bars: Vec<Bar>,
    /// 窗口内命中数是否超过本次返回上限（客户端据此翻页）。
    pub has_more: bool,
    /// 本次实际生效窗口起点（explicit=请求起点；recent=首根 start_time；空=None）。
    pub eff_start: Option<i64>,
    /// 本次实际生效窗口终点（explicit=请求终点；recent=末根 start_time；空=None）。
    pub eff_end: Option<i64>,
}

/// 合并热/冷两份 bar，按 start 去重（**热优先**）、升序，再按查询开窗/截断。
///
/// - 显式区间：只取 `[start,end)`，升序返回**最早** `limit` 根，`has_more=窗口内总数>limit`。
/// - 不给起点（取最近）：在全部可得 bar 上取**最后** `limit` 根（即最近 N 根），
///   `has_more=总数>limit`。
pub fn merge_and_window(hot: Vec<Bar>, cold: Vec<Bar>, q: &HistoryQuery) -> HistoryPage {
    // 先放冷、再用热覆盖：BTreeMap 按 start 排序，热为较新事实。
    let mut merged: BTreeMap<i64, Bar> = BTreeMap::new();
    for b in cold {
        merged.insert(b.start_time, b);
    }
    for b in hot {
        merged.insert(b.start_time, b);
    }

    match q.start {
        // 显式区间：按 [start,end) 左闭右开过滤，取最早 limit 根。
        Some(start) => {
            let end = q.end.unwrap_or(i64::MAX);
            let in_window: Vec<Bar> = merged
                .into_values()
                .filter(|b| b.start_time >= start && b.start_time < end)
                .collect();
            let total = in_window.len();
            let page: Vec<Bar> = in_window.into_iter().take(q.limit).collect();
            HistoryPage {
                bars: page,
                has_more: total > q.limit,
                // 显式区间原样回带请求键（空结果亦然），客户端据此归属。
                eff_start: Some(start),
                eff_end: q.end,
            }
        }
        // 取最近 N：末尾 limit 根；eff 键回填首/末根 start_time（空=None）。
        None => {
            let total = merged.len();
            let skip = total.saturating_sub(q.limit);
            let page: Vec<Bar> = merged.into_values().skip(skip).collect();
            let eff_start = page.first().map(|b| b.start_time);
            let eff_end = page.last().map(|b| b.start_time);
            HistoryPage {
                bars: page,
                has_more: total > q.limit,
                eff_start,
                eff_end,
            }
        }
    }
}

/// 行模型 [`Bar`] → 南向 `KlineEvent`（与实时推送同形）：`exchange` 映射为 `venue`，
/// 周期恒为 "1m"，extras 同源 `BTreeMap` 直接平铺（序列化时 flatten）。
pub fn bar_to_kline_event(bar: &Bar) -> KlineEvent {
    KlineEvent {
        venue: bar.exchange.clone(),
        symbol: bar.symbol.clone(),
        interval: "1m".to_string(),
        trading_day: bar.trading_day,
        start_time: bar.start_time,
        end_time: bar.end_time,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.volume,
        quote_volume: bar.quote_volume,
        extras: bar.extras.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use trading_models::KlineExtra;

    fn bar(venue: &str, symbol: &str, start: i64, close: i64) -> Bar {
        let mut b = Bar::new(venue, symbol, 20261024, start, start + 60_000);
        b.close = Decimal::from(close);
        b.volume = close;
        b
    }

    fn starts(page: &HistoryPage) -> Vec<i64> {
        page.bars.iter().map(|b| b.start_time).collect()
    }

    #[test]
    fn limits_normalize_missing_zero_oversize() {
        let l = HistoryLimits::new(300, 1500).unwrap();
        assert_eq!(l.normalize(None), 300);
        assert_eq!(l.normalize(Some(0)), 300);
        assert_eq!(l.normalize(Some(100)), 100);
        assert_eq!(l.normalize(Some(5000)), 1500); // 超硬上限截断
    }

    #[test]
    fn limits_reject_invalid_construction() {
        assert!(HistoryLimits::new(0, 100).is_none());
        assert!(HistoryLimits::new(100, 0).is_none());
        assert!(
            HistoryLimits::new(101, 100).is_none(),
            "default 不得大于 max"
        );
        assert!(HistoryLimits::new(1, 1).is_some());
    }

    #[test]
    fn merges_hot_and_cold_dedup_hot_wins_sorted() {
        // cold 有 start 1/2/3，hot 有 3(新值)/4；同 start=3 以热为准。
        let cold = vec![
            bar("binance", "BTCUSDT", 60_000, 10),
            bar("binance", "BTCUSDT", 120_000, 20),
            bar("binance", "BTCUSDT", 180_000, 30),
        ];
        let hot = vec![
            bar("binance", "BTCUSDT", 180_000, 999), // 覆盖 cold 的 30
            bar("binance", "BTCUSDT", 240_000, 40),
        ];
        let q = HistoryQuery {
            venue: "binance".into(),
            symbol: "BTCUSDT".into(),
            start: None,
            end: None,
            limit: 100,
        };
        let page = merge_and_window(hot, cold, &q);
        assert_eq!(starts(&page), vec![60_000, 120_000, 180_000, 240_000]);
        assert_eq!(page.bars[2].close, Decimal::from(999), "同 start 热覆盖冷");
        assert!(!page.has_more);
    }

    #[test]
    fn explicit_window_is_half_open_and_takes_earliest_limit() {
        let bars: Vec<Bar> = (0..5)
            .map(|i| bar("ctp", "cu2512", (i + 1) * 60_000, i))
            .collect();
        let q = HistoryQuery {
            venue: "ctp".into(),
            symbol: "cu2512".into(),
            start: Some(60_000),
            end: Some(240_000), // 不含 240_000 → 命中 1/2/3 分
            limit: 2,
        };
        let page = merge_and_window(Vec::new(), bars, &q);
        // 左闭右开：60k/120k/180k；取最早 2 根，has_more。
        assert_eq!(starts(&page), vec![60_000, 120_000]);
        assert!(page.has_more);
        assert_eq!(page.eff_start, Some(60_000));
        assert_eq!(page.eff_end, Some(240_000));
    }

    #[test]
    fn recent_query_returns_last_n_with_effective_keys() {
        let bars: Vec<Bar> = (0..5)
            .map(|i| bar("ctp", "cu2512", (i + 1) * 60_000, i))
            .collect();
        let q = HistoryQuery {
            venue: "ctp".into(),
            symbol: "cu2512".into(),
            start: None,
            end: None,
            limit: 2,
        };
        let page = merge_and_window(Vec::new(), bars, &q);
        // 最近 2 根＝最大两个 start；eff 键回填首/末根 start_time。
        assert_eq!(starts(&page), vec![240_000, 300_000]);
        assert!(page.has_more);
        assert_eq!(page.eff_start, Some(240_000));
        assert_eq!(page.eff_end, Some(300_000));
    }

    #[test]
    fn empty_inputs_yield_empty_page_without_error() {
        let q = HistoryQuery {
            venue: "ctp".into(),
            symbol: "cu2512".into(),
            start: Some(0),
            end: Some(i64::MAX),
            limit: 10,
        };
        let page = merge_and_window(Vec::new(), Vec::new(), &q);
        assert!(page.bars.is_empty());
        assert!(!page.has_more);
        assert_eq!(page.eff_start, Some(0)); // explicit 回显请求键
        assert_eq!(page.eff_end, Some(i64::MAX));

        let recent = HistoryQuery {
            venue: "ctp".into(),
            symbol: "cu2512".into(),
            start: None,
            end: None,
            limit: 10,
        };
        let empty = merge_and_window(Vec::new(), Vec::new(), &recent);
        assert_eq!(empty.eff_start, None);
        assert_eq!(empty.eff_end, None);
    }

    #[test]
    fn maps_bar_to_kline_event_with_interval_and_extras() {
        let mut b = bar("ctp", "cu2512", 60_000, 100);
        b.quote_volume = Decimal::from(33);
        b.extras
            .insert("exchange_id".into(), KlineExtra::Str("SHFE".into()));
        b.extras
            .insert("open_interest".into(), KlineExtra::Num(Decimal::from(10)));

        let ev = bar_to_kline_event(&b);

        assert_eq!(ev.venue, "ctp");
        assert_eq!(ev.symbol, "cu2512");
        assert_eq!(ev.interval, "1m");
        assert_eq!(ev.start_time, 60_000);
        assert_eq!(ev.end_time, 120_000);
        assert_eq!(ev.close, Decimal::from(100));
        assert_eq!(ev.quote_volume, Decimal::from(33));
        assert_eq!(
            ev.extras.get("exchange_id"),
            Some(&KlineExtra::Str("SHFE".into()))
        );
    }
}
