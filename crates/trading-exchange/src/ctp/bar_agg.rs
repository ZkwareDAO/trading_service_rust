//! CTP tick → bar 聚合 sink（K线集作用域）。
//!
//! 持有 `合约 → CandleAggregator`：每笔 K线集 tick 喂入对应聚合器，分钟翻转时产出
//! 定稿 [`Kline`]；**品种 key（如订阅了 `cu`）只放行当前主力合约的 bar**，具体合约
//! key（如 cu2512）直推。bar 经注入的 [`BarSink`] 同步回调（HubRegistry 再做
//! catch_unwind+计时摘除）。
//!
//! 本类型**不做主力选举**（选举在 [`crate::ctp::dominant`] + 收盘任务），只按
//! `DominantTable` 当前值过滤；选举冻结期间表沿用上一交易日值，故盘中不漂移。
//!
//! 纯内存、单写者（CTP drain 线程），方法同步廉价；TickTable 写入在 drain 调用点
//! 与本聚合同路径完成（见 B4 接线）。

use std::collections::HashMap;
use std::sync::Arc;

use rust_decimal::Decimal;
use trading_core::kline::DominantTable;

use crate::ctp::candle::{CandleAggregator, CandleTick};
use crate::ctp::dominant::product_prefix;
use crate::market::{BarSink, Kline};

/// K线集 tick 聚合器（单连接一份）。
pub struct BarAggSink {
    venue: String,
    /// 合约 → 聚合器（按出现的合约懒创建）。
    aggregators: HashMap<String, CandleAggregator>,
    /// 主力表（品种 → 当前主力），用于品种 key 过滤。
    dominants: Arc<DominantTable>,
    /// 定稿 bar 的下游（HubRegistry 路由给 K线消费者）。
    sink: Arc<dyn BarSink>,
}

impl BarAggSink {
    pub fn new(
        venue: impl Into<String>,
        dominants: Arc<DominantTable>,
        sink: Arc<dyn BarSink>,
    ) -> Self {
        Self {
            venue: venue.into(),
            aggregators: HashMap::new(),
            dominants,
            sink,
        }
    }

    /// 喂入一笔 K线集 tick：更新对应合约聚合器，若分钟翻转则产出定稿 bar 并过滤推送。
    /// 同路径的 TickTable/LastVolume 写入由 drain 调用点负责。
    pub fn on_tick(&mut self, symbol: &str, t: &CandleTick) {
        let venue = self.venue.clone();
        let agg = self
            .aggregators
            .entry(symbol.to_string())
            .or_insert_with(|| CandleAggregator::new(venue, symbol));
        if let Some(bar) = agg.on_tick(t) {
            self.dispatch_if_dominant(symbol, bar);
        }
    }

    /// 强制定稿所有未闭桶（日切/停机/测试）：返回全部定稿 bar，主力者同时推送。
    pub fn flush_all(&mut self) -> Vec<Kline> {
        let symbols: Vec<String> = self.aggregators.keys().cloned().collect();
        let mut finished = Vec::new();
        for symbol in symbols {
            if let Some(agg) = self.aggregators.get_mut(&symbol) {
                if let Some(bar) = agg.flush() {
                    if self.is_routed(&symbol) {
                        self.sink.on_bar(&bar);
                    }
                    finished.push(bar);
                }
            }
        }
        finished
    }

    /// 该合约的定稿 bar 是否应推送：消费者若以**品种**订阅（如 cu），仅当本合约是
    /// 当前主力；消费者以**具体合约**订阅时，hub 注册表按 bar.symbol 精确匹配放行。
    fn dispatch_if_dominant(&self, symbol: &str, bar: Kline) {
        if self.is_routed(symbol) {
            self.sink.on_bar(&bar);
        }
    }

    fn is_routed(&self, symbol: &str) -> bool {
        match product_prefix(symbol) {
            // 有品种前缀：仅当该品种当前主力就是本合约才推（无主力记录则暂不推，
            // 等收盘选举/重启沿用上一日值后再放行——避免冷启动把非主力误当主力）。
            Some(product) => self
                .dominants
                .dominant_symbol(&product)
                .is_some_and(|dom| dom == symbol),
            // 无字母前缀的非常规符号：按具体合约直推（不拦截）。
            None => true,
        }
    }
}

/// 把 CTP 行情快照字段映射为聚合输入（供 drain 调用；无 IO）。
pub fn to_candle_tick(
    trading_day: u32,
    update_ms: i64,
    price: Decimal,
    cum_volume: i64,
    cum_turnover: Decimal,
    open_interest: Decimal,
) -> CandleTick {
    CandleTick {
        trading_day,
        update_ms,
        price,
        cum_volume,
        cum_turnover,
        open_interest,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// 记录收到 bar 的 sink。
    #[derive(Default)]
    struct RecSink {
        bars: Mutex<Vec<String>>,
    }
    impl BarSink for RecSink {
        fn on_bar(&self, bar: &Kline) {
            self.bars.lock().unwrap().push(bar.symbol.clone());
        }
    }

    fn tick(day: u32, ms: i64, price: i64, vol: i64) -> CandleTick {
        CandleTick {
            trading_day: day,
            update_ms: ms,
            price: Decimal::from(price),
            cum_volume: vol,
            cum_turnover: Decimal::ZERO,
            open_interest: Decimal::ZERO,
        }
    }

    fn dominants(product: &str, symbol: &str, day: u32) -> Arc<DominantTable> {
        let t = DominantTable::new();
        t.put(
            product,
            trading_core::kline::DominantEntry {
                symbol: symbol.into(),
                trading_day: day,
                cum_volume: 0,
            },
        );
        Arc::new(t)
    }

    #[test]
    fn only_dominant_contract_bars_pass_product_filter() {
        let sink = Arc::new(RecSink::default());
        let dom = dominants("cu", "cu2512", 20261023);
        let mut agg = BarAggSink::new("ctp", dom, sink.clone() as Arc<dyn BarSink>);

        // 非主力 cu2601：分钟翻转也不推送。
        agg.on_tick("cu2601", &tick(20261023, 600_000, 100, 10));
        agg.on_tick("cu2601", &tick(20261023, 660_000, 101, 11));
        // 主力 cu2512：翻转推送。
        agg.on_tick("cu2512", &tick(20261023, 600_000, 200, 20));
        agg.on_tick("cu2512", &tick(20261023, 660_000, 201, 25));

        let got = sink.bars.lock().unwrap().clone();
        assert_eq!(got, vec!["cu2512".to_string()]);
    }

    #[test]
    fn no_dominant_record_blocks_until_elected() {
        let sink = Arc::new(RecSink::default());
        let dom = Arc::new(DominantTable::new()); // 尚未选举
        let mut agg = BarAggSink::new("ctp", dom, sink.clone() as Arc<dyn BarSink>);
        agg.on_tick("cu2512", &tick(20261023, 600_000, 100, 10));
        agg.on_tick("cu2512", &tick(20261023, 660_000, 101, 11));
        assert!(sink.bars.lock().unwrap().is_empty());
    }

    #[test]
    fn dominant_switch_then_new_contract_routes() {
        let sink = Arc::new(RecSink::default());
        let dom = Arc::new(DominantTable::new());
        dom.put(
            "cu",
            trading_core::kline::DominantEntry {
                symbol: "cu2512".into(),
                trading_day: 20261023,
                cum_volume: 0,
            },
        );
        let mut agg = BarAggSink::new("ctp", dom.clone(), sink.clone() as Arc<dyn BarSink>);
        agg.on_tick("cu2512", &tick(20261023, 600_000, 100, 10));
        agg.on_tick("cu2512", &tick(20261023, 660_000, 101, 11));
        assert_eq!(sink.bars.lock().unwrap().len(), 1);

        // 次日换月到 cu2601：旧主力不再推，新主力推。
        dom.put(
            "cu",
            trading_core::kline::DominantEntry {
                symbol: "cu2601".into(),
                trading_day: 20261024,
                cum_volume: 0,
            },
        );
        agg.on_tick("cu2512", &tick(20261024, 600_000, 100, 1));
        agg.on_tick("cu2512", &tick(20261024, 660_000, 101, 2));
        agg.on_tick("cu2601", &tick(20261024, 600_000, 100, 3));
        agg.on_tick("cu2601", &tick(20261024, 660_000, 101, 4));
        let got = sink.bars.lock().unwrap().clone();
        assert_eq!(got, vec!["cu2512", "cu2601"]);
    }

    #[test]
    fn to_candle_tick_carries_fields() {
        let t = to_candle_tick(
            20261023,
            1,
            Decimal::from(3),
            4,
            Decimal::from(5),
            Decimal::from(6),
        );
        assert_eq!(t.trading_day, 20261023);
        assert_eq!(t.cum_volume, 4);
        assert_eq!(t.open_interest, Decimal::from(6));
    }
}
