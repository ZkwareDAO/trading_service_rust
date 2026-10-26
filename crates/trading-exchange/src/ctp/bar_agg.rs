//! CTP tick → bar 聚合 sink（K线集作用域）。
//!
//! 持有 `合约 → CandleAggregator`：每笔 K线集 tick 喂入对应聚合器，分钟翻转时产出
//! 定稿 [`Kline`]，**一律**经注入的 [`BarSink`]（即 [`crate::ctp::market_stream::BarRouter`]）
//! 上抛；本类型不做主力过滤。
//!
//! 「品种订阅只收主力 / 具体合约订阅直收」的路由判定在 `BarRouter::dispatch`：
//! 它掌握每个消费者的订阅符号，精确合约命中无条件放行，仅品种前缀命中才查主力表。
//! 聚合器对 K线集内所有合约（含非主力）一律聚合，使 tick→bar 采集不因过滤丢失。
//!
//! 本类型**不做主力选举**（选举在 [`crate::ctp::dominant`] + 收盘任务）。
//!
//! 纯内存、单写者（CTP drain 线程），方法同步廉价；TickTable 写入在 drain 调用点
//! 与本聚合同路径完成（见 B4 接线）。

use std::collections::HashMap;
use std::sync::Arc;

use rust_decimal::Decimal;

use crate::ctp::candle::{CandleAggregator, CandleTick};
use crate::market::{BarSink, Kline};

/// K线集 tick 聚合器（单连接一份）。
pub struct BarAggSink {
    venue: String,
    /// 合约 → 聚合器（按出现的合约懒创建）。
    aggregators: HashMap<String, CandleAggregator>,
    /// 定稿 bar 的下游（BarRouter 按订阅意图 + 主力表路由给 K线消费者）。
    sink: Arc<dyn BarSink>,
}

impl BarAggSink {
    pub fn new(venue: impl Into<String>, sink: Arc<dyn BarSink>) -> Self {
        Self {
            venue: venue.into(),
            aggregators: HashMap::new(),
            sink,
        }
    }

    /// 喂入一笔 K线集 tick：更新对应合约聚合器，分钟翻转即把定稿 bar 交下游路由。
    /// 是否真正推给某消费者（品种订阅需主力）由下游 BarRouter 判定。
    /// 同路径的 TickTable/LastVolume 写入由 drain 调用点负责。
    pub fn on_tick(&mut self, symbol: &str, t: &CandleTick) {
        let venue = self.venue.clone();
        let agg = self
            .aggregators
            .entry(symbol.to_string())
            .or_insert_with(|| CandleAggregator::new(venue, symbol));
        if let Some(bar) = agg.on_tick(t) {
            tracing::debug!(
                symbol,
                trading_day = bar.trading_day,
                start_ms = bar.start_ms,
                end_ms = bar.end_ms,
                open = %bar.open,
                high = %bar.high,
                low = %bar.low,
                close = %bar.close,
                volume = bar.volume,
                "CTP 1m K线定稿"
            );
            self.sink.on_bar(&bar);
        }
    }

    /// 强制定稿所有未闭桶（日切/停机/测试）：返回全部定稿 bar 并逐个交下游路由。
    pub fn flush_all(&mut self) -> Vec<Kline> {
        let symbols: Vec<String> = self.aggregators.keys().cloned().collect();
        let mut finished = Vec::new();
        for symbol in symbols {
            if let Some(agg) = self.aggregators.get_mut(&symbol) {
                if let Some(bar) = agg.flush() {
                    self.sink.on_bar(&bar);
                    finished.push(bar);
                }
            }
        }
        finished
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

    #[test]
    fn finalized_bar_emitted_for_every_contract_without_dominant_filter() {
        // 聚合层不再懂主力：任何 K线集合约跨分钟定稿都上抛下游（过滤在 BarRouter）。
        let sink = Arc::new(RecSink::default());
        let mut agg = BarAggSink::new("ctp", sink.clone() as Arc<dyn BarSink>);

        // 两个合约（含一个"非主力"角色的 cu2601）各跨一分钟：都应上抛。
        agg.on_tick("cu2601", &tick(20261023, 600_000, 100, 10));
        agg.on_tick("cu2601", &tick(20261023, 660_000, 101, 11));
        agg.on_tick("cu2512", &tick(20261023, 600_000, 200, 20));
        agg.on_tick("cu2512", &tick(20261023, 660_000, 201, 25));

        let got = sink.bars.lock().unwrap().clone();
        assert_eq!(got, vec!["cu2601".to_string(), "cu2512".to_string()]);
    }

    #[test]
    fn no_bar_before_minute_rollover() {
        // 同一分钟内多笔不定稿，下游收不到。
        let sink = Arc::new(RecSink::default());
        let mut agg = BarAggSink::new("ctp", sink.clone() as Arc<dyn BarSink>);
        agg.on_tick("cu2512", &tick(20261023, 600_000, 100, 10));
        agg.on_tick("cu2512", &tick(20261023, 630_000, 101, 11));
        assert!(sink.bars.lock().unwrap().is_empty());
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
