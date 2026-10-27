//! Binance 具名行情流：本所**拥有**的 [`MarketDataStream`] 实现。
//!
//! 与交易侧 [`crate::binance::venue::BinanceUser`] 实现 [`crate::api::ExchangeApi`]
//! 平级——binance 行情侧的额外处理都收口在本类型，不改统一 trait、不改共享引擎、
//! 不动其他所。WS 连接生命周期（退避/Ping/看门狗/对账）仍复用
//! [`crate::market_ws::WsHubEngine`]，本类型只做委托；现货/合约是同类型的两个实例
//! （由 provider 各包一个引擎）。

use std::sync::Arc;

use crate::market::{BarSink, DesiredSource, MarketDataStream, SubscriptionGuard};
use crate::market_ws::WsHubEngine;

/// Binance 行情流（现货/合约各一个实例，内部各自持一条 [`WsHubEngine`]）。
pub struct BinanceMarketStream {
    /// 被委托的共享 WS 引擎（连接任务 + 消费者注册表都在其中）。
    engine: Arc<WsHubEngine>,
}

impl BinanceMarketStream {
    /// 由已 spawn 的 WS 引擎包壳（provider 装配时调用）。
    pub(crate) fn from_engine(engine: Arc<WsHubEngine>) -> Self {
        Self { engine }
    }
}

impl MarketDataStream for BinanceMarketStream {
    fn subscribe_prices(&self, src: Arc<dyn DesiredSource>) -> anyhow::Result<SubscriptionGuard> {
        self.engine.subscribe_prices(src)
    }

    fn subscribe_klines(
        &self,
        src: Arc<dyn DesiredSource>,
        sink: Arc<dyn BarSink>,
    ) -> anyhow::Result<SubscriptionGuard> {
        self.engine.subscribe_klines(src, sink)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::{FixedSymbols, Kline, KlineInterval};
    use rust_decimal::Decimal;
    use std::sync::Mutex;
    use std::time::Duration;

    /// 记录收到的 bar 符号的 sink。
    struct RecSink {
        seen: Mutex<Vec<String>>,
    }
    impl RecSink {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                seen: Mutex::new(Vec::new()),
            })
        }
        fn seen(&self) -> Vec<String> {
            self.seen.lock().unwrap().clone()
        }
    }
    impl BarSink for RecSink {
        fn on_bar(&self, bar: &Kline) {
            self.seen.lock().unwrap().push(bar.symbol.clone());
        }
    }

    fn bar(symbol: &str, volume: i64) -> Kline {
        Kline {
            venue: "binance".into(),
            symbol: symbol.into(),
            interval: KlineInterval::M1,
            trading_day: 20261024,
            start_ms: 0,
            end_ms: 60_000,
            open: Decimal::ONE,
            high: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::ONE,
            volume,
            turnover: Decimal::ONE,
            open_interest: Decimal::ZERO,
        }
    }

    /// 经具名流订阅的定稿 bar 能被引擎路由到消费者。
    #[test]
    fn delegates_klines_and_routes_finalized_bar() {
        // Arrange：无驱动任务引擎（不起连接），包成 binance 具名流。
        let engine = WsHubEngine::spawn_driverless();
        let reg = engine.registry_for_test();
        let stream: Arc<dyn MarketDataStream> =
            Arc::new(BinanceMarketStream::from_engine(engine.clone()));
        let sink = RecSink::new();

        // Act：经统一 trait 订阅，再模拟驱动收收盘帧后路由定稿 bar。
        let _guard = stream
            .subscribe_klines(
                Arc::new(FixedSymbols::new(vec!["btcusdt".into()])),
                sink.clone(),
            )
            .unwrap();
        assert_eq!(
            reg.dispatch_bar(&bar("btcusdt", 1), Duration::from_secs(1)),
            0
        );

        // Assert
        assert_eq!(sink.seen(), vec!["btcusdt".to_string()]);
    }

    /// 退订凭证 drop 后，该消费者不再收到 bar（下轮对账退订由引擎保证）。
    #[test]
    fn dropping_guard_stops_delivery() {
        let engine = WsHubEngine::spawn_driverless();
        let reg = engine.registry_for_test();
        let stream: Arc<dyn MarketDataStream> =
            Arc::new(BinanceMarketStream::from_engine(engine.clone()));
        let sink = RecSink::new();

        let guard = stream
            .subscribe_klines(
                Arc::new(FixedSymbols::new(vec!["ethusdt".into()])),
                sink.clone(),
            )
            .unwrap();
        reg.dispatch_bar(&bar("ethusdt", 1), Duration::from_secs(1));
        drop(guard);
        reg.dispatch_bar(&bar("ethusdt", 1), Duration::from_secs(1));

        assert_eq!(sink.seen(), vec!["ethusdt".to_string()]);
    }

    /// 扩展缝证明：交易所可在**自己的具名流**内包装/过滤 sink 增加额外处理，
    /// 统一 trait、共享引擎、上层 K线服务均无感知（不必改任何上层代码）。
    ///
    /// 这里模拟「丢弃零增量成交量的废 bar 并计数」的所内私有逻辑。
    struct FilteredStream {
        engine: Arc<WsHubEngine>,
    }
    impl FilteredStream {
        fn new(engine: Arc<WsHubEngine>) -> Self {
            Self { engine }
        }
    }
    /// 包一层的 sink：先做所内过滤，再把合格 bar 转交上游消费者。
    struct FilterSink {
        inner: Arc<dyn BarSink>,
    }
    impl BarSink for FilterSink {
        fn on_bar(&self, bar: &Kline) {
            // 关键：额外处理只出现在该所自己的 impl 内，对引擎与上层透明。
            if bar.volume > 0 {
                self.inner.on_bar(bar);
            }
        }
    }
    impl MarketDataStream for FilteredStream {
        fn subscribe_prices(
            &self,
            src: Arc<dyn DesiredSource>,
        ) -> anyhow::Result<SubscriptionGuard> {
            self.engine.subscribe_prices(src)
        }
        fn subscribe_klines(
            &self,
            src: Arc<dyn DesiredSource>,
            sink: Arc<dyn BarSink>,
        ) -> anyhow::Result<SubscriptionGuard> {
            let decorated: Arc<dyn BarSink> = Arc::new(FilterSink { inner: sink });
            self.engine.subscribe_klines(src, decorated)
        }
    }

    #[test]
    fn venue_specific_sink_decoration_needs_no_upper_layer_change() {
        // Arrange：一个「带额外处理」的具名流（同 ExchangeApi 自包含等级）。
        let engine = WsHubEngine::spawn_driverless();
        let reg = engine.registry_for_test();
        let stream: Arc<dyn MarketDataStream> = Arc::new(FilteredStream::new(engine.clone()));
        // 上层消费者对装饰毫无感知，只拿到最终结果。
        let upstream = RecSink::new();

        // Act
        let _guard = stream
            .subscribe_klines(
                Arc::new(FixedSymbols::new(vec!["x".into()])),
                upstream.clone(),
            )
            .unwrap();
        reg.dispatch_bar(&bar("x", 5), Duration::from_secs(1)); // 合格
        reg.dispatch_bar(&bar("x", 0), Duration::from_secs(1)); // 废 bar，被所内过滤
        reg.dispatch_bar(&bar("x", 2), Duration::from_secs(1)); // 合格

        // Assert：上游只收到 2 根合格 bar，过滤逻辑全在该所 impl 内。
        assert_eq!(upstream.seen(), vec!["x".to_string(), "x".to_string()]);
    }
}
