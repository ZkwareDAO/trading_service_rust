//! Hyperliquid 具名行情流：本所**拥有**的 [`MarketDataStream`] 实现。
//!
//! 与交易侧 [`crate::hyperliquid::venue::HyperliquidUser`] 实现
//! [`crate::api::ExchangeApi`] 平级。HL 行情的所内额外处理（建连常驻 allMids 已在 feed
//! 层；如需订阅期包装/过滤）收口在本类型，连接生命周期复用
//! [`crate::market_ws::WsHubEngine`]，这里只做委托。

use std::sync::Arc;

use crate::market::{BarSink, DesiredSource, MarketDataStream, SubscriptionGuard};
use crate::market_ws::WsHubEngine;

/// Hyperliquid 永续行情流（进程单例，内部持一条 [`WsHubEngine`]）。
pub struct HlMarketStream {
    engine: Arc<WsHubEngine>,
}

impl HlMarketStream {
    /// 由已 spawn 的 WS 引擎包壳（provider 装配时调用）。
    pub(crate) fn from_engine(engine: Arc<WsHubEngine>) -> Self {
        Self { engine }
    }

    /// 注册与句柄同寿的托管系统行情来源（活跃仓位 ∪ 规则），不返回 guard。
    pub(crate) fn register_managed_prices(&self, src: Arc<dyn DesiredSource>) {
        self.engine.register_managed_prices(src);
    }

    /// 注册无条件 bar 下游（全量持久化）：委托给引擎，装配期注册一次。
    pub(crate) fn register_unconditional_bar_sink(&self, sink: Arc<dyn BarSink>) {
        self.engine.register_unconditional_bar_sink(sink);
    }
}

impl MarketDataStream for HlMarketStream {
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

    fn bar(symbol: &str) -> Kline {
        Kline {
            venue: "hyperliquid".into(),
            symbol: symbol.into(),
            interval: KlineInterval::M1,
            trading_day: 20261024,
            start_ms: 0,
            end_ms: 60_000,
            open: Decimal::ONE,
            high: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::ONE,
            volume: 1,
            quote_volume: Decimal::ONE,
            extras: std::collections::BTreeMap::new(),
        }
    }

    /// 无条件 sink 经具名流注册到引擎：零路由消费者时 driverless 派发也能收到。
    #[test]
    fn unconditional_sink_delegates_to_engine() {
        let engine = WsHubEngine::spawn_driverless();
        let reg = engine.registry_for_test();
        let stream = HlMarketStream::from_engine(engine);

        let persist = RecSink::new();
        stream.register_unconditional_bar_sink(persist.clone() as Arc<dyn BarSink>);

        assert_eq!(reg.dispatch_bar(&bar("BTC"), Duration::from_secs(1)), 0);
        assert_eq!(persist.seen(), vec!["BTC".to_string()]);
    }

    #[test]
    fn delegates_klines_and_routes_finalized_bar() {
        let engine = WsHubEngine::spawn_driverless();
        let reg = engine.registry_for_test();
        let stream: Arc<dyn MarketDataStream> =
            Arc::new(HlMarketStream::from_engine(engine.clone()));
        let sink = RecSink::new();

        let _guard = stream
            .subscribe_klines(
                Arc::new(FixedSymbols::new(vec!["BTC".into()])),
                sink.clone(),
            )
            .unwrap();
        reg.dispatch_bar(&bar("BTC"), Duration::from_secs(1));

        assert_eq!(sink.seen(), vec!["BTC".to_string()]);
    }

    #[test]
    fn dropping_guard_stops_delivery() {
        let engine = WsHubEngine::spawn_driverless();
        let reg = engine.registry_for_test();
        let stream: Arc<dyn MarketDataStream> =
            Arc::new(HlMarketStream::from_engine(engine.clone()));
        let sink = RecSink::new();

        let guard = stream
            .subscribe_klines(
                Arc::new(FixedSymbols::new(vec!["ETH".into()])),
                sink.clone(),
            )
            .unwrap();
        reg.dispatch_bar(&bar("ETH"), Duration::from_secs(1));
        drop(guard);
        reg.dispatch_bar(&bar("ETH"), Duration::from_secs(1));

        assert_eq!(sink.seen(), vec!["ETH".to_string()]);
    }
}
