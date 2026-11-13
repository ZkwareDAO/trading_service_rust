//! 模拟所套件：把 [`SimLedger`] 接入宿主注册表（[`VenueProvider`]）。
//!
//! 复用真实宿主的账户事件消费循环（[`trading_exchange::account_stream::run`]），
//! 只在最外层替换「交易所适配器」，从而让 E2E 走同一条生产编排路径。

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use trading_core::quote::QuoteTable;
use trading_exchange::account_stream::{positive_millis, AccountUser};
use trading_exchange::api::ExchangeApi;
use trading_exchange::port::{OrderEventSink, PositionSnapshotSink, QuoteObserver};
use trading_exchange::position_profile::PositionProfile;
use trading_exchange::provider::{ProviderDeps, VenueProvider};
use trading_models::{Instrument, Quote};

use crate::api::SimApi;
use crate::ledger::SimLedger;
use crate::{now_ms, SimConfig};

/// 模拟所 provider：注册表里按 [`SimConfig::exchange`] 精确名路由。
pub struct SimVenue {
    ledger: Arc<SimLedger>,
    cfg: SimConfig,
    quotes: Arc<QuoteTable>,
    observer: Option<Arc<dyn QuoteObserver>>,
    position_sink: Arc<dyn PositionSnapshotSink>,
    debounce: Duration,
    reconcile: Duration,
}

impl SimVenue {
    /// 显式依赖构造（单测用）。
    pub fn new(
        ledger: Arc<SimLedger>,
        cfg: SimConfig,
        quotes: Arc<QuoteTable>,
        observer: Option<Arc<dyn QuoteObserver>>,
        position_sink: Arc<dyn PositionSnapshotSink>,
        debounce: Duration,
        reconcile: Duration,
    ) -> Self {
        Self {
            ledger,
            cfg,
            quotes,
            observer,
            position_sink,
            debounce,
            reconcile,
        }
    }

    /// 由宿主 [`ProviderDeps`] 构造（E2E registry 工厂用）。
    pub fn from_deps(ledger: Arc<SimLedger>, cfg: SimConfig, deps: &ProviderDeps) -> Self {
        Self::new(
            ledger,
            cfg,
            Arc::clone(&deps.quotes),
            deps.quote_observer.clone(),
            Arc::clone(&deps.position_sink),
            deps.position_debounce,
            deps.position_reconcile_interval,
        )
    }

    pub fn ledger(&self) -> Arc<SimLedger> {
        Arc::clone(&self.ledger)
    }

    /// 写 mark：更新台账 + 宿主行情表，并同步回调仓位重估观察口。
    pub fn set_mark(&self, instrument: &str, price: rust_decimal::Decimal) {
        self.ledger.set_mark(instrument, price);
        let ts = now_ms();
        self.quotes.put(
            &Instrument::new(self.cfg.quote_venue, self.cfg.quote_kind, instrument),
            Quote {
                last: Some(price),
                mark: Some(price),
                ts_exchange_ms: ts,
                ts_recv_ms: ts,
                ..Quote::default()
            },
        );
        if let Some(observer) = &self.observer {
            observer.on_quote(self.cfg.quote_venue, self.cfg.quote_kind, instrument, price);
        }
    }
}

impl VenueProvider for SimVenue {
    fn matches(&self, exchange: &str) -> bool {
        exchange == self.cfg.exchange
    }

    /// 接入仓位流水线：对账/行情入站键全部取自配置。
    fn position_profile(&self) -> Option<PositionProfile> {
        Some(PositionProfile {
            exchange: self.cfg.exchange,
            instrument_type: self.cfg.instrument_type,
            quote_venue: self.cfg.quote_venue,
            quote_kind: self.cfg.quote_kind,
            fallback_valuation: Some((self.cfg.quote_venue, self.cfg.quote_kind)),
        })
    }

    fn build_api(
        &self,
        _exchange: &str,
        _instrument_type: Option<&str>,
        account: &str,
    ) -> anyhow::Result<Arc<dyn ExchangeApi>> {
        Ok(Arc::new(SimApi::new(Arc::clone(&self.ledger), account)))
    }

    fn spawn_account_stream(
        &self,
        exchange: &str,
        account: &str,
        sink: Arc<dyn OrderEventSink>,
    ) -> Option<JoinHandle<()>> {
        if exchange != self.cfg.exchange {
            return None;
        }
        let name = self.cfg.exchange;
        let ledger = Arc::clone(&self.ledger);
        let position_sink = Arc::clone(&self.position_sink);
        let account = account.to_string();
        let debounce_ms = positive_millis(self.debounce);
        let full_ms = positive_millis(self.reconcile);
        Some(tokio::spawn(async move {
            let user_for = move |acct: &str| -> anyhow::Result<Arc<dyn AccountUser>> {
                Ok(Arc::new(SimApi::new(Arc::clone(&ledger), acct)))
            };
            trading_exchange::account_stream::run(
                name,
                &account,
                &user_for,
                &sink,
                &position_sink,
                debounce_ms,
                full_ms,
            )
            .await;
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::str::FromStr;
    use std::sync::Mutex;

    use rust_decimal::Decimal;
    use trading_exchange::api::PositionInfo;
    use trading_models::{MarketKind, Venue};

    use crate::SimConfig;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    struct NoopSink;
    #[async_trait]
    impl PositionSnapshotSink for NoopSink {
        async fn on_position_snapshot(&self, _: &str, _: &str, _: &[PositionInfo]) {}
    }

    #[derive(Default)]
    struct Recorder(Mutex<Vec<(Venue, MarketKind, String, Decimal)>>);
    impl QuoteObserver for Recorder {
        fn on_quote(&self, venue: Venue, kind: MarketKind, symbol: &str, price: Decimal) {
            self.0
                .lock()
                .unwrap()
                .push((venue, kind, symbol.to_string(), price));
        }
    }

    fn venue_with(recorder: Arc<Recorder>) -> (SimVenue, Arc<QuoteTable>) {
        let cfg = SimConfig::default();
        let ledger = Arc::new(SimLedger::new(cfg.clone()));
        let quotes = Arc::new(QuoteTable::new());
        let observer: Arc<dyn QuoteObserver> = recorder;
        let venue = SimVenue::new(
            ledger,
            cfg,
            Arc::clone(&quotes),
            Some(observer),
            Arc::new(NoopSink),
            Duration::from_millis(10),
            Duration::from_secs(60),
        );
        (venue, quotes)
    }

    #[test]
    fn position_profile_uses_configured_keys() {
        let venue = venue_with(Arc::new(Recorder::default())).0;
        let p = venue.position_profile().expect("sim 必须接入仓位流水线");
        assert_eq!(p.exchange, "sim");
        assert_eq!(p.quote_venue, Venue::BinanceFutures);
        assert_eq!(p.quote_kind, MarketKind::Futures);
    }

    #[test]
    fn set_mark_writes_quote_table_and_notifies_observer() {
        let recorder = Arc::new(Recorder::default());
        let (venue, quotes) = venue_with(Arc::clone(&recorder));

        venue.set_mark("BTCUSDT", dec("101.5"));

        let inst = Instrument::new(Venue::BinanceFutures, MarketKind::Futures, "BTCUSDT");
        assert_eq!(quotes.valuation_price(&inst), Some(dec("101.5")));
        let seen = recorder.0.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].2, "BTCUSDT");
        assert_eq!(seen[0].3, dec("101.5"));
        assert_eq!(venue.ledger().mark("BTCUSDT"), Some(dec("101.5")));
    }

    #[test]
    fn matches_is_exact_and_build_api_routes_to_ledger() {
        let venue = venue_with(Arc::new(Recorder::default())).0;
        assert!(venue.matches("sim"));
        assert!(!venue.matches("SIM"));
        let api = venue.build_api("sim", Some("futures"), "acct").unwrap();
        assert_eq!(api.exchange(), "sim");
    }
}
