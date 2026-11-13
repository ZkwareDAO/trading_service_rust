//! 按账户实现交易所双能力（REST 下单/查询 + 账户事件流）。

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::stream::{self, BoxStream, StreamExt};

use trading_exchange::api::{
    AccountEvent, AccountEventStream, BalanceInfo, ExchangeApi, OrderAck, OrderLookup, OrderRef,
    PlaceOrderRequest, PositionInfo, ReferencePrice,
};

use crate::ledger::SimLedger;

/// 模拟所「用户 × 交易所」主体：一个账户一个实例，共享同一份 [`SimLedger`]。
pub struct SimApi {
    ledger: Arc<SimLedger>,
    account: String,
}

impl SimApi {
    pub fn new(ledger: Arc<SimLedger>, account: impl Into<String>) -> Self {
        Self {
            ledger,
            account: account.into(),
        }
    }
}

#[async_trait]
impl ExchangeApi for SimApi {
    fn exchange(&self) -> &str {
        self.ledger.config().exchange
    }

    async fn place_order(&self, req: &PlaceOrderRequest) -> anyhow::Result<OrderAck> {
        self.ledger.place(&self.account, req)
    }

    async fn cancel_order(&self, _instrument: &str, order: &OrderRef) -> anyhow::Result<OrderAck> {
        self.ledger.cancel(order)
    }

    async fn query_order(&self, _instrument: &str, order: &OrderRef) -> OrderLookup {
        self.ledger.query(order)
    }

    async fn get_positions(&self) -> anyhow::Result<Vec<PositionInfo>> {
        Ok(self.ledger.positions(&self.account))
    }

    async fn get_balance(&self) -> anyhow::Result<Vec<BalanceInfo>> {
        Ok(self.ledger.balance(&self.account))
    }

    async fn set_leverage(&self, _instrument: &str, leverage: u32) -> anyhow::Result<()> {
        self.ledger.set_leverage(leverage);
        Ok(())
    }

    async fn get_reference_price(&self, instrument: &str) -> anyhow::Result<ReferencePrice> {
        self.ledger.reference_price(instrument)
    }
}

#[async_trait]
impl AccountEventStream for SimApi {
    async fn subscribe_account_events(&self) -> anyhow::Result<BoxStream<'static, AccountEvent>> {
        let ledger = Arc::clone(&self.ledger);
        let account = self.account.clone();
        // 每账户独立 Notify + 共享队列：先注册 notified 再查队列，避免唤醒丢失，
        // 且多账户流不会互相错唤醒。
        let notifier = ledger.notifier_for(&account);
        let stream = stream::unfold(
            (ledger, account, notifier),
            |(ledger, account, notifier)| async move {
                loop {
                    let notified = notifier.notified();
                    if let Some(ev) = ledger.pop_event(&account) {
                        drop(notified);
                        return Some((ev, (ledger, account, notifier)));
                    }
                    notified.await;
                }
            },
        );
        Ok(stream.boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    use rust_decimal::Decimal;
    use trading_exchange::api::{OrderPositionSide, PriceBasis};
    use trading_models::{InstrumentType, OrderStatus, OrderType, Side};

    use crate::SimConfig;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn market_req(cid: &str) -> PlaceOrderRequest {
        PlaceOrderRequest {
            instrument: "BTCUSDT".into(),
            instrument_type: InstrumentType::Futures,
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            quantity: dec("1"),
            reduce_only: false,
            position_side: OrderPositionSide::Both,
            time_in_force: None,
            client_order_id: cid.into(),
        }
    }

    #[tokio::test]
    async fn api_places_market_order_and_reports_position() {
        let ledger = Arc::new(SimLedger::new(SimConfig::default()));
        ledger.set_mark("BTCUSDT", dec("100"));
        let api = SimApi::new(Arc::clone(&ledger), "acct");

        let ack = api.place_order(&market_req("c1")).await.unwrap();
        assert!(ack.accepted);
        let positions = api.get_positions().await.unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].signed_quantity, dec("1"));

        let price = api.get_reference_price("BTCUSDT").await.unwrap();
        assert_eq!(price.basis, PriceBasis::Mark);
        assert_eq!(price.price, dec("100"));
    }

    #[tokio::test]
    async fn account_stream_yields_order_events_in_order() {
        let ledger = Arc::new(SimLedger::new(SimConfig::default()));
        ledger.set_mark("BTCUSDT", dec("100"));
        let api = SimApi::new(Arc::clone(&ledger), "acct");
        api.place_order(&market_req("c1")).await.unwrap();

        let mut stream = api.subscribe_account_events().await.unwrap();
        let first = stream.next().await.unwrap();
        let second = stream.next().await.unwrap();
        let statuses: Vec<_> = [first, second]
            .into_iter()
            .filter_map(|ev| match ev {
                AccountEvent::Order(o) => Some(o.status),
                AccountEvent::Account(_) => None,
            })
            .collect();
        assert_eq!(statuses, vec![OrderStatus::New, OrderStatus::Filled]);
    }
}
