//! Xili 交易所适配器（教学案例所，与 [`crate::binance`] 平级）。
//!
//! 它是一家**正常的交易所**：对内把统一请求翻译成 xili 进程的 HTTP/WS 线协议，
//! 对外实现统一 [`ExchangeApi`]；账户事件同样是服务**主动外连**一条 WS，
//! 解析出的订单回报经 [`OrderEventSink`] 上交宿主，不反向依赖服务内部状态。
//!
//! 初始化、连接方式、trait 实现、代码层级均与 binance 一致，没有为它做额外封装。

pub mod provider;
pub mod user_stream;

use async_trait::async_trait;
use reqwest::Client;
use rust_decimal::Decimal;
use trading_core::protocol::{
    PositionSnapshot, RestAck, RestCancelOrder, RestPlaceOrder, REST_CANCEL_PATH, REST_PLACE_PATH,
    REST_POSITIONS_PATH,
};
use trading_models::{InstrumentType, OrderStatus, TimeInForce};

use crate::api::{
    ExchangeApi, OrderAck, OrderLookup, OrderRef, PlaceOrderRequest, PositionInfo, ReferencePrice,
};
use crate::provider::VenueEndpoint;
use crate::translate::status_from_wire;

/// Xili 默认本地端点（未用 `--exchange` 覆盖时使用）。
pub const DEFAULT_NAME: &str = "xili";
const DEFAULT_REST_BASE_URL: &str = "http://127.0.0.1:9200";
const DEFAULT_WS_URL: &str = "ws://127.0.0.1:9200/ws";

/// 补出默认 xili 端点：配置未显式给出精确名为 xili 的端点时追加，否则以配置为准。
pub fn with_default_endpoint(mut endpoints: Vec<VenueEndpoint>) -> Vec<VenueEndpoint> {
    if !endpoints.iter().any(|e| e.name == DEFAULT_NAME) {
        endpoints.push(VenueEndpoint::xili_default());
    }
    endpoints
}

impl VenueEndpoint {
    /// 构造默认 xili 端点（127.0.0.1:9200）；常量直构，无失败路径。
    pub fn xili_default() -> Self {
        Self {
            name: DEFAULT_NAME.to_string(),
            rest_base_url: DEFAULT_REST_BASE_URL.to_string(),
            ws_url: DEFAULT_WS_URL.to_string(),
        }
    }
}

/// 对接 xili 的适配器。对外是统一 [`ExchangeApi`]，对内把统一请求翻译成
/// xili 进程的 HTTP 线协议。
pub struct XiliExchange {
    name: String,
    account: String,
    base_url: String,
    http: Client,
}

impl XiliExchange {
    pub fn new(
        name: impl Into<String>,
        base_url: impl Into<String>,
        http: Client,
        account: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            account: account.into(),
            base_url: base_url.into(),
            http,
        }
    }

    async fn post<Ack: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &impl serde::Serialize,
    ) -> anyhow::Result<Ack> {
        let url = format!("{}{}", self.base_url, path);
        tracing::info!(exchange = %self.name, url = %url, body = ?serde_json::to_value(body), "-> xili REST request");
        let resp = self.http.post(url).json(body).send().await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        tracing::info!(exchange = %self.name, http_status = %status, body = %text, "<- xili REST response");
        if !status.is_success() {
            anyhow::bail!("exchange {} 返回 HTTP {status}: {text}", self.name);
        }
        serde_json::from_str::<Ack>(&text)
            .map_err(|e| anyhow::anyhow!("解析应答失败: {e}; body={text}"))
    }

    /// POST 并要求交易所受理（accepted=true），统一转成 [`OrderAck`]。
    async fn post_accepted(
        &self,
        path: &str,
        body: &impl serde::Serialize,
        action: &str,
        client_order_id: &str,
    ) -> anyhow::Result<OrderAck> {
        let ack: RestAck = self.post(path, body).await?;
        if !ack.accepted {
            anyhow::bail!(
                "exchange {} 拒绝{action} {}: {}",
                self.name,
                client_order_id,
                ack.message.clone().unwrap_or_default()
            );
        }
        Ok(to_order_ack(ack))
    }
}

#[async_trait]
impl ExchangeApi for XiliExchange {
    fn exchange(&self) -> &str {
        &self.name
    }

    async fn place_order(&self, req: &PlaceOrderRequest) -> anyhow::Result<OrderAck> {
        // 统一请求 → xili 线协议；xili 不关心 account/leverage，给默认值。
        let body = RestPlaceOrder {
            client_order_id: req.client_order_id.clone(),
            account: self.account.clone(),
            instrument: req.instrument.clone(),
            side: req.side.to_string(),
            order_type: req.order_type.wire().to_string(),
            time_in_force: req
                .time_in_force
                .unwrap_or(TimeInForce::Gtc)
                .wire()
                .to_string(),
            price: req.price.unwrap_or(Decimal::ZERO),
            quantity: req.quantity,
            reduce_only: req.reduce_only,
            leverage: 1,
        };
        self.post_accepted(REST_PLACE_PATH, &body, "订单", &req.client_order_id)
            .await
    }

    async fn cancel_order(&self, instrument: &str, order: &OrderRef) -> anyhow::Result<OrderAck> {
        let body = RestCancelOrder {
            client_order_id: order.client_order_id.clone(),
            instrument: instrument.to_string(),
        };
        self.post_accepted(REST_CANCEL_PATH, &body, "撤单", &order.client_order_id)
            .await
    }

    async fn query_order(&self, _instrument: &str, _order: &OrderRef) -> OrderLookup {
        // xili 不支持查单：能力缺失而非"订单不存在"，按 Unavailable（不误判查无）。
        OrderLookup::Unavailable
    }

    async fn get_positions(&self) -> anyhow::Result<Vec<PositionInfo>> {
        let url = format!(
            "{}{}?account={}",
            self.base_url,
            REST_POSITIONS_PATH,
            urlencode(&self.account)
        );
        let resp = self.http.get(&url).send().await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("exchange {} positions HTTP {status}: {text}", self.name);
        }
        let snaps: Vec<PositionSnapshot> = serde_json::from_str(&text)
            .map_err(|e| anyhow::anyhow!("解析持仓应答失败: {e}; body={text}"))?;
        Ok(snaps
            .into_iter()
            .map(|s| {
                let is_short = s.side.eq_ignore_ascii_case("short");
                let signed_quantity = if is_short { -s.quantity } else { s.quantity };
                PositionInfo {
                    instrument: s.instrument,
                    // xili 不区分产品，按当前永续口径。
                    instrument_type: InstrumentType::Futures,
                    // xili 无 hedge 概念，方向一律 BOTH，多空由数量符号表达。
                    position_side: crate::api::OrderPositionSide::Both,
                    signed_quantity,
                    entry_price: s.pos_price,
                    mark_price: None,
                    unrealized_pnl: None,
                    leverage: None,
                    notional: None,
                    margin: None,
                    update_ms: None,
                }
            })
            .collect())
    }

    async fn set_leverage(&self, _instrument: &str, _leverage: u32) -> anyhow::Result<()> {
        // xili 无杠杆概念，静默成功。
        Ok(())
    }

    async fn get_reference_price(&self, _instrument: &str) -> anyhow::Result<ReferencePrice> {
        anyhow::bail!("xili 交易所不支持参考价")
    }
}

/// xili 应答 → 统一受理应答；状态串宽松解析，初始/未知归一为 NEW。
fn to_order_ack(ack: RestAck) -> OrderAck {
    let status = status_from_wire(&ack.status);
    OrderAck {
        accepted: ack.accepted,
        client_order_id: ack.client_order_id,
        exchange_order_id: ack.exchange_order_id,
        status: if status == OrderStatus::Unknown {
            OrderStatus::New
        } else {
            status
        },
        message: ack.message,
        placed_type: None,
        placed_price: None,
        placed_quantity: None,
    }
}

/// 极简查询串转义（账户名特殊字符防护）。
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_endpoint_uses_local_9200_and_ws_path() {
        let ep = VenueEndpoint::xili_default();
        assert_eq!(ep.name, "xili");
        assert_eq!(ep.rest_base_url, "http://127.0.0.1:9200");
        assert_eq!(ep.ws_url, "ws://127.0.0.1:9200/ws");
    }

    #[test]
    fn endpoint_new_maps_scheme_and_appends_ws() {
        let http = VenueEndpoint::new("xili", "http://127.0.0.1:9200/").unwrap();
        assert_eq!(http.rest_base_url, "http://127.0.0.1:9200");
        assert_eq!(http.ws_url, "ws://127.0.0.1:9200/ws");

        let tls = VenueEndpoint::new("xili", "https://example.test").unwrap();
        assert_eq!(tls.ws_url, "wss://example.test/ws");

        assert!(VenueEndpoint::new("xili", "ftp://x").is_err());
    }

    #[test]
    fn with_default_adds_xili_only_when_absent() {
        // 空配置：补一个默认 xili。
        let added = with_default_endpoint(vec![]);
        assert_eq!(added.len(), 1);
        assert_eq!(added[0].name, "xili");

        // 已显式配置 xili（含自定义地址）：不重复追加、保留配置。
        let custom = vec![VenueEndpoint {
            name: "xili".to_string(),
            rest_base_url: "http://127.0.0.1:9999".to_string(),
            ws_url: "ws://127.0.0.1:9999/ws".to_string(),
        }];
        let kept = with_default_endpoint(custom);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].rest_base_url, "http://127.0.0.1:9999");

        // 配置了另一个所名：默认 xili 仍补上。
        let other = vec![VenueEndpoint {
            name: "play".to_string(),
            rest_base_url: "http://127.0.0.1:9300".to_string(),
            ws_url: "ws://127.0.0.1:9300/ws".to_string(),
        }];
        let both = with_default_endpoint(other);
        assert_eq!(both.len(), 2);
        assert!(both.iter().any(|e| e.name == "xili"));
        assert!(both.iter().any(|e| e.name == "play"));
    }
}
