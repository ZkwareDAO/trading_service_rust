//! Hyperliquid 用户主体（一套 EVM 地址/私钥）：实现 [`ExchangeApi`] + 账户事件流。
//!
//! 职责接线（计算均在纯模块，签名在 signature）：
//! - 下单：读规则行取 asset 索引；市价单以 mid±slippage 的 IOC 限价、数量按步长对齐，
//!   限价单按 GTC；签名后 POST /exchange；
//! - 撤单：首选 cancelByCloid（asset+cloid）；
//! - 查单：queryOrderByOid（先 cloid 后 oid）；
//! - 持仓：clearinghouseState；杠杆：updateLeverage；参考价：allMids→mid；
//! - 账户私有 WS：orderUpdates + clearinghouseState（见 user_stream）。
//!
//! 见 `docs/扩展交易所.md` 第三节、`docs/扩展交易所.md` 第六节。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use reqwest::Client;
use rust_decimal::Decimal;
use serde_json::Value;
use trading_core::order_rule::OrderRuleTable;
use trading_models::{Instrument, InstrumentType, MarketKind, OrderStatus, OrderType, Side, Venue};

use crate::api::{
    AccountEventStream, ExchangeApi, OrderAck, OrderPositionSide, OrderRef, OrderState,
    PlaceErrorKind, PlaceOrderRequest, PriceBasis, ReferencePrice,
};
use crate::hyperliquid::endpoint;
use crate::hyperliquid::pricing;
use crate::hyperliquid::signature::{self, ActionsWire};

/// 交易所规范名 / grouping 常量。
pub(crate) const EXCHANGE: &str = "hyperliquid";
const GROUPING: &str = "na";

// =====================================================================
// 纯解析：下单状态 / clearinghouse 仓位
// =====================================================================

/// 下单三态（resting / filled / error）。
#[derive(Debug, Clone)]
pub(crate) enum PlaceOutcome {
    Resting {
        oid: u64,
    },
    Filled {
        oid: u64,
        avg_px: Decimal,
        total_sz: Decimal,
    },
}

/// 解析下单状态对象（`data.statuses` 内的单项）；error → 错误。
pub(crate) fn parse_place_status(v: &Value) -> anyhow::Result<PlaceOutcome> {
    if let Some(e) = v.get("error").and_then(|x| x.as_str()) {
        anyhow::bail!("hyperliquid 拒绝下单：{e}");
    }
    if let Some(r) = v.get("resting") {
        let oid = r.get("oid").and_then(|x| x.as_u64()).unwrap_or(0);
        return Ok(PlaceOutcome::Resting { oid });
    }
    if let Some(f) = v.get("filled") {
        let oid = f.get("oid").and_then(|x| x.as_u64()).unwrap_or(0);
        let avg_px = json_dec(f.get("avgPx")).unwrap_or(Decimal::ZERO);
        let total_sz = json_dec(f.get("totalSz")).unwrap_or(Decimal::ZERO);
        return Ok(PlaceOutcome::Filled {
            oid,
            avg_px,
            total_sz,
        });
    }
    anyhow::bail!("无法识别的下单状态：{v}")
}

/// 解析 clearinghouse `assetPositions` 单项（`{type,position}`）；0 量/畸形 → None。
///
/// `mark`：由调用方（`get_positions`）按 coin 从另调的 `allMids` 取到的当前价；
/// `clearinghouseState` 本身不回当前价，无补价时传 [`None`]，不得臆造。
pub(crate) fn map_clearing_position(
    item: &Value,
    mark: Option<Decimal>,
) -> Option<crate::api::PositionInfo> {
    let pos = item.get("position")?;
    let coin = pos.get("coin").and_then(|x| x.as_str())?;
    let signed = json_dec(pos.get("szi"))?;
    if signed == Decimal::ZERO {
        return None;
    }
    Some(crate::api::PositionInfo {
        instrument: coin.to_string(),
        instrument_type: InstrumentType::Futures,
        position_side: OrderPositionSide::Both,
        signed_quantity: signed,
        entry_price: json_dec(pos.get("entryPx")).unwrap_or(Decimal::ZERO),
        mark_price: mark,
        unrealized_pnl: json_dec(pos.get("unrealizedPnl")),
        leverage: pos
            .get("leverage")
            .and_then(|l| l.get("value"))
            .and_then(|x| x.as_u64()),
        multiplier: None,
        notional: json_dec(pos.get("positionValue")),
        margin: json_dec(pos.get("marginUsed")),
        update_ms: None,
    })
}

/// JSON 值（字符串或数字）→ Decimal；null/非法 → None。
fn json_dec(v: Option<&Value>) -> Option<Decimal> {
    v.and_then(|x| match x {
        Value::String(s) => s.parse().ok(),
        Value::Number(_) => Decimal::from_str_exact(&x.to_string()).ok(),
        _ => None,
    })
}

/// 解析 `allMids` 响应（顶层对象 `{"BTC":"80000", ...}`）→ `coin -> mid` map。
/// 非法/缺失字段直接跳过，绝不因单项异常而失败。
pub(crate) fn parse_all_mids(v: &Value) -> HashMap<String, Decimal> {
    let mut mids = HashMap::new();
    if let Some(obj) = v.as_object() {
        for (coin, val) in obj {
            if let Some(price) = json_dec(Some(val)) {
                mids.insert(coin.clone(), price);
            }
        }
    }
    mids
}

// =====================================================================
// 用户主体
// =====================================================================

/// Hyperliquid 用户主体：一套 EVM 地址 / 私钥。
pub(crate) struct HyperliquidUser {
    http: Client,
    rules: Arc<OrderRuleTable>,
    quotes: Arc<trading_core::quote::QuoteTable>,
    signer: signature::Signer,
    /// 用户钱包地址（= account.csv 的 api_key）。Hyperliquid 要求以此为
    /// `user` 字段，且原样保留大小写；不由私钥重新推导。
    address: String,
    testnet: bool,
    slippage: Decimal,
}

impl HyperliquidUser {
    /// 构造：注入共享 HTTP/规则/行情、钱包地址（api_key）、十六进制私钥、
    /// 选网与市价滑点。私钥仅用于签名，user 地址一律用 `account_addr`。
    pub(crate) fn new(
        http: Client,
        rules: Arc<OrderRuleTable>,
        quotes: Arc<trading_core::quote::QuoteTable>,
        account_addr: &str,
        secret: &str,
        testnet: bool,
        slippage: Decimal,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            http,
            rules,
            quotes,
            signer: signature::Signer::from_secret_hex(secret)?,
            address: account_addr.trim().to_string(),
            testnet,
            slippage,
        })
    }

    /// 用户钱包地址（原样大小写），作为下单/查单/查仓的 `user`。
    pub(crate) fn user_address(&self) -> &str {
        &self.address
    }

    /// 读规则行：key 用原生裸币名 + 本主体所在网络（主/测试网 asset index 不同）。
    fn rule_row(&self, coin: &str) -> Option<Arc<trading_models::OrderRule>> {
        let key = trading_models::OrderRuleKey::with_network(
            EXCHANGE,
            InstrumentType::Futures,
            coin,
            self.testnet,
        );
        self.rules.get(&key)
    }

    /// 从规则行 extra 取 asset 整数索引。
    fn asset_index(row: &trading_models::OrderRule) -> Option<u32> {
        row.extra.get("asset_index")?.parse().ok()
    }

    /// 从内存行情读某币中间价（市价 IOC 基准）。
    fn mid_of(&self, coin: &str) -> Option<Decimal> {
        let q = self.quotes.get(&Instrument::new(
            Venue::Hyperliquid,
            MarketKind::Futures,
            coin,
        ))?;
        q.mid
    }

    /// POST /info（公开/带 user 的查询），返回 JSON。
    async fn post_info(&self, body: Value) -> anyhow::Result<Value> {
        let (info_url, _, _) = endpoint::user_endpoints(self.testnet);
        let resp = self
            .http
            .post(info_url)
            .json(&body)
            .send()
            .await?
            .error_for_status()?;
        Ok(resp.json().await?)
    }

    /// 提交已签名 action 到 /exchange，返回 statuses 数组。
    async fn submit_action(&self, action: ActionsWire) -> anyhow::Result<Vec<Value>> {
        let nonce = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let conn = signature::connection_id(&action, nonce, None)?;
        let parts = self.signer.sign_l1_parts(conn, !self.testnet)?;
        // 请求体：action（短字段名）+ nonce + signature 分片；本期不使用 vault。
        let payload = serde_json::json!({
            "action": serde_json::to_value(&action)?,
            "nonce": nonce,
            "signature": {"r": parts.r, "s": parts.s, "v": parts.v},
        });
        let (_, exchange_url, _) = endpoint::user_endpoints(self.testnet);
        let resp = self.http.post(exchange_url).json(&payload).send().await?;
        // 顶层信封：{status:"ok"/"err", response:{data:{statuses:[...]}}。
        let env: Value = resp.json().await?;
        if env.get("status").and_then(|x| x.as_str()) == Some("err") {
            let msg = env.get("response").cloned().unwrap_or(env);
            anyhow::bail!("hyperliquid 拒绝请求：{msg}");
        }
        let statuses = env
            .pointer("/response/data/statuses")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(statuses)
    }

    /// 拉取全市场中间价 `allMids`，解析成 `coin -> mid` map（给 clearinghouse 仓位补当前价）。
    /// 仅告警并返回空 map，绝不因补价失败而阻断对账主流程。
    async fn fetch_all_mids(&self) -> HashMap<String, Decimal> {
        match self.post_info(serde_json::json!({"type":"allMids"})).await {
            Ok(v) => parse_all_mids(&v),
            Err(e) => {
                tracing::warn!(error = %e, "hyperliquid allMids 拉取失败：仓位快照缺补价");
                HashMap::new()
            }
        }
    }
}

#[async_trait]
impl ExchangeApi for HyperliquidUser {
    fn exchange(&self) -> &str {
        EXCHANGE
    }

    async fn place_order(&self, req: &PlaceOrderRequest) -> anyhow::Result<OrderAck> {
        let coin = req.instrument.trim();
        let row = self
            .rule_row(coin)
            .ok_or_else(|| anyhow::anyhow!("hyperliquid 规则未就绪（asset 索引未知）：{coin}"))?;
        let asset = Self::asset_index(&row)
            .ok_or_else(|| anyhow::anyhow!("hyperliquid asset 索引缺失：{coin}"))?;

        // 数量按步长向下对齐（不足一步直接拒，避免发出 0 量单）。
        let quantity = pricing::align_size(req.quantity, row.qty_step);
        if quantity <= Decimal::ZERO {
            anyhow::bail!("下单数量不足最小步长 {}：{coin}", row.qty_step);
        }

        // 市价单：mid 放滑点 → IOC；限价单：委托价 → GTC。
        let (limit_px, tif) = match req.order_type {
            OrderType::Market => {
                let mid = self
                    .mid_of(coin)
                    .ok_or_else(|| anyhow::anyhow!("hyperliquid mid 行情未就绪：{coin}"))?;
                let px = pricing::market_limit_price(mid, req.side == Side::Buy, self.slippage);
                (px, "Ioc")
            }
            OrderType::Limit => {
                let px = req
                    .price
                    .ok_or_else(|| anyhow::anyhow!("限价单必须提供价格：{coin}"))?;
                (px, "Gtc")
            }
            other => anyhow::bail!("hyperliquid 不支持的订单类型：{other:?}"),
        };

        let action = ActionsWire::Order(signature::BulkOrderWire {
            orders: vec![signature::OrderWire {
                asset,
                is_buy: req.side == Side::Buy,
                // 价格 / 数量字符串去尾随零：保证 msgpack 哈希与服务端重构一致。
                limit_px: pricing::wire_normalize(limit_px),
                sz: pricing::wire_normalize(quantity),
                reduce_only: req.reduce_only,
                order_type: signature::OrderTypeWire::Limit {
                    tif: tif.to_string(),
                },
                cloid: Some(req.client_order_id.clone()),
            }],
            grouping: GROUPING.to_string(),
            builder: None,
        });

        let statuses = self.submit_action(action).await?;
        let first = statuses
            .first()
            .ok_or_else(|| anyhow::anyhow!("hyperliquid 未返回下单状态：{coin}"))?;
        match parse_place_status(first).map_err(|e| {
            // 仅留痕：交易所拒单原因原样记录，错误继续向下传播，不改变任何业务处理。
            tracing::warn!(error = %e, coin, "hyperliquid 下单被交易所拒绝");
            e
        })? {
            PlaceOutcome::Resting { oid } => Ok(OrderAck {
                accepted: true,
                client_order_id: req.client_order_id.clone(),
                exchange_order_id: Some(oid.to_string()),
                status: OrderStatus::New,
                message: None,
                placed_type: (req.order_type == OrderType::Market).then_some(OrderType::Limit),
                placed_price: Some(limit_px),
                placed_quantity: Some(quantity),
            }),
            PlaceOutcome::Filled {
                oid,
                avg_px,
                total_sz,
            } => Ok(OrderAck {
                accepted: true,
                client_order_id: req.client_order_id.clone(),
                exchange_order_id: Some(oid.to_string()),
                status: OrderStatus::Filled,
                message: None,
                placed_type: (req.order_type == OrderType::Market).then_some(OrderType::Limit),
                placed_price: Some(avg_px),
                placed_quantity: Some(total_sz),
            }),
        }
    }

    fn classify_place_error(&self, err: &anyhow::Error) -> PlaceErrorKind {
        // 由本适配器在请求前/状态解析产出的确定性拒绝 → Rejected；
        // 网络/超时等不确定错误保持 Indeterminate。
        let rejected = err.to_string().contains("hyperliquid 拒绝")
            || err.to_string().contains("规则未就绪")
            || err.to_string().contains("hyperliquid 拒绝下单");
        if rejected {
            PlaceErrorKind::Rejected
        } else {
            PlaceErrorKind::Indeterminate
        }
    }

    async fn cancel_order(&self, instrument: &str, order: &OrderRef) -> anyhow::Result<OrderAck> {
        let coin = instrument.trim();
        let row = self
            .rule_row(coin)
            .ok_or_else(|| anyhow::anyhow!("hyperliquid 规则未就绪，无法撤单：{coin}"))?;
        let asset = Self::asset_index(&row)
            .ok_or_else(|| anyhow::anyhow!("hyperliquid asset 索引缺失：{coin}"))?;
        // 首选 cancelByCloid（asset + cloid）。
        let action = ActionsWire::CancelByCloid(signature::BulkCancelCloidWire {
            cancels: vec![signature::CancelCloidItem {
                asset,
                cloid: order.client_order_id.clone(),
            }],
        });
        self.submit_action(action).await?;
        Ok(OrderAck {
            accepted: true,
            client_order_id: order.client_order_id.clone(),
            exchange_order_id: None,
            status: OrderStatus::Canceled,
            message: None,
            placed_type: None,
            placed_price: None,
            placed_quantity: None,
        })
    }

    async fn query_order(&self, instrument: &str, order: &OrderRef) -> crate::api::OrderLookup {
        use crate::api::OrderLookup;
        let coin = instrument.trim();
        // 官方口径用 orderStatus（旧名 queryOrderByOid 会被服务端 422）；
        // oid 字段既接受整数交易所 oid，也接受 cloid 字符串。首选 cloid。
        let body = serde_json::json!({
            "type": "orderStatus",
            "user": self.user_address(),
            "oid": order.client_order_id,
        });
        match self.post_info(body).await {
            Ok(v) if !v.is_null() => OrderLookup::Found(map_query_order(&v, coin)),
            Ok(_) => OrderLookup::NotExists,
            Err(e) => {
                tracing::warn!(error = %e, "hyperliquid 查单失败");
                OrderLookup::Unavailable
            }
        }
    }

    async fn get_positions(&self) -> anyhow::Result<Vec<crate::api::PositionInfo>> {
        let body = serde_json::json!({
            "type": "clearinghouseState",
            "user": self.user_address(),
        });
        // clearinghouseState 本身不回当前价：另调一次 allMids，两者并行。
        // allMids 失败不得阻断主对账（仅缺补价），失败退化为空 map，由行情 tick/merge 防御兜底。
        let clearing = self.post_info(body);
        let mids_fut = self.fetch_all_mids();
        let (env, mids) = tokio::join!(clearing, mids_fut);
        let env = env?;

        let mut out = Vec::new();
        if let Some(items) = env.get("assetPositions").and_then(|x| x.as_array()) {
            for item in items {
                // 按 coin 从 allMids 取当前价补入（缺该币则 None）。
                let coin = item
                    .get("position")
                    .and_then(|p| p.get("coin"))
                    .and_then(|x| x.as_str());
                let mark = coin.and_then(|c| mids.get(c)).copied();
                if let Some(info) = map_clearing_position(item, mark) {
                    out.push(info);
                }
            }
        }
        Ok(out)
    }

    async fn set_leverage(&self, instrument: &str, leverage: u32) -> anyhow::Result<()> {
        let coin = instrument.trim();
        let row = self
            .rule_row(coin)
            .ok_or_else(|| anyhow::anyhow!("hyperliquid 规则未就绪，无法改杠杆：{coin}"))?;
        let asset = Self::asset_index(&row)
            .ok_or_else(|| anyhow::anyhow!("hyperliquid asset 索引缺失：{coin}"))?;
        let action = ActionsWire::UpdateLeverage(signature::UpdateLeverageWire {
            asset,
            is_cross: true,
            leverage,
        });
        self.submit_action(action).await?;
        Ok(())
    }

    async fn get_reference_price(&self, instrument: &str) -> anyhow::Result<ReferencePrice> {
        let coin = instrument.trim();
        // allMids 一次返回全市场 map，取本币 mid。
        let env = self
            .post_info(serde_json::json!({"type":"allMids"}))
            .await?;
        let price = json_dec(env.get(coin))
            .ok_or_else(|| anyhow::anyhow!("hyperliquid allMids 无该币价格：{coin}"))?;
        Ok(ReferencePrice {
            instrument: coin.to_string(),
            basis: PriceBasis::Mid,
            price,
        })
    }
}

#[async_trait]
impl AccountEventStream for HyperliquidUser {
    async fn subscribe_account_events(
        &self,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, crate::api::AccountEvent>> {
        let address = self.user_address().to_string();
        let (_, _, ws_url) = endpoint::user_endpoints(self.testnet);
        crate::hyperliquid::user_stream::connect(ws_url, address)
    }
}

/// Hyperliquid 订单状态字符串 → 统一 [`OrderStatus`]（WS orderUpdates 与
/// REST orderStatus 共用，未识别按 Unknown）。
pub(crate) fn map_order_status(raw: &str) -> OrderStatus {
    match raw {
        "filled" => OrderStatus::Filled,
        "open" => OrderStatus::New,
        "canceled" | "cancelled" => OrderStatus::Canceled,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

/// 解析 orderStatus 响应：顶层 `status:"order"`，内层 `order.status` 才是
/// 订单状态，订单字段在内层 `order.order`。
fn map_query_order(v: &Value, coin: &str) -> OrderState {
    // 内层包装与订单字段；结构异常时退化为空订单（返回 Unknown，不误判状态）。
    let wrapper = v.get("order");
    let order_obj = wrapper.and_then(|w| w.get("order"));
    let status = map_order_status(
        wrapper
            .and_then(|w| w.get("status"))
            .and_then(|x| x.as_str())
            .unwrap_or(""),
    );
    let cloid = order_obj
        .and_then(|x| x.get("cloid"))
        .and_then(|x| x.as_str())
        .map(str::to_string);
    let oid = order_obj.and_then(|x| x.get("oid")).map(|x| x.to_string());
    let instrument = order_obj
        .and_then(|x| x.get("coin"))
        .and_then(|x| x.as_str())
        .unwrap_or(coin)
        .to_string();
    // 成交量 = origSz − sz（剩余量）；缺字段按 0。
    let filled_quantity = match order_obj {
        Some(o) => {
            let orig = json_dec(o.get("origSz")).unwrap_or(Decimal::ZERO);
            let left = json_dec(o.get("sz")).unwrap_or(Decimal::ZERO);
            (orig - left).max(Decimal::ZERO)
        }
        None => Decimal::ZERO,
    };
    // orderStatus 不回成交均价（avgPx）；仅有委托价 limitPx，留空均价。
    OrderState {
        client_order_id: cloid,
        exchange_order_id: oid,
        instrument,
        status,
        filled_quantity,
        average_price: None,
        position_side: OrderPositionSide::Both,
        venue_extra: None,
    }
}

#[cfg(test)]
mod parse_tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn parses_resting_status() {
        let v = serde_json::json!({"resting":{"oid":123}});
        let out = parse_place_status(&v).expect("parse");
        assert!(matches!(out, PlaceOutcome::Resting { oid: 123 }));
    }

    #[test]
    fn parses_filled_status() {
        let v = serde_json::json!({
          "filled":{"totalSz":"0.1","avgPx":"2000","oid":456,"closedPnl":"0"}});
        let out = parse_place_status(&v).expect("parse");
        match out {
            PlaceOutcome::Filled {
                oid,
                avg_px,
                total_sz,
            } => {
                assert_eq!(oid, 456);
                assert_eq!(avg_px, Decimal::from(2000));
                assert_eq!(total_sz, Decimal::from_str_exact("0.1").unwrap());
            }
            other => panic!("期望 filled，得到 {other:?}"),
        }
    }

    #[test]
    fn error_status_is_error() {
        let v = serde_json::json!({"error":"Invalid cloid"});
        assert!(parse_place_status(&v).is_err());
    }

    #[test]
    fn maps_clearinghouse_position() {
        let v = serde_json::json!({
        "type":"oneWay",
        "position":{
          "coin":"BTC","szi":"0.001","entryPx":"80000","positionValue":"80",
          "unrealizedPnl":"5","marginUsed":"8",
          "leverage":{"type":"cross","value":10}
        }});
        // 传入 allMids 补到的 mark 价：应填入 mark_price。
        let mark = Decimal::from_str_exact("80100").unwrap();
        let p = map_clearing_position(&v, Some(mark)).expect("mapped");
        assert_eq!(p.instrument, "BTC");
        assert_eq!(p.instrument_type, InstrumentType::Futures);
        assert_eq!(p.position_side, OrderPositionSide::Both);
        assert_eq!(p.signed_quantity, Decimal::from_str_exact("0.001").unwrap());
        assert_eq!(p.entry_price, Decimal::from(80000));
        assert_eq!(p.mark_price, Some(mark));
        assert_eq!(p.unrealized_pnl, Some(Decimal::from(5)));
        assert_eq!(p.leverage, Some(10));
        assert_eq!(p.notional, Some(Decimal::from(80)));
        assert_eq!(p.margin, Some(Decimal::from(8)));
    }

    #[test]
    fn clearing_position_without_mark_has_none_price() {
        // 无 mark 可补（allMids 缺该币）：mark_price 必须为 None，不得臆造。
        let v = serde_json::json!({
        "position":{
          "coin":"FIL","szi":"56.2","entryPx":"1.0676",
          "leverage":{"type":"cross","value":2}
        }});
        let p = map_clearing_position(&v, None).expect("mapped");
        assert_eq!(p.instrument, "FIL");
        assert_eq!(p.mark_price, None);
    }

    #[test]
    fn zero_or_malformed_position_is_none() {
        let zero = serde_json::json!({"position":{"coin":"BTC","szi":"0"}});
        assert!(map_clearing_position(&zero, None).is_none());
    }

    #[test]
    fn map_query_order_parses_filled_status_response() {
        // orderStatus 真实响应：状态在 order.status，字段在 order.order。
        let raw = serde_json::json!({
          "status": "order",
          "order": {
            "order": {
              "coin":"BTC","side":"A","limitPx":"79024.0","sz":"0.0",
              "origSz":"0.0006","oid":61391108446u64,"tif":"Ioc",
              "cloid":"0xe5ef414515074ad0a89093bd82919e3e"
            },
            "status":"filled",
            "statusTimestamp":1790696255998i64
          }
        });
        let s = map_query_order(&raw, "BTC");
        assert_eq!(s.status, OrderStatus::Filled);
        assert_eq!(
            s.filled_quantity,
            Decimal::from_str_exact("0.0006").unwrap()
        );
        assert_eq!(
            s.client_order_id.as_deref(),
            Some("0xe5ef414515074ad0a89093bd82919e3e")
        );
        assert_eq!(s.exchange_order_id.as_deref(), Some("61391108446"));
    }
}

#[cfg(test)]
mod rule_row_tests {
    use super::*;
    use std::sync::Arc;
    use trading_models::OrderRule;

    /// 测试用已知私钥（官方 SDK 公共测试向量，非真实资产私钥）。
    const TEST_SECRET: &str = "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e";

    /// 构造一行 BTC 规则并写入指定网络与 asset 索引。
    fn btc_row(asset_index: u32, is_testnet: bool) -> OrderRule {
        let mut row = OrderRule::new(
            EXCHANGE,
            "BTC",
            InstrumentType::Futures,
            trading_models::QtyUnit::Base,
            Decimal::ONE,
            "USDC",
            "USDC",
            Decimal::from_str_exact("0.00001").unwrap(),
            "hyperliquid:meta",
            0,
        );
        row.extra
            .insert("asset_index".to_string(), asset_index.to_string());
        row.with_testnet(is_testnet)
    }

    fn user_with(table: OrderRuleTable, testnet: bool) -> HyperliquidUser {
        HyperliquidUser::new(
            reqwest::Client::new(),
            Arc::new(table),
            Arc::new(trading_core::quote::QuoteTable::new()),
            "0x0000000000000000000000000000000000000a93",
            TEST_SECRET,
            testnet,
            Decimal::from_str_exact("0.05").unwrap(),
        )
        .expect("user")
    }

    #[test]
    fn mainnet_user_gets_mainnet_asset_index() {
        // 两网同名 BTC 共存：主网主体必须取到主网 index=0。
        let table = OrderRuleTable::new();
        table.put(btc_row(0, false));
        table.put(btc_row(3, true));

        let mainnet = user_with(table, false);
        let row = mainnet.rule_row("BTC").expect("主网 BTC 行");
        assert_eq!(HyperliquidUser::asset_index(&row), Some(0));
    }

    #[test]
    fn testnet_user_gets_testnet_asset_index() {
        let table = OrderRuleTable::new();
        table.put(btc_row(0, false));
        table.put(btc_row(3, true));

        let testnet = user_with(table, true);
        let row = testnet.rule_row("BTC").expect("测试网 BTC 行");
        assert_eq!(HyperliquidUser::asset_index(&row), Some(3));
    }

    #[test]
    fn user_address_is_the_provided_api_key_not_derived() {
        // 与 Go 一致：user 地址必须取配置传入的 api_key，并原样保留大小写。
        // 地址为格式合法的示例（非真实资产钱包），私钥 TEST_SECRET 为官方 SDK 公共测试向量。
        let api_key = "0x0a93F6118Da879b057542c24495889bda88D158E";
        let user = HyperliquidUser::new(
            reqwest::Client::new(),
            Arc::new(OrderRuleTable::new()),
            Arc::new(trading_core::quote::QuoteTable::new()),
            api_key,
            TEST_SECRET,
            true,
            Decimal::from_str_exact("0.05").unwrap(),
        )
        .expect("user");
        assert_eq!(user.user_address(), api_key);
        // 不得使用私钥推导地址（那是另一个钱包）。
        assert_ne!(user.user_address(), user.signer.address_hex());
    }
}
