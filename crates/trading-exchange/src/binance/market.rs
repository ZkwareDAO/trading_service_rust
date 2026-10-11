//! Binance 公共行情数据流（[`MarketDataStream`] 的 Binance 实现）。
//!
//! 两个无认证「交易所主体」对象，所有账户共享，各开一条公共 WS：
//! * [`BinanceSpotMarket`]    —— 现货 `!miniTicker@arr`（全市场 1s），产出最新成交价；
//! * [`BinanceFuturesMarket`] —— U 本位合约 `!markPrice@arr@1s`（全市场 1s），产出标记价。
//!
//! 口径（见 `docs/行情订阅模型.md` 第 1、5 节）：现货没有标记价，估值/触发用 last；
//! 合约风控主口径用 mark，**不**订 miniTicker。当前均不订盘口（bookTicker），
//! 故 [`PriceTick::best_bid`]/[`PriceTick::best_ask`] 恒为 `None`，Binance 走原生市价单。
//!
//! [`subscribe_prices`](MarketDataStream::subscribe_prices) 返回的流内部自带
//! [`ReconnectBackoff`] 退避重连（10s→30s→1m→3m→5m，连上即归零）；接收方 drop 后
//! 连接任务自动收尾，不使用全局 channel 做业务数据传递（这里的 mpsc 仅为单条流的出口）。
//!
//! 流转存内存行情表由 [`forward_spot_quotes`] / [`forward_futures_quotes`] 两个 worker
//! 完成；行情只在内存、不持久化。

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use futures_util::stream::BoxStream;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::tungstenite::Message;
use trading_core::quote::{QuoteTable, QuoteUpdate};
use trading_models::{Instrument, MarketKind, Venue};

use crate::api::{MarketDataStream, PriceTick};
use crate::backoff::ReconnectBackoff;
use crate::heartbeat::{Heartbeat, HeartbeatAction};
use crate::port::QuoteObserver;

/// 出口通道容量：容得下全市场一帧（现货约 2000 标的），消费方实时写内存表，不积压。
const CHANNEL_CAP: usize = 8192;

const SPOT_URL: &str = "wss://stream.binance.com:9443/ws/!miniTicker@arr";
/// 合约公共行情恒定正式网（交易所级、与账户无关；账户测试网不影响公共行情）。
/// 2026 年 WS URL 迁移：旧 `/ws/...` 已下线（仍可连接但不再推数据），
/// 公共行情改走 `/market/ws/...`；该路径返回裸数组，解析口径与旧路径一致。
const FUTURES_URL: &str = "wss://fstream.binance.com/market/ws/!markPrice@arr@1s";

/// 解析出的一行：（symbol, 价格, 交易所事件时间毫秒）。
type PriceRow = (String, Decimal, i64);

/// Binance 现货公共行情（无认证，进程一份）。
#[derive(Debug, Clone, Copy, Default)]
pub struct BinanceSpotMarket;

/// Binance U 本位合约公共行情（无认证，进程一份，恒定正式网）。
#[derive(Debug, Clone, Copy, Default)]
pub struct BinanceFuturesMarket;

#[async_trait]
impl MarketDataStream for BinanceSpotMarket {
    async fn subscribe_prices(
        &self,
        instruments: &[String],
    ) -> anyhow::Result<BoxStream<'static, PriceTick>> {
        require_whole_market(instruments, "binance 现货")?;
        Ok(spawn_ws_stream(
            SPOT_URL,
            parse_spot_mini_ticker,
            "binance-spot-miniTicker",
        ))
    }
}

#[async_trait]
impl MarketDataStream for BinanceFuturesMarket {
    async fn subscribe_prices(
        &self,
        instruments: &[String],
    ) -> anyhow::Result<BoxStream<'static, PriceTick>> {
        require_whole_market(instruments, "binance 合约")?;
        Ok(spawn_ws_stream(
            FUTURES_URL,
            parse_futures_mark_price,
            "binance-futures-markPrice",
        ))
    }
}

/// 订阅规则 4.3：Binance 现货/合约均为启动即订的全市场常量集，不随持仓变化。
/// 单标的子集（`<sym>@bookTicker` 等）留待将来按风控子集再加。
fn require_whole_market(instruments: &[String], who: &str) -> anyhow::Result<()> {
    if instruments.is_empty() {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "{who} 行情当前仅支持全市场订阅，请传空列表（得到 {} 个标的）",
            instruments.len()
        ))
    }
}

/// 建一条 WS 行情流：立即返回出口流，连接/读帧/退避重连在后台任务中进行。
/// 接收方 drop（通道关闭）后后台任务收尾退出。
fn spawn_ws_stream(
    url: &'static str,
    parse: fn(&str) -> anyhow::Result<Vec<PriceRow>>,
    label: &'static str,
) -> BoxStream<'static, PriceTick> {
    let (tx, rx) = mpsc::channel(CHANNEL_CAP);
    tokio::spawn(async move {
        let mut backoff = ReconnectBackoff::new();
        loop {
            if tx.is_closed() {
                return;
            }
            if let Some(wait) = backoff.next_wait() {
                tokio::time::sleep(wait).await;
            }
            match run_connection(url, parse, &tx, &mut backoff).await {
                // 接收方离开：正常收尾，不再重连。
                Ok(()) => return,
                Err(e) => {
                    tracing::warn!(stream = label, error = %e, "market stream error, reconnecting");
                    backoff.note_retry();
                }
            }
        }
    });
    Box::pin(ReceiverStream::new(rx))
}

/// 单次连接；`Ok(())` 仅在出口通道关闭（接收方 drop）时返回，连接/读错返回 Err 触发重连。
async fn run_connection(
    url: &str,
    parse: fn(&str) -> anyhow::Result<Vec<PriceRow>>,
    tx: &mpsc::Sender<PriceTick>,
    backoff: &mut ReconnectBackoff,
) -> anyhow::Result<()> {
    let mut ws = crate::ws::connect(url).await?;
    backoff.reset();
    // 公共行情是高频流（markPrice 正常每秒有帧）：用更短的 10s/20s 心跳/空闲上限，
    // 「握手成功但被代理掐断数据流」这类死连最迟 20s 即判死重连（私有账户流仍用 20/60）。
    let mut hb = Heartbeat::new(Duration::from_secs(10), Duration::from_secs(20));
    tracing::info!(url, "market stream connected");

    loop {
        // 保活：到 deadline 前无任何帧则推进状态机；收到任意帧即证明存活并复位。
        let wait = hb.deadline();
        let frame = match tokio::time::timeout(wait, ws.next()).await {
            Ok(Some(frame)) => {
                hb.on_frame();
                frame?
            }
            Ok(None) => break, // 对端关闭
            Err(_) => match hb.advance(wait) {
                HeartbeatAction::Ping => {
                    ws.send(Message::Ping(Default::default())).await?;
                    continue;
                }
                HeartbeatAction::Timeout => {
                    anyhow::bail!("market stream idle timeout, reconnecting")
                }
                HeartbeatAction::Wait => continue,
            },
        };

        let text = match frame {
            Message::Text(t) => t.to_string(),
            Message::Binary(b) => String::from_utf8_lossy(&b).to_string(),
            Message::Ping(p) => {
                ws.send(Message::Pong(p)).await?;
                continue;
            }
            Message::Close(_) => break,
            _ => continue,
        };
        match parse(&text) {
            Ok(rows) => {
                // 批次级 debug（全市场每帧数千标的，逐条记录会爆量）：帧数与事件时间。
                tracing::debug!(ticks = rows.len(), "market frame parsed");
                for (symbol, price, event_ms) in rows {
                    // 通道满则等待（背压）；接收方关闭则收尾。
                    if tx
                        .send(PriceTick {
                            instrument: symbol,
                            price,
                            best_bid: None,
                            best_ask: None,
                            event_ms,
                        })
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, payload = %text, "bad market frame, skipped"),
        }
    }
    anyhow::bail!("market ws stream ended")
}

// =====================================================================
// 流转存内存行情表（venue 口径映射是 Binance 适配知识，留在本模块）
// =====================================================================

/// 价格写入 [`Quote`] 的哪个字段：现货只有 last，合约风控主口径是 mark。
#[derive(Clone, Copy)]
enum PriceField {
    Last,
    Mark,
}

/// 订阅现货全市场并把 last 价写入共享行情表；随进程生命周期运行。
/// 现货无杠杆仓位语义，不挂仓位重估观察口。
pub async fn forward_spot_quotes(quotes: Arc<QuoteTable>) {
    forward_quotes(
        quotes,
        BinanceSpotMarket,
        Venue::BinanceSpot,
        MarketKind::Spot,
        PriceField::Last,
        None,
    )
    .await;
}

/// 订阅合约全市场并把 mark 价写入共享行情表；随进程生命周期运行。恒定正式网。
/// `observer`：每写一笔 mark 价后同步回调，驱动仅本交易所仓位的重估。
pub async fn forward_futures_quotes(
    quotes: Arc<QuoteTable>,
    observer: Option<Arc<dyn QuoteObserver>>,
) {
    forward_quotes(
        quotes,
        BinanceFuturesMarket,
        Venue::BinanceFutures,
        MarketKind::Futures,
        PriceField::Mark,
        observer,
    )
    .await;
}

/// 订阅一条全市场行情流并逐 tick 写入共享行情表；启动失败仅告警退出（流内部自带重连）。
async fn forward_quotes<M: MarketDataStream>(
    quotes: Arc<QuoteTable>,
    market: M,
    venue: Venue,
    kind: MarketKind,
    field: PriceField,
    observer: Option<Arc<dyn QuoteObserver>>,
) {
    let label = match field {
        PriceField::Last => "binance-spot",
        PriceField::Mark => "binance-futures",
    };
    let mut ticks = match market.subscribe_prices(&[]).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(stream = label, error = %e, "market subscribe start failed");
            return;
        }
    };
    while let Some(tick) = ticks.next().await {
        let (last, mark) = match field {
            PriceField::Last => (Some(tick.price), None),
            PriceField::Mark => (None, Some(tick.price)),
        };
        let price = tick.price;
        let inst = Instrument::new(venue, kind, tick.instrument);
        quotes.apply(
            &inst,
            QuoteUpdate {
                last,
                mark,
                ts_exchange_ms: tick.event_ms,
                ts_recv_ms: Utc::now().timestamp_millis(),
                ..QuoteUpdate::default()
            },
        );
        // 写价后同步驱动仓位重估（纯内存；observer 内部按 venue/kind 自行取舍）。
        if let Some(ob) = &observer {
            ob.on_quote(venue, kind, &inst.symbol, price);
        }
    }
    // 后台连接任务自带重连、正常情况下出口流不会结束；到这里说明发送任务已终止（如 panic）。
    tracing::error!(
        stream = label,
        "market quote stream ended unexpectedly; quotes stopped"
    );
}

// =====================================================================
// 纯函数解析（零网络，单测覆盖）
// =====================================================================

fn parse_decimal(v: &serde_json::Value) -> Option<Decimal> {
    match v {
        serde_json::Value::String(s) => Decimal::from_str(s.trim()).ok(),
        serde_json::Value::Number(n) => Decimal::from_str(&n.to_string()).ok(),
        _ => None,
    }
}

/// 现货 `!miniTicker@arr`：逐元素取 `s`(symbol)、`c`(最新成交价 close)、`E`(事件时间)。
/// 个别元素缺字段则跳过，不影响同帧其它 symbol。
pub fn parse_spot_mini_ticker(payload: &str) -> anyhow::Result<Vec<PriceRow>> {
    let v: serde_json::Value = serde_json::from_str(payload)?;
    let arr = v
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("miniTicker payload is not an array"))?;
    Ok(parse_rows(arr, "c"))
}

/// 合约 `!markPrice@arr@1s`：逐元素取 `s`、`p`(标记价 markPrice)、`E`。
pub fn parse_futures_mark_price(payload: &str) -> anyhow::Result<Vec<PriceRow>> {
    let v: serde_json::Value = serde_json::from_str(payload)?;
    let arr = v
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("markPrice payload is not an array"))?;
    Ok(parse_rows(arr, "p"))
}

fn parse_rows(arr: &[serde_json::Value], price_key: &str) -> Vec<PriceRow> {
    let mut out = Vec::with_capacity(arr.len());
    for it in arr {
        let Some(symbol) = it.get("s").and_then(|x| x.as_str()) else {
            continue;
        };
        let Some(price) = it.get(price_key).and_then(parse_decimal) else {
            continue;
        };
        let event_ms = it.get("E").and_then(|x| x.as_i64()).unwrap_or(0);
        out.push((symbol.to_string(), price, event_ms));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn parses_spot_mini_ticker_array() {
        let payload = r#"[
            {"e":"24hrMiniTicker","E":1700000001000,"s":"BTCUSDT","c":"99999.50"},
            {"e":"24hrMiniTicker","E":1700000001000,"s":"ETHUSDT","c":"2000.25"}
        ]"#;
        assert_eq!(
            parse_spot_mini_ticker(payload).unwrap(),
            vec![
                ("BTCUSDT".to_string(), dec("99999.50"), 1700000001000),
                ("ETHUSDT".to_string(), dec("2000.25"), 1700000001000),
            ]
        );
    }

    #[test]
    fn parses_futures_mark_price_array() {
        let payload = r#"[
            {"e":"markPriceUpdate","E":1700000002000,"s":"BTCUSDT","p":"100000.25","r":"0.0001","T":1700000003000},
            {"e":"markPriceUpdate","E":1700000002000,"s":"ETHUSDT","p":2001.5}
        ]"#;
        // 第二行价格是 JSON 数字，也正常解析。
        assert_eq!(
            parse_futures_mark_price(payload).unwrap(),
            vec![
                ("BTCUSDT".to_string(), dec("100000.25"), 1700000002000),
                ("ETHUSDT".to_string(), dec("2001.5"), 1700000002000),
            ]
        );
    }

    #[test]
    fn skips_rows_with_missing_symbol_or_price() {
        let payload = r#"[{"E":1,"s":"BAD"},{"E":2,"c":"10"},{"s":"OK","c":"11","E":3}]"#;
        assert_eq!(
            parse_spot_mini_ticker(payload).unwrap(),
            vec![("OK".to_string(), dec("11"), 3)]
        );
    }

    #[test]
    fn non_array_or_bad_json_is_error() {
        assert!(parse_spot_mini_ticker(r#"{"s":"BTCUSDT"}"#).is_err());
        assert!(parse_futures_mark_price("not json").is_err());
    }

    #[test]
    fn only_whole_market_subscription_accepted() {
        assert!(require_whole_market(&[], "binance 现货").is_ok());
        assert!(require_whole_market(&["BTCUSDT".to_string()], "binance 现货").is_err());
    }

    #[test]
    fn futures_public_market_is_always_production() {
        // 公共行情是交易所级、与账户无关的监听，必须恒定正式网主机，
        // 不随任何账户的 is_testnet 切换（账户测试网只影响 REST/私有流）。
        // 2026 WS 架构迁移：全市场 markPrice 改走 /market/ws 路径（旧 /ws 不再推数据）。
        assert_eq!(
            FUTURES_URL,
            "wss://fstream.binance.com/market/ws/!markPrice@arr@1s"
        );
        assert!(
            !FUTURES_URL.contains("binancefuture"),
            "公共行情不得连接测试网主机"
        );
    }
}
