//! Hyperliquid 公共行情 WS（无认证，进程一条，恒定正式网）。
//!
//! 订阅口径：
//! - `allMids`（全量，常开）：data.mids 的 coin→mid，字段级写 `Quote.mid`；
//! - `activeAssetCtx`（仅订阅兴趣集内的币）：data.ctx.markPx 写 `Quote.mark`。
//!
//! 写价后回调 `QuoteObserver` 驱动仓位重估。断线清空订阅集整体重放；周期
//! reconcile allMids（常驻）+ 兴趣集币的 activeAssetCtx。预编译键（`#123`）
//! 在写表前用规则表（universe）过滤。
//!
//! 见 `docs/行情订阅模型.md`、`docs/扩展交易所.md` 6.3。

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use trading_core::quote::{QuoteTable, QuoteUpdate};
use trading_models::{Instrument, MarketKind, Venue};

use crate::backoff::ReconnectBackoff;
use crate::port::{InterestKey, MarketInterest, QuoteObserver};

/// 公共行情（恒定正式网）WS 地址。
const MARKET_WS: &str = "wss://api.hyperliquid.xyz/ws";
/// activeAssetCtx 订阅报文必带一个 EVM 地址字段，但服务端**不校验该地址身份**、
/// 不需要私钥，也不要求它持有该币——返回的 markPx/oraclePx/funding 等都是币级公开
/// 数据，与传入哪个地址无关（已实测零地址即可取得任意币 markPx）。故这里用全零
/// 地址作「无特定 user」的中性占位，避免依赖任何真实/第三方地址。
const CTX_ROUTE_USER: &str = "0x0000000000000000000000000000000000000000";
/// 兴趣集 reconcile 周期。
const RECONCILE_INTERVAL: Duration = Duration::from_millis(500);
/// 主动 Ping / 空闲看门狗（与币安行情流同口径 10s/20s）。
const PING_INTERVAL: Duration = Duration::from_secs(10);
const WATCHDOG_INTERVAL: Duration = Duration::from_secs(5);
const IDLE_TIMEOUT: Duration = Duration::from_secs(20);

/// JSON 值（字符串/数字）→ Decimal。
fn dec_of(v: &Value) -> Option<Decimal> {
    match v {
        Value::String(s) => s.parse().ok(),
        Value::Number(_) => Decimal::from_str_exact(&v.to_string()).ok(),
        _ => None,
    }
}

/// 解析 allMids 帧为 (coin, mid) 列表（按 coin 排序，保证确定性）。
pub(crate) fn parse_all_mids(raw: &str) -> Option<Vec<(String, Decimal)>> {
    let v: Value = serde_json::from_str(raw).ok()?;
    let mids = v.pointer("/data/mids")?.as_object()?;
    let mut rows = Vec::with_capacity(mids.len());
    for (coin, px) in mids {
        if let Some(price) = dec_of(px) {
            rows.push((coin.clone(), price));
        }
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    Some(rows)
}

/// 解析 activeAssetCtx 帧为 (coin, markPx)；缺字段 → None。
pub(crate) fn parse_active_ctx(raw: &str) -> Option<(String, Decimal)> {
    let v: Value = serde_json::from_str(raw).ok()?;
    let coin = v.pointer("/data/coin")?.as_str()?;
    let mark = dec_of(v.pointer("/data/ctx/markPx")?)?;
    Some((coin.to_string(), mark))
}

/// 从兴趣集取出 hyperliquid futures 币集合（去重、有序）。
fn desired_ctx_coins(keys: &[InterestKey]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for k in keys {
        if k.venue == Venue::Hyperliquid
            && k.kind == MarketKind::Futures
            && seen.insert(k.symbol.clone())
        {
            out.push(k.symbol.clone());
        }
    }
    out.sort();
    out
}

/// 发送订阅帧（allMids / activeAssetCtx）。
async fn send_subscribe(ws: &mut crate::ws::WsStream, subscription: Value) -> anyhow::Result<()> {
    let msg = serde_json::json!({"method":"subscribe","subscription":subscription});
    ws.send(WsMessage::Text(msg.to_string().into())).await?;
    Ok(())
}

/// 发送退订帧（activeAssetCtx）。
async fn send_unsubscribe(ws: &mut crate::ws::WsStream, subscription: Value) -> anyhow::Result<()> {
    let msg = serde_json::json!({"method":"unsubscribe","subscription":subscription});
    ws.send(WsMessage::Text(msg.to_string().into())).await?;
    Ok(())
}

/// 启动公共行情 worker（detached，随进程生命周期）。
///
/// `ctx_user`：订阅 activeAssetCtx 必带 user 参数，取一个已配置 HL 账户地址（仅作路由，
/// markPx 是币级公开数据、与传入地址无关）。
pub(crate) fn spawn_worker(
    quotes: Arc<QuoteTable>,
    interests: Arc<dyn MarketInterest>,
    observer: Option<Arc<dyn QuoteObserver>>,
    ctx_user: Option<String>,
) {
    tokio::spawn(async move {
        let mut backoff = ReconnectBackoff::new();
        loop {
            if let Some(wait) = backoff.next_wait() {
                tokio::time::sleep(wait).await;
            }
            if let Err(e) = run_worker(
                &quotes,
                interests.as_ref(),
                observer.as_deref(),
                ctx_user.as_deref(),
            )
            .await
            {
                tracing::warn!(error = %e, "hyperliquid 行情 WS 结束，重连");
            }
            backoff.note_retry();
        }
    });
}

/// 单次连接：建连 → 订 allMids → 周期 reconcile activeAssetCtx / 读帧写表。
async fn run_worker(
    quotes: &QuoteTable,
    interests: &dyn MarketInterest,
    observer: Option<&dyn QuoteObserver>,
    ctx_user: Option<&str>,
) -> anyhow::Result<()> {
    let mut ws = crate::ws::connect(MARKET_WS).await?;
    // allMids 常驻（全量 mid）。
    send_subscribe(&mut ws, serde_json::json!({"type":"allMids"})).await?;
    // 本连接已订阅 activeAssetCtx 的币（重连清空 → 整体重放）。
    let mut ctx_subscribed: HashSet<String> = HashSet::new();

    let mut reconcile_tick = tokio::time::interval(RECONCILE_INTERVAL);
    let mut ping_tick = tokio::time::interval(PING_INTERVAL);
    let mut watchdog_tick = tokio::time::interval(WATCHDOG_INTERVAL);
    let mut last_frame = Instant::now();

    loop {
        tokio::select! {
            // 周期：兴趣集 diff → 订/退 activeAssetCtx。
            _ = reconcile_tick.tick() => {
                let keys = interests.interests();
                let desired: HashSet<String> =
                    desired_ctx_coins(&keys).into_iter().collect();
                // activeAssetCtx 的 user：优先显式传入，否则用公开路由常量。
                let route_user = ctx_user.unwrap_or(CTX_ROUTE_USER);
                {
                    let user = route_user;
                    // 先收集增/删币（避免在持有 difference 引用时改集合）。
                    let to_add: Vec<String> =
                        desired.difference(&ctx_subscribed).cloned().collect();
                    let to_remove: Vec<String> =
                        ctx_subscribed.difference(&desired).cloned().collect();
                    for coin in to_add {
                        let sub = serde_json::json!({"type":"activeAssetCtx","coin":coin,"user":user});
                        if send_subscribe(&mut ws, sub).await.is_ok() {
                            ctx_subscribed.insert(coin);
                        }
                    }
                    for coin in to_remove {
                        let sub = serde_json::json!({"type":"activeAssetCtx","coin":coin,"user":user});
                        let _ = send_unsubscribe(&mut ws, sub).await;
                        ctx_subscribed.remove(&coin);
                    }
                }
            }
            // 保活。
            _ = ping_tick.tick() => {
                ws.send(WsMessage::Ping(Default::default())).await?;
            }
            // 看门狗。
            _ = watchdog_tick.tick() => {
                if last_frame.elapsed() >= IDLE_TIMEOUT {
                    anyhow::bail!("hyperliquid 行情空闲超时，重连");
                }
            }
            // 读帧：allMids 写 mid，activeAssetCtx 写 mark。
            msg = ws.next() => {
                let Some(msg) = msg else { break };
                last_frame = Instant::now();
                let text = match msg? {
                    WsMessage::Text(t) => t.as_str().to_string(),
                    WsMessage::Ping(p) => { ws.send(WsMessage::Pong(p)).await?; continue; }
                    WsMessage::Close(_) => break,
                    _ => continue,
                };
                if let Some(rows) = parse_all_mids(&text) {
                    for (coin, price) in rows {
                        apply_quote(
                            quotes,
                            observer,
                            &coin,
                            price,
                            QuoteUpdate {
                                mid: Some(price),
                                ..QuoteUpdate::default()
                            },
                        );
                    }
                }
                if let Some((coin, mark)) = parse_active_ctx(&text) {
                    apply_quote(
                        quotes,
                        observer,
                        &coin,
                        mark,
                        QuoteUpdate {
                            mark: Some(mark),
                            ..QuoteUpdate::default()
                        },
                    );
                }
            }
        }
    }
    Ok(())
}

/// 字段级写行情并回调 observer：`field` 指定本帧写 mid（allMids）还是
/// mark（activeAssetCtx），时间戳统一在此打点。
fn apply_quote(
    quotes: &QuoteTable,
    observer: Option<&dyn QuoteObserver>,
    coin: &str,
    price: Decimal,
    mut field: QuoteUpdate,
) {
    let now = chrono::Utc::now().timestamp_millis();
    field.ts_exchange_ms = now;
    field.ts_recv_ms = now;
    let inst = Instrument::new(Venue::Hyperliquid, MarketKind::Futures, coin);
    quotes.apply(&inst, field);
    if let Some(ob) = observer {
        ob.on_quote(Venue::Hyperliquid, MarketKind::Futures, coin, price);
    }
}

#[cfg(test)]
mod parse_tests {
    use super::*;

    #[test]
    fn parses_all_mids_frame() {
        let raw = r##"{"channel":"allMids","data":{"mids":{
          "BTC":"82966.5","ETH":"2650.25","#12090":"0.04887"}}}"##;
        let rows = parse_all_mids(raw).expect("parsed");
        // 预编译键也解析（过滤在写表侧），按 coin 排序保证确定性。
        assert_eq!(
            rows[0],
            (
                "#12090".to_string(),
                Decimal::from_str_exact("0.04887").unwrap()
            )
        );
        assert_eq!(
            rows[1],
            (
                "BTC".to_string(),
                Decimal::from_str_exact("82966.5").unwrap()
            )
        );
        assert_eq!(
            rows[2],
            (
                "ETH".to_string(),
                Decimal::from_str_exact("2650.25").unwrap()
            )
        );
    }

    #[test]
    fn parses_active_asset_ctx_frame() {
        let raw = r#"{"channel":"activeAssetCtx","data":{
          "coin":"BTC",
          "ctx":{"markPx":"82879","oraclePx":"82911.4","midPx":"82873.5"}}}"#;
        let (coin, mark) = parse_active_ctx(raw).expect("parsed");
        assert_eq!(coin, "BTC");
        assert_eq!(mark, Decimal::from(82879));
    }

    #[test]
    fn bad_frame_or_missing_fields_yield_nothing() {
        assert!(parse_all_mids("not json").is_none());
        assert!(parse_all_mids(r#"{"channel":"allMids"}"#).is_none());
        assert!(
            parse_active_ctx(r#"{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{}}}"#)
                .is_none()
        );
    }
}
