//! Deribit 公共行情 WS：一条**恒定主网**连接，按「持仓 ∪ 规则」兴趣集动态
//! subscribe/unsubscribe `ticker.<instrument>.100ms`；断线清空已订阅集整体重放。
//!
//! ticker 帧原子携带 mark 与 best bid/ask → 字段级写 `QuoteTable{mark, book}`，
//! 并回调 QuoteObserver 驱动期权仓位重估。
//!
//! 订阅规划与 ticker 解析做成纯函数（TDD）；连接/重连为 worker。
//!
//! 见 `docs/行情订阅模型.md` §4.3/5.4。

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use trading_core::quote::{QuoteTable, QuoteUpdate};
use trading_models::{Instrument, MarketKind, TopOfBook, Venue};

use crate::backoff::ReconnectBackoff;
use crate::deribit::mapping::parse_subscription;
use crate::deribit::rpc;
use crate::deribit::spec;
use crate::port::{InterestKey, MarketInterest, QuoteObserver};

/// 行情流主动 Ping 间隔：即使订阅集为空（无持仓/规则、无 ticker 推帧）也保持有帧，
/// 重置服务端/代理/NAT 空闲计时。
const PING_INTERVAL: Duration = Duration::from_secs(10);
/// 半开看门狗检查周期 / 空闲上限：超过该时长无任何帧即判死连重连。
const WATCHDOG_INTERVAL: Duration = Duration::from_secs(5);
const IDLE_TIMEOUT: Duration = Duration::from_secs(20);

/// ticker 频道名：`ticker.<instrument>.100ms`。
pub(crate) fn ticker_channel(symbol: &str) -> String {
    format!("ticker.{symbol}.100ms")
}

/// 订阅规划：desired 与 subscribed 的差集。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubsPlan {
    pub to_sub: Vec<String>,
    pub to_unsub: Vec<String>,
}

/// 计算 desired vs subscribed 的差集：新增加订阅、多余的退订（排序保证确定性）。
pub fn reconcile_subs(desired: &HashSet<String>, subscribed: &HashSet<String>) -> SubsPlan {
    let mut to_sub: Vec<String> = desired.difference(subscribed).cloned().collect();
    let mut to_unsub: Vec<String> = subscribed.difference(desired).cloned().collect();
    to_sub.sort();
    to_unsub.sort();
    SubsPlan { to_sub, to_unsub }
}

/// 从兴趣集（已在宿主侧只给 deribit/option）得到要订阅的 ticker 频道集合（去重）。
///
/// 防御性再过滤一次 venue/kind：只订阅 deribit option 标的。
pub fn desired_channels(keys: &[InterestKey]) -> HashSet<String> {
    keys.iter()
        .filter(|k| k.venue == Venue::Deribit && k.kind == MarketKind::Option)
        .map(|k| ticker_channel(&k.symbol))
        .collect()
}

/// 从兴趣集中找出「属于 deribit option、但规格表尚无行」的标的（去重、有序）。
///
/// worker 据此用 public `get_instrument` 预热静态规格（tick/步长/合约面值），
/// 使规则引擎的同步预检一定能取到规格，不必在预检里做网络 IO。
pub fn missing_spec_symbols(
    keys: &[InterestKey],
    rules: &trading_core::order_rule::OrderRuleTable,
) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for k in keys {
        if k.venue != Venue::Deribit || k.kind != MarketKind::Option {
            continue;
        }
        if out.iter().any(|s| s == &k.symbol) {
            continue;
        }
        let spec_key = trading_models::OrderRuleKey::new(
            "deribit",
            trading_models::InstrumentType::Option,
            &k.symbol,
        );
        if rules.get(&spec_key).is_none() {
            out.push(k.symbol.clone());
        }
    }
    out.sort();
    out
}

/// 对兴趣集中缺规格的标的逐个拉取 public `get_instrument`、解析后写入规格表。
///
/// 泛型注入 `fetcher`（生产侧走 [`rpc::call`]），便于单测；单个标的拉取/解析
/// 失败仅告警并跳过（下轮 reconcile 重试），不影响订阅 diff。返回本次新写入条数。
pub async fn warm_missing_specs<F, Fut>(
    keys: &[InterestKey],
    rules: &trading_core::order_rule::OrderRuleTable,
    fetcher: F,
) -> usize
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Value>>,
{
    let wanted = missing_spec_symbols(keys, rules);
    let mut written = 0usize;
    for symbol in wanted {
        match fetcher(symbol.clone()).await {
            Ok(result) => {
                let fetched = chrono::Utc::now().timestamp_millis();
                match spec::parse_instrument(&result.to_string(), fetched) {
                    Ok(row) => {
                        tracing::info!(instrument = symbol, "合约规格已预热写表");
                        rules.put(row);
                        written += 1;
                    }
                    Err(e) => {
                        tracing::warn!(instrument = symbol, error = %e, "规格解析失败，下轮重试")
                    }
                }
            }
            Err(e) => tracing::warn!(instrument = symbol, error = %e, "规格拉取失败，下轮重试"),
        }
    }
    written
}

/// ticker 帧解析出的定价要素（mark + book）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TickerTick {
    pub mark: Decimal,
    pub bid: Option<Decimal>,
    pub ask: Option<Decimal>,
}

/// 解析 ticker data：mark_price 必填；best_bid_price/best_ask_price 可缺侧。
pub fn parse_ticker(data: &Value) -> Option<TickerTick> {
    let mark = dec_field(data.get("mark_price"))?;
    Some(TickerTick {
        mark,
        bid: dec_field(data.get("best_bid_price")),
        ask: dec_field(data.get("best_ask_price")),
    })
}

fn dec_field(v: Option<&Value>) -> Option<Decimal> {
    v.and_then(|x| match x {
        Value::String(s) => s.parse().ok(),
        Value::Number(_) => Decimal::from_str_exact(&x.to_string()).ok(),
        Value::Null => None,
        _ => None,
    })
}

/// 一帧 subscription：若是 `ticker.*.100ms`，返回 (symbol, tick)；否则 None。
pub fn parse_ticker_subscription(raw: &str) -> Option<(String, TickerTick)> {
    let (channel, data) = parse_subscription(raw).ok()??;
    let symbol = channel
        .strip_prefix("ticker.")
        .and_then(|rest| rest.strip_suffix(".100ms"))?
        .to_string();
    let tick = parse_ticker(&data)?;
    Some((symbol, tick))
}

// =====================================================================
// 行情 worker：主网恒连 + 周期 reconcile 订阅 + ticker 帧写表
// =====================================================================

/// 公共行情 WS：恒定主网（即使账户是 testnet）。
/// 路径与官方/Go 服务一致，不带结尾斜杠；迁移期代理偶发 404 由重连退避兜住。
fn market_ws_url() -> &'static str {
    "wss://www.deribit.com/ws/api/v2"
}

/// 启动行情 worker（detached，随进程生命周期）。由 provider 在激活名单含 deribit 时 spawn。
pub fn spawn_worker(
    http: reqwest::Client,
    rules: Arc<trading_core::order_rule::OrderRuleTable>,
    quotes: Arc<QuoteTable>,
    interests: Arc<dyn MarketInterest>,
    observer: Option<Arc<dyn QuoteObserver>>,
    reconcile_interval: Duration,
) {
    tokio::spawn(async move {
        let mut backoff = ReconnectBackoff::new();
        loop {
            if let Some(wait) = backoff.next_wait() {
                tokio::time::sleep(wait).await;
            }
            let result = run_worker(
                &http,
                &rules,
                &quotes,
                interests.as_ref(),
                observer.as_deref(),
                reconcile_interval,
            )
            .await;
            if let Err(e) = result {
                tracing::warn!(error = %e, "deribit market ws ended; reconnecting");
            }
            // 连接结束（断开/错误）→ 退避重连；重连后 subscribed 清空整体重放。
            backoff.note_retry();
        }
    });
}

/// 单次连接：握手 → 周期 reconcile 订阅 / 读帧，直到关闭/错误。
async fn run_worker(
    http: &reqwest::Client,
    rules: &trading_core::order_rule::OrderRuleTable,
    quotes: &QuoteTable,
    interests: &dyn MarketInterest,
    observer: Option<&dyn QuoteObserver>,
    reconcile_interval: Duration,
) -> anyhow::Result<()> {
    let mut ws = crate::ws::connect(market_ws_url()).await?;
    // 本连接已订阅频道（连接重建即清空 → 整体重放）。
    let mut subscribed: HashSet<String> = HashSet::new();
    let mut reconcile_tick = tokio::time::interval(reconcile_interval);
    // 主动 Ping 定时器 + 空闲看门狗（与币安行情流同口径 10s/20s）。
    let mut ping_tick = tokio::time::interval(PING_INTERVAL);
    let mut watchdog_tick = tokio::time::interval(WATCHDOG_INTERVAL);
    let mut last_frame = Instant::now();

    loop {
        tokio::select! {
            // 周期：读兴趣集 → 预热规格 → diff → subscribe/unsubscribe。
            _ = reconcile_tick.tick() => {
                let keys = interests.interests();
                // 缺规格的标的先拉 public/get_instrument 写表（与订阅同一兴趣集）。
                let http_c = http.clone();
                let n = warm_missing_specs(&keys, rules, move |symbol| {
                    let http_c = http_c.clone();
                    async move {
                        rpc::call(
                            http_c,
                            rpc::rest_base(false),
                            "public/get_instrument",
                            serde_json::json!({ "instrument_name": symbol }),
                            None,
                        )
                        .await
                    }
                })
                .await;
                if n > 0 {
                    tracing::info!(count = n, "reconcile 预热合约规格");
                }
                let desired = desired_channels(&keys);
                let plan = reconcile_subs(&desired, &subscribed);
                if !plan.to_sub.is_empty() {
                    send_sub(&mut ws, "public/subscribe", &plan.to_sub).await?;
                    subscribed.extend(plan.to_sub.iter().cloned());
                }
                if !plan.to_unsub.is_empty() {
                    send_sub(&mut ws, "public/unsubscribe", &plan.to_unsub).await?;
                    for ch in &plan.to_unsub {
                        subscribed.remove(ch);
                    }
                }
            }
            // 主动保活：周期发一帧 Ping（即使无订阅也保持有帧）。
            _ = ping_tick.tick() => {
                ws.send(WsMessage::Ping(Default::default())).await?;
            }
            // 半开/空订阅看门狗：超 IDLE_TIMEOUT 无任何帧 → 判死重连。
            _ = watchdog_tick.tick() => {
                if last_frame.elapsed() >= IDLE_TIMEOUT {
                    anyhow::bail!("market stream idle timeout, reconnecting");
                }
            }
            // 读帧：ticker → 写 QuoteTable + observer。
            msg = ws.next() => {
                let Some(msg) = msg else { break };
                // 任意入站帧（含服务端 Ping/Pong）证明存活，刷新看门狗。
                last_frame = Instant::now();
                let text = match msg? {
                    WsMessage::Text(t) => t.as_str().to_string(),
                    WsMessage::Ping(p) => { ws.send(WsMessage::Pong(p)).await?; continue; }
                    WsMessage::Close(_) => break,
                    _ => continue,
                };
                if let Some((symbol, tick)) = parse_ticker_subscription(&text) {
                    apply_tick(quotes, observer, &symbol, tick);
                }
            }
        }
    }
    Ok(())
}

/// 发送 subscribe/unsubscribe 请求。
async fn send_sub(
    ws: &mut crate::ws::WsStream,
    method: &str,
    channels: &[String],
) -> anyhow::Result<()> {
    let body = serde_json::json!({
        "jsonrpc": "2.0", "id": 1,
        "method": method,
        "params": { "channels": channels },
    });
    ws.send(WsMessage::Text(body.to_string().into())).await?;
    Ok(())
}

/// ticker → QuoteTable 字段级写入（mark + book），并回调 observer 驱动仓位重估。
fn apply_tick(
    quotes: &QuoteTable,
    observer: Option<&dyn QuoteObserver>,
    symbol: &str,
    tick: TickerTick,
) {
    let now = chrono::Utc::now().timestamp_millis();
    let book = TopOfBook::new(tick.bid, tick.ask);
    let update = QuoteUpdate {
        last: None,
        mark: Some(tick.mark),
        mid: None,
        book: Some(book),
        ts_exchange_ms: now,
        ts_recv_ms: now,
    };
    let inst = Instrument::new(Venue::Deribit, MarketKind::Option, symbol);
    quotes.apply(&inst, update);
    if let Some(ob) = observer {
        ob.on_quote(Venue::Deribit, MarketKind::Option, symbol, tick.mark);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn reconcile_diffs_desired_and_subscribed() {
        let desired = set(&["a", "b"]);
        let subscribed = set(&["b", "c"]);
        let plan = reconcile_subs(&desired, &subscribed);
        assert_eq!(plan.to_sub, vec!["a".to_string()]);
        assert_eq!(plan.to_unsub, vec!["c".to_string()]);
    }

    #[test]
    fn reconcile_no_change_is_empty() {
        let s = set(&["a"]);
        assert_eq!(reconcile_subs(&s, &s), SubsPlan::default());
    }

    #[test]
    fn ticker_channel_name() {
        assert_eq!(ticker_channel("BTC-1"), "ticker.BTC-1.100ms");
    }

    /// 构造一个已写入规格表的期权标的。
    fn seed_spec(rules: &trading_core::order_rule::OrderRuleTable, symbol: &str) {
        use trading_models::{InstrumentType, OrderRule, QtyUnit};
        rules.put(OrderRule::new(
            "deribit",
            symbol,
            InstrumentType::Option,
            QtyUnit::Contract,
            Decimal::ONE,
            "BTC",
            "BTC",
            dec("0.1"),
            "deribit:get_instrument",
            0,
        ));
    }

    #[test]
    fn missing_spec_symbols_lists_unfetched_option_interest() {
        use crate::port::InterestKey;
        let rules = trading_core::order_rule::OrderRuleTable::default();
        seed_spec(&rules, "BTC-HAVE");
        let keys = vec![
            InterestKey::new(Venue::Deribit, MarketKind::Option, "BTC-NEED"),
            InterestKey::new(Venue::Deribit, MarketKind::Option, "BTC-NEED"), // 去重
            InterestKey::new(Venue::Deribit, MarketKind::Option, "BTC-HAVE"), // 已有规格→排除
            InterestKey::new(Venue::BinanceFutures, MarketKind::Futures, "BTCUSDT"), // 非 deribit→排除
        ];
        assert_eq!(
            missing_spec_symbols(&keys, &rules),
            vec!["BTC-NEED".to_string()]
        );
    }

    #[tokio::test]
    async fn warm_fetches_missing_and_skips_present() {
        use crate::port::InterestKey;
        use std::sync::{Arc, Mutex};
        // 假 fetcher：返回合成规格 result；记录被请求的标的。
        let asked: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        // fetcher 返回 RPC 的 result 对象（口径同 rpc::call）。
        let asked_c = Arc::clone(&asked);
        let fetcher = move |symbol: String| {
            let asked_c = Arc::clone(&asked_c);
            async move {
                asked_c.lock().unwrap().push(symbol.clone());
                Ok(serde_json::json!({
                    "instrument_name": symbol,
                    "base_currency":"BTC",
                    "contract_size":1,
                    "tick_size":"0.0001",
                    "min_trade_amount":"0.1"
                }))
            }
        };
        let rules = trading_core::order_rule::OrderRuleTable::default();
        seed_spec(&rules, "BTC-HAVE");
        let keys = vec![
            InterestKey::new(Venue::Deribit, MarketKind::Option, "BTC-NEED"),
            InterestKey::new(Venue::Deribit, MarketKind::Option, "BTC-HAVE"),
        ];
        let n = warm_missing_specs(&keys, &rules, fetcher).await;
        assert_eq!(n, 1);
        assert_eq!(*asked.lock().unwrap(), vec!["BTC-NEED".to_string()]);
        // 拉取后规格已写表。
        let spec_key = trading_models::OrderRuleKey::new(
            "deribit",
            trading_models::InstrumentType::Option,
            "BTC-NEED",
        );
        assert!(rules.get(&spec_key).is_some());
    }

    #[test]
    fn missing_spec_symbols_empty_when_all_present() {
        use crate::port::InterestKey;
        let rules = trading_core::order_rule::OrderRuleTable::default();
        seed_spec(&rules, "BTC-1");
        let keys = vec![InterestKey::new(
            Venue::Deribit,
            MarketKind::Option,
            "BTC-1",
        )];
        assert!(missing_spec_symbols(&keys, &rules).is_empty());
    }

    #[test]
    fn desired_channels_filters_and_maps_interest() {
        use crate::port::InterestKey;
        let keys = vec![
            InterestKey::new(Venue::Deribit, MarketKind::Option, "BTC-1"),
            InterestKey::new(Venue::Deribit, MarketKind::Option, "BTC-1"), // 去重
            InterestKey::new(Venue::BinanceFutures, MarketKind::Futures, "BTCUSDT"), // 过滤
        ];
        let ch = desired_channels(&keys);
        assert_eq!(ch, set(&["ticker.BTC-1.100ms"]));
    }

    #[test]
    fn parses_ticker_fields_and_missing_sides() {
        // 字段名按 Deribit 真实 ticker 报文：best_bid_price / best_ask_price。
        let data = serde_json::json!({
            "mark_price": 0.5, "best_bid_price": 0.49, "best_ask_price": 0.51
        });
        let t = parse_ticker(&data).expect("tick");
        assert_eq!(t.mark, Decimal::from_str("0.5").unwrap());
        assert_eq!(t.bid, Decimal::from_str("0.49").ok());
        assert_eq!(t.ask, Decimal::from_str("0.51").ok());

        let one = serde_json::json!({"mark_price":0.5,"best_bid_price":0.49});
        assert!(parse_ticker(&one).expect("tick").ask.is_none());
        assert!(parse_ticker(&serde_json::json!({"best_bid_price":0.49})).is_none());
    }

    #[test]
    fn parses_ticker_subscription_frame() {
        let raw = r#"{
          "jsonrpc":"2.0","method":"subscription",
          "params":{"channel":"ticker.BTC-1.100ms","data":{
            "mark_price":0.5,"best_bid_price":0.49,"best_ask_price":0.51
          }}
        }"#;
        let (symbol, t) = parse_ticker_subscription(raw).expect("parsed");
        assert_eq!(symbol, "BTC-1");
        assert_eq!(t.mark, Decimal::from_str("0.5").unwrap());
    }

    #[test]
    fn non_ticker_frame_is_none() {
        let raw = r#"{"jsonrpc":"2.0","method":"subscription",
            "params":{"channel":"user.orders.option.BTC.raw","data":{}}}"#;
        assert!(parse_ticker_subscription(raw).is_none());
    }
}
