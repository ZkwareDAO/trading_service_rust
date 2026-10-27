//! 三家 WS 交易所（deribit / hyperliquid / binance）共用的一套 WS 行情**引擎**。
//!
//! 本模块收口连接生命周期（重连退避 / 10s Ping / 5s 看门狗 20s 判死 / 500ms 对账 /
//! 重连整体重放）与消费者注册表，产出可组合的 [`WsHubEngine`]。引擎**自身不实现**
//! [`crate::market::MarketDataStream`]：各交易所的具名流（`binance/stream.rs` 等）
//! 持有引擎、委托订阅原语并实现该 trait；所内协议差异经 [`WsMarketFeed`] 的**协议纯函数**
//! 注入（频道映射、订阅编码、帧解码、规格预热）。
//!
//! 数据流（零 channel，同 `QuoteObserver` 模式）：
//! - 驱动读帧 → `feed.decode` 得 [`PriceTick`] / [`Kline`]；
//! - 价格 tick：`feed.write_quote` 同步直写 `QuoteTable` + observer（托管摄取主路径）；
//! - 定稿 bar：[`HubRegistry::dispatch_bar`] 按注册表路由给 K线消费者（catch_unwind+计时摘除）。
//!
//! 订阅集每 500ms 从 [`HubRegistry`] 锁读「价格并集 ∪ K线并集」，经 `feed.reconcile`
//! 做频道 diff 后增量订阅/退订；断线重连后本地 `subscribed` 清空 → 整体重放。

use std::collections::{BTreeSet, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use trading_core::quote::QuoteTable;

use crate::backoff::ReconnectBackoff;
use crate::market::{BarSink, DesiredSource, Kline, PriceTick, SubscriptionGuard};
use crate::market_hub::HubRegistry;
use crate::port::QuoteObserver;
use crate::ws::WsStream;

/// 兴趣集对账周期（三家统一）。
const RECONCILE_INTERVAL: Duration = Duration::from_millis(500);
/// 主动 Ping 间隔（空订阅也保活）。
const PING_INTERVAL: Duration = Duration::from_secs(10);
/// 看门狗检查周期。
const WATCHDOG_INTERVAL: Duration = Duration::from_secs(5);
/// 无任何入站帧超过该时长即判死连重连。
const IDLE_TIMEOUT: Duration = Duration::from_secs(20);
/// bar sink 单次回调的摘除阈值：目标 <1µs，1ms 为「卡死」兜底门槛（抖动不误摘）。
const BAR_SINK_BUDGET: Duration = Duration::from_millis(1);

/// 一轮对账产出的频道增/删（元素已是各所线上频道字符串）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChannelPlan {
    pub subscribe: Vec<String>,
    pub unsubscribe: Vec<String>,
}

impl ChannelPlan {
    pub fn is_empty(&self) -> bool {
        self.subscribe.is_empty() && self.unsubscribe.is_empty()
    }
}

/// 一帧解码结果：若干价格 tick（直写 QuoteTable）与定稿 bar（路由给 K线消费者）。
#[derive(Debug, Clone, Default)]
pub struct FeedMessage {
    pub ticks: Vec<PriceTick>,
    pub bars: Vec<Kline>,
}

impl FeedMessage {
    pub fn is_empty(&self) -> bool {
        self.ticks.is_empty() && self.bars.is_empty()
    }
}

/// 单家交易所的行情协议端口：只放**该所知识**，不含连接生命周期。
///
/// - 频道映射 / 订阅编码 / 帧解码是纯逻辑（解码为纯函数，便于单测）；
/// - [`Self::on_connect`] 做建连后一次性初始化帧（如 HL allMids）；
/// - [`Self::on_desired_async`] 做规格预热（如 deribit warm_missing_specs），允许 await/REST。
#[async_trait]
pub trait WsMarketFeed: Send + Sync {
    /// WS 地址。
    fn url(&self) -> &str;

    /// 建连成功后、首轮对账前的一次性初始化（如 HL 常驻 allMids）。默认无操作。
    async fn on_connect(&self, _ws: &mut WsStream) -> anyhow::Result<()> {
        Ok(())
    }

    /// 纯函数：给定当前价格/K线期望符号集与本连接已订阅频道，产出频道增删。
    fn reconcile(
        &self,
        price_symbols: &BTreeSet<String>,
        bar_symbols: &BTreeSet<String>,
        subscribed: &HashSet<String>,
    ) -> ChannelPlan;

    /// 发送增订频道（deribit 批量一帧；HL/binance 逐频道一帧）。
    async fn send_subscribe(&self, ws: &mut WsStream, channels: &[String]) -> anyhow::Result<()>;

    /// 发送退订频道。
    async fn send_unsubscribe(&self, ws: &mut WsStream, channels: &[String]) -> anyhow::Result<()>;

    /// 期望集变化时的异步副作用（规格预热等）；每轮对账在订阅前调用。默认无操作。
    async fn on_desired_async(
        &self,
        _price_symbols: &BTreeSet<String>,
        _bar_symbols: &BTreeSet<String>,
    ) {
    }

    /// 解码一帧文本为价格 tick / 定稿 bar（非订阅帧/非收盘帧返回空）。
    fn decode(&self, text: &str) -> FeedMessage;

    /// 价格 tick 同步直写 QuoteTable + observer（venue→Instrument 映射在各所实现内）。
    fn write_quote(
        &self,
        tick: &PriceTick,
        quotes: &QuoteTable,
        observer: Option<&dyn QuoteObserver>,
    );
}

/// 空闲看门狗纯判定：距最后一帧达到/超过超时即判死。
pub fn idle_dead(last_frame: Instant, now: Instant, timeout: Duration) -> bool {
    now.duration_since(last_frame) >= timeout
}

/// 桶起点 ms（UTC）→ YYYYMMDD 交易日。三家 7×24 交易所的原生 K线共用此口径。
pub fn trading_day_of_utc_ms(start_ms: i64) -> u32 {
    use chrono::TimeZone;
    chrono::Utc
        .timestamp_millis_opt(start_ms)
        .single()
        .unwrap_or_default()
        .format("%Y%m%d")
        .to_string()
        .parse()
        .unwrap_or(0)
}

/// WS 行情**引擎**：三家 WS 所共用的连接生命周期 + 消费者注册表，是一个被各所
/// **组合/委托**的库构件，自身**不实现** [`crate::market::MarketDataStream`]。
///
/// 所有权反转：各交易所在自己模块内持有本引擎并实现具名 `impl MarketDataStream`
/// （见 `binance/stream.rs` 等），把订阅原语委托到这里；交易所要加所内额外处理
/// （装饰/过滤 sink、挂第二来源等）只改自己的具名流，不必动本引擎或统一 trait。
///
/// 持注册表 + 可选驱动任务：[`WsHubEngine::spawn_hub`] 产出带连接任务的进程单例；
/// `spawn_driverless`（`#[cfg(test)]`）不起连接、仅供进程内单测。
pub struct WsHubEngine {
    hub: HubRegistry,
    /// 驱动任务（进程生命周期，不主动 abort；句柄仅持有，不 await）。
    /// driverless 构造（单测）时为 None。
    _task: Option<JoinHandle<()>>,
    /// 托管订阅凭证（系统行情 InterestSource 等），与句柄同寿，drop 才退订。
    /// std::Mutex：注册只在启动期同步 push（无 await、临界区极短）。
    managed: Mutex<Vec<SubscriptionGuard>>,
}

impl WsHubEngine {
    /// 内部构造：给定注册表与可选驱动任务。
    fn with_hub(hub: HubRegistry, task: Option<JoinHandle<()>>) -> Arc<Self> {
        Arc::new(Self {
            hub,
            _task: task,
            managed: Mutex::new(Vec::new()),
        })
    }

    /// 启动一条由 `feed` 描述协议的 WS 行情连接，返回其共享引擎单例。
    ///
    /// 驱动任务随进程生命周期运行：建连失败/断开按 [`ReconnectBackoff`] 退避重连，
    /// 重连后清空已订阅集，按当前全局并集整体重放。
    pub fn spawn_hub<F>(
        label: &'static str,
        feed: Arc<F>,
        quotes: Arc<QuoteTable>,
        observer: Option<Arc<dyn QuoteObserver>>,
    ) -> Arc<Self>
    where
        F: WsMarketFeed + 'static,
    {
        let hub = HubRegistry::new();
        let task = tokio::spawn(run_forever(label, feed, hub.clone(), quotes, observer));
        Self::with_hub(hub, Some(task))
    }

    /// 不起任何连接任务的引擎（仅进程内单测用）：注册/派发纯走 [`HubRegistry`]。
    #[cfg(test)]
    pub fn spawn_driverless() -> Arc<Self> {
        Self::with_hub(HubRegistry::new(), None)
    }

    /// 取出注册表句柄供测试注入定稿 bar（与引擎共享同一注册表）。
    #[cfg(test)]
    pub fn registry_for_test(&self) -> HubRegistry {
        self.hub.clone()
    }

    /// 登记系统行情来源，返回 drop 即退订的凭证（具名流的 `subscribe_prices` 委托此）。
    pub fn subscribe_prices(
        &self,
        src: Arc<dyn DesiredSource>,
    ) -> anyhow::Result<SubscriptionGuard> {
        Ok(self.hub.register_prices(src))
    }

    /// 登记 K线消费者（期望来源 + 定稿回调），返回 drop 即退订的凭证。
    pub fn subscribe_klines(
        &self,
        src: Arc<dyn DesiredSource>,
        sink: Arc<dyn BarSink>,
    ) -> anyhow::Result<SubscriptionGuard> {
        Ok(self.hub.register_klines(src, sink))
    }

    /// 注册一个与句柄同寿的托管系统行情来源（不返回 guard，进程期内不退订）。
    pub fn register_managed_prices(&self, src: Arc<dyn DesiredSource>) {
        let guard = self.hub.register_prices(src);
        self.managed
            .lock()
            .expect("managed guards lock")
            .push(guard);
    }
}

/// 与旧三家 worker 同构的「退避 → 单连接 → 断开退避」外层循环。
async fn run_forever<F: WsMarketFeed>(
    label: &'static str,
    feed: Arc<F>,
    hub: HubRegistry,
    quotes: Arc<QuoteTable>,
    observer: Option<Arc<dyn QuoteObserver>>,
) {
    let mut backoff = ReconnectBackoff::new();
    loop {
        if let Some(wait) = backoff.next_wait() {
            tokio::time::sleep(wait).await;
        }
        match run_connection(feed.as_ref(), &hub, &quotes, observer.as_deref()).await {
            Ok(()) => {
                // run_connection 仅在对端正常关闭时返回 Ok；仍退避重连保活。
                tracing::warn!(stream = label, "market ws closed, reconnecting");
            }
            Err(e) => {
                tracing::warn!(stream = label, error = %e, "market ws error, reconnecting")
            }
        }
        backoff.note_retry();
    }
}

/// 单次连接：握手 → on_connect → 周期对账 / Ping / 看门狗 / 读帧，直到关闭或错误。
async fn run_connection<F: WsMarketFeed>(
    feed: &F,
    hub: &HubRegistry,
    quotes: &QuoteTable,
    observer: Option<&dyn QuoteObserver>,
) -> anyhow::Result<()> {
    let mut ws = crate::ws::connect(feed.url()).await?;
    feed.on_connect(&mut ws).await?;
    // 本连接已订阅频道；连接重建即清空 → 下轮对账整体重放。
    let mut subscribed: HashSet<String> = HashSet::new();

    let mut reconcile_tick = tokio::time::interval(RECONCILE_INTERVAL);
    let mut ping_tick = tokio::time::interval(PING_INTERVAL);
    let mut watchdog_tick = tokio::time::interval(WATCHDOG_INTERVAL);
    let mut last_frame = Instant::now();

    loop {
        tokio::select! {
            // 周期：锁读并集 → 预热 → 频道 diff → 增订/退订。
            _ = reconcile_tick.tick() => {
                let price_syms = hub.desired_price_symbols();
                let bar_syms = hub.desired_bar_symbols();
                feed.on_desired_async(&price_syms, &bar_syms).await;
                let plan = feed.reconcile(&price_syms, &bar_syms, &subscribed);
                if !plan.subscribe.is_empty()
                    && feed.send_subscribe(&mut ws, &plan.subscribe).await.is_ok()
                {
                    subscribed.extend(plan.subscribe.iter().cloned());
                }
                if !plan.unsubscribe.is_empty()
                    && feed.send_unsubscribe(&mut ws, &plan.unsubscribe).await.is_ok()
                {
                    for ch in &plan.unsubscribe {
                        subscribed.remove(ch);
                    }
                }
            }
            // 主动保活 Ping（即使无订阅也保持有帧）。
            _ = ping_tick.tick() => {
                ws.send(WsMessage::Ping(Default::default())).await?;
            }
            // 半开/空订阅看门狗。
            _ = watchdog_tick.tick() => {
                if idle_dead(last_frame, Instant::now(), IDLE_TIMEOUT) {
                    anyhow::bail!("market stream idle timeout, reconnecting");
                }
            }
            // 读帧：解码 → 价格直写 / bar 路由。
            msg = ws.next() => {
                let Some(msg) = msg else { break };
                // 任意入站帧（含服务端 Ping/Pong）证明存活，刷新看门狗。
                last_frame = Instant::now();
                let text = match msg? {
                    WsMessage::Text(t) => t.as_str().to_string(),
                    WsMessage::Binary(b) => String::from_utf8_lossy(&b).to_string(),
                    WsMessage::Ping(p) => { ws.send(WsMessage::Pong(p)).await?; continue; }
                    WsMessage::Close(_) => break,
                    _ => continue,
                };
                let msg = feed.decode(&text);
                for tick in &msg.ticks {
                    feed.write_quote(tick, quotes, observer);
                }
                for bar in &msg.bars {
                    // 坏/慢 sink 在 dispatch_bar 内隔离摘除，不影响连接与价格路径。
                    hub.dispatch_bar(bar, BAR_SINK_BUDGET);
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_watchdog_threshold() {
        let now = Instant::now();
        let last = now - Duration::from_secs(19);
        assert!(!idle_dead(last, now, Duration::from_secs(20)));
        let last = now - Duration::from_secs(20);
        assert!(idle_dead(last, now, Duration::from_secs(20)));
        let last = now - Duration::from_secs(21);
        assert!(idle_dead(last, now, Duration::from_secs(20)));
    }

    #[test]
    fn channel_plan_empty_predicate() {
        assert!(ChannelPlan::default().is_empty());
        assert!(!ChannelPlan {
            subscribe: vec!["a".into()],
            unsubscribe: vec![],
        }
        .is_empty());
    }

    #[test]
    fn feed_message_default_is_empty() {
        assert!(FeedMessage::default().is_empty());
    }
}
