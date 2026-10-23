//! Binance「交易所级单例」共享服务的启动装配（全用户共享、进程内一份）。
//!
//! 本模块负责：
//! * 交易规则启动全量同步一次 + 到点整表刷新（[`spawn_rule_sync`]，失败仅告警）；
//! * 公共行情 hub 单例（[`spawn_market_hubs`]）：现货 last 与合约 mark 各一条
//!   无认证全市场 WS（生命周期在 [`crate::market_ws`]），写入共享行情表。
//!
//! 恒定连接正式网；账户测试网只影响用户级下单 REST 与账户事件流。

use std::sync::Arc;
use std::time::Duration;

use trading_core::order_rule::OrderRuleTable;
use trading_core::quote::QuoteTable;

use crate::api::RuleFetcher;
use crate::binance::market::BinanceMarketFeed;
use crate::binance::venue::BinanceRuleFetcher;
use crate::market_ws::{self, WsHubStream};
use crate::port::QuoteObserver;

/// 构造现货/合约两条全市场行情 hub（调用方按 instrument_type 取用并保存单例）。
///
/// 现货不挂仓位重估观察口；合约 mark 写价后驱动仅 binance 仓位的重估。
pub(crate) fn spawn_market_hubs(
    quotes: Arc<QuoteTable>,
    quote_observer: Option<Arc<dyn QuoteObserver>>,
) -> (Arc<WsHubStream>, Arc<WsHubStream>) {
    let spot = market_ws::spawn_hub(
        "binance-spot",
        Arc::new(BinanceMarketFeed::spot()),
        Arc::clone(&quotes),
        None,
    );
    let futures = market_ws::spawn_hub(
        "binance-futures",
        Arc::new(BinanceMarketFeed::futures()),
        quotes,
        quote_observer,
    );
    (spot, futures)
}

/// 规则：启动全量同步一次（失败仅告警、不阻断），随后按周期整表刷新。恒定正式网。
pub(crate) fn spawn_rule_sync(rules: Arc<OrderRuleTable>, interval: Duration) {
    // 启动全量同步一次（下单查不到规则会按 venue 口径处理，故不阻断启动）。
    {
        let rules = Arc::clone(&rules);
        tokio::spawn(async move {
            let fetcher = BinanceRuleFetcher::new_prod();
            match fetcher.sync_rules(&rules).await {
                Ok(n) => tracing::info!(symbols = n, "binance 交易规则初始同步完成"),
                Err(e) => tracing::error!(
                    error = %e,
                    "初始同步 binance 交易规则失败，等待下一定时周期"
                ),
            }
        });
    }

    // 到点全量刷新；失败仅告警并等下一周期，不做立即退避重试（已有内存规则继续服务）。
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        tick.tick().await; // 跳过首个即时 tick（启动已单独同步）
        loop {
            tick.tick().await;
            let fetcher = BinanceRuleFetcher::new_prod();
            match fetcher.sync_rules(&rules).await {
                Ok(n) => tracing::info!(symbols = n, "binance 交易规则定时刷新完成"),
                Err(e) => tracing::error!(
                    error = %e,
                    "定时刷新 binance 交易规则失败，等待下一周期"
                ),
            }
        }
    });
}
