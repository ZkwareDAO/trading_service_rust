//! Binance「交易所级单例」共享服务的启动装配（全用户共享、进程内一份）。
//!
//! 一次 [`spawn_shared`] 拉起四类后台任务（全部随进程生命周期）：
//! * 交易规则启动全量同步一次 + 到点整表刷新（失败仅告警、等下一周期）；
//! * 现货公共行情（last）与合约公共行情（mark）两条无认证全市场流，写入共享行情表。
//!
//! 宿主装配层只需提供共享的规则表/行情表与环境参数，不接触任何 Binance 具体类型。

use std::sync::Arc;
use std::time::Duration;

use trading_core::order_rule::OrderRuleTable;
use trading_core::quote::QuoteTable;

use crate::api::RuleFetcher;
use crate::binance::market::{forward_futures_quotes, forward_spot_quotes};
use crate::binance::venue::BinanceRuleFetcher;
use crate::port::QuoteObserver;

/// 拉起 Binance 交易所级共享任务（规则同步/刷新 + 现货/合约行情转发）。
///
/// 这些都是**交易所级、与账户无关**的公共任务，恒定连接正式网；账户的
/// 测试网选择只影响用户级下单 REST 与账户事件流。
///
/// `rule_refresh_interval`：规则整表刷新周期；启动时另做一次即时全量同步。
/// `quote_observer`：合约 mark 写价后的仓位重估观察口（现货不参与仓位流水线，传 None）。
pub fn spawn_shared(
    rules: Arc<OrderRuleTable>,
    quotes: Arc<QuoteTable>,
    rule_refresh_interval: Duration,
    quote_observer: Option<Arc<dyn QuoteObserver>>,
) {
    spawn_rule_sync(Arc::clone(&rules), rule_refresh_interval);

    // 公共行情：现货 last + 合约 mark，两条无认证全市场流（任务内部自带退避重连）。
    // 现货无杠杆仓位语义，不挂观察口；合约写价后驱动仅 binance 仓位的重估。
    tokio::spawn(forward_spot_quotes(Arc::clone(&quotes)));
    tokio::spawn(forward_futures_quotes(quotes, quote_observer));
}

/// 规则：启动全量同步一次（失败仅告警、不阻断），随后按周期整表刷新。恒定正式网。
fn spawn_rule_sync(rules: Arc<OrderRuleTable>, interval: Duration) {
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
