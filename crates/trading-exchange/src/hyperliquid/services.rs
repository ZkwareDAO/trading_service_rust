//! Hyperliquid「交易所级单例」共享服务的启动装配（全用户共享、进程一份）。
//!
//! [`spawn_shared`] 拉起：
//! - 交易规则：主网与测试网 meta 启动各全量同步一次（POST /info meta →
//!   parse_meta 打网络标 → 整表 put），随后按周期整表刷新（单网失败仅告警、
//!   等下一周期，不影响另一网）；
//! - 公共行情：allMids(mid) + 兴趣集币的 activeAssetCtx(mark) 的一条 WS worker。
//!
//! 账户私有 REST/WS 随账户选网；公共行情恒定正式网，但交易规则须两网并存
//! （asset index 随 universe 顺序不同）。
//! 见 `docs/扩展交易所.md` 第二节步骤 9。

use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use trading_core::order_rule::OrderRuleTable;
use trading_core::quote::QuoteTable;

use crate::hyperliquid::endpoint;
use crate::hyperliquid::market;
use crate::port::{MarketInterest, QuoteObserver};

/// 主网 + 测试网 meta 各拉一份、各自打标整表写入，两网同名币互不覆盖。
///
/// 单个网络失败只告警、不影响另一网络；返回错误仅在「两网都失败」时给出，
/// 供启动/定时刷新日志体现。
async fn sync_rules_once(http: &Client, table: &OrderRuleTable) -> anyhow::Result<()> {
    const NETWORKS: [(&str, bool); 2] = [
        (endpoint::MAINNET_REST_INFO, false),
        (endpoint::TESTNET_REST_INFO, true),
    ];
    let mut errors: Vec<String> = Vec::new();
    for (info_url, is_testnet) in NETWORKS {
        if let Err(e) = sync_one_network(http, table, info_url, is_testnet).await {
            let net = if is_testnet { "测试网" } else { "主网" };
            tracing::error!(error = %e, "hyperliquid {net} 规则同步失败");
            errors.push(format!("{net}: {e}"));
        }
    }
    if errors.len() == NETWORKS.len() {
        anyhow::bail!("hyperliquid 两网规则均同步失败：{}", errors.join("；"));
    }
    Ok(())
}

/// 拉取单网 meta → 解析（打网络标）→ 整表写入。
async fn sync_one_network(
    http: &Client,
    table: &OrderRuleTable,
    info_url: &str,
    is_testnet: bool,
) -> anyhow::Result<()> {
    let raw: String = http
        .post(info_url)
        .json(&serde_json::json!({"type":"meta"}))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let fetched = chrono::Utc::now().timestamp_millis();
    let rows = crate::hyperliquid::meta::parse_meta(&raw, fetched, is_testnet)?;
    for row in rows {
        table.put(row);
    }
    Ok(())
}

/// 启动交易所级共享服务：规则同步/刷新 + 公共行情。
///
/// `ctx_user`：activeAssetCtx 订阅所需 user 路由地址（一个已配置 HL 账户）。
pub(crate) fn spawn_shared(
    http: Client,
    rules: Arc<OrderRuleTable>,
    quotes: Arc<QuoteTable>,
    rule_refresh_interval: Duration,
    interests: Arc<dyn MarketInterest>,
    observer: Option<Arc<dyn QuoteObserver>>,
    ctx_user: Option<String>,
) {
    spawn_rule_sync(http, rules, rule_refresh_interval);
    market::spawn_worker(quotes, interests, observer, ctx_user);
}

/// 规则：启动全量一次 + 到点整表刷新。
fn spawn_rule_sync(http: Client, rules: Arc<OrderRuleTable>, interval: Duration) {
    // 启动全量（失败仅告警，不阻断）。
    {
        let rules = Arc::clone(&rules);
        let http = http.clone();
        tokio::spawn(async move {
            match sync_rules_once(&http, &rules).await {
                Ok(()) => tracing::info!("hyperliquid 交易规则初始同步完成"),
                Err(e) => {
                    tracing::error!(error = %e, "初始同步 hyperliquid 规则失败，等下一定时周期")
                }
            }
        });
    }
    // 定时整表刷新。
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        tick.tick().await; // 跳过首个即时 tick
        loop {
            tick.tick().await;
            match sync_rules_once(&http, &rules).await {
                Ok(()) => tracing::info!("hyperliquid 交易规则定时刷新完成"),
                Err(e) => tracing::error!(error = %e, "定时刷新 hyperliquid 规则失败"),
            }
        }
    });
}
