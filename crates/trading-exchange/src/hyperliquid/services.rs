//! Hyperliquid「交易所级单例」共享服务的启动装配（全用户共享、进程一份）。
//!
//! 本模块只负责**交易规则**同步：主网与测试网 meta 启动各全量同步一次
//! （POST /info meta → parse_meta 打网络标 → 整表 put），随后按周期整表刷新
//! （单网失败仅告警、等下一周期，不影响另一网）。
//!
//! 公共行情（allMids/activeAssetCtx/candle）已收口到 [`crate::market_ws`]，
//! 由 provider 构造 `HlMarketFeed` hub 单例承载，不在本模块。
//!
//! 账户私有 REST/WS 随账户选网；公共行情恒定正式网，但交易规则须两网并存
//! （asset index 随 universe 顺序不同）。
//! Hyperliquid 后置服务（L2 初始化/签名动作）。

use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use trading_core::order_rule::OrderRuleTable;

use crate::hyperliquid::endpoint;

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

/// 启动交易所级规则同步：启动全量一次 + 到点整表刷新。
pub(crate) fn spawn_rule_sync(http: Client, rules: Arc<OrderRuleTable>, interval: Duration) {
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
