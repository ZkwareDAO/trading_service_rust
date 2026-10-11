//! trading-service 命令行配置。

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

/// 核心交易服务：南向 bind 接 strategy；对交易所（真实所/本地模拟所 xili）一律主动外连。
#[derive(Debug, Parser, Clone)]
#[command(name = "trading-service", version)]
pub struct Args {
    /// 南向 WS 监听地址（strategy 客户端接入）。
    #[arg(long, default_value = "127.0.0.1:9100")]
    pub south_addr: String,

    /// 本地模拟所端点覆盖，格式 `name=http://host:port`，可重复（如
    /// `--exchange xili=http://127.0.0.1:9200`）。服务对其主动外连：REST 下单 + WS 收账户事件
    /// （WS 地址固定为 `<base>/ws`）。不给时 xili 用内置默认端点；真实交易所忽略此项。
    #[arg(long = "exchange", value_name = "name=url")]
    pub exchanges: Vec<String>,

    /// 订单 CSV 数据目录。
    #[arg(long, default_value = "./data")]
    pub data_dir: String,

    /// 落盘间隔（如 500ms / 5s / 1m）。
    #[arg(long, default_value = "5s")]
    pub flush_interval: String,

    /// 单次交易所 REST 调用超时。
    #[arg(long, default_value = "5s")]
    pub rest_timeout: String,

    /// 账户配置文件（人工可编辑，单向导入内存，支持热加载）。留空则用 <data-dir>/account.csv。
    #[arg(long, default_value = "")]
    pub account_file: String,

    /// 账户主密钥文件（0600，首启自动生成）。留空则用 <账户文件目录>/account.key。
    #[arg(long, default_value = "")]
    pub account_key_file: String,

    /// 账户事件连接的对账周期：管理 API 触发重载后，最迟在此周期内完成连接停旧起新。
    #[arg(long, default_value = "2s")]
    pub account_reload_interval: String,

    /// 交易规则整表刷新周期（默认 720h=30 天）。
    #[arg(long, default_value = "720h")]
    pub rule_refresh_interval: String,

    /// 仓位对账去抖窗口：窗口内的 ACCOUNT_UPDATE 脏信号合并为一次 REST 拉取。
    #[arg(long, default_value = "500ms")]
    pub position_debounce: String,

    /// 仓位定时全量对账周期（无信号也强制全量，兜底漏推/外部改仓）。
    #[arg(long, default_value = "60s")]
    pub position_reconcile_interval: String,

    /// 盯盘规则扫描周期：定时评估全部 active 规则（行情/仓位已在内存中刷新）。
    #[arg(long, default_value = "200ms")]
    pub rule_scan_interval: String,

    /// 南向策略连接共享密钥文件（64 hex，0600，首启自动生成）。留空则用 <data-dir>/south.key。
    #[arg(long, default_value = "")]
    pub south_secret_file: String,

    /// 仅本地联调：跳过南向 HMAC 验签（仍需 strategy_name）。严禁在生产开启。
    #[arg(long, default_value_t = false)]
    pub south_dev_insecure: bool,

    /// 企业微信「开仓成交」机器人 Webhook；空串（默认）= 不发开仓通知。
    #[arg(long, default_value = "")]
    pub notify_open_url: String,

    /// 企业微信「风控平仓成交」机器人 Webhook；空串（默认）= 不发平仓通知。
    #[arg(long, default_value = "")]
    pub notify_close_url: String,

    /// 企业微信「Deribit 订单状态/仓位播报」机器人 Webhook；空串（默认）= 不发。
    #[arg(long, default_value = "")]
    pub notify_test_url: String,

    /// 企业微信「期权价差告警（需人工平仓）」机器人 Webhook；空串（默认）=
    /// 该告警回退使用 close_url。
    #[arg(long, default_value = "")]
    pub notify_manual_close_url: String,

    /// 企业微信「Deribit 订单事件播报（通知 A）」机器人 Webhook；空串（默认）= 不发。
    #[arg(long, default_value = "")]
    pub notify_position_url: String,

    /// 仓位告警 deribit ROI 阈值（百分数，如 -20 表示 ROI ≤ -20% 告警）。
    #[arg(long, default_value = "-20")]
    pub roi_alert_threshold_pct: f64,

    /// Venue 配置文件（交易所可调参数，如 deribit 价差阈值/漂移档数）；
    /// 文件不存在时用内置默认值。
    #[arg(long, default_value = "./config/venue.toml")]
    pub venue_config: String,
}

/// 一个交易所实例的静态端点覆盖，直接复用接入层类型（服务主动外连）。
pub type ExchangeEndpoint = trading_exchange::provider::VenueEndpoint;

/// 校验后的运行期配置。
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub south_addr: String,
    pub exchanges: Vec<ExchangeEndpoint>,
    pub data_dir: String,
    pub flush_interval: Duration,
    pub rest_timeout: Duration,
    pub account_file: String,
    pub account_key_file: String,
    pub account_reload_interval: Duration,
    pub rule_refresh_interval: Duration,
    pub position_debounce: Duration,
    pub position_reconcile_interval: Duration,
    pub rule_scan_interval: Duration,
    pub south_secret_file: String,
    pub south_dev_insecure: bool,
    /// 企业微信开仓成交通知 Webhook（空串=关闭）。
    pub notify_open_url: String,
    /// 企业微信风控平仓成交通知 Webhook（空串=关闭）。
    pub notify_close_url: String,
    /// 企业微信 Deribit 订单状态/仓位播报 Webhook（空串=关闭）。
    pub notify_test_url: String,
    /// 企业微信期权价差告警 Webhook（空串=回退 close_url）。
    pub notify_manual_close_url: String,
    /// 企业微信 Deribit 订单事件播报 Webhook（空串=关闭）。
    pub notify_position_url: String,
    /// 仓位告警 deribit ROI 阈值（百分数，默认 -20）。
    pub roi_alert_threshold_pct: f64,
    /// Venue 配置文件路径。
    pub venue_config: String,
}

impl Args {
    pub fn parse_validated() -> anyhow::Result<ServiceConfig> {
        let args = Args::parse();
        // 账户文件默认落在 data-dir 内，保证 --data-dir 切换时一起切换；显式 flag 仍优先。
        let account_file = if args.account_file.trim().is_empty() {
            PathBuf::from(&args.data_dir).join("account.csv")
        } else {
            PathBuf::from(args.account_file)
        };
        let account_key_file = if args.account_key_file.trim().is_empty() {
            account_file
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .map(|p| p.join("account.key"))
                .unwrap_or_else(|| PathBuf::from("account.key"))
        } else {
            PathBuf::from(args.account_key_file)
        };
        // 南向共享密钥默认落在 data-dir，与 account.csv 一起随 --data-dir 切换。
        let south_secret_file = if args.south_secret_file.trim().is_empty() {
            PathBuf::from(&args.data_dir).join("south.key")
        } else {
            PathBuf::from(args.south_secret_file)
        }
        .to_string_lossy()
        .into_owned();
        let exchanges = parse_exchange_endpoints(&args.exchanges)?;
        Ok(ServiceConfig {
            south_addr: args.south_addr,
            exchanges,
            data_dir: args.data_dir,
            flush_interval: trading_core::timeutil::parse_duration(&args.flush_interval)?,
            rest_timeout: trading_core::timeutil::parse_duration(&args.rest_timeout)?,
            account_file: account_file.to_string_lossy().into_owned(),
            account_key_file: account_key_file.to_string_lossy().into_owned(),
            account_reload_interval: trading_core::timeutil::parse_duration(
                &args.account_reload_interval,
            )?,
            rule_refresh_interval: trading_core::timeutil::parse_duration(
                &args.rule_refresh_interval,
            )?,
            position_debounce: trading_core::timeutil::parse_duration(&args.position_debounce)?,
            position_reconcile_interval: trading_core::timeutil::parse_duration(
                &args.position_reconcile_interval,
            )?,
            rule_scan_interval: trading_core::timeutil::parse_duration(&args.rule_scan_interval)?,
            south_secret_file,
            south_dev_insecure: args.south_dev_insecure,
            notify_open_url: args.notify_open_url.trim().to_string(),
            notify_close_url: args.notify_close_url.trim().to_string(),
            notify_test_url: args.notify_test_url.trim().to_string(),
            notify_manual_close_url: args.notify_manual_close_url.trim().to_string(),
            notify_position_url: args.notify_position_url.trim().to_string(),
            roi_alert_threshold_pct: args.roi_alert_threshold_pct,
            venue_config: args.venue_config.trim().to_string(),
        })
    }
}

/// 解析 `--exchange name=http://host:port` 列表；WS 推导复用 [`ExchangeEndpoint::new`]。
/// 重名、缺名、非法 URL 均在启动时报错（fail-fast）。
fn parse_exchange_endpoints(raw: &[String]) -> anyhow::Result<Vec<ExchangeEndpoint>> {
    let mut out = Vec::with_capacity(raw.len());
    let mut seen = std::collections::HashSet::new();
    for item in raw {
        let (name, url) = item
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("--exchange 格式应为 name=url，收到 `{item}`"))?;
        let name = name.trim();
        if name.is_empty() || url.trim().is_empty() {
            anyhow::bail!("--exchange 的 name 与 url 均不能为空：`{item}`");
        }
        if !seen.insert(name.to_string()) {
            anyhow::bail!("--exchange 交易所名重复：`{name}`");
        }
        // ws_url 推导与协议校验集中在 VenueEndpoint::new 一处。
        out.push(ExchangeEndpoint::new(name, url)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_endpoints_and_ws_url() {
        let eps = parse_exchange_endpoints(&["xili=http://127.0.0.1:9200/".to_string()]).unwrap();
        assert_eq!(eps.len(), 1);
        assert_eq!(eps[0].name, "xili");
        assert_eq!(eps[0].rest_base_url, "http://127.0.0.1:9200");
        assert_eq!(eps[0].ws_url, "ws://127.0.0.1:9200/ws");

        let tls = parse_exchange_endpoints(&["play=https://example.test".to_string()]).unwrap();
        assert_eq!(tls[0].ws_url, "wss://example.test/ws");
    }

    #[test]
    fn rejects_bad_duplicate_and_non_http() {
        assert!(parse_exchange_endpoints(&["garbage".to_string()]).is_err());
        assert!(parse_exchange_endpoints(&["=http://x".to_string()]).is_err());
        assert!(parse_exchange_endpoints(&["x=ftp://x".to_string()]).is_err());
        assert!(
            parse_exchange_endpoints(&["x=http://a".to_string(), "x=http://b".to_string()])
                .is_err()
        ); // 重名
    }
}
