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

    /// K线（定稿 bar / CTP 原始 tick）批量落 CSV 的周期。
    #[arg(long, default_value = "5s")]
    pub kline_flush_interval: String,

    /// BarTable 每序列（venue×symbol）在内存保留的最近 bar 条数上限（1m，1440≈24h）。
    #[arg(long, default_value = "1440")]
    pub kline_retain_bars: String,

    /// BarTable 每序列 bar 的内存保留时长（如 24h / 6h）；与条数上限取先到者。
    #[arg(long, default_value = "24h")]
    pub kline_retain_age: String,

    /// get_klines 不传 limit 时缺省返回根数。
    #[arg(long, default_value = "300")]
    pub kline_history_default_limit: String,

    /// get_klines 单次返回根数硬上限（请求超限截断到此值）。
    #[arg(long, default_value = "1500")]
    pub kline_history_max_limit: String,

    /// 三家 K线缺口内部扫描周期（仅 binance/deribit/hyperliquid）；设 0 关闭周期扫描（实时尾检仍在）。
    #[arg(long, default_value = "5m")]
    pub kline_gap_scan_interval: String,

    /// 三家静默停推阈值：now-末键 超过此时长即登记缺口（如 3m），必须 >0。
    #[arg(long, default_value = "3m")]
    pub kline_stall_threshold: String,

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
    /// K线 bar/tick 批量落 CSV 周期。
    pub kline_flush_interval: Duration,
    /// BarTable 每序列内存保留 bar 条数上限。
    pub kline_retain_bars: usize,
    /// BarTable 每序列内存保留时长。
    pub kline_retain_age: Duration,
    /// get_klines 缺省/最大返回根数。
    pub kline_history_limits: crate::kline_history::HistoryLimits,
    /// 三家 K线缺口内部扫描周期（0=关闭周期扫描，实时尾检仍在）。
    pub kline_gap_scan_interval: Duration,
    /// 三家静默停推阈值（必须 >0）。
    pub kline_stall_threshold: Duration,
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
        let kline_retain_bars = parse_retain_bars(&args.kline_retain_bars)?;
        let kline_retain_age = trading_core::timeutil::parse_duration(&args.kline_retain_age)?;
        let history_limits = parse_history_limits(
            Some(&args.kline_history_default_limit),
            Some(&args.kline_history_max_limit),
        )?;
        let (kline_gap_scan_interval, kline_stall_threshold) =
            parse_gap_timings(&args.kline_gap_scan_interval, &args.kline_stall_threshold)?;
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
            kline_flush_interval: trading_core::timeutil::parse_duration(
                &args.kline_flush_interval,
            )?,
            kline_retain_bars,
            kline_retain_age,
            kline_history_limits: history_limits,
            kline_gap_scan_interval,
            kline_stall_threshold,
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

/// 解析 `--kline-retain-bars`：trim 后的正整数（≥1）；0/负数/非整数 fail-fast。
fn parse_retain_bars(raw: &str) -> anyhow::Result<usize> {
    let raw = raw.trim();
    let n: usize = raw
        .parse()
        .map_err(|_| anyhow::anyhow!("--kline-retain-bars 应为正整数，收到 `{raw}`"))?;
    if n == 0 {
        anyhow::bail!("--kline-retain-bars 必须 ≥1，收到 0");
    }
    Ok(n)
}

/// 解析 get_klines 的缺省/最大返回根数（None→内置缺省）；要求两者 ≥1 且 default ≤ max。
fn parse_history_limits(
    default_raw: Option<&str>,
    max_raw: Option<&str>,
) -> anyhow::Result<crate::kline_history::HistoryLimits> {
    use crate::kline_history::{HistoryLimits, DEFAULT_HISTORY_LIMIT, MAX_HISTORY_LIMIT};
    let parse = |raw: Option<&str>, builtin: usize, flag: &str| -> anyhow::Result<usize> {
        match raw {
            Some(s) => {
                let n: usize = s
                    .trim()
                    .parse()
                    .map_err(|_| anyhow::anyhow!("{flag} 应为正整数，收到 `{}`", s.trim()))?;
                if n == 0 {
                    anyhow::bail!("{flag} 必须 ≥1，收到 0");
                }
                Ok(n)
            }
            None => Ok(builtin),
        }
    };
    let default_limit = parse(
        default_raw,
        DEFAULT_HISTORY_LIMIT,
        "--kline-history-default-limit",
    )?;
    let max_limit = parse(max_raw, MAX_HISTORY_LIMIT, "--kline-history-max-limit")?;
    HistoryLimits::new(default_limit, max_limit).ok_or_else(|| {
        anyhow::anyhow!(
            "get_klines 限额非法：default({default_limit}) 必须 ≤ max({max_limit}) 且均 ≥1"
        )
    })
}

/// 解析缺口检测两时长：扫描周期（允许裸 "0"＝关闭周期扫描）与静默阈值（必须 >0）。
fn parse_gap_timings(scan_raw: &str, stall_raw: &str) -> anyhow::Result<(Duration, Duration)> {
    // parse_duration 不接受裸 0；扫描周期 0 是“关闭周期扫描”的合法开关，单独归一。
    let scan = if scan_raw.trim() == "0" {
        Duration::ZERO
    } else {
        trading_core::timeutil::parse_duration(scan_raw)?
    };
    let stall = trading_core::timeutil::parse_duration(stall_raw)?;
    // 静默阈值为 0 会把每根 bar 都判成“停推”，无意义；启动 fail-fast。
    if stall.is_zero() {
        anyhow::bail!("--kline-stall-threshold 必须 >0，收到 0");
    }
    Ok((scan, stall))
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

    #[test]
    fn kline_retention_args_default_to_1440_and_24h() {
        // 不给 flag 时缺省 1440 条 / 24h；时长走统一 parse_duration。
        let a = Args::try_parse_from(["svc"]).unwrap();
        assert_eq!(a.kline_retain_bars, "1440");
        assert_eq!(a.kline_retain_age, "24h");
        assert_eq!(
            trading_core::timeutil::parse_duration(&a.kline_retain_age).unwrap(),
            Duration::from_secs(24 * 60 * 60)
        );
        // 显式 flag 覆盖缺省。
        let a = Args::try_parse_from([
            "svc",
            "--kline-retain-bars",
            "600",
            "--kline-retain-age",
            "6h",
        ])
        .unwrap();
        assert_eq!(a.kline_retain_bars, "600");
        assert_eq!(a.kline_retain_age, "6h");
    }

    #[test]
    fn parse_retain_bars_accepts_positive_int_rejects_zero_and_garbage() {
        assert_eq!(parse_retain_bars("1440").unwrap(), 1440);
        assert_eq!(parse_retain_bars(" 100 ").unwrap(), 100);
        assert!(parse_retain_bars("0").is_err(), "0 无意义需 fail-fast");
        assert!(parse_retain_bars("-1").is_err());
        assert!(parse_retain_bars("abc").is_err());
    }

    #[test]
    fn invalid_retain_age_is_rejected() {
        assert!(trading_core::timeutil::parse_duration("nope").is_err());
    }

    #[test]
    fn history_limit_args_default_to_300_and_1500() {
        let a = Args::try_parse_from(["svc"]).unwrap();
        assert_eq!(a.kline_history_default_limit, "300");
        assert_eq!(a.kline_history_max_limit, "1500");
        let a = Args::try_parse_from([
            "svc",
            "--kline-history-default-limit",
            "120",
            "--kline-history-max-limit",
            "600",
        ])
        .unwrap();
        assert_eq!(a.kline_history_default_limit, "120");
        assert_eq!(a.kline_history_max_limit, "600");
    }

    #[test]
    fn parse_history_limits_validates_positive_and_ordering() {
        let ok = parse_history_limits(Some("300"), Some("1500")).unwrap();
        assert_eq!(ok.default_limit, 300);
        assert_eq!(ok.max_limit, 1500);
        // 缺省 → 内置 300/1500。
        let d = parse_history_limits(None, None).unwrap();
        assert_eq!((d.default_limit, d.max_limit), (300, 1500));
        // 0 / 非正 / 非整数 fail-fast。
        assert!(parse_history_limits(Some("0"), Some("1500")).is_err());
        assert!(parse_history_limits(Some("abc"), Some("1500")).is_err());
        assert!(
            parse_history_limits(Some("1501"), Some("1500")).is_err(),
            "default 不得大于 max"
        );
    }

    #[test]
    fn gap_timing_args_default_to_5m_scan_and_3m_stall() {
        let a = Args::try_parse_from(["svc"]).unwrap();
        assert_eq!(a.kline_gap_scan_interval, "5m");
        assert_eq!(a.kline_stall_threshold, "3m");
        let (scan, stall) =
            parse_gap_timings(&a.kline_gap_scan_interval, &a.kline_stall_threshold).unwrap();
        assert_eq!(scan, Duration::from_secs(300));
        assert_eq!(stall, Duration::from_secs(180));
    }

    #[test]
    fn gap_timings_allow_override_and_zero_scan_but_reject_zero_stall() {
        // 显式覆盖正常。
        let (scan, stall) = parse_gap_timings("1m", "2m").unwrap();
        assert_eq!(scan, Duration::from_secs(60));
        assert_eq!(stall, Duration::from_secs(120));
        // 扫描周期允许 0（关闭周期扫描，实时尾检仍在）。
        assert_eq!(parse_gap_timings("0", "3m").unwrap().0, Duration::ZERO);
        // 静默阈值 0 / 非法串 fail-fast。
        assert!(parse_gap_timings("5m", "0").is_err());
        assert!(parse_gap_timings("nope", "3m").is_err());
        assert!(parse_gap_timings("5m", "nope").is_err());
    }
}
