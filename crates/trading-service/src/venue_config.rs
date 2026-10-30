//! Venue 配置文件（可选 TOML，默认 `./config/venue.toml`）：
//! 承载各交易所的可调参数；文件/字段缺失时取默认值。
//!
//! ```toml
//! [deribit]
//! close_spread_max = "0.005"  # 平仓价差告警阈值（绝对价差，BTC）
//! open_drift_ticks = 5         # 市价开仓 mark±N tick
//! [hyperliquid]
//! slippage = "0.05"            # 市价 IOC 的滑点比率
//! ```

use std::path::Path;
use std::time::Duration;

use rust_decimal::Decimal;
use serde::Deserialize;
use trading_exchange::deribit::prep::PrepConfig;
use trading_exchange::hyperliquid::provider::HyperliquidConfig;

/// 默认平仓价差阈值（绝对价差，BTC）。
pub const DEFAULT_CLOSE_SPREAD_MAX: &str = "0.005";
/// 默认开仓漂移档数。
pub const DEFAULT_OPEN_DRIFT_TICKS: i64 = 5;
/// 默认 hyperliquid 市价滑点（5%）。
pub const DEFAULT_HYPERLIQUID_SLIPPAGE: &str = "0.05";

// —— K线历史回补缺省（[kline.backfill]）——
/// 自动回补轮询周期缺省（5 分钟）。
pub const DEFAULT_BACKFILL_INTERVAL: Duration = Duration::from_secs(300);
/// 单页根数缺省。
pub const DEFAULT_BACKFILL_PAGE_SIZE: usize = 1000;
/// 同所分页请求间隔缺省（500ms）。
pub const DEFAULT_BACKFILL_PAGE_DELAY: Duration = Duration::from_millis(500);
/// 每所并发上限缺省。
pub const DEFAULT_BACKFILL_CONCURRENCY: usize = 2;

/// 加载后的全部 venue 配置（CtpConfig 含 String 非 Copy，整体退化为 Clone）。
#[derive(Debug, Clone)]
pub struct LoadedVenueConfigs {
    /// Deribit：漂移档数 + 平仓价差。
    pub deribit: PrepConfig,
    /// Hyperliquid：市价滑点。
    pub hyperliquid: HyperliquidConfig,
    /// Binance：配置基线 K线标的（启动即订阅并全量落盘）。
    pub binance: BinanceVenueConfig,
    /// K线相关配置（历史缺失自动回补/手动补区间）。
    pub kline: KlineConfig,
    /// CTP：.so 路径/流目录基准（feature="ctp" 时由 registry 消费；生产模式
    /// 开关已下沉账户级 credentials）。
    #[cfg(feature = "ctp")]
    pub ctp: trading_exchange::ctp::CtpConfig,
}

/// K线配置段（当前仅历史回补）。
#[derive(Debug, Clone)]
pub struct KlineConfig {
    /// 历史缺失自动回补参数。
    pub backfill: KlineBackfillConfig,
}

/// `[kline.backfill]` 强类型配置（启动期解析+校验，非法 fail-fast）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KlineBackfillConfig {
    /// 自动回补轮询周期；`Duration::ZERO`（interval="0"）= 关闭后台自动回补，
    /// P5 管理面手动补仍可用。
    pub interval: Duration,
    /// 单次 REST 拉取的最大根数（三家按 1m，≤1000）。
    pub page_size: usize,
    /// 同所相邻两页请求间隔（限频节流；0=不 sleep）。
    pub page_delay: Duration,
    /// 单个交易所同时进行的回补任务上限。
    pub concurrency: usize,
}

impl Default for KlineBackfillConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_BACKFILL_INTERVAL,
            page_size: DEFAULT_BACKFILL_PAGE_SIZE,
            page_delay: DEFAULT_BACKFILL_PAGE_DELAY,
            concurrency: DEFAULT_BACKFILL_CONCURRENCY,
        }
    }
}

impl KlineBackfillConfig {
    /// 把 TOML section（字符串时长）解析为强类型并校验；非法值返回错误（启动 fail-fast）。
    fn from_section(s: &BackfillSection) -> anyhow::Result<Self> {
        // interval：裸 "0" 合法（关后台）；其余必须是 >0 的带单位时长。
        let interval = parse_optional_duration(&s.interval, DEFAULT_BACKFILL_INTERVAL, true)?;
        // page_delay：裸 "0" 也合法（不节流），其余 ≥0 带单位；缺省 500ms。
        let page_delay = parse_optional_duration(&s.page_delay, DEFAULT_BACKFILL_PAGE_DELAY, true)?;
        let page_size = s.page_size.unwrap_or(DEFAULT_BACKFILL_PAGE_SIZE);
        let concurrency = s.concurrency.unwrap_or(DEFAULT_BACKFILL_CONCURRENCY);
        if page_size < 1 {
            anyhow::bail!("[kline.backfill] page_size 必须 ≥1，实际 {page_size}");
        }
        if concurrency < 1 {
            anyhow::bail!("[kline.backfill] concurrency 必须 ≥1，实际 {concurrency}");
        }
        Ok(Self {
            interval,
            page_size,
            page_delay,
            concurrency,
        })
    }
}

/// 解析可选时长字段：缺省取 `default`；裸 `"0"` 在 `allow_zero` 时映射为 ZERO；
/// 其余交给 [`trading_core::timeutil::parse_duration`]（要求带单位且 >0）。
fn parse_optional_duration(
    raw: &Option<String>,
    default: Duration,
    allow_zero: bool,
) -> anyhow::Result<Duration> {
    let Some(raw) = raw else {
        return Ok(default);
    };
    let raw = raw.trim();
    if allow_zero && raw == "0" {
        return Ok(Duration::ZERO);
    }
    trading_core::timeutil::parse_duration(raw)
}

/// Binance 配置段：当前仅承载配置基线 K线标的。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BinanceVenueConfig {
    /// 配置基线 1m K线合约（venue.toml 原值；构造基线时归一为币安原生大写口径）。
    /// 启动即向合约行情流订阅这些标的并全量落盘，与南向客户端是否在线无关。
    pub kline_symbols: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct DeribitSection {
    #[serde(default = "default_spread")]
    close_spread_max: Decimal,
    #[serde(default = "default_drift")]
    open_drift_ticks: i64,
}

impl Default for DeribitSection {
    fn default() -> Self {
        Self {
            close_spread_max: default_spread(),
            open_drift_ticks: default_drift(),
        }
    }
}

/// hyperliquid 配置段：仅滑点；缺省取默认。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct HyperliquidSection {
    #[serde(default = "default_slippage")]
    slippage: Decimal,
}

impl Default for HyperliquidSection {
    fn default() -> Self {
        Self {
            slippage: default_slippage(),
        }
    }
}

fn default_spread() -> Decimal {
    DEFAULT_CLOSE_SPREAD_MAX.parse().unwrap()
}
fn default_drift() -> i64 {
    DEFAULT_OPEN_DRIFT_TICKS
}
fn default_slippage() -> Decimal {
    DEFAULT_HYPERLIQUID_SLIPPAGE.parse().unwrap()
}

/// 由 venue 配置构造 K线配置基线（当前仅 binance 合约作用域）。
///
/// binance 标的一律大写归一为原生口径（与 provider `normalize_instrument` 一致），
/// trim/去重/排序由 [`crate::kline::BaselineKlines::new`] 完成；空配置不产生基线。
pub fn kline_baselines(cfg: &LoadedVenueConfigs) -> Vec<crate::kline::BaselineKlines> {
    let symbols: Vec<String> = cfg
        .binance
        .kline_symbols
        .iter()
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    if symbols.is_empty() {
        return Vec::new();
    }
    vec![crate::kline::BaselineKlines::new(
        "binance",
        trading_models::InstrumentType::Futures,
        symbols,
    )]
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
struct VenueFile {
    #[serde(default)]
    deribit: DeribitSection,
    #[serde(default)]
    hyperliquid: HyperliquidSection,
    /// Binance 段（配置基线 K线标的；缺失＝无基线）。
    #[serde(default)]
    binance: BinanceSection,
    /// K线段（历史回补）。
    #[serde(default)]
    kline: KlineSection,
    /// CTP 段（feature="ctp" 时由 registry 消费；缺失回退 vendored 缺省）。
    #[serde(default)]
    #[cfg(feature = "ctp")]
    ctp: CtpSection,
}

/// `[kline]` 段（当前仅含 backfill 子表）。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
struct KlineSection {
    #[serde(default)]
    backfill: BackfillSection,
}

/// `[kline.backfill]` 原始 TOML（时长保持字符串，在 load() 中解析为 Duration）。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
struct BackfillSection {
    /// 自动回补轮询周期；"0"=关闭（缺省 5m）。
    interval: Option<String>,
    /// 单页根数（缺省 1000）。
    page_size: Option<usize>,
    /// 同所分页间隔（缺省 500ms；"0"=不节流）。
    page_delay: Option<String>,
    /// 每所并发上限（缺省 2）。
    concurrency: Option<usize>,
}

/// binance 配置段：仅配置基线 K线标的；全部可选，缺省为空。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
struct BinanceSection {
    /// 启动即订阅的 1m K线合约（建议币安原生大写，如 ["BTCUSDT"]；
    /// 小写也兼容，构造基线时统一大写归一）。
    #[serde(default)]
    kline_symbols: Vec<String>,
}

/// CTP 配置段：.so 路径/流目录/生产模式；全部可选。
#[cfg(feature = "ctp")]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
struct CtpSection {
    trader_so: Option<String>,
    md_so: Option<String>,
    flow_path: Option<String>,
    md_front: Option<String>,
    md_account: Option<String>,
    md_anon: Option<bool>,
    md_pass_is_authcode: Option<bool>,
    /// SHFE 市价开仓穿价漂移档数（缺省用 trading-exchange 内置默认 1）。
    shfe_market_drift_ticks: Option<i64>,
    /// 冷启动预设主力 `品种:合约` 列表（如 ["cu:cu2512"]）；缺省不预设。
    preset_dominants: Option<Vec<String>>,
    /// 配置基线 K线品种（如 ["cu","rb"]），默认就采集；缺省仅按客户端动态订阅。
    kline_products: Option<Vec<String>>,
    /// 冷启动合约规格种子 CSV 路径；缺省由 main 按 data-dir 补
    /// `<data>/ctp-specs/latest/instruments.csv`。
    instrument_specs_path: Option<String>,
}

/// 加载 venue 配置：文件不存在 → 全默认；存在则按字段补默认。
pub fn load(path: &Path) -> anyhow::Result<LoadedVenueConfigs> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(default_configs()),
        Err(e) => return Err(anyhow::anyhow!("读取 venue 配置失败 {path:?}: {e}")),
    };
    let parsed: VenueFile =
        toml::from_str(&text).map_err(|e| anyhow::anyhow!("解析 venue 配置 {path:?}: {e}"))?;
    // 回补配置非法（坏时长/越界）在启动期 fail-fast。
    let backfill = KlineBackfillConfig::from_section(&parsed.kline.backfill)
        .map_err(|e| anyhow::anyhow!("解析 venue 配置 {path:?}: {e}"))?;
    Ok(LoadedVenueConfigs {
        deribit: PrepConfig {
            drift_ticks: parsed.deribit.open_drift_ticks,
            spread_threshold: parsed.deribit.close_spread_max,
        },
        hyperliquid: HyperliquidConfig {
            slippage: parsed.hyperliquid.slippage,
        },
        binance: BinanceVenueConfig {
            kline_symbols: parsed.binance.kline_symbols,
        },
        kline: KlineConfig { backfill },
        #[cfg(feature = "ctp")]
        ctp: trading_exchange::ctp::from_values(
            parsed.ctp.trader_so,
            parsed.ctp.md_so,
            parsed.ctp.flow_path,
            parsed.ctp.md_front,
            parsed.ctp.md_account,
            parsed.ctp.md_anon,
            parsed.ctp.md_pass_is_authcode,
            parsed.ctp.shfe_market_drift_ticks,
            parsed.ctp.preset_dominants,
            parsed.ctp.kline_products,
            parsed
                .ctp
                .instrument_specs_path
                .map(std::path::PathBuf::from),
        ),
    })
}

/// 默认配置（文件缺失时）。
pub fn default_configs() -> LoadedVenueConfigs {
    LoadedVenueConfigs {
        deribit: PrepConfig {
            drift_ticks: DEFAULT_OPEN_DRIFT_TICKS,
            spread_threshold: default_spread(),
        },
        hyperliquid: HyperliquidConfig {
            slippage: default_slippage(),
        },
        binance: BinanceVenueConfig::default(),
        kline: KlineConfig {
            backfill: KlineBackfillConfig::default(),
        },
        #[cfg(feature = "ctp")]
        ctp: trading_exchange::ctp::from_values(
            None, None, None, None, None, None, None, None, None, None, None,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_yields_defaults() {
        let cfg = load(Path::new("/nonexistent/venue.toml")).expect("ok");
        assert_eq!(cfg.deribit, default_configs().deribit);
        assert_eq!(cfg.deribit.drift_ticks, 5);
        assert_eq!(cfg.deribit.spread_threshold, "0.005".parse().unwrap());
        // hyperliquid 缺省 0.05。
        assert_eq!(cfg.hyperliquid.slippage, "0.05".parse().unwrap());
    }

    #[test]
    fn parses_hyperliquid_slippage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("venue.toml");
        std::fs::write(&path, "[hyperliquid]\nslippage = \"0.02\"\n").unwrap();
        let cfg = load(&path).expect("ok");
        assert_eq!(cfg.hyperliquid.slippage, "0.02".parse().unwrap());
        // deribit 段缺失仍取默认。
        assert_eq!(cfg.deribit.drift_ticks, 5);
    }

    #[test]
    fn parses_full_sections() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("venue.toml");
        std::fs::write(
            &path,
            "[deribit]\nclose_spread_max = \"0.01\"\nopen_drift_ticks = 8\n[hyperliquid]\nslippage = \"0.1\"\n",
        )
        .unwrap();
        let cfg = load(&path).expect("ok");
        assert_eq!(cfg.deribit.drift_ticks, 8);
        assert_eq!(cfg.deribit.spread_threshold, "0.01".parse().unwrap());
        assert_eq!(cfg.hyperliquid.slippage, "0.1".parse().unwrap());
    }

    #[cfg(feature = "ctp")]
    #[test]
    fn parses_ctp_shfe_drift_ticks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("venue.toml");
        std::fs::write(
            &path,
            "[ctp]\nmd_front = \"tcp://x:1\"\nshfe_market_drift_ticks = 4\n",
        )
        .unwrap();
        let cfg = load(&path).expect("ok");
        assert_eq!(cfg.ctp.shfe_market_drift_ticks, 4);
        // 其他字段缺省仍成立。
        assert_eq!(cfg.ctp.md_front, "tcp://x:1");
    }

    #[cfg(feature = "ctp")]
    #[test]
    fn ctp_drift_defaults_to_one_when_absent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("venue.toml");
        std::fs::write(&path, "[ctp]\n").unwrap();
        let cfg = load(&path).expect("ok");
        assert_eq!(
            cfg.ctp.shfe_market_drift_ticks,
            trading_exchange::ctp::DEFAULT_SHFE_MARKET_DRIFT_TICKS
        );
    }

    #[test]
    fn empty_file_uses_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("venue.toml");
        std::fs::write(&path, "").unwrap();
        let cfg = load(&path).expect("ok");
        assert_eq!(cfg.deribit, default_configs().deribit);
        assert_eq!(cfg.hyperliquid, default_configs().hyperliquid);
        // binance 段缺失：基线 K线为空。
        assert!(cfg.binance.kline_symbols.is_empty());
    }

    #[test]
    fn parses_binance_kline_symbols_and_builds_baselines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("venue.toml");
        std::fs::write(
            &path,
            "[binance]\nkline_symbols = [\"btcusdt\", \"ETHUSDT\", \"btcusdt\"]\n",
        )
        .unwrap();
        let cfg = load(&path).expect("ok");
        // 配置值原样保留（不在解析层去重/改大小写）。
        assert_eq!(
            cfg.binance.kline_symbols,
            vec!["btcusdt", "ETHUSDT", "btcusdt"]
        );

        // 基线构造：归一为币安原生大写口径，去重、字典序。
        let baselines = kline_baselines(&cfg);
        assert_eq!(baselines.len(), 1);
        assert_eq!(baselines[0].venue, "binance");
        assert_eq!(baselines[0].itype, trading_models::InstrumentType::Futures);
        assert_eq!(
            baselines[0].symbols,
            vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
        );
    }

    #[test]
    fn binance_empty_kline_symbols_yields_no_baseline() {
        // 显式空表/缺字段：不产生基线作用域。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("venue.toml");
        std::fs::write(&path, "[binance]\nkline_symbols = []\n").unwrap();
        assert!(kline_baselines(&load(&path).expect("ok")).is_empty());

        assert!(kline_baselines(&default_configs()).is_empty());
    }

    #[test]
    fn bad_toml_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("venue.toml");
        std::fs::write(&path, "garbage =").unwrap();
        assert!(load(&path).is_err());
    }

    // ---- [kline.backfill] ----

    #[test]
    fn backfill_defaults_when_section_absent() {
        let cfg = default_configs();
        let b = cfg.kline.backfill;
        assert_eq!(b.interval, std::time::Duration::from_secs(300));
        assert_eq!(b.page_size, 1000);
        assert_eq!(b.page_delay, std::time::Duration::from_millis(500));
        assert_eq!(b.concurrency, 2);
        // 文件缺失同样取默认。
        let loaded = load(Path::new("/nonexistent/venue.toml")).unwrap();
        assert_eq!(loaded.kline.backfill.interval, b.interval);
    }

    #[test]
    fn parses_backfill_section() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("venue.toml");
        std::fs::write(
            &path,
            "[kline.backfill]\ninterval = \"1m\"\npage_size = 250\n\
             page_delay = \"20ms\"\nconcurrency = 4\n",
        )
        .unwrap();
        let b = load(&path).unwrap().kline.backfill;
        assert_eq!(b.interval, std::time::Duration::from_secs(60));
        assert_eq!(b.page_size, 250);
        assert_eq!(b.page_delay, std::time::Duration::from_millis(20));
        assert_eq!(b.concurrency, 4);
    }

    #[test]
    fn backfill_interval_zero_is_valid_and_disables_worker() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("venue.toml");
        std::fs::write(&path, "[kline.backfill]\ninterval = \"0\"\n").unwrap();
        let b = load(&path).unwrap().kline.backfill;
        assert_eq!(b.interval, std::time::Duration::ZERO);
    }

    #[test]
    fn backfill_invalid_values_fail_fast() {
        let dir = tempfile::tempdir().unwrap();
        // page_size 必须 ≥1。
        let p = dir.path().join("a.toml");
        std::fs::write(&p, "[kline.backfill]\npage_size = 0\n").unwrap();
        assert!(load(&p).is_err());
        // concurrency 必须 ≥1。
        let p = dir.path().join("b.toml");
        std::fs::write(&p, "[kline.backfill]\nconcurrency = 0\n").unwrap();
        assert!(load(&p).is_err());
        // 非法时长。
        let p = dir.path().join("c.toml");
        std::fs::write(&p, "[kline.backfill]\ninterval = \"abc\"\n").unwrap();
        assert!(load(&p).is_err());
        // "0s" 非法（关后台只能写无单位的 "0"），避免与正时长口径混淆。
        let p = dir.path().join("d.toml");
        std::fs::write(&p, "[kline.backfill]\ninterval = \"0s\"\n").unwrap();
        assert!(load(&p).is_err());
    }
}
