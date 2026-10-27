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

/// 加载后的全部 venue 配置（CtpConfig 含 String 非 Copy，整体退化为 Clone）。
#[derive(Debug, Clone)]
pub struct LoadedVenueConfigs {
    /// Deribit：漂移档数 + 平仓价差。
    pub deribit: PrepConfig,
    /// Hyperliquid：市价滑点。
    pub hyperliquid: HyperliquidConfig,
    /// Binance：配置基线 K线标的（启动即订阅并全量落盘）。
    pub binance: BinanceVenueConfig,
    /// CTP：.so 路径/流目录基准（feature="ctp" 时由 registry 消费；生产模式
    /// 开关已下沉账户级 credentials）。
    #[cfg(feature = "ctp")]
    pub ctp: trading_exchange::ctp::CtpConfig,
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
    /// CTP 段（feature="ctp" 时由 registry 消费；缺失回退 vendored 缺省）。
    #[serde(default)]
    #[cfg(feature = "ctp")]
    ctp: CtpSection,
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
}
