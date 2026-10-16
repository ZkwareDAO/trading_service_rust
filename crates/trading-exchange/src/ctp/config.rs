//! CTP 运行参数（.so 路径/流目录基准），来自 `config/venue.toml` 的 `[ctp]` 段。

use std::path::Path;

/// CTP 运行参数（纯部署资源：.so 路径与流目录基准）。
///
/// 前置环境开关（`is_production_mode`）不在此列：评测版合并 API（v6.7.11+）
/// 的该参数是**实例级**且创建后不可变，归账户所有（credentials JSON 的
/// `CtpCreds.is_production_mode`，随 [`crate::ctp::core::account::CtpAccount`]）。
#[derive(Debug, Clone)]
pub struct CtpConfig {
    /// 柜台交易 API 动态库路径（vendored 缺省或配置覆盖）。
    pub trader_so: String,
    /// 行情 API 动态库路径。
    pub md_so: String,
    /// 流文件目录基准（会话按账户名建子目录隔离各实例 con 文件）。
    pub flow_path: String,
    /// 行情前置地址（**部署级**：与交易前置端口分离，如瑞达 42205/42213）。
    /// 空 = 不启动行情线程。
    pub md_front: String,
    /// 行情登录用账户名（account.csv 中的 name；交易所级行情不随用户区分，
    /// 取一个账户的凭证结构登录即可）。None = 不启动行情线程。
    pub md_account: Option<String>,
    /// 行情登录形态：匿名（BrokerID/UserID/Password 全空，标准 CTP 行情前置形态）。
    pub md_anon: bool,
    /// 行情登录形态：Password 字段填认证码（部分柜台行情鉴权形态）。
    pub md_pass_is_authcode: bool,
    /// SHFE 市价单转穿价限价时，开仓价相对对手价向外漂移的最小变动价位档数
    /// （买开=ask+n·tick、卖开=bid−n·tick）；**CTP 独立配置，与 deribit 漂移无关**。
    /// 平仓不漂移（取 best bid/ask）。默认 1；0 = 纯对手价。
    pub shfe_market_drift_ticks: i64,
}

/// SHFE 市价转限价漂移档数缺省值（穿 1 个最小变动价位提成交确定性）。
pub const DEFAULT_SHFE_MARKET_DRIFT_TICKS: i64 = 1;

impl CtpConfig {
    /// vendored 缺省：仓库内 v6.7.13 linux64 动态库 + 系统临时流目录。
    pub fn vendored() -> Self {
        Self {
            trader_so: vendored_so("thosttraderapi_se.so"),
            md_so: vendored_so("thostmduserapi_se.so"),
            flow_path: std::env::temp_dir()
                .join("ctp-flow")
                .to_string_lossy()
                .to_string(),
            // 柜台地址不属于 vendored 缺省：未在 venue.toml 配置 = 行情不启动。
            md_front: String::new(),
            md_account: None,
            md_anon: false,
            md_pass_is_authcode: false,
            shfe_market_drift_ticks: DEFAULT_SHFE_MARKET_DRIFT_TICKS,
        }
    }

    /// 覆盖 SHFE 市价转限价的漂移档数（builder 风格，0=纯对手价）。
    pub fn with_shfe_market_drift_ticks(mut self, ticks: i64) -> Self {
        self.shfe_market_drift_ticks = ticks.max(0);
        self
    }

    /// 测试用：不存在的库路径（只跑纯逻辑路径，不触碰 FFI）。
    pub fn for_tests() -> Self {
        Self {
            trader_so: "/nonexistent/libthosttraderapi.so".into(),
            md_so: "/nonexistent/libthostmduserapi.so".into(),
            flow_path: "/tmp/ctp-flow-test".into(),
            md_front: "tcp://127.0.0.1:1".into(),
            md_account: Some("test".into()),
            md_anon: false,
            md_pass_is_authcode: false,
            shfe_market_drift_ticks: DEFAULT_SHFE_MARKET_DRIFT_TICKS,
        }
    }
}

/// 定位 vendored 的柜台 .so（相对 CARGO_MANIFEST_DIR 上溯到仓库根）。
fn vendored_so(name: &str) -> String {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    // trading-exchange 的 manifest 在 crates/ 下，仓库根在其上级。
    let repo_root = manifest
        .parent()
        .and_then(|p| p.parent())
        .unwrap_or(manifest);
    repo_root
        .join(
            "third_party/ctp2rs/ctp-dyn/api/ctp/v6.7.13/v6.7.13_20260225_api_traderapi_se_linux64",
        )
        .join(name)
        .to_string_lossy()
        .to_string()
}

/// 从 `venue.toml` 的 `[ctp]` 段值构造；缺省回退 vendored。
///
/// 由宿主（trading-service 的 venue_config）调用；本模块只提供从
/// TOML 值构造的入口，保持 trading-exchange 不依赖 toml crate。
#[allow(clippy::too_many_arguments)]
pub fn from_values(
    trader_so: Option<String>,
    md_so: Option<String>,
    flow_path: Option<String>,
    md_front: Option<String>,
    md_account: Option<String>,
    md_anon: Option<bool>,
    md_pass_is_authcode: Option<bool>,
    // SHFE 市价开仓穿价漂移档数（None=用内置默认 1）。
    shfe_market_drift_ticks: Option<i64>,
) -> CtpConfig {
    let base = CtpConfig::vendored();
    CtpConfig {
        trader_so: trader_so.unwrap_or(base.trader_so),
        md_so: md_so.unwrap_or(base.md_so),
        flow_path: flow_path.unwrap_or(base.flow_path),
        md_front: md_front.unwrap_or(base.md_front),
        md_account: md_account.or(base.md_account),
        md_anon: md_anon.unwrap_or(base.md_anon),
        md_pass_is_authcode: md_pass_is_authcode.unwrap_or(base.md_pass_is_authcode),
        // SHFE 市价穿价漂移档数：None 取 vendored 默认（1 档）。
        shfe_market_drift_ticks: shfe_market_drift_ticks
            .unwrap_or(base.shfe_market_drift_ticks)
            .max(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_values_overrides_partial() {
        // Arrange / Act：覆盖流目录/行情前置/行情账户，其余用 vendored 缺省。
        // （is_production_mode 已下沉账户级 credentials，不再属进程配置。）
        let cfg = from_values(
            None,
            None,
            Some("/tmp/ctp-flow-custom".into()),
            Some("tcp://101.226.250.133:42213".into()),
            Some("ctp-simnow".into()),
            None,
            None,
            None,
        );
        // Assert。
        assert!(cfg.trader_so.contains("thosttraderapi_se.so"));
        assert!(cfg.md_so.contains("thostmduserapi_se.so"));
        assert_eq!(cfg.flow_path, "/tmp/ctp-flow-custom");
        assert_eq!(cfg.md_front, "tcp://101.226.250.133:42213");
        assert_eq!(cfg.md_account.as_deref(), Some("ctp-simnow"));
    }

    #[test]
    fn md_front_empty_means_market_data_disabled() {
        // Arrange：未配置行情前置（部署未启用行情）。
        let cfg = from_values(
            None,
            None,
            None,
            Some(String::new()),
            None,
            None,
            None,
            None,
        );
        // Assert：md_front 为空 = 行情线程不启动的信号。
        assert!(cfg.md_front.is_empty());
        assert!(cfg.md_account.is_none());
        assert!(!cfg.md_anon);
        assert!(!cfg.md_pass_is_authcode);
    }

    #[test]
    fn md_login_mode_is_configurable() {
        // Arrange / Act：匿名行情 + 密码=授权码两种柜台鉴权形态均可配置。
        let anon = from_values(None, None, None, None, None, Some(true), None, None);
        let auth = from_values(None, None, None, None, None, None, Some(true), None);
        // Assert。
        assert!(anon.md_anon);
        assert!(!anon.md_pass_is_authcode);
        assert!(!auth.md_anon);
        assert!(auth.md_pass_is_authcode);
    }

    #[test]
    fn shfe_drift_is_configurable_and_defaults_to_one() {
        // 透传配置值（含 0=纯对手价）。
        let cfg = from_values(None, None, None, None, None, None, None, Some(3));
        assert_eq!(cfg.shfe_market_drift_ticks, 3);
        let zero = from_values(None, None, None, None, None, None, None, Some(0));
        assert_eq!(zero.shfe_market_drift_ticks, 0);
        // None 取内置默认；负值钳 0。
        let dflt = from_values(None, None, None, None, None, None, None, None);
        assert_eq!(
            dflt.shfe_market_drift_ticks,
            DEFAULT_SHFE_MARKET_DRIFT_TICKS
        );
        let neg = from_values(None, None, None, None, None, None, None, Some(-2));
        assert_eq!(neg.shfe_market_drift_ticks, 0);
    }

    #[test]
    fn for_tests_points_to_nonexistent_lib() {
        let cfg = CtpConfig::for_tests();
        assert!(!Path::new(&cfg.trader_so).exists());
    }
}
