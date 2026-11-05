//! CTP 运行参数（.so 路径/流目录基准），来自 `config/venue.toml` 的 `[ctp]` 段。

use std::path::Path;

use crate::ctp::dominant::DominantMetric;
use crate::ctp::prep::ShfeClosePriority;

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
    /// SHFE 平仓腿先后顺序：`today_first`（默认，先平今）/ `yesterday_first`
    /// （旧行为，先平昨）。两腿的实际 `ReqOrderInsert` 调用顺序即由此决定。
    pub shfe_close_priority: ShfeClosePriority,
    /// 哪些交易所的市价单必须转对手价限价（这些交易所不支持 CTP `AnyPrice`）。
    /// 默认 `["SHFE"]`；CFFEX 不支持 AnyPrice，需显式加入。
    pub market_limit_exchanges: Vec<String>,
    /// 哪些交易所需要显式拆平今/平昨（复用 SHFE 平仓路由逻辑）。
    /// 默认 `["SHFE"]`；CFFEX 不需要；INE 真机验证后可加入。
    pub close_today_yesterday_exchanges: Vec<String>,
    /// 哪些交易所按合约单笔上限拆单。默认空 = 完全不拆（现有行为不变）。
    pub split_max_volume_exchanges: Vec<String>,
    /// 主力选举指标：`volume`=当日累计成交量 / `open_interest`=当日持仓量（默认）。
    pub main_contract_criteria: DominantMetric,
    /// 主力切换确认天数：同一候选连续 N 个有数据交易日胜出才切换（默认 3；1=当天生效）。
    pub main_contract_confirm_days: u32,
    /// 冷启动预设主力：`品种:主力合约` 列表，如 `["cu:cu2512"]`。
    /// 进程启动时直接写入主力表（trading_day 取当日），使**盘中重启/未到 15:00
    /// 选举窗口**时品种订阅（cu）也能立即只推该合约；当日收盘选举对该品种跳过
    /// （视为当日已选），下一交易日由真实成交量选举接管。空 = 不预设。
    pub preset_dominants: Vec<String>,
    /// 默认采集的 K线品种（配置基线兴趣集），如 `["cu","rb"]`。这些品种在服务
    /// 启动后即展开为全部在交易合约采集（无需任何客户端订阅），与南向客户端运行时
    /// 订阅取并集；客户端退订不会移除本基线品种。空 = 仅按客户端动态订阅采集。
    pub kline_products: Vec<String>,
    /// 冷启动合约规格种子 CSV 路径（`<data>/ctp-specs/latest/instruments.csv`）。
    /// 进程启动时若文件存在，读入 InstrumentDir（仅在交易期货），使品种订阅
    /// （cu）在交易侧在线合约查询返回前即可展开；在线查询结果后到覆盖。
    /// None/文件缺失 = 不读种子，纯靠在线预热。exchange 只读、不写此文件。
    pub instrument_specs_path: Option<std::path::PathBuf>,
}

/// SHFE 市价转限价漂移档数缺省值（穿 1 个最小变动价位提成交确定性）。
pub const DEFAULT_SHFE_MARKET_DRIFT_TICKS: i64 = 1;

/// 主力切换确认天数缺省值（连续 3 个有数据交易日胜出才切换）。
pub const DEFAULT_MAIN_CONTRACT_CONFIRM_DAYS: u32 = 3;

/// 默认需要“市价转限价”的交易所（SHFE/INE 柜台不支持 CTP `AnyPrice`）。
pub const DEFAULT_MARKET_LIMIT_EXCHANGES: &[&str] = &["SHFE"];

/// 默认需要显式拆平今/平昨的交易所。
pub const DEFAULT_CLOSE_TODAY_YESTERDAY_EXCHANGES: &[&str] = &["SHFE"];

/// 交易所代码归一：trim + ASCII 大写 + 去空 + 去重（保持首次出现顺序）。
pub fn normalize_exchange_codes(codes: Vec<String>) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(codes.len());
    for code in codes {
        let code = code.trim().to_ascii_uppercase();
        if !code.is_empty() && !out.iter().any(|c| c == &code) {
            out.push(code);
        }
    }
    out
}

/// 交易所代码是否在配置集合内（双方 trim + ASCII 大小写不敏感）。
///
/// 配置加载时已归一化，但规则/API 路径也可能直接构造集合（测试、热加载），
/// 这里统一兜底，避免各处重复实现容错口径。
pub fn exchange_in_set(exchange: &str, set: &[String]) -> bool {
    let exchange = exchange.trim();
    set.iter().any(|e| e.trim().eq_ignore_ascii_case(exchange))
}

/// 常量列表 → `Vec<String>` 缺省值。
fn default_exchanges(codes: &[&str]) -> Vec<String> {
    codes.iter().map(|s| (*s).to_string()).collect()
}

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
            shfe_close_priority: ShfeClosePriority::default(),
            market_limit_exchanges: default_exchanges(DEFAULT_MARKET_LIMIT_EXCHANGES),
            close_today_yesterday_exchanges: default_exchanges(
                DEFAULT_CLOSE_TODAY_YESTERDAY_EXCHANGES,
            ),
            split_max_volume_exchanges: Vec::new(),
            main_contract_criteria: DominantMetric::default(),
            main_contract_confirm_days: DEFAULT_MAIN_CONTRACT_CONFIRM_DAYS,
            preset_dominants: Vec::new(),
            kline_products: Vec::new(),
            instrument_specs_path: None,
        }
    }

    /// 覆盖 SHFE 市价转限价的漂移档数（builder 风格，0=纯对手价）。
    pub fn with_shfe_market_drift_ticks(mut self, ticks: i64) -> Self {
        self.shfe_market_drift_ticks = ticks.max(0);
        self
    }

    /// 覆盖 SHFE 平仓腿顺序（builder 风格）。
    pub fn with_shfe_close_priority(mut self, priority: ShfeClosePriority) -> Self {
        self.shfe_close_priority = priority;
        self
    }

    /// 覆盖“市价转限价”交易所集合（builder 风格；自动归一）。
    pub fn with_market_limit_exchanges(mut self, exchanges: Vec<String>) -> Self {
        self.market_limit_exchanges = normalize_exchange_codes(exchanges);
        self
    }

    /// 覆盖“平今/平昨”交易所集合（builder 风格；自动归一）。
    pub fn with_close_today_yesterday_exchanges(mut self, exchanges: Vec<String>) -> Self {
        self.close_today_yesterday_exchanges = normalize_exchange_codes(exchanges);
        self
    }

    /// 覆盖“按单笔上限拆单”交易所集合（builder 风格；自动归一）。
    pub fn with_split_max_volume_exchanges(mut self, exchanges: Vec<String>) -> Self {
        self.split_max_volume_exchanges = normalize_exchange_codes(exchanges);
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
            shfe_close_priority: ShfeClosePriority::default(),
            market_limit_exchanges: default_exchanges(DEFAULT_MARKET_LIMIT_EXCHANGES),
            close_today_yesterday_exchanges: default_exchanges(
                DEFAULT_CLOSE_TODAY_YESTERDAY_EXCHANGES,
            ),
            split_max_volume_exchanges: Vec::new(),
            main_contract_criteria: DominantMetric::default(),
            main_contract_confirm_days: DEFAULT_MAIN_CONTRACT_CONFIRM_DAYS,
            preset_dominants: Vec::new(),
            kline_products: Vec::new(),
            instrument_specs_path: None,
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
    // SHFE 平仓腿顺序（None=默认先平今）。
    shfe_close_priority: Option<ShfeClosePriority>,
    // 主力选举指标（None=默认持仓量）。
    main_contract_criteria: Option<DominantMetric>,
    // 主力切换确认天数（None=默认 3；<1 由配置层 fail-fast）。
    main_contract_confirm_days: Option<u32>,
    // 冷启动预设主力 `品种:合约` 列表（None=不预设）。
    preset_dominants: Option<Vec<String>>,
    // 配置基线 K线品种列表（None=空，仅按客户端动态订阅采集）。
    kline_products: Option<Vec<String>>,
    // 冷启动合约规格种子 CSV 路径（None=不读种子）。
    instrument_specs_path: Option<std::path::PathBuf>,
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
        shfe_close_priority: shfe_close_priority.unwrap_or(base.shfe_close_priority),
        market_limit_exchanges: base.market_limit_exchanges,
        close_today_yesterday_exchanges: base.close_today_yesterday_exchanges,
        split_max_volume_exchanges: base.split_max_volume_exchanges,
        main_contract_criteria: main_contract_criteria.unwrap_or(base.main_contract_criteria),
        main_contract_confirm_days: main_contract_confirm_days
            .unwrap_or(base.main_contract_confirm_days)
            .max(1),
        preset_dominants: preset_dominants.unwrap_or(base.preset_dominants),
        kline_products: kline_products.unwrap_or(base.kline_products),
        instrument_specs_path,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 构造全部 None（vendored 缺省）的便捷入口。
    fn all_none() -> CtpConfig {
        from_values(
            None, None, None, None, None, None, None, None, None, None, None, None, None, None,
        )
    }

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
            None,
            None,
            None,
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
            None,
            None,
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
        let anon = from_values(
            None,
            None,
            None,
            None,
            None,
            Some(true),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let auth = from_values(
            None,
            None,
            None,
            None,
            None,
            None,
            Some(true),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        // Assert。
        assert!(anon.md_anon);
        assert!(!anon.md_pass_is_authcode);
        assert!(!auth.md_anon);
        assert!(auth.md_pass_is_authcode);
    }

    #[test]
    fn shfe_drift_is_configurable_and_defaults_to_one() {
        // 透传配置值（含 0=纯对手价）。
        let cfg = from_values(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(3),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(cfg.shfe_market_drift_ticks, 3);
        let zero = from_values(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(0),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(zero.shfe_market_drift_ticks, 0);
        // None 取内置默认；负值钳 0。
        let dflt = all_none();
        assert_eq!(
            dflt.shfe_market_drift_ticks,
            DEFAULT_SHFE_MARKET_DRIFT_TICKS
        );
        let neg = from_values(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(-2),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(neg.shfe_market_drift_ticks, 0);
    }

    #[test]
    fn shfe_close_priority_is_configurable_and_defaults_today_first() {
        // 缺省先平今（新默认）。
        assert_eq!(
            all_none().shfe_close_priority,
            ShfeClosePriority::TodayFirst
        );
        // 显式切回先平昨（旧行为）。
        let yd = from_values(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(ShfeClosePriority::YesterdayFirst),
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(yd.shfe_close_priority, ShfeClosePriority::YesterdayFirst);
        // builder 覆盖。
        assert_eq!(
            CtpConfig::for_tests()
                .with_shfe_close_priority(ShfeClosePriority::YesterdayFirst)
                .shfe_close_priority,
            ShfeClosePriority::YesterdayFirst
        );
    }

    #[test]
    fn exchange_sets_are_configurable_and_default_to_shfe() {
        // 默认：市价转限价/平今平昨均为 ["SHFE"]，拆单为空。
        let dflt = all_none();
        assert_eq!(dflt.market_limit_exchanges, vec!["SHFE".to_string()]);
        assert_eq!(
            dflt.close_today_yesterday_exchanges,
            vec!["SHFE".to_string()]
        );
        assert!(dflt.split_max_volume_exchanges.is_empty());

        // 归一：trim/大写/去空/去重；CFFEX 只加入市价转限价与拆单。
        let cfg = CtpConfig::for_tests()
            .with_market_limit_exchanges(vec![
                " shfe ".into(),
                "cffex".into(),
                "CFFEX".into(),
                "".into(),
            ])
            .with_close_today_yesterday_exchanges(vec!["SHFE".into()])
            .with_split_max_volume_exchanges(vec!["CFFEX".into()]);
        assert_eq!(
            cfg.market_limit_exchanges,
            vec!["SHFE".to_string(), "CFFEX".to_string()]
        );
        assert_eq!(
            cfg.close_today_yesterday_exchanges,
            vec!["SHFE".to_string()]
        );
        assert_eq!(cfg.split_max_volume_exchanges, vec!["CFFEX".to_string()]);
    }

    #[test]
    fn main_contract_selection_is_configurable_and_defaults() {
        // 默认：持仓量 + 3 日确认。
        let dflt = all_none();
        assert_eq!(dflt.main_contract_criteria, DominantMetric::OpenInterest);
        assert_eq!(
            dflt.main_contract_confirm_days,
            DEFAULT_MAIN_CONTRACT_CONFIRM_DAYS
        );
        // 显式覆盖为成交量 + 1 日确认（= 当天生效）。
        let cfg = from_values(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(DominantMetric::Volume),
            Some(1),
            None,
            None,
            None,
        );
        assert_eq!(cfg.main_contract_criteria, DominantMetric::Volume);
        assert_eq!(cfg.main_contract_confirm_days, 1);
        // 防御性钳制：0 不得使选举永不确认。
        let zero = from_values(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(0),
            None,
            None,
            None,
        );
        assert_eq!(zero.main_contract_confirm_days, 1);
    }

    #[test]
    fn for_tests_points_to_nonexistent_lib() {
        let cfg = CtpConfig::for_tests();
        assert!(!Path::new(&cfg.trader_so).exists());
    }
}
