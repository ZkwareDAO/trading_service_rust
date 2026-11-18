//! 接口测试公共装配与 HTTP 工具（最小实现，不复刻 e2e 的业务断言工具）。
//!
//! 两套装配：
//! * [`no_exchange_app`]：真实 `AppState`/router + 内置交易所注册表（不连网），
//!   用于协议、参数校验、40001/40002 与直接种表后的查询用例；
//! * [`sim_app`]：注入 `trading-sim` 模拟所（`SimVenue`/`SimLedger`），
//!   用于下单/撤单/查仓/查余额的正常路径。
//!
//! admin 端点（账户重载/K线补数/CTP 规格刷新）与生产同一份挂载，
//! 经 `testkit::admin_router` 合并到测试 Router。

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use rust_decimal::Decimal;
use serde_json::Value;
use tempfile::TempDir;
use tower::ServiceExt;

use trading_exchange::provider::ProviderDeps;
use trading_exchange::registry::ExchangeRegistry;
use trading_service::testkit::{
    admin_router, app_with_accounts, assemble_app, AppLite, AppState, RegistryFactory,
};
use trading_sim::ledger::SimLedger;
use trading_sim::venue::SimVenue;
use trading_sim::SimConfig;

/// 模拟所注册表精确名。
pub const SIM_EXCHANGE: &str = "sim";
/// 模拟所账户名（account.csv name 列）。
pub const SIM_ACCOUNT: &str = "simacct";
/// 默认测试标的。
pub const DEFAULT_INSTRUMENT: &str = "BTCUSDT";
/// 等待宿主状态收敛的上限（覆盖下单后台任务/去抖，远快于挂死）。
pub const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

/// 一个已配置的 binance 账户行（合成凭证，不连网）。
pub const BINANCE_ACCOUNT: &str =
    "0,acct,binance,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n";

/// 模拟所账户行（与 e2e harness 同一份合成凭证）。
pub const SIM_ACCOUNT_CSV: &str =
    "0,simacct,sim,\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n";

/// 已装配的接口测试应用；`_dir` 持有临时目录生命周期。
pub struct TestApi {
    pub state: AppState,
    pub router: Router,
    _dir: TempDir,
}

/// 带 sim 账户的模拟所应用 + 台账句柄（直接种仓/查单用）。
pub struct SimApi {
    pub api: TestApi,
    pub ledger: Arc<SimLedger>,
}

/// 不带交易所的协议测试应用（一个 binance 账户）。
pub async fn no_exchange_app() -> TestApi {
    app_with_csv(BINANCE_ACCOUNT).await
}

/// 用给定 account.csv 正文（不含表头；空串=无账户）装配协议测试应用。
pub async fn app_with_csv(csv_body: &str) -> TestApi {
    let dir = TempDir::new().expect("temp dir");
    let app = app_with_accounts(dir.path(), csv_body).await;
    finish(dir, app)
}

/// 装配收尾：把 admin 路由合并进真实 Router，并持有临时目录生命周期。
fn finish(dir: TempDir, app: AppLite) -> TestApi {
    TestApi {
        state: app.state.clone(),
        router: app.router.merge(admin_router().with_state(app.state)),
        _dir: dir,
    }
}

/// 只含模拟所的注册表工厂（不含 CTP provider）。
fn sim_registry(ledger: Arc<SimLedger>) -> RegistryFactory {
    let cfg = ledger.config().clone();
    Arc::new(move |deps: ProviderDeps| {
        let venue = Arc::new(SimVenue::from_deps(Arc::clone(&ledger), cfg.clone(), &deps));
        ExchangeRegistry::from_providers(vec![venue])
    })
}

/// 注册表含模拟所但**不含 CTP provider** 的应用（admin CTP 未启用分支用）。
///
/// 注册表不能为空：装配期仓位档案要求至少一家交易所接流水线；
/// 这里只要 `ctp_provider() == None` 即可走确定性 503 分支。
#[cfg_attr(not(feature = "ctp"), allow(dead_code))]
pub async fn bare_app() -> TestApi {
    let dir = TempDir::new().expect("temp dir");
    let ledger = Arc::new(SimLedger::new(SimConfig::new(SIM_EXCHANGE)));
    let app = assemble_app(
        dir.path(),
        BINANCE_ACCOUNT,
        Duration::from_millis(50),
        sim_registry(ledger),
    )
    .await;
    finish(dir, app)
}

/// 注入模拟所的全链路应用；默认标的预先设了标记价 100，市价单可立即成交。
pub async fn sim_app() -> SimApi {
    let dir = TempDir::new().expect("temp dir");
    let ledger = Arc::new(SimLedger::new(SimConfig::new(SIM_EXCHANGE)));
    ledger.set_mark(DEFAULT_INSTRUMENT, dec("100"));

    // 去抖窗口取小值让持仓/订单收敛更快；其余装配参数与 testkit 同一份内核。
    let app = assemble_app(
        dir.path(),
        SIM_ACCOUNT_CSV,
        Duration::from_millis(20),
        sim_registry(Arc::clone(&ledger)),
    )
    .await;
    SimApi {
        api: finish(dir, app),
        ledger,
    }
}

/// 发任意方法请求，返回 (状态码, JSON 信封)；空 body 返回 `Null`。
pub async fn request(router: &Router, method: &str, uri: &str, body: &str) -> (StatusCode, Value) {
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("build request"),
        )
        .await
        .expect("router call");
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body");
    if bytes.is_empty() {
        return (status, Value::Null);
    }
    (status, serde_json::from_slice(&bytes).expect("json body"))
}

pub async fn get_json(router: &Router, uri: &str) -> (StatusCode, Value) {
    request(router, "GET", uri, "").await
}

pub async fn post_json(router: &Router, uri: &str, body: &str) -> (StatusCode, Value) {
    request(router, "POST", uri, body).await
}

pub async fn post_value(router: &Router, uri: &str, body: &Value) -> (StatusCode, Value) {
    request(router, "POST", uri, &body.to_string()).await
}

pub async fn delete_json(router: &Router, uri: &str) -> (StatusCode, Value) {
    request(router, "DELETE", uri, "").await
}

/// 轮询等待宿主状态满足条件；超时 panic（带说明，便于定位）。
pub async fn wait_for(what: &str, mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    while !predicate() {
        assert!(
            Instant::now() < deadline,
            "等待 `{what}` 超时（{WAIT_TIMEOUT:?}）"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// 字符串转十进制（测试夹具用）。
pub fn dec(s: &str) -> Decimal {
    use std::str::FromStr;
    Decimal::from_str(s).expect("合法十进制")
}
