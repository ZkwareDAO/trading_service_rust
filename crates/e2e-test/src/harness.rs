//! E2E 共享 harness：临时目录 + 真实 `TradingStore/AppState/RuleEngine` +
//! 注入 `trading-sim` 模拟所。
//!
//! 走生产同一装配路径（`AppState::assemble` 经 `RegistryFactory` 注入），
//! 只替换最外层交易所适配器；账户事件侧复用生产 `account_stream::run`。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use rust_decimal::Decimal;
use tempfile::TempDir;
use trading_exchange::port::{AccountTargets, OrderEventSink};
use trading_exchange::provider::ProviderDeps;
use trading_exchange::registry::{AccountEventsHandle, ExchangeRegistry};
use trading_models::Position;
use trading_service::testkit::{AppState, RegistryFactory};
use trading_sim::ledger::SimLedger;
use trading_sim::venue::SimVenue;
use trading_sim::SimConfig;

/// 模拟所注册表精确名。
pub const SIM_EXCHANGE: &str = "sim";
/// 模拟所账户名（account.csv name 列）。
pub const SIM_ACCOUNT: &str = "simacct";
/// 默认测试标的。
pub const DEFAULT_INSTRUMENT: &str = "BTCUSDT";

/// 默认等待上限：宽松到足以覆盖去抖 + REST 快照 + 落库，仍远快于挂死。
pub const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

/// 已装配的 E2E 应用 + 模拟所句柄。
pub struct TestApp {
    pub state: AppState,
    pub router: Router,
    pub sim: Arc<SimVenue>,
    pub ledger: Arc<SimLedger>,
    stop: Arc<AtomicBool>,
    events: Option<AccountEventsHandle>,
    _dir: Option<TempDir>,
}

impl Drop for TestApp {
    fn drop(&mut self) {
        // 先置停机标志，避免 stop_all 与监督器重建连接之间重新拉起流。
        self.stop.store(true, Ordering::Relaxed);
        if let Some(events) = &self.events {
            events.stop_all();
        }
    }
}

impl TestApp {
    /// 活动仓位（宿主侧）。
    pub fn positions(&self) -> Vec<Position> {
        self.state
            .store
            .position_repo()
            .expect("position repo")
            .list_active()
    }

    /// 活动规则（宿主侧）。
    pub fn rules(&self) -> Vec<trading_models::WatchRule> {
        self.state
            .store
            .rule_repo()
            .expect("rule repo")
            .list_active()
    }

    /// 等待宿主状态满足条件；超时 panic 并 dump 全量现场。
    pub async fn wait_for(&self, what: &str, predicate: impl Fn(&Self) -> bool) {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            if predicate(self) {
                return;
            }
            if Instant::now() >= deadline {
                panic!(
                    "等待 `{what}` 超时（{:?}）。\n{}",
                    WAIT_TIMEOUT,
                    self.dump()
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// 模拟所账户在宿主账户表的 id（规则 account_id 用）。
    pub fn account_id(&self) -> u64 {
        self.state
            .resolve_trading_account(SIM_EXCHANGE, SIM_ACCOUNT)
            .expect("sim 账户可解析")
            .id
    }

    /// 按 cid 读系统订单（含终态 tombstone）。
    pub fn system_order(&self, cid: &str) -> Option<trading_models::Order> {
        self.state.store.order_repo().ok()?.peek_by_cid(cid)
    }

    /// 放入默认标的的交易所下单规则（tick/step/min-notional），供数量对齐/名义额校验。
    pub fn seed_order_rule(&self) -> trading_models::OrderRule {
        use trading_models::{InstrumentType, OrderRule, QtyUnit};

        let mut rule = OrderRule::new(
            SIM_EXCHANGE,
            DEFAULT_INSTRUMENT,
            InstrumentType::Futures,
            QtyUnit::Base,
            Decimal::ONE,
            "USDT",
            "USDT",
            dec("0.1"),
            "test",
            0,
        );
        rule.price_tick = Some(dec("0.1"));
        rule.qty_min = Some(dec("0.1"));
        rule.qty_max = Some(dec("1000"));
        rule.notional_min = Some(dec("1"));
        self.state.store.order_rules().put(rule.clone());
        rule
    }

    /// 直接放入一个活跃仓位（离线/多仓位场景；不经过交易所）。
    pub fn seed_position(
        &self,
        instrument: &str,
        side: trading_models::Side,
        quantity: Decimal,
        pos_price: Decimal,
    ) -> Option<trading_models::Position> {
        let leverage = u64::from(self.ledger.leverage().max(1));
        let mut p = Position::new(self.account_id(), SIM_EXCHANGE, instrument, side);
        p.quantity = quantity;
        p.pos_price = pos_price;
        p.current_price = pos_price;
        p.leverage = leverage;
        p.margin = pos_price * quantity / Decimal::from(leverage);
        self.state
            .store
            .position_repo()
            .expect("position repo")
            .upsert_matching(vec![p.revalued(pos_price)])
            .expect("seed position");
        self.positions()
            .into_iter()
            .find(|x| x.instrument == instrument && x.side == side)
    }

    /// 系统仓位 vs 模拟所台账逐字段差分（方向/数量/成本/现价/名义额/保证金/浮盈/ROI/杠杆）。
    ///
    /// 不一致时直接 panic（消息带全量现场），作为「逐字段一致」验收的复用断言。
    pub fn assert_position_matches_ledger(&self, instrument: &str) {
        let sys = self
            .positions()
            .into_iter()
            .find(|p| p.instrument == instrument)
            .unwrap_or_else(|| panic!("系统无 `{instrument}` 活跃仓位\n{}", self.dump()));
        let led = self
            .ledger
            .position_info(SIM_ACCOUNT, instrument)
            .unwrap_or_else(|| panic!("模拟所无 `{instrument}` 持仓\n{}", self.dump()));

        let expected_side = if led.signed_quantity.is_sign_negative() {
            trading_models::Side::Sell
        } else {
            trading_models::Side::Buy
        };
        assert_eq!(sys.side, expected_side, "方向不一致");
        assert_eq!(sys.quantity, led.signed_quantity.abs(), "数量不一致");
        assert_eq!(sys.pos_price, led.entry_price, "成本价不一致");
        assert_eq!(sys.current_price, led.mark_price.unwrap(), "现价不一致");
        assert_eq!(sys.notional, led.notional.unwrap(), "名义额不一致");
        assert_eq!(sys.margin, led.margin.unwrap(), "保证金不一致");
        assert_eq!(sys.pnl_value, led.unrealized_pnl.unwrap(), "浮盈不一致");
        assert_eq!(sys.leverage, led.leverage.unwrap(), "杠杆不一致");
        assert_eq!(sys.instrument_type, led.instrument_type);
        assert_eq!(sys.multiplier, Decimal::ONE);
        assert_eq!(sys.settle_currency, "USDT");
        if !led.margin.unwrap().is_zero() {
            let expected_roi = (led.unrealized_pnl.unwrap() / led.margin.unwrap()).round_dp(6);
            assert_eq!(sys.roi, expected_roi, "ROI 不一致");
        }
    }

    /// 插入一条盯盘规则（规则引擎下一轮 `run_scan` 即评估）。
    pub fn insert_rule(&self, rule: trading_models::WatchRule) -> trading_models::WatchRule {
        self.state
            .store
            .rule_repo()
            .expect("rule repo")
            .insert(rule)
            .expect("insert rule")
    }

    /// 手工落盘（重启恢复测试在 drop 前调用；生产由定时持久化驱动）。
    pub fn flush(&self) {
        self.state.store.flush().expect("flush store");
    }

    /// 按 id 读规则（含 retired tombstone）。
    pub fn rule(&self, id: u64) -> Option<trading_models::WatchRule> {
        self.state.store.rule_repo().ok()?.get_including_deleted(id)
    }

    /// 跑一轮规则扫描（生产由定时任务驱动；E2E 显式调用保证确定性）。
    pub async fn scan(&self) {
        self.state.rule_engine.run_scan().await;
    }

    /// 生产 REST 路由器上的 JSON 请求（复用真实 handler/信封）。
    pub async fn post_json(
        &self,
        uri: &str,
        body: serde_json::Value,
    ) -> (axum::http::StatusCode, serde_json::Value) {
        use tower::ServiceExt;

        let request = axum::http::Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body.to_string()))
            .unwrap();
        let response = self.router.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, value)
    }

    /// 经 REST 市价下单，返回服务端生成的 cid（响应非 200 直接失败）。
    pub async fn place_market(
        &self,
        instrument: &str,
        side: &str,
        quantity: &str,
        reduce_only: bool,
    ) -> String {
        self.place_order_body(serde_json::json!({
            "exchange": SIM_EXCHANGE,
            "account": SIM_ACCOUNT,
            "instrument": instrument,
            "instrument_type": "futures",
            "side": side,
            "order_type": "market",
            "quantity": quantity,
            "reduce_only": reduce_only,
        }))
        .await
    }

    /// 经 REST 限价下单，返回 cid。
    pub async fn place_limit(
        &self,
        instrument: &str,
        side: &str,
        price: &str,
        quantity: &str,
    ) -> String {
        self.place_order_body(serde_json::json!({
            "exchange": SIM_EXCHANGE,
            "account": SIM_ACCOUNT,
            "instrument": instrument,
            "instrument_type": "futures",
            "side": side,
            "order_type": "limit",
            "price": price,
            "quantity": quantity,
        }))
        .await
    }

    async fn place_order_body(&self, body: serde_json::Value) -> String {
        let (status, response) = self.post_json("/api/v1/orders", body).await;
        assert_eq!(status, axum::http::StatusCode::OK, "下单失败：{response}");
        response["data"]["client_order_id"]
            .as_str()
            .unwrap_or_else(|| panic!("响应缺少 client_order_id：{response}"))
            .to_string()
    }

    /// 失败现场：订单 / 仓位 / 规则 / 行情 / 模拟所台账。
    pub fn dump(&self) -> String {
        let orders = self
            .state
            .store
            .order_repo()
            .map(|r| r.list_active())
            .unwrap_or_default();
        let quotes = self.state.store.quotes().snapshot();
        format!(
            "orders={orders:#?}\npositions={:#?}\nrules={:#?}\nquotes={quotes:#?}\nledger_positions={:#?}",
            self.positions(),
            self.rules(),
            self.ledger.positions(SIM_ACCOUNT)
        )
    }
}

/// 起一个注入模拟所的全链路应用（独立临时目录，无网络、无真实凭证）。
pub async fn start_sim() -> TestApp {
    let dir = TempDir::new().unwrap();
    let mut app = assemble(dir.path(), true).await;
    app._dir = Some(dir);
    app
}

/// 在既有目录重新装配（重启恢复用；**不**启动账户事件流，避免空模拟所
/// 首轮对账覆盖刚恢复的 CSV 状态）。
pub async fn start_sim_at(dir_path: &std::path::Path) -> TestApp {
    assemble(dir_path, false).await
}

/// 在既有目录重新装配并启动账户事件流（重启恢复的「第一阶段」用）。
pub async fn start_sim_online_at(dir_path: &std::path::Path) -> TestApp {
    assemble(dir_path, true).await
}

async fn assemble(dir_path: &std::path::Path, online: bool) -> TestApp {
    // 重启复用同一目录：账户表已存在则传空正文（保留已落盘的 id/时间戳）。
    let csv_body = if dir_path.join("account.csv").exists() {
        String::new()
    } else {
        let mut body = format!("0,{SIM_ACCOUNT},{SIM_EXCHANGE},");
        // CSV 引号转义：字段内 `"` 写作 `""`。
        body.push_str("\"{\"\"api_key\"\":\"\"K\"\",\"\"api_secret\"\":\"\"s\"\"}\",0,0,0\n");
        body
    };

    let cfg = SimConfig::new(SIM_EXCHANGE);
    let ledger = Arc::new(SimLedger::new(cfg.clone()));

    // 工厂闭包在装配内被调用一次；把构造出的 SimVenue 回填给 harness。
    let slot: Arc<Mutex<Option<Arc<SimVenue>>>> = Arc::new(Mutex::new(None));
    let factory_slot = Arc::clone(&slot);
    let factory_ledger = Arc::clone(&ledger);
    let registry_factory: RegistryFactory = Arc::new(move |deps: ProviderDeps| {
        let venue = Arc::new(SimVenue::from_deps(
            Arc::clone(&factory_ledger),
            cfg.clone(),
            &deps,
        ));
        *factory_slot.lock().expect("factory slot 中毒") = Some(Arc::clone(&venue));
        ExchangeRegistry::from_providers(vec![venue])
    });

    // 去抖窗口取小值让 E2E 快速收敛；其余装配参数复用 testkit 的同一份内核。
    let app = trading_service::testkit::assemble_app(
        dir_path,
        &csv_body,
        Duration::from_millis(50),
        registry_factory,
    )
    .await;

    let sim = slot
        .lock()
        .expect("factory slot 中毒")
        .clone()
        .expect("registry factory 必须构造并回填 SimVenue");

    // 账户事件侧：生产同一消费循环（退避重连 + 去抖对账）。
    let stop = Arc::new(AtomicBool::new(false));
    let events = online.then(|| {
        let sink: Arc<dyn OrderEventSink> = Arc::new(app.state.clone());
        let targets: Arc<dyn AccountTargets> = Arc::new(app.state.accounts.clone());
        app.state.exchanges.spawn_account_events(
            sink,
            targets,
            Duration::from_millis(50),
            Arc::clone(&stop),
        )
    });

    TestApp {
        state: app.state,
        router: app.router,
        sim,
        ledger,
        stop,
        events,
        _dir: None,
    }
}

/// 便捷：字符串转十进制。
pub fn dec(s: &str) -> Decimal {
    use std::str::FromStr;
    Decimal::from_str(s).unwrap()
}

/// 便捷：给默认标的写 mark。
pub fn set_mark(app: &TestApp, price: &str) {
    app.sim.set_mark(DEFAULT_INSTRUMENT, dec(price));
}

/// 便捷：经真实注册表构造模拟所 API（等价宿主下单时的 `connect` 路径）。
pub fn sim_api(app: &TestApp) -> Arc<dyn trading_exchange::api::ExchangeApi> {
    app.state
        .exchanges
        .connect(SIM_EXCHANGE, Some("futures"), SIM_ACCOUNT)
        .expect("sim provider 已注册")
}

/// 便捷：构造下单请求。
pub fn order_request(
    cid: &str,
    side: trading_models::Side,
    order_type: trading_models::OrderType,
    price: Option<Decimal>,
    quantity: Decimal,
    reduce_only: bool,
) -> trading_exchange::api::PlaceOrderRequest {
    trading_exchange::api::PlaceOrderRequest {
        instrument: DEFAULT_INSTRUMENT.to_string(),
        instrument_type: trading_models::InstrumentType::Futures,
        side,
        order_type,
        price,
        quantity,
        reduce_only,
        position_side: trading_exchange::api::OrderPositionSide::Both,
        time_in_force: None,
        client_order_id: cid.to_string(),
    }
}
