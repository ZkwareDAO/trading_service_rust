//! CTP 合约规格「每日全量拉取」短会话（一次性交易连接，阻塞同步驱动）。
//!
//! 与生产长会话（[`crate::ctp::gateway::trader`]）刻意分开：规格刷新每天只跑
//! 一次（外部 cron 14:55 调管理 API），**不重连、不下单**，在调用线程内同步
//! 驱动，SPI 把应答行即时拷贝成 owned 行经同步通道交回。复用生产环境的
//! GB18030 定长编解码，不重写一套字符串逻辑。
//!
//! 拉取两类全量数据：
//! 1. `ReqQryInstrument` 全量合约（乘数/最小手数/跳价/多空保证金率等，
//!    保证金率以合约字段形式随本次查询一并取得，不单列保证金查询）；
//! 2. `ReqQryInstrumentCommissionRate` 该投资者的开仓/平仓/平今手续费
//!    （先按交易所批量查，再对未覆盖品种各取一个在交易代表合约补查）。
//!
//! 分层：本模块只产出 exchange 自有的行类型（[`FetchedInstrument`] /
//! [`FetchedCommission`]），**不依赖 trading-store、不做文件 IO**；落盘成
//! `data/ctp-specs/...csv` 由宿主配合 trading-store 的 `CtpSpecsStore` 完成。
//!
//! 安全：[`TraderApi`] 是 FFI 句柄，任何退出路径（含 `?`）都先 `release()`
//! 再回收 SPI，避免柜台内部线程回调访问已释放的 SPI（顺序与生产
//! `TraderHandle::drop` 一致，见 [`SessionGuard`]）。

use std::collections::{BTreeSet, HashSet};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::time::{Duration, Instant};

use ctp2rs::v1alpha1::{
    CThostFtdcInstrumentCommissionRateField, CThostFtdcInstrumentField, CThostFtdcRspInfoField,
    TraderApi, TraderSpi, THOST_TE_RESUME_TYPE,
};

use crate::ctp::core::account::CtpAccount;
use crate::ctp::core::encoding::{decode_fixed, encode_fixed};
use crate::ctp::core::event::InstrumentInfo;

/// CTP 产品类型常量（ThostFtdcUserApiDataType.h 的 THOST_FTDC_PC_Future，ASCII '1'）。
pub const PRODUCT_FUTURES: u8 = b'1';

/// 登录三步总窗口（结算确认在换日时可能要下载结算单，放宽到 5 分钟）。
const LOGIN_DEADLINE: Duration = Duration::from_secs(300);
/// 单个查询的收齐窗口（全量合约约数千行，正常秒级完成）。
const QUERY_DEADLINE: Duration = Duration::from_secs(180);
/// 事件等待粒度。
const POLL: Duration = Duration::from_secs(2);
/// CTP 查询流控约 1 秒/次，发查询前留出余量。
const QUERY_INTERVAL: Duration = Duration::from_millis(1100);

/// 一行合约规格（`ReqQryInstrument` 全量结果中的一条，字段口径与落盘 CSV 一致）。
#[derive(Clone, Debug, PartialEq)]
pub struct FetchedInstrument {
    /// 合约代码。
    pub instrument_id: String,
    /// 交易所代码。
    pub exchange_id: String,
    /// 产品代码（品种字母部分，如 rb / cu / i）。
    pub product_id: String,
    /// 合约中文名（如「螺纹钢2612」）。
    pub instrument_name: String,
    /// 产品类型（'1'=期货，见 [`PRODUCT_FUTURES`]；'2'=期权）。
    pub product_class: u8,
    /// 合约数量乘数（每手数量单位：吨/克/千克…）。
    pub volume_multiple: i32,
    /// 最小变动价位。
    pub price_tick: f64,
    /// 限价单最小下单量（手）。
    pub min_limit_volume: i32,
    /// 限价单最大下单量（手）。
    pub max_limit_volume: i32,
    /// 市价单最小下单量（手）。
    pub min_market_volume: i32,
    /// 市价单最大下单量（手）。
    pub max_market_volume: i32,
    /// 多头保证金率（随合约字段一并取得）。
    pub long_margin_ratio: f64,
    /// 空头保证金率。
    pub short_margin_ratio: f64,
    /// 当前是否可交易。
    pub is_trading: bool,
    /// 上市日（YYYYMMDD）。
    pub open_date: String,
    /// 到期日（YYYYMMDD）。
    pub expire_date: String,
}

/// 一行投资者手续费率（`ReqQryInstrumentCommissionRate` 结果中的一条）。
#[derive(Clone, Debug, PartialEq)]
pub struct FetchedCommission {
    /// 合约代码。
    pub instrument_id: String,
    /// 交易所代码（批量应答可能为空）。
    pub exchange_id: String,
    /// 开仓：按成交额比率（0 表示不采用）。
    pub open_by_money: f64,
    /// 开仓：按手固定费（元/手，0 表示不采用）。
    pub open_by_volume: f64,
    /// 平仓（平昨）：按成交额比率。
    pub close_by_money: f64,
    /// 平仓（平昨）：按手固定费。
    pub close_by_volume: f64,
    /// 平今：按成交额比率。
    pub close_today_by_money: f64,
    /// 平今：按手固定费（SHFE 平今免收时为 0）。
    pub close_today_by_volume: f64,
}

/// 一次全量拉取的结果（合约 + 投资者手续费率）。
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SpecSnapshot {
    /// 全量合约（含期权；内存目录只收期货）。
    pub instruments: Vec<FetchedInstrument>,
    /// 投资者手续费率（按合约去重）。
    pub commissions: Vec<FetchedCommission>,
}

impl SpecSnapshot {
    /// 其中期货合约条数（诊断/应答用）。
    pub fn futures_count(&self) -> usize {
        self.instruments
            .iter()
            .filter(|i| i.product_class == PRODUCT_FUTURES)
            .count()
    }
}

/// 从合约表提取去重且有序的交易所代码（BTreeSet 天然字母序：
/// CFFEX → CZCE → DCE → GFEX → INE → SHFE）。纯函数，空交易所代码剔除。
pub fn distinct_exchanges(instruments: &[FetchedInstrument]) -> Vec<String> {
    instruments
        .iter()
        .map(|i| i.exchange_id.clone())
        .filter(|e| !e.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// 按合约号反查它的（交易所, 产品）归属；非期货或查不到返回 None。
///
/// 手续费批量应答的 `ExchangeID` 可能为空，用合约反查产品归属以判断覆盖品种。
pub fn product_of(
    instruments: &[FetchedInstrument],
    instrument_id: &str,
) -> Option<(String, String)> {
    instruments
        .iter()
        .find(|i| i.instrument_id == instrument_id && i.product_class == PRODUCT_FUTURES)
        .map(|i| (i.exchange_id.clone(), i.product_id.clone()))
}

/// 选出尚未覆盖手续费的期货品种的「代表合约」：
/// 每个品种取合约号最小的当前可交易合约，结果按合约号排序。
///
/// 同品种各到期月费率一致，对代表合约查一次即可代表整品种，
/// 把逐合约全量查询（数百次、十几分钟）压缩到每品种一次（约数十次）。
pub fn product_representatives(
    instruments: &[FetchedInstrument],
    covered: &HashSet<(String, String)>,
) -> Vec<String> {
    // (交易所, 产品) → 最小合约号。
    let mut pick: std::collections::BTreeMap<(String, String), String> =
        std::collections::BTreeMap::new();
    for ins in instruments {
        if ins.product_class != PRODUCT_FUTURES || !ins.is_trading {
            continue;
        }
        let key = (ins.exchange_id.clone(), ins.product_id.clone());
        if covered.contains(&key) {
            continue;
        }
        pick.entry(key)
            .and_modify(|cur| {
                if ins.instrument_id < *cur {
                    *cur = ins.instrument_id.clone();
                }
            })
            .or_insert_with(|| ins.instrument_id.clone());
    }
    let mut reps: Vec<String> = pick.into_values().collect();
    reps.sort();
    reps
}

/// 把拉取到的全量合约收敛为可重灌共享 [`InstrumentDir`] 的静态信息行：
/// **仅当前在交易的期货**（`product_class='1'` 且 `is_trading`），与冷启动
/// `InstrumentDir::load_from_csv` 的过滤口径一致（期权/组合/摘牌一律剔除）。
///
/// 这是纯函数（不触碰 FFI/文件），刷新后「以最新快照替换内存目录」的重灌逻辑
/// 借此可单测：宿主把返回行逐条 `remember` 进新建目录，再原子替换共享实例。
pub fn to_trading_futures_dir_rows(snapshot: &SpecSnapshot) -> Vec<InstrumentInfo> {
    snapshot
        .instruments
        .iter()
        .filter(|i| i.product_class == PRODUCT_FUTURES && i.is_trading)
        .map(|i| InstrumentInfo {
            instrument: i.instrument_id.clone(),
            exchange_id: i.exchange_id.clone(),
            product_id: i.product_id.clone(),
            is_trading: true,
            volume_multiple: i.volume_multiple,
            price_tick: i.price_tick,
            max_limit_volume: i.max_limit_volume,
            max_market_volume: i.max_market_volume,
            long_margin_ratio: i.long_margin_ratio,
            short_margin_ratio: i.short_margin_ratio,
        })
        .collect()
}

/// 连一次柜台，拉全量合约规格与投资者手续费率（**阻塞**；含登录与多轮 1 秒
/// 流控查询，全市场约数十秒，调用方须放在 `spawn_blocking`）。
///
/// * `account`：连接/登录/结算确认所用账户（`front_address` 为**交易**前置）；
/// * `trader_so`：柜台交易库路径；
/// * `flow_base`：流文件目录基准；本函数自建 `spec-refresh/` 子目录隔离，
///   不与运行中服务按账户的 con 文件互踩。
pub fn fetch_all_specs(
    account: &CtpAccount,
    trader_so: &str,
    flow_base: &str,
) -> anyhow::Result<SpecSnapshot> {
    account
        .validate()
        .map_err(|e| anyhow::anyhow!("规格拉取账户参数不完整：{e}"))?;

    let flow_path = format!("{}/spec-refresh", flow_base.trim_end_matches('/'));
    std::fs::create_dir_all(&flow_path)
        .map_err(|e| anyhow::anyhow!("创建流水目录 {flow_path} 失败：{e}"))?;

    // 通道给足容量：全量合约数千行在主线程登录期间不消费，避免撑爆阻塞柜台线程。
    let (tx, rx): (SyncSender<Ev>, Receiver<Ev>) = sync_channel(16_384);
    let spi_raw = Box::into_raw(Box::new(CollectSpi { tx })) as *mut dyn TraderSpi;

    let api = TraderApi::create_api(trader_so, &flow_path, account.is_production_mode);
    api.register_spi(spi_raw);
    api.subscribe_private_topic(THOST_TE_RESUME_TYPE::THOST_TERT_QUICK, 1);
    api.subscribe_public_topic(THOST_TE_RESUME_TYPE::THOST_TERT_QUICK);
    api.register_front(&account.front_address);
    api.init();

    // 从这里开始，任何提前返回都会触发守卫释放。
    let guard = SessionGuard { api, spi_raw };

    let req_id = AtomicI32::new(1);
    let next_id = || req_id.fetch_add(1, Ordering::SeqCst);

    login(&guard.api, &rx, account, &next_id)?;

    // 查询 1：全量合约（空条件 = 全部）。
    std::thread::sleep(QUERY_INTERVAL);
    let instruments = collect_query(
        &rx,
        || {
            let mut req = ctp2rs::v1alpha1::CThostFtdcQryInstrumentField::default();
            guard.api.req_qry_instrument(&mut req, next_id())
        },
        |ev| match ev {
            Ev::Instrument(row) => Some(row.clone()),
            _ => None,
        },
    )?;

    // 查询 2：该投资者的手续费率。空条件在 SimNow 只回零散品种，
    // 按合约表中的交易所逐所查询更完整；每所之间遵守 1 秒查询流控。
    let exchanges = distinct_exchanges(&instruments);
    let mut commissions = Vec::new();
    for exchange in &exchanges {
        std::thread::sleep(QUERY_INTERVAL);
        // 借用闭包：本轮内同步发请求并收齐，不搬走 guard（跨轮复用同一 API）。
        let issue = || {
            let mut req = ctp2rs::v1alpha1::CThostFtdcQryInstrumentCommissionRateField::default();
            encode_fixed(&mut req.BrokerID, &account.broker_id);
            encode_fixed(&mut req.InvestorID, account.effective_investor_id());
            encode_fixed(&mut req.ExchangeID, exchange);
            guard
                .api
                .req_qry_instrument_commission_rate(&mut req, next_id())
        };
        let mut rows = collect_query(&rx, issue, |ev| match ev {
            Ev::Commission(row) => Some(row.clone()),
            _ => None,
        })?;
        tracing::debug!(
            exchange = %exchange,
            rows = rows.len(),
            "规格刷新：交易所手续费批量应答"
        );
        commissions.append(&mut rows);
    }

    // 部分柜台（SimNow）忽略交易所过滤条件、逐所返回同一批投资者费率，
    // 按合约号去重（同合约保留首条；各所返回的费率应一致）。
    let mut seen: HashSet<String> = commissions
        .iter()
        .map(|c| c.instrument_id.clone())
        .collect();

    // 手续费按「合约」返回，但同品种各到期月费率一致。批量查询未覆盖的品种，
    // 用该品种一个可交易代表合约（合约号最小者）逐个补查（1 秒流控），
    // 把 ~800 次全量逐查压缩到每品种 1 次（约数十次）。
    let covered_products: HashSet<(String, String)> = commissions
        .iter()
        .filter_map(|c| product_of(&instruments, &c.instrument_id))
        .collect();
    let representatives = product_representatives(&instruments, &covered_products);
    for instrument in &representatives {
        std::thread::sleep(QUERY_INTERVAL);
        let mut rows = collect_query(
            &rx,
            || {
                let mut req =
                    ctp2rs::v1alpha1::CThostFtdcQryInstrumentCommissionRateField::default();
                encode_fixed(&mut req.BrokerID, &account.broker_id);
                encode_fixed(&mut req.InvestorID, account.effective_investor_id());
                encode_fixed(&mut req.InstrumentID, instrument);
                guard
                    .api
                    .req_qry_instrument_commission_rate(&mut req, next_id())
            },
            |ev| match ev {
                Ev::Commission(row) => Some(row.clone()),
                _ => None,
            },
        )?;
        tracing::debug!(
            instrument = %instrument,
            rows = rows.len(),
            "规格刷新：品种代表合约补查手续费"
        );
        // 补查到的新合约并入，已有合约不覆盖。
        for r in rows.drain(..) {
            if seen.insert(r.instrument_id.clone()) {
                commissions.push(r);
            }
        }
    }

    Ok(SpecSnapshot {
        instruments,
        commissions,
    })
}

/// SPI → 主循环的事件帧（全部 owned，回调线程内拷贝完即脱离 FFI 结构）。
enum Ev {
    FrontConnected,
    FrontDisconnected(i32),
    /// 认证应答（code=0 通过）。
    Authed(i32, String),
    LoginOk,
    SettlementConfirmed,
    Instrument(FetchedInstrument),
    Commission(FetchedCommission),
    /// 当前查询末页到达。
    QueryEnd,
    /// 柜台错误（错误码 + 中文信息）。
    Error(i32, String),
}

/// 收集型 SPI：只实现登录/两个查询用到的回调，其余走 trait 默认空实现。
struct CollectSpi {
    tx: SyncSender<Ev>,
}

impl CollectSpi {
    fn emit(&self, ev: Ev) {
        // 接收端只在关停时先于本 SPI 消失；丢弃即可，绝不让回调线程 panic。
        let _ = self.tx.send(ev);
    }

    /// 读取应答错误（无 RspInfo 视为成功）；非 0 上抛错误事件，返回是否有错。
    fn emit_rsp_error(&self, info: Option<&CThostFtdcRspInfoField>) -> bool {
        if let Some(i) = info {
            if i.ErrorID != 0 {
                self.emit(Ev::Error(i.ErrorID, decode_fixed(&i.ErrorMsg)));
                return true;
            }
        }
        false
    }
}

impl TraderSpi for CollectSpi {
    fn on_front_connected(&mut self) {
        self.emit(Ev::FrontConnected);
    }

    fn on_front_disconnected(&mut self, reason: i32) {
        self.emit(Ev::FrontDisconnected(reason));
    }

    fn on_rsp_authenticate(
        &mut self,
        _field: Option<&ctp2rs::v1alpha1::CThostFtdcRspAuthenticateField>,
        info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        _is_last: bool,
    ) {
        let (code, msg) = info
            .map(|i| (i.ErrorID, decode_fixed(&i.ErrorMsg)))
            .unwrap_or((0, String::new()));
        self.emit(Ev::Authed(code, msg));
    }

    fn on_rsp_user_login(
        &mut self,
        login: Option<&ctp2rs::v1alpha1::CThostFtdcRspUserLoginField>,
        info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        _is_last: bool,
    ) {
        if login.is_some() {
            self.emit(Ev::LoginOk);
        }
        self.emit_rsp_error(info);
    }

    fn on_rsp_settlement_info_confirm(
        &mut self,
        _field: Option<&ctp2rs::v1alpha1::CThostFtdcSettlementInfoConfirmField>,
        info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        is_last: bool,
    ) {
        if self.emit_rsp_error(info) {
            return;
        }
        if is_last {
            self.emit(Ev::SettlementConfirmed);
        }
    }

    fn on_rsp_qry_instrument(
        &mut self,
        instrument: Option<&CThostFtdcInstrumentField>,
        info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        is_last: bool,
    ) {
        if let Some(i) = instrument {
            self.emit(Ev::Instrument(FetchedInstrument {
                instrument_id: decode_fixed(&i.InstrumentID),
                exchange_id: decode_fixed(&i.ExchangeID),
                product_id: decode_fixed(&i.ProductID),
                instrument_name: decode_fixed(&i.InstrumentName),
                product_class: i.ProductClass as u8,
                volume_multiple: i.VolumeMultiple,
                price_tick: i.PriceTick,
                min_limit_volume: i.MinLimitOrderVolume,
                max_limit_volume: i.MaxLimitOrderVolume,
                min_market_volume: i.MinMarketOrderVolume,
                max_market_volume: i.MaxMarketOrderVolume,
                long_margin_ratio: i.LongMarginRatio,
                short_margin_ratio: i.ShortMarginRatio,
                is_trading: i.IsTrading != 0,
                open_date: decode_fixed(&i.OpenDate),
                expire_date: decode_fixed(&i.ExpireDate),
            }));
        }
        self.emit_rsp_error(info);
        if is_last {
            self.emit(Ev::QueryEnd);
        }
    }

    fn on_rsp_qry_instrument_commission_rate(
        &mut self,
        rate: Option<&CThostFtdcInstrumentCommissionRateField>,
        info: Option<&CThostFtdcRspInfoField>,
        _request_id: i32,
        is_last: bool,
    ) {
        if let Some(r) = rate {
            self.emit(Ev::Commission(FetchedCommission {
                instrument_id: decode_fixed(&r.InstrumentID),
                exchange_id: decode_fixed(&r.ExchangeID),
                open_by_money: r.OpenRatioByMoney,
                open_by_volume: r.OpenRatioByVolume,
                close_by_money: r.CloseRatioByMoney,
                close_by_volume: r.CloseRatioByVolume,
                close_today_by_money: r.CloseTodayRatioByMoney,
                close_today_by_volume: r.CloseTodayRatioByVolume,
            }));
        }
        self.emit_rsp_error(info);
        if is_last {
            self.emit(Ev::QueryEnd);
        }
    }
}

/// 资源守卫：任何退出路径（含 `?` 错误）都先 Release 再回收 SPI，
/// 避免柜台内部线程回调访问已释放的 SPI（顺序与生产 TraderHandle::drop 一致）。
struct SessionGuard {
    api: TraderApi,
    spi_raw: *mut dyn TraderSpi,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.api.release();
        // SAFETY：spi_raw 来自唯一一次 Box::into_raw；守卫独占且 Drop 至多一次。
        unsafe {
            let _ = Box::from_raw(self.spi_raw);
        }
    }
}

/// 登录三步：等前置连接 → 认证 → 登录 → 结算确认（任一步受理码非 0 即失败）。
fn login(
    api: &TraderApi,
    rx: &Receiver<Ev>,
    account: &CtpAccount,
    next_id: &dyn Fn() -> i32,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + LOGIN_DEADLINE;

    // ① 等前置连接：未连上就发认证会被柜台直接忽略。
    loop {
        match wait_event(rx, deadline)? {
            Ev::FrontConnected => break,
            Ev::FrontDisconnected(r) => anyhow::bail!("连接阶段前置断开：{r:#x}"),
            _ => {}
        }
    }

    // ② 认证。
    let mut auth = ctp2rs::v1alpha1::CThostFtdcReqAuthenticateField::default();
    encode_fixed(&mut auth.BrokerID, &account.broker_id);
    encode_fixed(&mut auth.UserID, &account.user_id);
    encode_fixed(&mut auth.AppID, &account.app_id);
    encode_fixed(&mut auth.AuthCode, &account.auth_code);
    let ret = api.req_authenticate(&mut auth, next_id());
    if ret != 0 {
        anyhow::bail!("认证请求受理失败 code={ret}");
    }
    loop {
        match wait_event(rx, deadline)? {
            Ev::Authed(0, _) => break,
            Ev::Authed(code, msg) => anyhow::bail!("认证被拒 code={code} msg={msg}"),
            Ev::FrontDisconnected(r) => anyhow::bail!("认证阶段前置断开：{r:#x}"),
            Ev::Error(code, msg) => anyhow::bail!("认证阶段错误 code={code} msg={msg}"),
            _ => {}
        }
    }

    // ③ 登录。
    let mut login_field = ctp2rs::v1alpha1::CThostFtdcReqUserLoginField::default();
    encode_fixed(&mut login_field.BrokerID, &account.broker_id);
    encode_fixed(&mut login_field.UserID, &account.user_id);
    encode_fixed(&mut login_field.Password, &account.password);
    let ret = api.req_user_login(&mut login_field, next_id());
    if ret != 0 {
        anyhow::bail!("登录请求受理失败 code={ret}");
    }
    loop {
        match wait_event(rx, deadline)? {
            Ev::LoginOk => break,
            Ev::FrontDisconnected(r) => anyhow::bail!("登录阶段前置断开：{r:#x}"),
            Ev::Error(code, msg) => anyhow::bail!("登录被拒 code={code} msg={msg}"),
            _ => {}
        }
    }

    // ④ 结算单确认（不确认则查询/报单被柜台拒绝）。
    let mut confirm = ctp2rs::v1alpha1::CThostFtdcSettlementInfoConfirmField::default();
    encode_fixed(&mut confirm.BrokerID, &account.broker_id);
    encode_fixed(&mut confirm.InvestorID, account.effective_investor_id());
    let ret = api.req_settlement_info_confirm(&mut confirm, next_id());
    if ret != 0 {
        anyhow::bail!("结算确认请求受理失败 code={ret}");
    }
    loop {
        match wait_event(rx, deadline)? {
            Ev::SettlementConfirmed => return Ok(()),
            Ev::FrontDisconnected(r) => anyhow::bail!("结算确认阶段前置断开：{r:#x}"),
            Ev::Error(code, msg) => anyhow::bail!("结算确认失败 code={code} msg={msg}"),
            _ => {}
        }
    }
}

/// 发起一次查询并收齐应答行：`issue` 返回柜台受理码（0=受理），
/// `pick` 从事件帧中认出属于本次查询的数据行，`QueryEnd` 收尾。
fn collect_query<T>(
    rx: &Receiver<Ev>,
    issue: impl FnOnce() -> i32,
    pick: impl Fn(&Ev) -> Option<T>,
) -> anyhow::Result<Vec<T>> {
    let deadline = Instant::now() + QUERY_DEADLINE;
    let ret = issue();
    if ret != 0 {
        anyhow::bail!("查询请求受理失败 code={ret}（可能触发流控，请稍后重试）");
    }
    let mut rows = Vec::new();
    loop {
        match wait_event(rx, deadline)? {
            Ev::QueryEnd => return Ok(rows),
            Ev::FrontDisconnected(r) => anyhow::bail!("查询阶段前置断开：{r:#x}"),
            Ev::Error(code, msg) => anyhow::bail!("查询被拒 code={code} msg={msg}"),
            other => {
                if let Some(row) = pick(&other) {
                    rows.push(row);
                }
            }
        }
    }
}

/// 带总截止时间的事件等待：单轮超时（无事件）则按剩余时间继续等，
/// 超过总期限才报错；通道关闭直接失败。
fn wait_event(rx: &Receiver<Ev>, deadline: Instant) -> anyhow::Result<Ev> {
    loop {
        let now = Instant::now();
        if now >= deadline {
            anyhow::bail!("等待柜台应答超时");
        }
        match rx.recv_timeout(POLL.min(deadline - now)) {
            Ok(ev) => return Ok(ev),
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => {
                anyhow::bail!("事件通道关闭（SPI 已释放）")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inst(id: &str, exch: &str, product: &str, class: u8, trading: bool) -> FetchedInstrument {
        FetchedInstrument {
            instrument_id: id.into(),
            exchange_id: exch.into(),
            product_id: product.into(),
            instrument_name: id.into(),
            product_class: class,
            volume_multiple: 10,
            price_tick: 1.0,
            min_limit_volume: 1,
            max_limit_volume: 500,
            min_market_volume: 1,
            max_market_volume: 500,
            long_margin_ratio: 0.08,
            short_margin_ratio: 0.08,
            is_trading: trading,
            open_date: "20250101".into(),
            expire_date: "20261215".into(),
        }
    }

    fn futures(id: &str, exch: &str, product: &str) -> FetchedInstrument {
        inst(id, exch, product, PRODUCT_FUTURES, true)
    }

    #[test]
    fn distinct_exchanges_sorted_dedup_skips_blank() {
        let rows = vec![
            futures("cu2611", "SHFE", "cu"),
            futures("cu2612", "SHFE", "cu"),
            futures("rb2611", "DCE", "rb"),
            futures("x1", "", "x"),
        ];
        // BTreeSet 字母序去重；空交易所剔除。
        assert_eq!(
            distinct_exchanges(&rows),
            vec!["DCE".to_string(), "SHFE".to_string()]
        );
    }

    #[test]
    fn product_of_maps_instrument_back_to_product() {
        // 1 个期货 + 1 个同号期权：精确命中期货归属，期权不算。
        let fut = futures("cu2611", "SHFE", "cu");
        let opt = inst("cu2611", "SHFE", "cu_o", b'2', true);
        assert_eq!(
            product_of(&[fut.clone(), opt], "cu2611"),
            Some(("SHFE".into(), "cu".into()))
        );
        assert_eq!(product_of(&[fut], "cu9999"), None);
    }

    #[test]
    fn representatives_pick_one_per_uncovered_product() {
        // rb 两个月（未覆盖）取最小月；cu 已覆盖不列；al 跳过不可交易月取下一可交易月。
        let rows = vec![
            futures("rb2611", "SHFE", "rb"),
            futures("rb2612", "SHFE", "rb"),
            futures("cu2611", "SHFE", "cu"),
            inst("al2610", "SHFE", "al", PRODUCT_FUTURES, false),
            futures("al2611", "SHFE", "al"),
        ];
        let covered = HashSet::from([("SHFE".to_string(), "cu".to_string())]);

        let reps = product_representatives(&rows, &covered);

        assert_eq!(reps, vec!["al2611".to_string(), "rb2611".to_string()]);
    }

    #[test]
    fn futures_count_only_counts_class_one() {
        let snap = SpecSnapshot {
            instruments: vec![
                futures("cu2611", "SHFE", "cu"),
                inst("cu2611C80000", "SHFE", "cu_o", b'2', true),
                inst("zz9999", "XXXX", "zz", b'3', true),
            ],
            commissions: vec![],
        };
        assert_eq!(snap.futures_count(), 1);
    }

    #[test]
    fn dir_rows_keep_only_trading_futures_and_map_fields() {
        // 全量含：在交易期货 / 摘牌期货 / 期权。重灌目录只应收 1 行且字段映射正确。
        let active = futures("cu2611", "SHFE", "cu");
        let delisted = inst("cu2512", "SHFE", "cu", PRODUCT_FUTURES, false);
        let option = inst("cu2611C80000", "SHFE", "cu_o", b'2', true);
        let snap = SpecSnapshot {
            instruments: vec![active, delisted, option],
            commissions: vec![],
        };

        let rows = to_trading_futures_dir_rows(&snap);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].instrument, "cu2611");
        assert_eq!(rows[0].exchange_id, "SHFE");
        assert_eq!(rows[0].product_id, "cu");
        assert!(rows[0].is_trading);
        assert_eq!(rows[0].volume_multiple, 10);
        assert!((rows[0].long_margin_ratio - 0.08).abs() < 1e-12);
    }
}
