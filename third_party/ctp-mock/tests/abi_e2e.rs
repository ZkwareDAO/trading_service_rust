//! L2 dlopen ABI 冒烟：用**真实 ctp2rs 句柄**打开替身 .so。
//!
//! 这与服务内 `TraderHandle/MdHandle` 走完全相同的路径——dlsym mangled
//! 构造符号 → 经 `(*api).vtable_` 调白名单槽 → `SpiExt{vtable, spi_ptr}` 回调，
//! 只是不引入 trading-exchange（保持嵌套 workspace 与主 workspace 解耦）。
//!
//! 任务 1：建实例 / 注册 SPI / init / 白名单请求受理 / 取版本 / Release 不崩。
//! 任务 2–7 在此追加握手、下单成交、查询分页、行情 tick 等全生命周期断言。

use std::path::PathBuf;

use ctp2rs::v1alpha1::{MdApi, TraderApi};

/// 定位嵌套 workspace 构建出的 cdylib；允许用环境变量显式覆盖。
fn locate_so(lib_name: &str) -> PathBuf {
    if let Ok(p) = std::env::var(format!("CTP_MOCK_{}_SO", lib_name.to_uppercase())) {
        return PathBuf::from(p);
    }
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let file = format!("lib{lib_name}.so");
    for profile in ["debug", "release"] {
        let cand = base.join("target").join(profile).join(&file);
        if cand.exists() {
            return cand;
        }
    }
    panic!(
        "找不到 {file}；请先构建嵌套 workspace（cargo build --manifest-path {}/Cargo.toml）",
        base.display()
    );
}

/// 无操作交易 SPI：接收回调但不动作（任务 1 不产生回调）。
struct NoopTraderSpi;
impl ctp2rs::v1alpha1::TraderSpi for NoopTraderSpi {}

/// 无操作行情 SPI。
struct NoopMdSpi;
impl ctp2rs::v1alpha1::MdSpi for NoopMdSpi {}

#[test]
fn trader_dlopen_create_version_and_release_do_not_crash() {
    // Arrange：真实 ctp2rs 句柄加载替身 .so（union 版签名带 is_production）。
    let so = locate_so("ctpmock_td");
    let api = TraderApi::create_api(&so, "./td_flow_e2e_", false);

    // Act：版本符号可用；注册 SPI、订阅流、注册前置、init 均不崩。
    assert_eq!(api.get_api_version(), "V6.7.13-MOCK");
    let spi = Box::into_raw(Box::new(NoopTraderSpi)) as *mut dyn ctp2rs::v1alpha1::TraderSpi;
    api.register_spi(spi);
    api.subscribe_private_topic(ctp2rs::v1alpha1::THOST_TE_RESUME_TYPE::THOST_TERT_QUICK, 1);
    api.subscribe_public_topic(ctp2rs::v1alpha1::THOST_TE_RESUME_TYPE::THOST_TERT_QUICK);
    api.register_front("tcp://127.0.0.1:40001");
    api.init();

    // 白名单请求槽非 null 且受理（任务 1 占位返回 0）。
    let mut auth = ctp2rs::v1alpha1::CThostFtdcReqAuthenticateField::default();
    assert_eq!(api.req_authenticate(&mut auth, 1), 0);

    // Explicitly release then let Drop run（幂等闸门：二次 Release 不崩/不双关）。
    api.release();
    // 回收内层 dyn SPI（与 TraderHandle::drop 的职责一致：Ext 由 ctp2rs 回收）。
    drop(unsafe { Box::from_raw(spi) });
}

#[test]
fn md_dlopen_create_subscribe_login_and_release_do_not_crash() {
    let so = locate_so("ctpmock_md");
    let api = MdApi::create_api(&so, "./md_flow_e2e_", false, false, false);
    assert_eq!(api.get_api_version(), "V6.7.13-MOCK");

    let spi = Box::into_raw(Box::new(NoopMdSpi)) as *mut dyn ctp2rs::v1alpha1::MdSpi;
    api.register_spi(spi);
    api.register_front("tcp://127.0.0.1:42213");
    api.init();

    // 订阅与登录槽非 null、受理。
    assert_eq!(api.subscribe_market_data(&["rb2510"]), 0);
    let mut login = ctp2rs::v1alpha1::CThostFtdcReqUserLoginField::default();
    assert_eq!(api.req_user_login(&mut login, 1), 0);

    api.release();
    drop(unsafe { Box::from_raw(spi) });
}

/// 记录交易握手事件的 SPI（任务 2 L2）。
struct HandshakeTraderSpi {
    tx: std::sync::mpsc::SyncSender<&'static str>,
}
impl ctp2rs::v1alpha1::TraderSpi for HandshakeTraderSpi {
    fn on_front_connected(&mut self) {
        let _ = self.tx.send("connected");
    }
    fn on_rsp_authenticate(
        &mut self,
        _f: Option<&ctp2rs::v1alpha1::CThostFtdcRspAuthenticateField>,
        _i: Option<&ctp2rs::v1alpha1::CThostFtdcRspInfoField>,
        _id: i32,
        _last: bool,
    ) {
        let _ = self.tx.send("authed");
    }
    fn on_rsp_user_login(
        &mut self,
        f: Option<&ctp2rs::v1alpha1::CThostFtdcRspUserLoginField>,
        _i: Option<&ctp2rs::v1alpha1::CThostFtdcRspInfoField>,
        id: i32,
        last: bool,
    ) {
        if let Some(l) = f {
            // MaxOrderRef 初始为 0、Front/Session 为夹具值、RequestID 原样回传。
            assert_eq!(l.FrontID, 1);
            assert_eq!(l.SessionID, 7001);
            let m = l
                .MaxOrderRef
                .iter()
                .position(|&c| c == 0)
                .unwrap_or(l.MaxOrderRef.len());
            let bytes: Vec<u8> = l.MaxOrderRef[..m].iter().map(|&c| c as u8).collect();
            assert_eq!(String::from_utf8(bytes).unwrap(), "0");
            assert_eq!(id, 2);
            assert!(last);
        }
        let _ = self.tx.send("login");
    }
    fn on_rsp_settlement_info_confirm(
        &mut self,
        _f: Option<&ctp2rs::v1alpha1::CThostFtdcSettlementInfoConfirmField>,
        _i: Option<&ctp2rs::v1alpha1::CThostFtdcRspInfoField>,
        _id: i32,
        last: bool,
    ) {
        assert!(last);
        let _ = self.tx.send("settled");
    }
}

#[test]
fn trader_real_handle_completes_async_handshake() {
    use std::time::Duration;
    let so = locate_so("ctpmock_td");
    let api = TraderApi::create_api(&so, "./td_handshake_flow_", false);
    let (tx, rx) = std::sync::mpsc::sync_channel::<&'static str>(16);
    let spi =
        Box::into_raw(Box::new(HandshakeTraderSpi { tx })) as *mut dyn ctp2rs::v1alpha1::TraderSpi;
    api.register_spi(spi);
    api.subscribe_private_topic(ctp2rs::v1alpha1::THOST_TE_RESUME_TYPE::THOST_TERT_QUICK, 1);
    api.subscribe_public_topic(ctp2rs::v1alpha1::THOST_TE_RESUME_TYPE::THOST_TERT_QUICK);
    api.register_front("tcp://127.0.0.1:40001");

    // Init 必须异步先回 OnFrontConnected（真实 CTP 语义）。
    api.init();
    let recv = |tag: &'static str| -> &'static str {
        rx.recv_timeout(Duration::from_secs(2))
            .unwrap_or_else(|_| panic!("timeout waiting {tag}"))
    };
    assert_eq!(recv("connected"), "connected");

    // 认证 → 登录 → 结算确认，严格顺序；RequestID 原样回传。
    let mut auth = ctp2rs::v1alpha1::CThostFtdcReqAuthenticateField::default();
    assert_eq!(api.req_authenticate(&mut auth, 1), 0);
    assert_eq!(recv("authed"), "authed");

    let mut login = ctp2rs::v1alpha1::CThostFtdcReqUserLoginField::default();
    assert_eq!(api.req_user_login(&mut login, 2), 0);
    assert_eq!(recv("login"), "login");

    let mut conf = ctp2rs::v1alpha1::CThostFtdcSettlementInfoConfirmField::default();
    assert_eq!(api.req_settlement_info_confirm(&mut conf, 3), 0);
    assert_eq!(recv("settled"), "settled");

    api.release();
    drop(unsafe { Box::from_raw(spi) });
}

// ===== 任务 6：交易全生命周期 L2（真实 ctp2rs 句柄 dlopen 替身 .so） =====

use std::sync::mpsc::{Receiver, SyncSender};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
enum TE {
    Connected,
    Authed,
    Login,
    Settled,
    RtnOrder(char),
    RtnTrade,
    QryOrderRow,
    QryOrderLast,
    QryPosLast,
    QryInstRow,
    QryInstLast,
    CancelErr(i32),
    InsertRej(i32),
}

struct LifeSpi {
    tx: SyncSender<TE>,
}
impl ctp2rs::v1alpha1::TraderSpi for LifeSpi {
    fn on_front_connected(&mut self) {
        let _ = self.tx.send(TE::Connected);
    }
    fn on_rsp_authenticate(
        &mut self,
        _f: Option<&ctp2rs::v1alpha1::CThostFtdcRspAuthenticateField>,
        _i: Option<&ctp2rs::v1alpha1::CThostFtdcRspInfoField>,
        _id: i32,
        _last: bool,
    ) {
        let _ = self.tx.send(TE::Authed);
    }
    fn on_rsp_user_login(
        &mut self,
        _f: Option<&ctp2rs::v1alpha1::CThostFtdcRspUserLoginField>,
        _i: Option<&ctp2rs::v1alpha1::CThostFtdcRspInfoField>,
        _id: i32,
        _last: bool,
    ) {
        let _ = self.tx.send(TE::Login);
    }
    fn on_rsp_settlement_info_confirm(
        &mut self,
        _f: Option<&ctp2rs::v1alpha1::CThostFtdcSettlementInfoConfirmField>,
        _i: Option<&ctp2rs::v1alpha1::CThostFtdcRspInfoField>,
        _id: i32,
        _last: bool,
    ) {
        let _ = self.tx.send(TE::Settled);
    }
    fn on_rtn_order(&mut self, o: Option<&ctp2rs::v1alpha1::CThostFtdcOrderField>) {
        if let Some(o) = o {
            let _ = self.tx.send(TE::RtnOrder(o.OrderStatus as u8 as char));
        }
    }
    fn on_rtn_trade(&mut self, t: Option<&ctp2rs::v1alpha1::CThostFtdcTradeField>) {
        if t.is_some() {
            let _ = self.tx.send(TE::RtnTrade);
        }
    }
    fn on_rsp_qry_order(
        &mut self,
        o: Option<&ctp2rs::v1alpha1::CThostFtdcOrderField>,
        _i: Option<&ctp2rs::v1alpha1::CThostFtdcRspInfoField>,
        _id: i32,
        is_last: bool,
    ) {
        if o.is_some() {
            let _ = self.tx.send(TE::QryOrderRow);
        }
        if is_last {
            let _ = self.tx.send(TE::QryOrderLast);
        }
    }
    fn on_rsp_qry_investor_position(
        &mut self,
        _p: Option<&ctp2rs::v1alpha1::CThostFtdcInvestorPositionField>,
        _i: Option<&ctp2rs::v1alpha1::CThostFtdcRspInfoField>,
        _id: i32,
        is_last: bool,
    ) {
        if is_last {
            let _ = self.tx.send(TE::QryPosLast);
        }
    }
    fn on_rsp_qry_instrument(
        &mut self,
        i: Option<&ctp2rs::v1alpha1::CThostFtdcInstrumentField>,
        _x: Option<&ctp2rs::v1alpha1::CThostFtdcRspInfoField>,
        _id: i32,
        is_last: bool,
    ) {
        if i.is_some() {
            let _ = self.tx.send(TE::QryInstRow);
        }
        if is_last {
            let _ = self.tx.send(TE::QryInstLast);
        }
    }
    fn on_err_rtn_order_action(
        &mut self,
        _a: Option<&ctp2rs::v1alpha1::CThostFtdcOrderActionField>,
        info: Option<&ctp2rs::v1alpha1::CThostFtdcRspInfoField>,
    ) {
        let _ = self
            .tx
            .send(TE::CancelErr(info.map(|i| i.ErrorID).unwrap_or(-1)));
    }
    fn on_rsp_order_insert(
        &mut self,
        _o: Option<&ctp2rs::v1alpha1::CThostFtdcInputOrderField>,
        info: Option<&ctp2rs::v1alpha1::CThostFtdcRspInfoField>,
        _id: i32,
        _last: bool,
    ) {
        let _ = self
            .tx
            .send(TE::InsertRej(info.map(|i| i.ErrorID).unwrap_or(0)));
    }
}

fn wait(rx: &Receiver<TE>, want: &TE, tag: &str) {
    let ev = rx
        .recv_timeout(Duration::from_secs(3))
        .unwrap_or_else(|_| panic!("timeout waiting {tag}"));
    assert_eq!(&ev, want, "event mismatch at {tag}");
}

fn put(buf: &mut [i8], s: &str) {
    let b = s.as_bytes();
    let n = b.len().min(buf.len() - 1);
    for (i, &x) in b[..n].iter().enumerate() {
        buf[i] = x as i8;
    }
    buf[n] = 0;
}

#[test]
fn trader_full_lifecycle_order_query_cancel_position_instrument() {
    use ctp2rs::v1alpha1::*;
    let so = locate_so("ctpmock_td");
    let api = TraderApi::create_api(&so, "./td_life_flow_", false);
    let (tx, rx) = std::sync::mpsc::sync_channel::<TE>(256);
    let spi = Box::into_raw(Box::new(LifeSpi { tx })) as *mut dyn TraderSpi;
    api.register_spi(spi);
    api.register_front("tcp://127.0.0.1:40001");
    api.init();

    // 握手三步。
    wait(&rx, &TE::Connected, "connected");
    assert_eq!(
        api.req_authenticate(
            &mut ctp2rs::v1alpha1::CThostFtdcReqAuthenticateField::default(),
            1
        ),
        0
    );
    wait(&rx, &TE::Authed, "authed");
    assert_eq!(
        api.req_user_login(
            &mut ctp2rs::v1alpha1::CThostFtdcReqUserLoginField::default(),
            2
        ),
        0
    );
    wait(&rx, &TE::Login, "login");
    assert_eq!(
        api.req_settlement_info_confirm(
            &mut ctp2rs::v1alpha1::CThostFtdcSettlementInfoConfirmField::default(),
            3
        ),
        0
    );
    wait(&rx, &TE::Settled, "settled");

    // 限价买开 rb2510 2 手（默认即时全成）：受理 '3' → 成交 → 全成 '0'。
    let mut o = CThostFtdcInputOrderField::default();
    put(&mut o.BrokerID, "9999");
    put(&mut o.InvestorID, "123456");
    put(&mut o.OrderRef, "1");
    put(&mut o.InstrumentID, "rb2510");
    o.Direction = '0' as i8;
    o.CombOffsetFlag[0] = '0' as i8;
    o.CombHedgeFlag[0] = '1' as i8;
    o.OrderPriceType = '2' as i8;
    o.TimeCondition = '3' as i8;
    o.LimitPrice = 3200.0;
    o.VolumeTotalOriginal = 2;
    assert_eq!(api.req_order_insert(&mut o, 10), 0);
    wait(&rx, &TE::RtnOrder('3'), "queued");
    wait(&rx, &TE::RtnTrade, "trade");
    wait(&rx, &TE::RtnOrder('0'), "all-traded");

    // 查当日报单：至少 1 行 + 末帧。
    assert_eq!(
        api.req_qry_order(&mut CThostFtdcQryOrderField::default(), 20),
        0
    );
    wait(&rx, &TE::QryOrderRow, "qry-order-row");
    wait(&rx, &TE::QryOrderLast, "qry-order-last");

    // 对已全成单撤单：派生 26（状态不允许），经 on_err_rtn_order_action 回灌。
    let mut act = CThostFtdcInputOrderActionField::default();
    put(&mut act.OrderRef, "1");
    put(&mut act.InstrumentID, "rb2510");
    act.FrontID = 1;
    act.SessionID = 7001;
    assert_eq!(api.req_order_action(&mut act, 30), 0);
    wait(&rx, &TE::CancelErr(26), "cancel-26");

    // 查仓：成交已建净多仓，逐行后末帧。
    assert_eq!(
        api.req_qry_investor_position(&mut CThostFtdcQryInvestorPositionField::default(), 40),
        0
    );
    wait(&rx, &TE::QryPosLast, "qry-pos-last");

    // 合约查询：rb2510 已在成交时注册夹具规格，逐行后末帧。
    assert_eq!(
        api.req_qry_instrument(&mut CThostFtdcQryInstrumentField::default(), 50),
        0
    );
    wait(&rx, &TE::QryInstRow, "qry-inst-row");
    wait(&rx, &TE::QryInstLast, "qry-inst-last");

    api.release();
    drop(unsafe { Box::from_raw(spi) });
}

/// 直接经 rlib（同一镜像，非 dlopen）构造并驱动替身对象的小助手。
///
/// 用于验证 plan §8 的「进程内控制句柄」：同一镜像内 `control_for(flow)` 才能
/// 取回实例；dlopen 的 .so 是独立镜像、statics 不共享（那条路径走 TCP 控制面）。
struct InProcTd {
    api: *mut ctp2rs::v1alpha1::CThostFtdcTraderApi,
    ext: *mut ctp2rs::v1alpha1::CThostFtdcTraderSpiExt,
    spi: *mut dyn ctp2rs::v1alpha1::TraderSpi,
}

impl InProcTd {
    fn create(flow: &str, spi: *mut dyn ctp2rs::v1alpha1::TraderSpi) -> Self {
        use ctp2rs::v1alpha1::*;
        let cflow = std::ffi::CString::new(flow).unwrap();
        // 直接调 rlib 的构造函数（同镜像，registry 与 control_for 共享）。
        let api = unsafe { ctpmock_td::ffi::td_api::create_trader_api(cflow.as_ptr(), false) };
        let ext = Box::into_raw(Box::new(CThostFtdcTraderSpiExt::new(spi)));
        unsafe {
            ((*(*api).vtable_).CThostFtdcTraderApi_RegisterSpi)(
                api,
                ext as *mut CThostFtdcTraderSpi,
            );
        }
        Self { api, ext, spi }
    }
    fn init(&self) {
        unsafe {
            ((*(*self.api).vtable_).CThostFtdcTraderApi_Init)(self.api);
        }
    }
    fn authenticate(&self, id: i32) -> i32 {
        unsafe {
            ((*(*self.api).vtable_).CThostFtdcTraderApi_ReqAuthenticate)(
                self.api,
                &mut ctp2rs::v1alpha1::CThostFtdcReqAuthenticateField::default(),
                id,
            )
        }
    }
    fn login(&self, id: i32) -> i32 {
        unsafe {
            ((*(*self.api).vtable_).CThostFtdcTraderApi_ReqUserLogin)(
                self.api,
                &mut ctp2rs::v1alpha1::CThostFtdcReqUserLoginField::default(),
                id,
            )
        }
    }
    fn settle(&self, id: i32) -> i32 {
        unsafe {
            ((*(*self.api).vtable_).CThostFtdcTraderApi_ReqSettlementInfoConfirm)(
                self.api,
                &mut ctp2rs::v1alpha1::CThostFtdcSettlementInfoConfirmField::default(),
                id,
            )
        }
    }
    fn insert(&self, o: &mut ctp2rs::v1alpha1::CThostFtdcInputOrderField, id: i32) -> i32 {
        unsafe { ((*(*self.api).vtable_).CThostFtdcTraderApi_ReqOrderInsert)(self.api, o, id) }
    }
}
impl Drop for InProcTd {
    fn drop(&mut self) {
        unsafe {
            ((*(*self.api).vtable_).CThostFtdcTraderApi_Release)(self.api);
            // 替身不释放 Ext/SPI；本测试自行回收（先 Release join 回调线程）。
            drop(Box::from_raw(self.ext));
            drop(Box::from_raw(self.spi));
        }
    }
}

#[test]
fn trader_inprocess_control_rejects_next_insert_with_code_30() {
    use ctp2rs::v1alpha1::*;
    let flow = "./td_reject_flow_inproc_";
    let (tx, rx) = std::sync::mpsc::sync_channel::<TE>(64);
    let spi = Box::into_raw(Box::new(LifeSpi { tx })) as *mut dyn TraderSpi;
    let td = InProcTd::create(flow, spi);
    td.init();
    wait(&rx, &TE::Connected, "connected");
    assert_eq!(td.authenticate(1), 0);
    wait(&rx, &TE::Authed, "authed");
    assert_eq!(td.login(2), 0);
    wait(&rx, &TE::Login, "login");
    assert_eq!(td.settle(3), 0);
    wait(&rx, &TE::Settled, "settled");

    // 同镜像进程内句柄按 flow key 取回实例并注入「下一笔拒单 30」。
    let front = ctpmock_td::ffi::td_api::control_for(flow).expect("in-process handle");
    front.reject_next(30);

    // 用「限价卖开」触发注入：SHFE 笼统平仓会被内置规则 15 先拦（见下条用例），
    // 开仓不受规则约束，注入的 30 才会生效。
    let mut o = CThostFtdcInputOrderField::default();
    put(&mut o.OrderRef, "9");
    put(&mut o.InstrumentID, "rb2510");
    o.Direction = '1' as i8;
    o.CombOffsetFlag[0] = '0' as i8;
    o.CombHedgeFlag[0] = '1' as i8;
    o.OrderPriceType = '2' as i8;
    o.TimeCondition = '3' as i8;
    o.LimitPrice = 3200.0;
    o.VolumeTotalOriginal = 99;
    assert_eq!(td.insert(&mut o, 10), 0);
    wait(&rx, &TE::InsertRej(30), "insert-rejected-30");

    drop(td);
}

/// L2：柜台内置 SHFE 规则（真实 ctp2rs 句柄 dlopen 替身）。
/// * SHFE 市价（AnyPrice+IOC）→ `OnRspOrderInsert` 80；
/// * SHFE 限价笼统平仓 Close='1' → 15；
/// * 非 SHFE 合约市价 → 正常受理成交（IF 前缀夹具 exchange=""，规则只正向命中 SHFE）。
#[test]
fn trader_shfe_builtin_rules_80_15_and_non_shfe_market_fills() {
    use ctp2rs::v1alpha1::*;
    let flow = "./td_shfe_rules_flow_inproc_";
    let (tx, rx) = std::sync::mpsc::sync_channel::<TE>(64);
    let spi = Box::into_raw(Box::new(LifeSpi { tx })) as *mut dyn TraderSpi;
    let td = InProcTd::create(flow, spi);
    td.init();
    wait(&rx, &TE::Connected, "connected");
    assert_eq!(td.authenticate(1), 0);
    wait(&rx, &TE::Authed, "authed");
    assert_eq!(td.login(2), 0);
    wait(&rx, &TE::Login, "login");
    assert_eq!(td.settle(3), 0);
    wait(&rx, &TE::Settled, "settled");

    // ① SHFE 市价买开：80（受理码仍 0，错误走 OnRspOrderInsert）。
    let mut mkt = CThostFtdcInputOrderField::default();
    put(&mut mkt.OrderRef, "1");
    put(&mut mkt.InstrumentID, "rb2510");
    mkt.Direction = '0' as i8;
    mkt.CombOffsetFlag[0] = '0' as i8;
    mkt.CombHedgeFlag[0] = '1' as i8;
    mkt.OrderPriceType = '1' as i8; // AnyPrice
    mkt.TimeCondition = '1' as i8; // IOC
    mkt.VolumeTotalOriginal = 2;
    assert_eq!(td.insert(&mut mkt, 10), 0);
    wait(&rx, &TE::InsertRej(80), "shfe-market-80");

    // ② SHFE 限价笼统平仓 Close='1'：15。
    let mut close = CThostFtdcInputOrderField::default();
    put(&mut close.OrderRef, "2");
    put(&mut close.InstrumentID, "rb2510");
    close.Direction = '1' as i8;
    close.CombOffsetFlag[0] = '1' as i8;
    close.CombHedgeFlag[0] = '1' as i8;
    close.OrderPriceType = '2' as i8;
    close.TimeCondition = '3' as i8;
    close.LimitPrice = 3200.0;
    close.VolumeTotalOriginal = 1;
    assert_eq!(td.insert(&mut close, 11), 0);
    wait(&rx, &TE::InsertRej(15), "shfe-close-15");

    // ③ 非 SHFE（IF 前缀夹具交易所非 SHFE）市价：直接受理成交。
    let mut mkt2 = CThostFtdcInputOrderField::default();
    put(&mut mkt2.OrderRef, "3");
    put(&mut mkt2.InstrumentID, "IF2510");
    mkt2.Direction = '0' as i8;
    mkt2.CombOffsetFlag[0] = '0' as i8;
    mkt2.CombHedgeFlag[0] = '1' as i8;
    mkt2.OrderPriceType = '1' as i8;
    mkt2.TimeCondition = '1' as i8;
    mkt2.VolumeTotalOriginal = 2;
    assert_eq!(td.insert(&mut mkt2, 12), 0);
    wait(&rx, &TE::RtnOrder('3'), "non-shfe-queued");
    wait(&rx, &TE::RtnTrade, "non-shfe-trade");
    wait(&rx, &TE::RtnOrder('0'), "non-shfe-all-traded");

    drop(td);
}

// ===== 任务 7：行情 Md 全生命周期 L2 =====

#[derive(Debug, PartialEq)]
enum ME2 {
    Connected,
    Login(i32),
    Sub(bool),
    Tick { last: f64, bid: f64, ask: f64 },
}

struct RecMd2 {
    tx: std::sync::mpsc::SyncSender<ME2>,
}
impl ctp2rs::v1alpha1::MdSpi for RecMd2 {
    fn on_front_connected(&mut self) {
        let _ = self.tx.send(ME2::Connected);
    }
    fn on_rsp_user_login(
        &mut self,
        _f: Option<&ctp2rs::v1alpha1::CThostFtdcRspUserLoginField>,
        info: Option<&ctp2rs::v1alpha1::CThostFtdcRspInfoField>,
        _id: i32,
        _last: bool,
    ) {
        let _ = self
            .tx
            .send(ME2::Login(info.map(|i| i.ErrorID).unwrap_or(0)));
    }
    fn on_rsp_sub_market_data(
        &mut self,
        _s: Option<&ctp2rs::v1alpha1::CThostFtdcSpecificInstrumentField>,
        _i: Option<&ctp2rs::v1alpha1::CThostFtdcRspInfoField>,
        _id: i32,
        is_last: bool,
    ) {
        let _ = self.tx.send(ME2::Sub(is_last));
    }
    fn on_rtn_depth_market_data(
        &mut self,
        d: Option<&ctp2rs::v1alpha1::CThostFtdcDepthMarketDataField>,
    ) {
        if let Some(d) = d {
            let _ = self.tx.send(ME2::Tick {
                last: d.LastPrice,
                bid: d.BidPrice1,
                ask: d.AskPrice1,
            });
        }
    }
}

fn wait_md(rx: &std::sync::mpsc::Receiver<ME2>, want: &ME2, tag: &str) {
    let ev = rx
        .recv_timeout(Duration::from_secs(3))
        .unwrap_or_else(|_| panic!("timeout {tag}"));
    assert_eq!(&ev, want, "at {tag}");
}

/// dlopen 路径 + 环境变量开启自动行情：订阅后应收到正价、bid<last<ask 的 tick。
#[test]
fn md_dlopen_auto_walk_pushes_valid_ticks() {
    use ctp2rs::v1alpha1::*;
    unsafe {
        std::env::set_var("CTP_MOCK_AUTO_TICK", "1");
        std::env::set_var("CTP_MOCK_AUTO_INTERVAL_MS", "20");
    }
    let so = locate_so("ctpmock_md");
    let api = MdApi::create_api(&so, "./md_auto_flow_", false, false, false);
    let (tx, rx) = std::sync::mpsc::sync_channel::<ME2>(256);
    let spi = Box::into_raw(Box::new(RecMd2 { tx })) as *mut dyn MdSpi;
    api.register_spi(spi);
    api.register_front("tcp://127.0.0.1:42213");
    api.init();
    wait_md(&rx, &ME2::Connected, "connected");
    assert_eq!(
        api.req_user_login(&mut CThostFtdcReqUserLoginField::default(), 1),
        0
    );
    wait_md(&rx, &ME2::Login(0), "login");
    assert_eq!(api.subscribe_market_data(&["rb2510"]), 0);
    wait_md(&rx, &ME2::Sub(true), "sub");
    // 自动游走：至少两帧，正价且盘口有序。
    for _ in 0..2 {
        match rx.recv_timeout(Duration::from_secs(3)).expect("auto tick") {
            ME2::Tick { last, bid, ask } => {
                assert!(last > 0.0);
                assert!(bid < last && last < ask, "bid {bid} < {last} < ask {ask}");
            }
            other => panic!("expected tick, got {other:?}"),
        }
    }
    unsafe { std::env::remove_var("CTP_MOCK_AUTO_TICK") };
    api.release();
    drop(unsafe { Box::from_raw(spi) });
}

/// 同镜像 rlib 路径：脚本精确 tick / 缺侧 / 登录拒绝 64（经 vtable 直接驱动）。
#[test]
fn md_inproc_script_tick_missing_side_and_login_64() {
    use ctp2rs::v1alpha1::*;
    let flow = "./md_script_flow_";
    let cflow = std::ffi::CString::new(flow).unwrap();
    let api =
        unsafe { ctpmock_md::ffi::md_api::create_md_api(cflow.as_ptr(), false, false, false) };
    let (tx, rx) = std::sync::mpsc::sync_channel::<ME2>(128);
    let spi = Box::into_raw(Box::new(RecMd2 { tx })) as *mut dyn MdSpi;
    let ext = Box::into_raw(Box::new(CThostFtdcMdSpiExt::new(spi)));
    unsafe {
        ((*(*api).vtable_).CThostFtdcMdApi_RegisterSpi)(api, ext as *mut CThostFtdcMdSpi);
        ((*(*api).vtable_).CThostFtdcMdApi_Init)(api);
    }
    wait_md(&rx, &ME2::Connected, "connected");

    // 登录拒绝 64。
    let front = ctpmock_md::ffi::md_api::control_for(flow).expect("md handle");
    front.set_login_reject(64);
    let code = unsafe {
        ((*(*api).vtable_).CThostFtdcMdApi_ReqUserLogin)(
            api,
            &mut CThostFtdcReqUserLoginField::default(),
            1,
        )
    };
    assert_eq!(code, 0);
    wait_md(&rx, &ME2::Login(64), "login64");
    front.set_login_reject(0);

    // 脚本精确 tick。
    ctpmock_md::ffi::md_api::push_tick(&front, "rb2510", 3210.0, 3209.0, 3211.0);
    wait_md(
        &rx,
        &ME2::Tick {
            last: 3210.0,
            bid: 3209.0,
            ask: 3211.0,
        },
        "script-tick",
    );
    // 缺买侧。
    front.missing_bid("rb2510", 3210.0, 3211.0);
    wait_md(
        &rx,
        &ME2::Tick {
            last: 3210.0,
            bid: f64::MAX,
            ask: 3211.0,
        },
        "missing-bid",
    );

    unsafe { ((*(*api).vtable_).CThostFtdcMdApi_Release)(api) };
    unsafe {
        drop(Box::from_raw(ext));
        drop(Box::from_raw(spi));
    }
}
