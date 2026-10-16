//! 交易 API 替身：`#[repr(C)]` 对象 + 白名单 vtable，内部持有内核 [`MockTdFront`]。
//!
//! ABI 契约（与 vendored ctp2rs bindgen 严格对齐）：
//! * 对象首字段（偏移 0）是 `*const CThostFtdcTraderApi__bindgen_vtable`，
//!   客户端经 `(*api).vtable_->Method(this, …)` 调用；
//! * vtable 先全字节清零，再**只具名覆盖**白名单槽（具名字段编译期校验），
//!   其余槽保持 null——白名单外调用即跳到 null 显式崩溃，而非静默走错逻辑；
//! * 客户端经 `RegisterSpi` 传入 `*mut CThostFtdcTraderSpiExt`，替身只保存、
//!   **绝不释放**（Ext 归 ctp2rs）；`Release` 先 join 回调线程再回收自身，
//!   确保 ctp2rs 在 Release 返回后释放 Ext 时无回调在飞。

use std::collections::HashMap;
use std::mem::{size_of, MaybeUninit};
use std::os::raw::{c_char, c_int};
use std::ptr::{self, addr_of_mut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use ctp_mock_core::sim::front::{FrontConfig, MockTdFront};

use ctp2rs::v1alpha1::{
    CThostFtdcInputOrderActionField, CThostFtdcInputOrderField, CThostFtdcQryInstrumentField,
    CThostFtdcQryInvestorPositionField, CThostFtdcQryOrderField, CThostFtdcQryTradeField,
    CThostFtdcReqAuthenticateField, CThostFtdcReqUserLoginField,
    CThostFtdcSettlementInfoConfirmField, CThostFtdcTraderApi,
    CThostFtdcTraderApi__bindgen_vtable as TraderVtable, CThostFtdcTraderSpi,
    CThostFtdcTraderSpiExt, THOST_TE_RESUME_TYPE,
};

/// 按 flow_path 索引的在实例注册表（Weak：不延长对象生命周期，Release 后自动失效）。
///
/// 供同进程 L2 与（任务 8）进程内控制句柄 `mock_control()` 取回前置实例，
/// 在 dlopen 路径之外设置成交脚本/错误注入/播种持仓等。
fn registry() -> &'static Mutex<HashMap<String, Weak<MockTdFront>>> {
    static REG: OnceLock<Mutex<HashMap<String, Weak<MockTdFront>>>> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(HashMap::new()))
}

/// 供 rlib 同进程调用：按构造时的 flow_path 取回前置实例（已 Release 则 None）。
pub fn control_for(flow_path: &str) -> Option<Arc<MockTdFront>> {
    registry()
        .lock()
        .unwrap()
        .get(flow_path)
        .and_then(Weak::upgrade)
}

/// 替身交易 API 对象，`vtable` 必须位于偏移 0。
#[repr(C)]
pub struct MockTdApi {
    vtable: *const TraderVtable,
    /// 释放幂等闸门。
    released: AtomicBool,
    /// 模拟器内核（握手 / 后续报单、查询、台账）。
    front: Arc<MockTdFront>,
}

// 内核自身为 Send + Sync（Arc 共享）；对象仅经 vtable 在 FFI 边界使用。
unsafe impl Send for MockTdApi {}
unsafe impl Sync for MockTdApi {}

/// 客户端所见的不透明 API 类型。
type TraderApiType = CThostFtdcTraderApi;

impl MockTdApi {
    /// 分配对象并装上白名单 vtable，返回堆指针（所有权移交 Release）。
    fn allocate(flow: &str) -> *mut TraderApiType {
        let front = MockTdFront::new(FrontConfig::default());
        // 同进程控制句柄（Weak，按 flow key）与全局实例目录（强引用，默认 key）。
        registry()
            .lock()
            .unwrap()
            .insert(flow.to_string(), Arc::downgrade(&front));
        ctp_mock_core::control::register_td(flow, &front);
        if let Some(port) = env_port("CTP_MOCK_CTRL_TD") {
            // 端口已占用（多实例/测试并发）时静默复用：首个实例已在服务。
            let _ = ctp_mock_core::control::serve_td(port);
        }
        let api = Box::new(Self {
            vtable: vtable(),
            released: AtomicBool::new(false),
            front,
        });
        Box::into_raw(api) as *mut TraderApiType
    }
}

/// 读取形如端口号的环境变量（缺省/非法返回 None）。
fn env_port(name: &str) -> Option<u16> {
    std::env::var(name).ok().and_then(|v| v.trim().parse().ok())
}

/// # Safety
/// `this` 必须来自本替身构造且尚未 `Release`。
unsafe fn state(this: *mut TraderApiType) -> &'static MockTdApi {
    &*(this as *const MockTdApi)
}

/// 全局唯一白名单 vtable（构造说明见模块 ABI 契约）。
fn vtable() -> *const TraderVtable {
    static VT: OnceLock<&'static TraderVtable> = OnceLock::new();
    let v: &&TraderVtable = VT.get_or_init(|| {
        let raw = Box::into_raw(Box::new(MaybeUninit::<TraderVtable>::uninit()));
        // SAFETY：raw 指向大小/对齐正确的未初始化内存；write_bytes 初始化全部字节为 0。
        unsafe {
            ptr::write_bytes(raw as *mut u8, 0, size_of::<TraderVtable>());
            let vp = raw as *mut TraderVtable;
            macro_rules! set {
                ($field:ident, $f:expr) => {
                    addr_of_mut!((*vp).$field).write($f)
                };
            }
            set!(CThostFtdcTraderApi_Release, td_release);
            set!(CThostFtdcTraderApi_Init, td_init);
            set!(CThostFtdcTraderApi_Join, td_join);
            set!(CThostFtdcTraderApi_GetTradingDay, td_get_trading_day);
            set!(CThostFtdcTraderApi_RegisterFront, td_register_front);
            set!(CThostFtdcTraderApi_RegisterSpi, td_register_spi);
            set!(
                CThostFtdcTraderApi_SubscribePrivateTopic,
                td_subscribe_private_topic
            );
            set!(
                CThostFtdcTraderApi_SubscribePublicTopic,
                td_subscribe_public_topic
            );
            set!(CThostFtdcTraderApi_ReqAuthenticate, td_req_authenticate);
            set!(CThostFtdcTraderApi_ReqUserLogin, td_req_user_login);
            set!(
                CThostFtdcTraderApi_ReqSettlementInfoConfirm,
                td_req_settlement_confirm
            );
            set!(CThostFtdcTraderApi_ReqOrderInsert, td_req_order_insert);
            set!(CThostFtdcTraderApi_ReqOrderAction, td_req_order_action);
            set!(CThostFtdcTraderApi_ReqQryOrder, td_req_qry_order);
            set!(CThostFtdcTraderApi_ReqQryTrade, td_req_qry_trade);
            set!(
                CThostFtdcTraderApi_ReqQryInvestorPosition,
                td_req_qry_position
            );
            set!(CThostFtdcTraderApi_ReqQryInstrument, td_req_qry_instrument);
            // 已全字节初始化并泄漏为 'static；未命名槽刻意保留 null。
            &*vp
        }
    });
    *v as *const TraderVtable
}

/// 读取 `flow_path` 裸 C 字符串（失败返回空串）。
fn cstr_to_string(p: *const c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    unsafe { std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned() }
}

// ---------------- 生命周期 / 配置 ----------------

unsafe extern "C" fn td_release(this: *mut TraderApiType) {
    let api = unsafe { state(this) };
    // 幂等：仅第一次先停回调线程（join），再回收对象。
    if !api.released.swap(true, Ordering::SeqCst) {
        // 以流前缀无法在此反查；Weak 随对象 drop 自动失效，注册表懒清理即可。
        api.front.shutdown();
        drop(unsafe { Box::from_raw(this as *mut MockTdApi) });
    }
}

unsafe extern "C" fn td_init(this: *mut TraderApiType) {
    let api = unsafe { state(this) };
    // 启动回调线程并异步 OnFrontConnected。
    api.front.start();
}

unsafe extern "C" fn td_join(_this: *mut TraderApiType) -> c_int {
    0
}

unsafe extern "C" fn td_get_trading_day(_this: *mut TraderApiType) -> *const c_char {
    static DAY: &[u8] = b"20261015\0";
    DAY.as_ptr() as *const c_char
}

unsafe extern "C" fn td_register_front(_this: *mut TraderApiType, _addr: *mut c_char) {
    // 模拟器不真正建连；前置地址仅作配置，无需保存。
}

unsafe extern "C" fn td_register_spi(this: *mut TraderApiType, spi: *mut CThostFtdcTraderSpi) {
    let api = unsafe { state(this) };
    // ctp2rs 以不透明 CThostFtdcTraderSpi* 传入 SpiExt*；只保存/清空，绝不释放。
    // null（Drop 路径）只清指针，线程回收在 Release 完成。
    api.front.set_spi(spi as *mut CThostFtdcTraderSpiExt);
}

unsafe extern "C" fn td_subscribe_private_topic(
    _this: *mut TraderApiType,
    _resume: THOST_TE_RESUME_TYPE,
    _seq_no: c_int,
) {
}

unsafe extern "C" fn td_subscribe_public_topic(
    _this: *mut TraderApiType,
    _resume: THOST_TE_RESUME_TYPE,
) {
}

// ---------------- 握手三步（任务 2） ----------------

unsafe extern "C" fn td_req_authenticate(
    this: *mut TraderApiType,
    req: *mut CThostFtdcReqAuthenticateField,
    request_id: c_int,
) -> c_int {
    let api = unsafe { state(this) };
    if req.is_null() {
        return -1;
    }
    api.front.authenticate(unsafe { &*req }, request_id)
}

unsafe extern "C" fn td_req_user_login(
    this: *mut TraderApiType,
    req: *mut CThostFtdcReqUserLoginField,
    request_id: c_int,
) -> c_int {
    let api = unsafe { state(this) };
    if req.is_null() {
        return -1;
    }
    api.front.login(unsafe { &*req }, request_id)
}

unsafe extern "C" fn td_req_settlement_confirm(
    this: *mut TraderApiType,
    req: *mut CThostFtdcSettlementInfoConfirmField,
    request_id: c_int,
) -> c_int {
    let api = unsafe { state(this) };
    if req.is_null() {
        return -1;
    }
    api.front.settlement_confirm(unsafe { &*req }, request_id)
}

// ---------------- 报单 / 撤单 / 查询（任务 3–6 接内核；当前为受理占位） ----------------

unsafe extern "C" fn td_req_order_insert(
    this: *mut TraderApiType,
    req: *mut CThostFtdcInputOrderField,
    request_id: c_int,
) -> c_int {
    let api = unsafe { state(this) };
    if req.is_null() {
        return -1;
    }
    api.front.insert_order(unsafe { &*req }, request_id)
}

unsafe extern "C" fn td_req_order_action(
    this: *mut TraderApiType,
    req: *mut CThostFtdcInputOrderActionField,
    request_id: c_int,
) -> c_int {
    let api = unsafe { state(this) };
    if req.is_null() {
        return -1;
    }
    api.front.cancel_order(unsafe { &*req }, request_id)
}

unsafe extern "C" fn td_req_qry_order(
    this: *mut TraderApiType,
    req: *mut CThostFtdcQryOrderField,
    request_id: c_int,
) -> c_int {
    let api = unsafe { state(this) };
    if req.is_null() {
        return -1;
    }
    api.front.query_orders(unsafe { &*req }, request_id)
}

unsafe extern "C" fn td_req_qry_trade(
    this: *mut TraderApiType,
    req: *mut CThostFtdcQryTradeField,
    request_id: c_int,
) -> c_int {
    let api = unsafe { state(this) };
    if req.is_null() {
        return -1;
    }
    api.front.query_trades(unsafe { &*req }, request_id)
}

unsafe extern "C" fn td_req_qry_position(
    this: *mut TraderApiType,
    req: *mut CThostFtdcQryInvestorPositionField,
    request_id: c_int,
) -> c_int {
    let api = unsafe { state(this) };
    if req.is_null() {
        return -1;
    }
    api.front.query_positions(unsafe { &*req }, request_id)
}

unsafe extern "C" fn td_req_qry_instrument(
    this: *mut TraderApiType,
    req: *mut CThostFtdcQryInstrumentField,
    request_id: c_int,
) -> c_int {
    let api = unsafe { state(this) };
    if req.is_null() {
        return -1;
    }
    api.front.query_instruments(unsafe { &*req }, request_id)
}

/// 创建替身交易 API（union/评测版签名 `(flow_path, is_production)`）。
///
/// # Safety
/// `flow_path` 必须是以 NUL 结尾的 C 字符串；返回对象只能经其 vtable 访问，
/// 最终由 `Release` 回收。
pub unsafe extern "C" fn create_trader_api(
    flow_path: *const c_char,
    _is_production: bool,
) -> *mut TraderApiType {
    MockTdApi::allocate(&cstr_to_string(flow_path))
}
