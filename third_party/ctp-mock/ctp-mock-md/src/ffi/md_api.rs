//! 行情 API 替身：`#[repr(C)]` 对象 + 白名单 vtable，内部持有内核 [`MockMdFront`]。
//!
//! ABI 契约同 td_api：vtable 全字节清零后只具名覆盖白名单槽；只保存 SpiExt 不释放；
//! Release 先 join 回调/自动线程再回收自身。flow_path 作为同进程控制句柄的 key。

use std::collections::HashMap;
use std::mem::{size_of, MaybeUninit};
use std::os::raw::{c_char, c_int};
use std::ptr::{self, addr_of_mut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use ctp_mock_core::sim::market::Quote;
use ctp_mock_core::sim::md_front::MockMdFront;

use ctp2rs::v1alpha1::{
    CThostFtdcMdApi, CThostFtdcMdApi__bindgen_vtable as MdVtable, CThostFtdcMdSpi,
    CThostFtdcMdSpiExt, CThostFtdcReqUserLoginField,
};

type MdApiType = CThostFtdcMdApi;

/// 替身行情 API 对象，vtable 必须在偏移 0。
#[repr(C)]
pub struct MockMdApi {
    vtable: *const MdVtable,
    released: AtomicBool,
    front: Arc<MockMdFront>,
}

unsafe impl Send for MockMdApi {}
unsafe impl Sync for MockMdApi {}

/// flow_path → 在实例（Weak）注册表，供同进程控制句柄按 key 取回。
fn registry() -> &'static Mutex<HashMap<String, Weak<MockMdFront>>> {
    static REG: OnceLock<Mutex<HashMap<String, Weak<MockMdFront>>>> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(HashMap::new()))
}

/// 同进程控制句柄：按构造 flow_path 取回行情前置（已 Release 则 None）。
pub fn control_for(flow_path: &str) -> Option<Arc<MockMdFront>> {
    registry()
        .lock()
        .unwrap()
        .get(flow_path)
        .and_then(Weak::upgrade)
}

impl MockMdApi {
    fn allocate(flow: &str) -> *mut MdApiType {
        let front = MockMdFront::new("20261015");
        registry()
            .lock()
            .unwrap()
            .insert(flow.to_string(), Arc::downgrade(&front));
        ctp_mock_core::control::register_md(flow, &front);
        if let Some(port) = env_port("CTP_MOCK_CTRL_MD") {
            let _ = ctp_mock_core::control::serve_md(port);
        }
        let api = Box::new(Self {
            vtable: vtable(),
            released: AtomicBool::new(false),
            front,
        });
        Box::into_raw(api) as *mut MdApiType
    }
}

/// 读取形如端口号的环境变量（缺省/非法返回 None）。
fn env_port(name: &str) -> Option<u16> {
    std::env::var(name).ok().and_then(|v| v.trim().parse().ok())
}

/// # Safety：`this` 必须来自本替身构造且未 Release。
unsafe fn state(this: *mut MdApiType) -> &'static MockMdApi {
    &*(this as *const MockMdApi)
}

fn vtable() -> *const MdVtable {
    static VT: OnceLock<&'static MdVtable> = OnceLock::new();
    let v: &&MdVtable = VT.get_or_init(|| {
        let raw = Box::into_raw(Box::new(MaybeUninit::<MdVtable>::uninit()));
        // SAFETY：全字节清零后具名覆盖白名单槽；其余槽保持 null（同 td 契约）。
        unsafe {
            ptr::write_bytes(raw as *mut u8, 0, size_of::<MdVtable>());
            let vp = raw as *mut MdVtable;
            macro_rules! set {
                ($field:ident, $f:expr) => {
                    addr_of_mut!((*vp).$field).write($f)
                };
            }
            set!(CThostFtdcMdApi_Release, md_release);
            set!(CThostFtdcMdApi_Init, md_init);
            set!(CThostFtdcMdApi_Join, md_join);
            set!(CThostFtdcMdApi_GetTradingDay, md_get_trading_day);
            set!(CThostFtdcMdApi_RegisterFront, md_register_front);
            set!(CThostFtdcMdApi_RegisterSpi, md_register_spi);
            set!(
                CThostFtdcMdApi_SubscribeMarketData,
                md_subscribe_market_data
            );
            set!(CThostFtdcMdApi_ReqUserLogin, md_req_user_login);
            &*vp
        }
    });
    *v as *const MdVtable
}

fn cstr_to_string(p: *const c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    unsafe { std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned() }
}

/// 读取 `char**` 合约数组为 owned 列表。
fn read_instruments(pp: *mut *mut c_char, count: c_int) -> Vec<String> {
    let mut out = Vec::new();
    if pp.is_null() || count <= 0 {
        return out;
    }
    for i in 0..count as isize {
        let p = unsafe { *pp.offset(i) };
        let s = cstr_to_string(p);
        if !s.is_empty() {
            out.push(s);
        }
    }
    out
}

/// 是否对订阅集自动游走（env CTP_MOCK_AUTO_TICK 非空且非 0/false）。
fn auto_enabled() -> bool {
    matches!(
        std::env::var("CTP_MOCK_AUTO_TICK").ok().as_deref(),
        Some(v) if !matches!(v, "" | "0" | "false" | "no")
    )
}

/// 自动游走间隔（env CTP_MOCK_AUTO_INTERVAL_MS，默认 500，下限 10ms）。
fn auto_interval_ms() -> u64 {
    std::env::var("CTP_MOCK_AUTO_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500)
        .max(10)
}

unsafe extern "C" fn md_release(this: *mut MdApiType) {
    let api = unsafe { state(this) };
    if !api.released.swap(true, Ordering::SeqCst) {
        api.front.shutdown();
        drop(unsafe { Box::from_raw(this as *mut MockMdApi) });
    }
}

unsafe extern "C" fn md_init(this: *mut MdApiType) {
    let api = unsafe { state(this) };
    api.front.start();
}

unsafe extern "C" fn md_join(_this: *mut MdApiType) -> c_int {
    0
}

unsafe extern "C" fn md_get_trading_day(_this: *mut MdApiType) -> *const c_char {
    static DAY: &[u8] = b"20261015\0";
    DAY.as_ptr() as *const c_char
}

unsafe extern "C" fn md_register_front(_this: *mut MdApiType, _addr: *mut c_char) {}

unsafe extern "C" fn md_register_spi(this: *mut MdApiType, spi: *mut CThostFtdcMdSpi) {
    let api = unsafe { state(this) };
    api.front.set_spi(spi as *mut CThostFtdcMdSpiExt);
}

unsafe extern "C" fn md_subscribe_market_data(
    this: *mut MdApiType,
    pp_instrument: *mut *mut c_char,
    count: c_int,
) -> c_int {
    let api = unsafe { state(this) };
    let syms = read_instruments(pp_instrument, count);
    let code = api.front.subscribe(&syms);
    // 服务启动即全天行情：订阅生效后对订阅集自动游走（确定性、LastPrice>0）。
    if auto_enabled() {
        for s in &syms {
            let base = fixture_base(s);
            api.front.start_auto(s, base, auto_interval_ms());
        }
    }
    code
}

unsafe extern "C" fn md_req_user_login(
    this: *mut MdApiType,
    req: *mut CThostFtdcReqUserLoginField,
    request_id: c_int,
) -> c_int {
    let api = unsafe { state(this) };
    if req.is_null() {
        return -1;
    }
    api.front.login(unsafe { &*req }, request_id)
}

/// 合约代码 → 自动游走基准价的静态夹具（仅保证为正、字段非空）。
fn fixture_base(instrument: &str) -> f64 {
    // 简单确定性映射：字母前缀散列到 1000..5000，数字尾不影响量级。
    let h: u64 = instrument
        .bytes()
        .take(4)
        .fold(7u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
    1000.0 + (h % 4000) as f64
}

/// 供同镜像测试/控制面直接推一帧精确行情。
pub fn push_tick(front: &Arc<MockMdFront>, instrument: &str, last: f64, bid: f64, ask: f64) {
    front.tick(instrument, "", Quote::exact(last, bid, ask));
}

/// 创建替身行情 API（union 签名 `(flow, udp, multicast, is_production)`）。
///
/// # Safety
/// `flow_path` 必须是以 NUL 结尾的 C 字符串；对象仅经 vtable 访问，最终由 Release 回收。
pub unsafe extern "C" fn create_md_api(
    flow_path: *const c_char,
    _udp: bool,
    _multicast: bool,
    _is_production: bool,
) -> *mut MdApiType {
    MockMdApi::allocate(&cstr_to_string(flow_path))
}
