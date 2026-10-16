//! Itanium mangled 导出符号（Linux / union=v6.7.13 评测版）。
//!
//! 符号名与官方 `thosttraderapi_se.so` 完全一致，ctp2rs 的
//! `TDAPI_CREATE_API_SYMBOL` / `TDAPI_GET_API_VERSION_SYMBOL` 据此 dlsym。
//! 生产/非评测版（构造符号无 `b` 后缀）不在本期支持范围（见 plan §4、§15）。

use std::os::raw::c_char;

use ctp2rs::v1alpha1::CThostFtdcTraderApi;
use ctp_mock_core::MOCK_VERSION_C;

use crate::ffi::td_api;

/// `CThostFtdcTraderApi::CreateFtdcTraderApi(const char*, bool)`（union 版）。
///
#[export_name = "_ZN19CThostFtdcTraderApi19CreateFtdcTraderApiEPKcb"]
pub unsafe extern "C" fn create_ftdc_trader_api(
    flow_path: *const c_char,
    is_production: bool,
) -> *mut CThostFtdcTraderApi {
    // SAFETY: 见 td_api::create_trader_api 的契约；此处仅转发。
    unsafe { td_api::create_trader_api(flow_path, is_production) }
}

/// `CThostFtdcTraderApi::GetApiVersion()`：返回进程生命周期内有效的静态 C 串。
#[export_name = "_ZN19CThostFtdcTraderApi13GetApiVersionEv"]
pub unsafe extern "C" fn get_api_version() -> *const c_char {
    MOCK_VERSION_C.as_ptr() as *const c_char
}
