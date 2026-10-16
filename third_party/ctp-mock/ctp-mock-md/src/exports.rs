//! Itanium mangled 导出符号（Linux / union=v6.7.13 评测版）。

use std::os::raw::c_char;

use ctp2rs::v1alpha1::CThostFtdcMdApi;
use ctp_mock_core::MOCK_VERSION_C;

use crate::ffi::md_api;

/// `CThostFtdcMdApi::CreateFtdcMdApi(const char*, bool, bool, bool)`（union 版）。
#[export_name = "_ZN15CThostFtdcMdApi15CreateFtdcMdApiEPKcbbb"]
pub unsafe extern "C" fn create_ftdc_md_api(
    flow_path: *const c_char,
    udp: bool,
    multicast: bool,
    is_production: bool,
) -> *mut CThostFtdcMdApi {
    // SAFETY: 契约见 md_api::create_md_api。
    unsafe { md_api::create_md_api(flow_path, udp, multicast, is_production) }
}

/// `CThostFtdcMdApi::GetApiVersion()`。
#[export_name = "_ZN15CThostFtdcMdApi13GetApiVersionEv"]
pub unsafe extern "C" fn get_api_version() -> *const c_char {
    MOCK_VERSION_C.as_ptr() as *const c_char
}
