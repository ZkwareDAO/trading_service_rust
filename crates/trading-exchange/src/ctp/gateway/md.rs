//! MdApi 行情网关：生命周期、SPI→事件转发与订阅。
//!
//! 与 [`crate::trader`] 同型：柜台行情回调线程 → mpsc → 主循环；
//! 回调内即时拷贝 owned 快照，不跨回调持有 C++ 指针。

use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::time::Duration;

use crate::ctp::core::account::CtpAccount;
use crate::ctp::core::encoding::decode_fixed;
use crate::ctp::core::error::{CtpError, Result};
use crate::ctp::core::quote::TickSnapshot;
use ctp2rs::v1alpha1::{MdApi, MdSpi};

/// 行情侧事件。
#[derive(Debug, Clone)]
pub enum MdEvent {
    /// 行情前置连接建立。
    FrontConnected,
    /// 行情前置断开。
    FrontDisconnected(i32),
    /// 行情登录应答；错误码 0=通过。
    LoggedIn { code: i32, msg: String },
    /// 订阅应答；逐符号回调，instrument=合约号，错误码 0=成功。
    Subscribed {
        instrument: String,
        code: i32,
        msg: String,
    },
    /// 一笔行情 tick。
    Tick(TickSnapshot),
    /// 错误。
    Error { code: i32, msg: String },
}

/// 事件转发 SPI。
struct EventMdSpi {
    tx: SyncSender<MdEvent>,
}

impl EventMdSpi {
    fn emit(&self, event: MdEvent) {
        let _ = self.tx.send(event);
    }
}

fn read_rsp(info: Option<&ctp2rs::v1alpha1::CThostFtdcRspInfoField>) -> (i32, String) {
    info.map(|i| (i.ErrorID, decode_fixed(&i.ErrorMsg)))
        .unwrap_or((0, String::new()))
}

impl MdSpi for EventMdSpi {
    fn on_front_connected(&mut self) {
        self.emit(MdEvent::FrontConnected);
    }

    fn on_front_disconnected(&mut self, reason: i32) {
        self.emit(MdEvent::FrontDisconnected(reason));
    }

    fn on_rsp_user_login(
        &mut self,
        _login: Option<&ctp2rs::v1alpha1::CThostFtdcRspUserLoginField>,
        info: Option<&ctp2rs::v1alpha1::CThostFtdcRspInfoField>,
        _request_id: i32,
        _is_last: bool,
    ) {
        let (code, msg) = read_rsp(info);
        self.emit(MdEvent::LoggedIn { code, msg });
    }

    fn on_rsp_sub_market_data(
        &mut self,
        specific: Option<&ctp2rs::v1alpha1::CThostFtdcSpecificInstrumentField>,
        info: Option<&ctp2rs::v1alpha1::CThostFtdcRspInfoField>,
        _request_id: i32,
        _is_last: bool,
    ) {
        // 逐符号订阅应答：带上合约号，便于定位「整批订阅中某些符号被柜台静默拒绝」。
        let instrument = specific
            .map(|s| decode_fixed(&s.InstrumentID))
            .unwrap_or_default();
        let (code, msg) = read_rsp(info);
        self.emit(MdEvent::Subscribed {
            instrument,
            code,
            msg,
        });
    }

    fn on_rsp_error(
        &mut self,
        info: Option<&ctp2rs::v1alpha1::CThostFtdcRspInfoField>,
        _request_id: i32,
        _is_last: bool,
    ) {
        let (code, msg) = read_rsp(info);
        self.emit(MdEvent::Error { code, msg });
    }

    fn on_rtn_depth_market_data(
        &mut self,
        data: Option<&ctp2rs::v1alpha1::CThostFtdcDepthMarketDataField>,
    ) {
        if let Some(d) = data {
            self.emit(MdEvent::Tick(TickSnapshot {
                instrument: decode_fixed(&d.InstrumentID),
                last_price: d.LastPrice,
                bid_price: d.BidPrice1,
                bid_volume: d.BidVolume1,
                ask_price: d.AskPrice1,
                ask_volume: d.AskVolume1,
                volume: d.Volume,
                update_time: decode_fixed(&d.UpdateTime),
                update_millisec: d.UpdateMillisec,
            }));
        }
    }
}

/// 一个行情会话的高层句柄。
pub struct MdHandle {
    /// 柜台行情 API（自带 Drop 释放）。
    api: MdApi,
    /// 事件接收端。
    events: Receiver<MdEvent>,
    /// 行情登录形态（匿名 / 密码=授权码，venue.toml 配置）。
    md_anon: bool,
    md_pass_is_authcode: bool,
    /// 账户参数（行情登录填写 BrokerID/UserID/Password）。
    account: CtpAccount,
    /// 自定义 SPI 裸指针；Drop 时在柜台线程退出后回收。
    spi_raw: *mut dyn MdSpi,
}

impl MdHandle {
    /// 校验账户、打开动态库并完成注册，随后初始化连接。
    ///
    /// `is_production_mode`：评测版 API（v6.7.11+）连仿真环境时传 `false`。
    pub fn connect(
        account: &CtpAccount,
        md_front: &str,
        lib_path: &str,
        flow_path: &str,
        is_production_mode: bool,
        md_anon: bool,
        md_pass_is_authcode: bool,
    ) -> Result<Self> {
        account
            .validate()
            .map_err(|e| CtpError::Other(e.to_string()))?;

        // union 版创建：不使用 UDP/组播。
        let api = MdApi::create_api(lib_path, flow_path, false, false, is_production_mode);

        let (tx, rx): (SyncSender<MdEvent>, Receiver<MdEvent>) = sync_channel(4096);
        let spi_raw = Box::into_raw(Box::new(EventMdSpi { tx })) as *mut dyn MdSpi;

        api.register_spi(spi_raw);
        // 行情前置与交易前置端口分离（如瑞达 42213/42205），由部署配置传入。
        api.register_front(md_front);
        api.init();

        Ok(Self {
            api,
            events: rx,
            md_anon,
            md_pass_is_authcode,
            account: CtpAccount {
                broker_id: account.broker_id.clone(),
                user_id: account.user_id.clone(),
                password: account.password.clone(),
                app_id: account.app_id.clone(),
                auth_code: account.auth_code.clone(),
                investor_id: account.investor_id.clone(),
                front_address: account.front_address.clone(),
                is_production_mode: account.is_production_mode,
            },
            spi_raw,
        })
    }

    /// 等待下一个行情事件；超时返回 `None`。
    pub fn next_event(&mut self, timeout: Duration) -> Option<MdEvent> {
        match self.events.recv_timeout(timeout) {
            Ok(event) => Some(event),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => None,
        }
    }

    /// 发起行情登录。
    ///
    /// 登录凭据形态（部分仿真/期货公司行情前置开启了终端采集，标准匿名会被拒）：
    /// - 默认：BrokerID/UserID/Password 用交易账户；
    /// - `md_anon`：全空匿名（标准 CTP 行情前置形态，venue.toml 可配）；
    /// - `md_pass_is_authcode`：Password 字段填认证码（部分柜台行情鉴权形态）。
    pub fn login(&mut self) -> i32 {
        let mut req = ctp2rs::v1alpha1::CThostFtdcReqUserLoginField::default();
        let mode = self.md_anon;
        let pass_is_auth = self.md_pass_is_authcode;
        if mode {
            crate::ctp::core::encoding::encode_fixed(&mut req.BrokerID, "");
            crate::ctp::core::encoding::encode_fixed(&mut req.UserID, "");
            crate::ctp::core::encoding::encode_fixed(&mut req.Password, "");
        } else {
            crate::ctp::core::encoding::encode_fixed(&mut req.BrokerID, &self.account.broker_id);
            crate::ctp::core::encoding::encode_fixed(&mut req.UserID, &self.account.user_id);
            if pass_is_auth {
                crate::ctp::core::encoding::encode_fixed(
                    &mut req.Password,
                    &self.account.auth_code,
                );
            } else {
                crate::ctp::core::encoding::encode_fixed(&mut req.Password, &self.account.password);
            }
        }
        // 看穿式采集要求客户端产品信息标识；部分柜台缺失该字段会拒登录。
        crate::ctp::core::encoding::encode_fixed(&mut req.UserProductInfo, "ctp-rust-mvp");
        self.api.req_user_login(&mut req, 1)
    }

    /// 订阅行情；结果经 `MdEvent::Subscribed` 应答、tick 经 `MdEvent::Tick` 推送。
    pub fn subscribe(&self, instruments: &[&str]) -> i32 {
        self.api.subscribe_market_data(instruments)
    }

    /// 显式释放。
    pub fn release(&self) {
        self.api.release();
    }
}

impl Drop for MdHandle {
    fn drop(&mut self) {
        // SAFETY: `spi_raw` 由 `connect()` 中 `Box::into_raw` 创建，仅在此回收一次；
        // 先 `release()` 等柜台线程退出，再回收 SPI，避免悬垂回调。
        self.api.release();
        unsafe {
            let _ = Box::from_raw(self.spi_raw);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MdEvent, MdHandle};
    use crate::ctp::core::account::CtpAccount;

    fn account() -> CtpAccount {
        CtpAccount {
            broker_id: "9999".into(),
            user_id: "666666".into(),
            password: "p".into(),
            app_id: "a".into(),
            auth_code: "c".into(),
            investor_id: "666666".into(),
            front_address: "tcp://101.226.250.133:42213".into(),
            is_production_mode: false,
        }
    }

    #[test]
    fn connect_rejects_invalid_account_before_opening_library() {
        let mut bad = account();
        bad.broker_id = String::new();
        let r = MdHandle::connect(
            &bad,
            "tcp://127.0.0.1:1",
            "/nonexistent/x.so",
            "./md_",
            false,
            false,
            false,
        );
        assert!(r.is_err());
    }

    #[test]
    fn md_event_variants_constructible() {
        let _ = MdEvent::FrontConnected;
        let _ = MdEvent::Subscribed {
            instrument: String::new(),
            code: 0,
            msg: String::new(),
        };
    }
}
