//! MdApi 行情网关：生命周期、SPI→事件转发与订阅。
//!
//! 与 [`crate::trader`] 同型：柜台行情回调线程 → mpsc → 主循环；
//! 回调内即时拷贝 owned 快照，不跨回调持有 C++ 指针。

use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::time::Duration;

use crate::ctp::core::account::CtpAccount;
use crate::ctp::core::encoding::decode_fixed;
use crate::ctp::core::error::{CtpError, Result};
use crate::ctp::core::quote::{QuoteLevel, TickSnapshot};
use ctp2rs::v1alpha1::{CThostFtdcDepthMarketDataField as Depth, MdApi, MdSpi};

/// 柜台用 `DBL_MAX` 哨兵表示「无此价」；转换时归一为 0.0，避免把 f64::MAX 带进内存/落盘。
fn norm_price(p: f64) -> f64 {
    if p.is_finite() && p < f64::MAX {
        p
    } else {
        0.0
    }
}

/// FFI 深度行情 → 安全快照（纯函数：日 OHLC + 五档 + K线聚合字段逐档拷贝）。
///
/// 文档铁律：`day_open/high/low/close` 是 CTP **当日累计**的日开高低收
/// （盘中 close 常为 0），**不是** 1m bar 的 OHLC；1m OHLC 由 candle 聚合得到。
fn depth_to_snapshot(d: &Depth) -> TickSnapshot {
    let lvl = |price: f64, volume: i32| QuoteLevel::new(norm_price(price), volume);
    TickSnapshot {
        instrument: decode_fixed(&d.InstrumentID),
        exchange_id: decode_fixed(&d.ExchangeID),
        last_price: norm_price(d.LastPrice),
        bids: [
            lvl(d.BidPrice1, d.BidVolume1),
            lvl(d.BidPrice2, d.BidVolume2),
            lvl(d.BidPrice3, d.BidVolume3),
            lvl(d.BidPrice4, d.BidVolume4),
            lvl(d.BidPrice5, d.BidVolume5),
        ],
        asks: [
            lvl(d.AskPrice1, d.AskVolume1),
            lvl(d.AskPrice2, d.AskVolume2),
            lvl(d.AskPrice3, d.AskVolume3),
            lvl(d.AskPrice4, d.AskVolume4),
            lvl(d.AskPrice5, d.AskVolume5),
        ],
        day_open: norm_price(d.OpenPrice),
        day_high: norm_price(d.HighestPrice),
        day_low: norm_price(d.LowestPrice),
        day_close: norm_price(d.ClosePrice),
        volume: d.Volume,
        update_time: decode_fixed(&d.UpdateTime),
        update_millisec: d.UpdateMillisec,
        trading_day: decode_fixed(&d.TradingDay),
        action_day: decode_fixed(&d.ActionDay),
        turnover: d.Turnover,
        open_interest: d.OpenInterest,
    }
}

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
    /// 一笔行情 tick（Box：快照含五档+日OHLC，体积大，避免 enum 各变体尺寸悬殊）。
    Tick(Box<TickSnapshot>),
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
            self.emit(MdEvent::Tick(Box::new(depth_to_snapshot(d))));
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

    /// 退订行情（会话内可多次调用，用于 K线期望集动态收缩）。
    pub fn unsubscribe(&self, instruments: &[&str]) -> i32 {
        self.api.unsubscribe_market_data(instruments)
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

    use ctp2rs::v1alpha1::CThostFtdcDepthMarketDataField as Depth;

    fn encode(buf: &mut [i8], text: &str) {
        crate::ctp::core::encoding::encode_fixed(buf, text);
    }

    #[test]
    fn depth_to_snapshot_carries_daily_ohlc_and_five_levels() {
        let mut d = Depth::default();
        encode(&mut d.InstrumentID, "cu2512");
        encode(&mut d.ExchangeID, "SHFE");
        encode(&mut d.TradingDay, "20261027");
        encode(&mut d.ActionDay, "20261027");
        encode(&mut d.UpdateTime, "09:46:05");
        d.UpdateMillisec = 123;
        d.LastPrice = 74_500.0;
        // 日 OHLC（当日累计，非 1m）。
        d.OpenPrice = 74_100.0;
        d.HighestPrice = 74_800.0;
        d.LowestPrice = 74_000.0;
        d.ClosePrice = 0.0; // 盘中 close 常为 0，如实保留
        d.Volume = 1234;
        d.Turnover = 9.0e8;
        d.OpenInterest = 12_181.0;
        // 五档：价/量逐档。
        let bp = [74_490.0, 74_480.0, 74_470.0, 74_460.0, 74_450.0];
        let bv = [11, 12, 13, 14, 15];
        let ap = [74_510.0, 74_520.0, 74_530.0, 74_540.0, 74_550.0];
        let av = [21, 22, 23, 24, 25];
        d.BidPrice1 = bp[0];
        d.BidPrice2 = bp[1];
        d.BidPrice3 = bp[2];
        d.BidPrice4 = bp[3];
        d.BidPrice5 = bp[4];
        d.BidVolume1 = bv[0];
        d.BidVolume2 = bv[1];
        d.BidVolume3 = bv[2];
        d.BidVolume4 = bv[3];
        d.BidVolume5 = bv[4];
        d.AskPrice1 = ap[0];
        d.AskPrice2 = ap[1];
        d.AskPrice3 = ap[2];
        d.AskPrice4 = ap[3];
        d.AskPrice5 = ap[4];
        d.AskVolume1 = av[0];
        d.AskVolume2 = av[1];
        d.AskVolume3 = av[2];
        d.AskVolume4 = av[3];
        d.AskVolume5 = av[4];

        let s = super::depth_to_snapshot(&d);

        assert_eq!(s.instrument, "cu2512");
        assert_eq!(s.exchange_id, "SHFE");
        assert_eq!(s.trading_day, "20261027");
        assert_eq!(s.action_day, "20261027");
        assert_eq!(s.update_time, "09:46:05");
        assert_eq!(s.update_millisec, 123);
        assert_eq!(s.last_price, 74_500.0);
        assert_eq!(s.volume, 1234);
        assert_eq!(s.open_interest, 12_181.0);
        // 日 OHLC 透传。
        assert_eq!(s.day_open, 74_100.0);
        assert_eq!(s.day_high, 74_800.0);
        assert_eq!(s.day_low, 74_000.0);
        assert_eq!(s.day_close, 0.0);
        // 一档（bids[0]/asks[0]）与五档全量。
        assert_eq!(s.bids[0].price, 74_490.0);
        assert_eq!(s.bids[0].volume, 11);
        assert_eq!(s.asks[0].price, 74_510.0);
        for i in 0..5 {
            assert_eq!(s.bids[i].price, bp[i]);
            assert_eq!(s.bids[i].volume, bv[i]);
            assert_eq!(s.asks[i].price, ap[i]);
            assert_eq!(s.asks[i].volume, av[i]);
        }
    }

    #[test]
    fn depth_to_snapshot_normalizes_dbl_max_and_zero_sentinels() {
        // 柜台用 DBL_MAX 表示无对手价/无价：转换后归一为 0，不把 f64::MAX 带进快照。
        let d = Depth {
            LastPrice: f64::MAX,
            BidPrice1: f64::MAX,
            AskPrice1: f64::MAX,
            OpenPrice: f64::MAX,
            HighestPrice: 0.0,
            ..Default::default()
        };
        let s = super::depth_to_snapshot(&d);
        assert_eq!(s.last_price, 0.0);
        assert_eq!(s.bids[0].price, 0.0);
        assert_eq!(s.asks[0].price, 0.0);
        assert_eq!(s.day_open, 0.0);
        assert_eq!(s.day_high, 0.0);
    }
}
