//! 行情前置运行时：单回调线程 + 登录/订阅 + 自动游走 / 脚本 tick / 缺侧。
//!
//! 与 [`super::front`] 同型：请求线程做同步闸门，回报帧在单一回调线程上经
//! 客户端 `CThostFtdcMdSpiExt.spi_ptr`（`*mut dyn MdSpi`）派发；只保存 Ext，绝不释放。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicPtr, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use ctp2rs::v1alpha1::{
    CThostFtdcDepthMarketDataField, CThostFtdcMdSpiExt, CThostFtdcReqUserLoginField,
    CThostFtdcSpecificInstrumentField, MdSpi,
};

use super::frames::rsp_info;
use super::market::{depth_frame, Quote, Walk};
use crate::fields::{beijing_hms, now_ts_secs, put_fixed};

type Callback = Box<dyn FnOnce(&mut dyn MdSpi) + Send + 'static>;

struct SpiCell(AtomicPtr<CThostFtdcMdSpiExt>);
unsafe impl Send for SpiCell {}
unsafe impl Sync for SpiCell {}

struct SpiRef(*const AtomicPtr<CThostFtdcMdSpiExt>);
unsafe impl Send for SpiRef {}

/// 自动游走任务的句柄。
struct AutoJob {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// 行情前置模拟器。
pub struct MockMdFront {
    trading_day: String,
    logged_in: AtomicBool,
    /// 下一次行情登录的一次性/持续拒绝码（64 等；0=通过）。
    login_reject: AtomicI32,
    subscriptions: Mutex<Vec<String>>,
    /// 合约 → 交易所（回报帧填充，未知给空串）。
    exchanges: Mutex<HashMap<String, String>>,

    spi: SpiCell,
    tx: Mutex<Option<Sender<Callback>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
    autos: Mutex<HashMap<String, AutoJob>>,
    started: AtomicBool,
}

impl MockMdFront {
    pub fn new(trading_day: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            trading_day: trading_day.into(),
            logged_in: AtomicBool::new(false),
            login_reject: AtomicI32::new(0),
            subscriptions: Mutex::new(Vec::new()),
            exchanges: Mutex::new(HashMap::new()),
            spi: SpiCell(AtomicPtr::new(std::ptr::null_mut())),
            tx: Mutex::new(None),
            worker: Mutex::new(None),
            autos: Mutex::new(HashMap::new()),
            started: AtomicBool::new(false),
        })
    }

    pub fn set_spi(&self, spi: *mut CThostFtdcMdSpiExt) {
        self.spi.0.store(spi, Ordering::Release);
    }

    fn post<F>(&self, cb: F)
    where
        F: FnOnce(&mut dyn MdSpi) + Send + 'static,
    {
        if let Ok(g) = self.tx.lock() {
            if let Some(tx) = g.as_ref() {
                let _ = tx.send(Box::new(cb));
            }
        }
    }

    fn run(rx: Receiver<Callback>, spi: SpiRef) {
        while let Ok(cb) = rx.recv() {
            let ext = unsafe { &*spi.0 }.load(Ordering::Acquire);
            if ext.is_null() {
                continue;
            }
            // SAFETY：ext 归 ctp2rs 所有，API 生命周期内有效；单回调线程独占调用。
            let dyn_spi = unsafe { &mut *(*ext).spi_ptr };
            cb(dyn_spi);
        }
    }

    /// `Init`：启动回调线程并异步 OnFrontConnected。幂等。
    pub fn start(&self) {
        if self.started.swap(true, Ordering::SeqCst) {
            return;
        }
        let (tx, rx) = channel::<Callback>();
        let spi_ref = SpiRef(&self.spi.0 as *const AtomicPtr<CThostFtdcMdSpiExt>);
        let h = thread::Builder::new()
            .name("ctp-mock-md-cb".into())
            .spawn(move || Self::run(rx, spi_ref))
            .expect("spawn ctp-mock-md callback thread");
        *self.tx.lock().unwrap() = Some(tx);
        *self.worker.lock().unwrap() = Some(h);
        self.post(|spi| spi.on_front_connected());
    }

    /// 控制面：断线（保留订阅/自动配置）。
    pub fn disconnect(&self, reason: i32) {
        self.logged_in.store(false, Ordering::Release);
        self.post(move |spi| spi.on_front_disconnected(reason));
    }

    /// 重连：仅重发 OnFrontConnected（worker 常驻）。
    pub fn reconnect(&self) {
        self.post(|spi| spi.on_front_connected());
    }

    /// 行情登录：返回 0 受理；异步成功或按注入码（如 64）拒绝。
    pub fn login(&self, _req: &CThostFtdcReqUserLoginField, request_id: i32) -> i32 {
        let code = self.login_reject.load(Ordering::Relaxed);
        let day = self.trading_day.clone();
        if code != 0 {
            // 持续注入；一次登录拒绝不改变登录态，客户端可重连重试。
            self.post(move |spi| {
                let info = rsp_info(code, "客户端未认证");
                spi.on_rsp_user_login(None, Some(&info), request_id, true);
            });
        } else {
            self.logged_in.store(true, Ordering::Release);
            self.post(move |spi| {
                let mut f = ctp2rs::v1alpha1::CThostFtdcRspUserLoginField::default();
                put_fixed(&mut f.TradingDay, &day);
                put_fixed(&mut f.LoginTime, "09:00:00");
                spi.on_rsp_user_login(Some(&f), None, request_id, true);
            });
        }
        0
    }

    /// 订阅：登记合约集，逐合约异步回报订阅成功。
    pub fn subscribe(&self, instruments: &[String]) -> i32 {
        let n = instruments.len() as i32;
        {
            let mut subs = self.subscriptions.lock().unwrap();
            for i in instruments {
                if !subs.contains(i) {
                    subs.push(i.clone());
                }
            }
        }
        let total = instruments.len();
        for (idx, inst) in instruments.iter().enumerate() {
            let inst = inst.clone();
            let is_last = idx + 1 == total;
            self.post(move |spi| {
                let mut spec = CThostFtdcSpecificInstrumentField::default();
                put_fixed(&mut spec.InstrumentID, &inst);
                spi.on_rsp_sub_market_data(Some(&spec), None, n, is_last);
            });
        }
        0
    }

    /// 当前订阅合约快照。
    pub fn subscriptions(&self) -> Vec<String> {
        self.subscriptions.lock().unwrap().clone()
    }

    /// 注入登录拒绝码（64 等；0 取消）。
    pub fn set_login_reject(&self, code: i32) {
        self.login_reject.store(code, Ordering::Relaxed);
    }

    // ---------- 脚本行情 ----------

    /// 精确推一帧（脚本模式）。
    pub fn tick(&self, instrument: &str, exchange_id: &str, q: Quote) {
        let frame = self.build_frame(instrument, exchange_id, q);
        self.post(move |spi| spi.on_rtn_depth_market_data(Some(&frame)));
    }

    /// 造缺侧：缺买侧（bid=f64::MAX）。
    pub fn missing_bid(&self, instrument: &str, last: f64, ask: f64) {
        self.tick(
            instrument,
            &self.exchange_of(instrument),
            Quote::missing_bid(last, ask),
        );
    }

    /// 造缺侧：缺卖侧（ask=f64::MAX）。
    pub fn missing_ask(&self, instrument: &str, last: f64, bid: f64) {
        self.tick(
            instrument,
            &self.exchange_of(instrument),
            Quote::missing_ask(last, bid),
        );
    }

    fn exchange_of(&self, instrument: &str) -> String {
        self.exchanges
            .lock()
            .unwrap()
            .get(instrument)
            .cloned()
            .unwrap_or_default()
    }

    fn build_frame(
        &self,
        instrument: &str,
        exchange_id: &str,
        q: Quote,
    ) -> CThostFtdcDepthMarketDataField {
        depth_frame(
            instrument,
            exchange_id,
            &q,
            &self.trading_day,
            &beijing_hms(now_ts_secs()),
            millisec(),
        )
    }

    // ---------- 自动游走 ----------

    /// 对某合约启动/重启自动游走线程（确定性小幅报价）。
    pub fn start_auto(self: &Arc<Self>, instrument: &str, base: f64, interval_ms: u64) {
        self.stop_auto(instrument);
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();
        let this = Arc::clone(self);
        let inst = instrument.to_string();
        let tick_size = 1.0_f64.max((base * 0.0005).round()); // 约 5‱ 一步
        let h = thread::Builder::new()
            .name(format!("ctp-mock-md-auto-{instrument}"))
            .spawn(move || {
                let mut walk = Walk::new(base, tick_size);
                while !stop2.load(Ordering::Relaxed) {
                    let q = walk.next_quote();
                    this.auto_push(&inst, q);
                    sleep_brief(interval_ms.max(10), &stop2);
                }
            })
            .expect("spawn auto tick thread");
        self.autos.lock().unwrap().insert(
            instrument.to_string(),
            AutoJob {
                stop,
                handle: Some(h),
            },
        );
    }

    /// 停止某合约的自动游走（若在运行）。
    pub fn stop_auto(&self, instrument: &str) {
        if let Some(mut job) = self.autos.lock().unwrap().remove(instrument) {
            job.stop.store(true, Ordering::Relaxed);
            if let Some(h) = job.handle.take() {
                let _ = h.join();
            }
        }
    }

    /// 自动线程用：构造帧并投递到回调线程。
    fn auto_push(&self, instrument: &str, q: Quote) {
        let frame = self.build_frame(instrument, &self.exchange_of(instrument), q);
        self.post(move |spi| spi.on_rtn_depth_market_data(Some(&frame)));
    }

    /// 关停回调线程与全部自动任务（幂等）。
    pub fn shutdown(&self) {
        // 先停自动任务。
        let names: Vec<String> = self.autos.lock().unwrap().keys().cloned().collect();
        for n in names {
            self.stop_auto(&n);
        }
        if !self.started.swap(false, Ordering::SeqCst) {
            return;
        }
        if let Ok(mut g) = self.tx.lock() {
            drop(g.take());
        }
        if let Ok(mut g) = self.worker.lock() {
            if let Some(h) = g.take() {
                let _ = h.join();
            }
        }
    }
}

impl Drop for MockMdFront {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// 当前毫秒部分（0..999）。
fn millisec() -> i32 {
    use chrono::Utc;
    (Utc::now().timestamp_subsec_nanos() / 1_000_000) as i32
}

/// 可被 stop 提前唤醒的短睡眠（按 10ms 分片，关停更快）。
fn sleep_brief(total_ms: u64, stop: &AtomicBool) {
    let mut left = total_ms;
    while left > 0 && !stop.load(Ordering::Relaxed) {
        let step = left.min(10);
        thread::sleep(std::time::Duration::from_millis(step));
        left = left.saturating_sub(step);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fields::get_fixed;
    use ctp2rs::v1alpha1::CThostFtdcRspInfoField;
    use std::sync::mpsc::SyncSender;

    #[derive(Debug, PartialEq)]
    enum ME {
        Connected,
        Login(i32),
        Sub(String, bool),
        Tick(f64, f64, f64),
    }

    struct RecMdSpi {
        tx: SyncSender<ME>,
    }
    impl MdSpi for RecMdSpi {
        fn on_front_connected(&mut self) {
            let _ = self.tx.send(ME::Connected);
        }
        fn on_rsp_user_login(
            &mut self,
            _f: Option<&ctp2rs::v1alpha1::CThostFtdcRspUserLoginField>,
            info: Option<&CThostFtdcRspInfoField>,
            _id: i32,
            _last: bool,
        ) {
            let _ = self
                .tx
                .send(ME::Login(info.map(|i| i.ErrorID).unwrap_or(0)));
        }
        fn on_rsp_sub_market_data(
            &mut self,
            spec: Option<&CThostFtdcSpecificInstrumentField>,
            _i: Option<&CThostFtdcRspInfoField>,
            _id: i32,
            is_last: bool,
        ) {
            if let Some(s) = spec {
                let _ = self.tx.send(ME::Sub(get_fixed(&s.InstrumentID), is_last));
            }
        }
        fn on_rtn_depth_market_data(&mut self, d: Option<&CThostFtdcDepthMarketDataField>) {
            if let Some(d) = d {
                let _ = self
                    .tx
                    .send(ME::Tick(d.LastPrice, d.BidPrice1, d.AskPrice1));
            }
        }
    }

    struct Harness {
        front: Arc<MockMdFront>,
        rx: std::sync::mpsc::Receiver<ME>,
        spi: *mut dyn MdSpi,
        ext: *mut CThostFtdcMdSpiExt,
    }
    impl Harness {
        fn recv(&self, tag: &str) -> ME {
            self.rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap_or_else(|_| panic!("timeout {tag}"))
        }
    }
    impl Drop for Harness {
        fn drop(&mut self) {
            self.front.shutdown();
            unsafe {
                drop(Box::from_raw(self.spi));
                drop(Box::from_raw(self.ext));
            }
        }
    }

    fn spawn() -> Harness {
        let front = MockMdFront::new("20261015");
        let (tx, rx) = std::sync::mpsc::sync_channel::<ME>(256);
        let spi = Box::into_raw(Box::new(RecMdSpi { tx })) as *mut dyn MdSpi;
        let ext = Box::into_raw(Box::new(CThostFtdcMdSpiExt::new(spi)));
        front.set_spi(ext);
        front.start();
        Harness {
            front,
            rx,
            spi,
            ext,
        }
    }

    #[test]
    fn connect_login_subscribe_succeeds() {
        let h = spawn();
        assert_eq!(h.recv("conn"), ME::Connected);
        assert_eq!(h.front.login(&CThostFtdcReqUserLoginField::default(), 1), 0);
        assert_eq!(h.recv("login"), ME::Login(0));

        assert_eq!(
            h.front
                .subscribe(&["rb2510".to_string(), "IF2510".to_string()]),
            0
        );
        assert_eq!(h.recv("sub1"), ME::Sub("rb2510".into(), false));
        assert_eq!(h.recv("sub2"), ME::Sub("IF2510".into(), true));
        let subs = h.front.subscriptions();
        assert_eq!(subs, vec!["rb2510".to_string(), "IF2510".to_string()]);
    }

    #[test]
    fn login_reject_64_emits_error() {
        let h = spawn();
        h.recv("conn");
        h.front.set_login_reject(64);
        assert_eq!(h.front.login(&CThostFtdcReqUserLoginField::default(), 1), 0);
        assert_eq!(h.recv("login64"), ME::Login(64));
        // 未进入登录态：可继续重连（重发 Connected）。
        h.front.reconnect();
        assert_eq!(h.recv("reconn"), ME::Connected);
    }

    #[test]
    fn script_tick_emits_exact_quote() {
        let h = spawn();
        h.recv("conn");
        h.front
            .tick("rb2510", "SHFE", Quote::exact(3210.0, 3209.0, 3211.0));
        assert_eq!(h.recv("tick"), ME::Tick(3210.0, 3209.0, 3211.0));
    }

    #[test]
    fn missing_bid_and_ask_use_f64_max() {
        let h = spawn();
        h.recv("conn");
        h.front
            .tick("rb2510", "SHFE", Quote::missing_bid(3210.0, 3211.0));
        assert_eq!(h.recv("mb"), ME::Tick(3210.0, f64::MAX, 3211.0));
        h.front
            .tick("rb2510", "SHFE", Quote::missing_ask(3210.0, 3209.0));
        assert_eq!(h.recv("ma"), ME::Tick(3210.0, 3209.0, f64::MAX));
    }

    #[test]
    fn auto_walk_pushes_positive_quotes_then_stops() {
        let h = spawn();
        h.recv("conn");
        h.front.start_auto("rb2510", 3200.0, 30);
        // 至少收到一帧自动行情。
        let q = h.recv("auto");
        match q {
            ME::Tick(last, bid, ask) => {
                assert!(last > 0.0);
                assert!(bid < last && last < ask);
            }
            other => panic!("unexpected {other:?}"),
        }
        // 关停自动任务后不再来帧（排空后等待短暂窗口确认为空）。
        h.front.stop_auto("rb2510");
        std::thread::sleep(std::time::Duration::from_millis(80));
        while h.rx.try_recv().is_ok() {}
        assert!(h
            .rx
            .recv_timeout(std::time::Duration::from_millis(80))
            .is_err());
    }
}
