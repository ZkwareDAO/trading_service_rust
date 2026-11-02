//! 行情前置运行时：单回调线程 + 登录/订阅 + 自动游走 / 脚本 tick / 缺侧。
//!
//! 与 [`super::front`] 同型：请求线程做同步闸门，回报帧在单一回调线程上经
//! 客户端 `CThostFtdcMdSpiExt.spi_ptr`（`*mut dyn MdSpi`）派发；只保存 Ext，绝不释放。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicPtr, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use ctp2rs::v1alpha1::{
    CThostFtdcDepthMarketDataField, CThostFtdcMdSpiExt, CThostFtdcReqUserLoginField,
    CThostFtdcSpecificInstrumentField, MdSpi,
};

use super::frames::rsp_info;
use super::market::{depth_frame, depth_frame_from_hist, BookQuote, Quote, Walk};
use super::replay::{self, index_day, locate_config, next_delay, TickFileReader, TickReplayConfig};
use crate::fields::{beijing_day, beijing_hms, now_ts_secs, put_fixed};

type Callback = Box<dyn FnOnce(&mut dyn MdSpi) + Send + 'static>;

/// 逐 tick 推送日志的统一 target（便于 `CTP_MOCK_LOG=ctp_mock_core::tick=debug` 精确过滤）。
const TICK_LOG_TARGET: &str = "ctp_mock_core::tick";

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

/// 一个历史文件回放任务的参数（线程句柄同 [`AutoJob`]）。
pub struct ReplayJobSpec {
    /// 历史 tick CSV 路径（15 列、带表头）。
    pub path: PathBuf,
    /// >0：忽略时间戳按定速毫秒推送；0：按相邻 created_at 差值/speed。
    pub interval_ms: u64,
    /// 时间戳差回放倍速。
    pub speed: f64,
    /// 相邻 tick 等待上限。
    pub max_gap_ms: u64,
    /// 播完是否从头循环。
    pub r#loop: bool,
}

/// 回放任务句柄。
struct ReplayJob {
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
    /// 合约 → 历史文件回放任务。
    replays: Mutex<HashMap<String, ReplayJob>>,
    /// 已加载的回放配置（None＝未启用/未发现配置文件）。
    replay_cfg: Mutex<Option<ReplayConfig>>,
    started: AtomicBool,
}

/// 已解析并落地为绝对根的回放配置（运行时常驻，供订阅惰性启动/reload）。
struct ReplayConfig {
    /// 配置文件路径（reload 时重读同一文件）。
    config_path: PathBuf,
    /// 每个回放任务共用的默认参数。
    defaults: ReplayDefaults,
    /// 合约 → 历史文件索引。
    index: replay::DayIndex,
    /// 合约白名单（空＝全部）。
    whitelist: Vec<String>,
}

/// 与单文件无关、可复用到每个任务的回放默认参数。
#[derive(Clone, Copy)]
struct ReplayDefaults {
    interval_ms: u64,
    speed: f64,
    max_gap_ms: u64,
    r#loop: bool,
}

impl MockMdFront {
    pub fn new(trading_day: impl Into<String>) -> Arc<Self> {
        // 按 CTP_MOCK_LOG 安装一次日志（未设置则空操作，默认静默）。
        crate::logging::init();
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
            replays: Mutex::new(HashMap::new()),
            replay_cfg: Mutex::new(None),
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
        log::debug!(
            target: TICK_LOG_TARGET,
            "脚本推 tick {instrument}({exchange_id}) last={} bid={} ask={}",
            q.last, q.bid, q.ask
        );
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
        // 自动游走与历史回放互斥。
        self.stop_replay(instrument);
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
        log::debug!(
            target: TICK_LOG_TARGET,
            "自动游走推 tick {instrument} last={} bid={} ask={}",
            q.last, q.bid, q.ask
        );
        let frame = self.build_frame(instrument, &self.exchange_of(instrument), q);
        self.post(move |spi| spi.on_rtn_depth_market_data(Some(&frame)));
    }

    // ---------- 历史文件回放 ----------

    /// 启动/重启某合约的历史 tick 回放；文件打不开时**同步返回错误且不起线程**。
    pub fn start_replay(self: &Arc<Self>, instrument: &str, spec: ReplayJobSpec) -> std::io::Result<()> {
        self.stop_replay(instrument);
        // 先探测一次：坏路径/坏表头在此返回，避免静默起一个空线程。
        // 真正推送时线程会重新打开文件从头读。
        drop(TickFileReader::open(&spec.path)?);

        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();
        let this = Arc::clone(self);
        let inst = instrument.to_string();
        let defaults = ReplayDefaults {
            interval_ms: spec.interval_ms,
            speed: spec.speed,
            max_gap_ms: spec.max_gap_ms,
            r#loop: spec.r#loop,
        };
        let path = spec.path.clone();
        let h = thread::Builder::new()
            .name(format!("ctp-mock-md-replay-{instrument}"))
            .spawn(move || Self::run_replay(&this, &inst, path, defaults, &stop2))
            .expect("spawn replay thread");
        self.replays
            .lock()
            .unwrap()
            .insert(instrument.to_string(), ReplayJob { stop, handle: Some(h) });
        Ok(())
    }

    /// 停止某合约的回放（若在跑）。
    pub fn stop_replay(&self, instrument: &str) {
        if let Some(mut job) = self.replays.lock().unwrap().remove(instrument) {
            job.stop.store(true, Ordering::Relaxed);
            if let Some(h) = job.handle.take() {
                let _ = h.join();
            }
        }
    }

    /// 回放线程主体：流式逐行 → 盖当前墙钟构帧 → 投递；可循环、可被 stop 中断。
    fn run_replay(this: &Arc<Self>, inst: &str, path: PathBuf, d: ReplayDefaults, stop: &AtomicBool) {
        loop {
            let mut reader = match TickFileReader::open(&path) {
                Ok(r) => r,
                // start_replay 已探测过；走到这里说明文件在启动后被删/不可读，记录后结束任务。
                Err(e) => {
                    eprintln!("ctp-mock md 回放 {inst} 打开 {} 失败，停止回放: {e}", path.display());
                    return;
                }
            };
            let mut prev_ts: Option<i64> = None;
            // 绝对时间轴：epoch 固定于本轮文件起点，planned 是相对它的累计计划毫秒。
            // 每笔只累加不重排，因此睡眠过冲不会逐笔累积成整段漂移。
            let epoch = Instant::now();
            let mut planned_ms: u64 = 0;
            loop {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                let tick = match reader.next_tick() {
                    Ok(Some(t)) => t,
                    // EOF：按 loop 决定重头或退出；读错误记录后结束本任务。
                    Ok(None) => break,
                    Err(e) => {
                        eprintln!("ctp-mock md 回放 {inst} 读 {} 失败，停止回放: {e}", path.display());
                        return;
                    }
                };
                // 首帧立即（prev_ts=None 标志本文件尚未推过，循环重读时同样立即）；
                // 其后定速，或按相邻时间戳差（截断上限）。
                planned_ms = advance_planned(
                    planned_ms,
                    prev_ts,
                    tick.ts_ms,
                    d.interval_ms,
                    d.speed,
                    d.max_gap_ms,
                );
                if planned_ms > 0 {
                    sleep_until(epoch + Duration::from_millis(planned_ms), stop);
                }
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                prev_ts = Some(tick.ts_ms);
                this.replay_push(inst, &tick);
            }
            if !d.r#loop || stop.load(Ordering::Relaxed) {
                return;
            }
        }
    }

    /// 历史行 → SimNow 帧（数据取文件、时间盖推送当下）并投递到回调线程。
    fn replay_push(&self, instrument: &str, tick: &replay::HistTick) {
        let book = BookQuote::from_hist(tick);
        let now = now_ts_secs();
        log::debug!(
            target: TICK_LOG_TARGET,
            "历史回放推 tick {instrument}({}) last={} bid={:?} ask={:?} vol={} turnover={} oi={}",
            tick.exchange, tick.last, tick.bid, tick.ask, book.volume, tick.cum_amount, tick.cum_position
        );
        let frame = depth_frame_from_hist(
            instrument,
            &tick.exchange,
            &book,
            &beijing_day(now),
            &beijing_hms(now),
            millisec(),
        );
        self.post(move |spi| spi.on_rtn_depth_market_data(Some(&frame)));
    }

    /// 从当前工作目录发现配置文件并加载索引（无配置＝静默不启用）。
    pub fn discover_replay(self: &Arc<Self>) -> Result<(), String> {
        let cwd = std::env::current_dir().map_err(|e| format!("取 cwd 失败: {e}"))?;
        let Some(path) = locate_config(&cwd) else {
            return Ok(());
        };
        self.load_replay_config_file(&path)
    }

    /// 读指定配置文件 → 校验 → 解析绝对数据根 → 索引交易日，落地为运行时配置。
    pub fn load_replay_config_file(
        self: &Arc<Self>,
        path: &std::path::Path,
    ) -> Result<(), String> {
        let cfg = TickReplayConfig::load(path)?;
        if !cfg.enabled {
            // 显式关闭：清掉旧配置并停全部回放。
            *self.replay_cfg.lock().unwrap() = None;
            self.stop_all_replays();
            return Ok(());
        }
        cfg.validate()?;
        let config_dir = path.parent().map(|p| p.to_path_buf()).unwrap_or_default();
        let root = cfg.resolve_data_root(&config_dir);
        let index = index_day(&root, &cfg.kind, &cfg.date).map_err(|e| format!("索引失败: {e}"))?;
        let loaded = ReplayConfig {
            config_path: path.to_path_buf(),
            defaults: ReplayDefaults {
                interval_ms: cfg.interval_ms,
                speed: cfg.speed,
                max_gap_ms: cfg.max_gap_ms,
                r#loop: cfg.r#loop,
            },
            index,
            whitelist: cfg.instruments,
        };
        *self.replay_cfg.lock().unwrap() = Some(loaded);
        Ok(())
    }

    /// 订阅后惰性调用：配置允许且索引到文件则启动回放（回放优先于自动游走）。
    /// 返回 true 表示已由历史回放接管（调用方不应再起自动游走）。
    pub fn ensure_replay_for(self: &Arc<Self>, instrument: &str) -> bool {
        let spec = {
            let guard = self.replay_cfg.lock().unwrap();
            let Some(cfg) = guard.as_ref() else {
                return false;
            };
            // 白名单空＝全部；否则须命中。
            if !cfg.whitelist.is_empty() && !cfg.whitelist.iter().any(|s| s == instrument) {
                return false;
            }
            let Some(path) = cfg.index.files.get(instrument) else {
                return false;
            };
            ReplayJobSpec {
                path: path.clone(),
                interval_ms: cfg.defaults.interval_ms,
                speed: cfg.defaults.speed,
                max_gap_ms: cfg.defaults.max_gap_ms,
                r#loop: cfg.defaults.r#loop,
            }
        };
        // 回放与自动游走互斥：命中回放则停掉该合约的自动游走。
        self.stop_auto(instrument);
        match self.start_replay(instrument, spec) {
            Ok(()) => true,
            // 打不开文件：不致命，回落到不回放（订阅本身已成功），交调用方决定是否自动游走。
            Err(_) => false,
        }
    }

    /// 控制面 reload：重读配置/重建索引，并按当前订阅集（重）启动、停掉不再合格的任务。
    /// 返回（重）启动的合约数。
    pub fn reload_replay(self: &Arc<Self>) -> Result<usize, String> {
        let path = {
            let guard = self.replay_cfg.lock().unwrap();
            guard.as_ref().map(|c| c.config_path.clone())
        };
        let Some(path) = path else {
            return Err("尚未发现 ctp-mock.toml（无可 reload 的配置）".into());
        };
        self.load_replay_config_file(&path)?;
        let subs = self.subscriptions();
        // 先停掉全部，再按新配置重启，确保摘除的合约停止。
        self.stop_all_replays();
        let mut n = 0;
        for s in subs {
            if self.ensure_replay_for(&s) {
                n += 1;
            }
        }
        Ok(n)
    }

    /// 停掉全部回放任务（reload/禁用/shutdown 用）。
    fn stop_all_replays(&self) {
        let names: Vec<String> = self.replays.lock().unwrap().keys().cloned().collect();
        for n in names {
            self.stop_replay(&n);
        }
    }

    /// 关停回调线程与全部自动任务/回放任务（幂等）。
    pub fn shutdown(&self) {
        // 先停自动任务。
        let names: Vec<String> = self.autos.lock().unwrap().keys().cloned().collect();
        for n in names {
            self.stop_auto(&n);
        }
        // 再停历史回放。
        self.stop_all_replays();
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

/// 在已排期的累计毫秒上追加本 tick 的计划等待（首帧/循环重读整点不等待）。
///
/// 回放用「epoch + 累计计划毫秒」的**绝对 deadline** 调度，而不是逐笔
/// `sleep` 相对等待：`thread::sleep` 每次都存在毫秒级过冲，逐笔累积会在
/// 长回放中产生秒级漂移（实测 3 分钟漂 ~1.9s，30 分钟将错分钟桶）。
fn advance_planned(
    planned_ms: u64,
    prev_ts: Option<i64>,
    ts: i64,
    interval_ms: u64,
    speed: f64,
    max_gap_ms: u64,
) -> u64 {
    let wait = if interval_ms > 0 {
        if prev_ts.is_none() {
            0
        } else {
            interval_ms
        }
    } else {
        next_delay(prev_ts, ts, speed, max_gap_ms)
    };
    planned_ms.saturating_add(wait)
}

/// 睡到绝对 deadline（10ms 分片以便 stop 快速生效）；已过期立即返回。
fn sleep_until(deadline: Instant, stop: &AtomicBool) {
    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let now = Instant::now();
        if now >= deadline {
            return;
        }
        thread::sleep((deadline - now).min(Duration::from_millis(10)));
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
        /// 完整 tick：最新/买/卖 + 交易所 + 交易日 + 更新时分秒（回放断言用）。
        TickFull {
            last: f64,
            bid: f64,
            ask: f64,
            exchange: String,
            day: String,
            hms: String,
        },
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
                let _ = self.tx.send(ME::TickFull {
                    last: d.LastPrice,
                    bid: d.BidPrice1,
                    ask: d.AskPrice1,
                    exchange: get_fixed(&d.ExchangeID),
                    day: get_fixed(&d.TradingDay),
                    hms: get_fixed(&d.UpdateTime),
                });
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

    /// 从 TickFull 取 (last, bid, ask) 价量三元组（旧用例只关心价量）。
    fn prices(e: ME) -> (f64, f64, f64) {
        match e {
            ME::TickFull { last, bid, ask, .. } => (last, bid, ask),
            other => panic!("expected tick, got {other:?}"),
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
        assert_eq!(prices(h.recv("tick")), (3210.0, 3209.0, 3211.0));
    }

    #[test]
    fn missing_bid_and_ask_use_f64_max() {
        let h = spawn();
        h.recv("conn");
        h.front
            .tick("rb2510", "SHFE", Quote::missing_bid(3210.0, 3211.0));
        assert_eq!(prices(h.recv("mb")), (3210.0, f64::MAX, 3211.0));
        h.front
            .tick("rb2510", "SHFE", Quote::missing_ask(3210.0, 3209.0));
        assert_eq!(prices(h.recv("ma")), (3210.0, 3209.0, f64::MAX));
    }

    #[test]
    fn auto_walk_pushes_positive_quotes_then_stops() {
        let h = spawn();
        h.recv("conn");
        h.front.start_auto("rb2510", 3200.0, 30);
        // 至少收到一帧自动行情。
        let q = h.recv("auto");
        match q {
            ME::TickFull { last, bid, ask, .. } => {
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

    // ---- 历史文件回放 ----

    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    static REPLAY_SEQ: AtomicU64 = AtomicU64::new(0);

    struct Tmp(PathBuf);
    fn tmp() -> Tmp {
        let n = REPLAY_SEQ.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!(
            "ctpmock-mdreplay-{}-{}-{n}",
            std::process::id(),
            std::file!().replace('/', "_")
        ));
        std::fs::create_dir_all(&p).unwrap();
        Tmp(p)
    }
    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    const HIST_HEADER: &str = "\u{feff}exchange,symbol,open,high,low,price,cum_volume,cum_amount,last_volume,last_amount,trade_type,cum_position,quotes,created_at,type";

    /// 造一行 CU2706/SHFE 历史 tick（历史日固定 2026-10-08，买/卖围绕最新价）。
    fn hist_line(last: f64, hms: &str) -> String {
        format!(
            "SHFE,CU2706,1.0,2.0,1.0,{last},3,5.0,1,5.0,7,9,\"[{{'bid_p': {bid}, 'bid_v': 1, 'ask_p': {ask}, 'ask_v': 2}}]\",2026-10-08 {hms}+08:00,14\n",
            bid = last - 10.0,
            ask = last + 10.0
        )
    }

    fn write_hist(dir: &std::path::Path, name: &str, lasts: &[f64]) -> PathBuf {
        let mut s = String::from(HIST_HEADER);
        s.push('\n');
        for (i, p) in lasts.iter().enumerate() {
            s.push_str(&hist_line(*p, &format!("09:00:{i:02}")));
        }
        let path = dir.join(name);
        std::fs::write(&path, s).unwrap();
        path
    }

    /// 尽快、不循环的回放规格（定速 1ms，避开真实时间戳等待）。
    fn fast_spec(path: std::path::PathBuf, loop_: bool) -> ReplayJobSpec {
        ReplayJobSpec {
            path,
            interval_ms: 1,
            speed: 1.0,
            max_gap_ms: 30_000,
            r#loop: loop_,
        }
    }

    #[test]
    fn replay_pushes_rows_with_current_wallclock_day() {
        let h = spawn();
        h.recv("conn");
        let t = tmp();
        let path = write_hist(&t.0, "CU2706.csv", &[100.0, 110.0, 120.0]);
        h.front
            .start_replay("CU2706", fast_spec(path, false))
            .unwrap();

        for expect in [100.0, 110.0, 120.0] {
            match h.recv("replay") {
                ME::TickFull {
                    last,
                    exchange,
                    day,
                    ..
                } => {
                    assert_eq!(last, expect);
                    assert_eq!(exchange, "SHFE"); // 交易所取自数据
                    // 时间盖成推送当下：交易日＝今天北京日，而非历史 20261008。
                    assert_eq!(day, crate::fields::beijing_day(crate::fields::now_ts_secs()));
                    assert_ne!(day, "20261008");
                }
                other => panic!("unexpected {other:?}"),
            }
        }
        h.front.stop_replay("CU2706");
    }

    #[test]
    fn replay_loops_back_to_start() {
        let h = spawn();
        h.recv("conn");
        let t = tmp();
        let path = write_hist(&t.0, "CU2706.csv", &[100.0, 110.0]);
        h.front.start_replay("CU2706", fast_spec(path, true)).unwrap();
        // 两轮共 4 帧，顺序回绕。
        for expect in [100.0, 110.0, 100.0, 110.0] {
            assert_eq!(prices(h.recv("loop")), (expect, expect - 10.0, expect + 10.0));
        }
        h.front.stop_replay("CU2706");
    }

    #[test]
    fn replay_stop_halts_frames() {
        let h = spawn();
        h.recv("conn");
        let t = tmp();
        let path = write_hist(&t.0, "CU2706.csv", &[100.0, 110.0, 120.0, 130.0, 140.0]);
        h.front.start_replay("CU2706", fast_spec(path, true)).unwrap();
        let _ = h.recv("first");
        h.front.stop_replay("CU2706");
        std::thread::sleep(std::time::Duration::from_millis(80));
        while h.rx.try_recv().is_ok() {}
        assert!(h
            .rx
            .recv_timeout(std::time::Duration::from_millis(80))
            .is_err());
    }

    #[test]
    fn start_replay_replaces_existing_job_and_bad_path_errors() {
        let h = spawn();
        h.recv("conn");
        let t = tmp();
        // 不存在的文件：打开即错。
        let bad = t.0.join("nope.csv");
        assert!(h
            .front
            .start_replay("CU2706", fast_spec(bad, false))
            .is_err());

        // 同合约二次启动替换旧任务：第二份文件的值生效，旧任务不再推 100。
        let path_a = write_hist(&t.0, "a.csv", &[100.0]);
        let path_b = write_hist(&t.0, "b.csv", &[222.0]);
        h.front
            .start_replay("CU2706", fast_spec(path_a, true))
            .unwrap();
        assert_eq!(prices(h.recv("a")).0, 100.0);
        h.front
            .start_replay("CU2706", fast_spec(path_b, true))
            .unwrap();
        // 排空在飞旧帧后，应稳定只收到 b 的 222（连续 5 帧均为 222 即替换成功）。
        for _ in 0..12 {
            while h.rx.try_recv().is_ok() {}
            let last = prices(h.recv("b")).0;
            if last == 222.0 {
                for _ in 0..4 {
                    assert_eq!(prices(h.recv("b2")).0, 222.0);
                }
                h.front.stop_replay("CU2706");
                return;
            }
        }
        h.front.stop_replay("CU2706");
        panic!("第二个回放任务未接管");
    }

    #[test]
    fn config_subscribe_then_ensure_replay_pushes_indexed_file() {
        let h = spawn();
        h.recv("conn");
        let t = tmp();
        // 数据树：tmp/Tick/YYYYMM/YYYYMD/CU2706.csv。
        let day_dir = t.0.join("Tick/202610/20261008");
        std::fs::create_dir_all(&day_dir).unwrap();
        std::fs::write(day_dir.join("CU2706.csv"), {
            let mut s = String::from(HIST_HEADER);
            s.push('\n');
            s.push_str(&hist_line(500.0, "09:00:00"));
            s.push_str(&hist_line(501.0, "09:00:01"));
            s
        })
        .unwrap();
        // 配置就在临时目录；data_root 相对配置目录＝"."，定速 1ms、不循环。
        let cfg_path = t.0.join("ctp-mock.toml");
        std::fs::write(
            &cfg_path,
            "[tick_replay]\nenabled = true\ndata_root = \".\"\ninterval_ms = 1\nloop = false\n",
        )
        .unwrap();
        h.front.load_replay_config_file(&cfg_path).unwrap();
        h.front.subscribe(&["CU2706".to_string()]);
        // 排空两个订阅成功回报。
        assert!(matches!(h.recv("sub1"), ME::Sub(..)));

        h.front.ensure_replay_for("CU2706");
        assert_eq!(prices(h.recv("r1")).0, 500.0);
        assert_eq!(prices(h.recv("r2")).0, 501.0);

        // 未索引合约：ensure 为空操作，不起任务、不报错。
        h.front.ensure_replay_for("NOPE");
        h.front.shutdown();
    }

    #[test]
    fn advance_planned_accumulates_absolute_deadlines() {
        // 首帧不等待；其后按相邻时间差累加到同一时间轴（不重排、不丢增量）。
        let mut p = advance_planned(0, None, 1_000, 0, 1.0, 30_000);
        assert_eq!(p, 0);
        p = advance_planned(p, Some(1_000), 1_500, 0, 1.0, 30_000);
        assert_eq!(p, 500);
        p = advance_planned(p, Some(1_500), 2_000, 0, 1.0, 30_000);
        assert_eq!(p, 1_000);
        // 负差钳 0：时间轴不倒退。
        assert_eq!(
            advance_planned(p, Some(2_000), 1_900, 0, 1.0, 30_000),
            p
        );
    }

    #[test]
    fn advance_planned_honors_speed_interval_and_gap_cap() {
        // 相邻差按倍速缩放、超上限截断。
        assert_eq!(advance_planned(0, Some(0), 10_000, 0, 10.0, 30_000), 1_000);
        assert_eq!(advance_planned(0, Some(0), 100_000, 0, 1.0, 30_000), 30_000);
        // interval_ms > 0：忽略文件时间差，首帧仍立即。
        assert_eq!(advance_planned(0, None, 1_000, 200, 1.0, 30_000), 0);
        assert_eq!(advance_planned(0, Some(1_000), 2_000, 200, 1.0, 30_000), 200);
        // 饱和：极端累计不 panic。
        assert_eq!(
            advance_planned(u64::MAX - 1, Some(0), 1_000, 0, 1.0, 30_000),
            u64::MAX
        );
    }
}
