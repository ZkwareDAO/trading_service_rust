//! 交易前置运行时：独立回调线程 + 握手/脚本/台账 + 向客户端 SPI 派发回报。
//!
//! * 所有回报都在**单一回调线程**上产出（真实 CTP 即柜台线程回调）；请求槽在调用
//!   线程上做同步闸门（0/-1/-3）与台账变更，再把已构造好的回报帧投递到回调线程。
//! * 线程间用 `std::sync::mpsc`（FFI 边界，与 trading-exchange gateway 现状同理）。
//! * 回调直接经客户端 `CThostFtdcTraderSpiExt.spi_ptr`（`*mut dyn TraderSpi`）
//!   调用具名 trait 方法——签名编译期校验、bindgen 类型零偏差，且**绝不释放** Ext。
//! * 断线不清账：委托账本、持仓台账、IdGen、MaxOrderRef 都在前置对象上保留。

use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use ctp2rs::v1alpha1::{
    CThostFtdcInputOrderActionField, CThostFtdcInputOrderField, CThostFtdcQryInstrumentField,
    CThostFtdcQryInvestorPositionField, CThostFtdcQryOrderField, CThostFtdcQryTradeField,
    CThostFtdcReqAuthenticateField, CThostFtdcReqUserLoginField, CThostFtdcRspAuthenticateField,
    CThostFtdcRspUserLoginField, CThostFtdcSettlementInfoConfirmField, CThostFtdcTraderSpiExt,
    TraderSpi,
};

use super::frames::{instrument_frame, order_frame, position_frame, rsp_info, trade_frame, Party};
use super::ledger::{Fill, InstrumentSpec, OrderLedger, OrderRecord, PositionLedger, TradeRecord};
use super::script::{ctp_error_text, plan_insert, FillMode, FillScript, OrderReq};
use super::session::{
    Handshake, LoginInfo, Phase, QryKind, QueryGate, RET_NOT_CONNECTED, RET_OK,
    RET_QUERY_FLOW_CONTROL,
};
use crate::fields::{beijing_hms, get_fixed, now_ts_secs, parse_order_ref, put_fixed, IdGen};

type Callback = Box<dyn FnOnce(&mut dyn TraderSpi) + Send + 'static>;

/// 对原子裸指针的 Send/Sync 容器（FFI 边界）。
struct SpiCell(AtomicPtr<CThostFtdcTraderSpiExt>);
unsafe impl Send for SpiCell {}
unsafe impl Sync for SpiCell {}

/// 把 `&Mutex<QueryGate>` 移交给回调线程的 Send 句柄。
struct GatePtr(*const Mutex<QueryGate>);
unsafe impl Send for GatePtr {}

/// 被挂起的查询末帧：执行时发末帧并释放在途闸门。
struct HeldLast {
    fire: Callback,
}

/// 静态前置要素。
#[derive(Debug, Clone)]
pub struct FrontConfig {
    pub front_id: i32,
    pub session_id: i32,
    pub trading_day: String,
}

impl Default for FrontConfig {
    fn default() -> Self {
        Self {
            front_id: 1,
            session_id: 7001,
            trading_day: "20261015".into(),
        }
    }
}

/// 录单产出的回报帧（在调用线程构造、移交给回调线程派发）。
// 两个变体都是大型 bindgen 帧、体量相当，统一以枚举承载避免逐处装箱。
#[allow(clippy::large_enum_variant)]
enum OutFrame {
    Order(ctp2rs::v1alpha1::CThostFtdcOrderField),
    Trade(ctp2rs::v1alpha1::CThostFtdcTradeField),
}

/// 交易前置模拟器（一个 API 对象对应一个实例；断线不清账）。
pub struct MockTdFront {
    config: FrontConfig,
    handshake: Mutex<Handshake>,
    gate: Mutex<QueryGate>,
    party: Mutex<Party>,

    orders: Mutex<OrderLedger>,
    trades: Mutex<Vec<TradeRecord>>,
    positions: Mutex<PositionLedger>,
    idgen: Mutex<IdGen>,

    /// 当前成交脚本（默认即时全成）。
    fill_script: Mutex<FillScript>,
    /// 下一笔录入的一次性拒绝码（消费后清空）。
    next_reject: Mutex<Option<i32>>,
    /// 撤单错误注入（24/25/26；None=正常撤成）。
    cancel_error: Mutex<Option<i32>>,
    /// 市价单脚本成交价。
    marker_price: Mutex<f64>,
    /// 是否挂起查询末帧（确定性流控测试缝 / 慢速分页控制原语）。
    hold_last: AtomicBool,
    /// 被挂起的末帧（FIFO），每个内含末帧回调与闸门释放。
    held: Mutex<Vec<HeldLast>>,

    spi: SpiCell,
    tx: Mutex<Option<Sender<Callback>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
    started: AtomicBool,
}

impl MockTdFront {
    pub fn new(config: FrontConfig) -> Arc<Self> {
        let day = config.trading_day.clone();
        Arc::new(Self {
            handshake: Mutex::new(Handshake::new(
                config.front_id,
                config.session_id,
                config.trading_day.clone(),
            )),
            gate: Mutex::new(QueryGate::new()),
            party: Mutex::new(Party::default()),
            orders: Mutex::new(OrderLedger::new()),
            trades: Mutex::new(Vec::new()),
            positions: Mutex::new(PositionLedger::new()),
            idgen: Mutex::new(IdGen::new(day)),
            fill_script: Mutex::new(FillScript::instant_fill()),
            next_reject: Mutex::new(None),
            cancel_error: Mutex::new(None),
            marker_price: Mutex::new(3200.0),
            hold_last: AtomicBool::new(false),
            held: Mutex::new(Vec::new()),
            config,
            spi: SpiCell(AtomicPtr::new(std::ptr::null_mut())),
            tx: Mutex::new(None),
            worker: Mutex::new(None),
            started: AtomicBool::new(false),
        })
    }

    /// 当前握手阶段。
    pub fn phase(&self) -> Phase {
        self.handshake
            .lock()
            .map(|h| h.phase())
            .unwrap_or(Phase::Disconnected)
    }

    /// 注册/清空客户端 SPI；只保存不释放。
    pub fn set_spi(&self, spi: *mut CThostFtdcTraderSpiExt) {
        self.spi.0.store(spi, Ordering::Release);
    }

    fn post<F>(&self, cb: F)
    where
        F: FnOnce(&mut dyn TraderSpi) + Send + 'static,
    {
        if let Ok(guard) = self.tx.lock() {
            if let Some(tx) = guard.as_ref() {
                let _ = tx.send(Box::new(cb));
            }
        }
    }

    fn run(rx: Receiver<Callback>, spi: SpiAtomicRef) {
        while let Ok(cb) = rx.recv() {
            let ext = unsafe { &*spi.0 }.load(Ordering::Acquire);
            if ext.is_null() {
                continue;
            }
            // SAFETY：ext 指向 ctp2rs 拥有的 SpiExt，API 生命周期内有效；单回调线程独占。
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
        let spi_ref = SpiAtomicRef(&self.spi.0 as *const AtomicPtr<CThostFtdcTraderSpiExt>);
        let handle = thread::Builder::new()
            .name("ctp-mock-td-cb".into())
            .spawn(move || Self::run(rx, spi_ref))
            .expect("spawn ctp-mock-td callback thread");
        *self.tx.lock().unwrap() = Some(tx);
        *self.worker.lock().unwrap() = Some(handle);

        self.handshake.lock().unwrap().on_connected();
        self.post(|spi| spi.on_front_connected());
    }

    /// 控制面：断线（保留账本）。回调 OnFrontDisconnected，阶段回 Disconnected。
    pub fn disconnect(&self, reason: i32) {
        self.handshake.lock().unwrap().on_disconnected();
        // 在途查询作废（重连后由客户端重新全量查询）。
        self.gate.lock().unwrap().end();
        self.post(move |spi| spi.on_front_disconnected(reason));
    }

    /// 控制面：重连。回调线程常驻不停，仅复位握手阶段并异步再推 OnFrontConnected。
    pub fn reconnect(&self) {
        self.handshake.lock().unwrap().on_connected();
        self.post(|spi| spi.on_front_connected());
    }

    // ---------- 握手 ----------

    pub fn authenticate(&self, req: &CThostFtdcReqAuthenticateField, request_id: i32) -> i32 {
        if self
            .handshake
            .lock()
            .unwrap()
            .accept_authenticate()
            .is_err()
        {
            return RET_NOT_CONNECTED;
        }
        let broker = get_fixed(&req.BrokerID);
        let user = get_fixed(&req.UserID);
        let app = get_fixed(&req.AppID);
        {
            let mut p = self.party.lock().unwrap();
            p.broker = broker.clone();
            p.user = user.clone();
        }
        self.handshake.lock().unwrap().mark_authenticated();
        self.post(move |spi| {
            let mut field = CThostFtdcRspAuthenticateField::default();
            put_fixed(&mut field.BrokerID, &broker);
            put_fixed(&mut field.UserID, &user);
            put_fixed(&mut field.AppID, &app);
            spi.on_rsp_authenticate(Some(&field), None, request_id, true);
        });
        RET_OK
    }

    pub fn login(&self, _req: &CThostFtdcReqUserLoginField, request_id: i32) -> i32 {
        if self.handshake.lock().unwrap().accept_login().is_err() {
            return RET_NOT_CONNECTED;
        }
        let info: LoginInfo = self.handshake.lock().unwrap().mark_logged_in();
        self.post(move |spi| {
            let mut field = CThostFtdcRspUserLoginField::default();
            put_fixed(&mut field.TradingDay, &info.trading_day);
            put_fixed(&mut field.LoginTime, "09:00:00");
            put_fixed(&mut field.MaxOrderRef, &info.max_order_ref);
            field.FrontID = info.front_id;
            field.SessionID = info.session_id;
            spi.on_rsp_user_login(Some(&field), None, request_id, true);
        });
        RET_OK
    }

    pub fn settlement_confirm(
        &self,
        req: &CThostFtdcSettlementInfoConfirmField,
        request_id: i32,
    ) -> i32 {
        if self
            .handshake
            .lock()
            .unwrap()
            .accept_settlement_confirm()
            .is_err()
        {
            return RET_NOT_CONNECTED;
        }
        let broker = get_fixed(&req.BrokerID);
        let investor = get_fixed(&req.InvestorID);
        let day = self.config.trading_day.clone();
        {
            let mut p = self.party.lock().unwrap();
            p.broker = broker.clone();
            p.investor = investor.clone();
        }
        self.handshake.lock().unwrap().mark_settlement_confirmed();
        self.post(move |spi| {
            let mut field = CThostFtdcSettlementInfoConfirmField::default();
            put_fixed(&mut field.BrokerID, &broker);
            put_fixed(&mut field.InvestorID, &investor);
            put_fixed(&mut field.ConfirmDate, &day);
            put_fixed(&mut field.ConfirmTime, "09:00:00");
            field.SettlementID = 1;
            spi.on_rsp_settlement_info_confirm(Some(&field), None, request_id, true);
        });
        RET_OK
    }

    // ---------- 场景/控制面注入 ----------

    pub fn set_fill_script(&self, script: FillScript) {
        *self.fill_script.lock().unwrap() = script;
    }
    pub fn reject_next(&self, code: i32) {
        *self.next_reject.lock().unwrap() = Some(code);
    }
    pub fn set_cancel_error(&self, code: Option<i32>) {
        *self.cancel_error.lock().unwrap() = code;
    }
    pub fn set_marker_price(&self, price: f64) {
        *self.marker_price.lock().unwrap() = price;
    }
    /// 播种/覆盖持仓（断线不清）。
    pub fn seed_position(
        &self,
        instrument: &str,
        long: bool,
        total: i32,
        today: i32,
        yd: i32,
        avg: f64,
    ) {
        let mut pos = self.positions.lock().unwrap();
        pos.ensure_spec(instrument);
        pos.seed(instrument, long, total, today, yd, avg);
    }
    /// 注册合约静态属性（可选；默认按代码前缀给夹具）。
    pub fn register_spec(&self, spec: InstrumentSpec) {
        self.positions.lock().unwrap().register_spec(spec);
    }

    /// 台账只读快照（委托 + 派生持仓行），序列化为 JSON 字符串。
    pub fn ledger_snapshot(&self) -> String {
        let orders: Vec<serde_json::Value> = self
            .orders
            .lock()
            .unwrap()
            .all()
            .iter()
            .map(|o| {
                serde_json::json!({
                    "order_ref": o.order_ref,
                    "order_sys_id": o.order_sys_id.trim(),
                    "instrument": o.instrument_id,
                    "exchange": o.exchange_id,
                    "status": o.status.to_string(),
                    "volume_total": o.volume_total,
                    "volume_traded": o.volume_traded,
                    "front_id": o.front_id,
                    "session_id": o.session_id,
                })
            })
            .collect();
        let positions: Vec<serde_json::Value> = self
            .positions
            .lock()
            .unwrap()
            .all_rows()
            .iter()
            .map(|r| {
                serde_json::json!({
                    "instrument": r.instrument_id,
                    "exchange": r.exchange_id,
                    "posi_direction": r.posi_direction.to_string(),
                    "position_date": r.position_date.to_string(),
                    "position": r.position,
                    "today": r.today_position,
                    "yd": r.yd_position,
                    "position_cost": r.position_cost,
                    "use_margin": r.use_margin,
                    "position_profit": r.position_profit,
                })
            })
            .collect();
        serde_json::json!({ "orders": orders, "positions": positions }).to_string()
    }

    // ---------- 报单 ----------

    /// 录单：同步闸门（未就绪 -1），成功则台账记账并按脚本异步回报。
    pub fn insert_order(&self, req: &CThostFtdcInputOrderField, _request_id: i32) -> i32 {
        if self.phase() != Phase::Ready {
            return RET_NOT_CONNECTED;
        }
        // 账户三方以录单字段刷新（与握手回填互补）。
        {
            let mut p = self.party.lock().unwrap();
            let b = get_fixed(&req.BrokerID);
            let i = get_fixed(&req.InvestorID);
            let u = get_fixed(&req.UserID);
            if !b.is_empty() {
                p.broker = b;
            }
            if !i.is_empty() {
                p.investor = i;
            }
            if !u.is_empty() {
                p.user = u;
            }
        }

        let order_ref = get_fixed(&req.OrderRef);
        if let Some(n) = parse_order_ref(&req.OrderRef) {
            self.handshake.lock().unwrap().note_order_ref(n);
        }
        let instrument = get_fixed(&req.InstrumentID);
        let exchange = self.ensure_exchange(&instrument);

        // 一次性拒绝优先于脚本。
        let script = match self.next_reject.lock().unwrap().take() {
            Some(code) => FillScript::rejected(code),
            None => self.fill_script.lock().unwrap().clone(),
        };

        let order_req = OrderReq {
            order_ref: order_ref.clone(),
            instrument: instrument.clone(),
            exchange_id: exchange.clone(),
            direction: req.Direction as u8 as char,
            offset: req.CombOffsetFlag[0] as u8 as char,
            hedge_flag: req.CombHedgeFlag[0] as u8 as char,
            price_type: req.OrderPriceType as u8 as char,
            limit_price: req.LimitPrice,
            volume: req.VolumeTotalOriginal,
        };
        let marker = *self.marker_price.lock().unwrap();

        // 录入被拒：不分配系统号、不记账，异步 on_rsp_order_insert 带真实错误码。
        if script.mode == FillMode::Rejected {
            let code = script.reject_code;
            let msg = ctp_error_text(code).to_string();
            self.post(move |spi| {
                let info = rsp_info(code, &msg);
                spi.on_rsp_order_insert(None, Some(&info), _request_id, true);
            });
            return RET_OK;
        }

        // 同步台账变更 + 构造回报帧，移交回调线程派发。
        let party = self.party.lock().unwrap().clone();
        let day = self.config.trading_day.clone();
        let frames = self.apply_insert(order_req, &script, marker, &party, &day);
        self.post(move |spi| {
            for f in frames {
                match f {
                    OutFrame::Order(o) => spi.on_rtn_order(Some(&o)),
                    OutFrame::Trade(t) => spi.on_rtn_trade(Some(&t)),
                }
            }
        });
        RET_OK
    }

    /// 记账 + 构造帧；返回 None 表示录入被拒（不记账）。
    fn apply_insert(
        &self,
        req: OrderReq,
        script: &FillScript,
        marker: f64,
        party: &Party,
        day: &str,
    ) -> Vec<OutFrame> {
        // 调用方已排除 Rejected；此处脚本必产出委托/成交事件。
        let sys_id = self.idgen.lock().unwrap().next_sys_id();
        let ack = plan_insert(&req, &sys_id, script, marker);

        let mut record = OrderRecord {
            order_ref: req.order_ref.clone(),
            order_sys_id: sys_id.clone(),
            instrument_id: req.instrument.clone(),
            exchange_id: req.exchange_id.clone(),
            front_id: self.config.front_id,
            session_id: self.config.session_id,
            direction: req.direction,
            offset: req.offset,
            price_type: req.price_type,
            limit_price: req.limit_price,
            volume_total: req.volume,
            volume_traded: 0,
            status: '3',
        };

        let mut frames = Vec::new();
        let mut pos = self.positions.lock().unwrap();
        let mut trades = self.trades.lock().unwrap();
        let hms = beijing_hms(now_ts_secs());

        for ev in &ack.events {
            match ev {
                super::script::OrderEvent::Status {
                    status,
                    volume_traded,
                    status_msg,
                } => {
                    record.status = *status;
                    record.volume_traded = *volume_traded;
                    frames.push(OutFrame::Order(order_frame(
                        &record,
                        party,
                        *status,
                        *volume_traded,
                        status_msg,
                        day,
                        &hms,
                    )));
                }
                super::script::OrderEvent::Trade {
                    seq: _,
                    price,
                    volume,
                } => {
                    let trade_id = self.idgen.lock().unwrap().next_trade_id();
                    frames.push(OutFrame::Trade(trade_frame(
                        &record, party, &trade_id, *price, *volume, day, &hms,
                    )));
                    record.volume_traded += *volume;
                    trades.push(TradeRecord {
                        order_ref: record.order_ref.clone(),
                        trade_id,
                        instrument_id: record.instrument_id.clone(),
                        exchange_id: record.exchange_id.clone(),
                        direction: record.direction,
                        offset: record.offset,
                        price: *price,
                        volume: *volume,
                    });
                    pos.apply_fill(&Fill {
                        instrument_id: record.instrument_id.clone(),
                        direction: record.direction,
                        offset: record.offset,
                        price: *price,
                        volume: *volume,
                    });
                }
                super::script::OrderEvent::Rejected { .. } => {
                    // 调用方已排除 Rejected，不会进入。
                }
            }
        }

        self.orders.lock().unwrap().insert(record);
        frames
    }

    fn ensure_exchange(&self, instrument: &str) -> String {
        let mut pos = self.positions.lock().unwrap();
        pos.ensure_spec(instrument);
        pos.spec(instrument)
            .map(|s| s.exchange_id.clone())
            .unwrap_or_default()
    }

    // ---------- 撤单 ----------

    /// 撤单：两路定位；同步返 0，异步撤成 '5' 或 ErrRtnOrderAction(24/25/26)。
    pub fn cancel_order(&self, req: &CThostFtdcInputOrderActionField, _request_id: i32) -> i32 {
        if self.phase() != Phase::Ready {
            return RET_NOT_CONNECTED;
        }
        let order_ref = get_fixed(&req.OrderRef);
        let sys_id = get_fixed(&req.OrderSysID);

        // 两路定位（在账本锁内拷贝命中记录与判定，避免持锁跨线程）。
        let located = {
            let orders = self.orders.lock().unwrap();
            if !order_ref.is_empty() {
                orders
                    .find_by_triple(req.FrontID, req.SessionID, &order_ref)
                    .cloned()
            } else if !sys_id.is_empty() {
                orders.find_by_sys_id(&sys_id).cloned()
            } else {
                None
            }
        };

        let Some(mut record) = located else {
            self.emit_cancel_reject(25);
            return RET_OK;
        };

        // 显式注入优先；否则按当前委托状态派生 26（已全成/已撤不可撤）。
        let injected = *self.cancel_error.lock().unwrap();
        let code = match injected {
            Some(c) => Some(c),
            None if !record.is_cancellable() => Some(26),
            _ => None,
        };
        if let Some(code) = code {
            self.emit_cancel_reject(code);
            return RET_OK;
        }

        // 撤成：账本置 '5'，异步 RtnOrder。
        record.status = '5';
        let key = record.order_ref.clone();
        {
            let mut orders = self.orders.lock().unwrap();
            if let Some(r) = orders.record_mut(&key) {
                r.status = '5';
            }
        }
        let party = self.party.lock().unwrap().clone();
        let day = self.config.trading_day.clone();
        let hms = beijing_hms(now_ts_secs());
        self.post(move |spi| {
            let f = order_frame(
                &record,
                &party,
                '5',
                record.volume_traded,
                "已撤单",
                &day,
                &hms,
            );
            spi.on_rtn_order(Some(&f));
        });
        RET_OK
    }

    fn emit_cancel_reject(&self, code: i32) {
        let msg = super::script::ctp_error_text(code).to_string();
        self.post(move |spi| {
            let info = rsp_info(code, &msg);
            spi.on_err_rtn_order_action(None, Some(&info));
        });
    }

    // ---------- 查询（单在途闸门 + 逐行 is_last） ----------

    /// 通用闸门：未就绪 -1、在途 -3；成功返回并登记 kind。
    fn begin_query(&self, kind: QryKind, request_id: i32) -> Result<GatePtr, i32> {
        if self.phase() != Phase::Ready {
            return Err(RET_NOT_CONNECTED);
        }
        self.gate
            .lock()
            .unwrap()
            .begin(kind, request_id)
            .map_err(|_| RET_QUERY_FLOW_CONTROL)?;
        Ok(GatePtr(&self.gate as *const Mutex<QueryGate>))
    }

    /// 在回调线程发出末帧后释放闸门。
    fn end_gate_on_worker(gate: GatePtr) {
        // SAFETY：gate 指向 Arc<Self> 内的 Mutex，回调线程执行时对象仍存活。
        unsafe { (*gate.0).lock().unwrap().end() };
    }

    /// 调度查询末帧：正常即投递；开启挂起时扣留到 `release_last` 显式放行。
    ///
    /// `emit` 只发末帧；闸门释放在此统一包入，确保任何路径都会回收在途查询。
    fn schedule_last<E>(&self, gate: GatePtr, emit: E)
    where
        E: FnOnce(&mut dyn TraderSpi) + Send + 'static,
    {
        let fire: Callback = Box::new(move |spi: &mut dyn TraderSpi| {
            emit(spi);
            Self::end_gate_on_worker(gate);
        });
        if self.hold_last.load(Ordering::Relaxed) {
            self.held.lock().unwrap().push(HeldLast { fire });
        } else {
            self.post(move |spi| fire(spi));
        }
    }

    /// 控制面/测试：开启后查询逐行即投、末帧与闸门释放被挂起。
    pub fn set_hold_last(&self, on: bool) {
        self.hold_last.store(on, Ordering::Relaxed);
    }

    /// 放行最早一帧被挂起的末帧；无挂起时返回 false。
    pub fn release_last(&self) -> bool {
        let held = self.held.lock().unwrap().pop();
        if let Some(h) = held {
            self.post(move |spi| (h.fire)(spi));
            true
        } else {
            false
        }
    }

    pub fn query_orders(&self, req: &CThostFtdcQryOrderField, request_id: i32) -> i32 {
        let gate = match self.begin_query(QryKind::Order, request_id) {
            Ok(g) => g,
            Err(c) => return c,
        };
        let instrument = get_fixed(&req.InstrumentID);
        let rows: Vec<OrderRecord> = {
            self.orders
                .lock()
                .unwrap()
                .query(&instrument)
                .into_iter()
                .cloned()
                .collect()
        };
        let party = self.party.lock().unwrap().clone();
        let day = self.config.trading_day.clone();
        let hms = beijing_hms(now_ts_secs());
        self.post(move |spi| {
            for r in &rows {
                let f = order_frame(r, &party, r.status, r.volume_traded, "", &day, &hms);
                spi.on_rsp_qry_order(Some(&f), None, request_id, false);
            }
        });
        self.schedule_last(gate, move |spi| {
            spi.on_rsp_qry_order(None, None, request_id, true);
        });
        RET_OK
    }

    pub fn query_trades(&self, req: &CThostFtdcQryTradeField, request_id: i32) -> i32 {
        let gate = match self.begin_query(QryKind::Trade, request_id) {
            Ok(g) => g,
            Err(c) => return c,
        };
        let instrument = get_fixed(&req.InstrumentID);
        let rows: Vec<TradeRecord> = self
            .trades
            .lock()
            .unwrap()
            .iter()
            .filter(|t| instrument.trim().is_empty() || t.instrument_id == instrument)
            .cloned()
            .collect();
        let party = self.party.lock().unwrap().clone();
        let day = self.config.trading_day.clone();
        let hms = beijing_hms(now_ts_secs());
        self.post(move |spi| {
            for t in &rows {
                let rec = OrderRecord {
                    order_ref: t.order_ref.clone(),
                    order_sys_id: String::new(),
                    instrument_id: t.instrument_id.clone(),
                    exchange_id: t.exchange_id.clone(),
                    front_id: 0,
                    session_id: 0,
                    direction: t.direction,
                    offset: t.offset,
                    price_type: '2',
                    limit_price: t.price,
                    volume_total: t.volume,
                    volume_traded: t.volume,
                    status: '0',
                };
                let f = trade_frame(&rec, &party, &t.trade_id, t.price, t.volume, &day, &hms);
                spi.on_rsp_qry_trade(Some(&f), None, request_id, false);
            }
        });
        self.schedule_last(gate, move |spi| {
            spi.on_rsp_qry_trade(None, None, request_id, true);
        });
        RET_OK
    }

    pub fn query_positions(
        &self,
        req: &CThostFtdcQryInvestorPositionField,
        request_id: i32,
    ) -> i32 {
        let gate = match self.begin_query(QryKind::Position, request_id) {
            Ok(g) => g,
            Err(c) => return c,
        };
        let instrument = get_fixed(&req.InstrumentID);
        let party = self.party.lock().unwrap().clone();
        let day = self.config.trading_day.clone();
        // 在调用线程派生全部行（含 0 量行）。
        let rows = {
            let pos = self.positions.lock().unwrap();
            let insts: Vec<String> = if instrument.trim().is_empty() {
                pos.instruments()
            } else {
                vec![instrument.clone()]
            };
            insts
                .iter()
                .flat_map(|i| pos.derive_rows(i))
                .collect::<Vec<_>>()
        };
        self.post(move |spi| {
            for row in &rows {
                let f = position_frame(row, &party, &day);
                spi.on_rsp_qry_investor_position(Some(&f), None, request_id, false);
            }
        });
        self.schedule_last(gate, move |spi| {
            spi.on_rsp_qry_investor_position(None, None, request_id, true);
        });
        RET_OK
    }

    pub fn query_instruments(&self, req: &CThostFtdcQryInstrumentField, request_id: i32) -> i32 {
        let gate = match self.begin_query(QryKind::Instrument, request_id) {
            Ok(g) => g,
            Err(c) => return c,
        };
        let instrument = get_fixed(&req.InstrumentID);
        let specs = {
            let pos = self.positions.lock().unwrap();
            pos.known_specs()
                .into_iter()
                .filter(|s| instrument.trim().is_empty() || s.instrument_id == instrument)
                .cloned()
                .collect::<Vec<_>>()
        };
        self.post(move |spi| {
            for spec in &specs {
                let f = instrument_frame(spec);
                spi.on_rsp_qry_instrument(Some(&f), None, request_id, false);
            }
        });
        self.schedule_last(gate, move |spi| {
            spi.on_rsp_qry_instrument(None, None, request_id, true);
        });
        RET_OK
    }

    /// 关停：关闭回调通道并 join（幂等）。
    pub fn shutdown(&self) {
        if !self.started.swap(false, Ordering::SeqCst) {
            return;
        }
        if let Ok(mut guard) = self.tx.lock() {
            drop(guard.take());
        }
        if let Ok(mut guard) = self.worker.lock() {
            if let Some(h) = guard.take() {
                let _ = h.join();
            }
        }
    }
}

impl Drop for MockTdFront {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// 把 `&AtomicPtr<SpiExt>` 传给回调线程的 Send 句柄。
struct SpiAtomicRef(*const AtomicPtr<CThostFtdcTraderSpiExt>);
unsafe impl Send for SpiAtomicRef {}

#[cfg(test)]
mod tests {
    use super::*;
    use ctp2rs::v1alpha1::CThostFtdcRspInfoField;
    use std::sync::mpsc::SyncSender;

    /// 记录关键回调的测试 SPI。
    struct RecSpi {
        tx: SyncSender<String>,
    }
    impl RecSpi {
        fn emit(&self, s: impl Into<String>) {
            let _ = self.tx.send(s.into());
        }
    }
    impl TraderSpi for RecSpi {
        fn on_front_connected(&mut self) {
            self.emit("connected");
        }
        fn on_rsp_authenticate(
            &mut self,
            _f: Option<&CThostFtdcRspAuthenticateField>,
            _i: Option<&CThostFtdcRspInfoField>,
            _id: i32,
            _last: bool,
        ) {
            self.emit("authed");
        }
        fn on_rsp_user_login(
            &mut self,
            _f: Option<&CThostFtdcRspUserLoginField>,
            _i: Option<&CThostFtdcRspInfoField>,
            _id: i32,
            _last: bool,
        ) {
            self.emit("login");
        }
        fn on_rsp_settlement_info_confirm(
            &mut self,
            _f: Option<&CThostFtdcSettlementInfoConfirmField>,
            _i: Option<&CThostFtdcRspInfoField>,
            _id: i32,
            _last: bool,
        ) {
            self.emit("settled");
        }
        fn on_rtn_order(&mut self, o: Option<&ctp2rs::v1alpha1::CThostFtdcOrderField>) {
            if let Some(o) = o {
                self.emit(format!("order:{}", o.OrderStatus as u8 as char));
            }
        }
        fn on_rtn_trade(&mut self, _t: Option<&ctp2rs::v1alpha1::CThostFtdcTradeField>) {
            self.emit("trade");
        }
        fn on_err_rtn_order_action(
            &mut self,
            _a: Option<&ctp2rs::v1alpha1::CThostFtdcOrderActionField>,
            info: Option<&CThostFtdcRspInfoField>,
        ) {
            let code = info.map(|i| i.ErrorID).unwrap_or(-1);
            self.emit(format!("cancelerr:{code}"));
        }
        fn on_rsp_qry_order(
            &mut self,
            o: Option<&ctp2rs::v1alpha1::CThostFtdcOrderField>,
            _i: Option<&CThostFtdcRspInfoField>,
            _id: i32,
            is_last: bool,
        ) {
            if o.is_some() {
                self.emit("qryorder-row");
            }
            if is_last {
                self.emit("qryorder-last");
            }
        }
        fn on_rsp_qry_investor_position(
            &mut self,
            _p: Option<&ctp2rs::v1alpha1::CThostFtdcInvestorPositionField>,
            _i: Option<&CThostFtdcRspInfoField>,
            _id: i32,
            is_last: bool,
        ) {
            if is_last {
                self.emit("qrypos-last");
            }
        }
        fn on_rsp_order_insert(
            &mut self,
            _o: Option<&CThostFtdcInputOrderField>,
            info: Option<&CThostFtdcRspInfoField>,
            _id: i32,
            _last: bool,
        ) {
            let code = info.map(|i| i.ErrorID).unwrap_or(0);
            self.emit(format!("insertrej:{code}"));
        }
    }

    struct Harness {
        front: Arc<MockTdFront>,
        rx: std::sync::mpsc::Receiver<String>,
        spi: *mut dyn TraderSpi,
        ext: *mut CThostFtdcTraderSpiExt,
    }
    impl Harness {
        fn recv(&self, tag: &str) -> String {
            self.rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap_or_else(|_| panic!("timeout waiting {tag}"))
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

    fn ready_front() -> Harness {
        let front = MockTdFront::new(FrontConfig::default());
        let (tx, rx) = std::sync::mpsc::sync_channel::<String>(128);
        let spi = Box::into_raw(Box::new(RecSpi { tx })) as *mut dyn TraderSpi;
        let ext = Box::into_raw(Box::new(CThostFtdcTraderSpiExt::new(spi)));
        front.set_spi(ext);
        front.start();
        let h = Harness {
            front,
            rx,
            spi,
            ext,
        };
        assert_eq!(h.recv("connected"), "connected");
        h.front
            .authenticate(&CThostFtdcReqAuthenticateField::default(), 1);
        assert_eq!(h.recv("authed"), "authed");
        h.front.login(&CThostFtdcReqUserLoginField::default(), 2);
        assert_eq!(h.recv("login"), "login");
        h.front
            .settlement_confirm(&CThostFtdcSettlementInfoConfirmField::default(), 3);
        assert_eq!(h.recv("settled"), "settled");
        h
    }

    fn insert(order_ref: &str, instrument: &str, vol: i32) -> CThostFtdcInputOrderField {
        let mut req = CThostFtdcInputOrderField::default();
        put_fixed(&mut req.OrderRef, order_ref);
        put_fixed(&mut req.InstrumentID, instrument);
        req.Direction = '0' as i8;
        req.CombOffsetFlag[0] = '0' as i8;
        req.CombHedgeFlag[0] = '1' as i8;
        req.OrderPriceType = '2' as i8;
        req.LimitPrice = 3200.0;
        req.VolumeTotalOriginal = vol;
        req
    }

    #[test]
    fn instant_fill_emits_queued_trade_all_traded() {
        let h = ready_front();
        h.front.set_fill_script(FillScript::instant_fill());
        assert_eq!(h.front.insert_order(&insert("1", "rb2510", 2), 10), 0);
        assert_eq!(h.recv("order3"), "order:3");
        assert_eq!(h.recv("trade"), "trade");
        assert_eq!(h.recv("order0"), "order:0");
    }

    #[test]
    fn accepted_only_then_cancel_flows_cancelled() {
        let h = ready_front();
        h.front.set_fill_script(FillScript::accepted_only());
        assert_eq!(h.front.insert_order(&insert("5", "rb2510", 1), 10), 0);
        assert_eq!(h.recv("order3"), "order:3");

        let mut act = CThostFtdcInputOrderActionField::default();
        put_fixed(&mut act.OrderRef, "5");
        act.FrontID = 1;
        act.SessionID = 7001;
        assert_eq!(h.front.cancel_order(&act, 11), 0);
        assert_eq!(h.recv("order5"), "order:5");
    }

    #[test]
    fn cancel_missing_order_emits_error_25() {
        let h = ready_front();
        let mut act = CThostFtdcInputOrderActionField::default();
        put_fixed(&mut act.OrderRef, "999");
        act.FrontID = 1;
        act.SessionID = 7001;
        assert_eq!(h.front.cancel_order(&act, 11), 0);
        assert_eq!(h.recv("cancelerr25"), "cancelerr:25");
    }

    #[test]
    fn cancel_terminal_order_emits_error_26() {
        let h = ready_front();
        h.front.insert_order(&insert("1", "rb2510", 1), 10);
        assert_eq!(h.recv("order3"), "order:3");
        assert_eq!(h.recv("trade"), "trade");
        assert_eq!(h.recv("order0"), "order:0"); // 全成后
        let mut act = CThostFtdcInputOrderActionField::default();
        put_fixed(&mut act.OrderRef, "1");
        act.FrontID = 1;
        act.SessionID = 7001;
        assert_eq!(h.front.cancel_order(&act, 11), 0);
        assert_eq!(h.recv("cancelerr26"), "cancelerr:26");
    }

    #[test]
    fn reject_next_emits_rsp_order_insert_error() {
        let h = ready_front();
        h.front.reject_next(30);
        assert_eq!(h.front.insert_order(&insert("1", "rb2510", 1), 10), 0);
        assert_eq!(h.recv("insertrej30"), "insertrej:30");
    }

    #[test]
    fn query_gate_rejects_concurrent_with_minus_3_then_releases_on_last() {
        let h = ready_front();
        h.front.set_fill_script(FillScript::accepted_only());
        h.front.insert_order(&insert("1", "rb2510", 1), 10);
        h.recv("order3");
        h.front.insert_order(&insert("2", "rb2510", 1), 11);
        h.recv("order3");

        // 挂起末帧：逐行即投、is_last 与闸门释放被扣住，确定性制造在途窗口。
        h.front.set_hold_last(true);
        let q = CThostFtdcQryOrderField::default();
        assert_eq!(h.front.query_orders(&q, 20), 0);
        assert_eq!(h.recv("row"), "qryorder-row");
        assert_eq!(h.recv("row"), "qryorder-row");
        // 末帧未放行：第二次查询确定性命中 -3。
        assert_eq!(h.front.query_orders(&q, 21), -3);
        assert!(h.front.release_last());
        assert_eq!(h.recv("last"), "qryorder-last");

        // 闸门已释放：关闭挂起后可再次查询并收齐两行走马末帧。
        h.front.set_hold_last(false);
        assert_eq!(h.front.query_orders(&q, 22), 0);
        assert_eq!(h.recv("row"), "qryorder-row");
        assert_eq!(h.recv("row"), "qryorder-row");
        assert_eq!(h.recv("last"), "qryorder-last");
    }

    #[test]
    fn query_position_after_fill_returns_long_rows_and_last() {
        let h = ready_front();
        h.front.insert_order(&insert("1", "rb2510", 2), 10);
        assert_eq!(h.recv("order3"), "order:3");
        assert_eq!(h.recv("trade"), "trade");
        assert_eq!(h.recv("order0"), "order:0");
        let q = CThostFtdcQryInvestorPositionField::default();
        assert_eq!(h.front.query_positions(&q, 30), 0);
        // rb2510 SHFE 净多：多今有量 + 多昨0 + 空0，共 3 行后 last。
        assert_eq!(h.recv("last"), "qrypos-last");
    }

    #[test]
    fn disconnect_keeps_ledger_and_reconnect_login() {
        let h = ready_front();
        h.front.set_fill_script(FillScript::accepted_only());
        h.front.insert_order(&insert("7", "rb2510", 1), 10);
        h.recv("order3");
        h.front.disconnect(0x1001);
        // 未重新握手就绪前，录单被拒（-1）。
        assert_eq!(h.front.insert_order(&insert("8", "rb2510", 1), 12), -1);

        // 重连：worker 常驻，仅重发 OnFrontConnected。
        h.front.reconnect();
        assert_eq!(h.recv("connected"), "connected");
        h.front
            .authenticate(&CThostFtdcReqAuthenticateField::default(), 1);
        h.recv("authed");
        h.front.login(&CThostFtdcReqUserLoginField::default(), 2);
        h.recv("login");
        h.front
            .settlement_confirm(&CThostFtdcSettlementInfoConfirmField::default(), 3);
        h.recv("settled");

        // 账本保留：旧委托仍可查/撤。
        let mut act = CThostFtdcInputOrderActionField::default();
        put_fixed(&mut act.OrderRef, "7");
        act.FrontID = 1;
        act.SessionID = 7001;
        assert_eq!(h.front.cancel_order(&act, 40), 0);
        assert_eq!(h.recv("order5"), "order:5");
    }
}
