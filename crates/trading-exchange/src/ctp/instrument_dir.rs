//! 进程级「合约静态信息」目录：`合约 → {交易所, 产品类型, 乘数, 最小变动价位}`。
//!
//! 登录后 `ReqQryInstrument` 全量预热（与持仓/委托查询不同：合约规格是**全局一致**
//! 的静态数据，不随账户变化），故由 [`crate::ctp::provider::CtpProvider`] 持有**一份
//! 进程级共享实例**注入各账户会话，避免多账户重复预热。用途：
//! * 区分期货/期权（`product_class`），决定成本与市价单口径；
//! * 持仓成本折算单价（乘数）；
//! * SHFE 平今/平昨路由前判定合约所属交易所（交易所代码）；
//! * 市价单转穿价限价时按最小变动价位取整（price_tick）。
//!
//! 生命周期：进程内存（DashMap），重连可重新预热；不挂在持久 Position 上，
//! 每账户的今/昨路由明细仍由各自私有的 `PositionDir` 保存。

use dashmap::DashMap;

use crate::ctp::core::event::InstrumentInfo;

/// CTP 产品类型：期货（ThostFtdcUserApiDataType.h 的 `THOST_FTDC_PC_Future`，ASCII '1'）。
pub const PRODUCT_FUTURES: u8 = b'1';
/// CTP 产品类型：期权（`THOST_FTDC_PC_Options`，ASCII '2'）。
pub const PRODUCT_OPTIONS: u8 = b'2';

/// 合约静态信息目录（线程安全）。
#[derive(Default)]
pub struct InstrumentDir {
    by_instrument: DashMap<String, InstrumentInfo>,
}

impl InstrumentDir {
    pub fn new() -> Self {
        Self::default()
    }

    /// 喂入一条合约查询结果（合约代码 trim 后为键且同步规范化值，空白忽略；
    /// 后到覆盖，重连预热可纠正）。
    pub fn remember(&self, mut info: InstrumentInfo) {
        info.instrument = info.instrument.trim().to_string();
        if info.instrument.is_empty() {
            return;
        }
        self.by_instrument.insert(info.instrument.clone(), info);
    }

    /// 读取合约静态信息（trim 后精确匹配；未预热返回 None）。
    pub fn get(&self, instrument: &str) -> Option<InstrumentInfo> {
        self.by_instrument
            .get(instrument.trim())
            .map(|e| e.value().clone())
    }

    /// 合约所属交易所代码（未预热返回 None）。只读所需字段，不 clone 整条信息。
    pub fn exchange(&self, instrument: &str) -> Option<String> {
        self.by_instrument
            .get(instrument.trim())
            .map(|e| e.value().exchange_id.clone())
    }

    /// 产品类型（'1'期货/'2'期权；未预热返回 None）。
    pub fn product_class(&self, instrument: &str) -> Option<u8> {
        self.by_instrument
            .get(instrument.trim())
            .map(|e| e.value().product_class)
    }

    /// 是否为期权（未预热返回 false；此时乘数通常也未就绪，持仓聚合会整桶跳过）。
    pub fn is_option(&self, instrument: &str) -> bool {
        self.product_class(instrument) == Some(PRODUCT_OPTIONS)
    }

    /// 合约乘数（未预热/非正返回 0，调用方据此判乘数未就绪）。
    pub fn multiple(&self, instrument: &str) -> i32 {
        self.by_instrument
            .get(instrument.trim())
            .map_or(0, |e| e.value().volume_multiple)
    }

    /// 合约多/空保证金率 `(long, short)`（未预热返回 `(0,0)`，调用方据此判比率未就绪）。
    ///
    /// 仅供持仓回报 `UseMargin=0` 时在 ctp 映射层做「持仓成本×比率」兜底；
    /// 非法（0/负/NaN）值的过滤在 `mapping::effective_total_margin` 统一完成，此处只透传。
    pub fn margin_ratios(&self, instrument: &str) -> (f64, f64) {
        self.by_instrument
            .get(instrument.trim())
            .map_or((0.0, 0.0), |e| {
                (e.value().long_margin_ratio, e.value().short_margin_ratio)
            })
    }

    /// 列出属于给定产品代码集合且当前 `is_trading` 的**期货**合约（有序、去重）。
    /// 供品种（如 cu）展开为该品种全部在交易合约（K线订阅）。
    ///
    /// 显式按 `product_class='1'` 过滤：期权即便 product_id 与期货相同也不展开
    /// （K 线订阅只收期货）。
    pub fn trading_instruments_for(
        &self,
        products: &std::collections::HashSet<String>,
    ) -> Vec<String> {
        let mut out: Vec<String> = self
            .by_instrument
            .iter()
            .filter(|e| {
                e.value().is_trading
                    && e.value().product_class == PRODUCT_FUTURES
                    && products.contains(e.value().product_id.as_str())
            })
            .map(|e| e.key().clone())
            .collect();
        out.sort();
        out
    }

    /// 合约是否已知且在交易（K线订阅的成员判定/作用域分流用）。
    pub fn is_trading(&self, instrument: &str) -> bool {
        self.by_instrument
            .get(instrument.trim())
            .is_some_and(|e| e.value().is_trading)
    }

    /// 从 `data/ctp-specs/instruments.csv` 内容冷启动加载（只解析、不碰文件路径，
    /// 文件定位/读取由宿主装配层负责，保持本层不做文件 IO）。
    ///
    /// 列序固定（见 ctp-fetch-specs 产出）：
    /// `0 instrument_id, 1 exchange_id, 2 product_id, 4 product_class,
    ///  5 volume_multiple, 6 price_tick, 11 long_margin_ratio,
    ///  12 short_margin_ratio, 13 is_trading`。
    ///
    /// 过滤口径：**收 product_class=1（期货）或 2（期权）且 is_trading=1**；
    /// 组合(3,5,6,7) 与摘牌合约不入内存表。期权入表是期权成本（OpenCost）折算
    /// 与市价单转限价的前置；K 线品种展开由 [`Self::trading_instruments_for`]
    /// 再按期货过滤。
    /// 表头（含可能的 UTF-8 BOM）与缺列/坏行跳过，不致命；返回实际入表条数。
    pub fn load_from_csv(&self, reader: impl std::io::Read) -> std::io::Result<usize> {
        use std::io::{BufRead, BufReader};
        let mut loaded = 0usize;
        for (idx, line) in BufReader::new(reader).lines().enumerate() {
            let line = line?;
            // 首行表头：按列数与首列名识别（BOM 在首列名内也能命中 contains）。
            if idx == 0 && line.contains("instrument_id") {
                continue;
            }
            if let Some(info) = parse_instrument_csv_row(&line) {
                self.remember(info);
                loaded += 1;
            }
        }
        Ok(loaded)
    }

    /// 以最新快照**整体重灌**：先剔除新集合中已不存在（摘牌/不再可交易）
    /// 的旧合约，再逐条 upsert（[`Self::remember`] 负责 trim/规范化/后到覆盖），
    /// 返回新集合写入条数。
    ///
    /// 在**同一实例**（同一 `Arc`）上原地更新（DashMap `retain` + insert），
    /// 行情流/各账户会话/build_api 等既有持有方无需换句柄即可看到最新集合。
    /// 供每日规格刷新实现「以新替旧」语义；与 `load_from_csv` 的纯追加不同，
    /// 本方法会移除快照里已消失的合约。
    pub fn replace_all(&self, rows: impl IntoIterator<Item = InstrumentInfo>) -> usize {
        let incoming: Vec<InstrumentInfo> = rows.into_iter().collect();
        // 新集合的合约键（trim、去空白）；与 remember 的规范化口径一致。
        let keys: std::collections::HashSet<String> = incoming
            .iter()
            .map(|i| i.instrument.trim().to_string())
            .filter(|k| !k.is_empty())
            .collect();
        // 先剔除新集合中已不存在的旧合约……
        self.by_instrument.retain(|k, _| keys.contains(k));
        // ……再逐条 upsert（后到覆盖，可纠正乘数/跳价/保证金率漂移）。
        for info in incoming {
            self.remember(info);
        }
        keys.len()
    }

    /// 已缓存的合约数（诊断/测试用）。
    pub fn len(&self) -> usize {
        self.by_instrument.len()
    }

    /// 是否为空（诊断/测试用）。
    pub fn is_empty(&self) -> bool {
        self.by_instrument.is_empty()
    }
}

/// 解析 instruments.csv 的一行（固定列序，见 [`InstrumentDir::load_from_csv`]）。
/// 仅当 product_class 为 1（期货）或 2（期权）且 is_trading=1 时返回
/// [`InstrumentInfo`]；组合/摘牌/缺列/数值非法返回 None（跳过）。
fn parse_instrument_csv_row(line: &str) -> Option<InstrumentInfo> {
    let f: Vec<&str> = line.split(',').collect();
    // 至少需要到 is_trading（第 14 列，索引 13）。
    if f.len() < 14 {
        return None;
    }
    let instrument = f[0].trim().trim_start_matches('\u{feff}');
    if instrument.is_empty() {
        return None;
    }
    // product_class=1 期货、2 期权；其余（3 组合、5/6/7 等）不收。
    let product_class = match f[4].trim() {
        "1" => PRODUCT_FUTURES,
        "2" => PRODUCT_OPTIONS,
        _ => return None,
    };
    // is_trading=1 才在交易。
    if f[13].trim() != "1" {
        return None;
    }
    let parse_i32 = |s: &str| s.trim().parse::<i32>().unwrap_or(0);
    let parse_f64 = |s: &str| {
        s.trim()
            .parse::<f64>()
            .ok()
            .filter(|v| v.is_finite())
            .unwrap_or(0.0)
    };
    Some(InstrumentInfo {
        instrument: instrument.to_string(),
        exchange_id: f[1].trim().to_string(),
        product_id: f[2].trim().to_string(),
        product_class,
        is_trading: true,
        volume_multiple: parse_i32(f[5]),
        price_tick: parse_f64(f[6]),
        max_limit_volume: parse_i32(f[8]),
        max_market_volume: parse_i32(f[10]),
        long_margin_ratio: parse_f64(f[11]),
        short_margin_ratio: parse_f64(f[12]),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(instrument: &str, exchange: &str, multiple: i32, tick: f64) -> InstrumentInfo {
        InstrumentInfo {
            instrument: instrument.into(),
            exchange_id: exchange.into(),
            product_id: "rb".into(),
            product_class: PRODUCT_FUTURES,
            is_trading: true,
            volume_multiple: multiple,
            price_tick: tick,
            max_limit_volume: 0,
            max_market_volume: 0,
            long_margin_ratio: 0.0,
            short_margin_ratio: 0.0,
        }
    }

    #[test]
    fn remember_then_reads_static_fields() {
        let dir = InstrumentDir::new();
        dir.remember(info(" rb2510 ", "SHFE", 10, 1.0));
        assert_eq!(dir.exchange("rb2510").as_deref(), Some("SHFE"));
        assert_eq!(dir.multiple("rb2510"), 10);
        assert_eq!(dir.get("rb2510").map(|i| i.price_tick), Some(1.0));
        assert_eq!(dir.len(), 1, "trim 后同合约只占一条");
    }

    #[test]
    fn exchange_distinguishes_shfe_dce_and_unknown() {
        let dir = InstrumentDir::new();
        dir.remember(info("rb2510", "SHFE", 10, 1.0));
        dir.remember(info("i2601", "DCE", 100, 0.5));
        assert_eq!(dir.exchange("rb2510").as_deref(), Some("SHFE"));
        assert_eq!(dir.exchange("i2601").as_deref(), Some("DCE"));
        // 未预热合约返回 None（调用方据此不猜测为 SHFE）。
        assert_eq!(dir.exchange("xx9999"), None);
    }

    #[test]
    fn missing_fields_default_to_zero_and_blank_ignored() {
        let dir = InstrumentDir::new();
        assert_eq!(dir.multiple("nope"), 0);
        assert!(dir.exchange("nope").is_none());
        assert!(dir.get("nope").is_none());
        dir.remember(info("  ", "SHFE", 10, 1.0));
        assert!(dir.is_empty(), "空白合约代码忽略");
    }

    #[test]
    fn later_remember_overwrites() {
        let dir = InstrumentDir::new();
        dir.remember(info("rb2510", "SHFE", 10, 1.0));
        dir.remember(info("rb2510", "SHFE", 10, 2.0));
        assert_eq!(dir.get("rb2510").map(|i| i.price_tick), Some(2.0));
        assert_eq!(dir.len(), 1);
    }

    #[test]
    fn margin_ratios_read_back_and_default_to_zero_pair() {
        // 已预热合约：多/空保证金率原样读回（持仓 UseMargin=0 兜底依据）。
        let dir = InstrumentDir::new();
        dir.remember(InstrumentInfo {
            long_margin_ratio: 0.10,
            short_margin_ratio: 0.20,
            ..info("rb2510", "SHFE", 10, 1.0)
        });
        let (long, short) = dir.margin_ratios("rb2510");
        assert!((long - 0.10).abs() < 1e-12, "多头保证金率");
        assert!((short - 0.20).abs() < 1e-12, "空头保证金率");
        // 未预热合约：返回 (0,0)，调用方据此判比率未就绪（不猜测）。
        assert_eq!(dir.margin_ratios("nope"), (0.0, 0.0));
    }

    #[test]
    fn load_from_csv_ingests_trading_futures_and_options_and_maps_fields() {
        // 列序与 data/ctp-specs/instruments.csv 一致（BOM 仅在表头首列，按列索引取）。
        let csv = "\u{feff}instrument_id,exchange_id,product_id,instrument_name,product_class,volume_multiple,price_tick,min_limit_volume,max_limit_volume,min_market_volume,max_market_volume,long_margin_ratio,short_margin_ratio,is_trading,open_date,expire_date\n\
cu2611,SHFE,cu,cu2611,1,5,10,1,500,1,60,0.1,0.1,1,20251118,20261116\n\
cu2612,SHFE,cu,cu2612,1,5,10,1,500,1,60,0.07,0.07,1,20251216,20261215\n\
cu2612C146000,SHFE,cu_o,cu2612C146000,2,5,2,1,100,1,30,0.2,0.2,1,20260402,20261124\n\
zz9999,XXXX,zz,zz9999,3,1,1,1,10,1,10,0,0,1,20260101,20261231\n";
        let dir = InstrumentDir::new();
        let n = dir.load_from_csv(csv.as_bytes()).expect("parse ok");
        // 收在交易期货与在交易期权（2 + 1）；组合(3) 剔除。
        assert_eq!(n, 3);
        assert_eq!(dir.len(), 3);
        let cu2611 = dir.get("cu2611").expect("cu2611 loaded");
        assert_eq!(cu2611.exchange_id, "SHFE");
        assert_eq!(cu2611.product_id, "cu");
        assert_eq!(cu2611.product_class, PRODUCT_FUTURES);
        assert!(cu2611.is_trading);
        assert_eq!(cu2611.volume_multiple, 5);
        assert_eq!(cu2611.price_tick as i64, 10);
        assert_eq!(cu2611.max_limit_volume, 500);
        assert_eq!(cu2611.max_market_volume, 60);
        assert!((cu2611.long_margin_ratio - 0.1).abs() < 1e-12);
        // 期权与期货同表：保留 product_class 供成本/市价分支判定。
        let option = dir.get("cu2612C146000").expect("期权应入内存表");
        assert_eq!(option.product_class, PRODUCT_OPTIONS);
        assert_eq!(option.volume_multiple, 5);
        assert_eq!(option.price_tick as i64, 2);
        assert!(dir.get("zz9999").is_none(), "组合(3) 不入内存表");
    }

    #[test]
    fn product_class_accessors_distinguish_futures_option_and_unknown() {
        let dir = InstrumentDir::new();
        dir.remember(info("cu2611", "SHFE", 5, 10.0));
        dir.remember(InstrumentInfo {
            product_class: PRODUCT_OPTIONS,
            ..info("cu2612C146000", "SHFE", 5, 2.0)
        });

        assert_eq!(dir.product_class("cu2611"), Some(PRODUCT_FUTURES));
        assert_eq!(dir.product_class("cu2612C146000"), Some(PRODUCT_OPTIONS));
        assert_eq!(dir.product_class("unknown"), None);
        assert!(!dir.is_option("cu2611"));
        assert!(dir.is_option("cu2612C146000"));
        assert!(!dir.is_option("unknown"), "未预热不能判定为期权");
    }

    #[test]
    fn replace_all_keeps_trading_options() {
        // 规格刷新重灌：期权必须与期货一同保留，不能被 retain 剔除。
        let dir = InstrumentDir::new();
        dir.remember(info("cu2511", "SHFE", 5, 10.0));
        let loaded = dir.replace_all([
            info("cu2611", "SHFE", 5, 10.0),
            InstrumentInfo {
                product_class: PRODUCT_OPTIONS,
                ..info("cu2612C146000", "SHFE", 5, 2.0)
            },
        ]);

        assert_eq!(loaded, 2);
        assert!(dir.get("cu2511").is_none(), "消失合约被剔除");
        assert_eq!(
            dir.get("cu2612C146000").map(|i| i.product_class),
            Some(PRODUCT_OPTIONS),
            "刷新后期权仍在目录"
        );
        assert_eq!(dir.multiple("cu2612C146000"), 5, "期权乘数可查");
    }

    #[test]
    fn load_from_csv_skips_non_trading_and_bad_rows() {
        // is_trading=0 的摘牌期货/期权不收；缺列/坏行跳过不致命。
        let csv = "instrument_id,exchange_id,product_id,instrument_name,product_class,volume_multiple,price_tick,min_limit_volume,max_limit_volume,min_market_volume,max_market_volume,long_margin_ratio,short_margin_ratio,is_trading,open_date,expire_date\n\
rb2610,SHFE,rb,rb2610,1,10,1,1,500,1,60,0.1,0.1,0,20251018,20261016\n\
ag2612,SHFE,ag,ag2612,1,15,1,1,500,1,60,0.1,0.1,1,20251216,20261215\n\
ag2612C9000,SHFE,ag_o,ag2612C9000,2,15,1,1,100,1,30,0.1,0.1,0,20251216,20261215\n\
broken,row\n";
        let dir = InstrumentDir::new();
        let n = dir.load_from_csv(csv.as_bytes()).expect("parse ok");
        assert_eq!(n, 1, "只收在交易期货 ag2612");
        assert!(dir.get("rb2610").is_none());
        assert!(dir.get("ag2612C9000").is_none(), "摘牌期权不收");
        assert!(dir.get("ag2612").is_some());
    }

    #[test]
    fn replace_all_upserts_and_removes_missing_contracts() {
        // 初始：rb2510 + i2601 两条。
        let dir = InstrumentDir::new();
        dir.remember(info("rb2510", "SHFE", 10, 1.0));
        dir.remember(info("i2601", "DCE", 100, 0.5));
        assert_eq!(dir.len(), 2);

        // 新快照：rb2510（乘数/tick 变更）+ cu2611；i2601 已消失。
        let n = dir.replace_all([
            info("rb2510", "SHFE", 12, 1.0),
            info("cu2611", "SHFE", 5, 10.0),
        ]);

        // 返回新集合条数；同一实例上 i2601 被移除、rb2510 被覆盖、cu2611 新增。
        assert_eq!(n, 2);
        assert_eq!(dir.len(), 2);
        assert!(dir.get("i2601").is_none(), "消失合约应被剔除");
        assert_eq!(dir.multiple("rb2510"), 12, "已存在合约应被覆盖");
        assert_eq!(dir.get("cu2611").map(|i| i.price_tick), Some(10.0));
    }

    #[test]
    fn replace_all_clears_dir_when_snapshot_empty() {
        let dir = InstrumentDir::new();
        dir.remember(info("rb2510", "SHFE", 10, 1.0));
        assert_eq!(dir.replace_all([]), 0);
        assert!(dir.is_empty());
    }

    #[test]
    fn load_from_csv_trading_instruments_filters_product_and_product_class() {
        // 品种展开只得到该 product_id 的在交易期货合约：即便期权 product_id
        // 与期货相同（cu），也必须按 product_class 排除。
        let csv = "\u{feff}instrument_id,exchange_id,product_id,instrument_name,product_class,volume_multiple,price_tick,min_limit_volume,max_limit_volume,min_market_volume,max_market_volume,long_margin_ratio,short_margin_ratio,is_trading,open_date,expire_date\n\
cu2611,SHFE,cu,cu2611,1,5,10,1,500,1,60,0.1,0.1,1,20251118,20261116\n\
cu2612,SHFE,cu,cu2612,1,5,10,1,500,1,60,0.1,0.1,1,20251216,20261215\n\
cu2612C146000,SHFE,cu_o,cu2612C146000,2,5,2,1,100,1,30,0.2,0.2,1,20260402,20261124\n\
cu2613C150000,SHFE,cu,cu2613C150000,2,5,2,1,100,1,30,0.2,0.2,1,20260402,20261124\n";
        let dir = InstrumentDir::new();
        dir.load_from_csv(csv.as_bytes()).unwrap();
        let got = dir.trading_instruments_for(&std::collections::HashSet::from(["cu".to_string()]));
        assert_eq!(got, vec!["cu2611".to_string(), "cu2612".to_string()]);
    }
}
