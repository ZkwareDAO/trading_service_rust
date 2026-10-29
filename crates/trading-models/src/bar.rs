//! K线落盘行模型（与 Order/Position 同级的不可变 CSV 行）。
//!
//! 两种行：
//! * [`Bar`]——已定稿 1 分钟 K线，四家交易所共用同一组**公共强类型列**，
//!   各所差异（CTP 的 exchange_id/open_interest、币安的成交笔数等）以
//!   [`KlineExtra`] 平铺扩展列追加在公共列之后（WS 三所为原生收盘帧、
//!   CTP 为 tick 聚合产出）；
//! * [`RawTick`]——CTP 原始逐笔行情（仅 CTP 产生），五档量价在内存里是四个
//!   `[T; 5]` 数组，落 CSV 时由 [`RawTick::to_record`] 拍平成 33 列。
//!
//! 二者都**不实现 [`store_core::Model`]**：不进 store-core 快照/恢复机制，
//! 由 trading-store 的 KlineRepo 以 append-only 方式逐行追加 CSV。
//! 价格/金额一律 [`rust_decimal::Decimal`] 定点数，禁止浮点。

use std::collections::BTreeMap;
use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize, Serializer};

// =====================================================================
// 可扩展字段（公共强类型字段之外的各所平铺扩展）
// =====================================================================

/// 各所 K线扩展值的弱类型枚举：足以覆盖现有四家差异，又给新所留扩展口。
///
/// 南向 JSON 经 `#[serde(flatten)]` 平铺（Int 出整数、Num 出精确 JSON 数字、Str 出字符串）；
/// CSV 经 [`Self::to_csv`]/[`Self::from_csv`] 按各所 schema 的固定列序文本化。
///
/// 反序列化为 [`Deserialize`] 派生的 untagged（Int/Num/Str 顺序匹配）；序列化手写——
/// 因为本 crate 不能依赖 trading-core 的 `decimal_wire`，而 rust_decimal 默认派生会把
/// Decimal 输出成**字符串**，这里经 serde_json arbitrary_precision 把 Num 落成精确数字
/// （全程不碰 f64）。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum KlineExtra {
    /// 字符串扩展（如 CTP 的 `exchange_id="SHFE"`）。
    Str(String),
    /// 整数扩展（如币安成交笔数 `trade_num`）。
    Int(i64),
    /// 定点数扩展（量/额/持仓，如 `open_interest`、`active_buy_volume`）。
    Num(Decimal),
}

/// 把 [`Decimal`] 序列化为精确 JSON 数字（不经 f64）。
/// serde_json 开启 arbitrary_precision 后，`Number::from_str` 保留原始数字词法，
/// 与 trading-core `decimal_wire` 同口径（出参为数字、入参宽松接受数字或字符串）。
fn serialize_decimal_number<S: Serializer>(d: &Decimal, s: S) -> Result<S::Ok, S::Error> {
    let n = serde_json::Number::from_str(&d.to_string())
        .map_err(|e| serde::ser::Error::custom(format!("Decimal 转 JSON 数字失败：{e}")))?;
    n.serialize(s)
}

impl Serialize for KlineExtra {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            KlineExtra::Str(v) => s.serialize_str(v),
            KlineExtra::Int(v) => s.serialize_i64(*v),
            KlineExtra::Num(v) => serialize_decimal_number(v, s),
        }
    }
}

/// 扩展列的值类型（读回 CSV 时按 schema 决定解析口径）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtraKind {
    /// 文本列。
    Str,
    /// 整数列。
    Int,
    /// 定点数列。
    Num,
}

impl KlineExtra {
    /// 文本化为 CSV 单元格。
    pub fn to_csv(&self) -> String {
        match self {
            KlineExtra::Str(s) => s.clone(),
            KlineExtra::Int(i) => i.to_string(),
            KlineExtra::Num(d) => d.to_string(),
        }
    }

    /// 按列值类型从 CSV 单元格解析回扩展值。
    pub fn from_csv(kind: ExtraKind, raw: &str) -> Result<Self, String> {
        match kind {
            ExtraKind::Str => Ok(KlineExtra::Str(raw.to_string())),
            ExtraKind::Int => raw
                .parse::<i64>()
                .map(KlineExtra::Int)
                .map_err(|e| e.to_string()),
            ExtraKind::Num => raw
                .parse::<Decimal>()
                .map(KlineExtra::Num)
                .map_err(|e| e.to_string()),
        }
    }

    /// 取数值口径：Int/Num 都返回定点数，Str 尝试解析，非数值返回 None。
    /// 用于读回/比较——JSON 弱类型下整数值的 Num 可能被反序列化为 Int，数值仍相等。
    pub fn as_decimal(&self) -> Option<Decimal> {
        match self {
            KlineExtra::Int(i) => Some(Decimal::from(*i)),
            KlineExtra::Num(d) => Some(*d),
            KlineExtra::Str(s) => s.parse().ok(),
        }
    }
}

/// 某交易所的 bar extras 列定义：`(列名, 值类型)`，**顺序即该所 CSV 追加列序**
/// （非 BTreeMap 字母序，南向 flatten 用 BTreeMap 仅为 map 容器）。
///
/// 新增交易所只需在此补一行本所映射 + 本所 extras 键，公共协议/公共列零改动。
/// deribit/hyperliquid/未知所返回空切片（仅公共列）。
pub fn bar_extra_columns(venue: &str) -> &'static [(&'static str, ExtraKind)] {
    match venue.to_ascii_lowercase().as_str() {
        // 币安：成交笔数 + 主动买入（taker buy）基础量/计价额；无 exchange_id/open_interest。
        "binance" => &[
            ("trade_num", ExtraKind::Int),
            ("active_buy_volume", ExtraKind::Num),
            ("active_buy_quote_volume", ExtraKind::Num),
        ],
        // CTP：真实交易所代码（SHFE/DCE/…，venue 仍是 ctp）+ 周期末持仓量。
        "ctp" => &[
            ("exchange_id", ExtraKind::Str),
            ("open_interest", ExtraKind::Num),
        ],
        _ => &[],
    }
}

// =====================================================================
// Bar（定稿 1m K线）
// =====================================================================

/// Bar CSV 的**公共列**表头（11 列，四所严格一致；成交额键为 quote_volume）。
pub const BAR_CSV_COMMON_HEADER: &str = "exchange,symbol,trading_day,open,high,low,close,\
volume,quote_volume,start_time,end_time";

/// 公共列列数（extras 追加在此之后）。
pub const BAR_CSV_COMMON_COLUMNS: usize = 11;

/// 按交易所生成 Bar CSV 完整表头＝公共列 + 该所 extras 列（动态列序）。
/// 开发版直接换动态表头，不做旧文件兼容/迁移。
pub fn bar_csv_header(venue: &str) -> String {
    let schema = bar_extra_columns(venue);
    if schema.is_empty() {
        BAR_CSV_COMMON_HEADER.to_string()
    } else {
        let extra_names = schema
            .iter()
            .map(|(name, _)| *name)
            .collect::<Vec<_>>()
            .join(",");
        format!("{BAR_CSV_COMMON_HEADER},{extra_names}")
    }
}

/// RawTick CSV 固定表头（33 列：13 标量[含 exchange_id] + 五档价量拍平）。
pub const RAW_TICK_CSV_HEADER: &str = "exchange,exchange_id,symbol,timestamp,trading_day,\
price,open,high,low,close,volume,quote_volume,open_interest,\
bid_price1,bid_price2,bid_price3,bid_price4,bid_price5,\
ask_price1,ask_price2,ask_price3,ask_price4,ask_price5,\
bid_volume1,bid_volume2,bid_volume3,bid_volume4,bid_volume5,\
ask_volume1,ask_volume2,ask_volume3,ask_volume4,ask_volume5";

/// RawTick 落盘列数（13 标量 + 4 × 5 档）。
pub const RAW_TICK_COLUMNS: usize = 33;

/// 一根已定稿的 1 分钟 K线（不可变行；消费者只会收到定稿，不收分钟中间态）。
///
/// `volume`/`quote_volume` 为**周期内增量**（CTP 由相邻周期末累计量差分得到，
/// 绝不直接抄 tick 累计量）；`trading_day` 为 YYYYMMDD，夜盘归下一交易日，
/// 由交易所层归桶后给值，本层不重算。各所专有指标放 [`Self::extras`]。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bar {
    /// 交易所标识（binance / deribit / hyperliquid / ctp）。
    pub exchange: String,
    /// 标的：CTP 如实为具体合约（cu2512），不做主力连续拼接。
    pub symbol: String,
    /// 归属交易日 YYYYMMDD（夜盘归次日）。
    pub trading_day: u32,
    /// 开盘价。
    pub open: Decimal,
    /// 最高价。
    pub high: Decimal,
    /// 最低价。
    pub low: Decimal,
    /// 收盘价。
    pub close: Decimal,
    /// 周期内成交量（增量）。
    pub volume: i64,
    /// 周期内成交额（增量，计价货币口径；CTP=Turnover、币安=q）。
    pub quote_volume: Decimal,
    /// 桶开始时间（UTC 毫秒，含）。
    pub start_time: i64,
    /// 桶结束时间（UTC 毫秒，不含）。
    pub end_time: i64,
    /// 各所扩展字段（CTP: exchange_id/open_interest；币安: trade_num/active_buy_*）。
    /// CSV 按 [`bar_extra_columns`] 的本所固定列序追加，未知键不落盘。
    pub extras: BTreeMap<String, KlineExtra>,
}

/// Bar 记录解析错误（KlineRepo 读回/校验时用；坏行跳过不致命）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BarParseError {
    /// 列数与给定表头不符。
    BadColumnCount { expected: usize, actual: usize },
    /// 某列值无法解析（列名/列位；err 为底层解析错误文本，保 Eq 可测）。
    BadField {
        column: usize,
        name: String,
        value: String,
        err: String,
    },
}

impl std::fmt::Display for BarParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadColumnCount { expected, actual } => {
                write!(f, "Bar 列数错误：期望 {expected}，实际 {actual}")
            }
            Self::BadField {
                column,
                name,
                value,
                err,
            } => write!(
                f,
                "Bar 第 {column} 列({name})解析失败（值={value:?}）：{err}"
            ),
        }
    }
}

impl std::error::Error for BarParseError {}

impl Bar {
    /// 装配一根空桶 bar（OHLC/量额缺省为 0，extras 空，由聚合/解码侧填入）。
    pub fn new(
        exchange: impl Into<String>,
        symbol: impl Into<String>,
        trading_day: u32,
        start_time: i64,
        end_time: i64,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            symbol: symbol.into(),
            trading_day,
            open: Decimal::ZERO,
            high: Decimal::ZERO,
            low: Decimal::ZERO,
            close: Decimal::ZERO,
            volume: 0,
            quote_volume: Decimal::ZERO,
            start_time,
            end_time,
            extras: BTreeMap::new(),
        }
    }

    /// 按 [`bar_csv_header`] 的列序拍平成字符串字段：11 个公共列 + 本所 extras 列。
    pub fn to_record(&self) -> Vec<String> {
        let schema = bar_extra_columns(&self.exchange);
        let mut fields: Vec<String> = Vec::with_capacity(BAR_CSV_COMMON_COLUMNS + schema.len());
        fields.push(self.exchange.clone());
        fields.push(self.symbol.clone());
        fields.push(self.trading_day.to_string());
        for d in [self.open, self.high, self.low, self.close] {
            fields.push(d.to_string());
        }
        fields.push(self.volume.to_string());
        fields.push(self.quote_volume.to_string());
        fields.push(self.start_time.to_string());
        fields.push(self.end_time.to_string());
        for (name, _) in schema {
            fields.push(
                self.extras
                    .get(*name)
                    .map(KlineExtra::to_csv)
                    .unwrap_or_default(),
            );
        }
        fields
    }

    /// 按**表头列名**读回一行（公共列必填；extras 仅读本所 schema 中、表头存在的列，
    /// 空单元格视为缺省）。表头/列数不符即错，杜绝错列。
    pub fn from_record(header: &[&str], fields: &[String]) -> Result<Self, BarParseError> {
        if header.len() != fields.len() {
            return Err(BarParseError::BadColumnCount {
                expected: header.len(),
                actual: fields.len(),
            });
        }
        let exchange = fields[bar_index_of(header, "exchange")?].clone();
        let symbol = fields[bar_index_of(header, "symbol")?].clone();

        // extras：读本所 schema 且表头实际存在的列。
        let mut extras = BTreeMap::new();
        for (name, kind) in bar_extra_columns(&exchange) {
            if let Some(c) = header.iter().position(|h| h == name) {
                let raw = fields[c].trim();
                if !raw.is_empty() {
                    let v = KlineExtra::from_csv(*kind, raw).map_err(|err| {
                        BarParseError::BadField {
                            column: c,
                            name: (*name).to_string(),
                            value: fields[c].clone(),
                            err,
                        }
                    })?;
                    extras.insert((*name).to_string(), v);
                }
            }
        }

        Ok(Self {
            exchange,
            symbol,
            trading_day: bar_parse_at(header, fields, "trading_day")?,
            open: bar_parse_at(header, fields, "open")?,
            high: bar_parse_at(header, fields, "high")?,
            low: bar_parse_at(header, fields, "low")?,
            close: bar_parse_at(header, fields, "close")?,
            volume: bar_parse_at(header, fields, "volume")?,
            quote_volume: bar_parse_at(header, fields, "quote_volume")?,
            start_time: bar_parse_at(header, fields, "start_time")?,
            end_time: bar_parse_at(header, fields, "end_time")?,
            extras,
        })
    }
}

/// 按列名定位列位；公共列缺失属表头与 schema 不符（不应当发生）。
fn bar_index_of(header: &[&str], name: &str) -> Result<usize, BarParseError> {
    header
        .iter()
        .position(|h| *h == name)
        .ok_or_else(|| BarParseError::BadField {
            column: usize::MAX,
            name: name.to_string(),
            value: String::new(),
            err: "表头缺列".to_string(),
        })
}

/// 按列名取单元格并解析为 `T`；失败统一带列位/列名/原值/底层错误，便于定位坏行。
fn bar_parse_at<T>(header: &[&str], fields: &[String], name: &str) -> Result<T, BarParseError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let c = bar_index_of(header, name)?;
    fields[c].parse::<T>().map_err(|e| BarParseError::BadField {
        column: c,
        name: name.to_string(),
        value: fields[c].clone(),
        err: e.to_string(),
    })
}

/// 一笔 CTP 原始逐笔行情（WAL 落盘行，仅 CTP 产生）。
///
/// 时间戳由 ActionDay+UpdateTime+UpdateMillisec 合成 UTC 毫秒；
/// `volume`/`quote_volume` 为**当日累计**口径（与 [`Bar`] 的周期增量不同，
/// K线聚合在交易所层做差分）。五档缺档用 0 表示。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawTick {
    /// venue 名称（恒为 ctp）。
    pub exchange: String,
    /// 真实交易所代码（SHFE/DCE/CZCE/CFFEX/INE/GFEX，取自柜台 ExchangeID）。
    pub exchange_id: String,
    /// 标的名称。
    pub symbol: String,
    /// 交易所时间（UTC 毫秒）。
    pub timestamp: i64,
    /// 归属交易日 YYYYMMDD（夜盘已归次日）。
    pub trading_day: u32,
    /// 最新价格。
    pub price: Decimal,
    /// 当日开盘价。
    pub open: Decimal,
    /// 当日最高价。
    pub high: Decimal,
    /// 当日最低价。
    pub low: Decimal,
    /// 当日收盘价（盘中为最新价快照）。
    pub close: Decimal,
    /// 当日累计成交量。
    pub volume: i64,
    /// 当日累计成交金额。
    pub quote_volume: Decimal,
    /// 当前市场总持仓量。
    pub open_interest: Decimal,
    /// 买一~买五价。
    pub bid: [Decimal; 5],
    /// 卖一~卖五价。
    pub ask: [Decimal; 5],
    /// 买一~买五量。
    pub bid_vol: [i32; 5],
    /// 卖一~卖五量。
    pub ask_vol: [i32; 5],
}

/// RawTick 记录解析错误（KlineRepo 读回/校验时用；坏行跳过不致命）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawTickParseError {
    /// 列数与 33 列固定表头不符。
    BadColumnCount { expected: usize, actual: usize },
    /// 某列值无法解析（列位从 0 起；err 为底层解析错误文本，保 Eq 可测）。
    BadField {
        column: usize,
        value: String,
        err: String,
    },
}

impl std::fmt::Display for RawTickParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadColumnCount { expected, actual } => {
                write!(f, "RawTick 列数错误：期望 {expected}，实际 {actual}")
            }
            Self::BadField { column, value, err } => {
                write!(f, "RawTick 第 {column} 列解析失败（值={value:?}）：{err}")
            }
        }
    }
}

impl std::error::Error for RawTickParseError {}

// RawTick 固定列位（exchange_id 紧随 exchange；其后标量整体后移一位）。
const TICK_EXCHANGE: usize = 0;
const TICK_EXCHANGE_ID: usize = 1;
const TICK_SYMBOL: usize = 2;
const TICK_TIMESTAMP: usize = 3;
const TICK_TRADING_DAY: usize = 4;
const TICK_PRICE: usize = 5;
const TICK_VOLUME: usize = 10;
const TICK_QUOTE_VOLUME: usize = 11;
const TICK_OPEN_INTEREST: usize = 12;
const TICK_BID_START: usize = 13;
/// 五档价区终点（=卖价起点，不单独命名以避免半开区间歧义）。
const TICK_BID_VOL_START: usize = 23;

impl RawTick {
    /// 按 [`RAW_TICK_CSV_HEADER`] 的列序拍平成 33 个字符串字段（供 CSV writer）。
    pub fn to_record(&self) -> Vec<String> {
        let mut fields: Vec<String> = Vec::with_capacity(RAW_TICK_COLUMNS);
        fields.push(self.exchange.clone());
        fields.push(self.exchange_id.clone());
        fields.push(self.symbol.clone());
        fields.push(self.timestamp.to_string());
        fields.push(self.trading_day.to_string());
        for d in [self.price, self.open, self.high, self.low, self.close] {
            fields.push(d.to_string());
        }
        fields.push(self.volume.to_string());
        fields.push(self.quote_volume.to_string());
        fields.push(self.open_interest.to_string());
        for d in self.bid {
            fields.push(d.to_string());
        }
        for d in self.ask {
            fields.push(d.to_string());
        }
        for v in self.bid_vol {
            fields.push(v.to_string());
        }
        for v in self.ask_vol {
            fields.push(v.to_string());
        }
        fields
    }

    /// 从 33 列字段解析回行（列序同 [`Self::to_record`]）；列数/类型不符即错。
    pub fn from_record(fields: &[String]) -> Result<Self, RawTickParseError> {
        if fields.len() != RAW_TICK_COLUMNS {
            return Err(RawTickParseError::BadColumnCount {
                expected: RAW_TICK_COLUMNS,
                actual: fields.len(),
            });
        }
        let dec = |column: usize| -> Result<Decimal, RawTickParseError> {
            fields[column]
                .parse::<Decimal>()
                .map_err(|e| RawTickParseError::BadField {
                    column,
                    value: fields[column].clone(),
                    err: e.to_string(),
                })
        };
        let parse_i64 = |column: usize| -> Result<i64, RawTickParseError> {
            fields[column]
                .parse::<i64>()
                .map_err(|e| RawTickParseError::BadField {
                    column,
                    value: fields[column].clone(),
                    err: e.to_string(),
                })
        };
        let parse_u32 = |column: usize| -> Result<u32, RawTickParseError> {
            fields[column]
                .parse::<u32>()
                .map_err(|e| RawTickParseError::BadField {
                    column,
                    value: fields[column].clone(),
                    err: e.to_string(),
                })
        };
        let parse_i32 = |column: usize| -> Result<i32, RawTickParseError> {
            fields[column]
                .parse::<i32>()
                .map_err(|e| RawTickParseError::BadField {
                    column,
                    value: fields[column].clone(),
                    err: e.to_string(),
                })
        };

        let mut arrays: Vec<Decimal> = Vec::with_capacity(10);
        for column in TICK_BID_START..TICK_BID_VOL_START {
            arrays.push(dec(column)?);
        }
        let mut vols: Vec<i32> = Vec::with_capacity(10);
        for column in TICK_BID_VOL_START..RAW_TICK_COLUMNS {
            vols.push(parse_i32(column)?);
        }

        Ok(Self {
            exchange: fields[TICK_EXCHANGE].clone(),
            exchange_id: fields[TICK_EXCHANGE_ID].clone(),
            symbol: fields[TICK_SYMBOL].clone(),
            timestamp: parse_i64(TICK_TIMESTAMP)?,
            trading_day: parse_u32(TICK_TRADING_DAY)?,
            price: dec(TICK_PRICE)?,
            open: dec(6)?,
            high: dec(7)?,
            low: dec(8)?,
            close: dec(9)?,
            volume: parse_i64(TICK_VOLUME)?,
            quote_volume: dec(TICK_QUOTE_VOLUME)?,
            open_interest: dec(TICK_OPEN_INTEREST)?,
            bid: arrays[0..5].try_into().unwrap(),
            ask: arrays[5..10].try_into().unwrap(),
            bid_vol: vols[0..5].try_into().unwrap(),
            ask_vol: vols[5..10].try_into().unwrap(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    /// 测试用定点数装配（字符串口径，避免浮点）。
    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn sample_bar() -> Bar {
        let mut bar = Bar::new("ctp", "cu2512", 20261024, 60_000, 120_000);
        bar.open = d("75200");
        bar.high = d("75350");
        bar.low = d("75180");
        bar.close = d("75300");
        bar.volume = 128;
        bar.quote_volume = d("9650000");
        bar
    }

    #[test]
    fn extra_csv_text_round_trips_by_kind() {
        assert_eq!(KlineExtra::Str("SHFE".into()).to_csv(), "SHFE");
        assert_eq!(KlineExtra::Int(1280).to_csv(), "1280");
        assert_eq!(KlineExtra::Num(d("0.00001")).to_csv(), "0.00001");
        assert_eq!(
            KlineExtra::from_csv(ExtraKind::Str, "DCE").unwrap(),
            KlineExtra::Str("DCE".into())
        );
        assert_eq!(
            KlineExtra::from_csv(ExtraKind::Int, "42").unwrap(),
            KlineExtra::Int(42)
        );
        assert_eq!(
            KlineExtra::from_csv(ExtraKind::Num, "1.5").unwrap(),
            KlineExtra::Num(d("1.5"))
        );
        assert!(KlineExtra::from_csv(ExtraKind::Int, "x").is_err());
    }

    #[test]
    fn bar_csv_header_appends_venue_extras_after_common_prefix() {
        let common = [
            "exchange",
            "symbol",
            "trading_day",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "start_time",
            "end_time",
        ];
        // 公共前缀四所严格一致（11 列，成交额键为 quote_volume、无 open_interest）。
        let binance_h = bar_csv_header("binance");
        let binance: Vec<&str> = binance_h.split(',').collect();
        assert_eq!(&binance[..11], &common);
        // binance 按本所固定顺序追加 3 列（非 BTreeMap 字母序），且无 open_interest/exchange_id。
        assert_eq!(
            &binance[11..],
            &["trade_num", "active_buy_volume", "active_buy_quote_volume"]
        );
        assert!(!binance_h.contains("open_interest"));
        // CTP 追加 exchange_id,open_interest（open_interest 移到末尾 extras 区）。
        let ctp_h = bar_csv_header("ctp");
        let ctp: Vec<&str> = ctp_h.split(',').collect();
        assert_eq!(&ctp[..11], &common);
        assert_eq!(&ctp[11..], &["exchange_id", "open_interest"]);
        // deribit/hyperliquid/未知所：仅公共 11 列。
        let only_h = bar_csv_header("deribit");
        let only: Vec<&str> = only_h.split(',').collect();
        assert_eq!(only, common);
        assert_eq!(bar_csv_header("hyperliquid").split(',').count(), 11);
    }

    #[test]
    fn bar_record_round_trips_common_columns_with_fixed_order() {
        // 无 extras 的所（deribit）：11 个公共列固定列序、定点数无损往返。
        let mut bar = sample_bar();
        bar.exchange = "deribit".into();
        let header = bar_csv_header("deribit");
        let cols: Vec<&str> = header.split(',').collect();
        let rec = bar.to_record();
        assert_eq!(rec.len(), 11);
        let back = Bar::from_record(&cols, &rec).unwrap();
        assert_eq!(back, bar);
        assert_eq!(
            rec.join(","),
            "deribit,cu2512,20261024,75200,75350,75180,75300,128,9650000,60000,120000"
        );
    }

    #[test]
    fn bar_record_round_trips_binance_extras_in_schema_order() {
        // Arrange：一根 binance 定稿 bar，3 个 extras 故意乱序插入（CSV 仍按 schema 顺序）。
        let mut bar = Bar::new("binance", "BTCUSDT", 20261028, 0, 60_000);
        bar.quote_volume = d("900000.5");
        bar.extras.insert(
            "active_buy_quote_volume".into(),
            KlineExtra::Num(d("500.25")),
        );
        bar.extras.insert("trade_num".into(), KlineExtra::Int(1280));
        bar.extras
            .insert("active_buy_volume".into(), KlineExtra::Num(d("5.5")));

        // Act：按动态表头拍平再按列名读回。
        let header = bar_csv_header("binance");
        let cols: Vec<&str> = header.split(',').collect();
        let rec = bar.to_record();
        let back = Bar::from_record(&cols, &rec).unwrap();

        // Assert：列数＝表头；extras 按 schema 固定顺序（trade_num 在最前）；无损往返。
        assert_eq!(rec.len(), cols.len());
        assert_eq!(rec.get(8), Some(&"900000.5".to_string())); // quote_volume 公共列
        assert_eq!(rec.get(11), Some(&"1280".to_string())); // trade_num
        assert_eq!(rec.get(12), Some(&"5.5".to_string())); // active_buy_volume
        assert_eq!(rec.get(13), Some(&"500.25".to_string())); // active_buy_quote_volume
        assert_eq!(back, bar);
    }

    #[test]
    fn ctp_bar_carries_exchange_id_and_open_interest_as_extras() {
        let mut bar = Bar::new("ctp", "cu2512", 20261028, 60_000, 120_000);
        bar.quote_volume = d("9650000");
        bar.extras
            .insert("exchange_id".into(), KlineExtra::Str("SHFE".into()));
        bar.extras
            .insert("open_interest".into(), KlineExtra::Num(d("120000")));

        let header = bar_csv_header("ctp");
        let cols: Vec<&str> = header.split(',').collect();
        let rec = bar.to_record();
        let back = Bar::from_record(&cols, &rec).unwrap();

        assert_eq!(rec.get(11), Some(&"SHFE".to_string()));
        assert_eq!(rec.get(12), Some(&"120000".to_string()));
        assert_eq!(back, bar);
    }

    #[test]
    fn bar_from_record_rejects_wrong_column_count() {
        let header_h = bar_csv_header("ctp");
        let header: Vec<&str> = header_h.split(',').collect();
        let rec = vec!["ctp".to_string(), "cu2512".to_string()];
        let err = Bar::from_record(&header, &rec).unwrap_err();
        assert!(matches!(
            err,
            BarParseError::BadColumnCount {
                expected: 13,
                actual: 2
            }
        ));
    }

    #[test]
    fn raw_tick_carries_exchange_id_column() {
        // CTP 原始 tick CSV 新增 exchange_id（紧随 exchange）；写读往返钉死列位。
        let tick = RawTick {
            exchange: "ctp".into(),
            exchange_id: "DCE".into(),
            symbol: "m2512".into(),
            timestamp: 1_760_000_000_123,
            trading_day: 20261024,
            price: d("3000"),
            open: d("2990"),
            high: d("3010"),
            low: d("2980"),
            close: d("3000"),
            volume: 1234,
            quote_volume: d("3700000"),
            open_interest: d("50000"),
            bid: [
                d("2999"),
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
            ],
            ask: [
                d("3001"),
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
            ],
            bid_vol: [7, 0, 0, 0, 0],
            ask_vol: [9, 0, 0, 0, 0],
        };
        let rec = tick.to_record();
        let header: Vec<&str> = RAW_TICK_CSV_HEADER.split(',').collect();
        assert_eq!(rec.len(), header.len());
        assert_eq!(rec.len(), 33);
        assert_eq!(rec[0], "ctp");
        assert_eq!(rec[1], "DCE");
        let back = RawTick::from_record(&rec).unwrap();
        assert_eq!(back, tick);
    }

    #[test]
    fn raw_tick_header_flattens_five_level_book_into_33_columns() {
        // 列序钉死：13 个标量（exchange_id 紧随 exchange）后依次五档买价/卖价/买量/卖量。
        let cols: Vec<&str> = RAW_TICK_CSV_HEADER.split(',').collect();
        assert_eq!(cols.len(), 33);
        assert_eq!(
            &cols[..13],
            &[
                "exchange",
                "exchange_id",
                "symbol",
                "timestamp",
                "trading_day",
                "price",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "quote_volume",
                "open_interest",
            ]
        );
        assert_eq!(
            &cols[13..18],
            &[
                "bid_price1",
                "bid_price2",
                "bid_price3",
                "bid_price4",
                "bid_price5"
            ]
        );
        assert_eq!(
            &cols[28..33],
            &[
                "ask_volume1",
                "ask_volume2",
                "ask_volume3",
                "ask_volume4",
                "ask_volume5"
            ]
        );
    }

    #[test]
    fn raw_tick_record_round_trips_with_levels_flattened() {
        // Arrange：含五档量价的一笔原始 tick。
        let zero = Decimal::ZERO;
        let tick = RawTick {
            exchange: "ctp".into(),
            exchange_id: "SHFE".into(),
            symbol: "cu2512".into(),
            timestamp: 1_760_000_000_123,
            trading_day: 20261024,
            price: d("75300"),
            open: d("75000"),
            high: d("75400"),
            low: d("74950"),
            close: d("75300"),
            volume: 50_210,
            quote_volume: d("18900000000"),
            open_interest: d("120000"),
            bid: [d("75290"), zero, zero, zero, zero],
            ask: [d("75310"), zero, zero, zero, zero],
            bid_vol: [10, 4, 0, 0, 0],
            ask_vol: [6, 0, 0, 0, 0],
        };

        // Act：拍平成 CSV 记录再解析回行。
        let rec = tick.to_record();
        assert_eq!(rec.len(), 33);
        let back = RawTick::from_record(&rec).unwrap();

        // Assert：标量（含 exchange_id）与五档全部无损往返。
        assert_eq!(back, tick);
        assert_eq!(rec.get(1), Some(&"SHFE".to_string()));
        assert_eq!(rec.get(13), Some(&"75290".to_string())); // bid_price1
        assert_eq!(rec.get(32), Some(&"0".to_string())); // ask_volume5
    }

    #[test]
    fn raw_tick_from_record_rejects_wrong_column_count() {
        // Arrange：只有 3 列的坏记录。
        let rec = vec!["ctp".to_string(), "cu2512".to_string(), "1".to_string()];

        // Act + Assert：列数不符直接报错而非 panic/错列。
        let err = RawTick::from_record(&rec).unwrap_err();
        assert!(matches!(
            err,
            RawTickParseError::BadColumnCount { expected: 33, .. }
        ));
    }

    #[test]
    fn raw_tick_from_record_reports_bad_field_position() {
        // Arrange：33 列但 timestamp（第 3 列）不是整数。
        let mut rec = vec!["0".to_string(); 33];
        rec[0] = "ctp".into();
        rec[1] = "SHFE".into();
        rec[2] = "cu2512".into();
        rec[3] = "not-a-time".into();

        // Act + Assert：错误带列位，便于定位坏行。
        let err = RawTick::from_record(&rec).unwrap_err();
        match err {
            RawTickParseError::BadField { column, value, .. } => {
                assert_eq!(column, 3);
                assert_eq!(value, "not-a-time");
            }
            other => panic!("期望 BadField，实际 {other:?}"),
        }
    }
}
