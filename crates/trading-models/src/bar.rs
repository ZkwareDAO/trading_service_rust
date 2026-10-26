//! K线落盘行模型（与 Order/Position 同级的不可变 CSV 行）。
//!
//! 两种行：
//! * [`Bar`]——已定稿 1 分钟 K线，四家交易所同构（CTP 为 tick 聚合产出，
//!   WS 三家为交易所原生收盘帧），12 列固定列序；
//! * [`RawTick`]——CTP 原始逐笔行情（仅 CTP 产生），五档量价在内存里是四个
//!   `[T; 5]` 数组，落 CSV 时由 [`RawTick::to_record`] 拍平成 32 列。
//!
//! 二者都**不实现 [`store_core::Model`]**：不进 store-core 快照/恢复机制，
//! 由 trading-store 的 KlineRepo 以 append-only 方式逐行追加 CSV。
//! 价格/金额一律 [`rust_decimal::Decimal`] 定点数，禁止浮点。

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Bar CSV 固定表头（12 列，列序＝需求文档「CTP 1m 聚合数据」）。
pub const BAR_CSV_HEADER: &str = "exchange,symbol,trading_day,open,high,low,close,\
volume,turnover,open_interest,start_time,end_time";

/// RawTick CSV 固定表头（32 列：12 标量 + 五档价量拍平）。
pub const RAW_TICK_CSV_HEADER: &str = "exchange,symbol,timestamp,trading_day,\
price,open,high,low,close,volume,turnover,open_interest,\
bid_price1,bid_price2,bid_price3,bid_price4,bid_price5,\
ask_price1,ask_price2,ask_price3,ask_price4,ask_price5,\
bid_volume1,bid_volume2,bid_volume3,bid_volume4,bid_volume5,\
ask_volume1,ask_volume2,ask_volume3,ask_volume4,ask_volume5";

/// RawTick 落盘列数（12 标量 + 4 × 5 档）。
pub const RAW_TICK_COLUMNS: usize = 32;

/// 一根已定稿的 1 分钟 K线（不可变行；消费者只会收到定稿，不收分钟中间态）。
///
/// `volume`/`turnover` 为**周期内增量**（CTP 由相邻周期末累计量差分得到，
/// 绝不直接抄 tick 累计量）；`trading_day` 为 YYYYMMDD，夜盘归下一交易日，
/// 由交易所层归桶后给值，本层不重算。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// 周期内成交额（增量）。
    pub turnover: Decimal,
    /// 周期末总持仓量。
    pub open_interest: Decimal,
    /// 桶开始时间（UTC 毫秒，含）。
    pub start_time: i64,
    /// 桶结束时间（UTC 毫秒，不含）。
    pub end_time: i64,
}

impl Bar {
    /// 装配一根空桶 bar（OHLC/量额缺省为 0，由聚合/解码侧填入）。
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
            turnover: Decimal::ZERO,
            open_interest: Decimal::ZERO,
            start_time,
            end_time,
        }
    }
}

/// 一笔 CTP 原始逐笔行情（WAL 落盘行，仅 CTP 产生）。
///
/// 时间戳由 ActionDay+UpdateTime+UpdateMillisec 合成 UTC 毫秒；
/// `volume`/`turnover` 为**当日累计**口径（与 [`Bar`] 的周期增量不同，
/// K线聚合在交易所层做差分）。五档缺档用 0 表示。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawTick {
    /// 交易所名称。
    pub exchange: String,
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
    pub turnover: Decimal,
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
    /// 列数与 32 列固定表头不符。
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

impl RawTick {
    /// 按 [`RAW_TICK_CSV_HEADER`] 的列序拍平成 32 个字符串字段（供 CSV writer）。
    pub fn to_record(&self) -> Vec<String> {
        let mut fields: Vec<String> = Vec::with_capacity(RAW_TICK_COLUMNS);
        fields.push(self.exchange.clone());
        fields.push(self.symbol.clone());
        fields.push(self.timestamp.to_string());
        fields.push(self.trading_day.to_string());
        for d in [self.price, self.open, self.high, self.low, self.close] {
            fields.push(d.to_string());
        }
        fields.push(self.volume.to_string());
        fields.push(self.turnover.to_string());
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

    /// 从 32 列字段解析回行（列序同 [`Self::to_record`]）；列数/类型不符即错。
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
        for column in 12..22 {
            arrays.push(dec(column)?);
        }
        let mut vols: Vec<i32> = Vec::with_capacity(10);
        for column in 22..32 {
            vols.push(parse_i32(column)?);
        }

        Ok(Self {
            exchange: fields[0].clone(),
            symbol: fields[1].clone(),
            timestamp: parse_i64(2)?,
            trading_day: parse_u32(3)?,
            price: dec(4)?,
            open: dec(5)?,
            high: dec(6)?,
            low: dec(7)?,
            close: dec(8)?,
            volume: parse_i64(9)?,
            turnover: dec(10)?,
            open_interest: dec(11)?,
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
        bar.turnover = d("9650000");
        bar.open_interest = d("120000");
        bar
    }

    #[test]
    fn bar_csv_header_matches_ctp_aggregated_twelve_columns() {
        // 列序钉死需求文档「CTP 1m 聚合数据」：交易所/合约/交易日/OHLC/增量量额/持仓/起止。
        const EXPECTED: [&str; 12] = [
            "exchange",
            "symbol",
            "trading_day",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover",
            "open_interest",
            "start_time",
            "end_time",
        ];
        let cols: Vec<&str> = BAR_CSV_HEADER.split(',').collect();
        assert_eq!(cols, EXPECTED);
    }

    #[test]
    fn bar_round_trips_through_csv_with_fixed_column_order() {
        // Arrange：一根定稿 bar。
        let bar = sample_bar();

        // Act：序列化整行再反序列化。
        let mut wtr = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(vec![]);
        wtr.serialize(&bar).unwrap();
        let bytes = String::from_utf8(wtr.into_inner().unwrap()).unwrap();
        let got: Bar = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(bytes.as_bytes())
            .deserialize()
            .next()
            .unwrap()
            .unwrap();

        // Assert：字段与列位都不变（定点数无浮点漂移）。
        assert_eq!(got, bar);
        assert_eq!(
            bytes.trim_end(),
            "ctp,cu2512,20261024,75200,75350,75180,75300,128,9650000,120000,60000,120000"
        );
    }

    #[test]
    fn raw_tick_header_flattens_five_level_book_into_32_columns() {
        // 列序钉死需求文档「CTP tick 数据」：12 个标量后依次五档买价/卖价/买量/卖量。
        let cols: Vec<&str> = RAW_TICK_CSV_HEADER.split(',').collect();
        assert_eq!(cols.len(), 32);
        assert_eq!(
            &cols[..12],
            &[
                "exchange",
                "symbol",
                "timestamp",
                "trading_day",
                "price",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "turnover",
                "open_interest",
            ]
        );
        assert_eq!(
            &cols[12..17],
            &[
                "bid_price1",
                "bid_price2",
                "bid_price3",
                "bid_price4",
                "bid_price5"
            ]
        );
        assert_eq!(
            &cols[27..32],
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
            symbol: "cu2512".into(),
            timestamp: 1_760_000_000_123,
            trading_day: 20261024,
            price: d("75300"),
            open: d("75000"),
            high: d("75400"),
            low: d("74950"),
            close: d("75300"),
            volume: 50_210,
            turnover: d("18900000000"),
            open_interest: d("120000"),
            bid: [d("75290"), d("75280"), zero, zero, zero],
            ask: [d("75310"), zero, zero, zero, zero],
            bid_vol: [10, 4, 0, 0, 0],
            ask_vol: [6, 0, 0, 0, 0],
        };

        // Act：拍平成 CSV 记录再解析回行。
        let rec = tick.to_record();
        assert_eq!(rec.len(), 32);
        let back = RawTick::from_record(&rec).unwrap();

        // Assert：标量与五档全部无损往返。
        assert_eq!(back, tick);
        assert_eq!(rec.get(12), Some(&"75290".to_string())); // bid_price1
        assert_eq!(rec.get(31), Some(&"0".to_string())); // ask_volume5
    }

    #[test]
    fn raw_tick_from_record_rejects_wrong_column_count() {
        // Arrange：只有 3 列的坏记录。
        let rec = vec!["ctp".to_string(), "cu2512".to_string(), "1".to_string()];

        // Act + Assert：列数不符直接报错而非 panic/错列。
        let err = RawTick::from_record(&rec).unwrap_err();
        assert!(matches!(
            err,
            RawTickParseError::BadColumnCount { expected: 32, .. }
        ));
    }

    #[test]
    fn raw_tick_from_record_reports_bad_field_position() {
        // Arrange：32 列但 timestamp（第 2 列）不是整数。
        let mut rec = vec!["0".to_string(); 32];
        rec[0] = "ctp".into();
        rec[1] = "cu2512".into();
        rec[2] = "not-a-time".into();

        // Act + Assert：错误带列位，便于定位坏行。
        let err = RawTick::from_record(&rec).unwrap_err();
        match err {
            RawTickParseError::BadField { column, value, .. } => {
                assert_eq!(column, 2);
                assert_eq!(value, "not-a-time");
            }
            other => panic!("期望 BadField，实际 {other:?}"),
        }
    }
}
