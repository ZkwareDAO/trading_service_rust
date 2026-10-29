//! K线 CSV append-only 仓储（store-core 快照机制之外的新持久化形态）。
//!
//! 与 Order/Position 的「整表快照覆盖写」不同，bar 一旦定稿不可变、tick 逐笔
//! 产生，故采用**只追加**：
//! * 目录 `root/kline/{venue}/{interval|tick}/{trading_day}/{symbol}.csv`，
//!   夜盘归下一交易日（trading_day 由交易所层归桶给值，本层不重算）；
//! * 文件首次创建写一次固定表头，之后纯追加数据行；
//! * flush 从内存待写窗口 [`BarTable`]/[`TickTable`] 批量 drain，写成功即清空，
//!   空批不创建任何目录/文件。
//!
//! 分层约束：本类型是唯一碰 K线文件的地方（文件 IO 收口在 store 层），
//! exchange/service 只写内存表，不直接落盘。

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use store_core::persistence::sanitize_dir_name;
use trading_core::kline::{BarTable, TickTable};
use trading_models::{bar_csv_header, Bar, RawTick, RAW_TICK_CSV_HEADER};

/// 本期唯一周期对应的目录段（Bar 行不携带 interval，固定 1m）。
const M1_DIR: &str = "1m";
/// tick WAL 的目录段（与周期目录区分）。
const TICK_DIR: &str = "tick";

/// 一个 CSV 目标文件的身份：venue / 周期(或 tick) / 交易日 / 合约。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CsvTarget {
    venue: String,
    interval: String,
    trading_day: u32,
    symbol: String,
}

/// 待写盘窗口的最小契约：批量取出后清空，失败时整批放回队首。
trait BatchWindow<T> {
    fn drain_all(&self) -> Vec<T>;
    fn requeue_front(&self, batch: Vec<T>);
}

impl BatchWindow<Bar> for BarTable {
    fn drain_all(&self) -> Vec<Bar> {
        BarTable::drain_all(self)
    }
    fn requeue_front(&self, batch: Vec<Bar>) {
        BarTable::requeue_front(self, batch)
    }
}

impl BatchWindow<RawTick> for TickTable {
    fn drain_all(&self) -> Vec<RawTick> {
        TickTable::drain_all(self)
    }
    fn requeue_front(&self, batch: Vec<RawTick>) {
        TickTable::requeue_front(self, batch)
    }
}

/// 一次 flush 的产出计数（观测用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FlushReport {
    /// 本轮写入（或追加）的文件数。
    pub files: usize,
    /// 本轮落盘的数据行数。
    pub rows: usize,
}

/// K线 CSV 仓储：持有数据根目录，廉价克隆，无内部缓存。
#[derive(Debug, Clone)]
pub struct KlineRepo {
    root: PathBuf,
}

impl KlineRepo {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// 批量落盘定稿 bar：drain 待写窗口，按 venue/周期/交易日/合约分组追加，
    /// 写成功后窗口已空。空表直接返回不落任何文件。
    ///
    /// 表头按 **venue 动态**生成（公共列 + 该所 extras 列）；同一目标文件的 venue 恒定，
    /// 故分组内表头一致。
    pub fn flush_bars(&self, table: &BarTable) -> std::io::Result<FlushReport> {
        self.flush_window(
            table,
            |b| bar_csv_header(&b.exchange),
            bar_target,
            encode_bar,
        )
    }

    /// 批量落盘原始 tick：drain WAL 窗口，按 venue/tick/交易日/合约分组追加。
    /// 失败语义同 [`Self::flush_bars`]。
    pub fn flush_ticks(&self, table: &TickTable) -> std::io::Result<FlushReport> {
        self.flush_window(
            table,
            |_| RAW_TICK_CSV_HEADER.to_string(),
            tick_target,
            encode_tick,
        )
    }

    /// 通用「drain 窗口 → 按目标文件分组编码 → 逐文件 append（新文件写表头）」。
    ///
    /// `header_of` 给每个目标文件的表头（bar 按 venue 动态、tick 固定）；同一分组内
    /// venue 恒定，故取组内首行即可。
    ///
    /// 失败语义（at-least-once）：先完成全部序列化再逐文件追加；若中途 IO 失败，
    /// 整批放回窗口队首下轮重试——已写出的文件可能在重试时重复追加该批，
    /// 持久化数据不丢，读回侧按 (symbol,start_time) 去重。
    fn flush_window<T>(
        &self,
        window: &dyn BatchWindow<T>,
        header_of: impl Fn(&T) -> String,
        target: impl Fn(&T) -> CsvTarget,
        encode_row: impl Fn(&T) -> std::io::Result<Vec<u8>>,
    ) -> std::io::Result<FlushReport> {
        let rows = window.drain_all();
        if rows.is_empty() {
            return Ok(FlushReport::default());
        }
        // 分组键＝目标文件身份；BTreeMap 保证落盘顺序确定。
        let mut groups: BTreeMap<CsvTarget, Vec<&T>> = BTreeMap::new();
        for row in &rows {
            groups.entry(target(row)).or_default().push(row);
        }
        // 先在内存编码全部批次（编码失败时尚未触碰任何文件）。
        let mut encoded: Vec<(CsvTarget, String, Vec<u8>)> = Vec::new();
        for (key, batch) in groups {
            // 同组 venue 恒定 → 表头取首行；理论上 batch 非空（分组即有行）。
            let header = header_of(batch[0]);
            let mut buf = Vec::new();
            for row in batch {
                buf.extend_from_slice(&encode_row(row)?);
            }
            encoded.push((key, header, buf));
        }
        // 逐文件追加；任一失败把整批放回窗口队首。
        if let Err(err) = self.append_encoded(&encoded) {
            window.requeue_front(rows);
            return Err(err);
        }
        Ok(FlushReport {
            files: encoded.len(),
            rows: rows.len(),
        })
    }

    /// 把已编码的各分组按路径追加（新文件先写该组表头）。
    fn append_encoded(&self, encoded: &[(CsvTarget, String, Vec<u8>)]) -> std::io::Result<()> {
        for (t, header, buf) in encoded {
            let path = self.csv_path(&t.venue, &t.interval, t.trading_day, &t.symbol);
            append_with_header(&path, header, buf)?;
        }
        Ok(())
    }

    /// 目标 CSV 路径：`root/kline/{venue}/{interval}/{day}/{symbol}.csv`，
    /// 各段经 store-core 同名清洗，防止异常符号穿越目录。
    fn csv_path(&self, venue: &str, interval: &str, day: u32, symbol: &str) -> PathBuf {
        self.root
            .join("kline")
            .join(sanitize_dir_name(venue))
            .join(sanitize_dir_name(interval))
            .join(day.to_string())
            .join(format!("{}.csv", sanitize_dir_name(symbol)))
    }
}

/// bar 行 → 目标文件（固定 1m 周期目录）。
fn bar_target(bar: &Bar) -> CsvTarget {
    CsvTarget {
        venue: bar.exchange.clone(),
        interval: M1_DIR.to_string(),
        trading_day: bar.trading_day,
        symbol: bar.symbol.clone(),
    }
}

/// tick 行 → 目标文件（tick 目录段，非周期）。
fn tick_target(tick: &RawTick) -> CsvTarget {
    CsvTarget {
        venue: tick.exchange.clone(),
        interval: TICK_DIR.to_string(),
        trading_day: tick.trading_day,
        symbol: tick.symbol.clone(),
    }
}

/// 单行 bar 编码为无表头 CSV 字节（公共列 + 本所 extras 列，含换行）。
fn encode_bar(bar: &Bar) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut wtr = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(&mut buf);
        // Bar 携带按 venue 动态的 extras，不能用 serde 固定字段序，改为手动按列拍平。
        wtr.write_record(bar.to_record())?;
        wtr.flush()?;
    }
    Ok(buf)
}

/// 单行 tick 编码为无表头 CSV 字节（32 列拍平）。
fn encode_tick(tick: &RawTick) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut wtr = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(&mut buf);
        wtr.write_record(tick.to_record())?;
        wtr.flush()?;
    }
    Ok(buf)
}

/// 向 `path` 追加字节；目标文件为空（不存在，或崩溃残留的 0 字节）时先写一行表头，
/// 非空则纯追加。以「打开后的实际大小」而非 `exists()` 判定，避免建文件后、写表头前
/// 崩溃留下 0 字节文件，重启后被误判为「已有表头」而永久缺表头。
fn append_with_header(path: &Path, header: &str, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    if file.metadata()?.len() == 0 {
        writeln!(file, "{header}")?;
    }
    file.write_all(bytes)?;
    file.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::path::Path;
    use tempfile::tempdir;
    use trading_core::kline::{BarTable, TickTable};
    use trading_models::{Bar, RawTick};

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn bar(symbol: &str, day: u32, start: i64, vol: i64) -> Bar {
        use trading_models::KlineExtra;
        let mut b = Bar::new("ctp", symbol, day, start, start + 60_000);
        b.open = d("75000");
        b.close = d("75100");
        b.high = d("75200");
        b.low = d("74950");
        b.volume = vol;
        b.quote_volume = d("9000000");
        // CTP 专有指标：真实交易所代码 + 周期末持仓量（extras 追加在公共列后）。
        b.extras
            .insert("exchange_id".into(), KlineExtra::Str("SHFE".into()));
        b.extras
            .insert("open_interest".into(), KlineExtra::Num(d("120000")));
        b
    }

    fn zero() -> Decimal {
        Decimal::ZERO
    }

    fn tick(symbol: &str, day: u32, ts: i64, vol: i64) -> RawTick {
        RawTick {
            exchange: "ctp".into(),
            exchange_id: "SHFE".into(),
            symbol: symbol.into(),
            timestamp: ts,
            trading_day: day,
            price: d("75100"),
            open: zero(),
            high: zero(),
            low: zero(),
            close: zero(),
            volume: vol,
            quote_volume: d("8000000"),
            open_interest: d("120000"),
            bid: [d("75090"), zero(), zero(), zero(), zero()],
            ask: [d("75110"), zero(), zero(), zero(), zero()],
            bid_vol: [3, 0, 0, 0, 0],
            ask_vol: [5, 0, 0, 0, 0],
        }
    }

    /// 读取某 CSV 全部非表头行（校验用）。
    fn read_lines(path: &Path) -> Vec<String> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn flush_bars_appends_under_venue_interval_day_symbol_with_header_once() {
        // Arrange：两根 bar 落在同一目标文件。
        let dir = tempdir().unwrap();
        let repo = KlineRepo::new(dir.path().to_path_buf());
        let table = BarTable::new();
        table.push(bar("cu2512", 20261024, 60_000, 10));
        table.push(bar("cu2512", 20261024, 120_000, 7));

        // Act：首次 flush。
        let report = repo.flush_bars(&table).unwrap();

        // Assert：路径＝root/kline/{venue}/1m/{day}/{symbol}.csv，1 个文件 2 行。
        let path = dir.path().join("kline/ctp/1m/20261024/cu2512.csv");
        assert!(path.is_file(), "bar CSV 应落在约定目录：{path:?}");
        assert_eq!(report.files, 1);
        assert_eq!(report.rows, 2);
        let lines = read_lines(&path);
        assert_eq!(lines.len(), 3); // 表头 + 2 行
        assert_eq!(lines[0], trading_models::bar_csv_header("ctp"));
        // 11 公共列 + CTP extras（exchange_id,open_interest 追加末尾）。
        assert_eq!(
            lines[1],
            "ctp,cu2512,20261024,75000,75200,74950,75100,10,9000000,60000,120000,SHFE,120000"
        );
        // flush 后待写窗口清空。
        assert!(table.is_empty());

        // Act：第二批 flush 到同一文件：只追加数据行，不重复表头。
        table.push(bar("cu2512", 20261024, 180_000, 4));
        let report2 = repo.flush_bars(&table).unwrap();
        assert_eq!(report2.rows, 1);
        let lines2 = read_lines(&path);
        assert_eq!(lines2.len(), 4); // 表头仍只一行
        assert_eq!(lines2[0], trading_models::bar_csv_header("ctp"));
        assert!(lines2[3].starts_with("ctp,cu2512,20261024,"));
    }

    #[test]
    fn flush_recovers_header_when_target_file_is_zero_bytes() {
        // Arrange：模拟「建文件后、写表头前崩溃」残留的 0 字节目标文件。
        let dir = tempdir().unwrap();
        let repo = KlineRepo::new(dir.path().to_path_buf());
        let path = dir.path().join("kline/ctp/1m/20261024/cu2512.csv");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"").unwrap(); // 已存在但为空

        let table = BarTable::new();
        table.push(bar("cu2512", 20261024, 60_000, 10));

        // Act：flush 到该残留空文件。
        repo.flush_bars(&table).unwrap();

        // Assert：首行必须是表头（不得因文件 exists 而漏写）。
        let lines = read_lines(&path);
        assert_eq!(lines[0], trading_models::bar_csv_header("ctp"));
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn flush_bars_partitions_by_day_and_symbol() {
        let dir = tempdir().unwrap();
        let repo = KlineRepo::new(dir.path().to_path_buf());
        let table = BarTable::new();
        table.push(bar("cu2512", 20261024, 60_000, 1));
        table.push(bar("cu2512", 20261025, 60_000, 1)); // 夜盘归次日 → 分文件
        table.push(bar("rb2611", 20261024, 60_000, 1));

        let report = repo.flush_bars(&table).unwrap();

        assert_eq!(report.files, 3);
        assert!(dir
            .path()
            .join("kline/ctp/1m/20261024/cu2512.csv")
            .is_file());
        assert!(dir
            .path()
            .join("kline/ctp/1m/20261025/cu2512.csv")
            .is_file());
        assert!(dir
            .path()
            .join("kline/ctp/1m/20261024/rb2611.csv")
            .is_file());
    }

    #[test]
    fn flush_bars_writes_binance_extra_columns_and_round_trips_by_header() {
        use trading_models::KlineExtra;
        // Arrange：一根 binance bar（3 个 extras），落到 binance/1m 文件。
        let dir = tempdir().unwrap();
        let repo = KlineRepo::new(dir.path().to_path_buf());
        let mut b = Bar::new("binance", "BTCUSDT", 20261028, 0, 60_000);
        b.close = d("108");
        b.volume = 4;
        b.quote_volume = d("432");
        b.extras.insert("trade_num".into(), KlineExtra::Int(1280));
        b.extras
            .insert("active_buy_volume".into(), KlineExtra::Num(d("2.5")));
        b.extras
            .insert("active_buy_quote_volume".into(), KlineExtra::Num(d("270")));
        let table = BarTable::new();
        table.push(b);

        // Act：flush 后按列名读回。
        repo.flush_bars(&table).unwrap();
        let path = dir.path().join("kline/binance/1m/20261028/BTCUSDT.csv");
        let lines = read_lines(&path);
        assert_eq!(lines[0], trading_models::bar_csv_header("binance"));
        assert_eq!(
            lines[1],
            "binance,BTCUSDT,20261028,0,0,0,108,4,432,0,60000,1280,2.5,270"
        );
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path(&path)
            .unwrap();
        let header: Vec<String> = rdr.headers().unwrap().iter().map(str::to_string).collect();
        let hdr: Vec<&str> = header.iter().map(String::as_str).collect();
        let rec = rdr.records().next().unwrap().unwrap();
        let fields: Vec<String> = rec.iter().map(str::to_string).collect();
        let back = Bar::from_record(&hdr, &fields).unwrap();
        assert_eq!(back.quote_volume, d("432"));
        assert_eq!(back.extras.get("trade_num"), Some(&KlineExtra::Int(1280)));
        assert_eq!(
            back.extras
                .get("active_buy_quote_volume")
                .and_then(|v| v.as_decimal()),
            Some(d("270"))
        );
        assert!(!back.extras.contains_key("open_interest"));
    }

    #[test]
    fn flush_ticks_writes_33_columns_and_round_trips() {
        let dir = tempdir().unwrap();
        let repo = KlineRepo::new(dir.path().to_path_buf());
        let table = TickTable::new();
        table.append(tick("cu2512", 20261024, 1_760_000_000_000, 50_000));

        let report = repo.flush_ticks(&table).unwrap();

        // tick 目录段为 "tick"（非周期）；1 文件 1 行 + 表头。
        let path = dir.path().join("kline/ctp/tick/20261024/cu2512.csv");
        assert!(path.is_file());
        assert_eq!(report.files, 1);
        assert_eq!(report.rows, 1);
        let lines = read_lines(&path);
        assert_eq!(lines[0], trading_models::RAW_TICK_CSV_HEADER);
        assert_eq!(lines.len(), 2);

        // 用 csv 读回并经 RawTick::from_record 解析：五档/标量无损。
        let mut rdr = csv::Reader::from_path(&path).unwrap();
        let rec = rdr.records().next().unwrap().unwrap();
        let fields: Vec<String> = rec.iter().map(str::to_string).collect();
        let back = RawTick::from_record(&fields).unwrap();
        assert_eq!(back.symbol, "cu2512");
        assert_eq!(back.volume, 50_000);
        assert_eq!(back.bid[0], d("75090"));
        assert_eq!(back.ask_vol[0], 5);
    }

    #[test]
    fn flush_empty_tables_is_a_noop_creating_nothing() {
        let dir = tempdir().unwrap();
        let repo = KlineRepo::new(dir.path().to_path_buf());

        let bars = repo.flush_bars(&BarTable::new()).unwrap();
        let ticks = repo.flush_ticks(&TickTable::new()).unwrap();

        assert_eq!(bars.rows, 0);
        assert_eq!(ticks.rows, 0);
        assert!(!dir.path().join("kline").exists(), "空刷盘不应创建任何目录");
    }
}
