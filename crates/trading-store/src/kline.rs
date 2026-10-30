//! K线 CSV append-only 仓储（store-core 快照机制之外的新持久化形态）。
//!
//! 与 Order/Position 的「整表快照覆盖写」不同，bar 一旦定稿不可变、tick 逐笔
//! 产生，故采用**只追加**：
//! * 目录 `root/kline/{venue}/{interval|tick}/{trading_day}/{symbol}.csv`，
//!   夜盘归下一交易日（trading_day 由交易所层归桶给值，本层不重算）；
//! * 文件首次创建写一次固定表头，之后纯追加数据行；
//! * tick 从 [`TickTable`] drain 批量写、写成功即清空（WAL）；bar 表是单一事实源，
//!   [`KlineRepo::flush_bars`] 为**非破坏式**——只快照 dirty 追加，成功按 gen 确认清 dirty、
//!   bar 本体保留在内存；空批不创建任何目录/文件。
//!
//! 分层约束：本类型是唯一碰 K线文件的地方（文件 IO 收口在 store 层），
//! exchange/service 只写内存表，不直接落盘。

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use store_core::persistence::sanitize_dir_name;
use trading_core::kline::{BarTable, DirtyBar, SeriesKey, TickTable};
use trading_models::{bar_csv_header, Bar, RawTick, RAW_TICK_CSV_HEADER};

/// 本期唯一周期对应的目录段（Bar 行不携带 interval，固定 1m）。
const M1_DIR: &str = "1m";
/// tick WAL 的目录段（与周期目录区分）。
const TICK_DIR: &str = "tick";
/// read_range 在区间止日之后多探的交易日天数：CTP 夜盘归次日、周五夜盘归下周一，
/// 普通周末最多前滚 3 天（长假 >3 天需交易时段日历，本期不支持）。
const NIGHT_SESSION_FORWARD_DAYS: i64 = 3;

/// 一个 CSV 目标文件的身份：venue / 周期(或 tick) / 交易日 / 合约。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CsvTarget {
    venue: String,
    interval: String,
    trading_day: u32,
    symbol: String,
}

/// tick WAL 待写窗口的最小契约：批量取出后清空，失败时整批放回队首。
/// bar 不走这条 drain 路径——bar 表是单一事实源，flush 为非破坏式（见 [`KlineRepo::flush_bars`]）。
trait BatchWindow<T> {
    fn drain_all(&self) -> Vec<T>;
    fn requeue_front(&self, batch: Vec<T>);
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

    /// 批量落盘定稿 bar（**非破坏式**）：快照 dirty（bar 本体保留在表内可读），按
    /// venue/周期/交易日/合约分组追加，**全部文件成功后**才按 `(start,gen)` 确认清 dirty。
    ///
    /// 表头按 **venue 动态**生成（公共列 + 该所 extras 列）；同一目标文件的 venue 恒定，
    /// 故分组内表头一致。
    ///
    /// 失败语义（at-least-once）：编码阶段先在内存完成（未触碰文件）；append 阶段任一文件
    /// IO 失败则**不确认 dirty**，整组留下轮重写——已追加文件可能重复，读回按 start 去重兜底。
    /// 快照后同 start 又被 upsert（gen 推进）的键不会被误清（[`BarTable::mark_flushed`] 校验 gen）。
    /// 无 dirty 时空批返回、不创建任何目录/文件。
    pub fn flush_bars(&self, table: &BarTable) -> std::io::Result<FlushReport> {
        let dirty = table.snapshot_dirty();
        if dirty.is_empty() {
            return Ok(FlushReport::default());
        }
        // 分组键＝目标文件身份；BTreeMap 保证落盘顺序确定。
        let mut groups: BTreeMap<CsvTarget, Vec<&DirtyBar>> = BTreeMap::new();
        for d in &dirty {
            groups.entry(bar_target(&d.bar)).or_default().push(d);
        }
        // 先在内存编码全部批次（编码失败时尚未触碰任何文件，也未确认 dirty）。
        let mut encoded: Vec<(CsvTarget, String, Vec<u8>)> = Vec::new();
        for (target, batch) in groups {
            // 同组 venue 恒定 → 表头取首行；分组即有行，batch 必非空。
            let header = bar_csv_header(&batch[0].bar.exchange);
            let mut buf = Vec::new();
            for d in batch {
                buf.extend_from_slice(&encode_bar(&d.bar)?);
            }
            encoded.push((target, header, buf));
        }
        // 逐文件 append；任一失败：不确认 dirty，整组下轮重写（读回去重兜重复）。
        self.append_encoded(&encoded)?;
        // 全部落盘成功才确认；快照后被覆盖（gen 推进）的键由 mark 内部按 gen 保留。
        let confirms: Vec<(SeriesKey, i64, u64)> = dirty
            .iter()
            .map(|d| (d.key.clone(), d.start, d.gen))
            .collect();
        table.mark_flushed(&confirms);
        Ok(FlushReport {
            files: encoded.len(),
            rows: dirty.len(),
        })
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

    /// 读回某 venue/周期/交易日/合约的全部定稿 bar（去重、按 start 升序）。
    /// 目标不存在返回空（而非 Err）；坏行跳过、计数于 [`ReadBars::skipped`]。
    pub fn read_day(
        &self,
        venue: &str,
        interval: &str,
        trading_day: u32,
        symbol: &str,
    ) -> std::io::Result<ReadBars> {
        let path = self.csv_path(venue, interval, trading_day, symbol);
        let mut got = read_bar_file(&path)?;
        got.bars = finalize_bars(got.bars, i64::MIN, i64::MAX);
        Ok(got)
    }

    /// 读回某 venue/合约、桶起点落在 `[start_ms, end_ms)`（左闭右开，UTC 毫秒）的 bar。
    /// 枚举覆盖区间的交易日目录合并、去重、按 start 升序；不存在返回空。
    ///
    /// 目录归属：三家 7×24 所 trading_day＝UTC 自然日；CTP 夜盘归**次日**（周五夜盘归
    /// 下周一），故区间止日向后多探 3 天覆盖普通周末。最终一律按行内 `start_time` 做
    /// 左闭右开精确过滤——多读的相邻/空目录不会污染结果（长假 >3 天的前一夜盘需交易
    /// 时段日历，本期不支持）。
    pub fn read_range(
        &self,
        venue: &str,
        symbol: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> std::io::Result<ReadBars> {
        if end_ms <= start_ms {
            return Ok(ReadBars::default());
        }
        use chrono::TimeZone;
        let to_date = |ms: i64| {
            chrono::Utc
                .timestamp_millis_opt(ms)
                .single()
                .map(|t| t.date_naive())
        };
        let first = to_date(start_ms);
        // 无上界（end=i64::MAX 等非法大值）→ None，避免日历枚举溢出且不漏读。
        let last = to_date(end_ms.saturating_sub(1));

        // 只枚举磁盘上**真实存在**的交易日目录（IO 有界，与自 epoch 起的天数无关），
        // 再与查询日期窗求交；CTP 夜盘归次日，上界放宽 NIGHT_SESSION_FORWARD_DAYS。
        let interval_dir = self
            .root
            .join("kline")
            .join(sanitize_dir_name(venue))
            .join(sanitize_dir_name(M1_DIR));
        let mut acc = ReadBars::default();
        for day in list_day_dirs(&interval_dir) {
            if let Some(f) = first {
                if day < f {
                    continue;
                }
            }
            if let Some(l) = last {
                if day > l + chrono::Duration::days(NIGHT_SESSION_FORWARD_DAYS) {
                    continue;
                }
            }
            let day_u: u32 = day.format("%Y%m%d").to_string().parse().unwrap_or(0);
            let path = self.csv_path(venue, M1_DIR, day_u, symbol);
            if !path.is_file() {
                continue;
            }
            let part = read_bar_file(&path)?;
            acc.skipped += part.skipped;
            acc.bars.extend(part.bars);
        }
        acc.bars = finalize_bars(acc.bars, start_ms, end_ms);
        Ok(acc)
    }
}

/// 列出某 `…/{venue}/{interval}/` 目录下名字形如 YYYYMMDD 的真实交易日子目录（升序）。
/// 非日期名/不存在目录均忽略；只碰磁盘已有目录，避免对超大时间窗做空枚举。
fn list_day_dirs(interval_dir: &Path) -> Vec<chrono::NaiveDate> {
    use chrono::NaiveDate;
    let mut days = Vec::new();
    let Ok(rd) = fs::read_dir(interval_dir) else {
        return days;
    };
    for entry in rd.flatten() {
        if !entry.path().is_dir() {
            continue;
        }
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if let Ok(day) = NaiveDate::parse_from_str(name, "%Y%m%d") {
            days.push(day);
        }
    }
    days.sort();
    days
}

/// 解析单个 bar CSV：首行为按名映射的表头，逐行喂 [`Bar::from_record`]；
/// 坏列/坏列数的行跳过计 skipped（不致命），结构性 CSV/IO 错误才返回 Err。
/// 文件不存在按空结果处理（调用方多以 is_file 预判，此处再兜底）。
fn read_bar_file(path: &Path) -> std::io::Result<ReadBars> {
    if !path.is_file() {
        return Ok(ReadBars::default());
    }
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        // 允许列数与表头不一致的坏行被逐行读出，交给 from_record 判 BadColumnCount
        // 并计入 skipped；否则 reader 会在 UnequalLengths 处整体中断、好行也读不到。
        .flexible(true)
        .from_path(path)?;
    let header: Vec<String> = rdr.headers()?.iter().map(str::to_string).collect();
    if header.is_empty() {
        // 崩溃残留的 0 字节文件：无表头可读，按空文件处理。
        return Ok(ReadBars::default());
    }
    let hdr: Vec<&str> = header.iter().map(String::as_str).collect();

    let mut out = ReadBars::default();
    for rec in rdr.records() {
        let rec = rec?;
        let fields: Vec<String> = rec.iter().map(str::to_string).collect();
        match Bar::from_record(&hdr, &fields) {
            Ok(bar) => out.bars.push(bar),
            Err(err) => {
                tracing::debug!(?path, %err, "K线 bar CSV 坏行已跳过");
                out.skipped += 1;
            }
        }
    }
    Ok(out)
}

/// 跨文件按主键 `(trading_day, start_time)` 去重（首现保留，兜 at-least-once 重写），
/// 再按 `start_time` 升序，并按 `[lo, hi)` 左闭右开精确过滤。
fn finalize_bars(bars: Vec<Bar>, lo: i64, hi: i64) -> Vec<Bar> {
    let mut by_key: BTreeMap<(u32, i64), Bar> = BTreeMap::new();
    for bar in bars {
        // 同主键保留先读到的那根（同一物理 bar 重写时内容一致，首现即可）。
        by_key
            .entry((bar.trading_day, bar.start_time))
            .or_insert(bar);
    }
    let mut out: Vec<Bar> = by_key
        .into_values()
        .filter(|b| b.start_time >= lo && b.start_time < hi)
        .collect();
    out.sort_by_key(|b| b.start_time);
    out
}

/// 一次 bar 读回的结果：好行（去重、按 start 升序）+ 被跳过的坏行数。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReadBars {
    /// 解析成功、按主键去重后的 bar，按 start_time 升序。
    pub bars: Vec<Bar>,
    /// 坏列/坏列数等被跳过的数据行数（不含表头）。
    pub skipped: usize,
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
    use trading_core::kline::{BarTable, SeriesKey, TickTable};
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
        table.upsert(bar("cu2512", 20261024, 60_000, 10));
        table.upsert(bar("cu2512", 20261024, 120_000, 7));

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
        // 非破坏式：flush 后 bar 本体仍在表内可读，但 dirty 已清空。
        let k = SeriesKey::new("ctp", "cu2512");
        assert_eq!(table.coverage(&k), 2);
        assert!(table.snapshot_dirty().is_empty());

        // Act：无新 upsert 再 flush —— 空批，不重复写、不重建文件。
        let report_idle = repo.flush_bars(&table).unwrap();
        assert_eq!(report_idle.rows, 0);
        assert_eq!(read_lines(&path).len(), 3);

        // Act：新增一根后再 flush：只追加数据行，不重复表头。
        table.upsert(bar("cu2512", 20261024, 180_000, 4));
        let report2 = repo.flush_bars(&table).unwrap();
        assert_eq!(report2.rows, 1);
        let lines2 = read_lines(&path);
        assert_eq!(lines2.len(), 4); // 表头仍只一行
        assert_eq!(lines2[0], trading_models::bar_csv_header("ctp"));
        assert!(lines2[3].starts_with("ctp,cu2512,20261024,"));
        // bar 仍保留（共 3 根）。
        assert_eq!(table.coverage(&k), 3);
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
        table.upsert(bar("cu2512", 20261024, 60_000, 10));

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
        table.upsert(bar("cu2512", 20261024, 60_000, 1));
        // 夜盘 23:00 UTC 那根归次日 → 同序列不同 start、分属两个交易日文件。
        table.upsert(bar("cu2512", 20261025, 82_800_000, 1));
        table.upsert(bar("rb2611", 20261024, 60_000, 1));

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
        table.upsert(b);

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
    fn flush_bars_stale_gen_confirmation_rewrites_newer_value() {
        // 场景：snapshot(gen=1) 后、确认前同 start 又被覆盖（gen→2）。
        let dir = tempdir().unwrap();
        let repo = KlineRepo::new(dir.path().to_path_buf());
        let table = BarTable::new();
        let k = SeriesKey::new("ctp", "cu2512");

        // 第一轮 dirty 快照（gen=1，volume=10）。
        table.upsert(bar("cu2512", 20261024, 60_000, 10));
        let snap1 = table.snapshot_dirty();
        assert_eq!(snap1.len(), 1);

        // 写中同 start 被覆盖（gen→2，volume=20），随后用旧 gen=1 确认。
        table.upsert(bar("cu2512", 20261024, 60_000, 20));
        let stale: Vec<(SeriesKey, i64, u64)> = snap1
            .iter()
            .map(|d| (d.key.clone(), d.start, d.gen))
            .collect();
        table.mark_flushed(&stale);

        // 旧 gen 不得清 dirty：仍是 gen=2 的新值，下轮必须重写。
        let pending = table.snapshot_dirty();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].bar.volume, 20);

        // 真实下一轮 flush 写新值并成功确认。
        let report = repo.flush_bars(&table).unwrap();
        assert_eq!(report.rows, 1);
        assert!(table.snapshot_dirty().is_empty());
        assert_eq!(table.coverage(&k), 1); // bar 本体保留
        let path = dir.path().join("kline/ctp/1m/20261024/cu2512.csv");
        let lines = read_lines(&path);
        assert_eq!(lines.len(), 2); // 表头 + 唯一一行新值（旧值从未落盘）
        assert_eq!(
            lines[1],
            "ctp,cu2512,20261024,75000,75200,74950,75100,20,9000000,60000,120000,SHFE,120000"
        );
    }

    #[test]
    fn flush_bars_io_failure_keeps_dirty_and_succeeds_on_retry() {
        // 坏根：把一个普通文件当作数据目录，create_dir_all 必失败（模拟磁盘/权限故障）。
        let bad_base = tempdir().unwrap();
        let bad_root = bad_base.path().join("not_a_dir");
        fs::write(&bad_root, b"x").unwrap();
        let repo_bad = KlineRepo::new(bad_root);

        let table = BarTable::new();
        table.upsert(bar("cu2512", 20261024, 60_000, 10));

        // 首次落坏根：IO Err；非破坏式 flush 不确认，dirty 原样留下轮重试。
        assert!(repo_bad.flush_bars(&table).is_err());
        assert_eq!(table.snapshot_dirty().len(), 1);
        assert_eq!(table.coverage(&SeriesKey::new("ctp", "cu2512")), 1);

        // 换好根重试（生产为同根磁盘恢复后下一轮）：成功并清 dirty、bar 保留。
        let good = tempdir().unwrap();
        let repo_good = KlineRepo::new(good.path().to_path_buf());
        let report = repo_good.flush_bars(&table).unwrap();
        assert_eq!(report.rows, 1);
        assert!(table.snapshot_dirty().is_empty());
        assert_eq!(table.coverage(&SeriesKey::new("ctp", "cu2512")), 1);
        assert!(good
            .path()
            .join("kline/ctp/1m/20261024/cu2512.csv")
            .is_file());
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

    // ---- Task3：CSV 读回 ------------------------------------------------

    /// 直接造一个 bar CSV（表头 + 给定数据行），用于去重/乱序/坏行等读回用例。
    fn write_bar_csv(dir: &Path, venue: &str, day: u32, symbol: &str, rows: &[&str]) {
        let path = dir
            .join("kline")
            .join(venue)
            .join(M1_DIR)
            .join(day.to_string())
            .join(format!("{symbol}.csv"));
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut content = String::new();
        content.push_str(&bar_csv_header(venue));
        content.push('\n');
        for r in rows {
            content.push_str(r);
            content.push('\n');
        }
        fs::write(path, content).unwrap();
    }

    /// CTP 一行（start/vol 可变，其余固定），列序＝11 公共 + exchange_id,open_interest。
    fn ctp_row(day: u32, start: i64, vol: i64) -> String {
        format!(
            "ctp,cu2512,{day},75000,75200,74950,75100,{vol},9000000,{start},{},SHFE,120000",
            start + 60_000
        )
    }

    #[test]
    fn read_day_round_trips_after_flush_including_ctp_extras() {
        let dir = tempdir().unwrap();
        let repo = KlineRepo::new(dir.path().to_path_buf());
        let table = BarTable::new();
        table.upsert(bar("cu2512", 20261024, 60_000, 10));
        table.upsert(bar("cu2512", 20261024, 120_000, 7));
        repo.flush_bars(&table).unwrap();

        let got = repo.read_day("ctp", M1_DIR, 20261024, "cu2512").unwrap();
        assert_eq!(got.skipped, 0);
        assert_eq!(got.bars.len(), 2);
        assert_eq!(got.bars[0].start_time, 60_000);
        assert_eq!(got.bars[1].start_time, 120_000);
        assert_eq!(got.bars[0].volume, 10);
        // CTP 动态 extras 按表头名解析回来。
        assert_eq!(
            got.bars[0].extras.get("exchange_id"),
            Some(&trading_models::KlineExtra::Str("SHFE".into()))
        );
    }

    #[test]
    fn read_day_dedups_same_start_and_sorts_ascending() {
        let dir = tempdir().unwrap();
        write_bar_csv(
            dir.path(),
            "ctp",
            20261024,
            "cu2512",
            &[
                // 乱序到达；60000 重复两行（at-least-once 重写），去重保留首现。
                &ctp_row(20261024, 180_000, 3),
                &ctp_row(20261024, 60_000, 10),
                &ctp_row(20261024, 60_000, 99),
                &ctp_row(20261024, 120_000, 2),
            ],
        );
        let repo = KlineRepo::new(dir.path().to_path_buf());

        let got = repo.read_day("ctp", M1_DIR, 20261024, "cu2512").unwrap();

        let starts: Vec<i64> = got.bars.iter().map(|b| b.start_time).collect();
        assert_eq!(starts, vec![60_000, 120_000, 180_000]);
        // 同 start 去重保留首现（volume=10 而非 99）；重复好行不计 skipped。
        assert_eq!(got.bars[0].volume, 10);
        assert_eq!(got.skipped, 0);
    }

    #[test]
    fn read_day_skips_bad_rows_but_returns_good_ones() {
        let dir = tempdir().unwrap();
        write_bar_csv(
            dir.path(),
            "ctp",
            20261024,
            "cu2512",
            &[
                &ctp_row(20261024, 60_000, 10),
                "ctp,cu2512,only,three", // 列数不符
                "ctp,cu2512,20261024,notnum,0,0,0,1,0,0,60000,SHFE,120000", // open 非数字
                &ctp_row(20261024, 120_000, 2),
            ],
        );
        let repo = KlineRepo::new(dir.path().to_path_buf());

        let got = repo.read_day("ctp", M1_DIR, 20261024, "cu2512").unwrap();

        let starts: Vec<i64> = got.bars.iter().map(|b| b.start_time).collect();
        assert_eq!(starts, vec![60_000, 120_000]);
        assert_eq!(got.skipped, 2, "两坏行跳过并计数");
    }

    #[test]
    fn read_day_missing_symbol_or_day_returns_empty_not_error() {
        let dir = tempdir().unwrap();
        let repo = KlineRepo::new(dir.path().to_path_buf());

        let got = repo.read_day("ctp", M1_DIR, 20261024, "cu2512").unwrap();
        assert!(got.bars.is_empty());
        assert_eq!(got.skipped, 0);
    }

    /// 真实 UTC 毫秒（read_range 按 UTC 日历日枚举目录，测试需用真实日期）。
    fn utc_ms(y: i32, m: u32, d: u32, h: u32, min: u32) -> i64 {
        use chrono::TimeZone;
        chrono::Utc
            .with_ymd_and_hms(y, m, d, h, min, 0)
            .single()
            .unwrap()
            .timestamp_millis()
    }

    #[test]
    fn read_range_merges_across_day_dirs_and_filters_half_open() {
        let dir = tempdir().unwrap();
        // 两根真实 UTC bar，分属两个自然日目录（WS 所口径：trading_day＝UTC 日）。
        let s1 = utc_ms(2026, 10, 24, 0, 30);
        let s2 = utc_ms(2026, 10, 25, 0, 30);
        write_bar_csv(
            dir.path(),
            "binance",
            20261024,
            "BTCUSDT",
            &[&binance_row(20261024, s1, 1)],
        );
        write_bar_csv(
            dir.path(),
            "binance",
            20261025,
            "BTCUSDT",
            &[&binance_row(20261025, s2, 2)],
        );
        let repo = KlineRepo::new(dir.path().to_path_buf());

        // 宽区间跨两日：合并去重、按 start 升序。
        let got = repo
            .read_range("binance", "BTCUSDT", s1, s2 + 60_000)
            .unwrap();
        assert_eq!(got.skipped, 0);
        assert_eq!(
            got.bars.iter().map(|b| b.start_time).collect::<Vec<_>>(),
            vec![s1, s2]
        );

        // 右开：end_ms == s2 时不含第二根。
        let left = repo.read_range("binance", "BTCUSDT", s1, s2).unwrap();
        assert_eq!(
            left.bars.iter().map(|b| b.start_time).collect::<Vec<_>>(),
            vec![s1]
        );

        // 完全不命中的区间 → 空而非错误。
        let none = repo
            .read_range("binance", "BTCUSDT", s2 + 999_000, s2 + 1_000_000)
            .unwrap();
        assert!(none.bars.is_empty());
    }

    #[test]
    fn read_range_finds_ctp_night_bar_in_forward_attributed_day_dir() {
        // CTP 夜盘归次日（周末可前滚数天）：bar 的 UTC 桶在 10-24 23:00，但柜台归属交易日
        // 落在 3 天后的目录 20261027。窄区间（仅覆盖该分钟）也须靠前探 padding 找到它。
        let dir = tempdir().unwrap();
        let night = utc_ms(2026, 10, 24, 23, 0);
        write_bar_csv(
            dir.path(),
            "ctp",
            20261027,
            "cu2512",
            &[&ctp_row(20261027, night, 5)],
        );
        let repo = KlineRepo::new(dir.path().to_path_buf());

        let got = repo
            .read_range("ctp", "cu2512", night, night + 60_000)
            .unwrap();
        assert_eq!(got.bars.len(), 1);
        assert_eq!(got.bars[0].start_time, night);
        assert_eq!(got.bars[0].trading_day, 20261027);
    }

    /// binance 一行（14 列：11 公共 + trade_num/active_buy_volume/active_buy_quote_volume）。
    fn binance_row(day: u32, start: i64, vol: i64) -> String {
        format!(
            "binance,BTCUSDT,{day},0,0,0,108,{vol},432,{start},{},1280,2.5,270",
            start + 60_000
        )
    }
}
