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

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct CsvTarget {
    venue: String,
    interval: String,
    trading_day: u32,
    symbol: String,
}

/// 单批刷盘行数缺省上限：磁盘饱和时避免把数十万行一次性编码/写盘，
/// 把单批 IO 切成多个小拍（余量留在表内/窗口等下一拍）。
pub const DEFAULT_FLUSH_MAX_ROWS: usize = 20_000;

/// 每拍写盘时间预算缺省值：超过预算的分组留到下一拍，避免单次 flush 长时间占住
/// 刷盘线程（故障期 426 文件/批的 open/close 风暴）。
const DEFAULT_FLUSH_WRITE_BUDGET: Duration = Duration::from_millis(200);

/// 周期拍 bar 分片数：按文件分组轮转，每拍只写 1/N 组，其余保持 dirty。
pub const FLUSH_SHARD_COUNT: usize = 4;

/// 句柄空闲 TTL：最近该时长内写过就保留句柄，超时关闭。
/// 取 10 分钟而非按拍数：`--kline-flush-interval` 可配（默认 5s），TTL 必须显著大于
/// 刷盘周期，否则每次拍初 sweep 都会把上一拍的句柄全部关掉、缓存退化为无缓存。
/// 保留期只有 fd（数千个 ≈ 数 MB 内核对象），休市期延时关闭无副作用。
const HANDLE_IDLE_TTL: Duration = Duration::from_secs(600);

/// fd 安全阀：活跃句柄数超过该值才按最久未用淘汰（远低于 `ulimit -n` 524288）。
/// 正常规模（数千合约）下不会触发，只防极端配置把 fd 耗尽。
const HANDLE_MAX_OPEN: usize = 16_384;

/// 一次 flush 的调度选项（单拍行数上限 / 写盘时间预算 / 分片数）。
///
/// 缺省 `shard_count = 1`：一次 flush 即全量写 dirty（停机收尾/测试/历史预热口径）。
/// 周期刷盘由服务传 `FLUSH_SHARD_COUNT` 做分片轮转。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushOptions {
    /// 单拍最多处理的行数（`0` 按 1 处理，保证进度）。
    pub max_rows: usize,
    /// 单拍写盘时间预算；耗尽后其余分组留到下一拍（至少写一个分组）。
    pub write_budget: Duration,
    /// 分片数：>1 时按目标文件稳定哈希分片，每拍只写一组（未写组保持 dirty，仅 bar）。
    pub shard_count: usize,
}

impl Default for FlushOptions {
    fn default() -> Self {
        Self {
            max_rows: DEFAULT_FLUSH_MAX_ROWS,
            write_budget: DEFAULT_FLUSH_WRITE_BUDGET,
            shard_count: 1,
        }
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

/// 编码+追加的结果：已写分组（供 bar 按 `(start,gen)` 精确确认）、未写行
/// （供 tick WAL requeue）与首个 IO 错误。
struct AppendOutcome<T> {
    report: FlushReport,
    written: Vec<(CsvTarget, Vec<T>)>,
    unwritten: Vec<T>,
    error: Option<std::io::Error>,
}

/// 一个缓存的 CSV 写入句柄（append 模式）与最近使用时刻（空闲淘汰依据）。
#[derive(Debug)]
struct CachedHandle {
    file: File,
    last_used: Instant,
}

/// CSV 写入句柄缓存：按目标文件（venue/周期/交易日/合约）复用 append 句柄，
/// 消除「每批每文件 open/close」的 IO 风暴（故障期单批数百文件）。
///
/// 保留策略是**活跃集 + 空闲淘汰**（而非固定条数 LRU）：
/// * 最近 [`HANDLE_IDLE_TTL`] 内写过 → 保留（任意订阅数 N 都不会互相挤掉）；
/// * 超过 TTL 未写 → 关闭（drop 即释放 fd）；
/// * 活跃数超过 [`HANDLE_MAX_OPEN`] → 按最久未用淘汰到安全阀（仅防 fd 耗尽）。
///
/// 其他清理时机：
/// * 写失败 → 立即失效该句柄（下次重开，避免复用一个已坏句柄）；
/// * 换日写同一合约 → 清理旧交易日句柄；
/// * 停机由 [`KlineRepo::close_handles`] 统一关闭。
///
/// 已知限制：外部对已缓存文件的截断/轮转（如 logrotate）不会被感知，追加会落到旧
/// inode；文件按「交易日」轮换、且进程重启/换日/淘汰都会重开，故不额外做每次 stat。
#[derive(Debug)]
struct HandleCache {
    entries: HashMap<CsvTarget, CachedHandle>,
    /// fd 安全阀（活跃句柄数超过它才按最久未用淘汰）。
    max_open: usize,
    /// 空闲 TTL（生产用 [`HANDLE_IDLE_TTL`]；测试可注入缩短的值）。
    idle_ttl: Duration,
}

impl HandleCache {
    fn new(max_open: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_open: max_open.max(1),
            idle_ttl: HANDLE_IDLE_TTL,
        }
    }

    /// 向目标文件追加一行（或一批）字节；新文件/0 字节文件先写表头。
    /// `now` 用于记录最近使用时刻（测试可注入，生产取 `Instant::now()`）。
    fn append(
        &mut self,
        target: &CsvTarget,
        path: &Path,
        header: &str,
        bytes: &[u8],
        now: Instant,
    ) -> std::io::Result<()> {
        // 已有句柄：单次查找完成「刷新活跃时刻 + 追加」；写失败即失效（下轮重开重试）。
        let reused = self.entries.get_mut(target).map(|entry| {
            entry.last_used = now;
            entry.file.write_all(bytes).and_then(|_| entry.file.flush())
        });
        if let Some(result) = reused {
            if result.is_err() {
                self.entries.remove(target);
            }
            return result;
        }

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        // 以打开后的实际大小判定表头（0 字节残留也补表头）。
        if file.metadata()?.len() == 0 {
            writeln!(file, "{header}")?;
        }
        file.write_all(bytes)?;
        file.flush()?;

        // 换日清理：同 venue/周期/合约的旧交易日句柄不再需要。
        self.entries.retain(|k, _| {
            !(k.venue == target.venue
                && k.interval == target.interval
                && k.symbol == target.symbol
                && k.trading_day < target.trading_day)
        });
        self.entries.insert(
            target.clone(),
            CachedHandle {
                file,
                last_used: now,
            },
        );
        Ok(())
    }

    /// 每拍开始时调用：先按空闲 TTL 关闭句柄，再把活跃数压到安全阀以内
    /// （本拍新打开/刷新的句柄不会被误杀）。返回本次关闭的句柄数（0 = 无淘汰）。
    fn sweep(&mut self, now: Instant) -> usize {
        let before = self.entries.len();
        self.entries
            .retain(|_, entry| now.saturating_duration_since(entry.last_used) < self.idle_ttl);
        while self.entries.len() > self.max_open {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone());
            match oldest {
                Some(key) => {
                    self.entries.remove(&key);
                }
                None => break,
            }
        }
        before - self.entries.len()
    }

    /// 关闭并清空全部缓存句柄（停机用）。
    fn clear(&mut self) {
        self.entries.clear();
    }
}

/// K线 CSV 仓储：持有数据根目录、分片游标与句柄缓存，廉价克隆（`Arc` 共享状态）。
#[derive(Debug, Clone)]
pub struct KlineRepo {
    root: PathBuf,
    /// 周期拍 bar 分片轮转游标（多进程/多实例下仅本进程内有效）。
    shard: Arc<AtomicUsize>,
    /// CSV 写入句柄 LRU（进程单例：store 只构造一次，Clone 共享）。
    handles: Arc<Mutex<HandleCache>>,
}

impl KlineRepo {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            shard: Arc::new(AtomicUsize::new(0)),
            handles: Arc::new(Mutex::new(HandleCache::new(HANDLE_MAX_OPEN))),
        }
    }

    /// 关闭并清空全部缓存句柄（优雅停机收尾调用；进程退出亦会由 OS 关闭）。
    pub fn close_handles(&self) {
        if let Ok(mut cache) = self.handles.lock() {
            cache.clear();
        }
    }

    /// 每拍开始：按空闲 TTL / 安全阀关闭不再需要的句柄（仅在确有淘汰时打 debug）。
    /// 锁中毒（他线程持锁 panic）不阻断刷盘，仅告警并跳过本次淘汰。
    fn sweep_handles(&self, now: Instant) {
        match self.handles.lock() {
            Ok(mut cache) => {
                let evicted = cache.sweep(now);
                if evicted > 0 {
                    tracing::debug!(
                        evicted,
                        open_handles = cache.entries.len(),
                        "K线 CSV 句柄空闲淘汰"
                    );
                }
            }
            Err(_) => tracing::warn!("K线 CSV 句柄缓存锁中毒，跳过本次空闲淘汰"),
        }
    }

    /// 批量落盘定稿 bar（缺省选项：一次全量、单批 20k 行）。
    pub fn flush_bars(&self, table: &BarTable) -> std::io::Result<FlushReport> {
        self.flush_bars_with(table, FlushOptions::default())
    }

    /// 批量落盘定稿 bar（**非破坏式**）：快照 dirty（bar 本体保留在表内可读），按
    /// venue/周期/交易日/合约分组追加，**写成功的分组**才按 `(start,gen)` 确认清 dirty。
    ///
    /// 表头按 **venue 动态**生成（公共列 + 该所 extras 列）；同一目标文件的 venue 恒定，
    /// 故分组内表头一致。
    ///
    /// 失败语义（at-least-once）：任一写入/编码失败则**不确认**未写分组，留下轮重写——
    /// 已追加文件可能重复，读回按 start 去重兜底。快照后同 start 又被 upsert（gen 推进）
    /// 的键不会被误清（[`BarTable::mark_flushed`] 校验 gen）。
    /// 无 dirty 时空批返回、不创建任何目录/文件。
    ///
    /// `opts` 控制单拍行数上限与写盘预算：超限/超预算的分组保持 dirty 等下一拍；
    /// `shard_count > 1` 时按目标文件稳定哈希分片轮转，每拍只写其中一组分片。
    pub fn flush_bars_with(
        &self,
        table: &BarTable,
        opts: FlushOptions,
    ) -> std::io::Result<FlushReport> {
        let now = Instant::now();
        // 每拍都先淘汰空闲句柄（含无数据拍），保证休市期句柄也能按 TTL 关闭。
        self.sweep_handles(now);
        let dirty = table.snapshot_dirty();
        if dirty.is_empty() {
            return Ok(FlushReport::default());
        }
        // 分组键＝目标文件身份；BTreeMap 保证落盘顺序确定（分片轮转也据此稳定）。
        let mut groups: BTreeMap<CsvTarget, Vec<DirtyBar>> = BTreeMap::new();
        for d in dirty {
            groups.entry(bar_target(&d.bar)).or_default().push(d);
        }

        // 分片轮转：每拍取一组分片（`shard_count=1` 即全量）。
        // 分片归属按目标文件身份的稳定哈希，而不是「快照内序号」——已写分组从快照消失
        // 不会改变其余分组的归属，游标轮转 1..N 拍即可覆盖全部文件。
        let shards = opts.shard_count.max(1);
        let current = self.shard.fetch_add(1, Ordering::Relaxed) % shards;
        let max_rows = opts.max_rows.max(1);
        let mut selected: BTreeMap<CsvTarget, Vec<DirtyBar>> = BTreeMap::new();
        let mut planned_rows = 0usize;
        for (target, batch) in groups {
            if shards > 1 && shard_index(&target, shards) != current {
                continue;
            }
            // 单批上限：写满后其余分组留下拍（至少选一组保证进度）。
            if planned_rows >= max_rows && !selected.is_empty() {
                continue;
            }
            planned_rows += batch.len();
            selected.insert(target, batch);
        }

        let outcome = self.append_groups(
            selected,
            opts.write_budget,
            now,
            |d| bar_csv_header(&d.bar.exchange),
            |d| encode_bar(&d.bar),
        );
        if let Some(err) = outcome.error {
            return Err(err);
        }
        // 只确认**写成功**的分组；快照后被覆盖（gen 推进）的键由 mark 内部按 gen 保留。
        let confirms: Vec<(SeriesKey, i64, u64)> = outcome
            .written
            .iter()
            .flat_map(|(_, batch)| batch.iter().map(|d| (d.key.clone(), d.start, d.gen)))
            .collect();
        table.mark_flushed(&confirms);
        Ok(outcome.report)
    }

    /// 批量落盘原始 tick（缺省选项）。
    pub fn flush_ticks(&self, table: &TickTable) -> std::io::Result<FlushReport> {
        self.flush_ticks_with(table, FlushOptions::default())
    }

    /// 批量落盘原始 tick：从 WAL 窗口取**最多 `max_rows` 行**，按 venue/tick/交易日/合约
    /// 分组追加；预算耗尽/失败时未写行整批放回窗口队首，下轮重试（不丢数据）。
    pub fn flush_ticks_with(
        &self,
        table: &TickTable,
        opts: FlushOptions,
    ) -> std::io::Result<FlushReport> {
        let now = Instant::now();
        // 每拍都先淘汰空闲句柄（含无数据拍），保证休市期句柄也能按 TTL 关闭。
        self.sweep_handles(now);
        let rows = table.drain_n(opts.max_rows.max(1));
        if rows.is_empty() {
            return Ok(FlushReport::default());
        }
        let mut groups: BTreeMap<CsvTarget, Vec<RawTick>> = BTreeMap::new();
        for row in rows {
            groups.entry(tick_target(&row)).or_default().push(row);
        }
        let outcome = self.append_groups(
            groups,
            opts.write_budget,
            now,
            |_| RAW_TICK_CSV_HEADER.to_string(),
            encode_tick,
        );
        // 未写行（预算截留/失败）必须放回窗口队首，下轮重试；已写文件保持 at-least-once。
        if !outcome.unwritten.is_empty() {
            table.requeue_front(outcome.unwritten);
        }
        if let Some(err) = outcome.error {
            return Err(err);
        }
        Ok(outcome.report)
    }

    /// 逐分组「内存编码 → 追加」：超过写盘预算或遇到 IO 错误时提前收手，
    /// 返回已写分组与未写行（调用方决定确认/requeue），至少写一个分组保证进度。
    fn append_groups<T>(
        &self,
        groups: BTreeMap<CsvTarget, Vec<T>>,
        write_budget: Duration,
        now: Instant,
        header_of: impl Fn(&T) -> String,
        encode_row: impl Fn(&T) -> std::io::Result<Vec<u8>>,
    ) -> AppendOutcome<T> {
        let started = Instant::now();
        let mut iter = groups.into_iter();
        let mut written: Vec<(CsvTarget, Vec<T>)> = Vec::new();
        let mut files = 0usize;
        let mut rows = 0usize;
        while let Some((target, batch)) = iter.next() {
            // 预算耗尽：当前及后续分组整组留下拍（先写的已生效）。
            if files > 0 && started.elapsed() >= write_budget {
                return AppendOutcome {
                    report: FlushReport { files, rows },
                    written,
                    unwritten: collect_remaining(
                        std::iter::once(batch).chain(iter.map(|(_, b)| b)),
                    ),
                    error: None,
                };
            }
            // 同组 venue 恒定 → 表头取首行；分组即有行，batch 必非空。
            let header = header_of(&batch[0]);
            let result = encode_batch(&batch, &encode_row).and_then(|buf| {
                let path = self.csv_path(
                    &target.venue,
                    &target.interval,
                    target.trading_day,
                    &target.symbol,
                );
                // 句柄缓存：同一文件跨批复用 append 句柄；写失败内部失效后下轮重开。
                self.handles
                    .lock()
                    .map_err(|_| std::io::Error::other("K线句柄缓存锁中毒"))
                    .and_then(|mut cache| cache.append(&target, &path, &header, &buf, now))
            });
            if let Err(e) = result {
                // 当前分组（可能部分已写）与后续全部分组都留下轮重试（at-least-once）。
                return AppendOutcome {
                    report: FlushReport { files, rows },
                    written,
                    unwritten: collect_remaining(
                        std::iter::once(batch).chain(iter.map(|(_, b)| b)),
                    ),
                    error: Some(e),
                };
            }
            rows += batch.len();
            files += 1;
            written.push((target, batch));
        }
        AppendOutcome {
            report: FlushReport { files, rows },
            written,
            unwritten: Vec::new(),
            error: None,
        }
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
/// 分批编码：任一行失败即返回错误（此时尚未触碰任何文件）。
fn encode_batch<T>(
    batch: &[T],
    encode_row: &impl Fn(&T) -> std::io::Result<Vec<u8>>,
) -> std::io::Result<Vec<u8>> {
    batch
        .iter()
        .try_fold(Vec::with_capacity(batch.len() * 96), |mut buf, row| {
            buf.extend_from_slice(&encode_row(row)?);
            Ok(buf)
        })
}

/// 把当前分组与剩余分组按原顺序展平成未写行（下轮重试用）。
fn collect_remaining<T>(batches: impl Iterator<Item = Vec<T>>) -> Vec<T> {
    batches.flatten().collect()
}

/// 目标文件 → 分片号（稳定哈希：同一文件恒属同一分片，轮转游标覆盖全部）。
fn shard_index(target: &CsvTarget, shards: usize) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    target.hash(&mut hasher);
    (hasher.finish() as usize) % shards
}

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

    // ---- T4：CSV 文件句柄 LRU -------------------------------------------

    /// 同一文件多拍追加复用句柄（不再每批 open/close）。
    #[test]
    fn handle_cache_reuses_one_handle_per_file() {
        let dir = tempdir().unwrap();
        let repo = KlineRepo::new(dir.path().to_path_buf());
        let table = BarTable::new();
        table.upsert(bar("cu2512", 20261024, 60_000, 10));
        repo.flush_bars(&table).unwrap();
        table.upsert(bar("cu2512", 20261024, 120_000, 7));
        repo.flush_bars(&table).unwrap();

        assert_eq!(repo.handles.lock().unwrap().entries.len(), 1);
        let lines = read_lines(&dir.path().join("kline/ctp/1m/20261024/cu2512.csv"));
        assert_eq!(lines.len(), 3, "表头 + 两行，复用句柄不得重复表头");
    }

    /// 换日写同一合约：旧交易日句柄被清理，新日文件独立。
    #[test]
    fn handle_cache_evicts_previous_day_for_same_symbol() {
        let dir = tempdir().unwrap();
        let repo = KlineRepo::new(dir.path().to_path_buf());
        // 两天的 bar 放同一批：先写 day1 再写 day2（BTreeMap 按交易日升序）。
        let table = BarTable::new();
        table.upsert(bar("cu2512", 20261024, 60_000, 10));
        // 同一序列键下 start 必须互异（BarTable 按 start 存桶），故次日取 120_000。
        table.upsert(bar("cu2512", 20261025, 120_000, 20));
        repo.flush_bars(&table).unwrap();

        // 同一合约只保留当日句柄（day1 被清理）。
        assert_eq!(repo.handles.lock().unwrap().entries.len(), 1);
        assert_eq!(
            read_lines(&dir.path().join("kline/ctp/1m/20261024/cu2512.csv")).len(),
            2
        );
        assert_eq!(
            read_lines(&dir.path().join("kline/ctp/1m/20261025/cu2512.csv")).len(),
            2
        );
    }

    // ---- T4'：活跃集 + 空闲淘汰 ------------------------------------------

    fn csv_target(symbol: &str, day: u32) -> CsvTarget {
        CsvTarget {
            venue: "ctp".into(),
            interval: M1_DIR.into(),
            trading_day: day,
            symbol: symbol.into(),
        }
    }

    /// 空闲未到 TTL 保留，达到 TTL 才关闭（不再按条数误杀活跃文件）。
    #[test]
    fn idle_handle_is_evicted_only_after_ttl() {
        let dir = tempdir().unwrap();
        let mut cache = HandleCache::new(HANDLE_MAX_OPEN);
        let target = csv_target("cu2512", 20261024);
        let path = dir.path().join("cu2512.csv");
        let t0 = Instant::now();
        cache.append(&target, &path, "h", b"1\n", t0).unwrap();
        assert_eq!(cache.entries.len(), 1);

        // TTL 内：保留。
        assert_eq!(
            cache.sweep(t0 + HANDLE_IDLE_TTL - Duration::from_secs(1)),
            0
        );
        assert_eq!(cache.entries.len(), 1);
        // 到达 TTL：关闭句柄。
        assert_eq!(cache.sweep(t0 + HANDLE_IDLE_TTL), 1);
        assert!(cache.entries.is_empty());
    }

    /// 任意 N：TTL 内 600 个活跃文件全部持有句柄（旧固定 512 上限会抖动）。
    #[test]
    fn active_handles_beyond_old_fixed_cap_are_all_retained() {
        let dir = tempdir().unwrap();
        let repo = KlineRepo::new(dir.path().to_path_buf());
        let table = BarTable::new();
        for i in 0..600 {
            table.upsert(bar(&format!("s{i:04}"), 20261024, 60_000, 10));
        }
        repo.flush_bars(&table).unwrap();
        assert_eq!(
            repo.handles.lock().unwrap().entries.len(),
            600,
            "活跃集全部保留，不按固定条数淘汰"
        );
    }

    /// max_open 仅作 fd 安全阀：超过时按最久未用淘汰到上限。
    #[test]
    fn max_open_is_only_a_safety_valve() {
        let dir = tempdir().unwrap();
        let mut cache = HandleCache::new(2);
        let now = Instant::now();
        for i in 0..3 {
            let target = csv_target(&format!("s{i}"), 20261024);
            let path = dir.path().join(format!("s{i}.csv"));
            cache.append(&target, &path, "h", b"1\n", now).unwrap();
        }
        assert_eq!(cache.sweep(now), 1);
        assert_eq!(cache.entries.len(), 2);
    }

    /// 无数据拍也要 sweep：休市后没有任何 bar/tick 时，空闲句柄仍按 TTL 关闭。
    #[test]
    fn empty_flush_still_sweeps_idle_handles() {
        let dir = tempdir().unwrap();
        let repo = KlineRepo::new(dir.path().to_path_buf());
        let table = BarTable::new();
        table.upsert(bar("cu2512", 20261024, 60_000, 10));
        repo.flush_bars(&table).unwrap();
        assert_eq!(repo.handles.lock().unwrap().entries.len(), 1);

        // 缩短 TTL 后跑一次空拍（无 dirty）：句柄应被关闭。
        repo.handles.lock().unwrap().idle_ttl = Duration::ZERO;
        repo.flush_bars(&BarTable::new()).unwrap();
        assert!(repo.handles.lock().unwrap().entries.is_empty());
    }

    /// 停机清理：全部句柄关闭（fd 释放），随后写同一文件仍正确追加。
    #[test]
    fn handle_cache_is_fully_closable() {
        let dir = tempdir().unwrap();
        let repo = KlineRepo::new(dir.path().to_path_buf());
        let table = BarTable::new();
        for s in ["a2512", "b2512", "c2512"] {
            table.upsert(bar(s, 20261024, 60_000, 10));
        }
        repo.flush_bars(&table).unwrap();

        repo.close_handles();
        assert!(repo.handles.lock().unwrap().entries.is_empty());

        table.upsert(bar("a2512", 20261024, 120_000, 11));
        repo.flush_bars(&table).unwrap();
        assert_eq!(
            read_lines(&dir.path().join("kline/ctp/1m/20261024/a2512.csv")).len(),
            3
        ); // 表头 + 两行，无重复表头
    }

    // ---- T2：单批行数上限 / 每拍写盘预算 --------------------------------

    /// 单拍取 n 行、余量留窗，下一拍补齐且不重不丢。
    #[test]
    fn flush_ticks_caps_rows_per_batch_and_keeps_remainder() {
        let dir = tempdir().unwrap();
        let repo = KlineRepo::new(dir.path().to_path_buf());
        let table = TickTable::new();
        for i in 0..5 {
            table.append(tick("cu2512", 20261024, 1_760_000_000_000 + i, 100 + i));
        }

        let opts = FlushOptions {
            max_rows: 3,
            ..FlushOptions::default()
        };
        let first = repo.flush_ticks_with(&table, opts).unwrap();
        assert_eq!(first.rows, 3);
        assert_eq!(first.files, 1);
        assert_eq!(table.len(), 2, "超出单批上限的余量必须留在窗口");

        let second = repo.flush_ticks_with(&table, opts).unwrap();
        assert_eq!(second.rows, 2);
        assert!(table.is_empty());

        // CSV 恰为表头 + 5 行：两拍无丢无重。
        let lines = read_lines(&dir.path().join("kline/ctp/tick/20261024/cu2512.csv"));
        assert_eq!(lines.len(), 6);
    }

    /// 预算耗尽提前收手：未写分组整组放回窗口队首，后续拍补齐且不丢。
    #[test]
    fn flush_ticks_write_budget_requeues_unwritten_rows() {
        let dir = tempdir().unwrap();
        let repo = KlineRepo::new(dir.path().to_path_buf());
        let table = TickTable::new();
        // 两个目标文件：按 BTreeMap 顺序 cu2512 先写、rb2611 被预算截留。
        table.append(tick("cu2512", 20261024, 1_760_000_000_000, 1));
        table.append(tick("rb2611", 20261024, 1_760_000_000_001, 2));
        table.append(tick("cu2512", 20261024, 1_760_000_000_002, 3));

        let tight = FlushOptions {
            max_rows: 100,
            write_budget: std::time::Duration::ZERO,
            ..FlushOptions::default()
        };
        let first = repo.flush_ticks_with(&table, tight).unwrap();
        assert_eq!(first.files, 1, "预算=0 仍保证至少写一个分组（进度保证）");
        assert_eq!(first.rows, 2, "首组 cu2512 两行已写");
        assert_eq!(table.len(), 1, "未写分组 rb2611 必须放回窗口");

        let second = repo.flush_ticks_with(&table, tight).unwrap();
        assert_eq!(second.rows, 1);
        assert!(table.is_empty());

        assert_eq!(
            read_lines(&dir.path().join("kline/ctp/tick/20261024/cu2512.csv")).len(),
            3
        ); // 表头 + 2 行
        assert_eq!(
            read_lines(&dir.path().join("kline/ctp/tick/20261024/rb2611.csv")).len(),
            2
        ); // 表头 + 1 行
    }

    /// bar 单批上限：写满上限后其余分组保持 dirty，下一拍补齐。
    #[test]
    fn flush_bars_caps_rows_and_keeps_rest_dirty() {
        let dir = tempdir().unwrap();
        let repo = KlineRepo::new(dir.path().to_path_buf());
        let table = BarTable::new();
        table.upsert(bar("cu2512", 20261024, 60_000, 10));
        table.upsert(bar("rb2611", 20261024, 60_000, 20));
        assert_eq!(table.snapshot_dirty().len(), 2);

        let opts = FlushOptions {
            max_rows: 1,
            ..FlushOptions::default()
        };
        let first = repo.flush_bars_with(&table, opts).unwrap();
        assert_eq!(first.rows, 1);
        assert_eq!(table.snapshot_dirty().len(), 1, "未写分组必须保持 dirty");

        let second = repo.flush_bars_with(&table, opts).unwrap();
        assert_eq!(second.rows, 1);
        assert!(table.snapshot_dirty().is_empty());
    }

    /// 周期拍分片轮转：4 拍覆盖全部分组，无丢无重；未轮到的分组保持 dirty。
    #[test]
    fn flush_bars_rotates_shards_without_loss_or_duplication() {
        let dir = tempdir().unwrap();
        let repo = KlineRepo::new(dir.path().to_path_buf());
        let table = BarTable::new();
        let symbols = ["a2512", "b2512", "c2512", "d2512", "e2512"];
        for s in symbols {
            table.upsert(bar(s, 20261024, 60_000, 10));
        }

        let opts = FlushOptions {
            shard_count: FLUSH_SHARD_COUNT,
            ..FlushOptions::default()
        };
        // 首拍照游标=0 只写 0 号分片；其余分片的分组保持 dirty。
        // 期望文件数由稳定哈希算出（测试不依赖具体散列分布）。
        let target_of = |s: &str| CsvTarget {
            venue: "ctp".into(),
            interval: M1_DIR.into(),
            trading_day: 20261024,
            symbol: s.into(),
        };
        let expected_first = symbols
            .iter()
            .filter(|s| shard_index(&target_of(s), FLUSH_SHARD_COUNT) == 0)
            .count();
        let first = repo.flush_bars_with(&table, opts).unwrap();
        assert_eq!(first.files, expected_first, "首拍只写 0 号分片");
        if expected_first < symbols.len() {
            assert!(
                !table.snapshot_dirty().is_empty(),
                "未轮到分片必须保持 dirty"
            );
        }

        // 补齐剩余 3 拍：总行数恰为 5，dirty 清空。
        let mut total = first.rows;
        for _ in 0..(FLUSH_SHARD_COUNT - 1) {
            total += repo.flush_bars_with(&table, opts).unwrap().rows;
        }
        assert_eq!(total, symbols.len(), "分片轮转不得丢行");
        assert!(table.snapshot_dirty().is_empty());

        // 每个文件恰好表头 + 1 行：不得重复追加。
        for s in symbols {
            let path = dir.path().join(format!("kline/ctp/1m/20261024/{s}.csv"));
            assert_eq!(read_lines(&path).len(), 2, "{s} 行数异常（重复或缺失）");
        }
        // 无 dirty 时下一拍空转，不产生写入。
        assert_eq!(repo.flush_bars_with(&table, opts).unwrap().rows, 0);
    }

    /// bar 写盘预算耗尽：未写分组保持 dirty（非破坏式，下次重试）。
    #[test]
    fn flush_bars_write_budget_leaves_rest_dirty() {
        let dir = tempdir().unwrap();
        let repo = KlineRepo::new(dir.path().to_path_buf());
        let table = BarTable::new();
        table.upsert(bar("cu2512", 20261024, 60_000, 10));
        table.upsert(bar("rb2611", 20261024, 60_000, 20));

        let tight = FlushOptions {
            max_rows: 100,
            write_budget: std::time::Duration::ZERO,
            ..FlushOptions::default()
        };
        let first = repo.flush_bars_with(&table, tight).unwrap();
        assert_eq!(first.rows, 1);
        assert_eq!(table.snapshot_dirty().len(), 1);

        let second = repo.flush_bars_with(&table, tight).unwrap();
        assert_eq!(second.rows, 1);
        assert!(table.snapshot_dirty().is_empty());
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
