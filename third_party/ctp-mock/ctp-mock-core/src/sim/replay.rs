//! 历史 tick 文件回放（纯内核）：配置发现/解析、数据目录索引、15 列流式行解析、节奏换算。
//!
//! 数据源是仓库外的历史采集（如 `~/ctp_data/Tick/YYYYMM/YYYYMMDD/{合约}.csv`），
//! 与服务自身落盘的 33 列 RawTick **不同**：本表 15 列、带 UTF-8 BOM，盘口不是五档
//! 平铺而是一列 Python 字面量 `quotes`（恒 1 档）：
//!
//! ```text
//! exchange,symbol,open,high,low,price,cum_volume,cum_amount,last_volume,last_amount,\
//! trade_type,cum_position,quotes,created_at,type
//! SHFE,CU2706,...,108650.0,...,"[{'bid_p': 108000.0, 'bid_v': 3, 'ask_p': 109000.0, 'ask_v': 3}]",\
//! 2026-10-08 08:59:00.500000+08:00,14
//! ```
//!
//! 本模块只做「文件 → 领域行 [`HistTick`]」的转换（open 仅读文件、逐行流式，不涉线程/SPI），
//! 帧映射在 [`super::market`]，线程/推送在 [`super::md_front`]，便于 L1 全量单测。

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// 数据种类目录名（本次只接 Tick；1m 是 13 列 K线，不经 MdSpi 回放）。
pub const KIND_TICK: &str = "Tick";

/// 历史 tick 一行解析后的领域形态（盘口至多 1 档；某侧无源价时为 `None`）。
#[derive(Debug, Clone, PartialEq)]
pub struct HistTick {
    /// 真实交易所代码（SHFE/CZCE/DCE…，源 `exchange` 列）。
    pub exchange: String,
    /// 合约代码（源 `symbol` 列）。
    pub symbol: String,
    /// 今开（源 `open`）。
    pub open: f64,
    /// 最高（源 `high`）。
    pub high: f64,
    /// 最低（源 `low`）。
    pub low: f64,
    /// 最新价（源 `price`）。
    pub last: f64,
    /// 当日累计成交量（源 `cum_volume`，帧转换时饱和转 i32）。
    pub cum_volume: f64,
    /// 当日累计成交金额（源 `cum_amount` → Turnover）。
    pub cum_amount: f64,
    /// 当前市场总持仓（源 `cum_position` → OpenInterest）。
    pub cum_position: f64,
    /// 买一 (价, 量)；`quotes` 为空或买价 ≤ 0 时为 `None`。
    pub bid: Option<(f64, i32)>,
    /// 卖一 (价, 量)；缺侧同理。
    pub ask: Option<(f64, i32)>,
    /// `created_at` 解析出的北京墙钟毫秒（仅用于回放节奏，不写进帧）。
    pub ts_ms: i64,
}

/// [tick_replay] 配置（来自 ctp-mock 目录下的 ctp-mock.toml）。
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TickReplayConfig {
    /// 总开关；缺省关，保持既有 auto/脚本行为不变。
    #[serde(default)]
    pub enabled: bool,
    /// 数据根目录（支持 `~/`；相对路径相对配置文件目录）。
    #[serde(default = "default_data_root")]
    pub data_root: String,
    /// 数据种类，目前仅 "Tick"。
    #[serde(default = "default_kind")]
    pub kind: String,
    /// 指定交易日目录 YYYYMMDD；空＝自动选最新交易日目录。
    #[serde(default)]
    pub date: String,
    /// 按时间戳差回放的倍速（>0）。
    #[serde(default = "default_speed")]
    pub speed: f64,
    /// >0 时忽略时间戳，按定速毫秒推送；0＝按时间戳差。
    #[serde(default)]
    pub interval_ms: u64,
    /// 相邻 tick 等待上限，防历史稀疏段长时间静默。
    #[serde(default = "default_max_gap")]
    pub max_gap_ms: u64,
    /// 播完是否从头循环（历史文件默认需循环以持续模拟实时前置）。
    #[serde(default = "default_loop")]
    pub r#loop: bool,
    /// 合约白名单；空＝对所有「已订阅且索引到文件」的合约回放。
    #[serde(default)]
    pub instruments: Vec<String>,
}

fn default_data_root() -> String {
    "~/ctp_data".into()
}
fn default_kind() -> String {
    KIND_TICK.into()
}
fn default_speed() -> f64 {
    1.0
}
fn default_max_gap() -> u64 {
    // 5 分钟：如实复现文件里分钟级（含小节/午休）的自然 tick 间隔。
    // CTP 无行情回补接口，长间隔不应被压平成 30s 的均匀流。
    300_000
}
fn default_loop() -> bool {
    true
}

impl Default for TickReplayConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            data_root: default_data_root(),
            kind: default_kind(),
            date: String::new(),
            speed: default_speed(),
            interval_ms: 0,
            max_gap_ms: default_max_gap(),
            r#loop: default_loop(),
            instruments: Vec::new(),
        }
    }
}

/// TOML 文件顶层（仅取 [tick_replay]，其余表忽略）。
#[derive(Debug, Default, Deserialize)]
struct ConfigFile {
    #[serde(default)]
    tick_replay: Option<TickReplayConfig>,
}

impl TickReplayConfig {
    /// 解析 TOML 文本；无 [tick_replay] 表时返回缺省（enabled=false）。
    pub fn parse_str(text: &str) -> Result<Self, String> {
        let f: ConfigFile = toml::from_str(text).map_err(|e| format!("配置解析失败: {e}"))?;
        Ok(f.tick_replay.unwrap_or_default())
    }

    /// 从文件读取并解析。
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = fs::read_to_string(path)
            .map_err(|e| format!("读取配置 {} 失败: {e}", path.display()))?;
        Self::parse_str(&text)
    }

    /// 语义校验（kind/speed/date/root）。
    pub fn validate(&self) -> Result<(), String> {
        if self.kind != KIND_TICK {
            return Err(format!(
                "tick_replay.kind 仅支持 \"{KIND_TICK}\"（收到 {:?}；1m 不经行情回调回放）",
                self.kind
            ));
        }
        if self.speed <= 0.0 || !self.speed.is_finite() {
            return Err(format!("tick_replay.speed 必须为正有限数（收到 {}）", self.speed));
        }
        if self.enabled && self.data_root.trim().is_empty() {
            return Err("tick_replay.enabled=true 时 data_root 不能为空".into());
        }
        if !self.date.is_empty()
            && !(self.date.len() == 8 && self.date.bytes().all(|b| b.is_ascii_digit()))
        {
            return Err(format!(
                "tick_replay.date 必须是 8 位 YYYYMMDD（收到 {:?}）",
                self.date
            ));
        }
        Ok(())
    }

    /// 展开 `~/` 并把相对路径相对配置文件目录解析为绝对数据根。
    pub fn resolve_data_root(&self, config_dir: &Path) -> PathBuf {
        let raw = self.data_root.trim();
        if let Some(rest) = raw.strip_prefix("~/").or_else(|| (raw == "~").then_some("")) {
            if let Ok(home) = std::env::var("HOME") {
                return PathBuf::from(home).join(rest);
            }
        }
        let p = PathBuf::from(raw);
        if p.is_absolute() {
            p
        } else {
            config_dir.join(p)
        }
    }

    /// 某合约是否允许回放：白名单空＝全部，否则须命中。
    pub fn allows(&self, instrument: &str) -> bool {
        self.instruments.is_empty() || self.instruments.iter().any(|s| s == instrument)
    }
}

/// 某交易日目录的索引：合约 stem → CSV 路径。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DayIndex {
    /// 月目录 YYYYMM。
    pub month: String,
    /// 交易日目录 YYYYMMDD。
    pub day: String,
    /// 合约 → 文件。
    pub files: HashMap<String, PathBuf>,
}

/// 发现配置文件：从 `cwd` 逐级向上，先看 `./ctp-mock.toml` 再看
/// `./third_party/ctp-mock/ctp-mock.toml`；都没有则用编译期 manifest 兜底。
/// 仅返回**实际存在**的文件。
pub fn locate_config(cwd: &Path) -> Option<PathBuf> {
    let mut dir = Some(cwd);
    while let Some(d) = dir {
        let local = d.join("ctp-mock.toml");
        if local.is_file() {
            return Some(local);
        }
        let nested = d.join("third_party/ctp-mock/ctp-mock.toml");
        if nested.is_file() {
            return Some(nested);
        }
        dir = d.parent();
    }
    // ctp-mock-core 的 manifest 目录是 third_party/ctp-mock/ctp-mock-core；配置在其上一级。
    let fallback = Path::new(env!("CARGO_MANIFEST_DIR")).join("../ctp-mock.toml");
    fallback.is_file().then_some(fallback)
}

/// 索引 `root/{kind}/YYYYMM/YYYYMMDD/*.csv`；`date_override` 空时自动选最新交易日目录。
pub fn index_day(root: &Path, kind: &str, date_override: &str) -> std::io::Result<DayIndex> {
    let kind_root = root.join(kind);
    let (month, day) = if date_override.is_empty() {
        newest_month_day(&kind_root)?
    } else {
        (date_override[..6].to_string(), date_override.to_string())
    };
    let day_dir = kind_root.join(&month).join(&day);
    let mut files = HashMap::new();
    for entry in fs::read_dir(&day_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) == Some("csv") {
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                files.insert(stem.to_string(), path);
            }
        }
    }
    Ok(DayIndex { month, day, files })
}

/// 选最新月份目录与其下最新交易日目录（名称 YYYYMM/YYYYMMDD 字典序即时间序）。
fn newest_month_day(kind_root: &Path) -> std::io::Result<(String, String)> {
    let month = newest_entry_name(kind_root)?.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("数据种类目录为空或不存在: {}", kind_root.display()),
        )
    })?;
    let day = newest_entry_name(&kind_root.join(&month))?.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("月份目录下无交易日目录: {}", kind_root.join(&month).display()),
        )
    })?;
    Ok((month, day))
}

/// 列出目录下名字最大的子目录名（仅目录）。
fn newest_entry_name(dir: &Path) -> std::io::Result<Option<String>> {
    if !dir.is_dir() {
        return Ok(None);
    }
    let mut best: Option<String> = None;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        if let Some(name) = entry.file_name().to_str() {
            if best.as_deref().is_none_or(|b| name > b) {
                best = Some(name.to_string());
            }
        }
    }
    Ok(best)
}

/// 解析 `created_at`（带时区北京时间、小数秒位数可变）为 Unix 毫秒。
///
/// 形如 `2026-10-08 08:59:00.500000+08:00` 或 `2026-10-08 09:00:01+08:00`。
pub fn parse_created_at(s: &str) -> Option<i64> {
    const WITH_FRAC: &str = "%Y-%m-%d %H:%M:%S%.f%z";
    const NO_FRAC: &str = "%Y-%m-%d %H:%M:%S%z";
    for fmt in [WITH_FRAC, NO_FRAC] {
        if let Ok(fixed) = chrono::DateTime::parse_from_str(s, fmt) {
            return Some(fixed.timestamp_millis());
        }
    }
    None
}

/// 相邻 tick 的回放等待毫秒：首帧立即；负差钳 0；按倍速缩放；超过 `max_gap_ms` 截断。
pub fn next_delay(prev_ts: Option<i64>, ts: i64, speed: f64, max_gap_ms: u64) -> u64 {
    let Some(prev) = prev_ts else { return 0 };
    let speed = if speed > 0.0 && speed.is_finite() { speed } else { 1.0 };
    let delta = (ts - prev).max(0) as f64 / speed;
    if !delta.is_finite() || delta <= 0.0 {
        return 0;
    }
    (delta as u64).min(max_gap_ms)
}

/// 历史 tick 流式读取器：跳 BOM、按表头列名定位、逐行解析；坏行跳过并计数。
pub struct TickFileReader {
    /// 列名 → 列位（表头）。
    columns: HashMap<String, usize>,
    /// `quotes` 列的起始列位（quotes 内嵌逗号，尾两列 created_at/type 单独回取）。
    quotes_idx: usize,
    reader: BufReader<File>,
    line: String,
    /// 无法解析的数据行数（不含表头）。
    pub bad_lines: u64,
}

impl TickFileReader {
    /// 打开并读表头（剥离 UTF-8 BOM）；缺必需列即错。
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut header = String::new();
        if reader.read_line(&mut header)? == 0 {
            return Err(io_err("tick 文件为空（无表头）"));
        }
        let header = header.strip_prefix('\u{feff}').unwrap_or(&header);
        let columns: HashMap<String, usize> = header
            .trim_end()
            .split(',')
            .enumerate()
            .map(|(i, s)| (s.trim().to_string(), i))
            .collect();
        for required in [
            "exchange",
            "symbol",
            "open",
            "high",
            "low",
            "price",
            "cum_volume",
            "cum_amount",
            "cum_position",
            "quotes",
            "created_at",
        ] {
            if !columns.contains_key(required) {
                return Err(io_err(&format!("tick 表头缺少必需列: {required}")));
            }
        }
        let quotes_idx = columns["quotes"];
        Ok(Self {
            columns,
            quotes_idx,
            reader,
            line: String::new(),
            bad_lines: 0,
        })
    }

    /// 读取并解析下一数据行；EOF 返回 None；坏行跳过并累加 `bad_lines` 后继续。
    pub fn next_tick(&mut self) -> std::io::Result<Option<HistTick>> {
        loop {
            self.line.clear();
            if self.reader.read_line(&mut self.line)? == 0 {
                return Ok(None);
            }
            let raw = self.line.trim_end_matches(['\n', '\r']);
            if raw.is_empty() {
                continue;
            }
            match parse_row(raw, &self.columns, self.quotes_idx) {
                Some(t) => return Ok(Some(t)),
                None => self.bad_lines += 1,
            }
        }
    }
}

fn io_err(msg: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg.to_string())
}

/// 解析单行。
///
/// 标量列位于 `quotes` 之前（无内嵌逗号）可按列位直取；`quotes` 是唯一含逗号的列，
/// 故取「quotes 起始列 … 倒数第 3 列」重新拼回，尾两列恒为 created_at/type（不含逗号）。
fn parse_row(line: &str, columns: &HashMap<String, usize>, quotes_idx: usize) -> Option<HistTick> {
    let parts: Vec<&str> = line.split(',').collect();
    // 名义 15 列；quotes 内嵌逗号会让分片更多，少于名义列数必为坏行。
    if parts.len() < columns.len() {
        return None;
    }
    let scalar = |name: &str| -> Option<&str> {
        let idx = *columns.get(name)?;
        parts.get(idx).copied()
    };
    let num = |name: &str| -> Option<f64> { scalar(name)?.trim().parse().ok() };

    let exchange = scalar("exchange").unwrap_or("").trim().to_string();
    let symbol = scalar("symbol")?.trim().to_string();
    let open = num("open")?;
    let high = num("high")?;
    let low = num("low")?;
    let last = num("price")?;
    let cum_volume = num("cum_volume")?;
    let cum_amount = num("cum_amount")?;
    let cum_position = num("cum_position")?;

    // 尾两列恒为 created_at,type（均不含逗号）。
    let created_at = parts.get(parts.len() - 2)?.trim();
    let ts_ms = parse_created_at(created_at)?;

    // quotes blob = quotes 起始列到尾两列之前，按逗号重新拼回并去掉外层包裹双引号。
    let blob = parts[quotes_idx..parts.len().saturating_sub(2)]
        .join(",")
        .trim()
        .trim_matches('"')
        .to_string();
    let one = scan_one_quote(&blob);
    let bid = one.and_then(|q| OneQuote::side(q.bid_p, q.bid_v));
    let ask = one.and_then(|q| OneQuote::side(q.ask_p, q.ask_v));

    Some(HistTick {
        exchange,
        symbol,
        open,
        high,
        low,
        last,
        cum_volume,
        cum_amount,
        cum_position,
        bid,
        ask,
        ts_ms,
    })
}

/// 一个档位的四键值（容错扫描结果）。
#[derive(Debug, Default, Clone, Copy, PartialEq)]
struct OneQuote {
    bid_p: Option<f64>,
    bid_v: Option<f64>,
    ask_p: Option<f64>,
    ask_v: Option<f64>,
}
impl OneQuote {
    /// 把一侧的 (价, 量) 原始值归一为帧口径：价缺失/非正/非有限即视为无此侧。
    fn side(price: Option<f64>, volume: Option<f64>) -> Option<(f64, i32)> {
        let price = price?;
        if price <= 0.0 || !price.is_finite() {
            return None;
        }
        Some((price, (volume.unwrap_or(0.0) as i32).max(0)))
    }
}

/// 从 Python 字面量列表里取**首个**档位的四个键值。空列表/无任何价键 → None。
///
/// 不引 Python/regex 依赖：源格式固定为 `{'bid_p': .., 'bid_v': .., 'ask_p': .., 'ask_v': ..}`，
/// 按键名定位、读其后的数字 token 即可。
fn scan_one_quote(blob: &str) -> Option<OneQuote> {
    let trimmed = blob.trim();
    if trimmed.is_empty() || trimmed == "[]" {
        return None;
    }
    let bid_p = find_num_after_key(trimmed, "bid_p");
    let ask_p = find_num_after_key(trimmed, "ask_p");
    if bid_p.is_none() && ask_p.is_none() {
        return None;
    }
    Some(OneQuote {
        bid_p,
        bid_v: find_num_after_key(trimmed, "bid_v"),
        ask_p,
        ask_v: find_num_after_key(trimmed, "ask_v"),
    })
}

/// 在 `s` 中定位 `'key'`，跳过到冒号后的空白/引号，读取一个数字 token。
fn find_num_after_key(s: &str, key: &str) -> Option<f64> {
    let start = s.find(key)?;
    let after_key = &s[start + key.len()..];
    let colon = after_key.find(':')?;
    // find 返回字节偏移，直接在字节切片上取（避免 str 按字节切片的告警与边界假设）。
    let bytes = &after_key.as_bytes()[colon + 1..];
    let mut i = 0;
    // 跳过冒号后空白与可能的引号。
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c.is_whitespace() || c == '\'' || c == '"' {
            i += 1;
        } else {
            break;
        }
    }
    let mut j = i;
    while j < bytes.len() {
        let c = bytes[j] as char;
        if c.is_ascii_digit() || matches!(c, '.' | '-' | '+' | 'e' | 'E') {
            j += 1;
        } else {
            break;
        }
    }
    if j == i {
        return None;
    }
    std::str::from_utf8(&bytes[i..j]).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    /// 独立临时目录（每测试唯一，Drop 清理）。
    struct TmpDir(PathBuf);
    fn tmp_dir() -> TmpDir {
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!(
            "ctpmock-replay-{}-{}-{n}",
            std::process::id(),
            std::file!().replace('/', "_")
        ));
        fs::create_dir_all(&p).unwrap();
        TmpDir(p)
    }
    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    const HEADER: &str = "\u{feff}exchange,symbol,open,high,low,price,cum_volume,cum_amount,last_volume,last_amount,trade_type,cum_position,quotes,created_at,type";

    #[test]
    fn config_parses_defaults_and_overrides() {
        let def = TickReplayConfig::parse_str("").unwrap();
        assert!(!def.enabled);
        assert_eq!(def.data_root, "~/ctp_data");
        assert_eq!(def.kind, "Tick");
        assert_eq!(def.speed, 1.0);
        assert_eq!(def.max_gap_ms, 300_000);
        assert!(def.r#loop);
        assert!(def.instruments.is_empty());

        let text = "[tick_replay]\nenabled = true\ndate = \"20261008\"\nspeed = 4.0\ninterval_ms = 200\nloop = false\ninstruments = [\"CU2706\"]\n";
        let cfg = TickReplayConfig::parse_str(text).unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.date, "20261008");
        assert_eq!(cfg.speed, 4.0);
        assert_eq!(cfg.interval_ms, 200);
        assert!(!cfg.r#loop);
        assert_eq!(cfg.instruments, vec!["CU2706".to_string()]);
        assert!(cfg.allows("CU2706"));
        assert!(!cfg.allows("RB9999"));
        assert!(TickReplayConfig::default().allows("ANY"));
    }

    #[test]
    fn config_validates_kind_speed_date_root() {
        let mut c = TickReplayConfig {
            enabled: true,
            ..Default::default()
        };
        assert!(c.validate().is_ok());
        c.kind = "1m".into();
        assert!(c.validate().is_err());
        c.kind = "Tick".into();
        c.speed = 0.0;
        assert!(c.validate().is_err());
        c.speed = 1.0;
        c.date = "202610".into();
        assert!(c.validate().is_err());
        c.date = "20261008".into();
        assert!(c.validate().is_ok());
        c.data_root = "  ".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn resolve_data_root_expands_tilde_and_relative() {
        let home = std::env::var("HOME").unwrap();
        let c = TickReplayConfig::default();
        assert_eq!(
            c.resolve_data_root(Path::new("/cfg")),
            PathBuf::from(&home).join("ctp_data")
        );
        let rel = TickReplayConfig {
            data_root: "data/tick".into(),
            ..Default::default()
        };
        assert_eq!(
            rel.resolve_data_root(Path::new("/cfg")),
            PathBuf::from("/cfg/data/tick")
        );
        let abs = TickReplayConfig {
            data_root: "/data/tick".into(),
            ..Default::default()
        };
        assert_eq!(
            abs.resolve_data_root(Path::new("/cfg")),
            PathBuf::from("/data/tick")
        );
    }

    #[test]
    fn locate_config_finds_local_and_nested_via_ancestors() {
        let t = tmp_dir();
        // 情形 1：cwd 即含本地 ctp-mock.toml。
        let local_dir = t.0.join("proj/ctp-mock");
        fs::create_dir_all(&local_dir).unwrap();
        fs::write(local_dir.join("ctp-mock.toml"), "[tick_replay]\n").unwrap();
        assert_eq!(
            locate_config(&local_dir).unwrap(),
            local_dir.join("ctp-mock.toml")
        );

        // 情形 2：仅仓库根嵌套路径存在，从深层 cwd 向上命中。
        let t2 = tmp_dir();
        let nested = t2.0.join("repo/third_party/ctp-mock");
        fs::create_dir_all(&nested).unwrap();
        fs::write(nested.join("ctp-mock.toml"), "").unwrap();
        let deep = t2.0.join("repo/crates/svc/src");
        fs::create_dir_all(&deep).unwrap();
        assert_eq!(
            locate_config(&deep).unwrap(),
            t2.0.join("repo/third_party/ctp-mock/ctp-mock.toml")
        );
    }

    fn seed_day(root: &Path, day: &str, files: &[(&str, &str)]) {
        let dir = root.join("Tick").join(&day[..6]).join(day);
        fs::create_dir_all(&dir).unwrap();
        for (stem, content) in files {
            fs::write(dir.join(format!("{stem}.csv")), content).unwrap();
        }
    }

    #[test]
    fn index_picks_newest_day_and_maps_stems() {
        let t = tmp_dir();
        seed_day(&t.0, "20261008", &[("CU2706", "x"), ("RB9999", "x")]);
        seed_day(&t.0, "20261009", &[("CU2706", "x")]);
        seed_day(&t.0, "20261101", &[("NI2704", "x")]);
        let idx = index_day(&t.0, "Tick", "").unwrap();
        assert_eq!(idx.month, "202611");
        assert_eq!(idx.day, "20261101");
        assert!(idx.files.contains_key("NI2704"));

        let idx8 = index_day(&t.0, "Tick", "20261008").unwrap();
        assert_eq!(idx8.day, "20261008");
        assert_eq!(idx8.files.len(), 2);
        assert!(idx8.files["CU2706"].ends_with("Tick/202610/20261008/CU2706.csv"));
    }

    #[test]
    fn index_missing_dir_errors() {
        let t = tmp_dir();
        assert!(index_day(&t.0, "Tick", "").is_err());
        assert!(index_day(&t.0, "Tick", "20261008").is_err());
    }

    fn sample_csv() -> String {
        // 含 BOM、quotes 内嵌逗号、两种时间格式、空盘口。
        format!(
            "{HEADER}\n\
SHFE,CU2706,0.0,0.0,0.0,108650.0,0,0.0,0,0.0,7,9007,\"[{{'bid_p': 108000.0, 'bid_v': 3, 'ask_p': 109000.0, 'ask_v': 3}}]\",2026-10-08 08:59:00.500000+08:00,14\n\
SHFE,CU2706,108650.0,108700.0,108600.0,108660.0,12,900000.0,2,180000.0,8,9010,\"[{{'bid_p': 108650.0, 'bid_v': 7, 'ask_p': 109620.0, 'ask_v': 1}}]\",2026-10-08 09:00:01+08:00,14\n\
CZCE,WH2705,0.0,0.0,0.0,3198.0,0,0.0,0,0.0,7,0,\"[]\",2026-10-08 15:00:00+08:00,14\n"
        )
    }

    #[test]
    fn reader_parses_rows_quotes_and_two_time_formats() {
        let t = tmp_dir();
        let path = t.0.join("CU2706.csv");
        fs::write(&path, sample_csv()).unwrap();
        let mut r = TickFileReader::open(&path).unwrap();
        let a = r.next_tick().unwrap().unwrap();
        assert_eq!(a.exchange, "SHFE");
        assert_eq!(a.symbol, "CU2706");
        assert_eq!(a.last, 108650.0);
        assert_eq!(a.cum_position, 9007.0);
        assert_eq!(a.bid, Some((108000.0, 3)));
        assert_eq!(a.ask, Some((109000.0, 3)));
        assert!(a.ts_ms > 0);

        let b = r.next_tick().unwrap().unwrap();
        assert_eq!(b.bid, Some((108650.0, 7)));
        assert_eq!(b.ask, Some((109620.0, 1)));
        assert_eq!(b.cum_amount, 900_000.0);
        // 两种时间格式相差恰为 60.5s（09:00:01 vs 08:59:00.500）。
        assert_eq!(b.ts_ms - a.ts_ms, 60_500);

        // 空盘口：两侧均 None。
        let c = r.next_tick().unwrap().unwrap();
        assert_eq!(c.symbol, "WH2705");
        assert_eq!(c.bid, None);
        assert_eq!(c.ask, None);
        assert!(r.next_tick().unwrap().is_none());
        assert_eq!(r.bad_lines, 0);
    }

    #[test]
    fn reader_zero_price_side_is_missing_and_bad_lines_counted() {
        let t = tmp_dir();
        let path = t.0.join("X.csv");
        let body = format!(
            "{HEADER}\n\
DCE,X,1.0,2.0,1.0,10.0,1,5.0,1,5.0,7,3,\"[{{'bid_p': 0.0, 'bid_v': 0, 'ask_p': 11.0, 'ask_v': 2}}]\",2026-10-08 09:00:00+08:00,14\n\
THIS IS NOT VALID\n\
DCE,X,1.0,2.0,1.0,10.0,1,5.0,1,5.0,7,3,\"[{{'bid_p': 9.0, 'bid_v': 1, 'ask_p': 11.0, 'ask_v': 2}}]\",2026-10-08 09:00:01+08:00,14\n"
        );
        fs::write(&path, body).unwrap();
        let mut r = TickFileReader::open(&path).unwrap();
        let a = r.next_tick().unwrap().unwrap();
        assert_eq!(a.bid, None); // 买价 0 → 缺侧
        assert_eq!(a.ask, Some((11.0, 2)));
        let c = r.next_tick().unwrap().unwrap();
        assert_eq!(c.bid, Some((9.0, 1)));
        assert_eq!(r.bad_lines, 1);
    }

    #[test]
    fn reader_rejects_header_missing_required_column() {
        let t = tmp_dir();
        let path = t.0.join("bad.csv");
        fs::write(&path, "exchange,symbol\nSHFE,X\n").unwrap();
        assert!(TickFileReader::open(&path).is_err());
    }

    #[test]
    fn created_at_accepts_fractional_and_integer_seconds() {
        let a = parse_created_at("2026-10-08 08:59:00.500000+08:00").unwrap();
        let b = parse_created_at("2026-10-08 08:59:00+08:00").unwrap();
        assert_eq!(a - b, 500);
        assert!(parse_created_at("not a time").is_none());
    }

    #[test]
    fn next_delay_rules() {
        assert_eq!(next_delay(None, 1_000, 1.0, 30_000), 0);
        assert_eq!(next_delay(Some(1_000), 2_500, 1.0, 30_000), 1_500);
        assert_eq!(next_delay(Some(2_500), 1_000, 1.0, 30_000), 0); // 负差钳 0
        assert_eq!(next_delay(Some(0), 10_000, 10.0, 30_000), 1_000); // 10x 倍速
        assert_eq!(next_delay(Some(0), 100_000, 1.0, 30_000), 30_000); // 上限截断
    }

    /// 真实采集文件探针（默认忽略）：`CTPMOCK_REAL_TICK=/path/to.csv cargo test ... -- --ignored`。
    /// 仅在显式提供本机文件时运行，断言全文件可解析（坏行极少）、首行字段合理。
    #[test]
    #[ignore = "需要本机真实采集文件，经 CTPMOCK_REAL_TICK 指定"]
    fn parses_real_captured_tick_file() {
        let path = PathBuf::from(std::env::var("CTPMOCK_REAL_TICK").expect("CTPMOCK_REAL_TICK"));
        let mut r = TickFileReader::open(&path).unwrap();
        let mut n = 0u64;
        let mut with_both = 0u64;
        while let Some(t) = r.next_tick().unwrap() {
            assert!(t.last >= 0.0);
            if t.bid.is_some() && t.ask.is_some() {
                with_both += 1;
            }
            assert!(t.ts_ms > 0);
            n += 1;
        }
        assert!(n > 0, "文件应至少有一行");
        assert_eq!(r.bad_lines, 0, "真实文件不应有坏行");
        // 多数文件有双侧盘口；个别退化文件（如单行盘前 []）允许全缺侧。
        eprintln!("real tick rows={n} both_sides={with_both}");
    }
}
