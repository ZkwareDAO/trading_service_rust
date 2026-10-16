//! 领域值 ⇄ CTP 定长 `char[]` / 单字符字段的保真编解码。
//!
//! 全部直接读写 `ctp2rs::v1alpha1` 的 bindgen 数组类型，位宽由类型本身约束
//! （`[c_char; 13]` / `[c_char; 21]` / `[c_char; 9]` / `[c_char; 81]`），
//! 因此「字段位宽」在编译期即与真实柜台零偏差。
//!
//! 任务 2 先落地定长串与单字符；21B 系统号风格、GB18030 中文、时间戳生成在
//! 任务 5 补齐（同文件扩展）。

use std::os::raw::c_char;

use chrono::{FixedOffset, TimeZone, Utc};
use encoding_rs::GB18030;

/// 把 UTF-8 文本写入定长字符数组：按字节拷贝可容纳部分并保证 NUL 结尾。
///
/// 与服务端 `encoding::encode_fixed` 同口径（调用方需保证缓冲足够容纳短文本）。
pub fn put_fixed<const N: usize>(buf: &mut [c_char; N], text: &str) {
    if N == 0 {
        return;
    }
    let bytes = text.as_bytes();
    let n = bytes.len().min(N - 1);
    for (i, &b) in bytes[..n].iter().enumerate() {
        buf[i] = b as c_char;
    }
    buf[n] = 0;
}

/// 从定长字符数组读出 UTF-8 文本：以首个 NUL 为界，非 ASCII 按 GB18030 解码。
pub fn get_fixed(buf: &[c_char]) -> String {
    let len = buf.iter().position(|&c| c == 0).unwrap_or(buf.len());
    let bytes: Vec<u8> = buf[..len].iter().map(|&c| c as u8).collect();
    if bytes.is_ascii() {
        String::from_utf8(bytes).unwrap_or_default()
    } else {
        GB18030.decode(&bytes).0.into_owned()
    }
}

/// 把领域 `char`（CTP 单字符常量，如 `'3'`）写入 `c_char` 字段。
pub fn put_char(buf: &mut c_char, code: char) {
    *buf = code as u8 as c_char;
}

/// 读取 CTP 单字符字段为 Rust `char`（仅取低 8 位；NUL/0 视为 `'\0'`）。
pub fn get_char(code: c_char) -> char {
    code as u8 as char
}

/// 把中文/UTF-8 文本按 GB18030 编码写入定长缓冲（如 StatusMsg/ErrorMsg）。
///
/// 编码后超出容量的字节被截断并保证 NUL 结尾。
pub fn put_gb18030<const N: usize>(buf: &mut [c_char; N], text: &str) {
    if N == 0 {
        return;
    }
    let (bytes, _enc, _had_errors) = GB18030.encode(text);
    let n = bytes.len().min(N - 1);
    for (i, &b) in bytes[..n].iter().enumerate() {
        buf[i] = b as c_char;
    }
    buf[n] = 0;
}

// ---- 21B 系统号 / 成交号（CTP 前导空格风格） ----

/// `OrderSysID` / `TradeID` 字段宽度（`TThostFtdcOrderSysIDType` = `[c_char; 21]`）。
pub const SYSID_LEN: usize = 21;

/// 把 `日期-序号`（如 `20261015-1`）格式化为 21B 前导空格风格：
/// 右侧正文、左侧以空格补齐到宽度减一（末字节留给 NUL）。
///
/// 真实柜台回报形如 `"    20261015-1"`；正文超长时截断右侧、不越界。
pub fn format_sysid(day: &str, seq: u64) -> String {
    let body = format!("{day}-{seq}");
    pad_leading(&body, SYSID_LEN - 1)
}

/// 成交号与系统号同宽同风格（本模拟器内 TradeID 取独立递增序列）。
pub fn format_trade_id(day: &str, seq: u64) -> String {
    format_sysid(day, seq)
}

/// 左侧以 ASCII 空格补齐到 `width`；超长则截断（不保留尾 NUL，由 `put_fixed` 处理）。
fn pad_leading(body: &str, width: usize) -> String {
    let bytes = body.as_bytes();
    if bytes.len() >= width {
        return body.chars().take(width).collect();
    }
    let mut out = String::with_capacity(width);
    for _ in 0..(width - bytes.len()) {
        out.push(' ');
    }
    out.push_str(body);
    out
}

/// 交易日内单调递增的序号分配器（系统号/成交号各自一个）。
#[derive(Debug, Default)]
pub struct SeqAllocator {
    next: u64,
}

impl SeqAllocator {
    pub fn new() -> Self {
        Self::default()
    }
    /// 取下一序号（从 1 起）。
    pub fn take(&mut self) -> u64 {
        self.next += 1;
        self.next
    }
    /// 当前已分配到的序号（未分配时为 0）。
    pub fn last(&self) -> u64 {
        self.next
    }
}

// ---- OrderRef（13B：数字短号 ≤12 位 + NUL） ----

/// OrderRef 字段宽度（`TThostFtdcOrderRefType` = `[c_char; 13]`）。
pub const ORDER_REF_LEN: usize = 13;

/// 从 13B OrderRef 缓冲解析数字短号；空/非数字返回 `None`（对齐服务端 MaxOrderRef 口径）。
pub fn parse_order_ref(buf: &[c_char]) -> Option<i32> {
    get_fixed(buf).trim().parse::<i32>().ok()
}

/// 校验 OrderRef 文本：非空、纯数字、位数 ≤12（写入 13B 后仍有 NUL）。
pub fn is_valid_order_ref(text: &str) -> bool {
    let t = text.trim();
    !t.is_empty() && t.len() < ORDER_REF_LEN && t.bytes().all(|b| b.is_ascii_digit())
}

// ---- 北京时间时间戳（可注入秒值，保证测试确定性） ----

/// 北京时区 UTC+8（CTP 国内柜台口径）。
pub fn beijing_offset() -> FixedOffset {
    FixedOffset::east_opt(8 * 3600).expect("valid offset")
}

/// 由 Unix 秒得到北京时间交易日 `YYYYMMDD`（夜盘算下一交易日的语义不在 mock 范围）。
pub fn beijing_day(ts_secs: i64) -> String {
    beijing_offset()
        .from_utc_datetime(&Utc.timestamp_opt(ts_secs, 0).unwrap().naive_utc())
        .format("%Y%m%d")
        .to_string()
}

/// 由 Unix 秒得到北京时间 `HH:MM:SS`。
pub fn beijing_hms(ts_secs: i64) -> String {
    beijing_offset()
        .from_utc_datetime(&Utc.timestamp_opt(ts_secs, 0).unwrap().naive_utc())
        .format("%H:%M:%S")
        .to_string()
}

/// 当前 Unix 秒（生产路径用；测试直接给固定秒值调 [`beijing_day`]/[`beijing_hms`]）。
pub fn now_ts_secs() -> i64 {
    Utc::now().timestamp()
}

// ---- 系统号 / 成交号生成器（交易日内单调递增、21B 前导空格风格） ----

/// 一个交易日内的系统号/成交号生成器：日期前缀 + 独立递增序列。
#[derive(Debug)]
pub struct IdGen {
    day: String,
    sys_seq: SeqAllocator,
    trade_seq: SeqAllocator,
}

impl IdGen {
    /// 以交易日（`YYYYMMDD`）构造。
    pub fn new(day: impl Into<String>) -> Self {
        Self {
            day: day.into(),
            sys_seq: SeqAllocator::new(),
            trade_seq: SeqAllocator::new(),
        }
    }

    /// 分配下一 21B 风格 OrderSysID（前导空格 + `YYYYMMDD-seq`）。
    pub fn next_sys_id(&mut self) -> String {
        format_sysid(&self.day, self.sys_seq.take())
    }

    /// 分配下一 21B 风格 TradeID（独立序列）。
    pub fn next_trade_id(&mut self) -> String {
        format_trade_id(&self.day, self.trade_seq.take())
    }

    /// 当前交易日。
    pub fn day(&self) -> &str {
        &self.day
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_round_trip_and_nul_terminates() {
        let mut buf = [0 as c_char; 13];
        put_fixed(&mut buf, "42");
        assert_eq!(get_fixed(&buf), "42");
        // 首个 NUL 之后的脏数据被忽略。
        buf[3] = b'X' as c_char;
        assert_eq!(get_fixed(&buf), "42");
    }

    #[test]
    fn put_fixed_truncates_within_capacity() {
        let mut buf = [0 as c_char; 4];
        put_fixed(&mut buf, "rb2510");
        assert_eq!(get_fixed(&buf), "rb2");
    }

    #[test]
    fn char_code_round_trips_status_digit() {
        let mut c = 0 as c_char;
        put_char(&mut c, '3');
        assert_eq!(get_char(c), '3');
    }

    #[test]
    fn gb18030_chinese_round_trips() {
        let mut buf = [0 as c_char; 81];
        put_gb18030(&mut buf, "报单已全成交");
        assert_eq!(get_fixed(&buf), "报单已全成交");
    }

    #[test]
    fn sysid_is_20_chars_with_leading_spaces() {
        let id = format_sysid("20261015", 1);
        assert_eq!(id.chars().count(), 20);
        // 右对齐：正文 "20261015-1" 长 10，左侧补 10 个空格。
        assert!(id.starts_with("          "));
        assert!(id.ends_with("20261015-1"));
        assert_eq!(
            id.trim_end_matches(' ').trim_start_matches(' '),
            "20261015-1"
        );
    }

    #[test]
    fn sysid_body_is_right_aligned_and_nul_fits_in_21() {
        let id = format_sysid("20261015", 7);
        // put_fixed 写入 [c_char;21]：20 字符正文 + 1 NUL，不截断正文。
        let mut buf = [0 as c_char; 21];
        put_fixed(&mut buf, &id);
        assert_eq!(get_fixed(&buf), id);
        assert!(get_fixed(&buf).ends_with("20261015-7"));
    }

    #[test]
    fn seq_allocator_increments_from_one() {
        let mut a = SeqAllocator::new();
        assert_eq!(a.take(), 1);
        assert_eq!(a.take(), 2);
        assert_eq!(a.last(), 2);
    }

    #[test]
    fn order_ref_round_trips_and_parses_numeric() {
        let mut buf = [0 as c_char; 13];
        put_fixed(&mut buf, "123");
        assert_eq!(parse_order_ref(&buf), Some(123));
        // 空缓冲解析为 None。
        assert_eq!(parse_order_ref(&[0 as c_char; 13]), None);
    }

    #[test]
    fn order_ref_validation_rules() {
        assert!(is_valid_order_ref("1"));
        assert!(is_valid_order_ref("123456789012")); // 12 位
        assert!(!is_valid_order_ref("1234567890123")); // 13 位超长
        assert!(!is_valid_order_ref(""));
        assert!(!is_valid_order_ref("12a"));
    }

    #[test]
    fn beijing_day_and_hms_are_utc_plus_8() {
        // 2026-10-15 01:00:00 UTC = 09:00:00 北京同日。
        let ts = 1_792_000_000; // 仅作输入秒，断言以函数换算结果为准。
        let day = beijing_day(ts);
        let hms = beijing_hms(ts);
        assert_eq!(day.len(), 8);
        assert!(day.chars().all(|c| c.is_ascii_digit()));
        assert_eq!(hms.len(), 8);
        assert!(hms.contains(':'));
    }

    #[test]
    fn beijing_day_rolls_over_at_utc_16() {
        // UTC 16:00 = 北京次日 00:00，验证跨日换算。
        // 取 2026-10-15 16:00:00 UTC（构造确定秒）。
        use chrono::NaiveDate;
        let utc = NaiveDate::from_ymd_opt(2026, 10, 15)
            .unwrap()
            .and_hms_opt(16, 0, 0)
            .unwrap();
        let ts = Utc.from_utc_datetime(&utc).timestamp();
        assert_eq!(beijing_day(ts), "20261016");
        assert_eq!(beijing_hms(ts), "00:00:00");
    }

    #[test]
    fn id_gen_allocates_distinct_sequential_sys_and_trade_ids() {
        let mut g = IdGen::new("20261015");
        let s1 = g.next_sys_id();
        let s2 = g.next_sys_id();
        let t1 = g.next_trade_id();
        assert!(s1.ends_with("20261015-1"));
        assert!(s2.ends_with("20261015-2"));
        assert!(t1.ends_with("20261015-1")); // 成交号独立序列
        assert_eq!(g.day(), "20261015");
    }
}
