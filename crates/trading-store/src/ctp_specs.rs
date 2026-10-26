//! CTP 合约规格快照持久化（每日一份，全量覆盖写）。
//!
//! 目录布局（根 `<root>/ctp-specs/`）：
//! ```text
//! YYYYMM/instruments-YYYY-MM-DD.csv
//! YYYYMM/commissions-YYYY-MM-DD.csv
//! latest/instruments.csv        # 冷启动种子（副本，非软链）
//! latest/commissions.csv
//! ```
//! 冷启动读 `latest/instruments.csv`；缺失时回退最近月份目录里日期最大的
//! `instruments-*.csv`。本模块只做文件读写，在线查询在 trading-exchange/ctp。

use std::path::{Path, PathBuf};

/// 合约规格一行（与 data/ctp-specs/instruments.csv 列序一致）。
#[derive(Debug, Clone, PartialEq)]
pub struct InstrumentSpecRow {
    pub instrument_id: String,
    pub exchange_id: String,
    pub product_id: String,
    pub instrument_name: String,
    /// 产品类型（'1'=期货）。
    pub product_class: u8,
    pub volume_multiple: i32,
    pub price_tick: f64,
    pub min_limit_volume: i32,
    pub max_limit_volume: i32,
    pub min_market_volume: i32,
    pub max_market_volume: i32,
    pub long_margin_ratio: f64,
    pub short_margin_ratio: f64,
    pub is_trading: bool,
    pub open_date: String,
    pub expire_date: String,
}

/// 合约手续费率一行（与 commissions.csv 列序一致）。
#[derive(Debug, Clone, PartialEq)]
pub struct CommissionSpecRow {
    pub instrument_id: String,
    pub exchange_id: String,
    pub open_by_money: f64,
    pub open_by_volume: f64,
    pub close_by_money: f64,
    pub close_by_volume: f64,
    pub close_today_by_money: f64,
    pub close_today_by_volume: f64,
}

const INSTRUMENT_HEADER: &str = "instrument_id,exchange_id,product_id,instrument_name,product_class,volume_multiple,price_tick,min_limit_volume,max_limit_volume,min_market_volume,max_market_volume,long_margin_ratio,short_margin_ratio,is_trading,open_date,expire_date";
const COMMISSION_HEADER: &str = "instrument_id,exchange_id,open_by_money,open_by_volume,close_by_money,close_by_volume,close_today_by_money,close_today_by_volume";

/// CTP 合约规格快照存储（持有 `<data>` 根目录）。
#[derive(Debug, Clone)]
pub struct CtpSpecsStore {
    root: PathBuf,
}

impl CtpSpecsStore {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { root: data_dir }
    }

    fn base(&self) -> PathBuf {
        self.root.join("ctp-specs")
    }

    /// 写当日 instruments：`YYYYMM/instruments-YYYY-MM-DD.csv` + `latest/instruments.csv`。
    /// 先写临时文件再改名（同目录原子替换）；返回当日文件路径。
    pub fn save_instruments(
        &self,
        day: &str,
        rows: &[InstrumentSpecRow],
    ) -> std::io::Result<PathBuf> {
        let month = yyyymm_from_day(day);
        let dated = self
            .base()
            .join(&month)
            .join(format!("instruments-{day}.csv"));
        let mut buf = String::from(INSTRUMENT_HEADER);
        buf.push('\n');
        for r in rows {
            buf.push_str(&instrument_line(r));
            buf.push('\n');
        }
        atomic_write(&dated, buf.as_bytes())?;
        let latest = self.base().join("latest").join("instruments.csv");
        atomic_write(&latest, buf.as_bytes())?;
        Ok(dated)
    }

    /// 写当日 commissions：`YYYYMM/commissions-YYYY-MM-DD.csv` + `latest/commissions.csv`。
    pub fn save_commissions(
        &self,
        day: &str,
        rows: &[CommissionSpecRow],
    ) -> std::io::Result<PathBuf> {
        let month = yyyymm_from_day(day);
        let dated = self
            .base()
            .join(&month)
            .join(format!("commissions-{day}.csv"));
        let mut buf = String::from(COMMISSION_HEADER);
        buf.push('\n');
        for r in rows {
            buf.push_str(&commission_line(r));
            buf.push('\n');
        }
        atomic_write(&dated, buf.as_bytes())?;
        let latest = self.base().join("latest").join("commissions.csv");
        atomic_write(&latest, buf.as_bytes())?;
        Ok(dated)
    }

    /// 冷启动种子路径：优先 `latest/instruments.csv`；缺失则回退最近月份目录
    /// 里日期最大的 `instruments-*.csv`；都没有返回 None（调用方回退纯在线）。
    pub fn latest_instrument_path(&self) -> Option<PathBuf> {
        let latest = self.base().join("latest").join("instruments.csv");
        if latest.exists() {
            return Some(latest);
        }
        // 扫描 YYYYMM/instruments-YYYY-MM-DD.csv，取文件名（含日期）最大者。
        let mut dated: Vec<PathBuf> = Vec::new();
        if let Ok(months) = std::fs::read_dir(self.base()) {
            for m in months.flatten() {
                let dir = m.path();
                if !dir.is_dir() || dir.file_name() == Some(std::ffi::OsStr::new("latest")) {
                    continue;
                }
                if let Ok(files) = std::fs::read_dir(&dir) {
                    for f in files.flatten() {
                        let p = f.path();
                        if p.file_name()
                            .and_then(|n| n.to_str())
                            .is_some_and(|n| n.starts_with("instruments-") && n.ends_with(".csv"))
                        {
                            dated.push(p);
                        }
                    }
                }
            }
        }
        // 文件名形如 instruments-YYYY-MM-DD.csv：定宽零填充日期使 PathBuf 字典序
        // 恰好等于日期序，pop() 取到最新一日（依赖该命名不变量）。
        dated.sort();
        dated.pop()
    }
}

/// `2026-10-26` → `202610`（非法/过短返回空串由调用方落盘失败暴露）。
///
/// 不变量：`day` 来自管理 API 的 `chrono` 本地日期 `%Y-%m-%d`（非外部自由输入），
/// 形如严格 `YYYY-MM-DD`，取前 6 字符去横线即月份目录。若将来放开 day 来源，
/// 应在此先以 NaiveDate 解析校验，避免污染数据目录。
fn yyyymm_from_day(day: &str) -> String {
    day.replace('-', "").get(0..6).unwrap_or("").to_string()
}

fn instrument_line(r: &InstrumentSpecRow) -> String {
    // 不变量：CTP 合约代码/交易所/品种/中文名与 YYYYMMDD 日期均保证不含逗号或换行
    // （柜台字符集约束），故可直接按固定 16 列序拼接而无需 csv 转义；若字段来源
    // 放开到可能含逗号的输入，须改用 csv::Writer 以防列错位。
    format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        r.instrument_id,
        r.exchange_id,
        r.product_id,
        r.instrument_name,
        r.product_class,
        r.volume_multiple,
        r.price_tick,
        r.min_limit_volume,
        r.max_limit_volume,
        r.min_market_volume,
        r.max_market_volume,
        r.long_margin_ratio,
        r.short_margin_ratio,
        if r.is_trading { 1 } else { 0 },
        r.open_date,
        r.expire_date
    )
}

fn commission_line(r: &CommissionSpecRow) -> String {
    format!(
        "{},{},{},{},{},{},{},{}",
        r.instrument_id,
        r.exchange_id,
        r.open_by_money,
        r.open_by_volume,
        r.close_by_money,
        r.close_by_volume,
        r.close_today_by_money,
        r.close_today_by_volume
    )
}

/// 先写同目录 `.tmp` 再 rename 覆盖（同文件系统原子替换；目录自动创建）。
fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("csv.tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inst(id: &str, product: &str, class: u8, trading: bool) -> InstrumentSpecRow {
        InstrumentSpecRow {
            instrument_id: id.into(),
            exchange_id: "SHFE".into(),
            product_id: product.into(),
            instrument_name: id.into(),
            product_class: class,
            volume_multiple: 5,
            price_tick: 10.0,
            min_limit_volume: 1,
            max_limit_volume: 500,
            min_market_volume: 1,
            max_market_volume: 60,
            long_margin_ratio: 0.1,
            short_margin_ratio: 0.1,
            is_trading: trading,
            open_date: "20251118".into(),
            expire_date: "20261116".into(),
        }
    }

    #[test]
    fn save_writes_dated_file_and_latest_copy() {
        let dir = tempfile::tempdir().unwrap();
        let store = CtpSpecsStore::new(dir.path().to_path_buf());
        let rows = vec![inst("cu2611", "cu", 1, true), inst("cu2612", "cu", 1, true)];

        let dated = store
            .save_instruments("2026-10-26", &rows)
            .expect("save ok");

        assert!(dated.ends_with("202610/instruments-2026-10-26.csv"));
        assert!(dated.exists());
        let latest = dir.path().join("ctp-specs/latest/instruments.csv");
        assert!(latest.exists());
        let text = std::fs::read_to_string(&latest).unwrap();
        assert!(text.contains("cu2611") && text.contains("cu2612"));
        assert_eq!(text.lines().count(), 3);
    }

    #[test]
    fn latest_path_falls_back_to_previous_month() {
        let dir = tempfile::tempdir().unwrap();
        let store = CtpSpecsStore::new(dir.path().to_path_buf());
        store
            .save_instruments("2026-09-30", &[inst("cu2611", "cu", 1, true)])
            .unwrap();
        std::fs::remove_file(dir.path().join("ctp-specs/latest/instruments.csv")).unwrap();

        let found = store.latest_instrument_path().expect("回退到 9 月文件");
        assert!(found.ends_with("202609/instruments-2026-09-30.csv"));
    }

    #[test]
    fn latest_path_prefers_latest_copy_then_newest_dated() {
        let dir = tempfile::tempdir().unwrap();
        let store = CtpSpecsStore::new(dir.path().to_path_buf());
        assert!(store.latest_instrument_path().is_none());
        store
            .save_instruments("2026-10-26", &[inst("cu2612", "cu", 1, true)])
            .unwrap();
        let p = store.latest_instrument_path().unwrap();
        assert!(p.ends_with("latest/instruments.csv"));
    }
}
