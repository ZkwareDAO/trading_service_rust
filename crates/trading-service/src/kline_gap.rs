//! K线缺口检测的**服务层编排**（P3）：在纯内核 [`trading_core::kline`] 的判定函数与
//! [`GapRegistry`] 之上，组织「检测点 2（内部扫描）+ 检测点 3（静默停推）」的周期一拍。
//!
//! 检测点 1（实时尾部）在持久化 sink 内同步做（见 `kline.rs`）；本模块只负责低频扫描。
//! **仅三家 7×24 连续撮合所**（binance/deribit/hyperliquid）；CTP 有空分钟/午休/夜盘，
//! 一律跳过。全程纯内存、不连网、不读文件；网络回补是后续 P4。
//!
//! 可测性：核心一拍 [`GapScanner::scan_tick_at`] 由调用方注入 `now_ms`（对齐
//! `KlineService::history_at` 的注入时钟范式），后台线程只负责定时喂墙钟。

use std::sync::Arc;

use trading_core::kline::{scan_gaps, stall_gap, BarTable, GapRange, GapRegistry};

/// 参与缺口检测/回补的三家交易所（7×24 连续撮合，可用「相邻分钟连续」判缺）。
/// CTP 不在此列（期货空分钟合法，将来需先引入交易时段日历）。
pub fn gap_supported_venue(venue: &str) -> bool {
    matches!(venue, "binance" | "deribit" | "hyperliquid")
}

/// 缺口扫描参数（启动期由配置装配，禁硬编码）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GapScanConfig {
    /// 1m 步长毫秒（生产取 KLINE_STEP_MS）。
    pub step_ms: i64,
    /// 静默停推阈值毫秒：`now − 末键` 超过它才登记（默认 3min）。
    pub stall_threshold_ms: i64,
    /// 周期扫描间隔毫秒；0 = 不起后台扫描线程（实时尾检仍在）。
    pub scan_interval_ms: i64,
}

impl GapScanConfig {
    /// 校验构造：`step_ms/stall_threshold_ms` 必须 >0（否则检测静默全失效），
    /// `scan_interval_ms` 允许 0（关闭周期扫描）但不可为负（i64→u64 会 wrapping 成巨值）。
    /// 非法返回 None，由启动装配 fail-fast。
    pub fn new(step_ms: i64, stall_threshold_ms: i64, scan_interval_ms: i64) -> Option<Self> {
        if step_ms <= 0 || stall_threshold_ms <= 0 || scan_interval_ms < 0 {
            return None;
        }
        Some(Self {
            step_ms,
            stall_threshold_ms,
            scan_interval_ms,
        })
    }
}

/// 缺口周期扫描器：所有数据句柄取自 TradingStore（bar_table/gap_registry 均进程单例），
/// 自身无共享可变状态，可被后台线程与测试共享同一句柄。
pub struct GapScanner {
    /// 定稿 bar 单一内存事实源（读有序 start 键/末键）。
    bars: Arc<BarTable>,
    /// 检出缺口的去重登记集合（store 收口）。
    gaps: Arc<GapRegistry>,
    /// 步长/静默阈值参数。
    cfg: GapScanConfig,
}

impl GapScanner {
    pub fn new(bars: Arc<BarTable>, gaps: Arc<GapRegistry>, cfg: GapScanConfig) -> Self {
        Self { bars, gaps, cfg }
    }

    /// 生产入口：喂墙钟跑一拍。
    pub fn scan_tick(&self) -> usize {
        self.scan_tick_at(crate::kline::now_utc_ms())
    }

    /// 一拍扫描（`now_ms` 注入，确定性测试）：遍历全部序列，仅白名单所做内部扫描 +
    /// 静默检测。返回本拍**新登记**的缺失分钟总数（已存在的去重分钟不计）。
    pub fn scan_tick_at(&self, now_ms: i64) -> usize {
        self.bars
            .series_starts()
            .into_iter()
            .filter(|s| gap_supported_venue(&s.key.venue))
            .map(|s| self.scan_series(&s.key.venue, &s.key.symbol, &s.starts, now_ms))
            .sum()
    }

    /// 扫描单个白名单序列：检测点 2 对保留窗有序 start 逐对比（补登乱序/漏网），
    /// 检测点 3 对末键做静默判定；返回新登记分钟数。
    fn scan_series(&self, venue: &str, symbol: &str, starts: &[i64], now_ms: i64) -> usize {
        let mut added = 0;
        for gap in scan_gaps(starts.iter().copied(), self.cfg.step_ms) {
            added += self.record_gap(venue, symbol, gap, None);
        }
        if let Some(&last) = starts.last() {
            if let Some(gap) =
                stall_gap(last, now_ms, self.cfg.step_ms, self.cfg.stall_threshold_ms)
            {
                added += self.record_gap(venue, symbol, gap, Some(last));
            }
        }
        added
    }

    /// 登记一段缺口；仅当确有新分钟时打一次边沿日志（`last_start=None`=内部扫描 info，
    /// `Some`=静默停推 warn），避免周期扫描对同一缺口每拍刷屏。
    fn record_gap(
        &self,
        venue: &str,
        symbol: &str,
        gap: GapRange,
        last_start: Option<i64>,
    ) -> usize {
        let added = self.gaps.record(venue, symbol, &gap, self.cfg.step_ms);
        if added == 0 {
            return 0;
        }
        match last_start {
            Some(last_start) => tracing::warn!(
                venue,
                symbol,
                last_start,
                gap_start = gap.start,
                gap_end = gap.end,
                missing = added,
                "K线静默停推：超阈值未收新 bar（WS 可能假活，已登记缺口）"
            ),
            None => tracing::info!(
                venue,
                symbol,
                gap_start = gap.start,
                gap_end = gap.end,
                missing = added,
                "K线内部扫描发现缺口（已登记待回补集合）"
            ),
        }
        added
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trading_core::kline::SeriesKey;
    use trading_models::Bar;

    const STEP: i64 = 60_000;
    const STALL: i64 = 3 * STEP;

    /// 测试用：第 min 分钟的 start 毫秒。
    fn m(min: i64) -> i64 {
        min * STEP
    }

    /// 测试用：构造一根指定 start 的 1m bar。
    fn bar(venue: &str, symbol: &str, start: i64) -> Bar {
        Bar::new(venue, symbol, 20261029, start, start + STEP)
    }

    fn scanner_with(step: i64, stall: i64) -> (Arc<BarTable>, Arc<GapRegistry>, GapScanner) {
        let bars = Arc::new(BarTable::new());
        let gaps = Arc::new(GapRegistry::new());
        let cfg = GapScanConfig::new(step, stall, 0).unwrap(); // 单测不起线程
        let scanner = GapScanner::new(Arc::clone(&bars), Arc::clone(&gaps), cfg);
        (bars, gaps, scanner)
    }

    #[test]
    fn venue_whitelist_matches_only_three_continuous_venues() {
        assert!(gap_supported_venue("binance"));
        assert!(gap_supported_venue("deribit"));
        assert!(gap_supported_venue("hyperliquid"));
        assert!(!gap_supported_venue("ctp"));
        assert!(!gap_supported_venue("okx"));
        assert!(!gap_supported_venue(""));
    }

    #[test]
    fn scan_config_validates_and_allows_zero_scan_interval() {
        // 合法：scan=0 表示关闭周期扫描。
        assert!(GapScanConfig::new(STEP, STALL, 0).is_some());
        // 非法：步长/静默阈值非正、扫描周期为负，均 fail-fast。
        assert!(GapScanConfig::new(0, STALL, 100).is_none());
        assert!(GapScanConfig::new(STEP, 0, 100).is_none());
        assert!(GapScanConfig::new(STEP, STALL, -1).is_none());
    }

    #[test]
    fn scan_empty_table_reports_zero() {
        let (_bars, _gaps, scanner) = scanner_with(STEP, STALL);
        assert_eq!(scanner.scan_tick_at(m(10)), 0);
    }

    #[test]
    fn scan_detects_internal_gap_for_supported_venue() {
        // Arrange：binance 有第 0、3 分钟（缺 1、2）；now 距末键 1 分钟，静默不触发。
        let (bars, gaps, scanner) = scanner_with(STEP, STALL);
        bars.upsert(bar("binance", "BTCUSDT", m(0)));
        bars.upsert(bar("binance", "BTCUSDT", m(3)));

        // Act：内部扫描应补登 1、2 两分钟。
        let added = scanner.scan_tick_at(m(4));

        // Assert
        assert_eq!(added, 2);
        let got: Vec<i64> = gaps.snapshot().iter().map(|p| p.start).collect();
        assert_eq!(got, vec![m(1), m(2)]);
    }

    #[test]
    fn scan_detects_stall_when_no_bar_over_threshold() {
        // Arrange：仅有第 0 分钟，now=4 分 +1ms（越 3min 阈值，且第 4 分钟尚未收盘）。
        let (bars, gaps, scanner) = scanner_with(STEP, STALL);
        bars.upsert(bar("binance", "BTCUSDT", m(0)));

        // Act：静默检测登记 1、2、3（不含未收盘的第 4 分钟）。
        let added = scanner.scan_tick_at(m(4) + 1);

        assert_eq!(added, 3);
        let got: Vec<i64> = gaps.snapshot().iter().map(|p| p.start).collect();
        assert_eq!(got, vec![m(1), m(2), m(3)]);
    }

    #[test]
    fn scan_stall_at_exact_threshold_reports_nothing() {
        // now − last 恰好 = 3min 阈值 → 不报（且单键无内部缺口）。
        let (bars, gaps, scanner) = scanner_with(STEP, STALL);
        bars.upsert(bar("binance", "BTCUSDT", m(0)));
        assert_eq!(scanner.scan_tick_at(m(3)), 0);
        assert!(gaps.is_empty());
    }

    #[test]
    fn scan_skips_ctp_even_with_large_minute_gap() {
        // CTP 即便分钟大跳也不检测（白名单外），但 bar 本身已正常进表。
        let (bars, gaps, scanner) = scanner_with(STEP, STALL);
        bars.upsert(bar("ctp", "cu2512", m(0)));
        bars.upsert(bar("ctp", "cu2512", m(10)));

        assert_eq!(scanner.scan_tick_at(m(20)), 0);
        assert!(gaps.is_empty());
        assert_eq!(bars.coverage(&SeriesKey::new("ctp", "cu2512")), 2);
    }

    #[test]
    fn scan_contiguous_unsorted_or_repeated_is_not_a_gap() {
        // 乱序到齐但分钟连续；now 紧贴末键（<阈值），不应报缺口。
        let (bars, gaps, scanner) = scanner_with(STEP, STALL);
        bars.upsert(bar("deribit", "BTC-1", m(2)));
        bars.upsert(bar("deribit", "BTC-1", m(0)));
        bars.upsert(bar("deribit", "BTC-1", m(1)));
        bars.upsert(bar("deribit", "BTC-1", m(1))); // 重复覆盖

        assert_eq!(scanner.scan_tick_at(m(2) + 1), 0);
        assert!(gaps.is_empty());
    }

    #[test]
    fn scan_second_tick_is_idempotent_after_gaps_recorded() {
        let (bars, _gaps, scanner) = scanner_with(STEP, STALL);
        bars.upsert(bar("hyperliquid", "BTC", m(0)));
        bars.upsert(bar("hyperliquid", "BTC", m(5)));

        // 第一拍登记缺口；同参数第二拍不重复登记。
        assert!(scanner.scan_tick_at(m(6)) > 0);
        assert_eq!(scanner.scan_tick_at(m(6)), 0);
    }
}
