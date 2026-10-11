//! 历史归档回查（运行期读回 data/ 下历史 CSV 快照）：
//!
//! 内存表只保留当日数据——终态行当日可经 Repo peek，跨日 purge 后仅存在于
//! `{day}/{account}/{model}.csv`。本读取器按需扫描归档：
//! * 复用 store-core 的 [`paths::scan`] 枚举全部归档文件（天然按日期升序）；
//! * 查询时按日期**倒序**逐文件流式读取，命中即停，返回最新一日的状态；
//! * 不建索引、不缓存：调用点少、单文件仅 KB 级；将来量大再加优化不影响接口。

use std::path::{Path, PathBuf};

use store_core::persistence::{scan, DayFile};
use trading_models::{Order, Position, WatchRule};

/// 历史归档回查器：持有数据根目录，廉价构造、不持任何缓存。
#[derive(Debug, Clone)]
pub struct HistoryReader {
    root: PathBuf,
}

impl HistoryReader {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// 按客户单号回查订单；`account` 给定时只扫该账户目录，否则扫全部 scope。
    /// 多日命中时返回最新日期的状态。
    pub fn find_order(&self, account: Option<&str>, cid: &str) -> Option<Order> {
        self.find_model("order", account, |file| {
            read_matching(file, |o: &Order| o.client_order_id == cid)
        })
    }

    /// 按 `(strategy, signal_id)` 回查订单（含终态/跨日已 purge 行）。
    ///
    /// 策略侧往往只持有自己下发的 signal_id（cid 是服务生成、未必回传），
    /// 故提供此入口。必须同时给 strategy——signal 幂等以策略为作用域。
    pub fn find_order_by_signal(
        &self,
        account: Option<&str>,
        strategy: &str,
        signal_id: &str,
    ) -> Option<Order> {
        self.find_model("order", account, |file| {
            read_matching(file, |o: &Order| {
                o.strategy_name.as_deref() == Some(strategy)
                    && o.signal_id.as_deref() == Some(signal_id)
            })
        })
    }

    /// 按本地主键回查持仓（含已软平行）。
    pub fn find_position(&self, account: Option<&str>, id: u64) -> Option<Position> {
        self.find_model("position", account, |file| {
            read_matching(file, |p: &Position| p.id == id)
        })
    }

    /// 按本地主键回查规则（含 retired 行）。
    pub fn find_rule(&self, account: Option<&str>, id: u64) -> Option<WatchRule> {
        self.find_model("watch_rule", account, |file| {
            read_matching(file, |r: &WatchRule| r.id == id)
        })
    }

    /// 通用回查：枚举归档 → 过滤 model/账户 → 日期倒序逐文件读，命中即停。
    fn find_model<T, R>(&self, model: &str, account: Option<&str>, read: R) -> Option<T>
    where
        T: Sized,
        R: Fn(&Path) -> Option<T>,
    {
        let mut files = self.candidate_files(model, account);
        // scan 按日期升序；倒序后从最新日期开始读，命中即最新状态。
        files.reverse();
        files.into_iter().find_map(|f| read(&f.path))
    }

    /// 列出 model 匹配、账户 scope 匹配的归档文件（日期升序）。
    fn candidate_files(&self, model: &str, account: Option<&str>) -> Vec<DayFile> {
        let Ok(all) = scan(&self.root) else {
            return Vec::new();
        };
        all.into_iter()
            .filter(|f| f.model == model)
            .filter(|f| match account {
                // 账户目录名与持久化写入口径一致（store-core 会做同名清洗）。
                Some(name) => f.scope_dir == name,
                None => true,
            })
            .collect()
    }
}

/// 流式读取一个 CSV 文件，返回首个满足谓词的反序列化行；坏行跳过、读错返回 None。
fn read_matching<T, P>(path: &Path, predicate: P) -> Option<T>
where
    T: for<'de> serde::Deserialize<'de>,
    P: Fn(&T) -> bool,
{
    let Ok(mut reader) = csv::Reader::from_path(path) else {
        return None;
    };
    // flatten：个别坏行（Err）不致命，跳过继续读；find 命中首个满足谓词的行。
    reader.deserialize().flatten().find(|row| predicate(row))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::fs;
    use tempfile::tempdir;
    use trading_models::{Order, OrderStatus, Position, Side, WatchRule, WatchRuleStatus};

    /// 在临时根下写一份 `{day}/{account}/{model}.csv`（含表头）。
    fn write_csv<T: serde::Serialize>(
        root: &Path,
        day: &str,
        account: &str,
        model: &str,
        rows: &[T],
    ) {
        let dir = root.join(day).join(account);
        fs::create_dir_all(&dir).unwrap();
        let mut wtr = csv::Writer::from_path(dir.join(format!("{model}.csv"))).unwrap();
        for row in rows {
            wtr.serialize(row).unwrap();
        }
        wtr.flush().unwrap();
    }

    /// 装配一条订单（指定 cid/status/id）。
    fn order(cid: &str, status: OrderStatus, id: u64) -> Order {
        let mut o = Order::new(1, cid, "binance", "BTCUSDT", Side::Buy);
        o.id = id;
        o.status = status;
        o
    }

    /// 装配一条持仓（指定 id/status 由 deleted 表达）。
    fn position(id: u64, deleted: bool) -> Position {
        let mut p = Position::new(1, "binance", "BTCUSDT", Side::Buy);
        p.id = id;
        p.deleted = deleted;
        p
    }

    /// 装配一条规则（指定 id/status）。
    fn rule(id: u64, status: WatchRuleStatus) -> WatchRule {
        let mut r = WatchRule::new_risk(
            1,
            0,
            trading_models::WatchConditionKind::Roi,
            None,
            ">=",
            Decimal::ONE,
            trading_models::WatchRuleAction::UseSelf,
            trading_models::OrderType::Market,
            Decimal::ZERO,
            Decimal::ONE,
        );
        r.id = id;
        r.status = status;
        r
    }

    #[test]
    fn finds_order_in_older_day_snapshot() {
        // Arrange：订单只存在于历史日期目录。
        let root = tempdir().unwrap();
        let cid = "0x00000000000000000000000000000001";
        write_csv(
            root.path(),
            "2026-09-20",
            "alice",
            "order",
            &[order(cid, OrderStatus::Filled, 7)],
        );
        let reader = HistoryReader::new(root.path().to_path_buf());

        // Act + Assert：按账户+cid 命中历史行。
        let found = reader
            .find_order(Some("alice"), cid)
            .expect("应回查到历史订单");
        assert_eq!(found.id, 7);
        assert_eq!(found.status, OrderStatus::Filled);
    }

    #[test]
    fn returns_latest_day_state_when_same_order_spans_days() {
        // Arrange：同一 cid 在两个日期快照里状态不同（先 NEW 后 FILLED）。
        let root = tempdir().unwrap();
        let cid = "0x00000000000000000000000000000002";
        write_csv(
            root.path(),
            "2026-09-20",
            "alice",
            "order",
            &[order(cid, OrderStatus::New, 7)],
        );
        write_csv(
            root.path(),
            "2026-09-21",
            "alice",
            "order",
            &[order(cid, OrderStatus::Filled, 7)],
        );
        let reader = HistoryReader::new(root.path().to_path_buf());

        // Act：倒序扫描应返回最新一日的 FILLED。
        let found = reader.find_order(Some("alice"), cid).unwrap();
        assert_eq!(found.status, OrderStatus::Filled);
    }

    #[test]
    fn order_account_scope_filters_other_account_files() {
        // Arrange：cid 只写在 bob 目录；用 alice 过滤不得命中。
        let root = tempdir().unwrap();
        let cid = "0x00000000000000000000000000000003";
        write_csv(
            root.path(),
            "2026-09-21",
            "bob",
            "order",
            &[order(cid, OrderStatus::Filled, 7)],
        );
        let reader = HistoryReader::new(root.path().to_path_buf());

        // Act + Assert：账户隔离；不带账户则跨 scope 命中。
        assert!(reader.find_order(Some("alice"), cid).is_none());
        assert!(reader.find_order(None, cid).is_some());
    }

    /// 带 signal_id/strategy 的订单装配。
    fn signal_order(cid: &str, strategy: &str, signal: &str) -> Order {
        let mut o = Order::new(1, cid, "binance", "BTCUSDT", Side::Buy);
        o.strategy_name = Some(strategy.to_string());
        o.signal_id = Some(signal.to_string());
        o.status = OrderStatus::Filled;
        o
    }

    #[test]
    fn finds_order_by_strategy_and_signal_id() {
        // Arrange：历史日里订单按 (strategy, signal) 可定位。
        let root = tempdir().unwrap();
        let cid = "0x00000000000000000000000000000004";
        write_csv(
            root.path(),
            "2026-09-20",
            "alice",
            "order",
            &[signal_order(cid, "gridA", "sig-9")],
        );
        let reader = HistoryReader::new(root.path().to_path_buf());

        // Act + Assert：按 strategy+signal 命中；错 strategy/signal 不得命中。
        let found = reader
            .find_order_by_signal(Some("alice"), "gridA", "sig-9")
            .expect("应按 signal 回查");
        assert_eq!(found.client_order_id, cid);
        assert!(reader
            .find_order_by_signal(Some("alice"), "gridA", "sig-x")
            .is_none());
        assert!(reader
            .find_order_by_signal(Some("alice"), "gridB", "sig-9")
            .is_none());
    }

    #[test]
    fn missing_order_returns_none() {
        // 空目录下任何查询都为 None。
        let root = tempdir().unwrap();
        let reader = HistoryReader::new(root.path().to_path_buf());
        assert!(reader.find_order(Some("alice"), "0xdead").is_none());
    }

    #[test]
    fn finds_closed_position_by_id() {
        // Arrange：持仓只存在于历史日、且已软平（deleted=1）。
        let root = tempdir().unwrap();
        write_csv(
            root.path(),
            "2026-09-21",
            "alice",
            "position",
            &[position(11, true)],
        );
        let reader = HistoryReader::new(root.path().to_path_buf());

        // Act + Assert。
        let found = reader
            .find_position(Some("alice"), 11)
            .expect("应回查到历史持仓");
        assert!(found.deleted);
    }

    #[test]
    fn finds_retired_rule_by_id() {
        // Arrange：规则只存在于历史日、状态 retired。
        let root = tempdir().unwrap();
        write_csv(
            root.path(),
            "2026-09-21",
            "alice",
            "watch_rule",
            &[rule(22, WatchRuleStatus::Retired)],
        );
        let reader = HistoryReader::new(root.path().to_path_buf());

        // Act + Assert。
        let found = reader
            .find_rule(Some("alice"), 22)
            .expect("应回查到历史规则");
        assert_eq!(found.status, WatchRuleStatus::Retired);
    }

    #[test]
    fn unreadable_rows_are_skipped_not_fatal() {
        // Arrange：历史目录里是坏 CSV（目录结构合法但内容损坏）。
        let root = tempdir().unwrap();
        let dir = root.path().join("2026-09-21").join("alice");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("order.csv"), "id,not_a_real_header\n1,x\n").unwrap();
        let reader = HistoryReader::new(root.path().to_path_buf());

        // Act：解析失败行被跳过，返回 None 而非 panic。
        assert!(reader.find_order(Some("alice"), "0xdead").is_none());
    }
}
