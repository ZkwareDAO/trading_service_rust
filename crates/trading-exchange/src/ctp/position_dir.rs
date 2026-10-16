//! 会话态「(合约, 持仓方向) → (今仓, 昨仓)」路由表。
//!
//! 背景：上期所（SHFE）平仓**不接受笼统 Close**，必须显式发 CloseToday/
//! CloseYesterday；跨今昨还要拆成两笔物理报单（见 `prep::close_offsets`）。
//! 路由需要本地知道每个合约每个方向上今仓/昨仓各有多少手。
//!
//! 生命周期：**仅会话内存**（DashMap），不落盘、不产生第二条持久仓位——
//! 持久层一个 (账户,交易所,合约,方向) 始终只有一条整合 Position，本表仅供
//! 「这一下单平今还是平昨」的瞬间路由。真相源是周期持仓快照
//! （`merge_position_snapshots` 收齐后整批 `set` 覆盖），实时成交按开平标志
//! 做增量（`apply_trade`）；断线/重连后随快照重建，丢得起。

use dashmap::DashMap;

use crate::ctp::core::types::{Direction, Offset};

/// 持仓方向（多/空）；与成交的买卖方向不同——卖平平的是多头。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PositionSide {
    /// 多头（持仓方向 '2'）。
    Long,
    /// 空头（持仓方向 '3'）。
    Short,
}

impl PositionSide {
    /// 由持仓快照的柜台方向字符构造：'2'=多、'3'=空，其余 None。
    pub fn from_position_char(code: char) -> Option<Self> {
        match code {
            '2' => Some(PositionSide::Long),
            '3' => Some(PositionSide::Short),
            _ => None,
        }
    }

    /// 开仓成交的持仓方向 = 成交买卖方向（买开→多、卖开→空）。
    pub fn opened_by(trade_direction: Direction) -> Self {
        match trade_direction {
            Direction::Buy => PositionSide::Long,
            Direction::Sell => PositionSide::Short,
        }
    }

    /// 平仓成交的持仓方向 = 成交买卖方向的反向（卖平→平多、买平→平空）。
    pub fn closed_by(trade_direction: Direction) -> Self {
        match trade_direction {
            Direction::Buy => PositionSide::Short,
            Direction::Sell => PositionSide::Long,
        }
    }
}

/// (合约, 持仓方向) → (今仓手数, 昨仓手数) 的会话态路由表（线程安全）。
#[derive(Default)]
pub struct PositionDir {
    by_key: DashMap<(String, PositionSide), (i32, i32)>,
}

impl PositionDir {
    pub fn new() -> Self {
        Self::default()
    }

    /// 快照整批覆盖一条 (合约, 方向) 的 (今, 昨)。
    ///
    /// 合约空白或任一值为负（柜台异常行）忽略；今昨皆 0 表示该方向已无持仓，
    /// 移除表项（与"从未见过"在路由语义上等价：任何平仓量都会被判不足）。
    pub fn set(&self, instrument: &str, side: PositionSide, today: i32, yesterday: i32) {
        let instrument = instrument.trim();
        if instrument.is_empty() || today < 0 || yesterday < 0 {
            return;
        }
        if today == 0 && yesterday == 0 {
            self.by_key.remove(&(instrument.to_string(), side));
            return;
        }
        // 后到覆盖：快照是真相源，新一轮对账以柜台权威值整体替换本地增量。
        self.by_key
            .insert((instrument.to_string(), side), (today, yesterday));
    }

    /// 读取 (今, 昨)；表中无该键返回 None（未对账/无持仓）。
    pub fn get(&self, instrument: &str, side: PositionSide) -> Option<(i32, i32)> {
        self.by_key
            .get(&(instrument.trim().to_string(), side))
            .map(|e| *e.value())
    }

    /// 实时成交增量维护（主数据仍由周期快照刷新）：
    /// * 开仓 → 今仓 +n（当日新开，日切后由下一轮快照归为昨仓）；
    /// * 平今 → 今仓 −n；平昨 → 昨仓 −n；扣减钳到 0，不为负；
    /// * 笼统 `Close`（非上期所柜台自动冲抵）分不清今/昨，**不动表也不建键**——
    ///   SHFE 只会出现显式平今/平昨，其余交易所不用本表，等快照收敛。
    ///
    /// `holding` 为该成交实际增减的**持仓方向**（开仓取成交方向、
    /// 平仓取反向，见 [`PositionSide::opened_by`]/[`PositionSide::closed_by`]）。
    /// 非正手数/空白合约忽略。平仓成交若先于首轮快照到达（键缺席），钳到 (0,0)
    /// 后不保留僵尸表项——避免把"尚未对账"误表达成"已知零仓"。
    pub fn apply_trade(&self, instrument: &str, offset: Offset, holding: PositionSide, n: i32) {
        let instrument = instrument.trim();
        if instrument.is_empty() || n <= 0 {
            return;
        }
        if offset == Offset::Close {
            return;
        }
        let key = (instrument.to_string(), holding);
        let was_present = self.by_key.contains_key(&key);
        let mut entry = self.by_key.entry(key).or_insert((0, 0));
        let (today, yesterday) = entry.value_mut();
        match offset {
            Offset::Open => *today = today.saturating_add(n),
            Offset::CloseToday => *today = (*today - n).max(0),
            Offset::CloseYesterday => *yesterday = (*yesterday - n).max(0),
            // 笼统 Close 已在上方提前返回；保留 no-op 臂而非 unreachable!，
            // 使本函数在任何调用形态下都不可能 panic（笼统平仓本就应不动表）。
            Offset::Close => {}
        }
        if !was_present && *today == 0 && *yesterday == 0 {
            let key = entry.key().clone();
            drop(entry);
            self.by_key.remove(&key);
        }
    }

    /// 已缓存的 (合约,方向) 条数（诊断/测试用）。
    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    /// 是否为空（诊断/测试用）。
    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_then_get_returns_today_and_yesterday_separately() {
        // Arrange
        let dir = PositionDir::new();

        // Act：今 2 昨 3。
        dir.set("rb2510", PositionSide::Long, 2, 3);

        // Assert：今昨分别取回，不混淆。
        assert_eq!(dir.get("rb2510", PositionSide::Long), Some((2, 3)));
        assert_eq!(dir.len(), 1);
    }

    #[test]
    fn get_missing_returns_none() {
        let dir = PositionDir::new();
        assert_eq!(dir.get("cu2611", PositionSide::Long), None);
        assert!(dir.is_empty());
    }

    #[test]
    fn keys_are_isolated_by_instrument_and_side() {
        // Arrange：同合约多空各一条 + 另一合约一条。
        let dir = PositionDir::new();
        dir.set("rb2510", PositionSide::Long, 2, 3);
        dir.set("rb2510", PositionSide::Short, 1, 0);
        dir.set("cu2611", PositionSide::Long, 5, 0);

        // Act / Assert：三条互不串。
        assert_eq!(dir.get("rb2510", PositionSide::Long), Some((2, 3)));
        assert_eq!(dir.get("rb2510", PositionSide::Short), Some((1, 0)));
        assert_eq!(dir.get("cu2611", PositionSide::Long), Some((5, 0)));
        assert_eq!(dir.get("cu2611", PositionSide::Short), None);
        assert_eq!(dir.len(), 3);
    }

    #[test]
    fn later_set_overwrites_previous_snapshot() {
        // Arrange
        let dir = PositionDir::new();
        dir.set("rb2510", PositionSide::Long, 2, 3);

        // Act：新一轮快照整体覆盖（快照是真相源）。
        dir.set("rb2510", PositionSide::Long, 0, 5);

        // Assert
        assert_eq!(dir.get("rb2510", PositionSide::Long), Some((0, 5)));
    }

    #[test]
    fn set_zero_zero_removes_entry() {
        // Arrange
        let dir = PositionDir::new();
        dir.set("rb2510", PositionSide::Long, 2, 3);

        // Act：快照显示该方向已无持仓。
        dir.set("rb2510", PositionSide::Long, 0, 0);

        // Assert：表项移除（路由上等同无仓，任何平仓量都会被判不足）。
        assert_eq!(dir.get("rb2510", PositionSide::Long), None);
        assert!(dir.is_empty());
    }

    #[test]
    fn set_ignores_blank_instrument_and_negative_values() {
        // Arrange
        let dir = PositionDir::new();
        dir.set("rb2510", PositionSide::Long, 2, 3);

        // Act：空白合约（含 trim 后空白）、负值行均忽略，不污染既有数据。
        dir.set("  ", PositionSide::Long, 1, 1);
        dir.set("rb2510", PositionSide::Long, -1, 3);
        dir.set("rb2510", PositionSide::Long, 2, -1);

        // Assert
        assert_eq!(dir.get("rb2510", PositionSide::Long), Some((2, 3)));
        assert_eq!(dir.len(), 1);
    }

    #[test]
    fn set_trims_instrument_and_dedups() {
        let dir = PositionDir::new();
        dir.set(" rb2510 ", PositionSide::Long, 2, 3);
        assert_eq!(dir.get("rb2510", PositionSide::Long), Some((2, 3)));
        assert_eq!(dir.len(), 1, "trim 后同合约只占一条");
    }

    #[test]
    fn open_trade_increments_today_on_holding_side() {
        // Arrange：rb 多头已有今 1。
        let dir = PositionDir::new();
        dir.set("rb2510", PositionSide::Long, 1, 0);

        // Act：买开 2 手（持仓方向=多）。
        dir.apply_trade(
            "rb2510",
            Offset::Open,
            PositionSide::opened_by(Direction::Buy),
            2,
        );

        // Assert：今仓累加，昨仓不动。
        assert_eq!(dir.get("rb2510", PositionSide::Long), Some((3, 0)));
    }

    #[test]
    fn open_sell_creates_short_today_without_touching_long() {
        // Arrange
        let dir = PositionDir::new();
        dir.set("rb2510", PositionSide::Long, 2, 3);

        // Act：卖开 1 手 → 空头今仓 +1（不串多头）。
        dir.apply_trade(
            "rb2510",
            Offset::Open,
            PositionSide::opened_by(Direction::Sell),
            1,
        );

        // Assert
        assert_eq!(dir.get("rb2510", PositionSide::Short), Some((1, 0)));
        assert_eq!(dir.get("rb2510", PositionSide::Long), Some((2, 3)));
    }

    #[test]
    fn close_today_decrements_today_and_never_goes_negative() {
        // Arrange：多头今 2 昨 3。
        let dir = PositionDir::new();
        dir.set("rb2510", PositionSide::Long, 2, 3);

        // Act：卖平今 2（成交方向卖 → 持仓方向多）。
        dir.apply_trade(
            "rb2510",
            Offset::CloseToday,
            PositionSide::closed_by(Direction::Sell),
            2,
        );
        // 再来一笔超额平今（异常/迟到回报）：钳到 0，不得为负。
        dir.apply_trade(
            "rb2510",
            Offset::CloseToday,
            PositionSide::closed_by(Direction::Sell),
            1,
        );

        // Assert
        assert_eq!(dir.get("rb2510", PositionSide::Long), Some((0, 3)));
    }

    #[test]
    fn close_yesterday_decrements_yesterday() {
        // Arrange：空头今 0 昨 4。
        let dir = PositionDir::new();
        dir.set("rb2510", PositionSide::Short, 0, 4);

        // Act：买平昨 3（成交方向买 → 持仓方向空）。
        dir.apply_trade(
            "rb2510",
            Offset::CloseYesterday,
            PositionSide::closed_by(Direction::Buy),
            3,
        );

        // Assert
        assert_eq!(dir.get("rb2510", PositionSide::Short), Some((0, 1)));
    }

    #[test]
    fn generic_close_does_not_mutate_table() {
        // Arrange：笼统 Close（非上期所）分不清今昨。
        let dir = PositionDir::new();
        dir.set("m2509", PositionSide::Long, 2, 3);

        // Act
        dir.apply_trade(
            "m2509",
            Offset::Close,
            PositionSide::closed_by(Direction::Sell),
            2,
        );

        // Assert：表不动，等下一轮快照收敛。
        assert_eq!(dir.get("m2509", PositionSide::Long), Some((2, 3)));
    }

    #[test]
    fn close_trade_on_unknown_key_leaves_no_zombie_entry() {
        // Arrange：空表（平仓成交先于首轮持仓快照到达）。
        let dir = PositionDir::new();

        // Act：平昨 1（键缺席）→ 钳 0 后不留 (0,0) 僵尸项。
        dir.apply_trade(
            "rb2510",
            Offset::CloseYesterday,
            PositionSide::closed_by(Direction::Sell),
            1,
        );

        // Assert：表中无此键——"尚未对账"(None) 不得被误表达成"已知零仓"。
        assert_eq!(dir.get("rb2510", PositionSide::Long), None);
        assert!(dir.is_empty());
    }

    #[test]
    fn closing_last_today_keeps_zero_entry_until_next_snapshot() {
        // 已知键被平到今仓 0、但仍有昨仓：表项保留；今昨皆 0 是真实"已平光"
        // （由快照 set(0,0) 负责移除），成交增量路径只清理自己新建的僵尸项。
        let dir = PositionDir::new();
        dir.set("rb2510", PositionSide::Long, 1, 0);
        dir.apply_trade(
            "rb2510",
            Offset::CloseToday,
            PositionSide::closed_by(Direction::Sell),
            1,
        );
        assert_eq!(dir.get("rb2510", PositionSide::Long), Some((0, 0)));
    }

    #[test]
    fn apply_trade_ignores_blank_instrument_and_non_positive_n() {
        let dir = PositionDir::new();
        dir.set("rb2510", PositionSide::Long, 2, 3);
        dir.apply_trade(
            "  ",
            Offset::Open,
            PositionSide::opened_by(Direction::Buy),
            1,
        );
        dir.apply_trade(
            "rb2510",
            Offset::Open,
            PositionSide::opened_by(Direction::Buy),
            0,
        );
        dir.apply_trade(
            "rb2510",
            Offset::Open,
            PositionSide::opened_by(Direction::Buy),
            -1,
        );
        assert_eq!(dir.get("rb2510", PositionSide::Long), Some((2, 3)));
    }

    #[test]
    fn position_side_maps_from_snapshot_char_and_trade_directions() {
        // 持仓快照字符：'2' 多、'3' 空、其余忽略。
        assert_eq!(
            PositionSide::from_position_char('2'),
            Some(PositionSide::Long)
        );
        assert_eq!(
            PositionSide::from_position_char('3'),
            Some(PositionSide::Short)
        );
        assert_eq!(PositionSide::from_position_char('9'), None);
        // 开仓：买卖方向即持仓方向。
        assert_eq!(PositionSide::opened_by(Direction::Buy), PositionSide::Long);
        assert_eq!(
            PositionSide::opened_by(Direction::Sell),
            PositionSide::Short
        );
        // 平仓：方向相反（卖平多、买平空）。
        assert_eq!(PositionSide::closed_by(Direction::Sell), PositionSide::Long);
        assert_eq!(PositionSide::closed_by(Direction::Buy), PositionSide::Short);
    }
}
