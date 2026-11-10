//! 仓位对账纯内核（**交易所无关**）：交易所回查快照 vs 本地活跃仓位的三方对比。
//!
//! 本模块只认领域类型
//! [`Position`]：binance / hyperliquid / deribit 三家在符号、方向、合约乘数、结算
//! 币种上的差异都在上一层 mapper（交易所侧 `PositionInfo -> Position`）收敛，
//! 对比内核不做任何交易所分支，新增交易所无需改动此处。
//!
//! 纯裁决函数 [`diff_positions`] 零 IO、确定性、可独立单测；取快照、写表由上层
//! 编排（trading-service 的 mapper + trading-store 的 PositionRepo）负责。

use std::collections::HashMap;

use rust_decimal::Decimal;

use crate::position::Position;
use crate::types::{InstrumentType, Side};

/// 单条仓位对账裁决。
#[derive(Debug, Clone)]
pub enum PositionDiff {
    /// 交易所有、本地无：新建仓位（已按快照当前价重估 roi 等衍生字段）。
    Created(Position),
    /// 两边都有：以交易所快照覆盖变动字段、保留本地账簿字段后重估。
    Updated(Position),
    /// 本地有、交易所回查无：软平仓（`deleted=1` 并写 `close_time`，tombstone 保留当日）。
    Closed(Position),
}

impl PositionDiff {
    /// 裁决对应的仓位（读字段/打日志用）。
    pub fn position(&self) -> &Position {
        match self {
            PositionDiff::Created(p) | PositionDiff::Updated(p) | PositionDiff::Closed(p) => p,
        }
    }

    pub fn is_created(&self) -> bool {
        matches!(self, PositionDiff::Created(_))
    }
}

/// 一次对账的结果计数（日志/可观测用）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    pub created: usize,
    pub updated: usize,
    pub closed: usize,
}

/// 对账键：`(account, instrument, side, instrument_type)`。
/// `Side`/`InstrumentType` 未派生 `Hash`，统一拍平成整数与字符串参与哈希。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PositionKey {
    account_id: u64,
    instrument: String,
    side: u8,
    instrument_type: u8,
}

impl PositionKey {
    fn of(p: &Position) -> Self {
        Self {
            account_id: p.account_id,
            instrument: p.instrument.clone(),
            side: p.side as u8,
            instrument_type: p.instrument_type as u8,
        }
    }
}

/// 对比交易所回查快照与本地活跃仓位，产出应落库的裁决列表。
///
/// - 本地仅传对账作用域内（同账户/同交易所/同 instrument_type）`deleted=0` 的活跃仓位；
/// - `now_ms` 仅用于平仓行的 `close_time`；
/// - 返回顺序确定（Created/Updated 在前、Closed 在后，各段按键+id 排序），便于单测与日志。
///
/// **本地重复 key 自愈**：同一 `(account, instrument, side, instrument_type)` 若存在多条
/// 活跃行（历史脏数据/异常插入），本函数不再用 HashMap 静默覆盖：远端仍在时保留
/// [`pick_keeper`] 选出的 keeper 并 `Updated`，组内其余行全部 `Closed`；远端已无该 key
/// 时整组 `Closed`。远端适配层保证每个 key 只出现一条，本函数不处理远端重复。
pub fn diff_positions(
    exchange_back: Vec<Position>,
    local_active: Vec<Position>,
    now_ms: u64,
) -> Vec<PositionDiff> {
    // 同 key 收成一组（保留全部行，禁止后写覆盖前写）。
    let mut local: HashMap<PositionKey, Vec<Position>> = HashMap::new();
    for p in local_active {
        local.entry(PositionKey::of(&p)).or_default().push(p);
    }

    let mut diffs = Vec::new();

    for remote in exchange_back {
        let key = PositionKey::of(&remote);
        match local.remove(&key) {
            // 本地已无该 key（未持有或组已在上一轮消费）→ 新建。
            None => {
                let mut p = remote;
                p.deleted = false;
                p.close_time = None;
                let price = p.current_price;
                diffs.push(PositionDiff::Created(p.revalued(price)));
            }
            // 两边都有 → keeper 承接远端字段；组内其余重复行软平仓（自愈）。
            Some(group) => {
                let Some(keeper) = pick_keeper(&remote, &group) else {
                    // 组由非空 Vec 构造，此分支不可达；保守按远端新建，绝不 panic。
                    let mut p = remote;
                    p.deleted = false;
                    p.close_time = None;
                    let price = p.current_price;
                    diffs.push(PositionDiff::Created(p.revalued(price)));
                    continue;
                };
                for extra in group.iter().filter(|p| p.id != keeper.id) {
                    let mut closed = extra.clone();
                    closed.deleted = true;
                    closed.close_time = Some(now_ms);
                    diffs.push(PositionDiff::Closed(closed));
                }
                let merged = merge_remote_into_local(keeper, &remote);
                let price = merged.current_price;
                diffs.push(PositionDiff::Updated(merged.revalued(price)));
            }
        }
    }

    // 本地有、交易所无 → 整组软平仓（组内每条都不得泄漏）。
    for (_, group) in local {
        for existing in group {
            let mut p = existing;
            p.deleted = true;
            p.close_time = Some(now_ms);
            diffs.push(PositionDiff::Closed(p));
        }
    }

    // 确定顺序：Created/Updated 在前，Closed 在后；同键再按 id，消除 HashMap 顺序影响。
    diffs.sort_by_key(|d| {
        let p = d.position();
        (
            matches!(d, PositionDiff::Closed(_)) as u8,
            p.account_id,
            p.instrument.clone(),
            p.instrument_type as u8,
            p.side as u8,
            p.id,
        )
    });
    diffs
}

/// 从本地重复组中选出承接远端更新的 keeper：
/// 1. 优先 `(quantity, pos_price)` 与远端一致的行——对账一直在维护的那条；
///    多条一致时取 `id` 最大者，消除输入顺序依赖；
/// 2. 不匹配时同样取 `id` 最大者（最后插入）。
///
/// 空组返回 `None`（调用方以非空 Vec 构造组，正常路径不会走到）。
fn pick_keeper<'a>(remote: &Position, group: &'a [Position]) -> Option<&'a Position> {
    group
        .iter()
        .filter(|p| p.quantity == remote.quantity && p.pos_price == remote.pos_price)
        .max_by_key(|p| p.id)
        .or_else(|| group.iter().max_by_key(|p| p.id))
}

/// 同一对账键存在多条活跃仓位的组（**观测用**；裁决由 [`diff_positions`] 自愈）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateActiveGroup {
    pub account_id: u64,
    pub instrument: String,
    pub side: Side,
    pub instrument_type: InstrumentType,
    /// 组内仓位 id（升序）。
    pub ids: Vec<u64>,
}

/// 扫描本地活跃仓位中的重复对账键；返回按 key 排序、id 升序的分组。
///
/// 纯只读：不修改任何行；仅用于对账前 `warn!` 可观测性。
pub fn find_duplicate_active_groups(active: &[Position]) -> Vec<DuplicateActiveGroup> {
    // 只收集 id 与首次出现的元信息，避免为观测克隆整行 Position。
    let mut by_key: HashMap<PositionKey, (Side, InstrumentType, Vec<u64>)> = HashMap::new();
    for p in active {
        by_key
            .entry(PositionKey::of(p))
            .or_insert_with(|| (p.side, p.instrument_type, Vec::new()))
            .2
            .push(p.id);
    }
    let mut out: Vec<DuplicateActiveGroup> = by_key
        .into_iter()
        .filter(|(_, (_, _, ids))| ids.len() > 1)
        .map(|(key, (side, instrument_type, mut ids))| {
            ids.sort_unstable();
            DuplicateActiveGroup {
                account_id: key.account_id,
                instrument: key.instrument,
                side,
                instrument_type,
                ids,
            }
        })
        .collect();
    out.sort_by(|a, b| {
        (
            a.account_id,
            a.instrument.as_str(),
            a.side as u8,
            a.instrument_type as u8,
        )
            .cmp(&(
                b.account_id,
                b.instrument.as_str(),
                b.side as u8,
                b.instrument_type as u8,
            ))
    });
    out
}

/// 以交易所快照覆盖本地仓位的变动字段，保留本地权威的账簿/风控字段。
///
/// - 交易所权威：数量、成本价、杠杆、单位/乘数/结算币种、交易所回报时间、保证金；
///   当前价原则上也由交易所给，但**远端快照缺有效价（≤0）时保留本地上一轮好价**，
///   绝不把好价冲成 0（见下行赋值；本地也无价才维持 0，等待行情/下一轮补回）；
/// - 本地保留：`id`/`account_id`/`rule_id`/`created_at`（仓储 update 还会再保一次）、
///   `realized_pnl`（逐笔累计本地权威）；
/// - 特例——同向加仓（见 [`is_add_to_position`]）：成本基准已被摊薄改变，旧
///   `max_roi` 对新基准失效，须把 `max_roi`/`profit_drawdown` 清零重锚，否则会
///   立刻算出巨大回落、误触发回落止盈；其余情况 `max_roi` 仍延续；
/// - `notional`/`pnl_value`/`roi` 不在此赋值，统一由
///   [`Position::revalued`] 基于合并后的当前价重算（重锚时会把峰值顶到新 ROI），
///   保证「本地算」与「同步」两口径一致。
pub fn merge_remote_into_local(local: &Position, remote: &Position) -> Position {
    let mut p = local.clone();
    p.exchange = remote.exchange.clone();
    p.instrument_type = remote.instrument_type;
    p.instrument = remote.instrument.clone();
    p.side = remote.side;
    p.quantity = remote.quantity;
    p.qty_unit = remote.qty_unit;
    p.multiplier = remote.multiplier;
    p.pos_price = remote.pos_price;
    // 远端快照缺有效价（≤0）时保留本地上一轮有效价，绝不把好价冲成 0；
    // 本地也无价（首次建仓等）才维持 0，等待行情/下一轮补回。
    p.current_price = if remote.current_price > Decimal::ZERO {
        remote.current_price
    } else {
        local.current_price
    };
    p.leverage = remote.leverage;
    p.margin = remote.margin;
    p.settle_currency = remote.settle_currency.clone();
    p.exchange_update_time = remote.exchange_update_time;
    p.deleted = false;
    p.close_time = None;

    // 同向加仓摊薄了成本：峰值与回落按新成本基准重锚。
    if is_add_to_position(local, remote) {
        p.max_roi = Decimal::ZERO;
        p.profit_drawdown = Decimal::ZERO;
    }
    p
}

/// 是否为同向加仓：远端数量较本地增加，且成本均价已变。
///
/// 两者同时成立才认定为加仓——
/// * 减仓：数量下降，第一条件即不满足（且交易所减仓本就不改平均价）；
/// * 纯行情波动：数量与成本价都不变；
/// * 反向越过 0：`side` 翻转，在 [`diff_positions`] 中属另一条 key（Closed+Created），
///   不会进入本合并。
///
/// `quantity` 在全系统恒为无符号绝对值（方向由 `side` 表达），故无需取绝对值。
fn is_add_to_position(local: &Position, remote: &Position) -> bool {
    remote.quantity > local.quantity && remote.pos_price != local.pos_price
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{InstrumentType, Side};
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    /// 交易所快照仓位（mapper 产物）：id=0、当前价已给。
    #[allow(clippy::too_many_arguments)]
    fn remote(
        account: u64,
        inst: &str,
        itype: InstrumentType,
        side: Side,
        qty: &str,
        pos_price: &str,
        current: &str,
        leverage: u64,
    ) -> Position {
        let mut p = Position::new(account, "binance", inst, side);
        p.instrument_type = itype;
        p.quantity = dec(qty);
        p.pos_price = dec(pos_price);
        p.current_price = dec(current);
        p.leverage = leverage;
        // 占用保证金兜底口径 = 成本/杠杆（mult=1），与 engine 无柜台 margin 时一致。
        p.margin = (dec(pos_price) * dec(qty)) / Decimal::from(leverage.max(1));
        p.exchange_update_time = 9000;
        p
    }

    /// 本地活跃仓位：带 id / 账簿字段。
    fn local(id: u64, account: u64, inst: &str, itype: InstrumentType, side: Side) -> Position {
        let mut p = Position::new(account, "binance", inst, side);
        p.id = id;
        p.instrument_type = itype;
        p.rule_id = 3;
        p.realized_pnl = dec("7");
        p.max_roi = dec("0.5");
        p.created_at = 111;
        p.updated_at = 222;
        p
    }

    fn only(diffs: Vec<PositionDiff>) -> PositionDiff {
        assert_eq!(diffs.len(), 1, "期望单条裁决，实际 {diffs:?}");
        diffs.into_iter().next().unwrap()
    }

    #[test]
    fn creates_position_when_remote_only() {
        let snap = remote(
            1,
            "BTCUSDT",
            InstrumentType::Perpetual,
            Side::Buy,
            "1",
            "100",
            "125",
            10,
        );
        let d = only(diff_positions(vec![snap], vec![], 42));
        let p = d.position();
        assert!(!p.deleted);
        assert_eq!(p.close_time, None);
        // 成本口径：roi=25*10/100=2.5（pnl/保证金10）。
        assert_eq!(p.pnl_value, dec("25"));
        assert_eq!(p.roi, dec("2.5"));
        assert_eq!(p.max_roi, dec("2.5"));
    }

    #[test]
    fn closes_position_when_local_only() {
        let existing = local(10, 1, "BTCUSDT", InstrumentType::Perpetual, Side::Buy);
        let d = only(diff_positions(vec![], vec![existing], 77));
        let p = d.position();
        assert!(matches!(d, PositionDiff::Closed(_)));
        assert!(p.deleted);
        assert_eq!(p.close_time, Some(77));
        assert_eq!(p.id, 10);
    }

    // ---- 本地重复 key 自愈：显式保留 keeper，其余软平仓（禁止 HashMap 静默吞行）----

    /// 同 key 两条本地仓位，远端仍在（与维护中的一条一致）：
    /// keeper=与远端数量/成本一致的行；另一条必须产出 Closed。
    #[test]
    fn duplicate_local_keeps_remote_matching_row_and_closes_others() {
        // Arrange：ru2701 幽灵 1417(1@19910) + 实际 1418(2@18820)，柜台就是 2@18820。
        let mut phantom = local(1417, 1, "ru2701", InstrumentType::Futures, Side::Buy);
        phantom.quantity = dec("1");
        phantom.pos_price = dec("19910");
        let mut keeper = local(1418, 1, "ru2701", InstrumentType::Futures, Side::Buy);
        keeper.quantity = dec("2");
        keeper.pos_price = dec("18820");
        let snap = remote(
            1,
            "ru2701",
            InstrumentType::Futures,
            Side::Buy,
            "2",
            "18820",
            "19700",
            1,
        );

        // Act
        let diffs = diff_positions(vec![snap], vec![phantom, keeper], 99);

        // Assert：一条 Updated(1418) + 一条 Closed(1417)，不得静默丢行。
        assert_eq!(
            diffs.len(),
            2,
            "重复 key 必须产出 Updated + Closed，实际 {diffs:?}"
        );
        let updated = diffs
            .iter()
            .find_map(|d| match d {
                PositionDiff::Updated(p) => Some(p),
                _ => None,
            })
            .expect("应保留与远端一致的行");
        assert_eq!(updated.id, 1418);
        assert_eq!(updated.quantity, dec("2"));
        let closed = diffs
            .iter()
            .find_map(|d| match d {
                PositionDiff::Closed(p) => Some(p),
                _ => None,
            })
            .expect("幽灵行必须被软平仓");
        assert_eq!(closed.id, 1417);
        assert!(closed.deleted);
        assert_eq!(closed.close_time, Some(99));
    }

    /// 同 key 两条本地仓位、远端已无该 key：两条都必须 Closed。
    #[test]
    fn duplicate_local_without_remote_closes_all() {
        let a = local(10, 1, "ETHUSDT", InstrumentType::Futures, Side::Buy);
        let b = local(20, 1, "ETHUSDT", InstrumentType::Futures, Side::Buy);

        let diffs = diff_positions(vec![], vec![a, b], 55);

        assert_eq!(diffs.len(), 2, "两条重复行都应被平仓，实际 {diffs:?}");
        let mut ids: Vec<u64> = diffs
            .iter()
            .map(|d| {
                assert!(matches!(d, PositionDiff::Closed(_)));
                d.position().id
            })
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![10, 20]);
    }

    /// 同 key 两条且远端数量/成本都已变化（两条都不匹配）：
    /// 确定性保留 id 最大的一条，其余 Closed，且 keeper 采用远端值。
    #[test]
    fn duplicate_local_remote_changed_falls_back_to_newest_id() {
        let mut older = local(10, 1, "BTCUSDT", InstrumentType::Futures, Side::Buy);
        older.quantity = dec("1");
        older.pos_price = dec("100");
        let mut newer = local(20, 1, "BTCUSDT", InstrumentType::Futures, Side::Buy);
        newer.quantity = dec("3");
        newer.pos_price = dec("200");
        let snap = remote(
            1,
            "BTCUSDT",
            InstrumentType::Futures,
            Side::Buy,
            "5",
            "300",
            "310",
            1,
        );

        let diffs = diff_positions(vec![snap], vec![older, newer], 1);

        assert_eq!(diffs.len(), 2, "实际 {diffs:?}");
        let updated = diffs
            .iter()
            .find_map(|d| match d {
                PositionDiff::Updated(p) => Some(p),
                _ => None,
            })
            .expect("应更新 keeper");
        assert_eq!(updated.id, 20, "无匹配时保留 id 最大者");
        assert_eq!(updated.quantity, dec("5"));
        assert_eq!(updated.pos_price, dec("300"));
        let closed = diffs
            .iter()
            .find_map(|d| match d {
                PositionDiff::Closed(p) => Some(p),
                _ => None,
            })
            .expect("另一条应 Closed");
        assert_eq!(closed.id, 10);
    }

    /// 两条数量/成本都匹配远端时，keeper 取 id 最大者（不依赖输入顺序）。
    #[test]
    fn duplicate_local_identical_rows_prefer_highest_id() {
        let mut a = local(10, 1, "X", InstrumentType::Futures, Side::Buy);
        a.quantity = dec("1");
        a.pos_price = dec("100");
        let mut b = local(20, 1, "X", InstrumentType::Futures, Side::Buy);
        b.quantity = dec("1");
        b.pos_price = dec("100");
        let snap = remote(
            1,
            "X",
            InstrumentType::Futures,
            Side::Buy,
            "1",
            "100",
            "110",
            1,
        );

        let diffs = diff_positions(vec![snap], vec![a, b], 7);

        let updated = diffs
            .iter()
            .find_map(|d| match d {
                PositionDiff::Updated(p) => Some(p),
                _ => None,
            })
            .expect("应更新 keeper");
        assert_eq!(updated.id, 20);
        let closed = diffs
            .iter()
            .find_map(|d| match d {
                PositionDiff::Closed(p) => Some(p),
                _ => None,
            })
            .expect("另一条应 Closed");
        assert_eq!(closed.id, 10);
    }

    #[test]
    fn find_duplicate_active_groups_reports_same_key_ids() {
        let a = local(10, 1, "ETHUSDT", InstrumentType::Futures, Side::Buy);
        let b = local(20, 1, "ETHUSDT", InstrumentType::Futures, Side::Buy);
        // 不同标的/账户不参与分组。
        let c = local(30, 1, "BTCUSDT", InstrumentType::Futures, Side::Buy);

        let groups = find_duplicate_active_groups(&[a, b, c]);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].account_id, 1);
        assert_eq!(groups[0].instrument, "ETHUSDT");
        assert_eq!(groups[0].side, Side::Buy);
        assert_eq!(groups[0].instrument_type, InstrumentType::Futures);
        assert_eq!(groups[0].ids, vec![10, 20], "id 需升序");
    }

    #[test]
    fn merge_overwrites_exchange_fields_and_preserves_bookkeeping() {
        let mut existing = local(10, 1, "ETHUSDT", InstrumentType::Perpetual, Side::Buy);
        existing.quantity = dec("1");
        existing.pos_price = dec("100");
        existing.leverage = 10;

        let snap = remote(
            1,
            "ETHUSDT",
            InstrumentType::Perpetual,
            Side::Buy,
            "2",
            "110",
            "120",
            20,
        );
        let d = only(diff_positions(vec![snap], vec![existing], 1));
        let p = d.position();

        // 交易所权威字段被覆盖。
        assert_eq!(p.quantity, dec("2"));
        assert_eq!(p.pos_price, dec("110"));
        assert_eq!(p.leverage, 20);
        assert_eq!(p.current_price, dec("120"));
        assert_eq!(p.exchange_update_time, 9000);
        // 本地账簿/风控字段保留。
        assert_eq!(p.id, 10);
        assert_eq!(p.rule_id, 3);
        assert_eq!(p.realized_pnl, dec("7"));
        assert_eq!(p.created_at, 111);
        // 成本口径：cost=110*2=220，pnl=20，roi=20*20/220=1.818182；
        // notional 仍按当前价=240；峰值取 max(旧 0.5, 1.818182)。
        assert_eq!(p.pnl_value, dec("20"));
        assert_eq!(p.notional, dec("240"));
        assert_eq!(p.roi, dec("1.818182"));
        assert_eq!(p.max_roi, dec("1.818182"));
    }

    #[test]
    fn peak_is_carried_forward_when_roi_below_prior_max() {
        // 旧峰值 0.5；成本口径本次 roi=10/100=0.1，峰值保留 0.5，并据此算回落。
        let mut existing = local(10, 1, "X", InstrumentType::Perpetual, Side::Buy);
        existing.quantity = dec("1");
        existing.pos_price = dec("100");
        existing.leverage = 1;
        let snap = remote(
            1,
            "X",
            InstrumentType::Perpetual,
            Side::Buy,
            "1",
            "100",
            "110",
            1,
        );
        let d = only(diff_positions(vec![snap], vec![existing], 1));
        let p = d.position();
        assert_eq!(p.roi, dec("0.1"));
        assert_eq!(p.max_roi, dec("0.5"));
        // 回落=0.5-0.1=0.4（保证金口径百分点）。
        assert_eq!(p.profit_drawdown, dec("0.4"));
    }

    // ---- 加仓重锚：成本基准变化后旧峰值失效 ----

    /// 同向加仓（量增 + 成本价变）且新 ROI 低于旧峰值时：峰值必须按加仓后
    /// 新成本基准重锚，回落归零，避免回落止盈被误触发。
    ///
    /// 本地：100 @0.8、3x、现价 1（旧 ROI 0.75、峰值 0.75）；
    /// 快照：加仓到 200、交易所摊薄成本 0.9、现价仍 1。
    /// 新 ROI = (1−0.9)×3/0.9 = 0.333333，重锚后峰值应 = 0.333333。
    #[test]
    fn add_to_position_reanchors_peak_to_new_cost_basis() {
        let mut existing = local(10, 1, "FILUSDT", InstrumentType::Perpetual, Side::Buy);
        existing.quantity = dec("100");
        existing.pos_price = dec("0.8");
        existing.leverage = 3;
        existing.current_price = dec("1");
        existing.max_roi = dec("0.75");

        let snap = remote(
            1,
            "FILUSDT",
            InstrumentType::Perpetual,
            Side::Buy,
            "200",
            "0.9",
            "1",
            3,
        );
        let d = only(diff_positions(vec![snap], vec![existing], 1));
        let p = d.position();

        // 新成本基准下 ROI≈0.333333；峰值重锚、回落归零（不再是 0.75 / 0.416667）。
        assert_eq!(p.roi, dec("0.333333"));
        assert_eq!(p.max_roi, dec("0.333333"));
        assert_eq!(p.profit_drawdown, dec("0"));
    }

    /// 减仓（量减、成本价不变）：峰值必须保留，不触发重锚。
    #[test]
    fn reduce_position_keeps_prior_peak() {
        let mut existing = local(10, 1, "FILUSDT", InstrumentType::Perpetual, Side::Buy);
        existing.quantity = dec("200");
        existing.pos_price = dec("0.8");
        existing.leverage = 3;
        existing.current_price = dec("1");
        existing.max_roi = dec("0.75");

        // 部分平仓：200 → 100，平均价仍 0.8，现价 1 → ROI 0.75。
        let snap = remote(
            1,
            "FILUSDT",
            InstrumentType::Perpetual,
            Side::Buy,
            "100",
            "0.8",
            "1",
            3,
        );
        let d = only(diff_positions(vec![snap], vec![existing], 1));
        let p = d.position();

        assert_eq!(p.roi, dec("0.75"));
        assert_eq!(p.max_roi, dec("0.75"), "减仓不重锚，峰值保留");
        assert_eq!(p.profit_drawdown, dec("0"));
    }

    /// 量增但成本价不变（谓词要求两者皆变）：不重锚，峰值保留。
    #[test]
    fn quantity_up_but_same_price_does_not_reanchor() {
        let mut existing = local(10, 1, "FILUSDT", InstrumentType::Perpetual, Side::Buy);
        existing.quantity = dec("100");
        existing.pos_price = dec("0.8");
        existing.leverage = 3;
        existing.current_price = dec("1");
        existing.max_roi = dec("0.75");

        // 量 100→200 但成本价仍报 0.8：不视为摊薄成本的加仓。
        let snap = remote(
            1,
            "FILUSDT",
            InstrumentType::Perpetual,
            Side::Buy,
            "200",
            "0.8",
            "1",
            3,
        );
        let d = only(diff_positions(vec![snap], vec![existing], 1));
        let p = d.position();

        assert_eq!(p.max_roi, dec("0.75"), "成本价未变，峰值保留");
    }

    #[test]
    fn key_distinguishes_side_and_instrument_type() {
        // 同标的、同账户：多空是两条独立仓位（本地多被平、远端空新增）。
        let local_long = local(1, 1, "BTCUSDT", InstrumentType::Perpetual, Side::Buy);
        let remote_short = remote(
            1,
            "BTCUSDT",
            InstrumentType::Perpetual,
            Side::Sell,
            "1",
            "100",
            "90",
            10,
        );
        let diffs = diff_positions(vec![remote_short], vec![local_long], 1);
        assert_eq!(diffs.len(), 2);
        assert!(diffs
            .iter()
            .any(|d| d.is_created() && d.position().side == Side::Sell));
        assert!(diffs
            .iter()
            .any(|d| matches!(d, PositionDiff::Closed(_)) && d.position().side == Side::Buy));

        // 同标的、同方向：instrument_type 不同也是两条。
        let spot = local(2, 1, "BTCUSDT", InstrumentType::Spot, Side::Buy);
        let perp_remote = remote(
            1,
            "BTCUSDT",
            InstrumentType::Perpetual,
            Side::Buy,
            "1",
            "100",
            "100",
            1,
        );
        assert_eq!(diff_positions(vec![perp_remote], vec![spot], 1).len(), 2);
    }

    #[test]
    fn both_empty_yields_no_diff() {
        assert!(diff_positions(vec![], vec![], 1).is_empty());
    }

    #[test]
    fn update_keeps_local_good_price_when_remote_price_is_zero() {
        // 本地已有有效价 125（10x、成本 100）；本轮远端快照缺价（current=0）。
        let mut existing = local(10, 1, "BTCUSDT", InstrumentType::Perpetual, Side::Buy);
        existing.quantity = dec("1");
        existing.pos_price = dec("100");
        existing.leverage = 10;
        existing.current_price = dec("125");

        // 远端 current=0（如 HL clearinghouse 不回价、本轮又无 allMids）。
        let snap = remote(
            1,
            "BTCUSDT",
            InstrumentType::Perpetual,
            Side::Buy,
            "1",
            "100",
            "0",
            10,
        );
        let d = only(diff_positions(vec![snap], vec![existing], 1));
        assert!(matches!(d, PositionDiff::Updated(_)));
        let p = d.position();
        // 好价不得被 0 冲掉；按保住的价重算：pnl=25、roi=2.5。
        assert_eq!(p.current_price, dec("125"));
        assert_eq!(p.pnl_value, dec("25"));
        assert_eq!(p.roi, dec("2.5"));
    }

    #[test]
    fn update_overwrites_when_remote_has_valid_price() {
        // 正常路径钉死：远端有价时照常覆盖本地价，不因防御逻辑而漏更新。
        let mut existing = local(10, 1, "BTCUSDT", InstrumentType::Perpetual, Side::Buy);
        existing.quantity = dec("1");
        existing.pos_price = dec("100");
        existing.leverage = 10;
        existing.current_price = dec("125");

        let snap = remote(
            1,
            "BTCUSDT",
            InstrumentType::Perpetual,
            Side::Buy,
            "1",
            "100",
            "130",
            10,
        );
        let d = only(diff_positions(vec![snap], vec![existing], 1));
        let p = d.position();
        assert_eq!(p.current_price, dec("130"));
        // pnl=30，roi=30*10/100=3。
        assert_eq!(p.roi, dec("3"));
    }
}
