//! 仓位仓储：在通用 [`Table<Position>`] 之上收口仓位的内存读写与持久化。
//!
//! * 业务侧（trading-service）不直接操作仓位表，统一经本仓储；落盘仍由
//!   TradingStore 的既有定时持久化机制负责，本仓储不另建内存结构/落盘线程；
//! * 所有变更都经 [`Table::update`] / [`Table::insert`]，行锁内完成，
//!   `updated_at` 由框架维护，金额字段全程 [`Decimal`] 定点；
//! * 对账裁决（[`PositionDiff`]）与重估纯函数在 trading-models，本仓储只负责
//!   把裁决/价格落到表上（IO 侧）。

use std::sync::Arc;

use store_core::{StoreError, Table};
use trading_models::{
    merge_remote_into_local, revalue_positions, InstrumentType, Position, PositionDiff,
    PositionPrice, ReconcileReport, Side,
};

/// 仓位仓储句柄，廉价克隆（内部为 Arc 表）。
#[derive(Clone)]
pub struct PositionRepo {
    table: Arc<Table<Position>>,
}

impl PositionRepo {
    /// 绑定一张已注册并完成恢复的仓位表。
    pub fn new(table: Arc<Table<Position>>) -> Self {
        Self { table }
    }

    /// 全部活跃仓位（deleted=0）。
    pub fn list_active(&self) -> Vec<Position> {
        self.table.list()
    }

    /// 按对账作用域取活跃仓位：同一账户 + 同一交易所 + instrument_type 命中给定集合。
    ///
    /// 交易所仓位接口只覆盖其产品口径（如币安合约 v2 仅返回 U 本位永续），
    /// 现货余额等走另一套端点；故对账传入「本次快照覆盖的类型集」，避免把
    /// 作用域外（其它产品）的本地仓位误判为「远端无 → 软平仓」。
    pub fn list_active_scope(
        &self,
        account_id: u64,
        exchange: &str,
        instrument_types: &[InstrumentType],
    ) -> Vec<Position> {
        self.table
            .list()
            .into_iter()
            .filter(|p| {
                p.account_id == account_id
                    && p.exchange.eq_ignore_ascii_case(exchange)
                    && instrument_types.contains(&p.instrument_type)
            })
            .collect()
    }

    /// 按「账户 + 交易所 + 合约形状 + 标的 + 方向」精确定位活跃仓位（deleted=0）。
    ///
    /// 创建 risk 规则（及后续 REST 接口）的绑定前置：找不到即不得创建规则。
    /// 单向持仓一行；若历史上存在多/空双向行，返回 id 最小者（确定性行为）。
    pub fn find_active(
        &self,
        account_id: u64,
        exchange: &str,
        instrument_type: InstrumentType,
        instrument: &str,
        side: Side,
    ) -> Option<Position> {
        Self::pick(
            self.table.list(),
            account_id,
            exchange,
            instrument_type,
            instrument,
            side,
            false,
        )
    }

    /// 按条件匹配仓位，**含已软删（tombstone）行**；命中多条时取 **id 最大**
    /// （id 单调递增，即最近一次写入的该仓位）的一条。
    ///
    /// 与 [`Self::find_active`] 的唯一差别：数据源包含 deleted=1、取 id 最大而非最小。
    /// 供平仓通知使用——即便仓位在通知前已被软删，仍能取到对应行，盈亏按本次
    /// 平仓成交均价现算，避免「先软删 → 通知拿不到仓位」的时序不稳定。
    pub fn find_latest_including_deleted(
        &self,
        account_id: u64,
        exchange: &str,
        instrument_type: InstrumentType,
        instrument: &str,
        side: Side,
    ) -> Option<Position> {
        Self::pick(
            self.table.list_including_deleted(),
            account_id,
            exchange,
            instrument_type,
            instrument,
            side,
            true,
        )
    }

    /// 在给定候选仓位中，按「账户 + 交易所（忽略大小写）+ 合约形状 + 标的 + 方向」
    /// 过滤；两个公开查询方法共用。`latest=false` 取 id 最小（活跃查询），
    /// `true` 取 id 最大（含 tombstone 的最近一条）。
    fn pick(
        candidates: Vec<Position>,
        account_id: u64,
        exchange: &str,
        instrument_type: InstrumentType,
        instrument: &str,
        side: Side,
        latest: bool,
    ) -> Option<Position> {
        let matched = candidates.into_iter().filter(|p| {
            p.account_id == account_id
                && p.exchange.eq_ignore_ascii_case(exchange)
                && p.instrument_type == instrument_type
                && p.instrument == instrument
                && p.side == side
        });
        if latest {
            matched.max_by_key(|p| p.id)
        } else {
            matched.min_by_key(|p| p.id)
        }
    }

    /// 按主键取活跃仓位（deleted=0）；不存在或已软平仓返回 None。
    ///
    /// create_rule(risk) 按策略回传的 position_id 绑定仓位时使用；账户归属由
    /// 调用方（服务编排）用返回行的 `account_id` 再校验，仓储层不感知账户上下文。
    pub fn get_active(&self, id: u64) -> Option<Position> {
        self.table.get(id).ok().flatten()
    }

    /// 判断「账户 + 交易所 + 合约形状 + 标的」是否存在任意方向的活跃仓位（deleted=0）。
    ///
    /// 跨标的条件（如 `position_FILUSDT == 0` 绑定 BTC 仓）创建期的存在性校验；
    /// 运行期该标的被平掉后，指标按 0 处理，规则才可能命中。
    pub fn exists_active_instrument(
        &self,
        account_id: u64,
        exchange: &str,
        instrument_type: InstrumentType,
        instrument: &str,
    ) -> bool {
        self.table.list().iter().any(|p| {
            p.account_id == account_id
                && p.exchange.eq_ignore_ascii_case(exchange)
                && p.instrument_type == instrument_type
                && p.instrument == instrument
        })
    }

    /// 按「账户 + 交易所 + 合约形状 + 标的」取任意方向的活跃仓位（deleted=0）。
    ///
    /// 与 [`Self::exists_active_instrument`] 同一谓词、返回整行：跨标的 ROI/盈利
    /// 条件用本 rule 的账户取目标标的仓位时使用。多/空多行时返回 id 最小者。
    pub fn find_active_instrument(
        &self,
        account_id: u64,
        exchange: &str,
        instrument_type: InstrumentType,
        instrument: &str,
    ) -> Option<Position> {
        self.table
            .list()
            .into_iter()
            .filter(|p| {
                p.account_id == account_id
                    && p.exchange.eq_ignore_ascii_case(exchange)
                    && p.instrument_type == instrument_type
                    && p.instrument == instrument
            })
            .min_by_key(|p| p.id)
    }

    /// 把对账裁决写入内存仓位表（落盘由 TradingStore 统一负责）。
    ///
    /// - [`PositionDiff::Created`] → [`Table::insert`]（id=0 自动分配）；
    /// - [`PositionDiff::Updated`] → [`Table::update`] 整体替换（保留 created_at）；
    /// - [`PositionDiff::Closed`] → 单次 [`Table::update`] 置 deleted=1 + close_time。
    pub fn apply_diffs(&self, diffs: Vec<PositionDiff>) -> Result<ReconcileReport, StoreError> {
        let mut report = ReconcileReport::default();
        for diff in diffs {
            match diff {
                PositionDiff::Created(p) => {
                    self.table.insert(p)?;
                    report.created += 1;
                }
                PositionDiff::Updated(p) => {
                    let id = p.id;
                    self.table.update(id, |row| *row = p)?;
                    report.updated += 1;
                }
                PositionDiff::Closed(p) => {
                    let close_time = p.close_time;
                    self.table.update(p.id, |row| {
                        row.deleted = true;
                        row.close_time = close_time;
                    })?;
                    report.closed += 1;
                }
            }
        }
        Ok(report)
    }

    /// 批量行情重估：对一组最新价逐条调用 [`Self::revalue_by_price`]，
    /// 返回累计实际更新行数。每条价格只刷新其交易所/类型/标的匹配的仓位。
    pub fn revalue_by_prices(&self, prices: &[PositionPrice]) -> usize {
        prices.iter().map(|p| self.revalue_by_price(p)).sum()
    }

    /// 用一份仓位列表 reconcile 内存：按键
    /// （账户 + 交易所 + 合约形状 + 标的 + 方向）匹配活跃行——
    /// * 已存在：整体替换为入参行，**保留原 id 与 created_at**（原地更新）；
    /// * 不存在：作为新行插入（id=0 自动分配）。
    ///
    /// 与 [`Self::apply_diffs`] 的区别：调用方无需先取本地基线、无需自己跑裁决，
    /// 适用于“拿到一批仓位快照直接覆盖写”的场景；不处理平仓（不在这里的行不软删）。
    pub fn upsert_matching(&self, positions: Vec<Position>) -> Result<ReconcileReport, StoreError> {
        let mut report = ReconcileReport::default();
        for incoming in positions {
            // table.list() 只返回活跃（deleted=0）行，故无需再判 deleted。
            let matched = self.table.list().into_iter().find(|p| {
                p.account_id == incoming.account_id
                    && p.exchange.eq_ignore_ascii_case(&incoming.exchange)
                    && p.instrument_type == incoming.instrument_type
                    && p.instrument == incoming.instrument
                    && p.side == incoming.side
            });
            if let Some(existing) = &matched {
                let id = existing.id;
                // 复用与对账一致的合并口径：交易所快照覆盖变动字段，本地权威字段
                // （rule_id/realized_pnl/max_roi、id/created_at 等）保留，再按现价重估。
                let merged = merge_remote_into_local(existing, &incoming);
                let price = merged.current_price;
                let merged = merged.revalued(price);
                self.table.update(id, |row| *row = merged)?;
                report.updated += 1;
            } else {
                self.table.insert(incoming)?;
                report.created += 1;
            }
        }
        Ok(report)
    }

    /// 用一条最新价重估并落库所有匹配仓位（exchange + instrument_type + instrument），
    /// 返回实际更新行数。行情驱动：只写内存，不落盘额外动作（随定时持久化落盘）。
    pub fn revalue_by_price(&self, price: &PositionPrice) -> usize {
        // 先在表外做纯函数重估（入参为活跃行快照），再逐行回写；行锁内只做赋值。
        let updated = revalue_positions(price, self.table.list());
        let n = updated.len();
        for p in updated {
            // 更新瞬间以行内现值为基准再重估一次，缩小与对账流并发写同一行的竞争窗口。
            let id = p.id;
            let price = p.current_price;
            if let Err(e) = self.table.update(id, |row| *row = row.revalued(price)) {
                tracing::warn!(error = %e, id, "仓位行情重估回写失败，跳过本行");
            }
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use std::str::FromStr;
    use std::sync::Arc;
    use store_core::{Store, SystemClock, Table};
    use trading_models::{diff_positions, InstrumentType, Position, PositionPrice, Side};

    use super::PositionRepo;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    /// 独立内存 Store 注册仓位表（不触盘）。
    fn repo() -> PositionRepo {
        let store = Store::new(Arc::new(SystemClock));
        let table = store.register::<Position>().unwrap();
        PositionRepo::new(table)
    }

    fn remote(account: u64, inst: &str, side: Side, qty: &str, current: &str) -> Position {
        let mut p = Position::new(account, "binance", inst, side);
        p.instrument_type = InstrumentType::Futures;
        p.quantity = dec(qty);
        p.pos_price = dec("100");
        p.current_price = dec(current);
        p.leverage = 10;
        // 占用保证金兜底 = cost/lev = 100×qty/10（生产由快照填充）。
        p.margin = (dec("100") * dec(qty)) / Decimal::from(10);
        p
    }

    #[test]
    fn apply_diffs_creates_updates_and_closes() {
        let repo = repo();

        // 新建：id 自动分配、活跃可见。
        let diffs = diff_positions(vec![remote(1, "BTCUSDT", Side::Buy, "1", "125")], vec![], 1);
        let report = repo.apply_diffs(diffs).unwrap();
        assert_eq!(report.created, 1);
        assert_eq!(repo.list_active().len(), 1);
        let stored_id = repo.list_active()[0].id;
        assert!(stored_id > 0);
        assert_eq!(repo.list_active()[0].roi, dec("2.5"));

        // 更新：同键再来快照，数量改变、id 保留。
        let existing = repo.list_active();
        let diffs = diff_positions(
            vec![remote(1, "BTCUSDT", Side::Buy, "3", "130")],
            existing,
            2,
        );
        let report = repo.apply_diffs(diffs).unwrap();
        assert_eq!(report.updated, 1);
        assert_eq!(repo.list_active()[0].id, stored_id);
        assert_eq!(repo.list_active()[0].quantity, dec("3"));

        // 平仓：作用域内无远端行 → 软删，活跃不可见、tombstone 保留。
        let diffs = diff_positions(vec![], repo.list_active(), 99);
        let report = repo.apply_diffs(diffs).unwrap();
        assert_eq!(report.closed, 1);
        assert!(repo.list_active().is_empty());
    }

    #[test]
    fn get_active_returns_row_by_id_and_hides_closed() {
        let repo = repo();
        let diffs = diff_positions(vec![remote(1, "BTCUSDT", Side::Buy, "2", "125")], vec![], 1);
        repo.apply_diffs(diffs).unwrap();
        let id = repo.list_active()[0].id;

        // 按主键取到活跃仓位。
        let got = repo.get_active(id).expect("活跃仓位应可按 id 取到");
        assert_eq!(got.id, id);
        assert_eq!(got.instrument, "BTCUSDT");
        assert_eq!(got.side, Side::Buy);

        // 不存在的 id 返回 None。
        assert!(repo.get_active(999_999).is_none());

        // 软平仓后按 id 不可见（tombstone 不算活跃）。
        let diffs = diff_positions(vec![], repo.list_active(), 99);
        repo.apply_diffs(diffs).unwrap();
        assert!(repo.get_active(id).is_none());
    }

    #[test]
    fn find_active_instrument_returns_row_and_hides_closed() {
        let repo = repo();
        // 账户1 binance 合约 BTCUSDT 多仓。
        let diffs = diff_positions(vec![remote(1, "BTCUSDT", Side::Buy, "2", "125")], vec![], 1);
        repo.apply_diffs(diffs).unwrap();

        // 按 account+exchange+itype+symbol 取到整行（任意方向）。
        let got = repo
            .find_active_instrument(1, "binance", InstrumentType::Futures, "BTCUSDT")
            .expect("活跃仓位应可取到");
        assert_eq!(got.instrument, "BTCUSDT");
        assert_eq!(got.side, Side::Buy);

        // 交易所大小写不敏感、跨账户/标的不匹配返回 None。
        assert!(repo
            .find_active_instrument(1, "BINANCE", InstrumentType::Futures, "BTCUSDT")
            .is_some());
        assert!(repo
            .find_active_instrument(2, "binance", InstrumentType::Futures, "BTCUSDT")
            .is_none());
        assert!(repo
            .find_active_instrument(1, "binance", InstrumentType::Futures, "ETHUSDT")
            .is_none());

        // 软平仓后取不到。
        let diffs = diff_positions(vec![], repo.list_active(), 99);
        repo.apply_diffs(diffs).unwrap();
        assert!(repo
            .find_active_instrument(1, "binance", InstrumentType::Futures, "BTCUSDT")
            .is_none());
    }

    #[test]
    fn list_active_scope_filters_account_exchange_and_type() {
        let repo = repo();
        // 账户1 binance 合约（作用域内）。
        let in_scope = remote(1, "BTCUSDT", Side::Buy, "1", "100");
        // 账户2 binance 合约（不同账户，排除）。
        let other_account = remote(2, "BTCUSDT", Side::Buy, "1", "100");
        // 账户1 xili 合约（不同交易所，排除）。
        let mut other_exchange = remote(1, "BTCUSDT", Side::Buy, "1", "100");
        other_exchange.exchange = "xili".into();
        // 账户1 binance 现货（不同 instrument_type，排除 —— 合约快照不得软删现货）。
        let mut spot = remote(1, "BTCUSDT", Side::Buy, "1", "100");
        spot.instrument_type = InstrumentType::Spot;

        for p in [in_scope, other_account, other_exchange, spot] {
            repo.apply_diffs(diff_positions(vec![p], vec![], 1))
                .unwrap();
        }

        let scoped = repo.list_active_scope(1, "binance", &[InstrumentType::Futures]);
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].account_id, 1);
        assert_eq!(scoped[0].exchange, "binance");
        assert_eq!(scoped[0].instrument_type, InstrumentType::Futures);
    }

    #[test]
    fn revalue_by_price_updates_only_matching_rows() {
        let repo = repo();
        // binance 合约 BTC 多仓 + xili 合约 BTC 仓。
        repo.apply_diffs(diff_positions(
            vec![remote(1, "BTCUSDT", Side::Buy, "1", "100"), {
                let mut p = remote(1, "BTCUSDT", Side::Buy, "1", "100");
                p.exchange = "xili".into();
                p
            }],
            vec![],
            1,
        ))
        .unwrap();

        let price = PositionPrice::new("binance", InstrumentType::Futures, "BTCUSDT", dec("125"));
        let n = repo.revalue_by_price(&price);
        assert_eq!(n, 1, "只刷新 binance 一行");

        let rows = repo.list_active();
        let b = rows.iter().find(|p| p.exchange == "binance").unwrap();
        let x = rows.iter().find(|p| p.exchange == "xili").unwrap();
        assert_eq!(b.current_price, dec("125"));
        assert_eq!(b.roi, dec("2.5"));
        // xili 行不被触碰。
        assert_eq!(x.current_price, dec("100"));
        assert_eq!(x.roi, dec("0"));
    }

    #[test]
    fn revalue_by_prices_updates_each_scope_and_counts_rows() {
        let repo = repo();
        repo.apply_diffs(diff_positions(
            vec![remote(1, "BTCUSDT", Side::Buy, "1", "100"), {
                let mut p = remote(1, "ETHUSDT", Side::Buy, "2", "100");
                p.exchange = "xili".into();
                p
            }],
            vec![],
            1,
        ))
        .unwrap();

        // 一次传两条最新价：分别只刷各自交易所仓位，返回总更新行数。
        let n = repo.revalue_by_prices(&[
            PositionPrice::new("binance", InstrumentType::Futures, "BTCUSDT", dec("125")),
            PositionPrice::new("xili", InstrumentType::Futures, "ETHUSDT", dec("150")),
        ]);
        assert_eq!(n, 2);
        let rows = repo.list_active();
        let btc = rows.iter().find(|p| p.instrument == "BTCUSDT").unwrap();
        let eth = rows.iter().find(|p| p.instrument == "ETHUSDT").unwrap();
        assert_eq!(btc.current_price, dec("125"));
        assert_eq!(eth.current_price, dec("150"));

        // 不匹配任何仓位的价格不计数。
        let n = repo.revalue_by_prices(&[PositionPrice::new(
            "okx",
            InstrumentType::Futures,
            "BTCUSDT",
            dec("999"),
        )]);
        assert_eq!(n, 0);
    }

    #[test]
    fn upsert_matching_inserts_new_and_updates_same_key_in_place() {
        let repo = repo();

        // 首次：内存无同 (账户+所+类型+标的+方向) 行 → 新增。
        let btc = remote(1, "BTCUSDT", Side::Buy, "1", "100");
        let report = repo.upsert_matching(vec![btc]).unwrap();
        assert_eq!(report.created, 1);
        assert_eq!(report.updated, 0);
        let stored_id = repo.list_active()[0].id;

        // 同键再来（数量变化）→ 原地更新、id 保留、不新增行。
        let changed = remote(1, "BTCUSDT", Side::Buy, "3", "130");
        let report = repo.upsert_matching(vec![changed]).unwrap();
        assert_eq!(report.created, 0);
        assert_eq!(report.updated, 1);
        let rows = repo.list_active();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, stored_id);
        assert_eq!(rows[0].quantity, dec("3"));

        // 不同方向 → 视为另一行，新增。
        let short = remote(1, "BTCUSDT", Side::Sell, "1", "100");
        let report = repo.upsert_matching(vec![short]).unwrap();
        assert_eq!(report.created, 1);
        assert_eq!(repo.list_active().len(), 2);
    }

    #[test]
    fn upsert_matching_preserves_local_bookkeeping_fields() {
        let repo = repo();
        // 本地行带有本地权威字段：rule_id 与盈利峰值 max_roi。
        let mut local = remote(1, "BTCUSDT", Side::Buy, "1", "100");
        local.rule_id = 99;
        local.max_roi = dec("0.5");
        repo.upsert_matching(vec![local]).unwrap();
        let id = repo.list_active()[0].id;

        // 同键快照（远端不带本地字段：rule_id=0、max_roi=0）覆盖写；价格取成本价，
        // 使本轮重估 roi=0，从而能干净地验证峰值不被重置。
        let snapshot = remote(1, "BTCUSDT", Side::Buy, "2", "100");
        repo.upsert_matching(vec![snapshot]).unwrap();

        // 交易所字段被刷新（数量=2），但本地账簿/风控字段必须保留。
        let row = repo.list_active().into_iter().find(|p| p.id == id).unwrap();
        assert_eq!(row.quantity, dec("2"));
        assert_eq!(row.rule_id, 99, "rule_id 属本地权威，不得被快照清零");
        assert_eq!(
            row.max_roi,
            dec("0.5"),
            "max_roi 盈利峰值须延续，不被快照清零"
        );
        assert_eq!(row.id, id);
    }

    /// 测试用 store + 仓位表（与 repo() 同源，便于直接 insert 含 tombstone 的行）。
    fn store_table() -> (Arc<Store>, Arc<Table<Position>>) {
        let store = Store::new(Arc::new(SystemClock));
        let table = store.register::<Position>().unwrap();
        (store, table)
    }

    /// 直接向表插入一条仓位（可指定 deleted / created_at），返回其 id。
    fn seed(
        table: &Table<Position>,
        inst: &str,
        side: Side,
        deleted: bool,
        created_at: u64,
    ) -> u64 {
        let mut p = Position::new(7, "binance", inst, side);
        p.instrument_type = InstrumentType::Futures;
        p.quantity = dec("2");
        p.pos_price = dec("100");
        p.deleted = deleted;
        p.created_at = created_at;
        table.insert(p).unwrap().id
    }

    #[test]
    fn find_latest_including_deleted_returns_newest_matching_even_if_soft_deleted() {
        let (_store, table) = store_table();
        let repo = PositionRepo::new(Arc::clone(&table));
        // 同键两条：一条活跃(较早)、一条已删但 created_at 更晚。
        let id_older = seed(&table, "BTCUSDT", Side::Sell, false, 100);
        let id_newer = seed(&table, "BTCUSDT", Side::Sell, true, 200);

        // 新方法：含已删，取 created_at 最晚 → 已删那条。
        let got = repo
            .find_latest_including_deleted(
                7,
                "binance",
                InstrumentType::Futures,
                "BTCUSDT",
                Side::Sell,
            )
            .expect("应匹配到已删仓位");
        assert_eq!(got.id, id_newer);
        assert_ne!(got.id, id_older);

        // 对照：find_active 只返回活跃(较早)那条。
        let active = repo.find_active(7, "binance", InstrumentType::Futures, "BTCUSDT", Side::Sell);
        assert_eq!(active.unwrap().id, id_older);
    }

    #[test]
    fn find_latest_including_deleted_when_only_deleted_exists() {
        let (_store, table) = store_table();
        let repo = PositionRepo::new(Arc::clone(&table));
        let id = seed(&table, "BTCUSDT", Side::Sell, true, 50);

        // 仅有一条已删行：新方法仍取到，find_active 取不到。
        let got = repo
            .find_latest_including_deleted(
                7,
                "binance",
                InstrumentType::Futures,
                "BTCUSDT",
                Side::Sell,
            )
            .unwrap();
        assert_eq!(got.id, id);
        assert!(repo
            .find_active(7, "binance", InstrumentType::Futures, "BTCUSDT", Side::Sell)
            .is_none());
    }

    #[test]
    fn find_latest_including_deleted_no_match_returns_none() {
        let (_store, table) = store_table();
        let repo = PositionRepo::new(Arc::clone(&table));
        seed(&table, "BTCUSDT", Side::Sell, false, 1);
        // side 不匹配 → None。
        assert!(repo
            .find_latest_including_deleted(
                7,
                "binance",
                InstrumentType::Futures,
                "BTCUSDT",
                Side::Buy
            )
            .is_none());
    }
}
