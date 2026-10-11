//! 仓位编排引擎（trading-service 侧唯一的仓位流程编排点）。
//!
//! 自身不含计算与存储：交易所口径快照 [`PositionInfo`] → 领域 [`Position`] 的
//! 映射（各所差异收口）在此；对账裁决（`diff_positions`）在 trading-models，
//! 表读写在 trading-store 的 [`PositionRepo`]。两条入口：
//!
//! * [`PositionSnapshotSink`]（账户事件去抖到点，适配器拉完 REST 回调）：
//!   映射快照 → 取本账户/本所/本产品作用域的本地活跃仓位 → 裁决 → 落库；
//! * [`QuoteObserver`]（公共行情写价后同步回调）：只把对应交易所/产品/标的的
//!   最新价交给仓储做批量重估，绝不触碰其它交易所仓位。

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use trading_core::quote::QuoteTable;
use trading_exchange::api::{OrderPositionSide, PositionInfo};
use trading_exchange::port::{PositionSnapshotSink, QuoteObserver};
use trading_exchange::PositionProfile;
use trading_models::{
    diff_positions, Instrument, InstrumentType, MarketKind, Position, PositionPrice, QtyUnit, Side,
    Venue,
};
use trading_store::{LeverageKey, LeverageRepo, PositionRepo};

/// 行情入站键 `(quote_venue, quote_kind)`：on_quote 据此反查到所属档案。
type QuoteKey = (Venue, MarketKind);

/// 仓位档案的**只读索引**：由 [`PositionProfile`] 列表在启动前一次性构建。
///
/// 两套查询各取所需：
/// * 按 `exchange` 取档案（对账作用域、快照兜底估值标的）；
/// * 按行情入站键 `(venue, kind)` 反查档案（`on_quote` 路由到对应交易所）。
///
/// 内部用单个 [`OnceLock`] 同时承载两张不可变 HashMap：[`PositionEngine`] 克隆廉价
/// （整结构仍可 `derive(Clone)`），所有克隆共享同一份装配结果；装配在
/// [`PositionEngine::load_and_verify`] 成功时一次性完成，此后运行期只读。
/// 装配前查询一律 miss（与「未接入」回退等价）。
#[derive(Default)]
struct ProfileIndex {
    maps: OnceLock<ProfileMaps>,
}

/// 装配后两张只读索引表（一次性整体装入 [`OnceLock`]）。
struct ProfileMaps {
    by_exchange: HashMap<&'static str, PositionProfile>,
    by_quote: HashMap<QuoteKey, PositionProfile>,
}

impl ProfileIndex {
    /// 由档案列表构建两张索引表。调用方已保证列表非空且 `exchange` 无重复。
    fn build_maps(profiles: &[PositionProfile]) -> ProfileMaps {
        ProfileMaps {
            by_exchange: profiles.iter().map(|p| (p.exchange, *p)).collect(),
            by_quote: profiles
                .iter()
                .map(|p| ((p.quote_venue, p.quote_kind), *p))
                .collect(),
        }
    }

    /// 按交易所规范名取档案。
    fn exchange(&self, name: &str) -> Option<PositionProfile> {
        self.maps.get()?.by_exchange.get(name).copied()
    }

    /// 按行情入站键 `(venue, kind)` 反查档案。
    fn quote(&self, venue: Venue, kind: MarketKind) -> Option<PositionProfile> {
        self.maps.get()?.by_quote.get(&(venue, kind)).copied()
    }
}

/// 账户名 → 账户表行 id 的解析端口（按交易所匹配）。
/// 抽成小端口便于编排单测用内存假实现，无需落 account.csv。
/// 生产实现见服务层 [`crate::account_hub::AccountHub`]（委托 store 账户管理器）。
pub trait AccountLookup: Send + Sync {
    fn account_id(&self, exchange: &str, account: &str) -> Option<u64>;
}

/// 仓位流程编排引擎（廉价克隆，内部全 Arc 句柄）。
#[derive(Clone)]
pub struct PositionEngine {
    positions: PositionRepo,
    quotes: Arc<QuoteTable>,
    accounts: Arc<dyn AccountLookup>,
    /// 杠杆缓存：对账快照写入，下单前据此按需设置。
    leverages: LeverageRepo,
    /// 仓位档案只读索引：启动 `load_and_verify` 成功后装配，运行期只读。
    index: Arc<ProfileIndex>,
}

impl PositionEngine {
    pub fn new(
        positions: PositionRepo,
        quotes: Arc<QuoteTable>,
        accounts: Arc<dyn AccountLookup>,
        leverages: LeverageRepo,
    ) -> Self {
        Self {
            positions,
            quotes,
            accounts,
            leverages,
            index: Arc::new(ProfileIndex::default()),
        }
    }

    /// 装入各交易所自描述的仓位档案并自校验，成功后装配只读索引。
    ///
    /// 启动装配期（服务开始监听前）调用一次。校验规则，任一不满足即 [`Err`]
    /// （装配层 `.expect(...)` fail-fast，拒绝带病启动）：
    /// * `profiles` 非空（至少一家交易所接入仓位流水线）；
    /// * `exchange` 无重复（同所两份档案会让对账作用域/计数不确定）；
    /// * 档案条数与去重后条数一致（=无重复，全部成功建索引）。
    pub fn load_and_verify(&self, profiles: &[PositionProfile]) -> Result<(), String> {
        if profiles.is_empty() {
            return Err("仓位档案为空：至少需要一家交易所接入仓位流水线".to_string());
        }
        let unique: HashSet<&str> = profiles.iter().map(|p| p.exchange).collect();
        if unique.len() != profiles.len() {
            return Err(format!(
                "仓位档案存在重复 exchange：装入 {} 条但唯一 exchange 仅 {} 个",
                profiles.len(),
                unique.len()
            ));
        }
        // 校验通过后一次性装入两张不可变索引表；锁若已被装配（重复调用）视为失败。
        self.index
            .maps
            .set(ProfileIndex::build_maps(profiles))
            .map_err(|_| "仓位档案已装配：load_and_verify 不允许重复调用".to_string())
    }

    /// 某交易所仓位快照覆盖的产品类型集（决定本地对账作用域）。
    /// 查到档案 → `vec![instrument_type]`；未接入所 → 空 Vec（与历史回退一致）。
    fn snapshot_types(&self, exchange: &str) -> Vec<InstrumentType> {
        self.index
            .exchange(exchange)
            .map(|p| vec![p.instrument_type])
            .unwrap_or_default()
    }

    /// 快照无 mark 时的兜底估值价：取该所档案的 `fallback_valuation`，
    /// 用 `(venue, kind, symbol)` 查行情表（mark>mid>last）。
    /// fallback 为 None（deribit）或查无行情 → None（与历史行为一致）。
    fn quote_price(&self, exchange: &str, symbol: &str) -> Option<Decimal> {
        let (venue, kind) = self.index.exchange(exchange)?.fallback_valuation?;
        let inst = Instrument::new(venue, kind, symbol);
        self.quotes.valuation_price(&inst)
    }

    /// 实时查询映射（不落库）：把交易所 `PositionInfo` 列表经同一套 mapper
    /// （方向/数量/估值价/保证金口径与对账一致）转成领域 [`Position`]。
    ///
    /// 供 `POST /exchange/positions`：handler 负责 connect+拉取，此处只映射，
    /// 不读/写本地仓位表。
    pub fn map_exchange_positions(
        &self,
        account_id: u64,
        exchange: &str,
        infos: &[PositionInfo],
        now_ms: u64,
    ) -> Vec<Position> {
        let price_of = |symbol: &str| self.quote_price(exchange, symbol);
        snapshot_positions(account_id, exchange, infos, &price_of, now_ms)
    }

    /// 执行一次对账：映射交易所快照 → 作用域本地仓位 → 裁决 → 落库。
    /// 账户解析失败（已被软删/未知）直接跳过，不产生任何写入。
    fn reconcile(&self, exchange: &str, account: &str, infos: &[PositionInfo], now_ms: u64) {
        let Some(account_id) = self.accounts.account_id(exchange, account) else {
            tracing::warn!(exchange, account, "仓位对账：账户不可解析，跳过本次快照");
            return;
        };
        // 作用域由「交易所覆盖的产品类型」决定，而非快照是否非空：币安合约
        // get_positions 会滤掉 0 量行，账户全平时返回空 Ok 快照，此时仍须把
        // 作用域内活跃仓位软删（远端确实无仓）。现货余额走另一套端点，不在此类型集，
        // 故不会被误删。
        let types = self.snapshot_types(exchange);

        let price_of = |symbol: &str| self.quote_price(exchange, symbol);
        let snapshot = snapshot_positions(account_id, exchange, infos, &price_of, now_ms);

        // 把交易所快照里的杠杆写入缓存（供下单前按需设置，零额外查询）。
        // 仅写快照出现的标的；空快照（全平）不清缓存——平仓后杠杆配置不变。
        for info in infos {
            if let Some(lev) = info.leverage {
                self.leverages.put(
                    &LeverageKey::new(account_id, exchange, &info.instrument),
                    lev as u32,
                );
            }
        }

        let local = self
            .positions
            .list_active_scope(account_id, exchange, &types);
        let diffs = diff_positions(snapshot, local, now_ms);
        if diffs.is_empty() {
            return;
        }
        match self.positions.apply_diffs(diffs) {
            Ok(report) => tracing::info!(
                exchange,
                account,
                created = report.created,
                updated = report.updated,
                closed = report.closed,
                "仓位对账落库完成"
            ),
            Err(e) => tracing::warn!(exchange, account, error = %e, "仓位对账落库失败"),
        }
    }
}

#[async_trait]
impl PositionSnapshotSink for PositionEngine {
    async fn on_position_snapshot(
        &self,
        exchange: &str,
        account: &str,
        positions: &[PositionInfo],
    ) {
        // 纯内存 + 同步 Table 写，直接在当前任务执行；REST 延迟已在适配器侧 spawn 隔离。
        // exchange 已是全局精确名（无别名），直接作为存储/对账口径。
        let now_ms = Utc::now().timestamp_millis().max(0) as u64;
        self.reconcile(exchange, account, positions, now_ms);
    }
}

impl QuoteObserver for PositionEngine {
    fn on_quote(&self, venue: Venue, kind: MarketKind, symbol: &str, price: Decimal) {
        // 用行情入站键反查档案：命中才重估对应交易所/产品的仓位；
        // 未接入流水线的 venue/kind（现货等）查不到 → 直接忽略（与旧逻辑一致）。
        let Some(profile) = self.index.quote(venue, kind) else {
            return;
        };
        let pp = PositionPrice::new(profile.exchange, profile.instrument_type, symbol, price);
        self.positions.revalue_by_price(&pp);
    }
}

// =====================================================================
// 交易所快照 PositionInfo -> 领域 Position 映射（各所差异收口层）
// =====================================================================

/// 交易所持仓快照（[`PositionInfo`]）→ 领域仓位（[`Position`]）列表。
///
/// 方向（hedge LONG/SHORT、BOTH 按符号）、带符号量取绝对值、估值价（交易所
/// 标记价优先，缺失再查宿主内存行情）、杠杆/回报时间、逐仓保证金都在此归一；
/// 产出即可直接喂 `diff_positions` 做交易所无关的三方对比。
fn snapshot_positions<F>(
    account_id: u64,
    exchange: &str,
    infos: &[PositionInfo],
    price_of: &F,
    now_ms: u64,
) -> Vec<Position>
where
    F: Fn(&str) -> Option<Decimal>,
{
    infos
        .iter()
        .map(|info| snapshot_one(account_id, exchange, info, price_of, now_ms))
        .collect()
}

fn snapshot_one<F>(
    account_id: u64,
    exchange: &str,
    info: &PositionInfo,
    price_of: &F,
    now_ms: u64,
) -> Position
where
    F: Fn(&str) -> Option<Decimal>,
{
    let side = match info.position_side {
        OrderPositionSide::Long => Side::Buy,
        OrderPositionSide::Short => Side::Sell,
        OrderPositionSide::Both => {
            if info.signed_quantity.is_sign_negative() {
                Side::Sell
            } else {
                Side::Buy
            }
        }
    };

    let mut p = Position::new(account_id, exchange, &info.instrument, side);
    p.instrument_type = info.instrument_type;
    // 单位/乘数/结算币种按交易所收敛：
    // binance USDM：按币、乘数 1、USDT；
    // deribit 期权：按张(contract)、乘数 1（精确 contract_size 在 OrderRule）、
    //   结算币种取合约首段（BTC/ETH）。
    if exchange == "deribit" {
        p.qty_unit = QtyUnit::Contract;
        p.multiplier = Decimal::ONE;
        p.settle_currency = info
            .instrument
            .split('-')
            .next()
            .unwrap_or("BTC")
            .to_string();
    } else {
        p.qty_unit = QtyUnit::Base;
        p.multiplier = Decimal::ONE;
        p.settle_currency = "USDT".to_string();
    }
    p.quantity = info.signed_quantity.abs();
    p.pos_price = info.entry_price;
    p.leverage = info.leverage.unwrap_or(1);
    p.exchange_update_time = info
        .update_ms
        .filter(|ms| *ms >= 0)
        .map(|ms| ms as u64)
        .unwrap_or(now_ms);

    // 估值价：交易所标记价优先，行情表兜底；都无价时按 0 重估（指标安全归零）。
    let price = info
        .mark_price
        .or_else(|| price_of(&info.instrument))
        .unwrap_or(Decimal::ZERO);
    let mut valued = p.revalued(price);
    // 保证金：逐仓取交易所口径；交叉（无值）按重算后的 notional/leverage 自算。
    valued.margin = info
        .margin
        .unwrap_or_else(|| margin_from_notional(valued.notional, valued.leverage));
    // 无价但交易所直接回报了未实现盈亏：采纳之（比率仍待行情价补算）。
    if price == Decimal::ZERO {
        if let Some(pnl) = info.unrealized_pnl {
            valued.pnl_value = pnl;
        }
    }
    valued
}

/// 交叉保证金口径：notional / leverage；杠杆为 0 返回 0，不除零。
fn margin_from_notional(notional: Decimal, leverage: u64) -> Decimal {
    if leverage == 0 {
        Decimal::ZERO
    } else {
        notional / Decimal::from(leverage)
    }
}

// 测试用的最小账户解析口：name → id（不触盘/不加解密）。
#[cfg(test)]
#[derive(Default)]
struct FakeAccounts {
    ids: HashMap<String, u64>,
}

#[cfg(test)]
impl FakeAccounts {
    fn with(pairs: &[(&str, u64)]) -> Arc<dyn AccountLookup> {
        Arc::new(FakeAccounts {
            ids: pairs
                .iter()
                .map(|(n, id)| ((*n).to_string(), *id))
                .collect(),
        })
    }
}

#[cfg(test)]
impl AccountLookup for FakeAccounts {
    fn account_id(&self, _exchange: &str, account: &str) -> Option<u64> {
        self.ids.get(account).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use trading_exchange::PositionProfile;
    use trading_models::Quote;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn engine_with(accounts: Arc<dyn AccountLookup>) -> (PositionEngine, PositionRepo) {
        let store = store_core::Store::new(Arc::new(store_core::SystemClock));
        let table = store.register::<Position>().unwrap();
        let repo = PositionRepo::new(table);
        let engine = PositionEngine::new(
            repo.clone(),
            Arc::new(QuoteTable::new()),
            accounts,
            LeverageRepo::new(),
        );
        (engine, repo)
    }

    /// binance/deribit/hyperliquid 三家完整档案（与生产 registry.position_profiles() 同形）。
    fn all_profiles() -> Vec<PositionProfile> {
        vec![
            PositionProfile::binance(),
            PositionProfile::deribit(),
            PositionProfile::hyperliquid(),
        ]
    }

    /// 一条 HL 合约 PositionInfo 快照（裸币名、instrument_type=Futures）。
    fn hl_info(
        coin: &str,
        signed: &str,
        entry: &str,
        mark: Option<&str>,
        leverage: Option<u64>,
    ) -> PositionInfo {
        PositionInfo {
            instrument: coin.into(),
            instrument_type: InstrumentType::Futures,
            position_side: OrderPositionSide::Both,
            signed_quantity: dec(signed),
            entry_price: dec(entry),
            mark_price: mark.map(dec),
            unrealized_pnl: None,
            leverage,
            notional: None,
            margin: None,
            update_ms: Some(1_000),
        }
    }

    /// 向行情表写一条「只有 mark 价」的报价。
    fn put_mark(quotes: &QuoteTable, venue: Venue, kind: MarketKind, symbol: &str, mark: &str) {
        quotes.put(
            &Instrument::new(venue, kind, symbol),
            Quote {
                mark: Some(dec(mark)),
                ..Quote::default()
            },
        );
    }

    /// 一条交易所 PositionInfo 快照。
    #[allow(clippy::too_many_arguments)]
    fn info(
        inst: &str,
        side: OrderPositionSide,
        signed: &str,
        entry: &str,
        mark: Option<&str>,
        leverage: Option<u64>,
        margin: Option<&str>,
        pnl: Option<&str>,
    ) -> PositionInfo {
        PositionInfo {
            instrument: inst.into(),
            instrument_type: InstrumentType::Futures,
            position_side: side,
            signed_quantity: dec(signed),
            entry_price: dec(entry),
            mark_price: mark.map(dec),
            unrealized_pnl: pnl.map(dec),
            leverage,
            notional: None,
            margin: margin.map(dec),
            update_ms: Some(1_000),
        }
    }

    // ---- mapper 纯映射（自 sync.rs 随迁）----

    #[test]
    fn snapshot_maps_both_long_to_valued_position() {
        let no_price = |_: &str| None;
        let v = snapshot_positions(
            7,
            "binance",
            &[info(
                "BTCUSDT",
                OrderPositionSide::Both,
                "0.1",
                "80000",
                Some("80100"),
                Some(10),
                None,
                None,
            )],
            &no_price,
            2_000,
        );
        assert_eq!(v.len(), 1);
        let p = &v[0];
        assert_eq!(p.account_id, 7);
        assert_eq!(p.exchange, "binance");
        assert_eq!(p.side, Side::Buy);
        assert_eq!(p.quantity, dec("0.1"));
        assert_eq!(p.pos_price, dec("80000"));
        assert_eq!(p.current_price, dec("80100"));
        assert_eq!(p.notional, dec("8010"));
        assert_eq!(p.margin, dec("801"));
        assert_eq!(p.leverage, 10);
        assert_eq!(p.exchange_update_time, 1000);
    }

    #[test]
    fn snapshot_short_uses_abs_qty_and_hedge_side_wins() {
        let no_price = |_: &str| None;
        let v = snapshot_positions(
            1,
            "binance",
            &[
                info(
                    "BTCUSDT",
                    OrderPositionSide::Both,
                    "-2",
                    "2000",
                    Some("1990"),
                    Some(5),
                    None,
                    None,
                ),
                info(
                    "BTCUSDT",
                    OrderPositionSide::Long,
                    "1",
                    "2000",
                    None,
                    Some(5),
                    None,
                    None,
                ),
            ],
            &no_price,
            2_000,
        );
        assert_eq!(v[0].side, Side::Sell);
        assert_eq!(v[0].quantity, dec("2"));
        assert_eq!(v[1].side, Side::Buy);
    }

    #[test]
    fn snapshot_prefers_exchange_isolated_margin() {
        let no_price = |_: &str| None;
        let v = snapshot_positions(
            7,
            "binance",
            &[info(
                "ETHUSDT",
                OrderPositionSide::Both,
                "-2",
                "2000",
                Some("1990"),
                Some(5),
                Some("796"),
                None,
            )],
            &no_price,
            2_000,
        );
        assert_eq!(v[0].margin, dec("796"));
    }

    // ---- 编排：PositionSnapshotSink ----

    #[tokio::test]
    async fn snapshot_creates_then_empty_snapshot_closes() {
        let (engine, repo) = engine_with(FakeAccounts::with(&[("alice", 7)]));
        // 与生产一致：监听前先完成档案装配（否则作用域为空，无法对账/软删）。
        engine.load_and_verify(&all_profiles()).expect("档案装配");

        // 首次快照：远端有、本地无 → 建行。
        engine
            .on_position_snapshot(
                "binance",
                "alice",
                &[info(
                    "BTCUSDT",
                    OrderPositionSide::Both,
                    "1",
                    "100",
                    Some("125"),
                    Some(10),
                    None,
                    None,
                )],
            )
            .await;
        let rows = repo.list_active();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].account_id, 7);
        // 成本口径：roi=25*10/100=2.5。
        assert_eq!(rows[0].roi, dec("2.5"));

        // 空快照（get_positions 滤掉 0 量行后返回 []）= 合约账户确无持仓 → 软删。
        engine.on_position_snapshot("binance", "alice", &[]).await;
        assert!(repo.list_active().is_empty());
        assert!(repo
            .list_active_scope(7, "binance", &[InstrumentType::Futures])
            .is_empty());
    }

    /// 对账快照带 leverage 时，应写入杠杆缓存（供下单前按需设置）。
    #[tokio::test]
    async fn snapshot_writes_leverage_cache() {
        // Arrange：自建引擎并持有同一份杠杆仓储。
        let accounts = FakeAccounts::with(&[("alice", 7)]);
        let store = store_core::Store::new(Arc::new(store_core::SystemClock));
        let table = store.register::<Position>().unwrap();
        let repo = PositionRepo::new(table);
        let leverages = LeverageRepo::new();
        let engine = PositionEngine::new(
            repo.clone(),
            Arc::new(QuoteTable::new()),
            accounts,
            leverages.clone(),
        );

        // Act：对账一条 leverage=3 的快照。
        engine
            .on_position_snapshot(
                "binance",
                "alice",
                &[info(
                    "BTCUSDT",
                    OrderPositionSide::Both,
                    "0.002",
                    "80000",
                    Some("80100"),
                    Some(3),
                    None,
                    None,
                )],
            )
            .await;

        // Assert：缓存写入 3（后续同杠杆开仓可跳过 set_leverage）。
        assert_eq!(
            leverages.get(&LeverageKey::new(7, "binance", "BTCUSDT")),
            Some(3)
        );
    }

    #[tokio::test]
    async fn unknown_account_is_skipped_without_write() {
        let (engine, repo) = engine_with(FakeAccounts::with(&[]));
        engine
            .on_position_snapshot(
                "binance",
                "ghost",
                &[info(
                    "BTCUSDT",
                    OrderPositionSide::Both,
                    "1",
                    "100",
                    Some("100"),
                    Some(1),
                    None,
                    None,
                )],
            )
            .await;
        assert!(repo.list_active().is_empty());
    }

    #[tokio::test]
    async fn reconcile_scopes_by_account_and_type() {
        let (engine, repo) = engine_with(FakeAccounts::with(&[("alice", 7), ("bob", 8)]));
        // 与生产一致：监听前先完成档案装配（否则 binance 作用域为空）。
        engine.load_and_verify(&all_profiles()).expect("档案装配");

        // bob 的合约 BTC + alice 的现货 BTC（合约流水线均不应触碰）。
        let mut alice_spot = Position::new(7, "binance", "BTCUSDT", Side::Buy);
        alice_spot.instrument_type = InstrumentType::Spot;
        let bob = Position::new(8, "binance", "BTCUSDT", Side::Buy);
        // alice 现有合约 BTC 仓。
        let mut alice_btc = Position::new(7, "binance", "BTCUSDT", Side::Buy);
        alice_btc.quantity = dec("1");
        alice_btc.pos_price = dec("100");
        repo.apply_diffs(diff_positions(vec![alice_spot, bob, alice_btc], vec![], 1))
            .unwrap();

        // alice 快照只有合约 ETH：她的合约 BTC 被软删，ETH 新建；bob/现货不动。
        engine
            .on_position_snapshot(
                "binance",
                "alice",
                &[info(
                    "ETHUSDT",
                    OrderPositionSide::Both,
                    "1",
                    "100",
                    Some("100"),
                    Some(1),
                    None,
                    None,
                )],
            )
            .await;

        let active = repo.list_active();
        assert!(active
            .iter()
            .any(|p| p.account_id == 8 && p.instrument == "BTCUSDT"));
        assert!(active
            .iter()
            .any(|p| p.account_id == 7 && p.instrument_type == InstrumentType::Spot));
        assert!(!active.iter().any(|p| p.account_id == 7
            && p.instrument_type == InstrumentType::Futures
            && p.instrument == "BTCUSDT"));
        assert!(active
            .iter()
            .any(|p| p.account_id == 7 && p.instrument == "ETHUSDT"));
    }

    // ---- 编排：QuoteObserver ----

    #[tokio::test]
    async fn quote_revalues_only_binance_futures_positions() {
        let (engine, repo) = engine_with(FakeAccounts::with(&[("alice", 7)]));
        // 与生产一致：监听前先完成档案装配（否则 on_quote 反查 miss）。
        engine.load_and_verify(&all_profiles()).expect("档案装配");
        let mut btc = Position::new(7, "binance", "BTCUSDT", Side::Buy);
        btc.quantity = dec("1");
        btc.pos_price = dec("100");
        btc.leverage = 10;
        let mut xili = Position::new(7, "xili", "BTCUSDT", Side::Buy);
        xili.quantity = dec("1");
        xili.pos_price = dec("100");
        repo.apply_diffs(diff_positions(vec![btc, xili], vec![], 1))
            .unwrap();

        // binance 合约行情 → 只重估 binance 行。
        engine.on_quote(
            Venue::BinanceFutures,
            MarketKind::Futures,
            "BTCUSDT",
            dec("125"),
        );
        let rows = repo.list_active();
        let b = rows.iter().find(|p| p.exchange == "binance").unwrap();
        let x = rows.iter().find(|p| p.exchange == "xili").unwrap();
        assert_eq!(b.current_price, dec("125"));
        // 成本口径：roi=25*10/100=2.5。
        assert_eq!(b.roi, dec("2.5"));
        assert_eq!(x.current_price, dec("0"));

        // 非仓位流水线的 venue（现货）忽略。
        engine.on_quote(Venue::BinanceSpot, MarketKind::Spot, "BTCUSDT", dec("999"));
        assert_eq!(
            repo.list_active()
                .iter()
                .find(|p| p.exchange == "binance")
                .unwrap()
                .current_price,
            dec("125")
        );
    }

    // ---- Hyperliquid：tick 重估 / 对账作用域 / 估值兜底（新增行为，RED→GREEN）----

    #[tokio::test]
    async fn hl_quote_tick_revalues_position() {
        let (engine, repo) = engine_with(FakeAccounts::with(&[("hy", 9)]));
        engine.load_and_verify(&all_profiles()).expect("档案装配");

        // 本地先有一条 HL BTC 多仓（量/成本已知，3x）。
        let mut btc = Position::new(9, "hyperliquid", "BTC", Side::Buy);
        btc.quantity = dec("0.001");
        btc.pos_price = dec("80000");
        btc.leverage = 3;
        repo.apply_diffs(diff_positions(vec![btc], vec![], 1))
            .unwrap();

        // HL 合约行情 tick：现价 84000（旧逻辑在 quote_target 处把 HL 丢弃）。
        engine.on_quote(Venue::Hyperliquid, MarketKind::Futures, "BTC", dec("84000"));

        let row = repo
            .list_active()
            .into_iter()
            .find(|p| p.exchange == "hyperliquid")
            .expect("HL 仓位应存在");
        assert_eq!(row.current_price, dec("84000"));
        // pnl=(84000−80000)×0.001=4；cost=80；roi=4×3/80=0.15。
        assert_eq!(row.pnl_value, dec("4"));
        assert_eq!(row.roi, dec("0.15"));
        assert_eq!(row.max_roi, dec("0.15"));
    }

    #[tokio::test]
    async fn hl_reconcile_updates_instead_of_duplicate_create() {
        let (engine, repo) = engine_with(FakeAccounts::with(&[("hy", 9)]));
        engine.load_and_verify(&all_profiles()).expect("档案装配");

        // 首次快照：HL BTC 多仓（快照本身无 mark_price，与真实 clearinghouse 一致）。
        engine
            .on_position_snapshot(
                "hyperliquid",
                "hy",
                &[hl_info("BTC", "0.001", "80000", None, Some(3))],
            )
            .await;
        assert_eq!(repo.list_active().len(), 1);
        let first_id = repo.list_active()[0].id;

        // 第二次快照：同一仓位数量已变（交易所侧加仓）。须 Updated 同 id，而非再 Created。
        engine
            .on_position_snapshot(
                "hyperliquid",
                "hy",
                &[hl_info("BTC", "0.002", "80500", None, Some(3))],
            )
            .await;

        let active = repo.list_active();
        assert_eq!(active.len(), 1, "同一仓位不得反复插新行");
        assert_eq!(active[0].id, first_id, "更新须保留原行 id");
        assert_eq!(active[0].quantity, dec("0.002"));
    }

    #[tokio::test]
    async fn hl_fallback_valuation_uses_hl_quote() {
        // 自建引擎并持有同一份行情表，以便写入 HL mark。
        let accounts = FakeAccounts::with(&[("hy", 9)]);
        let store = store_core::Store::new(Arc::new(store_core::SystemClock));
        let table = store.register::<Position>().unwrap();
        let repo = PositionRepo::new(table);
        let quotes = Arc::new(QuoteTable::new());
        let engine = PositionEngine::new(
            repo.clone(),
            Arc::clone(&quotes),
            accounts,
            LeverageRepo::new(),
        );
        engine.load_and_verify(&all_profiles()).expect("档案装配");

        // HL 快照本身无 mark_price；行情表里有 HL BTC mark=84000。
        put_mark(
            &quotes,
            Venue::Hyperliquid,
            MarketKind::Futures,
            "BTC",
            "84000",
        );
        engine
            .on_position_snapshot(
                "hyperliquid",
                "hy",
                &[hl_info("BTC", "0.001", "80000", None, Some(3))],
            )
            .await;

        let row = repo
            .list_active()
            .into_iter()
            .find(|p| p.exchange == "hyperliquid")
            .expect("HL 仓位");
        assert_eq!(row.current_price, dec("84000"));
        // 多仓按 0 价会得 roi=−3（=−杠杆）；兜底取价后 roi=0.15 为合理值。
        assert_eq!(row.roi, dec("0.15"));
    }

    // ---- load_and_verify 启动自检（fail-fast）----

    #[test]
    fn load_and_verify_rejects_empty_or_duplicate_exchange() {
        let (engine, _repo) = engine_with(FakeAccounts::with(&[]));

        // 空集合：拒绝（至少要有一家接入仓位流水线）。
        assert!(engine.load_and_verify(&[]).is_err());
        // 重复 exchange：拒绝（同所两份档案会让索引/计数不确定）。
        let dup = vec![PositionProfile::binance(), PositionProfile::binance()];
        assert!(engine.load_and_verify(&dup).is_err());
    }

    #[test]
    fn load_and_verify_accepts_complete_profiles_and_indexes_them() {
        let (engine, _repo) = engine_with(FakeAccounts::with(&[]));
        assert!(engine.load_and_verify(&all_profiles()).is_ok());

        // 装配后 snapshot_types 按档案返回产品形状；未接入所仍为空（与旧回退等价）。
        assert_eq!(
            engine.snapshot_types("hyperliquid"),
            vec![InstrumentType::Futures]
        );
        assert_eq!(
            engine.snapshot_types("deribit"),
            vec![InstrumentType::Option]
        );
        assert!(engine.snapshot_types("unknown").is_empty());
    }

    // ---- 回归钉死：binance/deribit 连续快照第二次为 Updated 而非 Created ----

    /// 一条 deribit 期权 PositionInfo（instrument_type=Option、合约名，走 deribit mapper）。
    fn deribit_info(inst: &str, signed: &str, mark: Option<&str>) -> PositionInfo {
        PositionInfo {
            instrument: inst.into(),
            instrument_type: InstrumentType::Option,
            position_side: OrderPositionSide::Both,
            signed_quantity: dec(signed),
            entry_price: dec("2000"),
            mark_price: mark.map(dec),
            unrealized_pnl: None,
            leverage: Some(1),
            notional: None,
            margin: None,
            update_ms: Some(1_000),
        }
    }

    #[test]
    fn second_snapshot_updates_binance_and_deribit_not_creates() {
        let (engine, repo) = engine_with(FakeAccounts::with(&[("alice", 7), ("bob", 8)]));
        engine.load_and_verify(&all_profiles()).expect("档案装配");

        // binance alice：首快照 1 BTC → 建行。
        engine.reconcile(
            "binance",
            "alice",
            &[info(
                "BTCUSDT",
                OrderPositionSide::Both,
                "1",
                "100",
                Some("100"),
                Some(1),
                None,
                None,
            )],
            1_000,
        );
        // deribit bob：首快照 1 张期权 → 建行。
        engine.reconcile(
            "deribit",
            "bob",
            &[deribit_info("BTC-18SEP26-2550-P", "1", Some("2000"))],
            1_000,
        );
        let binance_id = repo
            .list_active()
            .iter()
            .find(|p| p.exchange == "binance")
            .expect("binance 行")
            .id;
        let deribit_id = repo
            .list_active()
            .iter()
            .find(|p| p.exchange == "deribit")
            .expect("deribit 行")
            .id;

        // 第二次快照：两所仓位数量均已变。
        engine.reconcile(
            "binance",
            "alice",
            &[info(
                "BTCUSDT",
                OrderPositionSide::Both,
                "2",
                "100",
                Some("100"),
                Some(1),
                None,
                None,
            )],
            2_000,
        );
        engine.reconcile(
            "deribit",
            "bob",
            &[deribit_info("BTC-18SEP26-2550-P", "2", Some("2000"))],
            2_000,
        );

        // 断言：两所各仍只有一行、id 不变（Updated），数量已刷新；若误判 Created，
        // 会出现两行且新行 id 不同（即日志里 created=1 updated=0 的回归）。
        let binance_rows: Vec<_> = repo
            .list_active()
            .into_iter()
            .filter(|p| p.exchange == "binance")
            .collect();
        let deribit_rows: Vec<_> = repo
            .list_active()
            .into_iter()
            .filter(|p| p.exchange == "deribit")
            .collect();
        assert_eq!(binance_rows.len(), 1, "binance 不得重复建行");
        assert_eq!(deribit_rows.len(), 1, "deribit 不得重复建行");
        assert_eq!(binance_rows[0].id, binance_id, "binance 须原地 Updated");
        assert_eq!(deribit_rows[0].id, deribit_id, "deribit 须原地 Updated");
        assert_eq!(binance_rows[0].quantity, dec("2"));
        assert_eq!(deribit_rows[0].quantity, dec("2"));
    }
}
