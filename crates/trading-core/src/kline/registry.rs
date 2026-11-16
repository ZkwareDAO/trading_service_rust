//! 客户端 K线订阅登记表。
//!
//! key=(venue, itype, symbol, interval) → 订阅客户端集合（引用计数＝集合大小）：
//! 同一客户端对同一 key 重复幂等；多个客户端订阅同一 key 只向交易所订一次；
//! 最后一个客户端退订后 key 消失，交易所侧下轮 500ms 对账退订；
//! 客户端断连一次摘除其全部订阅。
//!
//! 客户端身份直接用**南向策略名**（与 StrategyHub 一致，断连可一次清理）。
//!
//! 纯内存（[`std::sync::Mutex`]，登记动作低频、非行情热路径），不实现 Model、
//! 不快照恢复；进程重启后由客户端重新订阅填充。

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use trading_models::InstrumentType;

/// 一路 K线订阅的身份：交易所 × 标的类别 × 合约 × 周期。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KlineKey {
    /// 交易所标识（binance / deribit / hyperliquid / ctp）。
    pub venue: String,
    /// 标的类别（现货 / 期货 / 期权）。
    pub itype: InstrumentType,
    /// 合约/交易对（南向登记原值；CTP 品种前缀由交易所层展开）。
    pub symbol: String,
    /// 周期标识（本期仅 "1m"）。
    pub interval: String,
    /// 品种订阅范围：false＝只推当前主力（缺省语义）；true＝推该品种全部在交易合约。
    /// 仅对品种前缀订阅有意义，具体合约订阅不受影响。
    pub all_contracts: bool,
}

impl KlineKey {
    pub fn new(
        venue: impl Into<String>,
        itype: InstrumentType,
        symbol: impl Into<String>,
        interval: impl Into<String>,
    ) -> Self {
        Self {
            venue: venue.into(),
            itype,
            symbol: symbol.into(),
            interval: interval.into(),
            all_contracts: false,
        }
    }

    /// 设置品种订阅范围（true＝全部在交易合约；缺省 false＝仅当前主力）。
    /// 构造器保持 4 参以兼容既有调用点，模式用该 builder 追加。
    pub fn with_all_contracts(mut self, all_contracts: bool) -> Self {
        self.all_contracts = all_contracts;
        self
    }
}

/// 定稿 bar 的一个南向接收目标：命中方式（精确/品种前缀）与订阅范围。
///
/// 本层只做结构匹配，**不查主力**；「品种订阅是否只推主力」由服务层
/// 结合 [`crate::kline::DominantTable`] 判定（`all_contracts=false` 时）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarTarget {
    /// 南向客户端（策略名）。
    pub client: String,
    /// 精确命中（该 bar 合约本身被订阅）；true 时无条件推送。
    pub exact: bool,
    /// 品种订阅的返回范围：true＝该品种全部合约；false＝仅当前主力。
    /// `exact=true` 时该值无意义。
    pub all_contracts: bool,
}

/// 客户端订阅登记表（client 为南向策略名；同 key 多 client 即引用计数）。
#[derive(Debug, Default)]
pub struct KlineRegistry {
    subs: Mutex<BTreeMap<KlineKey, BTreeSet<String>>>,
}

impl KlineRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 登记某客户端对一路 K线的订阅（重复登记幂等）。
    pub fn subscribe(&self, client: impl Into<String>, key: KlineKey) {
        self.subs
            .lock()
            .unwrap()
            .entry(key)
            .or_default()
            .insert(client.into());
    }

    /// 移除某客户端的一路订阅；计数归零后 key 删除（触发交易所退订）。
    pub fn unsubscribe(&self, client: &str, key: &KlineKey) {
        let mut subs = self.subs.lock().unwrap();
        if let Some(clients) = subs.get_mut(key) {
            clients.remove(client);
            if clients.is_empty() {
                subs.remove(key);
            }
        }
    }

    /// 客户端断连：一次摘除它的全部订阅；与他人共享的 key 仅减员。
    pub fn disconnect(&self, client: &str) {
        let mut subs = self.subs.lock().unwrap();
        for clients in subs.values_mut() {
            clients.remove(client);
        }
        subs.retain(|_, clients| !clients.is_empty());
    }

    /// 一路订阅的客户端列表（有序、去重；空＝无人订阅）。
    pub fn clients_for(&self, key: &KlineKey) -> Vec<String> {
        self.subs
            .lock()
            .unwrap()
            .get(key)
            .map(|c| c.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// 一路订阅的客户端计数（引用计数）。
    pub fn client_count(&self, key: &KlineKey) -> usize {
        self.subs
            .lock()
            .unwrap()
            .get(key)
            .map(BTreeSet::len)
            .unwrap_or(0)
    }

    /// 某 (venue, itype, interval) 作用域下被订阅的合约（去重、字典序）；
    /// 即交易所侧期望来源的当前快照（CTP 品种前缀也原样返回，由交易所层展开）。
    pub fn symbols(&self, venue: &str, itype: InstrumentType, interval: &str) -> Vec<String> {
        let subs = self.subs.lock().unwrap();
        let mut out: BTreeSet<String> = BTreeSet::new();
        for (key, clients) in subs.iter() {
            if !clients.is_empty()
                && key.venue == venue
                && key.itype == itype
                && key.interval == interval
            {
                out.insert(key.symbol.clone());
            }
        }
        out.into_iter().collect()
    }

    /// 定稿 bar 的南向接收目标：遍历同 (venue,itype,interval) 作用域的所有 key，
    /// 精确合约命中（`exact=true`），或其订阅符号是 bar 合约的**品种前缀**
    /// （CTP 订阅品种 `cu` 可收 `cu2512` 的 bar，按 key 的 `all_contracts` 返回模式）。
    /// 同一客户端命中多个 key 时合并为一条（exact/all 均取或），结果按客户端字典序，
    /// 由调用方据此判定是否推送、并保证不重复推送。
    pub fn targets_for_bar(
        &self,
        venue: &str,
        itype: InstrumentType,
        interval: &str,
        bar_symbol: &str,
    ) -> Vec<BarTarget> {
        let product = product_prefix(bar_symbol);
        let subs = self.subs.lock().unwrap();
        // 先按客户端合并：同一客户端可能同时精确订阅与品种订阅。
        let mut merged: BTreeMap<String, BarTarget> = BTreeMap::new();
        for (key, clients) in subs.iter() {
            if key.venue != venue || key.itype != itype || key.interval != interval {
                continue;
            }
            let exact = key.symbol == bar_symbol;
            let product_hit = product.as_deref() == Some(key.symbol.as_str());
            if !exact && !product_hit {
                continue;
            }
            for client in clients.iter() {
                let target = merged.entry(client.clone()).or_insert_with(|| BarTarget {
                    client: client.clone(),
                    exact: false,
                    all_contracts: false,
                });
                target.exact |= exact;
                target.all_contracts |= key.all_contracts;
            }
        }
        merged.into_values().collect()
    }
}

/// 剥掉合约尾部数字得品种前缀（`cu2512`→`cu`；纯数字/空前缀返回 None）。
/// 与 CTP 适配层 dominant 规则同口径（内核一份，避免跨层重复）。
pub fn product_prefix(symbol: &str) -> Option<String> {
    let prefix: &str = symbol.trim_end_matches(|c: char| c.is_ascii_digit());
    if prefix.is_empty() {
        None
    } else {
        Some(prefix.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trading_models::InstrumentType;

    const M1: &str = "1m";

    fn key_cu() -> KlineKey {
        KlineKey::new("ctp", InstrumentType::Futures, "cu2512", M1)
    }

    #[test]
    fn subscribe_is_idempotent_per_client_and_counts_clients() {
        // 同一客户端重复订阅同一 key 不重复计数；两个客户端 → 计数 2。
        let r = KlineRegistry::new();
        r.subscribe("alpha", key_cu());
        r.subscribe("alpha", key_cu());
        r.subscribe("beta", key_cu());
        assert_eq!(r.client_count(&key_cu()), 2);
        assert_eq!(r.clients_for(&key_cu()), vec!["alpha", "beta"]);
    }

    #[test]
    fn unsubscribe_drops_to_zero_then_key_vanishes() {
        let r = KlineRegistry::new();
        r.subscribe("alpha", key_cu());
        r.subscribe("beta", key_cu());
        r.unsubscribe("alpha", &key_cu());
        assert_eq!(r.client_count(&key_cu()), 1);
        // 最后一个客户端退订：key 不再出现在符号快照里（触发交易所退订）。
        r.unsubscribe("beta", &key_cu());
        assert_eq!(r.client_count(&key_cu()), 0);
        assert!(r.symbols("ctp", InstrumentType::Futures, M1).is_empty());
    }

    #[test]
    fn disconnect_removes_all_subscriptions_of_one_client_only() {
        // 断连清理只影响该连接：它独占的 key 删除，与他人共享的 key 保留并减员。
        let r = KlineRegistry::new();
        r.subscribe(
            "alpha",
            KlineKey::new("ctp", InstrumentType::Futures, "cu2512", M1),
        );
        r.subscribe(
            "alpha",
            KlineKey::new("ctp", InstrumentType::Futures, "rb2611", M1),
        );
        r.subscribe(
            "beta",
            KlineKey::new("ctp", InstrumentType::Futures, "rb2611", M1),
        );

        r.disconnect("alpha");

        assert_eq!(
            r.clients_for(&KlineKey::new("ctp", InstrumentType::Futures, "rb2611", M1)),
            vec!["beta".to_string()]
        );
        assert_eq!(
            r.client_count(&KlineKey::new("ctp", InstrumentType::Futures, "cu2512", M1)),
            0
        );
    }

    #[test]
    fn symbols_scoped_by_venue_itype_interval_and_sorted() {
        // 交易所期望集按 (venue, itype, interval) 作用域取符号，去重且字典序。
        let r = KlineRegistry::new();
        r.subscribe(
            "a",
            KlineKey::new("ctp", InstrumentType::Futures, "cu2512", M1),
        );
        r.subscribe(
            "b",
            KlineKey::new("ctp", InstrumentType::Futures, "cu2512", M1),
        );
        r.subscribe(
            "a",
            KlineKey::new("ctp", InstrumentType::Futures, "ag2612", M1),
        );
        r.subscribe(
            "a",
            KlineKey::new("binance", InstrumentType::Futures, "BTCUSDT", M1),
        );
        r.subscribe(
            "a",
            KlineKey::new("ctp", InstrumentType::Futures, "cu2512", "5m"),
        );

        assert_eq!(
            r.symbols("ctp", InstrumentType::Futures, M1),
            vec!["ag2612".to_string(), "cu2512".to_string()]
        );
        // 不同作用域互不串扰。
        assert_eq!(
            r.symbols("binance", InstrumentType::Futures, M1),
            vec!["BTCUSDT".to_string()]
        );
        assert!(r.symbols("ctp", InstrumentType::Spot, M1).is_empty());
    }

    #[test]
    fn clients_for_unknown_key_is_empty() {
        let r = KlineRegistry::new();
        assert!(r.clients_for(&key_cu()).is_empty());
        assert_eq!(r.client_count(&key_cu()), 0);
    }

    #[test]
    fn targets_for_bar_marks_exact_and_carries_product_mode() {
        // 具体合约订阅 exact=true；品种订阅 exact=false 并带 all_contracts 模式。
        let r = KlineRegistry::new();
        r.subscribe(
            "direct",
            KlineKey::new("ctp", InstrumentType::Futures, "cu2512", M1),
        );
        r.subscribe(
            "dom",
            KlineKey::new("ctp", InstrumentType::Futures, "cu", M1),
        );
        r.subscribe(
            "all",
            KlineKey::new("ctp", InstrumentType::Futures, "cu", M1).with_all_contracts(true),
        );
        // 无关品种 / 别的作用域不应命中。
        r.subscribe(
            "rb",
            KlineKey::new("ctp", InstrumentType::Futures, "rb2611", M1),
        );
        r.subscribe(
            "spot",
            KlineKey::new("binance", InstrumentType::Futures, "BTCUSDT", M1),
        );

        assert_eq!(
            r.targets_for_bar("ctp", InstrumentType::Futures, M1, "cu2512"),
            vec![
                BarTarget {
                    client: "all".into(),
                    exact: false,
                    all_contracts: true
                },
                BarTarget {
                    client: "direct".into(),
                    exact: true,
                    all_contracts: false
                },
                BarTarget {
                    client: "dom".into(),
                    exact: false,
                    all_contracts: false
                },
            ]
        );
        // 非主力 cu 合约：只有品种订阅命中，模式随 key 携带（是否放行由调用方判）。
        assert_eq!(
            r.targets_for_bar("ctp", InstrumentType::Futures, M1, "cu2601"),
            vec![
                BarTarget {
                    client: "all".into(),
                    exact: false,
                    all_contracts: true
                },
                BarTarget {
                    client: "dom".into(),
                    exact: false,
                    all_contracts: false
                },
            ]
        );
        // 无关品种 / 别的作用域不命中。
        assert!(r
            .targets_for_bar("ctp", InstrumentType::Futures, M1, "rb2611")
            .iter()
            .all(|t| t.client != "dom"));
        assert!(r
            .targets_for_bar("binance", InstrumentType::Futures, M1, "cu2512")
            .is_empty());
    }

    #[test]
    fn targets_for_bar_merges_modes_of_same_client() {
        // 同一客户端同时精确订阅与品种订阅：合并为一条（exact 取或、all 取或），
        // 避免同一根 bar 重复推送。
        let r = KlineRegistry::new();
        r.subscribe(
            "x",
            KlineKey::new("ctp", InstrumentType::Futures, "cu2512", M1),
        );
        r.subscribe(
            "x",
            KlineKey::new("ctp", InstrumentType::Futures, "cu", M1).with_all_contracts(true),
        );
        assert_eq!(
            r.targets_for_bar("ctp", InstrumentType::Futures, M1, "cu2512"),
            vec![BarTarget {
                client: "x".into(),
                exact: true,
                all_contracts: true
            }]
        );
    }

    #[test]
    fn product_prefix_strips_trailing_digits() {
        assert_eq!(product_prefix("cu2512").as_deref(), Some("cu"));
        assert_eq!(product_prefix("rb2601").as_deref(), Some("rb"));
        assert_eq!(product_prefix("BTCUSDT").as_deref(), Some("BTCUSDT"));
        assert_eq!(product_prefix("123"), None);
    }
}
