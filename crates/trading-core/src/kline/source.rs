//! 订阅登记表 → 交易所期望来源的适配。
//!
//! [`RegistrySource`] 绑定一个 (venue, itype, interval) 作用域，
//! [`Self::symbols`] 即该作用域下当前被任一客户端订阅的合约快照；
//! 服务层把它薄封装成交易所层的 DesiredSource，CTP 500ms 轮询产生增订/退订。
//! 放在 core（而非实现 DesiredSource 的 service）：注册表/快照纯内核可测，
//! 不引入 trading-core → trading-exchange 的依赖。

use std::sync::Arc;

use trading_models::InstrumentType;

use super::KlineRegistry;

/// 登记表在单个 (venue, itype, interval) 作用域上的符号快照视图。
#[derive(Clone)]
pub struct RegistrySource {
    registry: Arc<KlineRegistry>,
    venue: String,
    itype: InstrumentType,
    interval: String,
}

impl RegistrySource {
    pub fn new(
        registry: Arc<KlineRegistry>,
        venue: String,
        itype: InstrumentType,
        interval: String,
    ) -> Self {
        Self {
            registry,
            venue,
            itype,
            interval,
        }
    }

    /// 当前作用域下被订阅的合约（去重、字典序；随登记变化即时反映）。
    pub fn symbols(&self) -> Vec<String> {
        self.registry
            .symbols(&self.venue, self.itype, &self.interval)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{KlineKey, KlineRegistry};
    use super::*;
    use trading_models::InstrumentType;

    #[test]
    fn symbols_track_registry_changes_within_scope() {
        // 登记变化即时反映到来源快照（无需重新构造来源）。
        let registry = Arc::new(KlineRegistry::new());
        let src = RegistrySource::new(
            Arc::clone(&registry),
            "ctp".to_string(),
            InstrumentType::Futures,
            "1m".to_string(),
        );
        assert!(src.symbols().is_empty());

        registry.subscribe(
            "a",
            KlineKey::new("ctp", InstrumentType::Futures, "cu2512", "1m"),
        );
        registry.subscribe(
            "b",
            KlineKey::new("ctp", InstrumentType::Futures, "ag2612", "1m"),
        );
        // 别的作用域订阅不应泄漏进来。
        registry.subscribe(
            "a",
            KlineKey::new("ctp", InstrumentType::Futures, "rb2611", "5m"),
        );
        assert_eq!(
            src.symbols(),
            vec!["ag2612".to_string(), "cu2512".to_string()]
        );

        // 全部客户端退订后来源快照重新为空 → 交易所下轮对账退订。
        registry.unsubscribe(
            "a",
            &KlineKey::new("ctp", InstrumentType::Futures, "cu2512", "1m"),
        );
        registry.unsubscribe(
            "b",
            &KlineKey::new("ctp", InstrumentType::Futures, "ag2612", "1m"),
        );
        assert!(src.symbols().is_empty());
    }

    #[test]
    fn disconnected_client_symbols_disappear() {
        let registry = Arc::new(KlineRegistry::new());
        let src = RegistrySource::new(
            Arc::clone(&registry),
            "ctp".to_string(),
            InstrumentType::Futures,
            "1m".to_string(),
        );
        registry.subscribe(
            "nine",
            KlineKey::new("ctp", InstrumentType::Futures, "cu2512", "1m"),
        );
        assert_eq!(src.symbols(), vec!["cu2512".to_string()]);

        registry.disconnect("nine");
        assert!(src.symbols().is_empty());
    }
}
