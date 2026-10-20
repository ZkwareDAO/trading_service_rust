//! 进程级「合约静态信息」目录：`合约 → {交易所, 乘数, 最小变动价位}`。
//!
//! 登录后 `ReqQryInstrument` 全量预热（与持仓/委托查询不同：合约规格是**全局一致**
//! 的静态数据，不随账户变化），故由 [`crate::ctp::provider::CtpProvider`] 持有**一份
//! 进程级共享实例**注入各账户会话，避免多账户重复预热。用途：
//! * 持仓成本折算单价（乘数）；
//! * SHFE 平今/平昨路由前判定合约所属交易所（交易所代码）；
//! * SHFE 市价单转穿价限价时按最小变动价位取整（price_tick）。
//!
//! 生命周期：进程内存（DashMap），重连可重新预热；不挂在持久 Position 上，
//! 每账户的今/昨路由明细仍由各自私有的 `PositionDir` 保存。

use dashmap::DashMap;

use crate::ctp::core::event::InstrumentInfo;

/// 合约静态信息目录（线程安全）。
#[derive(Default)]
pub struct InstrumentDir {
    by_instrument: DashMap<String, InstrumentInfo>,
}

impl InstrumentDir {
    pub fn new() -> Self {
        Self::default()
    }

    /// 喂入一条合约查询结果（合约代码 trim 后为键且同步规范化值，空白忽略；
    /// 后到覆盖，重连预热可纠正）。
    pub fn remember(&self, mut info: InstrumentInfo) {
        info.instrument = info.instrument.trim().to_string();
        if info.instrument.is_empty() {
            return;
        }
        self.by_instrument.insert(info.instrument.clone(), info);
    }

    /// 读取合约静态信息（trim 后精确匹配；未预热返回 None）。
    pub fn get(&self, instrument: &str) -> Option<InstrumentInfo> {
        self.by_instrument
            .get(instrument.trim())
            .map(|e| e.value().clone())
    }

    /// 合约所属交易所代码（未预热返回 None）。只读所需字段，不 clone 整条信息。
    pub fn exchange(&self, instrument: &str) -> Option<String> {
        self.by_instrument
            .get(instrument.trim())
            .map(|e| e.value().exchange_id.clone())
    }

    /// 合约乘数（未预热/非正返回 0，调用方据此判乘数未就绪）。
    pub fn multiple(&self, instrument: &str) -> i32 {
        self.by_instrument
            .get(instrument.trim())
            .map_or(0, |e| e.value().volume_multiple)
    }

    /// 合约多/空保证金率 `(long, short)`（未预热返回 `(0,0)`，调用方据此判比率未就绪）。
    ///
    /// 仅供持仓回报 `UseMargin=0` 时在 ctp 映射层做「持仓成本×比率」兜底；
    /// 非法（0/负/NaN）值的过滤在 `mapping::effective_total_margin` 统一完成，此处只透传。
    pub fn margin_ratios(&self, instrument: &str) -> (f64, f64) {
        self.by_instrument
            .get(instrument.trim())
            .map_or((0.0, 0.0), |e| {
                (e.value().long_margin_ratio, e.value().short_margin_ratio)
            })
    }

    /// 已缓存的合约数（诊断/测试用）。
    pub fn len(&self) -> usize {
        self.by_instrument.len()
    }

    /// 是否为空（诊断/测试用）。
    pub fn is_empty(&self) -> bool {
        self.by_instrument.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(instrument: &str, exchange: &str, multiple: i32, tick: f64) -> InstrumentInfo {
        InstrumentInfo {
            instrument: instrument.into(),
            exchange_id: exchange.into(),
            product_id: "rb".into(),
            is_trading: true,
            volume_multiple: multiple,
            price_tick: tick,
            long_margin_ratio: 0.0,
            short_margin_ratio: 0.0,
        }
    }

    #[test]
    fn remember_then_reads_static_fields() {
        let dir = InstrumentDir::new();
        dir.remember(info(" rb2510 ", "SHFE", 10, 1.0));
        assert_eq!(dir.exchange("rb2510").as_deref(), Some("SHFE"));
        assert_eq!(dir.multiple("rb2510"), 10);
        assert_eq!(dir.get("rb2510").map(|i| i.price_tick), Some(1.0));
        assert_eq!(dir.len(), 1, "trim 后同合约只占一条");
    }

    #[test]
    fn exchange_distinguishes_shfe_dce_and_unknown() {
        let dir = InstrumentDir::new();
        dir.remember(info("rb2510", "SHFE", 10, 1.0));
        dir.remember(info("i2601", "DCE", 100, 0.5));
        assert_eq!(dir.exchange("rb2510").as_deref(), Some("SHFE"));
        assert_eq!(dir.exchange("i2601").as_deref(), Some("DCE"));
        // 未预热合约返回 None（调用方据此不猜测为 SHFE）。
        assert_eq!(dir.exchange("xx9999"), None);
    }

    #[test]
    fn missing_fields_default_to_zero_and_blank_ignored() {
        let dir = InstrumentDir::new();
        assert_eq!(dir.multiple("nope"), 0);
        assert!(dir.exchange("nope").is_none());
        assert!(dir.get("nope").is_none());
        dir.remember(info("  ", "SHFE", 10, 1.0));
        assert!(dir.is_empty(), "空白合约代码忽略");
    }

    #[test]
    fn later_remember_overwrites() {
        let dir = InstrumentDir::new();
        dir.remember(info("rb2510", "SHFE", 10, 1.0));
        dir.remember(info("rb2510", "SHFE", 10, 2.0));
        assert_eq!(dir.get("rb2510").map(|i| i.price_tick), Some(2.0));
        assert_eq!(dir.len(), 1);
    }

    #[test]
    fn margin_ratios_read_back_and_default_to_zero_pair() {
        // 已预热合约：多/空保证金率原样读回（持仓 UseMargin=0 兜底依据）。
        let dir = InstrumentDir::new();
        dir.remember(InstrumentInfo {
            long_margin_ratio: 0.10,
            short_margin_ratio: 0.20,
            ..info("rb2510", "SHFE", 10, 1.0)
        });
        let (long, short) = dir.margin_ratios("rb2510");
        assert!((long - 0.10).abs() < 1e-12, "多头保证金率");
        assert!((short - 0.20).abs() < 1e-12, "空头保证金率");
        // 未预热合约：返回 (0,0)，调用方据此判比率未就绪（不猜测）。
        assert_eq!(dir.margin_ratios("nope"), (0.0, 0.0));
    }
}
