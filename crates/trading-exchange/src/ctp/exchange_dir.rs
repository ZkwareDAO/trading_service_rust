//! 会话态「合约 → 交易所代码」目录（跨会话撤单定位 ExchangeID 用）。
//!
//! 背景：撤单结构 `CThostFtdcInputOrderActionField` 有两种定位方式——
//! 本会话用 `FrontID+SessionID+OrderRef`，**重启后跨会话**只能用
//! `ExchangeID+OrderSysID`。重连后 `FrontID/SessionID` 已变，三元组失效，
//! 必须知道订单所在交易所。
//!
//! 数据零额外请求：重连 `query_orders("")` 重放的当日委托行与实时
//! `on_rtn_order` 回报都已带 `ExchangeID`，顺手喂入本目录即可。将来 SHFE
//! 平今/平昨按交易所路由（`prep.rs`）也复用它。
//!
//! 生命周期：会话态、进程内存（DashMap），丢得起——预热缺失时跨会话撤单
//! 返回「会话未就绪」清晰错误，绝不凭猜测填 ExchangeID。

use dashmap::DashMap;

/// 合约代码 → 交易所代码目录（会话态，线程安全）。
#[derive(Default)]
pub struct ExchangeDir {
    /// instrument → exchange_id（如 rb2510 → SHFE）。
    by_instrument: DashMap<String, String>,
}

impl ExchangeDir {
    pub fn new() -> Self {
        Self::default()
    }

    /// 喂入一条「合约 → 交易所」映射（委托回报/重放行到达时调用）。
    ///
    /// 任一关键字段为空白则忽略（柜台查询行可能有空字段，污染目录会导致
    /// 跨会话撤单定位到空 ExchangeID）。
    pub fn remember(&self, instrument: &str, exchange_id: &str) {
        let instrument = instrument.trim();
        let exchange_id = exchange_id.trim();
        if instrument.is_empty() || exchange_id.is_empty() {
            return;
        }
        // 后到覆盖：同合约再次喂入时以新值为准（可纠正）。
        self.by_instrument
            .insert(instrument.to_string(), exchange_id.to_string());
    }

    /// 读取合约所在交易所（未预热返回 None）。
    pub fn get(&self, instrument: &str) -> Option<String> {
        self.by_instrument
            .get(instrument.trim())
            .map(|e| e.value().clone())
    }

    /// 移除一条映射（热加载/测试清理用）。
    pub fn remove(&self, instrument: &str) {
        self.by_instrument.remove(instrument.trim());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remember_then_get_returns_exchange() {
        // Arrange
        let dir = ExchangeDir::new();

        // Act
        dir.remember("rb2510", "SHFE");

        // Assert：原值取回。
        assert_eq!(dir.get("rb2510").as_deref(), Some("SHFE"));
    }

    #[test]
    fn get_missing_returns_none() {
        // Arrange / Act / Assert：未预热的合约查不到。
        let dir = ExchangeDir::new();
        assert_eq!(dir.get("ni2510"), None);
    }

    #[test]
    fn remember_blank_fields_is_ignored() {
        // Arrange：先有一条有效映射。
        let dir = ExchangeDir::new();
        dir.remember("rb2510", "SHFE");

        // Act：喂入空白交易所 / 空白合约（柜台空行不得污染目录）。
        dir.remember("rb2510", "   ");
        dir.remember("", "DCE");

        // Assert：有效映射保持不变，空白键不入目录。
        assert_eq!(dir.get("rb2510").as_deref(), Some("SHFE"));
        assert_eq!(dir.get(""), None);
    }

    #[test]
    fn later_remember_overwrites_previous() {
        // Arrange
        let dir = ExchangeDir::new();
        dir.remember("x2510", "DCE");

        // Act：同一合约再次喂入（不同交易所——异常但以后到为准，保证可纠正）。
        dir.remember("x2510", "CZCE");

        // Assert
        assert_eq!(dir.get("x2510").as_deref(), Some("CZCE"));
    }

    #[test]
    fn remove_drops_mapping() {
        // Arrange
        let dir = ExchangeDir::new();
        dir.remember("rb2510", "SHFE");

        // Act
        dir.remove("rb2510");

        // Assert
        assert_eq!(dir.get("rb2510"), None);
    }
}
