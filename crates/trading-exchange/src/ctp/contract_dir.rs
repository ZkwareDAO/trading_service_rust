//! 会话态「合约 → 合约乘数（VolumeMultiple）」目录。
//!
//! 背景：CTP 持仓回报的 `PositionCost` 是**总持仓成本**（已含合约乘数），
//! 而 tick/现价是「每单位单价」。要把成本折算成与现价同量纲的单价，必须知道
//! 每个合约的乘数（如螺纹 rb=10、塑料 l=5、玻璃 FG=20、股指期货 IF=300）。
//! 乘数不挂在持仓结构上，只能在登录后经 `ReqQryInstrument` 全量拉取一次缓存于此。
//!
//! 生命周期：会话态、进程内存（DashMap），每次重连重新预热。乘数是交易所静态
//! 合约规格、当日不变；预热缺失（查询失败）时 mapper 选择过滤该批持仓而非产出
//! 错误量纲的价，下一轮（重连/重试）对账补齐。

use dashmap::DashMap;

/// 合约代码 → 合约乘数（每手对应的标的数量，单位：吨/股/点等）。
#[derive(Default)]
pub struct ContractDir {
    /// instrument → VolumeMultiple（恒为正整数）。
    by_instrument: DashMap<String, i32>,
}

impl ContractDir {
    pub fn new() -> Self {
        Self::default()
    }

    /// 喂入一条「合约 → 乘数」（合约查询应答逐条到达时调用）。
    ///
    /// 合约代码空白或乘数非正（柜台缺省/异常行）一律忽略，避免污染后被用于除零
    /// 或产出错误单价。后到覆盖（同合约再次预热以新值为准）。
    pub fn remember(&self, instrument: &str, volume_multiple: i32) {
        let instrument = instrument.trim();
        if instrument.is_empty() || volume_multiple <= 0 {
            return;
        }
        self.by_instrument
            .insert(instrument.to_string(), volume_multiple);
    }

    /// 读取合约乘数（未预热返回 None）。
    pub fn get(&self, instrument: &str) -> Option<i32> {
        self.by_instrument
            .get(instrument.trim())
            .map(|e| *e.value())
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

    #[test]
    fn remember_then_get_returns_multiple() {
        let dir = ContractDir::new();
        dir.remember("rb2510", 10);
        assert_eq!(dir.get("rb2510"), Some(10));
        assert_eq!(dir.len(), 1);
    }

    #[test]
    fn get_missing_returns_none() {
        let dir = ContractDir::new();
        assert_eq!(dir.get("cu2611"), None);
        assert!(dir.is_empty());
    }

    #[test]
    fn non_positive_multiple_is_ignored() {
        let dir = ContractDir::new();
        dir.remember("rb2510", 0);
        dir.remember("l2701", -5);
        assert!(dir.is_empty());
        assert_eq!(dir.get("rb2510"), None);
    }

    #[test]
    fn blank_instrument_is_ignored() {
        let dir = ContractDir::new();
        dir.remember("   ", 10);
        assert!(dir.is_empty());
    }

    #[test]
    fn later_value_overwrites_and_trim_dedups() {
        let dir = ContractDir::new();
        dir.remember("rb2510", 10);
        dir.remember(" rb2510 ", 10);
        assert_eq!(dir.get("rb2510"), Some(10));
        assert_eq!(dir.len(), 1, "trim 后同合约只占一条");
    }
}
