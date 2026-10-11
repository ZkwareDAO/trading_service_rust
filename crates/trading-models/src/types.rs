use std::fmt;

use serde::{Deserialize, Serialize};

/// 标的/合约类型（Order / Position 共用）。CSV 中存小写字符串。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum InstrumentType {
    /// 现货（缺省值）。
    #[default]
    Spot,
    /// 预留扩展：当前不产出、勿用作默认。Binance U 本位永续在本系统统一记为
    /// [`InstrumentType::Futures`]；保留此值仅为将来区分真正的永续形态并兼容旧数据。
    Perpetual,
    /// 期货/合约。Binance U 本位永续（fapi）统一用本值；现货/合约同名标的靠它区分。
    Futures,
    /// 期权（Deribit）。
    Option,
    /// 股票（预留）。
    Equity,
    /// 基金（预留）。
    Fund,
}

/// 账户持仓模式（账户级）。CSV 中存小写下划线字符串，缺省单向。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PositionMode {
    /// 单向持仓：positionSide=BOTH，多空由 BUY/SELL 决定。
    #[default]
    OneWay,
    /// 双向持仓（hedge）：positionSide=LONG/SHORT。
    Hedge,
}

/// 买卖方向（Order / Position 共用）。CSV 中存大写字符串。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    #[default]
    Buy,
    Sell,
}

impl Side {
    /// 取反：买 ↔ 卖。平仓单方向与持仓相反时统一用此方法。
    pub fn opposite(self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }

    /// 解析策略端/交易所报文中的方向：`long`/`buy`/`bid`/`多` 与
    /// `short`/`sell`/`ask`/`空` 均可（兼容旧内核的 long/short 词汇）。
    pub fn from_wire(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "long" | "buy" | "bid" | "多" => Some(Side::Buy),
            "short" | "sell" | "ask" | "空" => Some(Side::Sell),
            _ => None,
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn side_accepts_long_short_aliases() {
        // 兼容旧内核 long/short 与交易所 buy/sell、中文多/空。
        assert_eq!(Side::from_wire("LONG"), Some(Side::Buy));
        assert_eq!(Side::from_wire("buy"), Some(Side::Buy));
        assert_eq!(Side::from_wire("bid"), Some(Side::Buy));
        assert_eq!(Side::from_wire("多"), Some(Side::Buy));
        assert_eq!(Side::from_wire("SHORT"), Some(Side::Sell));
        assert_eq!(Side::from_wire("sell"), Some(Side::Sell));
        assert_eq!(Side::from_wire("空"), Some(Side::Sell));
        assert_eq!(Side::from_wire("nope"), None);
    }

    #[test]
    fn side_wire_is_uppercase() {
        assert_eq!(Side::Buy.to_string(), "BUY");
        assert_eq!(Side::Sell.to_string(), "SELL");
    }

    #[test]
    fn position_mode_defaults_one_way() {
        assert_eq!(PositionMode::default(), PositionMode::OneWay);
    }
}
