use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use store_core::model::bool_u8;
use store_core::{Model, StorageScope};

use crate::types::{InstrumentType, Side};

/// 订单类型。CSV 中存大写字符串。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderType {
    #[default]
    Limit,
    Market,
    StopMarket,
    TakeMarket,
}

impl OrderType {
    /// 交易所 REST/线上使用的小写串（`stop_market` 等）。
    pub fn wire(self) -> &'static str {
        match self {
            OrderType::Limit => "limit",
            OrderType::Market => "market",
            OrderType::StopMarket => "stop_market",
            OrderType::TakeMarket => "take_market",
        }
    }

    /// 宽松解析策略端下单类型：缺省/未识别按 limit；`stop`/`take` 为简写别名。
    pub fn from_wire(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "market" => OrderType::Market,
            "stop_market" | "stop" => OrderType::StopMarket,
            "take_market" | "take" => OrderType::TakeMarket,
            _ => OrderType::Limit,
        }
    }
}

/// 有效期策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum TimeInForce {
    #[default]
    Gtc,
    Ioc,
    Fok,
    Day,
}

impl TimeInForce {
    /// 交易所 REST/线上使用的大写串（serde rename 同形）。
    pub fn wire(self) -> &'static str {
        match self {
            TimeInForce::Gtc => "GTC",
            TimeInForce::Ioc => "IOC",
            TimeInForce::Fok => "FOK",
            TimeInForce::Day => "DAY",
        }
    }
}

/// 唯一权威订单状态机。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStatus {
    #[default]
    Placing,
    Unknown,
    New,
    Partial,
    Filled,
    Canceled,
    Rejected,
    Expired,
}

impl OrderStatus {
    /// 该状态意味着订单生命周期终结，不应再被回报推进。
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            OrderStatus::Filled
                | OrderStatus::Canceled
                | OrderStatus::Rejected
                | OrderStatus::Expired
        )
    }

    /// 宽松解析交易所/内部状态串：大小写、连字符、空格均容忍
    /// （`new`/`PARTIAL`/`new ` 均可），无法识别返回 None。
    pub fn from_wire(raw: &str) -> Option<Self> {
        let norm = raw.trim().to_ascii_uppercase().replace(['-', ' '], "_");
        Some(match norm.as_str() {
            "PLACING" => OrderStatus::Placing,
            "UNKNOWN" => OrderStatus::Unknown,
            "NEW" => OrderStatus::New,
            "PARTIAL" => OrderStatus::Partial,
            "FILLED" => OrderStatus::Filled,
            "CANCELED" => OrderStatus::Canceled,
            "REJECTED" => OrderStatus::Rejected,
            "EXPIRED" => OrderStatus::Expired,
            _ => return None,
        })
    }

    /// 线上/落盘统一使用的状态串（与架构文档枚举逐字一致）。
    pub fn wire(self) -> &'static str {
        match self {
            OrderStatus::Placing => "PLACING",
            OrderStatus::Unknown => "UNKNOWN",
            OrderStatus::New => "NEW",
            OrderStatus::Partial => "PARTIAL",
            OrderStatus::Filled => "FILLED",
            OrderStatus::Canceled => "CANCELED",
            OrderStatus::Rejected => "REJECTED",
            OrderStatus::Expired => "EXPIRED",
        }
    }
}

/// 订单。账户作用域表，持久化于 `{day}/{account_name}/order.csv`。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    /// 本地主键。
    pub id: u64,
    /// 本地生成全局唯一键，`0x` + 32 hex；透传交易所、回报原样带回，查单首选。
    pub client_order_id: String,
    pub account_id: u64,
    /// 来源信号 id（信号单）；规则单为空。一个 signal_id 仅对应一笔订单。
    pub signal_id: Option<String>,
    /// 触发本单的规则 id；信号单为 0。
    pub rule_id: u64,
    pub strategy_name: Option<String>,
    pub exchange: String,
    pub instrument_type: InstrumentType,
    /// 是否走交易所测试网（false=正式网）。
    #[serde(with = "bool_u8")]
    pub test_net: bool,
    /// 交易所原生标的符号：BTCUSDT、BTC-27DEC26-100000-C、ZC=F …
    pub instrument: String,
    pub side: Side,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    /// 委托价（market=0），定点。
    pub order_price: Decimal,
    /// 委托数量（币/张/股），定点。
    pub order_quantity: Decimal,
    /// 委托金额（市价按金额，与 quantity 二选一），定点。
    pub order_cash: Decimal,
    /// stop/take 触发价；普通单=0，定点。
    pub trigger_price: Decimal,
    /// 只减仓不反手（现货忽略）。
    #[serde(with = "bool_u8")]
    pub reduce_only: bool,
    /// 杠杆（现货/股票=1）。
    pub leverage: u64,
    /// 交易所原生订单号（受理后才有；仅兜底）。
    pub exchange_order_id: Option<String>,
    /// 传统市场数字客户端引用（初版预留）。
    pub exchange_order_ref: Option<String>,
    pub status: OrderStatus,
    /// 成交均价，定点；禁止浮点比较。
    pub filled_price: Decimal,
    /// 累计成交量，定点；守卫用定点比较 filled_quantity >= order_quantity。
    pub filled_quantity: Decimal,
    /// 软删除：0=正常，1=已删除。
    #[serde(with = "bool_u8")]
    pub deleted: bool,
    /// 最近一次回报时间戳（毫秒）。
    pub exchange_update_time: u64,
    pub created_at: u64,
    pub updated_at: u64,
}

impl Order {
    /// 校验 client_order_id：`0x` + 32 个十六进制字符。
    pub fn is_valid_client_order_id(cid: &str) -> bool {
        match cid.strip_prefix("0x") {
            Some(hex) => hex.len() == 32 && hex.chars().all(|c| c.is_ascii_hexdigit()),
            None => false,
        }
    }

    /// 测试/装配用最小构造，其余字段走默认值（id/timestamps 由框架填）。
    pub fn new(
        account_id: u64,
        client_order_id: impl Into<String>,
        exchange: impl Into<String>,
        instrument: impl Into<String>,
        side: Side,
    ) -> Self {
        Self {
            id: 0,
            client_order_id: client_order_id.into(),
            account_id,
            signal_id: None,
            rule_id: 0,
            strategy_name: None,
            exchange: exchange.into(),
            // Binance U 本位永续在本系统统一记为 Futures（Perpetual 为预留扩展）。
            instrument_type: InstrumentType::Futures,
            test_net: false,
            instrument: instrument.into(),
            side,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
            order_price: Decimal::ZERO,
            order_quantity: Decimal::ZERO,
            order_cash: Decimal::ZERO,
            trigger_price: Decimal::ZERO,
            reduce_only: false,
            leverage: 1,
            exchange_order_id: None,
            exchange_order_ref: None,
            status: OrderStatus::Placing,
            filled_price: Decimal::ZERO,
            filled_quantity: Decimal::ZERO,
            deleted: false,
            exchange_update_time: 0,
            created_at: 0,
            updated_at: 0,
        }
    }
}

impl Model for Order {
    const MODEL_NAME: &'static str = "order";

    fn id(&self) -> u64 {
        self.id
    }
    fn set_id(&mut self, id: u64) {
        self.id = id;
    }
    fn deleted(&self) -> bool {
        self.deleted
    }
    fn set_deleted(&mut self, deleted: bool) {
        self.deleted = deleted;
    }
    fn created_at(&self) -> u64 {
        self.created_at
    }
    fn updated_at(&self) -> u64 {
        self.updated_at
    }
    fn set_timestamps(&mut self, created_at: u64, updated_at: u64) {
        self.created_at = created_at;
        self.updated_at = updated_at;
    }
    fn scope(&self) -> StorageScope {
        StorageScope::Account(self.account_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_order_id_validation() {
        assert!(Order::is_valid_client_order_id(
            "0x0123456789abcdef0123456789abcdef"
        ));
        assert!(!Order::is_valid_client_order_id(
            "0x0123456789abcdef0123456789abcde"
        ));
        assert!(!Order::is_valid_client_order_id(
            "1x0123456789abcdef0123456789abcdef"
        ));
        assert!(!Order::is_valid_client_order_id(
            "0x0123456789abcdeg123456789abcdef0"
        ));
    }

    #[test]
    fn terminal_states_are_final() {
        assert!(OrderStatus::Filled.is_terminal());
        assert!(OrderStatus::Rejected.is_terminal());
        assert!(OrderStatus::Canceled.is_terminal());
        assert!(OrderStatus::Expired.is_terminal());
        assert!(!OrderStatus::New.is_terminal());
        assert!(!OrderStatus::Partial.is_terminal());
        assert!(!OrderStatus::Placing.is_terminal());
        assert!(!OrderStatus::Unknown.is_terminal());
    }

    #[test]
    fn status_wire_matches_doc_enum() {
        assert_eq!(OrderStatus::Placing.wire(), "PLACING");
        assert_eq!(OrderStatus::Unknown.wire(), "UNKNOWN");
        assert_eq!(OrderStatus::New.wire(), "NEW");
        assert_eq!(OrderStatus::Partial.wire(), "PARTIAL");
        assert_eq!(OrderStatus::Filled.wire(), "FILLED");
        assert_eq!(OrderStatus::Canceled.wire(), "CANCELED");
        assert_eq!(OrderStatus::Rejected.wire(), "REJECTED");
        assert_eq!(OrderStatus::Expired.wire(), "EXPIRED");
    }

    #[test]
    fn new_order_defaults_test_net_false() {
        let o = Order::new(7, "0x01", "xili", "btcusdt", Side::Buy);
        assert!(!o.test_net);
        assert!(!o.deleted);
    }

    /// test_net / deleted / 枚举经 CSV（框架的实际落盘格式）0-1、小写往返。
    #[test]
    fn csv_roundtrip_preserves_flags_and_enums() {
        let mut o = Order::new(
            9,
            "0x0123456789abcdef0123456789abcdef",
            "xili",
            "btcusdt",
            Side::Sell,
        );
        o.test_net = true;
        o.instrument_type = InstrumentType::Futures;
        o.order_type = OrderType::Market;
        o.time_in_force = TimeInForce::Ioc;
        o.status = OrderStatus::Partial;

        let mut wtr = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(Vec::new());
        wtr.serialize(&o).unwrap();
        let bytes = wtr.into_inner().unwrap();
        let line = String::from_utf8(bytes).unwrap();
        // test_net 是第 9 列（下标 8），bool_u8 在磁盘上存 1。
        assert_eq!(line.split(',').nth(8), Some("1"));

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(line.as_bytes());
        let back: Order = rdr.deserialize().next().unwrap().unwrap();
        assert!(back.test_net);
        assert_eq!(back.instrument_type, InstrumentType::Futures);
        assert_eq!(back.side, Side::Sell);
        assert_eq!(back.order_type, OrderType::Market);
        assert_eq!(back.time_in_force, TimeInForce::Ioc);
        assert_eq!(back.status, OrderStatus::Partial);
        assert!(!back.deleted);
    }
}
