//! Deribit `public/get_instrument` 合约规格 → 统一 [`OrderRule`] 的纯解析。
//!
//! 期权走「懒加载 + 进程内缓存」，不随全量规则表同步。字段映射见
//! `docs/交易规则.md` 第三/六节与 6.4。
//!
//! 本模块零 IO：只解析已取到的 JSON；HTTP/RPC 拉取在 venue/rpc 层。

use rust_decimal::Decimal;
use trading_models::{InstrumentType, OrderRule, PriceTier, QtyUnit};

/// 解析一个 get_instrument 应答为期权 [`OrderRule`]。
///
/// `fetched_at_ms` 由调用方打点；字段缺失/非法返回错误以便发现映射问题。
pub fn parse_instrument(raw: &str, fetched_at_ms: i64) -> anyhow::Result<OrderRule> {
    let v: serde_json::Value = serde_json::from_str(raw)?;
    let name = field_str(&v, "instrument_name")?;
    let base = v
        .get("base_currency")
        .and_then(|x| x.as_str())
        .unwrap_or("BTC")
        .to_string();
    let contract_size = field_dec(&v, "contract_size")?;
    let base_tick = field_dec(&v, "tick_size")?;

    // 数量步长：首选 qty_tick_size；≤0 或缺失时回退 min_trade_amount。
    let qty_tick = json_dec(v.get("qty_tick_size")).filter(|d| *d > Decimal::ZERO);
    let min_trade = field_dec(&v, "min_trade_amount")?;
    let qty_step = qty_tick.unwrap_or(min_trade);

    let mut row = OrderRule::new(
        "deribit",
        name,
        InstrumentType::Option,
        QtyUnit::Contract,
        contract_size,
        base.clone(), // 期权费以基础币（BTC）报价
        base.clone(), // 结算/盈亏币种
        qty_step,
        "deribit:get_instrument",
        fetched_at_ms,
    );
    // 基础 tick 必须保留：价格低于首档阈值（便宜期权）分档不适用，回退用它；
    // 价格≥above_price 时 tick_for_price 仍选分档较大 tick，二者不冲突。
    row.price_tick = Some(base_tick);
    if let Some(steps) = v.get("tick_size_steps").and_then(|x| x.as_array()) {
        let tiers: Vec<PriceTier> = steps
            .iter()
            .map(|s| {
                Ok(PriceTier {
                    above_price: json_dec(s.get("above_price"))
                        .ok_or_else(|| anyhow::anyhow!("tick step missing above_price"))?,
                    tick: json_dec(s.get("tick_size"))
                        .ok_or_else(|| anyhow::anyhow!("tick step missing tick_size"))?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        if !tiers.is_empty() {
            row.price_tiers = Some(tiers);
        }
    }
    row.qty_min = Some(min_trade);
    row.underlying = Some(base);
    row.expires_at_ms = v.get("expiration_timestamp").and_then(|x| x.as_i64());
    Ok(row)
}

/// 取字符串必填字段。
fn field_str<'a>(v: &'a serde_json::Value, key: &str) -> anyhow::Result<&'a str> {
    v.get(key)
        .and_then(|x| x.as_str())
        .ok_or_else(|| anyhow::anyhow!("get_instrument missing `{key}`"))
}

/// 取 Decimal 必填字段（接受数字或字符串）。
fn field_dec(v: &serde_json::Value, key: &str) -> anyhow::Result<Decimal> {
    json_dec(v.get(key)).ok_or_else(|| anyhow::anyhow!("get_instrument missing/invalid `{key}`"))
}

/// JSON 值（数字或字符串）→ Decimal。
fn json_dec(v: Option<&serde_json::Value>) -> Option<Decimal> {
    v.and_then(|x| match x {
        serde_json::Value::String(s) => s.parse().ok(),
        serde_json::Value::Number(_) => Decimal::from_str_exact(&x.to_string()).ok(),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    const SAMPLE: &str = r#"{
      "instrument_name": "BTC-27DEC26-100000-C",
      "kind": "option",
      "base_currency": "BTC",
      "contract_size": 1.0,
      "tick_size": 0.0005,
      "tick_size_steps": [
        {"above_price": 0.0, "tick_size": 0.0005},
        {"above_price": 0.001, "tick_size": 0.001}
      ],
      "qty_tick_size": 0.1,
      "min_trade_amount": 0.1,
      "expiration_timestamp": 1798329600000
    }"#;

    #[test]
    fn maps_option_instrument_fields() {
        let row = parse_instrument(SAMPLE, 123).expect("parse");
        assert_eq!(row.exchange, "deribit");
        assert_eq!(row.symbol, "BTC-27DEC26-100000-C");
        assert_eq!(row.instrument_type, InstrumentType::Option);
        assert_eq!(row.qty_unit, QtyUnit::Contract);
        assert_eq!(row.multiplier, Decimal::ONE);
        assert_eq!(row.quote_currency, "BTC");
        assert_eq!(row.settle_currency, "BTC");
        // 数量步长首选 qty_tick_size。
        assert_eq!(row.qty_step, dec("0.1"));
        assert_eq!(row.qty_min, Some(dec("0.1")));
        assert_eq!(row.underlying, Some("BTC".to_string()));
        assert_eq!(row.expires_at_ms, Some(1_798_329_600_000));
        assert_eq!(row.source, "deribit:get_instrument");
        assert_eq!(row.fetched_at_ms, 123);
        // 分档 tick。
        let tiers = row.price_tiers.expect("tiers");
        assert_eq!(tiers.len(), 2);
        assert_eq!(tiers[1].above_price, dec("0.001"));
        assert_eq!(tiers[1].tick, dec("0.001"));
        // 基础 tick 必须保留供便宜期权回退（SAMPLE tick_size=0.0005）。
        assert_eq!(row.price_tick, Some(dec("0.0005")));
    }

    #[test]
    fn real_single_tier_drives_5tick_drift_to_quarter_cent() {
        // 真实 get_instrument 形态：基础 tick 0.0001，单一分档
        // above_price=0.005 → 0.0005（价 0.164 适用该档）。
        let raw = r#"{
          "instrument_name": "BTC-23SEP26-72000-C", "kind": "option",
          "base_currency": "BTC", "contract_size": 1,
          "tick_size": 0.0001,
          "tick_size_steps": [{"above_price": 0.005, "tick_size": 0.0005}],
          "min_trade_amount": 0.1
        }"#;
        let row = parse_instrument(raw, 0).expect("parse");
        // 分档已进表：0.164 命中 0.005 档 → tick=0.0005。
        let tick = crate::deribit::pricing::tick_for_price(
            dec("0.164"),
            row.price_tiers.as_deref().unwrap_or(&[]),
            dec("0.0001"),
        );
        assert_eq!(tick, dec("0.0005"));
        // 5 档漂移：0.164 − 5×0.0005 = 0.1615（BUY 向下取整）。
        let price = crate::deribit::pricing::open_price(
            dec("0.164"),
            trading_models::Side::Buy,
            row.price_tiers.as_deref().unwrap_or(&[]),
            dec("0.0001"),
            5,
        );
        assert_eq!(price, dec("0.1615"));
    }

    #[test]
    fn qty_step_falls_back_to_min_trade_when_qty_tick_absent() {
        let raw = r#"{
          "instrument_name": "BTC-1-1-C", "contract_size": 1,
          "tick_size": 0.0005, "min_trade_amount": 1
        }"#;
        let row = parse_instrument(raw, 0).expect("parse");
        assert_eq!(row.qty_step, dec("1"));
        assert_eq!(row.qty_min, Some(dec("1")));
        // 无 tick_size_steps → tiers 留空。
        assert!(row.price_tiers.is_none());
    }

    #[test]
    fn missing_required_fields_and_bad_json_error() {
        assert!(parse_instrument("not json", 0).is_err());
        assert!(parse_instrument(r#"{"instrument_name":"X"}"#, 0).is_err()); // 缺 contract_size 等
        assert!(parse_instrument(
            r#"{"instrument_name":"X","contract_size":1,"tick_size":0.0005,"min_trade_amount":1}"#,
            0
        )
        .is_ok());
    }
}
