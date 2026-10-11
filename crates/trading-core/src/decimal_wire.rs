//! 线上 JSON 的定点数编解码：出参为精确 JSON **数字**（`80000.5`），入参宽松接受
//! 数字或字符串（`80000` 与 `"80000"` 皆可）。全程 [`rust_decimal::Decimal`]，
//! 序列化借助 `serde_json` 的 `arbitrary_precision`，**不经过 f64**，杜绝精度损失。
//!
//! 这里直接复用 rust_decimal 自带的 arbitrary_precision 编解码模块，
//! `domain`/`protocol` 中的字段统一以 `#[serde(with = "crate::decimal_wire")]` 引用，
//! 使底层实现可在一处替换而不动领域模型。

pub use rust_decimal::serde::arbitrary_precision::{deserialize, serialize};

/// 可选定点数：`null`/缺省 -> None，有值时同样以精确数字收发。
pub mod opt {
    pub use rust_decimal::serde::arbitrary_precision_option::{deserialize, serialize};
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use serde::Deserialize;
    use std::str::FromStr;

    #[derive(Debug, Deserialize)]
    struct Required(#[serde(with = "super")] Decimal);

    #[derive(Debug, Deserialize)]
    struct Optional(#[serde(with = "super::opt")] Option<Decimal>);

    #[derive(Debug, serde::Serialize)]
    struct Out(#[serde(with = "super")] Decimal);

    #[test]
    fn parses_number_and_string() {
        assert_eq!(
            serde_json::from_str::<Required>("80000").unwrap().0,
            Decimal::from(80000)
        );
        assert_eq!(
            serde_json::from_str::<Required>("\"0.00001\"")
                .unwrap()
                .0
                .to_string(),
            "0.00001"
        );
    }

    #[test]
    fn parses_optional() {
        assert_eq!(serde_json::from_str::<Optional>("null").unwrap().0, None);
        assert_eq!(
            serde_json::from_str::<Optional>("\"1.5\"").unwrap().0,
            Some(Decimal::from_str("1.5").unwrap())
        );
    }

    #[test]
    fn emits_number_not_string_without_f64() {
        let json = serde_json::to_string(&Out(Decimal::from_str("80000.5").unwrap())).unwrap();
        assert_eq!(json, "80000.5");
        assert!(
            !json.contains('"'),
            "decimal must serialize as a JSON number, got {json}"
        );
    }
}
