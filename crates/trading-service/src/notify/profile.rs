//! 各交易所通知差异档案（唯一扩展点）。
//!
//! 通知层的所有「按交易所分叉」都必须收口到 [`VenueNotifyProfile`]：
//! 新增交易所只需在 [`NotifyProfiles::from_config`] 登记一条档案，
//! 禁止在通知代码里散落 `if exchange == "xxx"`。

use std::collections::BTreeMap;

use rust_decimal::Decimal;
use trading_models::InstrumentType;

/// 平仓成交通知的 ROI 分母口径。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseRoiBasis {
    /// 名义成本：`pos_price × 平仓量 × 乘数`（deribit/binance 现状）。
    Cost,
    /// 占用保证金：`position.margin × min(平仓量, 持仓量) / 持仓量`（CTP 期货）。
    Margin,
}

/// 每小时仓位播报的持仓来源。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportSource {
    /// 逐账户调交易所只读持仓接口（deribit/binance 现状）。
    ExchangeSnapshot,
    /// 本地活跃仓位（已由行情驱动重估，CTP 无实时 mark 时的唯一正确来源）。
    LocalPositions,
}

/// 播报中杠杆相关行的展示方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportLeverageLine {
    /// 交易所自带杠杆倍数（「杠杆倍数: N」）。
    ExchangeMultiple,
    /// 保证金比例（「保证金比例: X.XX%」）。
    MarginRatio,
}

/// 订单事件播报（通知 A）配置：标题 + 查 ROI 用的标的类别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderEventBroadcast {
    /// 消息标题（如 `DeribitPositions` / `CtpPositions`）。
    pub title: &'static str,
    /// 查本地活跃仓位时使用的标的类别。
    pub instrument_type: InstrumentType,
}

/// 单个交易所的通知行为档案。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VenueNotifyProfile {
    /// 交易所名；未登记交易所的默认档案为空串（不会与真实交易所混淆）。
    pub exchange: &'static str,
    /// 平仓成交通知的 ROI 分母口径。
    pub close_roi: CloseRoiBasis,
    /// ROI 告警阈值（比率口径）；None = 该所不参与 ROI/未设规则告警。
    pub alert_threshold: Option<Decimal>,
    /// 每小时仓位播报的持仓来源。
    pub report_source: ReportSource,
    /// 播报中杠杆相关行的展示方式。
    pub report_leverage: ReportLeverageLine,
    /// 订单事件播报配置；None = 该所不播订单事件。
    pub order_events: Option<OrderEventBroadcast>,
    /// 是否对订单事件去重（CTP 在线会重复推同一状态；deribit 不会）。
    pub dedup_order_events: bool,
}

impl VenueNotifyProfile {
    /// 未登记交易所的默认档案：与引入本档案前的 deribit/binance 行为完全一致。
    fn default_profile() -> Self {
        Self {
            exchange: "",
            close_roi: CloseRoiBasis::Cost,
            alert_threshold: None,
            report_source: ReportSource::ExchangeSnapshot,
            report_leverage: ReportLeverageLine::ExchangeMultiple,
            order_events: None,
            dedup_order_events: false,
        }
    }
}

/// 来源/交易所 → 通知档案注册表（启动期一次构造，运行期只读）。
#[derive(Debug, Clone)]
pub struct NotifyProfiles {
    by_source: BTreeMap<&'static str, VenueNotifyProfile>,
}

impl NotifyProfiles {
    /// 按两个独立 CLI 阈值构造注册表（均为比率口径，如 -0.20）。
    ///
    /// 登记项见计划 §5；未登记交易所走 [`VenueNotifyProfile::default_profile`]。
    pub fn from_config(deribit_roi: Decimal, ctp_roi: Decimal) -> Self {
        // CTP 主档案与物理腿通知档案（`ctp-leg`）除告警阈值外完全一致：
        // 腿事件标题/文案相同，exchange 仍为 "ctp" 供 ROI 查本地仓位。
        let ctp_profile = |alert_threshold: Option<Decimal>| VenueNotifyProfile {
            exchange: "ctp",
            close_roi: CloseRoiBasis::Margin,
            alert_threshold,
            report_source: ReportSource::LocalPositions,
            report_leverage: ReportLeverageLine::MarginRatio,
            order_events: Some(OrderEventBroadcast {
                title: "CtpPositions",
                instrument_type: InstrumentType::Futures,
            }),
            dedup_order_events: true,
        };
        let by_source = BTreeMap::from([
            (
                "deribit",
                VenueNotifyProfile {
                    exchange: "deribit",
                    close_roi: CloseRoiBasis::Cost,
                    alert_threshold: Some(deribit_roi),
                    report_source: ReportSource::ExchangeSnapshot,
                    report_leverage: ReportLeverageLine::ExchangeMultiple,
                    order_events: Some(OrderEventBroadcast {
                        title: "DeribitPositions",
                        instrument_type: InstrumentType::Option,
                    }),
                    dedup_order_events: false,
                },
            ),
            ("ctp", ctp_profile(Some(ctp_roi))),
            // 一单两腿（SHFE 平今/平昨）的物理腿通知专用来源；不参与告警。
            ("ctp-leg", ctp_profile(None)),
        ]);
        Self { by_source }
    }

    /// 取来源/交易所档案；未登记来源返回默认档案（改造前行为）。
    pub fn get(&self, source: &str) -> VenueNotifyProfile {
        self.by_source
            .get(source)
            .copied()
            .unwrap_or_else(VenueNotifyProfile::default_profile)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn registry_deribit_profile_matches_legacy_behavior() {
        // deribit 档案必须与引入档案前完全一致：成本口径 ROI、交易所快照播报、
        // 杠杆倍数行、DeribitPositions 订单播报、不去重。
        let profiles = NotifyProfiles::from_config(dec("-0.20"), dec("-0.30"));
        let p = profiles.get("deribit");

        assert_eq!(p.exchange, "deribit");
        assert_eq!(p.close_roi, CloseRoiBasis::Cost);
        assert_eq!(p.alert_threshold, Some(dec("-0.20")));
        assert_eq!(p.report_source, ReportSource::ExchangeSnapshot);
        assert_eq!(p.report_leverage, ReportLeverageLine::ExchangeMultiple);
        assert_eq!(
            p.order_events,
            Some(OrderEventBroadcast {
                title: "DeribitPositions",
                instrument_type: InstrumentType::Option,
            })
        );
        assert!(!p.dedup_order_events);
    }

    #[test]
    fn registry_ctp_profile_uses_margin_and_local_positions() {
        // CTP 档案：保证金口径 ROI、本地仓位播报、保证金比例行、
        // CtpPositions 订单播报（合约）、需要去重。
        let profiles = NotifyProfiles::from_config(dec("-0.20"), dec("-0.30"));
        let p = profiles.get("ctp");

        assert_eq!(p.exchange, "ctp");
        assert_eq!(p.close_roi, CloseRoiBasis::Margin);
        assert_eq!(p.alert_threshold, Some(dec("-0.30")));
        assert_eq!(p.report_source, ReportSource::LocalPositions);
        assert_eq!(p.report_leverage, ReportLeverageLine::MarginRatio);
        assert_eq!(
            p.order_events,
            Some(OrderEventBroadcast {
                title: "CtpPositions",
                instrument_type: InstrumentType::Futures,
            })
        );
        assert!(p.dedup_order_events);

        // 物理腿通知来源：与 CTP 主档案同文案/同去重，但不参与告警。
        let leg = profiles.get("ctp-leg");
        assert_eq!(leg.exchange, "ctp");
        assert_eq!(leg.order_events, p.order_events);
        assert!(leg.dedup_order_events);
        assert_eq!(leg.alert_threshold, None);
    }

    #[test]
    fn unknown_exchange_gets_legacy_default_profile() {
        // 未登记交易所（binance/hyperliquid 及其它）：默认档案 = 改造前行为。
        let profiles = NotifyProfiles::from_config(dec("-0.20"), dec("-0.30"));
        for exchange in ["binance", "hyperliquid", "unknown-venue"] {
            let p = profiles.get(exchange);
            assert_eq!(p.exchange, "", "exchange={exchange}");
            assert_eq!(p.close_roi, CloseRoiBasis::Cost, "exchange={exchange}");
            assert_eq!(p.alert_threshold, None, "exchange={exchange}");
            assert_eq!(
                p.report_source,
                ReportSource::ExchangeSnapshot,
                "exchange={exchange}"
            );
            assert_eq!(
                p.report_leverage,
                ReportLeverageLine::ExchangeMultiple,
                "exchange={exchange}"
            );
            assert_eq!(p.order_events, None, "exchange={exchange}");
            assert!(!p.dedup_order_events, "exchange={exchange}");
        }
    }

    #[test]
    fn alert_thresholds_are_independent_per_venue() {
        // deribit 与 CTP 阈值互不影响（CLI 两个参数各自生效）。
        let profiles = NotifyProfiles::from_config(dec("-0.05"), dec("-0.75"));
        assert_eq!(profiles.get("deribit").alert_threshold, Some(dec("-0.05")));
        assert_eq!(profiles.get("ctp").alert_threshold, Some(dec("-0.75")));
        // 未登记交易所不受任何阈值影响。
        assert_eq!(profiles.get("binance").alert_threshold, None);
    }
}
