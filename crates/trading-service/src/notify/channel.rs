//! 通知通道与 Webhook URL 对照表。
//!
//! 把「通道」抽象成枚举（[`NotifyChannel`]），URL 统一登记进 [`ChannelUrls`]，
//! 避免通道增多时构造函数位置参数膨胀。文档约定的「`manual_close_url` 留空回退
//! `close_url`」收口在 [`ChannelUrls::target_url`] 一处。

use std::collections::HashMap;
use std::sync::Arc;

/// 通知通道（与企业微信群机器人 Webhook 一一对应）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NotifyChannel {
    /// 开仓成交通知。
    Open,
    /// 风控平仓成交通知。
    Close,
    /// Deribit 订单状态播报 / 仓位定时播报。
    Test,
    /// 期权价差告警（需人工平仓）。
    ManualClose,
    /// Deribit 订单事件播报（通知 A，独立 `position_notify_url`）。
    Position,
}

/// 通道 → Webhook URL 对照表（廉价克隆，内部 `Arc<HashMap>`，clone 仅增引用）。
#[derive(Debug, Clone)]
pub struct ChannelUrls {
    urls: Arc<HashMap<NotifyChannel, String>>,
}

impl ChannelUrls {
    /// 由五个通道的 URL 构造（空串=未配置）。
    pub fn new(
        open: String,
        close: String,
        test: String,
        manual_close: String,
        position: String,
    ) -> Self {
        let raw = [
            (NotifyChannel::Open, open),
            (NotifyChannel::Close, close),
            (NotifyChannel::Test, test),
            (NotifyChannel::ManualClose, manual_close),
            (NotifyChannel::Position, position),
        ];
        // 统一 trim；空串不进表，target_url 据此判定「未配置」。
        let urls = raw
            .into_iter()
            .map(|(ch, url)| (ch, url.trim().to_string()))
            .filter(|(_, url)| !url.is_empty())
            .collect();
        Self {
            urls: Arc::new(urls),
        }
    }

    /// 取通道的实际投递 URL：
    /// * [`NotifyChannel::ManualClose`] 未配置时**回退** [`NotifyChannel::Close`]；
    /// * 其余通道未配置返回 None（对应消息静默跳过）。
    pub fn target_url(&self, channel: NotifyChannel) -> Option<&str> {
        if let Some(url) = self.urls.get(&channel) {
            return Some(url);
        }
        if channel == NotifyChannel::ManualClose {
            return self.urls.get(&NotifyChannel::Close).map(String::as_str);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn urls(
        open: &str,
        close: &str,
        test: &str,
        manual_close: &str,
        position: &str,
    ) -> ChannelUrls {
        ChannelUrls::new(
            open.to_string(),
            close.to_string(),
            test.to_string(),
            manual_close.to_string(),
            position.to_string(),
        )
    }

    #[test]
    fn target_url_returns_configured_channel() {
        let u = urls(
            "http://open",
            "http://close",
            "http://test",
            "http://manual",
            "http://position",
        );
        assert_eq!(u.target_url(NotifyChannel::Open), Some("http://open"));
        assert_eq!(u.target_url(NotifyChannel::Close), Some("http://close"));
        assert_eq!(u.target_url(NotifyChannel::Test), Some("http://test"));
        assert_eq!(
            u.target_url(NotifyChannel::ManualClose),
            Some("http://manual")
        );
        assert_eq!(
            u.target_url(NotifyChannel::Position),
            Some("http://position")
        );
    }

    #[test]
    fn empty_url_is_none() {
        // 空串视为未配置。
        let u = urls("", "", "", "", "");
        assert_eq!(u.target_url(NotifyChannel::Open), None);
        assert_eq!(u.target_url(NotifyChannel::Test), None);
    }

    #[test]
    fn manual_close_falls_back_to_close_when_blank() {
        // 文档约定：manual_close_url 留空 → 回退 close_url。
        let u = urls("http://open", "http://close", "", "", "");
        assert_eq!(
            u.target_url(NotifyChannel::ManualClose),
            Some("http://close")
        );
    }

    #[test]
    fn manual_close_and_close_both_blank_is_none() {
        let u = urls("", "", "", "", "");
        assert_eq!(u.target_url(NotifyChannel::ManualClose), None);
    }

    #[test]
    fn manual_close_does_not_fall_back_when_set() {
        // 显式配置 manual_close 时不得回退（两群可能不同）。
        let u = urls("", "http://close", "", "http://manual", "");
        assert_eq!(
            u.target_url(NotifyChannel::ManualClose),
            Some("http://manual")
        );
    }
}
