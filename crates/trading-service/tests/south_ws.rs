//! 南向 WS 接口契约测试（crate 级真实临时端口）。
//!
//! 覆盖：dev 旁路鉴权升级、严格 HMAC 验签失败/成功/nonce 重放、非法命令
//! 报文回错误帧。业务命令路由的正确性由 `tests/e2e` 场景与 service 单测覆盖。

use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use store_core::MockClock;
use tempfile::TempDir;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use trading_core::southauth;

use trading_service::testkit::{
    app_with_accounts, serve_south_ws, NonceGuard, SouthAuth, NONCE_TTL,
};

/// dev 旁路：只认 strategy_name，握手成功；非法报文回错误帧。
#[tokio::test]
async fn dev_bypass_upgrades_and_replies_error_frame_for_bad_command() {
    let dir = TempDir::new().expect("temp dir");
    let app = app_with_accounts(dir.path(), "").await;
    let base = serve_south_ws(&app.state).await;

    let (mut ws, _) = connect_async(format!("{base}?strategy_name=s1"))
        .await
        .expect("dev 旁路应允许无签名升级");

    ws.send(Message::Text("{not-json}".into()))
        .await
        .expect("send bad command");

    let frame = ws.next().await.expect("应有应答帧").expect("帧可读");
    let text = match frame {
        Message::Text(t) => t.to_string(),
        other => panic!("期望文本错误帧，实际 {other:?}"),
    };
    assert!(
        text.contains("报文解析失败"),
        "非法命令应回解析错误，实际：{text}"
    );
    let _ = ws.close(None).await;
}

/// 严格模式：无签名 / 错签名 / 重放 nonce 均 401，合法签名可升级。
#[tokio::test]
async fn strict_hmac_rejects_missing_bad_and_replayed_signatures() {
    const SECRET: &str = "shared-secret";

    let dir = TempDir::new().expect("temp dir");
    let app = app_with_accounts(dir.path(), "").await;
    // 换成严格鉴权的同一 AppState（字段 pub，克隆后仅替换鉴权门面）。
    let mut state = app.state.clone();
    let clock: Arc<dyn store_core::Clock> = Arc::new(MockClock::new(1_700_000_000_000));
    state.south_auth = SouthAuth::from_secret(SECRET, NonceGuard::new(clock, NONCE_TTL), false);
    let base = serve_south_ws(&state).await;

    // 缺 nonce/signature → 升级前拒绝。
    assert!(
        connect_async(format!("{base}?strategy_name=s1"))
            .await
            .is_err(),
        "严格模式缺签名必须 401"
    );

    // 错签名 → 拒绝。
    let nonce = southauth::generate_nonce();
    assert!(
        connect_async(format!(
            "{base}?strategy_name=s1&nonce={nonce}&signature=deadbeef"
        ))
        .await
        .is_err(),
        "错误签名必须 401"
    );

    // 合法签名 → 升级成功；同一 nonce 再连 = 重放 → 拒绝。
    let nonce = southauth::generate_nonce();
    let sig = southauth::sign(SECRET, "s1", &nonce);
    let url = format!("{base}?strategy_name=s1&nonce={nonce}&signature={sig}");
    let (mut ws, _) = connect_async(url.clone())
        .await
        .expect("合法签名应升级成功");
    assert!(
        connect_async(url).await.is_err(),
        "同一 nonce 重放必须被拒绝"
    );
    let _ = ws.close(None).await;
}
