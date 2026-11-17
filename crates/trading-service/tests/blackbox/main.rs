//! 进程级黑盒 smoke：启动真实 `trading-service` 与 `xili-exchange` 二进制，
//! 只经 REST 验证进程/协议边界（启动、账户、下单成交、规则写入、停机恢复）。
//!
//! 业务语义场景在 `tests/e2e`（in-process + trading-sim）；xili 不推仓位快照，
//! 因此本目标不覆盖持仓流水线，只覆盖订单与进程边界。

mod support;

use serde_json::{json, Value};
use std::time::{Duration, Instant};

const EXCHANGE: &str = "xili";
const ACCOUNT: &str = "e2e";
const INSTRUMENT: &str = "BTCUSDT";

/// 轮询 GET，直到谓词成立或超时；失败时打印最后一次响应。
async fn wait_json(
    client: &support::RestClient,
    path: &str,
    pred: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let v = client.get_json(path).await;
        if pred(&v) {
            return v;
        }
        if Instant::now() >= deadline {
            panic!("等待 `{path}` 超时；最后一次响应={v}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// POST 市价单并返回服务端生成的 client_order_id。
async fn place_market(client: &support::RestClient) -> String {
    let resp = client
        .post_json(
            "/api/v1/orders",
            json!({
                "exchange": EXCHANGE,
                "account": ACCOUNT,
                "instrument": INSTRUMENT,
                "instrument_type": "futures",
                "side": "buy",
                "order_type": "market",
                "quantity": 1
            }),
        )
        .await;
    assert_eq!(resp["code"], 0, "下单失败：{resp}");
    resp["data"]["client_order_id"]
        .as_str()
        .unwrap_or_else(|| panic!("下单响应缺少 client_order_id：{resp}"))
        .to_string()
}

fn filled_order_path(cid: &str) -> String {
    format!("/api/v1/orders?exchange={EXCHANGE}&account={ACCOUNT}&client_order_id={cid}")
}

fn is_filled(v: &Value) -> bool {
    v["code"] == 0
        && v["data"]["list"]
            .as_array()
            .is_some_and(|list| list.iter().any(|o| o["status"] == "FILLED"))
}

#[tokio::test]
async fn boot_exposes_health_accounts_and_state() {
    // Arrange + Act：起真实二进制（内部会等到 /health 就绪）。
    let stack = support::TestStack::start().await;
    let client = stack.client();

    // Assert：健康探针、账户投影、状态摘要都可用。
    let health = client.get("/health").await;
    assert_eq!(health.status(), 200);

    let accounts = client
        .get_json(&format!(
            "/api/v1/accounts?exchange={EXCHANGE}&name={ACCOUNT}"
        ))
        .await;
    assert_eq!(accounts["code"], 0, "{accounts}");
    assert_eq!(
        accounts["data"]["list"].as_array().map(Vec::len),
        Some(1),
        "{accounts}"
    );

    let state = client.get_json("/api/v1/state").await;
    assert_eq!(state["code"], 0, "{state}");
    assert!(!state["data"].is_null(), "{state}");
}

#[tokio::test]
async fn market_order_fills() {
    // Arrange
    let stack = support::TestStack::start().await;
    let client = stack.client();

    // Act：经真实 REST 下市价单，xili 自动撮合。
    let cid = place_market(&client).await;

    // Assert：系统订单收敛到 FILLED。
    wait_json(&client, &filled_order_path(&cid), is_filled).await;
}

#[tokio::test]
async fn rule_upsert_is_visible_over_rest() {
    // Arrange
    let stack = support::TestStack::start().await;
    let client = stack.client();

    // Act：创建一条价格触发的开仓规则。
    let created = client
        .post_json(
            "/api/v1/rules",
            json!({
                "account": ACCOUNT,
                "exchange": EXCHANGE,
                "rule_type": "new",
                "instrument": INSTRUMENT,
                "instrument_type": "futures",
                "side": "buy",
                "condition_kind": "price",
                "operator": "<=",
                // 阈值取永不成立值：本用例只验证写入后可查回，
                // 不依赖 xili 当前是否具备行情推送。
                "value": 1,
                "action": "self",
                "order_type": "market",
                "quantity": 1
            }),
        )
        .await;
    assert_eq!(created["code"], 0, "创建规则失败：{created}");
    let id = created["data"]["id"]
        .as_u64()
        .unwrap_or_else(|| panic!("创建响应缺少 id：{created}"));

    // Assert：按 id 精确查回且条件字段一致。
    let listed = wait_json(
        &client,
        &format!("/api/v1/rules?exchange={EXCHANGE}&account={ACCOUNT}&id={id}"),
        |v| {
            v["data"]["list"]
                .as_array()
                .is_some_and(|list| !list.is_empty())
        },
    )
    .await;
    let rule = &listed["data"]["list"][0];
    assert_eq!(rule["id"], json!(id), "{listed}");
    assert_eq!(rule["condition_kind"], "price", "{listed}");
    assert_eq!(rule["rule_type"], "new", "{listed}");
}

#[tokio::test]
async fn graceful_shutdown_and_restart_recover_order() {
    // Arrange：第一阶段落一笔已成交订单。
    let mut stack = support::TestStack::start().await;
    let cid = place_market(&stack.client()).await;
    wait_json(&stack.client(), &filled_order_path(&cid), is_filled).await;

    // Act：SIGINT 优雅停机，再以同一数据目录重启服务。
    stack.stop_service_graceful().await;
    stack.start_service().await;

    // Assert：订单从落盘恢复，终态不倒退。
    wait_json(&stack.client(), &filled_order_path(&cid), is_filled).await;
}
