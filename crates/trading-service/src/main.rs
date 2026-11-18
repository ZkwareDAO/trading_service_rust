//! trading-service 二进制入口（薄壳）。
//!
//! 启动逻辑已迁入库的 [`trading_service::run`]，二进制只负责拉起 tokio 运行时；
//! 这样集成测试可复用同一装配路径（见 `tests/e2e`）。

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    trading_service::run().await
}
