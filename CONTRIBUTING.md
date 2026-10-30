# 贡献指南

感谢你对本项目的关注！请先阅读 [README.md](README.md) 了解项目结构与核心设计。

## 开发环境

- Rust 1.80+（见 `rust-toolchain.toml`）

常用命令（来源：workspace `Cargo.toml`、`crates/trading-service/Cargo.toml`、`scripts/`）：

| 命令 | 说明 |
| --- | --- |
| `cargo build --workspace` | 构建全部 crate（默认 feature） |
| `cargo build -p trading-service --features ctp` | 构建含 CTP 期货适配的服务 |
| `cargo test --workspace` | 默认测试集（**不含** `ctp` feature 代码） |
| `cargo test -p trading-exchange --features ctp ctp` | CTP 适配层单测 |
| `cargo test -p trading-service --features ctp` | 含 CTP feature 的服务测试 |
| `cargo clippy --workspace --all-targets -- -D warnings` | 全目标 lint（含测试代码） |
| `cargo fmt --all -- --check` | 格式检查 |
| `scripts/build-ctp-mock.sh` | 构建 CTP 模拟器 `.so` 与 `ctp-mock-ctl`（联调 / 历史 tick 回放） |

> `ctp` 不是默认 feature：`cargo test --workspace` 不会编译 CTP 适配代码。改动
> `crates/trading-exchange/src/ctp/**` 或 `trading-service` 的 CTP 路径时，必须额外跑带
> `--features ctp` 的测试。

## 提交规范

采用 Conventional Commits 格式：

```
<type>: <description>

<optional body>
```

type 取值：`feat` / `fix` / `refactor` / `docs` / `test` / `chore` / `perf` / `ci`

## PR 流程

1. 从 `main` 拉出 feature 分支；
2. 实现 + 测试——**每个 PR 必须包含完整、逻辑清晰的测试代码**，方便 reviewer 审核；
3. 提交前自查：
   - `cargo clippy --workspace --all-targets -- -D warnings` 零警告
   - `cargo fmt --all -- --check` 通过
   - `cargo test --workspace` 全绿；涉及 CTP 时补 `--features ctp`
4. PR 描述写清：动机、方案要点、测试方式。

## 代码约定（摘自项目原则）

- **可测性**：模块必须可测、易测；业务计算优先写成纯函数；
- **扩展性**：新增功能预留扩展点，拒绝打补丁式堆叠；
- **中文注释**：代码注释使用中文；
- **不使用 channel**：任务间通过共享状态协作，内存管理统一走 trading-store；
- **定点记账**：金额/价格/数量一律 `rust_decimal::Decimal`，禁用 f64。

## 适配新交易所

参考 [docs/扩展交易所.md](docs/扩展交易所.md)：实现 `trading-exchange` 的
`VenueProvider` 契约后经 registry 注册即可，无需改动服务层。

## 报告问题

提 issue 时请附：复现步骤、期望行为、日志片段（**脱敏后**的）与环境信息。
切勿在 issue 中粘贴真实 API key、账户名或 webhook 地址。
