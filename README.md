# trading_service_rust

多交易所交易服务（Rust）。一个 `trading-service` 居中：

- **南向 WS**：bind 端口对接多个策略客户端（HMAC 鉴权握手）；
- **交易所方向（一律主动外连）**：trading-service 始终作为客户端——REST 下单 + 外向
  「账户事件 WS」收订单回报。已接入 **binance（U 本位永续）/ deribit（期权）/
  hyperliquid** 真实适配器；本地 `exchange-mock` 模拟所用于联调；
- **管理 REST**：`/api/v1/*` 提供订单/仓位/规则/账户查询与下单、建规则等运维接口；
- **盯盘规则引擎**：定时扫描 active 规则，条件命中自动开仓/平仓，支持链式规则激活。

约束：**任务间不使用 channel**，全部通过共享状态（`DashMap` / `RwLock` / `Atomic*` /
共享 WS 写端）协作。内存 CRUD 与 CSV 持久化统一走 `store-core` / `trading-store`：
业务只改内存表，定时持久化由 tokio 后台任务执行（脏表才写、tmp + rename 原子替换），
主流程不碰文件 IO。

## 文档索引

| 文档 | 内容 |
| --- | --- |
| [docs/接口文档.md](docs/接口文档.md) | 南向 WS 协议 + 管理 REST 接口（合并版） |
| [docs/状态机设计.md](docs/状态机设计.md) | 订单 / 仓位 / 规则三个实体的状态机与流转 |
| [docs/rule设计.md](docs/rule设计.md) | 盯盘/风控规则语义与引擎设计 |
| [docs/交易规则.md](docs/交易规则.md) | 下单业务规则（取量、杠杆、幂等等） |
| [docs/内存和持久化管理.md](docs/内存和持久化管理.md) | 领域模型字段设计与内存表 / CSV 持久化（模型层权威文档） |
| [docs/行情订阅模型.md](docs/行情订阅模型.md) | 行情数据模型与订阅 |
| [docs/服务端与客户端连接.md](docs/服务端与客户端连接.md) | 南向连接鉴权设计 |
| [docs/deribit开平仓处理.md](docs/deribit开平仓处理.md) | deribit 期权开平仓处理 |
| [docs/扩展交易所.md](docs/扩展交易所.md) | 新增交易所适配器指南 |
| [docs/部署文档.md](docs/部署文档.md) | 生产部署手册 |

## Crate 组织（workspace）

| 路径 | 角色 |
| --- | --- |
| `crates/trading-models` | 库：Account / Order / Position / WatchRule 领域模型与枚举（定点 Decimal，统一实现 `Model`），对账 diff 裁决等纯计算 |
| `crates/store-core` | 库：通用内存表（表锁 + 行子锁）、`Model` 约束、CSV 快照原子读写、跨日 rollover |
| `crates/trading-store` | 库：业务装配门面，注册各内存表 + `OrderRepo`/`PositionRepo`/`RuleRepo` 等业务状态机仓储；`src/async_rt.rs` 为 store-core 的 tokio 适配（`spawn_blocking` 恢复/落盘 + 定时任务） |
| `crates/trading-core` | 库：定点 Decimal 线格式、ID/时间工具、WS/REST 协议（模型类型来自 trading-models） |
| `crates/trading-exchange` | 库：交易所接入层。统一契约（`api`：`ExchangeApi`/`AccountEventStream`/`MarketDataStream`/`RuleFetcher`）、对宿主的出站端口（`port`：凭证解析/订单回报）、纯映射/退避/心跳/对账内核、`binance`/`deribit`/`hyperliquid` 适配器 + `xili` 本地模拟所适配器、`provider` 装配工厂、`runtime` 账户连接监督引擎；只向下依赖 trading-core/trading-models，不反向依赖服务 |
| `crates/trading-service/src/main.rs` | 二进制 **trading-service**：解析配置 → 实现出站端口 → 构造交易所注册表 → 南向 bind WS、管理 REST 与业务编排 |
| `crates/trading-service/src/bin/exchange-mock.rs` | 二进制 **exchange-mock**：固定端口同时提供 REST + `/ws` 账户事件，按节奏模拟成交 |
| `crates/trading-service/src/bin/strategy-mock.rs` | 二进制 **strategy-mock**：WS 客户端，发一条命令并打印全部回报 |
| `crates/trading-service/src/bin/strategy-client.rs` | 二进制 **strategy-client**：参考策略客户端（含 HMAC 签名与各命令参数装配） |
| `crates/trading-service/src/bin/account-tool.rs` | 二进制 **account-tool**：账户管理（`add-account` 追加/更新账户、密钥生成、secret 加密；交互式问答或参数直传） |

三个二进制同属一个 package，因此统一用 `cargo run -p trading-service --bin <名字>` 启动。

## 核心业务流程

1. 策略客户端携带 HMAC 鉴权参数连接南向 WS
   （协议见 [docs/接口文档.md](docs/接口文档.md) 第一部分），发送
   `place_order / cancel_order / get_order / get_positions / create_rule`。
2. 服务在内存表创建订单（状态 `PLACING`，生成 `0x`+32hex 的 `client_order_id`），
   `spawn` 一个 task 调用交易所 REST 下单；受理/成交回报经外向「账户事件 WS」推进
   内存订单状态机，并按 `strategy_name` 路由回南向连接。
3. 仓位不由下单直接维护，而由对账流水线产生：ACCOUNT_UPDATE 脏信号去抖对账 +
   定时全量对账 + （重）连补发，公共行情每笔最新价同步重估活跃仓位
   （见 [docs/状态机设计.md](docs/状态机设计.md) §2）。
4. 盯盘规则引擎周期扫描 active 规则（roi/price/position/profit_drawdown 四类条件），
   命中后开仓（`new`）或反向 reduce_only 平仓（`risk`），支持 `rule:<id>` 链式激活。
5. 后台任务按 `--flush-interval` 把**脏表**原子（tmp + rename）写到
   `data/{YYYY-MM-DD}/{账户名|_system}/{model}.csv`；
   启动时只恢复最新数据日快照，UTC 跨日清理前一日软删行并归档。

价格/数量使用 `rust_decimal::Decimal`，线上为精确 JSON 数字（arbitrary precision，禁用 f64）。

## 运行（三个终端，本地模拟联调）

```bash
# 终端 1：交易所模拟器（服务端：固定端口同时提供 REST + /ws 账户事件；可起多个，实例名/端口不同）
cargo run -p trading-service --bin exchange-mock -- \
  --mock-instance demo_venue --addr 127.0.0.1:9200

# 终端 2：核心服务（只 bind 南向；用 --mock 静态告知模拟所地址并主动外连，可重复多个）
cargo run -p trading-service --bin trading-service -- \
  --south-addr 127.0.0.1:9100 \
  --mock demo_venue=http://127.0.0.1:9200 \
  --data-dir ./data \
  --flush-interval 5s

# 终端 3：策略模拟器（可启动多个，--strategy_name 不同）
cargo run -p trading-service --bin strategy-mock -- \
  --strategy_name test_strategy \
  --exchange_mock demo_venue \
  --account test_binance \
  --instrument btcusdt \
  --quantity 1 \
  --side long \
  --price 80000
```

> CLI 同时接受下划线与连字符形式（clap 默认两者等价），例如 `--mock_instance` 或 `--mock-instance`。

### 其它命令

```bash
# 分多次成交，观察 PARTIAL -> FILLED
cargo run -p trading-service --bin exchange-mock -- \
  --mock_instance demo_venue --order_fill_times 5 --order_fill_interval 10s

# 撤单（需要上一笔返回的 client_order_id）
cargo run -p trading-service --bin strategy-mock -- \
  --option cancel_order --strategy_name test_strategy --exchange_mock demo_venue \
  --client_order_id 0x....

# 查持仓
cargo run -p trading-service --bin strategy-mock -- \
  --option get_positions --exchange_mock demo_venue --account test_binance
```

## 南向连接鉴权

南向策略连接默认强制 HMAC-SHA256 握手鉴权（协议细节见 [docs/接口文档.md](docs/接口文档.md) 第一部分 §2，
设计见 [docs/服务端与客户端连接.md](docs/服务端与客户端连接.md)）：

- 所有策略共用一个**共享密钥**，存于 `<data-dir>/south.key`（64 个十六进制字符 =
  32 随机字节，权限 `0600`，首启自动生成）。新增策略无需逐名配置，只需离线拿到同一密钥。
- 握手参数在 WebSocket 升级前校验，非法请求统一返回 `401 unauthorized`（不区分原因）：

  ```
  signature = hex_lower(HMAC_SHA256(secret, strategy_name || nonce))
  ws://<south-addr>/ws?strategy_name=<name>&nonce=<32hex>&signature=<64hex>
  ```

  签名用恒定时间比较；`nonce` 一次性、5 分钟 TTL，重放即拒（过期条目后台 60s 清扫）。
- `strategy-mock` 默认读 `./data/south.key` 自动签名，可用 `--secret_file <path>` 指定。

仅本地联调可临时关闭验签（**严禁用于生产**，启动会打 ERROR 告警）：

```bash
# 服务端
cargo run -p trading-service --bin trading-service -- --south-dev-insecure
# 客户端（不带签名）
cargo run -p trading-service --bin strategy-mock -- --insecure
```

生产部署应在 TLS（`wss://`）之后承载，防止密钥相关参数在传输中被窃听或篡改。

## 账户热加载（对接真实交易所的前置）

账户**不读环境变量**，来源是人工可编辑的 `<data-dir>/account.csv`：

```
id,name,exchange,api_key,api_secret,api_passphrase,position_mode,is_testnet,deleted,created_at,updated_at
0,test_binance,binance,YOUR_API_KEY,your-secret,,one_way,0,0,0,0
```

- 重载经管理 API `POST /admin/accounts/reload` 显式触发；交易所连接随后按重载版本停旧起新（对账周期 `--account-reload-interval`，默认 2s）。
- 新增账户即由 supervisor 拉起其用户数据流连接；删除/`deleted=1` 即停用连接；坏文件保留旧表。
- `is_testnet=1` 时该账户的**下单 REST 与账户事件流**走用户级测试网（如 `testnet.binancefuture.com`）；公共行情恒定正式网。
- `position_mode`：`one_way` / `hedge`。
- `api_secret` 支持明文（仅联调，启动会告警并在内存归一化）或加密形态 `enc.v1.*`（AES-256-GCM，主密钥在 `<data-dir>/account.key`，0600，首启自动生成）。模板见 `deploy/account.example.csv`，账户管理工具见 `account-tool`。
- 下单校验：真实 venue（如 binance）必须有名称与交易所都匹配的活动账户，否则直接回 `REJECTED`；本地模拟所账户可选。
- `account.csv`、`*.key` 已在 `.gitignore`，禁止入库。

## 仓位对账与行情重估

仓位流水线分四层：纯计算在 **trading-models**（`Position::revalued` / `revalue_positions` / `diff_positions`），内存读写在 **trading-store** 的 `PositionRepo`，交易所在 **trading-exchange**，编排只在 **trading-service** 的 `PositionEngine`。

- **同步对账**：ACCOUNT_UPDATE 脏信号经去抖窗口合并，到点后异步拉一次 `get_positions` REST 真值再对账落库；另有定时全量兜底与（重）连补发。
  - `--position-debounce`：去抖窗口，默认 `500ms`。
  - `--position-reconcile-interval`：定时全量周期，默认 `60s`（无信号也强制全量）。
  - 外部 REST 延迟在独立任务中执行，不阻塞订单回报；同账户在途拉取用原子标记防重叠。
- **行情重估**：公共行情每写一笔最新价即同步回调，只刷新「同交易所 + 同产品类型 + 同标的」的活跃仓位（exchange 不同的仓位不受影响）。

### WebSocket 代理与连接超时

统一建连器 `trading-exchange/src/ws.rs` 收口所有出站 WS（公共行情、私有账户流等）：

- `tokio-tungstenite` 默认不读代理变量；建连器按下列环境变量决定是否经 HTTP `CONNECT` 隧道（再在隧道上做 rustls TLS）：
  - `wss://` 读 `HTTPS_PROXY`/`https_proxy`，`ws://` 读 `HTTP_PROXY`/`http_proxy`；
  - `NO_PROXY`/`no_proxy` 支持 `*` 与主机后缀；`localhost`、`127.*`、`::1` **无条件直连**；
  - 仅支持 HTTP 代理（含 `http://user:pass@host:port` Basic 鉴权），不支持 socks。
- TCP/隧道/TLS/WS 握手总超时 10s，避免建连无限挂起；连上后的死连由各流心跳空闲超时判定重连（公共行情 10s ping / 20s 空闲，私有账户流 20s / 60s）。
- 若公共行情「connected 但长时间无帧」，多为代理对该域名的流量限制，需在代理侧放行或换出口；REST 对账不受影响，仍会按全量周期刷新仓位。

## 部署

生产环境一键部署（/opt 一体化布局，详见 [docs/部署文档.md](docs/部署文档.md)）：

```bash
sudo ./deploy/deploy.sh      # 构建并安装到 /opt/trading_service_rust，注册 systemd 并启动
```

- 安装目录属主为执行 sudo 的用户，日常管理用 `trading-service {status|log|reload|account|restart}`（管理入口已软链到 PATH，`account` 即交互式添加账户）；
- `account.csv` 首次部署自动创建仅含表头的文件；添加账户用
  `/opt/trading_service_rust/bin/account-tool add-account`（逐项提问、secret 不回显、
  写入前自动加密、可选测试网），落盘后 `trading-service reload` 热加载；
- **代理继承**：部署脚本自动探测执行用户的代理环境（`http_proxy`/`https_proxy`
  环境变量，或 `~/.proxy_env` 文件）注入 systemd 单元；直连外网的机器保持为空。
  注意 binance 等为 https/wss 目标，`https_proxy` 必须存在；
- 升级即重跑部署脚本（配置与数据保留）；卸载用 `sudo ./deploy/uninstall.sh`。

## 运行参数（trading-service CLI）

<!-- AUTO-GENERATED: 来源 crates/trading-service/src/config.rs Args 结构，改动参数时同步本表 -->

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--south-addr` | `127.0.0.1:9100` | 南向 WS 监听地址（策略客户端接入） |
| `--exchange` | （可重复） | 本地模拟所端点，格式 `name=url` |
| `--data-dir` | `./data` | 数据目录（账本快照、`south.key`、`account.csv` 默认位于此） |
| `--account-file` | 空 | 账户文件路径（缺省取 `<data-dir>/account.csv`） |
| `--account-key-file` | 空 | 账户主密钥路径（缺省取 `<data-dir>/account.key`，首启自动生成） |
| `--south-secret-file` | 空 | 南向 HMAC 密钥路径（缺省取 `<data-dir>/south.key`，首启自动生成 0600） |
| `--flush-interval` | `5s` | 脏表定时落盘周期 |
| `--rest-timeout` | `5s` | 交易所 REST 超时 |
| `--account-reload-interval` | `2s` | 账户表对账周期（重载版本变化即停旧起新） |
| `--rule-refresh-interval` | `720h` | 交易所规则表刷新周期 |
| `--rule-scan-interval` | `200ms` | 盯盘规则扫描周期 |
| `--position-debounce` | `500ms` | ACCOUNT_UPDATE 去抖窗口 |
| `--position-reconcile-interval` | `60s` | 仓位定时全量对账周期 |
| `--venue-config` | `./config/venue.toml` | 交易所可调参数文件（缺失用内置默认） |
| `--south-dev-insecure` | `false` | 关闭南向 HMAC 验签（**严禁生产**，启动打 ERROR） |
| `--notify-open-url` | 空 | 企业微信「开仓成交」Webhook（空=关闭） |
| `--notify-close-url` | 空 | 企业微信「风控平仓成交」Webhook |
| `--notify-test-url` | 空 | 企业微信「Deribit 订单状态/仓位定时播报」Webhook |
| `--notify-manual-close-url` | 空 | 企业微信「需人工平仓告警」Webhook（空=回退 close） |
| `--notify-position-url` | 空 | 企业微信「Deribit 订单事件播报」Webhook |
| `--roi-alert-threshold-pct` | `-20` | 仓位播报的 ROI 告警阈值（百分点） |

<!-- END AUTO-GENERATED -->

## 环境变量

<!-- AUTO-GENERATED: 来源 deploy/deploy.sh 与 crates/trading-service 源码引用，改动时同步 -->

| 变量 | 必需 | 说明 |
| --- | --- | --- |
| `http_proxy` / `https_proxy` | 否 | 出站代理（binance/deribit 为 https/wss 目标，`https_proxy` 必需才走代理）；部署脚本自动从执行用户环境或 `~/.proxy_env` 注入 systemd 单元 |
| `no_proxy` | 否 | 直连例外列表；`localhost`、`127.*`、`::1` 恒直连（代码内建） |
| `TRADING_DATA_DIR` | 否 | account-tool 默认路径基准目录（manage.sh 自动导出；未设时回退 `/opt/.../data` 探测，再回退 `./data`） |

<!-- END AUTO-GENERATED -->

## 测试

```bash
cargo test --workspace
cargo clippy --workspace -- -D warnings
cargo fmt --check
```

## 贡献

PR 欢迎提交前阅读 [CONTRIBUTING.md](CONTRIBUTING.md)（提交规范、测试要求、代码约定）。

## 范围说明

已实现：南向命令与回报路由、binance/deribit/hyperliquid 适配器、订单/仓位/规则内存状态机与
CSV 异步落盘、重启恢复、账户热加载、管理 REST、盯盘规则引擎（含链式规则）、定点记账、
企业微信通知（开平仓播报/人工介入告警）。

进行中/留待后续：历史跨日数据查询接口、更多产品类型（现货余额端点等）、REST 校验补全与错误码细分。

---

## License

[Apache-2.0](LICENSE)。贡献前请阅读 [CONTRIBUTING.md](CONTRIBUTING.md)；
安全漏洞请勿走公开 issue，见 [SECURITY.md](SECURITY.md)。
