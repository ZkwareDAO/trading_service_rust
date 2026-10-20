# WatchRule 盯盘规则模型设计

> 对应代码：`crates/trading-models/src/watch_rule.rs`（结构体 `WatchRule` 及
> `WatchRuleType` / `WatchRuleStatus` / `WatchConditionKind` / `WatchRuleAction` / `WatchRuleParams`），
> 由 `crates/trading-store` 门面注册为第四张表，账户作用域，
> 持久化于 `{root}/{UTC日}/{账户名}/watch_rule.csv`。
>
> 持久化/并发机制与 account/order/position 完全一致，参见 [`内存和持久化管理.md`](./内存和持久化管理.md)。

## 1. 两类「规则」不要混淆

| | 本模型 `WatchRule`（策略/风控规则） | `OrderRule`（交易所下单规格） |
|---|---|---|
| 文件 | `watch_rule.rs` | `order_rule.rs` |
| 含义 | 挂在仓位上的开仓/止盈止损触发条件 | 交易所发布的 tick/步长/最小名义额等公开静态数据 |
| 来源 | 策略端创建 | 各交易所接口同步 |
| 主键 | `id`（框架分配） | 无，键为 `(exchange, instrument_type, symbol)` |
| 持久化 | **落 `watch_rule.csv`，参与日切恢复** | 纯内存，不落盘，重启重拉 |
| 外键关系 | `Order.rule_id` / `Position.rule_id` 指向 `WatchRule.id` | 下单前校验时按 symbol 查询 |

## 2. 生命周期

```
pending ──(被前一条链式规则激活)──> active ──(条件命中)──> in_use ──(平仓单终结)──> retired
                                     │                                          ▲
                                     └──────────────(撤单/作废)──────────────────┘
```

- `pending`：链式规则等待前一条命中后激活；
- `active`：生效中，每轮行情判定都检查（默认状态）；
- `in_use`：已命中、平仓单已下发尚未终结；
- `retired`：终态。

**`status` 与 `deleted` 的关系**：`status=retired` 是业务终态；`deleted=1` 是框架的
软删除标记（与 order/position 同一机制：同日快照仍保留以防重启复活，UTC 跨日
purge 物理清除）。规则终结时业务侧置 retired 并 `soft_delete`，日常
`get/list` 只对 `deleted=0` 的行可见。

## 3. 字段表（`watch_rule.csv`）

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | u64 | 表主键，id=0 插入时框架自动分配 |
| `position_id` | u64 | 对应仓位 id |
| `rule_type` | enum | `new` 开仓 / `risk` 风控（小写串；原稿字段名 `type`） |
| `condition_kind` | enum | `roi` / `price` / `position` / `profit_drawdown` |
| `condition_instrument` | string? | price_/position_ 条件的标的后缀；空=本仓位标的 |
| `operator` | string | 比较操作符：`>=` `<=` `>` `<` `==`，风控引擎解释 |
| `value` | decimal | 条件阈值，**定点 `rust_decimal::Decimal`**，禁 f64 |
| `action` | string | 命中动作，与 `rule_type` 正交：`self`（执行本规则自身）或 `rule:{id}`（转而激活另一条规则）。「执行自身」具体做什么由 `rule_type` 决定 |
| `status` | enum | `active` / `retired` / `pending` / `in_use`（小写串，默认 active） |
| `exchange` | string | new 单的交易所（risk 留空） |
| `account_id` | u64 | 所属账号（同时决定落盘账户目录） |
| `instrument` | string | new 单的目标代币 |
| `price` | decimal | 下单价格，定点（市价单为 0） |
| `quantity` | decimal | 下单数量，语义随 `rule_type`：`new`=开仓数量；`risk`=平仓**绝对数量**（按比例平仓时为 0，数量取 `quantity_pct`） |
| `order_type` | enum | 订单类型：new=开仓单类型，risk=平仓单类型；`limit` / `market` / `stop_market` / `take_market`（CSV 存大写串 `LIMIT` 等） |
| `quantity_pct` | decimal | risk 按当前持仓数量的比例减/平仓（比率：全平=1、半仓=0.5）；仅 risk 比例平仓使用，此时 `quantity=0`。与 `quantity` 互斥、至多一个非零；缺省 1 |
| `leverage` | u64 | 开仓杠杆：仅 `new` 规则消费（命中下单前先 `set_leverage`）；risk 不读。新增列，旧快照缺列按 1 恢复 |
| `instrument_type` | enum | 合约形状：`spot` 现货 / `futures`（Binance U 本位永续，本系统统一口径）/ `option` 期权 / `equity` 股票 / `fund` 基金；`perpetual` 为预留扩展、当前不产出（risk 规则默认 `futures`） |
| `side` | enum | **开仓方向**：`BUY`/`SELL`（CSV 大写串）。仅 `new` 规则消费（多=BUY/空=SELL）；risk 平仓方向由持仓反向推出，不读本字段（构造时留默认 `BUY`）。新增列，旧快照缺列按 `BUY` 恢复 |
| `deleted` | bool | **软删除：`0`=活跃状态，`1`=终止态**；`#[serde(with = "store_core::model::bool_u8")]`，与 Order/Position/Account 中的 deleted 完全一致 |
| `created_at` | u64 | 创建时间，UTC 毫秒，框架维护 |
| `updated_at` | u64 | 更新时间，UTC 毫秒，框架维护 |

> 原始设计中的 `condition_name`（`roi` / `price_{instrument}` /
> `position_{instrument}` / `profit_drawdown`）落盘时拆为 `condition_kind` +
> `condition_instrument` 两列，避免在字符串里解析业务键；
> `WatchRule::condition_name()` 仍可拼回文档原形式，`WatchConditionKind::parse_wire()`
> 可反向解析。

合成示例行（列序以 struct 实际序列化为准）：

```csv
id,position_id,rule_type,condition_kind,condition_instrument,operator,value,action,status,exchange,account_id,instrument,instrument_type,side,price,quantity,order_type,quantity_pct,leverage,deleted,created_at,updated_at
1,100,risk,roi,,">=",0.12,self,active,,7,,futures,BUY,0,0,MARKET,1,1,0,1726400000000,1726400000000
2,100,risk,price,BTCUSDT,"<=",90000,rule:3,pending,,7,,futures,BUY,0,0,LIMIT,1,1,0,1726400000000,1726400000000
3,0,new,roi,,,0,self,active,binance,7,BTCUSDT,futures,BUY,50000,0.1,LIMIT,1,2,0,1726400000000,1726400000000
```

- 行 1：risk、动作 self，按持仓**比例**全市价平仓 → `quantity=0`、`order_type=MARKET`、`quantity_pct=1`；
- 行 3：new、动作 self，按本行要素开仓 → `quantity=0.1` 即开仓数量，`quantity_pct` 不消费。

## 4. 平仓数量的两种口径（risk 规则）

risk 规则命中且动作是 `self` 时，平仓数量在同一行里二选一表达：

- **按持仓比例**：`quantity = 0`，`quantity_pct` 给比率（全平 1、半仓 0.5；缺省 1），平仓单类型取本行 `order_type`；
- **按绝对数量**：`quantity` 给要平的数量（币/张/股），`quantity_pct = 0`。

`quantity` 与 `quantity_pct` 互斥、至多一个非零，由风控引擎换算实际下单量。new 规则只用 `quantity`（开仓量），不读 `quantity_pct`。

## 5. 条件语义

| condition_kind | condition_name 拼回 | 判定输入 | value 口径 |
|---|---|---|---|
| `roi` | `roi` | **保证金收益率口径** = 直接取 `Position.roi`（`pnl ÷ 占用保证金`），不再 ÷ leverage；+10% 存 0.1。deribit/现货 lev=1 与旧价格涨跌幅口径数值一致，binance/HL 等含杠杆所阈值按保证金收益率表达 | 比率 |
| `price` | `price_{instrument}` | 指定标的最新价（空后缀=仓位标的） | 价格 |
| `position` | `position_{instrument}` | 指定标的持仓数量（空后缀=仓位标的） | 数量 |
| `profit_drawdown` | `profit_drawdown` | 盈利自最高盈利的回落幅度 | 百分点（保证金口径：max_roi − roi） |

操作符与阈值由风控引擎解释；model 只保证定点存储与枚举合法。

## 6. 调用方式（经 trading-store 门面）

CRUD/并发/落盘语义与其它三张表完全相同（全部同步、线程安全，热路径无 `.await`）：

```rust
let rules = store.watch_rules()?;

// 挂载一条「ROI ≥ 12% 全市价平仓」风控规则：动作 self，比例平仓 quantity=0、quantity_pct=1
let rule = WatchRule::new_risk(
    account_id, position_id,
    WatchConditionKind::Roi, None,
    ">=", Decimal::new(12, 2),
    WatchRuleAction::UseSelf,
    OrderType::Market, Decimal::ZERO, Decimal::ONE,
);
let rule = rules.insert(rule)?;          // 框架分配 id、时间戳

// 按绝对数量平仓 3 个单位：quantity=3、quantity_pct=0
let _abs = WatchRule::new_risk(
    account_id, position_id,
    WatchConditionKind::Price, None,
    "<=", Decimal::from(90000),
    WatchRuleAction::UseSelf,
    OrderType::Limit, Decimal::from(3), Decimal::ZERO,
);

// 链式：命中后激活另一条规则（数量/类型不被消费，传默认即可）
let next = WatchRule::new_risk(
    account_id, position_id,
    WatchConditionKind::Price, Some("BTCUSDT".into()),
    "<=", Decimal::from(90000),
    WatchRuleAction::ActivateRule { rule_id: next_id },
    OrderType::Limit, Decimal::ZERO, Decimal::ONE,
);

// —— 状态推进必须走 RuleRepo（trading-store），不要直接 table.update 状态 ——
let repo = store.rule_repo()?;

// 命中 self：原子 active→in_use（行锁 CAS，并发只有一个赢家，返回 false 即跳过）
if repo.claim_self(rule.id)? {
    // 随后才建 Order（Order.rule_id=rule.id）并下单；锁不跨网络
}
// 链式：当前 active→retired(+deleted) 与目标 pending→active 在一个短临界区内成对完成
repo.chain_fire(current_id, target_id)?;
// 关联 Order 到达终态（FILLED/CANCELED/REJECTED/EXPIRED）后：in_use→retired(+deleted=1)
repo.retire(rule.id)?;   // 幂等
```

通用表 `watch_rules()` 仅用于 CRUD/查询；**业务状态机一律走 `RuleRepo`**
（`claim_self` / `chain_fire` / `retire` / `list_active(_for_position)`），
禁止在 `update`/`with_row` 闭包内回查本表、不要在外层再包 `Mutex`，
并发约束详见 [`内存和持久化管理.md`](./内存和持久化管理.md) 的「核心设计原则」。

## 7. 运行时引擎（trading-service）

盯盘判定与下单编排在 `crates/trading-service/src/rules/`，分层与 `positions/` 一致：

| 文件 | 职责 | IO |
|---|---|---|
| `create.rs` | 远程 `create_rule` 的纯构造/校验：risk 必须解析到 deleted=0 绑定仓位（账户+所+合约类型+标的+方向），`position_X` 条件标的也必须有活跃仓位；链式动作透传（目标 pending 校验在 service） | 经仓位查询端口，零网络 |
| `eval.rs` | 操作符 `>= <= > < ==` 的纯 Decimal 判定 | 纯函数 |
| `metrics.rs` | `Position + Market → EvaluationContext`：**规则 roi 直接取仓位 roi（`pnl÷占用保证金`，保证金收益率口径）**，profit_drawdown 取仓位字段，price/position 跨标的经注入闭包；price 缺价本轮不触发，跨标的**持仓量**无仓补 0 | 零 IO |
| `order.rs` | 命中规则 → `Order`/`PlaceOrderRequest`：risk 取持仓反向 + `reduce_only`，平仓量 `quantity>0 ? quantity : 持仓量×quantity_pct`，杠杆随持仓；new 用规则行 `side`/价/量/`leverage`；统一写 `Order.rule_id`；另有纯判定 `terminal_rule_id` | 纯构造 |
| `engine.rs` | 一轮扫描：只评估 `active` 规则 → risk 绑定仓位消失即 retire；命中后链式 `chain_fire`，或预检→`claim_self`→落内存订单→下单端口；跨标的价 **QuoteTable 优先、缺失回退活跃仓位 current_price**；另有 `reconcile_terminal_orders` REST 终态兜底 | 状态走 store，网络经端口 |
| `placer.rs` | 生产下单口（futures 且 leverage>1 先 `set_leverage`：开仓失败按拒单 retire、平仓失败不阻断）；另有 `ExchangeRuleOrderQuerier` REST 查单口 | 网络 |

关键口径：

- **预检在 claim 之前**（账户解析/仓位读取/订单构造都无副作用），只有确有单可下才
  `claim_self`，缺账户不会把规则卡死在 `in_use`；risk 绑定仓位已消失则直接 retire；
- **网络在 claim 之后、且不持任何规则锁**；订单先落内存（PLACING）再异步提交；
- main 以 `--rule-scan-interval`（默认 200ms）定时调 `run_scan`，以
  `--position-reconcile-interval`（默认 60s）定时调 `reconcile_terminal_orders`
  （用户流 WS 漏推时 REST 查单收敛终态）；仓位指标由流水线（行情重估 + 对账）刷新；
- 订单终态回收统一经 `event.rs::retire_rule_for_terminal_order`：
  `rule_id != 0 && status.is_terminal()` → `RuleRepo::retire`（幂等），WS 回报与
  REST 兜底共用同一收口，覆盖 FILLED/CANCELED/REJECTED/EXPIRED。

## 8. 远程建规则（南向 create_rule）

策略端经 WS 发 `option=create_rule`（字段见 `SouthCommand`），服务端按
**account + exchange + instrument_type + instrument + side** 解析绑定仓位，策略端
不接触 `position_id/rule_id`：

- risk 规则：绑定仓位必须存在，否则拒绝；`position_X` 跨标的条件还要求 X 当前有活跃仓位；
- new 规则：携带开仓要素（exchange/instrument/side/order_type/price/quantity/leverage），position_id=0；
- 链式：先以 `pending=true` 建目标规则，再建源规则并传 `action=rule:<目标id>`
  （strategy-client `--target-rule-id`）；服务端校验目标存在且为 pending，否则拒绝；
- 成功回 `type=ack,status=OK` 文本（不带 id），失败回 `type=error`。

## 9. REST 连锁规则批量创建（实现设计）

> 对应接口：`POST /api/v1/rules/batch` 的 `{ "rules": [ ... ] }` 形态（接口契约见
> 《接口文档.md》第二部分 §4.13）。本节固化其**内部实现方案**，接口文档只描述报文。
>
> 与第 8 节南向 WS 的区别：REST 建规则时 **risk 规则直接传 `position_id` 绑定仓位**，
> 不再像 WS 那样按 账户+所+类型+标的+方向 反查，因此 risk 请求不消费 `side`；
> `side` 仅 new 规则用作开仓方向。

### 9.1 要解决的问题

链式动作 `rule:{id}` 引用的是服务端分配的数字主键，而一次批量请求里后继规则尚未落库、
还没有 id。不能要求客户端先逐条创建再回填 id（多一次往返且中途失败会留下残链）。

### 9.2 请求内 ref 占位

- 批量体每条规则可带一个**仅请求内有效**的字符串字段 `ref`（如 `"breakeven"`）；
- 源规则的 `action` 写 `rule:<某条 ref>`（如 `"rule:breakeven"`）；
- 同一 `ref` 可被多条规则引用（支持分叉链）；引用本批不存在的 ref 必须拒绝；
- 被引用的后继规则通常带 `"pending": true`（链尾等待激活）。

### 9.3 action 的中间解析态

model 的 `action` 列本身是 `String`，**无需改表结构**。在纯构造层引入一个只在
请求处理期存在的中间枚举（不落盘、不进 model）：

```rust
enum RawAction {
    Self_,                 // "self"
    Ref(String),           // "rule:breakeven" —— 批量体内的占位
    Id(u64),               // "rule:42"       —— 单条创建引用已存在规则
}
```

- 现有 `WatchRuleAction::parse_wire` 只认数字 `rule:42`；批量路径先用 `RawAction`
  宽松承接 `rule:<ref>`，第一遍把占位串**原样**写进 `WatchRule.action`；
- 第二遍拿到真实 id 后，再用 `table.update` 把该行 action 改写成 `rule:<真实id>`，
  此时它才是一个能被 `WatchRuleAction::parse_wire` 解析的合法终态串。

### 9.4 两遍落库

1. **第一遍（insert）**：按数组顺序对每条 `RuleRepo.insert`，取得框架分配的真实 id，
   同时在内存建 `ref -> id` 映射；此阶段 `rule:<ref>` 占位串原样存。
   因为解析被推迟到第二遍，**前向引用合法**（第 1 条可引用排在后面的第 2 条）。
2. **第二遍（回填 action）**：遍历所有 action 为 `rule:<ref>` 的行，用映射查出真实 id，
   逐行 `table.update` 改写为 `rule:<id>`。

### 9.5 校验时机

- **落库前**（零 IO，可整体拒绝）：扫描全部条目，所有 `rule:<ref>` 的 ref 必须能在
  本批 `ref` 集合中找到；重复 ref、缺失字段、枚举非法等也在此判；
- risk 的 `position_id` 绑定仓位存在性、new 的开仓要素校验，复用 `rules/create.rs`
  的纯构造/校验口径；
- 单条（非批量）创建保留旧语义：`rule:42` 要求目标规则**已存在且为 pending**
  （service 层现有校验）；`rule:<ref>` 占位形态只允许出现在批量体内。

### 9.6 运行期激活不在创建路径

批量创建只负责落库并把后继置为 `pending`。真正命中时的状态推进**复用现成的
`RuleRepo::chain_fire(current_id, target_id)`**：在一把短临界区（`chain_lock`，
临界区内只有两次内存 update、不含网络/await）内原子完成

- 当前规则 `active -> retired(+deleted=1)`；
- 目标规则 `pending -> active`；

任一前置不符则两行都不动。创建路径不得另写一套链式状态机。

### 9.7 失败语义与可测性

- `Table` 当前无批量事务：第一遍中途某条 insert 失败时**已插入行保留**（非原子），
  响应按条回 `ok`/失败下标，调用方可据此补偿；后续如需可升级为批量回滚；
- 纯计算部分（`RawAction` 解析、ref 集合校验、`ref -> id` 映射、action 回填串生成、
  `WatchRule` 构造）放进类似 `rules/create.rs` 的**零 IO 模块**，不接触 store/网络，
  用内存假数据做单测；两遍落库的 IO 编排留在 service 薄层。

## 10. 「重设」语义：身份键 + find-or-update

> 对应代码：`trading-models/src/watch_rule.rs`（`RuleIdentity`、
> `WatchRule::identity()`）、`trading-store/src/rules.rs`
> （`find_active_by_identity`）、`api/handlers/rule_writes.rs`（upsert 接线）。

### 10.1 问题
口语「重新设置规则」时，客户端往往**不带 id、也不知道已存在同类型规则**。
旧实现对无 id 的请求一律 insert（追加），旧规则仍 active，导致同一意图出现
多条规则、重复触发。

### 10.2 身份键（`RuleIdentity`）
字段虽多，但分成「身份字段（决定是否同一条）」与「可变参数（设成什么，命中即覆盖）」：

- 通用身份：`account_id`、`rule_type`、`condition_kind`、`condition_instrument`、
  **`operator`**（`>=` 与 `<=` 语义相反，必须区分，否则反向阈值的规则会被误并）、
  **`value`**（阈值不同即不同规则，如 `roi<=0.1` 与 `roi<=0.2` 应并存为两条独立规则）；
- risk：目标标的取 **`position_id`**（规则绑定具体仓位）；
- new：目标标的取 `instrument` + `instrument_type`，并按 **`side` 区分多/空**。

**不参与身份（命中时被覆盖）**：`action`、
`quantity`/`quantity_pct`、`price`、`order_type`、`leverage`、`order_cash`、
`status`、时间戳。

### 10.3 find-or-update
`POST /api/v1/rules`（单条与批量同一口径）在无 id 的创建路径：先用身份键查活跃规则——

- 命中（含 `value` 在内的全部身份字段一致）：复用其 id，走既有「只覆盖可变字段」的更新；
- 未命中（任意身份字段不同，包括仅 `value` 不同）：insert 新规则。

显式带 id 的更新行为不变。身份键为纯函数，可独立单测。

### 10.4 边界
- `condition_kind` 不同（如 ROI 止盈 vs 价格止损）即使同仓位也是两条；
- `operator` 不同（如 `roi>=0.55` 武装 vs `roi<=-0.50` 止损）是两条；
- 仅 `value` 不同（同条件、同 operator）也是两条，各自独立触发；
- new 的多、空（side 不同）各一条。
