
> 2026-11-24 增补：本表 #4/#5/#6 的判定以文末「防御与触发强化用例」为准，并新增 #16、#17 两项防御用例。

## 一、测试项

> 统一 **3x 杠杆**（风控规则按 ROI 口径，3x 下 ROI 0.5% ≈ 价格波动 0.167%，更易触发）。主标的 SOLUSDT（现价约 74.73），关联/条件项保留 ETHUSDT。

| # | 测试项 | 前置条件 | 测试语句（trade-ops 自然语言） | 预期结果 |
|---|--------|----------|-------------------------------|----------|
| 1 | 市价下单 | 账户可用保证金充足 | `市价开仓 B 合约 test_binance SOLUSDT long 3x 100u` | 下单 `code=0`；订单 `PLACING→NEW→FILLED`；仓位出现 SOLUSDT 多单，数量 = 100 ÷ 成交价（约 1.34），方向、杠杆正确 |
| 2 | 限价下单 | 账户可下单 | `限价开仓 B 合约 test_binance SOLUSDT 74.00 long 0.002 3x` | 订单 `NEW`，`order_price=74.00`，`filled_quantity=0`；活跃订单查询可见；不产生仓位 |
| 3 | 查找仓位 | 无 | `B test_binance 仓位列表` | `code=0`，返回该账户全部活跃仓位；字段含标的/方向/数量/开仓价/标记价/ROI；与交易所实时仓位数量、方向一致 |
| 4 | 创建止盈配置 | 先开仓：`市价开仓 B 合约 test_binance SOLUSDT long 3x 100u` | `风控 B test_binance SOLUSDT 止盈值0.5%` | 规则创建成功、`active`、`condition_kind=roi`、`operator>=`、`value=0.005`；绑定刚建立的 SOLUSDT 多仓；盯盘规则查询可见 |
| 5 | 创建回落止盈配置 | 先开仓：`市价开仓 B 合约 test_binance SOLUSDT long 3x 100u` | `风控 B test_binance SOLUSDT 回落止盈0.5%回落1%触发` | 批量创建两条：arm `active`（roi>=0.005，`action=rule:trail`）、trail `pending`（profit_drawdown>=0.01）；绑定同一仓位；平仓量为全部 |
| 6 | 创建止损配置 | 先开仓：`市价开仓 B 合约 test_binance SOLUSDT long 3x 100u` | `风控 B test_binance SOLUSDT 止损值1%` | 规则 `active`、`condition_kind=roi`、`operator<=`、`value=-0.01`；绑定目标仓位 |
| 7 | 重复创建回落止盈配置（幂等） | 先开仓；先执行一次第 5 项并记录规则 id | `风控 B test_binance SOLUSDT 回落止盈0.5%回落1%触发`（再执行一次） | 第二次提交后该仓位等价有效规则数量不增加（幂等），或后端明确返回重复/冲突错误；不得出现两套同时生效的等价链式规则 |
| 8 | 创建价格风控配置 | 先开仓：`市价开仓 B 合约 test_binance SOLUSDT long 3x 100u` | `风控 B test_binance SOLUSDT SOLUSDT价格涨过75.00` | 规则 `active`、`condition_kind=price`、`operator>=`、`value=75.00`；若观察窗内价格触发，则仓位被平掉、规则转终态并记录触发时间 |
| 9 | 创建关联仓位风控配置 | 先开两个仓：`市价开仓 B 合约 test_binance SOLUSDT long 3x 100u` 与 `市价开仓 B 合约 test_binance ETHUSDT long 3x 100u` | `风控 B test_binance SOLUSDT 关联仓位ETHUSDT` | 规则 `active`、`condition_kind=position`、`operator==`、`value=0`、`condition_instrument=ETHUSDT`；用 trade-ops 平掉 ETHUSDT 仓位后，SOLUSDT 仓位在观察窗内被自动平掉，记录两者平仓时间与数量 |
| 10 | 创建市价条件开仓配置 | 无同向持仓 | `市价条件开仓 B 合约 test_binance SOLUSDT long 3x 100u SOLUSDT价格涨过74.50` | 创建 `rule_type=new`、`action=self` 规则，状态 `active`；条件命中后生成市价订单并 `FILLED`，仓位建立，数量与金额口径正确；规则转 `in_use/retired`，不重复下单 |
| 11 | 创建限价条件开仓配置 | 无同向持仓 | `限价条件开仓 B 合约 test_binance ETHUSDT 1815 long 0.01 3x ETHUSDT价格涨过1805` | 创建 new 规则 `active`、`order_type=limit`、`price=1815`；条件命中后挂出限价订单，状态 `NEW` 或 `FILLED`，订单价格等于规则价；规则转终态 |
| 12 | 查找盯盘规则 | 第 4/5/6/8 项创建的规则至少有一项仍生效；若均已清理，先创建一条止盈规则 | `/watch-list B test_binance 全部` | 仅返回 `active/pending` 规则，不含 `in_use/retired`；开仓/风控分类正确；与 `test_binance 风控查询` 结果交叉核对一致 |

---


## 防御与触发强化用例（2026-11-24 增补）

> 本节为强制补充：#4、#5、#6 的判定以本节为准；并新增 #16、#17 两项防御用例。

### 强化 #4 止盈 / #6 止损：必须验证真实触发平仓

- 规则创建成功后**必须等待真实触发**（ROI 类最长 15 分钟，5 秒轮询），不能以「规则创建成功」判 PASS。
- 触发后验证：规则转终态（retired/inuse）、产生真实平仓订单且 `FILLED`、仓位按平仓量归零/减少。
- 超时未触发：判 `BLOCKED`，附最后 `roi` / `max_roi` / 规则状态。

### 强化 #5 回落止盈：三阶段判定（缺一不可）

1. **盈利阈值到达**：arm 规则转终态（retired），trail 规则 `pending → active`（原子切换）。
2. **激活后不得误平**：trail 已 active 但 `profit_drawdown < 回落阈值` 时，至少观察 60 秒——仓位数量不变、无平仓订单。
3. **回落达标真实平仓**：`profit_drawdown >= 回落阈值` 后产生真实平仓订单且 `FILLED`，仓位按 `quantity/quantity_pct` 归零/减少。
- 阈值设计：盈利阈值取低（易达），回落值相对取大（为阶段 2 留观察窗）；记录 `max_roi` / `profit_drawdown`。
- 任一阶段不符 → `FAIL`；行情未达阈值 → `BLOCKED`（附最后观测值）。

### 阈值选择（暧昧值原则，适用于 #4/#5/#6）

目标：阈值**不能创建即触发**，也不能几乎不可达；取「中等偏易」值，保证观察窗内有真实触发机会。
- 判定：创建后 2 分钟内即触发 → 阈值过松，上调后重测（记前置调整表）；15 分钟未触发 → `BLOCKED`（附 `roi`/`max_roi`），不得改判 PASS。
- 参考区间（以执行时波动为准，优先取区间中值）：

| 交易所 | #4 止盈 | #6 止损 | #5 盈利阈值 | #5 回落值 |
|--------|---------|---------|-------------|-----------|
| binance（3x） | 0.3%–0.5% | -0.5%–-1% | 0.3%–0.5% | 0.5%–1% |
| deribit（期权） | 5%–10% | -10%–-20% | 5%–10% | 3%–5% |
| ctp（保证金≈10x） | 3%–5% | -3%–-5% | 3%–5% | 1%–2% |

- 禁止：盈利阈值 ≥50%、回落 ≥5%（binance/ctp）等几乎不可达值；也不得设 0/负值导致创建即触发。
- 回落值相对盈利阈值取 50%–70%，为阶段 2（激活后不误平）留出观察窗。

### #16 价格风控不误触发（防御）

- 指令：`风控 … {sym}价格涨过 {现价×1.05（取符合合约精度）}`（阈值远离现价）。
- 预期：规则 `active`；观察 3–5 分钟，期间**无任何平仓订单、仓位数量不变、规则不触发**；记录最后价格与规则状态。
- 误触发 → `FAIL`；观察窗内价格逼近阈值导致无法判定 → `BLOCKED`。
- 清理：停用该规则。

### #17 关联仓位不存在拒建（防御）

- 指令：`风控 … {主标的} 关联仓位{不存在合约}`：
  - binance：`NOPEUSDT`
  - deribit：`BTC-01JAN99-100000-C`
  - ctp：`rb9999`
- 预期：创建请求返回**非 0 code**（40001/40004 等），`message` 原样记录；规则总数不增加；主仓位不受影响。
- 若返回 `code=0` 且创建成功 → `FAIL`；若误创建则停用清理。
