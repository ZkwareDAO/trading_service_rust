# 命令重放 / 端到端链 / 错误处理 详情

> trade-ops 参考文档：命令重放 /x（§3.8）、端到端核心链（§4）、错误处理（§5）。
> 通用解析约定、确认铁律见上级 SKILL.md §2；各端点字段细节见 api-orders.md / api-positions.md / risk-rules.md。

---

## 3.8 命令重放（/x）

**指令语法**
```
/x [symbol] 开仓|risk
```
- `symbol`：新标的（币安符号如 `BTCUSDT`，deribit 完整合约名 `BTC-25SEP26-80000-P`，或 CTP 合约号如 `rb2510`）；**由 symbol 判定交易所**（`…USDT`→binance，期权合约名→deribit，国内合约号形态如 `品种字母+4位数字`：`rb2510`/`cu2512`/`IF2512`→ctp）；形态无法唯一归属时追问用户、不猜测；
- 类型 **必带**：`开仓`→重放该交易所的 `last_open`，`risk`→重放 `last_risk`；**缺类型或写成别的词 → 追问，禁止猜测执行**。
- 语义：除标的（及其派生项）外，其余参数（**账户**、方向、数量/金额、杠杆、价格、条件阈值、平仓量等）与该交易所上次命令**完全一致**；账户沿用记录里的 account，无需在 `/x` 中给出。

**重放记忆：`reference/last_commands.json`（按交易所分组）**

记忆按交易所隔离（同交易所命令格式一致），每个交易所下分两类各记一条：
```json
{
  "exchanges": {
    "deribit": {
      "last_open": { "kind": "open", "raw": "条件限价开仓 D test_deribit …", "payload": { }, "account": "test_deribit", "symbol": "原标的", "saved_at": 0 },
      "last_risk": { "kind": "risk", "raw": "风控 D test_deribit …", "payload": { }, "account": "test_deribit", "symbol": "原标的", "saved_at": 0 }
    }
  }
}
```
- `payload` 为该命令**已解析、可直接重放的请求要素**（含 endpoint、method、body、account），重放时直接复用、**不再重新解析自然语言**，避免解析漂移；
- 由 symbol 推出交易所后，取 `exchanges.<exchange>`；缺失该交易所或对应槽位（该类型从没执行过）→ 告知「该交易所没有可重放的开仓/风控命令」并结束，不执行任何操作；
- 文件不存在按「无任何记录」处理，不报错。

**何时写入记忆**
- 仅在命令**经用户确认并实际发送成功后**，写入 `exchanges.<本命令交易所>` 的对应槽位；
- 记入 `last_open`：即时市价/限价开仓、条件市价/限价开仓（new）；
- 记入 `last_risk`：创建风控（risk）；
- **不记**：各类查询（仓位/订单/规则/账户）、撤单、即时平仓、停用规则、`/x` 自身。

**重放构造（标的替换规则）**

1. 通用：把 payload 的主标的替换为新 `symbol`；
2. **risk —— position_id 必须重查（重要）**：`position_id` 是标的的派生项，不沿用旧值。交易所由 symbol 判定、账户沿用记录 payload，按此重新查仓位（`GET /positions`，多空多行命中让用户选，无仓则提示先有持仓、终止重放），用**新 position_id**；其余风控要素（condition_*、operator、value、quantity/quantity_pct、order_type）照旧；
3. **条件开仓（new）—— 条件标的**：替换主下单 `instrument`；当 `condition_instrument` 与原标的相同（自引用条件）时同步替换为新 symbol，跨标的的外部条件（condition_instrument 是另一个符号）保持不变。

**强制确认（铁律，不可绕过）**

重放后先以 GFM 表格回显**替换后的完整要素**，表头注明类型与「重放」。**只显示替换后的信息，不展示原标的、不做新旧对比**：
```
将重放上一次【风控】命令：
| 项目 | 内容 |
|------|------|
| 交易所/账户 | deribit / test_deribit |
| 标的 | BTC-25SEP26-80000-P |
| 方向 / 平仓量 | 空 / 全平 |
| 条件 | ROI <= -30% |
**确认执行？(y/n)**
```
- 用户 `y` → 才调用 payload 对应端点实际操作；`n` → 取消、不发送；
- 未得到明确 `y` 之前，**绝不进行实际操作**；
- 执行失败 → 原样返回后端 `message`（SKILL.md §2.7），不自行改参数重发；成功后本次命令同样按上面规则更新对应记忆槽位。

---

## 4. 端到端核心链（最常用路径）

**「开多 BTC → 看仓位 → 设止盈止损 → 取消止盈止损」**

```
1) 开仓
   POST /api/v1/orders
   {exchange:binance, account:test_binance, instrument:BTCUSDT,
    instrument_type:futures, side:buy, order_type:market, quantity:0.1, leverage:10}
   → 记 client_order_id；轮询 GET /orders?client_order_id= 直到 NEW/FILLED

2) 查仓位（拿 position_id）
   GET /api/v1/positions?account=test_binance&exchange=binance
   → 找到 BTCUSDT 多仓，id=42

3) 设止盈止损（risk，绑定 position_id；value 为保证金收益率，口径见 SKILL.md §2.3）
   POST /api/v1/rules  {rule_type:risk, position_id:42, condition_kind:roi, >=, 0.6,  market, quantity_pct:1}  # 止盈 +60%
   POST /api/v1/rules  {rule_type:risk, position_id:42, condition_kind:roi, <=,-0.3,  market, quantity_pct:1}  # 止损 -30%

4) 查询配置
   GET /api/v1/rules?account=test_binance&rule_type=risk
   （关联 GET /positions 显示标的/方向）

5) 取消
   POST /api/v1/rules/status {ids:[...本次查询的 id...], status:retired}
```

---

## 5. 错误处理

统一信封：

```json
{ "code": 40002, "message": "交易所 `binance` 需要在 account.csv 配置有效账户 `test`", "data": null }
```

- 成功 `code=0`；非 0 时 `data=null`，把 `message` **原样**展示给用户并给建议，不要臆测原因；**禁止自行改参数后自动重发**（铁律见 SKILL.md §2.7）。
- 常见 code：`40001` 参数缺失/非法、`40002` 账户/交易所未配置、`40004` 资源不存在、`40901` 状态冲突（重复 client_order_id、规则状态非法迁移）、`50001` 交易所调用失败、`50002/50000` 内部错误。

```
操作失败：
| 错误码 | 详情 |
| 40002 | 交易所 `binance` 无有效账户 `test` |
建议：用账户查询确认账户名，或检查交易所是否输入正确（需精确名 binance）
```
