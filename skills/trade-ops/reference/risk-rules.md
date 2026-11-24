# 止盈止损（risk 规则）详情

> trade-ops 参考文档：规则查询（§3.5）、云盯盘 /watch-list（§3.5.1）、创建止盈止损（§3.6，含回落批量）、停用（§3.7）、未设置风控仓位筛查（§3.10）。
> 条件词典、平仓量写法、确认铁律见上级 SKILL.md §2；仓位查询字段见 api-positions.md §3.4。小节号与 SKILL.md 一致。

---

## 3.5 止盈止损查询

触发词：`{账户} 止盈止损配置` / `风控查询` / `风控列表`；账户名后的 `risk` 同样按查询处理。可只传 account 不传 exchange（同 3.4）。

**GET** `{base}/api/v1/rules`

止盈止损固定加 `rule_type=risk`，默认只返回未终止规则：

```bash
curl -s -G {api.base}/api/v1/rules \
  --data-urlencode "account=test_binance" \
  --data-urlencode "rule_type=risk"
```

query（可选）：`account`/`exchange`、`position_id`、`rule_type`、`condition_kind`、`status`（`active`/`pending`/`in_use`/`retired`，可重复）、`instrument_type`、`include_retired=true`（含已终止）、分页。

`data.list[]`（RuleDTO，关键字段）：

| 字段 | 说明 |
|------|------|
| **`id`** | 规则 id（停用规则时用） |
| **`position_id`** | 绑定仓位 id（关联仓位查 instrument/方向） |
| rule_type | `risk` / `new` |
| condition_kind | `roi`/`price`/`position`/`profit_drawdown` |
| condition_instrument | 跨标的条件后缀（本仓位标的为 null） |
| operator / value | 比较符 / 阈值 |
| action | `self` 或 `rule:<id>`（链式激活） |
| order_type | 平仓单类型（market/limit） |
| quantity / quantity_pct | 绝对平仓量 / 比例平仓量 |
| status | `active`/`pending`/`in_use`/`retired` |
| created_at | 创建时间 ms |

**展示需关联仓位**：规则本身不带标的名/方向（risk 不读 side）。批量查一次 `GET /api/v1/positions`，用 `position_id == 仓位.id` 关联出 instrument 与方向。

条件描述格式化：
- roi：`ROI <= -30%` / `ROI >= +60%`（保证金收益率口径，value 直接 ×100 展示）；
- price：`BTCUSDT价格 >= 90000`；
- position：`关联仓位 ETHUSDT 归零`；
- profit_drawdown：`盈利回落 >= 5%`；
- `action=rule:<id>` 标注「链式，等待激活/命中后激活下一条」。
- 状态：active→✅生效中，pending→⏳待激活，in_use→🔄已触发平仓中，retired→⏹已终止。

```
账户 test_binance 的止盈止损：
| 标的 | 方向 | 条件 | 平仓量 | 类型 | 状态 |
| BTCUSDT | 多 | ROI <= -30% | 全平 | 市价 | ✅ 生效中 |
| BTCUSDT | 多 | ROI >= +60% | 全平 | 市价 | ✅ 生效中 |
共 2 条（规则 id 不展示给用户，内部留存供停用）
```

---

### 3.5.1 云盯盘规则查询（/watch-list）

**指令语法**
```
/watch-list [交易所] [用户] [开仓|风控|全部]
```
触发词：`/watch-list`；自然语言 `盯盘规则` / `仓位规则` 同样进入此查询。**纯查询、只读，不走写操作确认。**

**入参解析**

| 要素 | 规则 |
|------|------|
| 交易所 | **必传**：B/币安/binance→`binance`，D/deribit→`deribit`，C/ctp/CTP→`ctp`；缺失或多所歧义→追问，不猜 |
| 用户 | **必传**：账户名；缺失→追问 |
| 类型 | `开仓`→`rule_type=new`；`风控`→`rule_type=risk`；`缺省/全部`→**不传** `rule_type` |

**调用（关键：不传 status）**

规则接口的 `status` 多值参数当前不可用（裸 `status=` 报 400、`status[]=` 被静默忽略，详见 SKILL.md §6）。因此**不传 `status`**，取接口默认「未终止全集」，再在客户端过滤：

```bash
# 全部类型（开仓+风控）
curl -s -G {api.base}/api/v1/rules \
  --data-urlencode "exchange=binance" \
  --data-urlencode "account=test_binance" \
  --data-urlencode "page_size=500"
# 开仓加  --data-urlencode "rule_type=new"；风控加 --data-urlencode "rule_type=risk"
```

**客户端处理**
1. 从 `data.list[]` 只保留 `status ∈ {active, pending}`，**剔除 in_use**（retired 后端默认已排除）；
2. 字段取数按类型区分：
   - **new（开仓）行**：直接读行上的 `instrument` / `instrument_type` / `side` / `price` / `quantity` / `order_cash` / `leverage` / `order_type`；
   - **risk（风控）行**：行内不带标的/方向，批量查一次 `GET /api/v1/positions`，用 `position_id == 仓位.id` 关联出标的与方向；平仓量取 `quantity`（绝对）或 `quantity_pct`（比例），平仓单类型取 `order_type`。

> 内存表是「滚动存活集」：跨日 rollover 只物理清除 `deleted=1` 的终态行，未置终态（deleted=0）的 active/pending 规则跨日仍在内存、照样列出。此口径对用户不额外提示。

**条件描述格式化**：`condition_name + operator + value`
- roi：value ×100 带符号，如 `ROI >= +60%` / `ROI <= -30%`；
- price：`BTCUSDT价格 >= 90000`；
- position：`关联仓位 ETHUSDT == 0`；
- profit_drawdown：`盈利回落 >= 5%`。
- 状态：active→✅生效中，pending→⏳待激活。

**输出（文本表格，一张表，类型列区分）**
```
账户 test_deribit（deribit）云盯盘规则：
| 类型 | 标的 | 方向 | 条件 | 下单/平仓要素 | 状态 |
|------|------|------|------|---------------|------|
| 开仓 | ETH-25SEP26-2400-P | 多 | ROI >= +10% | 市价 | ✅ 生效中 |
| 风控 | BTCUSDT | 多 | ROI <= -30% | 市价·全平 | ⏳ 待激活 |
共 2 条（active/pending）
```
查无 active/pending 规则时，输出该账户「暂无生效中的云盯盘规则」。规则 id 不展示给用户（内部留存，供后续停用）。

---

## 3.6 创建止盈止损（watch_rule · risk）

**POST** `{base}/api/v1/rules`

前置：先查仓位拿到 **`position_id`**（用户说 symbol 时，用 3.4 按 instrument 匹配仓位；多/空两行都命中则让用户选方向；查不到则提示先有持仓）。

**公共必填（risk）**：`account`、`rule_type:"risk"`、`position_id`、`condition_kind`、`operator`、`value`。
**不填**：`side`、`instrument`（risk 由 position_id 解析，下单要素以绑定仓位为准）。
**`exchange`**：币安可省略；**deribit 等非默认所必须显式带**（服务端据此解析账户/凭证，规则行不消费）。

### 3.6.1 ROI 止损 / 止盈

```bash
# 止损：仓位亏损 30%（亏 30% 保证金）全市价平
curl -s -X POST {api.base}/api/v1/rules -H "Content-Type: application/json" -d '{
  "account":"test_binance","rule_type":"risk","position_id":42,
  "condition_kind":"roi","operator":"<=","value":-0.3,
  "order_type":"market","quantity_pct":1
}'

# 止盈：仓位盈利 60%（赚 60% 保证金）全市价平
curl -s -X POST {api.base}/api/v1/rules -H "Content-Type: application/json" -d '{
  "account":"test_binance","rule_type":"risk","position_id":42,
  "condition_kind":"roi","operator":">=","value":0.6,
  "order_type":"market","quantity_pct":1
}'
```

自然语言「止损值 30%、止盈值 60%」直接换算为 `-0.3` / `0.6`（保证金收益率口径，见 SKILL.md §2.3）。

### 3.6.2 价格条件止盈止损（本仓位标的）

`condition_instrument` 留空即本仓位标的，无需再传白名单资产名：

```bash
# BTCUSDT 价格涨过 90000 止盈
{ "account":"test_binance","rule_type":"risk","position_id":42,
  "condition_kind":"price","operator":">=","value":90000,
  "order_type":"market","quantity_pct":1 }

# 跌破 70000 止损
{ "account":"test_binance","rule_type":"risk","position_id":42,
  "condition_kind":"price","operator":"<=","value":70000,
  "order_type":"market","quantity_pct":1 }
```

跨标的触发（用 BTC 价格平 ETH 仓）：`condition_kind:"price"` + `condition_instrument:"BTCUSDT"`。

### 3.6.3 关联仓位止盈止损

关联仓位被平掉（数量归零）时平本仓位：

```bash
{ "account":"test_binance","rule_type":"risk","position_id":42,
  "condition_kind":"position","condition_instrument":"ETHUSDT",
  "operator":"==","value":0,
  "order_type":"market","quantity_pct":1 }
```

### 3.6.4 回落止盈（连锁批量，替代旧 fallback_rule）⭐

语义：盈利先达到目标（如 ROI +55%）后，改为监控「自盈利峰值回落」，回落达阈值（如 5%）才平仓。
用**一次批量请求**建两条 risk 规则：触发规则 `action:"rule:<ref>"`（命中后不自己平仓，转激活后继），后继规则 `pending:true` + `profit_drawdown`。

下例对应 `风控 D monthopt ETH-18SEP26-2550-P 5 回落止盈55%回落5%触发，止损50%`——指令带平仓量 `5`，故**批量两条都带 `quantity:5`**，并另发一条同平仓量的独立止损：

```bash
# 1) 回落链式：一次批量 2 条，均 quantity:5
curl -s -X POST {api.base}/api/v1/rules/batch -H "Content-Type: application/json" -d '{
  "rules": [
    {
      "ref": "arm",
      "account":"monthopt","exchange":"deribit","rule_type":"risk","position_id":42,
      "condition_kind":"roi","operator":">=","value":0.55,
      "action":"rule:trail","order_type":"market","quantity":5
    },
    {
      "ref": "trail",
      "account":"monthopt","exchange":"deribit","rule_type":"risk","position_id":42,
      "condition_kind":"profit_drawdown","operator":">=","value":0.05,
      "action":"self","order_type":"market","quantity":5,
      "pending":true
    }
  ]
}'

# 2) 独立止损：ROI <= -50%，同样 quantity:5
curl -s -X POST {api.base}/api/v1/rules -H "Content-Type: application/json" -d '{
  "account":"monthopt","exchange":"deribit","rule_type":"risk","position_id":42,
  "condition_kind":"roi","operator":"<=","value":-0.5,
  "order_type":"market","quantity":5
}'
```

> 若指令写的是 `20%`，把上例三处 `quantity:5` 全部换成 `quantity_pct:0.2`；若**不带平仓量**，则用 `quantity_pct:1`（全平）。平仓量取值规则见 SKILL.md §2.5。

- 服务端两遍落库：先插入两条拿到 id，再把 `rule:trail` 回填为真实 `rule:<id>`；`ref` 仅请求内有效。
- `data.list[]` 返回两条 RuleDTO（顺序同入参），第一条 active、第二条 pending。
- 运行期：第一条命中 → 原子地「第一条 retired + 第二条 pending→active」，随后第二条按回落率监控。

**响应展示**：

```
止盈止损已创建：
| 标的 | 方向 | 条件 | 平仓量 | 状态 |
| ETH-18SEP26-2550-P | 空 | ROI >= +55% → 盈利回落 >= 5% | 5 张 | ✅ 生效中（链式） |
| ETH-18SEP26-2550-P | 空 | ROI <= -50% | 5 张 | ✅ 生效中 |
状态：已生效（规则 id 内部留存，不展示给用户）
```

### 3.6.5 指令与交互

自然语言：

```
风控|risk|止盈止损 {交易所} {账户} {symbol} {平仓量} 止损值{n}%，止盈值{m}%   ← 前缀，{平仓量} 可选（如 `20%` 或 `5`），条件在最后部分
{交易所} {账户} {symbol} 止盈止损，止损{n}%，止盈{m}%                        ← 后缀形态
风控|risk|止盈止损 {交易所} {账户} {symbol} {平仓量} {asset}价格涨过…          ← 如 …5 btc价格涨过70000
风控|risk|止盈止损 {交易所} {账户} {symbol} 关联仓位{关联symbol}
风控|risk|止盈止损 {交易所} {账户} {symbol} {平仓量} 回落止盈{n}%回落{k}%触发，止损{p}%
风控|risk|止盈止损 {交易所} {账户}                                          ← 交互式：先列仓位让用户选
```

- 条件一律来自指令**最后部分的文字**（提取规则见 SKILL.md §2.4），不要求 `==…==`。
- 条件里裸币种（btc/eth/sol）由 agent 转大写（→`BTCUSDT` 等，跨所引用）；完整合约名原样透传。
- `平仓量` 解析见 SKILL.md §2.5：`n%`/`n`/不写（默认 `quantity_pct:1` 全平，方括号只是文档中的可选标记，实际指令不带）；该平仓量对本次每条规则（含批量每条）统一生效。
- deribit / ctp 风控请求体**必须带 `exchange`**（deribit 带 `"deribit"`、CTP 带 `"ctp"`；仅用于账户/凭证解析，规则行的下单要素仍以 `position_id` 绑定仓位为准），否则服务端按默认所解析账户会报 40002。
- CTP 风控平仓量建议直接给**正整数手数**（`quantity`，如 `2`）；百分比（`quantity_pct`）仅在「持仓手数 × 比例」仍为整数手数时可行（如 4 手平 50%=2 手），否则会因非整数手数被柜台拒绝；不接受金额（无 order_cash 语义）。
- 一条指令同时含「回落止盈 + 独立止损」：回落用 3.6.4 一次批量 2 条，再追加 1 条单规则 POST。

流程：
1. 查仓位（按 symbol/方向定位 `position_id`，多个让用户选，无仓提示先开仓）；
2. 从尾部文字解析条件为 risk 规则字段（ROI 按保证金收益率直接换算，止损 30%→-0.3，口径见 SKILL.md §2.3）；
3. **回显确认表，等用户 `y`** 后才 POST；
4. 一条用户指令含止损+止盈两条条件 → 顺序 POST 两次单规则（回落止盈用一次批量）。

---

## 3.7 取消（停用）止盈止损

**POST** `{base}/api/v1/rules/status`

触发词：`停用止盈止损` / `取消止盈止损`（等价）。典型形态：
- `{交易所} {账户} {symbol} 停用止盈止损`：停用该标的下所有生效规则；
- `{交易所} {账户} 停用止盈止损`：交互式，列出该账户全部生效规则按序号挑选。

把规则置为终态 **`retired`**（等价 deleted=true）。**本功能只停用。**

```bash
curl -s -X POST {api.base}/api/v1/rules/status \
  -H "Content-Type: application/json" \
  -d '{"ids":[201,202],"status":"retired"}'
```

| 字段 | 说明 |
|------|------|
| ids | 规则 id 列表，**必须取自本次命令刚查询的 GET /rules 结果**，不用旧列表 |
| status | `retired` 停用；（重新启用可用 `active`，仅对 retired 行） |

也支持逐条形态：`{"items":[{"id":201,"status":"retired"}]}`。`data` 返回 `{updated, items:[{id,status,ok}]}`。

流程：
1. `GET /rules?account=&rule_type=risk`（本次新鲜列表，默认仅未终止）；
2. `GET /positions` 关联出标的/方向；
3. symbol 路径取该仓位下所有 active/pending 规则；交互路径渲染序号列表让用户选；
4. 回显确认表，`y` 后 POST retired，`n` 终止。

> 停用后该仓位即不再有此风控，如需保护请重新创建，不会自动恢复。

```
将停用以下止盈止损：
| 标的 | 方向 | 条件 | 当前状态 |
| BTCUSDT | 多 | ROI <= -30% | 生效中 |
确认停用？(y/n)
```

---

## 3.10 未设置风控仓位筛查与批量设置

触发词：`{交易所} {账户} 未设置风控仓位` / `{交易所} {账户} 无风控仓位`（可加前缀 `查看`，如 `查看 d test_deribit 无风控仓位`）。

### 3.10.1 筛查流程（两次查询 + 差集，纯查询、无需确认）

1. **查活跃仓位**：`GET /api/v1/positions?account={账户}&exchange={交易所}`（可只传 account，同 3.4）。取持仓中（`deleted=0`）的仓位，收集每条的 `id`、`instrument`、方向、数量、`roi`。
2. **查活跃 risk 规则**：`GET /api/v1/rules?account={账户}&rule_type=risk`（同 3.5）。取未终止（`active`/`pending`/`in_use`）规则的 `position_id`，经关联得出**已有风控的 instrument 集合**。
3. **求差集**：**仓位存在、但没有任何一条活跃 risk 规则绑定它** 的仓位即为「未设置风控仓位」。按仓位维度判定（同一 instrument 多行独立仓位时逐行判断），不按 instrument 去重合并。
4. **表格展示**（带序号，供后续选择）：

   ```
   账户 test_deribit（deribit）未设置风控的仓位：

   | 序号 | 标的 | 方向 | 数量 | 开仓价 | ROI |
   |------|------|------|------|--------|-----|
   | 1 | BTC-PERPETUAL | 多 | 0.5 | 80000 | +12.00% |
   | 2 | ETH-PERPETUAL | 空 | 3 | 3200 | -5.00% |

   共 2 个仓位未设置风控。请告知要为哪些仓位设置风控：
   - 直接回复序号（如 `1,2`）或「设置全部」，将使用**默认规则：止盈=盈利55%回落5%触发，止损=亏损30%，全平**；
   - 也可自带规则与平仓量，如 `1,2 盈利60%回落10%,止损50% 平仓50%` 或 `1,2 盈利60%回落10%,止损50% 平仓5`；
   - 自带规则不带平仓量时默认全平，确认前会明确说明。
   ```

5. 全部仓位都有风控时，告知「该账户所有仓位均已设置风控」并结束，不进入设置流程。

### 3.10.2 批量设置流程（写操作，必须确认）

用户回复后解析**选择、规则、平仓量**三要素：

| 用户回复 | 采用的规则 | 平仓量 |
|----------|-----------|--------|
| 仅序号（`1,2,3`）/「设置全部」/「全部」 | **默认规则**：回落止盈 `盈利55%回落5%触发` + 独立止损 `亏损30%` | 全平（`quantity_pct:1`） |
| 序号 + 显式规则，不带平仓量（`1,2,3 盈利60%回落10%,止损50%`） | 用户给出的规则（回落止盈 + 止损） | 全平（`quantity_pct:1`），**回显确认时明确说明「未指定平仓量，默认全部数量平仓」** |
| 序号 + 显式规则 + `平仓n%`（`… 盈利60%回落10%,止损30% 平仓50%`） | 用户给出的规则 | 比例平仓 `quantity_pct: n/100`（`50%`→`0.5`） |
| 序号 + 显式规则 + `平仓n`（`… 盈利60%回落10%,止损30% 平仓5`） | 用户给出的规则 | 绝对数量平仓 `quantity: n`（`5`→`quantity:5`） |

> - 平仓量写法同 SKILL.md §2.5（`n%`=比例、`n`=绝对数量），关键词 `平仓` 可省略（尾部裸 `50%`/`5` 在规则阈值之后出现时也按平仓量解析，有歧义先追问）。
> - 平仓量对每个选中仓位的**全部规则统一生效**（回落 arm/trail 两条 + 独立止损一条，见 SKILL.md §2.5 的统一生效原则）。

1. **回显确认表**：将「序号 → 仓位（标的/方向/数量/position_id）+ 将创建的规则明细（回落 arm/trail 两条 + 独立止损一条，均按 3.6.4 结构）+ 平仓量」整体列表展示，**等用户明确确认（`y`）后才发请求**；有不明确处（如规则阈值缺失、序号不存在、平仓量歧义）先追问，不盲目执行。用户规则未带平仓量时，确认表中必须**明确写「未指定平仓量，默认全部数量平仓」**。
2. 用户确认后**依次**为每个选中仓位创建规则，每个仓位一次：回落部分 `POST /api/v1/rules/batch`（arm + trail 两条，见 3.6.4），止损部分 `POST /api/v1/rules`（`roi <= -n/100`）。平仓量按上表解析结果统一应用于该仓位的全部规则（默认/未指定时全平 `quantity_pct:1`；用户显式给出时按 `quantity_pct` 或 `quantity`）。
3. **失败跳过、不重试**：某个仓位创建失败（`code != 0` 或网络错误）时**直接跳过该仓位继续下一个**，不自行改参数重试（铁律 SKILL.md §2.7）；该仓位的错误 message 留存。
4. **结束后汇总展示**：

   ```
   风控设置结果：

   | 序号 | 标的 | 方向 | 规则 | 结果 |
   |------|------|------|------|------|
   | 1 | BTC-PERPETUAL | 多 | 回落止盈55%回落5% + 止损30% | ✅ 成功 |
   | 2 | ETH-PERPETUAL | 空 | 回落止盈55%回落5% + 止损30% | ❌ 40901 规则状态非法迁移 |

   成功 1 个，失败 1 个。
   ```

> - 本流程中的「设置全部」只针对**本次筛查出的未设置风控仓位**，不包含已有风控的仓位。
> - 每个仓位的规则创建是独立的；部分成功部分失败属正常结果，如实汇总即可。
> - 若用户回复的序号超出表格范围，指出来并请其更正，不要擅自取舍。
