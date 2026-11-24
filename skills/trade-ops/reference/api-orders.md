# 账户 / 开仓 / 订单 详情

> trade-ops 参考文档：账户查询（§3.1）、即时开仓（§3.2）、条件开仓（§3.2.1）、订单查询与撤单（§3.9）。
> 通用解析约定、确认铁律见上级 SKILL.md §2；本文只含端点、参数、示例与分步流程。

---

## 3.1 账户查询

**GET** `{base}/api/v1/accounts`

可选 query：`exchange`、`name`、`is_testnet`、`position_mode`。

```bash
curl -s -G {api.base}/api/v1/accounts
```

`data.list[]` 字段（**只有非敏感字段，绝不会有 api_key/secret**）：

| 字段 | 说明 |
|------|------|
| name | 账户名（下单的 `account`） |
| exchange | 交易所精确名 |
| position_mode | `one_way` 单向 / `hedge` 双向 |
| is_testnet | 是否测试网 |

展示：

```
可用账户：
| 账户名 | 交易所 | 持仓模式 | 测试网 |
|--------|--------|----------|--------|
| test_binance | binance | 单向 | 是 |
```

下单/查仓/建规则前若用户给的 account 不确定，先用本接口校验账户存在且交易所匹配。

---

## 3.2 开仓

**POST** `{base}/api/v1/orders`（异步：立即返回内存 `PLACING` 单，最终状态需轮询）

**合约市价开多**：

```bash
curl -s -X POST {api.base}/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "exchange": "binance",
    "account": "test_binance",
    "instrument": "BTCUSDT",
    "instrument_type": "futures",
    "side": "buy",
    "order_type": "market",
    "quantity": 0.1,
    "leverage": 10
  }'
```

**按金额市价开多**（用户说「1000u、10x 开多 BTC」，直接传 order_cash，服务端换算数量）：

```bash
curl -s -X POST {api.base}/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "exchange": "binance",
    "account": "test_binance",
    "instrument": "BTCUSDT",
    "instrument_type": "futures",
    "side": "buy",
    "order_type": "market",
    "order_cash": 1000,
    "leverage": 10
  }'
```

**合约限价开空**：

```bash
curl -s -X POST {api.base}/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "exchange": "binance",
    "account": "test_binance",
    "instrument": "BTCUSDT",
    "instrument_type": "futures",
    "side": "sell",
    "order_type": "limit",
    "price": 80000,
    "quantity": 0.1,
    "leverage": 10,
    "time_in_force": "GTC"
  }'
```

**参数**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| exchange | string | ✓ | 精确交易所名 |
| account | string | ✓ | 账户名；**指令中缺账户词位时必须追问用户，禁止缺省 `account` 向服务端发请求**（不依赖服务端 default 兜底）；不确定时先查 `/accounts` 让用户选，不要自造账户名 |
| instrument | string | ✓ | 原生符号（BTCUSDT / 期权完整名） |
| instrument_type | string | ✓ | `futures`/`spot`/`option` |
| side | string | ✓ | `buy`/`sell`（兼容 long/short/多/空） |
| order_type | string | ✓ | `market`/`limit`（默认 limit） |
| price | number | 限价必填 | 委托价；市价按金额下单时作为换算计价基准（缺省服务端取最新行情价） |
| quantity | number | 二选一 | 委托数量（币/张），必须为正；与 order_cash 二选一 |
| order_cash | number | 二选一 | 下单金额（**名义金额**，单位 U，即仓位名义价值、非保证金，如 `1000u`→1000）；服务端按 `数量=order_cash÷参考价` 换算，杠杆仅影响保证金，agent 不换算 |
| leverage | int | ✗ | 杠杆，默认 1；期货 >1 时服务端下单前先 set_leverage |
| time_in_force | string | ✗ | `GTC`/`IOC`/`FOK`/`DAY`，默认 GTC |
| reduce_only | bool | ✗ | 仅减仓；**平仓单 true**，开仓不传或 false |
| strategy_name | string | ✗ | 策略身份（需要 WS 回报路由时） |
| signal_id | string | ✗ | 信号幂等键（一个 signal_id 仅一笔订单） |

> **不接受** `client_order_id`（一律服务端生成，响应里返回）；`test_net` 随账户属性，不由请求传。

**响应 `data`（OrderDTO，关键字段）**：`id`、`client_order_id`、`status`（建单即时 `PLACING`）、`exchange`、`instrument`、`side`、`order_price`、`order_quantity`、`order_cash`、`leverage`、`reduce_only`、`filled_quantity`、`created_at`。

**下单后轮询确认**：

```bash
curl -s -G {api.base}/api/v1/orders \
  --data-urlencode "account=test_binance" \
  --data-urlencode "exchange=binance" \
  --data-urlencode "client_order_id=0x...."
```

状态机：`PLACING → NEW → PARTIAL → FILLED`；失败 `REJECTED`/`UNKNOWN`，撤单 `CANCELED`。终态单当前仍 `deleted=0`，可直接查到。

**指令格式（各交易所参数形态不同，务必分开解析）**：

**① Binance（现货 / U 本位永续）—— 有「现货/合约」词位、有杠杆、可用金额**

| 类型 | 指令 |
|------|------|
| 市价开仓（金额，最常用） | `市价开仓 {交易所} 合约 {账户} {symbol} {side} {n}x {金额}u` |
| 市价开仓（数量） | `市价开仓 {交易所} 合约 {账户} {symbol} {side} {数量} {n}x` |
| 限价开仓（金额） | `限价开仓 {交易所} 合约 {账户} {symbol} {价格} {side} {n}x {金额}u` |
| 限价开仓（数量） | `限价开仓 {交易所} 合约 {账户} {symbol} {价格} {side} {数量} {n}x` |

- 例：`市价开仓 B 合约 u BTC long 1x 80u`；`限价开仓 币安 合约 u BTC 60000 long 1x 80u`。
- 杠杆位 `{n}x` 必填语义位；金额 `{金额}u`→`order_cash`，或直接给数量→`quantity`，二选一。

**② Deribit（期权）—— 无「现货/合约」词位、无杠杆、无金额，只认张数**

| 类型 | 指令 |
|------|------|
| 市价开仓（向下五档，order_type=market） | `市价开仓 {交易所} {账户} {期权合约名} {side} {张数}` |
| 限价开仓（以传入价为准，order_type=limit） | `限价开仓 {交易所} {账户} {期权合约名} {价格} {side} {张数}` |

- 例：`市价开仓 D monthopt ETH-18SEP26-2550-P short 2`；`限价开仓 D monthopt ETH-18SEP26-2550-P 0.085 short 2`。
- `{账户}` 即紧随交易所的词位（如 `monthopt`、`user_name`）；**不要**给 deribit 形态找杠杆/金额位，`{张数}` 裸数字→`quantity`，`instrument_type:"option"`。

**③ CTP（国内期货）—— 无「现货/合约」词位、无杠杆、无金额，只认正整数手数**

| 类型 | 指令 |
|------|------|
| 市价开仓 | `市价开仓 {交易所:C/ctp} {账户} {合约号} {side} {手数}` |
| 限价开仓 | `限价开仓 {交易所:C/ctp} {账户} {合约号} {价格} {side} {手数}` |

- 例：`市价开仓 C sim_ctp rb2510 long 2`；`限价开仓 ctp sim_ctp rb2510 3200 short 1`。
- `{手数}` 为**裸正整数** → `quantity`（1 手 = 1 张；小数手数如 `1.5` 会被服务端/柜台拒绝，遇到先追问，不自行取整）。
- **禁止**在 CTP 形态解析 `{n}u`/`order_cash`（金额）与 `{n}x`/`leverage`（杠杆）；用户写了金额或杠杆 → 不换算、不静默丢弃，明确提示「CTP 只用手数、无程序化杠杆」并请其确认/改报。
- `{合约号}` 原样透传（大小写敏感，如 `rb2510`、`cu2512`、`IF2512`），`instrument_type:"futures"`、`exchange:"ctp"`。
- 市价单柜台口径为 AnyPrice+IOC；**上期所/能源中心品种不支持市价**（会被柜台拒绝），此类合约请用户改用限价，失败时按 §2.7 原样返回 message。

**CTP 开仓请求示例（限价 3200 卖开 1 手 rb2510）**：

```bash
curl -s -X POST {api.base}/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "exchange": "ctp",
    "account": "sim_ctp",
    "instrument": "rb2510",
    "instrument_type": "futures",
    "side": "sell",
    "order_type": "limit",
    "price": 3200,
    "quantity": 1
  }'
```

**通用解析规则**

- token 按语义识别而非下标：带 `x`=杠杆、带 `u`=金额（`order_cash`）、`%`=比率、纯数字按所在形态与句式判为价格/数量/张数/手数，**不要**只按下标取值。
- 仅 Binance 形态有 `现货/合约` 词位；出现完整期权合约名（`ETH-18SEP26-2550-P`）即 deribit/option 形态；交易所为 `C/ctp/CTP` 即 CTP 形态（裸整数=手数，无 x/u 词位，合约号原样）。
- **账户词位必填**：指令中没有账户（如裸写交易所后直接跟合约名）时**必须先追问用户账户名，禁止不带 `account` 向服务端发请求**（不依赖服务端 default 兜底）；用户不确定时先查 `/accounts` 列出可用账户让其选择，不要自造账户名。该规则对**全部交易所**生效（含已支持下单形态的 binance / deribit / ctp，以及保留代号识别的 hyperliquid；hyperliquid 服务端未接通时如实告知，不实际发请求）。

---

### 3.2.1 条件开仓（单条 new 规则）

**POST** `{base}/api/v1/rules`，**单对象**。条件与下单要素在**同一条** new 规则上：引擎每轮评估规则自身的 `condition_*`，命中则**当轮**用规则行上的下单要素建单（`action:"self"`，命中即转 in_use，天然只下一单）。无需批量、无需 pending。

**触发动词决定 `order_type`**：

| 触发前缀 | rule_type | order_type |
|----------|-----------|------------|
| `市价条件开仓` | `new` | **`market`**（条件命中后市价下单，无 `price`） |
| `限价条件开仓` | `new` | **`limit`**（条件命中后按规则行 `price` 挂限价） |

下例为**币安 · 市价条件开仓**（`市价条件开仓 B 合约 u BTC long 1x 80u btc价格涨过70000`）：

```bash
curl -s -X POST {api.base}/api/v1/rules \
  -H "Content-Type: application/json" \
  -d '{
    "account": "{账户}",
    "rule_type": "new",
    "exchange": "binance",
    "instrument": "BTCUSDT",
    "instrument_type": "futures",
    "side": "buy",
    "order_type": "market",
    "order_cash": 80,
    "leverage": 1,
    "condition_kind": "price",
    "condition_instrument": "BTCUSDT",
    "operator": ">=",
    "value": 70000,
    "action": "self"
  }'
```

- 条件来自指令**尾部整段文字**（提取规则见 SKILL.md §2.4），不要求用户写 `==…==`；上例条件即 `btc价格涨过70000`。
- **`condition_instrument` 必须显式填写**，即使与下单标的相同；裸币种由 agent 转大写（btc→`BTCUSDT`）。
- 下单要素各交易所形态同 3.2：
  - **Binance**：有杠杆、可用 `order_cash`（金额）或 `quantity`；限价条件另带 `price`。
  - **Deribit（期权）**：无杠杆/金额，只要合约名 + 方向 + 张数（`quantity`），`instrument_type:"option"`。
    - 市价：`市价条件开仓 D monthopt ETH-18SEP26-2550-P short 2 ETH-18SEP26-2500-P 仓位大于 0`；
    - 限价：`限价条件开仓 D monthopt ETH-18SEP26-2550-P 0.095 short 2 btc价格涨过70000`（`0.095` 为命中后限价单的 `price`）。
  - **CTP（国内期货）**：无杠杆/金额，只要合约号 + 方向 + **正整数手数**（`quantity`），`exchange:"ctp"`、`instrument_type:"futures"`；合约号原样透传。
    - 市价：`市价条件开仓 C sim_ctp rb2510 long 2 rb2510价格涨过3300`；
    - 限价：`限价条件开仓 ctp sim_ctp rb2510 3200 short 1 rb2510价格跌破3000`（`3200` 为命中后限价单的 `price`）。
- **new 规则一条只承载一个条件**：尾部出现多个条件且分不出主次时，向用户确认采用哪一个，不要合并或建多条。
- 创建前先回显确认表（触发条件 + 下单要素），用户 `y` 后才 POST。

给用户展示（条件命中后系统会自动下单；展示规则已建立）：

```
条件开仓已建立：
| 账户 | 触发条件 | 标的 | 方向 | 类型 | 价格 | 数量/金额 | 杠杆 | 状态 |
| {账户} | BTCUSDT价格 >= 70000 | BTCUSDT | 多 | 市价 | - | 80u | 1x | ✅ 生效中 |
```

---

## 3.9 订单查询与撤单

### 3.9.1 订单查询

触发词：`{账户} 订单列表` / `订单查询` / `订单`。

**GET** `{base}/api/v1/orders`，只按 `account`（建议配 `exchange`）即可，无需 client_order_id。

**默认只看活跃挂单**：「订单列表/订单查询」默认只返回未成交 / 部分成交单，**必须带状态过滤**（否则后端不按状态筛选、会把 FILLED/CANCELED/REJECTED 等终态全部返回）：

```bash
curl -s -G {api.base}/api/v1/orders \
  --data-urlencode "account=test_deribit" \
  --data-urlencode "exchange=deribit" \
  --data-urlencode "status[]=NEW" \
  --data-urlencode "status[]=PARTIAL"
```

- 状态参数用 `status[]=` 方括号写法（裸 `status=` 后端不认）。
- 用户明确要「全部订单 / 历史 / 已成交 / 已取消」时，才去掉状态过滤或改成对应 `status[]`（如已成交 `status[]=FILLED`、全部则不传 status）。

query（全部可选、AND 组合；可重复的按多值过滤）：

| 参数 | 说明 |
|------|------|
| account / exchange | 账户名 + 交易所（建议成对） |
| instrument | 原生符号精确过滤（BTCUSDT / `ETH-18SEP26-2550-P`） |
| instrument_type / side | `futures`/`spot`/`option`；`BUY`/`SELL` |
| status | 可重复：`NEW`/`PARTIAL`/`FILLED`/`CANCELED`/`REJECTED` |
| client_order_id / exchange_order_id | 精确查单 |
| strategy_name / signal_id / rule_id | 按来源过滤 |
| start_time / end_time | Unix 毫秒闭区间 |
| deleted | `0` 仅未删 / `1` 仅已删 / 不传含全部 |
| page / page_size | 分页，默认 id 倒序 |

展示（关键字段取自 OrderDTO：`client_order_id`、`instrument`、`side`、`order_type`、`order_price`、`order_quantity`、`status`、`created_at`）：

```
账户 test_binance 的订单：
| 序号 | 标的 | 方向 | 类型 | 委托价 | 数量 | 状态 | 时间 |
|------|------|------|------|--------|------|------|------|
| 1 | BTCUSDT | 多 | 市价 | - | 0.10 | FILLED | 09-23 10:05 |
共 1 条
```

> client_order_id 常规不展示长串；在撤单场景内部留存用于逐笔 DELETE。

### 3.9.2 撤单（含按标的「取消挂单」）

**DELETE** `{base}/api/v1/orders` —— `client_order_id`（及 account/exchange）走 **query 参数**，不读 JSON body。**后端只能按 client_order_id 逐笔撤，不支持只给标的一键批量撤**。

```bash
curl -s -X DELETE "{api.base}/api/v1/orders?client_order_id=0x9f1c4a2b8d7e4f60a1c3d5e7f90214b6&account=test_binance&exchange=binance"
```

`{交易所} {账户} {标的} 取消挂单`（如 `B u ETH-18SEP26-2550-P 取消挂单`）按下列流程实现，**不要**直接对 DELETE 只传 instrument（会报 `40001 缺少 client_order_id`）：

1. **查挂单**：`GET /orders?account=&exchange=&instrument=&status[]=NEW&status[]=PARTIAL`（状态参数必须用 `status[]=` 方括号写法，裸 `status=` 会被 serde_qs 拒绝），列出该标的未成交/部分成交单；
2. 无挂单则告知「该标的没有可撤销的挂单」并结束；
3. **回显将撤销的订单列表，等用户 `y`**；
4. `y` 后对每一笔的 `client_order_id` **逐个发 DELETE**，汇总每笔结果（成功 → `CANCELED`）；
5. 任一单失败，如实展示该笔 message，不影响其余撤单。

> 注：对 `/orders` DELETE 发 JSON body 会被忽略。撤单异步，必要时再用 3.9.1 轮询确认 `CANCELED`。
