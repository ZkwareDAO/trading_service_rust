# 策略端 WebSocket 接口文档

> 面向对象：**策略客户端**。策略进程通过一条 WebSocket（南向）长连接接入 `trading-service`，
> 在连接上发送下单 / 撤单 / 查持仓 / 建盯盘规则等命令，并接收服务端异步推送的订单受理与成交回报。
>
> 本文描述的是**当前代码实现的真实协议**（报文权威定义见 `crates/trading-core/src/protocol.rs`，
> 连接处理见 `crates/trading-service/src/south.rs`，业务编排见 `crates/trading-service/src/service.rs`）。
> 管理类 REST 接口见 `docs/接口开发文档.md`，连接鉴权设计见 `docs/服务端与客户端连接.md`。

---

## 1. 总览

```
策略客户端  ─────────── WebSocket（南向，长连接）──────────▶ trading-service
   (strategy)        ws://<south-addr>/ws?strategy_name=…        │
       ◀────────────  ack / order_event / error / positions     │
                                                                 │ 主动外连
                                                                 ▼
                                                          各交易所（REST + 用户数据流）
```

- **传输**：标准 WebSocket，文本帧（UTF-8 JSON）；二进制帧按 UTF-8 解析，等价处理。
- **默认地址**：`ws://127.0.0.1:9100/ws`（服务端 `--south-addr`，默认 `127.0.0.1:9100`；`/` 与 `/ws` 两条路径等价）。
- **一条连接 = 一个策略身份**：身份在握手时由 `strategy_name` 绑定，连接上所有命令都归属该策略；回报也按此身份路由回本连接。
- **异步请求-回调模型**：命令发出后不立即在同一帧得到结果。服务端先做内存操作，再异步调用交易所，处理结果通过**同一连接**以事件帧推回（用 `client_order_id` 关联）。
- **无应用层心跳要求**：客户端可发 WebSocket Ping，服务端回 Pong；建议客户端自行做读超时与断线重连。
- 生产环境应在前置网关启用 TLS，对外使用 `wss://`。

---

## 2. 建立连接（鉴权握手）

### 2.1 连接 URL

```
ws://<south-addr>/ws?strategy_name=<策略名>&nonce=<32hex>&signature=<64hex>
```

| Query 参数      | 必填 | 说明                                                                 |
| --------------- | ---- | -------------------------------------------------------------------- |
| `strategy_name` | 是   | 策略身份，同时是回报路由键；非空。也兼容旧参数名 `strategy`。        |
| `nonce`         | 是   | 客户端**每次连接随机生成**的一次性随机数，128-bit 的 32 位小写 hex。 |
| `signature`     | 是   | HMAC 签名，64 位小写 hex，算法见下。                                 |

### 2.2 签名算法

```
signature = hex_lower( HMAC_SHA256( key = SECRET_KEY,
                                    msg = strategy_name || nonce ) )
```

- `SECRET_KEY` 是服务端与所有策略**共享的同一把密钥**（64 hex，32 字节），由服务端首启自动生成于
  `data/south.key`（权限 `0600`），需通过带外安全渠道分发给各策略客户端。**密钥永不上网传输。**
- 签名消息中 `strategy_name` 与 `nonce` **直接拼接、无分隔符**；两端必须用同一字节布局，不要各自手拼。
- 服务端在 **WebSocket 升级之前**完成全部校验：字段非空 → 恒定时间比较验签 → nonce 防重放占用。
  任一失败统一返回 HTTP **401 `unauthorized`**（不区分原因、不回显策略名/签名），握手无法完成。
- **nonce 防重放**：同一 nonce 在 **5 分钟 TTL** 内只能使用一次，重复即拒（长度不足 32 hex 也拒）。
  因此**每次重连都必须重新随机生成 nonce 并重新签名**。

### 2.3 连接示例（Python）

```python
import asyncio, hmac, hashlib, secrets, json
import websockets

SOUTH = "ws://127.0.0.1:9100/ws"
SECRET = open("data/south.key").read().strip()   # 与服务端同一份 64hex 密钥
STRATEGY = "test_strategy"

def signed_url() -> str:
    nonce = secrets.token_hex(16)                 # 128-bit -> 32 hex
    signature = hmac.new(
        SECRET.encode(),
        (STRATEGY + nonce).encode(),             # 直接拼接，无分隔符
        hashlib.sha256,
    ).hexdigest()
    return f"{SOUTH}?strategy_name={STRATEGY}&nonce={nonce}&signature={signature}"

async def main():
    async with websockets.connect(signed_url()) as ws:
        print("connected, strategy =", STRATEGY)
        await ws.send(json.dumps({
            "option": "place_order",
            "exchange": "binance",
            "account": "test_binance",
            "instrument": "BTCUSDT",
            "instrument_type": "futures",
            "side": "long",
            "order_type": "market",
            "quantity": "0.01",
        }))
        async for frame in ws:                    # 持续接收回调
            print("<<", frame)

asyncio.run(main())
```

仓库自带一个可运行的参考客户端：`cargo run -p trading-service --bin strategy-client -- --option place_order ...`。

### 2.4 本地联调旁路（仅限开发）

服务端以 `--south-dev-insecure` 启动时，连接只需带 `strategy_name`，可省略 `nonce`/`signature`
（参考客户端加 `--insecure`）。**该模式不验签，严禁用于生产**，服务端启动时会显著告警。

### 2.5 连接生命周期与重连

- 同名策略的**新连接会覆盖旧连接**；旧连接随后被读循环关闭并按令牌摘除，不会误删新连接。
- 断线期间该策略的回报会被**丢弃**（服务端只在连接存活时写帧）。重连后不会补推历史事件，
  需要时改用管理 REST（`GET /api/v1/orders?client_order_id=…`、`GET /api/v1/positions`）对账。
- 重连请重新走一遍 2.1/2.2（新 nonce + 新签名）。

---

## 3. 通用报文约定

### 3.1 请求体（策略 → 服务，SouthCommand）

所有命令都是一个 JSON 对象，靠 `option` 字段区分；其余字段宽松可选，服务端按 `option` 校验必填项。

```json
{ "option": "place_order", "exchange": "binance" }
```

> **身份不采信报文体**：即使在对象里填了 `strategy_name`/`strategy`，服务端也只以**握手绑定**的身份为准，
> 命令无需、也不应依赖在体里重复传策略名。

`option` 取值（别名在括号内）：

| option         | 别名                          | 用途                       |
| -------------- | ----------------------------- | -------------------------- |
| `place_order`  | `place`                       | 下单（开仓）               |
| `cancel_order` | `cancel`                      | 撤销订单                   |
| `get_order`    | —                             | 查询单笔订单（活动/终态/历史）|
| `get_positions` | —                            | 查询**本地**持仓（不调交易所）|
| `create_rule`  | `create_watch_rule`           | 创建盯盘/风控规则（含平仓）|
| `subscribe_kline` | `subscribe_klines`         | 订阅 1 分钟 K线推送（`kline_1m_batch`），见第 10 节 |
| `unsubscribe_kline` | `unsubscribe_klines`     | 退订 1 分钟 K线，见第 10 节 |
| `get_klines` | — | 查询本地历史 1 分钟 K线（应答 `kline_history` 帧），见 10.4 |

### 3.2 数据格式

- **定点数**：价格/数量/比率一律定点，禁止浮点语义。请求可传 JSON 数字（`80000`）或字符串（`"80000.5"`）；
  服务端**回调统一为 JSON 数字**。
- **比率口径**：roi / 阈值 / 平仓比例等用小数而非百分数：`12%` 传 `0.12`，全平传 `1`，半仓传 `0.5`。
- **枚举入参宽松、出参规范**：
  - `side` 入参兼容 `long`/`buy`/`bid`/`多`（买）与 `short`/`sell`/`ask`/`空`（卖）；**回调中为大写 `BUY`/`SELL`**。
  - `order_type`：`market` / `limit`（缺省 `limit`）；回调为小写 `market`/`limit`。
  - `instrument_type`：`spot` / `futures` / `option`（缺省 `spot`）；入参 `perpetual`/`perp`/`swap`/`usdm`/`futures_usdm` 均归一为 `futures`，`option`/`options` 归一为 `option`。本项目约定 `futures` 即币安 U 本位永续；`option` 用于 deribit 期权。
  - `exchange`：**大小写敏感、精确匹配**（如 `binance` 或配置的本地模拟所名 `demo_venue`），仅 trim 空白，无别名。
- **标的符号**：传交易所原生符号即可，服务端按归属所归一（如币安收 `btcusdt`，内存/回报均为 `BTCUSDT`）。
- 字段命名统一 `snake_case`（部分字段兼容历史驼峰别名，如 `instrumentType`、`conditionSymbol`）。

### 3.3 回调事件（服务 → 策略，ClientEvent）

所有推送都是同一个 JSON 结构，靠 `type` 区分。字段按事件类型按需出现：**无值的字段直接省略（不输出 `null` 键）**；策略侧解析时对缺省字段按空值处理即可（服务端反序列化同样兼容字段缺省）。

| 字段                | 事件                         | 说明                                                |
| ------------------- | ---------------------------- | --------------------------------------------------- |
| `type`              | 全部                         | `ack` / `order_event` / `order` / `error` / `positions` / `kline_1m` / `kline_1m_batch` |
| `status`            | 全部                         | 订单状态串或 `OK` / `REJECTED`，见 3.4              |
| `client_order_id`   | ack/order_event/部分 error   | `0x`+32hex，下单后关联命令与回报的首选键            |
| `signal_id`         | ack/order_event/部分 error   | 原样回传下单时的来源信号 id，供策略据此识别回报；规则单为 null |
| `exchange_order_id` | ack/order_event              | 交易所原生订单号（受理后才有，可能为 null）         |
| `exchange`          | ack/order_event              | 交易所精确名                                        |
| `strategy_name`     | 全部                         | 回报绑定的策略身份（握手名）                        |
| `instrument`        | ack/order_event              | 归一后的标的                                        |
| `side`              | ack/order_event              | `BUY` / `SELL`（大写）                              |
| `order_type`        | ack/order_event              | `limit` / `market`（小写）                          |
| `order_price`       | ack/order_event              | 委托价（市价单为 `0`）                              |
| `order_quantity`    | ack/order_event              | 委托数量                                            |
| `reduce_only`       | ack/order_event              | 是否只减仓平仓单（`true`=平仓）；其他事件为 null    |
| `filled_price`      | ack/order_event              | 累计成交均价                                        |
| `filled_quantity`   | ack/order_event              | 累计成交量                                          |
| `message`           | ack/error/positions          | 文本说明（错误原因、UNKNOWN 原因、持仓来源说明）    |
| `positions`         | positions                    | 持仓快照数组，见 3.5；其他事件省略该字段            |
| `kline`             | kline_1m                     | 定稿 1 分钟 K线载荷（单根对象），其他事件省略；见第 10 节 |
| `klines`            | kline_1m_batch               | 定稿 1 分钟 K线**数组**（同一交易所同一分钟多标的合为一帧），其他事件省略；见第 10 节 |

这些 `type`：

- **`ack`**：命令已被服务端/交易所受理。
  - 下单受理成功：`status="NEW"`，带订单快照与 `exchange_order_id`。
  - 下单 REST 调用失败但**结果不确定**：`status="UNKNOWN"`，`message` 带原因（**不丢单**，等待用户数据流/对账收敛，可能随后再来 `order_event`）。
  - 撤单成功：`status="CANCELED"`。
  - `create_rule` 成功：纯文本 `ack`，`status="OK"`，`message` 描述规则（本期不回 `rule_id`）。
- **`order_event`**：订单状态被交易所用户数据流推进，体为订单快照，`status` 为新状态。
- **`order`**：`get_order` 的应答，体为该订单快照（字段与下单回报完全同形，见第 7 节）。
- **`error`**：前置校验失败、无法连接交易所、建单失败等；`status="REJECTED"`，原因在 `message`。
- **`positions`**：`get_positions` 的应答，`status="OK"`，本地持仓在 `positions` 数组（字段完整，见 3.5）。
- **`kline_1m`**：定稿 1 分钟 K线**服务端主动推送**（不是某条命令的应答），载荷在 `kline` 对象。**12 个公共字段四所一致**：`venue`/`symbol`/`interval`（固定 `1m`）/`trading_day`（YYYYMMDD，CTP 夜盘归次日）/`start_time`/`end_time`（UTC 毫秒，[start,end)）/`open`/`high`/`low`/`close`/`volume`（周期内增量）/`quote_volume`（周期内计价货币成交额增量；原 `turnover` 已更名）。各所差异指标以**同层平铺字段（extras）**追加，不另开嵌套对象：CTP 出 `exchange_id`（SHFE/DCE/… 真实交易所代码，`venue` 仍是 `ctp`）+ `open_interest`（周期末持仓量）；Binance 出 `trade_num`（成交笔数）/`active_buy_volume`/`active_buy_quote_volume`；deribit/hyperliquid 当前无 extras。extras 键集合按所固定，未来新增所/键为向后兼容的额外字段，**客户端勿开启 deny_unknown_fields**。只推定稿、不补历史；断开期间不补发。
- **`kline_1m_batch`**：同一交易所、同一分钟定稿的**多标的 K线数组合帧**，载荷在 `klines` 数组（每个元素字段与上面 `kline` 对象完全相同，组内按 `(symbol, start_time)` 升序）。服务端按分钟桶水位线/超时把当分钟多根 bar 聚成一帧下发，以减少帧数量；策略侧按数组消费即可。语义与 `kline_1m` 一致（只推定稿、不补历史、断开不补发）。详见第 10 节。

### 3.4 订单状态机（status）

```
PLACING → NEW → PARTIAL → FILLED
   │        └→ CANCELED / EXPIRED
   ├→ REJECTED          （前置校验/连接失败/明确拒单，终态）
   └→ UNKNOWN           （REST 结果不确定，非终态，等待回报收敛）
```

终态：`FILLED` / `CANCELED` / `REJECTED` / `EXPIRED`。
建单瞬间内存为 `PLACING`，但**该态不会主动推给策略**；策略收到的第一个订单类回调通常是 `ack/NEW`
（或 `ack/UNKNOWN`、`error/REJECTED`），其后是若干 `order_event`。

### 3.5 持仓快照（positions[] 元素）

`positions[]` 每个元素的字段与 REST `GET /api/v1/positions` 返回的 **PositionDTO 完全一致（26 个字段）**——
WS 与 REST 走同一个查询核心，业务载荷相同，仅外层帧封装不同。

| 分组 | 字段 |
| --- | --- |
| 身份 | `id`、`account_id`、`rule_id`、`exchange`、`instrument_type`、`instrument`、`side`（大写 `BUY`/`SELL`） |
| 数量/价格 | `quantity`、`multiplier`、`pos_price`、`current_price`、`notional`、`leverage`、`margin` |
| 盈亏 | `settle_currency`、`pnl_value`、`realized_pnl`、`roi`、`max_roi`、`profit_drawdown` |
| 时间/状态 | `expires_at`、`deleted`、`close_time`、`exchange_update_time`、`created_at`、`updated_at` |

> 注意：元素用 **`account_id`**（数值），不含账户名 `account`；`deleted=true` 表示当日已平仓但仍在内存的仓位。
> 本系统仓位主键即 `id`，供 `create_rule`(risk) 绑定（value 来自本节元素的 `id`）。

---

## 4. 下单（开仓）：place_order

### 4.1 请求参数

| 字段              | 必填 | 类型    | 说明                                                                                  |
| ----------------- | ---- | ------- | ------------------------------------------------------------------------------------- |
| `option`          | 是   | string  | 固定 `place_order`                                                                    |
| `exchange`        | 是   | string  | 交易所精确名（大小写敏感）；未注册直接 `error`                                        |
| `account`         | 否   | string  | 账户名（`account.csv` 中的 name），缺省 `default`；须在该交易所下精确存在            |
| `instrument`      | 是   | string  | 交易所原生标的（兼容旧字段 `symbol`）                                                 |
| `instrument_type` | 否   | string  | `futures` / `spot`，缺省 `spot`（兼容 `perpetual/swap/usdm`）                         |
| `side`            | 是   | string  | `long`/`short`（兼容 buy/sell/多/空）；非法回 `error`                                 |
| `order_type`      | 否   | string  | `limit`（缺省）/ `market`                                                             |
| `price`           | 条件 | decimal | 限价单必填且为正；市价单可省略（按 0）；现金口径限价单也用它换算数量              |
| `quantity`        | 二选一 | decimal | 委托/平仓**绝对数量**，**必须为正**；全平传当前持仓全量、减仓传部分量（WS 平仓不支持 quantity_pct）。与 `order_cash` 二选一，传了优先用它 |
| `order_cash`      | 二选一 | decimal | 下单金额（报价币种，如 USDT）。未传 `quantity` 时按 `order_cash ÷ 参考价` 换算数量（截断 8 位小数）：**限价单用委托价**；**市价单用可成交侧盘口价（买吃卖一、卖吃买一），盘口缺失回退估值价 mark>mid>last**；行情未就绪/未接入该所则回 `error` |
| `signal_id`       | 是   | string  | **来源信号 id，WS 下单必传**。一个 signal_id 只对应一笔订单；同 signal_id 重复下单**直接忽略**（不再向交易所下单、也不回推） |
| `leverage`        | 否   | u64     | 期货开仓杠杆，三态：**缺省（不传）= 不调用 set_leverage，维持账户当前杠杆**；显式传值（**含 `1`**）= 下单前总是下发该倍数（可把账户杠杆从 3 调回 1），设置失败该单 REJECTED。平仓单（reduce_only=true）永不设置杠杆 |
| `reduce_only`     | 否   | bool    | `true`=立即平仓信号：配合**反向下单**只减仓不反手；缺省 `false`=普通开仓。仅单向(BOTH)持仓由交易所保证只减仓 |
| `client_order_id` | 否   | string  | 自带幂等键，必须为 `0x`+32hex；非法或缺省由服务端生成                                 |

> 账户与交易所必须能解析（交易所已注册、`account.csv` 有精确匹配且未删除），否则在下单前即回 `error`。
> `test_net`（测试网）是账户属性，不由请求传入，随账户自动带出。
> **取量规则**：`quantity` 与 `order_cash` 至少给一个且为正；都不给回 `error`。现金口径市价单依赖本地行情已就绪，刚启动行情未到达时可能暂时换算失败（回 error），策略可稍后重试。
>
> **立即平仓**：WS 平仓就是 `place_order` + `reduce_only=true` + 反向 `side` + 绝对量 `quantity`
> （多仓平仓发 `short/SELL`，空仓平仓发 `long/BUY`）。平仓单回报与开仓走同一 `ack/order_event` 链路，
> 正常回推本连接（见 4.2，回调多出 `reduce_only:true`）。条件触发式平仓请用第 5 节 risk 规则。

请求示例（市价开多 0.01 BTC 永续，3 倍杠杆）：

```json
{
  "option": "place_order",
  "exchange": "binance",
  "account": "test_binance",
  "instrument": "BTCUSDT",
  "instrument_type": "futures",
  "side": "long",
  "order_type": "market",
  "quantity": "0.01",
  "leverage": 3,
  "signal_id": "sig-20260920-0001"
}
```

现金口径示例（不传 quantity，用 100 USDT 按市价换算数量，如参考价 80000 → quantity=0.00125）：

```json
{
  "option": "place_order",
  "exchange": "binance",
  "account": "test_binance",
  "instrument": "BTCUSDT",
  "instrument_type": "futures",
  "side": "long",
  "order_type": "market",
  "order_cash": "100",
  "signal_id": "sig-20260920-0002"
}
```

立即平仓示例（平掉 0.01 BTC 多仓：反向 SELL + reduce_only，不带 leverage；signal_id 用新的平仓信号 id）：

```json
{
  "option": "place_order",
  "exchange": "binance",
  "account": "test_binance",
  "instrument": "BTCUSDT",
  "instrument_type": "futures",
  "side": "short",
  "order_type": "market",
  "quantity": "0.01",
  "reduce_only": true,
  "signal_id": "sig-20260920-0003"
}
```

> 重复 `signal_id` 的下单命令会被静默丢弃（无任何回调），策略侧不要把"无回调"误当失败重试同一个 signal_id。

### 4.2 应答与回调时序

1. 服务端先在内存建 `PLACING` 单（**不推帧**），随后异步调用交易所。
2. 交易所受理：推一帧 `ack`，`status="NEW"`，含服务端生成/沿用的 `client_order_id` 与 `exchange_order_id`。
3. 成交推进：交易所用户数据流每推进一步，推一帧 `order_event`（`PARTIAL` → `FILLED`），
   含 `filled_quantity` / `filled_price`（成交量仓储保证单调不减、封顶委托量）。
4. 异常：
   - 前置校验失败（缺 `signal_id`、quantity/order_cash 都没给、order_cash 换算时无可用参考价、无法连接交易所、`set_leverage` 失败等）→ `error`（`status="REJECTED"`）。
   - 同 `signal_id` 已有活动订单 → **静默忽略，不回推**（幂等）。
   - REST 已发出但应答不确定 → `ack` + `status="UNKNOWN"` + `message`，不丢单，等待后续 `order_event` 收敛。

受理回调示例：

```json
{
  "type": "ack",
  "status": "NEW",
  "client_order_id": "0x0123456789abcdef0123456789abcdef",
  "signal_id": "sig-20260920-0001",
  "exchange_order_id": "BTC-123456789",
  "exchange": "binance",
  "strategy_name": "test_strategy",
  "instrument": "BTCUSDT",
  "side": "BUY",
  "order_type": "market",
  "order_price": 0,
  "order_quantity": 0.01,
  "reduce_only": false,
  "filled_price": 0,
  "filled_quantity": 0,
  "message": null,
  "positions": null
}
```

成交回报示例：

```json
{
  "type": "order_event",
  "status": "FILLED",
  "client_order_id": "0x0123456789abcdef0123456789abcdef",
  "signal_id": "sig-20260920-0001",
  "exchange_order_id": "BTC-123456789",
  "exchange": "binance",
  "strategy_name": "test_strategy",
  "instrument": "BTCUSDT",
  "side": "BUY",
  "order_type": "market",
  "order_price": 0,
  "order_quantity": 0.01,
  "reduce_only": false,
  "filled_price": 80123.5,
  "filled_quantity": 0.01,
  "message": null,
  "positions": null
}
```

错误示例：

```json
{ "type": "error", "status": "REJECTED", "strategy_name": "test_strategy", "message": "quantity 必须为正数" }
```

---

## 5. 条件平仓：create_rule（risk 风控规则）

WS 平仓有两条路径，按是否需要条件触发选择：

| 场景 | 方式 |
| --- | --- |
| **立即平仓**（信号触发，无条件） | `place_order` + `reduce_only:true` + 反向 `side` + 绝对量 `quantity`，见第 4 节；回报正常回推本连接 |
| **条件平仓**（止盈/止损/回撤等） | `create_rule` 建 `rule_type="risk"` 规则，引擎条件命中时自动下反向、`reduce_only=true` 的平仓单（本节） |

### 5.1 risk 规则如何绑定仓位与平仓

- **risk 不使用 `side`**：创建时由策略直接回传 `position_id`（值来自 `get_positions` 返回元素的 `id`，见 3.5/第 8 节）。
  服务端按 id 取**活跃持仓（deleted=0）**并校验归属同一账户；找不到、已平仓或跨账户直接 `error`，规则不创建。
- 命中时平仓单的交易所/标的/合约形状/**方向**/**杠杆**全部从该仓位派生：方向自动取反
  （多仓→SELL、空仓→BUY），置 `reduce_only=true`，沿用持仓杠杆（不再 set_leverage）。
- 平仓量二选一：
  - **绝对量**：`quantity > 0` 时按该数量平仓；
  - **比例**：否则用 `quantity_pct`，缺省/0 按**全平 `1`**，半仓 `0.5`。
  （注意：这是**条件规则**的比例口径；第 4 节的立即平仓只认绝对量 `quantity`。）
- 平仓单类型由 `order_type` 决定：条件平仓常用 `market`（`price` 省略）；也可 `limit`（`price` 为限价）。
- 条件命中前规则处于 `active`；命中后引擎抢占为 `in_use` 并下出平仓单，平仓单到终态后规则自动 `retired`。

### 5.2 请求参数（risk 平仓规则）

| 字段                   | 必填 | 类型    | 说明                                                                                   |
| ---------------------- | ---- | ------- | -------------------------------------------------------------------------------------- |
| `option`               | 是   | string  | 固定 `create_rule`                                                                     |
| `rule_type`            | 否   | string  | 固定 `risk`（缺省即为 risk）                                                           |
| `position_id`          | 是   | u64     | **绑定的本地仓位 id**（来自 get_positions 元素的 `id`）；risk 不使用 side        |
| `exchange`             | 是   | string  | 交易所精确名（用于解析账户/凭证；规则下单要素以绑定仓位为准）                          |
| `account`              | 否   | string  | 账户名，缺省 `default`；position_id 必须属于该账户                                     |
| `condition_kind`       | 否   | string  | 条件主类：`roi`（缺省）/ `price` / `position` / `profit_drawdown`（`drawdown` 为别名） |
| `condition_instrument` | 否   | string  | price_/position_ 跨标的条件的标的后缀；空=绑定仓位自身标的                             |
| `operator`             | 是   | string  | 比较符：`>=` `<=` `>` `<` `==`                                                         |
| `value`                | 是   | decimal | 阈值（roi 为比率；profit_drawdown 为百分点如 `0.05`；price 为价格；position 为数量）             |
| `action`               | 否   | string  | 命中动作，远程创建仅支持 `self`（缺省）；链式 `rule:<id>` 远程创建本期不开放           |
| `order_type`           | 否   | string  | 平仓单类型，缺省 `limit`；条件平仓一般用 `market`                                      |
| `price`                | 条件 | decimal | 限价平仓价；`market` 可省略                                                            |
| `quantity`             | 否   | decimal | 平仓绝对量；`>0` 时优先于比例；缺省/0 走比例                                           |
| `quantity_pct`         | 否   | decimal | 平仓比例：全平 `1`（缺省）、半仓 `0.5`；`quantity>0` 时忽略                            |
| `pending`              | 否   | bool    | 一般平仓规则留空（缺省 false，创建即 active）                                          |

> risk 规则**不消费** `side` / `instrument` / `instrument_type` / `leverage`，这些均以绑定仓位为准。
> 先 `get_positions`、取匹配元素的 `id` 作为 `position_id`，再建规则。

#### 示例 A：仓位 42  ROI ≥ +12% 时市价全平

```json
{
  "option": "create_rule",
  "rule_type": "risk",
  "exchange": "binance",
  "account": "test_binance",
  "position_id": 42,
  "condition_kind": "roi",
  "operator": ">=",
  "value": "0.12",
  "order_type": "market",
  "quantity_pct": "1"
}
```

#### 示例 B：仓位 42 跌破 80000 时市价全平（止损）

```json
{
  "option": "create_rule",
  "rule_type": "risk",
  "exchange": "binance",
  "account": "test_binance",
  "position_id": 42,
  "condition_kind": "price",
  "operator": "<=",
  "value": "80000",
  "order_type": "market"
}
```

#### 示例 C：仓位 42 盈利回撤超 5% 市价全平（profit_drawdown）

```json
{
  "option": "create_rule",
  "rule_type": "risk",
  "exchange": "binance",
  "account": "test_binance",
  "position_id": 42,
  "condition_kind": "profit_drawdown",
  "operator": ">=",
  "value": "0.05",
  "order_type": "market",
  "quantity_pct": "1"
}
```

### 5.3 创建应答

成功：一帧纯文本 `ack`（`status="OK"`），**不带 rule_id / 平仓单号**：

```json
{ "type": "ack", "status": "OK", "strategy_name": "test_strategy",
  "message": "规则创建成功（risk roi >= 0.12）" }
```

失败：`error`（`status="REJECTED"`），常见原因：

- 未传 `position_id`：`缺少必填字段：position_id（risk 规则必填，来自 get_positions 元素的 id）`；
- 仓位不存在/已平仓/跨账户：`position_id=42 不存在、已平仓或不属于该账户，不能创建风控规则`；
- 缺 `operator` / `value`、操作符非法、阈值缺失等。

### 5.4 条件平仓命中后的回报（重要限制）

- 条件命中后引擎下出的平仓单是**规则单**（带内部 `rule_id`），其订单 **`strategy_name` 为空**，
  因此**该平仓单的 `ack` / `order_event` 不会推送到策略 WS 连接**（回报路由只认 `strategy_name`）。
  需要成交即时回报的立即平仓请走第 4 节 `place_order + reduce_only`（回报正常回推）。
- 条件平仓结果的确认方式：
  1. 调用 `get_positions`（见第 8 节）查看持仓是否归零/减量；
  2. 或经管理 REST `GET /api/v1/orders`、`GET /api/v1/positions` 查询。

### 5.5 开仓规则（rule_type="new"）与 order_cash

`create_rule` 也可建**开仓规则**：`rule_type="new"`，命中后按规则行自带的
`exchange`/`instrument`/`instrument_type`/`side`/价/量下单（非 reduce_only）。
与 risk 的区别：

| 项 | new 开仓规则 | risk 风控规则 |
| --- | --- | --- |
| `position_id` | 固定不传（0） | 必填，绑定活跃仓位 |
| `side` | **必填**，开仓方向 | 不消费（命中方向取仓位反向） |
| `instrument`/`exchange`/`instrument_type` | 必填，下单要素 | 不消费，以绑定仓位为准 |
| 取量 | `quantity` 或 `order_cash` 二选一 | `quantity` 或 `quantity_pct` |
| `leverage` | 可带，命中先 set_leverage | 不消费 |

**new 规则支持 `order_cash`**：`quantity=0`、给 `order_cash` 即可；规则**命中的瞬间**
服务端按 `order_cash ÷ 参考价` 换算数量——口径与 `place_order` 完全一致（复用同一换算：
限价用委托价；市价用可成交侧盘口、回退 mark>mid>last）。

- 命中时若行情尚未就绪（无可用参考价）：本轮**不抢占规则、不下单**，规则保持
  `active`，下一轮扫描自动重试（不会像 place_order 那样直接 REJECTED）。
- 换算示例：`order_cash=100`、mark=50000 → `quantity=0.002`。

new + order_cash 示例（BTCUSDT 市价买，金额 100 USDT）：

```json
{
  "option": "create_rule",
  "rule_type": "new",
  "exchange": "binance",
  "account": "test_binance",
  "instrument": "BTCUSDT",
  "instrument_type": "futures",
  "side": "long",
  "order_type": "market",
  "order_cash": "100",
  "condition_kind": "price",
  "condition_instrument": "BTCUSDT",
  "operator": ">=",
  "value": "0"
}
```

### 5.6 跨标的条件（condition_instrument）与链式激活

`condition_instrument`（条件后缀）让规则的判定**不读自身绑定仓位、改读指定标的**：

| condition_kind | 空后缀 | 填 `condition_instrument`（跨标的） |
| --- | --- | --- |
| `price` | 绑定/规则标的自身价 | 按**标的名称**查行情：本所优先，查不到全局按名回退 |
| `position` | 自身持仓量 | 本账户在该标的的持仓量 |
| `roi` | 绑定仓位自身 ROI | **本 rule 账户**在该标的活跃仓位的 ROI |
| `profit_drawdown` | 绑定仓位自身回撤 | **本 rule 账户**在该标的活跃仓位的盈利回撤 |

- **跨标的取价**不再固定走币安合约：先按规则所属所/合约形状精确查，查不到（如
  deribit 账户的"BTC 价涨过 70000"）再全局按 symbol 匹配，确定性优先级取
  （futures > option > spot，价格 mark>mid>last）。后续期权某一标的的价格条件同理。
- **跨标的 ROI/盈利**：服务端用**本 rule 的 account** + 目标标的查活跃仓位，读其
  roi/profit_drawdown；该账户在目标标的无活跃仓位则本轮不触发。
- 典型用法是跨标的条件命中后 **`action="rule:<id>"` 激活另一条 pending 的开仓规则**，
  被激活的 new 规则再按 5.5 下单（注意：链式动作远程创建本期不开放，但可在条件
  规则里引用已存在的 pending 规则 id）。

跨标的 ROI 示例：本账户的 ETHUSDT 仓位 ROI ≥ 20% 时，激活 id=50 的开仓规则：

```json
{
  "option": "create_rule",
  "rule_type": "risk",
  "exchange": "binance",
  "account": "test_binance",
  "position_id": 42,
  "condition_kind": "roi",
  "condition_instrument": "ETHUSDT",
  "operator": ">=",
  "value": "0.2",
  "action": "rule:50"
}
```

---

## 6. 撤单：cancel_order

| 字段              | 必填 | 类型   | 说明                                                     |
| ----------------- | ---- | ------ | -------------------------------------------------------- |
| `option`          | 是   | string | 固定 `cancel_order`                                      |
| `client_order_id` | 是   | string | 待撤订单的客户单号（系统查/撤首选键）                    |
| `account`         | 否   | string | 撤单 REST 需要凭证，真实所建议与下单一致；缺省 `default` |

```json
{ "option": "cancel_order", "client_order_id": "0x0123456789abcdef0123456789abcdef", "account": "test_binance" }
```

应答：

- 撤单成功：`ack`，`status="CANCELED"`，体为该订单快照。
- 订单不存在：`error`（`订单不存在`）。
- 无法连接交易所 / 交易所报错：`error`，`message` 带原因（订单状态不变）。

---

## 7. 查询订单：get_order

查询**单笔订单**，与下单/回报共用同一订单快照结构。支持**两个等价的键**（二选一）：

- `client_order_id`（cid）；
- `signal_id`（策略只持有自己下发的信号 id、cid 由服务生成未回传时用它）。

**三级查询（对策略透明）**：

1. 活动内存订单（PLACING/NEW/PARTIAL 等）；
2. 当日已终态、被软删的 tombstone（FILLED/CANCELED/REJECTED）；
3. 跨日已从内存 purge 的订单 → 自动回读历史归档 CSV，返回最新一日状态。

| 字段              | 必填 | 类型   | 说明                                              |
| ----------------- | ---- | ------ | ------------------------------------------------ |
| `option`          | 是   | string | 固定 `get_order`                                 |
| `exchange`        | 是   | string | 交易所精确名（大小写敏感），如 `binance`         |
| `account`         | 是   | string | 账户名（服务据此校验所/账户归属并取凭证）         |
| `client_order_id` | 二选一 | string | 按客户单号查（`0x`+32hex）                       |
| `signal_id`       | 二选一 | string | 按信号 id 查；自动限定在握手策略名下             |

`client_order_id` 与 `signal_id` **至少给一个**，否则回 error。

示例 A（按 cid）：

```json
{ "option": "get_order", "exchange": "binance", "account": "test_binance",
  "client_order_id": "0x0123456789abcdef0123456789abcdef" }
```

示例 B（按 signal_id，不带 cid）：

```json
{ "option": "get_order", "exchange": "binance", "account": "test_binance",
  "signal_id": "sig_c143d1e7af13b2e2" }
```

应答（`type="order"`，字段与 `ack`/`order_event` 完全同形）：

```json
{
  "type": "order",
  "status": "FILLED",
  "strategy_name": "sar_supertrend",
  "client_order_id": "0xa1cd49ac62b54b2f832a855b3cfaafce",
  "signal_id": "sig_c143d1e7af13b2e2",
  "exchange": "binance",
  "exchange_order_id": "28598855743",
  "instrument": "BTCUSDT",
  "side": "BUY",
  "order_type": "market",
  "order_price": 0,
  "order_quantity": 0.002,
  "reduce_only": false,
  "filled_price": 86235,
  "filled_quantity": 0.002,
  "message": null
}
```

- **归属校验**：只能查询握手策略名下的订单；查询其他策略的单、订单不存在，统一回 `error`（`订单不存在`），不泄露订单是否存在。
- 缺 `exchange` / `account`、或 cid 与 signal_id 都不传：`error`。

> REST 等价查询：`GET /api/v1/orders?account=&signal_id=&strategy_name=`。两条入口共用同一个查询核心，行为一致。

---

## 8. 查询持仓：get_positions

查询**本地仓位表**（不调用交易所 API），与 REST `GET /api/v1/positions` 走**同一个查询核心**。
所有参数都可传可不传——**传了就按该条件筛选，不传不处理**；账户合法但无匹配时返回空列表。

| 字段              | 必填 | 类型   | 说明                                                                 |
| ----------------- | ---- | ------ | -------------------------------------------------------------------- |
| `option`          | 是   | string | 固定 `get_positions`                                                |
| `account`         | 否   | string | 账户名（`account.csv` 的 name）；给了连同 exchange 解析，不给不按账户筛 |
| `exchange`        | 否   | string | 交易所精确名（大小写不敏感匹配仓位）                                 |
| `instrument`      | 否   | string | 标的，精确匹配                                                       |
| `instrument_type` | 否   | string | `futures`/`spot`/`option`                                           |
| `side`            | 否   | string | 方向，`long`/`short`（兼容 buy/sell）                               |
| `min_roi`         | 否   | decimal| 仅返回 ROI ≥ 该值的仓位                                              |
| `max_roi`         | 否   | decimal| 仅返回 ROI ≤ 该值的仓位                                              |
| `deleted`         | 否   | i64    | `0`=仅活跃、`1`=仅当日已平仓、不传=全部                              |

```json
{ "option": "get_positions", "account": "test_binance", "exchange": "binance",
  "instrument_type": "futures", "instrument": "BTCUSDT" }
```

应答（`type="positions"`，元素为完整 26 字段的 PositionDTO，见 3.5）：

```json
{
  "type": "positions",
  "status": "OK",
  "positions": [
    { "id": 42, "account_id": 1, "rule_id": 0, "exchange": "binance",
      "instrument_type": "futures", "instrument": "BTCUSDT", "side": "BUY",
      "quantity": 0.01, "pos_price": 80000.0, "roi": 0.0, "deleted": false }
  ]
}
```

- 账户合法但无匹配（或全平）：`positions: []`，**不是错误**。
- `account + exchange` 解析失败（账户在该所未配置）、或 side/instrument_type/deleted 非法：回 `error`。
- **范围限制**：只返回内存当日数据；跨日已 purge 的历史仓位本期无法经此查询（历史仓位查询待后续版本）。

---

## 9. 端到端时序（开仓 → 风控平仓）

```
策略                                        trading-service                     交易所
 │  WS 握手(strategy_name+nonce+sig) ───────────────▶│ 验签/注册连接
 │◀──────────── 101 Switching Protocols ──────────────│
 │  place_order(market,long,BTCUSDT,qty=0.01,lev=3) ▶ │ 内存 PLACING ──REST─▶ set_leverage/place
 │◀──── ack NEW (client_order_id, exchange_order_id) ─│◀──── 受理 ────────────│
 │◀──── order_event PARTIAL (filled_quantity…) ───────│◀──── 用户数据流 ──────│
 │◀──── order_event FILLED ───────────────────────────│◀──── 用户数据流 ──────│
 │  get_positions ───────────────────────────────────▶│ 读本地仓位表          │
 │◀──── positions（含 position_id=42）────────────────│
 │  —— 需要立即平仓时 ——                                │
 │  place_order(side=short,qty=0.01,reduce_only) ─────▶│──── REST 平仓 ───────▶│
 │◀──── ack NEW (reduce_only=true) ───────────────────│◀──── 受理 ────────────│
 │◀──── order_event FILLED ───────────────────────────│◀──── 用户数据流 ──────│
 │  —— 需要条件平仓时 ——                                │
 │  create_rule(risk, position_id=42, roi>=0.12 全平) ▶│ 按 id 绑定活跃仓位/落 active
 │◀──── ack OK "规则创建成功…" ───────────────────────│
 │                                                    │ 命中 roi 条件：自动下反向 reduce_only 平仓单
 │  (平仓单为规则单，无 strategy_name，不回推本连接)    │──── REST 平仓 ───────▶│
 │  get_positions ───────────────────────────────────▶│ 读本地仓位表          │
 │◀──── positions（持仓已归零 → []）──────────────────│
```

---

## 10. K线订阅：subscribe_kline / unsubscribe_kline

> 2026-10-25 新增。当前仅 1 分钟周期。
> 四家交易所（binance / deribit / hyperliquid / ctp）走同一套命令；**xili 等本地模拟所不支持**。

### 10.1 订阅 subscribe_kline

```json
{
  "option": "subscribe_kline",
  "exchange": "ctp",
  "instrument_type": "futures",
  "symbols": ["cu", "cu2512"],
  "interval": "1m"
}
```

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `exchange` | 是 | 精确名：`binance` / `deribit` / `hyperliquid` / `ctp` |
| `symbols` | 是* | 标的数组：具体合约（`BTCUSDT`/`cu2512`）或 **CTP 品种前缀**（`cu`，服务端展开为该品种全部在交易合约并只推当前主力）。也可改用单个 `symbol`/`instrument` 字段 |
| `instrument_type` | CTP/币圈建议传 | `futures`（binance/hyperliquid/ctp）或 `option`（deribit）；缺省按 `spot`，订阅真实所时请显式传 |
| `interval` | 否 | 仅支持 `1m`（兼容 `1min`/`1minute`/缺省）；其它周期返回 error |

- 同一连接对同一 (exchange,symbol,interval) 重复订阅**幂等**；多个连接订阅同一标的时只向交易所订一份（引用计数）。
- 成功回纯文本 `ack`（`status="OK"`）；缺 `exchange`、未知所、不支持的周期、空标的列表均回 `error`（`status="REJECTED"`）。
- 订阅后无需重复发送：服务端每 500ms 对账，首次 key 出现即自动向交易所增订。

### 10.2 退订 unsubscribe_kline

同样字段（`symbols` 指定要退的标的）。最后一个连接退订某标的后，服务端下轮对账向交易所退订。

```json
{ "option": "unsubscribe_kline", "exchange": "ctp", "symbols": ["cu2512"] }
```

### 10.3 推送 kline_1m_batch

只在 1 分钟 K线**定稿**后推送，盘中变化不推，不经命令应答通道、无历史补发。服务端统一用 **`kline_1m_batch`** 帧：同一交易所、同一分钟定稿的多根 bar 聚成一帧的 `klines` 数组（组内按 `(symbol, start_time)` 升序）；该分钟只有一根时数组长度也为 1。

```json
{
  "type": "kline_1m_batch",
  "exchange": "ctp",
  "klines": [
    {
      "venue": "ctp",
      "symbol": "cu2512",
      "interval": "1m",
      "trading_day": 20261024,
      "start_time": 60000,
      "end_time": 120000,
      "open": 75000, "high": 75200, "low": 74950, "close": 75100,
      "volume": 12,
      "quote_volume": 9000000,
      "exchange_id": "SHFE",
      "open_interest": 120000
    }
  ]
}
```

Binance 同信封、同 12 个公共字段，差异只在 bar 根平铺的 extras（无 `exchange_id`/`open_interest`）：

```json
{
  "type": "kline_1m_batch",
  "exchange": "binance",
  "klines": [
    {
      "venue": "binance", "symbol": "BTCUSDT", "interval": "1m",
      "trading_day": 20261028, "start_time": 0, "end_time": 60000,
      "open": 100, "high": 111, "low": 99, "close": 108,
      "volume": 4, "quote_volume": 432.0,
      "trade_num": 1280,
      "active_buy_volume": 2.5,
      "active_buy_quote_volume": 270.0
    }
  ]
}
```

- 信封只出现有值字段：批量帧顶层即 `type`/`exchange`/`klines`，不输出订单/持仓等无关键（无值字段省略，不是 `null`）。
- `start_time`/`end_time` 为 UTC 毫秒、区间 [start,end)；`trading_day` 为 YYYYMMDD（CTP 夜盘归下一交易日）。
- `volume` 是该分钟**增量**（CTP 由当日累计量差分得到）；`quote_volume` 为计价货币成交额增量，四所公共字段恒在（不产出时为 0）。extras 按所平铺：CTP=`exchange_id`/`open_interest`，Binance=`trade_num`/`active_buy_volume`/`active_buy_quote_volume`，其余所不带 extras。
- CTP 由墙钟在分钟边界定稿（稀疏成交也不拖到下一笔 tick）；只对收到过 tick 的窗口产 bar，空窗不补。
- **订阅品种 `cu` 只收到当前主力合约**（如 cu2512）的 bar；订阅具体合约 `cu2512` 则直推该合约。主力每天收盘后按可配置指标选举、**候选需连续 N 个有数据交易日胜出**才切换（默认 3，防止一日噪声），且**月份只能前进不回退**；下一交易日全天冻结，换月随帧内 `symbol` 自然变化（切换后不会退回旧月份）。
- 推送复用本连接的发送队列：某连接不可达时该帧丢弃（不阻塞行情线程），服务端累计丢弃计数并周期告警；断开期间的定稿 bar 不补发。
- 连接断开（含同名重连替换旧连接）会自动清退该连接的全部 K线订阅。
- 兼容说明：协议结构里仍保留单根形态（`type=kline_1m`、载荷在 `kline` 对象，字段与数组元素相同），但当前服务端**统一只下发 `kline_1m_batch`**；策略按数组消费即可。

### 10.4 历史查询 get_klines

只读服务端本地（内存保留窗 + 归档 CSV），**不调交易所**；无数据返回空数组，不报错。

```json
{
  "option": "get_klines",
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "start_time": 1761696000000,
  "end_time": 1761699600000,
  "limit": 300
}
```

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `exchange` | 是 | `binance` / `deribit` / `hyperliquid` / `ctp` |
| `symbol` | 是 | 具体合约（也兼容 `instrument` 字段） |
| `instrument_type` | 否 | 历史查询按 `(exchange, symbol)` 取数，可省略 |
| `start_time` | 否 | UTC 毫秒，含；缺省=取最近 N 根 |
| `end_time` | 否 | UTC 毫秒，不含；缺省=开放 |
| `limit` | 否 | 返回根数上限；缺省 300，超过服务端硬上限 1500 截断 |

应答为自定义 `kline_history` 帧（不是 `ack`）：

```json
{
  "type": "kline_history",
  "status": "OK",
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "interval": "1m",
  "start_time": 1761696000000,
  "end_time": 1761699600000,
  "has_more": false,
  "klines": [
    {
      "venue": "binance", "symbol": "BTCUSDT", "interval": "1m",
      "trading_day": 20261028, "start_time": 0, "end_time": 60000,
      "open": 100, "high": 111, "low": 99, "close": 108,
      "volume": 4, "quote_volume": 432.0,
      "trade_num": 1280,
      "active_buy_volume": 2.5,
      "active_buy_quote_volume": 270.0
    }
  ]
}
```

- `klines` 元素字段与 `kline_1m_batch` 的 bar 完全相同（12 公共字段 + extras）。
- `start_time`/`end_time` 回带本次实际生效的查询键；空结果且未给区间时为 `null`。
- `has_more=true` 表示还有更多数据；用最后一根 bar 的 `end_time` 作为下次 `start_time` 翻页。
- 参数非法（缺 `exchange`/`symbol`、`start_time >= end_time`、不支持的 `interval`）回 `error` 帧。
- 数据来源为本地留存；未落盘/已过保留窗且未归档的历史区间可能为空。

### 10.5 落盘（与推送并行，非南向内容）

服务端另把定稿 bar 与 CTP 原始逐笔 tick 批量 append 成 CSV（目录 `data/kline/{venue}/{1m|tick}/{YYYYMMDD}/{symbol}.csv`），
写盘周期由启动参数 `--kline-flush-interval` 控制（默认 `5s`）。这部分不经过南向 WS，仅作本地留存；历史查询走南向 `get_klines`（见 10.4）。

---

## 11. 错误处理与重连清单（策略实现建议）

1. **握手 401**：检查密钥是否与服务端 `data/south.key` 一致、nonce 是否 32hex、拼接顺序是否为 `strategy_name||nonce`、nonce 是否在 5 分钟内被复用。
2. **解析失败**：报文不是合法 `SouthCommand` JSON 时回 `error`（`报文解析失败: …`），连接保持，可继续发命令。
3. **未知 option**：回 `error`（`未知命令 …`）。
4. **断线**：指数退避重连，**每次用新 nonce 重新签名**；断线期间回报不补推，必要时用 REST 对账。
5. **关联命令与回报**：下单必带唯一 `signal_id`（一个信号一笔单，重复 signal_id 被静默忽略、无回调，切勿据此重发同一 id）；如需自行掌控订单幂等键可另带合法 `client_order_id`（`0x`+32hex），否则记录服务端在 `ack/NEW` 中返回的值。
   - 金额下单：不给 `quantity` 改给 `order_cash`；现金口径市价单依赖行情就绪，换算失败会收到 REJECTED，可稍后重试。
6. **UNKNOWN 不是终态**：收到 `ack/UNKNOWN` 不要假定失败，继续等待 `order_event` 或用 REST 查单。
7. **平仓语义**：
   - **立即平仓**：`place_order` + `reduce_only:true` + 反向 `side` + 绝对量 `quantity`（WS 平仓不支持 quantity_pct），回报经本连接回推；平仓单不要传 `leverage`（永不设置杠杆）。
   - **条件平仓**：先 `get_positions` 取匹配元素的 `id` 作为 `position_id`，再 `create_rule(risk)` 时回传 `position_id`、不传 `side`；规则平仓是规则单、不回推本连接，用 `get_positions`/REST 确认。
   - 显式传 `leverage`（含 1）总是会调 set_leverage；不传则维持账户当前杠杆。

---

## 12. 附录：参考客户端

仓库内参考实现（含签名、各 option 的参数装配）：

```bash
# 市价开多（联调旁路，服务端需 --south-dev-insecure）；下单必带 --signal-id
cargo run -p trading-service --bin strategy-client -- \
  --option place_order --strategy-name test_strategy --exchange binance --account test_binance \
  --instrument BTCUSDT --instrument-type futures --side long --order-type market \
  --quantity 0.01 --leverage 3 --signal-id sig-0001 --insecure

# 现金口径开多：不给 --quantity，用 100 USDT 按市价换算数量
cargo run -p trading-service --bin strategy-client -- \
  --option place_order --strategy-name test_strategy --exchange binance --account test_binance \
  --instrument BTCUSDT --instrument-type futures --side long --order-type market \
  --order-cash 100 --signal-id sig-0002 --insecure

# 立即市价平多 0.01（反向 short + reduce-only，不带 leverage）
cargo run -p trading-service --bin strategy-client -- \
  --option place_order --strategy-name test_strategy --exchange binance --account test_binance \
  --instrument BTCUSDT --instrument-type futures --side short --order-type market \
  --quantity 0.01 --reduce-only --insecure

# 先查本地仓位拿元素 id 作为 position_id，再创建 ROI>=12% 市价全平风控规则（risk 用 --position-id，不用 --side）
cargo run -p trading-service --bin strategy-client -- \
  --option get_positions --exchange binance --account test_binance \
  --instrument-type futures --insecure
cargo run -p trading-service --bin strategy-client -- \
  --option create_rule --rule-type risk --exchange binance --account test_binance \
  --position-id 42 \
  --condition-kind roi --operator '>=' --value 0.12 --order-type market --quantity-pct 1 --insecure
```

正式（HMAC 签名）模式去掉 `--insecure`，并用 `--secret-file` 指向与服务端相同的密钥文件（默认 `./data/south.key`）。

