# 平仓 / 仓位查询 详情

> trade-ops 参考文档：即时平仓（§3.3）、仓位查询（§3.4）。
> 通用解析约定、确认铁律见上级 SKILL.md §2；本文只含端点、参数、字段、示例与分步流程。

---

## 3.3 平仓（全部 / 部分）

无独立平仓接口，**平仓 = 对持仓反方向下单 + `reduce_only:true`**：

1. 先 `GET /api/v1/positions` 拿到目标仓位的 `side`、`quantity`、`instrument`；
2. 平仓方向取反：多仓（BUY）→ `sell`，空仓（SELL）→ `buy`；
3. 数量：全平 = 仓位 `quantity`；部分平仓 = `仓位数量 × 比例`；
4. `POST /api/v1/orders`，`reduce_only:true`，市价 `order_type:"market"`（推荐）。

```bash
# 平掉 0.1 BTCUSDT 多仓的 50%（0.05）
curl -s -X POST {api.base}/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "exchange":"binance","account":"test_binance",
    "instrument":"BTCUSDT","instrument_type":"futures",
    "side":"sell","order_type":"market",
    "quantity":0.05,"leverage":1,"reduce_only":true
  }'
```

> 「到价/到比例自动平仓」不是这里的即时平仓，应创建 risk 规则（见 risk-rules.md §3.6）。
> **CTP 平仓**：流程相同（反向 + `reduce_only:true`，适配器据此走平仓方向 Offset::Close），但数量必须是**正整数手数**；按比例部分平仓只有在「持仓手数 × 比例」为整数时可行（如 4 手平 50%=2 手），非整数结果先与用户确认改成整数手数，不自行取整。

---

## 3.4 仓位查询（唯一仓位接口）

触发词：`{账户} 仓位列表` / `仓位查询` / `仓位`。

**GET** `{base}/api/v1/positions`

```bash
curl -s -G {api.base}/api/v1/positions \
  --data-urlencode "account=test_binance" \
  --data-urlencode "exchange=binance"
```

- 口语通常给 `{账户} 仓位…`，可**只传 account 不传 exchange**（服务端先全局按名解析账户，再返回该账户全部持仓）；账户名有歧义（多所同名）时再让用户补交易所。

query（全部可选、AND 组合）：

| 参数 | 说明 |
|------|------|
| account / exchange | 账户名 + 交易所（建议成对） |
| instrument | 原生符号精确匹配（BTCUSDT） |
| instrument_type | `futures`/`spot`/`option` |
| side | `BUY`/`SELL` |
| min_roi / max_roi | 按当前 roi 比率过滤 |
| deleted | **可选**：`0`=仅持仓中，`1`=仅已平仓，不传=不筛选 |
| page / page_size | 分页，默认 page=1、page_size=50 |

`data.list[]`（PositionDTO，关键字段）：

| 字段 | 说明 |
|------|------|
| **`id`** | **仓位 id = 创建 risk 规则用的 `position_id`** |
| instrument / instrument_type / side | 标的、类型、方向（BUY 多/SELL 空） |
| quantity | 持仓数量 |
| pos_price | 开仓均价 |
| current_price | 当前估值价 |
| leverage / margin / notional | 杠杆 / 占用保证金 / 名义价值 |
| pnl_value | 未实现盈亏（settle_currency 计） |
| roi | 未实现盈亏率（保证金收益率 = 浮盈 ÷ 占用保证金；比率，展示 ×100 加 %） |
| max_roi / profit_drawdown | 峰值盈利率 / 盈利回落率 |
| settle_currency | 结算币种（USDT） |
| deleted / close_time | 是否平仓 / 平仓时间 |

> **没有 merge**：单向持仓同标的只有一行；`hedge` 双向持仓时多、空是**各自独立两行**，原样展示，不做净仓合并。
> 仓位来自服务端对账流水线（交易所回报 + 定时对账 + 行情重估），刚下单立即查可能尚未反映。

展示（roi/pnl 正数加 `+`，roi ×100 保留 2 位加 %）：

```
账户 test_binance（binance）持仓：
| 仓位ID | 标的 | 方向 | 数量 | 开仓价 | 标记价 | 杠杆 | 盈亏 | ROI |
|--------|------|------|------|--------|--------|------|------|-----|
| 42 | BTCUSDT | 多 | 0.5 | 80000 | 84000 | 10x | +200U | +50.00% |
共 1 个持仓
```

**单条**：`GET /api/v1/positions/{id}`。
**交易所实时仓位**（可选，直连交易所、默认不改内存）：`POST /api/v1/exchange/positions`，体 `{exchange, account, instrument_type:"futures", sync:false}`，`data.list[]` 字段为交易所快照口径（instrument/side/quantity/pos_price/current_price/leverage/margin/pnl_value 等）。
