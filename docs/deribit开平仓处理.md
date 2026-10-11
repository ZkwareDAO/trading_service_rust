### 3.1 价格 tick 的分档选择

Deribit 期权 tick 不是常量，而是价格的函数。给定当前价（mark）从 `tick_size_steps` 中选档：

```
function tick_for_price(price, details):
    for step in reverse(details.tick_size_steps):   # 从最高档位往下
        if price >= step.above_price:
            return step.tick_size
    return details.tick_size                        # 回退基础 tick

function truncate_to_tick(price, tick, ceil):       # 对齐到 tick 整数倍
    return ceil ? ceil(price/tick)*tick : floor(price/tick)*tick
```

### 3.2 市价单转限价 —— 开仓的「漂移档位」定价

Deribit 无真市价单，市价统一转限价。开仓（reduce_only=false）采用**相对 mark 价漂移 5 个 tick 的被动挂价**（不直接吃盘口，争取滑点优势；是否成交由后续监控/重试推进）：

```
function open_price(symbol, side):
    ticker  = get_ticker(symbol)        # mark_price + best_bid + best_ask
    details = get_instrument(symbol)
    tick    = tick_for_price(ticker.mark_price, details)

    if side == BUY:                     # 开多：低于 mark 5 tick，向下取整
        price = truncate_to_tick(mark_price - 5*tick, tick, floor)
    else:                               # 开空：高于 mark 5 tick，向上取整
        price = truncate_to_tick(mark_price + 5*tick, tick, ceil)
    return price
```

说明：这是有意的被动挂单（价格不保证立刻成交，triggered ≠ 已成交）。取整方向保证挂单价不越过目标价。

### 3.3 平仓的价差处理（reduce_only=true）

分两层：**先盘口价差闸门（SpreadChecker），过闸后定价直接用对手价**。

（a）价差闸门 —— 平仓前检查买一卖一价差，过宽则不下单、转人工告警：

```
function check_spread_before_close(symbol, threshold, cooldown=10min):
    ticker = get_ticker(symbol)
    spread = abs(ticker.best_ask - ticker.best_bid)
    log(bid, ask, spread, threshold)

    if spread > threshold:
        # 异步发企业微信「买一卖一差距悬殊，需要手动操作」，附带 bid/ask/spread/当前ROI
        if now - last_notify_time[symbol] >= cooldown:   # 每 symbol 限流，冷却内不重复发
            send_manual_close_notification(...)
            last_notify_time[symbol] = now
        return SpreadTooWide                               # 关键：返回专用错误
    return ok
```

调用方（风控执行器）行为：

```
if exchange == "deribit":
    spread, threshold, err = check_spread_before_close(...)
    if err is SpreadTooWide:
        # 不下单；风控规则保持 active，后续周期重试（同一仓位多次重试合并为一次事件）
        return SpreadTooWideError(...)
```

（b）过闸后的平仓定价 —— **不用 mark、不做 tick 截断，直接挂可成交对手价**：

```
function close_price(symbol, side):
    ticker = get_ticker(symbol)
    if side == BUY:   # 平空：买回，吃卖一
        return ticker.best_ask
    else:             # 平多：卖出，吃买一
        return ticker.best_bid
```

要点：
- 开仓 = 被动价（买 = mark−5 tick、卖 = mark+5 tick，取整到分档 tick）；平仓 = 直接用 best_bid/best_ask（不截断）。
- 价差闸门只对 Deribit 平仓生效；阈值为绝对价差，告警每 symbol 10 分钟冷却。
- 价差过宽不是「报错放弃」，而是「保留规则、周期重试 + 通知人工」。

### 3.4 下单整体流程

```
function place_deribit_order(req):
    if req.order_type == MARKET:
        req.price      = req.reduce_only ? close_price(symbol, side)   # 3.5 对手价
                                         : open_price(symbol, side)    # 3.4 买 mark−5tick / 卖 mark+5tick 被动价
        req.order_type = LIMIT                                        # 市价一律转限价
    amount = aligned_amount(symbol, req.quantity)                     # 3.2 数量对齐
    method = side == BUY ? "private/buy" : "private/sell"
    call_rpc(method, {instrument_name, amount, type=limit, price, reduce_only?})

    on "amount invalid" error:
        invalidate_cached_size_step(symbol)                           # 3.1 缓存失效
    on tick_size / invalid price|amount error:
        返回精度错误（让交易所校验结果透出）
```