# ctp-mock — CTP 前置事件模拟器

一个**全天在线、可被真实 trading-service 进程当作 CTP 测试网连接**的模拟器。
它不实现交易所撮合，而是作为「前置事件模拟器」：接收 CTP 请求、按可配置脚本
回推回调事件，并用一本**最小但自洽的账**保证「下单成交 → 查仓对账 → 重估」
等全生命周期链路数据一致。

- **ABI 替身 .so**：`libctpmock_td.so` / `libctpmock_md.so`，导出与官方
  v6.7.13（union/评测版）同名的 Itanium mangled 构造/版本符号；业务代码零改动，
  把 `venue.toml` 的 `trader_so/md_so` 指向替身即可（对标 binance / Hyperliquid
  / Deribit 测试网）。
- **不实现 FTD/TCP 线协议**：官方 `.so` 内部是私有的 FTD/TCP，无法用普通 TCP
  服务器冒充；因此采用 ABI 兼容替身（见 plan §1）。
- **不做交易所细节**：不撮合、不校验手数上下限/涨跌停/PriceTick、不做保证金/
  风控/限仓、不生成盘口、无交易日历。除少数**柜台字段级内置规则**（SHFE 不支持
  市价、强制显式平今/平昨等，见「柜台内置规则」）外，错误分支不由规则「推出」，
  而由控制面/场景**显式注入**。

> 承诺的是「处理逻辑一致」（回调时序、字段口径、状态机、台账自洽），不保证
> 参数真值。仅用于服务全生命周期联调；真实参数/行为以 SimNow 对拍为准（见文末）。

## 目录

```
third_party/ctp-mock/
├── Cargo.toml                # 嵌套 workspace（独立 target，不拖慢主 workspace）
├── ctp-mock-core/            # rlib：会话协议/成交脚本/台账/字段/行情/控制面（纯逻辑 L1）
├── ctp-mock-td/              # cdylib：交易前置替身 libctpmock_td.so
├── ctp-mock-md/              # cdylib：行情前置替身 libctpmock_md.so
├── ctp-mock-ctl/             # bin：控制面 CLI ctp-mock-ctl
├── tests/
│   ├── abi_e2e.rs            # L2：真实 ctp2rs 句柄 dlopen 替身（默认运行）
│   └── blackbox_e2e.rs       # L3：真实常驻进程黑盒（#[ignore]，-- --ignored 运行）
└── examples/headless_runner.rs  # 联调用常驻进程（dlopen 两个 .so）
```

## 构建

需要 libclang（ctp2rs 经 bindgen 解析官方头）。脚本会自动探测本机 LIBCLANG_PATH。

```bash
scripts/build-ctp-mock.sh            # debug
scripts/build-ctp-mock.sh --release  # release
```

产物在 `third_party/ctp-mock/target/<profile>/`：
`libctpmock_td.so`、`libctpmock_md.so`、`ctp-mock-ctl`、
`examples/headless_runner`。

## ABI 与字段口径

- 复用 vendored `ctp2rs` 的全部 bindgen 类型，字段位宽零偏差；替身 API 对象
  `#[repr(C)]` 首字段为同构 vtable 指针，vtable **全字节清零后只具名覆盖白名单槽**，
  其余槽为 null（白名单外调用即显式崩溃，而非静默走错）。
- 回调直接经客户端 `CThostFtdc{Spi}Ext.spi_ptr`（`*mut dyn TraderSpi/MdSpi`）
  调用 Rust trait 方法，替身只保存 Ext、**绝不释放**；`Release` 先 join 回调线程
  再回收自身，保证 ctp2rs 在 Release 返回后释放 Ext 时无回调在飞。
- 定长字段：OrderRef `[c_char;13]`（数字短号原样回传 ≤12 位 + NUL）；
  OrderSysID/TradeID `[c_char;21]`（`"    YYYYMMDD-seq"` 前导空格风格）；
  TradingDay/TradeDate/InsertTime/TradeTime `[c_char;9]`（`YYYYMMDD` / `HH:MM:SS`）；
  StatusMsg/ErrorMsg `[c_char;81]`（GB18030 中文）；时间均为北京时间 UTC+8。
- 字符码：买卖 `'0'/'1'`，开平 `'0'/'1'/'3'/'4'`，投机 `'1'`；价格 `'1'`市价/
  `'2'`限价；时间条件 `'1'IOC/'3'GFD`；委托状态 `'3'未成交排队/'1'部分成交排队/
  `'0'全成/'5'撤`；持仓方向 多`'2'`/空`'3'`；无对手价用 `f64::MAX`。

## 六条生命周期链路

1. **下单→订单事件**：受理 `'3'` →（脚本成交 RtnTrade + `'1'` 累计）→ `'0'` 全成；
   OrderRef 原样、分配 21B OrderSysID；eid 受理态→SysID 升级、终态回收会话表。
2. **查单**：`ActiveQuery` 单在途闸门（在途再发 `-3`），逐行回账本 + `bIsLast` 收尾。
3. **撤单**：三元组（Front+Session+OrderRef）与系统号（Exchange+OrderSysID）两路命中；
   撤成异步 `'5'`，找不到 `25`，已全成/已撤 `26`，重复撤 `24`（显式注入）。
4. **查仓**：成交做带符号净仓/今昨简单加减（无风控），查仓派生多/空、今/昨行
   （含 0 量行）；乘数/保证金率为静态夹具，PositionCost/UseMargin/PositionProfit
   字段非空且自洽；SHFE/INE 区分今昨，其他所净额。
5. **行情→重估**：Md 登录/订阅后，自动模式按确定性游走推 tick（LastPrice>0、
   bid<last<ask），脚本可精确推价、造缺侧（f64::MAX）、注入登录拒绝 64。
6. **断线重连**：`disconnect(reason)` 保留账本，`reconnect()` 重发 OnFrontConnected；
   重连登录回传的 MaxOrderRef 延续，旧委托仍可查/撤。

## 柜台内置规则（默认常开，与成交脚本正交）

不做保证金/限仓等风控，但对**真实柜台的字段级合法性**做最小模拟（不由场景注入、
不可关）；拒绝都走异步 `on_rsp_order_insert`（受理码仍返 0），错误码/文案逐字取自
v6.7.13 `error.xml`：

| 触发条件 | ErrorID | 文案 |
| --- | --- | --- |
| **SHFE** 市价单（`OrderPriceType=AnyPrice`，无论开平） | `80` | 交易所不支持的价格类型 |
| **SHFE** 笼统平仓 `CombOffsetFlag=Close('1')` | `15` | 报单字段有误 |
| **SHFE** 显式平今 `'3'` 量 > 该方向今仓 | `50` | 平今仓位不足 |
| **SHFE** 显式平昨 `'4'` 量 > 该方向昨仓 | `51` | 平昨仓位不足 |

- 即：SHFE 平仓**必须由客户端拆成显式平今/平昨两笔**（跨今昨一单两腿），模拟器与
  真实上期所一致拒绝笼统 `Close`；非 SHFE 合约的 `AnyPrice+IOC` 市价单与笼统
  `Close`（柜台自动冲抵今/昨）一律照常受理。
- 回报帧回填 `TimeCondition`：市价 IOC `'1'` / 限价 GFD `'3'`（请求未显式给时按
  价格类型推断）。
- 判定只按 `ExchangeID=="SHFE"` 正向命中（INE 能源中心规则类似，首版不纳入）；
  夹具对非 SHFE 前缀合约给空 `ExchangeID`，天然按非 SHFE 放行。
- 一次性脚本注入（`reject-next` / `Reject(code)`）在内置规则放行后才生效，规则拒单
  不会消耗注入。

> 码 `15` 为 SHFE 笼统平仓的现口径（error.xml 无专用码）；SimNow 对拍若回其他码
> （如 91），改 `ctp-mock-core/src/sim/rules.rs` 单一常量即可。

## 成交脚本 FillMode（不撮合，不看盘口）

- `InstantFill`（默认）：`'3'` → 一笔成交（限价用委托价，市价用脚本价）→ `'0'`。
- `AcceptedOnly`：仅受理挂 `'3'`（用于挂单→查询→撤单链路）。
- `PartialThenFill`：`'3'` → 按脚本逐笔（`'1'` 累计）→ `'0'`（脚本量不足自动补齐）。
- `Reject(code)`：录入即拒，`on_rsp_order_insert` 带真实 CTP ErrorID（如 30）。

## 控制面（行分隔 JSON over 127.0.0.1 TCP）

替身构造时读环境变量起控制服务器（仅 loopback）：

| 环境变量 | 含义 | 缺省 |
| --- | --- | --- |
| `CTP_MOCK_CTRL_TD` | 交易控制端口（如 43011） | 不启动 |
| `CTP_MOCK_CTRL_MD` | 行情控制端口（如 43012） | 不启动 |
| `CTP_MOCK_AUTO_TICK` | 非空/非 0：订阅后自动游走行情 | 关 |
| `CTP_MOCK_AUTO_INTERVAL_MS` | 自动行情间隔 | 500 |
| `CTP_MOCK_PRESET` | 非空/非 0：构造时预置主测合约规格（rb2612/cu2611/au2612/ag2701/ru2612/i2701 真实交易所+乘数）并播 rb2612 今2/昨3 多仓，解决宿主冷启动“合约预热先于下单、行情兴趣集为空”两个时序缺口 | 关 |
| `CTP_MOCK_LOG` | 日志级别（`error/warn/info/debug/trace/off`）；设置后安装 stderr logger。联调排查 tick 用 `CTP_MOCK_LOG=debug`，逐 tick 推送走 target `ctp_mock_core::tick`；未设置则完全静默（若宿主自装 `log` 后端则交由宿主） | 不安装（静默） |

协议：一行一个 JSON 请求，一行一个 JSON 应答 `{"ok":true,...}` /
`{"ok":false,"error":...}`。用 `ctp-mock-ctl td|md <cmd>` 驱动，或直接发 JSON。

```bash
export CTP_MOCK_CTRL_TD=43011 CTP_MOCK_CTRL_MD=43012   # CLI 与 .so 须一致

ctp-mock-ctl td set-fill InstantFill
ctp-mock-ctl td set-fill AcceptedOnly
ctp-mock-ctl td set-fill Partial:1,1
ctp-mock-ctl td reject-next 30
ctp-mock-ctl td cancel-error 25        # 24 重复撤 / 25 找不到 / 26 状态不允许；0 取消
ctp-mock-ctl td seed-position rb2510 long 3 --today 2 --yd 1 --avg 3200
ctp-mock-ctl td ledger
ctp-mock-ctl td disconnect 0x1001
ctp-mock-ctl td reconnect

ctp-mock-ctl md tick rb2510 --last 3210 --bid 3209 --ask 3211
ctp-mock-ctl md auto rb2510 --base 3200 --interval-ms 500
ctp-mock-ctl md auto-stop rb2510
ctp-mock-ctl md missing-bid rb2510 --last 3210 --ask 3211
ctp-mock-ctl md missing-ask rb2510 --last 3210 --bid 3209
ctp-mock-ctl md login-reject 64
```

同进程（不跨 dlopen）另有进程内句柄（按构造 flow_path 取实例）：
`ctpmock_td::ffi::td_api::control_for(flow)` /
`ctpmock_md::ffi::md_api::control_for(flow)`，供 L2 直接注入。

## 历史 tick 文件回放（模拟实时行情前置）

除自动游走/脚本精确推价外，行情替身可从**仓库外的历史采集文件**读逐笔 tick，
按真实前置的回调结构 `CThostFtdcDepthMarketDataField` 推送——**数据值取历史文件，
帧上 `TradingDay/ActionDay/UpdateTime/UpdateMillisec` 一律盖成推送当下的北京时间**，
对外等同「此刻在线的真实前置」，只是行情来自历史；播完默认循环。

**配置走 ctp-mock 目录下的配置文件（不用环境变量）**：复制模板

```bash
cp third_party/ctp-mock/ctp-mock.example.toml third_party/ctp-mock/ctp-mock.toml
```

`.so` 被服务 dlopen、CWD 不可控，发现顺序：从 cwd 逐级向上找 `./ctp-mock.toml`
与 `./third_party/ctp-mock/ctp-mock.toml`，再回落到 `third_party/ctp-mock/ctp-mock.toml`。
找不到/`enabled=false` 时回放静默关闭，自动游走/脚本行为不变。实配 `ctp-mock.toml`
不入库（见 `.gitignore`）。

数据目录按 `{data_root}/Tick/YYYYMM/YYYYMMDD/{合约}.csv` 索引，`date` 留空时自动选
最新交易日目录；合约＝文件 stem，订阅某合约且命中索引才惰性启动回放（与自动游走互斥、
回放优先）。配置项：`data_root`(支持 `~/`)、`speed`、`interval_ms`、`max_gap_ms`、
`loop`、`instruments`(白名单，空＝全部)，语义见模板注释。

> 回放按**绝对 deadline**（文件起点 epoch + 累计计划毫秒）调度，不是逐笔相对 `sleep`；
> 长回放不会累积睡眠过冲造成的漂移（实测 30 分钟偏移跨度 <10ms）。单笔相邻间隔仍受
> `max_gap_ms` 截断，`loop=true` 每轮重读时首帧立即推送。

源文件为 **15 列、带 UTF-8 BOM** 的采集格式（与服务自身落盘的 33 列 RawTick 不同），
盘口是一列 Python 字面量 `quotes`（恒 1 档），表头：

```text
exchange,symbol,open,high,low,price,cum_volume,cum_amount,last_volume,last_amount,\
trade_type,cum_position,quotes,created_at,type
```

其中 `quotes` 形如 `"[{'bid_p': 108000.0, 'bid_v': 3, 'ask_p': 109000.0, 'ask_v': 3}]"`，
`created_at` 为带时区北京时间、小数秒位数可变（如 `2026-10-08 08:59:00.500000+08:00`）。

字段映射到 SimNow 回调（源中没有的列保持柜台默认 0/空，不伪造）：

| 回调字段 | 源列 / 取值 |
| --- | --- |
| `InstrumentID` / `ExchangeID` | `symbol` / `exchange` |
| `LastPrice` / `OpenPrice` / `HighestPrice` / `LowestPrice` | `price` / `open` / `high` / `low` |
| `Volume`(i32) / `Turnover` / `OpenInterest` | `cum_volume`(饱和转 i32) / `cum_amount` / `cum_position` |
| `BidPrice1`/`BidVolume1`、`AskPrice1`/`AskVolume1` | `quotes` 首档 `bid_p/bid_v`、`ask_p/ask_v` |
| 缺侧（`quotes=[]` 或价 ≤ 0） | 该侧价发 `DBL_MAX`、量 0（服务端归一为 0，与真实前置一致） |
| `TradingDay`/`ActionDay`/`UpdateTime`/`UpdateMillisec` | **推送当下**北京日/时分秒/毫秒 |
| 2~5 档、昨结/昨收/今收/结算价/涨跌停/均价/虚实度等 | 源无此列，保持 0/空 |

`trade_type`/`type`/`last_volume`/`last_amount` 在深度行情结构里无对应字段，忽略。
逐行流式读取（不整载入），坏行跳过计数。**`1m`（13 列 K线）本次不接行情回调。**

运行期可经控制面重读配置/停某合约回放：

```bash
ctp-mock-ctl md replay-reload        # 重读 ctp-mock.toml、重建索引、按订阅重启回放
ctp-mock-ctl md replay-stop CU2706   # 停止某合约回放
```

## 真实进程黑盒联调（trading-service）

```bash
# 1) 构建替身 + CLI
scripts/build-ctp-mock.sh

# 2) 加假凭证（is_production_mode=false）
cargo run -p trading-service --features ctp --bin account-tool -- add-account   # 按提示

# 3) 带控制面环境变量启动服务，使用 mock venue 配置
export CTP_MOCK_CTRL_TD=43011 CTP_MOCK_CTRL_MD=43012 CTP_MOCK_AUTO_TICK=1
cargo run -p trading-service --features ctp -- \
  --venue-config config/venue.ctp-mock.toml

# 4) strategy-client 南向 WS 下「限价买开 rb2510 2 手」；另一终端：
curl -s localhost:<port>/api/v1/positions       # 成交去抖对账后建多仓 +2
ctp-mock-ctl td ledger                          # mock 侧台账
# 挂单→查询→撤单：
ctp-mock-ctl td set-fill AcceptedOnly
# （再下一单，REST 查到 New 后撤单）；# 观察 backoff/重连与补全量：
ctp-mock-ctl td disconnect 0x1001
```

## 测试

```bash
cd third_party/ctp-mock
# L1 纯逻辑（会话/脚本/台账/字段/行情/控制协议）+ L2 真实句柄 dlopen 冒烟
LIBCLANG_PATH=/usr/lib/x86_64-linux-gnu cargo test --workspace
# L3 真实常驻进程黑盒（默认忽略，需先 build 替身与 headless_runner）
cargo build --workspace && cargo build --example headless_runner
cargo test --test blackbox_e2e -- --ignored
# clippy
LIBCLANG_PATH=/usr/lib/x86_64-linux-gnu cargo clippy --workspace --all-targets
```

主仓 ABI 零漂移检查：mock 与服务复用同一份 vendored ctp2rs bindgen，
`cargo build -p trading-exchange --features ctp` 不受影响。

## 边界与后续

明确**不做**：FTD/TCP 线协议、撮合/订单簿、Parked/Exec/Quote/Comb/期权/银期/
OTP、白名单外 CTP 接口（其 vtable 槽保持 null）。夹具（乘数/保证金率、
OrderSysID 风格、自动游走、MaxMarginSideAlgorithm 等）可随时换；与 SimNow
真实行为对拍的校验留作后续（本模拟器只承诺处理逻辑一致，不承诺参数真值）。
