//! strategy-client（trading-service 包内二进制）：策略模拟器（WS 客户端）。
//! 连接南向端口（URL 上带 strategy_name 绑定身份），按命令行参数发送一条
//! place_order / cancel_order / get_position 命令，并持续打印收到的回报。

use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use tokio_tungstenite::tungstenite::Message;
use trading_core::protocol::SouthCommand;
use trading_core::southauth;

#[derive(Debug, Parser)]
#[command(name = "strategy-client", version)]
struct Args {
    #[arg(long = "option", default_value = "place_order")]
    option: String,
    #[arg(long = "strategy-name", alias = "strategy_name", default_value = "erp")]
    strategy_name: String,
    #[arg(long = "exchange", alias = "exchange_mock", default_value = "xili")]
    exchange: String,
    #[arg(long = "account", default_value = "test_binance")]
    account: String,
    #[arg(long = "instrument", default_value = "BTCUSDT")]
    instrument: String,
    #[arg(long = "side", default_value = "long")]
    side: String,
    #[arg(long = "order-type", alias = "order_type", default_value = "limit")]
    order_type: String,
    #[arg(long = "price", default_value = "80000")]
    price: String,
    /// 委托/开仓数量；risk 绝对量平仓也用它（比例平仓留空，走 --quantity-pct）。
    #[arg(long = "quantity", default_value = "")]
    quantity: String,
    /// 下单金额（USDT 等报价币种）：与 --quantity 二选一，缺 quantity 时按
    /// order_cash/参考价 换算数量（limit 用委托价；market 用盘口/估值价）。
    #[arg(long = "order-cash", alias = "order_cash", default_value = "")]
    order_cash: String,
    /// 来源信号 id（WS 下单必传）；同一 signal_id 重复下单会被服务端忽略。
    #[arg(long = "signal-id", alias = "signal_id")]
    signal_id: Option<String>,
    #[arg(long = "client-order-id", alias = "client_order_id")]
    client_order_id: Option<String>,
    /// 标的类型：spot / futures（默认 spot；打币安合约传 futures）。
    #[arg(
        long = "instrument-type",
        alias = "instrument_type",
        default_value = "spot"
    )]
    instrument_type: String,
    // —— create_rule 专用（place_order 仅额外消费 leverage）——
    /// 期货开仓杠杆：显式给值（含 1）下单前总是 set_leverage（可从高倍调回 1），
    /// 缺省则维持账户当前杠杆；平仓单（--reduce-only）与 risk 规则忽略本参数。
    #[arg(long)]
    leverage: Option<u64>,
    /// 规则类型：new / risk。
    #[arg(long = "rule-type", alias = "rule_type", default_value = "risk")]
    rule_type: String,
    /// 条件主类：roi / price / position / profit_drawdown。
    #[arg(
        long = "condition-kind",
        alias = "condition_kind",
        alias = "condition",
        default_value = "roi"
    )]
    condition_kind: String,
    /// price_/position_ 条件的跨标的后缀（如 FILUSDT）；空=绑定仓位标的。
    #[arg(
        long = "condition-instrument",
        alias = "condition_instrument",
        default_value = ""
    )]
    condition_instrument: String,
    /// 条件操作符：>= <= > < ==。
    #[arg(long, default_value = "")]
    operator: String,
    /// 条件阈值（比率/价格/数量）。
    #[arg(long, default_value = "")]
    value: String,
    /// 命中动作：self；链式传 rule:<目标规则id>（或用 --target-rule-id）。
    #[arg(long, default_value = "self")]
    action: String,
    /// 链式动作目标规则 id（设置后 action 自动为 rule:<id>）。
    #[arg(long = "target-rule-id", alias = "target_rule_id")]
    target_rule_id: Option<u64>,
    /// risk 按比例减/平仓：1=全平、0.5=半仓；空=服务端按全平处理。
    #[arg(long = "quantity-pct", alias = "quantity_pct", default_value = "")]
    quantity_pct: String,
    /// 创建为 pending 规则（链式后继等待激活）。
    #[arg(long, default_value_t = false)]
    pending: bool,
    /// 立即平仓信号：反向下单且只减仓（配合 --side 与 --quantity；不再 set_leverage）。
    #[arg(long = "reduce-only", alias = "reduce_only", default_value_t = false)]
    reduce_only: bool,
    /// create_rule(risk) 绑定的本地仓位 id（来自 get_position 回填的 position_id）。
    #[arg(long = "position-id", alias = "position_id")]
    position_id: Option<u64>,
    #[arg(
        long = "south-addr",
        alias = "south_addr",
        default_value = "127.0.0.1:9100"
    )]
    south_addr: String,
    /// 南向共享密钥文件（与服务端 south.key 同一份；默认 ./data/south.key）。
    #[arg(
        long = "secret-file",
        alias = "secret_file",
        default_value = "./data/south.key"
    )]
    secret_file: PathBuf,
    /// 仅联调：跳过签名（服务端须以 --south-dev-insecure 启动）。
    #[arg(long = "insecure", default_value_t = false)]
    insecure: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // 握手鉴权参数：insecure 模式仅带 strategy_name（服务端需 --south-dev-insecure）；
    // 否则读取共享密钥，生成一次性 nonce 并计算 HMAC 签名。
    let mut url = format!(
        "ws://{}/ws?strategy_name={}",
        args.south_addr,
        urlencode(&args.strategy_name)
    );
    if !args.insecure {
        let secret = std::fs::read_to_string(&args.secret_file)
            .map(|s| s.trim().to_string())
            .map_err(|e| {
                anyhow::anyhow!(
                    "读取南向密钥 {:?} 失败：{e}（可先用服务端生成 data/south.key，或加 --insecure）",
                    args.secret_file
                )
            })?;
        let nonce = southauth::generate_nonce();
        let signature = southauth::sign(&secret, &args.strategy_name, &nonce);
        url.push_str(&format!("&nonce={nonce}&signature={signature}"));
    }
    println!("[strategy-client:{}] connecting {url}", args.strategy_name);

    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await?;
    println!("[strategy-client:{}] connected", args.strategy_name);

    let command = build_command(&args)?;
    let payload = serde_json::to_string(&command)?;
    println!("[strategy-client:{}] >> {payload}", args.strategy_name);
    ws.send(Message::Text(payload.into())).await?;

    while let Some(frame) = ws.next().await {
        match frame {
            Ok(Message::Text(text)) => {
                println!("[strategy-client:{}] << {text}", args.strategy_name)
            }
            Ok(Message::Binary(bytes)) => {
                if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                    println!(
                        "[strategy-client:{}] << (binary) {text}",
                        args.strategy_name
                    );
                }
            }
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) | Ok(Message::Frame(_)) => {}
            Ok(Message::Close(c)) => {
                println!(
                    "[strategy-client:{}] connection closed: {c:?}",
                    args.strategy_name
                );
                break;
            }
            Err(e) => {
                eprintln!("[strategy-client:{}] ws error: {e}", args.strategy_name);
                break;
            }
        }
    }
    Ok(())
}

fn build_command(args: &Args) -> anyhow::Result<SouthCommand> {
    let opt = args.option.trim().to_ascii_lowercase();
    // --target-rule-id 优先，组装链式动作 rule:<id>；否则用显式 --action（缺省 self）。
    let action = match args.target_rule_id {
        Some(id) => format!("rule:{id}"),
        None => args.action.clone(),
    };
    let base = SouthCommand {
        option: opt.clone(),
        strategy_name: Some(args.strategy_name.clone()),
        strategy: None,
        exchange: Some(args.exchange.clone()),
        account: Some(args.account.clone()),
        instrument: Some(args.instrument.clone()),
        symbol: None,
        side: Some(args.side.clone()),
        order_type: Some(args.order_type.clone()),
        price: None,
        quantity: None,
        client_order_id: args.client_order_id.clone(),
        instrument_type: Some(args.instrument_type.clone()),
        leverage: args.leverage,
        rule_type: Some(args.rule_type.clone()),
        condition_kind: Some(args.condition_kind.clone()),
        condition_instrument: non_empty(&args.condition_instrument),
        operator: non_empty(&args.operator),
        value: parse_opt_decimal(&args.value)?,
        action: Some(action),
        quantity_pct: parse_opt_decimal(&args.quantity_pct)?,
        pending: args.pending,
        reduce_only: args.reduce_only,
        position_id: args.position_id,
        signal_id: args.signal_id.clone(),
        order_cash: parse_opt_decimal(&args.order_cash)?,
        // get_positions 过滤项：本期 CLI 暂不下发，保持缺省（不筛选）。
        min_roi: None,
        max_roi: None,
        deleted: None,
    };
    match opt.as_str() {
        "place_order" | "place" => {
            // quantity 与 order_cash 二选一；都不给无法下单。
            if args.quantity.trim().is_empty() && args.order_cash.trim().is_empty() {
                anyhow::bail!("place_order 需要 --quantity 或 --order-cash（二选一）");
            }
            Ok(SouthCommand {
                price: parse_opt_decimal(&args.price)?,
                quantity: parse_opt_decimal(&args.quantity)?,
                ..base
            })
        }
        "cancel_order" | "cancel" => {
            if args.client_order_id.is_none() {
                anyhow::bail!("cancel_order 需要 --client-order-id");
            }
            Ok(base)
        }
        // WS 查仓位仅认 get_positions（旧别名 get_position/positions 不再支持）。
        "get_positions" => Ok(base),
        "create_rule" | "create_watch_rule" => {
            if args.operator.trim().is_empty() {
                anyhow::bail!("create_rule 需要 --operator（>= <= > < ==）");
            }
            if args.value.trim().is_empty() {
                anyhow::bail!("create_rule 需要 --value 阈值");
            }
            // risk 缺省全平；new 必须给开仓量。
            if args.rule_type == "new" && Decimal::from_str(&args.quantity)? <= Decimal::ZERO {
                anyhow::bail!("new 规则需要正数 --quantity");
            }
            Ok(SouthCommand {
                // risk 平仓量：比例口径给 0（服务端按 quantity_pct/全平），绝对量口径才填 quantity。
                quantity: match args.rule_type.as_str() {
                    "new" => Some(Decimal::from_str(&args.quantity)?),
                    _ => parse_opt_decimal(&args.quantity)?,
                },
                price: parse_opt_decimal(&args.price)?,
                ..base
            })
        }
        other => anyhow::bail!("未知 --option `{other}`"),
    }
}

/// 空串归一为 None（协议字段缺省语义）。
fn non_empty(s: &str) -> Option<String> {
    let s = s.trim();
    (!s.is_empty()).then(|| s.to_string())
}

/// 空串=缺省 None；非空必须是合法 Decimal。
fn parse_opt_decimal(s: &str) -> anyhow::Result<Option<Decimal>> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(None);
    }
    Ok(Some(Decimal::from_str(s)?))
}

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
