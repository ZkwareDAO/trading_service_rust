//! ctp-mock 控制面 CLI。
//!
//! 经行分隔 JSON over TCP（仅 127.0.0.1）驱动 mock 前置。端口取环境变量
//! `CTP_MOCK_CTRL_TD` / `CTP_MOCK_CTRL_MD`（与 mock .so 启动控制服务器一致），
//! 缺省 43011 / 43012。
//!
//! 示例见 `third_party/ctp-mock/README.md` 与 plan §8。

use std::env;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::ExitCode;

const DEFAULT_TD_PORT: u16 = 43011;
const DEFAULT_MD_PORT: u16 = 43012;

/// 端别：交易 / 行情。
#[derive(Clone, Copy)]
enum Side {
    Td,
    Md,
}

impl Side {
    fn default_port(self) -> u16 {
        match self {
            Side::Td => DEFAULT_TD_PORT,
            Side::Md => DEFAULT_MD_PORT,
        }
    }
    fn port_env(self) -> &'static str {
        match self {
            Side::Td => "CTP_MOCK_CTRL_TD",
            Side::Md => "CTP_MOCK_CTRL_MD",
        }
    }
    fn port(self) -> u16 {
        env::var(self.port_env())
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| self.default_port())
    }
}

fn usage() -> ! {
    eprintln!(
        "ctp-mock-ctl — CTP 模拟器控制面 CLI\n\n\
USAGE:\n  ctp-mock-ctl <td|md> <command> [args...]\n\n\
TD:\n  set-fill <InstantFill|AcceptedOnly|Partial:1,1|Reject:30>\n  reject-next <code>\n  \
cancel-error <0|24|25|26>\n  seed-position <inst> <long|short> <total> [--today N] [--yd N] [--avg P]\n  \
ledger\n  disconnect <reason, e.g. 0x1001>\n  reconnect\n  ping\n\n\
MD:\n  tick <inst> --last P --bid P --ask P\n  auto <inst> [--base P] [--interval-ms N]\n  \
auto-stop <inst>\n  missing-bid <inst> --last P --ask P\n  missing-ask <inst> --last P --bid P\n  \
login-reject <code>\n  disconnect <reason>\n  reconnect\n  ping"
    );
    std::process::exit(2);
}

/// 解析 `--key value` 形式的选项。
struct Opts<'a> {
    args: &'a [String],
}
impl<'a> Opts<'a> {
    fn get(&self, key: &str) -> Option<&'a str> {
        self.args
            .iter()
            .position(|a| a == key)
            .and_then(|i| self.args.get(i + 1))
            .map(|s| s.as_str())
    }
}

fn build_td(cmd: &str, rest: &[String]) -> Result<serde_json::Value, String> {
    let o = Opts { args: rest };
    let v = match cmd {
        "set-fill" => {
            let spec = rest.first().ok_or("set-fill 需要策略参数")?;
            serde_json::json!({"cmd":"set-fill","spec":spec})
        }
        "reject-next" => {
            let code: i32 = parse_code(rest.first().ok_or("reject-next 需要 code")?)?;
            serde_json::json!({"cmd":"reject-next","code":code})
        }
        "cancel-error" => {
            let raw = rest.first().ok_or("cancel-error 需要 code")?;
            let code: Option<i32> = if raw == "0" {
                None
            } else {
                Some(parse_code(raw)?)
            };
            serde_json::json!({"cmd":"cancel-error","code":code})
        }
        "seed-position" => {
            let inst = rest.first().ok_or("seed-position 需要合约")?;
            let side = rest.get(1).ok_or("seed-position 需要 long|short")?;
            let total: i32 = rest
                .get(2)
                .ok_or("seed-position 需要总量")?
                .parse()
                .map_err(|_| "总量必须是整数")?;
            serde_json::json!({
                "cmd":"seed-position","instrument":inst,"side":side,"total":total,
                "today": o.get("--today").and_then(|v| v.parse().ok()).unwrap_or(total),
                "yd": o.get("--yd").and_then(|v| v.parse().ok()).unwrap_or(0),
                "avg": o.get("--avg").and_then(|v| v.parse().ok()).unwrap_or(0f64),
            })
        }
        "ledger" => serde_json::json!({"cmd":"ledger"}),
        "disconnect" => {
            let code = parse_reason(rest.first().map(|s| s.as_str()).unwrap_or("0x1001"))?;
            serde_json::json!({"cmd":"disconnect","reason":code})
        }
        "reconnect" => serde_json::json!({"cmd":"reconnect"}),
        "ping" => serde_json::json!({"cmd":"ping"}),
        other => return Err(format!("未知 td 命令 `{other}`")),
    };
    Ok(v)
}

fn build_md(cmd: &str, rest: &[String]) -> Result<serde_json::Value, String> {
    let o = Opts { args: rest };
    let v = match cmd {
        "tick" => {
            let inst = rest.first().ok_or("tick 需要合约")?;
            serde_json::json!({
                "cmd":"tick","instrument":inst,
                "last": need_f64(&o, "--last")?,
                "bid": need_f64(&o, "--bid")?,
                "ask": need_f64(&o, "--ask")?,
            })
        }
        "auto" => {
            let inst = rest.first().ok_or("auto 需要合约")?;
            serde_json::json!({
                "cmd":"auto","instrument":inst,
                "base": o.get("--base").and_then(|v| v.parse().ok()).unwrap_or(3200f64),
                "interval_ms": o.get("--interval-ms").and_then(|v| v.parse().ok()).unwrap_or(500u64),
            })
        }
        "auto-stop" => {
            let inst = rest.first().ok_or("auto-stop 需要合约")?;
            serde_json::json!({"cmd":"auto-stop","instrument":inst})
        }
        "missing-bid" => {
            let inst = rest.first().ok_or("missing-bid 需要合约")?;
            serde_json::json!({
                "cmd":"missing-bid","instrument":inst,
                "last": need_f64(&o, "--last")?, "ask": need_f64(&o, "--ask")?,
            })
        }
        "missing-ask" => {
            let inst = rest.first().ok_or("missing-ask 需要合约")?;
            serde_json::json!({
                "cmd":"missing-ask","instrument":inst,
                "last": need_f64(&o, "--last")?, "bid": need_f64(&o, "--bid")?,
            })
        }
        "login-reject" => {
            let code: i32 = parse_code(rest.first().ok_or("login-reject 需要 code")?)?;
            serde_json::json!({"cmd":"login-reject","code":code})
        }
        "disconnect" => {
            let code = parse_reason(rest.first().map(|s| s.as_str()).unwrap_or("0x1001"))?;
            serde_json::json!({"cmd":"disconnect","reason":code})
        }
        "reconnect" => serde_json::json!({"cmd":"reconnect"}),
        "ping" => serde_json::json!({"cmd":"ping"}),
        other => return Err(format!("未知 md 命令 `{other}`")),
    };
    Ok(v)
}

fn need_f64(o: &Opts, key: &str) -> Result<f64, String> {
    o.get(key)
        .ok_or_else(|| format!("缺少参数 {key}"))?
        .parse()
        .map_err(|_| format!("{key} 必须是数字"))
}

fn parse_code(s: &str) -> Result<i32, String> {
    s.parse()
        .map_err(|_| format!("错误码必须是整数，收到 `{s}`"))
}

fn parse_reason(s: &str) -> Result<i32, String> {
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        i32::from_str_radix(hex, 16).map_err(|_| format!("非法十六进制原因 `{s}`"))
    } else {
        s.parse::<i32>().map_err(|_| format!("非法原因 `{s}`"))
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    let side = match args.first().map(|s| s.as_str()) {
        Some("td") => Side::Td,
        Some("md") => Side::Md,
        _ => usage(),
    };
    let cmd = match args.get(1) {
        Some(c) => c.as_str(),
        None => usage(),
    };
    let rest = &args[2..];

    let payload = match side {
        Side::Td => build_td(cmd, rest),
        Side::Md => build_md(cmd, rest),
    };
    let payload = match payload {
        Ok(v) => v,
        Err(e) => {
            eprintln!("参数错误: {e}");
            return ExitCode::from(2);
        }
    };

    let port = side.port();
    let mut line = serde_json::to_string(&payload).expect("serialize command");
    line.push('\n');

    match TcpStream::connect(("127.0.0.1", port)) {
        Ok(mut stream) => {
            if let Err(e) = stream.write_all(line.as_bytes()) {
                eprintln!("写入控制端口 {port} 失败: {e}");
                return ExitCode::FAILURE;
            }
            let mut reader = BufReader::new(stream);
            let mut resp = String::new();
            match reader.read_line(&mut resp) {
                Ok(0) => {
                    eprintln!("控制端关闭了连接（无应答）");
                    ExitCode::FAILURE
                }
                Ok(_) => {
                    let ok = resp.contains("\"ok\":true");
                    // 原样美化输出应答 JSON。
                    match serde_json::from_str::<serde_json::Value>(resp.trim()) {
                        Ok(v) => println!(
                            "{}",
                            serde_json::to_string_pretty(&v).unwrap_or_else(|_| resp.clone())
                        ),
                        Err(_) => print!("{resp}"),
                    }
                    if ok {
                        ExitCode::SUCCESS
                    } else {
                        ExitCode::FAILURE
                    }
                }
                Err(e) => {
                    eprintln!("读取应答失败: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        Err(e) => {
            eprintln!(
                "连接 127.0.0.1:{port} 失败: {e}（确认服务以 {env}=... 启动了 mock 控制面）",
                env = side.port_env()
            );
            ExitCode::FAILURE
        }
    }
}
