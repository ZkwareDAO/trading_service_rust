//! 控制面协议与纯分发：行分隔 JSON over loopback TCP。
//!
//! 协议为「一行一个 JSON 请求 / 一行一个 JSON 应答」。本模块只定义可序列化的
//! 命令/应答与纯函数（FillMode 解析、命令校验）；实例目录与 TCP 收发在
//! [`crate::registry`] / [`crate::server`]，不引 tokio（std::net + 短连接线程）。

use serde::{Deserialize, Serialize};

use crate::sim::script::FillScript;

/// 交易侧（td）控制命令（播种均价为 f64，仅 PartialEq）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "kebab-case")]
pub enum TdCommand {
    /// 设置成交策略：InstantFill | AcceptedOnly | Partial:1,1 | Reject:30
    SetFill { spec: String },
    /// 下一笔录入以给定 ErrorID 拒绝（一次性）。
    RejectNext { code: i32 },
    /// 撤单错误注入（24/25/26）；0/缺省取消注入。
    CancelError { code: Option<i32> },
    /// 播种/覆盖持仓。
    SeedPosition {
        instrument: String,
        side: String, // long | short
        total: i32,
        #[serde(default)]
        today: i32,
        #[serde(default)]
        yd: i32,
        #[serde(default)]
        avg: f64,
    },
    /// 读台账快照（委托/持仓）。
    Ledger,
    /// 断线（reason 形如 0x1001）。
    Disconnect { reason: i32 },
    /// 重连（重发 OnFrontConnected；客户端重新握手）。
    Reconnect,
    /// 健康检查。
    Ping,
}

/// 行情侧（md）控制命令（含 f64 报价，仅 PartialEq）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "kebab-case")]
pub enum MdCommand {
    /// 精确推一帧。
    Tick {
        instrument: String,
        last: f64,
        bid: f64,
        ask: f64,
    },
    /// 启动/重启自动游走。
    Auto {
        instrument: String,
        #[serde(default)]
        base: f64,
        #[serde(default = "default_interval")]
        interval_ms: u64,
    },
    /// 停止自动游走。
    AutoStop {
        instrument: String,
    },
    /// 造缺买侧。
    MissingBid {
        instrument: String,
        last: f64,
        ask: f64,
    },
    /// 造缺卖侧。
    MissingAsk {
        instrument: String,
        last: f64,
        bid: f64,
    },
    /// 行情登录拒绝码（64 等；0 取消）。
    LoginReject {
        code: i32,
    },
    Disconnect {
        reason: i32,
    },
    Reconnect,
    Ping,
}

fn default_interval() -> u64 {
    500
}

/// 统一应答。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// 台账/状态等只读结果（JSON 字符串透传，保持本类型 Eq）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
}

impl Response {
    pub fn ok() -> Self {
        Self {
            ok: true,
            error: None,
            data: None,
        }
    }
    pub fn data(json: impl Into<String>) -> Self {
        Self {
            ok: true,
            error: None,
            data: Some(json.into()),
        }
    }
    pub fn err(msg: impl Into<String>) -> Self {
        Self {
            ok: false,
            error: Some(msg.into()),
            data: None,
        }
    }
}

/// 解析成交策略文本为 [`FillScript`]。
///
/// 语法：`InstantFill`、`AcceptedOnly`、`Partial:1,1`、`Reject:30`。
pub fn parse_fill_spec(spec: &str) -> Result<FillScript, String> {
    let s = spec.trim();
    match s {
        "InstantFill" => Ok(FillScript::instant_fill()),
        "AcceptedOnly" => Ok(FillScript::accepted_only()),
        _ if s.starts_with("Partial:") => {
            let body = &s["Partial:".len()..];
            let steps: Result<Vec<i32>, String> = body
                .split(',')
                .map(|x| {
                    x.trim()
                        .parse::<i32>()
                        .map_err(|_| format!("非法分步量: {x}"))
                })
                .collect();
            let steps = steps?;
            if steps.is_empty() || steps.iter().any(|&n| n <= 0) {
                return Err("Partial 分步量必须为正数且非空".into());
            }
            Ok(FillScript::partial_then_fill(steps))
        }
        _ if s.starts_with("Reject:") => {
            let code = s["Reject:".len()..]
                .trim()
                .parse::<i32>()
                .map_err(|_| "Reject 需要错误码，如 Reject:30".to_string())?;
            Ok(FillScript::rejected(code))
        }
        other => Err(format!(
            "未知成交策略 `{other}`（InstantFill|AcceptedOnly|Partial:a,b|Reject:code）"
        )),
    }
}

/// 校验撤单错误码：仅允许 0(取消)/24/25/26。
pub fn validate_cancel_code(code: Option<i32>) -> Result<(), String> {
    match code {
        None | Some(0) | Some(24) | Some(25) | Some(26) => Ok(()),
        Some(c) => Err(format!("撤单错误码仅支持 24/25/26（收到 {c}）")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::script::FillMode;

    #[test]
    fn parses_builtin_fill_modes() {
        assert_eq!(
            parse_fill_spec("InstantFill").unwrap().mode,
            FillMode::InstantFill
        );
        assert_eq!(
            parse_fill_spec("AcceptedOnly").unwrap().mode,
            FillMode::AcceptedOnly
        );
    }

    #[test]
    fn parses_partial_steps() {
        let f = parse_fill_spec("Partial:1,2,3").unwrap();
        assert_eq!(f.mode, FillMode::PartialThenFill);
        assert_eq!(f.partial_steps, vec![1, 2, 3]);
    }

    #[test]
    fn parses_reject_code() {
        let f = parse_fill_spec("Reject:30").unwrap();
        assert_eq!(f.mode, FillMode::Rejected);
        assert_eq!(f.reject_code, 30);
    }

    #[test]
    fn rejects_bad_fill_spec() {
        assert!(parse_fill_spec("Nonsense").is_err());
        assert!(parse_fill_spec("Partial:").is_err());
        assert!(parse_fill_spec("Partial:0").is_err());
        assert!(parse_fill_spec("Reject:").is_err());
    }

    #[test]
    fn cancel_code_validation() {
        assert!(validate_cancel_code(None).is_ok());
        assert!(validate_cancel_code(Some(24)).is_ok());
        assert!(validate_cancel_code(Some(26)).is_ok());
        assert!(validate_cancel_code(Some(99)).is_err());
    }

    #[test]
    fn commands_round_trip_json() {
        let cmd = TdCommand::SetFill {
            spec: "Partial:1,1".into(),
        };
        let line = serde_json::to_string(&cmd).unwrap();
        assert!(line.contains("set-fill"));
        let back: TdCommand = serde_json::from_str(&line).unwrap();
        assert_eq!(back, cmd);

        let md = MdCommand::Tick {
            instrument: "rb2510".into(),
            last: 3210.0,
            bid: 3209.0,
            ask: 3211.0,
        };
        let line = serde_json::to_string(&md).unwrap();
        assert_eq!(serde_json::from_str::<MdCommand>(&line).unwrap(), md);
    }

    #[test]
    fn response_helpers_shapes() {
        assert!(Response::ok().ok);
        assert!(!Response::err("x").ok);
        assert_eq!(Response::err("x").error.unwrap(), "x");
    }
}

// ===== 实例目录 + 命令分发 + loopback TCP 服务器（std::net，不引 tokio）=====

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Mutex, OnceLock, Weak};

use crate::sim::front::MockTdFront;
use crate::sim::market::Quote;
use crate::sim::md_front::MockMdFront;

/// 默认实例 key（单交易进程仅 dlopen 一个 td / 一个 md，CLI 无需知道 flow）。
pub const DEFAULT_KEY: &str = "default";

/// 全局实例目录：同镜像内按 key 取前置（FFI 构造时注册）。
pub struct InstanceDir {
    // Weak：不延长前置生命周期；API Release 后自动失效（默认 key 指向最新实例）。
    td: Mutex<HashMap<String, Weak<MockTdFront>>>,
    md: Mutex<HashMap<String, Weak<MockMdFront>>>,
}
use std::sync::Arc;

static DIR: OnceLock<InstanceDir> = OnceLock::new();

fn dir() -> &'static InstanceDir {
    DIR.get_or_init(|| InstanceDir {
        td: Mutex::new(HashMap::new()),
        md: Mutex::new(HashMap::new()),
    })
}

/// 注册交易前置到默认 key 与 flow key。
pub fn register_td(flow_key: &str, front: &Arc<MockTdFront>) {
    let mut td = dir().td.lock().unwrap();
    // default 始终指向最新注册的交易前置（单服务进程只有一个 td 实例）。
    td.insert(DEFAULT_KEY.to_string(), Arc::downgrade(front));
    td.insert(flow_key.to_string(), Arc::downgrade(front));
}
/// 注册行情前置。
pub fn register_md(flow_key: &str, front: &Arc<MockMdFront>) {
    let mut md = dir().md.lock().unwrap();
    md.insert(DEFAULT_KEY.to_string(), Arc::downgrade(front));
    md.insert(flow_key.to_string(), Arc::downgrade(front));
}
/// 按 key 取交易前置（默认 [`DEFAULT_KEY`]）。
pub fn get_td(key: Option<&str>) -> Option<Arc<MockTdFront>> {
    dir()
        .td
        .lock()
        .unwrap()
        .get(key.unwrap_or(DEFAULT_KEY))
        .and_then(Weak::upgrade)
}
/// 按 key 取行情前置。
pub fn get_md(key: Option<&str>) -> Option<Arc<MockMdFront>> {
    dir()
        .md
        .lock()
        .unwrap()
        .get(key.unwrap_or(DEFAULT_KEY))
        .and_then(Weak::upgrade)
}

/// 把交易命令应用到前置（纯分发，便于 L1）。
pub fn apply_td(front: &MockTdFront, cmd: &TdCommand) -> Response {
    match cmd {
        TdCommand::Ping => Response::ok(),
        TdCommand::SetFill { spec } => match parse_fill_spec(spec) {
            Ok(script) => {
                front.set_fill_script(script);
                Response::ok()
            }
            Err(e) => Response::err(e),
        },
        TdCommand::RejectNext { code } => {
            front.reject_next(*code);
            Response::ok()
        }
        TdCommand::CancelError { code } => match validate_cancel_code(*code) {
            Ok(()) => {
                front.set_cancel_error(code.filter(|c| *c != 0));
                Response::ok()
            }
            Err(e) => Response::err(e),
        },
        TdCommand::SeedPosition {
            instrument,
            side,
            total,
            today,
            yd,
            avg,
        } => {
            let long = matches!(side.trim().to_ascii_lowercase().as_str(), "long" | "多");
            front.seed_position(instrument, long, *total, *today, *yd, *avg);
            Response::ok()
        }
        TdCommand::Ledger => Response::data(front.ledger_snapshot()),
        TdCommand::Disconnect { reason } => {
            front.disconnect(*reason);
            Response::ok()
        }
        TdCommand::Reconnect => {
            front.reconnect();
            Response::ok()
        }
    }
}

/// 把行情命令应用到前置（纯分发）。
pub fn apply_md(front: &Arc<MockMdFront>, cmd: &MdCommand) -> Response {
    match cmd {
        MdCommand::Ping => Response::ok(),
        MdCommand::Tick {
            instrument,
            last,
            bid,
            ask,
        } => {
            front.tick(instrument, "", Quote::exact(*last, *bid, *ask));
            Response::ok()
        }
        MdCommand::Auto {
            instrument,
            base,
            interval_ms,
        } => {
            front.start_auto(instrument, *base, *interval_ms);
            Response::ok()
        }
        MdCommand::AutoStop { instrument } => {
            front.stop_auto(instrument);
            Response::ok()
        }
        MdCommand::MissingBid {
            instrument,
            last,
            ask,
        } => {
            front.missing_bid(instrument, *last, *ask);
            Response::ok()
        }
        MdCommand::MissingAsk {
            instrument,
            last,
            bid,
        } => {
            front.missing_ask(instrument, *last, *bid);
            Response::ok()
        }
        MdCommand::LoginReject { code } => {
            front.set_login_reject(*code);
            Response::ok()
        }
        MdCommand::Disconnect { reason } => {
            front.disconnect(*reason);
            Response::ok()
        }
        MdCommand::Reconnect => {
            front.reconnect();
            Response::ok()
        }
    }
}

/// 启动交易侧控制服务器（仅绑 127.0.0.1；后台线程，每连接按行处理）。
pub fn serve_td(port: u16) -> std::io::Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", port))?;
    std::thread::Builder::new()
        .name(format!("ctp-mock-ctrl-td-{port}"))
        .spawn(move || {
            for stream in listener.incoming().flatten() {
                std::thread::spawn(|| {
                    if let Err(e) = handle_td_conn(stream) {
                        eprintln!("ctp-mock td ctrl connection error: {e}");
                    }
                });
            }
        })
        .expect("spawn td control server");
    Ok(())
}

/// 启动行情侧控制服务器。
pub fn serve_md(port: u16) -> std::io::Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", port))?;
    std::thread::Builder::new()
        .name(format!("ctp-mock-ctrl-md-{port}"))
        .spawn(move || {
            for stream in listener.incoming().flatten() {
                std::thread::spawn(|| {
                    if let Err(e) = handle_md_conn(stream) {
                        eprintln!("ctp-mock md ctrl connection error: {e}");
                    }
                });
            }
        })
        .expect("spawn md control server");
    Ok(())
}

fn reply(stream: &mut TcpStream, resp: &Response) -> std::io::Result<()> {
    let mut line = serde_json::to_string(resp).unwrap_or_else(|_| r#"{"ok":false}"#.into());
    line.push('\n');
    stream.write_all(line.as_bytes())
}

/// 单条控制命令最大字节数；超过即视为异常并关闭连接（仅绑 loopback 的本地加固）。
const MAX_CMD_LEN: usize = 64 * 1024;

/// 行分隔控制连接的通用处理：逐行读 JSON、由 `dispatch` 解析并执行、回写一行应答。
///
/// 对单行长度设上限，避免无换行的超长输入把 `read_line` 撑到 OOM。
fn handle_conn<C, D>(stream: TcpStream, mut dispatch: D) -> std::io::Result<()>
where
    C: serde::de::DeserializeOwned,
    D: FnMut(C) -> Response,
{
    let peer = stream.try_clone()?;
    let mut reader = BufReader::new(peer);
    let mut writer = stream;
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        if line.len() > MAX_CMD_LEN {
            // 异常长行：回错后立即关闭，不继续消费可能的巨量数据。
            reply(&mut writer, &Response::err("命令过长"))?;
            break;
        }
        let resp: Response = match serde_json::from_str::<C>(line.trim()) {
            Ok(cmd) => dispatch(cmd),
            Err(e) => Response::err(format!("命令解析失败: {e}")),
        };
        reply(&mut writer, &resp)?;
    }
    Ok(())
}

fn handle_td_conn(stream: TcpStream) -> std::io::Result<()> {
    handle_conn::<TdCommand, _>(stream, |cmd| match get_td(None) {
        Some(front) => apply_td(&front, &cmd),
        None => Response::err("交易前置尚未就绪"),
    })
}

fn handle_md_conn(stream: TcpStream) -> std::io::Result<()> {
    handle_conn::<MdCommand, _>(stream, |cmd| match get_md(None) {
        Some(front) => apply_md(&front, &cmd),
        None => Response::err("行情前置尚未就绪"),
    })
}

#[cfg(test)]
mod runtime_tests {
    use super::*;
    use crate::sim::front::FrontConfig;
    use crate::sim::md_front::MockMdFront;

    #[test]
    fn apply_td_set_fill_and_cancel_error_validates() {
        let front = MockTdFront::new(FrontConfig::default());
        assert!(
            apply_td(
                &front,
                &TdCommand::SetFill {
                    spec: "AcceptedOnly".into()
                }
            )
            .ok
        );
        // 非法撤单错误码被拒。
        let r = apply_td(&front, &TdCommand::CancelError { code: Some(99) });
        assert!(!r.ok);
        assert!(apply_td(&front, &TdCommand::CancelError { code: Some(25) }).ok);
        assert!(apply_td(&front, &TdCommand::RejectNext { code: 30 }).ok);
        assert!(apply_td(&front, &TdCommand::Ping).ok);
    }

    #[test]
    fn apply_td_seed_position_then_ledger_has_row() {
        let front = MockTdFront::new(FrontConfig::default());
        apply_td(
            &front,
            &TdCommand::SeedPosition {
                instrument: "rb2510".into(),
                side: "long".into(),
                total: 3,
                today: 2,
                yd: 1,
                avg: 3200.0,
            },
        );
        let r = apply_td(&front, &TdCommand::Ledger);
        let data = r.data.expect("ledger data");
        let v: serde_json::Value = serde_json::from_str(&data).unwrap();
        assert!(v["positions"].is_array());
        assert!(data.contains("rb2510"));
    }

    #[test]
    fn apply_md_login_reject_and_missing_side() {
        let front = Arc::new(MockMdFront::new("20261015"));
        assert!(apply_md(&front, &MdCommand::LoginReject { code: 64 }).ok);
        assert!(
            apply_md(
                &front,
                &MdCommand::MissingBid {
                    instrument: "rb2510".into(),
                    last: 3210.0,
                    ask: 3211.0
                }
            )
            .ok
        );
        assert!(apply_md(&front, &MdCommand::Ping).ok);
    }

    #[test]
    fn td_control_server_round_trip_over_loopback() {
        // 唯一端口，避免并行测试冲突。
        let port = 43_101;
        let f = MockTdFront::new(FrontConfig::default());
        register_td("test-flow", &f);
        serve_td(port).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));

        use std::net::TcpStream;
        let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.write_all(b"{\"cmd\":\"ping\"}\n").unwrap();
        let mut r = BufReader::new(s.try_clone().unwrap());
        let mut line = String::new();
        r.read_line(&mut line).unwrap();
        let resp: Response = serde_json::from_str(line.trim()).unwrap();
        assert!(resp.ok);

        // set-fill 非法值返回错误应答。
        s.write_all(b"{\"cmd\":\"set-fill\",\"spec\":\"Nope\"}\n")
            .unwrap();
        line.clear();
        r.read_line(&mut line).unwrap();
        let resp: Response = serde_json::from_str(line.trim()).unwrap();
        assert!(!resp.ok);
    }

    #[test]
    fn overlong_command_line_is_rejected_and_connection_closed() {
        let port = 43_107;
        let f = MockTdFront::new(FrontConfig::default());
        register_td("overflow-flow", &f);
        serve_td(port).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));

        use std::io::{Read, Write};
        use std::net::TcpStream;
        let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        // 无换行的超长行：超过 MAX_CMD_LEN，服务回错后关闭。
        s.write_all(&vec![b'a'; MAX_CMD_LEN + 10]).unwrap();
        s.write_all(b"\n").unwrap();
        let mut buf = String::new();
        // 读到 EOF/若干字节即可；应包含“命令过长”。
        let _ = s.read_to_string(&mut buf);
        assert!(buf.contains("命令过长"), "resp={buf}");
    }
}
