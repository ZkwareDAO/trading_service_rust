//! L3 真实进程黑盒（默认 `#[ignore]`，用 `--ignored` 运行）。
//!
//! 拉起常驻进程 `headless_runner`——它与 trading-service 一样经**真实 ctp2rs**
//! dlopen 加载 td/md 两个替身 `.so`——再通过仅绑 127.0.0.1 的 TCP 控制面（进程边界、
//! 行分隔 JSON）驱动场景并断言，验证 ABI 替身 + 控制面 + 最小台账在独立进程内闭环。
#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn send(port: u16, json: &str) -> serde_json::Value {
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect ctrl");
    s.write_all(json.as_bytes()).unwrap();
    s.write_all(b"\n").unwrap();
    let mut r = BufReader::new(s);
    let mut line = String::new();
    r.read_line(&mut line).expect("resp line");
    serde_json::from_str(line.trim()).expect("resp json")
}

struct Runner {
    child: Child,
    td_port: u16,
    md_port: u16,
}

fn spawn_runner() -> Runner {
    // 选择不常冲突的高端口。
    let (td_port, md_port) = (43_211, 43_212);
    let base = manifest_dir().join("target/debug");
    let runner = base.join("examples/headless_runner");
    let td_so = base.join("libctpmock_td.so");
    let md_so = base.join("libctpmock_md.so");
    assert!(
        runner.exists(),
        "先构建: cargo build --example headless_runner"
    );

    let child = Command::new(runner)
        .env("TD_SO", td_so)
        .env("MD_SO", md_so)
        .env("CTP_MOCK_CTRL_TD", td_port.to_string())
        .env("CTP_MOCK_CTRL_MD", md_port.to_string())
        .env("CTP_MOCK_AUTO_TICK", "1")
        .env("CTP_MOCK_AUTO_INTERVAL_MS", "100")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("spawn runner");

    // 等控制服务器起来（轮询 td 端口的 ping）。
    let mut ready = false;
    for _ in 0..100 {
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", td_port)) {
            let _ = s.write_all(b"{\"cmd\":\"ping\"}\n");
            let mut buf = [0u8; 64];
            if s.read(&mut buf).unwrap_or(0) > 0 {
                ready = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(ready, "runner control server not ready");

    Runner {
        child,
        td_port,
        md_port,
    }
}

impl Drop for Runner {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
#[ignore = "L3 真实进程黑盒：需先 build 替身 .so 与 headless_runner"]
fn process_blackbox_control_plane_and_ledger() {
    let runner = spawn_runner();

    // td：ping。
    assert_eq!(send(runner.td_port, r#"{"cmd":"ping"}"#)["ok"], true);

    // 设置成交策略为仅受理；播种多仓 3（今 2 / 昨 1）。
    assert_eq!(
        send(
            runner.td_port,
            r#"{"cmd":"set-fill","spec":"AcceptedOnly"}"#
        )["ok"],
        true
    );
    assert_eq!(
        send(
            runner.td_port,
            r#"{"cmd":"seed-position","instrument":"rb2510","side":"long","total":3,"today":2,"yd":1,"avg":3200}"#
        )["ok"],
        true
    );

    // ledger：能查到 rb2510 的多今 2、多昨 1。
    let ledger = send(runner.td_port, r#"{"cmd":"ledger"}"#);
    assert_eq!(ledger["ok"], true);
    let data = ledger["data"].as_str().expect("data string");
    let v: serde_json::Value = serde_json::from_str(data).unwrap();
    let positions = v["positions"].as_array().unwrap();
    let long_today = positions
        .iter()
        .find(|p| p["posi_direction"] == "2" && p["position_date"] == "1")
        .expect("long today row");
    assert_eq!(long_today["position"], 2);
    let long_yd = positions
        .iter()
        .find(|p| p["posi_direction"] == "2" && p["position_date"] == "2")
        .expect("long yd row");
    assert_eq!(long_yd["position"], 1);

    // 断线 / 重连均受理（账本保留，前面的 seed 仍可查）。
    assert_eq!(
        send(runner.td_port, r#"{"cmd":"disconnect","reason":4097}"#)["ok"],
        true
    );
    assert_eq!(send(runner.td_port, r#"{"cmd":"reconnect"}"#)["ok"], true);
    let ledger2 = send(runner.td_port, r#"{"cmd":"ledger"}"#);
    assert!(ledger2["data"].as_str().unwrap().contains("rb2510"));

    // md：精确 tick / 缺侧 / 登录拒绝 / 自动行情均受理。
    assert_eq!(
        send(
            runner.md_port,
            r#"{"cmd":"tick","instrument":"rb2510","last":3210.0,"bid":3209.0,"ask":3211.0}"#
        )["ok"],
        true
    );
    assert_eq!(
        send(
            runner.md_port,
            r#"{"cmd":"missing-bid","instrument":"rb2510","last":3210.0,"ask":3211.0}"#
        )["ok"],
        true
    );
    assert_eq!(
        send(runner.md_port, r#"{"cmd":"login-reject","code":64}"#)["ok"],
        true
    );
    assert_eq!(
        send(
            runner.md_port,
            r#"{"cmd":"auto","instrument":"IF2510","base":4000.0,"interval_ms":100}"#
        )["ok"],
        true
    );
}
