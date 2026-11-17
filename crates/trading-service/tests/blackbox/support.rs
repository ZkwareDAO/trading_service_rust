//! 黑盒 smoke 支撑：临时目录 + 两个真实二进制 + 端口/就绪轮询 + REST 客户端。
//!
//! 只做进程编排，不触碰任何业务装配：`trading-service` 与 `xili-exchange`
//! 都按生产 CLI 启动，测试只通过 HTTP 观察结果。

use std::fs::OpenOptions;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use serde_json::Value;
use tempfile::TempDir;
use tokio::sync::{Mutex, MutexGuard};

/// 与生产一致的 account.csv 表头（7 列）。
const ACCOUNT_HEADER: &str = "id,name,exchange,credentials,deleted,created_at,updated_at\n";
/// 进程就绪等待上限。
const READY_TIMEOUT: Duration = Duration::from_secs(15);

/// 极简 REST 客户端：断言 HTTP 成功并返回 JSON。
#[derive(Clone)]
pub struct RestClient {
    base: String,
    http: reqwest::Client,
}

impl RestClient {
    pub async fn get(&self, path: &str) -> reqwest::Response {
        self.http
            .get(format!("{}{path}", self.base))
            .send()
            .await
            .unwrap_or_else(|e| panic!("GET {path} 失败：{e}"))
    }

    pub async fn get_json(&self, path: &str) -> Value {
        let resp = self.get(path).await;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        assert!(status.is_success(), "GET {path} HTTP {status}：{text}");
        serde_json::from_str(&text)
            .unwrap_or_else(|e| panic!("GET {path} JSON 解析失败：{e}；body={text}"))
    }

    pub async fn post_json(&self, path: &str, body: Value) -> Value {
        let resp = self
            .http
            .post(format!("{}{path}", self.base))
            .json(&body)
            .send()
            .await
            .unwrap_or_else(|e| panic!("POST {path} 失败：{e}"));
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        assert!(status.is_success(), "POST {path} HTTP {status}：{text}");
        serde_json::from_str(&text)
            .unwrap_or_else(|e| panic!("POST {path} JSON 解析失败：{e}；body={text}"))
    }
}

/// 一套隔离的黑盒环境：临时目录 + xili-exchange + trading-service。
///
/// `_serial` 持有到整套栈销毁：每套栈占 2 个真实进程，串行化既避免端口
/// 分配竞态（bind 后释放→子进程再 bind 的 TOCTOU），也避免并发进程压垮本机。
pub struct TestStack {
    _xili: ChildGuard,
    service: Option<ChildGuard>,
    xili_port: u16,
    service_port: u16,
    temp: TempDir,
    http: reqwest::Client,
    /// 声明在最后：字段按声明序 drop，确保子进程先退出、再释放串行锁。
    _serial: MutexGuard<'static, ()>,
}

/// 进程内串行锁：同一时刻只允许一套黑盒栈存活。
///
/// 用 tokio 异步锁而非 std 锁：`start()` 在持有期间要 await 子进程就绪，
/// std 锁跨 await 会被 clippy `await_holding_lock` 拦下。
async fn serial_guard() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().await
}

impl TestStack {
    /// 启动模拟所与真实服务，并等到 `/health` 就绪。
    pub async fn start() -> Self {
        let serial = serial_guard().await;
        let temp = TempDir::new().expect("创建临时目录");
        let xili_port = free_port();

        let xili = ChildGuard::spawn(
            Command::new(env!("CARGO_BIN_EXE_xili-exchange")).args([
                "--addr",
                &format!("127.0.0.1:{xili_port}"),
                "--instance",
                "xili",
                "--price",
                "80000",
                "--order-fill-times",
                "1",
                "--order-fill-interval",
                "50ms",
            ]),
            temp.path().join("xili.log"),
        );

        let http = reqwest::Client::new();
        wait_http(
            &http,
            &format!("http://127.0.0.1:{xili_port}/positions?account=e2e"),
            &xili,
        )
        .await;

        write_account_file(temp.path());
        std::fs::create_dir_all(temp.path().join("data")).expect("创建 data 目录");

        let mut stack = Self {
            _serial: serial,
            _xili: xili,
            service: None,
            xili_port,
            service_port: 0,
            temp,
            http,
        };
        stack.start_service().await;
        stack
    }

    /// 指向当前服务实例的 REST 客户端。
    pub fn client(&self) -> RestClient {
        RestClient {
            base: format!("http://127.0.0.1:{}", self.service_port),
            http: self.http.clone(),
        }
    }

    /// 用 SIGINT 让服务走生产停机路径，并断言退出码为 0。
    pub async fn stop_service_graceful(&mut self) {
        let Some(mut service) = self.service.take() else {
            return;
        };
        service.sigint();
        let status = service.wait_exit(Duration::from_secs(8)).await;
        assert!(
            status.is_some_and(|s| s.success()),
            "SIGINT 后服务未优雅退出：{status:?}\n{}",
            service.log_tail()
        );
    }

    /// 以同一数据目录重新启动服务（端口重新分配）。
    pub async fn start_service(&mut self) {
        let port = free_port();
        let service = ChildGuard::spawn(
            Command::new(env!("CARGO_BIN_EXE_trading-service")).args(self.service_args(port)),
            self.temp.path().join("service.log"),
        );
        self.service_port = port;
        wait_http(
            &self.http,
            &format!("http://127.0.0.1:{port}/health"),
            &service,
        )
        .await;
        self.service = Some(service);
    }

    /// 生产 CLI 参数：临时目录 + xili 端点 + 缩短的周期，关闭外部通知。
    fn service_args(&self, port: u16) -> Vec<String> {
        let path = |name: &str| self.temp.path().join(name).to_string_lossy().into_owned();
        vec![
            "--south-addr".into(),
            format!("127.0.0.1:{port}"),
            "--data-dir".into(),
            path("data"),
            "--account-file".into(),
            path("account.csv"),
            "--account-key-file".into(),
            path("account.key"),
            "--south-secret-file".into(),
            path("south.key"),
            // 文件不存在时 venue 配置取默认值，避免读取仓库内生产配置。
            "--venue-config".into(),
            path("venue.toml"),
            "--exchange".into(),
            format!("xili=http://127.0.0.1:{}", self.xili_port),
            "--south-dev-insecure".into(),
            "--flush-interval".into(),
            "100ms".into(),
            "--rest-timeout".into(),
            "2s".into(),
            "--account-reload-interval".into(),
            "200ms".into(),
            "--position-debounce".into(),
            "50ms".into(),
            "--position-reconcile-interval".into(),
            "200ms".into(),
            "--rule-scan-interval".into(),
            "100ms".into(),
            "--kline-flush-interval".into(),
            "100ms".into(),
        ]
    }
}

/// 子进程句柄：Drop 时兜底强杀，日志落临时文件便于失败诊断。
struct ChildGuard {
    child: Child,
    log_path: PathBuf,
}

impl ChildGuard {
    fn spawn(cmd: &mut Command, log_path: PathBuf) -> Self {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .unwrap_or_else(|e| panic!("创建日志 {} 失败：{e}", log_path.display()));
        let stderr = file.try_clone().expect("复制日志句柄");
        cmd.stdout(Stdio::from(file)).stderr(Stdio::from(stderr));
        let child = cmd.spawn().unwrap_or_else(|e| panic!("启动进程失败：{e}"));
        Self { child, log_path }
    }

    /// 发送 SIGINT（生产 Ctrl-C 路径）。
    fn sigint(&mut self) {
        #[cfg(unix)]
        {
            let _ = Command::new("kill")
                .arg("-INT")
                .arg(self.child.id().to_string())
                .status();
        }
        #[cfg(not(unix))]
        {
            let _ = self.child.kill();
        }
    }

    async fn wait_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.child.try_wait() {
                Ok(Some(status)) => return Some(status),
                Ok(None) => {}
                Err(e) => panic!("等待子进程退出失败：{e}"),
            }
            if Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn log_tail(&self) -> String {
        let text = std::fs::read_to_string(&self.log_path).unwrap_or_default();
        let start = text
            .char_indices()
            .rev()
            .take(4_000)
            .last()
            .map(|(i, _)| i)
            .unwrap_or(0);
        text[start..].to_string()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// 取一个当前空闲的本地端口（bind 后立即释放，供子进程监听）。
fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定临时端口");
    listener.local_addr().expect("读取临时端口").port()
}

/// 写 7 列 account.csv（与生产同口径，凭证为合成值）。
fn write_account_file(dir: &Path) {
    let credentials = r#"{"api_key":"K","api_secret":"s"}"#;
    let escaped = credentials.replace('"', "\"\"");
    let body = format!("{ACCOUNT_HEADER}0,e2e,xili,\"{escaped}\",0,0,0\n");
    std::fs::write(dir.join("account.csv"), body).expect("写 account.csv");
}

/// 轮询 HTTP 就绪；超时打印子进程日志尾部。
async fn wait_http(http: &reqwest::Client, url: &str, guard: &ChildGuard) {
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        if let Ok(resp) = http.get(url).send().await {
            if resp.status().is_success() {
                return;
            }
        }
        if Instant::now() >= deadline {
            panic!("等待 {url} 就绪超时；进程日志：\n{}", guard.log_tail());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
