//! 统一 WebSocket 建连器（交易所无关基础设施）。
//!
//! `tokio_tungstenite::connect_async` 不读取 `HTTP(S)_PROXY`，而正式网
//! `wss://fstream.binance.com` 在部分网络下必须经代理；故公共行情与私有账户流
//! 统一走本模块建连：
//! * `ws://`（本地模拟所）直连；
//! * `wss://` 按 `HTTPS_PROXY/HTTP_PROXY + NO_PROXY` 决定是否经 HTTP `CONNECT`
//!   隧道，再在隧道（或直连 TCP）上用 rustls 完成 TLS，最后做明文 WS 握手；
//! * TCP/隧道/TLS/握手全程受 [`CONNECT_TIMEOUT`] 约束，避免建连无期限挂起。
//!
//! 返回类型刻意与 `connect_async` 相同（`WebSocketStream<MaybeTlsStream<TcpStream>>`），
//! 调用点只替换建连原语，重连/心跳/退避逻辑不变。
//!
//! 纯决策（代理选择、CONNECT 应答解析）抽成无 IO 小函数以便单测。

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use rustls::client::ClientConfig as TlsClientConfig;
use rustls::pki_types::ServerName;
use rustls::RootCertStore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{client_async, MaybeTlsStream, WebSocketStream};

/// 建连总超时：TCP 连接 + 代理 CONNECT + TLS 握手 + WS 握手。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// 统一建连返回类型：与 `connect_async` 一致。
pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// 解析出的 HTTP 代理。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proxy {
    pub host: String,
    pub port: u16,
    /// `user:pass` 原文（用于 Basic，发送前 base64）。
    pub basic_auth: Option<String>,
}

impl Proxy {
    /// 解析 `http://[user:pass@]host:port`。仅支持 HTTP 代理（CONNECT），
    /// socks 与缺端口明确报错。
    pub fn parse(raw: &str) -> Result<Self> {
        let rest = raw
            .trim()
            .strip_prefix("http://")
            .ok_or_else(|| anyhow!("仅支持 http 代理（CONNECT）：`{raw}`"))?;
        let (auth, hostport) = match rest.split_once('@') {
            Some((a, h)) => (Some(a.to_string()), h),
            None => (None, rest),
        };
        let (host, port) = hostport
            .rsplit_once(':')
            .ok_or_else(|| anyhow!("代理地址缺少端口：`{raw}`"))?;
        let port: u16 = port
            .parse()
            .with_context(|| format!("代理端口非法：`{port}`"))?;
        if host.is_empty() {
            return Err(anyhow!("代理主机为空：`{raw}`"));
        }
        Ok(Proxy {
            host: host.to_string(),
            port,
            basic_auth: auth,
        })
    }
}

/// 建连入口（三处调用点统一使用）：超时内完成（代理）TCP + TLS + WS 握手。
pub(crate) async fn connect(url: &str) -> Result<WsStream> {
    let parsed = parse_url(url)?;

    let env = ProxyEnv::from_env();
    let proxy = select_proxy(
        url,
        env.https.as_deref(),
        env.http.as_deref(),
        env.no_proxy.as_deref(),
    );

    let tcp = tokio::time::timeout(CONNECT_TIMEOUT, async {
        match parsed.scheme {
            Scheme::Wss => connect_tls(&parsed, proxy.as_ref()).await,
            Scheme::Ws => connect_plain(&parsed, proxy.as_ref()).await,
        }
    })
    .await
    .map_err(|_| anyhow!("WS 建连超时（{}s）：{url}", CONNECT_TIMEOUT.as_secs()))??;

    // 在已就绪的（TLS/明文）字节流上做标准 WS 握手（头由 tungstenite 生成）。
    let request = url.into_client_request()?;
    let (ws, _resp) = tokio::time::timeout(CONNECT_TIMEOUT, client_async(request, tcp))
        .await
        .map_err(|_| anyhow!("WS 握手超时：{url}"))?
        .context("WS 握手失败")?;
    Ok(ws)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scheme {
    Ws,
    Wss,
}

#[derive(Debug, Clone)]
struct ParsedUrl {
    scheme: Scheme,
    host: String,
    port: u16,
    /// TLS SNI / Host 用的主机名（不含端口）。
    server_name: String,
}

/// 极简 `ws(s)://host[:port][/path]` 解析（路径交给 tungstenite 请求）。
fn parse_url(url: &str) -> Result<ParsedUrl> {
    let (scheme, rest) = if let Some(r) = url.strip_prefix("wss://") {
        (Scheme::Wss, r)
    } else if let Some(r) = url.strip_prefix("ws://") {
        (Scheme::Ws, r)
    } else {
        return Err(anyhow!("WS URL 必须以 ws:// 或 wss:// 开头：`{url}`"));
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    // 区分 IPv6 字面量（`[::1]` 或 `[::1]:9200`）与普通 host:port。
    let (host, port) = if let Some(close) = authority.find(']') {
        let bracketed = &authority[1..close];
        let port = match authority[close + 1..].strip_prefix(':') {
            Some(p) => p.parse::<u16>().context("URL 端口非法")?,
            None => match scheme {
                Scheme::Wss => 443,
                Scheme::Ws => 80,
            },
        };
        (bracketed, port)
    } else {
        match authority.rsplit_once(':') {
            Some((h, p)) => (h, p.parse::<u16>().context("URL 端口非法")?),
            None => (
                authority,
                match scheme {
                    Scheme::Wss => 443,
                    Scheme::Ws => 80,
                },
            ),
        }
    };
    if host.is_empty() {
        return Err(anyhow!("WS URL 主机为空：`{url}`"));
    }
    Ok(ParsedUrl {
        scheme,
        host: host.to_string(),
        port,
        server_name: host.to_string(),
    })
}

/// 当前进程的代理环境变量（大小写都认，小写优先，与常见约定一致）。
struct ProxyEnv {
    https: Option<String>,
    http: Option<String>,
    no_proxy: Option<String>,
}

impl ProxyEnv {
    fn from_env() -> Self {
        let pick = |a: &str, b: &str| {
            std::env::var(a)
                .ok()
                .filter(|s| !s.trim().is_empty())
                .or_else(|| std::env::var(b).ok().filter(|s| !s.trim().is_empty()))
        };
        Self {
            https: pick("https_proxy", "HTTPS_PROXY"),
            http: pick("http_proxy", "HTTP_PROXY"),
            no_proxy: pick("no_proxy", "NO_PROXY"),
        }
    }
}

/// 纯决策：为目标 URL 选择代理。
/// - 本地地址（localhost / 环回 IP / ::1）**无条件直连**，与 NO_PROXY 是否配置无关；
/// - `wss` 用 https 代理，`ws` 用 http 代理；
/// - `NO_PROXY` 命中（`*`、主机后缀）则直连。
pub fn select_proxy(
    url: &str,
    https_proxy: Option<&str>,
    http_proxy: Option<&str>,
    no_proxy: Option<&str>,
) -> Option<Proxy> {
    let parsed = parse_url(url).ok()?;
    // 本地模拟所等环回目标永不走 HTTP 代理（即使 NO_PROXY 漏配）。
    if is_loopback(&parsed.host) {
        return None;
    }
    let candidate = match parsed.scheme {
        Scheme::Wss => https_proxy,
        Scheme::Ws => http_proxy,
    }?;
    if let Some(rules) = no_proxy {
        if rules
            .split([',', ' '])
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .any(|rule| no_proxy_matches(rule, &parsed.host))
        {
            return None;
        }
    }
    Proxy::parse(candidate).ok()
}

/// 环回/本地主机判定：localhost、127.0.0.0/8、IPv6 ::1 一律直连。
fn is_loopback(host: &str) -> bool {
    let h = host.to_ascii_lowercase();
    h == "localhost" || h == "::1" || h.starts_with("127.")
}

/// `*` 全绕过；`.x.com`/`x.com` 后缀匹配。
/// （本地地址的无条件直连由 [`is_loopback`] 在规则匹配之前处理。）
fn no_proxy_matches(rule: &str, host: &str) -> bool {
    if rule == "*" {
        return true;
    }
    let h = host.to_ascii_lowercase();
    let r = rule.trim_start_matches('.').to_ascii_lowercase();
    h == r || h.ends_with(&format!(".{r}"))
}

/// 明文 ws：有代理时对目标做 CONNECT（罕见，一般本地 ws 直连）；否则直连。
async fn connect_plain(
    parsed: &ParsedUrl,
    proxy: Option<&Proxy>,
) -> Result<MaybeTlsStream<TcpStream>> {
    let tcp = match proxy {
        Some(p) => connect_via_proxy(p, parsed).await?,
        None => TcpStream::connect((parsed.host.as_str(), parsed.port))
            .await
            .with_context(|| format!("TCP 直连失败 {}:{}", parsed.host, parsed.port))?,
    };
    Ok(MaybeTlsStream::Plain(tcp))
}

/// 加密 wss：在直连 TCP 或代理 CONNECT 隧道上跑 rustls。
async fn connect_tls(
    parsed: &ParsedUrl,
    proxy: Option<&Proxy>,
) -> Result<MaybeTlsStream<TcpStream>> {
    let tcp = match proxy {
        Some(p) => connect_via_proxy(p, parsed).await?,
        None => TcpStream::connect((parsed.host.as_str(), parsed.port))
            .await
            .with_context(|| format!("TCP 直连失败 {}:{}", parsed.host, parsed.port))?,
    };

    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = TlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
    let server_name = ServerName::try_from(parsed.server_name.clone())
        .map_err(|e| anyhow!("非法 TLS SNI `{}`: {e}", parsed.server_name))?;
    let tls = connector
        .connect(server_name, tcp)
        .await
        .context("rustls TLS 握手失败")?;
    Ok(MaybeTlsStream::Rustls(tls))
}

/// 经 HTTP 代理建立到目标 host:port 的 CONNECT 隧道。
async fn connect_via_proxy(proxy: &Proxy, target: &ParsedUrl) -> Result<TcpStream> {
    let mut stream = TcpStream::connect((proxy.host.as_str(), proxy.port))
        .await
        .with_context(|| format!("连接代理失败 {}:{}", proxy.host, proxy.port))?;

    let mut req = format!(
        "CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\nProxy-Connection: keep-alive\r\n",
        host = target.host,
        port = target.port
    );
    if let Some(auth) = &proxy.basic_auth {
        use base64_compat::b64_encode;
        req.push_str(&format!(
            "Proxy-Authorization: Basic {}\r\n",
            b64_encode(auth.as_bytes())
        ));
    }
    req.push_str("\r\n");
    stream
        .write_all(req.as_bytes())
        .await
        .context("发送 CONNECT")?;

    // 读到 \r\n\r\n 即应答头结束（隧道之后是透传字节，不能多读）。
    let mut buf = Vec::with_capacity(256);
    let mut chunk = [0u8; 128];
    loop {
        let n = stream.read(&mut chunk).await.context("读取 CONNECT 应答")?;
        if n == 0 {
            return Err(anyhow!("代理在 CONNECT 应答前关闭连接"));
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if buf.len() > 8192 {
            return Err(anyhow!("CONNECT 应答头过大"));
        }
    }
    if !is_connect_established(&buf) {
        return Err(anyhow!(
            "代理 CONNECT 被拒：{}",
            String::from_utf8_lossy(&buf).lines().next().unwrap_or("")
        ));
    }
    Ok(stream)
}

/// 仅当 CONNECT 状态行为 `HTTP/1.x 2xx` 时视为隧道就绪。
pub fn is_connect_established(resp: &[u8]) -> bool {
    let mut lines = resp.split(|&b| b == b'\r').filter_map(|l| {
        std::str::from_utf8(l)
            .ok()
            .map(|s| s.trim_start_matches('\n'))
    });
    matches!(lines.next(), Some(status) if status.starts_with("HTTP/") && status.contains(" 2"))
}

/// 避免为一个 Basic 头引入 base64 独立版本：局部最小标准 base64 实现。
mod base64_compat {
    const TBL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    pub fn b64_encode(input: &[u8]) -> String {
        let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
        for chunk in input.chunks(3) {
            let b0 = chunk[0] as u32;
            let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
            let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
            let triple = (b0 << 16) | (b1 << 8) | b2;
            out.push(TBL[((triple >> 18) & 63) as usize] as char);
            out.push(TBL[((triple >> 12) & 63) as usize] as char);
            if chunk.len() > 1 {
                out.push(TBL[((triple >> 6) & 63) as usize] as char);
            } else {
                out.push('=');
            }
            if chunk.len() > 2 {
                out.push(TBL[(triple & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proxy(p: &str) -> Proxy {
        Proxy::parse(p).unwrap()
    }

    #[test]
    fn select_proxy_prefers_https_env_for_wss() {
        let p = select_proxy(
            "wss://fstream.binance.com/ws/x",
            Some("http://h:8080"),
            None,
            None,
        )
        .expect("应选 HTTPS_PROXY");
        assert_eq!(p.host, "h");
        assert_eq!(p.port, 8080);
    }

    #[test]
    fn select_proxy_ws_uses_http_env() {
        // ws 目标不读 HTTPS_PROXY，只认 HTTP_PROXY。
        assert!(select_proxy("ws://127.0.0.1:9200/ws", Some("http://h:1"), None, None).is_none());
        let p = select_proxy("ws://example.com/ws", None, Some("http://g:3128"), None).unwrap();
        assert_eq!(p.host, "g");
        assert_eq!(p.port, 3128);
    }

    #[test]
    fn no_proxy_localhost_and_suffix_bypasses() {
        // 本地模拟所即便配置了代理也必须直连。
        assert!(select_proxy(
            "ws://127.0.0.1:9200/ws",
            None,
            Some("http://g:1"),
            Some("*")
        )
        .is_none());
        assert!(select_proxy(
            "wss://api.xili.local/ws",
            Some("http://g:1"),
            None,
            Some("xili.local")
        )
        .is_none());
        // 不命中后缀仍走代理。
        assert!(select_proxy(
            "wss://fstream.binance.com/ws",
            Some("http://g:1"),
            None,
            Some("example.com")
        )
        .is_some());
    }

    #[test]
    fn no_env_means_direct() {
        assert!(select_proxy("wss://fstream.binance.com/ws", None, None, None).is_none());
    }

    #[test]
    fn loopback_is_always_direct_even_with_proxy_and_no_noproxy() {
        // 即使配置了代理、且 NO_PROXY 完全没提环回，本地地址也必须直连。
        for url in [
            "ws://127.0.0.1:9200/ws",
            "ws://127.1.2.3:80/ws",
            "ws://localhost:9200/ws",
            "ws://[::1]:9200/ws",
        ] {
            assert!(
                select_proxy(url, Some("http://g:1"), Some("http://g:1"), None).is_none(),
                "{url} 应无条件直连"
            );
        }
        // 对照组：非环回外网在同样条件下仍走代理。
        assert!(select_proxy(
            "wss://fstream.binance.com/ws",
            Some("http://g:1"),
            None,
            None
        )
        .is_some());
    }

    #[test]
    fn proxy_parse_with_and_without_auth() {
        let p = proxy("http://user:pass@proxy.host:10809");
        assert_eq!(p.host, "proxy.host");
        assert_eq!(p.port, 10809);
        assert_eq!(p.basic_auth.as_deref(), Some("user:pass"));

        // 192.0.2.x 为 TEST-NET 保留段，仅作示例地址。
        let p = proxy("http://192.0.2.1:10809");
        assert_eq!(p.host, "192.0.2.1");
        assert_eq!(p.port, 10809);
        assert!(p.basic_auth.is_none());
    }

    #[test]
    fn proxy_parse_rejects_unsupported() {
        assert!(Proxy::parse("socks5://h:1080").is_err());
        assert!(Proxy::parse("not-a-url").is_err());
    }

    #[test]
    fn connect_response_accepts_2xx_only() {
        assert!(is_connect_established(
            b"HTTP/1.1 200 Connection established\r\n\r\n"
        ));
        assert!(is_connect_established(
            b"HTTP/1.1 200 Connection established\r\nProxy-Agent: x\r\n\r\n"
        ));
        assert!(!is_connect_established(
            b"HTTP/1.1 407 Proxy Auth Required\r\n\r\n"
        ));
        assert!(!is_connect_established(b"HTTP/1.1 502 Bad Gateway\r\n\r\n"));
        assert!(!is_connect_established(b""));
    }

    #[test]
    fn parse_url_default_ports_and_explicit() {
        let u = parse_url("wss://fstream.binance.com/ws/x").unwrap();
        assert_eq!(u.port, 443);
        assert_eq!(u.server_name, "fstream.binance.com");
        let u = parse_url("ws://127.0.0.1:9200/ws").unwrap();
        assert_eq!(u.scheme, Scheme::Ws);
        assert_eq!(u.port, 9200);
        assert!(parse_url("http://x").is_err());
    }

    #[test]
    fn base64_basic_header() {
        assert_eq!(base64_compat::b64_encode(b"user:pass"), "dXNlcjpwYXNz");
    }
}
