//! 南向连接握手鉴权（服务端，升级前完成全部校验）。
//!
//! * 单一共享密钥（所有策略同一个 `SECRET_KEY`），存于独立文件（默认
//!   `<data-dir>/south.key`，0600，首启自动生成）；新增策略无需逐名配置。
//! * 每次连接校验 `strategy_name + nonce + signature`：
//!   `signature = HMAC-SHA256(secret, strategy_name || nonce)`，见
//!   [`trading_core::southauth`]。
//! * nonce 一次性、带 TTL（默认 5 分钟）防重放；[`NonceGuard`] 用 DashMap
//!   原子去重，过期由后台 sweeper 周期清理。
//! * 默认强制鉴权；`dev_insecure` 显式打开后只认 strategy_name、不验签
//!   （仅限本地联调，启动会显著告警）。
//!
//! 本模块不使用 channel：任务间共享廉价克隆的 [`SouthAuth`] 句柄。

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use store_core::Clock;
use tokio::task::JoinHandle;
use trading_core::southauth;

/// nonce 最短长度（hex 字符）。客户端生成的是 32 hex（128-bit）。
pub const MIN_NONCE_LEN: usize = 32;
/// nonce 默认存活期：5 分钟。
pub const NONCE_TTL: Duration = Duration::from_secs(5 * 60);
/// sweeper 默认周期：60 秒。
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// 鉴权未通过；刻意不携带原因（统一错误，不泄露内部结构）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthRejected;

/// 已使用 nonce 的去重表：nonce -> 首次见到的时间（unix ms）。
#[derive(Clone)]
pub struct NonceGuard {
    seen: Arc<DashMap<String, u64>>,
    clock: Arc<dyn Clock>,
    ttl: Duration,
}

impl NonceGuard {
    pub fn new(clock: Arc<dyn Clock>, ttl: Duration) -> Self {
        NonceGuard {
            seen: Arc::new(DashMap::new()),
            clock,
            ttl,
        }
    }

    /// 原子地占用一个 nonce：未出现（或旧记录已过期）-> 记录并返回 true；
    /// 已在 TTL 内使用 -> 返回 false（重放）。DashMap 分片锁保证并发下原子。
    pub fn consume(&self, nonce: &str) -> bool {
        let now = self.clock.now_ms();
        let ttl_ms = self.ttl.as_millis() as u64;
        use dashmap::mapref::entry::Entry;
        match self.seen.entry(nonce.to_string()) {
            // 首次出现：占用。
            Entry::Vacant(v) => {
                v.insert(now);
                true
            }
            // 已存在：TTL 内为重放；过期则覆盖续用。
            Entry::Occupied(mut o) => {
                let seen_at = *o.get();
                if now.saturating_sub(seen_at) < ttl_ms {
                    false
                } else {
                    o.insert(now);
                    true
                }
            }
        }
    }

    /// 清理所有过期 nonce，返回移除条数。
    pub fn sweep(&self) -> usize {
        let now = self.clock.now_ms();
        let ttl_ms = self.ttl.as_millis() as u64;
        let mut removed = 0usize;
        self.seen.retain(|_, seen_at| {
            let fresh = now.saturating_sub(*seen_at) < ttl_ms;
            if !fresh {
                removed += 1;
            }
            fresh
        });
        removed
    }
}

/// 南向鉴权门面（廉价克隆，内部 Arc）。
#[derive(Clone)]
pub struct SouthAuth {
    secret: Arc<String>,
    nonces: NonceGuard,
    dev_insecure: bool,
}

impl SouthAuth {
    /// 用给定密钥与 nonce 守卫构造。
    pub fn from_secret(secret: impl Into<String>, nonces: NonceGuard, dev_insecure: bool) -> Self {
        SouthAuth {
            secret: Arc::new(secret.into()),
            nonces,
            dev_insecure,
        }
    }

    /// 升级前鉴权：成功返回绑定的策略身份；任一环节不符返回 [`AuthRejected`]。
    ///
    /// 顺序：身份/字段非空 -> 恒定时间验签 -> 占用 nonce。先验签再占 nonce，
    /// 避免垃圾请求耗尽/污染去重表；所有失败原因统一为 [`AuthRejected`]。
    pub fn authenticate(
        &self,
        strategy: Option<&str>,
        nonce: Option<&str>,
        signature: Option<&str>,
    ) -> Result<String, AuthRejected> {
        let strategy = strategy.map(str::trim).filter(|s| !s.is_empty());
        let strategy = strategy.ok_or(AuthRejected)?;

        // dev 旁路：只认身份，不验签、不占 nonce（仅供本地联调）。
        if self.dev_insecure {
            return Ok(strategy.to_string());
        }

        let nonce = nonce
            .map(str::trim)
            .filter(|n| n.len() >= MIN_NONCE_LEN)
            .ok_or(AuthRejected)?;
        let signature = signature.map(str::trim).filter(|s| !s.is_empty());
        let signature = signature.ok_or(AuthRejected)?;

        if !southauth::verify(&self.secret, strategy, nonce, signature) {
            return Err(AuthRejected);
        }
        // 签名正确但 nonce 在 TTL 内用过 -> 重放，拒绝。
        if !self.nonces.consume(nonce) {
            return Err(AuthRejected);
        }
        Ok(strategy.to_string())
    }
}

/// 密钥文件不存在则生成 32 字节随机密钥（64 hex，0600 写盘），已存在则校验后返回。
pub fn load_or_create_secret(path: &Path) -> anyhow::Result<String> {
    match std::fs::read_to_string(path) {
        Ok(raw) => validate_secret(raw.trim()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let secret = southauth::generate_secret(); // 64 hex = 32 字节
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)?;
                }
            }
            // 原子地以 0600 新建（无 0644 窗口、不跟随预放的符号链接/不覆盖）。
            store_core::fs::write_new_0600(path, secret.as_bytes())?;
            tracing::warn!(
                path = ?path,
                "已生成新的南向共享密钥文件（0600）；请把同一密钥离线分发给各策略客户端"
            );
            Ok(secret)
        }
        Err(e) => Err(e.into()),
    }
}

/// 校验密钥为 64 个十六进制字符（32 字节）。
fn validate_secret(secret: &str) -> anyhow::Result<String> {
    let ok = secret.len() == 64 && secret.chars().all(|c| c.is_ascii_hexdigit());
    if !ok {
        anyhow::bail!(
            "south.key 非法：期望 64 个十六进制字符，实际 {} 字符",
            secret.len()
        );
    }
    Ok(secret.to_string())
}

/// 启动后台 nonce 清扫任务；`stop` 置 true 后下一周期退出。
pub fn spawn_sweeper(
    guard: NonceGuard,
    interval: Duration,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> JoinHandle<()> {
    use std::sync::atomic::Ordering;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        while !stop.load(Ordering::Relaxed) {
            tick.tick().await;
            if stop.load(Ordering::Relaxed) {
                break;
            }
            let removed = guard.sweep();
            if removed > 0 {
                tracing::debug!(removed, "swept expired south nonces");
            }
        }
        tracing::info!("south nonce sweeper stopped");
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use store_core::MockClock;

    const SECRET: &str = "shared-secret";
    const STRATEGY: &str = "btc_strategy";

    fn auth_with_clock(clock: Arc<MockClock>, dev_insecure: bool) -> SouthAuth {
        let guard = NonceGuard::new(clock.clone(), NONCE_TTL);
        SouthAuth::from_secret(SECRET, guard, dev_insecure)
    }

    fn signed(clock: Arc<MockClock>) -> (SouthAuth, String, String, String) {
        let auth = auth_with_clock(clock.clone(), false);
        let nonce = southauth::generate_nonce();
        let sig = southauth::sign(SECRET, STRATEGY, &nonce);
        (auth, STRATEGY.to_string(), nonce, sig)
    }

    #[test]
    fn accepts_valid_signature_and_binds_strategy() {
        let clock = Arc::new(MockClock::new(1_000));
        let (auth, strategy, nonce, sig) = signed(clock);
        let bound = auth
            .authenticate(Some(&strategy), Some(&nonce), Some(&sig))
            .expect("valid handshake must pass");
        assert_eq!(bound, STRATEGY);
    }

    #[test]
    fn rejects_replayed_nonce() {
        let clock = Arc::new(MockClock::new(1_000));
        let (auth, strategy, nonce, sig) = signed(clock.clone());
        assert!(auth
            .authenticate(Some(&strategy), Some(&nonce), Some(&sig))
            .is_ok());
        // 同一 nonce 再放一枪：即便签名本身正确也必须拒绝。
        let err = auth.authenticate(Some(&strategy), Some(&nonce), Some(&sig));
        assert_eq!(err, Err(AuthRejected));
    }

    #[test]
    fn nonce_becomes_reusable_after_ttl() {
        let clock = Arc::new(MockClock::new(1_000));
        let (auth, strategy, nonce, sig) = signed(clock.clone());
        assert!(auth
            .authenticate(Some(&strategy), Some(&nonce), Some(&sig))
            .is_ok());
        // TTL 内仍是重放。
        clock.advance(NONCE_TTL.as_millis() as u64 - 1);
        assert!(auth
            .authenticate(Some(&strategy), Some(&nonce), Some(&sig))
            .is_err());
        // 超过 TTL，旧 nonce 可再次使用。
        clock.advance(2);
        assert!(auth
            .authenticate(Some(&strategy), Some(&nonce), Some(&sig))
            .is_ok());
    }

    #[test]
    fn rejects_missing_or_empty_strategy() {
        let clock = Arc::new(MockClock::new(1));
        let auth = auth_with_clock(clock, false);
        let nonce = southauth::generate_nonce();
        let sig = southauth::sign(SECRET, "", &nonce);
        assert_eq!(
            auth.authenticate(None, Some(&nonce), Some(&sig)),
            Err(AuthRejected)
        );
        assert_eq!(
            auth.authenticate(Some("  "), Some(&nonce), Some(&sig)),
            Err(AuthRejected)
        );
    }

    #[test]
    fn rejects_bad_signature() {
        let clock = Arc::new(MockClock::new(1));
        let auth = auth_with_clock(clock, false);
        let nonce = southauth::generate_nonce();
        let good = southauth::sign(SECRET, STRATEGY, &nonce);
        // 翻转末位 hex 字符（确保与原字符不同）。
        let last = if good.ends_with('0') { '1' } else { '0' };
        let tampered = format!("{}{}", &good[..good.len() - 1], last);
        assert_eq!(
            auth.authenticate(Some(STRATEGY), Some(&nonce), Some(&tampered)),
            Err(AuthRejected)
        );
    }

    #[test]
    fn rejects_missing_or_short_nonce_and_signature() {
        let clock = Arc::new(MockClock::new(1));
        let auth = auth_with_clock(clock, false);
        assert_eq!(
            auth.authenticate(Some(STRATEGY), None, Some("deadbeef")),
            Err(AuthRejected)
        );
        assert_eq!(
            auth.authenticate(Some(STRATEGY), Some("tooshort"), Some("deadbeef")),
            Err(AuthRejected)
        );
        assert_eq!(
            auth.authenticate(Some(STRATEGY), Some(&"a".repeat(32)), None),
            Err(AuthRejected)
        );
    }

    #[test]
    fn dev_insecure_skips_signature_but_keeps_identity() {
        let clock = Arc::new(MockClock::new(1));
        let auth = auth_with_clock(clock, true);
        // 不带 nonce/签名也放行，身份来自 strategy_name。
        assert_eq!(
            auth.authenticate(Some(STRATEGY), None, None).unwrap(),
            STRATEGY
        );
        // 但没有身份仍拒绝（无法路由回报）。
        assert_eq!(auth.authenticate(None, None, None), Err(AuthRejected));
        // insecure 模式不应污染 nonce 表。
        assert_eq!(auth.nonces.seen.len(), 0);
    }

    #[test]
    fn sweep_removes_only_expired_nonces() {
        let clock = Arc::new(MockClock::new(0));
        let guard = NonceGuard::new(clock.clone(), NONCE_TTL);
        assert!(guard.consume(&southauth::generate_nonce()));
        clock.advance(100);
        assert!(guard.consume(&southauth::generate_nonce()));
        assert_eq!(guard.seen.len(), 2);
        // 推进到 now=TTL：第一条（t=0）刚好过期，第二条（t=100）还差 100ms 才过期。
        clock.advance(NONCE_TTL.as_millis() as u64 - 100);
        let removed = guard.sweep();
        assert_eq!(removed, 1);
        assert_eq!(guard.seen.len(), 1);
    }

    #[test]
    fn secret_file_roundtrips_and_is_generated_when_absent() {
        let dir = std::env::temp_dir().join(format!(
            "south-key-{}-{}",
            std::process::id(),
            unique_suffix()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("south.key");

        // 不存在 -> 自动生成。
        let first = load_or_create_secret(&path).expect("generate secret");
        assert_eq!(first.len(), 64, "secret should be 64 hex chars");
        assert!(first.chars().all(|c| c.is_ascii_hexdigit()));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "key file must be 0600");
        }

        // 再次加载应得到同一密钥。
        let second = load_or_create_secret(&path).expect("load secret");
        assert_eq!(first, second);

        // 已存在的密钥文件允许带首尾空白，加载时 trim 后仍是同一密钥。
        std::fs::write(&path, format!("  {second} \n")).unwrap();
        assert_eq!(load_or_create_secret(&path).unwrap(), second);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn rejects_ill_sized_secret_file() {
        let dir = std::env::temp_dir().join(format!(
            "south-key-bad-{}-{}",
            std::process::id(),
            unique_suffix()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("south.key");
        std::fs::write(&path, "abc").unwrap(); // 非 64 hex
        assert!(load_or_create_secret(&path).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    // 测试间目录隔离用的进程内递增后缀（避免与时间/random 耦合）。
    fn unique_suffix() -> u64 {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        N.fetch_add(1, Ordering::Relaxed)
    }

    #[tokio::test]
    async fn sweeper_task_runs_until_stopped() {
        let clock = Arc::new(MockClock::new(0));
        let guard = NonceGuard::new(clock, NONCE_TTL);
        guard.consume(&southauth::generate_nonce());
        let stop = Arc::new(AtomicBool::new(false));
        let handle = spawn_sweeper(guard.clone(), Duration::from_millis(1), stop.clone());
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    }
}
