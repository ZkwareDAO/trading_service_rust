//! 账户连接监督的**通用对账引擎**（与具体交易所/宿主服务无关）。
//!
//! 维护 `(exchange, account) -> 用户数据流任务` 的运行态表。后台周期把它与宿主喂入的
//! 期望集合（desired targets）做对账：新账户出现 -> 经 [`VenueConnector`] spawn 连接任务；
//! 账户被软删/移除 -> abort；换交易所 -> 停旧起新。新增账户后**无需重启**即可监听其订单流。
//!
//! 本引擎不含任何交易所知识或账户存储：期望集合与连接器都由宿主注入。

use dashmap::DashMap;
use tokio::task::{AbortHandle, JoinHandle};

/// 连接键：交易所 + 账户名。
pub type ConnKey = (String, String);

/// 期望保持在线的一个账户目标。
///
/// `version` 表达该目标内容的代际（宿主侧账户重载版本号）：键相同但 version
/// 变化时，监督器按「停旧连接、起新连接」处理，使改密/切换测试网即时生效。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnTarget {
    pub exchange: String,
    pub account: String,
    pub version: u64,
}

impl ConnTarget {
    pub fn key(&self) -> ConnKey {
        (self.exchange.clone(), self.account.clone())
    }
}

/// 某个 venue 的连接工厂：为指定账户启动一个长期运行的用户数据流任务。
pub trait VenueConnector: Send + Sync {
    fn spawn(&self, exchange: &str, account: &str) -> JoinHandle<()>;
}

/// 运行态一项：连接任务句柄 + 其对应的目标版本。
struct RunningConn {
    handle: AbortHandle,
    version: u64,
}

#[derive(Clone, Default)]
pub struct Supervisor {
    running: std::sync::Arc<DashMap<ConnKey, RunningConn>>,
}

impl Supervisor {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)] // 供观测/测试
    pub fn len(&self) -> usize {
        self.running.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.running.is_empty()
    }

    #[allow(dead_code)]
    pub fn is_running(&self, exchange: &str, account: &str) -> bool {
        self.running
            .contains_key(&(exchange.to_string(), account.to_string()))
    }

    /// 与期望账户目标集对账：补齐缺失连接、停掉多余连接、版本变化的键停旧起新。幂等。
    pub fn reconcile<C: VenueConnector>(&self, desired: &[ConnTarget], connector: &C) {
        let desired_map: std::collections::HashMap<ConnKey, u64> =
            desired.iter().map(|t| (t.key(), t.version)).collect();

        // 停掉不再需要、或版本已变化的连接（后者随即将在下方重拉）。
        let to_stop: Vec<(ConnKey, AbortHandle)> = self
            .running
            .iter()
            .filter_map(|kv| {
                let key = kv.key().clone();
                let need_restart = desired_map
                    .get(&key)
                    .map(|v| *v != kv.value().version)
                    .unwrap_or(true);
                need_restart.then(|| {
                    // 先摘表再 abort，避免迭代中移除。
                    (key, kv.value().handle.clone())
                })
            })
            .collect();
        for (key, handle) in to_stop {
            if self.running.remove(&key).is_some() {
                handle.abort();
                tracing::info!(exchange = %key.0, account = %key.1, "账户移除/变更，停止其用户数据流连接");
            }
        }

        // 拉起新出现（或刚停掉）的连接。
        for t in desired {
            let key = t.key();
            if self.running.contains_key(&key) {
                continue; // 同版本仍在运行：幂等，不重连。
            }
            let handle = connector.spawn(&t.exchange, &t.account).abort_handle();
            self.running.insert(
                key,
                RunningConn {
                    handle,
                    version: t.version,
                },
            );
            tracing::info!(exchange = %t.exchange, account = %t.account, version = t.version, "检测到新账户/账户变更，已启动其用户数据流连接");
        }
    }

    /// 停止全部连接（优雅关闭时调用）。
    pub fn stop_all(&self) {
        let keys: Vec<ConnKey> = self.running.iter().map(|kv| kv.key().clone()).collect();
        for key in keys {
            if let Some((_, c)) = self.running.remove(&key) {
                c.handle.abort();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试用连接器：立即完成的占位任务（不联网）。
    struct NoopConnector;
    impl VenueConnector for NoopConnector {
        fn spawn(&self, _exchange: &str, _account: &str) -> JoinHandle<()> {
            tokio::spawn(async {})
        }
    }

    fn target(exchange: &str, account: &str, version: u64) -> ConnTarget {
        ConnTarget {
            exchange: exchange.to_string(),
            account: account.to_string(),
            version,
        }
    }

    #[tokio::test]
    async fn reconcile_starts_stops_and_replaces() {
        let sup = Supervisor::new();
        let conn = NoopConnector;

        let alice = target("binance", "alice", 0);
        let bob = target("binance", "bob", 0);

        sup.reconcile(&[alice.clone(), bob.clone()], &conn);
        assert_eq!(sup.len(), 2);
        assert!(sup.is_running("binance", "alice"));
        assert!(sup.is_running("binance", "bob"));

        // 相同集合再对账应幂等。
        sup.reconcile(&[alice.clone(), bob.clone()], &conn);
        assert_eq!(sup.len(), 2);

        // 移除 bob -> 只停 bob。
        sup.reconcile(std::slice::from_ref(&alice), &conn);
        assert_eq!(sup.len(), 1);
        assert!(sup.is_running("binance", "alice"));
        assert!(!sup.is_running("binance", "bob"));

        // alice 换到 deribit -> 旧键停、新键起。
        let alice_d = target("deribit", "alice", 0);
        sup.reconcile(std::slice::from_ref(&alice_d), &conn);
        assert!(!sup.is_running("binance", "alice"));
        assert!(sup.is_running("deribit", "alice"));

        sup.stop_all();
        assert!(sup.is_empty());
    }

    /// 计数连接器：记录每次 spawn 的目标，用于验证 version 变化触发重连。
    #[derive(Default)]
    struct CountingConnector {
        spawned: std::sync::Mutex<Vec<(String, String)>>,
    }
    impl VenueConnector for CountingConnector {
        fn spawn(&self, exchange: &str, account: &str) -> JoinHandle<()> {
            self.spawned
                .lock()
                .unwrap()
                .push((exchange.to_string(), account.to_string()));
            tokio::spawn(async {})
        }
    }

    #[tokio::test]
    async fn same_key_with_new_version_restarts_connection() {
        // Arrange：alice 以 version=1 上线。
        let sup = Supervisor::new();
        let conn = CountingConnector::default();
        let v1 = vec![ConnTarget {
            exchange: "binance".into(),
            account: "alice".into(),
            version: 1,
        }];
        sup.reconcile(&v1, &conn);
        assert_eq!(conn.spawned.lock().unwrap().len(), 1);

        // Act：相同 (exchange, account)，version 升到 2（改密/切网后重载）。
        let v2 = vec![ConnTarget {
            exchange: "binance".into(),
            account: "alice".into(),
            version: 2,
        }];
        sup.reconcile(&v2, &conn);

        // Assert：旧连接被停、新连接被起（spawn 再发生一次），键仍唯一在线。
        assert_eq!(conn.spawned.lock().unwrap().len(), 2);
        assert_eq!(sup.len(), 1);
        assert!(sup.is_running("binance", "alice"));

        // 相同 version 再次对账必须幂等（不重连）。
        sup.reconcile(&v2, &conn);
        assert_eq!(conn.spawned.lock().unwrap().len(), 2);
    }
}
