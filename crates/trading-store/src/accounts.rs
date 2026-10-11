//! 账户配置管理（trading-store 侧，不读环境变量）：
//!
//! * 账户来源是**人工可编辑的固定文件** `data/account.csv`（单向导入源）；
//! * 内存账户表复用 store-core 的 [`Table<Account>`]（内存为可信源，与订单/仓位同库、
//!   同一恢复与定时落盘）；
//! * 不做文件轮询热加载：重载由上层管理 API 显式触发，收到触发后重新读取并
//!   reconcile（新增/改密/禁用生效，无需重启）；
//! * 每次成功重载后递增重载体，上层账户事件监督器据此把版本变化的账户连接
//!   停旧起新（改密/切测试网即时生效）；
//! * 下单时按账户名现取解密后的凭证，不长期缓存 secret。
//!
//! 本模块是**纯账户域**：只依赖 store-core/trading-models，不认识交易所网络层与
//! 交易服务。对接外部端口（凭证解析 `CredentialResolver`、账户目标 `AccountTargets`、
//! 规则账户反查）的 trait 实现由服务层 `AccountHub` newtype 委托本管理器，
//! 以同时规避跨 crate 依赖与孤儿规则。不使用 channel。

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Context;
use store_core::Table;
use trading_models::{Account, SecretCipher};

/// 下单时取用的、已解密凭证（[`std::fmt::Debug`] 刻意不打印 secret）。
#[derive(Clone)]
pub struct ResolvedAccount {
    pub id: u64,
    pub name: String,
    pub exchange: String,
    pub api_key: String,
    pub api_secret: String,
    pub api_passphrase: Option<String>,
    /// 持仓模式：one_way（BOTH）/ hedge（LONG/SHORT）。
    pub position_mode: trading_models::PositionMode,
    /// 该账户连接测试网还是正式网（用户级路径：下单 REST + 账户事件流）。
    pub is_testnet: bool,
}

impl std::fmt::Debug for ResolvedAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedAccount")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("exchange", &self.exchange)
            .field("position_mode", &self.position_mode)
            .field("is_testnet", &self.is_testnet)
            .field("api_key", &mask(&self.api_key))
            .field("api_secret", &"<redacted>")
            .field(
                "api_passphrase",
                &self.api_passphrase.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// 不含密钥的账户只读简报（规则引擎账户反查、交易所去重、REST 账户投影等场景）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountBrief {
    pub name: String,
    pub exchange: String,
    /// 持仓模式：one_way / hedge（账户级属性）。
    pub position_mode: trading_models::PositionMode,
    pub is_testnet: bool,
}

/// 账户门面：持有框架内存表、加解密器、文件路径与重载代际，供 store/上层共享。
#[derive(Clone)]
pub struct AccountManager {
    table: Arc<Table<Account>>,
    cipher: SecretCipher,
    csv_path: PathBuf,
    /// 每次成功重载后 +1；作为全部账户目标的 version（触发监督器重连）。
    reload_epoch: Arc<AtomicU64>,
}

impl AccountManager {
    /// 绑定共享 Store 中的账户表，首启加载人工 account.csv；
    /// 文件不存在则为空表（告警，不报错）。
    ///
    /// 账户表由 `TradingStore::open*` 统一注册（与 order/position 同库、
    /// 同一恢复与定时落盘），本管理器不再自建 Store。
    pub fn bootstrap(
        table: Arc<Table<Account>>,
        data_dir: &str,
        csv_path: PathBuf,
        key_file: PathBuf,
    ) -> anyhow::Result<Self> {
        let cipher = load_or_create_cipher(&key_file)?;
        let manager = AccountManager {
            table,
            cipher,
            csv_path,
            reload_epoch: Arc::new(AtomicU64::new(0)),
        };
        let _ = std::fs::create_dir_all(data_dir);
        let _ = manager.load_from_csv(true)?;
        Ok(manager)
    }

    /// 当前重载体（version）。账户事件监督器据此识别变更。
    pub fn reload_version(&self) -> u64 {
        self.reload_epoch.load(Ordering::Relaxed)
    }

    /// 管理 API 触发：重读并 reconcile account.csv；成功后递增重载体，
    /// 使账户事件监督器在下一对账周期停旧起新。
    pub fn reload(&self) -> anyhow::Result<usize> {
        let n = self.load_from_csv(true)?;
        self.reload_epoch.fetch_add(1, Ordering::Relaxed);
        Ok(n)
    }

    /// 解析当前活动账户并解密凭证。`exchange` 给 Some 时同时做交易所**精确名**
    /// （大小写敏感）校验：字面不一致即视为不匹配。
    pub fn resolve(&self, name: &str, exchange: Option<&str>) -> Option<ResolvedAccount> {
        let acc = self.table.list().into_iter().find(|a| a.name == name)?;
        if let Some(ex) = exchange {
            if acc.exchange != ex {
                tracing::warn!(
                    account = %name,
                    account_exchange = %acc.exchange,
                    requested = %ex,
                    "账户交易所与下单目标不一致"
                );
                return None;
            }
        }
        let secret = self.cipher.decrypt(&acc.api_secret).ok()?;
        Some(ResolvedAccount {
            id: acc.id,
            name: acc.name,
            exchange: acc.exchange,
            api_key: acc.api_key,
            api_secret: secret,
            api_passphrase: acc.api_passphrase,
            position_mode: acc.position_mode,
            is_testnet: acc.is_testnet,
        })
    }

    /// 当前活动账户所属交易所的去重列表（按交易所名字典序排序，保证跨进程/
    /// HashMap 迭代顺序不确定时结果稳定）。
    ///
    /// 装配层据此做**交易所级**连接（公共行情/规则）：3 个账户里 2 个 binance、
    /// 1 个 okx，只返回 `["binance","okx"]`，不再无条件硬连某一家交易所。
    pub fn distinct_exchanges(&self) -> Vec<String> {
        self.table
            .list()
            .into_iter()
            .map(|a| a.exchange)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// 全部活动账户的不含密钥简报（顺序不保证，调用方按需自行排序）。
    /// 上层账户连接监督器据此 + [`Self::reload_version`] 组 (交易所,账户) 目标集。
    pub fn list_active_briefs(&self) -> Vec<AccountBrief> {
        self.table
            .list()
            .into_iter()
            .map(|a| AccountBrief {
                name: a.name,
                exchange: a.exchange,
                position_mode: a.position_mode,
                is_testnet: a.is_testnet,
            })
            .collect()
    }

    /// 按账户表主键取账户简报（不解密密钥），供规则引擎账户反查等只读场景。
    pub fn account_by_id(&self, id: u64) -> Option<AccountBrief> {
        self.table.get(id).ok().flatten().map(|a| AccountBrief {
            name: a.name,
            exchange: a.exchange,
            position_mode: a.position_mode,
            is_testnet: a.is_testnet,
        })
    }

    /// 按账户名取表主键（不解密、不做交易所二次过滤）。
    /// 账户名全局唯一，且归属在入站时已按交易所精确名校验，故此处只按名取 id。
    pub fn account_id_by_name(&self, account: &str) -> Option<u64> {
        self.table
            .list()
            .into_iter()
            .find(|a| a.name == account)
            .map(|a| a.id)
    }

    /// 读取并 reconcile account.csv；`quiet_absent=true` 时文件不存在只告警不报错。
    /// 解析失败保留旧表、返回错误（调用方据此跳过本轮，避免半更新）。
    pub fn load_from_csv(&self, quiet_absent: bool) -> anyhow::Result<usize> {
        let rows = match read_csv_rows(&self.csv_path) {
            Ok(rows) => rows,
            Err(e) if quiet_absent && is_not_found(&e) => {
                tracing::warn!(path = ?self.csv_path, "account.csv 不存在：暂无可用账户，可调用重载 API 触发热加载");
                return Ok(0);
            }
            Err(e) => return Err(e),
        };

        let mut desired: std::collections::BTreeMap<String, CsvRow> = Default::default();
        for row in rows {
            let name = row
                .get("name")
                .cloned()
                .unwrap_or_default()
                .trim()
                .to_string();
            if name.is_empty() {
                tracing::warn!("跳过缺少 name 的账户行");
                continue;
            }
            desired.insert(name, row);
        }

        // 文件中消失的活动账户 -> 软删除并停用连接。
        for acc in self.table.list_including_deleted() {
            if !desired.contains_key(&acc.name) && !acc.deleted {
                let _ = self.table.soft_delete(acc.id);
                tracing::warn!(account = %acc.name, "账户已从 account.csv 移除，软删除并停用其连接");
            }
        }

        let mut applied = 0usize;
        for (name, row) in desired {
            let exchange = row.get("exchange").cloned().unwrap_or_default();
            let api_key = row.get("api_key").cloned().unwrap_or_default();
            let raw_secret = row.get("api_secret").cloned().unwrap_or_default();
            let passphrase = row
                .get("api_passphrase")
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            let deleted = matches!(
                row.get("deleted").map(|s| s.as_str()).unwrap_or("0").trim(),
                "1" | "true" | "TRUE"
            );
            let is_testnet = parse_bool_column(
                row.get("is_testnet").map(|s| s.as_str()).unwrap_or(""),
                "is_testnet",
                &name,
            );

            // 明文 secret 在内存中归一化为密文形态（不回写人工文件）。
            let (stored_secret, _was_plain) = self.cipher.encrypt_if_plaintext(&raw_secret)?;

            let mut account = Account::new(&name, &exchange);
            account.id = stable_id(&name);
            account.api_key = api_key.trim().to_string();
            account.api_secret = stored_secret;
            account.api_passphrase = passphrase;
            account.position_mode =
                parse_position_mode(row.get("position_mode").map(|s| s.as_str()).unwrap_or(""));
            account.is_testnet = is_testnet;
            account.deleted = deleted;
            if let Ok(ts) = row
                .get("updated_at")
                .cloned()
                .unwrap_or_default()
                .trim()
                .parse::<u64>()
            {
                account.updated_at = ts;
            }
            self.table.upsert(account)?;
            applied += 1;
            tracing::info!(account = %name, exchange = %exchange, is_testnet, deleted, "account loaded");
        }
        Ok(applied)
    }
}

/// 加载账户主密钥；文件不存在则生成 32 字节随机密钥并以 0600 原子新建。
///
/// 加解密原语在 [`SecretCipher`]；此处只负责密钥文件的读取/首启生成与
/// 0600 落盘（与 south.key 共用 [`fsutil`]）。
fn load_or_create_cipher(key_file: &Path) -> anyhow::Result<SecretCipher> {
    if let Some(parent) = key_file.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create key dir {parent:?}"))?;
        }
    }
    let key_bytes = match std::fs::read(key_file) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let key = SecretCipher::random_key();
            // 原子地以 0600 新建（无 0644 窗口、不跟随预放的符号链接/不覆盖）。
            store_core::fs::write_new_0600(key_file, &key)
                .with_context(|| format!("write account key {key_file:?}"))?;
            tracing::warn!(path = ?key_file, "已生成新的账户主密钥文件（0600）；请妥善备份，丢失后历史密文将无法解密");
            key
        }
        Err(e) => return Err(e).with_context(|| format!("read account key {key_file:?}")),
    };
    SecretCipher::from_key_bytes(&key_bytes)
}

type CsvRow = std::collections::BTreeMap<String, String>;

fn read_csv_rows(path: &Path) -> anyhow::Result<Vec<CsvRow>> {
    let content =
        std::fs::read_to_string(path).with_context(|| format!("读取 account.csv 失败 {path:?}"))?;
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(content.as_bytes());
    let headers: Vec<String> = rdr
        .headers()?
        .iter()
        .map(|h| h.trim().to_string())
        .collect();
    let mut rows = Vec::new();
    for rec in rdr.records() {
        let rec = rec?;
        let mut map = CsvRow::new();
        for (i, h) in headers.iter().enumerate() {
            if let Some(v) = rec.get(i) {
                map.insert(h.clone(), v.trim().to_string());
            }
        }
        rows.push(map);
    }
    Ok(rows)
}

fn is_not_found(e: &anyhow::Error) -> bool {
    e.chain().any(|c| {
        c.downcast_ref::<std::io::Error>()
            .map(|io| io.kind() == std::io::ErrorKind::NotFound)
            .unwrap_or(false)
    })
}

/// 账户名 -> 稳定正数 id（同名跨重载一致，不依赖自增序列）。
fn stable_id(name: &str) -> u64 {
    let mut h = DefaultHasher::new();
    name.hash(&mut h);
    match h.finish() {
        0 | u64::MAX => 1,
        v => v,
    }
}

/// account.csv 的 position_mode 列解析；非法/缺省按 one_way。
fn parse_position_mode(raw: &str) -> trading_models::PositionMode {
    match raw.trim().to_ascii_lowercase().as_str() {
        "hedge" | "dual" | "true" | "1" => trading_models::PositionMode::Hedge,
        _ => trading_models::PositionMode::OneWay,
    }
}

/// 布尔列（is_testnet 等）解析：1/true 为真，其余（含缺列/空）为假。
fn parse_bool_column(raw: &str, column: &str, account: &str) -> bool {
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "0" | "false" => false,
        "1" | "true" => true,
        other => {
            tracing::warn!(
                account,
                column,
                value = other,
                "布尔列值非法，按 false 处理"
            );
            false
        }
    }
}

fn mask(s: &str) -> String {
    if s.len() <= 4 {
        "****".to_string()
    } else {
        format!("{}…***", &s[..4])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_core::{Store, SystemClock};

    /// 测试用：建一个独立 Store 并注册账户表（生产环境由 TradingStore 统一注册）。
    fn account_table() -> Arc<Table<Account>> {
        Store::new(Arc::new(SystemClock))
            .register_account::<Account>()
            .unwrap()
    }

    fn tmp_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "acct-mgr-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    const HEADER: &str =
        "id,name,exchange,api_key,api_secret,api_passphrase,position_mode,is_testnet,deleted,created_at,updated_at\n";

    #[test]
    fn position_mode_and_testnet_parse() {
        use trading_models::PositionMode;
        let dir = tmp_dir();
        let csv = dir.join("account.csv");
        let key = dir.join("account.key");
        // hedge + 测试网账户；默认（缺列）账户走正式网/单向。
        std::fs::write(
            &csv,
            format!("{HEADER}0,hedger,binance,K,s,,hedge,1,0,0,0\n0,plain,binance,K2,s2,,one_way,0,0,0,0\n"),
        )
        .unwrap();
        let mgr = AccountManager::bootstrap(
            account_table(),
            dir.to_str().unwrap(),
            csv.clone(),
            key.clone(),
        )
        .unwrap();
        let hedger = mgr.resolve("hedger", Some("binance")).unwrap();
        assert_eq!(hedger.position_mode, PositionMode::Hedge);
        assert!(hedger.is_testnet);
        let plain = mgr.resolve("plain", Some("binance")).unwrap();
        assert_eq!(plain.position_mode, PositionMode::OneWay);
        assert!(!plain.is_testnet);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn loads_resolves_and_reloads() {
        let dir = tmp_dir();
        let csv = dir.join("account.csv");
        let key = dir.join("account.key");
        std::fs::write(
            &csv,
            format!("{HEADER}0,alice,binance,KEY-A,secret-a,,,0,0,0,0\n"),
        )
        .unwrap();

        let mgr = AccountManager::bootstrap(
            account_table(),
            dir.to_str().unwrap(),
            csv.clone(),
            key.clone(),
        )
        .unwrap();

        let acc = mgr.resolve("alice", Some("binance")).expect("alice active");
        assert_eq!(acc.api_secret, "secret-a");
        assert!(mgr.resolve("alice", Some("deribit")).is_none());
        assert!(mgr.resolve("bob", None).is_none());

        // 重载：改密 + 新增 bob。
        std::fs::write(
            &csv,
            format!("{HEADER}0,alice,binance,KEY-A,secret-a2,,,0,0,0,0\n0,bob,binance,KEY-B,secret-b,,,0,0,0,0\n"),
        )
        .unwrap();
        mgr.reload().unwrap();
        assert_eq!(mgr.resolve("alice", None).unwrap().api_secret, "secret-a2");
        assert!(mgr.resolve("bob", None).is_some());

        // 文件中移除 alice -> 软删除。
        std::fs::write(
            &csv,
            format!("{HEADER}0,bob,binance,KEY-B,secret-b,,,0,0,0,0\n"),
        )
        .unwrap();
        mgr.reload().unwrap();
        assert!(mgr.resolve("alice", None).is_none());
        assert!(mgr.resolve("bob", None).is_some());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reload_bumps_target_version() {
        // Arrange：首载一个账户，记录其目标 version。
        let dir = tmp_dir();
        let csv = dir.join("account.csv");
        let key = dir.join("account.key");
        std::fs::write(
            &csv,
            format!("{HEADER}0,alice,binance,KEY-A,secret-a,,,0,0,0,0\n"),
        )
        .unwrap();
        let mgr = AccountManager::bootstrap(
            account_table(),
            dir.to_str().unwrap(),
            csv.clone(),
            key.clone(),
        )
        .unwrap();
        let v0 = mgr.reload_version();
        assert_eq!(mgr.distinct_exchanges(), vec!["binance".to_string()]);

        // Act：文件变化后经 API 口径重载。
        std::fs::write(
            &csv,
            format!("{HEADER}0,alice,binance,KEY-A,secret-a2,,,1,0,0,0\n"),
        )
        .unwrap();
        mgr.reload().unwrap();

        // Assert：version 单调递增，且 is_testnet 已切换。
        let v1 = mgr.reload_version();
        assert!(v1 > v0);
        assert!(mgr.resolve("alice", None).unwrap().is_testnet);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn distinct_exchanges_dedups_and_sorts_by_name() {
        // 3 个账户：2 binance、1 okx；HashMap 迭代序不定，结果必须按名稳定排序。
        let dir = tmp_dir();
        let csv = dir.join("account.csv");
        let key = dir.join("account.key");
        std::fs::write(
            &csv,
            format!(
                "{HEADER}\
                 0,a1,binance,K,s,,,0,0,0,0,0\n\
                 0,a2,okx,K,s,,,0,0,0,0,0\n\
                 0,a3,binance,K,s,,,0,0,0,0,0\n"
            ),
        )
        .unwrap();
        let mgr =
            AccountManager::bootstrap(account_table(), dir.to_str().unwrap(), csv, key).unwrap();
        // 去重且按账户表首次出现顺序保序：binance 先于 okx。
        assert_eq!(
            mgr.distinct_exchanges(),
            vec!["binance".to_string(), "okx".to_string()]
        );
        let brief = mgr
            .account_by_id(mgr.account_id_by_name("a2").unwrap())
            .unwrap();
        assert_eq!(brief.name, "a2");
        assert_eq!(brief.exchange, "okx");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn active_briefs_carry_position_mode() {
        // Arrange：一个 hedge 测试网账户、一个 one_way 正式网账户。
        let dir = tmp_dir();
        let csv = dir.join("account.csv");
        let key = dir.join("account.key");
        std::fs::write(
            &csv,
            format!(
                "{HEADER}\
                 0,hedger,binance,K,s,,hedge,1,0,0,0\n\
                 0,plain,binance,K2,s2,,one_way,0,0,0,0\n"
            ),
        )
        .unwrap();
        let mgr =
            AccountManager::bootstrap(account_table(), dir.to_str().unwrap(), csv, key).unwrap();

        // Act
        let mut briefs = mgr.list_active_briefs();
        briefs.sort_by(|a, b| a.name.cmp(&b.name));

        // Assert：简报携带持仓模式（REST 账户安全投影消费）。
        assert_eq!(briefs.len(), 2);
        assert_eq!(briefs[0].name, "hedger");
        assert_eq!(briefs[0].position_mode, trading_models::PositionMode::Hedge);
        assert!(briefs[0].is_testnet);
        assert_eq!(
            briefs[1].position_mode,
            trading_models::PositionMode::OneWay
        );
        assert!(!briefs[1].is_testnet);
        // account_by_id 口径一致。
        let by_id = mgr
            .account_by_id(mgr.account_id_by_name("hedger").unwrap())
            .unwrap();
        assert_eq!(by_id.position_mode, trading_models::PositionMode::Hedge);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn encrypted_secret_roundtrips_through_file() {
        let dir = tmp_dir();
        let csv = dir.join("account.csv");
        let key = dir.join("account.key");
        let mgr = AccountManager::bootstrap(
            account_table(),
            dir.to_str().unwrap(),
            csv.clone(),
            key.clone(),
        )
        .unwrap();
        let enc = mgr.cipher.encrypt("topsecret").unwrap();
        std::fs::write(&csv, format!("{HEADER}0,carol,binance,K,{enc},,,0,0,0,0\n")).unwrap();
        mgr.load_from_csv(false).unwrap();
        assert_eq!(mgr.resolve("carol", None).unwrap().api_secret, "topsecret");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn missing_file_is_empty_not_fatal() {
        let dir = tmp_dir();
        let mgr = AccountManager::bootstrap(
            account_table(),
            dir.to_str().unwrap(),
            dir.join("account.csv"),
            dir.join("account.key"),
        )
        .unwrap();
        assert_eq!(mgr.distinct_exchanges().len(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }
}
