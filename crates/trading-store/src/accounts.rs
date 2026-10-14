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
//! 本模块是**纯账户域**：只依赖 store-core/trading-models/trading-exchange 端口，
//! 不认识交易所网络层与交易服务。对接外部端口（凭证解析 `CredentialResolver`、
//! 账户目标 `AccountTargets`、规则账户反查）的 trait 实现由服务层 `AccountHub`
//! newtype 委托本管理器，以同时规避跨 crate 依赖与孤儿规则。不使用 channel。
//!
//! **新 7 列格式是唯一读取口径**（迁移方案 B；Phase 2 的旧宽表兼容读路径已随
//! Phase 4 删除，不留双轨）：旧宽表文件拒载并提示 `account-tool migrate`，
//! 旧→新映射逻辑归 account-tool 的 `migrate` 子命令所有。

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Context;
use store_core::Table;
use trading_exchange::port::{credential_from_json, ExchangeCredential};
use trading_models::{Account, SecretCipher};

/// 下单/建连时取用的、已解密凭证（[`std::fmt::Debug`] 脱敏由
/// [`ExchangeCredential`] 自带实现，不打印任何密钥字段）。
#[derive(Clone)]
pub struct ResolvedAccount {
    pub id: u64,
    pub name: String,
    pub exchange: String,
    /// 解密后的强类型凭证：REST 签名所 [`ExchangeCredential::ApiSecret`]，
    /// CTP 期货 [`ExchangeCredential::Ctp`]（按 exchange 路由 schema）。
    pub credential: ExchangeCredential,
}

impl ResolvedAccount {
    /// 账户级持仓模式：REST 所取自凭证；CTP 无该概念按单向（one_way）。
    pub fn position_mode(&self) -> trading_models::PositionMode {
        match &self.credential {
            ExchangeCredential::ApiSecret { position_mode, .. } => *position_mode,
            ExchangeCredential::Ctp { .. } => trading_models::PositionMode::OneWay,
        }
    }

    /// 账户级选网：REST 所取自凭证；CTP 无测试网概念，恒按正式网。
    pub fn is_testnet(&self) -> bool {
        match &self.credential {
            ExchangeCredential::ApiSecret { is_testnet, .. } => *is_testnet,
            ExchangeCredential::Ctp { .. } => false,
        }
    }
}

/// 不含密钥的账户只读简报（规则引擎账户反查、交易所去重、REST 账户投影等场景）。
///
/// `position_mode` / `is_testnet` 是账户级属性，迁移后存于 credentials JSON，
/// 构建简报时现取现解密提取（只读投影，失败按缺省告警，不阻断）。
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
    ///
    /// credentials JSON 解密/反序列化失败返回 None 并告警——拒载不静默。
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
        let plain = match acc.decode_credentials(&self.cipher) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(account = %name, "账户凭证解密失败：{e:#}");
                return None;
            }
        };
        let credential = match credential_from_json(&acc.exchange, &plain) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    account = %name,
                    exchange = %acc.exchange,
                    "credentials JSON 解析失败，拒载该账户：{e:#}"
                );
                return None;
            }
        };
        Some(ResolvedAccount {
            id: acc.id,
            name: acc.name,
            exchange: acc.exchange,
            credential,
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
        self.table.list().iter().map(|a| self.brief_of(a)).collect()
    }

    /// 按账户表主键取账户简报（不解密密钥字段本身，但需解出 credentials JSON
    /// 提取账户级属性），供规则引擎账户反查等只读场景。
    pub fn account_by_id(&self, id: u64) -> Option<AccountBrief> {
        self.table.get(id).ok().flatten().map(|a| self.brief_of(&a))
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

        // 新 7 列格式是唯一支持的读取口径（兼容读路径已随迁移方案 B Phase 4
        // 删除，不留双轨）：旧宽表（含 api_key 列）或表头缺 credentials 列 →
        // 在任何表变更（软删/写入）前整体拒载，并给出可操作的迁移提示。
        if let Some(row) = desired.values().next() {
            if row.contains_key("api_key") || !row.contains_key("credentials") {
                return Err(anyhow::anyhow!(
                    "account.csv 为旧宽表格式（含 api_key 列）或表头非法：请先运行 \
                     `account-tool migrate --csv <account.csv> --key <account.key>` \
                     迁移到新 7 列格式（原文件自动备份 .bak）"
                ));
            }
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
            let credentials_plain = row.get("credentials").cloned().unwrap_or_default();
            // 明文 credentials JSON 归一化为密文形态（不回写人工文件）。
            let (stored, _was_plain) = self.cipher.encrypt_if_plaintext(&credentials_plain)?;

            let deleted = matches!(
                row.get("deleted").map(|s| s.as_str()).unwrap_or("0").trim(),
                "1" | "true" | "TRUE"
            );
            let mut account = Account::new(&name, &exchange);
            account.id = stable_id(&name);
            account.credentials = stored;
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
            tracing::info!(account = %name, exchange = %exchange, deleted, "account loaded");
        }
        Ok(applied)
    }

    /// 账户只读简报：账户级属性（position_mode/is_testnet）从 credentials JSON
    /// 现取现解密提取。空串/解析失败按缺省（正式网 + 单向）并告警——简报是
    /// 只读投影，不阻断调用方。
    fn brief_of(&self, acc: &Account) -> AccountBrief {
        const DEFAULT: (trading_models::PositionMode, bool) =
            (trading_models::PositionMode::OneWay, false);
        let attrs = self.brief_attributes(acc, DEFAULT);
        AccountBrief {
            name: acc.name.clone(),
            exchange: acc.exchange.clone(),
            position_mode: attrs.0,
            is_testnet: attrs.1,
        }
    }

    /// 从 credentials JSON 解出 (position_mode, is_testnet)；任一环节失败
    /// 告警后按 default 归一（CTP 凭证天然无该属性，亦走缺省）。
    fn brief_attributes(
        &self,
        acc: &Account,
        default: (trading_models::PositionMode, bool),
    ) -> (trading_models::PositionMode, bool) {
        let plain = match acc.decode_credentials(&self.cipher) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    account = %acc.name,
                    "简报解密 credentials 失败，按缺省属性处理：{e:#}"
                );
                return default;
            }
        };
        if plain.is_empty() {
            // 空 credentials：未配置凭证的账户行。
            return default;
        }
        match credential_from_json(&acc.exchange, &plain) {
            Ok(ExchangeCredential::ApiSecret {
                position_mode,
                is_testnet,
                ..
            }) => (position_mode, is_testnet),
            Ok(ExchangeCredential::Ctp { .. }) => default,
            Err(e) => {
                tracing::warn!(
                    account = %acc.name,
                    exchange = %acc.exchange,
                    "简报提取 credentials JSON 失败，按缺省属性处理：{e:#}"
                );
                default
            }
        }
    }
}

/// 加载账户主密钥；文件不存在则生成 32 字节随机密钥并以 0600 原子新建。
///
/// 加解密原语在 [`SecretCipher`]；此处只负责密钥文件的读取/首启生成与
/// 0600 落盘（与 south.key 共用 fsutil）。
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

#[cfg(test)]
mod tests {
    use super::*;
    use store_core::{Store, SystemClock};
    use trading_exchange::port::ExchangeCredential;
    use trading_models::PositionMode;

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

    /// 新 7 列表头（恒定，见 trading_models::ACCOUNT_CSV_HEADER）。
    const NEW_HEADER: &str = trading_models::ACCOUNT_CSV_HEADER;
    /// 旧宽表表头（REST 签名所变体；仅用于钉「拒载 + 迁移提示」语义）。
    const LEGACY_HEADER: &str = "id,name,exchange,api_key,api_secret,api_passphrase,position_mode,is_testnet,deleted,created_at,updated_at";

    fn bootstrap(dir: &Path) -> AccountManager {
        AccountManager::bootstrap(
            account_table(),
            dir.to_str().unwrap(),
            dir.join("account.csv"),
            dir.join("account.key"),
        )
        .unwrap()
    }

    fn write_csv(dir: &Path, text: &str) {
        std::fs::write(dir.join("account.csv"), text).unwrap();
    }

    /// JSON 单元格的 CSV 转义：引号包裹且内嵌双引号双写。
    fn csv_cell(json: &str) -> String {
        format!("\"{}\"", json.replace('"', "\"\""))
    }

    // ---- 新 7 列格式读取 ----

    #[test]
    fn new_format_row_resolves_typed_credential() {
        // Arrange：新 7 列行，credentials 为整串密文 JSON。
        // 先 bootstrap 生成主密钥，再用管理器 cipher 加密（保证密钥一致）。
        let dir = tmp_dir();
        let mgr = bootstrap(&dir);
        let json = r#"{"api_key":"KEY-A","api_secret":"secret-a","position_mode":"hedge","is_testnet":true}"#;
        write_csv(
            &dir,
            &format!(
                "{NEW_HEADER}\n0,alice,binance,{},0,0,0\n",
                mgr.cipher.encrypt(json).unwrap()
            ),
        );
        mgr.load_from_csv(false).unwrap();

        // Act
        let acc = mgr.resolve("alice", Some("binance")).expect("alice active");

        // Assert：强类型枚举字段逐一就位（ResolvedAccount 直接携带枚举）。
        assert_eq!(acc.name, "alice");
        assert_eq!(acc.exchange, "binance");
        match acc.credential {
            ExchangeCredential::ApiSecret {
                api_key,
                api_secret,
                api_passphrase,
                position_mode,
                is_testnet,
            } => {
                assert_eq!(api_key, "KEY-A");
                assert_eq!(api_secret, "secret-a");
                assert_eq!(api_passphrase, None);
                assert_eq!(position_mode, PositionMode::Hedge);
                assert!(is_testnet);
            }
            other => panic!("应为 ApiSecret 凭证，实际 {other:?}"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn plaintext_credentials_json_is_normalized_and_resolves() {
        // Arrange：credentials 列为无前缀明文 JSON（联调口径），导入侧归一化加密。
        let dir = tmp_dir();
        let json = r#"{"api_key":"k","api_secret":"s"}"#;
        write_csv(
            &dir,
            &format!("{NEW_HEADER}\n0,alice,binance,{},0,0,0\n", csv_cell(json)),
        );
        let mgr = bootstrap(&dir);

        // Act / Assert：明文 JSON 可解析（解密路径对无前缀输入透传兼容）。
        let acc = mgr.resolve("alice", None).expect("plaintext compatible");
        match acc.credential {
            ExchangeCredential::ApiSecret { api_key, .. } => assert_eq!(api_key, "k"),
            other => panic!("应为 ApiSecret 凭证，实际 {other:?}"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- 旧宽表兼容已删除（Phase 4 收尾，不留双轨）：拒载语义见
    // legacy_wide_csv_is_rejected_with_migrate_hint；旧→新映射逻辑归
    // account-tool migrate 所有。----

    // ---- 解析失败拒载不静默 ----

    #[test]
    fn resolved_account_accessors_expose_account_attributes() {
        // Arrange：hedge+测试网 binance 行（新格式）与 CTP 行并存。
        let dir = tmp_dir();
        let mgr = bootstrap(&dir);
        let binance_json =
            r#"{"api_key":"k","api_secret":"s","position_mode":"hedge","is_testnet":true}"#;
        let ctp_json = r#"{"broker_id":"9999","user_id":"u","password":"p","app_id":"a","auth_code":"c","front_address":"tcp://f:1"}"#;
        write_csv(
            &dir,
            &format!(
                "{NEW_HEADER}\n\
                 0,alice,binance,{},0,0,0\n\
                 0,ctp-acct,ctp,{},0,0,0\n",
                mgr.cipher.encrypt(binance_json).unwrap(),
                mgr.cipher.encrypt(ctp_json).unwrap(),
            ),
        );
        mgr.load_from_csv(false).unwrap();

        // Act / Assert：REST 所属性取自凭证；CTP 无该概念按缺省（正式网 + 单向）。
        let alice = mgr.resolve("alice", None).unwrap();
        assert_eq!(alice.position_mode(), PositionMode::Hedge);
        assert!(alice.is_testnet());
        let ctp = mgr.resolve("ctp-acct", None).unwrap();
        assert_eq!(ctp.position_mode(), PositionMode::OneWay);
        assert!(!ctp.is_testnet());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn legacy_wide_csv_is_rejected_with_migrate_hint() {
        // Arrange：旧宽表 binance 行（迁移期已过，兼容读路径已删，不留双轨）。
        let dir = tmp_dir();
        write_csv(
            &dir,
            &format!("{LEGACY_HEADER}\n0,alice,binance,KEY-A,secret-a,,hedge,1,0,0,0\n"),
        );

        // Act / Assert：拒载并给出可操作的迁移提示。
        let err = AccountManager::bootstrap(
            account_table(),
            dir.to_str().unwrap(),
            dir.join("account.csv"),
            dir.join("account.key"),
        )
        .err()
        .expect("旧宽表文件必须拒载");
        assert!(format!("{err:#}").contains("account-tool migrate"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn malformed_credentials_json_rejects_resolve() {
        // Arrange：credentials 列非法（非 JSON）。
        let dir = tmp_dir();
        write_csv(
            &dir,
            &format!("{NEW_HEADER}\n0,bad,binance,not-json,0,0,0\n"),
        );
        let mgr = bootstrap(&dir);

        // Act / Assert：resolve 返回 None（告警拒载），而非带空凭证的账户。
        assert!(mgr.resolve("bad", None).is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- 简报属性从 credentials JSON 提取 ----

    #[test]
    fn briefs_carry_attributes_from_credentials() {
        // Arrange：hedge+测试网的 binance 行（新格式）与 CTP 行并存。
        // 先 bootstrap 生成主密钥，再用管理器 cipher 加密（保证密钥一致）。
        let dir = tmp_dir();
        let mgr = bootstrap(&dir);
        let binance_json =
            r#"{"api_key":"k","api_secret":"s","position_mode":"hedge","is_testnet":true}"#;
        let ctp_json = r#"{"broker_id":"9999","user_id":"u","password":"p","app_id":"a","auth_code":"c","front_address":"tcp://f:1"}"#;
        write_csv(
            &dir,
            &format!(
                "{NEW_HEADER}\n\
                 0,hedger,binance,{},0,0,0\n\
                 0,ctp-acct,ctp,{},0,0,0\n",
                mgr.cipher.encrypt(binance_json).unwrap(),
                mgr.cipher.encrypt(ctp_json).unwrap(),
            ),
        );
        mgr.load_from_csv(false).unwrap();

        // Act
        let mut briefs = mgr.list_active_briefs();
        briefs.sort_by(|a, b| a.name.cmp(&b.name));

        // Assert：REST 所属性取自 JSON；CTP 无该属性按缺省（正式网 + 单向）。
        assert_eq!(briefs.len(), 2);
        assert_eq!(briefs[0].name, "ctp-acct");
        assert!(!briefs[0].is_testnet);
        assert_eq!(briefs[0].position_mode, PositionMode::OneWay);
        assert_eq!(briefs[1].name, "hedger");
        assert!(briefs[1].is_testnet);
        assert_eq!(briefs[1].position_mode, PositionMode::Hedge);
        // account_by_id 口径一致。
        let by_id = mgr
            .account_by_id(mgr.account_id_by_name("hedger").unwrap())
            .unwrap();
        assert_eq!(by_id.position_mode, PositionMode::Hedge);
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- 既有行为保留（重载 / 软删 / 去重 / 加密往返）----

    #[test]
    fn loads_resolves_and_reloads() {
        let dir = tmp_dir();
        let json_a = r#"{"api_key":"KEY-A","api_secret":"secret-a"}"#;
        write_csv(
            &dir,
            &format!("{NEW_HEADER}\n0,alice,binance,{},0,0,0\n", csv_cell(json_a)),
        );
        let mgr = bootstrap(&dir);

        assert!(mgr.resolve("alice", Some("binance")).is_some());
        assert!(mgr.resolve("alice", Some("deribit")).is_none());
        assert!(mgr.resolve("bob", None).is_none());

        // 重载：改密 + 新增 bob。
        let json_a2 = r#"{"api_key":"KEY-A","api_secret":"secret-a2"}"#;
        let json_b = r#"{"api_key":"KEY-B","api_secret":"secret-b"}"#;
        write_csv(
            &dir,
            &format!(
                "{NEW_HEADER}\n0,alice,binance,{},0,0,0\n0,bob,binance,{},0,0,0\n",
                csv_cell(json_a2),
                csv_cell(json_b),
            ),
        );
        mgr.reload().unwrap();
        match mgr.resolve("alice", None).unwrap().credential {
            ExchangeCredential::ApiSecret { api_secret, .. } => {
                assert_eq!(api_secret, "secret-a2")
            }
            other => panic!("应为 ApiSecret 凭证，实际 {other:?}"),
        }
        assert!(mgr.resolve("bob", None).is_some());

        // 文件中移除 alice -> 软删除。
        write_csv(
            &dir,
            &format!("{NEW_HEADER}\n0,bob,binance,{},0,0,0\n", csv_cell(json_b)),
        );
        mgr.reload().unwrap();
        assert!(mgr.resolve("alice", None).is_none());
        assert!(mgr.resolve("bob", None).is_some());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reload_bumps_target_version() {
        // Arrange：首载一个账户，记录其目标 version。
        let dir = tmp_dir();
        let json = r#"{"api_key":"KEY-A","api_secret":"secret-a"}"#;
        write_csv(
            &dir,
            &format!("{NEW_HEADER}\n0,alice,binance,{},0,0,0\n", csv_cell(json)),
        );
        let mgr = bootstrap(&dir);
        let v0 = mgr.reload_version();
        assert_eq!(mgr.distinct_exchanges(), vec!["binance".to_string()]);

        // Act：文件变化后经 API 口径重载。
        let json2 = r#"{"api_key":"KEY-A","api_secret":"secret-a2","is_testnet":true}"#;
        write_csv(
            &dir,
            &format!("{NEW_HEADER}\n0,alice,binance,{},0,0,0\n", csv_cell(json2)),
        );
        mgr.reload().unwrap();

        // Assert：version 单调递增，且 is_testnet 已切换。
        let v1 = mgr.reload_version();
        assert!(v1 > v0);
        match mgr.resolve("alice", None).unwrap().credential {
            ExchangeCredential::ApiSecret { is_testnet, .. } => assert!(is_testnet),
            other => panic!("应为 ApiSecret 凭证，实际 {other:?}"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn distinct_exchanges_dedups_and_sorts_by_name() {
        // 3 个账户：2 binance、1 okx；迭代序不定，结果必须稳定。
        let dir = tmp_dir();
        let json = r#"{"api_key":"k","api_secret":"s"}"#;
        write_csv(
            &dir,
            &format!(
                "{NEW_HEADER}\n\
                 0,a1,binance,{},0,0,0\n\
                 0,a2,okx,{},0,0,0\n\
                 0,a3,binance,{},0,0,0\n",
                csv_cell(json),
                csv_cell(json),
                csv_cell(json),
            ),
        );
        let mgr = bootstrap(&dir);
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
    fn encrypted_json_roundtrips_through_file() {
        // Arrange：文件里预置整串加密 credentials。
        let dir = tmp_dir();
        let mgr = bootstrap(&dir);
        let enc = mgr
            .cipher
            .encrypt(r#"{"api_key":"k","api_secret":"topsecret"}"#)
            .unwrap();
        write_csv(
            &dir,
            &format!("{NEW_HEADER}\n0,carol,binance,{enc},0,0,0\n"),
        );

        // Act / Assert：加载后解密还原 secret。
        mgr.load_from_csv(false).unwrap();
        match mgr.resolve("carol", None).unwrap().credential {
            ExchangeCredential::ApiSecret { api_secret, .. } => {
                assert_eq!(api_secret, "topsecret")
            }
            other => panic!("应为 ApiSecret 凭证，实际 {other:?}"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn missing_file_is_empty_not_fatal() {
        let dir = tmp_dir();
        let mgr = bootstrap(&dir);
        assert_eq!(mgr.distinct_exchanges().len(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }
}
