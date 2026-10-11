use std::any::TypeId;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, RwLock};

use tracing::warn;

use crate::error::{Result, StoreError};
use crate::model::{AccountIndex, AccountModel, Model, StorageScope};
use crate::persistence::paths::{atomic_write, scan};
use crate::table::{AnyTable, Table};
use crate::time::{day_key, SharedClock};

/// 持久化路径相关的可配目录名。
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// 全局表（账户等）所在目录。
    pub global_dir: String,
    /// 记录的 account_id 找不到存活账户时的兜底目录。
    pub unassigned_dir: String,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            global_dir: "_system".to_string(),
            unassigned_dir: "_unassigned".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FlushReport {
    pub day: String,
    pub files: usize,
    pub rows: usize,
}

#[derive(Debug, Clone, Default)]
pub struct RecoveryReport {
    /// 成功导入的文件数。
    pub files: usize,
    /// 导入的数据行数。
    pub rows: usize,
    /// 跳过的未知 model 文件名。
    pub skipped_unknown: Vec<String>,
    /// 启动时检测到停机跨日，物理清除的 tombstone 行数。
    pub purged_on_startup: usize,
}

struct Registry {
    by_type: HashMap<TypeId, Arc<dyn AnyTable>>,
    by_name: HashMap<&'static str, Arc<dyn AnyTable>>,
}

/// 所有内存表的注册中心与持久化门面。
pub struct Store {
    registry: RwLock<Registry>,
    account_index: RwLock<Option<Arc<dyn AccountIndex>>>,
    clock: SharedClock,
    config: StoreConfig,
}

impl Store {
    pub fn new(clock: SharedClock) -> Arc<Self> {
        Self::with_config(clock, StoreConfig::default())
    }

    pub fn with_config(clock: SharedClock, config: StoreConfig) -> Arc<Self> {
        Arc::new(Self {
            registry: RwLock::new(Registry {
                by_type: HashMap::new(),
                by_name: HashMap::new(),
            }),
            account_index: RwLock::new(None),
            clock,
            config,
        })
    }

    /// 注册一张 model 表，返回其类型化句柄。
    pub fn register<T: Model>(self: &Arc<Self>) -> Result<Arc<Table<T>>> {
        let table = Table::<T>::new(self.clock.clone());
        let mut registry = self.registry.write().unwrap();
        if registry.by_type.contains_key(&TypeId::of::<T>())
            || registry.by_name.contains_key(T::MODEL_NAME)
        {
            return Err(StoreError::AlreadyRegistered(T::MODEL_NAME));
        }
        registry
            .by_type
            .insert(TypeId::of::<T>(), table.clone() as Arc<dyn AnyTable>);
        registry
            .by_name
            .insert(T::MODEL_NAME, table.clone() as Arc<dyn AnyTable>);
        Ok(table)
    }

    /// 注册账户表：同时建立 account_id -> 账户名索引，供 flush 拼路径。
    pub fn register_account<T: AccountModel>(self: &Arc<Self>) -> Result<Arc<Table<T>>> {
        let table = self.register::<T>()?;
        *self.account_index.write().unwrap() = Some(table.clone() as Arc<dyn AccountIndex>);
        Ok(table)
    }

    /// 取已注册表的类型化句柄。
    pub fn table<T: Model>(self: &Arc<Self>) -> Result<Arc<Table<T>>> {
        let registry = self.registry.read().unwrap();
        let any = registry
            .by_type
            .get(&TypeId::of::<T>())
            .ok_or(StoreError::UnknownModel(T::MODEL_NAME))?;
        any.clone()
            .as_any()
            .downcast::<Table<T>>()
            .map_err(|_| StoreError::UnknownModel(T::MODEL_NAME))
    }

    pub fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }

    /// 全量快照落盘：`{root}/{UTC day}/{scope}/{model}.csv`，原子替换。
    ///
    /// - `force == false`：只写有变更（dirty）的表，无变更空转不落盘；
    /// - `force == true`：所有表强制重写（跨日 rollover 时必须，保证新日期
    ///   目录下每个非空表都有最新日文件，启动恢复只认最新日）。
    pub fn flush(&self, root: &Path, force: bool) -> Result<FlushReport> {
        let day = day_key(self.clock.now_ms());
        let names = self
            .account_index
            .read()
            .unwrap()
            .as_ref()
            .map(|idx| idx.name_map())
            .unwrap_or_default();

        let registry = self.registry.read().unwrap();
        let mut files = 0;
        let mut rows = 0;

        for table in registry.by_name.values() {
            if !force && !table.is_dirty() {
                continue;
            }

            // 快照期间可能有并发变更：一轮写出后用 generation 判定快照是否覆盖
            // 最新状态；不稳定就重写，避免"写后清标记"吞掉变更。8 次后仍在
            // 竞争（持续高写入）则保留 dirty，由下一 tick 补写。
            let mut stable = false;
            let mut kept_dirs = HashSet::new();
            for _ in 0..8 {
                (stable, kept_dirs) = self.write_table_once(root, &day, &names, table)?;
                if stable {
                    break;
                }
            }
            files += kept_dirs.len();

            // 某作用域内该表行已全部消失（如账户改名/清空）时删掉陈旧文件。
            remove_stale_scopes(root, &day, table.name(), &kept_dirs)?;
            if !stable {
                warn!(
                    model = table.name(),
                    "flush raced with writes 8 times; deferred to next tick"
                );
            }
            rows += table.len();
        }
        Ok(FlushReport { day, files, rows })
    }

    /// 单轮快照落盘：返回 (快照是否覆盖最新 generation, 本次写出的作用域目录集合)。
    fn write_table_once(
        &self,
        root: &Path,
        day: &str,
        names: &HashMap<u64, String>,
        table: &Arc<dyn AnyTable>,
    ) -> Result<(bool, HashSet<String>)> {
        let gen = table.generation();
        let mut kept_dirs = HashSet::new();
        for (scope, bytes) in table.export_day()? {
            let scope_dir = self.scope_dir(names, scope);
            let path = root
                .join(day)
                .join(&scope_dir)
                .join(format!("{}.csv", table.name()));
            atomic_write(&path, &bytes)?;
            kept_dirs.insert(scope_dir);
        }
        Ok((table.mark_flushed(gen), kept_dirs))
    }

    /// 从快照恢复：**只导入最新数据日的全量快照**。
    ///
    /// 每个 scope 的日文件是当日内存全集；跨日 rollover 已把 tombstone 物理
    /// purge（新日文件中不再有这些行），若回溯读取旧日期文件会把它们"复活"，
    /// 因此旧日期文件在启动恢复时一律忽略。多日去重取最新属于按需历史查询，
    /// 不走本路径。若最新数据日不是今天（停机跨日），导入后再物理清除
    /// tombstone，等价于补做一次 rollover。
    pub fn recover(&self, root: &Path) -> Result<RecoveryReport> {
        let mut report = RecoveryReport::default();
        let files = scan(root)?;
        if files.is_empty() {
            return Ok(report);
        }

        let newest_day = files.last().expect("non-empty").day.clone();

        for file in files.iter().filter(|f| f.day == newest_day) {
            let table = {
                let registry = self.registry.read().unwrap();
                registry.by_name.get(file.model.as_str()).cloned()
            };
            let Some(table) = table else {
                warn!(
                    model = %file.model,
                    path = %file.path.display(),
                    "skip snapshot for unregistered model"
                );
                report.skipped_unknown.push(file.model.clone());
                continue;
            };
            let bytes = std::fs::read(&file.path)?;
            report.rows += table.import_bytes(&bytes)?;
            report.files += 1;
        }

        let today = day_key(self.clock.now_ms());
        if newest_day != today {
            report.purged_on_startup = self.purge_all();
        }
        Ok(report)
    }

    /// 物理清除所有表的 tombstone（跨日 rollover），返回总清除行数。
    pub fn purge_all(&self) -> usize {
        self.registry
            .read()
            .unwrap()
            .by_name
            .values()
            .map(|t| t.purge_deleted())
            .sum()
    }

    fn scope_dir(&self, names: &HashMap<u64, String>, scope: StorageScope) -> String {
        match scope {
            StorageScope::Global => self.config.global_dir.clone(),
            StorageScope::Account(id) => match names.get(&id) {
                Some(name) => name.clone(),
                None => {
                    warn!(
                        account_id = id,
                        "account name unresolved, using fallback dir"
                    );
                    self.config.unassigned_dir.clone()
                }
            },
        }
    }
}

/// 删除当日目录下、不属于现存作用域集合的陈旧 `{model}.csv`（并清理空目录）。
fn remove_stale_scopes(root: &Path, day: &str, model: &str, keep: &HashSet<String>) -> Result<()> {
    let day_dir = root.join(day);
    let Ok(scopes) = std::fs::read_dir(&day_dir) else {
        return Ok(());
    };
    let file_name = format!("{model}.csv");
    for entry in scopes.flatten() {
        let dir = entry.path();
        if !dir.is_dir() {
            continue;
        }
        let os_name = entry.file_name();
        let Some(dir_name) = os_name.to_str() else {
            continue;
        };
        if keep.contains(dir_name) {
            continue;
        }
        let stale = dir.join(&file_name);
        if !stale.is_file() {
            continue;
        }
        std::fs::remove_file(&stale)?;
        // 目录空了就顺手删掉，非空（还有别的 model 文件）则保留。
        let _ = std::fs::remove_dir(&dir);
    }
    Ok(())
}
