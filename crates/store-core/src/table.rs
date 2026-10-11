use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::error::{Result, StoreError};
use crate::model::{Model, StorageScope};
use crate::time::SharedClock;

/// 单张 model 内存表。
///
/// 结构：`RwLock<HashMap<u64, Arc<RwLock<T>>>>` —— 表读锁仅保护索引，
/// 列表类查询先在表锁内 clone 出 `Arc<RwLock<T>>` 句柄立即放锁，再逐行加子锁，
/// 避免慢读者长期持有表锁。
///
/// **注意**：传给 [`update`](Table::update) / [`with_row`](Table::with_row)
/// 的闭包内不得再访问本表（也不要获取其它可能回环的锁），否则有自死锁风险。
pub struct Table<T: Model> {
    rows: RwLock<HashMap<u64, Arc<RwLock<T>>>>,
    next_id: AtomicU64,
    /// 数据版本：每次业务变更（insert/upsert/update/soft_delete）+1。
    generation: AtomicU64,
    /// 已成功落盘的版本。generation != flushed 即为 dirty。
    flushed: AtomicU64,
    clock: SharedClock,
}

impl<T: Model> Table<T> {
    pub fn new(clock: SharedClock) -> Arc<Self> {
        Arc::new(Self {
            rows: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            generation: AtomicU64::new(0),
            flushed: AtomicU64::new(0),
            clock,
        })
    }

    /// 是否有未落盘变更。
    pub(crate) fn is_dirty(&self) -> bool {
        self.generation.load(Ordering::Relaxed) != self.flushed.load(Ordering::Relaxed)
    }

    /// 当前数据版本（快照前读取）。
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    /// 快照写出后调用：若期间没有新变更则记录已刷版本并返回 true。
    ///
    /// 无需 CAS：检查与写入之间若发生变更，generation 已变大，留下
    /// `generation > flushed` 的 dirty 状态由下一 tick 补写。
    pub(crate) fn mark_flushed(&self, gen: u64) -> bool {
        if self.generation.load(Ordering::Relaxed) == gen {
            self.flushed.store(gen, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    fn touch_generation(&self) {
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    /// 插入一条记录。`id == 0` 时自动分配；`created_at/updated_at` 自动注入。
    pub fn insert(&self, mut item: T) -> Result<T> {
        let now = self.clock.now_ms();
        let mut rows = self.rows.write().unwrap();
        let id = if item.id() == 0 {
            self.next_id.fetch_add(1, Ordering::SeqCst)
        } else {
            let id = item.id();
            if rows.contains_key(&id) {
                return Err(StoreError::DuplicateId(id));
            }
            self.next_id.fetch_max(id + 1, Ordering::SeqCst);
            id
        };
        item.set_id(id);
        item.set_timestamps(now, now);
        let stored = item.clone();
        rows.insert(id, Arc::new(RwLock::new(item)));
        drop(rows);
        self.touch_generation();
        Ok(stored)
    }

    /// 存在则整体替换（保留原 created_at），不存在则插入。时间戳自动维护。
    pub fn upsert(&self, mut item: T) -> Result<T> {
        let now = self.clock.now_ms();
        let mut rows = self.rows.write().unwrap();
        let id = if item.id() == 0 {
            self.next_id.fetch_add(1, Ordering::SeqCst)
        } else {
            self.next_id.fetch_max(item.id() + 1, Ordering::SeqCst);
            item.id()
        };
        item.set_id(id);
        let created = rows
            .get(&id)
            .map(|row| row.read().unwrap().created_at())
            .unwrap_or(now);
        item.set_timestamps(created, now);

        let stored = item.clone();
        rows.insert(id, Arc::new(RwLock::new(item)));
        drop(rows);
        self.touch_generation();
        Ok(stored)
    }

    /// 取一条**未删除**记录的深拷贝；已删除按不存在处理。
    pub fn get(&self, id: u64) -> Result<Option<T>> {
        Ok(self.get_raw(id)?.filter(|v| !v.deleted()))
    }

    /// 取一条记录，含已软删除的 tombstone。
    pub fn get_including_deleted(&self, id: u64) -> Result<Option<T>> {
        self.get_raw(id)
    }

    fn get_raw(&self, id: u64) -> Result<Option<T>> {
        let Some(row) = self.row_handle_opt(id)? else {
            return Ok(None);
        };
        let guard = row.read().unwrap();
        Ok(Some(guard.clone()))
    }

    /// 全部**未删除**记录，按 id 升序。
    pub fn list(&self) -> Vec<T> {
        self.snapshot(false)
    }

    /// 全部记录（含 tombstone），按 id 升序。
    pub fn list_including_deleted(&self) -> Vec<T> {
        self.snapshot(true)
    }

    /// 在行子锁内原地修改，自动刷新 `updated_at`。已删除行返回 NotFound。
    pub fn update<F>(&self, id: u64, f: F) -> Result<T>
    where
        F: FnOnce(&mut T),
    {
        let row = self.row_handle(id)?;
        let mut guard = row.write().unwrap();
        if guard.deleted() {
            return Err(StoreError::NotFound(id));
        }
        let created = guard.created_at();
        f(&mut guard);
        guard.set_timestamps(created, self.clock.now_ms());
        let stored = guard.clone();
        drop(guard);
        self.touch_generation();
        Ok(stored)
    }

    /// 零拷贝只读访问（不 clone 整行），返回闭包的计算结果。已删除行不可见。
    pub fn with_row<R, F>(&self, id: u64, f: F) -> Result<Option<R>>
    where
        F: FnOnce(&T) -> R,
    {
        let Some(row) = self.row_handle_opt(id)? else {
            return Ok(None);
        };
        let guard = row.read().unwrap();
        if guard.deleted() {
            return Ok(None);
        }
        Ok(Some(f(&guard)))
    }

    /// 软删除：置 `deleted=1` 并刷新 `updated_at`。行在当日快照中以 tombstone 保留。
    pub fn soft_delete(&self, id: u64) -> Result<T> {
        let row = self.row_handle(id)?;
        let mut guard = row.write().unwrap();
        let created = guard.created_at();
        guard.set_deleted(true);
        guard.set_timestamps(created, self.clock.now_ms());
        let stored = guard.clone();
        drop(guard);
        self.touch_generation();
        Ok(stored)
    }

    pub fn len(&self) -> usize {
        self.rows.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.read().unwrap().is_empty()
    }

    /// 深拷贝快照（锁内只 clone，调用方拿到后可在锁外做序列化等慢操作）。
    pub(crate) fn snapshot(&self, include_deleted: bool) -> Vec<T> {
        let mut handles: Vec<Arc<RwLock<T>>> =
            self.rows.read().unwrap().values().map(Arc::clone).collect();
        handles.sort_by_key(|r| r.read().unwrap().id());
        handles
            .iter()
            .map(|r| r.read().unwrap().clone())
            .filter(|v| include_deleted || !v.deleted())
            .collect()
    }

    /// 物理清除 `deleted=1` 的行（跨日 rollover 时调用），返回清除条数。
    pub(crate) fn purge_deleted(&self) -> usize {
        let mut rows = self.rows.write().unwrap();
        let before = rows.len();
        rows.retain(|_, r| !r.read().unwrap().deleted());
        before - rows.len()
    }

    /// 恢复专用写入：按 `updated_at` 最新者胜，不改写时间戳。
    pub(crate) fn recover_put(&self, item: T) {
        let id = item.id();
        let mut rows = self.rows.write().unwrap();
        match rows.get(&id) {
            Some(existing) if item.updated_at() < existing.read().unwrap().updated_at() => {}
            Some(existing) => *existing.write().unwrap() = item,
            None => {
                rows.insert(id, Arc::new(RwLock::new(item)));
            }
        }
        self.next_id
            .fetch_max(id.saturating_add(1), Ordering::SeqCst);
    }

    /// 导出当日快照：按 scope 分组，每组一个完整 CSV 字节块（含表头）。
    pub(crate) fn export_day(&self) -> Result<Vec<(StorageScope, Vec<u8>)>> {
        let rows = self.snapshot(true);
        let mut groups: BTreeMap<StorageScope, Vec<T>> = BTreeMap::new();
        for row in rows {
            groups.entry(row.scope()).or_default().push(row);
        }
        groups
            .into_iter()
            .map(|(scope, group)| {
                let mut writer = csv::Writer::from_writer(Vec::new());
                for row in &group {
                    writer.serialize(row)?;
                }
                let bytes = writer
                    .into_inner()
                    .map_err(|e| StoreError::Io(e.into_error()))?;
                Ok((scope, bytes))
            })
            .collect()
    }

    /// 导入一个 CSV 字节块（含表头），按 `updated_at` 最新者胜，返回导入行数。
    pub(crate) fn import_bytes(&self, data: &[u8]) -> Result<usize> {
        let mut reader = csv::Reader::from_reader(data);
        let mut n = 0;
        for record in reader.deserialize() {
            self.recover_put(record?);
            n += 1;
        }
        Ok(n)
    }

    fn row_handle_opt(&self, id: u64) -> Result<Option<Arc<RwLock<T>>>> {
        Ok(self.rows.read().unwrap().get(&id).map(Arc::clone))
    }

    fn row_handle(&self, id: u64) -> Result<Arc<RwLock<T>>> {
        self.row_handle_opt(id)?.ok_or(StoreError::NotFound(id))
    }
}

/// 类型擦除接口，供 [`Store`](crate::store::Store) 以同质集合管理不同 model 的表。
pub(crate) trait AnyTable: Send + Sync {
    fn name(&self) -> &'static str;
    fn len(&self) -> usize;
    fn is_dirty(&self) -> bool;
    fn generation(&self) -> u64;
    fn mark_flushed(&self, gen: u64) -> bool;
    fn purge_deleted(&self) -> usize;
    fn export_day(&self) -> Result<Vec<(StorageScope, Vec<u8>)>>;
    fn import_bytes(&self, data: &[u8]) -> Result<usize>;
    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}

impl<T: Model> AnyTable for Table<T> {
    fn name(&self) -> &'static str {
        T::MODEL_NAME
    }

    fn len(&self) -> usize {
        Table::len(self)
    }

    fn is_dirty(&self) -> bool {
        Table::is_dirty(self)
    }

    fn generation(&self) -> u64 {
        Table::generation(self)
    }

    fn mark_flushed(&self, gen: u64) -> bool {
        Table::mark_flushed(self, gen)
    }

    fn purge_deleted(&self) -> usize {
        Table::purge_deleted(self)
    }

    fn export_day(&self) -> Result<Vec<(StorageScope, Vec<u8>)>> {
        Table::export_day(self)
    }

    fn import_bytes(&self, data: &[u8]) -> Result<usize> {
        Table::import_bytes(self, data)
    }

    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}
