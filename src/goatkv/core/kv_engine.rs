use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::goatkv::core::cleanup_worker::CleanupWorker;
use crate::goatkv::core::flush_worker::{FlushTask, FlushWorker};
use crate::goatkv::core::lsm_state::{ImmutableMemTableEntry, LSMState};
use crate::goatkv::core::mem_table::{ImmutableMemTable, MemTable};
use crate::goatkv::encoding::internal_key::{InternalKey, InternalKeyKind};
use crate::goatkv::storage::wal_manager::{replay_wal_file, WalManager, WalReplayStats};
use crate::goatkv::utils::db_path_manager::DbPathManager;
use crate::goatkv::utils::options::KvEngineOptions;
use crate::goatkv::utils::sequence_number::SequenceNumber;

/// LSM-Tree 键值存储引擎
#[derive(Debug)]
pub struct KvEngine {
    /// WAL 管理器，负责写前日志
    wal_manager: Arc<Mutex<WalManager>>,
    /// 序列号生成器
    sequence_number: Arc<SequenceNumber>,
    /// LSM 状态管理器（包含 VersionSet）
    lsm_state: Arc<RwLock<LSMState>>,
    /// 配置选项
    options: Arc<KvEngineOptions>,
    /// 后台刷盘 Worker
    flush_worker: FlushWorker,
    /// 后台清理 Worker
    cleanup_worker: CleanupWorker,
    /// 当前正在执行的 FlushTask 的 ID
    flush_task_id: AtomicUsize,
    /// 当前 WAL 日志编号
    current_log_number: AtomicU64,
    /// WAL 引用计数，用于延迟删除
    wal_refcounts: Arc<Mutex<std::collections::HashMap<u64, usize>>>,
}

impl Default for KvEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl KvEngine {
    /// 创建新的 KvEngine，使用默认数据目录（当前目录下的 goatdb_data）
    pub fn new() -> Self {
        let options = KvEngineOptions::default();
        Self::new_with_options(options).expect("Failed to create KvEngine with default options")
    }

    /// 创建新的 KvEngine，使用指定的配置选项
    ///
    /// # 参数
    /// - `options`: KvEngine 配置选项
    ///
    /// # 返回
    /// - `Ok(KvEngine)`: 创建成功
    /// - `Err(std::io::Error)`: 创建目录或初始化失败
    pub fn new_with_options(options: KvEngineOptions) -> Result<Self, std::io::Error> {
        // 初始化全局路径管理器单例
        // 使用 try_init 以便在测试中可以重用已初始化的单例
        let _ = DbPathManager::try_init(&options.data_dir)?;
        let _ = DbPathManager::global().cleanup_tmp_dir();

        let (cleanup_worker, obsolete_sender) =
            CleanupWorker::new(DbPathManager::global().data_dir().into());

        let mem_table = Arc::new(MemTable::new(options.mem_table_size));

        // 创建 LSM 状态管理器（内部会创建 VersionSet）
        let lsm_state = Arc::new(RwLock::new(LSMState::new(
            &options,
            mem_table.clone(),
            obsolete_sender.clone(),
        )?));

        let (wal_stats, _wal_max_number) = if options.recover_from_wal {
            let min_log_number = {
                let guard = lsm_state.read().unwrap();
                let vs_guard = guard.version_set.read().unwrap();
                vs_guard.log_number()
            };
            Self::replay_into_state(&lsm_state, options.mem_table_size, min_log_number)?
        } else {
            (
                WalReplayStats {
                    max_sequence: 0,
                    entries: 0,
                    truncated: false,
                },
                0,
            )
        };

        if wal_stats.truncated {
            eprintln!("WAL replay truncated due to corruption or partial record.");
        }

        // 延后推进 log_number：只有当新的 WAL 真正写入并在 manifest 中记录后才前进。
        // 这里仅保证存在一个可写 WAL，如果 manifest 中 log_number 为 0，则用 1。
        let current_log_number = {
            let guard = lsm_state.read().unwrap();
            let mut vs_guard = guard.version_set.write().unwrap();
            let mut log_number = vs_guard.log_number();
            if log_number == 0 {
                log_number = 1;
                let mut edit = crate::goatkv::metadata::version_edit::VersionEdit::new();
                edit.set_log_number(log_number);
                vs_guard.apply_edit(edit)?;
            }
            log_number
        };

        let wal_path = DbPathManager::global().wal_path_by_id(current_log_number);
        let wal_manager = WalManager::new(wal_path, options.wal_sync)
            .map_err(|e| std::io::Error::other(format!("Failed to open WAL file: {}", e)))?;

        let last_sequence = {
            let lsm_guard = lsm_state.read().unwrap();
            let vs_guard = lsm_guard.version_set.read().unwrap();
            std::cmp::max(wal_stats.max_sequence, vs_guard.last_sequence())
        };

        // 创建序列号生成器（从最后序列号继续）
        let sequence_number = Arc::new(SequenceNumber::with_start(last_sequence + 1));

        let wal_refcounts = Arc::new(Mutex::new(std::collections::HashMap::new()));

        let engine = Self {
            wal_manager: Arc::new(Mutex::new(wal_manager)),
            sequence_number,
            lsm_state: lsm_state.clone(),
            options: Arc::new(options),
            flush_worker: FlushWorker::new(lsm_state.clone(), obsolete_sender, wal_refcounts.clone()),
            cleanup_worker,
            flush_task_id: AtomicUsize::new(0),
            current_log_number: AtomicU64::new(current_log_number),
            wal_refcounts,
        };

        // 提交恢复阶段遗留的 immutable memtables
        {
            let state = engine.lsm_state.read().unwrap();
            for entry in state.immutable_mem_tables.iter() {
                if entry.wal_log_number > 0 {
                    *engine
                        .wal_refcounts
                        .lock()
                        .unwrap()
                        .entry(entry.wal_log_number)
                        .or_insert(0) += 1;
                }
                let task_id = engine.flush_task_id.fetch_add(1, Ordering::SeqCst);
                let _ = engine.flush_worker.submit_task(FlushTask {
                    id: task_id,
                    immutable_mem_table: entry.table.clone(),
                    wal_log_number: entry.wal_log_number,
                    new_log_number: 0,
                });
            }
        }

        Ok(engine)
    }

    /// 创建一个新的KvEngine，不尝试从WAL恢复
    /// 主要用于测试
    #[cfg(test)]
    pub fn new_for_test() -> Self {
        let options = KvEngineOptions::for_test();
        Self::new_with_options(options).expect("Failed to create test KvEngine")
    }

    /// 从 WAL 文件恢复数据到内存表
    fn replay_into_state(
        lsm_state: &Arc<RwLock<LSMState>>,
        mem_table_size: usize,
        min_log_number: u64,
    ) -> Result<(WalReplayStats, u64), std::io::Error> {
        let wal_files = Self::list_wal_files(min_log_number)?;
        let mut stats = WalReplayStats {
            max_sequence: 0,
            entries: 0,
            truncated: false,
        };
        let mut max_log_number = 0u64;

        for (log_number, wal_path) in wal_files {
            if log_number > max_log_number {
                max_log_number = log_number;
            }
            if !wal_path.exists() {
                continue;
            }
            let file_stats = replay_wal_file(&wal_path, |key, value| {
                let mut state = lsm_state.write().unwrap();
                state.mem_table.put(key, value.into());
                if state.mem_table.should_flush() {
                    let imm = Arc::new(ImmutableMemTable::new(state.mem_table.inner()));
                    state.immutable_mem_tables.push_back(ImmutableMemTableEntry {
                        table: imm,
                        wal_log_number: log_number,
                    });
                    state.mem_table = Arc::new(MemTable::new(mem_table_size));
                }
            })?;
            stats.max_sequence = stats.max_sequence.max(file_stats.max_sequence);
            stats.entries += file_stats.entries;
            stats.truncated |= file_stats.truncated;
            // 不因为截断停止，继续尝试后续 WAL，保证尽量多恢复

            // 完成一个 WAL 文件后，封存当前 memtable，确保 WAL 边界清晰
            {
                let mut state = lsm_state.write().unwrap();
                if !state.mem_table.is_empty() {
                    let imm = Arc::new(ImmutableMemTable::new(state.mem_table.inner()));
                    state.immutable_mem_tables.push_back(ImmutableMemTableEntry {
                        table: imm,
                        wal_log_number: log_number,
                    });
                    state.mem_table = Arc::new(MemTable::new(mem_table_size));
                }
            }
        }

        Ok((stats, max_log_number))
    }

    fn list_wal_files(min_log_number: u64) -> Result<Vec<(u64, PathBuf)>, std::io::Error> {
        let path_manager = DbPathManager::global();
        let wal_dir = path_manager.wal_dir();
        let mut wal_files = Vec::new();

        if wal_dir.exists() {
            for entry in std::fs::read_dir(wal_dir)? {
                let entry = entry?;
                let path = entry.path();
                if !path.is_file() {
                    continue;
                }
                if path.extension().and_then(|ext| ext.to_str()) != Some("wal") {
                    continue;
                }
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(number) = stem.parse::<u64>() {
                        if number >= min_log_number {
                            wal_files.push((number, path));
                        }
                    }
                }
            }
        }

        wal_files.sort_by_key(|(num, _)| *num);

        let main_wal = path_manager.main_wal_path();
        if min_log_number == 0 && main_wal.exists() {
            wal_files.insert(0, (0, main_wal));
        }

        Ok(wal_files)
    }
}

impl KvEngine {
    /// 获取路径管理器引用（全局单例）
    pub fn path_manager() -> &'static DbPathManager {
        DbPathManager::global()
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let (mem_table, immutable_mem_tables, version_set) = {
            let lsm_state = self.lsm_state.read().unwrap();
            let mem_table = lsm_state.mem_table.clone();
            let immutable_mem_tables = lsm_state.immutable_mem_tables.clone();
            let version_set = lsm_state.version_set.clone();
            (mem_table, immutable_mem_tables, version_set)
        };

        // First check memtable
        if let Some((internal_key, value)) = mem_table.get(key) {
            if internal_key.kind() != InternalKeyKind::Delete {
                return Some(value);
            } else {
                return None;
            }
        }

        // Then check immutable memtables in order (newer first)
        for entry in immutable_mem_tables.iter().rev() {
            if let Some((internal_key, value)) = entry.table.get(key) {
                if internal_key.kind() != InternalKeyKind::Delete {
                    return Some(value);
                } else {
                    return None;
                }
            }
        }

        // Then check SSTables via VersionSet
        if let Ok(vs) = version_set.read() {
            let version = vs.current();
            if let Some((internal_key, value)) = version.get(key) {
                if internal_key.kind() != InternalKeyKind::Delete {
                    return Some(value);
                } else {
                    return None;
                }
            }
        }

        // Key not found
        None
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        // 构造InternalKey
        let internal_key = InternalKey::new(key, self.sequence_number.next(), InternalKeyKind::Put);

        // 先写入wal
        self.wal_manager
            .lock()
            .unwrap()
            .write(&internal_key, &value)
            .expect("Failed to write to WAL");

        // 再写入memtable
        let needs_flush = {
            let guard = self.lsm_state.read().unwrap();
            guard.mem_table.put(internal_key, value.into());
            guard.mem_table.should_flush()
        };

        // 判断memtable是否已达到容量限制，
        // 达到容量限制则转换成immutable_mem_tables
        if needs_flush {
            self.flush();
        }
    }

    pub fn delete(&self, key: Vec<u8>) {
        // 先构造InternalKey
        let internal_key =
            InternalKey::new(key, self.sequence_number.next(), InternalKeyKind::Delete);

        // 先写入wal
        self.wal_manager
            .lock()
            .unwrap()
            .write(&internal_key, &[][..])
            .expect("Failed to write to WAL");

        // 再写入memtable
        let needs_flush = {
            let guard = self.lsm_state.read().unwrap();
            guard.mem_table.put(internal_key, vec![].into());
            guard.mem_table.should_flush()
        };

        // 判断memtable是否已达到容量限制，
        // 达到容量限制则转换成immutable_mem_tables
        if needs_flush {
            self.flush();
        }
    }

    pub fn flush(&self) {
        let (immutable_mem_table, old_log_number, new_log_number, rotation_succeeded) = {
            let mut wal_guard = self.wal_manager.lock().unwrap();
            let mut state = self.lsm_state.write().unwrap();

            // 克隆当前的 memtable
            let mem_table = state.mem_table.clone();

            // 创建 immutable_mem_table
            let immutable_mem_table = Arc::new(ImmutableMemTable::new(mem_table.inner()));

            // 将 immutable_mem_table 放入队列并创建新的 memtable
            state.immutable_mem_tables.push_back(ImmutableMemTableEntry {
                table: immutable_mem_table.clone(),
                wal_log_number: self.current_log_number.load(Ordering::SeqCst),
            });
            state.mem_table = Arc::new(MemTable::new(self.options.mem_table_size));

            let old_log_number = self.current_log_number.load(Ordering::SeqCst);
            let candidate_log_number = {
                let vs = state.version_set.read().unwrap();
                vs.log_number() + 1
            };
            let new_wal_path = DbPathManager::global().wal_path_by_id(candidate_log_number);
            let (new_log_number, rotation_succeeded) =
                match WalManager::new(new_wal_path, self.options.wal_sync) {
                Ok(new_manager) => {
                    *wal_guard = new_manager;
                    let new_log_number = candidate_log_number;
                    self.current_log_number
                        .store(new_log_number, Ordering::SeqCst);
                    (new_log_number, true)
                }
                Err(e) => {
                    eprintln!("Failed to rotate WAL: {}", e);
                    (old_log_number, false)
                }
            };

            (
                immutable_mem_table,
                old_log_number,
                new_log_number,
                rotation_succeeded,
            )
        };

        // 发送 flush 任务到后台线程
        let task_id = self.flush_task_id.fetch_add(1, Ordering::SeqCst);
        if rotation_succeeded && old_log_number > 0 {
            *self
                .wal_refcounts
                .lock()
                .unwrap()
                .entry(old_log_number)
                .or_insert(0) += 1;
        }
        if let Err(e) = self.flush_worker.submit_task(FlushTask {
            id: task_id,
            immutable_mem_table,
            wal_log_number: if rotation_succeeded { old_log_number } else { 0 },
            new_log_number: if rotation_succeeded { new_log_number } else { 0 },
        }) {
            eprintln!("Failed to send flush task: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let engine = KvEngine::new_for_test();

        // Test put and get
        engine.put(b"key1".to_vec(), b"value1".to_vec());
        assert_eq!(engine.get(b"key1"), Some(b"value1".to_vec()));

        engine.put(b"key2".to_vec(), b"value2".to_vec());
        assert_eq!(engine.get(b"key2"), Some(b"value2".to_vec()));

        // Get non-existent key
        assert_eq!(engine.get(b"nonexistent"), None);
    }

    #[test]
    fn test_update_existing_key() {
        let engine = KvEngine::new_for_test();

        // Insert key
        engine.put(b"key1".to_vec(), b"value1".to_vec());
        assert_eq!(engine.get(b"key1"), Some(b"value1".to_vec()));

        // Update key
        engine.put(b"key1".to_vec(), b"newvalue".to_vec());
        assert_eq!(engine.get(b"key1"), Some(b"newvalue".to_vec()));
    }

    #[test]
    fn test_delete_key() {
        let engine = KvEngine::new_for_test();

        // Insert key
        engine.put(b"key1".to_vec(), b"value1".to_vec());
        assert_eq!(engine.get(b"key1"), Some(b"value1".to_vec()));

        // Delete key
        engine.delete(b"key1".to_vec());
        assert_eq!(engine.get(b"key1"), None);

        // Delete non-existent key (should insert delete marker)
        engine.delete(b"nonexistent".to_vec());
        assert_eq!(engine.get(b"nonexistent"), None);
    }

    #[test]
    fn test_delete_then_reinsert() {
        let engine = KvEngine::new_for_test();

        // Insert and delete
        engine.put(b"key1".to_vec(), b"value1".to_vec());
        engine.delete(b"key1".to_vec());
        assert_eq!(engine.get(b"key1"), None);

        // Re-insert same key
        engine.put(b"key1".to_vec(), b"value2".to_vec());
        assert_eq!(engine.get(b"key1"), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_multiple_operations() {
        let engine = KvEngine::new_for_test();

        // Multiple operations
        engine.put(b"key1".to_vec(), b"value1".to_vec());
        engine.put(b"key2".to_vec(), b"value2".to_vec());
        engine.delete(b"key1".to_vec());
        engine.put(b"key3".to_vec(), b"value3".to_vec());
        engine.put(b"key2".to_vec(), b"updated_value2".to_vec());

        assert_eq!(engine.get(b"key1"), None);
        assert_eq!(engine.get(b"key2"), Some(b"updated_value2".to_vec()));
        assert_eq!(engine.get(b"key3"), Some(b"value3".to_vec()));
    }

    #[test]
    fn test_path_manager_integration() {
        let _engine = KvEngine::new_for_test();
        let path_manager = KvEngine::path_manager();

        // Verify path manager is properly integrated
        assert!(path_manager.base_path().exists());
        assert!(path_manager.data_dir().exists());
        assert!(path_manager.wal_dir().exists());
        assert!(path_manager.log_dir().exists());
        assert!(path_manager.tmp_dir().exists());

        // Verify WAL file is in the correct location
        let wal_path = path_manager.main_wal_path();
        assert!(wal_path.parent().unwrap() == path_manager.wal_dir());
    }
}
