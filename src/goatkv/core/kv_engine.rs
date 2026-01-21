use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::goatkv::core::cleanup_worker::CleanupWorker;
use crate::goatkv::core::flush_worker::{FlushTask, FlushWorker};
use crate::goatkv::core::lsm_state::LSMState;
use crate::goatkv::core::mem_table::{ImmutableMemTable, MemTable};
use crate::goatkv::encoding::internal_key::{InternalKey, InternalKeyKind};
use crate::goatkv::storage::wal_manager::{WalIterator, WalManager};
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

        // 获取主 WAL 文件路径
        let wal_path = DbPathManager::global().main_wal_path();

        // 创建内存表
        let mem_table = Arc::new(MemTable::new(options.mem_table_size));

        // 如果启用 WAL 恢复，则尝试从 WAL 恢复
        let mut wal_max_sequence = 0;
        if options.recover_from_wal {
            wal_max_sequence = Self::replay(&mem_table, &wal_path)?;
        }

        // 创建 WAL 管理器
        let wal_manager = WalManager::new(wal_path)
            .map_err(|e| std::io::Error::other(format!("Failed to open WAL file: {}", e)))?;

        let (cleanup_worker, obsolete_sender) =
            CleanupWorker::new(DbPathManager::global().data_dir().into());

        // 创建 LSM 状态管理器（内部会创建 VersionSet）
        let lsm_state = Arc::new(RwLock::new(LSMState::new(
            &options,
            mem_table.clone(),
            obsolete_sender.clone(),
        )?));

        let last_sequence = {
            let lsm_guard = lsm_state.read().unwrap();
            let vs_guard = lsm_guard.version_set.read().unwrap();
            std::cmp::max(wal_max_sequence, vs_guard.last_sequence())
        };

        // 创建序列号生成器（从最后序列号继续）
        let sequence_number = Arc::new(SequenceNumber::with_start(last_sequence + 1));

        // 创建后台刷盘 Worker
        let flush_worker = FlushWorker::new(lsm_state.clone(), obsolete_sender);

        Ok(Self {
            wal_manager: Arc::new(Mutex::new(wal_manager)),
            sequence_number,
            lsm_state,
            options: Arc::new(options),
            flush_worker,
            cleanup_worker,
            flush_task_id: AtomicUsize::new(0),
        })
    }

    /// 创建一个新的KvEngine，不尝试从WAL恢复
    /// 主要用于测试
    #[cfg(test)]
    pub fn new_for_test() -> Self {
        let options = KvEngineOptions::for_test();
        Self::new_with_options(options).expect("Failed to create test KvEngine")
    }

    /// 从 WAL 文件恢复数据到内存表
    fn replay(mem_table: &MemTable, exec_path: &PathBuf) -> Result<u64, std::io::Error> {
        let wal_iterator = match WalIterator::new(exec_path) {
            Ok(iterator) => iterator,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(0);
            }
            Err(e) => return Err(e),
        };
        let mut max_seq = 0u64;
        for entry in wal_iterator {
            match entry {
                Ok((key, value)) => {
                    max_seq = max_seq.max(key.sequence_number());
                    mem_table.put(key, value.into());
                }
                Err(err) => {
                    println!("Failed to replay WAL entry: {}, skipped", err);
                }
            }
        }
        Ok(max_seq)
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
        for table in immutable_mem_tables.iter().rev() {
            if let Some((internal_key, value)) = table.get(key) {
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
        let immutable_mem_table = {
            let mut state = self.lsm_state.write().unwrap();

            // 克隆当前的 memtable
            let mem_table = state.mem_table.clone();

            // 创建 immutable_mem_table
            let immutable_mem_table = Arc::new(ImmutableMemTable::new(mem_table.inner()));

            // 将 immutable_mem_table 放入队列并创建新的 memtable
            state
                .immutable_mem_tables
                .push_back(immutable_mem_table.clone());
            state.mem_table = Arc::new(MemTable::new(self.options.mem_table_size));

            immutable_mem_table
        };

        // 发送 flush 任务到后台线程
        let task_id = self.flush_task_id.fetch_add(1, Ordering::SeqCst);
        if let Err(e) = self.flush_worker.submit_task(FlushTask {
            id: task_id,
            immutable_mem_table,
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
