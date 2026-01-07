use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread;

use crate::goatkv::core::lsm_state::LSMState;
use crate::goatkv::core::mem_table::{ImmutableMemTable, MemTable};
use crate::goatkv::encoding::internal_key::{InternalKey, InternalKeyKind};
use crate::goatkv::storage::sstable_builder::SSTableBuilder;
use crate::goatkv::storage::wal_manager::{WalIterator, WalManager};
use crate::goatkv::utils::db_path_manager::DbPathManager;
use crate::goatkv::utils::options::KvEngineOptions;
use crate::goatkv::utils::sequence_number::SequenceNumber;

#[derive(Debug)]
struct FlushTask {
    _mem_table: Arc<MemTable>,
    id: usize,
}

/// LSM-Tree 键值存储引擎
#[derive(Debug)]
pub struct KvEngine {
    /// 路径管理器，统一管理所有数据库文件路径
    path_manager: Arc<DbPathManager>,
    /// WAL 管理器，负责写前日志
    wal_manager: Arc<Mutex<WalManager>>,
    /// 序列号生成器
    sequence_number: Arc<SequenceNumber>,
    /// LSM 状态管理器
    lsm_state: Arc<RwLock<LSMState>>,
    /// 配置选项
    options: Arc<KvEngineOptions>,
    /// 发送器，用于发送 FlushTask 到后台线程
    sender: mpsc::Sender<FlushTask>,
    /// 后台线程句柄，用于管理后台线程
    _bg_thread_handle: thread::JoinHandle<()>,
    /// 当前正在执行的 FlushTask 的 ID
    flush_task_id: AtomicUsize,
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
        // 创建路径管理器
        let path_manager = Arc::new(DbPathManager::new(&options.data_dir)?);

        // 获取主 WAL 文件路径
        let wal_path = path_manager.main_wal_path();

        // 创建内存表
        let mut mem_table = MemTable::new(options.mem_table_size);

        // 如果启用 WAL 恢复，则尝试从 WAL 恢复
        if options.recover_from_wal {
            let _ = Self::replay(&mut mem_table, &wal_path);
        }

        // 创建 WAL 管理器
        let wal_manager = WalManager::new(wal_path).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to open WAL file: {}", e),
            )
        })?;

        // 创建 LSM 状态管理器
        let lsm_state = Arc::new(RwLock::new(LSMState::new(&options)));

        // 启动后台线程，用于处理 FlushTask
        let cloned_lsm_state = lsm_state.clone();
        let cloned_db_path = path_manager.clone();
        let (tx, rx) = mpsc::channel::<FlushTask>();
        let bg_thread_handle = thread::spawn(move || {
            Self::flush_loop(rx, cloned_lsm_state, cloned_db_path);
        });

        Ok(Self {
            path_manager,
            wal_manager: Arc::new(Mutex::new(wal_manager)),
            sequence_number: Arc::new(SequenceNumber::new()),
            lsm_state,
            options: Arc::new(options),
            sender: tx,
            _bg_thread_handle: bg_thread_handle,
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
    fn replay(mem_table: &mut MemTable, exec_path: &PathBuf) -> Result<(), std::io::Error> {
        let wal_iterator = WalIterator::new(exec_path)?;
        for entry in wal_iterator {
            match entry {
                Ok((key, value)) => {
                    mem_table.put(key, value.into());
                }
                Err(err) => {
                    println!("Failed to replay WAL entry: {}, skipped", err);
                }
            }
        }
        Ok(())
    }
}

impl KvEngine {
    /// 获取路径管理器引用
    pub fn path_manager(&self) -> &DbPathManager {
        &self.path_manager
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let lsm_state = self.lsm_state.read().unwrap();
        let mem_table = lsm_state.mem_table.clone();
        let immutable_mem_tables = lsm_state.immutable_mem_tables.clone();

        // First check memtable
        if let Some((internal_key, value)) = mem_table.get(key) {
            if internal_key.kind() != InternalKeyKind::Delete {
                return Some(value);
            } else {
                return None;
            }
        }

        // Then check immutable memtables in order (newer first)
        for table in immutable_mem_tables {
            if let Some((internal_key, value)) = table.get(key) {
                if internal_key.kind() != InternalKeyKind::Delete {
                    return Some(value);
                } else {
                    return None;
                }
            }
        }

        // todo sstable

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
        let mem_table = {
            let mut state = self.lsm_state.write().unwrap();

            // 克隆当前的 memtable
            let mem_table = state.mem_table.clone();

            // 创建 immutable_mem_table
            let immutable_mem_table = ImmutableMemTable::new(mem_table.inner());

            // 将 immutable_mem_table 放入队列并创建新的 memtable
            state
                .immutable_mem_tables
                .push_front(Arc::new(immutable_mem_table));
            state.mem_table = Arc::new(MemTable::new(self.options.mem_table_size));

            mem_table
        };

        // 发送 flush 任务到后台线程
        let task_id = self.flush_task_id.fetch_add(1, Ordering::SeqCst);
        if let Err(e) = self.sender.send(FlushTask {
            id: task_id,
            _mem_table: mem_table,
        }) {
            eprintln!("Failed to send flush task: {}", e);
        }
    }

    fn flush_loop(
        rx: mpsc::Receiver<FlushTask>,
        lsm_state: Arc<RwLock<LSMState>>,
        db_path_manager: Arc<DbPathManager>,
    ) {
        while let Ok(task) = rx.recv() {
            let mut sst_builder =
                match SSTableBuilder::new(task.id as u64, db_path_manager.data_dir().into()) {
                    Ok(builder) => builder,
                    Err(e) => {
                        eprintln!("Failed to create SSTableBuilder: {}", e);
                        continue;
                    }
                };

            // 获取 immutable_memtable 并克隆数据，避免长时间持有锁
            let entries: Vec<(Vec<u8>, Vec<u8>)> = {
                let lsm_state_guard = lsm_state.read().unwrap();
                let imm_table = match lsm_state_guard.immutable_mem_tables.front() {
                    Some(t) => t,
                    None => {
                        eprintln!("No immutable memtable found for task id={}", task.id);
                        continue;
                    }
                };

                imm_table
                    .iter()
                    .map(|(key, value)| {
                        let mut serialized_key = Vec::new();
                        serialized_key.extend_from_slice(key.user_key());
                        serialized_key
                            .extend_from_slice(&key.encoded_sequence_number().to_le_bytes());
                        (serialized_key, value.to_vec())
                    })
                    .collect()
            };

            // 在不持有锁的情况下写入 SSTable
            for (key, value) in entries {
                sst_builder.write(&key, &value);
            }

            sst_builder.finish();

            // 从 immutable_mem_tables 中移除已刷盘的 memtable
            let mut lsm_state_guard = lsm_state.write().unwrap();
            lsm_state_guard.immutable_mem_tables.pop_front();
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
        let engine = KvEngine::new_for_test();
        let path_manager = engine.path_manager();

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
