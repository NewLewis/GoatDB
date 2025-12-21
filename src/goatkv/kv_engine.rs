use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use crate::goatkv::db_path_manager::DbPathManager;
use crate::goatkv::immu_mem_table::ImmutableMemTable;
use crate::goatkv::internal_key::{InternalKey, InternalKeyKind};
use crate::goatkv::mem_table::MemTable;
use crate::goatkv::options::KvEngineOptions;
use crate::goatkv::sequence_number::SequenceNumber;
use crate::goatkv::wal_manager::{WalIterator, WalManager};

/// LSM-Tree 键值存储引擎
#[derive(Debug)]
pub struct KvEngine {
    /// 路径管理器，统一管理所有数据库文件路径
    path_manager: DbPathManager,
    /// WAL 管理器，负责写前日志
    wal_manager: WalManager,
    /// 序列号生成器
    sequence_number: SequenceNumber,
    /// 内存表（可写）
    mem_table: MemTable,
    /// 不可变内存表队列（待刷盘）
    immutable_mem_tables: VecDeque<ImmutableMemTable>,
}

impl KvEngine {
    /// 创建新的 KvEngine，使用默认数据目录（当前目录下的 goatdb_data）
    pub fn new() -> Self {
        let options = KvEngineOptions::default();
        Self::new_with_options(options).expect("Failed to create KvEngine with default options")
    }

    /// 创建新的 KvEngine，使用指定的数据目录
    ///
    /// # 参数
    /// - `data_dir`: 数据目录路径，类似 PostgreSQL 的 pgdata
    ///
    /// # 返回
    /// - `Ok(KvEngine)`: 创建成功
    /// - `Err(std::io::Error)`: 创建目录或初始化失败
    pub fn new_with_data_dir<P: AsRef<Path>>(data_dir: P) -> Result<Self, std::io::Error> {
        let options = KvEngineOptions::default().with_data_dir(data_dir);
        Self::new_with_options(options)
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
        let path_manager = DbPathManager::new(&options.data_dir)?;

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

        Ok(Self {
            path_manager,
            wal_manager,
            mem_table,
            immutable_mem_tables: VecDeque::new(),
            sequence_number: SequenceNumber::new(),
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
                    mem_table.put(key, value);
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
        // Helper function to check a single table
        fn check_table<'a>(
            table: &'a dyn TableLookup,
            key: &[u8],
        ) -> Option<(Option<&'a InternalKey>, Option<&'a [u8]>)> {
            if let Some((internal_key, value)) = table.seek(key) {
                // Check if user key matches exactly
                if internal_key.user_key() == key {
                    Some((Some(internal_key), Some(value)))
                } else {
                    // seek returned a key with user_key > target
                    Some((None, None))
                }
            } else {
                // No key >= target found
                None
            }
        }

        // First check memtable
        if let Some((Some(internal_key), Some(value))) = check_table(&self.mem_table, key) {
            if internal_key.kind() != InternalKeyKind::Delete {
                return Some(value.to_vec());
            } else {
                // Latest version is a delete marker
                return None;
            }
        }

        // Then check immutable memtables in order (newer first)
        for table in &self.immutable_mem_tables {
            if let Some((Some(internal_key), Some(value))) = check_table(table, key) {
                if internal_key.kind() != InternalKeyKind::Delete {
                    return Some(value.to_vec());
                } else {
                    // Latest version is a delete marker
                    return None;
                }
            }
        }

        // Key not found
        None
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // 构造InternalKey
        let internal_key = InternalKey::new(key, self.sequence_number.next(), InternalKeyKind::Put);

        // 先写入wal
        self.wal_manager
            .write(&internal_key, &value)
            .expect("Failed to write to WAL");

        // 再写入memtable
        self.mem_table.put(internal_key, value.clone());

        // 判断memtable是否已达到容量限制，
        // 达到容量限制则转换成immutable_mem_tables
        if self.mem_table.should_flush() {
            self.flush();
        }
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        // 先构造InternalKey
        let internal_key =
            InternalKey::new(key, self.sequence_number.next(), InternalKeyKind::Delete);

        // 先写入wal
        self.wal_manager
            .write(&internal_key, &[] as &[u8])
            .expect("Failed to write to WAL");

        // 再写入memtable
        self.mem_table.put(internal_key, vec![]);

        // 判断memtable是否已达到容量限制，
        // 达到容量限制则转换成immutable_mem_tables
        if self.mem_table.should_flush() {
            self.flush();
        }
    }

    fn flush(&mut self) {
        // memtable中的跳表取出旧值，并放入新的空的跳表
        let old_skiplist = self.mem_table.replace_skiplist().unwrap();
        // 用旧的跳表初始化一个immutable_mem_table
        let immutable_mem_table = ImmutableMemTable::new(old_skiplist);
        // 将immutable_mem_table放入immutable_mem_tables中
        self.immutable_mem_tables.push_front(immutable_mem_table);
    }
}

// Trait for table lookup (memtable and immutable memtable)
trait TableLookup {
    fn seek(&self, key: &[u8]) -> Option<(&InternalKey, &[u8])>;
}

impl TableLookup for MemTable {
    fn seek(&self, key: &[u8]) -> Option<(&InternalKey, &[u8])> {
        self.seek(key)
    }
}

impl TableLookup for ImmutableMemTable {
    fn seek(&self, key: &[u8]) -> Option<(&InternalKey, &[u8])> {
        self.seek(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let mut engine = KvEngine::new_for_test();

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
        let mut engine = KvEngine::new_for_test();

        // Insert key
        engine.put(b"key1".to_vec(), b"value1".to_vec());
        assert_eq!(engine.get(b"key1"), Some(b"value1".to_vec()));

        // Update key
        engine.put(b"key1".to_vec(), b"newvalue".to_vec());
        assert_eq!(engine.get(b"key1"), Some(b"newvalue".to_vec()));
    }

    #[test]
    fn test_delete_key() {
        let mut engine = KvEngine::new_for_test();

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
        let mut engine = KvEngine::new_for_test();

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
        let mut engine = KvEngine::new_for_test();

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
