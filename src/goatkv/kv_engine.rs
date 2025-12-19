use std::collections::VecDeque;
use std::env;
use std::path::PathBuf;
use std::time::SystemTime;

use crate::goatkv::immu_mem_table::ImmutableMemTable;
use crate::goatkv::internal_key::{InternalKey, InternalKeyKind};
use crate::goatkv::mem_table::{self, MemTable};
use crate::goatkv::sequence_number::SequenceNumber;
use crate::goatkv::wal_manager::{WalIterator, WalManager};

#[derive(Debug)]
pub struct KvEngine {
    wal_manager: WalManager,
    sequence_number: SequenceNumber,
    mem_table: MemTable,
    immutable_mem_tables: VecDeque<ImmutableMemTable>,
}

impl KvEngine {
    const DEFAULT_MEM_TABLE_SIZE: usize = 1024 * 1024; // 默认大小为1MB

    pub fn new() -> Self {
        // todo 暂时将启动路径定位wal日志的存放路径
        let mut exec_path = env::current_exe().unwrap();
        exec_path.pop();
        exec_path.push("wal.log");

        let mut mem_table = mem_table::MemTable::new(Self::DEFAULT_MEM_TABLE_SIZE);
        let _ = Self::replay(&mut mem_table, &exec_path);

        let wal_manager = WalManager::new(exec_path).expect("failed to open wal log file");
        Self {
            wal_manager,
            mem_table,
            immutable_mem_tables: VecDeque::new(),
            sequence_number: SequenceNumber::new(),
        }
    }

    /// 创建一个新的KvEngine，不尝试从WAL恢复
    /// 主要用于测试
    pub fn new_for_test() -> Self {
        let mut exec_path = env::current_exe().unwrap();
        exec_path.pop();

        // 使用时间戳生成唯一文件名，避免测试间的冲突
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let filename = format!("test_wal_{}.log", timestamp);
        exec_path.push(filename);

        let mem_table = mem_table::MemTable::new(Self::DEFAULT_MEM_TABLE_SIZE);
        let wal_manager = WalManager::new(exec_path).expect("failed to open wal log file");
        Self {
            wal_manager,
            mem_table,
            immutable_mem_tables: VecDeque::new(),
            sequence_number: SequenceNumber::new(),
        }
    }

    fn replay(mem_table: &mut MemTable, exec_path: &PathBuf) -> Result<(), std::io::Error> {
        let wal_iterator = WalIterator::new(exec_path)?;
        for entry in wal_iterator {
            match entry {
                Ok((key, value)) => {
                    mem_table.put(key, value);
                }
                Err(err) => {
                    println!("Failed to replay WAL entry: {}, skiped", err);
                }
            }
        }
        Ok(())
    }
}

impl KvEngine {
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
}
