use std::hash::Hasher;
use std::path::Path;
use std::sync::Arc;

use crate::goatkv::core::sequence_number::SequenceNumber;
use crate::goatkv::core::shard::Shard;
use crate::goatkv::storage::wal::WalPaths;
use crate::goatkv::utils::db_meta::{ensure_db_meta, HASH_SEED};
use crate::goatkv::utils::options::KvEngineOptions;
use crate::goatkv::utils::paths::{ManifestPaths, SstablePaths};
use twox_hash::XxHash64;

type DbPaths = (Arc<WalPaths>, Arc<SstablePaths>, Arc<ManifestPaths>);

/// KvEngine 架构的 LSM-Tree 键值存储引擎（按 key 哈希分片）
#[derive(Debug)]
pub struct KvEngine {
    shards: Vec<Arc<Shard>>,
    shard_count: usize,
}

impl Default for KvEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl KvEngine {
    pub fn init_db_paths<P: AsRef<Path>>(base_dir: P) -> Result<DbPaths, std::io::Error> {
        Shard::init_db_paths_with_shard(base_dir, Some("shard0"))
    }

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
        if options.shard_count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "shard_count must be greater than 0",
            ));
        }

        let per_shard_mem_table_size = match options.mem_table_budget {
            Some(budget) => {
                if budget < options.shard_count {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "mem_table_budget too small for shard_count",
                    ));
                }
                budget / options.shard_count
            }
            None => options.mem_table_size,
        };

        ensure_db_meta(&options.data_dir, options.shard_count)?;

        let mut shards = Vec::with_capacity(options.shard_count);
        let sequence_number = Arc::new(SequenceNumber::with_start(1));
        for shard_index in 0..options.shard_count {
            let mut shard_options = options.clone();
            shard_options.shard_count = 1;
            shard_options.mem_table_size = per_shard_mem_table_size;
            let shard_name = format!("shard{}", shard_index);
            let shard = Shard::new_with_options_and_shard(
                shard_options,
                Some(&shard_name),
                Some(sequence_number.clone()),
            )?;
            shards.push(Arc::new(shard));
        }

        Ok(Self {
            shards,
            shard_count: options.shard_count,
        })
    }

    /// 创建一个新的 KvEngine，不尝试从 WAL 恢复
    /// 主要用于测试
    #[cfg(test)]
    pub fn new_for_test() -> Self {
        let options = KvEngineOptions::for_test();
        Self::new_with_options(options).expect("Failed to create test KvEngine")
    }

    fn shard_index(&self, key: &[u8]) -> usize {
        let mut hasher = XxHash64::with_seed(HASH_SEED);
        hasher.write(key);
        (hasher.finish() as usize) % self.shard_count
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let shard_index = self.shard_index(key);
        self.shards[shard_index].get(key)
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        let shard_index = self.shard_index(&key);
        self.shards[shard_index].put(key, value);
    }

    pub fn delete(&self, key: Vec<u8>) {
        let shard_index = self.shard_index(&key);
        self.shards[shard_index].delete(key);
    }

    pub fn flush(&self) {
        for shard in &self.shards {
            shard.flush();
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
    fn test_empty_flush_is_noop() {
        let engine = KvEngine::new_for_test();

        engine.flush();

        let shard = &engine.shards[0];
        assert!(shard.is_mem_state_empty());
        assert_eq!(shard.level_file_count(0), 0);
    }

    #[test]
    fn test_mem_table_budget_distributed() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let options = KvEngineOptions::default()
            .with_data_dir(tmp.path())
            .with_shard_count(4)
            .with_mem_table_budget(4 * 64 * 1024)
            .with_wal_sync(false)
            .with_recover_from_wal(false);

        let engine = KvEngine::new_with_options(options).expect("create engine");
        for shard in &engine.shards {
            assert_eq!(shard.mem_table_size(), 64 * 1024);
        }
    }

    #[test]
    fn test_mem_table_budget_too_small() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let options = KvEngineOptions::default()
            .with_data_dir(tmp.path())
            .with_shard_count(4)
            .with_mem_table_budget(3)
            .with_wal_sync(false)
            .with_recover_from_wal(false);

        let err = KvEngine::new_with_options(options).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert!(err
            .to_string()
            .contains("mem_table_budget too small"));
    }

    #[test]
    fn test_mem_table_budget_overrides_mem_table_size() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let options = KvEngineOptions::default()
            .with_data_dir(tmp.path())
            .with_shard_count(2)
            .with_mem_table_size(128 * 1024)
            .with_mem_table_budget(3 * 1024)
            .with_wal_sync(false)
            .with_recover_from_wal(false);

        let engine = KvEngine::new_with_options(options).expect("create engine");
        for shard in &engine.shards {
            assert_eq!(shard.mem_table_size(), 1536);
        }
    }

    #[test]
    fn test_mem_table_budget_non_divisible() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let options = KvEngineOptions::default()
            .with_data_dir(tmp.path())
            .with_shard_count(3)
            .with_mem_table_budget(1000)
            .with_wal_sync(false)
            .with_recover_from_wal(false);

        let engine = KvEngine::new_with_options(options).expect("create engine");
        for shard in &engine.shards {
            assert_eq!(shard.mem_table_size(), 333);
        }
    }

    #[test]
    fn test_mem_table_budget_equals_shard_count() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let options = KvEngineOptions::default()
            .with_data_dir(tmp.path())
            .with_shard_count(4)
            .with_mem_table_budget(4)
            .with_wal_sync(false)
            .with_recover_from_wal(false);

        let engine = KvEngine::new_with_options(options).expect("create engine");
        for shard in &engine.shards {
            assert_eq!(shard.mem_table_size(), 1);
        }
    }

    #[test]
    fn test_mem_table_budget_zero_is_invalid() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let options = KvEngineOptions::default()
            .with_data_dir(tmp.path())
            .with_shard_count(1)
            .with_mem_table_budget(0)
            .with_wal_sync(false)
            .with_recover_from_wal(false);

        let err = KvEngine::new_with_options(options).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert!(err
            .to_string()
            .contains("mem_table_budget too small"));
    }

    #[test]
    fn test_mem_table_size_used_when_budget_missing() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let options = KvEngineOptions::default()
            .with_data_dir(tmp.path())
            .with_shard_count(2)
            .with_mem_table_size(96 * 1024)
            .with_wal_sync(false)
            .with_recover_from_wal(false);

        let engine = KvEngine::new_with_options(options).expect("create engine");
        for shard in &engine.shards {
            assert_eq!(shard.mem_table_size(), 96 * 1024);
        }
    }
}
