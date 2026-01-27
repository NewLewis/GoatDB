use std::env;
use std::path::{Path, PathBuf};

use crate::goatkv::storage::sstable::TableCacheOptions;

/// Configuration options for creating a KvEngine
///
/// # Examples
///
/// Basic usage with default values:
/// ```
/// use goat_db::goatkv::KvEngineOptions;
///
/// let options = KvEngineOptions::default();
/// ```
///
/// Custom configuration:
/// ```
/// use goat_db::goatkv::KvEngineOptions;
///
/// let options = KvEngineOptions::default()
///     .with_data_dir("/path/to/data")
///     .with_mem_table_size(2 * 1024 * 1024) // 2MB
///     .with_wal_sync(false);
/// ```
///
/// For testing:
/// ```
/// use goat_db::goatkv::KvEngineOptions;
///
/// ```
#[derive(Debug, Clone)]
pub struct KvEngineOptions {
    /// Data directory path where all database files are stored
    /// Default: current directory joined with "goatdb_data"
    pub data_dir: PathBuf,

    /// Maximum size of the mutable memtable in bytes
    /// Default: 1MB (1024 * 1024 bytes)
    pub mem_table_size: usize,

    /// Whether to attempt recovery from WAL on startup
    /// Default: true
    pub recover_from_wal: bool,

    /// Whether to synchronize WAL writes to disk
    /// Default: true (safer but slower)
    pub wal_sync: bool,

    // ===== VersionSet Options =====
    /// 保留的历史版本数量
    /// Default: 10
    pub max_versions: usize,

    /// MANIFEST 文件大小限制（超过则重写）
    /// Default: 32MB
    pub manifest_max_size: u64,

    /// 触发 MANIFEST 重写的版本编辑数量
    /// Default: 10000
    pub manifest_rewrite_edit_count: usize,

    /// LSM 层级数量
    /// Default: 7
    pub num_levels: usize,

    /// SSTable TableCache 配置
    /// Default: TableCacheOptions::default()
    pub table_cache: TableCacheOptions,
}

impl Default for KvEngineOptions {
    fn default() -> Self {
        let current_dir = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let default_data_dir = current_dir.join("goatdb_data");

        Self {
            data_dir: default_data_dir,
            mem_table_size: 1024 * 1024, // 1MB
            recover_from_wal: true,
            wal_sync: true,
            // VersionSet defaults
            max_versions: 10,
            manifest_max_size: 32 * 1024 * 1024, // 32MB
            manifest_rewrite_edit_count: 10000,
            num_levels: 7,
            table_cache: TableCacheOptions::default(),
        }
    }
}

impl KvEngineOptions {
    /// Creates a new options instance with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the data directory path
    pub fn with_data_dir<P: AsRef<Path>>(mut self, data_dir: P) -> Self {
        self.data_dir = data_dir.as_ref().to_path_buf();
        self
    }

    /// Sets the maximum memtable size in bytes
    pub fn with_mem_table_size(mut self, size: usize) -> Self {
        self.mem_table_size = size;
        self
    }

    /// Sets whether to attempt recovery from WAL on startup
    pub fn with_recover_from_wal(mut self, recover: bool) -> Self {
        self.recover_from_wal = recover;
        self
    }

    /// Sets whether to synchronize WAL writes to disk
    pub fn with_wal_sync(mut self, sync: bool) -> Self {
        self.wal_sync = sync;
        self
    }

    /// Sets the maximum number of versions to keep in history
    pub fn with_max_versions(mut self, max: usize) -> Self {
        self.max_versions = max;
        self
    }

    /// Sets the MANIFEST file size limit (after which it will be rewritten)
    pub fn with_manifest_max_size(mut self, size: u64) -> Self {
        self.manifest_max_size = size;
        self
    }

    /// Sets the number of VersionEdits before triggering MANIFEST rewrite
    pub fn with_manifest_rewrite_edit_count(mut self, count: usize) -> Self {
        self.manifest_rewrite_edit_count = count;
        self
    }

    /// Sets the number of LSM levels
    pub fn with_num_levels(mut self, levels: usize) -> Self {
        self.num_levels = levels;
        self
    }

    /// Sets the table cache options
    pub fn with_table_cache_options(mut self, table_cache: TableCacheOptions) -> Self {
        self.table_cache = table_cache;
        self
    }

    /// Sets the table cache max entries
    pub fn with_table_cache_max_entries(mut self, max_entries: usize) -> Self {
        self.table_cache = self.table_cache.with_max_entries(max_entries);
        self
    }

    /// Sets the table cache max charge (bytes)
    pub fn with_table_cache_max_charge(mut self, max_charge: usize) -> Self {
        self.table_cache = self.table_cache.with_max_charge(max_charge);
        self
    }

    /// Disables table cache max charge
    pub fn without_table_cache_max_charge(mut self) -> Self {
        self.table_cache = self.table_cache.without_max_charge();
        self
    }

    /// Sets the table cache shards
    pub fn with_table_cache_shards(mut self, shards: usize) -> Self {
        self.table_cache = self.table_cache.with_shards(shards);
        self
    }

    /// Sets promote-on-get for table cache
    pub fn with_table_cache_promote_on_get(mut self, promote_on_get: bool) -> Self {
        self.table_cache = self.table_cache.with_promote_on_get(promote_on_get);
        self
    }

    /// Sets the table cache hash capacity
    pub fn with_table_cache_hash_capacity(mut self, hash_capacity: usize) -> Self {
        self.table_cache = self.table_cache.with_hash_capacity(hash_capacity);
        self
    }

    /// Sets metrics options for table cache
    pub fn with_table_cache_metrics(
        mut self,
        metrics: crate::goatkv::utils::SharedLruMetricsOptions,
    ) -> Self {
        self.table_cache = self.table_cache.with_metrics(metrics);
        self
    }

    /// Sets whether to charge by file size for table cache
    pub fn with_table_cache_charge_by_file_size(mut self, charge_by_file_size: bool) -> Self {
        self.table_cache = self
            .table_cache
            .with_charge_by_file_size(charge_by_file_size);
        self
    }

    /// Creates options suitable for testing
    ///
    /// This creates a KvEngineOptions with a temporary data directory
    /// and disables WAL synchronization for faster tests.
    #[cfg(test)]
    pub fn for_test() -> Self {
        use std::fs;
        use std::time::{SystemTime, UNIX_EPOCH};

        // Create a temporary directory for testing
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos();
        let temp_dir =
            env::temp_dir().join(format!("goatdb_test_{}_{}", std::process::id(), nanos));
        if !temp_dir.exists() {
            fs::create_dir_all(&temp_dir).expect("Failed to create test directory");
        }

        Self {
            data_dir: temp_dir,
            mem_table_size: 1024 * 1024, // 1MB
            recover_from_wal: false,     // Don't recover in tests
            wal_sync: false,             // Don't sync in tests for speed
            // VersionSet defaults (use same defaults as production)
            max_versions: 10,
            manifest_max_size: 32 * 1024 * 1024, // 32MB
            manifest_rewrite_edit_count: 10000,
            num_levels: 7,
            table_cache: TableCacheOptions::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let options = KvEngineOptions::default();
        assert!(options.data_dir.ends_with("goatdb_data"));
        assert_eq!(options.mem_table_size, 1024 * 1024);
        assert!(options.recover_from_wal);
        assert!(options.wal_sync);
    }

    #[test]
    fn test_with_data_dir() {
        let options = KvEngineOptions::default().with_data_dir("/custom/path");
        assert_eq!(options.data_dir, PathBuf::from("/custom/path"));
        assert_eq!(options.mem_table_size, 1024 * 1024);
        assert!(options.recover_from_wal);
        assert!(options.wal_sync);
    }

    #[test]
    fn test_with_mem_table_size() {
        let options = KvEngineOptions::default().with_mem_table_size(2048 * 1024);
        assert_eq!(options.mem_table_size, 2048 * 1024);
    }

    #[test]
    fn test_with_recover_from_wal() {
        let options = KvEngineOptions::default().with_recover_from_wal(false);
        assert!(!options.recover_from_wal);
    }

    #[test]
    fn test_with_wal_sync() {
        let options = KvEngineOptions::default().with_wal_sync(false);
        assert!(!options.wal_sync);
    }

    #[test]
    fn test_new() {
        let options = KvEngineOptions::new();
        assert!(options.data_dir.ends_with("goatdb_data"));
        assert_eq!(options.mem_table_size, 1024 * 1024);
        assert!(options.recover_from_wal);
        assert!(options.wal_sync);
    }

    #[test]
    fn test_for_test() {
        let options = KvEngineOptions::for_test();
        let dir_name = options
            .data_dir
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        assert!(dir_name.starts_with("goatdb_test_"));
        assert_eq!(options.mem_table_size, 1024 * 1024);
        assert!(!options.recover_from_wal);
        assert!(!options.wal_sync);
    }
}
