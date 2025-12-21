use std::env;
use std::path::{Path, PathBuf};

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
/// let test_options = KvEngineOptions::for_test();
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

    /// Creates options suitable for testing
    ///
    /// This creates a KvEngineOptions with a temporary data directory
    /// and disables WAL synchronization for faster tests.
    #[cfg(test)]
    pub fn for_test() -> Self {
        use std::fs;

        // Create a temporary directory for testing
        let temp_dir = env::temp_dir().join("goatdb_test");
        if !temp_dir.exists() {
            fs::create_dir_all(&temp_dir).expect("Failed to create test directory");
        }

        Self {
            data_dir: temp_dir,
            mem_table_size: 1024 * 1024, // 1MB
            recover_from_wal: false,     // Don't recover in tests
            wal_sync: false,             // Don't sync in tests for speed
        }
    }
}
