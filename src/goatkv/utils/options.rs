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

    /// Maximum operation count per WAL group commit
    /// Default: 4096
    pub wal_max_group_ops: usize,

    /// Maximum encoded bytes per WAL group commit
    /// Default: 2MB
    pub wal_max_group_bytes: usize,

    /// Micro-wait for WAL leader to collect a larger group
    /// Default: 20us
    pub wal_group_wait_us: u64,

    /// Maximum operation count per MemTable apply group
    /// Default: 4096
    pub mem_max_group_ops: usize,

    /// Maximum bytes per MemTable apply group
    /// Default: 2MB
    pub mem_max_group_bytes: usize,

    /// Micro-wait for MemTable leader to collect a larger group
    /// Default: 0us
    pub mem_group_wait_us: u64,

    /// Maximum number of requests allowed in WAL queue
    /// Default: 65536
    pub max_wal_queue_reqs: usize,

    /// Maximum bytes allowed in WAL queue
    /// Default: 256MB
    pub max_wal_queue_bytes: usize,

    /// Maximum number of requests allowed in MemTable queue
    /// Default: 65536
    pub max_mem_queue_reqs: usize,

    /// Maximum bytes allowed in MemTable queue
    /// Default: 256MB
    pub max_mem_queue_bytes: usize,

    /// Maximum immutable memtables allowed before write path enters fail-fast
    /// backpressure to avoid unbounded memory growth.
    /// Default: 8
    pub max_immutable_memtables: usize,

    /// Consecutive flush failures required to open flush circuit breaker.
    /// Default: 3
    pub flush_failure_streak_limit: usize,

    /// Maximum number of opened SSTable readers in table cache.
    /// Default: 64
    pub table_cache_capacity: usize,

    /// Maximum bytes for block cache (shared across SSTables).
    /// Default: 64MB
    pub block_cache_capacity_bytes: usize,

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

    /// L0 文件数达到该阈值后触发 L0 compaction
    /// Default: 4
    pub l0_compaction_file_trigger: usize,

    /// L1 基础目标大小（字节）
    /// Default: 64KB
    pub compaction_max_bytes_for_level_base: u64,

    /// 各层级目标大小倍数（L{n+1} = L{n} * multiplier）
    /// Default: 10
    pub compaction_max_bytes_for_level_multiplier: u64,

    /// grandparent overlap 限制因子
    /// Default: 10
    pub compaction_max_grandparent_overlap_bytes_factor: u64,

    /// L0 文件数达到该阈值时进入写入减速（slowdown）
    /// Default: 20
    pub l0_slowdown_writes_trigger: usize,

    /// L0 文件数达到该阈值时停止写入（stop）
    /// Default: 36
    pub l0_stop_writes_trigger: usize,

    /// Pending compaction bytes 软阈值（超过进入 slowdown）
    /// Default: 64MB
    pub soft_pending_compaction_bytes_limit: u64,

    /// Pending compaction bytes 硬阈值（超过 stop）
    /// Default: 256MB
    pub hard_pending_compaction_bytes_limit: u64,

    /// slowdown 时每轮等待时长（毫秒）
    /// Default: 1ms
    pub write_slowdown_delay_ms: u64,
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
            wal_max_group_ops: 4096,
            wal_max_group_bytes: 2 * 1024 * 1024,
            wal_group_wait_us: 20,
            mem_max_group_ops: 4096,
            mem_max_group_bytes: 2 * 1024 * 1024,
            mem_group_wait_us: 0,
            max_wal_queue_reqs: 65_536,
            max_wal_queue_bytes: 256 * 1024 * 1024,
            max_mem_queue_reqs: 65_536,
            max_mem_queue_bytes: 256 * 1024 * 1024,
            max_immutable_memtables: 8,
            flush_failure_streak_limit: 3,
            table_cache_capacity: 64,
            block_cache_capacity_bytes: 64 * 1024 * 1024,
            // VersionSet defaults
            max_versions: 10,
            manifest_max_size: 32 * 1024 * 1024, // 32MB
            manifest_rewrite_edit_count: 10000,
            num_levels: 7,
            l0_compaction_file_trigger: 4,
            compaction_max_bytes_for_level_base: 64 * 1024,
            compaction_max_bytes_for_level_multiplier: 10,
            compaction_max_grandparent_overlap_bytes_factor: 10,
            l0_slowdown_writes_trigger: 20,
            l0_stop_writes_trigger: 36,
            soft_pending_compaction_bytes_limit: 64 * 1024 * 1024,
            hard_pending_compaction_bytes_limit: 256 * 1024 * 1024,
            write_slowdown_delay_ms: 1,
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

    /// Sets maximum operation count per WAL group commit
    pub fn with_wal_max_group_ops(mut self, ops: usize) -> Self {
        self.wal_max_group_ops = ops;
        self
    }

    /// Sets maximum encoded bytes per WAL group commit
    pub fn with_wal_max_group_bytes(mut self, bytes: usize) -> Self {
        self.wal_max_group_bytes = bytes;
        self
    }

    /// Sets WAL leader micro-wait in microseconds
    pub fn with_wal_group_wait_us(mut self, wait_us: u64) -> Self {
        self.wal_group_wait_us = wait_us;
        self
    }

    /// Sets maximum operation count per MemTable apply group
    pub fn with_mem_max_group_ops(mut self, ops: usize) -> Self {
        self.mem_max_group_ops = ops;
        self
    }

    /// Sets maximum bytes per MemTable apply group
    pub fn with_mem_max_group_bytes(mut self, bytes: usize) -> Self {
        self.mem_max_group_bytes = bytes;
        self
    }

    /// Sets MemTable leader micro-wait in microseconds
    pub fn with_mem_group_wait_us(mut self, wait_us: u64) -> Self {
        self.mem_group_wait_us = wait_us;
        self
    }

    /// Sets WAL queue request limit
    pub fn with_max_wal_queue_reqs(mut self, reqs: usize) -> Self {
        self.max_wal_queue_reqs = reqs;
        self
    }

    /// Sets WAL queue byte limit
    pub fn with_max_wal_queue_bytes(mut self, bytes: usize) -> Self {
        self.max_wal_queue_bytes = bytes;
        self
    }

    /// Sets MemTable queue request limit
    pub fn with_max_mem_queue_reqs(mut self, reqs: usize) -> Self {
        self.max_mem_queue_reqs = reqs;
        self
    }

    /// Sets MemTable queue byte limit
    pub fn with_max_mem_queue_bytes(mut self, bytes: usize) -> Self {
        self.max_mem_queue_bytes = bytes;
        self
    }

    /// Sets immutable memtable backlog limit for write fail-fast backpressure.
    pub fn with_max_immutable_memtables(mut self, max_tables: usize) -> Self {
        self.max_immutable_memtables = max_tables;
        self
    }

    /// Sets consecutive flush failure threshold to open flush circuit breaker.
    pub fn with_flush_failure_streak_limit(mut self, limit: usize) -> Self {
        self.flush_failure_streak_limit = limit;
        self
    }

    /// Sets table cache capacity (entry count).
    pub fn with_table_cache_capacity(mut self, capacity: usize) -> Self {
        self.table_cache_capacity = capacity;
        self
    }

    /// Sets block cache capacity in bytes.
    pub fn with_block_cache_capacity_bytes(mut self, capacity_bytes: usize) -> Self {
        self.block_cache_capacity_bytes = capacity_bytes;
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

    /// Sets L0 compaction file trigger.
    pub fn with_l0_compaction_file_trigger(mut self, trigger: usize) -> Self {
        self.l0_compaction_file_trigger = trigger.max(1);
        self
    }

    /// Sets compaction base bytes for level targets.
    pub fn with_compaction_max_bytes_for_level_base(mut self, bytes: u64) -> Self {
        self.compaction_max_bytes_for_level_base = bytes.max(1);
        self
    }

    /// Sets compaction level size multiplier.
    pub fn with_compaction_max_bytes_for_level_multiplier(mut self, multiplier: u64) -> Self {
        self.compaction_max_bytes_for_level_multiplier = multiplier.max(2);
        self
    }

    /// Sets compaction grandparent overlap bytes factor.
    pub fn with_compaction_max_grandparent_overlap_bytes_factor(mut self, factor: u64) -> Self {
        self.compaction_max_grandparent_overlap_bytes_factor = factor.max(1);
        self
    }

    /// Sets L0 slowdown trigger for writes.
    pub fn with_l0_slowdown_writes_trigger(mut self, trigger: usize) -> Self {
        self.l0_slowdown_writes_trigger = trigger.max(1);
        self
    }

    /// Sets L0 stop trigger for writes.
    pub fn with_l0_stop_writes_trigger(mut self, trigger: usize) -> Self {
        self.l0_stop_writes_trigger = trigger.max(1);
        self
    }

    /// Sets soft pending compaction bytes limit.
    pub fn with_soft_pending_compaction_bytes_limit(mut self, bytes: u64) -> Self {
        self.soft_pending_compaction_bytes_limit = bytes.max(1);
        self
    }

    /// Sets hard pending compaction bytes limit.
    pub fn with_hard_pending_compaction_bytes_limit(mut self, bytes: u64) -> Self {
        self.hard_pending_compaction_bytes_limit = bytes.max(1);
        self
    }

    /// Sets write slowdown delay in milliseconds.
    pub fn with_write_slowdown_delay_ms(mut self, delay_ms: u64) -> Self {
        self.write_slowdown_delay_ms = delay_ms.max(1);
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
            wal_max_group_ops: 4096,
            wal_max_group_bytes: 2 * 1024 * 1024,
            wal_group_wait_us: 0,
            mem_max_group_ops: 4096,
            mem_max_group_bytes: 2 * 1024 * 1024,
            mem_group_wait_us: 0,
            max_wal_queue_reqs: 65_536,
            max_wal_queue_bytes: 256 * 1024 * 1024,
            max_mem_queue_reqs: 65_536,
            max_mem_queue_bytes: 256 * 1024 * 1024,
            max_immutable_memtables: 128,
            flush_failure_streak_limit: 3,
            table_cache_capacity: 64,
            block_cache_capacity_bytes: 64 * 1024 * 1024,
            // VersionSet defaults (use same defaults as production)
            max_versions: 10,
            manifest_max_size: 32 * 1024 * 1024, // 32MB
            manifest_rewrite_edit_count: 10000,
            num_levels: 7,
            l0_compaction_file_trigger: 4,
            compaction_max_bytes_for_level_base: 64 * 1024,
            compaction_max_bytes_for_level_multiplier: 10,
            compaction_max_grandparent_overlap_bytes_factor: 10,
            l0_slowdown_writes_trigger: 20,
            l0_stop_writes_trigger: 36,
            soft_pending_compaction_bytes_limit: 64 * 1024 * 1024,
            hard_pending_compaction_bytes_limit: 256 * 1024 * 1024,
            write_slowdown_delay_ms: 1,
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
    fn test_with_max_immutable_memtables() {
        let options = KvEngineOptions::default().with_max_immutable_memtables(16);
        assert_eq!(options.max_immutable_memtables, 16);
    }

    #[test]
    fn test_with_flush_failure_streak_limit() {
        let options = KvEngineOptions::default().with_flush_failure_streak_limit(5);
        assert_eq!(options.flush_failure_streak_limit, 5);
    }

    #[test]
    fn test_with_table_cache_capacity() {
        let options = KvEngineOptions::default().with_table_cache_capacity(128);
        assert_eq!(options.table_cache_capacity, 128);
    }

    #[test]
    fn test_with_block_cache_capacity_bytes() {
        let options = KvEngineOptions::default().with_block_cache_capacity_bytes(8 * 1024 * 1024);
        assert_eq!(options.block_cache_capacity_bytes, 8 * 1024 * 1024);
    }

    #[test]
    fn test_with_l0_compaction_file_trigger() {
        let options = KvEngineOptions::default().with_l0_compaction_file_trigger(8);
        assert_eq!(options.l0_compaction_file_trigger, 8);
    }

    #[test]
    fn test_with_compaction_level_targets() {
        let options = KvEngineOptions::default()
            .with_compaction_max_bytes_for_level_base(256 * 1024)
            .with_compaction_max_bytes_for_level_multiplier(12)
            .with_compaction_max_grandparent_overlap_bytes_factor(7);
        assert_eq!(options.compaction_max_bytes_for_level_base, 256 * 1024);
        assert_eq!(options.compaction_max_bytes_for_level_multiplier, 12);
        assert_eq!(options.compaction_max_grandparent_overlap_bytes_factor, 7);
    }

    #[test]
    fn test_with_write_stall_thresholds() {
        let options = KvEngineOptions::default()
            .with_l0_slowdown_writes_trigger(10)
            .with_l0_stop_writes_trigger(20)
            .with_soft_pending_compaction_bytes_limit(8 * 1024 * 1024)
            .with_hard_pending_compaction_bytes_limit(32 * 1024 * 1024)
            .with_write_slowdown_delay_ms(5);
        assert_eq!(options.l0_slowdown_writes_trigger, 10);
        assert_eq!(options.l0_stop_writes_trigger, 20);
        assert_eq!(options.soft_pending_compaction_bytes_limit, 8 * 1024 * 1024);
        assert_eq!(
            options.hard_pending_compaction_bytes_limit,
            32 * 1024 * 1024
        );
        assert_eq!(options.write_slowdown_delay_ms, 5);
    }

    #[test]
    fn test_with_l0_write_triggers_clamp_lower_bound() {
        let options = KvEngineOptions::default()
            .with_l0_slowdown_writes_trigger(0)
            .with_l0_stop_writes_trigger(0);
        assert_eq!(options.l0_slowdown_writes_trigger, 1);
        assert_eq!(options.l0_stop_writes_trigger, 1);
    }

    #[test]
    fn test_with_pending_compaction_limits_clamp_lower_bound() {
        let options = KvEngineOptions::default()
            .with_soft_pending_compaction_bytes_limit(0)
            .with_hard_pending_compaction_bytes_limit(0);
        assert_eq!(options.soft_pending_compaction_bytes_limit, 1);
        assert_eq!(options.hard_pending_compaction_bytes_limit, 1);
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
