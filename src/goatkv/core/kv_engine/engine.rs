use std::cmp::max;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;

use tracing::{error, warn};

use super::reader::KvReader;
use super::writer::{KvWriter, WriteOp, WriterQueueMetrics};
use crate::goatkv::core::cleanup_worker::CleanupWorker;
use crate::goatkv::core::flush_worker::{CompactionConfig, FlushTask, FlushWorker};
use crate::goatkv::core::lsm_state::{ImmutableMemTableEntry, LSMState};
use crate::goatkv::core::mem_table::{ImmutableMemTable, MemTable};
use crate::goatkv::core::sequence_number::SequenceNumber;
use crate::goatkv::core::snapshot_manager::{SnapshotHandle, SnapshotManager};
use crate::goatkv::error::{Error as GoatError, Result as GoatResult};
use crate::goatkv::format::internal_key::SEQUENCE_NUMBER_MAX;
use crate::goatkv::metadata::version::Version;
use crate::goatkv::metadata::version_set::{VersionSet, VersionSetOptions};
use crate::goatkv::storage::sstable::ReadCacheMetrics;
use crate::goatkv::storage::wal::{
    replay_wal_file, WalHandle, WalPaths, WalReplayStats, WalWriter, WalWriterConfig,
};
use crate::goatkv::utils::cleanup_task::CleanupTask;
use crate::goatkv::utils::options::KvEngineOptions;
use crate::goatkv::utils::paths::{ManifestPaths, SstablePaths};

type DbPaths = (Arc<WalPaths>, Arc<SstablePaths>, Arc<ManifestPaths>);
const SHUTDOWN_FLUSH_WAIT_TIMEOUT_MS: u64 = 30_000;
const SHUTDOWN_FLUSH_WAIT_INTERVAL_MS: u64 = 10;

#[derive(Debug, Clone, Copy, Default)]
pub struct EngineRuntimeMetrics {
    pub immutable_memtable_backlog: usize,
    pub flush_failure_streak: usize,
    pub flush_circuit_open: bool,
    pub l0_file_count: usize,
    pub pending_compaction_bytes: u64,
    pub writer_queue_metrics: WriterQueueMetrics,
    pub write_pressure_level: u8,
    pub read_cache_metrics: Option<ReadCacheMetrics>,
}

struct BuildEngineInput {
    wal_writer: WalWriter,
    sequence_number: Arc<SequenceNumber>,
    lsm_state: Arc<RwLock<LSMState>>,
    version_set: Arc<RwLock<VersionSet>>,
    cleanup_sender: UnboundedSender<CleanupTask>,
    cleanup_enabled: Arc<AtomicBool>,
    options: KvEngineOptions,
    wal_paths: Arc<WalPaths>,
    sstable_paths: Arc<SstablePaths>,
    manifest_paths: Arc<ManifestPaths>,
    cleanup_worker: CleanupWorker,
    current_log_number: u64,
}

/// LSM-Tree 键值存储引擎
#[derive(Debug)]
pub struct KvEngine {
    /// WAL 写入器，负责写前日志
    wal_writer: Arc<WalWriter>,
    /// 清理任务发送端
    cleanup_sender: UnboundedSender<CleanupTask>,
    /// 是否允许删除 WAL（关闭时禁用）
    cleanup_enabled: Arc<AtomicBool>,
    /// LSM 状态管理器（memtables + current version）
    lsm_state: Arc<RwLock<LSMState>>,
    /// 写入与 flush 的全局门闩，确保 WAL 与 memtable 边界一致
    write_gate: Arc<RwLock<()>>,
    /// VersionSet 管理 manifest 与版本演进
    version_set: Arc<RwLock<VersionSet>>,
    /// 配置选项
    options: Arc<KvEngineOptions>,
    /// WAL 路径集合
    wal_paths: Arc<WalPaths>,
    /// SSTable 路径集合
    sstable_paths: Arc<SstablePaths>,
    /// MANIFEST/CURRENT 路径集合
    manifest_paths: Arc<ManifestPaths>,
    /// 后台刷盘 Worker
    flush_worker: FlushWorker,
    /// 活跃快照管理器
    snapshot_manager: Arc<RwLock<SnapshotManager>>,
    /// 读取协调器
    reader: KvReader,
    /// 写入协调器
    writer: KvWriter,
    /// 后台清理 Worker
    #[allow(dead_code)]
    cleanup_worker: CleanupWorker,
    /// 当前 WAL 日志编号
    current_log_number: AtomicU64,
}

impl Default for KvEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl KvEngine {
    pub fn init_db_paths<P: AsRef<Path>>(base_dir: P) -> GoatResult<DbPaths> {
        let base_dir = base_dir.as_ref().to_path_buf();
        let data_dir = base_dir.join("data");
        let wal_dir = base_dir.join("wal");
        let log_dir = base_dir.join("log");
        let tmp_dir = base_dir.join("tmp");

        let dirs = [&base_dir, &data_dir, &wal_dir, &log_dir, &tmp_dir];
        for dir in dirs {
            if !dir.exists() {
                fs::create_dir_all(dir).map_err(|e| GoatError::io("init_db_paths_mkdir", e))?;
            }
        }

        let wal_paths = Arc::new(WalPaths::new(wal_dir));
        let sstable_paths = Arc::new(SstablePaths::new(data_dir.clone(), tmp_dir));
        let manifest_paths = Arc::new(ManifestPaths::new(base_dir, data_dir));

        Ok((wal_paths, sstable_paths, manifest_paths))
    }

    /// 创建新的 KvEngine，使用默认数据目录（当前目录下的 goatdb_data）
    pub fn new() -> Self {
        let options = KvEngineOptions::default();
        Self::new_with_options(options).expect("Failed to create KvEngine with default options")
    }

    /// 创建新的 KvEngine，使用指定的配置选项
    pub fn new_with_options(options: KvEngineOptions) -> GoatResult<Self> {
        let (wal_paths, sstable_paths, manifest_paths) = Self::prepare_paths(&options)?;

        let (cleanup_worker, cleanup_sender, cleanup_enabled) =
            Self::init_cleanup(wal_paths.clone(), sstable_paths.clone())?;

        let mem_table = Arc::new(MemTable::new(options.mem_table_size));
        let (version_set, current_version) = Self::open_version_set(
            manifest_paths.clone(),
            sstable_paths.clone(),
            &options,
            cleanup_sender.clone(),
        )?;
        let lsm_state = Arc::new(RwLock::new(LSMState::new(
            mem_table.clone(),
            current_version,
        )));

        let min_log_number = version_set.read().unwrap().log_number();
        let (wal_stats, wal_max_number) = Self::recover_from_wal_if_needed(
            &options,
            &wal_paths,
            &lsm_state,
            min_log_number,
            &cleanup_sender,
            &cleanup_enabled,
        )?;
        Self::cleanup_obsolete_wals(&wal_paths, min_log_number);
        if wal_stats.truncated {
            warn!("WAL replay truncated due to corruption or partial record.");
        }

        let current_log_number = Self::select_start_log_number(&version_set, wal_max_number);
        let wal_writer = Self::open_wal_writer(&options, &wal_paths, current_log_number)?;
        let sequence_number = Self::init_sequence_number(&version_set, wal_stats.max_sequence)?;

        let engine = Self::build_engine(BuildEngineInput {
            wal_writer,
            sequence_number,
            lsm_state,
            version_set,
            cleanup_sender,
            cleanup_enabled,
            options,
            wal_paths,
            sstable_paths,
            manifest_paths,
            cleanup_worker,
            current_log_number,
        });

        engine.submit_recovery_flushes();
        Ok(engine)
    }

    /// 创建一个新的 KvEngine，不尝试从 WAL 恢复（测试用）
    #[cfg(test)]
    pub fn new_for_test() -> Self {
        let options = KvEngineOptions::for_test();
        Self::new_with_options(options).expect("Failed to create test KvEngine")
    }

    /// 获取 WAL 路径集合
    pub fn wal_paths(&self) -> &WalPaths {
        &self.wal_paths
    }

    /// 获取 SSTable 路径集合
    pub fn sstable_paths(&self) -> &SstablePaths {
        &self.sstable_paths
    }

    /// 获取 MANIFEST/CURRENT 路径集合
    pub fn manifest_paths(&self) -> &ManifestPaths {
        &self.manifest_paths
    }

    pub fn get(&self, key: &[u8]) -> GoatResult<Option<Vec<u8>>> {
        self.reader.get(key)
    }

    pub fn multi_get(&self, keys: &[Vec<u8>]) -> GoatResult<Vec<Option<Vec<u8>>>> {
        self.reader.multi_get(keys)
    }

    pub fn create_snapshot(&self) -> GoatResult<SnapshotHandle> {
        let seq = self.writer.last_published_sequence();
        let mut manager = self.snapshot_manager.write().unwrap();
        Ok(manager.create(seq))
    }

    pub fn release_snapshot(&self, snapshot_id: u64) -> GoatResult<()> {
        let mut manager = self.snapshot_manager.write().unwrap();
        if manager.release(snapshot_id) {
            Ok(())
        } else {
            Err(GoatError::not_found(
                "snapshot",
                format!("snapshot {} not found", snapshot_id),
            ))
        }
    }

    pub fn get_with_snapshot(&self, key: &[u8], snapshot_id: u64) -> GoatResult<Option<Vec<u8>>> {
        let seq = {
            let manager = self.snapshot_manager.read().unwrap();
            manager.lookup_sequence(snapshot_id).ok_or_else(|| {
                GoatError::not_found("snapshot", format!("snapshot {} not found", snapshot_id))
            })?
        };
        self.reader.get_at_seq(key, seq)
    }

    pub fn read_cache_metrics(&self) -> Option<ReadCacheMetrics> {
        let lsm_state = self.lsm_state.read().unwrap();
        lsm_state.version.read_cache_metrics()
    }

    pub fn runtime_metrics(&self) -> EngineRuntimeMetrics {
        let (immutable_memtable_backlog, flush_failure_streak, flush_circuit_open, l0_file_count) = {
            let lsm_state = self.lsm_state.read().unwrap();
            (
                lsm_state.immutable_mem_tables.len(),
                lsm_state.flush_failure_streak,
                lsm_state.flush_circuit_open,
                lsm_state.version.get_files(0).len(),
            )
        };
        let pending_compaction_bytes = self.estimated_pending_compaction_bytes();
        EngineRuntimeMetrics {
            immutable_memtable_backlog,
            flush_failure_streak,
            flush_circuit_open,
            l0_file_count,
            pending_compaction_bytes,
            writer_queue_metrics: self.writer.queue_metrics(),
            write_pressure_level: self.writer.write_pressure_level_code(),
            read_cache_metrics: self.read_cache_metrics(),
        }
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> GoatResult<()> {
        self.submit_write(vec![WriteOp::Put(key, value)])
    }

    pub fn put_batch(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> GoatResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let ops = entries
            .into_iter()
            .map(|(key, value)| WriteOp::Put(key, value))
            .collect();
        self.submit_write(ops)
    }

    pub fn delete(&self, key: Vec<u8>) -> GoatResult<()> {
        self.submit_write(vec![WriteOp::Delete(key)])
    }

    pub fn flush(&self) {
        let _barrier = self.writer.enter_flush_barrier();
        {
            let _gate = self.write_gate.write().unwrap();
            self.flush_inner();
        }
    }

    /// 优雅停机：
    /// 1) 关闭写入入口，拒绝新写请求；
    /// 2) 在写入门闩保护下封存当前 memtable 并触发 flush；
    /// 3) 等待 immutable 队列清空（后台 flush 完成）。
    pub fn shutdown(&self) -> GoatResult<()> {
        self.writer.close();
        self.flush();
        if let Err(e) =
            self.wait_for_immutable_memtables(Duration::from_millis(SHUTDOWN_FLUSH_WAIT_TIMEOUT_MS))
        {
            self.cleanup_enabled.store(false, Ordering::SeqCst);
            return Err(e);
        }
        self.cleanup_enabled.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn flush_inner(&self) {
        let candidate_log_number = self.next_log_number_candidate();
        let old_log_number = self.current_log_number.load(Ordering::SeqCst);
        let (new_log_number, rotation_succeeded) =
            self.rotate_wal(candidate_log_number, old_log_number);
        let wal_handle = self.wal_handle_for_flush(rotation_succeeded, old_log_number);

        let immutable_mem_table = self.seal_memtable(wal_handle);
        let Some(immutable_mem_table) = immutable_mem_table else {
            return;
        };

        self.submit_flush_task(immutable_mem_table, new_log_number, rotation_succeeded);
    }

    fn wait_for_immutable_memtables(&self, timeout: Duration) -> GoatResult<()> {
        let start = Instant::now();
        loop {
            let pending = self.lsm_state.read().unwrap().immutable_mem_tables.len();
            if pending == 0 {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                return Err(GoatError::unavailable(
                    "engine_shutdown",
                    format!(
                        "timeout waiting for flush completion, pending immutable memtables={}",
                        pending
                    ),
                ));
            }
            thread::sleep(Duration::from_millis(SHUTDOWN_FLUSH_WAIT_INTERVAL_MS));
        }
    }
}

impl Drop for KvEngine {
    fn drop(&mut self) {
        // Shutdown: avoid deleting WAL files when in-memory state may not be flushed.
        self.cleanup_enabled.store(false, Ordering::SeqCst);
    }
}

impl KvEngine {
    fn prepare_paths(options: &KvEngineOptions) -> GoatResult<DbPaths> {
        let (wal_paths, sstable_paths, manifest_paths) = Self::init_db_paths(&options.data_dir)?;
        let _ = sstable_paths.cleanup_tmp_dir();
        Ok((wal_paths, sstable_paths, manifest_paths))
    }

    fn init_cleanup(
        wal_paths: Arc<WalPaths>,
        sstable_paths: Arc<SstablePaths>,
    ) -> GoatResult<(CleanupWorker, UnboundedSender<CleanupTask>, Arc<AtomicBool>)> {
        let (cleanup_worker, cleanup_sender) = CleanupWorker::new(wal_paths, sstable_paths)?;
        let cleanup_enabled = Arc::new(AtomicBool::new(true));
        Ok((cleanup_worker, cleanup_sender, cleanup_enabled))
    }

    fn open_version_set(
        manifest_paths: Arc<ManifestPaths>,
        sstable_paths: Arc<SstablePaths>,
        options: &KvEngineOptions,
        cleanup_sender: UnboundedSender<CleanupTask>,
    ) -> GoatResult<(Arc<RwLock<VersionSet>>, Arc<Version>)> {
        let vs_options = VersionSetOptions::from(options);
        let version_set =
            VersionSet::open(manifest_paths, sstable_paths, vs_options, cleanup_sender)?;
        let version_set = Arc::new(RwLock::new(version_set));
        let current_version = version_set.read().unwrap().current();
        Ok((version_set, current_version))
    }

    fn recover_from_wal_if_needed(
        options: &KvEngineOptions,
        wal_paths: &WalPaths,
        lsm_state: &Arc<RwLock<LSMState>>,
        min_log_number: u64,
        cleanup_sender: &UnboundedSender<CleanupTask>,
        cleanup_enabled: &Arc<AtomicBool>,
    ) -> GoatResult<(WalReplayStats, u64)> {
        if !options.recover_from_wal {
            return Ok((
                WalReplayStats {
                    max_sequence: 0,
                    entries: 0,
                    truncated: false,
                },
                0,
            ));
        }
        Self::replay_into_state(
            wal_paths,
            lsm_state,
            options.mem_table_size,
            min_log_number,
            cleanup_sender,
            cleanup_enabled,
        )
    }

    fn select_start_log_number(version_set: &Arc<RwLock<VersionSet>>, wal_max: u64) -> u64 {
        let vs_guard = version_set.read().unwrap();
        let mut log_number = vs_guard.log_number();
        if log_number == 0 {
            log_number = 1;
        }
        if wal_max >= log_number {
            log_number = wal_max + 1;
        }
        log_number
    }

    fn open_wal_writer(
        options: &KvEngineOptions,
        wal_paths: &WalPaths,
        log_number: u64,
    ) -> GoatResult<WalWriter> {
        let wal_path = wal_paths.wal_path_by_id(log_number);
        WalWriter::new(
            wal_path,
            WalWriterConfig {
                wal_sync: options.wal_sync,
                wal_preallocate_bytes: options.wal_preallocate_bytes,
                wal_bytes_per_sync: options.wal_bytes_per_sync,
            },
        )
        .map_err(|e| {
            GoatError::internal_with_source("open_wal_writer", "failed to open wal writer", e)
        })
    }

    fn init_sequence_number(
        version_set: &Arc<RwLock<VersionSet>>,
        wal_max_sequence: u64,
    ) -> GoatResult<Arc<SequenceNumber>> {
        let vs_guard = version_set.read().unwrap();
        let last_sequence = max(wal_max_sequence, vs_guard.last_sequence());
        if last_sequence >= SEQUENCE_NUMBER_MAX {
            return Err(GoatError::unavailable(
                "sequence_number",
                format!("sequence number exhausted at {}", last_sequence),
            ));
        }
        Ok(Arc::new(SequenceNumber::with_start(
            last_sequence.saturating_add(1),
        )))
    }

    fn build_engine(input: BuildEngineInput) -> Self {
        let BuildEngineInput {
            wal_writer,
            sequence_number,
            lsm_state,
            version_set,
            cleanup_sender,
            cleanup_enabled,
            options,
            wal_paths,
            sstable_paths,
            manifest_paths,
            cleanup_worker,
            current_log_number,
        } = input;
        let snapshot_manager = Arc::new(RwLock::new(SnapshotManager::new()));
        let flush_worker = FlushWorker::new(
            lsm_state.clone(),
            version_set.clone(),
            sstable_paths.clone(),
            snapshot_manager.clone(),
            options.flush_failure_streak_limit,
            CompactionConfig {
                l0_compaction_file_trigger: options.l0_compaction_file_trigger,
                max_bytes_for_level_base: options.compaction_max_bytes_for_level_base,
                max_bytes_for_level_multiplier: options.compaction_max_bytes_for_level_multiplier,
                max_grandparent_overlap_bytes_factor: options
                    .compaction_max_grandparent_overlap_bytes_factor,
            },
            options.bloom_prefix_extractor_len,
        );
        let wal_writer = Arc::new(wal_writer);
        let options = Arc::new(options);
        let write_gate = Arc::new(RwLock::new(()));
        let initial_published_seq = sequence_number.current().saturating_sub(1);
        let writer = KvWriter::new(
            wal_writer.clone(),
            sequence_number,
            lsm_state.clone(),
            write_gate.clone(),
            options.clone(),
            initial_published_seq,
        );
        let reader = KvReader::new(lsm_state.clone());
        Self {
            wal_writer,
            lsm_state: lsm_state.clone(),
            version_set: version_set.clone(),
            cleanup_sender: cleanup_sender.clone(),
            cleanup_enabled: cleanup_enabled.clone(),
            write_gate,
            options,
            flush_worker,
            snapshot_manager,
            reader,
            writer,
            cleanup_worker,
            current_log_number: AtomicU64::new(current_log_number),
            wal_paths,
            sstable_paths,
            manifest_paths,
        }
    }

    fn submit_recovery_flushes(&self) {
        let state = self.lsm_state.read().unwrap();
        for entry in state.immutable_mem_tables.iter() {
            let _ = self.flush_worker.submit_task(FlushTask {
                immutable_mem_table: entry.table.clone(),
                new_log_number: 0,
            });
        }
    }

    // Read path is handled by kv_engine::reader

    fn next_log_number_candidate(&self) -> u64 {
        let vs = self.version_set.read().unwrap();
        let vs_next = vs.log_number().saturating_add(1);
        let current_next = self
            .current_log_number
            .load(Ordering::SeqCst)
            .saturating_add(1);
        max(vs_next, current_next)
    }

    fn rotate_wal(&self, candidate: u64, old_log_number: u64) -> (u64, bool) {
        let new_wal_path = self.wal_paths.wal_path_by_id(candidate);
        match self.wal_writer.rotate(new_wal_path) {
            Ok(()) => {
                self.current_log_number.store(candidate, Ordering::SeqCst);
                (candidate, true)
            }
            Err(e) => {
                error!("Failed to rotate WAL: {}", e);
                (old_log_number, false)
            }
        }
    }

    fn wal_handle_for_flush(
        &self,
        rotation_succeeded: bool,
        old_log_number: u64,
    ) -> Option<Arc<WalHandle>> {
        if !rotation_succeeded || old_log_number == 0 {
            return None;
        }
        Some(Arc::new(WalHandle::new(
            old_log_number,
            self.cleanup_sender.clone(),
            self.cleanup_enabled.clone(),
        )))
    }

    fn seal_memtable(&self, wal_handle: Option<Arc<WalHandle>>) -> Option<Arc<ImmutableMemTable>> {
        let mut state = self.lsm_state.write().unwrap();
        let mem_table = state.mem_table.clone();
        if mem_table.is_empty() {
            return None;
        }

        let immutable_mem_table = Arc::new(ImmutableMemTable::new(mem_table.inner()));
        state
            .immutable_mem_tables
            .push_back(ImmutableMemTableEntry {
                table: immutable_mem_table.clone(),
                wal_handle,
            });
        state.mem_table = Arc::new(MemTable::new(self.options.mem_table_size));
        Some(immutable_mem_table)
    }

    fn submit_flush_task(
        &self,
        immutable_mem_table: Arc<ImmutableMemTable>,
        new_log_number: u64,
        rotation_succeeded: bool,
    ) {
        let log_number = if rotation_succeeded {
            new_log_number
        } else {
            0
        };
        if let Err(e) = self.flush_worker.submit_task(FlushTask {
            immutable_mem_table,
            new_log_number: log_number,
        }) {
            error!("Failed to send flush task: {}", e);
        }
    }
}

impl KvEngine {
    fn submit_write(&self, ops: Vec<WriteOp>) -> GoatResult<()> {
        self.writer.submit_write(ops, || self.flush())
    }

    fn estimated_pending_compaction_bytes(&self) -> u64 {
        let lsm_state = self.lsm_state.read().unwrap();
        let version = &lsm_state.version;
        let num_levels = version.num_levels();
        if num_levels <= 1 {
            return 0;
        }

        let base = self.options.compaction_max_bytes_for_level_base.max(1);
        let multiplier = self
            .options
            .compaction_max_bytes_for_level_multiplier
            .max(2);
        let mut level_target = base;
        let mut pending = 0u64;
        for level in 1..num_levels {
            let level_size = version.get_level_size(level);
            if level_size > level_target {
                pending = pending.saturating_add(level_size - level_target);
            }
            level_target = level_target.saturating_mul(multiplier);
        }

        if version.get_files(0).len() > self.options.l0_compaction_file_trigger.max(1) {
            pending = pending.saturating_add(version.get_level_size(0));
        }

        pending
    }
}

impl KvEngine {
    /// 从 WAL 文件恢复数据到内存表
    fn replay_into_state(
        wal_paths: &WalPaths,
        lsm_state: &Arc<RwLock<LSMState>>,
        mem_table_size: usize,
        min_log_number: u64,
        cleanup_sender: &UnboundedSender<CleanupTask>,
        cleanup_enabled: &Arc<AtomicBool>,
    ) -> GoatResult<(WalReplayStats, u64)> {
        let wal_files = Self::list_wal_files(wal_paths, min_log_number)?;
        let mut stats = WalReplayStats {
            max_sequence: 0,
            entries: 0,
            truncated: false,
        };
        let mut max_log_number = 0u64;

        for (log_number, wal_path) in wal_files {
            max_log_number = max_log_number.max(log_number);
            if !wal_path.exists() {
                continue;
            }
            let mut wal_handle: Option<Arc<WalHandle>> = None;
            let file_stats = Self::replay_single_wal(
                &wal_path,
                lsm_state,
                mem_table_size,
                log_number,
                &mut wal_handle,
                cleanup_sender,
                cleanup_enabled,
            )?;
            stats.max_sequence = stats.max_sequence.max(file_stats.max_sequence);
            stats.entries += file_stats.entries;
            stats.truncated |= file_stats.truncated;

            // 完成一个 WAL 文件后，封存当前 memtable，确保 WAL 边界清晰
            Self::finalize_memtable_for_log(
                lsm_state,
                mem_table_size,
                log_number,
                &mut wal_handle,
                cleanup_sender,
                cleanup_enabled,
            );
        }

        Ok((stats, max_log_number))
    }

    fn replay_single_wal(
        wal_path: &PathBuf,
        lsm_state: &Arc<RwLock<LSMState>>,
        mem_table_size: usize,
        log_number: u64,
        wal_handle: &mut Option<Arc<WalHandle>>,
        cleanup_sender: &UnboundedSender<CleanupTask>,
        cleanup_enabled: &Arc<AtomicBool>,
    ) -> GoatResult<WalReplayStats> {
        replay_wal_file(wal_path, |key, value| {
            let mut state = lsm_state.write().unwrap();
            state.mem_table.put(key, value.into());
            if state.mem_table.should_flush() {
                Self::freeze_memtable(
                    &mut state,
                    mem_table_size,
                    log_number,
                    wal_handle,
                    cleanup_sender,
                    cleanup_enabled,
                );
            }
        })
    }

    fn freeze_memtable(
        state: &mut LSMState,
        mem_table_size: usize,
        log_number: u64,
        wal_handle: &mut Option<Arc<WalHandle>>,
        cleanup_sender: &UnboundedSender<CleanupTask>,
        cleanup_enabled: &Arc<AtomicBool>,
    ) {
        let wal_handle =
            Self::wal_handle_for_log(log_number, wal_handle, cleanup_sender, cleanup_enabled);
        let imm = Arc::new(ImmutableMemTable::new(state.mem_table.inner()));
        state
            .immutable_mem_tables
            .push_back(ImmutableMemTableEntry {
                table: imm,
                wal_handle,
            });
        state.mem_table = Arc::new(MemTable::new(mem_table_size));
    }

    fn finalize_memtable_for_log(
        lsm_state: &Arc<RwLock<LSMState>>,
        mem_table_size: usize,
        log_number: u64,
        wal_handle: &mut Option<Arc<WalHandle>>,
        cleanup_sender: &UnboundedSender<CleanupTask>,
        cleanup_enabled: &Arc<AtomicBool>,
    ) {
        let mut state = lsm_state.write().unwrap();
        if state.mem_table.is_empty() {
            return;
        }
        Self::freeze_memtable(
            &mut state,
            mem_table_size,
            log_number,
            wal_handle,
            cleanup_sender,
            cleanup_enabled,
        );
    }

    fn wal_handle_for_log(
        log_number: u64,
        wal_handle: &mut Option<Arc<WalHandle>>,
        cleanup_sender: &UnboundedSender<CleanupTask>,
        cleanup_enabled: &Arc<AtomicBool>,
    ) -> Option<Arc<WalHandle>> {
        if log_number == 0 {
            return None;
        }
        let handle = wal_handle.get_or_insert_with(|| {
            Arc::new(WalHandle::new(
                log_number,
                cleanup_sender.clone(),
                cleanup_enabled.clone(),
            ))
        });
        Some(handle.clone())
    }

    fn list_wal_files(
        wal_paths: &WalPaths,
        min_log_number: u64,
    ) -> GoatResult<Vec<(u64, PathBuf)>> {
        let wal_dir = wal_paths.wal_dir();
        let mut wal_files = Vec::new();

        if wal_dir.exists() {
            for entry in std::fs::read_dir(wal_dir)
                .map_err(|e| GoatError::io("list_wal_files_read_dir", e))?
            {
                let entry = entry.map_err(|e| GoatError::io("list_wal_files_entry", e))?;
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

        let main_wal = wal_paths.main_wal_path();
        if min_log_number == 0 && main_wal.exists() {
            wal_files.insert(0, (0, main_wal));
        }

        Ok(wal_files)
    }

    fn cleanup_obsolete_wals(wal_paths: &WalPaths, min_log_number: u64) {
        if min_log_number == 0 {
            return;
        }
        let wal_dir = wal_paths.wal_dir();
        if !wal_dir.exists() {
            return;
        }
        let entries = match std::fs::read_dir(wal_dir) {
            Ok(entries) => entries,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            if path.extension().and_then(|ext| ext.to_str()) != Some("wal") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let Ok(number) = stem.parse::<u64>() else {
                continue;
            };
            if number >= min_log_number {
                continue;
            }
            if let Err(e) = std::fs::remove_file(&path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!("Failed to delete obsolete WAL {:?}: {}", path, e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::KvEngine;
    use crate::goatkv::error::ErrorKind;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build tokio runtime for kv engine tests")
    }

    fn wait_for_base_level_compaction(engine: &KvEngine, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            let (l0_len, non_l0_file_count) = {
                let vs = engine.version_set.read().unwrap();
                let v = vs.current();
                let non_l0_file_count = (1..v.num_levels())
                    .map(|level| v.get_files(level).len())
                    .sum::<usize>();
                (v.get_files(0).len(), non_l0_file_count)
            };
            if l0_len <= 4 && non_l0_file_count > 0 {
                break;
            }
            if Instant::now() >= deadline {
                panic!(
                    "timeout waiting base-level compaction: l0={}, non_l0_files={}",
                    l0_len, non_l0_file_count
                );
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn test_put_and_get() {
        let rt = test_runtime();
        let _guard = rt.enter();
        let engine = KvEngine::new_for_test();

        engine.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), Some(b"value1".to_vec()));

        engine.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();
        assert_eq!(engine.get(b"key2").unwrap(), Some(b"value2".to_vec()));

        assert_eq!(engine.get(b"nonexistent").unwrap(), None);
    }

    #[test]
    fn test_multi_get_mixed_hits_misses_and_delete() {
        let rt = test_runtime();
        let _guard = rt.enter();
        let engine = KvEngine::new_for_test();

        engine.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
        engine.put(b"k2".to_vec(), b"v2".to_vec()).unwrap();
        engine.put(b"k3".to_vec(), b"v3".to_vec()).unwrap();
        engine.delete(b"k2".to_vec()).unwrap();
        engine.flush();
        engine
            .wait_for_immutable_memtables(Duration::from_secs(3))
            .unwrap();

        let keys = vec![
            b"k1".to_vec(),
            b"k2".to_vec(),
            b"missing".to_vec(),
            b"k3".to_vec(),
        ];
        let results = engine.multi_get(&keys).unwrap();
        assert_eq!(
            results,
            vec![Some(b"v1".to_vec()), None, None, Some(b"v3".to_vec())]
        );
    }

    #[test]
    fn test_multi_get_empty_keys() {
        let rt = test_runtime();
        let _guard = rt.enter();
        let engine = KvEngine::new_for_test();

        let keys: Vec<Vec<u8>> = Vec::new();
        let results = engine.multi_get(&keys).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_multi_get_reuses_results_for_duplicate_keys() {
        let rt = test_runtime();
        let _guard = rt.enter();
        let engine = KvEngine::new_for_test();

        engine.put(b"dup".to_vec(), b"v_dup".to_vec()).unwrap();
        engine.put(b"other".to_vec(), b"v_other".to_vec()).unwrap();

        let keys = vec![
            b"dup".to_vec(),
            b"dup".to_vec(),
            b"missing".to_vec(),
            b"dup".to_vec(),
            b"other".to_vec(),
            b"other".to_vec(),
        ];
        let results = engine.multi_get(&keys).unwrap();
        assert_eq!(
            results,
            vec![
                Some(b"v_dup".to_vec()),
                Some(b"v_dup".to_vec()),
                None,
                Some(b"v_dup".to_vec()),
                Some(b"v_other".to_vec()),
                Some(b"v_other".to_vec()),
            ]
        );
    }

    #[test]
    fn test_update_existing_key() {
        let rt = test_runtime();
        let _guard = rt.enter();
        let engine = KvEngine::new_for_test();

        engine.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), Some(b"value1".to_vec()));

        engine.put(b"key1".to_vec(), b"newvalue".to_vec()).unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), Some(b"newvalue".to_vec()));
    }

    #[test]
    fn test_delete_key() {
        let rt = test_runtime();
        let _guard = rt.enter();
        let engine = KvEngine::new_for_test();

        engine.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), Some(b"value1".to_vec()));

        engine.delete(b"key1".to_vec()).unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), None);

        engine.delete(b"nonexistent".to_vec()).unwrap();
        assert_eq!(engine.get(b"nonexistent").unwrap(), None);
    }

    #[test]
    fn test_delete_then_reinsert() {
        let rt = test_runtime();
        let _guard = rt.enter();
        let engine = KvEngine::new_for_test();

        engine.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        engine.delete(b"key1".to_vec()).unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), None);

        engine.put(b"key1".to_vec(), b"value2".to_vec()).unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_multiple_operations() {
        let rt = test_runtime();
        let _guard = rt.enter();
        let engine = KvEngine::new_for_test();

        engine.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        engine.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();
        engine.delete(b"key1".to_vec()).unwrap();
        engine.put(b"key3".to_vec(), b"value3".to_vec()).unwrap();
        engine
            .put(b"key2".to_vec(), b"updated_value2".to_vec())
            .unwrap();

        assert_eq!(engine.get(b"key1").unwrap(), None);
        assert_eq!(
            engine.get(b"key2").unwrap(),
            Some(b"updated_value2".to_vec())
        );
        assert_eq!(engine.get(b"key3").unwrap(), Some(b"value3".to_vec()));
    }

    #[test]
    fn test_snapshot_get_sees_old_value_after_put() {
        let rt = test_runtime();
        let _guard = rt.enter();
        let engine = KvEngine::new_for_test();

        engine.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
        let snapshot = engine.create_snapshot().unwrap();

        engine.put(b"k1".to_vec(), b"v2".to_vec()).unwrap();

        assert_eq!(
            engine.get_with_snapshot(b"k1", snapshot.id).unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(engine.get(b"k1").unwrap(), Some(b"v2".to_vec()));
        engine.release_snapshot(snapshot.id).unwrap();
    }

    #[test]
    fn test_snapshot_get_sees_old_state_after_delete() {
        let rt = test_runtime();
        let _guard = rt.enter();
        let engine = KvEngine::new_for_test();

        engine.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
        let snapshot = engine.create_snapshot().unwrap();

        engine.delete(b"k1".to_vec()).unwrap();

        assert_eq!(
            engine.get_with_snapshot(b"k1", snapshot.id).unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(engine.get(b"k1").unwrap(), None);
        engine.release_snapshot(snapshot.id).unwrap();
    }

    #[test]
    fn test_snapshot_survives_flush_and_compaction() {
        let rt = test_runtime();
        let _guard = rt.enter();
        let engine = KvEngine::new_for_test();

        engine.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
        engine.flush();
        let snapshot = engine.create_snapshot().unwrap();

        for i in 2..=10u64 {
            engine
                .put(b"k1".to_vec(), format!("v{}", i).into_bytes())
                .unwrap();
            engine.flush();
        }

        wait_for_base_level_compaction(&engine, Duration::from_secs(5));

        assert_eq!(
            engine.get_with_snapshot(b"k1", snapshot.id).unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(engine.get(b"k1").unwrap(), Some(b"v10".to_vec()));
        engine.release_snapshot(snapshot.id).unwrap();
    }

    #[test]
    fn test_snapshot_row_cache_respects_read_seq_visibility() {
        let rt = test_runtime();
        let _guard = rt.enter();
        let engine = KvEngine::new_for_test();

        engine.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
        engine.flush();
        engine
            .wait_for_immutable_memtables(Duration::from_secs(3))
            .unwrap();
        let snapshot_v1 = engine.create_snapshot().unwrap();

        engine.put(b"k1".to_vec(), b"v2".to_vec()).unwrap();
        engine.flush();
        engine
            .wait_for_immutable_memtables(Duration::from_secs(3))
            .unwrap();
        let snapshot_v2 = engine.create_snapshot().unwrap();

        assert_eq!(
            engine.get_with_snapshot(b"k1", snapshot_v1.id).unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            engine.get_with_snapshot(b"k1", snapshot_v2.id).unwrap(),
            Some(b"v2".to_vec())
        );
        assert_eq!(
            engine.get_with_snapshot(b"k1", snapshot_v1.id).unwrap(),
            Some(b"v1".to_vec())
        );

        let metrics = engine.read_cache_metrics().unwrap();
        assert!(
            metrics.row_misses >= 2,
            "expected at least two row cache misses for two visibility keys, got {:?}",
            metrics
        );
        assert!(
            metrics.row_hits >= 1,
            "expected row cache hit on repeated snapshot read, got {:?}",
            metrics
        );

        engine.release_snapshot(snapshot_v1.id).unwrap();
        engine.release_snapshot(snapshot_v2.id).unwrap();
    }

    #[test]
    fn test_release_unknown_snapshot_returns_not_found() {
        let rt = test_runtime();
        let _guard = rt.enter();
        let engine = KvEngine::new_for_test();

        let err = engine
            .release_snapshot(42)
            .expect_err("release unknown snapshot should fail");
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn test_empty_flush_is_noop() {
        let rt = test_runtime();
        let _guard = rt.enter();
        let engine = KvEngine::new_for_test();

        engine.flush();

        let state = engine.lsm_state.read().unwrap();
        assert!(state.immutable_mem_tables.is_empty());
        assert!(state.mem_table.is_empty());
        drop(state);

        let version = engine.version_set.read().unwrap().current();
        assert!(version.get_files(0).is_empty());
    }

    #[test]
    fn test_shutdown_rejects_new_writes() {
        let rt = test_runtime();
        let _guard = rt.enter();
        let engine = KvEngine::new_for_test();
        engine
            .put(b"key_before_shutdown".to_vec(), b"value".to_vec())
            .unwrap();

        engine.shutdown().unwrap();

        let err = engine
            .put(b"key_after_shutdown".to_vec(), b"value".to_vec())
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unavailable);
        assert_eq!(
            err.to_string(),
            "unavailable write_coordinator: write coordinator closed"
        );
    }

    #[test]
    fn test_shutdown_is_idempotent() {
        let rt = test_runtime();
        let _guard = rt.enter();
        let engine = KvEngine::new_for_test();
        engine.put(b"k".to_vec(), b"v".to_vec()).unwrap();

        engine.shutdown().unwrap();
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_paths_integration() {
        let rt = test_runtime();
        let _guard = rt.enter();
        let engine = KvEngine::new_for_test();
        let wal_paths = engine.wal_paths();
        let sstable_paths = engine.sstable_paths();
        let manifest_paths = engine.manifest_paths();

        assert!(manifest_paths.base_dir().exists());
        assert!(manifest_paths.data_dir().exists());
        assert!(wal_paths.wal_dir().exists());
        assert!(sstable_paths.tmp_dir().exists());

        let wal_path = wal_paths.main_wal_path();
        assert!(wal_path.parent().unwrap() == wal_paths.wal_dir());
    }

    #[test]
    fn test_shutdown_write_race_only_returns_unavailable() {
        let rt = test_runtime();
        let _guard = rt.enter();
        let engine = Arc::new(KvEngine::new_for_test());

        let error_kinds = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::new();
        for tid in 0..4usize {
            let engine = Arc::clone(&engine);
            let error_kinds = Arc::clone(&error_kinds);
            handles.push(thread::spawn(move || {
                for i in 0..20_000usize {
                    let key = format!("k{}_{}", tid, i).into_bytes();
                    match engine.put(key, b"v".to_vec()) {
                        Ok(()) => {}
                        Err(e) => {
                            error_kinds.lock().unwrap().push(e.kind());
                            break;
                        }
                    }
                }
            }));
        }

        thread::sleep(Duration::from_millis(20));
        engine.shutdown().unwrap();
        for handle in handles {
            handle.join().expect("writer thread panicked");
        }

        for kind in error_kinds.lock().unwrap().iter() {
            assert_eq!(
                *kind,
                ErrorKind::Unavailable,
                "shutdown race should not surface non-unavailable errors"
            );
        }
    }

    #[test]
    fn test_l0_compacts_to_base_level_when_l0_exceeds_threshold() {
        let rt = test_runtime();
        let _guard = rt.enter();
        let engine = KvEngine::new_for_test();

        for i in 0..6usize {
            engine
                .put(
                    format!("k{:02}", i).into_bytes(),
                    format!("v{:02}", i).into_bytes(),
                )
                .unwrap();
            engine.flush();
        }

        wait_for_base_level_compaction(&engine, Duration::from_secs(3));
    }

    #[test]
    fn test_compaction_cascades_to_l2_when_l1_exceeds_threshold() {
        let rt = test_runtime();
        let _guard = rt.enter();
        let engine = KvEngine::new_for_test();

        let big_value = vec![b'v'; 16 * 1024];
        let total = 25usize;
        for i in 0..total {
            engine
                .put(format!("cascade_k{:02}", i).into_bytes(), big_value.clone())
                .unwrap();
            engine.flush();
        }

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let (l0_len, l1_len, l2_len) = {
                let vs = engine.version_set.read().unwrap();
                let v = vs.current();
                (
                    v.get_files(0).len(),
                    v.get_files(1).len(),
                    v.get_files(2).len(),
                )
            };
            if l0_len <= 4 && l1_len <= 4 && l2_len > 0 {
                break;
            }
            if Instant::now() >= deadline {
                panic!(
                    "timeout waiting multi-level compaction: l0={}, l1={}, l2={}",
                    l0_len, l1_len, l2_len
                );
            }
            thread::sleep(Duration::from_millis(20));
        }

        for i in 0..total {
            let key = format!("cascade_k{:02}", i).into_bytes();
            let expected = big_value.clone();
            let got = engine.get(&key).unwrap();
            assert_eq!(got, Some(expected), "value mismatch for key {}", i);
        }
    }
}
