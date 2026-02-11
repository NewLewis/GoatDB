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
use super::writer::{KvWriter, WriteOp};
use crate::goatkv::core::cleanup_worker::CleanupWorker;
use crate::goatkv::core::flush_worker::{FlushTask, FlushWorker};
use crate::goatkv::core::lsm_state::{ImmutableMemTableEntry, LSMState};
use crate::goatkv::core::mem_table::{ImmutableMemTable, MemTable};
use crate::goatkv::core::sequence_number::SequenceNumber;
use crate::goatkv::error::{Error as GoatError, Result as GoatResult};
use crate::goatkv::metadata::version::Version;
use crate::goatkv::metadata::version_set::{VersionSet, VersionSetOptions};
use crate::goatkv::storage::wal::{
    replay_wal_file, WalHandle, WalManager, WalManagerConfig, WalPaths, WalReplayStats,
};
use crate::goatkv::utils::cleanup_task::CleanupTask;
use crate::goatkv::utils::options::KvEngineOptions;
use crate::goatkv::utils::paths::{ManifestPaths, SstablePaths};

type DbPaths = (Arc<WalPaths>, Arc<SstablePaths>, Arc<ManifestPaths>);
const SHUTDOWN_FLUSH_WAIT_TIMEOUT_MS: u64 = 30_000;
const SHUTDOWN_FLUSH_WAIT_INTERVAL_MS: u64 = 10;

/// LSM-Tree 键值存储引擎
#[derive(Debug)]
pub struct KvEngine {
    /// WAL 写入器，负责写前日志
    wal_manager: Arc<WalManager>,
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
        let wal_manager = Self::open_wal_manager(&options, &wal_paths, current_log_number)?;
        let sequence_number = Self::init_sequence_number(&version_set, wal_stats.max_sequence);

        let engine = Self::build_engine(
            wal_manager,
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
        );

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
        let _gate = self.write_gate.write().unwrap();
        self.flush_inner();
    }

    /// 优雅停机：
    /// 1) 关闭写入入口，拒绝新写请求；
    /// 2) 在写入门闩保护下封存当前 memtable 并触发 flush；
    /// 3) 等待 immutable 队列清空（后台 flush 完成）。
    pub fn shutdown(&self) -> GoatResult<()> {
        self.writer.close();
        {
            let _gate = self.write_gate.write().unwrap();
            self.flush_inner();
        }
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

    fn open_wal_manager(
        options: &KvEngineOptions,
        wal_paths: &WalPaths,
        log_number: u64,
    ) -> GoatResult<WalManager> {
        let wal_path = wal_paths.wal_path_by_id(log_number);
        WalManager::new(
            wal_path,
            WalManagerConfig {
                wal_sync: options.wal_sync,
                sync_interval_ms: options.wal_sync_interval_ms,
                sync_bytes: options.wal_sync_bytes,
                max_buffer_bytes: options.wal_max_buffer_bytes,
            },
        )
        .map_err(|e| {
            GoatError::internal_with_source("open_wal_manager", "failed to open wal manager", e)
        })
    }

    fn init_sequence_number(
        version_set: &Arc<RwLock<VersionSet>>,
        wal_max_sequence: u64,
    ) -> Arc<SequenceNumber> {
        let vs_guard = version_set.read().unwrap();
        let last_sequence = max(wal_max_sequence, vs_guard.last_sequence());
        Arc::new(SequenceNumber::with_start(last_sequence + 1))
    }

    fn build_engine(
        wal_manager: WalManager,
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
    ) -> Self {
        let flush_worker = FlushWorker::new(
            lsm_state.clone(),
            version_set.clone(),
            sstable_paths.clone(),
        );
        let wal_manager = Arc::new(wal_manager);
        let options = Arc::new(options);
        let write_gate = Arc::new(RwLock::new(()));
        let writer = KvWriter::new(
            wal_manager.clone(),
            sequence_number,
            lsm_state.clone(),
            write_gate.clone(),
            options.clone(),
        );
        let reader = KvReader::new(lsm_state.clone());
        Self {
            wal_manager,
            lsm_state: lsm_state.clone(),
            version_set: version_set.clone(),
            cleanup_sender: cleanup_sender.clone(),
            cleanup_enabled: cleanup_enabled.clone(),
            write_gate,
            options,
            flush_worker,
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
        match self.wal_manager.rotate(new_wal_path) {
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

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build tokio runtime for kv engine tests")
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
}
