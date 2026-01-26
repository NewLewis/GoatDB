use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};

use crate::goatkv::core::cleanup_worker::CleanupWorker;
use crate::goatkv::core::flush_worker::{FlushTask, FlushWorker};
use crate::goatkv::core::lsm_state::{ImmutableMemTableEntry, LSMState};
use crate::goatkv::core::mem_table::{ImmutableMemTable, MemTable};
use crate::goatkv::core::sequence_number::SequenceNumber;
use crate::goatkv::core::wal_handle::WalHandle;
use crate::goatkv::format::internal_key::{InternalKey, InternalKeyKind};
use crate::goatkv::metadata::version_set::{VersionSet, VersionSetOptions};
use crate::goatkv::storage::wal::WalPaths;
use crate::goatkv::storage::wal::{replay_wal_file, WalReplayStats, WalWriter};
use crate::goatkv::utils::cleanup_task::CleanupTask;
use crate::goatkv::utils::options::KvEngineOptions;
use crate::goatkv::utils::paths::{ManifestPaths, SstablePaths};
use tracing::{error, warn};

type DbPaths = (Arc<WalPaths>, Arc<SstablePaths>, Arc<ManifestPaths>);

/// LSM-Tree 键值存储引擎
#[derive(Debug)]
pub struct KvEngine {
    /// WAL 写入器，负责写前日志
    wal_writer: Arc<Mutex<WalWriter>>,
    /// 序列号生成器
    sequence_number: Arc<SequenceNumber>,
    /// 清理任务发送端
    cleanup_sender: mpsc::Sender<CleanupTask>,
    /// 是否允许删除 WAL（关闭时禁用）
    cleanup_enabled: Arc<std::sync::atomic::AtomicBool>,
    /// LSM 状态管理器（memtables + current version）
    lsm_state: Arc<RwLock<LSMState>>,
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
    /// 后台清理 Worker
    #[allow(dead_code)] // 持有以保持清理线程存活
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
    pub fn init_db_paths<P: AsRef<Path>>(base_dir: P) -> Result<DbPaths, std::io::Error> {
        let base_dir = base_dir.as_ref().to_path_buf();
        let data_dir = base_dir.join("data");
        let wal_dir = base_dir.join("wal");
        let log_dir = base_dir.join("log");
        let tmp_dir = base_dir.join("tmp");

        let dirs = [&base_dir, &data_dir, &wal_dir, &log_dir, &tmp_dir];
        for dir in dirs {
            if !dir.exists() {
                fs::create_dir_all(dir)?;
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
    ///
    /// # 参数
    /// - `options`: KvEngine 配置选项
    ///
    /// # 返回
    /// - `Ok(KvEngine)`: 创建成功
    /// - `Err(std::io::Error)`: 创建目录或初始化失败
    pub fn new_with_options(options: KvEngineOptions) -> Result<Self, std::io::Error> {
        let (wal_paths, sstable_paths, manifest_paths) = Self::init_db_paths(&options.data_dir)?;
        let _ = sstable_paths.cleanup_tmp_dir();

        let (cleanup_worker, cleanup_sender) =
            CleanupWorker::new(wal_paths.clone(), sstable_paths.clone());
        let cleanup_enabled = Arc::new(std::sync::atomic::AtomicBool::new(true));

        let mem_table = Arc::new(MemTable::new(options.mem_table_size));

        // 创建 VersionSet 并获取当前版本快照
        let vs_options = VersionSetOptions::from(&options);
        let version_set = VersionSet::open(
            manifest_paths.clone(),
            sstable_paths.clone(),
            vs_options,
            cleanup_sender.clone(),
        )?;
        let version_set = Arc::new(RwLock::new(version_set));
        let current_version = {
            let vs_guard = version_set.read().unwrap();
            vs_guard.current()
        };

        // 创建 LSM 状态管理器（仅保存 memtables + version）
        let lsm_state = Arc::new(RwLock::new(LSMState::new(
            mem_table.clone(),
            current_version,
        )));

        let (wal_stats, wal_max_number) = if options.recover_from_wal {
            let min_log_number = {
                let vs_guard = version_set.read().unwrap();
                vs_guard.log_number()
            };
            Self::replay_into_state(
                &wal_paths,
                &lsm_state,
                options.mem_table_size,
                min_log_number,
                &cleanup_sender,
                &cleanup_enabled,
            )?
        } else {
            (
                WalReplayStats {
                    max_sequence: 0,
                    entries: 0,
                    truncated: false,
                },
                0,
            )
        };

        if wal_stats.truncated {
            warn!("WAL replay truncated due to corruption or partial record.");
        }

        // 选择一个新的 WAL 编号，但不在恢复时推进 manifest 的 log_number。
        // 这样可以避免“恢复后尚未 flush 又崩溃”导致跳过旧 WAL。
        let current_log_number = {
            let vs_guard = version_set.read().unwrap();
            let mut log_number = vs_guard.log_number();
            if log_number == 0 {
                log_number = 1;
            }
            if wal_max_number >= log_number {
                log_number = wal_max_number + 1;
            }
            log_number
        };

        let wal_path = wal_paths.wal_path_by_id(current_log_number);
        let wal_writer = WalWriter::new(wal_path, options.wal_sync)
            .map_err(|e| std::io::Error::other(format!("Failed to open WAL file: {}", e)))?;

        let last_sequence = {
            let vs_guard = version_set.read().unwrap();
            std::cmp::max(wal_stats.max_sequence, vs_guard.last_sequence())
        };

        // 创建序列号生成器（从最后序列号继续）
        let sequence_number = Arc::new(SequenceNumber::with_start(last_sequence + 1));

        let engine = Self {
            wal_writer: Arc::new(Mutex::new(wal_writer)),
            sequence_number,
            lsm_state: lsm_state.clone(),
            version_set: version_set.clone(),
            cleanup_sender: cleanup_sender.clone(),
            cleanup_enabled: cleanup_enabled.clone(),
            options: Arc::new(options),
            flush_worker: FlushWorker::new(
                lsm_state.clone(),
                version_set.clone(),
                sstable_paths.clone(),
            ),
            cleanup_worker,
            current_log_number: AtomicU64::new(current_log_number),
            wal_paths,
            sstable_paths,
            manifest_paths,
        };

        // 提交恢复阶段遗留的 immutable memtables
        {
            let state = engine.lsm_state.read().unwrap();
            for entry in state.immutable_mem_tables.iter() {
                let _ = engine.flush_worker.submit_task(FlushTask {
                    immutable_mem_table: entry.table.clone(),
                    new_log_number: 0,
                });
            }
        }

        Ok(engine)
    }

    /// 创建一个新的KvEngine，不尝试从WAL恢复
    /// 主要用于测试
    #[cfg(test)]
    pub fn new_for_test() -> Self {
        let options = KvEngineOptions::for_test();
        Self::new_with_options(options).expect("Failed to create test KvEngine")
    }

    /// 从 WAL 文件恢复数据到内存表
    fn replay_into_state(
        wal_paths: &WalPaths,
        lsm_state: &Arc<RwLock<LSMState>>,
        mem_table_size: usize,
        min_log_number: u64,
        cleanup_sender: &mpsc::Sender<CleanupTask>,
        cleanup_enabled: &Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<(WalReplayStats, u64), std::io::Error> {
        let wal_files = Self::list_wal_files(wal_paths, min_log_number)?;
        let mut stats = WalReplayStats {
            max_sequence: 0,
            entries: 0,
            truncated: false,
        };
        let mut max_log_number = 0u64;

        for (log_number, wal_path) in wal_files {
            if log_number > max_log_number {
                max_log_number = log_number;
            }
            if !wal_path.exists() {
                continue;
            }
            let mut wal_handle: Option<Arc<WalHandle>> = None;
            let file_stats = replay_wal_file(&wal_path, |key, value| {
                let mut state = lsm_state.write().unwrap();
                state.mem_table.put(key, value.into());
                if state.mem_table.should_flush() {
                    let wal_handle = if log_number > 0 {
                        let handle = wal_handle.get_or_insert_with(|| {
                            Arc::new(WalHandle::new(
                                log_number,
                                cleanup_sender.clone(),
                                cleanup_enabled.clone(),
                            ))
                        });
                        Some(handle.clone())
                    } else {
                        None
                    };
                    let imm = Arc::new(ImmutableMemTable::new(state.mem_table.inner()));
                    state
                        .immutable_mem_tables
                        .push_back(ImmutableMemTableEntry {
                            table: imm,
                            wal_handle: wal_handle.clone(),
                        });
                    state.mem_table = Arc::new(MemTable::new(mem_table_size));
                }
            })?;
            stats.max_sequence = stats.max_sequence.max(file_stats.max_sequence);
            stats.entries += file_stats.entries;
            stats.truncated |= file_stats.truncated;
            // 不因为截断停止，继续尝试后续 WAL，保证尽量多恢复

            // 完成一个 WAL 文件后，封存当前 memtable，确保 WAL 边界清晰
            {
                let mut state = lsm_state.write().unwrap();
                if !state.mem_table.is_empty() {
                    let wal_handle = if log_number > 0 {
                        let handle = wal_handle.get_or_insert_with(|| {
                            Arc::new(WalHandle::new(
                                log_number,
                                cleanup_sender.clone(),
                                cleanup_enabled.clone(),
                            ))
                        });
                        Some(handle.clone())
                    } else {
                        None
                    };
                    let imm = Arc::new(ImmutableMemTable::new(state.mem_table.inner()));
                    state
                        .immutable_mem_tables
                        .push_back(ImmutableMemTableEntry {
                            table: imm,
                            wal_handle: wal_handle.clone(),
                        });
                    state.mem_table = Arc::new(MemTable::new(mem_table_size));
                }
            }
        }

        Ok((stats, max_log_number))
    }

    fn list_wal_files(
        wal_paths: &WalPaths,
        min_log_number: u64,
    ) -> Result<Vec<(u64, PathBuf)>, std::io::Error> {
        let wal_dir = wal_paths.wal_dir();
        let mut wal_files = Vec::new();

        if wal_dir.exists() {
            for entry in std::fs::read_dir(wal_dir)? {
                let entry = entry?;
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
}

impl Drop for KvEngine {
    fn drop(&mut self) {
        // Shutdown: avoid deleting WAL files when in-memory state may not be flushed.
        self.cleanup_enabled.store(false, Ordering::SeqCst);
    }
}

impl KvEngine {
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

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let (mem_table, immutable_mem_tables, version) = {
            let lsm_state = self.lsm_state.read().unwrap();
            let mem_table = lsm_state.mem_table.clone();
            let immutable_mem_tables = lsm_state.immutable_mem_tables.clone();
            let version = lsm_state.version.clone();
            (mem_table, immutable_mem_tables, version)
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
        for entry in immutable_mem_tables.iter().rev() {
            if let Some((internal_key, value)) = entry.table.get(key) {
                if internal_key.kind() != InternalKeyKind::Delete {
                    return Some(value);
                } else {
                    return None;
                }
            }
        }

        // Then check SSTables via version snapshot
        if let Some((internal_key, value)) = version.get(key) {
            if internal_key.kind() != InternalKeyKind::Delete {
                return Some(value);
            } else {
                return None;
            }
        }

        // Key not found
        None
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        // 构造InternalKey
        let internal_key = InternalKey::new(key, self.sequence_number.next(), InternalKeyKind::Put);

        // 先写入wal
        self.wal_writer
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
        self.wal_writer
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
        let (immutable_mem_table, new_log_number, rotation_succeeded) = {
            let mut wal_guard = self.wal_writer.lock().unwrap();
            let candidate_log_number = {
                let vs = self.version_set.read().unwrap();
                let vs_next = vs.log_number().saturating_add(1);
                let current_next = self
                    .current_log_number
                    .load(Ordering::SeqCst)
                    .saturating_add(1);
                std::cmp::max(vs_next, current_next)
            };
            let old_log_number = self.current_log_number.load(Ordering::SeqCst);
            let new_wal_path = self.wal_paths.wal_path_by_id(candidate_log_number);
            let (new_log_number, rotation_succeeded) =
                match WalWriter::new(new_wal_path, self.options.wal_sync) {
                    Ok(new_manager) => {
                        *wal_guard = new_manager;
                        let new_log_number = candidate_log_number;
                        self.current_log_number
                            .store(new_log_number, Ordering::SeqCst);
                        (new_log_number, true)
                    }
                    Err(e) => {
                        error!("Failed to rotate WAL: {}", e);
                        (old_log_number, false)
                    }
                };
            let wal_handle = if rotation_succeeded && old_log_number > 0 {
                Some(Arc::new(WalHandle::new(
                    old_log_number,
                    self.cleanup_sender.clone(),
                    self.cleanup_enabled.clone(),
                )))
            } else {
                None
            };
            let mut state = self.lsm_state.write().unwrap();

            // 克隆当前的 memtable
            let mem_table = state.mem_table.clone();

            // 创建 immutable_mem_table
            let immutable_mem_table = Arc::new(ImmutableMemTable::new(mem_table.inner()));

            // 将 immutable_mem_table 放入队列并创建新的 memtable
            state
                .immutable_mem_tables
                .push_back(ImmutableMemTableEntry {
                    table: immutable_mem_table.clone(),
                    wal_handle,
                });
            state.mem_table = Arc::new(MemTable::new(self.options.mem_table_size));

            (immutable_mem_table, new_log_number, rotation_succeeded)
        };

        // 发送 flush 任务到后台线程
        if let Err(e) = self.flush_worker.submit_task(FlushTask {
            immutable_mem_table,
            new_log_number: if rotation_succeeded {
                new_log_number
            } else {
                0
            },
        }) {
            error!("Failed to send flush task: {}", e);
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
    fn test_paths_integration() {
        let engine = KvEngine::new_for_test();
        let wal_paths = engine.wal_paths();
        let sstable_paths = engine.sstable_paths();
        let manifest_paths = engine.manifest_paths();

        // Verify paths are properly integrated
        assert!(manifest_paths.base_dir().exists());
        assert!(manifest_paths.data_dir().exists());
        assert!(wal_paths.wal_dir().exists());
        assert!(sstable_paths.tmp_dir().exists());

        // Verify WAL file is in the correct location
        let wal_path = wal_paths.main_wal_path();
        assert!(wal_path.parent().unwrap() == wal_paths.wal_dir());
    }
}
