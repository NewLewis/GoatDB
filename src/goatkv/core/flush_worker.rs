use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::{mpsc, Arc, RwLock};
use std::thread;

use crate::goatkv::core::lsm_state::LSMState;
use crate::goatkv::core::mem_table::ImmutableMemTable;
use crate::goatkv::core::snapshot_manager::SnapshotManager;
use crate::goatkv::error::{Error as GoatError, Result as GoatResult};
use crate::goatkv::format::internal_key::InternalKey;
use crate::goatkv::metadata::file_metadata::{FileMetadata, TableProperties};
use crate::goatkv::metadata::version::Version;
use crate::goatkv::metadata::version_edit::{NewFile, VersionEdit};
use crate::goatkv::metadata::version_set::VersionSet;
use crate::goatkv::storage::sstable::{SSTableBuilder, SSTableReader, SSTableScanIterator};
use crate::goatkv::utils::paths::SstablePaths;
use tracing::{error, warn};

#[derive(Debug, Clone, Copy)]
pub struct CompactionConfig {
    pub l0_compaction_file_trigger: usize,
    pub max_bytes_for_level_base: u64,
    pub max_bytes_for_level_multiplier: u64,
    pub max_grandparent_overlap_bytes_factor: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            l0_compaction_file_trigger: 4,
            max_bytes_for_level_base: 64 * 1024,
            max_bytes_for_level_multiplier: 10,
            max_grandparent_overlap_bytes_factor: 10,
        }
    }
}

impl CompactionConfig {
    fn normalized(mut self) -> Self {
        self.l0_compaction_file_trigger = self.l0_compaction_file_trigger.max(1);
        self.max_bytes_for_level_base = self.max_bytes_for_level_base.max(1);
        self.max_bytes_for_level_multiplier = self.max_bytes_for_level_multiplier.max(2);
        self.max_grandparent_overlap_bytes_factor =
            self.max_grandparent_overlap_bytes_factor.max(1);
        self
    }
}

/// 刷盘任务
#[derive(Debug)]
pub struct FlushTask {
    pub(crate) immutable_mem_table: Arc<ImmutableMemTable>,
    pub(crate) new_log_number: u64,
}

/// 后台刷盘 Worker
///
/// 负责在独立线程中处理 MemTable 到 SSTable 的刷盘任务。
/// 使用 mpsc channel 接收任务，确保任务按顺序执行。
#[derive(Debug)]
pub struct FlushWorker {
    flush_sender: mpsc::Sender<FlushTask>,
    flush_handle: Option<thread::JoinHandle<()>>,
    compaction_sender: mpsc::Sender<()>,
    compaction_handle: Option<thread::JoinHandle<()>>,
}

#[derive(Debug)]
struct CompactionPlan {
    source_level: usize,
    target_level: usize,
    source_files: Vec<Arc<FileMetadata>>,
    target_files: Vec<Arc<FileMetadata>>,
    grandparent_files: Vec<Arc<FileMetadata>>,
    grandparent_overlap_bytes_limit: u64,
}

struct CompactionStream {
    iterators: Vec<SSTableScanIterator>,
    heap: BinaryHeap<HeapItem>,
}

#[derive(Debug)]
struct HeapItem {
    key: InternalKey,
    value: Vec<u8>,
    iter_idx: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.iter_idx == other.iter_idx
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering so BinaryHeap behaves as min-heap by InternalKey.
        match other.key.cmp(&self.key) {
            Ordering::Equal => other.iter_idx.cmp(&self.iter_idx),
            ord => ord,
        }
    }
}

impl CompactionStream {
    fn from_files(
        sstable_paths: &SstablePaths,
        source_files: &[Arc<FileMetadata>],
        target_files: &[Arc<FileMetadata>],
    ) -> GoatResult<Self> {
        let mut iterators = Vec::new();
        let mut heap = BinaryHeap::new();

        for file in source_files.iter().chain(target_files.iter()) {
            let path = sstable_paths.sstable_path_by_id(file.file_id);
            let reader = SSTableReader::open(&path).map_err(|e| {
                GoatError::internal_with_source(
                    "level_compaction_open_sstable",
                    format!("failed to open sstable {:?}", path),
                    e,
                )
            })?;
            let mut iter = reader.into_scan_iterator();
            if let Some((key, value)) = iter.next_entry().map_err(|e| {
                GoatError::internal_with_source(
                    "level_compaction_scan_sstable",
                    format!("failed to scan sstable {:?}", path),
                    e,
                )
            })? {
                heap.push(HeapItem {
                    key,
                    value,
                    iter_idx: iterators.len(),
                });
            }
            iterators.push(iter);
        }

        Ok(Self { iterators, heap })
    }

    fn next_entry(&mut self) -> GoatResult<Option<(InternalKey, Vec<u8>)>> {
        let Some(item) = self.heap.pop() else {
            return Ok(None);
        };

        if let Some((next_key, next_value)) = self.iterators[item.iter_idx].next_entry()? {
            self.heap.push(HeapItem {
                key: next_key,
                value: next_value,
                iter_idx: item.iter_idx,
            });
        }

        Ok(Some((item.key, item.value)))
    }
}

impl FlushWorker {
    /// 创建新的 FlushWorker 并启动后台线程
    ///
    /// # 参数
    /// - `lsm_state`: LSM 状态管理器，用于访问 immutable memtables 和 version snapshot
    /// - `version_set`: VersionSet 管理 manifest 与版本演进
    /// - `sstable_paths`: SSTable 路径集合
    ///
    /// # 返回
    /// 返回新创建的 FlushWorker 实例
    pub fn new(
        lsm_state: Arc<RwLock<LSMState>>,
        version_set: Arc<RwLock<VersionSet>>,
        sstable_paths: Arc<SstablePaths>,
        snapshot_manager: Arc<RwLock<SnapshotManager>>,
        flush_failure_streak_limit: usize,
        compaction_config: CompactionConfig,
        bloom_prefix_extractor_len: usize,
    ) -> Self {
        let (flush_tx, flush_rx) = mpsc::channel();
        let (compaction_tx, compaction_rx) = mpsc::channel();
        let flush_failure_streak_limit = flush_failure_streak_limit.max(1);
        let compaction_config = compaction_config.normalized();
        let bloom_prefix_extractor_len = bloom_prefix_extractor_len.min(u16::MAX as usize);

        let compaction_handle = {
            let lsm_state = Arc::clone(&lsm_state);
            let version_set = Arc::clone(&version_set);
            let sstable_paths = Arc::clone(&sstable_paths);
            let snapshot_manager = Arc::clone(&snapshot_manager);
            thread::spawn(move || {
                Self::run_compaction_loop(
                    compaction_rx,
                    lsm_state,
                    version_set,
                    sstable_paths,
                    snapshot_manager,
                    compaction_config,
                    bloom_prefix_extractor_len,
                );
            })
        };

        let flush_handle = {
            let compaction_tx_for_flush = compaction_tx.clone();
            thread::spawn(move || {
                Self::run_flush_loop(
                    flush_rx,
                    lsm_state,
                    version_set,
                    sstable_paths,
                    compaction_tx_for_flush,
                    flush_failure_streak_limit,
                    bloom_prefix_extractor_len,
                );
            })
        };

        Self {
            flush_sender: flush_tx,
            flush_handle: Some(flush_handle),
            compaction_sender: compaction_tx,
            compaction_handle: Some(compaction_handle),
        }
    }

    /// 提交刷盘任务到后台线程
    ///
    /// # 参数
    /// - `task`: 要执行的刷盘任务
    ///
    /// # 返回
    /// - `Ok(())`: 任务提交成功
    /// - `Err`: 任务提交失败（通常表示后台线程已终止）
    pub fn submit_task(&self, task: FlushTask) -> Result<(), mpsc::SendError<FlushTask>> {
        self.flush_sender.send(task)
    }

    /// 后台线程主循环
    ///
    /// 持续从 channel 接收刷盘任务并执行：
    /// 1. 从 immutable memtable 读取数据
    /// 2. 创建 SSTable 文件
    /// 3. 创建 VersionEdit 并应用到 VersionSet
    /// 4. 移除已刷盘的 immutable memtable
    fn run_flush_loop(
        rx: mpsc::Receiver<FlushTask>,
        lsm_state: Arc<RwLock<LSMState>>,
        version_set: Arc<RwLock<VersionSet>>,
        sstable_paths: Arc<SstablePaths>,
        compaction_sender: mpsc::Sender<()>,
        flush_failure_streak_limit: usize,
        bloom_prefix_extractor_len: usize,
    ) {
        while let Ok(task) = rx.recv() {
            // 从任务中获取要刷盘的 immutable memtable
            let imm_table = task.immutable_mem_table.clone();

            // 在不持有锁的情况下处理数据
            let mut max_sequence = 0u64;

            // Check if empty
            if imm_table.iter().next().is_none() {
                let mut version_edit = VersionEdit::new();
                if task.new_log_number > 0 {
                    version_edit.set_log_number(task.new_log_number);
                }
                let last_sequence = {
                    let vs = version_set.read().unwrap();
                    std::cmp::max(max_sequence, vs.last_sequence())
                };
                version_edit.set_last_sequence(last_sequence);

                let current_version = {
                    let mut vs = version_set.write().unwrap();
                    if let Err(e) = vs.apply_edit(version_edit) {
                        Self::record_flush_failure(
                            &lsm_state,
                            flush_failure_streak_limit,
                            &e.to_string(),
                        );
                        error!("Failed to apply VersionEdit: {}", e);
                        continue; // 跳过当前任务，继续处理后续任务
                    }
                    vs.current()
                };

                Self::reset_flush_failure_streak(&lsm_state);

                Self::update_version_and_remove_task_memtable(
                    &lsm_state,
                    current_version,
                    &imm_table,
                );
                continue;
            }

            // 分配文件 ID
            let (file_id, next_file_number) = {
                let mut vs = version_set.write().unwrap();
                let file_id = vs.allocate_file_number();
                let next_file_number = vs.next_file_number();
                (file_id, next_file_number)
            };

            // 单次尝试：失败直接报错，不进行重试。
            let mut sst_builder = match SSTableBuilder::new_with_bloom_prefix_extractor(
                file_id,
                &sstable_paths,
                bloom_prefix_extractor_len,
            ) {
                Ok(builder) => builder,
                Err(e) => {
                    Self::record_flush_failure(
                        &lsm_state,
                        flush_failure_streak_limit,
                        &e.to_string(),
                    );
                    error!("Failed to create SSTableBuilder: {}", e);
                    continue;
                }
            };

            // 在不持有锁的情况下写入 SSTable
            let mut key_buf = Vec::new();
            let mut write_failed = false;

            let iter = imm_table.iter();
            for (key, value) in iter {
                max_sequence = max_sequence.max(key.sequence_number());
                key.serialize_into(&mut key_buf);
                if let Err(e) = sst_builder.write(&key_buf, value.as_ref()) {
                    Self::record_flush_failure(
                        &lsm_state,
                        flush_failure_streak_limit,
                        &e.to_string(),
                    );
                    error!("Failed to write entry to SSTable {}: {}", file_id, e);
                    write_failed = true;
                    break;
                }
            }

            if write_failed {
                continue;
            }

            let props = match sst_builder.finish() {
                Ok(meta) => meta,
                Err(e) => {
                    Self::record_flush_failure(
                        &lsm_state,
                        flush_failure_streak_limit,
                        &e.to_string(),
                    );
                    error!("Failed to finish SSTable {}: {}", file_id, e);
                    continue;
                }
            };

            // 创建 VersionEdit 记录新增的 SSTable
            let mut version_edit = VersionEdit::new();
            version_edit.add_file(0, NewFile::new_with_props(file_id, props));
            version_edit.set_next_file_number(next_file_number);
            // 在 manifest 中记录新 WAL 号，表示此前的 WAL 已可被忽略
            if task.new_log_number > 0 {
                version_edit.set_log_number(task.new_log_number);
            }
            let last_sequence = {
                let vs = version_set.read().unwrap();
                std::cmp::max(max_sequence, vs.last_sequence())
            };
            version_edit.set_last_sequence(last_sequence);

            let current_version = {
                let mut vs = version_set.write().unwrap();
                if let Err(e) = vs.apply_edit(version_edit) {
                    Self::record_flush_failure(
                        &lsm_state,
                        flush_failure_streak_limit,
                        &e.to_string(),
                    );
                    error!("Failed to apply VersionEdit: {}", e);
                    continue; // 不移除该任务 memtable，避免丢失内存中数据
                }
                vs.current()
            };

            Self::reset_flush_failure_streak(&lsm_state);

            // 从 immutable_mem_tables 中精确移除当前任务对应的 memtable。
            // 不能无条件 pop_front：若前序任务失败并未出队，会导致错删队头。
            Self::update_version_and_remove_task_memtable(&lsm_state, current_version, &imm_table);
            let _ = compaction_sender.send(());
        }
    }

    fn run_compaction_loop(
        rx: mpsc::Receiver<()>,
        lsm_state: Arc<RwLock<LSMState>>,
        version_set: Arc<RwLock<VersionSet>>,
        sstable_paths: Arc<SstablePaths>,
        snapshot_manager: Arc<RwLock<SnapshotManager>>,
        compaction_config: CompactionConfig,
        bloom_prefix_extractor_len: usize,
    ) {
        while rx.recv().is_ok() {
            Self::maybe_compact_levels(
                &lsm_state,
                &version_set,
                &sstable_paths,
                &snapshot_manager,
                compaction_config,
                bloom_prefix_extractor_len,
            );
        }
    }

    fn update_version_and_remove_task_memtable(
        lsm_state: &Arc<RwLock<LSMState>>,
        current_version: Arc<Version>,
        task_memtable: &Arc<ImmutableMemTable>,
    ) {
        let mut lsm_state_guard = lsm_state.write().unwrap();
        lsm_state_guard.version = current_version;
        let remove_pos = lsm_state_guard
            .immutable_mem_tables
            .iter()
            .position(|entry| Arc::ptr_eq(&entry.table, task_memtable));
        if let Some(pos) = remove_pos {
            lsm_state_guard.immutable_mem_tables.remove(pos);
        } else {
            warn!("Flush task memtable was not found in immutable queue; skip remove");
        }
    }

    fn record_flush_failure(
        lsm_state: &Arc<RwLock<LSMState>>,
        flush_failure_streak_limit: usize,
        cause: &str,
    ) {
        let mut state = lsm_state.write().unwrap();
        state.flush_failure_streak = state.flush_failure_streak.saturating_add(1);
        if state.flush_failure_streak >= flush_failure_streak_limit {
            if !state.flush_circuit_open {
                warn!(
                    "Flush circuit opened after {} consecutive failures: {}",
                    state.flush_failure_streak, cause
                );
            }
            state.flush_circuit_open = true;
        }
    }

    fn reset_flush_failure_streak(lsm_state: &Arc<RwLock<LSMState>>) {
        let mut state = lsm_state.write().unwrap();
        if state.flush_failure_streak > 0 || state.flush_circuit_open {
            state.flush_failure_streak = 0;
            state.flush_circuit_open = false;
        }
    }

    fn maybe_compact_levels(
        lsm_state: &Arc<RwLock<LSMState>>,
        version_set: &Arc<RwLock<VersionSet>>,
        sstable_paths: &Arc<SstablePaths>,
        snapshot_manager: &Arc<RwLock<SnapshotManager>>,
        compaction_config: CompactionConfig,
        bloom_prefix_extractor_len: usize,
    ) {
        loop {
            let plan = {
                let vs = version_set.read().unwrap();
                let current = vs.current();
                let compact_pointers = vs.compact_pointers_snapshot();
                Self::pick_compaction_plan(&current, &compact_pointers, compaction_config)
            };
            let Some(plan) = plan else {
                break;
            };
            let snapshot_seqs = snapshot_manager.read().unwrap().snapshot_sequences_sorted();

            if !Self::compact_one_level(
                lsm_state,
                version_set,
                sstable_paths,
                &snapshot_seqs,
                plan,
                bloom_prefix_extractor_len,
            ) {
                break;
            }
        }
    }

    fn pick_compaction_plan(
        current: &Version,
        compact_pointers: &[Option<Vec<u8>>],
        compaction_config: CompactionConfig,
    ) -> Option<CompactionPlan> {
        if current.num_levels() < 2 {
            return None;
        }

        let level_targets = Self::build_level_size_targets(current.num_levels(), compaction_config);
        let (source_level, target_level) =
            Self::pick_compaction_priority(current, &level_targets, compaction_config)?;
        let seed_files = Self::pick_seed_files(current, source_level, compact_pointers)?;
        Self::expand_inputs_by_overlap(
            current,
            source_level,
            target_level,
            seed_files,
            &level_targets,
            compaction_config,
        )
    }

    fn build_level_size_targets(
        num_levels: usize,
        compaction_config: CompactionConfig,
    ) -> Vec<u64> {
        let mut targets = vec![0; num_levels];
        let mut target = compaction_config.max_bytes_for_level_base;
        for slot in targets.iter_mut().skip(1) {
            *slot = target;
            target = target.saturating_mul(compaction_config.max_bytes_for_level_multiplier);
        }
        targets
    }

    fn pick_compaction_priority(
        current: &Version,
        level_targets: &[u64],
        compaction_config: CompactionConfig,
    ) -> Option<(usize, usize)> {
        let mut best_level: Option<usize> = None;
        let mut best_score = 1.0f64;
        let max_source_level = current.num_levels() - 2;

        for source_level in 0..=max_source_level {
            let score = if source_level == 0 {
                current.get_files(0).len() as f64
                    / compaction_config.l0_compaction_file_trigger as f64
            } else {
                let target_bytes = *level_targets.get(source_level).unwrap_or(&0);
                if target_bytes == 0 {
                    0.0
                } else {
                    current.get_level_size(source_level) as f64 / target_bytes as f64
                }
            };

            if score > best_score {
                best_score = score;
                best_level = Some(source_level);
            }
        }

        let source_level = best_level?;
        let target_level = if source_level == 0 {
            Self::pick_l0_base_level(current, level_targets)
        } else {
            source_level + 1
        };
        Some((source_level, target_level))
    }

    fn pick_l0_base_level(current: &Version, level_targets: &[u64]) -> usize {
        let max_level = current.num_levels() - 1;
        for level in 1..max_level {
            let target_bytes = *level_targets.get(level).unwrap_or(&0);
            if target_bytes == 0 {
                continue;
            }
            if current.get_level_size(level) < target_bytes {
                return level;
            }
        }
        max_level
    }

    fn max_grandparent_overlap_bytes(
        level_targets: &[u64],
        target_level: usize,
        compaction_config: CompactionConfig,
    ) -> u64 {
        let target = *level_targets
            .get(target_level)
            .unwrap_or(&compaction_config.max_bytes_for_level_base);
        target
            .max(compaction_config.max_bytes_for_level_base)
            .saturating_mul(compaction_config.max_grandparent_overlap_bytes_factor)
    }

    fn pick_seed_files(
        current: &Version,
        source_level: usize,
        compact_pointers: &[Option<Vec<u8>>],
    ) -> Option<Vec<Arc<FileMetadata>>> {
        let source_files = current.get_files(source_level);
        let pointer = compact_pointers
            .get(source_level)
            .and_then(|value| value.as_deref());
        let seed = match pointer {
            Some(pointer) => source_files
                .iter()
                .find(|file| file.largest_user_key() > pointer)
                .or_else(|| source_files.first())?,
            None => source_files.first()?,
        };
        Some(vec![Arc::clone(seed)])
    }

    fn expand_inputs_by_overlap(
        current: &Version,
        source_level: usize,
        target_level: usize,
        seed_files: Vec<Arc<FileMetadata>>,
        level_targets: &[u64],
        compaction_config: CompactionConfig,
    ) -> Option<CompactionPlan> {
        let source_level_files = current.get_files(source_level);
        let target_level_files = current.get_files(target_level);

        let mut source_files = seed_files;
        let mut target_files: Vec<Arc<FileMetadata>> = Vec::new();

        loop {
            let (source_smallest, source_largest) = Self::user_key_range(&source_files)?;
            let new_target_files =
                Self::overlapping_files(target_level_files, &source_smallest, &source_largest);
            let (expanded_smallest, expanded_largest) =
                Self::user_key_range_iter(source_files.iter().chain(new_target_files.iter()))?;
            let new_source_files =
                Self::overlapping_files(source_level_files, &expanded_smallest, &expanded_largest);

            if Self::same_file_list(&source_files, &new_source_files)
                && Self::same_file_list(&target_files, &new_target_files)
            {
                source_files = new_source_files;
                target_files = new_target_files;
                break;
            }

            source_files = new_source_files;
            target_files = new_target_files;
        }

        if source_files.is_empty() {
            warn!(
                "Skip L{}->L{} compaction due to empty source inputs",
                source_level, target_level
            );
            return None;
        }

        let (smallest, largest) =
            Self::user_key_range_iter(source_files.iter().chain(target_files.iter()))?;
        let grandparent_level = if target_level + 1 < current.num_levels() {
            Some(target_level + 1)
        } else {
            None
        };
        let grandparent_files = grandparent_level
            .map(|level| Self::overlapping_files(current.get_files(level), &smallest, &largest))
            .unwrap_or_default();
        let grandparent_overlap_bytes_limit =
            Self::max_grandparent_overlap_bytes(level_targets, target_level, compaction_config);

        Some(CompactionPlan {
            source_level,
            target_level,
            source_files,
            target_files,
            grandparent_files,
            grandparent_overlap_bytes_limit,
        })
    }

    fn overlapping_files(
        files: &[Arc<FileMetadata>],
        smallest: &[u8],
        largest: &[u8],
    ) -> Vec<Arc<FileMetadata>> {
        files
            .iter()
            .filter(|f| f.smallest_user_key() <= largest && f.largest_user_key() >= smallest)
            .cloned()
            .collect()
    }

    fn same_file_list(lhs: &[Arc<FileMetadata>], rhs: &[Arc<FileMetadata>]) -> bool {
        lhs.len() == rhs.len()
            && lhs
                .iter()
                .zip(rhs.iter())
                .all(|(left, right)| left.file_id == right.file_id)
    }

    fn overlap_bytes(files: &[Arc<FileMetadata>], smallest: &[u8], largest: &[u8]) -> u64 {
        files
            .iter()
            .filter(|f| f.smallest_user_key() <= largest && f.largest_user_key() >= smallest)
            .map(|f| f.file_size())
            .sum()
    }

    fn is_trivial_move(plan: &CompactionPlan) -> bool {
        if plan.source_files.len() != 1 || !plan.target_files.is_empty() {
            return false;
        }
        let Some((smallest, largest)) = Self::user_key_range(&plan.source_files) else {
            return false;
        };
        let overlap_bytes = Self::overlap_bytes(&plan.grandparent_files, &smallest, &largest);
        overlap_bytes <= plan.grandparent_overlap_bytes_limit
    }

    fn next_compact_pointer(plan: &CompactionPlan) -> Option<Vec<u8>> {
        let (_, largest) =
            Self::user_key_range_iter(plan.source_files.iter().chain(plan.target_files.iter()))?;
        Some(largest)
    }

    fn compact_one_level(
        lsm_state: &Arc<RwLock<LSMState>>,
        version_set: &Arc<RwLock<VersionSet>>,
        sstable_paths: &Arc<SstablePaths>,
        snapshot_seqs: &[u64],
        plan: CompactionPlan,
        bloom_prefix_extractor_len: usize,
    ) -> bool {
        let mut version_edit = VersionEdit::new();
        for file in &plan.source_files {
            version_edit.delete_file(plan.source_level, file.file_id);
        }
        for file in &plan.target_files {
            version_edit.delete_file(plan.target_level, file.file_id);
        }
        if let Some(pointer) = Self::next_compact_pointer(&plan) {
            version_edit.set_compact_pointer(plan.source_level, pointer);
        }

        if Self::is_trivial_move(&plan) {
            let source_file = &plan.source_files[0];
            version_edit.add_file(
                plan.target_level,
                NewFile::new_with_props(source_file.file_id, source_file.props.clone()),
            );
            let (next_file_number, base_last_sequence) = {
                let vs = version_set.read().unwrap();
                (vs.next_file_number(), vs.last_sequence())
            };
            version_edit.set_next_file_number(next_file_number);
            version_edit.set_last_sequence(base_last_sequence);

            let current_version = {
                let mut vs = version_set.write().unwrap();
                match vs.apply_edit(version_edit) {
                    Ok(()) => vs.current(),
                    Err(e) => {
                        error!(
                            "L{}->L{} trivial move apply edit failed: {}",
                            plan.source_level, plan.target_level, e
                        );
                        return false;
                    }
                }
            };

            let mut state = lsm_state.write().unwrap();
            state.version = current_version;
            return true;
        }

        let mut stream = match CompactionStream::from_files(
            sstable_paths,
            &plan.source_files,
            &plan.target_files,
        ) {
            Ok(stream) => stream,
            Err(e) => {
                error!(
                    "L{}->L{} compaction read failed: {}",
                    plan.source_level, plan.target_level, e
                );
                return false;
            }
        };

        let (outputs, max_seq) = match Self::build_compacted_sstables(
            version_set,
            sstable_paths,
            &mut stream,
            snapshot_seqs,
            &plan.grandparent_files,
            plan.grandparent_overlap_bytes_limit,
            bloom_prefix_extractor_len,
        ) {
            Ok(result) => result,
            Err(e) => {
                error!(
                    "L{}->L{} compaction build failed: {}",
                    plan.source_level, plan.target_level, e
                );
                return false;
            }
        };

        let output_file_ids: Vec<u64> = outputs.iter().map(|(id, _)| *id).collect();
        for (file_id, props) in outputs {
            version_edit.add_file(plan.target_level, NewFile::new_with_props(file_id, props));
        }
        let (next_file_number, base_last_sequence) = {
            let vs = version_set.read().unwrap();
            (vs.next_file_number(), vs.last_sequence())
        };
        version_edit.set_next_file_number(next_file_number);
        version_edit.set_last_sequence(std::cmp::max(base_last_sequence, max_seq));

        let current_version = {
            let mut vs = version_set.write().unwrap();
            match vs.apply_edit(version_edit) {
                Ok(()) => vs.current(),
                Err(e) => {
                    Self::cleanup_generated_sstables(sstable_paths, &output_file_ids);
                    error!(
                        "L{}->L{} compaction apply edit failed: {}",
                        plan.source_level, plan.target_level, e
                    );
                    return false;
                }
            }
        };

        let mut state = lsm_state.write().unwrap();
        state.version = current_version;
        true
    }

    fn build_compacted_sstables(
        version_set: &Arc<RwLock<VersionSet>>,
        sstable_paths: &SstablePaths,
        stream: &mut CompactionStream,
        snapshot_seqs: &[u64],
        grandparent_files: &[Arc<FileMetadata>],
        grandparent_overlap_bytes_limit: u64,
        bloom_prefix_extractor_len: usize,
    ) -> GoatResult<(Vec<(u64, TableProperties)>, u64)> {
        let mut outputs = Vec::new();
        let mut current_file_id: Option<u64> = None;
        let mut builder: Option<SSTableBuilder> = None;
        let mut current_smallest_user: Option<Vec<u8>> = None;
        let mut current_entries = 0usize;
        let mut max_seq = 0u64;
        let mut last_emitted_user_key: Option<Vec<u8>> = None;
        let mut last_emitted_snapshot_stripe: Option<u64> = None;

        while let Some((internal_key, value)) = stream.next_entry()? {
            max_seq = max_seq.max(internal_key.sequence_number());
            let snapshot_stripe =
                Self::find_earliest_visible_snapshot(snapshot_seqs, internal_key.sequence_number());
            let same_user_key = last_emitted_user_key
                .as_ref()
                .map(|k| k.as_slice() == internal_key.user_key())
                .unwrap_or(false);

            if same_user_key && last_emitted_snapshot_stripe == Some(snapshot_stripe) {
                continue;
            }

            if current_entries > 0 {
                if let Some(current_smallest) = current_smallest_user.as_ref() {
                    let overlap_bytes = Self::overlap_bytes(
                        grandparent_files,
                        current_smallest.as_slice(),
                        internal_key.user_key(),
                    );
                    if overlap_bytes > grandparent_overlap_bytes_limit {
                        if let (Some(mut current_builder), Some(file_id)) =
                            (builder.take(), current_file_id.take())
                        {
                            let props = match current_builder.finish() {
                                Ok(props) => props,
                                Err(e) => {
                                    let output_ids =
                                        outputs.iter().map(|(id, _)| *id).collect::<Vec<_>>();
                                    Self::cleanup_generated_sstables(sstable_paths, &output_ids);
                                    return Err(e);
                                }
                            };
                            outputs.push((file_id, props));
                        }
                        current_smallest_user = None;
                        current_entries = 0;
                    }
                }
            }

            if builder.is_none() {
                let file_id = {
                    let mut vs = version_set.write().unwrap();
                    vs.allocate_file_number()
                };
                builder = Some(
                    match SSTableBuilder::new_with_bloom_prefix_extractor(
                        file_id,
                        sstable_paths,
                        bloom_prefix_extractor_len,
                    ) {
                        Ok(builder) => builder,
                        Err(e) => {
                            let output_ids = outputs.iter().map(|(id, _)| *id).collect::<Vec<_>>();
                            Self::cleanup_generated_sstables(sstable_paths, &output_ids);
                            return Err(e);
                        }
                    },
                );
                current_file_id = Some(file_id);
                current_smallest_user = Some(internal_key.user_key().to_vec());
            }

            let key = internal_key.serialize();
            if let Some(current_builder) = builder.as_mut() {
                if let Err(e) = current_builder.write(&key, &value) {
                    let output_ids = outputs.iter().map(|(id, _)| *id).collect::<Vec<_>>();
                    Self::cleanup_generated_sstables(sstable_paths, &output_ids);
                    return Err(e);
                }
            }
            current_entries += 1;
            last_emitted_user_key = Some(internal_key.user_key().to_vec());
            last_emitted_snapshot_stripe = Some(snapshot_stripe);
        }

        if let (Some(mut current_builder), Some(file_id)) = (builder.take(), current_file_id.take())
        {
            let props = match current_builder.finish() {
                Ok(props) => props,
                Err(e) => {
                    let output_ids = outputs.iter().map(|(id, _)| *id).collect::<Vec<_>>();
                    Self::cleanup_generated_sstables(sstable_paths, &output_ids);
                    return Err(e);
                }
            };
            outputs.push((file_id, props));
        }

        Ok((outputs, max_seq))
    }

    fn find_earliest_visible_snapshot(snapshot_seqs: &[u64], sequence: u64) -> u64 {
        match snapshot_seqs.binary_search(&sequence) {
            Ok(idx) => snapshot_seqs[idx],
            Err(idx) => snapshot_seqs.get(idx).copied().unwrap_or(u64::MAX),
        }
    }

    fn cleanup_generated_sstables(sstable_paths: &SstablePaths, file_ids: &[u64]) {
        for file_id in file_ids {
            let path = sstable_paths.sstable_path_by_id(*file_id);
            if let Err(e) = std::fs::remove_file(&path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!(
                        "Failed to cleanup orphan compaction output {:?}: {}",
                        path, e
                    );
                }
            }
        }
    }

    fn user_key_range(files: &[Arc<FileMetadata>]) -> Option<(Vec<u8>, Vec<u8>)> {
        Self::user_key_range_iter(files.iter())
    }

    fn user_key_range_iter<'a, I>(files: I) -> Option<(Vec<u8>, Vec<u8>)>
    where
        I: IntoIterator<Item = &'a Arc<FileMetadata>>,
    {
        let mut smallest: Option<Vec<u8>> = None;
        let mut largest: Option<Vec<u8>> = None;
        for file in files {
            let s = file.smallest_key();
            let l = file.largest_key();
            if s.len() < 8 || l.len() < 8 {
                return None;
            }
            let s_user = s[..s.len() - 8].to_vec();
            let l_user = l[..l.len() - 8].to_vec();
            smallest = Some(match smallest {
                Some(cur) => cur.min(s_user),
                None => s_user,
            });
            largest = Some(match largest {
                Some(cur) => cur.max(l_user),
                None => l_user,
            });
        }
        Some((smallest?, largest?))
    }
}

impl Drop for FlushWorker {
    fn drop(&mut self) {
        // Close flush channel and wait flush thread first, so it releases
        // its compaction sender clone.
        let (flush_tx, _flush_rx) = mpsc::channel();
        let old_flush_sender = std::mem::replace(&mut self.flush_sender, flush_tx);
        drop(old_flush_sender);

        if let Some(handle) = self.flush_handle.take() {
            let _ = handle.join();
        }

        // Then close compaction channel and join compaction thread.
        let (compaction_tx, _compaction_rx) = mpsc::channel();
        let old_compaction_sender = std::mem::replace(&mut self.compaction_sender, compaction_tx);
        drop(old_compaction_sender);

        if let Some(handle) = self.compaction_handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::goatkv::core::kv_engine::KvEngine;
    use crate::goatkv::core::mem_table::MemTable;
    use crate::goatkv::format::internal_key::InternalKeyKind;
    use crate::goatkv::metadata::version_set::VersionSetOptions;
    use crate::goatkv::utils::cleanup_task::CleanupTask;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::mpsc::unbounded_channel;

    fn make_file(
        file_id: u64,
        smallest_user: &[u8],
        largest_user: &[u8],
        file_size: u64,
    ) -> Arc<FileMetadata> {
        let smallest =
            InternalKey::new(smallest_user.to_vec(), 1, InternalKeyKind::Put).serialize();
        let largest = InternalKey::new(largest_user.to_vec(), 1, InternalKeyKind::Put).serialize();
        let props = TableProperties::new(file_size, smallest, largest, 1, 1);
        let (tx, _rx) = unbounded_channel::<CleanupTask>();
        Arc::new(FileMetadata::from_props(file_id, props, tx))
    }

    fn make_version(files: Vec<Vec<Arc<FileMetadata>>>) -> Version {
        let base = std::env::temp_dir().join(format!(
            "goatdb_flush_worker_test_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time went backwards")
                .as_nanos()
        ));
        let sstable_paths = Arc::new(SstablePaths::new(base.join("sst"), base.join("tmp")));
        Version::from_files(files, 0, sstable_paths)
    }

    fn build_sstable_file(
        sstable_paths: &SstablePaths,
        obsolete_sender: tokio::sync::mpsc::UnboundedSender<CleanupTask>,
        file_id: u64,
        entries: Vec<(InternalKey, Vec<u8>)>,
    ) -> Arc<FileMetadata> {
        let mut builder = SSTableBuilder::new(file_id, sstable_paths).expect("create sstable");
        for (key, value) in entries {
            let raw_key = key.serialize();
            builder
                .write(&raw_key, &value)
                .expect("write sstable entry");
        }
        let props = builder.finish().expect("finish sstable");
        Arc::new(FileMetadata::from_props(file_id, props, obsolete_sender))
    }

    #[test]
    fn pick_seed_files_respects_compaction_pointer() {
        let l1_files = vec![
            make_file(1, b"a", b"c", 100),
            make_file(2, b"d", b"f", 100),
            make_file(3, b"g", b"i", 100),
        ];
        let version = make_version(vec![Vec::new(), l1_files.clone(), Vec::new()]);

        let mut pointers = vec![None; 3];
        pointers[1] = Some(b"e".to_vec());
        let picked =
            FlushWorker::pick_seed_files(&version, 1, &pointers).expect("should pick seed file");
        assert_eq!(picked[0].file_id, 2);

        pointers[1] = Some(b"z".to_vec());
        let wrapped =
            FlushWorker::pick_seed_files(&version, 1, &pointers).expect("should wrap to first");
        assert_eq!(wrapped[0].file_id, 1);
    }

    #[test]
    fn trivial_move_respects_grandparent_overlap_limit() {
        let source_file = make_file(10, b"d", b"f", 100);
        let grandparent_file = make_file(20, b"e", b"h", 600);

        let strict_plan = CompactionPlan {
            source_level: 1,
            target_level: 2,
            source_files: vec![Arc::clone(&source_file)],
            target_files: Vec::new(),
            grandparent_files: vec![Arc::clone(&grandparent_file)],
            grandparent_overlap_bytes_limit: 500,
        };
        assert!(!FlushWorker::is_trivial_move(&strict_plan));

        let loose_plan = CompactionPlan {
            source_level: 1,
            target_level: 2,
            source_files: vec![source_file],
            target_files: Vec::new(),
            grandparent_files: vec![grandparent_file],
            grandparent_overlap_bytes_limit: 1000,
        };
        assert!(FlushWorker::is_trivial_move(&loose_plan));
    }

    #[test]
    fn flush_failure_streak_opens_and_success_resets_circuit() {
        let version = Arc::new(make_version(vec![Vec::new(), Vec::new()]));
        let lsm_state = Arc::new(RwLock::new(LSMState::new(
            Arc::new(MemTable::new(1024)),
            version,
        )));

        FlushWorker::record_flush_failure(&lsm_state, 2, "first failure");
        {
            let state = lsm_state.read().unwrap();
            assert_eq!(state.flush_failure_streak, 1);
            assert!(!state.flush_circuit_open);
        }

        FlushWorker::record_flush_failure(&lsm_state, 2, "second failure");
        {
            let state = lsm_state.read().unwrap();
            assert_eq!(state.flush_failure_streak, 2);
            assert!(state.flush_circuit_open);
        }

        FlushWorker::reset_flush_failure_streak(&lsm_state);
        {
            let state = lsm_state.read().unwrap();
            assert_eq!(state.flush_failure_streak, 0);
            assert!(!state.flush_circuit_open);
        }
    }

    #[test]
    fn compaction_apply_edit_failure_cleans_generated_sstable() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let (_wal_paths, sstable_paths, manifest_paths) =
            KvEngine::init_db_paths(temp_dir.path()).expect("init paths");

        let (obsolete_tx, _obsolete_rx) = unbounded_channel::<CleanupTask>();
        let source_file = build_sstable_file(
            &sstable_paths,
            obsolete_tx.clone(),
            100,
            vec![(
                InternalKey::new(b"k1".to_vec(), 10, InternalKeyKind::Put),
                b"v1".to_vec(),
            )],
        );
        let target_file = build_sstable_file(
            &sstable_paths,
            obsolete_tx.clone(),
            101,
            vec![(
                InternalKey::new(b"k0".to_vec(), 9, InternalKeyKind::Put),
                b"v0".to_vec(),
            )],
        );

        let base_version = Arc::new(Version::from_files(
            vec![
                vec![Arc::clone(&source_file)],
                vec![Arc::clone(&target_file)],
                Vec::new(),
            ],
            10,
            sstable_paths.clone(),
        ));
        let lsm_state = Arc::new(RwLock::new(LSMState::new(
            Arc::new(MemTable::new(4 * 1024)),
            base_version,
        )));

        // Create a VersionSet without manifest writer. Compaction edit commit will fail.
        let version_set = VersionSet::new_with_options(
            manifest_paths,
            sstable_paths.clone(),
            VersionSetOptions {
                num_levels: 3,
                ..VersionSetOptions::default()
            },
            obsolete_tx,
        )
        .expect("create version set");
        let version_set = Arc::new(RwLock::new(version_set));

        let expected_output_file_id = version_set.read().unwrap().next_file_number();
        let plan = CompactionPlan {
            source_level: 0,
            target_level: 1,
            source_files: vec![source_file],
            target_files: vec![target_file],
            grandparent_files: Vec::new(),
            grandparent_overlap_bytes_limit: u64::MAX,
        };

        let ok =
            FlushWorker::compact_one_level(&lsm_state, &version_set, &sstable_paths, &[], plan, 0);
        assert!(!ok, "compaction should fail when apply_edit fails");
        assert!(
            version_set.read().unwrap().next_file_number() > expected_output_file_id,
            "compaction should have allocated output file id before commit failure"
        );

        let orphan_output = sstable_paths.sstable_path_by_id(expected_output_file_id);
        assert!(
            !orphan_output.exists(),
            "orphan compaction output should be cleaned: {:?}",
            orphan_output
        );
    }

    #[test]
    fn compaction_keeps_snapshot_stripes_for_same_user_key() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let (_wal_paths, sstable_paths, manifest_paths) =
            KvEngine::init_db_paths(temp_dir.path()).expect("init paths");
        let (obsolete_tx, _obsolete_rx) = unbounded_channel::<CleanupTask>();

        let source_file = build_sstable_file(
            &sstable_paths,
            obsolete_tx.clone(),
            200,
            vec![
                (
                    InternalKey::new(b"k1".to_vec(), 30, InternalKeyKind::Put),
                    b"v30".to_vec(),
                ),
                (
                    InternalKey::new(b"k1".to_vec(), 20, InternalKeyKind::Put),
                    b"v20".to_vec(),
                ),
                (
                    InternalKey::new(b"k1".to_vec(), 10, InternalKeyKind::Put),
                    b"v10".to_vec(),
                ),
                (
                    InternalKey::new(b"k2".to_vec(), 8, InternalKeyKind::Put),
                    b"v8".to_vec(),
                ),
            ],
        );
        let source_files = vec![source_file];
        let target_files = Vec::new();
        let version_set = Arc::new(RwLock::new(
            VersionSet::new_with_options(
                manifest_paths,
                sstable_paths.clone(),
                VersionSetOptions {
                    num_levels: 3,
                    ..VersionSetOptions::default()
                },
                obsolete_tx.clone(),
            )
            .expect("create version set"),
        ));

        let mut stream_without_snapshot =
            CompactionStream::from_files(&sstable_paths, &source_files, &target_files)
                .expect("build stream without snapshot");
        let (outputs_without_snapshot, _) = FlushWorker::build_compacted_sstables(
            &version_set,
            &sstable_paths,
            &mut stream_without_snapshot,
            &[],
            &[],
            u64::MAX,
            0,
        )
        .expect("build compacted sstable without snapshot");
        assert_eq!(outputs_without_snapshot.len(), 1);
        let output_file_id = outputs_without_snapshot[0].0;
        let entries_without_snapshot =
            SSTableReader::open(sstable_paths.sstable_path_by_id(output_file_id))
                .expect("open output sstable without snapshot")
                .scan_all()
                .expect("scan output sstable without snapshot");
        let k1_versions_without_snapshot: Vec<u64> = entries_without_snapshot
            .iter()
            .filter(|(key, _)| key.user_key() == b"k1")
            .map(|(key, _)| key.sequence_number())
            .collect();
        assert_eq!(k1_versions_without_snapshot, vec![30]);

        let mut stream_with_snapshot =
            CompactionStream::from_files(&sstable_paths, &source_files, &target_files)
                .expect("build stream with snapshot");
        let (outputs_with_snapshot, _) = FlushWorker::build_compacted_sstables(
            &version_set,
            &sstable_paths,
            &mut stream_with_snapshot,
            &[15],
            &[],
            u64::MAX,
            0,
        )
        .expect("build compacted sstable with snapshot");
        assert_eq!(outputs_with_snapshot.len(), 1);
        let output_file_id = outputs_with_snapshot[0].0;
        let entries_with_snapshot =
            SSTableReader::open(sstable_paths.sstable_path_by_id(output_file_id))
                .expect("open output sstable with snapshot")
                .scan_all()
                .expect("scan output sstable with snapshot");
        let k1_versions_with_snapshot: Vec<u64> = entries_with_snapshot
            .iter()
            .filter(|(key, _)| key.user_key() == b"k1")
            .map(|(key, _)| key.sequence_number())
            .collect();
        assert_eq!(k1_versions_with_snapshot, vec![30, 10]);

        let source_after_snapshot: Vec<Arc<FileMetadata>> = outputs_with_snapshot
            .iter()
            .map(|(file_id, props)| {
                Arc::new(FileMetadata::from_props_with_sstable_paths(
                    *file_id,
                    props.clone(),
                    obsolete_tx.clone(),
                    &sstable_paths,
                ))
            })
            .collect();
        let mut stream_after_release =
            CompactionStream::from_files(&sstable_paths, &source_after_snapshot, &target_files)
                .expect("build stream after snapshot release");
        let (outputs_after_release, _) = FlushWorker::build_compacted_sstables(
            &version_set,
            &sstable_paths,
            &mut stream_after_release,
            &[],
            &[],
            u64::MAX,
            0,
        )
        .expect("build compacted sstable after snapshot release");
        assert_eq!(outputs_after_release.len(), 1);
        let output_file_id = outputs_after_release[0].0;
        let entries_after_release =
            SSTableReader::open(sstable_paths.sstable_path_by_id(output_file_id))
                .expect("open output sstable after snapshot release")
                .scan_all()
                .expect("scan output sstable after snapshot release");
        let k1_versions_after_release: Vec<u64> = entries_after_release
            .iter()
            .filter(|(key, _)| key.user_key() == b"k1")
            .map(|(key, _)| key.sequence_number())
            .collect();
        assert_eq!(k1_versions_after_release, vec![30]);
    }
}
