use std::sync::{mpsc, Arc, RwLock};
use std::thread;

use crate::goatkv::core::lsm_state::LSMState;
use crate::goatkv::format::internal_key::{InternalKey, InternalKeyKind};
use crate::goatkv::metadata::file_metadata::FileMetadata;
use crate::goatkv::metadata::version::Version;
use crate::goatkv::metadata::version_edit::{NewFile, VersionEdit};
use crate::goatkv::metadata::version_set::VersionSet;
use crate::goatkv::storage::sstable::{SSTableBuilder, SSTableReader};
use crate::goatkv::utils::options::KvEngineOptions;
use crate::goatkv::utils::paths::SstablePaths;
use tracing::{info, warn};

#[derive(Debug)]
pub enum CompactionTask {
    Maybe,
}

#[derive(Debug)]
struct CompactionPlan {
    level: usize,
    target_level: usize,
    level_files: Vec<Arc<FileMetadata>>,
    next_level_files: Vec<Arc<FileMetadata>>,
}

#[derive(Debug)]
struct CompactionEntry {
    key: InternalKey,
    value: Vec<u8>,
}

#[derive(Debug)]
pub struct CompactionWorker {
    sender: mpsc::Sender<CompactionTask>,
    handle: Option<thread::JoinHandle<()>>,
}

impl CompactionWorker {
    pub fn new(
        lsm_state: Arc<RwLock<LSMState>>,
        version_set: Arc<RwLock<VersionSet>>,
        sstable_paths: Arc<SstablePaths>,
        options: Arc<KvEngineOptions>,
    ) -> Self {
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            Self::run_loop(rx, lsm_state, version_set, sstable_paths, options);
        });
        Self {
            sender: tx,
            handle: Some(handle),
        }
    }

    pub fn submit_task(&self, task: CompactionTask) -> Result<(), mpsc::SendError<CompactionTask>> {
        self.sender.send(task)
    }

    fn run_loop(
        rx: mpsc::Receiver<CompactionTask>,
        lsm_state: Arc<RwLock<LSMState>>,
        version_set: Arc<RwLock<VersionSet>>,
        sstable_paths: Arc<SstablePaths>,
        options: Arc<KvEngineOptions>,
    ) {
        while rx.recv().is_ok() {
            loop {
                let plan_with_level = {
                    let vs_guard = version_set.read().unwrap();
                    let version = vs_guard.current();
                    let num_levels = version.num_levels();
                    if num_levels < 2 {
                        None
                    } else {
                        let level_targets = Self::level_targets(&options, num_levels);
                        if !version.needs_compaction(
                            &level_targets,
                            options.level0_file_num_compaction_trigger,
                        ) {
                            None
                        } else {
                            let plan =
                                Self::pick_compaction_plan(&version, &level_targets, &options);
                            plan.map(|plan| (plan, num_levels - 1))
                        }
                    }
                };

                let Some((plan, last_level)) = plan_with_level else {
                    break;
                };

                if let Err(err) =
                    Self::run_compaction(plan, last_level, &sstable_paths, &version_set, &lsm_state)
                {
                    warn!("Compaction failed: {}", err);
                    break;
                }
            }
        }
        info!("[Compaction] Worker thread stopped.");
    }

    fn level_targets(options: &KvEngineOptions, num_levels: usize) -> Vec<u64> {
        let mut targets = vec![0u64; num_levels];
        let mut size = options.level_base_size_bytes.max(1);
        for level in 1..num_levels {
            targets[level] = size;
            size = size.saturating_mul(options.level_size_multiplier.max(1));
        }
        targets
    }

    fn pick_compaction_plan(
        version: &Version,
        level_targets: &[u64],
        options: &KvEngineOptions,
    ) -> Option<CompactionPlan> {
        let num_levels = version.num_levels();
        if num_levels < 2 {
            return None;
        }

        if version.get_files(0).len() > options.level0_file_num_compaction_trigger {
            let level_files = version.get_files(0).to_vec();
            if level_files.is_empty() {
                return None;
            }
            let (smallest_key, largest_key) = Self::key_range(&level_files)?;
            let next_level_files = version.get_overlapping_files(1, &smallest_key, &largest_key);
            return Some(CompactionPlan {
                level: 0,
                target_level: 1,
                level_files,
                next_level_files,
            });
        }

        for level in 1..num_levels - 1 {
            if level < level_targets.len() && version.get_level_size(level) > level_targets[level] {
                let Some(first) = version.get_files(level).first() else {
                    continue;
                };
                let level_files = vec![Arc::clone(first)];
                let smallest_key = first.smallest_key().to_vec();
                let largest_key = first.largest_key().to_vec();
                let next_level_files =
                    version.get_overlapping_files(level + 1, &smallest_key, &largest_key);
                return Some(CompactionPlan {
                    level,
                    target_level: level + 1,
                    level_files,
                    next_level_files,
                });
            }
        }

        None
    }

    fn key_range(files: &[Arc<FileMetadata>]) -> Option<(Vec<u8>, Vec<u8>)> {
        let mut smallest = None;
        let mut largest = None;
        for file in files {
            if smallest
                .as_ref()
                .map_or(true, |key: &Vec<u8>| file.smallest_key() < key.as_slice())
            {
                smallest = Some(file.smallest_key().to_vec());
            }
            if largest
                .as_ref()
                .map_or(true, |key: &Vec<u8>| file.largest_key() > key.as_slice())
            {
                largest = Some(file.largest_key().to_vec());
            }
        }
        Some((smallest?, largest?))
    }

    fn run_compaction(
        plan: CompactionPlan,
        last_level: usize,
        sstable_paths: &SstablePaths,
        version_set: &Arc<RwLock<VersionSet>>,
        lsm_state: &Arc<RwLock<LSMState>>,
    ) -> Result<(), String> {
        let mut entries = Vec::new();
        for file in plan.level_files.iter().chain(plan.next_level_files.iter()) {
            Self::load_entries(file, sstable_paths, &mut entries)?;
        }

        let last_sequence = {
            let vs = version_set.read().unwrap();
            vs.last_sequence()
        };

        if entries.is_empty() {
            return Self::apply_compaction_edit(
                plan,
                None,
                0,
                last_sequence,
                version_set,
                lsm_state,
            );
        }

        entries.sort_by(|a, b| a.key.cmp(&b.key));

        let mut output_entries = Vec::new();
        let mut last_user_key: Option<Vec<u8>> = None;
        for entry in entries {
            if last_user_key
                .as_ref()
                .map_or(false, |key| key.as_slice() == entry.key.user_key())
            {
                continue;
            }
            if entry.key.kind() == InternalKeyKind::Delete && plan.target_level == last_level {
                last_user_key = Some(entry.key.user_key().to_vec());
                continue;
            }
            last_user_key = Some(entry.key.user_key().to_vec());
            output_entries.push(entry);
        }

        if output_entries.is_empty() {
            return Self::apply_compaction_edit(
                plan,
                None,
                0,
                last_sequence,
                version_set,
                lsm_state,
            );
        }

        let (file_id, next_file_number) = {
            let mut vs = version_set.write().unwrap();
            let file_id = vs.allocate_file_number();
            let next_file_number = vs.next_file_number();
            (file_id, next_file_number)
        };

        let props = Self::build_sstable(file_id, sstable_paths, &output_entries)
            .map_err(|e| format!("Failed to build SSTable: {}", e))?;

        let result = Self::apply_compaction_edit(
            plan,
            Some((file_id, props)),
            next_file_number,
            last_sequence,
            version_set,
            lsm_state,
        );
        if result.is_err() {
            let _ = std::fs::remove_file(sstable_paths.sstable_path_by_id(file_id));
        }

        result
    }

    fn build_sstable(
        file_id: u64,
        sstable_paths: &SstablePaths,
        entries: &[CompactionEntry],
    ) -> Result<crate::goatkv::metadata::file_metadata::TableProperties, String> {
        let mut builder = SSTableBuilder::new(file_id, sstable_paths)
            .map_err(|e| format!("Failed to create SSTableBuilder: {}", e))?;
        for entry in entries {
            builder.write(&entry.key.serialize(), &entry.value);
        }
        builder
            .finish()
            .map_err(|e| format!("Failed to finish SSTable: {}", e))
    }

    fn apply_compaction_edit(
        plan: CompactionPlan,
        output: Option<(u64, crate::goatkv::metadata::file_metadata::TableProperties)>,
        next_file_number: u64,
        last_sequence: u64,
        version_set: &Arc<RwLock<VersionSet>>,
        lsm_state: &Arc<RwLock<LSMState>>,
    ) -> Result<(), String> {
        let mut edit = VersionEdit::new();
        for file in &plan.level_files {
            edit.delete_file(plan.level, file.file_id);
        }
        for file in &plan.next_level_files {
            edit.delete_file(plan.target_level, file.file_id);
        }
        if let Some((file_id, props)) = output {
            edit.add_file(plan.target_level, NewFile::new_with_props(file_id, props));
        }
        if next_file_number > 0 {
            edit.set_next_file_number(next_file_number);
        }
        edit.set_last_sequence(last_sequence);

        let current_version = {
            let mut vs = version_set.write().unwrap();
            vs.apply_edit(edit).map_err(|e| e.to_string())?;
            vs.current()
        };

        {
            let mut state = lsm_state.write().unwrap();
            state.version = current_version;
        }

        Ok(())
    }

    fn load_entries(
        file: &FileMetadata,
        sstable_paths: &SstablePaths,
        entries: &mut Vec<CompactionEntry>,
    ) -> Result<(), String> {
        let sstable_path = sstable_paths.sstable_path_by_id(file.file_id);
        let mut reader = SSTableReader::open(&sstable_path)
            .map_err(|e| format!("Failed to open SSTable {:?}: {}", sstable_path, e))?;
        let pairs = reader
            .iter_all()
            .map_err(|e| format!("Failed to read SSTable {:?}: {}", sstable_path, e))?;
        for (raw_key, value) in pairs {
            let Some(key) = InternalKey::parse_from_bytes(&raw_key) else {
                warn!(
                    "Skip invalid internal key in SSTable {:?} file {}",
                    sstable_path, file.file_id
                );
                continue;
            };
            entries.push(CompactionEntry { key, value });
        }
        Ok(())
    }
}

impl Drop for CompactionWorker {
    fn drop(&mut self) {
        let (tx, _rx) = mpsc::channel();
        let old_sender = std::mem::replace(&mut self.sender, tx);
        drop(old_sender);

        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
