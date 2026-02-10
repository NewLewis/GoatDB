use std::sync::{mpsc, Arc, RwLock};
use std::thread;

use crate::goatkv::core::lsm_state::LSMState;
use crate::goatkv::core::mem_table::ImmutableMemTable;
use crate::goatkv::metadata::version::Version;
use crate::goatkv::metadata::version_edit::{NewFile, VersionEdit};
use crate::goatkv::metadata::version_set::VersionSet;
use crate::goatkv::storage::sstable::SSTableBuilder;
use crate::goatkv::utils::paths::SstablePaths;
use tracing::{error, warn};

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
    sender: mpsc::Sender<FlushTask>,
    handle: Option<thread::JoinHandle<()>>,
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
    ) -> Self {
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            Self::run_loop(rx, lsm_state, version_set, sstable_paths);
        });

        Self {
            sender: tx,
            handle: Some(handle),
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
        self.sender.send(task)
    }

    /// 后台线程主循环
    ///
    /// 持续从 channel 接收刷盘任务并执行：
    /// 1. 从 immutable memtable 读取数据
    /// 2. 创建 SSTable 文件
    /// 3. 创建 VersionEdit 并应用到 VersionSet
    /// 4. 移除已刷盘的 immutable memtable
    fn run_loop(
        rx: mpsc::Receiver<FlushTask>,
        lsm_state: Arc<RwLock<LSMState>>,
        version_set: Arc<RwLock<VersionSet>>,
        sstable_paths: Arc<SstablePaths>,
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
                        error!("Failed to apply VersionEdit: {}", e);
                        continue; // 不重试，直接退出
                    }
                    vs.current()
                };

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

            // Retry loop for SSTable creation and writing
            let props = loop {
                let mut sst_builder = match SSTableBuilder::new(file_id, &sstable_paths) {
                    Ok(builder) => builder,
                    Err(e) => {
                        error!(
                            "Failed to create SSTableBuilder (will retry in 1s): {}",
                            e
                        );
                        std::thread::sleep(std::time::Duration::from_secs(1));
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
                        error!("Failed to write entry to SSTable {} (will retry): {}", file_id, e);
                        write_failed = true;
                        break;
                    }
                }

                if write_failed {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    continue;
                }

                match sst_builder.finish() {
                    Ok(meta) => break meta,
                    Err(e) => {
                        error!("Failed to finish SSTable {} (will retry in 1s): {}", file_id, e);
                        std::thread::sleep(std::time::Duration::from_secs(1));
                        continue;
                    }
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
                    error!("Failed to apply VersionEdit: {}", e);
                    continue; // 不 pop_front，等待后续重试
                }
                vs.current()
            };

            // 从 immutable_mem_tables 中精确移除当前任务对应的 memtable。
            // 不能无条件 pop_front：若前序任务失败并未出队，会导致错删队头。
            Self::update_version_and_remove_task_memtable(&lsm_state, current_version, &imm_table);
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
}

impl Drop for FlushWorker {
    fn drop(&mut self) {
        // Close channel before joining so the worker can exit.
        let (tx, _rx) = mpsc::channel();
        let old_sender = std::mem::replace(&mut self.sender, tx);
        drop(old_sender);

        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}