use std::sync::{mpsc, Arc, RwLock};
use std::thread;

use crate::goatkv::core::lsm_state::LSMState;
use crate::goatkv::core::mem_table::ImmutableMemTable;
use crate::goatkv::metadata::version_edit::{NewFile, VersionEdit};
use crate::goatkv::storage::sstable_builder::SSTableBuilder;

/// 刷盘任务
#[derive(Debug)]
pub struct FlushTask {
    pub(crate) immutable_mem_table: Arc<ImmutableMemTable>,
    pub(crate) id: usize,
    pub(crate) wal_log_number: u64,
    pub(crate) new_log_number: u64,
}

/// 后台刷盘 Worker
///
/// 负责在独立线程中处理 MemTable 到 SSTable 的刷盘任务。
/// 使用 mpsc channel 接收任务，确保任务按顺序执行。
#[derive(Debug)]
pub struct FlushWorker {
    sender: mpsc::Sender<FlushTask>,
    _handle: thread::JoinHandle<()>,
}

impl FlushWorker {
    /// 创建新的 FlushWorker 并启动后台线程
    ///
    /// # 参数
    /// - `lsm_state`: LSM 状态管理器，用于访问 immutable memtables 和 version_set
    ///
    /// # 返回
    /// 返回新创建的 FlushWorker 实例
    pub fn new(
        lsm_state: Arc<RwLock<LSMState>>,
        obsolete_sender: mpsc::Sender<u64>,
        wal_refcounts: Arc<std::sync::Mutex<std::collections::HashMap<u64, usize>>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            Self::run_loop(rx, lsm_state, obsolete_sender, wal_refcounts);
        });

        Self {
            sender: tx,
            _handle: handle,
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
        _obsolete_sender: mpsc::Sender<u64>,
        wal_refcounts: Arc<std::sync::Mutex<std::collections::HashMap<u64, usize>>>,
    ) {
        while let Ok(task) = rx.recv() {
            // 从任务中获取要刷盘的 immutable memtable
            let imm_table = task.immutable_mem_table.clone();

            // 获取 version_set 引用和分配文件 ID
            let (file_id, next_file_number, version_set) = {
                let version_set = lsm_state.read().unwrap().version_set.clone();

                let (file_id, next_file_number) = {
                    let mut vs = version_set.write().unwrap();
                    let file_id = vs.allocate_file_number();
                    let next_file_number = vs.next_file_number();
                    (file_id, next_file_number)
                };

                (file_id, next_file_number, version_set)
            };

            let mut sst_builder = match SSTableBuilder::new(file_id) {
                Ok(builder) => builder,
                Err(e) => {
                    eprintln!("Failed to create SSTableBuilder: {}", e);
                    continue;
                }
            };

            // 在不持有锁的情况下处理数据
            let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            let mut max_sequence = 0u64;
            for (key, value) in imm_table.iter() {
                max_sequence = max_sequence.max(key.sequence_number());
                entries.push((key.serialize(), value.to_vec()));
            }

            // 在不持有锁的情况下写入 SSTable
            for (key, value) in entries {
                sst_builder.write(&key, &value);
            }

            let props = match sst_builder.finish() {
                Ok(meta) => meta,
                Err(e) => {
                    eprintln!("Failed to finish SSTable {}: {}", task.id, e);
                    continue; // 保持队列状态不变，稍后重试
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

            // 应用 VersionEdit 到 VersionSet
            {
                let mut vs = version_set.write().unwrap();
                if let Err(e) = vs.apply_edit(version_edit) {
                    eprintln!("Failed to apply VersionEdit: {}", e);
                    continue; // 不 pop_front，等待后续重试
                }
            }

            // 从 immutable_mem_tables 中移除已刷盘的 memtable
            // 注意：需要获取 lsm_state 写锁，并且需要找到对应的任务
            {
                let mut lsm_state_guard = lsm_state.write().unwrap();
                lsm_state_guard.immutable_mem_tables.pop_front();
            }

            if task.wal_log_number > 0 {
                let should_delete = {
                    let mut refs = wal_refcounts.lock().unwrap();
                    if let Some(count) = refs.get_mut(&task.wal_log_number) {
                        if *count > 1 {
                            *count -= 1;
                            false
                        } else {
                            refs.remove(&task.wal_log_number);
                            true
                        }
                    } else {
                        true
                    }
                };
                if should_delete {
                    let wal_path =
                        crate::goatkv::utils::db_path_manager::DbPathManager::global()
                            .wal_path_by_id(task.wal_log_number);
                    if let Err(e) = std::fs::remove_file(&wal_path) {
                        eprintln!("Failed to remove WAL {:?}: {}", wal_path, e);
                    }
                }
            }
        }
    }
}
