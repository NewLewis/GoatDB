use std::sync::{mpsc, Arc, RwLock};
use std::thread;

use crate::goatkv::core::lsm_state::LSMState;
use crate::goatkv::core::mem_table::ImmutableMemTable;
use crate::goatkv::metadata::file_metadata::FileMetadata;
use crate::goatkv::metadata::version_edit::VersionEdit;
use crate::goatkv::storage::sstable_builder::SSTableBuilder;

/// 刷盘任务
#[derive(Debug)]
pub struct FlushTask {
    pub(crate) immutable_mem_table: Arc<ImmutableMemTable>,
    pub(crate) id: usize,
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
    pub fn new(lsm_state: Arc<RwLock<LSMState>>, obsolete_sender: mpsc::Sender<u64>) -> Self {
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            Self::run_loop(rx, lsm_state, obsolete_sender);
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
        obsolete_sender: mpsc::Sender<u64>,
    ) {
        while let Ok(task) = rx.recv() {
            // 从任务中获取要刷盘的 immutable memtable
            let imm_table = task.immutable_mem_table.clone();

            // 获取 version_set 引用和分配文件 ID
            let (file_id, version_set) = {
                let version_set = lsm_state.read().unwrap().version_set.clone();

                let file_id = {
                    let mut vs = version_set.write().unwrap();
                    vs.allocate_file_number()
                };

                (file_id, version_set)
            };

            let mut sst_builder = match SSTableBuilder::new(file_id) {
                Ok(builder) => builder,
                Err(e) => {
                    eprintln!("Failed to create SSTableBuilder: {}", e);
                    continue;
                }
            };

            // 在不持有锁的情况下处理数据
            let entries: Vec<(Vec<u8>, Vec<u8>)> = imm_table
                .iter()
                .map(|(key, value)| (key.serialize(), value.to_vec()))
                .collect();

            // 在不持有锁的情况下写入 SSTable
            for (key, value) in entries {
                sst_builder.write(&key, &value);
            }

            let props = match sst_builder.finish() {
                Ok(meta) => meta,
                Err(e) => {
                    eprintln!("Failed to finish SSTable {}: {}", task.id, e);
                    continue;
                }
            };
            let metadata = Arc::new(FileMetadata {
                file_id,
                props,
                obsolete_sender,
            });

            // 创建 VersionEdit 记录新增的 SSTable
            let mut version_edit = VersionEdit::new();
            version_edit.add_file(0, metadata.clone());

            // 应用 VersionEdit 到 VersionSet
            {
                let mut vs = version_set.write().unwrap();
                if let Err(e) = vs.apply_edit(version_edit) {
                    eprintln!("Failed to apply VersionEdit: {}", e);
                    continue;
                }
            }

            // 从 immutable_mem_tables 中移除已刷盘的 memtable
            // 注意：需要获取 lsm_state 写锁，并且需要找到对应的任务
            let mut lsm_state_guard = lsm_state.write().unwrap();
            // 从队列前端移除（因为我们使用 push_back 和 pop_front 实现 FIFO）
            lsm_state_guard.immutable_mem_tables.pop_front();
        }
    }
}
