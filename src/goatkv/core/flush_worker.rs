use std::sync::{mpsc, Arc, RwLock};
use std::thread;

use crate::goatkv::core::lsm_state::LSMState;
use crate::goatkv::core::mem_table::MemTable;
use crate::goatkv::metadata::version_edit::VersionEdit;
use crate::goatkv::metadata::version_set::VersionSet;
use crate::goatkv::storage::sstable_builder::SSTableBuilder;

/// 刷盘任务
#[derive(Debug)]
pub struct FlushTask {
    #[allow(dead_code)]
    pub(crate) mem_table: Arc<MemTable>,
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
    /// - `lsm_state`: LSM 状态管理器，用于访问 immutable memtables
    /// - `version_set`: VersionSet 用于记录 SSTable 元数据变更
    ///
    /// # 返回
    /// 返回新创建的 FlushWorker 实例
    pub fn new(lsm_state: Arc<RwLock<LSMState>>, version_set: Arc<RwLock<VersionSet>>) -> Self {
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            Self::run_loop(rx, lsm_state, version_set);
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
        version_set: Arc<RwLock<VersionSet>>,
    ) {
        while let Ok(task) = rx.recv() {
            // 分配文件 ID
            let file_id = {
                let mut vs = version_set.write().unwrap();
                vs.allocate_file_number()
            };

            let mut sst_builder = match SSTableBuilder::new(file_id) {
                Ok(builder) => builder,
                Err(e) => {
                    eprintln!("Failed to create SSTableBuilder: {}", e);
                    continue;
                }
            };

            // 获取 immutable_memtable 并克隆数据，避免长时间持有锁
            let entries: Vec<(Vec<u8>, Vec<u8>)> = {
                let lsm_state_guard = lsm_state.read().unwrap();
                let imm_table = match lsm_state_guard.immutable_mem_tables.front() {
                    Some(t) => t,
                    None => {
                        eprintln!("No immutable memtable found for task id={}", task.id);
                        continue;
                    }
                };

                imm_table
                    .iter()
                    .map(|(key, value)| {
                        let mut serialized_key = Vec::new();
                        serialized_key.extend_from_slice(key.user_key());
                        // 关键修正：使用 Big Endian 并取反 (!seq)
                        // 原因：
                        // 1. 我们希望 Sequence Number 越大，Key 越小 (Logical Order: Seq Desc)
                        // 2. SSTable 字节序排序是 Ascending
                        // 3. !seq (取反) 后，大 Seq 变成小数值
                        // 4. Big Endian 保证字节序比较等同于数值比较
                        // 例如:
                        // Seq 200 (Encoded) -> !200 -> Small Value -> Small Bytes -> First in SSTable
                        // Seq 100 (Encoded) -> !100 -> Large Value -> Large Bytes -> Later in SSTable
                        serialized_key
                            .extend_from_slice(&(!key.encoded_sequence_number()).to_be_bytes());
                        (serialized_key, value.to_vec())
                    })
                    .collect()
            };

            // 在不持有锁的情况下写入 SSTable
            for (key, value) in entries {
                sst_builder.write(&key, &value);
            }

            let metadata = match sst_builder.finish() {
                Ok(meta) => meta,
                Err(e) => {
                    eprintln!("Failed to finish SSTable {}: {}", task.id, e);
                    continue;
                }
            };

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
            // 注意：需要获取写锁
            let mut lsm_state_guard = lsm_state.write().unwrap();
            lsm_state_guard.immutable_mem_tables.pop_front();
        }
    }
}
