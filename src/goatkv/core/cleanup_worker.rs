use std::fs;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;

use crate::goatkv::utils::cleanup_task::CleanupTask;
use crate::goatkv::storage::wal::WalPaths;
use crate::goatkv::utils::paths::SstablePaths;

#[derive(Debug)]
pub struct CleanupWorker {
    // 保存句柄，防止线程被 detach，也方便未来实现 Drop 时的 graceful shutdown
    _handle: thread::JoinHandle<()>,
}

impl CleanupWorker {
    /// 创建并启动清理线程
    ///
    /// # 参数
    /// - `wal_paths`: WAL 路径集合
    /// - `sstable_paths`: SSTable 路径集合
    ///
    /// # Returns
    /// - `Self`: Worker 实例（持有线程句柄）
    /// - `Sender<CleanupTask>`: 删除信号发送端，你需要把这个传给 VersionSet
    pub fn new(
        wal_paths: Arc<WalPaths>,
        sstable_paths: Arc<SstablePaths>,
    ) -> (Self, Sender<CleanupTask>) {
        // 1. 在内部创建通道
        let (tx, rx) = mpsc::channel();

        // 2. 在内部启动线程
        // 注意：这里把 path 和 rx move 进去了，不需要 self 参与
        let handle = thread::spawn(move || {
            Self::run_loop(rx, wal_paths, sstable_paths);
        });

        // 3. 返回 Worker 实例和 Sender
        (
            Self { _handle: handle },
            tx, // 把 Sender 抛出去给外部使用
        )
    }

    /// 后台主循环
    fn run_loop(
        rx: Receiver<CleanupTask>,
        wal_paths: Arc<WalPaths>,
        sstable_paths: Arc<SstablePaths>,
    ) {
        // 只要 tx 还有人持有，recv 就会阻塞等待；tx 全部销毁，recv 返回 Err，循环退出
        while let Ok(task) = rx.recv() {
            let (file_path, label) = match task {
                CleanupTask::Sstable(file_number) => {
                    (sstable_paths.sstable_path_by_id(file_number), "sstable")
                }
                CleanupTask::Wal(log_number) => (wal_paths.wal_path_by_id(log_number), "wal"),
            };

            match fs::remove_file(&file_path) {
                Ok(_) => {
                    // TODO: 替换为实际的日志宏
                    println!("[CleanUp] Deleted {}: {:?}", label, file_path);
                }
                Err(e) => {
                    // 忽略文件不存在的错误，可能是重复删除
                    if e.kind() != std::io::ErrorKind::NotFound {
                        eprintln!("[CleanUp] Failed to delete {:?}: {}", file_path, e);
                    }
                }
            }
        }
        println!("[CleanUp] Worker thread stopped.");
    }
}
