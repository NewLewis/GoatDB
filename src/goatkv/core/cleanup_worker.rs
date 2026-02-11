use std::sync::Arc;

use crate::goatkv::error::{Error as GoatError, Result as GoatResult};
use crate::goatkv::storage::wal::WalPaths;
use crate::goatkv::utils::cleanup_task::CleanupTask;
use crate::goatkv::utils::paths::SstablePaths;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tracing::{info, warn};

#[derive(Debug)]
pub struct CleanupWorker {
    // 保存句柄，防止后台 task 被 detach。
    _handle: tokio::task::JoinHandle<()>,
}

impl CleanupWorker {
    /// 创建并启动纯异步清理 worker（要求当前线程已在 Tokio runtime 中）
    ///
    /// # 参数
    /// - `wal_paths`: WAL 路径集合
    /// - `sstable_paths`: SSTable 路径集合
    ///
    /// # Returns
    /// - `Self`: Worker 实例（持有后台 task 句柄）
    /// - `UnboundedSender<CleanupTask>`: 删除信号发送端，你需要把这个传给 VersionSet
    pub fn new(
        wal_paths: Arc<WalPaths>,
        sstable_paths: Arc<SstablePaths>,
    ) -> GoatResult<(Self, UnboundedSender<CleanupTask>)> {
        Handle::try_current().map_err(|_| {
            GoatError::unavailable(
                "cleanup_worker",
                "tokio runtime required for async cleanup worker",
            )
        })?;

        // 1. 在内部创建通道
        let (tx, rx) = mpsc::unbounded_channel();

        // 2. 在当前 Tokio runtime 上启动异步任务
        let handle = tokio::spawn(Self::run_loop_async(rx, wal_paths, sstable_paths));
        let worker = Self { _handle: handle };

        // 3. 返回 Worker 实例和 Sender
        Ok((worker, tx))
    }

    fn task_to_path(
        task: CleanupTask,
        wal_paths: Arc<WalPaths>,
        sstable_paths: Arc<SstablePaths>,
    ) -> (std::path::PathBuf, &'static str) {
        match task {
            CleanupTask::Sstable(file_number) => {
                (sstable_paths.sstable_path_by_id(file_number), "sstable")
            }
            CleanupTask::Wal(log_number) => (wal_paths.wal_path_by_id(log_number), "wal"),
        }
    }

    fn log_delete_result(file_path: &std::path::Path, label: &str, result: std::io::Result<()>) {
        match result {
            Ok(_) => {
                info!("[CleanUp] Deleted {}: {:?}", label, file_path);
            }
            Err(e) => {
                // 忽略文件不存在的错误，可能是重复删除
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!("[CleanUp] Failed to delete {:?}: {}", file_path, e);
                }
            }
        }
    }

    /// Tokio 后台主循环
    async fn run_loop_async(
        mut rx: UnboundedReceiver<CleanupTask>,
        wal_paths: Arc<WalPaths>,
        sstable_paths: Arc<SstablePaths>,
    ) {
        while let Some(task) = rx.recv().await {
            let (file_path, label) =
                Self::task_to_path(task, wal_paths.clone(), sstable_paths.clone());
            let remove_path = file_path.clone();
            let remove_result = tokio::fs::remove_file(&remove_path).await;
            Self::log_delete_result(&file_path, label, remove_result);
        }
        info!("[CleanUp] Worker task stopped.");
    }
}
