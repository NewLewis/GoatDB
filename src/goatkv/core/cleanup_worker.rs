use std::fs;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;

#[derive(Debug)]
pub struct CleanupWorker {
    // 保存句柄，防止线程被 detach，也方便未来实现 Drop 时的 graceful shutdown
    _handle: thread::JoinHandle<()>,
}

impl CleanupWorker {
    /// 创建并启动清理线程
    ///
    /// # Returns
    /// - `Self`: Worker 实例（持有线程句柄）
    /// - `Sender<u64>`: 删除信号发送端，你需要把这个传给 VersionSet/FlushWorker
    pub fn new(db_path: PathBuf) -> (Self, Sender<u64>) {
        // 1. 在内部创建通道
        let (tx, rx) = mpsc::channel();

        // 2. 在内部启动线程
        // 注意：这里把 path 和 rx move 进去了，不需要 self 参与
        let handle = thread::spawn(move || {
            Self::run_loop(db_path, rx);
        });

        // 3. 返回 Worker 实例和 Sender
        (
            Self { _handle: handle },
            tx, // 把 Sender 抛出去给外部使用
        )
    }

    /// 后台主循环
    fn run_loop(db_path: PathBuf, rx: Receiver<u64>) {
        // 只要 tx 还有人持有，recv 就会阻塞等待；tx 全部销毁，recv 返回 Err，循环退出
        while let Ok(file_number) = rx.recv() {
            // todo 名字拼接
            let filename = format!("{:06}.sst", file_number);
            let file_path = db_path.join(&filename);

            match fs::remove_file(&file_path) {
                Ok(_) => {
                    // TODO: 替换为实际的日志宏
                    println!("[CleanUp] Deleted sstable: {:?}", file_path);
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
