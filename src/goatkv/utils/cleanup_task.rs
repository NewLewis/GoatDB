#[derive(Debug, Clone)]
pub enum CleanupTask {
    Sstable(u64),
    Wal(u64),
}
