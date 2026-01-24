use std::time::{SystemTime, UNIX_EPOCH};

/// WAL 文件名规则
/// - 如果 log_number < 1,000,000：格式为 `{log_number:06}.wal`（如 000001.wal）
/// - 如果 log_number >= 1,000,000：格式为 `{log_number}.wal`（如 1234567.wal）
pub fn format_wal_filename(log_number: u64) -> String {
    if log_number < 1_000_000 {
        format!("{:06}.wal", log_number)
    } else {
        format!("{}.wal", log_number)
    }
}

/// SSTable 文件名规则
/// - 如果 file_id < 1,000,000：格式为 `{file_id:06}.sst`（如 000001.sst）
/// - 如果 file_id >= 1,000,000：格式为 `{file_id}.sst`（如 1234567.sst）
pub fn format_sstable_filename(file_id: u64) -> String {
    if file_id < 1_000_000 {
        format!("{:06}.sst", file_id)
    } else {
        format!("{}.sst", file_id)
    }
}

/// 基于当前时间戳生成 SSTable 文件名（用于 flush 操作）
pub fn timestamped_sstable_filename() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("sstable_{}.db", timestamp)
}
