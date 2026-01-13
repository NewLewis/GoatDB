use serde::{Deserialize, Serialize};
use std::collections::HashSet;

// 类型别名，增加代码可读性
type Level = usize;
type FileId = u64;

/// FileMetaData 描述了一个具体的 SSTable 文件的元数据
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileMetaData {
    pub file_id: FileId,
    pub file_size: u64,
    // 最小 Key 和 最大 Key 用于读路径上的快速过滤
    // 必须存储为 Vec<u8> 因为 Key 是二进制安全的
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,

    // 可选：记录该文件的生成序列号范围，用于 MVCC
    pub smallest_seqno: u64,
    pub largest_seqno: u64,
}

/// VersionEdit 记录了一次状态变更的增量
/// 它可以被序列化后追加写到 MANIFEST 文件中
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VersionEdit {
    // 1. 全局状态字段 (Option 表示该次 Edit 是否修改了这个值)
    pub comparator_name: Option<String>, // 用于检查打开 DB 时比较器是否匹配
    pub log_number: Option<u64>,         // 当前有效的 WAL 日志编号
    pub prev_log_number: Option<u64>,    // 废弃，通常设为 0
    pub next_file_number: Option<u64>,   // 下一个可用的文件编号
    pub last_sequence: Option<u64>,      // 全局最大的 Sequence Number

    // 2. 压缩指针 (Compaction Pointer)
    // 记录每一层下一次 Compaction 应该从哪个 Key 开始
    // Vec<(Level, Key)>
    pub compact_pointers: Vec<(Level, Vec<u8>)>,

    // 3. 删除的文件
    // 记录 (Level, FileId)。为什么需要 Level？因为不同 Level 可能恰好有同名文件（虽然严格设计下很难，但带上 Level 更安全）
    pub deleted_files: HashSet<(Level, FileId)>,

    // 4. 新增的文件
    // 记录 (Level, FileMetaData)。Compaction 会产生新文件并放到特定 Level。
    pub new_files: Vec<(Level, FileMetaData)>,
}
