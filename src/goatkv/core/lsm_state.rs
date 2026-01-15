use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::RwLock;

use crate::goatkv::core::mem_table::{ImmutableMemTable, MemTable};
use crate::goatkv::metadata::version_set::{VersionSet, VersionSetOptions};
use crate::goatkv::utils::options::KvEngineOptions;

#[derive(Debug)]
pub struct LSMState {
    /// 内存表（可写）
    pub mem_table: Arc<MemTable>,
    /// 不可变内存表队列（待刷盘）
    pub immutable_mem_tables: VecDeque<Arc<ImmutableMemTable>>,
    /// VersionSet 管理所有 SSTable 元数据
    pub version_set: Arc<RwLock<VersionSet>>,
}

impl LSMState {
    pub fn new(options: &KvEngineOptions) -> Self {
        // 从 KvEngineOptions 创建 VersionSetOptions
        let vs_options = VersionSetOptions {
            max_versions: options.max_versions,
            manifest_max_size: options.manifest_max_size,
            manifest_rewrite_edit_count: options.manifest_rewrite_edit_count,
            num_levels: options.num_levels,
        };

        // 创建 VersionSet
        let version_set = Arc::new(RwLock::new(
            VersionSet::new_with_options(
                &options.data_dir,
                "leveldb.BytewiseComparator".to_string(),
                vs_options,
            )
            .expect("Failed to create VersionSet"),
        ));

        LSMState {
            mem_table: Arc::new(MemTable::new(options.mem_table_size)),
            immutable_mem_tables: VecDeque::new(),
            version_set,
        }
    }
}
