use std::collections::VecDeque;
use std::sync::Arc;

use crate::goatkv::mem_table::{ImmutableMemTable, MemTable};
use crate::goatkv::options::KvEngineOptions;

#[derive(Debug)]
pub struct LSMState {
    /// 内存表（可写）
    pub mem_table: Arc<MemTable>,
    /// 不可变内存表队列（待刷盘）
    pub immutable_mem_tables: VecDeque<Arc<ImmutableMemTable>>,
}

impl LSMState {
    pub fn new(options: &KvEngineOptions) -> Self {
        LSMState {
            mem_table: Arc::new(MemTable::new(options.mem_table_size)),
            immutable_mem_tables: VecDeque::new(),
        }
    }
}
