use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use crate::goatkv::core::mem_table::{ImmutableMemTable, MemTable};
use crate::goatkv::storage::sstable_reader::SSTableReader;
use crate::goatkv::utils::options::KvEngineOptions;

#[derive(Debug)]
pub struct LSMState {
    /// 内存表（可写）
    pub mem_table: Arc<MemTable>,
    /// 不可变内存表队列（待刷盘）
    pub immutable_mem_tables: VecDeque<Arc<ImmutableMemTable>>,
    /// SSTable 列表 (L0)
    /// 使用 Mutex 因为 SSTableReader::get 需要 &mut self (文件IO)
    pub sstables: Vec<Arc<Mutex<SSTableReader>>>,
}

impl LSMState {
    pub fn new(options: &KvEngineOptions) -> Self {
        LSMState {
            mem_table: Arc::new(MemTable::new(options.mem_table_size)),
            immutable_mem_tables: VecDeque::new(),
            sstables: Vec::new(),
        }
    }
}
