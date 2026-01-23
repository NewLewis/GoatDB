use std::collections::VecDeque;
use std::sync::Arc;

use crate::goatkv::core::mem_table::{ImmutableMemTable, MemTable};
use crate::goatkv::metadata::version::Version;

#[derive(Debug)]
pub struct LSMState {
    /// Mutable memtable
    pub mem_table: Arc<MemTable>,
    /// Immutable memtables waiting for flush
    pub immutable_mem_tables: VecDeque<ImmutableMemTableEntry>,
    /// Current version snapshot for SSTable reads
    pub version: Arc<Version>,
}

#[derive(Debug, Clone)]
pub struct ImmutableMemTableEntry {
    pub table: Arc<ImmutableMemTable>,
    pub wal_log_number: u64,
}

impl LSMState {
    pub fn new(mem_table: Arc<MemTable>, version: Arc<Version>) -> Self {
        LSMState {
            mem_table,
            immutable_mem_tables: VecDeque::new(),
            version,
        }
    }
}
