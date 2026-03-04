use std::collections::VecDeque;
use std::sync::Arc;

use crate::goatkv::core::mem_table::{ImmutableMemTable, MemTable};
use crate::goatkv::metadata::version::Version;
use crate::goatkv::storage::wal::WalHandle;

#[derive(Debug)]
pub struct LSMState {
    /// Mutable memtable
    pub mem_table: Arc<MemTable>,
    /// Immutable memtables waiting for flush
    pub immutable_mem_tables: VecDeque<ImmutableMemTableEntry>,
    /// Consecutive flush failures observed by background flush worker.
    pub flush_failure_streak: usize,
    /// Circuit-breaker flag: true means write path should fail fast to avoid
    /// unbounded immutable backlog growth.
    pub flush_circuit_open: bool,
    /// Current version snapshot for SSTable reads
    pub version: Arc<Version>,
}

#[derive(Debug, Clone)]
pub struct ImmutableMemTableEntry {
    pub table: Arc<ImmutableMemTable>,
    pub wal_handle: Option<Arc<WalHandle>>,
}

impl LSMState {
    pub fn new(mem_table: Arc<MemTable>, version: Arc<Version>) -> Self {
        LSMState {
            mem_table,
            immutable_mem_tables: VecDeque::new(),
            flush_failure_streak: 0,
            flush_circuit_open: false,
            version,
        }
    }
}
