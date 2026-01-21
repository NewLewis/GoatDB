use std::collections::VecDeque;
use std::sync::mpsc::Sender;
use std::sync::{Arc, RwLock};

use crate::goatkv::core::mem_table::{ImmutableMemTable, MemTable};
use crate::goatkv::metadata::version_set::{VersionSet, VersionSetOptions};
use crate::goatkv::utils::options::KvEngineOptions;

#[derive(Debug)]
pub struct LSMState {
    /// Mutable memtable
    pub mem_table: Arc<MemTable>,
    /// Immutable memtables waiting for flush
    pub immutable_mem_tables: VecDeque<Arc<ImmutableMemTable>>,
    /// VersionSet tracks SSTable metadata
    pub version_set: Arc<RwLock<VersionSet>>,
}

impl LSMState {
    pub fn new(
        options: &KvEngineOptions,
        mem_table: Arc<MemTable>,
        obsolete_sender: Sender<u64>,
    ) -> Result<Self, std::io::Error> {
        let vs_options = VersionSetOptions::from(options);
        let version_set = VersionSet::open(&options.data_dir, vs_options, obsolete_sender)?;

        Ok(LSMState {
            mem_table,
            immutable_mem_tables: VecDeque::new(),
            version_set: Arc::new(RwLock::new(version_set)),
        })
    }
}
