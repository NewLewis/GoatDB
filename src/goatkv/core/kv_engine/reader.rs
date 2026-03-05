use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

use crate::goatkv::core::lsm_state::{ImmutableMemTableEntry, LSMState};
use crate::goatkv::core::mem_table::MemTable;
use crate::goatkv::error::Result as GoatResult;
use crate::goatkv::format::internal_key::InternalKeyKind;
use crate::goatkv::metadata::version::Version;
use crate::goatkv::storage::sstable::PinnedValue;

#[derive(Debug)]
pub struct KvReader {
    lsm_state: Arc<RwLock<LSMState>>,
}

impl KvReader {
    pub fn new(lsm_state: Arc<RwLock<LSMState>>) -> Self {
        Self { lsm_state }
    }

    pub fn get(&self, key: &[u8]) -> GoatResult<Option<Vec<u8>>> {
        let (mem_table, immutable_mem_tables, version) = self.snapshot_read_state();

        if let Some(result) = Self::get_from_memtable(&mem_table, key)? {
            return Ok(result.map(|value| value.to_vec()));
        }
        for entry in immutable_mem_tables.iter().rev() {
            if let Some(result) = Self::get_from_immutable(entry, key)? {
                return Ok(result.map(|value| value.to_vec()));
            }
        }
        Self::get_from_version(&version, key).map(|result| result.map(|value| value.to_vec()))
    }

    fn snapshot_read_state(
        &self,
    ) -> (
        Arc<MemTable>,
        VecDeque<ImmutableMemTableEntry>,
        Arc<Version>,
    ) {
        let lsm_state = self.lsm_state.read().unwrap();
        (
            lsm_state.mem_table.clone(),
            lsm_state.immutable_mem_tables.clone(),
            lsm_state.version.clone(),
        )
    }

    fn get_from_memtable(
        mem_table: &Arc<MemTable>,
        key: &[u8],
    ) -> GoatResult<Option<Option<PinnedValue>>> {
        mem_table
            .get_pinned(key)
            .map(|(internal_key, value)| {
                Ok(if internal_key.kind()? == InternalKeyKind::Delete {
                    None
                } else {
                    Some(PinnedValue::from_bytes(value))
                })
            })
            .transpose()
    }

    fn get_from_immutable(
        entry: &ImmutableMemTableEntry,
        key: &[u8],
    ) -> GoatResult<Option<Option<PinnedValue>>> {
        entry
            .table
            .get_pinned(key)
            .map(|(internal_key, value)| {
                Ok(if internal_key.kind()? == InternalKeyKind::Delete {
                    None
                } else {
                    Some(PinnedValue::from_bytes(value))
                })
            })
            .transpose()
    }

    fn get_from_version(version: &Arc<Version>, key: &[u8]) -> GoatResult<Option<PinnedValue>> {
        match version.get_pinned(key)? {
            Some((internal_key, value)) => {
                if internal_key.kind()? == InternalKeyKind::Delete {
                    Ok(None)
                } else {
                    Ok(Some(value))
                }
            }
            None => Ok(None),
        }
    }
}
