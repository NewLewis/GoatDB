use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::goatkv::error::Result as GoatResult;

use super::reader::SSTableReader;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadCacheMetrics {
    pub table_hits: u64,
    pub table_misses: u64,
    pub table_evictions: u64,
    pub block_hits: u64,
    pub block_misses: u64,
    pub block_evictions: u64,
}

#[derive(Debug)]
pub struct TableCache {
    capacity: usize,
    state: Mutex<TableCacheState>,
    block_cache: Arc<BlockCache>,
    table_hits: AtomicU64,
    table_misses: AtomicU64,
    table_evictions: AtomicU64,
}

#[derive(Debug, Default)]
struct TableCacheState {
    clock: u64,
    entries: HashMap<u64, TableCacheEntry>,
}

#[derive(Debug, Clone)]
struct TableCacheEntry {
    reader: Arc<SSTableReader>,
    last_access: u64,
}

impl TableCacheState {
    fn touch(&mut self) -> u64 {
        self.clock = self.clock.saturating_add(1);
        self.clock
    }

    fn evict_lru_file_id(&self) -> Option<u64> {
        self.entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_access)
            .map(|(file_id, _)| *file_id)
    }
}

impl TableCache {
    pub fn new(capacity: usize, block_cache_capacity_bytes: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(TableCacheState::default()),
            block_cache: Arc::new(BlockCache::new(block_cache_capacity_bytes)),
            table_hits: AtomicU64::new(0),
            table_misses: AtomicU64::new(0),
            table_evictions: AtomicU64::new(0),
        }
    }

    pub fn get_or_open<P: AsRef<Path>>(
        &self,
        file_id: u64,
        path: P,
    ) -> GoatResult<Arc<SSTableReader>> {
        if self.capacity > 0 {
            let mut state = self.state.lock().unwrap();
            let access = state.touch();
            if let Some(entry) = state.entries.get_mut(&file_id) {
                entry.last_access = access;
                self.table_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(entry.reader.clone());
            }
        }

        self.table_misses.fetch_add(1, Ordering::Relaxed);

        let opened = Arc::new(SSTableReader::open_with_block_cache(
            path,
            file_id,
            Some(self.block_cache.clone()),
        )?);

        if self.capacity == 0 {
            return Ok(opened);
        }

        let mut state = self.state.lock().unwrap();
        let access = state.touch();
        if let Some(entry) = state.entries.get_mut(&file_id) {
            entry.last_access = access;
            self.table_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(entry.reader.clone());
        }

        state.entries.insert(
            file_id,
            TableCacheEntry {
                reader: opened.clone(),
                last_access: access,
            },
        );

        while state.entries.len() > self.capacity {
            let Some(victim) = state.evict_lru_file_id() else {
                break;
            };
            if state.entries.remove(&victim).is_some() {
                self.table_evictions.fetch_add(1, Ordering::Relaxed);
            } else {
                break;
            }
        }

        Ok(opened)
    }

    pub fn metrics(&self) -> ReadCacheMetrics {
        let block = self.block_cache.metrics();
        ReadCacheMetrics {
            table_hits: self.table_hits.load(Ordering::Relaxed),
            table_misses: self.table_misses.load(Ordering::Relaxed),
            table_evictions: self.table_evictions.load(Ordering::Relaxed),
            block_hits: block.block_hits,
            block_misses: block.block_misses,
            block_evictions: block.block_evictions,
        }
    }
}

#[derive(Debug)]
pub(crate) struct BlockCache {
    capacity_bytes: usize,
    state: Mutex<BlockCacheState>,
    block_hits: AtomicU64,
    block_misses: AtomicU64,
    block_evictions: AtomicU64,
}

#[derive(Debug, Default)]
struct BlockCacheState {
    clock: u64,
    used_bytes: usize,
    entries: HashMap<BlockCacheKey, BlockCacheEntry>,
}

impl BlockCacheState {
    fn touch(&mut self) -> u64 {
        self.clock = self.clock.saturating_add(1);
        self.clock
    }

    fn evict_lru_key(&self) -> Option<BlockCacheKey> {
        self.entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_access)
            .map(|(key, _)| key.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BlockCacheKey {
    file_id: u64,
    block_offset: u64,
    block_size: u64,
}

impl BlockCacheKey {
    pub(crate) fn new(file_id: u64, block_offset: u64, block_size: u64) -> Self {
        Self {
            file_id,
            block_offset,
            block_size,
        }
    }
}

#[derive(Debug, Clone)]
struct BlockCacheEntry {
    payload: Arc<Vec<u8>>,
    bytes: usize,
    last_access: u64,
}

impl BlockCache {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            state: Mutex::new(BlockCacheState::default()),
            block_hits: AtomicU64::new(0),
            block_misses: AtomicU64::new(0),
            block_evictions: AtomicU64::new(0),
        }
    }

    pub(crate) fn get(&self, key: &BlockCacheKey) -> Option<Arc<Vec<u8>>> {
        if self.capacity_bytes == 0 {
            self.block_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        let mut state = self.state.lock().unwrap();
        let access = state.touch();
        if let Some(entry) = state.entries.get_mut(key) {
            entry.last_access = access;
            self.block_hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.payload.clone())
        } else {
            self.block_misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub(crate) fn insert(&self, key: BlockCacheKey, payload: Vec<u8>) -> Arc<Vec<u8>> {
        let payload = Arc::new(payload);
        let bytes = payload.len();
        if self.capacity_bytes == 0 || bytes > self.capacity_bytes {
            return payload;
        }

        let mut state = self.state.lock().unwrap();
        let access = state.touch();

        if let Some(prev) = state.entries.remove(&key) {
            state.used_bytes = state.used_bytes.saturating_sub(prev.bytes);
        }

        state.used_bytes = state.used_bytes.saturating_add(bytes);
        state.entries.insert(
            key,
            BlockCacheEntry {
                payload: payload.clone(),
                bytes,
                last_access: access,
            },
        );

        while state.used_bytes > self.capacity_bytes {
            let Some(victim) = state.evict_lru_key() else {
                break;
            };
            if let Some(removed) = state.entries.remove(&victim) {
                state.used_bytes = state.used_bytes.saturating_sub(removed.bytes);
                self.block_evictions.fetch_add(1, Ordering::Relaxed);
            } else {
                break;
            }
        }

        payload
    }

    fn metrics(&self) -> ReadCacheMetrics {
        ReadCacheMetrics {
            table_hits: 0,
            table_misses: 0,
            table_evictions: 0,
            block_hits: self.block_hits.load(Ordering::Relaxed),
            block_misses: self.block_misses.load(Ordering::Relaxed),
            block_evictions: self.block_evictions.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::goatkv::core::kv_engine::KvEngine;
    use crate::goatkv::format::internal_key::{InternalKey, InternalKeyKind};
    use crate::goatkv::storage::sstable::SSTableBuilder;

    use super::TableCache;

    fn build_sstable(file_id: u64) -> (tempfile::TempDir, PathBuf) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let (_, sstable_paths, _) = KvEngine::init_db_paths(temp_dir.path()).unwrap();
        let mut builder = SSTableBuilder::new_with_manager(file_id, &sstable_paths).unwrap();
        for i in 0..8 {
            let key = format!("k{:02}", i).into_bytes();
            let value = format!("v{:02}", i).into_bytes();
            let internal_key = InternalKey::new(key, 100 - i, InternalKeyKind::Put);
            builder.write(&internal_key.serialize(), &value).unwrap();
        }
        let _props = builder.finish().unwrap();
        let path = sstable_paths.sstable_path_by_id(file_id);
        (temp_dir, path)
    }

    #[test]
    fn table_cache_reports_hits_and_evictions() {
        let (_d1, p1) = build_sstable(1);
        let (_d2, p2) = build_sstable(2);
        let cache = TableCache::new(1, 0);

        let _r1 = cache.get_or_open(1, &p1).unwrap();
        let _r1_again = cache.get_or_open(1, &p1).unwrap();
        let _r2 = cache.get_or_open(2, &p2).unwrap();

        let metrics = cache.metrics();
        assert_eq!(metrics.table_hits, 1);
        assert_eq!(metrics.table_misses, 2);
        assert_eq!(metrics.table_evictions, 1);
    }

    #[test]
    fn block_cache_reports_hit_after_warmup() {
        let (_dir, path) = build_sstable(7);
        let cache = TableCache::new(2, 8 * 1024 * 1024);
        let reader = cache.get_or_open(7, &path).unwrap();

        let key = b"k03".to_vec();
        assert!(reader.get(&key).unwrap().is_some());
        assert!(reader.get(&key).unwrap().is_some());

        let metrics = cache.metrics();
        assert!(metrics.block_misses >= 1);
        assert!(metrics.block_hits >= 1);
    }

    #[test]
    fn table_cache_can_be_disabled() {
        let (_dir, path) = build_sstable(8);
        let cache = TableCache::new(0, 0);
        let _ = cache.get_or_open(8, &path).unwrap();
        let _ = cache.get_or_open(8, &path).unwrap();
        let metrics = cache.metrics();
        assert_eq!(metrics.table_hits, 0);
        assert_eq!(metrics.table_misses, 2);
    }
}
