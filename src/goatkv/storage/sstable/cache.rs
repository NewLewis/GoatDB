use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::goatkv::error::Result as GoatResult;
use crate::goatkv::format::internal_key::InternalKey;
use bytes::Bytes;

use super::bloom::BloomFilter;
use super::reader::SSTableReader;

const TABLE_CACHE_MAX_SHARDS: usize = 16;
const BLOCK_CACHE_MAX_SHARDS: usize = 16;
const ROW_CACHE_MAX_SHARDS: usize = 16;
const FILTER_CACHE_MAX_SHARDS: usize = 16;
const MIN_BLOCK_BYTES_PER_SHARD: usize = 64 * 1024;
const MIN_ROW_BYTES_PER_SHARD: usize = 64 * 1024;
const MIN_FILTER_BYTES_PER_SHARD: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadCacheMetrics {
    pub table_hits: u64,
    pub table_misses: u64,
    pub table_evictions: u64,
    pub row_hits: u64,
    pub row_misses: u64,
    pub row_evictions: u64,
    pub block_hits: u64,
    pub block_misses: u64,
    pub block_evictions: u64,
    pub filter_hits: u64,
    pub filter_misses: u64,
    pub filter_evictions: u64,
}

#[derive(Debug)]
pub struct TableCache {
    capacity: usize,
    shards: Vec<RwLock<TableCacheState>>,
    shard_capacities: Vec<usize>,
    row_cache: Arc<RowCache>,
    block_cache: Arc<BlockCache>,
    filter_cache: Arc<FilterPartitionCache>,
    table_hits: AtomicU64,
    table_misses: AtomicU64,
    table_evictions: AtomicU64,
}

#[derive(Debug, Default)]
struct TableCacheState {
    entries: HashMap<u64, TableCacheEntry>,
    clock_keys: Vec<u64>,
    hand: usize,
}

#[derive(Debug)]
struct TableCacheEntry {
    reader: Arc<SSTableReader>,
    referenced: AtomicBool,
    hot: AtomicBool,
}

impl TableCacheState {
    fn mark_hit(&self, file_id: u64) -> Option<Arc<SSTableReader>> {
        let entry = self.entries.get(&file_id)?;
        entry.referenced.store(true, Ordering::Relaxed);
        entry.hot.store(true, Ordering::Relaxed);
        Some(entry.reader.clone())
    }

    fn insert_new(&mut self, file_id: u64, reader: Arc<SSTableReader>) {
        self.entries.insert(
            file_id,
            TableCacheEntry {
                reader,
                referenced: AtomicBool::new(true),
                // 新条目先按 cold 处理，避免“永久热”占位。
                hot: AtomicBool::new(false),
            },
        );
        self.clock_keys.push(file_id);
    }

    fn advance_hand(&mut self) {
        if self.clock_keys.is_empty() {
            self.hand = 0;
        } else {
            self.hand = (self.hand + 1) % self.clock_keys.len();
        }
    }

    fn remove_hand_slot(&mut self) {
        if self.clock_keys.is_empty() {
            self.hand = 0;
            return;
        }
        self.clock_keys.swap_remove(self.hand);
        if self.hand >= self.clock_keys.len() && !self.clock_keys.is_empty() {
            self.hand = 0;
        }
    }

    fn evict_one(&mut self) -> Option<u64> {
        if self.entries.is_empty() {
            return None;
        }

        // HyperClock 风格：referenced -> 清引用；hot -> 降级为 cold；cold 且未引用 -> 淘汰。
        let max_scan = self.clock_keys.len().saturating_mul(3).max(1);
        for _ in 0..max_scan {
            if self.clock_keys.is_empty() {
                return None;
            }
            if self.hand >= self.clock_keys.len() {
                self.hand = 0;
            }
            let file_id = self.clock_keys[self.hand];
            let mut should_evict = false;
            let mut missing = false;

            match self.entries.get(&file_id) {
                Some(entry) => {
                    if entry.referenced.swap(false, Ordering::Relaxed)
                        || entry.hot.swap(false, Ordering::Relaxed)
                    {
                        self.advance_hand();
                    } else {
                        should_evict = true;
                    }
                }
                None => {
                    missing = true;
                }
            }

            if missing {
                self.remove_hand_slot();
                continue;
            }

            if should_evict {
                self.entries.remove(&file_id);
                self.remove_hand_slot();
                return Some(file_id);
            }
        }

        // 兜底：强制回收一个条目，保证容量约束可收敛。
        while !self.clock_keys.is_empty() {
            if self.hand >= self.clock_keys.len() {
                self.hand = 0;
            }
            let file_id = self.clock_keys[self.hand];
            self.remove_hand_slot();
            if self.entries.remove(&file_id).is_some() {
                return Some(file_id);
            }
        }
        None
    }
}

impl TableCache {
    pub fn new(
        capacity: usize,
        block_cache_capacity_bytes: usize,
        row_cache_capacity_bytes: usize,
        filter_cache_capacity_bytes: usize,
    ) -> Self {
        let table_shard_count = if capacity == 0 {
            1
        } else {
            capacity.clamp(1, TABLE_CACHE_MAX_SHARDS)
        };
        let shard_capacities = split_capacity(capacity, table_shard_count);
        let shards = (0..table_shard_count)
            .map(|_| RwLock::new(TableCacheState::default()))
            .collect();

        Self {
            capacity,
            shards,
            shard_capacities,
            row_cache: Arc::new(RowCache::new(row_cache_capacity_bytes)),
            block_cache: Arc::new(BlockCache::new(block_cache_capacity_bytes)),
            filter_cache: Arc::new(FilterPartitionCache::new(filter_cache_capacity_bytes)),
            table_hits: AtomicU64::new(0),
            table_misses: AtomicU64::new(0),
            table_evictions: AtomicU64::new(0),
        }
    }

    fn table_shard_index(&self, file_id: u64) -> usize {
        shard_index_from_u64(file_id, self.shards.len())
    }

    pub fn get_or_open<P: AsRef<Path>>(
        &self,
        file_id: u64,
        path: P,
    ) -> GoatResult<Arc<SSTableReader>> {
        if self.capacity > 0 {
            let shard_idx = self.table_shard_index(file_id);
            if self.shard_capacities[shard_idx] > 0 {
                let state = self.shards[shard_idx].read().unwrap();
                if let Some(reader) = state.mark_hit(file_id) {
                    self.table_hits.fetch_add(1, Ordering::Relaxed);
                    return Ok(reader);
                }
            }
        }

        self.table_misses.fetch_add(1, Ordering::Relaxed);

        let opened = Arc::new(SSTableReader::open_with_block_cache(
            path,
            file_id,
            Some(self.block_cache.clone()),
            Some(self.filter_cache.clone()),
        )?);

        if self.capacity == 0 {
            return Ok(opened);
        }

        let shard_idx = self.table_shard_index(file_id);
        let shard_capacity = self.shard_capacities[shard_idx];
        if shard_capacity == 0 {
            return Ok(opened);
        }

        let mut state = self.shards[shard_idx].write().unwrap();
        if let Some(reader) = state.mark_hit(file_id) {
            self.table_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(reader);
        }

        state.insert_new(file_id, opened.clone());
        while state.entries.len() > shard_capacity {
            if state.evict_one().is_none() {
                break;
            }
            self.table_evictions.fetch_add(1, Ordering::Relaxed);
        }

        Ok(opened)
    }

    pub fn metrics(&self) -> ReadCacheMetrics {
        let row = self.row_cache.metrics();
        let block = self.block_cache.metrics();
        let filter = self.filter_cache.metrics();
        ReadCacheMetrics {
            table_hits: self.table_hits.load(Ordering::Relaxed),
            table_misses: self.table_misses.load(Ordering::Relaxed),
            table_evictions: self.table_evictions.load(Ordering::Relaxed),
            row_hits: row.row_hits,
            row_misses: row.row_misses,
            row_evictions: row.row_evictions,
            block_hits: block.block_hits,
            block_misses: block.block_misses,
            block_evictions: block.block_evictions,
            filter_hits: filter.filter_hits,
            filter_misses: filter.filter_misses,
            filter_evictions: filter.filter_evictions,
        }
    }

    pub(crate) fn row_cache_get(
        &self,
        version_seqno: u64,
        read_seq: u64,
        user_key: &[u8],
    ) -> Option<RowCacheValue> {
        self.row_cache.get(version_seqno, read_seq, user_key)
    }

    pub(crate) fn row_cache_insert(
        &self,
        version_seqno: u64,
        read_seq: u64,
        user_key: &[u8],
        value: RowCacheValue,
    ) {
        self.row_cache
            .insert(version_seqno, read_seq, user_key, value);
    }
}

#[derive(Debug, Clone)]
pub(crate) enum RowCacheValue {
    Hit {
        internal_key: InternalKey,
        value: Bytes,
    },
    Miss,
}

impl RowCacheValue {
    fn estimated_bytes(&self) -> usize {
        match self {
            RowCacheValue::Hit {
                internal_key,
                value,
            } => internal_key.user_key().len() + std::mem::size_of::<u64>() + value.len(),
            RowCacheValue::Miss => 0,
        }
    }
}

#[derive(Debug)]
struct RowCache {
    capacity_bytes: usize,
    shards: Vec<RwLock<RowCacheState>>,
    shard_capacities: Vec<usize>,
    row_hits: AtomicU64,
    row_misses: AtomicU64,
    row_evictions: AtomicU64,
}

#[derive(Debug, Default)]
struct RowCacheState {
    used_bytes: usize,
    entries: HashMap<RowCacheKey, RowCacheEntry>,
    clock_keys: Vec<RowCacheKey>,
    hand: usize,
}

impl RowCacheState {
    fn mark_hit(&self, key: &RowCacheKey) -> Option<RowCacheValue> {
        let entry = self.entries.get(key)?;
        entry.referenced.store(true, Ordering::Relaxed);
        entry.hot.store(true, Ordering::Relaxed);
        Some(entry.value.clone())
    }

    fn insert_or_update(&mut self, key: RowCacheKey, value: RowCacheValue, bytes: usize) {
        if let Some(entry) = self.entries.get_mut(&key) {
            self.used_bytes = self.used_bytes.saturating_sub(entry.bytes);
            self.used_bytes = self.used_bytes.saturating_add(bytes);
            entry.value = value;
            entry.bytes = bytes;
            entry.referenced.store(true, Ordering::Relaxed);
            entry.hot.store(true, Ordering::Relaxed);
            return;
        }

        self.used_bytes = self.used_bytes.saturating_add(bytes);
        self.entries.insert(
            key.clone(),
            RowCacheEntry {
                value,
                bytes,
                referenced: AtomicBool::new(true),
                hot: AtomicBool::new(false),
            },
        );
        self.clock_keys.push(key);
    }

    fn advance_hand(&mut self) {
        if self.clock_keys.is_empty() {
            self.hand = 0;
        } else {
            self.hand = (self.hand + 1) % self.clock_keys.len();
        }
    }

    fn remove_hand_slot(&mut self) {
        if self.clock_keys.is_empty() {
            self.hand = 0;
            return;
        }
        self.clock_keys.swap_remove(self.hand);
        if self.hand >= self.clock_keys.len() && !self.clock_keys.is_empty() {
            self.hand = 0;
        }
    }

    fn evict_one(&mut self) -> bool {
        if self.entries.is_empty() {
            return false;
        }

        let max_scan = self.clock_keys.len().saturating_mul(3).max(1);
        for _ in 0..max_scan {
            if self.clock_keys.is_empty() {
                return false;
            }
            if self.hand >= self.clock_keys.len() {
                self.hand = 0;
            }
            let key = self.clock_keys[self.hand].clone();
            let mut should_evict = false;
            let mut missing = false;

            match self.entries.get(&key) {
                Some(entry) => {
                    if entry.referenced.swap(false, Ordering::Relaxed)
                        || entry.hot.swap(false, Ordering::Relaxed)
                    {
                        self.advance_hand();
                    } else {
                        should_evict = true;
                    }
                }
                None => {
                    missing = true;
                }
            }

            if missing {
                self.remove_hand_slot();
                continue;
            }

            if should_evict {
                if let Some(removed) = self.entries.remove(&key) {
                    self.used_bytes = self.used_bytes.saturating_sub(removed.bytes);
                }
                self.remove_hand_slot();
                return true;
            }
        }

        while !self.clock_keys.is_empty() {
            if self.hand >= self.clock_keys.len() {
                self.hand = 0;
            }
            let key = self.clock_keys[self.hand].clone();
            self.remove_hand_slot();
            if let Some(removed) = self.entries.remove(&key) {
                self.used_bytes = self.used_bytes.saturating_sub(removed.bytes);
                return true;
            }
        }
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RowCacheKey {
    version_seqno: u64,
    read_seq: u64,
    user_key: Vec<u8>,
}

impl RowCacheKey {
    fn new(version_seqno: u64, read_seq: u64, user_key: &[u8]) -> Self {
        Self {
            version_seqno,
            read_seq,
            user_key: user_key.to_vec(),
        }
    }

    fn shard_hash(&self) -> u64 {
        // 64-bit FNV-1a mix.
        let mut hash = 0xcbf2_9ce4_8422_2325u64 ^ self.version_seqno.rotate_left(17);
        hash ^= self.read_seq.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        for b in self.user_key.iter().copied() {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x1000_0000_01b3);
        }
        hash
    }
}

#[derive(Debug)]
struct RowCacheEntry {
    value: RowCacheValue,
    bytes: usize,
    referenced: AtomicBool,
    hot: AtomicBool,
}

impl RowCache {
    fn new(capacity_bytes: usize) -> Self {
        let shard_count = shard_count_for_bytes(
            capacity_bytes,
            MIN_ROW_BYTES_PER_SHARD,
            ROW_CACHE_MAX_SHARDS,
        );
        let shard_capacities = split_capacity(capacity_bytes, shard_count);
        let shards = (0..shard_count)
            .map(|_| RwLock::new(RowCacheState::default()))
            .collect();

        Self {
            capacity_bytes,
            shards,
            shard_capacities,
            row_hits: AtomicU64::new(0),
            row_misses: AtomicU64::new(0),
            row_evictions: AtomicU64::new(0),
        }
    }

    fn shard_index(&self, key: &RowCacheKey) -> usize {
        shard_index_from_u64(key.shard_hash(), self.shards.len())
    }

    fn get(&self, version_seqno: u64, read_seq: u64, user_key: &[u8]) -> Option<RowCacheValue> {
        if self.capacity_bytes == 0 {
            self.row_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        let cache_key = RowCacheKey::new(version_seqno, read_seq, user_key);
        let shard_idx = self.shard_index(&cache_key);
        if self.shard_capacities[shard_idx] == 0 {
            self.row_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        let state = self.shards[shard_idx].read().unwrap();
        if let Some(value) = state.mark_hit(&cache_key) {
            self.row_hits.fetch_add(1, Ordering::Relaxed);
            Some(value)
        } else {
            self.row_misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    fn insert(&self, version_seqno: u64, read_seq: u64, user_key: &[u8], value: RowCacheValue) {
        if self.capacity_bytes == 0 {
            return;
        }

        let key = RowCacheKey::new(version_seqno, read_seq, user_key);
        let bytes = key
            .user_key
            .len()
            .saturating_add(value.estimated_bytes())
            .saturating_add(std::mem::size_of::<RowCacheEntry>());

        let shard_idx = self.shard_index(&key);
        let shard_capacity = self.shard_capacities[shard_idx];
        if shard_capacity == 0 || bytes > shard_capacity {
            return;
        }

        let mut state = self.shards[shard_idx].write().unwrap();
        state.insert_or_update(key, value, bytes);
        while state.used_bytes > shard_capacity {
            if !state.evict_one() {
                break;
            }
            self.row_evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn metrics(&self) -> ReadCacheMetrics {
        ReadCacheMetrics {
            table_hits: 0,
            table_misses: 0,
            table_evictions: 0,
            row_hits: self.row_hits.load(Ordering::Relaxed),
            row_misses: self.row_misses.load(Ordering::Relaxed),
            row_evictions: self.row_evictions.load(Ordering::Relaxed),
            block_hits: 0,
            block_misses: 0,
            block_evictions: 0,
            filter_hits: 0,
            filter_misses: 0,
            filter_evictions: 0,
        }
    }
}

#[derive(Debug)]
pub(crate) struct BlockCache {
    capacity_bytes: usize,
    shards: Vec<RwLock<BlockCacheState>>,
    shard_capacities: Vec<usize>,
    block_hits: AtomicU64,
    block_misses: AtomicU64,
    block_evictions: AtomicU64,
}

#[derive(Debug, Default)]
struct BlockCacheState {
    used_bytes: usize,
    entries: HashMap<BlockCacheKey, BlockCacheEntry>,
    clock_keys: Vec<BlockCacheKey>,
    hand: usize,
}

impl BlockCacheState {
    fn mark_hit(&self, key: &BlockCacheKey) -> Option<Arc<[u8]>> {
        let entry = self.entries.get(key)?;
        entry.referenced.store(true, Ordering::Relaxed);
        entry.hot.store(true, Ordering::Relaxed);
        Some(entry.payload.clone())
    }

    fn insert_or_update(&mut self, key: BlockCacheKey, payload: Arc<[u8]>, bytes: usize) {
        if let Some(entry) = self.entries.get_mut(&key) {
            self.used_bytes = self.used_bytes.saturating_sub(entry.bytes);
            self.used_bytes = self.used_bytes.saturating_add(bytes);
            entry.payload = payload;
            entry.bytes = bytes;
            entry.referenced.store(true, Ordering::Relaxed);
            entry.hot.store(true, Ordering::Relaxed);
            return;
        }

        self.used_bytes = self.used_bytes.saturating_add(bytes);
        self.entries.insert(
            key.clone(),
            BlockCacheEntry {
                payload,
                bytes,
                referenced: AtomicBool::new(true),
                // 新条目先按 cold 处理，避免 cache 污染。
                hot: AtomicBool::new(false),
            },
        );
        self.clock_keys.push(key);
    }

    fn advance_hand(&mut self) {
        if self.clock_keys.is_empty() {
            self.hand = 0;
        } else {
            self.hand = (self.hand + 1) % self.clock_keys.len();
        }
    }

    fn remove_hand_slot(&mut self) {
        if self.clock_keys.is_empty() {
            self.hand = 0;
            return;
        }
        self.clock_keys.swap_remove(self.hand);
        if self.hand >= self.clock_keys.len() && !self.clock_keys.is_empty() {
            self.hand = 0;
        }
    }

    fn evict_one(&mut self) -> bool {
        if self.entries.is_empty() {
            return false;
        }

        let max_scan = self.clock_keys.len().saturating_mul(3).max(1);
        for _ in 0..max_scan {
            if self.clock_keys.is_empty() {
                return false;
            }
            if self.hand >= self.clock_keys.len() {
                self.hand = 0;
            }
            let key = self.clock_keys[self.hand].clone();
            let mut should_evict = false;
            let mut missing = false;

            match self.entries.get(&key) {
                Some(entry) => {
                    if entry.referenced.swap(false, Ordering::Relaxed)
                        || entry.hot.swap(false, Ordering::Relaxed)
                    {
                        self.advance_hand();
                    } else {
                        should_evict = true;
                    }
                }
                None => {
                    missing = true;
                }
            }

            if missing {
                self.remove_hand_slot();
                continue;
            }

            if should_evict {
                if let Some(removed) = self.entries.remove(&key) {
                    self.used_bytes = self.used_bytes.saturating_sub(removed.bytes);
                }
                self.remove_hand_slot();
                return true;
            }
        }

        while !self.clock_keys.is_empty() {
            if self.hand >= self.clock_keys.len() {
                self.hand = 0;
            }
            let key = self.clock_keys[self.hand].clone();
            self.remove_hand_slot();
            if let Some(removed) = self.entries.remove(&key) {
                self.used_bytes = self.used_bytes.saturating_sub(removed.bytes);
                return true;
            }
        }
        false
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

    fn shard_hash(&self) -> u64 {
        let mut x = self.file_id.rotate_left(17);
        x ^= self.block_offset.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        x ^= self.block_size.rotate_left(9);
        x ^= x >> 33;
        x = x.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        x ^ (x >> 33)
    }
}

#[derive(Debug)]
struct BlockCacheEntry {
    payload: Arc<[u8]>,
    bytes: usize,
    referenced: AtomicBool,
    hot: AtomicBool,
}

impl BlockCache {
    fn new(capacity_bytes: usize) -> Self {
        let block_shard_count = shard_count_for_bytes(
            capacity_bytes,
            MIN_BLOCK_BYTES_PER_SHARD,
            BLOCK_CACHE_MAX_SHARDS,
        );
        let shard_capacities = split_capacity(capacity_bytes, block_shard_count);
        let shards = (0..block_shard_count)
            .map(|_| RwLock::new(BlockCacheState::default()))
            .collect();

        Self {
            capacity_bytes,
            shards,
            shard_capacities,
            block_hits: AtomicU64::new(0),
            block_misses: AtomicU64::new(0),
            block_evictions: AtomicU64::new(0),
        }
    }

    fn shard_index(&self, key: &BlockCacheKey) -> usize {
        shard_index_from_u64(key.shard_hash(), self.shards.len())
    }

    pub(crate) fn get(&self, key: &BlockCacheKey) -> Option<Arc<[u8]>> {
        if self.capacity_bytes == 0 {
            self.block_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        let shard_idx = self.shard_index(key);
        if self.shard_capacities[shard_idx] == 0 {
            self.block_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        let state = self.shards[shard_idx].read().unwrap();
        if let Some(payload) = state.mark_hit(key) {
            self.block_hits.fetch_add(1, Ordering::Relaxed);
            Some(payload)
        } else {
            self.block_misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub(crate) fn insert(&self, key: BlockCacheKey, payload: Vec<u8>) -> Arc<[u8]> {
        let payload: Arc<[u8]> = Arc::from(payload.into_boxed_slice());
        let bytes = payload.len();
        if self.capacity_bytes == 0 {
            return payload;
        }

        let shard_idx = self.shard_index(&key);
        let shard_capacity = self.shard_capacities[shard_idx];
        if shard_capacity == 0 || bytes > shard_capacity {
            return payload;
        }

        let mut state = self.shards[shard_idx].write().unwrap();
        state.insert_or_update(key, payload.clone(), bytes);

        while state.used_bytes > shard_capacity {
            if !state.evict_one() {
                break;
            }
            self.block_evictions.fetch_add(1, Ordering::Relaxed);
        }

        payload
    }

    fn metrics(&self) -> ReadCacheMetrics {
        ReadCacheMetrics {
            table_hits: 0,
            table_misses: 0,
            table_evictions: 0,
            row_hits: 0,
            row_misses: 0,
            row_evictions: 0,
            block_hits: self.block_hits.load(Ordering::Relaxed),
            block_misses: self.block_misses.load(Ordering::Relaxed),
            block_evictions: self.block_evictions.load(Ordering::Relaxed),
            filter_hits: 0,
            filter_misses: 0,
            filter_evictions: 0,
        }
    }
}

#[derive(Debug)]
pub(crate) struct FilterPartitionCache {
    capacity_bytes: usize,
    shards: Vec<RwLock<FilterPartitionCacheState>>,
    shard_capacities: Vec<usize>,
    filter_hits: AtomicU64,
    filter_misses: AtomicU64,
    filter_evictions: AtomicU64,
}

#[derive(Debug, Default)]
struct FilterPartitionCacheState {
    used_bytes: usize,
    entries: HashMap<FilterPartitionCacheKey, FilterPartitionCacheEntry>,
    clock_keys: Vec<FilterPartitionCacheKey>,
    hand: usize,
}

impl FilterPartitionCacheState {
    fn mark_hit(&self, key: &FilterPartitionCacheKey) -> Option<Arc<BloomFilter>> {
        let entry = self.entries.get(key)?;
        entry.referenced.store(true, Ordering::Relaxed);
        entry.hot.store(true, Ordering::Relaxed);
        Some(entry.filter.clone())
    }

    fn insert_or_update(
        &mut self,
        key: FilterPartitionCacheKey,
        filter: Arc<BloomFilter>,
        bytes: usize,
    ) {
        if let Some(entry) = self.entries.get_mut(&key) {
            self.used_bytes = self.used_bytes.saturating_sub(entry.bytes);
            self.used_bytes = self.used_bytes.saturating_add(bytes);
            entry.filter = filter;
            entry.bytes = bytes;
            entry.referenced.store(true, Ordering::Relaxed);
            entry.hot.store(true, Ordering::Relaxed);
            return;
        }

        self.used_bytes = self.used_bytes.saturating_add(bytes);
        self.entries.insert(
            key.clone(),
            FilterPartitionCacheEntry {
                filter,
                bytes,
                referenced: AtomicBool::new(true),
                hot: AtomicBool::new(false),
            },
        );
        self.clock_keys.push(key);
    }

    fn advance_hand(&mut self) {
        if self.clock_keys.is_empty() {
            self.hand = 0;
        } else {
            self.hand = (self.hand + 1) % self.clock_keys.len();
        }
    }

    fn remove_hand_slot(&mut self) {
        if self.clock_keys.is_empty() {
            self.hand = 0;
            return;
        }
        self.clock_keys.swap_remove(self.hand);
        if self.hand >= self.clock_keys.len() && !self.clock_keys.is_empty() {
            self.hand = 0;
        }
    }

    fn evict_one(&mut self) -> bool {
        if self.entries.is_empty() {
            return false;
        }

        let max_scan = self.clock_keys.len().saturating_mul(3).max(1);
        for _ in 0..max_scan {
            if self.clock_keys.is_empty() {
                return false;
            }
            if self.hand >= self.clock_keys.len() {
                self.hand = 0;
            }
            let key = self.clock_keys[self.hand].clone();
            let mut should_evict = false;
            let mut missing = false;

            match self.entries.get(&key) {
                Some(entry) => {
                    if entry.referenced.swap(false, Ordering::Relaxed)
                        || entry.hot.swap(false, Ordering::Relaxed)
                    {
                        self.advance_hand();
                    } else {
                        should_evict = true;
                    }
                }
                None => {
                    missing = true;
                }
            }

            if missing {
                self.remove_hand_slot();
                continue;
            }

            if should_evict {
                if let Some(removed) = self.entries.remove(&key) {
                    self.used_bytes = self.used_bytes.saturating_sub(removed.bytes);
                }
                self.remove_hand_slot();
                return true;
            }
        }

        while !self.clock_keys.is_empty() {
            if self.hand >= self.clock_keys.len() {
                self.hand = 0;
            }
            let key = self.clock_keys[self.hand].clone();
            self.remove_hand_slot();
            if let Some(removed) = self.entries.remove(&key) {
                self.used_bytes = self.used_bytes.saturating_sub(removed.bytes);
                return true;
            }
        }
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct FilterPartitionCacheKey {
    file_id: u64,
    block_index: u64,
}

impl FilterPartitionCacheKey {
    pub(crate) fn new(file_id: u64, block_index: usize) -> Self {
        Self {
            file_id,
            block_index: block_index as u64,
        }
    }

    fn shard_hash(&self) -> u64 {
        let mut x = self.file_id.rotate_left(19);
        x ^= self.block_index.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        x ^= x >> 33;
        x = x.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        x ^ (x >> 33)
    }
}

#[derive(Debug)]
struct FilterPartitionCacheEntry {
    filter: Arc<BloomFilter>,
    bytes: usize,
    referenced: AtomicBool,
    hot: AtomicBool,
}

impl FilterPartitionCache {
    fn new(capacity_bytes: usize) -> Self {
        let shard_count = shard_count_for_bytes(
            capacity_bytes,
            MIN_FILTER_BYTES_PER_SHARD,
            FILTER_CACHE_MAX_SHARDS,
        );
        let shard_capacities = split_capacity(capacity_bytes, shard_count);
        let shards = (0..shard_count)
            .map(|_| RwLock::new(FilterPartitionCacheState::default()))
            .collect();

        Self {
            capacity_bytes,
            shards,
            shard_capacities,
            filter_hits: AtomicU64::new(0),
            filter_misses: AtomicU64::new(0),
            filter_evictions: AtomicU64::new(0),
        }
    }

    fn shard_index(&self, key: &FilterPartitionCacheKey) -> usize {
        shard_index_from_u64(key.shard_hash(), self.shards.len())
    }

    pub(crate) fn get(&self, key: &FilterPartitionCacheKey) -> Option<Arc<BloomFilter>> {
        if self.capacity_bytes == 0 {
            self.filter_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        let shard_idx = self.shard_index(key);
        if self.shard_capacities[shard_idx] == 0 {
            self.filter_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        let state = self.shards[shard_idx].read().unwrap();
        if let Some(filter) = state.mark_hit(key) {
            self.filter_hits.fetch_add(1, Ordering::Relaxed);
            Some(filter)
        } else {
            self.filter_misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub(crate) fn insert(
        &self,
        key: FilterPartitionCacheKey,
        filter: BloomFilter,
    ) -> Arc<BloomFilter> {
        let filter = Arc::new(filter);
        let bytes = filter.size();
        if self.capacity_bytes == 0 {
            return filter;
        }

        let shard_idx = self.shard_index(&key);
        let shard_capacity = self.shard_capacities[shard_idx];
        if shard_capacity == 0 || bytes > shard_capacity {
            return filter;
        }

        let mut state = self.shards[shard_idx].write().unwrap();
        state.insert_or_update(key, filter.clone(), bytes);
        while state.used_bytes > shard_capacity {
            if !state.evict_one() {
                break;
            }
            self.filter_evictions.fetch_add(1, Ordering::Relaxed);
        }

        filter
    }

    fn metrics(&self) -> ReadCacheMetrics {
        ReadCacheMetrics {
            table_hits: 0,
            table_misses: 0,
            table_evictions: 0,
            row_hits: 0,
            row_misses: 0,
            row_evictions: 0,
            block_hits: 0,
            block_misses: 0,
            block_evictions: 0,
            filter_hits: self.filter_hits.load(Ordering::Relaxed),
            filter_misses: self.filter_misses.load(Ordering::Relaxed),
            filter_evictions: self.filter_evictions.load(Ordering::Relaxed),
        }
    }
}

fn split_capacity(total: usize, shards: usize) -> Vec<usize> {
    if shards == 0 {
        return Vec::new();
    }
    let base = total / shards;
    let rem = total % shards;
    let mut caps = Vec::with_capacity(shards);
    for i in 0..shards {
        let extra = usize::from(i < rem);
        caps.push(base + extra);
    }
    caps
}

fn shard_count_for_bytes(
    total_bytes: usize,
    min_bytes_per_shard: usize,
    max_shards: usize,
) -> usize {
    if total_bytes == 0 {
        return 1;
    }
    let by_size = (total_bytes / min_bytes_per_shard.max(1)).max(1);
    by_size.clamp(1, max_shards.max(1))
}

fn shard_index_from_u64(hash: u64, shard_count: usize) -> usize {
    debug_assert!(shard_count > 0);
    // simple 64->usize mix
    let x = hash ^ (hash >> 33);
    (x as usize) % shard_count
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::goatkv::core::kv_engine::KvEngine;
    use crate::goatkv::format::internal_key::{InternalKey, InternalKeyKind};
    use crate::goatkv::storage::sstable::SSTableBuilder;
    use bytes::Bytes;

    use super::{RowCacheValue, TableCache};

    fn build_sstable(file_id: u64) -> (tempfile::TempDir, PathBuf) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let (_, sstable_paths, _) = KvEngine::init_db_paths(temp_dir.path()).unwrap();
        let mut builder = SSTableBuilder::new_with_manager(file_id, &sstable_paths, 0).unwrap();
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
        let cache = TableCache::new(1, 0, 0, 0);

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
        let cache = TableCache::new(2, 8 * 1024 * 1024, 0, 0);
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
        let cache = TableCache::new(0, 0, 0, 0);
        let _ = cache.get_or_open(8, &path).unwrap();
        let _ = cache.get_or_open(8, &path).unwrap();
        let metrics = cache.metrics();
        assert_eq!(metrics.table_hits, 0);
        assert_eq!(metrics.table_misses, 2);
    }

    #[test]
    fn row_cache_distinguishes_visibility_sequence() {
        let cache = TableCache::new(1, 0, 1024 * 1024, 0);
        let user_key = b"k1";
        let version_seqno = 100u64;

        cache.row_cache_insert(
            version_seqno,
            10,
            user_key,
            RowCacheValue::Hit {
                internal_key: InternalKey::new(user_key.to_vec(), 10, InternalKeyKind::Put),
                value: Bytes::from_static(b"v10"),
            },
        );
        cache.row_cache_insert(
            version_seqno,
            20,
            user_key,
            RowCacheValue::Hit {
                internal_key: InternalKey::new(user_key.to_vec(), 20, InternalKeyKind::Put),
                value: Bytes::from_static(b"v20"),
            },
        );

        let old = cache.row_cache_get(version_seqno, 10, user_key);
        let new = cache.row_cache_get(version_seqno, 20, user_key);
        let middle = cache.row_cache_get(version_seqno, 15, user_key);

        match old {
            Some(RowCacheValue::Hit {
                internal_key,
                value,
            }) => {
                assert_eq!(internal_key.sequence_number(), 10);
                assert_eq!(value.as_ref(), b"v10");
            }
            other => panic!("unexpected old visibility cache value: {:?}", other),
        }
        match new {
            Some(RowCacheValue::Hit {
                internal_key,
                value,
            }) => {
                assert_eq!(internal_key.sequence_number(), 20);
                assert_eq!(value.as_ref(), b"v20");
            }
            other => panic!("unexpected new visibility cache value: {:?}", other),
        }
        assert!(middle.is_none());
    }

    #[test]
    fn filter_cache_reports_hit_after_warmup() {
        let (_dir, path) = build_sstable(9);
        let cache = TableCache::new(2, 0, 0, 1024 * 1024);
        let reader = cache.get_or_open(9, &path).unwrap();
        let key = b"k03".to_vec();

        assert!(reader.get(&key).unwrap().is_some());
        assert!(reader.get(&key).unwrap().is_some());

        let metrics = cache.metrics();
        assert!(metrics.filter_misses >= 1);
        assert!(metrics.filter_hits >= 1);
    }
}
