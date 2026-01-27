use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::goatkv::storage::sstable::SSTableReader;
use crate::goatkv::utils::{
    SharedLruCache, SharedLruMetrics, SharedLruMetricsOptions, SharedLruOptions,
};

#[derive(Debug, Clone)]
pub struct TableCacheOptions {
    pub max_entries: usize,
    pub max_charge: Option<usize>,
    pub shards: usize,
    pub promote_on_get: bool,
    pub hash_capacity: usize,
    pub metrics: SharedLruMetricsOptions,
    pub charge_by_file_size: bool,
}

impl Default for TableCacheOptions {
    fn default() -> Self {
        Self {
            max_entries: 1024,
            max_charge: None,
            shards: 16,
            promote_on_get: true,
            hash_capacity: 1024,
            metrics: SharedLruMetricsOptions::default(),
            charge_by_file_size: true,
        }
    }
}

impl TableCacheOptions {
    pub fn with_max_entries(mut self, max_entries: usize) -> Self {
        self.max_entries = max_entries;
        if self.hash_capacity == 0 {
            self.hash_capacity = max_entries;
        }
        self
    }

    pub fn with_max_charge(mut self, max_charge: usize) -> Self {
        self.max_charge = Some(max_charge);
        self
    }

    pub fn without_max_charge(mut self) -> Self {
        self.max_charge = None;
        self
    }

    pub fn with_shards(mut self, shards: usize) -> Self {
        self.shards = shards.max(1);
        self
    }

    pub fn with_promote_on_get(mut self, promote_on_get: bool) -> Self {
        self.promote_on_get = promote_on_get;
        self
    }

    pub fn with_hash_capacity(mut self, hash_capacity: usize) -> Self {
        self.hash_capacity = hash_capacity;
        self
    }

    pub fn with_metrics(mut self, metrics: SharedLruMetricsOptions) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn with_charge_by_file_size(mut self, charge_by_file_size: bool) -> Self {
        self.charge_by_file_size = charge_by_file_size;
        self
    }
}

#[derive(Debug)]
pub struct TableHandle {
    reader: Mutex<SSTableReader>,
    charge: usize,
}

impl TableHandle {
    fn new(reader: SSTableReader, charge: usize) -> Self {
        Self {
            reader: Mutex::new(reader),
            charge,
        }
    }

    pub fn lock(&self) -> MutexGuard<'_, SSTableReader> {
        self.reader.lock().expect("TableHandle mutex poisoned")
    }

    pub fn charge(&self) -> usize {
        self.charge
    }
}

pub struct TableCache {
    cache: SharedLruCache<u64, Arc<TableHandle>>,
    options: TableCacheOptions,
}

impl std::fmt::Debug for TableCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableCache")
            .field("options", &self.options)
            .finish()
    }
}

impl TableCache {
    pub fn new(options: TableCacheOptions) -> Self {
        let mut lru_options: SharedLruOptions<u64, Arc<TableHandle>> = SharedLruOptions::default()
            .with_max_entries(options.max_entries)
            .with_hash_capacity(options.hash_capacity)
            .with_shards(options.shards)
            .with_promote_on_get(options.promote_on_get)
            .with_metrics(options.metrics.clone())
            .with_weigher(|_k: &u64, v: &Arc<TableHandle>| v.charge());

        if let Some(max_charge) = options.max_charge {
            lru_options = lru_options.with_max_charge(max_charge);
        }

        let cache = SharedLruCache::new(lru_options);
        Self { cache, options }
    }

    pub fn get(&self, file_id: u64) -> Option<Arc<TableHandle>> {
        self.cache.get(&file_id)
    }

    pub fn get_or_open(
        &self,
        file_id: u64,
        file_size: u64,
        path: &Path,
    ) -> io::Result<Arc<TableHandle>> {
        if let Some(handle) = self.cache.get(&file_id) {
            return Ok(handle);
        }

        let charge = self.entry_charge(file_size);
        let reader = SSTableReader::open(path)?;
        let handle = Arc::new(TableHandle::new(reader, charge));

        if self.should_cache(charge) {
            self.cache.insert(file_id, handle.clone());
        }

        Ok(handle)
    }

    pub fn remove(&self, file_id: u64) -> Option<Arc<TableHandle>> {
        self.cache.remove(&file_id)
    }

    pub fn clear(&self) {
        self.cache.clear();
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn charge(&self) -> usize {
        self.cache.charge()
    }

    pub fn metrics(&self) -> SharedLruMetrics {
        self.cache.metrics()
    }

    pub fn reset_metrics(&self) {
        self.cache.reset_metrics();
    }

    fn entry_charge(&self, file_size: u64) -> usize {
        if !self.options.charge_by_file_size {
            return 1;
        }
        let max = usize::MAX as u64;
        file_size.min(max) as usize
    }

    fn should_cache(&self, charge: usize) -> bool {
        if self.options.max_entries == 0 {
            return false;
        }
        match self.options.max_charge {
            Some(max_charge) if max_charge == 0 => false,
            Some(max_charge) => charge <= max_charge,
            None => true,
        }
    }
}
