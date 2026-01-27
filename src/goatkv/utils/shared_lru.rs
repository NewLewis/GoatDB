use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct SharedLruMetricsOptions {
    pub hits: bool,
    pub misses: bool,
    pub inserts: bool,
    pub updates: bool,
    pub evictions: bool,
    pub removals: bool,
}

impl Default for SharedLruMetricsOptions {
    fn default() -> Self {
        Self {
            hits: true,
            misses: true,
            inserts: true,
            updates: true,
            evictions: true,
            removals: true,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct SharedLruMetrics {
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub updates: u64,
    pub evictions: u64,
    pub removals: u64,
}

pub struct SharedLruOptions<K, V> {
    pub max_entries: usize,
    pub max_charge: Option<usize>,
    pub promote_on_get: bool,
    pub hash_capacity: usize,
    pub shards: usize,
    pub weigher: Option<Arc<dyn Fn(&K, &V) -> usize + Send + Sync>>,
    pub metrics: SharedLruMetricsOptions,
    pub evict_hook: Option<Arc<dyn Fn(&K, &V) + Send + Sync>>,
}

impl<K, V> Clone for SharedLruOptions<K, V> {
    fn clone(&self) -> Self {
        Self {
            max_entries: self.max_entries,
            max_charge: self.max_charge,
            promote_on_get: self.promote_on_get,
            hash_capacity: self.hash_capacity,
            shards: self.shards,
            weigher: self.weigher.clone(),
            metrics: self.metrics.clone(),
            evict_hook: self.evict_hook.clone(),
        }
    }
}

impl<K, V> Default for SharedLruOptions<K, V> {
    fn default() -> Self {
        Self {
            max_entries: 1024,
            max_charge: None,
            promote_on_get: true,
            hash_capacity: 1024,
            shards: 16,
            weigher: None,
            metrics: SharedLruMetricsOptions::default(),
            evict_hook: None,
        }
    }
}

impl<K, V> SharedLruOptions<K, V> {
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

    pub fn with_promote_on_get(mut self, promote_on_get: bool) -> Self {
        self.promote_on_get = promote_on_get;
        self
    }

    pub fn with_hash_capacity(mut self, hash_capacity: usize) -> Self {
        self.hash_capacity = hash_capacity;
        self
    }

    pub fn with_shards(mut self, shards: usize) -> Self {
        self.shards = shards.max(1);
        self
    }

    pub fn with_weigher<F>(mut self, weigher: F) -> Self
    where
        F: Fn(&K, &V) -> usize + Send + Sync + 'static,
    {
        self.weigher = Some(Arc::new(weigher));
        self
    }

    pub fn with_metrics(mut self, metrics: SharedLruMetricsOptions) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn with_evict_hook<F>(mut self, evict_hook: F) -> Self
    where
        F: Fn(&K, &V) + Send + Sync + 'static,
    {
        self.evict_hook = Some(Arc::new(evict_hook));
        self
    }
}

pub struct SharedLruCache<K, V> {
    shards: Vec<LruShard<K, V>>,
    hasher: RandomState,
}

impl<K, V> SharedLruCache<K, V>
where
    K: Eq + Hash + Clone,
{
    pub fn new(mut options: SharedLruOptions<K, V>) -> Self {
        if options.hash_capacity == 0 {
            options.hash_capacity = options.max_entries;
        }
        if options.shards == 0 {
            options.shards = 1;
        }

        let shard_count = options.shards;
        let mut shards = Vec::with_capacity(shard_count);
        for shard_idx in 0..shard_count {
            let mut shard_options = options.clone();
            shard_options.max_entries = split_quota(options.max_entries, shard_idx, shard_count);
            shard_options.max_charge = options
                .max_charge
                .map(|max_charge| split_quota(max_charge, shard_idx, shard_count));
            shard_options.hash_capacity =
                split_quota(options.hash_capacity, shard_idx, shard_count).max(1);
            let inner = LruInner::new(shard_options.hash_capacity);
            shards.push(LruShard {
                options: shard_options,
                inner: Mutex::new(inner),
            });
        }

        Self {
            shards,
            hasher: RandomState::new(),
        }
    }

    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let shard = self.pick_shard(&key);
        let charge = self.entry_charge(&shard.options, &key, &value);
        if is_disabled(&shard.options, charge) {
            let mut inner = shard.inner.lock().expect("SharedLruCache mutex poisoned");
            return inner.remove(&shard.options, &key);
        }

        let mut evicted = Vec::new();
        let mut inner = shard.inner.lock().expect("SharedLruCache mutex poisoned");
        let old = inner.insert(&shard.options, key, value, charge, &mut evicted);
        drop(inner);
        run_evict_hook(&shard.options, evicted);
        old
    }

    pub fn get(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let shard = self.pick_shard(key);
        let mut inner = shard.inner.lock().expect("SharedLruCache mutex poisoned");
        inner.get(&shard.options, key, true)
    }

    pub fn peek(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let shard = self.pick_shard(key);
        let mut inner = shard.inner.lock().expect("SharedLruCache mutex poisoned");
        inner.get(&shard.options, key, false)
    }

    pub fn contains(&self, key: &K) -> bool {
        let shard = self.pick_shard(key);
        let inner = shard.inner.lock().expect("SharedLruCache mutex poisoned");
        inner.map.contains_key(key)
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        let shard = self.pick_shard(key);
        let mut inner = shard.inner.lock().expect("SharedLruCache mutex poisoned");
        inner.remove(&shard.options, key)
    }

    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                let inner = shard.inner.lock().expect("SharedLruCache mutex poisoned");
                inner.len
            })
            .sum()
    }

    pub fn charge(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                let inner = shard.inner.lock().expect("SharedLruCache mutex poisoned");
                inner.charge
            })
            .sum()
    }

    pub fn clear(&self) {
        for shard in &self.shards {
            let mut inner = shard.inner.lock().expect("SharedLruCache mutex poisoned");
            inner.clear(&shard.options);
        }
    }

    pub fn metrics(&self) -> SharedLruMetrics {
        let mut aggregated = SharedLruMetrics::default();
        for shard in &self.shards {
            let inner = shard.inner.lock().expect("SharedLruCache mutex poisoned");
            aggregated.hits = aggregated.hits.saturating_add(inner.metrics.hits);
            aggregated.misses = aggregated.misses.saturating_add(inner.metrics.misses);
            aggregated.inserts = aggregated.inserts.saturating_add(inner.metrics.inserts);
            aggregated.updates = aggregated.updates.saturating_add(inner.metrics.updates);
            aggregated.evictions = aggregated.evictions.saturating_add(inner.metrics.evictions);
            aggregated.removals = aggregated.removals.saturating_add(inner.metrics.removals);
        }
        aggregated
    }

    pub fn reset_metrics(&self) {
        for shard in &self.shards {
            let mut inner = shard.inner.lock().expect("SharedLruCache mutex poisoned");
            inner.metrics = SharedLruMetrics::default();
        }
    }

    fn pick_shard(&self, key: &K) -> &LruShard<K, V> {
        let mut hasher = self.hasher.build_hasher();
        key.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        let idx = hash % self.shards.len();
        &self.shards[idx]
    }

    fn entry_charge(&self, options: &SharedLruOptions<K, V>, key: &K, value: &V) -> usize {
        match options.weigher.as_ref() {
            Some(weigher) => weigher(key, value),
            None => 1,
        }
    }
}

struct LruShard<K, V> {
    options: SharedLruOptions<K, V>,
    inner: Mutex<LruInner<K, V>>,
}

fn run_evict_hook<K, V>(options: &SharedLruOptions<K, V>, evicted: Vec<(K, V)>) {
    if let Some(hook) = options.evict_hook.as_ref() {
        for (key, value) in evicted {
            hook(&key, &value);
        }
    }
}

fn is_disabled<K, V>(options: &SharedLruOptions<K, V>, charge: usize) -> bool {
    if options.max_entries == 0 {
        return true;
    }
    match options.max_charge {
        Some(max_charge) if max_charge == 0 => true,
        Some(max_charge) => charge > max_charge,
        None => false,
    }
}

fn split_quota(total: usize, idx: usize, shards: usize) -> usize {
    if shards == 0 {
        return 0;
    }
    let base = total / shards;
    let extra = total % shards;
    base + if idx < extra { 1 } else { 0 }
}

struct Node<K, V> {
    key: K,
    value: V,
    charge: usize,
    prev: Option<usize>,
    next: Option<usize>,
}

struct LruInner<K, V> {
    map: HashMap<K, usize>,
    nodes: Vec<Option<Node<K, V>>>,
    free: Vec<usize>,
    head: Option<usize>,
    tail: Option<usize>,
    len: usize,
    charge: usize,
    metrics: SharedLruMetrics,
}

impl<K, V> LruInner<K, V>
where
    K: Eq + Hash + Clone,
{
    fn new(hash_capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(hash_capacity),
            nodes: Vec::new(),
            free: Vec::new(),
            head: None,
            tail: None,
            len: 0,
            charge: 0,
            metrics: SharedLruMetrics::default(),
        }
    }

    fn insert(
        &mut self,
        options: &SharedLruOptions<K, V>,
        key: K,
        value: V,
        charge: usize,
        evicted: &mut Vec<(K, V)>,
    ) -> Option<V> {
        if let Some(&idx) = self.map.get(&key) {
            let node = self.nodes[idx].as_mut().expect("LRU node missing");
            let old_value = std::mem::replace(&mut node.value, value);
            let old_charge = node.charge;
            node.charge = charge;
            self.charge = self
                .charge
                .saturating_sub(old_charge)
                .saturating_add(charge);
            if options.metrics.updates {
                self.metrics.updates += 1;
            }
            self.touch(idx);
            self.evict_if_needed(options, evicted);
            return Some(old_value);
        }

        let node = Node {
            key: key.clone(),
            value,
            charge,
            prev: None,
            next: None,
        };
        let idx = self.alloc_node(node);
        self.map.insert(key, idx);
        self.attach_front(idx);
        self.len += 1;
        self.charge = self.charge.saturating_add(charge);
        if options.metrics.inserts {
            self.metrics.inserts += 1;
        }
        self.evict_if_needed(options, evicted);
        None
    }

    fn get(&mut self, options: &SharedLruOptions<K, V>, key: &K, promote: bool) -> Option<V>
    where
        V: Clone,
    {
        let idx = match self.map.get(key) {
            Some(&idx) => idx,
            None => {
                if options.metrics.misses {
                    self.metrics.misses += 1;
                }
                return None;
            }
        };

        if options.metrics.hits {
            self.metrics.hits += 1;
        }
        if promote && options.promote_on_get {
            self.touch(idx);
        }
        self.nodes[idx].as_ref().map(|node| node.value.clone())
    }

    fn remove(&mut self, options: &SharedLruOptions<K, V>, key: &K) -> Option<V> {
        let idx = match self.map.remove(key) {
            Some(idx) => idx,
            None => return None,
        };

        let (_removed_key, removed_value, removed_charge) = self.remove_idx(idx);
        if options.metrics.removals {
            self.metrics.removals += 1;
        }
        self.charge = self.charge.saturating_sub(removed_charge);
        self.len = self.len.saturating_sub(1);
        Some(removed_value)
    }

    fn clear(&mut self, options: &SharedLruOptions<K, V>) {
        let removal_count = self.len as u64;
        self.map.clear();
        self.nodes.clear();
        self.free.clear();
        self.head = None;
        self.tail = None;
        self.len = 0;
        self.charge = 0;
        if options.metrics.removals {
            self.metrics.removals = self.metrics.removals.saturating_add(removal_count);
        }
    }

    fn evict_if_needed(&mut self, options: &SharedLruOptions<K, V>, evicted: &mut Vec<(K, V)>) {
        while self.over_capacity(options) {
            let idx = match self.tail {
                Some(idx) => idx,
                None => break,
            };
            let (key, value, charge) = self.remove_idx(idx);
            if options.metrics.evictions {
                self.metrics.evictions += 1;
            }
            self.charge = self.charge.saturating_sub(charge);
            self.len = self.len.saturating_sub(1);
            self.map.remove(&key);
            evicted.push((key, value));
        }
    }

    fn over_capacity(&self, options: &SharedLruOptions<K, V>) -> bool {
        if options.max_entries > 0 && self.len > options.max_entries {
            return true;
        }
        match options.max_charge {
            Some(max_charge) => self.charge > max_charge,
            None => false,
        }
    }

    fn touch(&mut self, idx: usize) {
        if self.head == Some(idx) {
            return;
        }
        self.detach(idx);
        self.attach_front(idx);
    }

    fn detach(&mut self, idx: usize) {
        let (prev, next) = {
            let node = self.nodes[idx].as_ref().expect("LRU node missing");
            (node.prev, node.next)
        };

        if let Some(prev_idx) = prev {
            if let Some(prev_node) = self.nodes[prev_idx].as_mut() {
                prev_node.next = next;
            }
        } else {
            self.head = next;
        }

        if let Some(next_idx) = next {
            if let Some(next_node) = self.nodes[next_idx].as_mut() {
                next_node.prev = prev;
            }
        } else {
            self.tail = prev;
        }
    }

    fn attach_front(&mut self, idx: usize) {
        let old_head = self.head;
        {
            let node = self.nodes[idx].as_mut().expect("LRU node missing");
            node.prev = None;
            node.next = old_head;
        }

        if let Some(old_head_idx) = old_head {
            if let Some(old_head_node) = self.nodes[old_head_idx].as_mut() {
                old_head_node.prev = Some(idx);
            }
        } else {
            self.tail = Some(idx);
        }

        self.head = Some(idx);
    }

    fn remove_idx(&mut self, idx: usize) -> (K, V, usize) {
        self.detach(idx);
        let node = self.nodes[idx].take().expect("LRU node missing");
        self.free.push(idx);
        (node.key, node.value, node.charge)
    }

    fn alloc_node(&mut self, node: Node<K, V>) -> usize {
        if let Some(idx) = self.free.pop() {
            self.nodes[idx] = Some(node);
            idx
        } else {
            self.nodes.push(Some(node));
            self.nodes.len() - 1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_lru_basic_insert_get() {
        let cache = SharedLruCache::new(
            SharedLruOptions::default()
                .with_max_entries(2)
                .with_shards(1),
        );
        cache.insert("a", 10);
        cache.insert("b", 20);

        assert_eq!(cache.get(&"a"), Some(10));
        assert_eq!(cache.get(&"b"), Some(20));
        assert_eq!(cache.get(&"c"), None);
    }

    #[test]
    fn shared_lru_evicts_lru_entry() {
        let cache = SharedLruCache::new(
            SharedLruOptions::default()
                .with_max_entries(2)
                .with_shards(1),
        );
        cache.insert("a", 10);
        cache.insert("b", 20);
        cache.get(&"a");
        cache.insert("c", 30);

        assert_eq!(cache.get(&"a"), Some(10));
        assert_eq!(cache.get(&"b"), None);
        assert_eq!(cache.get(&"c"), Some(30));
    }

    #[test]
    fn shared_lru_respects_charge_limit() {
        let options = SharedLruOptions::default()
            .with_max_entries(10)
            .with_max_charge(3)
            .with_shards(1)
            .with_weigher(|_k: &&str, v: &usize| *v);
        let cache = SharedLruCache::new(options);
        cache.insert("a", 1);
        cache.insert("b", 1);
        cache.insert("c", 1);
        cache.insert("d", 1);

        assert_eq!(cache.len(), 3);
        assert_eq!(cache.charge(), 3);
    }

    #[test]
    fn shared_lru_metrics_toggle() {
        let metrics = SharedLruMetricsOptions {
            hits: true,
            misses: false,
            inserts: true,
            updates: true,
            evictions: true,
            removals: true,
        };
        let options = SharedLruOptions::default()
            .with_max_entries(1)
            .with_shards(1)
            .with_metrics(metrics);
        let cache = SharedLruCache::new(options);
        cache.insert("a", 1);
        cache.get(&"a");
        cache.get(&"missing");
        cache.insert("b", 2);
        cache.remove(&"b");

        let stats = cache.metrics();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.inserts, 2);
        assert_eq!(stats.evictions, 1);
        assert_eq!(stats.removals, 1);
    }
}
