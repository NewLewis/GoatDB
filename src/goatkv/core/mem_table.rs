use bytes::Bytes;
use ouroboros::self_referencing;
use std::sync::{Arc, RwLock, RwLockReadGuard};

use crate::goatkv::core::skip_list::{Iter, SkipList};
use crate::goatkv::encoding::internal_key::InternalKey;

// ==================== LSM MemTable 封装 ====================

#[derive(Debug)]
pub struct MemTableInner {
    skiplist: RwLock<SkipList<InternalKey>>,
    size_limit: usize,
}

impl MemTableInner {
    pub fn new(size_limit: usize) -> Self {
        Self {
            skiplist: RwLock::new(SkipList::new()),
            size_limit,
        }
    }

    /// 插入键值对
    pub fn put(&self, key: InternalKey, value: Bytes) {
        self.skiplist.write().unwrap().insert(key, value.into());
    }

    /// 获取值
    pub fn get(&self, key: &[u8]) -> Option<(InternalKey, Vec<u8>)> {
        match self.seek(key) {
            Some((inter_key, value)) => {
                if inter_key.user_key() == key {
                    Some((inter_key, value))
                } else {
                    None
                }
            }
            None => None,
        }
    }

    /// 查找键值对，返回 (InternalKey, value) 元组
    pub fn seek(&self, key: &[u8]) -> Option<(InternalKey, Vec<u8>)> {
        self.skiplist
            .read()
            .unwrap()
            .seek(key)
            .map(|(k, v)| (k.clone(), v.clone().into()))
    }

    pub fn iter(&self) -> impl Iterator<Item = (InternalKey, Bytes)> + '_ {
        let guard = self.skiplist.read().unwrap();

        MemTableIterBuilder {
            guard: guard,
            iter_builder: |guard| guard.iter(),
        }
        .build()
    }

    /// 是否需要 flush 到 immutable memtable
    pub fn should_flush(&self) -> bool {
        self.skiplist.read().unwrap().memory_usage() >= self.size_limit
    }

    pub fn len(&self) -> usize {
        self.skiplist.read().unwrap().len()
    }

    pub fn memory_usage(&self) -> usize {
        self.skiplist.read().unwrap().memory_usage()
    }
}

#[self_referencing]
struct MemTableIter<'a> {
    guard: RwLockReadGuard<'a, SkipList<InternalKey>>,
    #[borrows(guard)]
    #[covariant]
    iter: Iter<'this, InternalKey>,
}

impl<'a> Iterator for MemTableIter<'a> {
    type Item = (InternalKey, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        self.with_iter_mut(|iter| iter.next())
    }
}

/// LSM-Tree 的 MemTable，使用跳表实现
#[derive(Debug)]
pub struct MemTable {
    inner: Arc<MemTableInner>,
}

impl MemTable {
    pub fn new(size: usize) -> Self {
        Self {
            inner: Arc::new(MemTableInner::new(size)),
        }
    }

    /// 插入键值对
    pub fn put(&self, key: InternalKey, value: Bytes) {
        self.inner.put(key, value);
    }

    /// 获取值
    pub fn get(&self, key: &[u8]) -> Option<(InternalKey, Vec<u8>)> {
        self.inner.get(key)
    }

    /// 查找键值对，返回 (InternalKey, value) 元组
    pub fn seek(&self, key: &[u8]) -> Option<(InternalKey, Vec<u8>)> {
        self.inner.seek(key)
    }

    /// 是否需要 flush 到 immutable memtable
    pub fn should_flush(&self) -> bool {
        self.inner.should_flush()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn memory_usage(&self) -> usize {
        self.inner.memory_usage()
    }

    pub fn inner(&self) -> Arc<MemTableInner> {
        self.inner.clone()
    }
}

#[derive(Debug)]
pub struct ImmutableMemTable {
    inner: Arc<MemTableInner>,
}

impl ImmutableMemTable {
    pub fn new(inner: Arc<MemTableInner>) -> Self {
        Self { inner }
    }

    /// 获取值
    pub fn get(&self, key: &[u8]) -> Option<(InternalKey, Vec<u8>)> {
        self.inner.get(key)
    }

    /// 查找键值对，返回 (InternalKey, value) 元组
    pub fn seek(&self, key: &[u8]) -> Option<(InternalKey, Vec<u8>)> {
        self.inner.seek(key)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (InternalKey, Bytes)> + '_ {
        self.inner.iter()
    }
}

#[cfg(test)]
mod tests {
    use crate::goatkv::encoding::internal_key::InternalKey;

    use super::MemTable;

    #[test]
    fn test_memtable() {
        let memtable = MemTable::new(1024 * 1024); // 1MB

        for i in 0..1000 {
            let key = format!("key_{:06}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            memtable.put(InternalKey::new(key, 0, 1.into()), value.into());
        }

        assert_eq!(memtable.len(), 1000);

        let result = memtable.get(b"key_000500");
        assert_eq!(result.is_some(), true);
        let (_, val) = result.unwrap();
        assert_eq!(val, b"value_500".to_vec());
    }
}
