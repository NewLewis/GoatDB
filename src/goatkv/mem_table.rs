use crate::goatkv::skip_list::SkipList;

// ==================== LSM MemTable 封装 ====================

/// LSM-Tree 的 MemTable，使用跳表实现
#[derive(Debug)]
pub struct MemTable {
    skiplist: SkipList<Vec<u8>, Vec<u8>>,
    size_limit: usize,
}

impl MemTable {
    pub fn new(size_limit: usize) -> Self {
        Self {
            skiplist: SkipList::new(),
            size_limit,
        }
    }

    /// 插入键值对
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> bool {
        self.skiplist.insert(key, value);
        self.should_flush()
    }

    /// 获取值
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.skiplist.get(&key.to_vec()).map(|v| v.as_slice())
    }

    /// 是否需要 flush 到 SSTable
    pub fn should_flush(&self) -> bool {
        self.skiplist.memory_usage() >= self.size_limit
    }

    /// 遍历所有键值对（用于 flush）
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.skiplist
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
    }

    pub fn range_iter<'a>(
        &'a self,
        start: &'a Vec<u8>,
        end: &'a Vec<u8>,
    ) -> impl Iterator<Item = (&'a [u8], &'a [u8])> {
        self.skiplist
            .range(start, end)
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
    }

    pub fn len(&self) -> usize {
        self.skiplist.len()
    }

    pub fn memory_usage(&self) -> usize {
        self.skiplist.memory_usage()
    }
}

#[cfg(test)]
mod tests {
    use super::MemTable;

    #[test]
    fn test_memtable() {
        let mut memtable = MemTable::new(1024 * 1024); // 1MB

        for i in 0..1000 {
            let key = format!("key_{:06}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            memtable.put(key, value);
        }

        assert_eq!(memtable.len(), 1000);

        let val = memtable.get(b"key_000500");
        assert_eq!(val, Some(b"value_500".as_slice()));
    }
}
