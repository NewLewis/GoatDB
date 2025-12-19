use crate::goatkv::skip_list::SkipList;

// ==================== LSM MemTable 封装 ====================

/// LSM-Tree 的 MemTable，使用跳表实现
#[derive(Debug)]
pub struct ImmutableMemTable {
    skiplist: SkipList<Vec<u8>>,
}

impl ImmutableMemTable {
    pub fn new(skiplist: SkipList<Vec<u8>>) -> Self {
        Self { skiplist }
    }

    /// 获取值
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.skiplist.get(&key.to_vec()).map(|v| v.as_slice())
    }

    /// 查找键值对
    pub fn seek(&self, key: &[u8]) -> Option<&[u8]> {
        self.skiplist.seek(key).map(|(_, v)| v.as_slice())
    }

    // /// 遍历所有键值对（用于 flush）
    // pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
    //     self.skiplist
    //         .iter()
    //         .map(|(k, v)| (k.as_slice(), v.as_slice()))
    // }

    // pub fn range_iter<'a>(
    //     &'a self,
    //     start: &'a Vec<u8>,
    //     end: &'a Vec<u8>,
    // ) -> impl Iterator<Item = (&'a [u8], &'a [u8])> {
    //     self.skiplist
    //         .range(start, end)
    //         .map(|(k, v)| (k.as_slice(), v.as_slice()))
    // }

    pub fn len(&self) -> usize {
        self.skiplist.len()
    }
}
