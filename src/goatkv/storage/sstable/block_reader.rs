use std::cmp::Ordering;

use crate::goatkv::format::coding;

/// SSTable块读取器，用于解码BlockBuilder创建的块
#[derive(Debug)]
pub struct BlockReader<'a> {
    /// 块的原始数据
    data: &'a [u8],
    /// 重启点数组
    restarts: Vec<u32>,
    /// 数据部分的结束位置（不包括重启点数组）
    data_end: usize,
}

impl<'a> BlockReader<'a> {
    /// 从原始字节创建BlockReader
    pub fn new(data: &'a [u8]) -> Result<Self, &'static str> {
        if data.len() < 4 {
            return Err("Block too small to contain restart count");
        }

        // 最后4字节是重启点数量
        let restart_count_bytes = &data[data.len() - 4..];
        let restart_count = u32::from_le_bytes([
            restart_count_bytes[0],
            restart_count_bytes[1],
            restart_count_bytes[2],
            restart_count_bytes[3],
        ]) as usize;

        if restart_count == 0 {
            // When restart count is 0, data ends at restart count position (last 4 bytes)
            // The actual data ends just before the restart count
            return Ok(Self {
                data,
                restarts: Vec::new(),
                data_end: data.len() - 4,
            });
        }

        // 检查是否有足够的空间容纳重启点数组
        let restart_array_size = restart_count * 4;
        if data.len() < 4 + restart_array_size {
            return Err("Block too small for restart array");
        }

        // 重启点数组位于数据末尾之前（在重启点数量之前）
        let restart_start = data.len() - 4 - restart_array_size;
        let restart_array_bytes = &data[restart_start..data.len() - 4];

        // 解析重启点
        let mut restarts = Vec::with_capacity(restart_count);
        for i in 0..restart_count {
            let start = i * 4;
            let restart_point = u32::from_le_bytes([
                restart_array_bytes[start],
                restart_array_bytes[start + 1],
                restart_array_bytes[start + 2],
                restart_array_bytes[start + 3],
            ]);
            restarts.push(restart_point);
        }

        // data_end should be restart_start to exclude restart array from iteration
        Ok(Self {
            data,
            restarts,
            data_end: restart_start,
        })
    }

    /// 获取块中的条目数量
    pub fn entry_count(&self) -> usize {
        self.restarts.len() * 16 // 每个重启点对应16个条目
    }

    /// 在块中查找给定的键
    /// 如果找到，返回对应的值；否则返回None
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if self.restarts.is_empty() {
            // 没有重启点，从块开头进行线性搜索
            return self.linear_search_from_start(key);
        }

        // 使用重启点进行二分查找
        let mut left = 0;
        let mut right = self.restarts.len() - 1;

        while left <= right {
            let mid = (left + right) / 2;
            let restart_pos = self.restarts[mid] as usize;

            // 从重启点读取条目
            match self.decode_entry_at(restart_pos) {
                Ok((_shared, entry_key, _)) => {
                    match entry_key.as_slice().cmp(key) {
                        Ordering::Less => {
                            // 当前键小于目标键，向右搜索
                            left = mid + 1;
                        }
                        Ordering::Greater => {
                            // 当前键大于目标键，向左搜索
                            if mid == 0 {
                                break;
                            }
                            right = mid - 1;
                        }
                        Ordering::Equal => {
                            // 找到完全匹配，但需要检查后续条目（因为重启点可能不是精确匹配）
                            return self.linear_search_from_restart(mid, key);
                        }
                    }
                }
                Err(_) => {
                    // 解码失败，回退到线性搜索
                    return self.linear_search_full(key);
                }
            }
        }

        // 没有在重启点找到精确匹配，在最后的候选区间进行线性搜索
        left = left.saturating_sub(1);
        self.linear_search_from_restart(left, key)
    }

    /// 从指定的重启点开始线性搜索
    fn linear_search_from_restart(&self, restart_index: usize, key: &[u8]) -> Option<Vec<u8>> {
        let restart_pos = self.restarts[restart_index] as usize;
        let mut current_pos = restart_pos;

        // 计算这个重启点组的结束位置
        let end_pos = if restart_index + 1 < self.restarts.len() {
            self.restarts[restart_index + 1] as usize
        } else {
            self.data_end
        };

        let mut prev_key = Vec::new();

        while current_pos < end_pos {
            match self.decode_entry_at(current_pos) {
                Ok((shared, entry_key, entry_value)) => {
                    // 重建完整的key
                    let full_key = if prev_key.is_empty() {
                        // 第一个条目的共享长度应该为0
                        if shared != 0 {
                            return None; // 数据损坏，第一个条目的共享长度应该为0
                        }
                        entry_key.clone()
                    } else {
                        // 检查共享长度是否有效
                        if shared as usize > prev_key.len() {
                            return None; // 数据损坏，共享长度无效
                        }
                        let mut full_key = Vec::new();
                        full_key.extend_from_slice(&prev_key[..shared as usize]);
                        full_key.extend_from_slice(&entry_key);
                        full_key
                    };

                    match full_key.as_slice().cmp(key) {
                        Ordering::Less => {
                            // 继续搜索
                            prev_key = full_key;
                            current_pos += self.entry_size_at(current_pos)?;
                        }
                        Ordering::Equal => {
                            return Some(entry_value);
                        }
                        Ordering::Greater => {
                            // 已超过目标键，停止搜索
                            return None;
                        }
                    }
                }
                Err(_) => {
                    return None;
                }
            }
        }

        None
    }

    /// 从块开头进行线性搜索
    fn linear_search_from_start(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut current_pos = 0;
        let mut prev_key = Vec::new();

        while current_pos < self.data_end {
            match self.decode_entry_at(current_pos) {
                Ok((shared, unshared_key, value)) => {
                    // Stop at empty entries - this indicates we've reached padding or restart array
                    if unshared_key.is_empty() && value.is_empty() {
                        return None;
                    }

                    // 重建完整的key
                    let full_key = if prev_key.is_empty() {
                        // 第一个条目的共享长度应该为0
                        if shared != 0 {
                            return None; // 数据损坏，第一个条目的共享长度应该为0
                        }
                        unshared_key.clone()
                    } else {
                        // 检查共享长度是否有效
                        if shared as usize > prev_key.len() {
                            return None; // 数据损坏，共享长度无效
                        }
                        let mut full_key = Vec::new();
                        full_key.extend_from_slice(&prev_key[..shared as usize]);
                        full_key.extend_from_slice(&unshared_key);
                        full_key
                    };

                    match full_key.as_slice().cmp(key) {
                        Ordering::Less => {
                            // 继续搜索
                            prev_key = full_key;
                            current_pos += self.entry_size_at(current_pos)?;
                        }
                        Ordering::Equal => {
                            return Some(value);
                        }
                        Ordering::Greater => {
                            // 已超过目标键，停止搜索
                            return None;
                        }
                    }
                }
                Err(_) => {
                    return None;
                }
            }
        }

        None
    }

    /// 在整个块中进行线性搜索（回退策略）
    fn linear_search_full(&self, key: &[u8]) -> Option<Vec<u8>> {
        let iter = self.iter();
        let mut prev_key = Vec::new();

        for (entry_key, entry_value) in iter {
            // 重建完整的key
            let full_key = if prev_key.is_empty() {
                entry_key.clone()
            } else {
                let shared = self.compute_shared(&prev_key, &entry_key);
                let mut full_key = Vec::new();
                full_key.extend_from_slice(&prev_key[..shared as usize]);
                full_key.extend_from_slice(&entry_key);
                full_key
            };

            match full_key.as_slice().cmp(key) {
                Ordering::Less => {
                    prev_key = full_key;
                }
                Ordering::Equal => {
                    return Some(entry_value);
                }
                Ordering::Greater => {
                    return None;
                }
            }
        }

        None
    }

    /// 解码指定位置的条目
    fn decode_entry_at(&self, pos: usize) -> Result<(u32, Vec<u8>, Vec<u8>), &'static str> {
        if pos >= self.data_end {
            return Err("Position out of bounds");
        }

        let mut offset = pos;

        // 解码共享长度
        let (shared, bytes_read) = self.decode_varint_at(offset)?;
        offset += bytes_read;

        // 解码非共享长度
        let (unshared, bytes_read) = self.decode_varint_at(offset)?;
        offset += bytes_read;

        // 解码值长度
        let (value_len, bytes_read) = self.decode_varint_at(offset)?;
        offset += bytes_read;

        // 检查边界
        if offset + unshared as usize + value_len as usize > self.data_end {
            return Err("Entry exceeds block boundary");
        }

        // 读取非共享的key部分
        let unshared_key = &self.data[offset..offset + unshared as usize];
        offset += unshared as usize;

        // 读取值
        let value = &self.data[offset..offset + value_len as usize];

        Ok((shared as u32, unshared_key.to_vec(), value.to_vec()))
    }

    /// 计算指定位置条目的总大小
    fn entry_size_at(&self, pos: usize) -> Option<usize> {
        if pos >= self.data_end {
            return None;
        }

        let mut offset = pos;
        let mut total_size = 0;

        // 解码共享长度并累加大小
        match self.decode_varint_at(offset) {
            Ok((_, bytes_read)) => {
                total_size += bytes_read;
                offset += bytes_read;
            }
            Err(_) => return None,
        }

        // 解码非共享长度并累加大小
        match self.decode_varint_at(offset) {
            Ok((unshared, bytes_read)) => {
                total_size += bytes_read;
                offset += bytes_read;

                // 解码值长度并累加大小
                match self.decode_varint_at(offset) {
                    Ok((value_len, bytes_read)) => {
                        total_size += bytes_read;
                        total_size += unshared as usize + value_len as usize;
                    }
                    Err(_) => return None,
                }
            }
            Err(_) => return None,
        }

        Some(total_size)
    }

    /// 在指定位置解码varint
    fn decode_varint_at(&self, pos: usize) -> Result<(u64, usize), &'static str> {
        if pos >= self.data_end {
            return Err("Position out of bounds");
        }

        let bytes = &self.data[pos..self.data_end];
        coding::decode_varint64_with_length(bytes)
    }

    /// 静态方法：在指定位置解码varint（用于构造函数中）
    /// 计算两个键之间的共享前缀长度
    fn compute_shared(&self, key1: &[u8], key2: &[u8]) -> u32 {
        let min_len = key1.len().min(key2.len());
        let mut shared = 0;

        for i in 0..min_len {
            if key1[i] == key2[i] {
                shared += 1;
            } else {
                break;
            }
        }

        shared as u32
    }

    /// 创建块的迭代器
    pub fn iter(&self) -> BlockIterator<'_, 'a> {
        BlockIterator::new(self)
    }
}

/// BlockReader的迭代器，用于顺序遍历所有条目
pub struct BlockIterator<'iter, 'data: 'iter> {
    reader: &'iter BlockReader<'data>,
    current_pos: usize,
    prev_key: Vec<u8>,
    end_pos: usize,
}

impl<'iter, 'data: 'iter> BlockIterator<'iter, 'data> {
    fn new(reader: &'iter BlockReader<'data>) -> Self {
        Self {
            reader,
            current_pos: 0,
            prev_key: Vec::new(),
            end_pos: reader.data_end,
        }
    }
}

impl<'iter, 'data> Iterator for BlockIterator<'iter, 'data>
where
    'data: 'iter,
{
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_pos >= self.end_pos {
            return None;
        }

        match self.reader.decode_entry_at(self.current_pos) {
            Ok((shared, unshared_key, value)) => {
                // Stop at empty entries - this indicates we've reached padding or restart array
                if unshared_key.is_empty() && value.is_empty() {
                    self.current_pos = self.end_pos;
                    return None;
                }

                // 重建完整的key
                let full_key = if self.prev_key.is_empty() {
                    unshared_key.clone()
                } else {
                    let mut full_key = Vec::new();
                    if shared as usize <= self.prev_key.len() {
                        full_key.extend_from_slice(&self.prev_key[..shared as usize]);
                    }
                    full_key.extend_from_slice(&unshared_key);
                    full_key
                };

                // 更新位置和prev_key
                if let Some(entry_size) = self.reader.entry_size_at(self.current_pos) {
                    self.current_pos += entry_size;
                } else {
                    // 无法计算条目大小，停止迭代
                    self.current_pos = self.end_pos;
                }
                self.prev_key = full_key.clone();

                Some((full_key, value))
            }
            Err(_) => {
                // 解码失败，停止迭代
                self.current_pos = self.end_pos;
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::goatkv::storage::sstable::BlockBuilder;

    #[test]
    fn test_block_reader_new() {
        let mut builder = BlockBuilder::new();
        builder.add(b"key1", b"value1");
        builder.add(b"key2", b"value2");
        let (data, _) = builder.finish();

        let reader = BlockReader::new(data);
        assert!(reader.is_ok());
    }

    #[test]
    fn test_block_reader_empty() {
        // 测试空块
        let mut builder = BlockBuilder::new();
        let (data, _) = builder.finish();

        let reader = BlockReader::new(data).unwrap();
        assert_eq!(reader.restarts.len(), 0);
        assert_eq!(reader.entry_count(), 0);
    }

    #[test]
    fn test_block_reader_iter() {
        let mut builder = BlockBuilder::new();
        builder.add(b"key1", b"value1");
        builder.add(b"key2", b"value2");
        builder.add(b"key3", b"value3");
        let (data, _) = builder.finish();

        let reader = BlockReader::new(data).unwrap();
        let mut iter = reader.iter();

        assert_eq!(iter.next(), Some((b"key1".to_vec(), b"value1".to_vec())));
        assert_eq!(iter.next(), Some((b"key2".to_vec(), b"value2".to_vec())));
        assert_eq!(iter.next(), Some((b"key3".to_vec(), b"value3".to_vec())));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_block_reader_get() {
        let mut builder = BlockBuilder::new();
        builder.add(b"key1", b"value1");
        builder.add(b"key2", b"value2");
        builder.add(b"key3", b"value3");
        let (data, _) = builder.finish();

        let reader = BlockReader::new(data).unwrap();

        let result1 = reader.get(b"key1");
        let result2 = reader.get(b"key2");
        let result3 = reader.get(b"key3");

        assert_eq!(result1, Some(b"value1".to_vec()));
        assert_eq!(result2, Some(b"value2".to_vec()));
        assert_eq!(result3, Some(b"value3".to_vec()));
        assert_eq!(reader.get(b"key4"), None);
    }

    #[test]
    fn test_block_reader_with_shared_prefix() {
        let mut builder = BlockBuilder::new();
        builder.add(b"apple", b"value1");
        builder.add(b"application", b"value2");
        builder.add(b"apply", b"value3");
        let (data, _) = builder.finish();

        let reader = BlockReader::new(data).unwrap();

        // 测试获取
        assert_eq!(reader.get(b"apple"), Some(b"value1".to_vec()));
        assert_eq!(reader.get(b"application"), Some(b"value2".to_vec()));
        assert_eq!(reader.get(b"apply"), Some(b"value3".to_vec()));
        assert_eq!(reader.get(b"app"), None);
        assert_eq!(reader.get(b"appl"), None);

        // 测试迭代
        let mut iter = reader.iter();
        assert_eq!(iter.next(), Some((b"apple".to_vec(), b"value1".to_vec())));
        assert_eq!(
            iter.next(),
            Some((b"application".to_vec(), b"value2".to_vec()))
        );
        assert_eq!(iter.next(), Some((b"apply".to_vec(), b"value3".to_vec())));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_block_reader_many_entries() {
        let mut builder = BlockBuilder::new();

        // 添加10个简单的条目
        for i in 0..10 {
            let key = format!("k{}", i);
            let value = format!("v{}", i);
            builder.add(key.as_bytes(), value.as_bytes());
        }

        let (data, _) = builder.finish();
        let reader = BlockReader::new(data).unwrap();

        // 验证所有条目都能正确读取
        for i in 0..10 {
            let key = format!("k{}", i);
            let expected_value = format!("v{}", i);
            assert_eq!(
                reader.get(key.as_bytes()),
                Some(expected_value.into_bytes()),
                "Failed to get key {}",
                i
            );
        }

        // 验证迭代器
        let mut iter = reader.iter();
        for i in 0..10 {
            let expected_key = format!("k{}", i);
            let expected_value = format!("v{}", i);
            assert_eq!(
                iter.next(),
                Some((expected_key.into_bytes(), expected_value.into_bytes())),
                "Failed to iterate at index {}",
                i
            );
        }
        assert_eq!(iter.next(), None, "Iterator should be exhausted");
    }

    #[test]
    fn test_block_reader_corrupted_data() {
        // 测试数据太小（小于4字节，无法包含restart count）
        let data = vec![0u8; 3];
        let reader = BlockReader::new(&data);
        assert!(reader.is_err(), "Data too small should return error");

        // 测试无效的varint（所有字节都有延续位）
        let data = vec![
            0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00,
        ];
        let reader = BlockReader::new(&data);
        assert!(reader.is_err(), "Incomplete varint should return error");

        // 测试restart array超出边界
        let mut data = vec![0u8; 100];
        // Set last 4 bytes as restart count = 100 (too large)
        data[96..100].copy_from_slice(&100u32.to_le_bytes());
        let reader = BlockReader::new(&data);
        assert!(reader.is_err(), "Invalid restart count should return error");
    }
}
