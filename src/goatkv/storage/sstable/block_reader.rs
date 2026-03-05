use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use crate::goatkv::error::{Error as GoatError, Result as GoatResult};
use crate::goatkv::format::coding;
use crate::goatkv::format::internal_key::InternalKey;

#[derive(Debug)]
pub(crate) struct BlockSearchIndex {
    restarts: Arc<[u32]>,
    restart_keys: Arc<[Vec<u8>]>,
    user_key_hash_index: Arc<HashMap<Vec<u8>, UserKeyHashEntry>>,
    data_end: usize,
}

#[derive(Debug, Clone)]
struct UserKeyHashEntry {
    internal_key: InternalKey,
    value_offset: u32,
    value_len: u32,
}

/// SSTable块读取器，用于解码BlockBuilder创建的块
#[derive(Debug)]
pub struct BlockReader<'a> {
    /// 块的原始数据
    data: &'a [u8],
    /// 重启点数组
    restarts: Arc<[u32]>,
    /// 每个重启点对应的完整 key（解码缓存，避免查找时重复解码）
    restart_keys: Arc<[Vec<u8>]>,
    /// user_key -> (latest internal key, value range) hash index for fast point lookup.
    user_key_hash_index: Arc<HashMap<Vec<u8>, UserKeyHashEntry>>,
    /// 数据部分的结束位置（不包括重启点数组）
    data_end: usize,
}

#[derive(Debug, Clone, Copy)]
struct DecodedEntryView<'a> {
    shared: u32,
    unshared_key: &'a [u8],
    value: &'a [u8],
    value_offset: usize,
    total_len: usize,
}

impl<'a> BlockReader<'a> {
    /// 从原始字节创建BlockReader
    pub fn new(data: &'a [u8]) -> GoatResult<Self> {
        let search_index = Arc::new(Self::parse_search_index(data)?);
        Self::with_search_index(data, search_index)
    }

    pub(crate) fn parse_search_index(data: &'a [u8]) -> GoatResult<BlockSearchIndex> {
        if data.len() < 4 {
            return Err(GoatError::corruption(
                "block_reader",
                "block too small to contain restart count",
            ));
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
            let data_end = data.len() - 4;
            let reader = Self {
                data,
                restarts: Arc::from(Vec::<u32>::new().into_boxed_slice()),
                restart_keys: Arc::from(Vec::<Vec<u8>>::new().into_boxed_slice()),
                user_key_hash_index: Arc::new(HashMap::new()),
                data_end,
            };
            return Ok(BlockSearchIndex {
                restarts: Arc::from(Vec::<u32>::new().into_boxed_slice()),
                restart_keys: Arc::from(Vec::<Vec<u8>>::new().into_boxed_slice()),
                user_key_hash_index: Arc::new(reader.collect_user_key_hash_index()),
                data_end,
            });
        }

        // 检查是否有足够的空间容纳重启点数组
        let restart_array_size = restart_count * 4;
        if data.len() < 4 + restart_array_size {
            return Err(GoatError::corruption(
                "block_reader",
                "block too small for restart array",
            ));
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

        // 兼容当前 block 编码：restart 数组不包含首段 0 偏移，这里补一个虚拟 0，
        // 让 restart-based 查找也能覆盖第一段数据。
        if restarts.first().copied() != Some(0) {
            restarts.insert(0, 0);
        }

        // data_end should be restart_start to exclude restart array from iteration
        let reader = Self {
            data,
            restarts: Arc::from(restarts.clone().into_boxed_slice()),
            restart_keys: Arc::from(Vec::<Vec<u8>>::new().into_boxed_slice()),
            user_key_hash_index: Arc::new(HashMap::new()),
            data_end: restart_start,
        };
        let restart_keys = reader.collect_restart_keys();
        let user_key_hash_index = reader.collect_user_key_hash_index();

        Ok(BlockSearchIndex {
            restarts: Arc::from(restarts.into_boxed_slice()),
            restart_keys: Arc::from(restart_keys.into_boxed_slice()),
            user_key_hash_index: Arc::new(user_key_hash_index),
            data_end: restart_start,
        })
    }

    pub(crate) fn with_search_index(
        data: &'a [u8],
        search_index: Arc<BlockSearchIndex>,
    ) -> GoatResult<Self> {
        if data.len() < 4 {
            return Err(GoatError::corruption(
                "block_reader",
                "block too small to contain restart count",
            ));
        }
        if search_index.data_end > data.len() {
            return Err(GoatError::corruption(
                "block_reader",
                "search index data_end out of range",
            ));
        }
        if search_index.restart_keys.len() != search_index.restarts.len() {
            return Err(GoatError::corruption(
                "block_reader",
                "search index restart key count mismatch",
            ));
        }

        Ok(Self {
            data,
            restarts: search_index.restarts.clone(),
            restart_keys: search_index.restart_keys.clone(),
            user_key_hash_index: search_index.user_key_hash_index.clone(),
            data_end: search_index.data_end,
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

    /// 在块中按 UserKey 查找第一个匹配条目。
    /// 返回 (InternalKey, value)。
    pub fn get_by_user_key(&self, user_key: &[u8]) -> Option<(InternalKey, Vec<u8>)> {
        self.get_by_user_key_entry(user_key)
            .and_then(|(internal_key, value_offset, value_len)| {
                self.value_slice_to_vec(value_offset, value_len)
                    .map(|value| (internal_key, value))
            })
    }

    pub(crate) fn get_by_user_key_with_value_range(
        &self,
        user_key: &[u8],
    ) -> Option<(InternalKey, usize, usize)> {
        self.get_by_user_key_entry(user_key)
    }

    pub(crate) fn get_by_user_key_with_value_range_at_seq(
        &self,
        user_key: &[u8],
        read_seq: u64,
    ) -> Option<(InternalKey, usize, usize)> {
        if let Some((internal_key, value_offset, value_len)) = self.get_by_user_key_entry(user_key)
        {
            if internal_key.sequence_number() <= read_seq {
                return Some((internal_key, value_offset, value_len));
            }
        }

        if self.restarts.is_empty() {
            return self.linear_search_range_by_user_key_entry_at_seq(
                0,
                self.data_end,
                user_key,
                read_seq,
            );
        }
        if self.restart_keys.len() != self.restarts.len() {
            return self.linear_search_full_by_user_key_entry_at_seq(user_key, read_seq);
        }

        let prefix_probe = (self.restarts.len() - 1).min(8);
        if prefix_probe > 0 {
            let cmp = match self.restart_user_key_cmp(prefix_probe, user_key) {
                Some(cmp) => cmp,
                None => {
                    return self.linear_search_full_by_user_key_entry_at_seq(user_key, read_seq)
                }
            };
            if cmp == Ordering::Greater {
                let end_pos = self.restarts[prefix_probe] as usize;
                return self
                    .linear_search_range_by_user_key_entry_at_seq(0, end_pos, user_key, read_seq);
            }
        }

        let mut left = 0usize;
        let mut right = self.restarts.len();
        while left < right {
            let mid = left + (right - left) / 2;
            let cmp = match self.restart_user_key_cmp(mid, user_key) {
                Some(cmp) => cmp,
                None => {
                    return self.linear_search_full_by_user_key_entry_at_seq(user_key, read_seq)
                }
            };
            if cmp == Ordering::Less {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if left >= self.restarts.len() {
            return self.linear_search_from_restart_by_user_key_entry_at_seq(
                self.restarts.len() - 1,
                user_key,
                read_seq,
            );
        }

        let cmp_at_left = match self.restart_user_key_cmp(left, user_key) {
            Some(cmp) => cmp,
            None => return self.linear_search_full_by_user_key_entry_at_seq(user_key, read_seq),
        };

        match cmp_at_left {
            Ordering::Greater => self.linear_search_from_restart_by_user_key_entry_at_seq(
                left.saturating_sub(1),
                user_key,
                read_seq,
            ),
            Ordering::Equal => {
                if left > 0 {
                    if let Some(found) = self.linear_search_from_restart_by_user_key_entry_at_seq(
                        left - 1,
                        user_key,
                        read_seq,
                    ) {
                        return Some(found);
                    }
                }
                self.linear_search_from_restart_by_user_key_entry_at_seq(left, user_key, read_seq)
            }
            Ordering::Less => {
                self.linear_search_from_restart_by_user_key_entry_at_seq(left, user_key, read_seq)
            }
        }
    }

    fn get_by_user_key_entry(&self, user_key: &[u8]) -> Option<(InternalKey, usize, usize)> {
        if let Some(entry) = self.user_key_hash_index.get(user_key) {
            return Some((
                entry.internal_key.clone(),
                entry.value_offset as usize,
                entry.value_len as usize,
            ));
        }

        if self.restarts.is_empty() {
            return self.linear_search_from_start_by_user_key_entry(user_key);
        }
        if self.restart_keys.len() != self.restarts.len() {
            return self.linear_search_full_by_user_key_entry(user_key);
        }

        // 热点 key 常落在块头：先快速判断是否命中前缀区间，避免每次都做完整二分。
        let prefix_probe = (self.restarts.len() - 1).min(8);
        if prefix_probe > 0 {
            let cmp = match self.restart_user_key_cmp(prefix_probe, user_key) {
                Some(cmp) => cmp,
                None => return self.linear_search_full_by_user_key_entry(user_key),
            };
            if cmp == Ordering::Greater {
                let end_pos = self.restarts[prefix_probe] as usize;
                return self.linear_search_range_by_user_key_entry(0, end_pos, user_key);
            }
        }

        // 在 restart key 上做 lower_bound，定位到第一个 restart_user >= user_key
        let mut left = 0usize;
        let mut right = self.restarts.len();
        while left < right {
            let mid = left + (right - left) / 2;
            let cmp = match self.restart_user_key_cmp(mid, user_key) {
                Some(cmp) => cmp,
                None => return self.linear_search_full_by_user_key_entry(user_key),
            };

            if cmp == Ordering::Less {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if left >= self.restarts.len() {
            return self
                .linear_search_from_restart_by_user_key_entry(self.restarts.len() - 1, user_key);
        }

        let cmp_at_left = match self.restart_user_key_cmp(left, user_key) {
            Some(cmp) => cmp,
            None => return self.linear_search_full_by_user_key_entry(user_key),
        };

        match cmp_at_left {
            Ordering::Greater => {
                // target 落在前一个 interval；无需扫描 left interval。
                self.linear_search_from_restart_by_user_key_entry(left.saturating_sub(1), user_key)
            }
            Ordering::Equal => {
                // 可能存在跨 interval 的同 user_key 版本链，先查前一个再查当前。
                if left > 0 {
                    if let Some(found) =
                        self.linear_search_from_restart_by_user_key_entry(left - 1, user_key)
                    {
                        return Some(found);
                    }
                }
                self.linear_search_from_restart_by_user_key_entry(left, user_key)
            }
            Ordering::Less => {
                // lower_bound 下理论不会出现，保守回退当前 interval。
                self.linear_search_from_restart_by_user_key_entry(left, user_key)
            }
        }
    }

    fn restart_user_key_cmp(&self, restart_index: usize, user_key: &[u8]) -> Option<Ordering> {
        let restart_key = self.restart_keys.get(restart_index)?;
        let restart_user_key = Self::user_key_of_internal_key(restart_key)?;
        Some(restart_user_key.cmp(user_key))
    }

    fn collect_restart_keys(&self) -> Vec<Vec<u8>> {
        let mut keys = Vec::with_capacity(self.restarts.len());
        for &restart in self.restarts.iter() {
            let restart_pos = restart as usize;
            if restart_pos >= self.data_end {
                keys.push(Vec::new());
                continue;
            }
            match self.decode_entry_view_at(restart_pos) {
                Ok(entry) if entry.shared == 0 => keys.push(entry.unshared_key.to_vec()),
                _ => keys.push(Vec::new()),
            }
        }
        keys
    }

    fn collect_user_key_hash_index(&self) -> HashMap<Vec<u8>, UserKeyHashEntry> {
        let mut index = HashMap::new();
        let mut current_pos = 0usize;
        let mut prev_key = Vec::new();

        while current_pos < self.data_end {
            let entry = match self.decode_entry_view_at(current_pos) {
                Ok(entry) => entry,
                Err(_) => break,
            };
            if entry.unshared_key.is_empty() && entry.value.is_empty() {
                break;
            }

            if prev_key.is_empty() {
                if entry.shared != 0 {
                    break;
                }
                prev_key.extend_from_slice(entry.unshared_key);
            } else {
                let shared = entry.shared as usize;
                if shared > prev_key.len() {
                    break;
                }
                prev_key.truncate(shared);
                prev_key.extend_from_slice(entry.unshared_key);
            }

            if let Some(user_key) = Self::user_key_of_internal_key(&prev_key) {
                if !index.contains_key(user_key) {
                    if let Some(internal_key) = Self::decode_internal_key_slice(&prev_key) {
                        index.insert(
                            user_key.to_vec(),
                            UserKeyHashEntry {
                                internal_key,
                                value_offset: entry.value_offset as u32,
                                value_len: entry.value.len() as u32,
                            },
                        );
                    }
                }
            }

            current_pos = current_pos.saturating_add(entry.total_len);
        }

        index
    }

    fn value_slice_to_vec(&self, value_offset: usize, value_len: usize) -> Option<Vec<u8>> {
        self.data
            .get(value_offset..value_offset.saturating_add(value_len))
            .map(|slice| slice.to_vec())
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

    fn linear_search_from_restart_by_user_key_entry(
        &self,
        restart_index: usize,
        user_key: &[u8],
    ) -> Option<(InternalKey, usize, usize)> {
        let restart_pos = self.restarts[restart_index] as usize;
        let end_pos = if restart_index + 1 < self.restarts.len() {
            self.restarts[restart_index + 1] as usize
        } else {
            self.data_end
        };
        self.linear_search_range_by_user_key_entry(restart_pos, end_pos, user_key)
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

    fn linear_search_from_start_by_user_key_entry(
        &self,
        user_key: &[u8],
    ) -> Option<(InternalKey, usize, usize)> {
        self.linear_search_range_by_user_key_entry(0, self.data_end, user_key)
    }

    fn linear_search_from_restart_by_user_key_entry_at_seq(
        &self,
        restart_index: usize,
        user_key: &[u8],
        read_seq: u64,
    ) -> Option<(InternalKey, usize, usize)> {
        let restart_pos = self.restarts[restart_index] as usize;
        let end_pos = if restart_index + 1 < self.restarts.len() {
            self.restarts[restart_index + 1] as usize
        } else {
            self.data_end
        };
        self.linear_search_range_by_user_key_entry_at_seq(restart_pos, end_pos, user_key, read_seq)
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

    fn linear_search_full_by_user_key_entry(
        &self,
        user_key: &[u8],
    ) -> Option<(InternalKey, usize, usize)> {
        self.linear_search_range_by_user_key_entry(0, self.data_end, user_key)
    }

    fn linear_search_full_by_user_key_entry_at_seq(
        &self,
        user_key: &[u8],
        read_seq: u64,
    ) -> Option<(InternalKey, usize, usize)> {
        self.linear_search_range_by_user_key_entry_at_seq(0, self.data_end, user_key, read_seq)
    }

    fn linear_search_range_by_user_key_entry(
        &self,
        start_pos: usize,
        end_pos: usize,
        user_key: &[u8],
    ) -> Option<(InternalKey, usize, usize)> {
        let mut current_pos = start_pos;
        let mut prev_key = Vec::new();

        while current_pos < end_pos {
            let entry = self.decode_entry_view_at(current_pos).ok()?;
            if entry.unshared_key.is_empty() && entry.value.is_empty() {
                return None;
            }

            if prev_key.is_empty() {
                if entry.shared != 0 {
                    return None;
                }
                prev_key.extend_from_slice(entry.unshared_key);
            } else {
                let shared = entry.shared as usize;
                if shared > prev_key.len() {
                    return None;
                }
                prev_key.truncate(shared);
                prev_key.extend_from_slice(entry.unshared_key);
            }

            let full_user_key = match Self::user_key_of_internal_key(&prev_key) {
                Some(k) => k,
                None => {
                    current_pos += entry.total_len;
                    continue;
                }
            };

            match full_user_key.cmp(user_key) {
                Ordering::Less => {
                    current_pos += entry.total_len;
                }
                Ordering::Equal => {
                    let raw_internal_key = std::mem::take(&mut prev_key);
                    let internal_key = Self::decode_internal_key_owned(raw_internal_key)?;
                    return Some((internal_key, entry.value_offset, entry.value.len()));
                }
                Ordering::Greater => {
                    return None;
                }
            }
        }

        None
    }

    fn linear_search_range_by_user_key_entry_at_seq(
        &self,
        start_pos: usize,
        end_pos: usize,
        user_key: &[u8],
        read_seq: u64,
    ) -> Option<(InternalKey, usize, usize)> {
        let mut current_pos = start_pos;
        let mut prev_key = Vec::new();
        let mut seen_target_user_key = false;

        while current_pos < end_pos {
            let entry = self.decode_entry_view_at(current_pos).ok()?;
            if entry.unshared_key.is_empty() && entry.value.is_empty() {
                return None;
            }

            if prev_key.is_empty() {
                if entry.shared != 0 {
                    return None;
                }
                prev_key.extend_from_slice(entry.unshared_key);
            } else {
                let shared = entry.shared as usize;
                if shared > prev_key.len() {
                    return None;
                }
                prev_key.truncate(shared);
                prev_key.extend_from_slice(entry.unshared_key);
            }

            let full_user_key = match Self::user_key_of_internal_key(&prev_key) {
                Some(k) => k,
                None => {
                    current_pos += entry.total_len;
                    continue;
                }
            };

            match full_user_key.cmp(user_key) {
                Ordering::Less => {
                    current_pos += entry.total_len;
                }
                Ordering::Equal => {
                    seen_target_user_key = true;
                    let seq = match Self::sequence_of_internal_key(&prev_key) {
                        Some(seq) => seq,
                        None => {
                            current_pos += entry.total_len;
                            continue;
                        }
                    };
                    if seq <= read_seq {
                        let internal_key = Self::decode_internal_key_slice(&prev_key)?;
                        return Some((internal_key, entry.value_offset, entry.value.len()));
                    }
                    current_pos += entry.total_len;
                }
                Ordering::Greater => {
                    if seen_target_user_key {
                        return None;
                    }
                    return None;
                }
            }
        }

        None
    }

    fn decode_entry_view_at(&self, pos: usize) -> GoatResult<DecodedEntryView<'_>> {
        if pos >= self.data_end {
            return Err(GoatError::corruption(
                "block_reader",
                "position out of bounds",
            ));
        }

        let mut offset = pos;

        let (shared, bytes_read) = self.decode_varint_at(offset)?;
        offset += bytes_read;

        let (unshared, bytes_read) = self.decode_varint_at(offset)?;
        offset += bytes_read;

        let (value_len, bytes_read) = self.decode_varint_at(offset)?;
        offset += bytes_read;

        let unshared = unshared as usize;
        let value_len = value_len as usize;
        let end = offset + unshared + value_len;
        if end > self.data_end {
            return Err(GoatError::corruption(
                "block_reader",
                "entry exceeds block boundary",
            ));
        }

        let unshared_key = &self.data[offset..offset + unshared];
        let value_offset = offset + unshared;
        let value = &self.data[value_offset..end];

        Ok(DecodedEntryView {
            shared: shared as u32,
            unshared_key,
            value,
            value_offset,
            total_len: end - pos,
        })
    }

    /// 解码指定位置的条目
    fn decode_entry_at(&self, pos: usize) -> GoatResult<(u32, Vec<u8>, Vec<u8>)> {
        let entry = self.decode_entry_view_at(pos)?;
        Ok((
            entry.shared,
            entry.unshared_key.to_vec(),
            entry.value.to_vec(),
        ))
    }

    /// 计算指定位置条目的总大小
    fn entry_size_at(&self, pos: usize) -> Option<usize> {
        self.decode_entry_view_at(pos)
            .ok()
            .map(|entry| entry.total_len)
    }

    /// 在指定位置解码varint
    fn decode_varint_at(&self, pos: usize) -> GoatResult<(u64, usize)> {
        if pos >= self.data_end {
            return Err(GoatError::corruption(
                "block_reader",
                "position out of bounds",
            ));
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

    fn user_key_of_internal_key(raw: &[u8]) -> Option<&[u8]> {
        raw.get(..raw.len().checked_sub(8)?)
    }

    fn decode_internal_key_owned(mut raw_key: Vec<u8>) -> Option<InternalKey> {
        if raw_key.len() < 8 {
            return None;
        }

        let n = raw_key.len();
        let mut encoded_trailer = [0u8; 8];
        encoded_trailer.copy_from_slice(&raw_key[n - 8..]);
        raw_key.truncate(n - 8);
        let encoded = !u64::from_be_bytes(encoded_trailer);
        Some(InternalKey::from_encoded(raw_key, encoded))
    }

    fn decode_internal_key_slice(raw_key: &[u8]) -> Option<InternalKey> {
        if raw_key.len() < 8 {
            return None;
        }
        let n = raw_key.len();
        let mut encoded_trailer = [0u8; 8];
        encoded_trailer.copy_from_slice(&raw_key[n - 8..]);
        let encoded = !u64::from_be_bytes(encoded_trailer);
        Some(InternalKey::from_encoded(
            raw_key[..n - 8].to_vec(),
            encoded,
        ))
    }

    fn sequence_of_internal_key(raw_key: &[u8]) -> Option<u64> {
        if raw_key.len() < 8 {
            return None;
        }
        let n = raw_key.len();
        let mut encoded_trailer = [0u8; 8];
        encoded_trailer.copy_from_slice(&raw_key[n - 8..]);
        let encoded = !u64::from_be_bytes(encoded_trailer);
        Some(encoded >> 8)
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
    use crate::goatkv::format::internal_key::{InternalKey, InternalKeyKind};
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

    #[test]
    fn test_block_reader_get_by_user_key_with_versions() {
        let mut builder = BlockBuilder::new();
        let k1_latest = InternalKey::new(b"k1".to_vec(), 30, InternalKeyKind::Put).serialize();
        let k1_old = InternalKey::new(b"k1".to_vec(), 20, InternalKeyKind::Put).serialize();
        let k2_latest = InternalKey::new(b"k2".to_vec(), 10, InternalKeyKind::Put).serialize();
        builder.add(&k1_latest, b"v30");
        builder.add(&k1_old, b"v20");
        builder.add(&k2_latest, b"v10");
        let (data, _) = builder.finish();

        let reader = BlockReader::new(data).unwrap();
        let (internal_key, value) = reader.get_by_user_key(b"k1").unwrap();
        assert_eq!(internal_key.serialize(), k1_latest);
        assert_eq!(value, b"v30".to_vec());
        assert!(reader.get_by_user_key(b"absent").is_none());
    }

    #[test]
    fn test_block_reader_get_by_user_key_cross_restart_boundary() {
        let mut builder = BlockBuilder::new();

        // 前15个条目是 "aa"，第16个开始是 "bb"，确保 "bb" 跨越 restart 边界。
        for seq in (86u64..=100u64).rev() {
            let key = InternalKey::new(b"aa".to_vec(), seq, InternalKeyKind::Put).serialize();
            let value = format!("aa_{}", seq).into_bytes();
            builder.add(&key, &value);
        }

        let mut expected_latest_bb_key = Vec::new();
        for seq in (180u64..=200u64).rev() {
            let key = InternalKey::new(b"bb".to_vec(), seq, InternalKeyKind::Put).serialize();
            if seq == 200 {
                expected_latest_bb_key = key.clone();
            }
            let value = format!("bb_{}", seq).into_bytes();
            builder.add(&key, &value);
        }

        let (data, _) = builder.finish();
        let reader = BlockReader::new(data).unwrap();
        let (internal_key, value) = reader.get_by_user_key(b"bb").unwrap();
        assert_eq!(internal_key.serialize(), expected_latest_bb_key);
        assert_eq!(value, b"bb_200".to_vec());
    }

    #[test]
    fn test_block_reader_get_by_user_key_at_seq_with_versions() {
        let mut builder = BlockBuilder::new();
        let k1_v30 = InternalKey::new(b"k1".to_vec(), 30, InternalKeyKind::Put).serialize();
        let k1_v20 = InternalKey::new(b"k1".to_vec(), 20, InternalKeyKind::Put).serialize();
        let k1_v10 = InternalKey::new(b"k1".to_vec(), 10, InternalKeyKind::Put).serialize();
        builder.add(&k1_v30, b"v30");
        builder.add(&k1_v20, b"v20");
        builder.add(&k1_v10, b"v10");
        let (data, _) = builder.finish();

        let reader = BlockReader::new(data).unwrap();
        let (internal_key, _, _) = reader
            .get_by_user_key_with_value_range_at_seq(b"k1", 25)
            .unwrap();
        assert_eq!(internal_key.sequence_number(), 20);

        let (internal_key, _, _) = reader
            .get_by_user_key_with_value_range_at_seq(b"k1", 10)
            .unwrap();
        assert_eq!(internal_key.sequence_number(), 10);
        assert!(reader
            .get_by_user_key_with_value_range_at_seq(b"k1", 5)
            .is_none());
    }

    #[test]
    fn test_block_reader_get_by_user_key_at_seq_cross_restart_boundary() {
        let mut builder = BlockBuilder::new();

        for seq in (86u64..=100u64).rev() {
            let key = InternalKey::new(b"aa".to_vec(), seq, InternalKeyKind::Put).serialize();
            let value = format!("aa_{}", seq).into_bytes();
            builder.add(&key, &value);
        }

        for seq in (180u64..=200u64).rev() {
            let key = InternalKey::new(b"bb".to_vec(), seq, InternalKeyKind::Put).serialize();
            let value = format!("bb_{}", seq).into_bytes();
            builder.add(&key, &value);
        }

        let (data, _) = builder.finish();
        let reader = BlockReader::new(data).unwrap();

        let (internal_key, _, _) = reader
            .get_by_user_key_with_value_range_at_seq(b"bb", 189)
            .unwrap();
        assert_eq!(internal_key.sequence_number(), 189);
    }
}
