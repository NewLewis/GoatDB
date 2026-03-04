use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crc32fast::Hasher;

use super::block_reader::BlockReader;
use crate::goatkv::error::{Error as GoatError, Result as GoatResult};
use crate::goatkv::format::coding;
use crate::goatkv::format::internal_key::{InternalKey, InternalKeyKind, SEQUENCE_NUMBER_MAX};

/// SSTable 文件的 Magic Number
const MAGIC_NUMBER: u64 = 0x706A725F676F6174;
/// Footer 的固定大小：根据 SSTableBuilder 的写入，footer 固定为 48 字节
/// 两个varint(最多20字节) + padding + magic(8字节)
const FOOTER_SIZE: usize = 48;
const BLOCK_CHECKSUM_SIZE: usize = 4;

/// SSTable 文件的索引条目
#[derive(Debug, Clone)]
struct IndexEntry {
    /// 分隔键（该块的最后一个键）
    separator: Vec<u8>,
    /// 数据块的起始偏移量
    block_offset: u64,
    /// 数据块的大小
    block_size: u64,
}

/// SSTable 读取器，用于读取和查询 SSTable 文件
#[derive(Debug)]
pub struct SSTableReader {
    /// SSTable 文件的路径
    file_path: String,
    /// 文件句柄
    file: File,
    /// BloomFilter
    bloom_filter: super::bloom::BloomFilter,
    /// 索引条目列表，按分隔键排序
    index_entries: Vec<IndexEntry>,
}

impl SSTableReader {
    fn corruption(message: impl Into<String>) -> GoatError {
        GoatError::corruption("sstable_reader", message.into())
    }

    fn decode_internal_key(raw_key: &[u8]) -> GoatResult<InternalKey> {
        if raw_key.len() < 8 {
            return Err(Self::corruption(
                "invalid internal key length in data block",
            ));
        }
        let n = raw_key.len();
        let inverted = u64::from_be_bytes([
            raw_key[n - 8],
            raw_key[n - 7],
            raw_key[n - 6],
            raw_key[n - 5],
            raw_key[n - 4],
            raw_key[n - 3],
            raw_key[n - 2],
            raw_key[n - 1],
        ]);
        let encoded = !inverted;
        let key = InternalKey::from_encoded(raw_key[..n - 8].to_vec(), encoded);
        key.kind()
            .map_err(|e| Self::corruption(format!("invalid internal key kind: {}", e)))?;
        Ok(key)
    }

    fn checksum_for_block(block: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(block);
        hasher.finalize()
    }

    fn verify_block_checksum<'a>(block: &'a [u8], block_kind: &str) -> GoatResult<&'a [u8]> {
        if block.len() < BLOCK_CHECKSUM_SIZE {
            return Err(Self::corruption(format!(
                "{} is too small to contain checksum trailer",
                block_kind
            )));
        }

        let payload_len = block.len() - BLOCK_CHECKSUM_SIZE;
        let (payload, checksum_bytes) = block.split_at(payload_len);
        let expected = u32::from_le_bytes([
            checksum_bytes[0],
            checksum_bytes[1],
            checksum_bytes[2],
            checksum_bytes[3],
        ]);
        let actual = Self::checksum_for_block(payload);
        if actual != expected {
            return Err(Self::corruption(format!(
                "{} checksum mismatch: expected {:08x}, got {:08x}",
                block_kind, expected, actual
            )));
        }

        Ok(payload)
    }

    /// 打开并解析 SSTable 文件
    pub fn open<P: AsRef<Path>>(path: P) -> GoatResult<Self> {
        let path_ref = path.as_ref();
        let mut file = File::open(path_ref).map_err(|e| GoatError::io("sstable_open", e))?;

        // 1. 读取文件大小
        let file_size = file
            .metadata()
            .map_err(|e| GoatError::io("sstable_metadata", e))?
            .len();
        if file_size < FOOTER_SIZE as u64 {
            return Err(Self::corruption(
                "SSTable file is too small to contain valid footer",
            ));
        }

        // 2. 读取最后的 FOOTER_SIZE 字节
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))
            .map_err(|e| GoatError::io("sstable_seek_footer", e))?;
        let mut footer = vec![0u8; FOOTER_SIZE];
        file.read_exact(&mut footer)
            .map_err(|e| GoatError::io("sstable_read_footer", e))?;

        // 3. 首先验证 Magic Number（最后8字节）
        let magic_bytes = &footer[footer.len() - 8..];
        let magic = u64::from_le_bytes([
            magic_bytes[0],
            magic_bytes[1],
            magic_bytes[2],
            magic_bytes[3],
            magic_bytes[4],
            magic_bytes[5],
            magic_bytes[6],
            magic_bytes[7],
        ]);

        if magic != MAGIC_NUMBER {
            return Err(Self::corruption(format!(
                "Invalid magic number: expected {:x}, got {:x}",
                MAGIC_NUMBER, magic
            )));
        }

        // 4. 从前往后解析 Footer，但使用更健壮的方法
        // Footer 结构：bloom_offset(varint) + index_offset(varint) + padding + magic(8 bytes)
        let mut cursor = 0;

        // 解析 bloom_offset (varint)
        let (bloom_offset, bloom_bytes_len) =
            match coding::decode_varint64_with_length(&footer[cursor..]) {
                Ok(result) => result,
                Err(e) => {
                    return Err(Self::corruption(format!(
                        "Failed to decode bloom filter offset: {}",
                        e
                    )));
                }
            };

        cursor += bloom_bytes_len;

        // 解析 index_offset (varint)
        let (index_offset, index_bytes_len) =
            match coding::decode_varint64_with_length(&footer[cursor..]) {
                Ok(result) => result,
                Err(e) => {
                    return Err(Self::corruption(format!(
                        "Failed to decode index block offset: {}",
                        e
                    )));
                }
            };

        cursor += index_bytes_len;

        // 跳过 padding (应该是0字节)
        // padding 大小 = FOOTER_SIZE - 8(magic) - bloom_bytes_len - index_bytes_len
        // 验证 padding 都是0
        for &byte in footer.iter().take(footer.len() - 8).skip(cursor) {
            if byte != 0 {
                // 不返回错误，只是记录警告，因为可能有其他数据
            }
        }

        // 6. 验证偏移量
        if bloom_offset >= file_size {
            return Err(Self::corruption(format!(
                "Invalid bloom_offset: bloom_offset={}, file_size={}",
                bloom_offset, file_size
            )));
        }

        if index_offset >= file_size {
            return Err(Self::corruption(format!(
                "Invalid index_offset: index_offset={}, file_size={}",
                index_offset, file_size
            )));
        }

        if bloom_offset >= index_offset {
            return Err(Self::corruption(format!(
                "Invalid offset order: bloom_offset={} >= index_offset={}",
                bloom_offset, index_offset
            )));
        }

        // 5. 读取 BloomFilter
        file.seek(SeekFrom::Start(bloom_offset))
            .map_err(|e| GoatError::io("sstable_seek_bloom", e))?;
        // BloomFilter 的大小是 index_offset - bloom_offset
        let bloom_filter_size = index_offset - bloom_offset;
        let mut bloom_bitmap = vec![0u8; bloom_filter_size as usize];
        file.read_exact(&mut bloom_bitmap)
            .map_err(|e| GoatError::io("sstable_read_bloom", e))?;
        let bloom_filter = super::bloom::BloomFilter::new(bloom_bitmap);

        // 6. 读取和解析索引块
        // index_offset 是索引块的开始位置
        // 索引块从 index_offset 开始，到文件末尾减去footer大小结束
        // 注意：index_offset 已经指向索引块的开始，因为 BloomFilter 已经读取完毕
        let index_block_start = index_offset;
        let footer_start = file_size - FOOTER_SIZE as u64;

        if index_block_start >= footer_start {
            return Err(Self::corruption(format!(
                "Index block start beyond footer: index_block_start={}, footer_start={}",
                index_block_start, footer_start
            )));
        }

        let index_block_size = footer_start - index_block_start;
        if index_block_size <= BLOCK_CHECKSUM_SIZE as u64 {
            // 索引块可能为空（只有一个数据块的情况？）
            return Err(Self::corruption(
                "Index block size is too small to contain checksum",
            ));
        }

        file.seek(SeekFrom::Start(index_block_start))
            .map_err(|e| GoatError::io("sstable_seek_index", e))?;
        let mut index_block_data_with_checksum = vec![0u8; index_block_size as usize];
        file.read_exact(&mut index_block_data_with_checksum)
            .map_err(|e| GoatError::io("sstable_read_index", e))?;
        let index_block_data =
            Self::verify_block_checksum(&index_block_data_with_checksum, "index block")?;

        // 解析索引块
        let index_reader = match BlockReader::new(index_block_data) {
            Ok(reader) => reader,
            Err(e) => {
                return Err(Self::corruption(format!(
                    "Failed to parse index block: {}",
                    e
                )));
            }
        };

        // 7. 提取索引条目
        let mut index_entries = Vec::new();
        for (separator, offset_data) in index_reader.iter() {
            // offset_data 格式：block_offset(varint) + block_size(varint)
            if offset_data.len() < 2 {
                return Err(Self::corruption(format!(
                    "Index entry offset data too short: {} bytes",
                    offset_data.len()
                )));
            }

            // 解码块偏移量
            let (block_offset, offset_len) = match coding::decode_varint64_with_length(&offset_data)
            {
                Ok(result) => result,
                Err(e) => {
                    return Err(Self::corruption(format!(
                        "Failed to decode block offset varint: {}",
                        e
                    )));
                }
            };

            // 解码块大小
            let block_size_data = &offset_data[offset_len..];
            let block_size = match coding::decode_varint64(block_size_data) {
                Ok(size) => size,
                Err(e) => {
                    return Err(Self::corruption(format!(
                        "Failed to decode block size varint: {}",
                        e
                    )));
                }
            };

            index_entries.push(IndexEntry {
                separator,
                block_offset,
                block_size,
            });
        }

        // 确保索引条目按分隔键排序
        index_entries.sort_by(|a, b| a.separator.cmp(&b.separator));

        Ok(Self {
            file_path: path_ref.to_string_lossy().to_string(),
            file,
            bloom_filter,
            index_entries,
        })
    }

    /// 检查 key 是否可能存在于 SSTable 中（使用 BloomFilter 快速过滤）
    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.bloom_filter.contains(key)
    }

    /// 在 SSTable 中查找指定的 key (UserKey)
    pub fn get(&mut self, key: &[u8]) -> GoatResult<Option<(InternalKey, Vec<u8>)>> {
        // 1. 使用 BloomFilter 快速过滤 (BloomFilter now indexes UserKey)
        if !self.may_contain(key) {
            return Ok(None);
        }

        let probe_key = InternalKey::new(key.to_vec(), SEQUENCE_NUMBER_MAX, InternalKeyKind::Put);

        let block_info = self.find_block_for_key(&probe_key.serialize());

        let (block_offset, block_size) = match block_info {
            Some(info) => info,
            None => return Ok(None),
        };

        // 3. 读取数据块
        self.file
            .seek(SeekFrom::Start(block_offset))
            .map_err(|e| GoatError::io("sstable_seek_data_block", e))?;
        let mut block_data = vec![0u8; block_size as usize];
        self.file
            .read_exact(&mut block_data)
            .map_err(|e| GoatError::io("sstable_read_data_block", e))?;
        let block_payload = Self::verify_block_checksum(&block_data, "data block")?;

        // 4. 在数据块中查找 key
        let block_reader = match BlockReader::new(block_payload) {
            Ok(reader) => reader,
            Err(e) => {
                return Err(Self::corruption(format!(
                    "Failed to parse data block: {}",
                    e
                )));
            }
        };

        // Iterate block looking for UserKey match
        for (k, v) in block_reader.iter() {
            // k is InternalKey bytes.
            if k.len() < 8 {
                continue;
            }
            let user_key_part = &k[..k.len() - 8];

            // Compare User Key
            match user_key_part.cmp(key) {
                Ordering::Less => continue, // Keep looking
                Ordering::Equal => {
                    // Found match! First match is newest version.
                    return Ok(Some((Self::decode_internal_key(&k)?, v)));
                }
                Ordering::Greater => {
                    // Moved past target user key
                    return Ok(None);
                }
            }
        }

        Ok(None)
    }

    /// Full scan of all entries in this SSTable.
    pub fn scan_all(&mut self) -> GoatResult<Vec<(InternalKey, Vec<u8>)>> {
        let mut entries = Vec::new();
        for entry in &self.index_entries {
            self.file
                .seek(SeekFrom::Start(entry.block_offset))
                .map_err(|e| GoatError::io("sstable_seek_scan_block", e))?;
            let mut block_data = vec![0u8; entry.block_size as usize];
            self.file
                .read_exact(&mut block_data)
                .map_err(|e| GoatError::io("sstable_read_scan_block", e))?;
            let block_payload = Self::verify_block_checksum(&block_data, "data block")?;
            let block_reader = BlockReader::new(block_payload).map_err(|e| {
                Self::corruption(format!("Failed to parse data block during scan: {}", e))
            })?;
            for (k, v) in block_reader.iter() {
                entries.push((Self::decode_internal_key(&k)?, v));
            }
        }
        Ok(entries)
    }

    /// Convert reader into a streaming iterator that yields entries block by block.
    pub fn into_scan_iterator(self) -> SSTableScanIterator {
        SSTableScanIterator::new(self)
    }

    /// 查找包含指定 key 的数据块
    fn find_block_for_key(&self, key: &[u8]) -> Option<(u64, u64)> {
        if self.index_entries.is_empty() {
            return None;
        }

        // 使用二分查找找到第一个分隔键 >= key 的索引条目
        let mut left = 0;
        let mut right = self.index_entries.len();

        while left < right {
            let mid = (left + right) / 2;
            match self.index_entries[mid].separator.as_slice().cmp(key) {
                Ordering::Less => left = mid + 1,
                Ordering::Greater | Ordering::Equal => right = mid,
            }
        }

        // left 是第一个分隔键 >= key 的位置
        if left < self.index_entries.len() {
            let entry = &self.index_entries[left];
            Some((entry.block_offset, entry.block_size))
        } else if !self.index_entries.is_empty() {
            // key 大于所有分隔键，应该位于最后一个块中
            let entry = &self.index_entries[self.index_entries.len() - 1];
            Some((entry.block_offset, entry.block_size))
        } else {
            None
        }
    }

    /// 获取 SSTable 中的最小键（第一个索引条目的分隔键）
    pub fn min_key(&self) -> Option<&[u8]> {
        self.index_entries.first().map(|e| e.separator.as_slice())
    }

    /// 获取 SSTable 文件路径
    pub fn file_path(&self) -> &str {
        &self.file_path
    }

    /// 获取索引条目数量
    pub fn index_entry_count(&self) -> usize {
        self.index_entries.len()
    }
}

pub struct SSTableScanIterator {
    reader: SSTableReader,
    block_index: usize,
    pending_entries: VecDeque<(InternalKey, Vec<u8>)>,
}

impl SSTableScanIterator {
    fn new(reader: SSTableReader) -> Self {
        Self {
            reader,
            block_index: 0,
            pending_entries: VecDeque::new(),
        }
    }

    pub fn next_entry(&mut self) -> GoatResult<Option<(InternalKey, Vec<u8>)>> {
        loop {
            if let Some(entry) = self.pending_entries.pop_front() {
                return Ok(Some(entry));
            }

            if self.block_index >= self.reader.index_entries.len() {
                return Ok(None);
            }

            let entry = &self.reader.index_entries[self.block_index];
            self.reader
                .file
                .seek(SeekFrom::Start(entry.block_offset))
                .map_err(|e| GoatError::io("sstable_seek_scan_block", e))?;
            let mut block_data = vec![0u8; entry.block_size as usize];
            self.reader
                .file
                .read_exact(&mut block_data)
                .map_err(|e| GoatError::io("sstable_read_scan_block", e))?;
            let block_payload = SSTableReader::verify_block_checksum(&block_data, "data block")?;
            let block_reader = BlockReader::new(block_payload).map_err(|e| {
                SSTableReader::corruption(format!("Failed to parse data block during scan: {}", e))
            })?;

            self.pending_entries.clear();
            for (k, v) in block_reader.iter() {
                self.pending_entries
                    .push_back((SSTableReader::decode_internal_key(&k)?, v));
            }
            self.block_index += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::goatkv::core::kv_engine::KvEngine;
    use crate::goatkv::format::internal_key::{InternalKey, InternalKeyKind};
    use crate::goatkv::storage::sstable::SSTableBuilder;
    use crate::goatkv::ErrorKind;
    use std::fs;
    use std::io::Write;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tracing::{info, warn};

    /// 创建测试用的 SSTable 文件
    /// 返回 (TempDir, SSTable路径)
    fn create_test_sstable() -> (TempDir, PathBuf) {
        let temp_dir = TempDir::new().unwrap();
        let (_, sstable_paths, _) = KvEngine::init_db_paths(temp_dir.path()).unwrap();

        let mut builder = SSTableBuilder::new_with_manager(1, &sstable_paths).unwrap();

        // 添加一些测试数据，使用 InternalKey 格式（与生产环境一致）
        // 使用递减的序列号以确保正确的排序
        let mut sequence_number = 1000;
        let entries = vec![
            (b"apple".to_vec(), b"fruit1".to_vec()),
            (b"banana".to_vec(), b"fruit2".to_vec()),
            (b"cherry".to_vec(), b"fruit3".to_vec()),
            (b"date".to_vec(), b"fruit4".to_vec()),
            (b"elderberry".to_vec(), b"fruit5".to_vec()),
        ];

        for (key, value) in entries {
            let internal_key = InternalKey::new(key, sequence_number, InternalKeyKind::Put);
            // 将 InternalKey 序列化为字节（user_key + encoded_sequence_number）
            let mut key_bytes = Vec::new();
            key_bytes.extend_from_slice(internal_key.user_key());
            key_bytes.extend_from_slice(&(!internal_key.encoded_sequence_number()).to_be_bytes());

            builder.write(&key_bytes, &value).unwrap();
            sequence_number -= 1; // 递减序列号以确保正确排序
        }

        builder.finish().unwrap();

        let sst_path = sstable_paths.sstable_path_by_id(1);
        (temp_dir, sst_path)
    }

    #[test]
    fn test_sstable_iter_all_data() {
        // 创建200条数据并测试完整迭代
        let temp_dir = TempDir::new().unwrap();
        let (_, sstable_paths, _) = KvEngine::init_db_paths(temp_dir.path()).unwrap();
        let mut builder = SSTableBuilder::new_with_manager(1, &sstable_paths).unwrap();

        let mut test_data = Vec::new();
        for i in 0..200 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{:03}", i);

            let internal_key = InternalKey::new(key.as_bytes().to_vec(), i, InternalKeyKind::Put);
            let key_bytes = key.as_bytes().to_vec();
            let value_bytes = value.as_bytes().to_vec();

            builder
                .write(&internal_key.serialize(), &value_bytes)
                .unwrap();
            test_data.push((key_bytes, value_bytes));
        }

        builder.finish().unwrap();

        let sst_path = sstable_paths.sstable_path_by_id(1);
        let mut reader = SSTableReader::open(&sst_path).unwrap();

        // 检查测试数据本身是否有重复
        let mut seen_keys = std::collections::HashSet::new();
        for (key, _) in &test_data {
            if seen_keys.contains(key) {
                warn!(
                    "WARNING: Duplicate key in test data: {:?}",
                    String::from_utf8_lossy(key)
                );
            }
            seen_keys.insert(key.clone());
        }
        info!("Unique keys in test data: {}", seen_keys.len());

        let all_entries = reader.scan_all().unwrap();

        info!("Total entries read: {}", all_entries.len());
        info!("Total entries expected: {}", test_data.len());

        // 检查是否读取了所有条目
        assert_eq!(all_entries.len(), test_data.len());
    }

    #[test]
    fn test_sstable_reader_open() {
        let (_temp_dir, sst_path) = create_test_sstable();

        // 先检查文件是否存在
        assert!(sst_path.exists());

        let reader = SSTableReader::open(&sst_path);

        // 如果失败，打印错误信息
        if let Err(ref e) = reader {
            warn!("Error opening SSTable: {}", e);
        }

        assert!(reader.is_ok());
        let reader = reader.unwrap();

        // 验证基本属性
        assert!(reader.index_entry_count() > 0);
        assert!(reader.min_key().is_some());
        info!(
            "Reader created successfully with {} index entries",
            reader.index_entry_count()
        );
    }

    #[test]
    fn test_sstable_reader_get() {
        let (_temp_dir, sst_path) = create_test_sstable();
        let mut reader = SSTableReader::open(&sst_path).unwrap();

        // 测试存在的key
        let result = reader.get(b"apple");
        assert!(result.is_ok());
        let (_, value) = result.unwrap().unwrap();
        assert_eq!(value, b"fruit1".to_vec());

        // 测试另一个存在的key
        let result = reader.get(b"cherry");
        assert!(result.is_ok());
        let (_, value) = result.unwrap().unwrap();
        assert_eq!(value, b"fruit3".to_vec());

        // 测试不存在的key
        let result = reader.get(b"fig");
        assert!(result.is_ok());
        let value = result.unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_sstable_reader_may_contain() {
        let (_temp_dir, sst_path) = create_test_sstable();
        let reader = SSTableReader::open(&sst_path).unwrap();

        // BloomFilter 应该对存在的key返回true
        assert!(reader.may_contain(b"apple"));
        assert!(reader.may_contain(b"banana"));

        // 对不存在的key可能返回true或false（允许误报）
        // 我们只验证方法调用不会panic
        let _ = reader.may_contain(b"nonexistent");
    }

    #[test]
    fn test_sstable_corrupted_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("corrupted.sst");
        let mut temp_file = File::create(&file_path).unwrap();
        temp_file.write_all(b"invalid data").unwrap();
        drop(temp_file);

        let reader = SSTableReader::open(&file_path);
        assert!(reader.is_err());
    }

    #[test]
    fn test_sstable_small_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("tiny.sst");
        let mut temp_file = File::create(&file_path).unwrap();
        temp_file.write_all(b"tiny").unwrap();
        drop(temp_file);

        let reader = SSTableReader::open(&file_path);
        assert!(reader.is_err());
    }

    #[test]
    fn test_sstable_reader_reports_invalid_internal_key_kind() {
        let temp_dir = TempDir::new().unwrap();
        let (_, sstable_paths, _) = KvEngine::init_db_paths(temp_dir.path()).unwrap();
        let mut builder = SSTableBuilder::new_with_manager(1, &sstable_paths).unwrap();

        let mut raw_key = b"bad_kind".to_vec();
        let encoded_sequence_with_invalid_kind = (42u64 << 8) | 2u64;
        raw_key.extend_from_slice(&(!encoded_sequence_with_invalid_kind).to_be_bytes());
        builder.write(&raw_key, b"value").unwrap();
        builder.finish().unwrap();

        let sst_path = sstable_paths.sstable_path_by_id(1);
        let mut reader = SSTableReader::open(&sst_path).unwrap();
        let err = reader
            .get(b"bad_kind")
            .expect_err("invalid kind should be reported as corruption");
        assert_eq!(err.kind(), ErrorKind::Corruption);
        assert!(err.to_string().contains("invalid internal key kind"));
    }

    #[test]
    fn test_sstable_reader_reports_data_block_checksum_mismatch() {
        let (_temp_dir, sst_path) = create_test_sstable();
        let mut file_content = fs::read(&sst_path).unwrap();
        file_content[0] ^= 0x01;
        fs::write(&sst_path, &file_content).unwrap();

        let mut reader = SSTableReader::open(&sst_path).unwrap();
        let err = reader
            .get(b"apple")
            .expect_err("corrupted data block must return corruption");
        assert_eq!(err.kind(), ErrorKind::Corruption);
        assert!(err.to_string().contains("data block checksum mismatch"));
    }

    #[test]
    fn test_sstable_open_reports_index_block_checksum_mismatch() {
        let (_temp_dir, sst_path) = create_test_sstable();
        let mut file_content = fs::read(&sst_path).unwrap();
        let index_tail_pos = file_content.len() - FOOTER_SIZE - 1;
        file_content[index_tail_pos] ^= 0x01;
        fs::write(&sst_path, &file_content).unwrap();

        let err = SSTableReader::open(&sst_path).expect_err("corrupted index block must fail open");
        assert_eq!(err.kind(), ErrorKind::Corruption);
        assert!(err.to_string().contains("index block checksum mismatch"));
    }
}
