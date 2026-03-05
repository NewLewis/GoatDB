use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

use crc32fast::Hasher;

use super::block_reader::{BlockReader, BlockSearchIndex};
use super::bloom::{
    bloom_lookup_key, BloomFilter, PARTITIONED_BLOOM_HEADER_SIZE,
    PARTITIONED_BLOOM_INDEX_ENTRY_SIZE, PARTITIONED_BLOOM_MAGIC, PARTITIONED_BLOOM_VERSION,
};
use super::cache::{BlockCache, BlockCacheKey, FilterPartitionCache, FilterPartitionCacheKey};
use crate::goatkv::error::{Error as GoatError, Result as GoatResult};
use crate::goatkv::format::coding;
use crate::goatkv::format::internal_key::{InternalKey, InternalKeyKind, SEQUENCE_NUMBER_MAX};

/// SSTable 文件的 Magic Number
const MAGIC_NUMBER: u64 = 0x706A725F676F6174;
/// Footer 的固定大小：根据 SSTableBuilder 的写入，footer 固定为 48 字节
/// 两个varint(最多20字节) + padding + magic(8字节)
const FOOTER_SIZE: usize = 48;
const BLOCK_CHECKSUM_SIZE: usize = 4;
const FOOTER_FORMAT_MARKER: [u8; 4] = *b"GKFV";
const FOOTER_FORMAT_METADATA_SIZE: usize = 8;
const SSTABLE_FORMAT_VERSION_LEGACY: u8 = 0;
const SSTABLE_FORMAT_VERSION_CURRENT: u8 = 1;
const SCAN_ITERATOR_DEFAULT_READAHEAD_BLOCKS: usize = 2;

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

#[derive(Debug, Clone, Copy)]
struct BloomPartitionIndexEntry {
    offset: u64,
    size: u64,
}

#[derive(Debug)]
enum BloomFilterStorage {
    Legacy(BloomFilter),
    Partitioned(PartitionedBloomFilter),
}

#[derive(Debug)]
struct PartitionedBloomFilter {
    prefix_extractor_len: usize,
    file_id: Option<u64>,
    partitions: Vec<BloomPartitionIndexEntry>,
    partition_cache: Option<Arc<FilterPartitionCache>>,
    loaded_partitions: Mutex<HashMap<usize, Arc<BloomFilter>>>,
}

#[derive(Debug, Clone)]
pub struct PinnedValue {
    repr: PinnedValueRepr,
}

#[derive(Debug, Clone)]
enum PinnedValueRepr {
    Shared {
        payload: Arc<[u8]>,
        offset: usize,
        len: usize,
    },
    Owned(Bytes),
}

impl PinnedValue {
    pub(crate) fn from_block(payload: Arc<[u8]>, offset: usize, len: usize) -> Option<Self> {
        let end = offset.checked_add(len)?;
        if end > payload.len() {
            return None;
        }
        Some(Self {
            repr: PinnedValueRepr::Shared {
                payload,
                offset,
                len,
            },
        })
    }

    pub(crate) fn from_bytes(bytes: Bytes) -> Self {
        Self {
            repr: PinnedValueRepr::Owned(bytes),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        match &self.repr {
            PinnedValueRepr::Shared {
                payload,
                offset,
                len,
            } => &payload[*offset..(*offset + *len)],
            PinnedValueRepr::Owned(bytes) => bytes.as_ref(),
        }
    }

    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.as_slice().to_vec()
    }
}

/// SSTable 读取器，用于读取和查询 SSTable 文件
#[derive(Debug)]
pub struct SSTableReader {
    /// SSTable 文件的路径
    file_path: String,
    /// SSTable 文件 id（可选，用于 block cache key）
    file_id: Option<u64>,
    /// 文件句柄
    file: File,
    /// BloomFilter
    bloom_filter: BloomFilterStorage,
    /// 索引条目列表，按分隔键排序
    index_entries: Vec<IndexEntry>,
    /// 可选数据块缓存
    block_cache: Option<Arc<BlockCache>>,
    /// 数据块解析索引缓存（按 data block 索引缓存 restart 索引，避免热点 get 重复解码）
    block_search_indexes: Vec<OnceLock<Arc<BlockSearchIndex>>>,
    /// footer 中记录的 SSTable 格式版本（legacy 文件默认 0）
    format_version: u8,
}

impl SSTableReader {
    fn read_exact_at(
        file: &File,
        mut offset: u64,
        mut buf: &mut [u8],
        op: &'static str,
    ) -> GoatResult<()> {
        while !buf.is_empty() {
            #[cfg(unix)]
            let bytes = file
                .read_at(buf, offset)
                .map_err(|e| GoatError::io(op, e))?;
            #[cfg(windows)]
            let bytes = file
                .seek_read(buf, offset)
                .map_err(|e| GoatError::io(op, e))?;

            if bytes == 0 {
                return Err(GoatError::corruption(
                    "sstable_read_at",
                    "unexpected EOF while reading at offset",
                ));
            }

            let (_, rest) = buf.split_at_mut(bytes);
            buf = rest;
            offset += bytes as u64;
        }

        Ok(())
    }

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

    fn user_key_of_internal_key(raw_key: &[u8]) -> Option<&[u8]> {
        raw_key.get(..raw_key.len().checked_sub(8)?)
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

    fn read_legacy_bloom_filter(
        file: &File,
        bloom_offset: u64,
        size: u64,
    ) -> GoatResult<BloomFilter> {
        let mut bloom_bitmap = vec![0u8; size as usize];
        Self::read_exact_at(file, bloom_offset, &mut bloom_bitmap, "sstable_read_bloom")?;
        Ok(BloomFilter::new(bloom_bitmap))
    }

    fn parse_partitioned_bloom_filter(
        file: &File,
        bloom_offset: u64,
        bloom_size: u64,
        file_id: Option<u64>,
        partition_cache: Option<Arc<FilterPartitionCache>>,
    ) -> GoatResult<Option<PartitionedBloomFilter>> {
        if bloom_size < PARTITIONED_BLOOM_HEADER_SIZE as u64 {
            return Ok(None);
        }

        let mut header = [0u8; PARTITIONED_BLOOM_HEADER_SIZE];
        Self::read_exact_at(file, bloom_offset, &mut header, "sstable_read_bloom_header")?;
        if header[..4] != PARTITIONED_BLOOM_MAGIC {
            return Ok(None);
        }

        let version = header[4];
        if version != PARTITIONED_BLOOM_VERSION {
            return Err(Self::corruption(format!(
                "unsupported partitioned bloom version: {}",
                version
            )));
        }

        let prefix_extractor_len = u16::from_le_bytes([header[5], header[6]]) as usize;
        let partition_count =
            u32::from_le_bytes([header[7], header[8], header[9], header[10]]) as usize;
        let index_size = PARTITIONED_BLOOM_INDEX_ENTRY_SIZE.saturating_mul(partition_count);
        let data_start = PARTITIONED_BLOOM_HEADER_SIZE.saturating_add(index_size);
        if data_start as u64 > bloom_size {
            return Err(Self::corruption(format!(
                "partitioned bloom index out of range: data_start={}, bloom_size={}",
                data_start, bloom_size
            )));
        }

        let mut index_buf = vec![0u8; index_size];
        if !index_buf.is_empty() {
            Self::read_exact_at(
                file,
                bloom_offset + PARTITIONED_BLOOM_HEADER_SIZE as u64,
                &mut index_buf,
                "sstable_read_bloom_index",
            )?;
        }

        let mut partitions = Vec::with_capacity(partition_count);
        for entry in index_buf.chunks_exact(PARTITIONED_BLOOM_INDEX_ENTRY_SIZE) {
            let offset = u64::from_le_bytes([
                entry[0], entry[1], entry[2], entry[3], entry[4], entry[5], entry[6], entry[7],
            ]);
            let size = u64::from_le_bytes([
                entry[8], entry[9], entry[10], entry[11], entry[12], entry[13], entry[14],
                entry[15],
            ]);
            let end = offset.saturating_add(size);
            if end > bloom_size || offset < data_start as u64 {
                return Err(Self::corruption(format!(
                    "partitioned bloom entry out of range: offset={}, size={}, bloom_size={}",
                    offset, size, bloom_size
                )));
            }
            partitions.push(BloomPartitionIndexEntry {
                offset: bloom_offset.saturating_add(offset),
                size,
            });
        }

        Ok(Some(PartitionedBloomFilter {
            prefix_extractor_len,
            file_id,
            partitions,
            partition_cache,
            loaded_partitions: Mutex::new(HashMap::new()),
        }))
    }

    fn parse_footer_format_version(padding: &[u8]) -> u8 {
        if padding.len() >= FOOTER_FORMAT_METADATA_SIZE && padding[..4] == FOOTER_FORMAT_MARKER {
            return padding[4];
        }
        SSTABLE_FORMAT_VERSION_LEGACY
    }

    /// 打开并解析 SSTable 文件
    pub fn open<P: AsRef<Path>>(path: P) -> GoatResult<Self> {
        Self::open_internal(path, None, None, None)
    }

    pub(crate) fn open_with_block_cache<P: AsRef<Path>>(
        path: P,
        file_id: u64,
        block_cache: Option<Arc<BlockCache>>,
        partition_cache: Option<Arc<FilterPartitionCache>>,
    ) -> GoatResult<Self> {
        Self::open_internal(path, Some(file_id), block_cache, partition_cache)
    }

    fn open_internal<P: AsRef<Path>>(
        path: P,
        file_id: Option<u64>,
        block_cache: Option<Arc<BlockCache>>,
        partition_cache: Option<Arc<FilterPartitionCache>>,
    ) -> GoatResult<Self> {
        let path_ref = path.as_ref();
        let file = File::open(path_ref).map_err(|e| GoatError::io("sstable_open", e))?;

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
        let footer_offset = file_size - FOOTER_SIZE as u64;
        let mut footer = vec![0u8; FOOTER_SIZE];
        Self::read_exact_at(&file, footer_offset, &mut footer, "sstable_read_footer")?;

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
        let footer_padding = &footer[cursor..footer.len() - 8];
        let format_version = Self::parse_footer_format_version(footer_padding);
        if format_version > SSTABLE_FORMAT_VERSION_CURRENT {
            return Err(Self::corruption(format!(
                "unsupported sstable format version {}, max supported {}",
                format_version, SSTABLE_FORMAT_VERSION_CURRENT
            )));
        }
        for &byte in footer_padding {
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

        // 5. 读取 BloomFilter（兼容 legacy bitmap 和 partitioned bloom）
        let bloom_filter_size = index_offset - bloom_offset;
        let bloom_filter = if let Some(partitioned) = Self::parse_partitioned_bloom_filter(
            &file,
            bloom_offset,
            bloom_filter_size,
            file_id,
            partition_cache,
        )? {
            BloomFilterStorage::Partitioned(partitioned)
        } else {
            BloomFilterStorage::Legacy(Self::read_legacy_bloom_filter(
                &file,
                bloom_offset,
                bloom_filter_size,
            )?)
        };

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

        let mut index_block_data_with_checksum = vec![0u8; index_block_size as usize];
        Self::read_exact_at(
            &file,
            index_block_start,
            &mut index_block_data_with_checksum,
            "sstable_read_index",
        )?;
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

        if let BloomFilterStorage::Partitioned(partitioned) = &bloom_filter {
            if partitioned.partitions.len() != index_entries.len() {
                return Err(Self::corruption(format!(
                    "partitioned bloom partition count {} does not match data blocks {}",
                    partitioned.partitions.len(),
                    index_entries.len()
                )));
            }
        }

        let block_search_indexes = (0..index_entries.len()).map(|_| OnceLock::new()).collect();

        Ok(Self {
            file_path: path_ref.to_string_lossy().to_string(),
            file_id,
            file,
            bloom_filter,
            index_entries,
            block_cache,
            block_search_indexes,
            format_version,
        })
    }

    pub fn format_version(&self) -> u8 {
        self.format_version
    }

    /// 检查 key 是否可能存在于 SSTable 中（使用 BloomFilter 快速过滤）
    pub fn may_contain(&self, key: &[u8]) -> bool {
        match &self.bloom_filter {
            BloomFilterStorage::Legacy(filter) => filter.contains(key),
            BloomFilterStorage::Partitioned(_) => {
                let probe_key =
                    InternalKey::new(key.to_vec(), SEQUENCE_NUMBER_MAX, InternalKeyKind::Put);
                let Some(block_index) = self.find_block_index_for_key(&probe_key.serialize())
                else {
                    return false;
                };
                self.may_contain_for_block(key, block_index).unwrap_or(true)
            }
        }
    }

    fn may_contain_for_block(&self, key: &[u8], block_index: usize) -> GoatResult<bool> {
        match &self.bloom_filter {
            BloomFilterStorage::Legacy(filter) => Ok(filter.contains(key)),
            BloomFilterStorage::Partitioned(partitioned) => {
                partitioned.may_contain(&self.file, key, block_index)
            }
        }
    }

    fn read_block_from_file(&self, block_offset: u64, block_size: u64) -> GoatResult<Vec<u8>> {
        let mut block_data = vec![0u8; block_size as usize];
        Self::read_exact_at(
            &self.file,
            block_offset,
            &mut block_data,
            "sstable_read_data_block",
        )?;
        Ok(block_data)
    }

    fn load_data_block_payload(&self, block_offset: u64, block_size: u64) -> GoatResult<Arc<[u8]>> {
        if let (Some(file_id), Some(block_cache)) = (self.file_id, self.block_cache.as_ref()) {
            let cache_key = BlockCacheKey::new(file_id, block_offset, block_size);
            if let Some(payload) = block_cache.get(&cache_key) {
                return Ok(payload);
            }
            let block_data = self.read_block_from_file(block_offset, block_size)?;
            let block_payload = Self::verify_block_checksum(&block_data, "data block")?.to_vec();
            return Ok(block_cache.insert(cache_key, block_payload));
        }

        let block_data = self.read_block_from_file(block_offset, block_size)?;
        let block_payload = Self::verify_block_checksum(&block_data, "data block")?;
        Ok(Arc::from(block_payload.to_vec().into_boxed_slice()))
    }

    fn load_block_search_index(
        &self,
        block_index: usize,
        block_payload: &Arc<[u8]>,
    ) -> GoatResult<Arc<BlockSearchIndex>> {
        let Some(slot) = self.block_search_indexes.get(block_index) else {
            return Err(Self::corruption(format!(
                "data block index out of range: {}",
                block_index
            )));
        };

        if let Some(index) = slot.get() {
            return Ok(index.clone());
        }

        let parsed = Arc::new(
            BlockReader::parse_search_index(block_payload.as_ref()).map_err(|e| {
                Self::corruption(format!("Failed to parse data block index: {}", e))
            })?,
        );

        Ok(slot.get_or_init(|| parsed).clone())
    }

    /// 在 SSTable 中查找指定的 key (UserKey)
    pub fn get(&self, key: &[u8]) -> GoatResult<Option<(InternalKey, Vec<u8>)>> {
        self.get_pinned(key)
            .map(|opt| opt.map(|(internal_key, value)| (internal_key, value.to_vec())))
    }

    pub(crate) fn get_pinned(&self, key: &[u8]) -> GoatResult<Option<(InternalKey, PinnedValue)>> {
        let probe_key = InternalKey::new(key.to_vec(), SEQUENCE_NUMBER_MAX, InternalKeyKind::Put);

        let block_info = self.find_block_for_key(&probe_key.serialize());

        let (block_index, block_offset, block_size) = match block_info {
            Some(info) => info,
            None => return Ok(None),
        };

        // 1. 使用 BloomFilter 快速过滤（partitioned bloom 只加载命中分区）
        if !self.may_contain_for_block(key, block_index)? {
            return Ok(None);
        }

        // 3. 读取数据块（优先 block cache）
        let block_payload = self.load_data_block_payload(block_offset, block_size)?;
        let block_search_index = self.load_block_search_index(block_index, &block_payload)?;

        // 4. 在数据块中查找 key
        let block_reader =
            match BlockReader::with_search_index(block_payload.as_ref(), block_search_index) {
                Ok(reader) => reader,
                Err(e) => {
                    return Err(Self::corruption(format!(
                        "Failed to parse data block: {}",
                        e
                    )));
                }
            };

        if let Some((internal_key, value_offset, value_len)) =
            block_reader.get_by_user_key_with_value_range(key)
        {
            internal_key
                .kind()
                .map_err(|e| Self::corruption(format!("invalid internal key kind: {}", e)))?;
            let end = value_offset.saturating_add(value_len);
            if end > block_payload.len() {
                return Err(Self::corruption(format!(
                    "value range out of data block bounds: offset={}, len={}, block_len={}",
                    value_offset,
                    value_len,
                    block_payload.len()
                )));
            }
            let pinned = PinnedValue::from_block(block_payload.clone(), value_offset, value_len)
                .ok_or_else(|| {
                    Self::corruption(format!(
                        "failed to pin value range: offset={}, len={}, block_len={}",
                        value_offset,
                        value_len,
                        block_payload.len()
                    ))
                })?;
            return Ok(Some((internal_key, pinned)));
        }

        Ok(None)
    }

    pub(crate) fn get_pinned_at_seq(
        &self,
        key: &[u8],
        read_seq: u64,
    ) -> GoatResult<Option<(InternalKey, PinnedValue)>> {
        let probe_key = InternalKey::new(key.to_vec(), SEQUENCE_NUMBER_MAX, InternalKeyKind::Put);
        let Some(start_block_index) = self.find_block_index_for_key(&probe_key.serialize()) else {
            return Ok(None);
        };
        let mut seen_target_user_key = false;

        for block_index in start_block_index..self.index_entries.len() {
            if !self.may_contain_for_block(key, block_index)? {
                continue;
            }
            let entry = &self.index_entries[block_index];
            let block_payload =
                self.load_data_block_payload(entry.block_offset, entry.block_size)?;
            let block_search_index = self.load_block_search_index(block_index, &block_payload)?;
            let block_reader =
                BlockReader::with_search_index(block_payload.as_ref(), block_search_index)
                    .map_err(|e| Self::corruption(format!("Failed to parse data block: {}", e)))?;

            if let Some((internal_key, value_offset, value_len)) =
                block_reader.get_by_user_key_with_value_range_at_seq(key, read_seq)
            {
                internal_key
                    .kind()
                    .map_err(|e| Self::corruption(format!("invalid internal key kind: {}", e)))?;
                let pinned =
                    PinnedValue::from_block(block_payload.clone(), value_offset, value_len)
                        .ok_or_else(|| {
                            Self::corruption(format!(
                                "failed to pin value range: offset={}, len={}, block_len={}",
                                value_offset,
                                value_len,
                                block_payload.len()
                            ))
                        })?;
                return Ok(Some((internal_key, pinned)));
            }

            if block_reader.get_by_user_key_with_value_range(key).is_some() {
                seen_target_user_key = true;
                continue;
            }

            if seen_target_user_key {
                return Ok(None);
            }

            if let Some(separator_user_key) = Self::user_key_of_internal_key(&entry.separator) {
                if separator_user_key > key {
                    return Ok(None);
                }
            }
        }

        Ok(None)
    }

    /// Full scan of all entries in this SSTable.
    pub fn scan_all(&self) -> GoatResult<Vec<(InternalKey, Vec<u8>)>> {
        let mut entries = Vec::new();
        for entry in &self.index_entries {
            let block_payload =
                self.load_data_block_payload(entry.block_offset, entry.block_size)?;
            let block_reader = BlockReader::new(block_payload.as_ref()).map_err(|e| {
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
    fn find_block_for_key(&self, key: &[u8]) -> Option<(usize, u64, u64)> {
        let block_index = self.find_block_index_for_key(key)?;
        let entry = &self.index_entries[block_index];
        Some((block_index, entry.block_offset, entry.block_size))
    }

    fn find_block_index_for_key(&self, key: &[u8]) -> Option<usize> {
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
            Some(left)
        } else if !self.index_entries.is_empty() {
            // key 大于所有分隔键，应该位于最后一个块中
            Some(self.index_entries.len() - 1)
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

    #[cfg(test)]
    fn loaded_bloom_partition_count_for_test(&self) -> usize {
        match &self.bloom_filter {
            BloomFilterStorage::Legacy(_) => 0,
            BloomFilterStorage::Partitioned(partitioned) => {
                partitioned.loaded_partition_count_for_test()
            }
        }
    }

    #[cfg(test)]
    fn cached_block_search_index_count_for_test(&self) -> usize {
        self.block_search_indexes
            .iter()
            .filter(|slot| slot.get().is_some())
            .count()
    }
}

impl PartitionedBloomFilter {
    fn may_contain(&self, file: &File, key: &[u8], block_index: usize) -> GoatResult<bool> {
        let Some(partition) = self.partitions.get(block_index) else {
            return Ok(true);
        };
        let lookup_key = bloom_lookup_key(key, self.prefix_extractor_len);

        if let (Some(cache), Some(file_id)) = (self.partition_cache.as_ref(), self.file_id) {
            let cache_key = FilterPartitionCacheKey::new(file_id, block_index);
            if let Some(filter) = cache.get(&cache_key) {
                return Ok(filter.contains(lookup_key));
            }

            let mut partition_bitmap = vec![0u8; partition.size as usize];
            SSTableReader::read_exact_at(
                file,
                partition.offset,
                &mut partition_bitmap,
                "sstable_read_bloom_partition",
            )?;
            let filter = cache.insert(cache_key, BloomFilter::new(partition_bitmap));
            return Ok(filter.contains(lookup_key));
        }

        if let Some(filter) = self
            .loaded_partitions
            .lock()
            .unwrap()
            .get(&block_index)
            .cloned()
        {
            return Ok(filter.contains(lookup_key));
        }

        let mut partition_bitmap = vec![0u8; partition.size as usize];
        SSTableReader::read_exact_at(
            file,
            partition.offset,
            &mut partition_bitmap,
            "sstable_read_bloom_partition",
        )?;
        let loaded = Arc::new(BloomFilter::new(partition_bitmap));
        let filter = {
            let mut guard = self.loaded_partitions.lock().unwrap();
            guard
                .entry(block_index)
                .or_insert_with(|| loaded.clone())
                .clone()
        };
        Ok(filter.contains(lookup_key))
    }

    #[cfg(test)]
    fn loaded_partition_count_for_test(&self) -> usize {
        self.loaded_partitions.lock().unwrap().len()
    }
}

pub struct SSTableScanIterator {
    reader: SSTableReader,
    block_index: usize,
    pending_entries: VecDeque<(InternalKey, Vec<u8>)>,
    prefetched_blocks: HashMap<usize, Arc<[u8]>>,
    readahead_blocks: usize,
}

impl SSTableScanIterator {
    fn new(reader: SSTableReader) -> Self {
        let max_upcoming = reader.index_entries.len().saturating_sub(1);
        let mut iter = Self {
            reader,
            block_index: 0,
            pending_entries: VecDeque::new(),
            prefetched_blocks: HashMap::new(),
            readahead_blocks: SCAN_ITERATOR_DEFAULT_READAHEAD_BLOCKS.min(max_upcoming),
        };
        iter.prefetch_upcoming_blocks();
        iter
    }

    pub fn next_entry(&mut self) -> GoatResult<Option<(InternalKey, Vec<u8>)>> {
        loop {
            if let Some(entry) = self.pending_entries.pop_front() {
                return Ok(Some(entry));
            }

            if self.block_index >= self.reader.index_entries.len() {
                return Ok(None);
            }

            let block_payload = self.load_block_payload(self.block_index)?;
            let block_reader = BlockReader::new(block_payload.as_ref()).map_err(|e| {
                SSTableReader::corruption(format!("Failed to parse data block during scan: {}", e))
            })?;

            self.pending_entries.clear();
            for (k, v) in block_reader.iter() {
                self.pending_entries
                    .push_back((SSTableReader::decode_internal_key(&k)?, v));
            }
            self.block_index += 1;
            self.prefetch_upcoming_blocks();
        }
    }

    fn load_block_payload(&mut self, block_index: usize) -> GoatResult<Arc<[u8]>> {
        if let Some(payload) = self.prefetched_blocks.remove(&block_index) {
            return Ok(payload);
        }
        let entry = &self.reader.index_entries[block_index];
        self.reader
            .load_data_block_payload(entry.block_offset, entry.block_size)
    }

    fn prefetch_upcoming_blocks(&mut self) {
        if self.readahead_blocks == 0 {
            return;
        }
        self.prefetched_blocks
            .retain(|idx, _| *idx >= self.block_index);

        let start = self.block_index.saturating_add(1);
        let end = start
            .saturating_add(self.readahead_blocks)
            .min(self.reader.index_entries.len());
        for idx in start..end {
            if self.prefetched_blocks.contains_key(&idx) {
                continue;
            }
            let entry = &self.reader.index_entries[idx];
            if let Ok(payload) = self
                .reader
                .load_data_block_payload(entry.block_offset, entry.block_size)
            {
                self.prefetched_blocks.insert(idx, payload);
            }
        }
    }

    #[cfg(test)]
    fn prefetched_block_count_for_test(&self) -> usize {
        self.prefetched_blocks.len()
    }

    #[cfg(test)]
    fn readahead_blocks_for_test(&self) -> usize {
        self.readahead_blocks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::goatkv::core::kv_engine::KvEngine;
    use crate::goatkv::format::coding;
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

        let mut builder = SSTableBuilder::new_with_manager(1, &sstable_paths, 0).unwrap();

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

    fn footer_padding_range(file_content: &[u8]) -> (usize, usize) {
        let footer_start = file_content.len() - FOOTER_SIZE;
        let footer = &file_content[footer_start..];
        let (_, bloom_bytes_len) =
            coding::decode_varint64_with_length(footer).expect("decode bloom offset");
        let (_, index_bytes_len) = coding::decode_varint64_with_length(&footer[bloom_bytes_len..])
            .expect("decode index offset");
        let padding_start = footer_start + bloom_bytes_len + index_bytes_len;
        let padding_end = file_content.len() - 8;
        (padding_start, padding_end)
    }

    #[test]
    fn test_sstable_iter_all_data() {
        // 创建200条数据并测试完整迭代
        let temp_dir = TempDir::new().unwrap();
        let (_, sstable_paths, _) = KvEngine::init_db_paths(temp_dir.path()).unwrap();
        let mut builder = SSTableBuilder::new_with_manager(1, &sstable_paths, 0).unwrap();

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
        let reader = SSTableReader::open(&sst_path).unwrap();

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
    fn test_scan_iterator_prefetches_upcoming_blocks() {
        let temp_dir = TempDir::new().unwrap();
        let (_, sstable_paths, _) = KvEngine::init_db_paths(temp_dir.path()).unwrap();
        let mut builder = SSTableBuilder::new_with_manager(1, &sstable_paths, 0).unwrap();

        let total_entries = 240usize;
        for i in 0..total_entries {
            let key = format!("scan_readahead_key_{:04}", i);
            let value = vec![b'x'; 1024];
            let internal_key = InternalKey::new(key.into_bytes(), i as u64, InternalKeyKind::Put);
            builder.write(&internal_key.serialize(), &value).unwrap();
        }
        builder.finish().unwrap();

        let reader = SSTableReader::open(sstable_paths.sstable_path_by_id(1)).unwrap();
        assert!(
            reader.index_entry_count() > 1,
            "test requires multiple data blocks"
        );
        let mut iter = reader.into_scan_iterator();
        assert!(
            iter.readahead_blocks_for_test() > 0,
            "readahead should be enabled for multi-block sstable"
        );
        assert!(
            iter.prefetched_block_count_for_test() > 0,
            "iterator should prefetch upcoming blocks at startup"
        );

        let mut seen = 0usize;
        while let Some((_key, _value)) = iter.next_entry().unwrap() {
            seen += 1;
            if seen == 1 {
                assert!(
                    iter.prefetched_block_count_for_test() > 0,
                    "iterator should keep prefetched upcoming blocks while scanning"
                );
            }
        }
        assert_eq!(seen, total_entries);
    }

    #[test]
    fn test_scan_iterator_disables_readahead_for_single_block_sstable() {
        let temp_dir = TempDir::new().unwrap();
        let (_, sstable_paths, _) = KvEngine::init_db_paths(temp_dir.path()).unwrap();
        let mut builder = SSTableBuilder::new_with_manager(1, &sstable_paths, 0).unwrap();

        let key = InternalKey::new(b"single_block_key".to_vec(), 1, InternalKeyKind::Put);
        builder
            .write(&key.serialize(), b"single_block_value")
            .unwrap();
        builder.finish().unwrap();

        let reader = SSTableReader::open(sstable_paths.sstable_path_by_id(1)).unwrap();
        assert_eq!(reader.index_entry_count(), 1);

        let mut iter = reader.into_scan_iterator();
        assert_eq!(iter.readahead_blocks_for_test(), 0);
        assert_eq!(iter.prefetched_block_count_for_test(), 0);
        assert!(iter.next_entry().unwrap().is_some());
        assert!(iter.next_entry().unwrap().is_none());
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
        assert_eq!(reader.format_version(), SSTABLE_FORMAT_VERSION_CURRENT);
    }

    #[test]
    fn test_sstable_reader_compat_legacy_footer_without_format_marker() {
        let (_temp_dir, sst_path) = create_test_sstable();
        let mut file_content = fs::read(&sst_path).expect("read sstable");
        let (padding_start, padding_end) = footer_padding_range(&file_content);
        file_content[padding_start..padding_end].fill(0);
        fs::write(&sst_path, &file_content).expect("rewrite footer as legacy");

        let reader = SSTableReader::open(&sst_path).expect("open legacy-compatible sstable");
        assert_eq!(reader.format_version(), SSTABLE_FORMAT_VERSION_LEGACY);
        assert!(reader.get(b"apple").expect("read").is_some());
    }

    #[test]
    fn test_sstable_reader_rejects_unsupported_format_version() {
        let (_temp_dir, sst_path) = create_test_sstable();
        let mut file_content = fs::read(&sst_path).expect("read sstable");
        let (padding_start, padding_end) = footer_padding_range(&file_content);
        let padding = &mut file_content[padding_start..padding_end];
        assert!(padding.len() >= FOOTER_FORMAT_METADATA_SIZE);
        padding[..4].copy_from_slice(&FOOTER_FORMAT_MARKER);
        padding[4] = SSTABLE_FORMAT_VERSION_CURRENT + 1;
        fs::write(&sst_path, &file_content).expect("write unsupported format footer");

        let err = SSTableReader::open(&sst_path).expect_err("unsupported format should fail open");
        assert_eq!(err.kind(), ErrorKind::Corruption);
        assert!(err
            .to_string()
            .contains("unsupported sstable format version"));
    }

    #[test]
    fn test_sstable_reader_get() {
        let (_temp_dir, sst_path) = create_test_sstable();
        let reader = SSTableReader::open(&sst_path).unwrap();

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
    fn test_sstable_reader_get_pinned_at_seq_returns_visible_version() {
        let temp_dir = TempDir::new().unwrap();
        let (_, sstable_paths, _) = KvEngine::init_db_paths(temp_dir.path()).unwrap();
        let mut builder = SSTableBuilder::new_with_manager(1, &sstable_paths, 0).unwrap();

        for seq in (10u64..=30u64).rev() {
            let internal_key = InternalKey::new(b"k1".to_vec(), seq, InternalKeyKind::Put);
            builder
                .write(&internal_key.serialize(), format!("v{}", seq).as_bytes())
                .unwrap();
        }
        builder.finish().unwrap();

        let reader = SSTableReader::open(sstable_paths.sstable_path_by_id(1)).unwrap();

        let (internal_key, value) = reader.get_pinned_at_seq(b"k1", 25).unwrap().unwrap();
        assert_eq!(internal_key.sequence_number(), 25);
        assert_eq!(value.as_slice(), b"v25");

        assert!(reader.get_pinned_at_seq(b"k1", 5).unwrap().is_none());
    }

    #[test]
    fn test_sstable_reader_get_pinned_at_seq_crosses_blocks() {
        let temp_dir = TempDir::new().unwrap();
        let (_, sstable_paths, _) = KvEngine::init_db_paths(temp_dir.path()).unwrap();
        let mut builder = SSTableBuilder::new_with_manager(1, &sstable_paths, 0).unwrap();

        for seq in (1u64..=200u64).rev() {
            let internal_key = InternalKey::new(b"k1".to_vec(), seq, InternalKeyKind::Put);
            let value = vec![(seq % 251) as u8; 256];
            builder.write(&internal_key.serialize(), &value).unwrap();
        }
        builder.finish().unwrap();

        let reader = SSTableReader::open(sstable_paths.sstable_path_by_id(1)).unwrap();
        assert!(
            reader.index_entry_count() > 1,
            "test requires multiple data blocks"
        );

        let (internal_key, value) = reader.get_pinned_at_seq(b"k1", 123).unwrap().unwrap();
        assert_eq!(internal_key.sequence_number(), 123);
        assert_eq!(value.as_slice(), vec![123u8; 256].as_slice());
    }

    #[test]
    fn test_sstable_reader_reuses_cached_block_search_index_for_hot_get() {
        let (_temp_dir, sst_path) = create_test_sstable();
        let reader = SSTableReader::open(&sst_path).unwrap();

        assert_eq!(reader.cached_block_search_index_count_for_test(), 0);
        assert!(reader.get(b"apple").unwrap().is_some());
        assert_eq!(reader.cached_block_search_index_count_for_test(), 1);

        // Same data block hit should reuse cached restart index instead of reparsing.
        assert!(reader.get(b"banana").unwrap().is_some());
        assert_eq!(reader.cached_block_search_index_count_for_test(), 1);
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
    fn test_partitioned_bloom_respects_prefix_extractor() {
        let temp_dir = TempDir::new().unwrap();
        let (_, sstable_paths, _) = KvEngine::init_db_paths(temp_dir.path()).unwrap();
        let mut builder = SSTableBuilder::new_with_bloom_prefix_extractor(1, &sstable_paths, 3)
            .expect("create sstable builder");

        let rows = [
            (b"abc-001".as_slice(), b"v1".as_slice()),
            (b"abc-002".as_slice(), b"v2".as_slice()),
            (b"xyz-001".as_slice(), b"v3".as_slice()),
        ];
        for (idx, (user_key, value)) in rows.iter().enumerate() {
            let internal_key = InternalKey::new(
                (*user_key).to_vec(),
                (100 - idx) as u64,
                InternalKeyKind::Put,
            );
            builder
                .write(&internal_key.serialize(), value)
                .expect("write row");
        }
        builder.finish().expect("finish sstable");

        let sst_path = sstable_paths.sstable_path_by_id(1);
        let reader = SSTableReader::open(&sst_path).expect("open reader");

        // Prefix bloom should return true for any key that shares an inserted prefix.
        assert!(reader.may_contain(b"abc-999"));
        assert_eq!(reader.get(b"abc-999").expect("get"), None);
        assert!(!reader.may_contain(b"qqq-111"));
    }

    #[test]
    fn test_partitioned_bloom_loads_partitions_lazily() {
        let temp_dir = TempDir::new().unwrap();
        let (_, sstable_paths, _) = KvEngine::init_db_paths(temp_dir.path()).unwrap();
        let mut builder = SSTableBuilder::new_with_bloom_prefix_extractor(1, &sstable_paths, 0)
            .expect("create sstable builder");

        for i in 0..128u64 {
            let user_key = format!("lazy_key_{:03}", i).into_bytes();
            let internal_key = InternalKey::new(user_key, i, InternalKeyKind::Put);
            let value = vec![b'x'; 1024];
            builder
                .write(&internal_key.serialize(), &value)
                .expect("write row");
        }
        builder.finish().expect("finish sstable");

        let sst_path = sstable_paths.sstable_path_by_id(1);
        let reader = SSTableReader::open(&sst_path).expect("open reader");

        assert_eq!(reader.loaded_bloom_partition_count_for_test(), 0);
        assert!(reader.may_contain(b"lazy_key_003"));
        assert_eq!(reader.loaded_bloom_partition_count_for_test(), 1);
        assert!(reader.may_contain(b"lazy_key_120"));
        assert!(reader.loaded_bloom_partition_count_for_test() >= 2);
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
        let mut builder = SSTableBuilder::new_with_manager(1, &sstable_paths, 0).unwrap();

        let mut raw_key = b"bad_kind".to_vec();
        let encoded_sequence_with_invalid_kind = (42u64 << 8) | 2u64;
        raw_key.extend_from_slice(&(!encoded_sequence_with_invalid_kind).to_be_bytes());
        builder.write(&raw_key, b"value").unwrap();
        builder.finish().unwrap();

        let sst_path = sstable_paths.sstable_path_by_id(1);
        let reader = SSTableReader::open(&sst_path).unwrap();
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

        let reader = SSTableReader::open(&sst_path).unwrap();
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
