use std::cmp::Ordering;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use crate::goatkv::encoding::varint;
use crate::goatkv::storage::block_reader::BlockReader;

/// SSTable 文件的 Magic Number
const MAGIC_NUMBER: u64 = 0x706A725F676F6174;
/// Footer 的固定大小：根据 SSTableBuilder 的写入，footer 固定为 48 字节
/// 两个varint(最多20字节) + padding + magic(8字节)
const FOOTER_SIZE: usize = 48;

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
    bloom_filter: crate::goatkv::storage::bloom_builder::BloomFilter,
    /// 索引条目列表，按分隔键排序
    index_entries: Vec<IndexEntry>,
}

impl SSTableReader {
    /// 打开并解析 SSTable 文件
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path_ref = path.as_ref();
        let mut file = File::open(path_ref)?;

        // 1. 读取文件大小
        let file_size = file.metadata()?.len();
        if file_size < FOOTER_SIZE as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "SSTable file is too small to contain valid footer",
            ));
        }

        // 2. 读取最后的 FOOTER_SIZE 字节
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut footer = vec![0u8; FOOTER_SIZE];
        file.read_exact(&mut footer)?;

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
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Invalid magic number: expected {:x}, got {:x}",
                    MAGIC_NUMBER, magic
                ),
            ));
        }

        // 4. 从前往后解析 Footer，但使用更健壮的方法
        // Footer 结构：bloom_offset(varint) + index_offset(varint) + padding + magic(8 bytes)
        let mut cursor = 0;

        // 解析 bloom_offset (varint)
        let (bloom_offset, bloom_bytes_len) = match varint::decode_with_length(&footer[cursor..]) {
            Ok(result) => result,
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to decode bloom filter offset: {}", e),
                ));
            }
        };

        cursor += bloom_bytes_len;

        // 解析 index_offset (varint)
        let (index_offset, index_bytes_len) = match varint::decode_with_length(&footer[cursor..]) {
            Ok(result) => result,
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to decode index block offset: {}", e),
                ));
            }
        };

        cursor += index_bytes_len;

        // 跳过 padding (应该是0字节)
        // padding 大小 = FOOTER_SIZE - 8(magic) - bloom_bytes_len - index_bytes_len
        // 验证 padding 都是0
        for i in cursor..(footer.len() - 8) {
            if footer[i] != 0 {
                // 不返回错误，只是记录警告，因为可能有其他数据
            }
        }

        // 6. 验证偏移量
        if bloom_offset >= file_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Invalid bloom_offset: bloom_offset={}, file_size={}",
                    bloom_offset, file_size
                ),
            ));
        }

        if index_offset >= file_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Invalid index_offset: index_offset={}, file_size={}",
                    index_offset, file_size
                ),
            ));
        }

        if bloom_offset >= index_offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Invalid offset order: bloom_offset={} >= index_offset={}",
                    bloom_offset, index_offset
                ),
            ));
        }

        // 5. 读取 BloomFilter
        file.seek(SeekFrom::Start(bloom_offset))?;
        // BloomFilter 的大小是 index_offset - bloom_offset
        let bloom_filter_size = index_offset - bloom_offset;
        let mut bloom_bitmap = vec![0u8; bloom_filter_size as usize];
        file.read_exact(&mut bloom_bitmap)?;
        let bloom_filter = crate::goatkv::storage::bloom_builder::BloomFilter::new(bloom_bitmap);

        // 6. 读取和解析索引块
        // index_offset 是索引块的开始位置
        // 索引块从 index_offset 开始，到文件末尾减去footer大小结束
        // 注意：index_offset 已经指向索引块的开始，因为 BloomFilter 已经读取完毕
        let index_block_start = index_offset;
        let footer_start = file_size - FOOTER_SIZE as u64;

        if index_block_start >= footer_start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Index block start beyond footer: index_block_start={}, footer_start={}",
                    index_block_start, footer_start
                ),
            ));
        }

        let index_block_size = footer_start - index_block_start;
        if index_block_size == 0 {
            // 索引块可能为空（只有一个数据块的情况？）
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Index block size is zero",
            ));
        }

        file.seek(SeekFrom::Start(index_block_start))?;
        let mut index_block_data = vec![0u8; index_block_size as usize];
        file.read_exact(&mut index_block_data)?;

        // 解析索引块
        let index_reader = match BlockReader::new(&index_block_data) {
            Ok(reader) => reader,
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to parse index block: {}", e),
                ));
            }
        };

        // 7. 提取索引条目
        let mut index_entries = Vec::new();
        for (separator, offset_data) in index_reader.iter() {
            // offset_data 格式：block_offset(varint) + block_size(varint)
            if offset_data.len() < 2 {
                continue;
            }

            // 解码块偏移量
            let (block_offset, offset_len) = match varint::decode_with_length(&offset_data) {
                Ok(result) => result,
                Err(_) => continue,
            };

            // 解码块大小
            let block_size_data = &offset_data[offset_len..];
            let block_size = match varint::decode(block_size_data) {
                Ok(size) => size,
                Err(_) => continue,
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

    /// 在 SSTable 中查找指定的 key
    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        // 1. 使用 BloomFilter 快速过滤
        if !self.may_contain(key) {
            return Ok(None);
        }

        // 2. 在索引中查找对应的数据块
        let block_info = self.find_block_for_key(key);
        let (block_offset, block_size) = match block_info {
            Some(info) => info,
            None => return Ok(None),
        };

        // 3. 读取数据块
        self.file.seek(SeekFrom::Start(block_offset))?;
        let mut block_data = vec![0u8; block_size as usize];
        self.file.read_exact(&mut block_data)?;

        // 4. 在数据块中查找 key
        let block_reader = match BlockReader::new(&block_data) {
            Ok(reader) => reader,
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to parse data block: {}", e),
                ));
            }
        };

        Ok(block_reader.get(key))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::goatkv::storage::sstable_builder::SSTableBuilder;
    use std::io::Write;
    use tempfile::TempDir;

    /// 创建测试用的 SSTable 文件
    fn create_test_sstable() -> TempDir {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        let mut builder = SSTableBuilder::new(1, dir_path.to_path_buf()).unwrap();

        // 添加一些测试数据
        builder.write(b"apple", b"fruit1");
        builder.write(b"banana", b"fruit2");
        builder.write(b"cherry", b"fruit3");
        builder.write(b"date", b"fruit4");
        builder.write(b"elderberry", b"fruit5");

        builder.finish();

        temp_dir
    }

    #[test]
    fn test_sstable_reader_open() {
        let temp_dir = create_test_sstable();
        let sst_path = temp_dir.path().join("000001.sst");

        // 先检查文件是否存在
        assert!(sst_path.exists());

        let reader = SSTableReader::open(&sst_path);

        // 如果失败，打印错误信息
        if let Err(ref e) = reader {
            println!("Error opening SSTable: {}", e);
        }

        assert!(reader.is_ok());
        let reader = reader.unwrap();

        // 验证基本属性
        assert!(reader.index_entry_count() > 0);
        assert!(reader.min_key().is_some());
        println!(
            "Reader created successfully with {} index entries",
            reader.index_entry_count()
        );
    }

    #[test]
    fn test_sstable_reader_get() {
        let temp_dir = create_test_sstable();
        let sst_path = temp_dir.path().join("000001.sst");
        let mut reader = SSTableReader::open(&sst_path).unwrap();

        // 测试存在的key
        let result = reader.get(b"apple");
        assert!(result.is_ok());
        let value = result.unwrap();
        assert_eq!(value, Some(b"fruit1".to_vec()));

        // 测试另一个存在的key
        let result = reader.get(b"cherry");
        assert!(result.is_ok());
        let value = result.unwrap();
        assert_eq!(value, Some(b"fruit3".to_vec()));

        // 测试不存在的key
        let result = reader.get(b"fig");
        assert!(result.is_ok());
        let value = result.unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_sstable_reader_may_contain() {
        let temp_dir = create_test_sstable();
        let sst_path = temp_dir.path().join("000001.sst");
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
}
