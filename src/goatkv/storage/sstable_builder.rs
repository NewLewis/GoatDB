use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::PathBuf;

use crate::goatkv::encoding::varint;
use crate::goatkv::storage::block_builder::BlockBuilder;
use crate::goatkv::storage::bloom_builder::BloomBuilder;
use crate::goatkv::storage::sstable_reader::SSTable;

/// SSTable文件的魔数（Magic Number）
/// 用于标识文件格式，固定值为 0x706A725F676F6174
/// 对应的ASCII字符串为 "pjr_goat"（反向）
const MAGIC_NUMBER: u64 = 0x706A725F676F6174;

/// SSTable的固定Footer大小
/// Footer包含两个varint偏移量（最多20字节）+ padding + magic number（8字节）
/// Footer的总大小固定为48字节，确保可以从文件末尾读取Footer
#[cfg(test)]
const FOOTER_SIZE: usize = 48;

/// SSTable文件的最大索引条目分隔符长度
/// 用于限制索引块中的分隔符大小，防止内存过度使用
// const MAX_SEPARATOR_LENGTH: usize = 256; // 暂未使用，注释掉避免警告

/// SSTableBuilder用于构建SSTable文件
///
/// # SSTable文件结构
/// SSTable（Sorted String Table）是一种有序的、不可变的键值存储格式，
/// 适用于大规模数据的持久化和高效查询：
///
/// ```
/// +------------------+
/// |  Data Block 0    |  数据块：存储实际key-value对
/// +------------------+
/// |  Data Block 1    |  使用前缀压缩和重启点
/// +------------------+
/// |  ...             |
/// +------------------+
/// |  Data Block N    |
/// +------------------+
/// |  Bloom Filter    |  布隆过滤器：快速过滤不存在的key
/// +------------------+
/// |  Index Block     |  索引块：记录每个数据块的位置和大小
/// +------------------+
/// |  Footer          |  文件尾：记录BloomFilter和IndexBlock的偏移量
/// +------------------+
/// ```
///
/// # Footer结构（48字节）
/// Footer位于文件末尾，用于定位各个组件的位置：
/// ```
/// +------------------+
/// |  Bloom Offset    |  varint编码的BloomFilter起始偏移量
/// +------------------+
/// |  Index Offset    |  varint编码的IndexBlock起始偏移量
/// +------------------+
/// |  Padding         |  0字节填充，使Footer总大小为48字节
/// +------------------+
/// |  Magic Number    |  8字节魔数，标识文件格式
/// +------------------+
/// ```
///
/// # 索引块结构
/// 索引块用于快速定位包含特定key的数据块：
/// ```
/// Index Entry格式：
/// [separator (varint-encoded key)] -> [block_offset (varint), block_size (varint)]
///
/// separator是一个特殊计算的key，表示该数据块中最大的key
/// 或是两个key之间的最小分隔符
/// ```
///
/// # 写入流程
/// 1. 调用write()添加key-value对
/// 2. 当数据块达到4KB时，自动完成当前数据块并开始新的数据块
/// 3. 同时更新索引块和布隆过滤器
/// 4. 调用finish()完成SSTable构建，写入BloomFilter、IndexBlock和Footer
///
/// # 示例
/// ```
/// use std::path::PathBuf;
/// let mut builder = SSTableBuilder::new(1, PathBuf::from("./data"))?;
/// builder.write(b"apple", b"fruit");
/// builder.write(b"banana", b"fruit");
/// builder.finish();
/// ```
pub struct SSTableBuilder {
    /// 带缓冲的文件写入器
    /// 使用BufWriter提高写入性能
    writer: io::BufWriter<File>,

    /// 当前正在构建的数据块构建器
    /// 当数据块达到4KB时，会被finish并写入文件
    data_block_builder: BlockBuilder,

    /// 索引块构建器
    /// 用于记录每个数据块的位置和大小信息
    index_block_builder: BlockBuilder,

    /// 布隆过滤器构建器
    /// 用于快速过滤不存在的key，减少磁盘I/O
    bloom_builder: BloomBuilder,

    /// 当前文件写入偏移量
    /// 用于跟踪文件中的写入位置，记录数据块的偏移量
    offset: u64,

    /// SSTable ID
    id: u64,
    /// SSTable 文件路径
    path: PathBuf,
}


impl SSTableBuilder {
    /// 创建一个新的SSTableBuilder
    ///
    /// # 参数
    /// - `id`: SSTable的唯一标识符
    /// - `path`: 存放SSTable文件的目录路径
    ///
    /// # 文件命名规则
    /// - 如果id < 1,000,000：格式为 `{id:06}.sst`（如 000001.sst）
    /// - 如果id >= 1,000,000：格式为 `{id}.sst`（如 1234567.sst）
    ///
    /// # 错误
    /// 返回io::Error，如果文件创建失败
    ///
    /// # 示例
    /// ```
    /// use std::path::PathBuf;
    /// let builder = SSTableBuilder::new(1, PathBuf::from("./data"))?;
    /// // 创建文件 ./data/000001.sst
    /// ```
    pub fn new(id: u64, path: PathBuf) -> io::Result<Self> {
        let filename = Self::get_file_name(id, path);

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&filename)?;

        Ok(Self {
            writer: io::BufWriter::new(file),
            data_block_builder: BlockBuilder::new(),
            index_block_builder: BlockBuilder::new(),
            bloom_builder: BloomBuilder::new(),
            offset: 0,
            id,
            path: filename.into(),
        })
    }


    /// 生成SSTable文件名
    ///
    /// # 参数
    /// - `id`: SSTable的唯一标识符
    /// - `path`: 存放文件的目录路径
    ///
    /// # 返回值
    /// 返回完整的文件路径字符串
    ///
    /// # 示例
    /// ```
    /// let filename = SSTableBuilder::get_file_name(1, PathBuf::from("./data"));
    /// // 返回 "./data/000001.sst"
    ///
    /// let filename = SSTableBuilder::get_file_name(1234567, PathBuf::from("./data"));
    /// // 返回 "./data/1234567.sst"
    /// ```
    fn get_file_name(id: u64, path: PathBuf) -> String {
        if id < 1000000 {
            format!("{}/{:06}.sst", path.display(), id)
        } else {
            format!("{}/{}.sst", path.display(), id)
        }
    }

    /// 向SSTable中写入一个key-value对
    ///
    /// # 写入流程
    /// 1. 检查当前数据块是否已满（4KB）
    /// 2. 如果满了，完成当前数据块并开始新的数据块
    /// 3. 将key-value对添加到当前数据块
    /// 4. 将key添加到布隆过滤器
    ///
    /// # 数据块管理
    /// - 当数据块达到4KB时，自动完成并写入文件
    /// - 完成数据块时会更新索引块
    /// - 自动管理多个数据块的分隔
    ///
    /// # 参数
    /// - `key`: 键的字节数据
    /// - `value`: 值的字节数据
    ///
    /// # 注意事项
    /// - key必须按字典序递增添加，SSTable假设key是有序的
    /// - 乱序添加会导致索引查询失败
    ///
    /// # 示例
    /// ```
    /// let mut builder = SSTableBuilder::new(1, PathBuf::from("./data"))?;
    /// builder.write(b"apple", b"fruit");
    /// builder.write(b"banana", b"fruit");
    /// builder.write(b"cherry", b"fruit");
    /// ```
    pub fn write(&mut self, key: &[u8], value: &[u8]) {
        // 检查当前数据块是否已满（>= 4KB）
        if self.data_block_builder.should_finish() {
            // 完成当前数据块，使用下一个key作为separator的参考
            self.finish_data_block(key);
        }

        // 将key-value对添加到当前数据块
        // 数据块内部使用前缀压缩和重启点机制
        self.data_block_builder.add(key, value);

        // 将key添加到布隆过滤器
        // 注意：BloomFilter 应该索引 UserKey，以便于查询
        // key 是 InternalKey (UserKey + 8 bytes Seq/Kind)
        if key.len() >= 8 {
            let user_key = &key[..key.len() - 8];
            self.bloom_builder.add(user_key);
        } else {
            // Fallback (should not happen for valid InternalKey)
            self.bloom_builder.add(key);
        }
    }

    /// 完成当前数据块的构建并写入文件
    ///
    /// # 流程
    /// 1. 完成数据块的编码（包含restart array和count）
    /// 2. 计算分隔符（separator）用于索引
    /// 3. 将数据块的位置和大小信息添加到索引块
    /// 4. 写入数据块到文件
    /// 5. 更新文件偏移量
    /// 6. 重置数据块构建器
    ///
    /// # 参数
    /// - `key`: 下一个要写入的key（用于计算separator）
    ///
    /// # Separator的作用
    /// Separator是一个特殊的key，用于在索引中表示该数据块包含的key范围
    /// - 它是该数据块中最大的key
    /// - 或是该数据块中最后一个key与下一个key之间的最小分隔符
    /// - 用于快速定位包含特定key的数据块
    fn finish_data_block(&mut self, key: &[u8]) {
        // 完成数据块的编码
        // finish()会写入restart array和restart count
        let (block_content, last_key) = self.data_block_builder.finish();

        // 计算separator用于索引
        // separator是用于索引的特殊key，表示该数据块的key范围
        let separator = Self::compute_separator(&last_key, key);

        // 将separator和数据块信息添加到索引块
        // 索引格式：separator -> (block_offset, block_size)
        let mut separator_val = Vec::new();
        separator_val.extend_from_slice(&varint::encode(self.offset));
        separator_val.extend_from_slice(&varint::encode(block_content.len() as u64));
        self.index_block_builder.add(&separator, &separator_val);

        // 将数据块写入文件
        self.writer.write_all(&block_content).unwrap();
        self.offset += block_content.len() as u64;

        // 重置数据块构建器，准备开始新的数据块
        self.data_block_builder.reset();
    }

    /// 完成SSTable的构建，写入所有元数据
    ///
    /// # 完成流程
    /// 1. 如果有未完成的数据块，完成并写入
    /// 2. 写入BloomFilter到文件
    /// 3. 写入IndexBlock到文件
    /// 4. 写入Footer（包含BloomFilter和IndexBlock的偏移量）
    /// 5. 刷新缓冲区，确保所有数据写入磁盘
    ///
    /// # 文件最终结构
    /// ```
    /// [Data Block 0][Data Block 1]...[Data Block N]
    /// [Bloom Filter]
    /// [Index Block]
    /// [Footer]
    /// ```
    ///
    /// # 注意事项
    /// - 调用finish()后不能再调用write()
    /// - 确保调用finish()以完成文件写入
    /// - 内部会自动flush缓冲区
    ///
    /// # 示例
    /// ```
    /// let mut builder = SSTableBuilder::new(1, PathBuf::from("./data"))?;
    /// builder.write(b"key1", b"value1");
    /// builder.write(b"key2", b"value2");
    /// builder.finish(); // 完成构建
    /// ```
    /// builder.finish().unwrap(); // 完成构建
    /// ```
    pub fn finish(&mut self) -> io::Result<SSTable> {

        // 如果有未完成的数据块，先完成它
        if !self.data_block_builder.empty() {
            let (block_content, last_key) = self.data_block_builder.finish();

            // 将最后一个数据块的信息添加到索引
            // 注意：这里使用last_key作为separator（因为这是最后一个数据块）
            let mut separator_val = Vec::new();
            separator_val.extend_from_slice(&varint::encode(self.offset));
            separator_val.extend_from_slice(&varint::encode(block_content.len() as u64));
            self.index_block_builder.add(&last_key, &separator_val);

            // 写入最后一个数据块
            self.writer.write_all(&block_content).unwrap();
            self.offset += block_content.len() as u64;

            // 重置数据块构建器
            self.data_block_builder.reset();
        }

        // 写入BloomFilter
        // BloomFilter用于快速过滤不存在的key
        self.writer.write_all(self.bloom_builder.bitmap()).unwrap();
        let bloom_offset = self.offset;
        self.offset += self.bloom_builder.bitmap().len() as u64;

        // 写入IndexBlock
        // IndexBlock包含所有数据块的位置和大小信息
        let (block_content, _) = self.index_block_builder.finish();
        self.writer.write_all(&block_content).unwrap();
        let index_offset = self.offset;
        self.offset += block_content.len() as u64;

        // 写入Footer
        // Footer包含BloomFilter和IndexBlock的偏移量，用于读取时定位
        let bloom_offset_bytes = varint::encode(bloom_offset);
        let bloom_offset_len = bloom_offset_bytes.len();

        let index_offset_bytes = varint::encode(index_offset);
        let index_offset_len = index_offset_bytes.len();

        // 写入两个偏移量
        self.writer.write_all(&bloom_offset_bytes).unwrap();
        self.writer.write_all(&index_offset_bytes).unwrap();

        // 填充0字节，使Footer总大小为48字节
        // Padding = 48 - (bloom_offset_len + index_offset_len + magic_len)
        // magic_len固定为8字节
        let padding = vec![0; 40 - (bloom_offset_len + index_offset_len)];
        self.writer.write_all(&padding).unwrap();

        // 写入魔数（8字节），标识文件格式
        self.writer.write_all(&MAGIC_NUMBER.to_le_bytes()).unwrap();

        // 刷新缓冲区，确保所有数据写入磁盘
        // 刷新缓冲区，确保所有数据写入磁盘
        self.writer.flush()?;

        Ok(SSTable::new(self.id, self.path.clone()))
    }


    /// 计算索引中使用的分隔符（separator）
    ///
    /// # Separator的作用
    /// Separator是一个特殊的key，用于索引中表示数据块的key范围：
    /// - 它是该数据块中最大的key
    /// - 或是该数据块最后一个key与下一个key之间的最小分隔符
    /// - 查询时，如果目标key <= separator，则该数据块可能包含该key
    ///
    /// # 计算算法
    /// 1. 如果两个key长度相同且最后一个字符相差1（递增序列），
    ///    直接返回last_key作为separator
    /// 2. 否则，逐字节比较，找到第一个不同的位置
    /// 3. 返回last_key的共享部分 + 下一个字节值（如果<0xff）
    ///
    /// # 示例
    /// ```
    /// last_key = b"apple"
    /// key = b"application"
    /// // 比较：a=a, p=p, p=p, l=l, e=i (不同)
    /// // separator = b"appl" + b"m" = b"applm"
    ///
    /// last_key = b"key001"
    /// key = b"key002"
    /// // 递增序列，直接返回last_key
    /// // separator = b"key001"
    /// ```
    ///
    /// # 参数
    /// - `last_key`: 当前数据块的最后一个key
    /// - `key`: 下一个要写入的key
    ///
    /// # 返回值
    /// 返回计算得到的separator
    fn compute_separator(last_key: &[u8], key: &[u8]) -> Vec<u8> {
        let mut result = Vec::new();
        let mut i = 0;

        // 特殊情况：如果两个key长度相同且是递增序列
        // 例如：key001 -> key002
        if last_key.len() == key.len() && last_key[last_key.len() - 1] + 1 == key[key.len() - 1] {
            // 检查前面的字符是否都相同
            if last_key[..last_key.len() - 1] == key[..key.len() - 1] {
                return last_key.to_vec();
            }
        }

        // 逐字节比较，找到第一个不同的位置
        while i < last_key.len() && i < key.len() {
            if last_key[i] != key[i] {
                // 找到不同位置，如果last_key[i] < 0xff，可以加1
                if last_key[i] < 0xff {
                    result.push(last_key[i] + 1);
                    return result;
                }
                // 如果是0xff，需要继续向后查找
                return last_key.to_vec();
            }
            // 相同的字符加入结果
            result.push(last_key[i]);
            i += 1;
        }

        // 如果其中一个key是另一个的前缀，返回last_key
        last_key.to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// 测试SSTableBuilder的基本创建和写入功能
    #[test]
    fn test_sstable_builder_basic() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        let mut builder = SSTableBuilder::new(1, dir_path.to_path_buf()).unwrap();

        // 写入几个简单的key-value对
        builder.write(b"apple", b"fruit");
        builder.write(b"banana", b"fruit");
        builder.write(b"cherry", b"fruit");

        // 完成构建
        builder.finish();

        // 验证文件已创建
        let sst_path = dir_path.join("000001.sst");
        assert!(sst_path.exists());

        // 验证文件不为空
        let metadata = std::fs::metadata(&sst_path).unwrap();
        assert!(metadata.len() > 0);
    }

    /// 测试文件名生成
    #[test]
    fn test_sstable_builder_file_name() {
        // 测试id < 1,000,000的情况
        let filename1 = SSTableBuilder::get_file_name(1, PathBuf::from("./data"));
        assert_eq!(filename1, "./data/000001.sst");

        let filename2 = SSTableBuilder::get_file_name(999999, PathBuf::from("./data"));
        assert_eq!(filename2, "./data/999999.sst");

        // 测试id >= 1,000,000的情况
        let filename3 = SSTableBuilder::get_file_name(1000000, PathBuf::from("./data"));
        assert_eq!(filename3, "./data/1000000.sst");

        let filename4 = SSTableBuilder::get_file_name(1234567, PathBuf::from("./data"));
        assert_eq!(filename4, "./data/1234567.sst");
    }

    /// 测试单个数据块的情况（数据量小于4KB）
    #[test]
    fn test_sstable_builder_single_block() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        let mut builder = SSTableBuilder::new(1, dir_path.to_path_buf()).unwrap();

        // 只写入少量数据，不会触发多个数据块
        for i in 0..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            builder.write(key.as_bytes(), value.as_bytes());
        }

        builder.finish();

        let sst_path = dir_path.join("000001.sst");
        assert!(sst_path.exists());

        // 验证文件大小合理（包含数据块、BloomFilter、IndexBlock和Footer）
        let metadata = std::fs::metadata(&sst_path).unwrap();
        assert!(metadata.len() > 100 && metadata.len() < 5000); // 100字节到5KB之间
    }

    /// 测试多个数据块的情况（数据量超过4KB）
    #[test]
    fn test_sstable_builder_multiple_blocks() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        let mut builder = SSTableBuilder::new(1, dir_path.to_path_buf()).unwrap();

        // 写入大量数据以触发多个数据块
        for i in 0..500 {
            let key = format!("key_{:010}", i);
            let value = format!("value_{}", i);
            builder.write(key.as_bytes(), value.as_bytes());
        }

        builder.finish();

        let sst_path = dir_path.join("000001.sst");
        assert!(sst_path.exists());

        // 验证文件较大（应该包含多个数据块）
        let metadata = std::fs::metadata(&sst_path).unwrap();
        assert!(metadata.len() > 3000); // 大于3KB（包含多个数据块、BloomFilter和IndexBlock）
    }

    /// 测试分隔符计算
    #[test]
    fn test_compute_separator() {
        // 测试递增序列
        let separator = SSTableBuilder::compute_separator(b"key001", b"key002");
        assert_eq!(separator, b"key001");

        let separator = SSTableBuilder::compute_separator(b"key099", b"key100");
        // key099和key100在'9'和'1'处不同，separator = b"key1"
        assert_eq!(separator, b"key1");

        // 测试有共同前缀的key
        let separator = SSTableBuilder::compute_separator(b"apple", b"application");
        // apple vs application: 'appl' 相同，'e' vs 'i' 不同
        // separator = 'appl' + ('e' + 1) = 'applf'
        assert_eq!(separator, b"applf");

        let separator = SSTableBuilder::compute_separator(b"banana", b"band");
        // banana vs band: 'ban' 相同，'a' vs 'd' 不同
        // separator = 'ban' + ('a' + 1) = 'banb'
        assert_eq!(separator, b"banb");

        // 测试完全不相同的key
        let separator = SSTableBuilder::compute_separator(b"aaa", b"bbb");
        assert_eq!(separator, b"b");

        // 测试一个key是另一个的前缀
        let separator = SSTableBuilder::compute_separator(b"app", b"apple");
        assert_eq!(separator, b"app");
    }

    /// 测试空key的处理
    #[test]
    fn test_sstable_builder_empty_key() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        let mut builder = SSTableBuilder::new(1, dir_path.to_path_buf()).unwrap();

        // 写入包含空key的数据
        builder.write(b"", b"empty_key_value");
        builder.write(b"a", b"next_value");

        builder.finish();

        let sst_path = dir_path.join("000001.sst");
        assert!(sst_path.exists());
    }

    /// 测试空value的处理
    #[test]
    fn test_sstable_builder_empty_value() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        let mut builder = SSTableBuilder::new(1, dir_path.to_path_buf()).unwrap();

        // 写入包含空value的数据
        builder.write(b"key1", b"");
        builder.write(b"key2", b"");
        builder.write(b"key3", b"");

        builder.finish();

        let sst_path = dir_path.join("000001.sst");
        assert!(sst_path.exists());
    }

    /// 测试特殊字符的key
    #[test]
    fn test_sstable_builder_special_keys() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        let mut builder = SSTableBuilder::new(1, dir_path.to_path_buf()).unwrap();

        // 写入包含特殊字符的key
        builder.write(b"key_with_underscore", b"value1");
        builder.write(b"key-with-dash", b"value2");
        builder.write(b"key.with.dot", b"value3");
        builder.write(b"key space", b"value4");
        builder.write(b"123number", b"value5");

        builder.finish();

        let sst_path = dir_path.join("000001.sst");
        assert!(sst_path.exists());
    }

    /// 测试大value的处理
    #[test]
    fn test_sstable_builder_large_value() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        let mut builder = SSTableBuilder::new(1, dir_path.to_path_buf()).unwrap();

        // 写入包含大value的数据
        let large_value = vec![b'x'; 10000];
        builder.write(b"key1", &large_value);
        builder.write(b"key2", &large_value);

        builder.finish();

        let sst_path = dir_path.join("000001.sst");
        assert!(sst_path.exists());

        // 验证文件较大
        let metadata = std::fs::metadata(&sst_path).unwrap();
        assert!(metadata.len() > 20000);
    }

    /// 测试Footer的正确性
    #[test]
    fn test_sstable_builder_footer() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        let mut builder = SSTableBuilder::new(1, dir_path.to_path_buf()).unwrap();

        // 写入一些数据
        for i in 0..50 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            builder.write(key.as_bytes(), value.as_bytes());
        }

        builder.finish();

        let sst_path = dir_path.join("000001.sst");

        // 读取文件内容
        let mut file = File::open(&sst_path).unwrap();
        let file_size = file.metadata().unwrap().len();

        // 读取最后48字节（Footer）
        let mut footer = vec![0u8; FOOTER_SIZE];
        file.seek(std::io::SeekFrom::End(-(FOOTER_SIZE as i64)))
            .unwrap();
        file.read_exact(&mut footer).unwrap();

        // 验证魔数
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
        assert_eq!(magic, MAGIC_NUMBER, "Magic number mismatch");

        // 解析bloom_offset和index_offset
        let mut cursor = 0;
        let (bloom_offset, bloom_bytes_len) =
            varint::decode_with_length(&footer[cursor..]).unwrap();
        cursor += bloom_bytes_len;

        let (index_offset, _) = varint::decode_with_length(&footer[cursor..]).unwrap();

        // 验证偏移量合理
        assert!(bloom_offset < file_size, "Bloom offset out of bounds");
        assert!(index_offset < file_size, "Index offset out of bounds");
        assert!(
            bloom_offset < index_offset,
            "Bloom offset should be before index offset"
        );
    }

    /// 测试多个SSTable的创建
    #[test]
    fn test_sstable_builder_multiple_files() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        // 创建多个SSTable
        for id in 1..=5 {
            let mut builder = SSTableBuilder::new(id, dir_path.to_path_buf()).unwrap();

            for i in 0..10 {
                let key = format!("key_{}_{}", id, i);
                let value = format!("value_{}_{}", id, i);
                builder.write(key.as_bytes(), value.as_bytes());
            }

            builder.finish();
        }

        // 验证所有文件都已创建
        for id in 1..=5 {
            let sst_path = dir_path.join(format!("{:06}.sst", id));
            assert!(sst_path.exists(), "SSTable {} not created", id);
        }
    }

    /// 测试连续写入
    #[test]
    fn test_sstable_builder_sequential_writes() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        let mut builder = SSTableBuilder::new(1, dir_path.to_path_buf()).unwrap();

        // 连续写入100个key-value对
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{:03}", i);
            builder.write(key.as_bytes(), value.as_bytes());
        }

        builder.finish();

        let sst_path = dir_path.join("000001.sst");
        assert!(sst_path.exists());
    }

    /// 测试非常小的key和value
    #[test]
    fn test_sstable_builder_small_entries() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        let mut builder = SSTableBuilder::new(1, dir_path.to_path_buf()).unwrap();

        // 写入单字节的key和value
        builder.write(b"a", b"1");
        builder.write(b"b", b"2");
        builder.write(b"c", b"3");

        builder.finish();

        let sst_path = dir_path.join("000001.sst");
        assert!(sst_path.exists());
    }

    /// 测试大量的条目
    #[test]
    fn test_sstable_builder_many_entries() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        let mut builder = SSTableBuilder::new(1, dir_path.to_path_buf()).unwrap();

        // 写入1000个条目
        for i in 0..1000 {
            let key = format!("key_{:010}", i);
            let value = format!("value_{:010}", i);
            builder.write(key.as_bytes(), value.as_bytes());
        }

        builder.finish();

        let sst_path = dir_path.join("000001.sst");
        assert!(sst_path.exists());

        // 验证文件较大
        let metadata = std::fs::metadata(&sst_path).unwrap();
        // 1000个条目，每个约20字节，加上BloomFilter、IndexBlock和Footer
        // 估算：1000 * 20 + 1024(bloom) + 200(index) + 48(footer) ≈ 22KB
        assert!(metadata.len() > 20000); // 应该大于20KB
    }

    /// 测试BloomFilter的正确性（间接测试）
    #[test]
    fn test_sstable_builder_bloom_filter() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        let mut builder = SSTableBuilder::new(1, dir_path.to_path_buf()).unwrap();

        // 写入一些已知的key
        for i in 0..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            builder.write(key.as_bytes(), value.as_bytes());
        }

        builder.finish();

        let sst_path = dir_path.join("000001.sst");
        assert!(sst_path.exists());

        // 读取文件并验证BloomFilter存在
        let mut file = File::open(&sst_path).unwrap();
        let _file_size = file.metadata().unwrap().len();

        // 读取Footer获取bloom_offset
        let mut footer = vec![0u8; FOOTER_SIZE];
        file.seek(std::io::SeekFrom::End(-(FOOTER_SIZE as i64)))
            .unwrap();
        file.read_exact(&mut footer).unwrap();

        let mut cursor = 0;
        let (bloom_offset, bloom_bytes_len) =
            varint::decode_with_length(&footer[cursor..]).unwrap();
        cursor += bloom_bytes_len;

        let (index_offset, _) = varint::decode_with_length(&footer[cursor..]).unwrap();

        // 验证BloomFilter在IndexBlock之前
        assert!(bloom_offset < index_offset);

        // 验证BloomFilter有合理的大小（至少有数据）
        let bloom_size = index_offset - bloom_offset;
        assert!(bloom_size > 0, "BloomFilter should have data");
    }

    /// 测试索引块的正确性（间接测试）
    #[test]
    fn test_sstable_builder_index_block() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path();

        let mut builder = SSTableBuilder::new(1, dir_path.to_path_buf()).unwrap();

        // 写入足够多的数据以创建多个数据块
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{:03}", i);
            builder.write(key.as_bytes(), value.as_bytes());
        }

        builder.finish();

        let sst_path = dir_path.join("000001.sst");
        assert!(sst_path.exists());

        // 读取文件并验证IndexBlock存在
        let mut file = File::open(&sst_path).unwrap();
        let file_size = file.metadata().unwrap().len();

        // 读取Footer获取index_offset
        let mut footer = vec![0u8; FOOTER_SIZE];
        file.seek(std::io::SeekFrom::End(-(FOOTER_SIZE as i64)))
            .unwrap();
        file.read_exact(&mut footer).unwrap();

        let mut cursor = 0;
        let (bloom_offset, bloom_bytes_len) =
            varint::decode_with_length(&footer[cursor..]).unwrap();
        cursor += bloom_bytes_len;

        let (index_offset, _) = varint::decode_with_length(&footer[cursor..]).unwrap();

        // 验证IndexBlock在文件末尾（在Footer之前）
        assert!(index_offset < file_size - FOOTER_SIZE as u64);

        // 验证IndexBlock在BloomFilter之后
        assert!(index_offset > bloom_offset);

        // IndexBlock应该有合理的大小
        let index_size = file_size - FOOTER_SIZE as u64 - index_offset;
        assert!(index_size > 0, "IndexBlock should have data");
    }
}
