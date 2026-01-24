use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use super::block_builder::BlockBuilder;
use super::bloom::BloomBuilder;
use crate::goatkv::encoding::coding;
use crate::goatkv::encoding::internal_key::InternalKey;
use crate::goatkv::metadata::file_metadata::TableProperties;
use crate::goatkv::utils::paths::SstablePaths;

/// SSTable文件的魔数（Magic Number）
/// 用于标识文件格式，固定值为 0x706A725F676F6174
/// 对应的ASCII字符串为 "pjr_goat"（反向）
const MAGIC_NUMBER: u64 = 0x706A725F676F6174;

/// SSTableBuilder用于构建SSTable文件
///
/// # SSTable文件结构
/// SSTable（Sorted String Table）是一种有序的、不可变的键值存储格式，
/// 适用于大规模数据的持久化和高效查询：
///
/// ```text
/// +------------------+
/// |  Data Block 0    |  数据块:存储实际key-value对
/// +------------------+
/// |  Data Block 1    |  使用前缀压缩和重启点
/// +------------------+
/// |  ...             |
/// +------------------+
/// |  Data Block N    |
/// +------------------+
/// |  Bloom Filter    |  布隆过滤器:快速过滤不存在的key
/// +------------------+
/// |  Index Block     |  索引块:记录每个数据块的位置和大小
/// +------------------+
/// |  Footer          |  文件尾:记录BloomFilter和IndexBlock的偏移量
/// +------------------+
/// ```
///
/// # Footer结构（48字节）
/// Footer位于文件末尾，用于定位各个组件的位置：
/// ```text
/// +------------------+
/// |  Bloom Offset    |  varint编码的BloomFilter起始偏移量
/// +------------------+
/// |  Index Offset    |  varint编码的IndexBlock起始偏移量
/// +------------------+
/// |  Padding         |  0字节填充,使Footer总大小为48字节
/// +------------------+
/// |  Magic Number    |  8字节魔数,标识文件格式
/// +------------------+
/// ```
///
/// # 索引块结构
/// 索引块用于快速定位包含特定key的数据块：
/// ```text
/// Index Entry格式:
/// [separator (varint-encoded key)] -> [block_offset (varint), block_size (varint)]
///
/// separator是一个特殊计算的key,表示该数据块中最大的key
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
/// ```no_run
/// # use goat_db::goatkv::core::KvEngine;
/// # use goat_db::goatkv::storage::sstable::SSTableBuilder;
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let (_wal_paths, sstable_paths, _manifest_paths) = KvEngine::init_db_paths("./data")?;
/// let mut builder = SSTableBuilder::new(1, &sstable_paths)?;
/// builder.write(b"apple", b"fruit");
/// builder.write(b"banana", b"fruit");
/// builder.finish();
/// # Ok(())
/// # }
/// ```
pub struct SSTableBuilder {
    /// 带缓冲的文件写入器
    /// 使用BufWriter提高写入性能
    writer: Option<io::BufWriter<File>>,

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
    #[allow(dead_code)] // 保留用于调试/日志
    id: u64,

    /// 最小的键（第一个写入的键）
    smallest_key: Option<Vec<u8>>,
    /// 最大的键（最后一个写入的键）
    largest_key: Option<Vec<u8>>,

    /// 临时文件路径（写入完成后重命名）
    tmp_path: PathBuf,
    /// 最终文件路径
    final_path: PathBuf,
}

impl SSTableBuilder {
    /// 创建一个新的SSTableBuilder
    ///
    /// # 参数
    /// - `id`: SSTable的唯一标识符
    ///
    /// # 文件命名规则（由统一命名规则管理）
    /// - 如果 file_id < 1,000,000：格式为 `{file_id:06}.sst`（如 000001.sst）
    /// - 如果 file_id >= 1,000,000：格式为 `{file_id}.sst`（如 1234567.sst）
    ///
    /// # 错误
    /// 返回io::Error，如果文件创建失败
    ///
    /// # 注意
    /// 需要显式传入 SSTablePaths
    ///
    /// # 示例
    /// ```no_run
    /// # use goat_db::goatkv::storage::sstable::SSTableBuilder;
    /// # use goat_db::goatkv::core::KvEngine;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let (_wal_paths, sstable_paths, _manifest_paths) =
    ///     goat_db::goatkv::core::KvEngine::init_db_paths("./data")?;
    /// let builder = SSTableBuilder::new(1, &sstable_paths)?;
    /// // 创建文件 ./data/000001.sst
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(id: u64, sstable_paths: &SstablePaths) -> io::Result<Self> {
        Self::new_with_manager(id, sstable_paths)
    }

    /// 创建一个新的SSTableBuilder，使用指定的 SSTablePaths
    ///
    /// # 参数
    /// - `id`: SSTable的唯一标识符
    /// - `sstable_paths`: SSTable 路径集合
    ///
    /// # 错误
    /// 返回io::Error，如果文件创建失败
    ///
    /// # 注意
    /// 此方法主要用于测试，允许使用临时 SSTablePaths
    pub fn new_with_manager(id: u64, sstable_paths: &SstablePaths) -> io::Result<Self> {
        let sstable_path = sstable_paths.sstable_path_by_id(id);
        let tmp_path = sstable_paths.tmp_path(format!("sstable_{:06}.tmp", id));

        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;

        Ok(Self {
            writer: Some(io::BufWriter::new(file)),
            data_block_builder: BlockBuilder::new(),
            index_block_builder: BlockBuilder::new(),
            bloom_builder: BloomBuilder::new(),
            offset: 0,
            id,

            smallest_key: None,
            largest_key: None,
            tmp_path,
            final_path: sstable_path,
        })
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
    /// ```no_run
    /// # use goat_db::goatkv::core::KvEngine;
    /// # use goat_db::goatkv::storage::sstable::SSTableBuilder;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let (_wal_paths, sstable_paths, _manifest_paths) = KvEngine::init_db_paths("./data")?;
    /// let mut builder = SSTableBuilder::new(1, &sstable_paths)?;
    /// builder.write(b"apple", b"fruit");
    /// builder.write(b"banana", b"fruit");
    /// builder.write(b"cherry", b"fruit");
    /// # Ok(())
    /// # }
    /// ```
    pub fn write(&mut self, key: &[u8], value: &[u8]) {
        // 更新 smallest_key 和 largest_key
        if self.smallest_key.is_none() {
            self.smallest_key = Some(key.to_vec());
        }
        self.largest_key = Some(key.to_vec());

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
        debug_assert!(key.len() >= 8);
        let user_key = &key[..key.len() - 8];
        self.bloom_builder.add(user_key);
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
        // 只用userkey来计算
        let separator_user_key =
            Self::compute_separator(&last_key[0..last_key.len() - 8], &key[0..key.len() - 8]);
        let separator = InternalKey::new_separator(separator_user_key);

        // 将separator和数据块信息添加到索引块
        // 索引格式：separator -> (block_offset, block_size)
        let mut separator_val = Vec::new();
        coding::put_varint64(&mut separator_val, self.offset);
        coding::put_varint64(&mut separator_val, block_content.len() as u64);
        self.index_block_builder
            .add(&separator.serialize(), &separator_val);

        // 将数据块写入文件
        self.writer
            .as_mut()
            .expect("SSTable writer missing")
            .write_all(block_content)
            .unwrap();
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
    /// ```text
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
    /// # 返回值
    /// 返回 `FileMetadata`，包含文件的完整元数据：
    /// - file_id: SSTable 的唯一标识符
    /// - file_size: 文件总大小
    /// - path: SSTable 文件路径
    /// - smallest_key/largest_key: 文件中的最小/最大键
    ///
    /// # 示例
    /// ```no_run
    /// # use goat_db::goatkv::core::KvEngine;
    /// # use goat_db::goatkv::storage::sstable::SSTableBuilder;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let (_wal_paths, sstable_paths, _manifest_paths) = KvEngine::init_db_paths("./data")?;
    /// let mut builder = SSTableBuilder::new(1, &sstable_paths)?;
    /// builder.write(b"key1", b"value1");
    /// builder.write(b"key2", b"value2");
    /// let metadata = builder.finish()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn finish(&mut self) -> io::Result<TableProperties> {
        // 如果有未完成的数据块，先完成它
        if !self.data_block_builder.empty() {
            let (block_content, last_key) = self.data_block_builder.finish();

            // 将最后一个数据块的信息添加到索引
            // 注意：这里使用last_key作为separator（因为这是最后一个数据块）
            let mut separator_val = Vec::new();
            coding::put_varint64(&mut separator_val, self.offset);
            coding::put_varint64(&mut separator_val, block_content.len() as u64);
            self.index_block_builder.add(last_key, &separator_val);

            // 写入最后一个数据块
            self.writer
                .as_mut()
                .expect("SSTable writer missing")
                .write_all(block_content)
                .unwrap();
            self.offset += block_content.len() as u64;

            // 重置数据块构建器
            self.data_block_builder.reset();
        }

        // 写入BloomFilter
        // BloomFilter用于快速过滤不存在的key
        self.writer
            .as_mut()
            .expect("SSTable writer missing")
            .write_all(self.bloom_builder.bitmap())
            .unwrap();
        let bloom_offset = self.offset;
        self.offset += self.bloom_builder.bitmap().len() as u64;

        // 写入IndexBlock
        // IndexBlock包含所有数据块的位置和大小信息
        let (block_content, _) = self.index_block_builder.finish();
        self.writer
            .as_mut()
            .expect("SSTable writer missing")
            .write_all(block_content)
            .unwrap();
        let index_offset = self.offset;
        self.offset += block_content.len() as u64;

        // 写入Footer
        // Footer包含BloomFilter和IndexBlock的偏移量，用于读取时定位
        let bloom_offset_bytes = coding::encode_varint64(bloom_offset);
        let bloom_offset_len = bloom_offset_bytes.len();

        let index_offset_bytes = coding::encode_varint64(index_offset);
        let index_offset_len = index_offset_bytes.len();

        // 写入两个偏移量
        self.writer
            .as_mut()
            .expect("SSTable writer missing")
            .write_all(&bloom_offset_bytes)
            .unwrap();
        self.writer
            .as_mut()
            .expect("SSTable writer missing")
            .write_all(&index_offset_bytes)
            .unwrap();

        // 填充0字节，使Footer总大小为48字节
        // Padding = 48 - (bloom_offset_len + index_offset_len + magic_len)
        // magic_len固定为8字节
        let padding = vec![0; 40 - (bloom_offset_len + index_offset_len)];
        self.writer
            .as_mut()
            .expect("SSTable writer missing")
            .write_all(&padding)
            .unwrap();

        // 写入魔数（8字节），标识文件格式
        self.writer
            .as_mut()
            .expect("SSTable writer missing")
            .write_all(&MAGIC_NUMBER.to_le_bytes())
            .unwrap();

        // 更新 offset 以反映 Footer 的大小
        // Footer 总大小为 48 字节
        self.offset += 48;

        // 刷新缓冲区，确保所有数据写入磁盘
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| io::Error::other("SSTable writer missing"))?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop(writer);

        std::fs::rename(&self.tmp_path, &self.final_path)?;
        sync_dir(
            self.final_path
                .parent()
                .unwrap_or_else(|| std::path::Path::new(".")),
        )?;

        // 创建并返回 FileMetadata
        // 注意：smallest_key 和 largest_key 应该是 InternalKey，但为了简化，这里直接存储原始 key
        // 序列号信息可以从 InternalKey 中解析出来（如果需要）
        // 路径可以根据 file_id 和数据目录由调用方生成
        let file_metadata = TableProperties {
            file_size: self.offset, // offset 现在是完整的文件大小
            smallest_key: self.smallest_key.clone().unwrap_or_default(),
            largest_key: self.largest_key.clone().unwrap_or_default(),
            smallest_seqno: 0, // TODO: 从 InternalKey 中解析
            largest_seqno: 0,  // TODO: 从 InternalKey 中解析
        };

        Ok(file_metadata)
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
    /// ```text
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

fn sync_dir(path: &Path) -> io::Result<()> {
    let dir = File::open(path)?;
    dir.sync_all()
}
