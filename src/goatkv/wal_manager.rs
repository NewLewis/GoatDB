use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

use crc32fast::Hasher;

use crate::goatkv::internal_key::InternalKey;

/// WAL（Write-Ahead Log）管理器，负责将数据操作持久化到日志文件。
///
/// WAL 是 LSM-Tree 架构中的关键组件，确保在数据写入内存表（MemTable）之前，
/// 操作已经持久化到磁盘。这样即使进程崩溃，也能通过重放 WAL 恢复数据。
///
/// ## WAL 文件格式
///
/// 每个 WAL 条目（Entry）的二进制格式如下：
///
/// ```text
/// +----------------+----------------+----------------+----------------+
/// |                         Checksum (4 bytes)                       |
/// |                       u32, little-endian                         |
/// +----------------+----------------+----------------+----------------+
/// |                    Key Total Length (4 bytes)                    |
/// |                       u32, little-endian                         |
/// +----------------+----------------+----------------+----------------+
/// |                     User Key (variable length)                   |
/// |                   长度 = Key Total Length - 8                    |
/// +----------------+----------------+----------------+----------------+
/// |               Encoded Sequence Number (8 bytes)                  |
/// |                       u64, little-endian                         |
/// +----------------+----------------+----------------+----------------+
/// |                    Value Length (4 bytes)                        |
/// |                       u32, little-endian                         |
/// +----------------+----------------+----------------+----------------+
/// |                      Value (variable length)                     |
/// +----------------+----------------+----------------+----------------+
/// ```
///
/// ### 字段详解：
///
/// 1. **Checksum (4 bytes)**: CRC32 校验和，用于数据完整性验证。
///     - 计算范围：Key Total Length + User Key + Encoded Sequence Number + Value Length + Value
///     - 使用 `crc32fast` 库计算
///
/// 2. **Key Total Length (4 bytes)**: InternalKey 的总字节数。
///     - 计算公式：`user_key.len() + 8` （8 字节用于 encoded_sequence_number）
///     - 类型：u32, little-endian
///
/// 3. **User Key (variable length)**: 用户原始键的字节数组。
///     - 长度：Key Total Length - 8
///     - 直接存储原始字节，不进行额外编码
///
/// 4. **Encoded Sequence Number (8 bytes)**: 编码后的序列号和操作类型。
///     - 格式：`(sequence_number << 8) | kind_byte`
///     - sequence_number: 56位（7字节），最大值为 2^56-1 ≈ 7.2e16
///     - kind_byte: 8位（1字节），0 = Put, 1 = Delete
///     - 类型：u64, little-endian
///
/// 5. **Value Length (4 bytes)**: 值的字节长度。
///     - 类型：u32, little-endian
///     - 对于 Delete 操作，value 长度可能为 0
///
/// 6. **Value (variable length)**: 值的字节数组。
///     - 长度由 Value Length 指定
///     - 对于 Delete 操作，可能为空字节数组
///
/// ### 数据恢复流程：
///
/// 1. 打开 WAL 文件，按上述格式顺序读取
/// 2. 对每个条目验证 Checksum，确保数据完整性
/// 3. 构造 InternalKey 和 Value，插入到内存表
/// 4. 遇到文件结束或校验失败时停止
///
/// ### 性能考虑：
///
/// - 每次写入后都调用 `flush()` 确保数据持久化，牺牲部分性能换取数据安全
/// - 使用缓冲写入（BufWriter）减少系统调用次数
/// - CRC32 校验提供快速的数据完整性检查
#[derive(Debug)]
pub struct WalManager {
    writer: BufWriter<File>,
}

impl WalManager {
    /// 创建或打开 WAL 文件。
    ///
    /// # 参数
    /// - `path`: WAL 文件路径
    ///
    /// # 返回
    /// - `Ok(WalManager)`: 成功打开文件
    /// - `Err(io::Error)`: 文件操作失败
    ///
    /// # 注意
    /// - 使用 `OpenOptions::new().create(true).append(true)` 模式
    /// - 如果文件不存在则创建，存在则追加写入
    /// - 不截断文件，保留历史日志
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    /// 将键值对写入 WAL（Write-Ahead Log）。
    ///
    /// # 参数
    /// - `key`: InternalKey，包含用户键、序列号和操作类型
    /// - `value`: 值的字节数组
    ///
    /// # 返回
    /// - `Ok(())`: 写入成功
    /// - `Err(io::Error)`: 写入或刷新失败
    ///
    /// # 写入流程
    /// 1. 计算整个条目的 CRC32 校验和
    /// 2. 按 WAL 格式顺序写入所有字段
    /// 3. 刷新缓冲区确保数据持久化
    ///
    /// # 注意
    /// - 写入后立即调用 `flush()`，确保数据持久化
    /// - 对于 Delete 操作，value 可能为空字节数组
    /// - 包含调试输出（仅在调试编译时生效）
    pub fn write(&mut self, key: &InternalKey, value: &[u8]) -> io::Result<()> {
        let checksum =
            Self::get_checksum(key, key.serialized_size() as u32, value, value.len() as u32);

        // 调试输出，帮助理解写入的内容
        println!(
            "key: {}, seq: {}, kind: {}, checksum: {}",
            String::from_utf8_lossy(key.user_key()),
            key.sequence_number(),
            key.kind(),
            checksum
        );

        // 写入校验和
        self.writer.write_all(&checksum.to_le_bytes())?;

        // 写入 InternalKey 总长度
        self.writer
            .write_all(&(key.serialized_size() as u32).to_le_bytes())?;

        // 写入用户键
        self.writer.write_all(key.user_key())?;

        // 写入编码后的序列号（包含操作类型）
        self.writer
            .write_all(&key.encoded_sequence_number().to_le_bytes())?;

        // 写入值长度
        self.writer.write_all(&(value.len() as u32).to_le_bytes())?;

        // 写入值
        self.writer.write_all(value)?;

        // 刷新缓冲区，确保数据持久化到磁盘
        // 注意：这会降低性能，但保证了数据安全（Durability）
        // 在实际生产环境中，可以考虑批量刷新或使用更复杂的持久化策略
        self.writer.flush()?;

        Ok(())
    }

    /// 计算 WAL 条目的 CRC32 校验和。
    ///
    /// # 参数
    /// - `key`: InternalKey
    /// - `key_len`: InternalKey 的总字节数（user_key.len() + 8）
    /// - `value`: 值的字节数组
    /// - `value_len`: 值的字节长度
    ///
    /// # 返回
    /// - `u32`: CRC32 校验和
    ///
    /// # 计算范围
    /// 校验和计算包括以下数据（按顺序）：
    /// 1. key_len (4 bytes, u32, little-endian)
    /// 2. user_key (key_len - 8 bytes)
    /// 3. encoded_sequence_number (8 bytes, u64, little-endian)
    /// 4. value_len (4 bytes, u32, little-endian)
    /// 5. value (value_len bytes)
    ///
    /// # 注意
    /// - 使用 `crc32fast` 库，性能较好
    /// - 计算顺序必须与写入顺序完全一致
    /// - 用于写入时生成校验和和读取时验证数据完整性
    pub fn get_checksum(key: &InternalKey, key_len: u32, value: &[u8], value_len: u32) -> u32 {
        let mut hasher = Hasher::new();

        // 注意：更新顺序必须与写入顺序完全一致
        hasher.update(&key_len.to_le_bytes());
        hasher.update(key.user_key());
        hasher.update(&key.encoded_sequence_number().to_le_bytes());
        hasher.update(&value_len.to_le_bytes());
        hasher.update(value);

        return hasher.finalize();
    }
}

/// WAL 迭代器，用于从 WAL 文件中顺序读取条目。
///
/// 主要用于数据库启动时的数据恢复（Replay）。
///
/// # 特性
/// - 顺序读取，不支持随机访问
/// - 自动验证每个条目的校验和
/// - 遇到文件结束或数据损坏时优雅停止
///
/// # 错误处理
/// - 校验和失败：返回 `InvalidData` 错误
/// - 文件格式损坏：返回相应的 IO 错误
/// - 正常文件结束：返回 `None`
#[derive(Debug)]
pub struct WalIterator {
    reader: BufReader<File>,
}

impl WalIterator {
    /// 创建 WAL 迭代器，打开指定文件用于读取。
    ///
    /// # 参数
    /// - `path`: WAL 文件路径
    ///
    /// # 返回
    /// - `Ok(WalIterator)`: 成功打开文件
    /// - `Err(io::Error)`: 文件打开失败
    ///
    /// # 注意
    /// - 使用只读模式打开文件
    /// - 文件不存在会返回错误
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            reader: BufReader::new(file),
        })
    }
}

impl Iterator for WalIterator {
    type Item = io::Result<(InternalKey, Vec<u8>)>;

    /// 读取下一个 WAL 条目。
    ///
    /// # 返回
    /// - `Some(Ok((key, value)))`: 成功读取一个条目
    /// - `Some(Err(e))`: 读取过程中发生错误（校验失败、格式错误等）
    /// - `None`: 已到达文件末尾
    ///
    /// # 读取流程
    /// 1. 读取校验和（4字节）
    /// 2. 读取 Key Total Length（4字节）
    /// 3. 读取 User Key（key_len - 8 字节）
    /// 4. 读取 Encoded Sequence Number（8字节）
    /// 5. 构造 InternalKey
    /// 6. 读取 Value Length（4字节）
    /// 7. 读取 Value（value_len 字节）
    /// 8. 验证校验和
    /// 9. 返回 (InternalKey, Value)
    ///
    /// # 错误处理
    /// - 文件结束（UnexpectedEof）：返回 `None`，迭代结束
    /// - 校验和失败：返回 `InvalidData` 错误
    /// - 其他 IO 错误：直接返回错误
    fn next(&mut self) -> Option<Self::Item> {
        // 读取校验和
        let mut checksum_bytes = [0u8; 4];
        match self.reader.read_exact(&mut checksum_bytes) {
            Ok(_) => {}
            Err(e) => {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    // 正常文件结束
                    return None;
                } else {
                    // 其他读取错误
                    return Some(Err(e));
                }
            }
        }
        let checksum = u32::from_le_bytes(checksum_bytes);

        // 读取 InternalKey 总长度
        let mut key_len_bytes = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut key_len_bytes) {
            return Some(Err(e));
        }
        let key_len = u32::from_le_bytes(key_len_bytes) as usize;

        // 读取 User Key（总长度减去8字节的encoded_sequence_number）
        let user_key_len = key_len - 8;
        let mut user_key = vec![0u8; user_key_len];
        if let Err(e) = self.reader.read_exact(&mut user_key) {
            return Some(Err(e));
        }

        // 读取 Encoded Sequence Number
        let mut encoded_sequence_number_bytes = [0u8; 8];
        if let Err(e) = self.reader.read_exact(&mut encoded_sequence_number_bytes) {
            return Some(Err(e));
        }
        let encoded_sequence_number = u64::from_le_bytes(encoded_sequence_number_bytes);

        // 构造 InternalKey
        let key = InternalKey::from_encoded(user_key, encoded_sequence_number);

        // 读取 Value 长度
        let mut value_len_bytes = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut value_len_bytes) {
            return Some(Err(e));
        }
        let value_len = u32::from_le_bytes(value_len_bytes) as usize;

        // 读取 Value
        let mut value = vec![0u8; value_len];
        if let Err(e) = self.reader.read_exact(&mut value) {
            return Some(Err(e));
        }

        // 验证校验和
        if WalManager::get_checksum(&key, key_len as u32, &value, value_len as u32) != checksum {
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "CRC mismatch",
            )));
        }

        Some(Ok((key, value)))
    }
}
