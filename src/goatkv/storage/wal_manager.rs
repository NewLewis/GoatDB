use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::PathBuf;

use crc32fast::Hasher;

use crate::goatkv::encoding::internal_key::InternalKey;

/// Write-Ahead Log (WAL) 管理器
///
/// ## 文件格式
/// ```text
/// |                         Checksum (4 bytes)                       |
/// |                       u32, little-endian                         |
/// |               InternalKey Total Length (4 bytes)                 |
/// |                       u32, little-endian                         |
/// |                      User Key (variable)                         |
/// |              Encoded Sequence Number (8 bytes)                   |
/// |                       u64, little-endian                         |
/// |                      Value Length (4 bytes)                      |
/// |                       u32, little-endian                         |
/// |                      Value (variable)                            |
/// ```
///
/// ## 关键特性
/// 1. **Checksum (4 bytes)**: CRC32 校验和，用于数据完整性验证。
///     - 计算范围：Key Total Length + User Key + Encoded Sequence Number + Value Length + Value
///     - 每次写入前计算，读取时验证
/// 2. **InternalKey Total Length (4 bytes)**: InternalKey 总字节数
///     - 包括 User Key + Encoded Sequence Number
///     - 用于解析时确定 InternalKey 边界
/// 3. **User Key (variable)**: 用户原始键，可变长度
/// 4. **Encoded Sequence Number (8 bytes)**: 编码后的序列号（包含 Kind）
/// 5. **Value Length (4 bytes)**: 值的字节数
/// 6. **Value (variable)**: 原始值，可变长度
#[derive(Debug)]
pub struct WalManager {
    writer: io::BufWriter<File>,
}

impl WalManager {
    /// 创建新的 WAL 管理器，指定 WAL 文件路径
    pub fn new(file_path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)?;
        let writer = io::BufWriter::new(file);
        Ok(Self { writer })
    }

    /// 写入一个键值对到 WAL
    ///
    /// # 参数
    /// - `key`: 要写入的 InternalKey
    /// - `value`: 要写入的值
    ///
    /// # 返回
    /// - `Ok(())`: 写入成功
    /// - `Err(io::Error)`: 写入失败
    ///
    /// # 注意
    /// - 写入后立即调用 `flush()`，确保数据持久化
    /// - 对于 Delete 操作，value 可能为空字节数组
    pub fn write(&mut self, key: &InternalKey, value: &[u8]) -> io::Result<()> {
        let checksum =
            Self::get_checksum(key, key.serialized_size() as u32, value, value.len() as u32);

        // 写入校验和
        self.writer.write_all(&checksum.to_le_bytes())?;

        // 写入 InternalKey 总长度
        self.writer
            .write_all(&(key.serialized_size() as u32).to_le_bytes())?;

        // 写入用户键
        self.writer.write_all(key.user_key())?;

        // 写入编码后的序列号
        self.writer
            .write_all(&key.encoded_sequence_number().to_le_bytes())?;

        // 写入值长度
        self.writer.write_all(&(value.len() as u32).to_le_bytes())?;

        // 写入值
        self.writer.write_all(value)?;

        // 立即刷新，确保数据持久化
        self.writer.flush()
    }

    /// 计算 WAL 条目的 CRC32 校验和
    ///
    /// # 参数
    /// - `key`: InternalKey
    /// - `key_len`: InternalKey 总长度
    /// - `value`: 值字节数组
    /// - `value_len`: 值长度
    ///
    /// # 返回
    /// - `u32`: CRC32 校验和
    ///
    /// # 注意
    /// - 计算顺序必须与写入顺序完全一致
    /// - 用于写入时生成校验和和读取时验证数据完整性
    pub fn get_checksum(key: &InternalKey, key_len: u32, value: &[u8], value_len: u32) -> u32 {
        let mut hasher = Hasher::new();

        // 按照写入顺序更新哈希
        hasher.update(&key_len.to_le_bytes());
        hasher.update(key.user_key());
        hasher.update(&key.encoded_sequence_number().to_le_bytes());
        hasher.update(&value_len.to_le_bytes());
        hasher.update(value);

        hasher.finalize()
    }
}

/// WAL 迭代器，用于读取 WAL 文件
#[derive(Debug)]
pub struct WalIterator {
    reader: io::BufReader<File>,
}

impl WalIterator {
    /// 创建新的 WAL 迭代器，指定 WAL 文件路径
    pub fn new(file_path: &PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(file_path)?;
        let reader = io::BufReader::new(file);
        Ok(Self { reader })
    }
}

impl Iterator for WalIterator {
    type Item = io::Result<(InternalKey, Vec<u8>)>;

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
        let key_len = u32::from_le_bytes(key_len_bytes);

        // 读取用户键
        let user_key_len = key_len as usize - 8; // 减去 encoded_sequence_number 的 8 字节
        let mut user_key = vec![0u8; user_key_len];
        if let Err(e) = self.reader.read_exact(&mut user_key) {
            return Some(Err(e));
        }

        // 读取编码后的序列号
        let mut encoded_seq_bytes = [0u8; 8];
        if let Err(e) = self.reader.read_exact(&mut encoded_seq_bytes) {
            return Some(Err(e));
        }
        let encoded_seq = u64::from_le_bytes(encoded_seq_bytes);

        // 构造 InternalKey
        let key = InternalKey::from_encoded(user_key, encoded_seq);

        // 读取值长度
        let mut value_len_bytes = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut value_len_bytes) {
            return Some(Err(e));
        }
        let value_len = u32::from_le_bytes(value_len_bytes);

        // 读取值
        let mut value = vec![0u8; value_len as usize];
        if let Err(e) = self.reader.read_exact(&mut value) {
            return Some(Err(e));
        }

        // 验证校验和
        if WalManager::get_checksum(&key, key_len, &value, value_len) != checksum {
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Checksum mismatch for key: {}",
                    String::from_utf8_lossy(key.user_key())
                ),
            )));
        }

        Some(Ok((key, value)))
    }
}
