use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

use crc32fast::Hasher;

#[derive(Debug)]
pub struct WalManager {
    writer: BufWriter<File>,
}

impl WalManager {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    /// Writes a key-value pair to the WAL (Write-Ahead Log).
    /// The WAL format is as follows:
    /// ```
    /// +----------------+----------------+----------------+----------------+
    /// |   Checksum (4 bytes, u32, little-endian)                       |
    /// +----------------+----------------+----------------+----------------+
    /// |   Key Length (4 bytes, u32, little-endian)                    |
    /// +----------------+----------------+----------------+----------------+
    /// |   Key (N bytes, raw bytes)                                    |
    /// +----------------+----------------+----------------+----------------+
    /// |   Value Length (4 bytes, u32, little-endian)                  |
    /// +----------------+----------------+----------------+----------------+
    /// |   Value (M bytes, raw bytes)                                  |
    /// +----------------+----------------+----------------+----------------+
    /// ```
    /// After writing, the data is flushed to ensure durability.
    pub fn write(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let checksum = Self::get_checksum(key, value);

        self.writer.write_all(&checksum.to_le_bytes())?;

        self.writer.write_all(&(key.len() as u32).to_le_bytes())?;
        self.writer.write_all(key)?;

        self.writer.write_all(&(value.len() as u32).to_le_bytes())?;
        self.writer.write_all(value)?;

        // 如果追求极致性能，可以积累一定量再 flush，但为了数据安全（Durability），
        // 简单的 KV 数据库通常每次写完都要 flush。
        self.writer.flush()?;

        Ok(())
    }

    fn get_checksum(key: &[u8], value: &[u8]) -> u32 {
        let mut hasher = Hasher::new();

        hasher.update(&(key.len() as u32).to_le_bytes());
        hasher.update(key);
        hasher.update(&(value.len() as u32).to_le_bytes());
        hasher.update(value);

        return hasher.finalize();
    }
}

pub struct WalIterator {
    reader: BufReader<File>,
}

impl WalIterator {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            reader: BufReader::new(file),
        })
    }
}

impl Iterator for WalIterator {
    type Item = io::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut checksum_bytes = [0u8; 4];
        match self.reader.read_exact(&mut checksum_bytes) {
            Ok(_) => {}
            Err(e) => {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    return None;
                } else {
                    return Some(Err(e));
                }
            }
        }
        let checksum = u32::from_le_bytes(checksum_bytes);

        let mut key_len_bytes = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut key_len_bytes) {
            return Some(Err(e));
        }
        let key_len = u32::from_le_bytes(key_len_bytes) as usize;

        let mut key = vec![0u8; key_len];
        if let Err(e) = self.reader.read_exact(&mut key) {
            return Some(Err(e));
        }

        let mut value_len_bytes = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut value_len_bytes) {
            return Some(Err(e));
        }
        let value_len = u32::from_le_bytes(value_len_bytes) as usize;

        let mut value = vec![0u8; value_len];
        if let Err(e) = self.reader.read_exact(&mut value) {
            return Some(Err(e));
        }

        // 校验crc
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&key_len.to_le_bytes());
        hasher.update(&key);
        hasher.update(&value_len.to_le_bytes());
        hasher.update(&value);
        let calculated_checksum = hasher.finalize();
        if calculated_checksum != checksum {
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "CRC mismatch",
            )));
        }

        Some(Ok((key, value)))
    }
}
