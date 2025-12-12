use std::fs::{ File, OpenOptions };
use std::io::{ self, BufWriter, Write };
use std::path::{ Path, PathBuf };

use crc32fast::Hasher;

pub struct WalManager {
    writer: BufWriter<File>,
}

impl WalManager {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

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