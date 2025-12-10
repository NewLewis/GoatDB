use std::fs::{ File, OpenOptions };
use std::io::{ self, BufWriter, Write };
use std::path::{ Path, PathBuf };

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

    pub fn write(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.writer.write_all(&(key.len() as u32).to_le_bytes())?;
        self.writer.write_all(key)?;
        
        self.writer.write_all(&(value.len() as u32).to_le_bytes())?;
        self.writer.write_all(value)?;

        // 如果追求极致性能，可以积累一定量再 flush，但为了数据安全（Durability），
        // 简单的 KV 数据库通常每次写完都要 flush。
        self.writer.flush()?;
        
        Ok(())
    }
}