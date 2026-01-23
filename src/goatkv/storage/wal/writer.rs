use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;

use crate::goatkv::encoding::internal_key::InternalKey;

use super::format::checksum_for;

/// Write-ahead log writer.
#[derive(Debug)]
pub struct WalWriter {
    writer: io::BufWriter<File>,
    wal_sync: bool,
    #[allow(dead_code)]
    file_path: PathBuf,
}

impl WalWriter {
    pub fn new(file_path: PathBuf, wal_sync: bool) -> io::Result<Self> {
        println!("new WalWriter, wal_sync: {}", wal_sync);
        let open_path = file_path.clone();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(open_path)?;
        let writer = io::BufWriter::new(file);
        Ok(Self {
            writer,
            wal_sync,
            file_path,
        })
    }

    pub fn append(&mut self, key: &InternalKey, value: &[u8]) -> io::Result<()> {
        let key_len = key.serialized_size() as u32;
        let value_len = value.len() as u32;
        let checksum = checksum_for(key, key_len, value, value_len);

        self.writer.write_all(&checksum.to_le_bytes())?;
        self.writer.write_all(&key_len.to_le_bytes())?;
        self.writer.write_all(key.user_key())?;
        self.writer
            .write_all(&key.encoded_sequence_number().to_le_bytes())?;
        self.writer.write_all(&value_len.to_le_bytes())?;
        self.writer.write_all(value)?;

        self.writer.flush()?;
        if self.wal_sync {
            self.writer.get_ref().sync_data()?;
        }
        Ok(())
    }

    pub fn write(&mut self, key: &InternalKey, value: &[u8]) -> io::Result<()> {
        self.append(key, value)
    }
}
