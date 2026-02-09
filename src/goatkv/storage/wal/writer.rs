use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;

use tracing::debug;

/// Write-ahead log writer.
#[derive(Debug)]
pub struct WalWriter {
    writer: io::BufWriter<File>,
    #[allow(dead_code)]
    file_path: PathBuf,
}

impl WalWriter {
    pub fn new(file_path: PathBuf) -> io::Result<Self> {
        debug!("new WalWriter");
        let open_path = file_path.clone();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(open_path)?;
        let writer = io::BufWriter::new(file);
        Ok(Self { writer, file_path })
    }

    pub fn write_bytes(&mut self, data: &[u8]) -> io::Result<()> {
        self.writer.write_all(data)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    pub fn sync_data(&mut self) -> io::Result<()> {
        self.writer.get_ref().sync_data()
    }
}
