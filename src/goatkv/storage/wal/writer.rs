use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;

use tracing::debug;

use crate::goatkv::error::{Error as GoatError, Result as GoatResult};

/// Write-ahead log writer.
#[derive(Debug)]
pub struct WalWriter {
    writer: io::BufWriter<File>,
    #[allow(dead_code)]
    file_path: PathBuf,
}

impl WalWriter {
    pub fn new(file_path: PathBuf) -> GoatResult<Self> {
        debug!("new WalWriter");
        let open_path = file_path.clone();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(open_path)
            .map_err(|e| GoatError::io("wal_open_for_append", e))?;
        let writer = io::BufWriter::new(file);
        Ok(Self { writer, file_path })
    }

    pub fn write_bytes(&mut self, data: &[u8]) -> GoatResult<()> {
        self.writer
            .write_all(data)
            .map_err(|e| GoatError::io("wal_write_bytes", e))
    }

    pub fn flush(&mut self) -> GoatResult<()> {
        self.writer
            .flush()
            .map_err(|e| GoatError::io("wal_flush", e))
    }

    pub fn sync_data(&mut self) -> GoatResult<()> {
        self.writer
            .get_ref()
            .sync_data()
            .map_err(|e| GoatError::io("wal_sync_data", e))
    }
}
