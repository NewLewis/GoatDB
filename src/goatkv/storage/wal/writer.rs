use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use bytes::Bytes;

use crate::goatkv::format::internal_key::InternalKey;
use crate::goatkv::{Error as GoatError, Result as GoatResult};

use super::codec::WalCodec;

/// WAL 写入配置
#[derive(Debug, Clone)]
pub struct WalWriterConfig {
    /// 是否启用同步写入（fsync）
    pub wal_sync: bool,
}

impl Default for WalWriterConfig {
    fn default() -> Self {
        Self { wal_sync: true }
    }
}

#[derive(Debug)]
struct WalWriterState {
    writer: io::BufWriter<File>,
    closed: bool,
}

/// WAL 写入器（业务线程内联写入版本）
///
/// 与旧实现不同：不再使用后台写线程，调用方线程直接执行 WAL 写入。
#[derive(Debug)]
pub struct WalWriter {
    state: Mutex<WalWriterState>,
    config: WalWriterConfig,
}

impl WalWriter {
    /// 创建新的 WAL 写入器
    pub fn new(path: PathBuf, config: WalWriterConfig) -> GoatResult<Self> {
        let writer = Self::open_writer(path)?;
        Ok(Self {
            state: Mutex::new(WalWriterState {
                writer,
                closed: false,
            }),
            config,
        })
    }

    /// 追加单个键值对到 WAL
    pub fn append(&self, key: &InternalKey, value: &[u8]) -> GoatResult<()> {
        let record = WalCodec::encode_record(key, value);
        self.append_raw(&record)
    }

    /// 批量追加键值对到 WAL
    pub fn append_batch(&self, records: &[(InternalKey, Bytes)]) -> GoatResult<()> {
        if records.is_empty() {
            return Ok(());
        }

        let mut encoded = Vec::new();
        for (key, value) in records {
            WalCodec::encode_record_into(&mut encoded, key, value.as_ref());
        }
        self.append_raw(&encoded)
    }

    /// 轮换 WAL 文件（切换到新文件）
    pub fn rotate(&self, new_path: PathBuf) -> GoatResult<()> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(GoatError::unavailable("wal_writer", "WAL writer closed"));
        }

        Self::flush_writer(&mut state.writer)?;
        if self.config.wal_sync {
            Self::sync_writer(&mut state.writer)?;
        }

        state.writer = Self::open_writer(new_path)?;
        Ok(())
    }

    fn append_raw(&self, data: &[u8]) -> GoatResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(GoatError::unavailable("wal_writer", "WAL writer closed"));
        }

        Self::write_bytes(&mut state.writer, data)?;
        // Keep old semantics where records are flushed from user-space buffer
        // promptly, while fsync remains controlled by wal_sync.
        Self::flush_writer(&mut state.writer)?;
        if self.config.wal_sync {
            Self::sync_writer(&mut state.writer)?;
        }
        Ok(())
    }

    fn open_writer(file_path: PathBuf) -> GoatResult<io::BufWriter<File>> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
            .map_err(|e| GoatError::io("wal_open_for_append", e))?;
        Ok(io::BufWriter::new(file))
    }

    fn write_bytes(writer: &mut io::BufWriter<File>, data: &[u8]) -> GoatResult<()> {
        writer
            .write_all(data)
            .map_err(|e| GoatError::io("wal_write_bytes", e))
    }

    fn flush_writer(writer: &mut io::BufWriter<File>) -> GoatResult<()> {
        writer.flush().map_err(|e| GoatError::io("wal_flush", e))
    }

    fn sync_writer(writer: &mut io::BufWriter<File>) -> GoatResult<()> {
        writer
            .get_ref()
            .sync_data()
            .map_err(|e| GoatError::io("wal_sync_data", e))
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        let _ = Self::flush_writer(&mut state.writer);
        if self.config.wal_sync {
            let _ = Self::sync_writer(&mut state.writer);
        }
    }
}
