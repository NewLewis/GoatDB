use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
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
    /// 预分配步长（字节），0 表示禁用。
    pub wal_preallocate_bytes: u64,
    /// 累计写入达到该阈值后执行一次 sync_data（仅在 wal_sync=false 时生效）。
    /// 0 表示禁用周期 sync。
    pub wal_bytes_per_sync: u64,
}

impl Default for WalWriterConfig {
    fn default() -> Self {
        Self {
            wal_sync: true,
            wal_preallocate_bytes: 0,
            wal_bytes_per_sync: 0,
        }
    }
}

#[derive(Debug)]
struct WalWriterState {
    writer: io::BufWriter<File>,
    closed: bool,
    logical_size: u64,
    preallocated_size: u64,
    bytes_since_last_sync: u64,
    sync_calls: u64,
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
        let mut state = Self::open_state(path)?;
        if config.wal_preallocate_bytes > 0 {
            let preallocate_bytes = config.wal_preallocate_bytes;
            Self::ensure_preallocated(&mut state, 1, preallocate_bytes)?;
        }
        Ok(Self {
            state: Mutex::new(state),
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

        Self::finalize_active_file(&mut state, self.config.wal_sync)?;

        let mut next = Self::open_state(new_path)?;
        if self.config.wal_preallocate_bytes > 0 {
            let preallocate_bytes = self.config.wal_preallocate_bytes;
            Self::ensure_preallocated(&mut next, 1, preallocate_bytes)?;
        }
        *state = next;
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

        if self.config.wal_preallocate_bytes > 0 {
            Self::ensure_preallocated(
                &mut state,
                data.len() as u64,
                self.config.wal_preallocate_bytes,
            )?;
        }
        Self::write_bytes(&mut state.writer, data)?;
        state.logical_size = state.logical_size.saturating_add(data.len() as u64);
        state.bytes_since_last_sync = state
            .bytes_since_last_sync
            .saturating_add(data.len() as u64);
        // Keep old semantics where records are flushed from user-space buffer
        // promptly, while fsync remains controlled by wal_sync.
        Self::flush_writer(&mut state.writer)?;
        let should_sync = self.config.wal_sync
            || (self.config.wal_bytes_per_sync > 0
                && state.bytes_since_last_sync >= self.config.wal_bytes_per_sync);
        if should_sync {
            Self::sync_state(&mut state)?;
            state.bytes_since_last_sync = 0;
        }
        Ok(())
    }

    fn open_state(file_path: PathBuf) -> GoatResult<WalWriterState> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(file_path)
            .map_err(|e| GoatError::io("wal_open_for_rw", e))?;
        let logical_size = file
            .metadata()
            .map_err(|e| GoatError::io("wal_metadata", e))?
            .len();
        file.seek(SeekFrom::Start(logical_size))
            .map_err(|e| GoatError::io("wal_seek_to_end", e))?;
        Ok(WalWriterState {
            writer: io::BufWriter::new(file),
            closed: false,
            logical_size,
            preallocated_size: logical_size,
            bytes_since_last_sync: 0,
            sync_calls: 0,
        })
    }

    fn ensure_preallocated(
        state: &mut WalWriterState,
        incoming_bytes: u64,
        preallocate_bytes: u64,
    ) -> GoatResult<()> {
        if preallocate_bytes == 0 {
            return Ok(());
        }

        let required_size = state.logical_size.saturating_add(incoming_bytes);
        if required_size <= state.preallocated_size {
            return Ok(());
        }

        let rounded = required_size.div_ceil(preallocate_bytes) * preallocate_bytes;
        state
            .writer
            .get_ref()
            .set_len(rounded)
            .map_err(|e| GoatError::io("wal_preallocate_set_len", e))?;
        state.preallocated_size = rounded;
        Ok(())
    }

    fn truncate_to_logical_size(state: &mut WalWriterState) -> GoatResult<()> {
        if state.preallocated_size <= state.logical_size {
            return Ok(());
        }
        state
            .writer
            .get_ref()
            .set_len(state.logical_size)
            .map_err(|e| GoatError::io("wal_truncate_to_logical_size", e))?;
        state.preallocated_size = state.logical_size;
        Ok(())
    }

    fn finalize_active_file(state: &mut WalWriterState, wal_sync: bool) -> GoatResult<()> {
        Self::flush_writer(&mut state.writer)?;
        Self::truncate_to_logical_size(state)?;
        if wal_sync {
            Self::sync_state(state)?;
        }
        Ok(())
    }

    fn write_bytes(writer: &mut io::BufWriter<File>, data: &[u8]) -> GoatResult<()> {
        writer
            .write_all(data)
            .map_err(|e| GoatError::io("wal_write_bytes", e))
    }

    fn flush_writer(writer: &mut io::BufWriter<File>) -> GoatResult<()> {
        writer.flush().map_err(|e| GoatError::io("wal_flush", e))
    }

    fn sync_state(state: &mut WalWriterState) -> GoatResult<()> {
        state
            .writer
            .get_ref()
            .sync_data()
            .map_err(|e| GoatError::io("wal_sync_data", e))?;
        state.sync_calls = state.sync_calls.saturating_add(1);
        Ok(())
    }

    #[cfg(test)]
    pub fn sync_calls_for_test(&self) -> u64 {
        self.state.lock().unwrap().sync_calls
    }

    #[cfg(test)]
    pub fn logical_size_for_test(&self) -> u64 {
        self.state.lock().unwrap().logical_size
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        let _ = Self::finalize_active_file(&mut state, self.config.wal_sync);
    }
}
