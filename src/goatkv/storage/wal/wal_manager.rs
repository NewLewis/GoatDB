use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::goatkv::format::internal_key::InternalKey;
use bytes::Bytes;

use super::format::checksum_for;
use super::writer::WalWriter;

#[derive(Debug, Clone)]
pub struct WalManagerConfig {
    pub wal_sync: bool,
    pub sync_interval_ms: u64,
    pub sync_bytes: usize,
    pub max_buffer_bytes: usize,
}

impl Default for WalManagerConfig {
    fn default() -> Self {
        Self {
            wal_sync: true,
            sync_interval_ms: 10,
            sync_bytes: 1024 * 1024,
            max_buffer_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug)]
struct WalState {
    segments: Vec<WalSegment>,
    buffered_bytes: usize,
    buffer_lsn_start: u64,
    next_lsn: u64,
    durable_lsn: u64,
    sync_requested_lsn: u64,
    closed: bool,
    rotate_pending: Option<PathBuf>,
    rotate_requested: u64,
    rotate_completed: u64,
    rotate_error: Option<String>,
}

#[derive(Debug)]
struct WalShards {
    // Per-shard buffers reduce contention on the async WAL path.
    buffers: Vec<Mutex<Vec<u8>>>,
    // Aggregate buffered size provides backpressure without locking each shard.
    buffered_bytes: AtomicUsize,
}

#[derive(Debug)]
struct WalSegment {
    header1: [u8; 8],
    header2: [u8; 12],
    key: Bytes,
    value: Bytes,
    len: usize,
}

impl WalSegment {
    fn new(key: &InternalKey, value: Bytes) -> Self {
        let key_len = key.serialized_size() as u32;
        let value_len = value.len() as u32;
        let checksum = checksum_for(key, key_len, value.as_ref(), value_len);
        let mut header1 = [0u8; 8];
        header1[..4].copy_from_slice(&checksum.to_le_bytes());
        header1[4..].copy_from_slice(&key_len.to_le_bytes());
        let mut header2 = [0u8; 12];
        header2[..8].copy_from_slice(&key.encoded_sequence_number().to_le_bytes());
        header2[8..].copy_from_slice(&value_len.to_le_bytes());
        let len = 12 + key_len as usize + value_len as usize;
        Self {
            header1,
            header2,
            key: key.user_key_bytes(),
            value,
            len,
        }
    }
}

impl WalShards {
    fn new(shard_count: usize) -> Self {
        let mut buffers = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            buffers.push(Mutex::new(Vec::new()));
        }
        Self {
            buffers,
            buffered_bytes: AtomicUsize::new(0),
        }
    }

    fn shard_index(&self) -> usize {
        // Use the current thread id to keep a stable shard per writer thread.
        let mut hasher = DefaultHasher::new();
        thread::current().id().hash(&mut hasher);
        (hasher.finish() as usize) % self.buffers.len()
    }

    fn buffered(&self) -> usize {
        self.buffered_bytes.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub struct WalManager {
    state: Arc<Mutex<WalState>>,
    cv: Arc<Condvar>,
    config: WalManagerConfig,
    handle: Option<thread::JoinHandle<()>>,
    // Sharded buffers are only used when wal_sync is disabled.
    shards: Arc<WalShards>,
}

impl WalManager {
    pub fn new(path: PathBuf, config: WalManagerConfig) -> io::Result<Self> {
        // Use more shards than CPUs to spread writers while keeping merge cost modest.
        let shard_count = std::thread::available_parallelism()
            .map(|n| n.get().saturating_mul(2).max(1))
            .unwrap_or(8);
        let state = WalState {
            segments: Vec::new(),
            buffered_bytes: 0,
            buffer_lsn_start: 0,
            next_lsn: 0,
            durable_lsn: 0,
            sync_requested_lsn: 0,
            closed: false,
            rotate_pending: None,
            rotate_requested: 0,
            rotate_completed: 0,
            rotate_error: None,
        };
        let mut manager = Self {
            state: Arc::new(Mutex::new(state)),
            cv: Arc::new(Condvar::new()),
            config: config.clone(),
            handle: None,
            shards: Arc::new(WalShards::new(shard_count)),
        };
        manager.spawn_writer(path)?;
        Ok(manager)
    }

    pub fn append(&self, key: &InternalKey, value: &[u8]) -> io::Result<()> {
        if !self.config.wal_sync {
            let record = encode_record(key, value);
            // Async WAL path: enqueue into a shard and return without waiting for disk.
            // Backpressure kicks in only when the global buffered size exceeds the cap.
            let record_bytes = record.len();
            loop {
                let current = self.shards.buffered();
                if current + record_bytes <= self.config.max_buffer_bytes {
                    if self
                        .shards
                        .buffered_bytes
                        .compare_exchange(
                            current,
                            current + record_bytes,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        break;
                    }
                    continue;
                }
                let mut state = self.state.lock().unwrap();
                while !state.closed
                    && self.shards.buffered() + record_bytes > self.config.max_buffer_bytes
                {
                    state = self.cv.wait(state).unwrap();
                }
                if state.closed {
                    return Err(io::Error::other("WAL manager closed"));
                }
            }

            let shard = self.shards.shard_index();
            {
                let mut buf = self.shards.buffers[shard].lock().unwrap();
                buf.extend_from_slice(&record);
            }
            // Wake the writer thread to merge shards and flush.
            self.cv.notify_one();
            return Ok(());
        }

        let segment = WalSegment::new(key, Bytes::copy_from_slice(value));
        let total_len = segment.len;
        let mut segments = vec![segment];
        let lsn_end = self.enqueue_segments(&mut segments, total_len)?;
        self.wait_for_durable(lsn_end)
    }

    pub fn append_batch(&self, records: &[(InternalKey, Bytes)]) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        if !self.config.wal_sync {
            for (key, value) in records {
                self.append(key, value.as_ref())?;
            }
            return Ok(());
        }

        let mut segments = Vec::with_capacity(records.len());
        let mut total_len = 0usize;
        for (key, value) in records {
            let segment = WalSegment::new(key, value.clone());
            total_len = total_len.saturating_add(segment.len);
            segments.push(segment);
        }
        let lsn_end = self.enqueue_segments(&mut segments, total_len)?;
        self.wait_for_durable(lsn_end)
    }

    fn enqueue_segments(
        &self,
        segments: &mut Vec<WalSegment>,
        total_len: usize,
    ) -> io::Result<u64> {
        if segments.is_empty() {
            return Ok(0);
        }
        let total_len_u64 = total_len as u64;
        let lsn_end = loop {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return Err(io::Error::other("WAL manager closed"));
            }
            if !state.segments.is_empty()
                && state.buffered_bytes + total_len > self.config.max_buffer_bytes
            {
                drop(state);
                let mut wait_guard = self.state.lock().unwrap();
                wait_guard = self.cv.wait(wait_guard).unwrap();
                drop(wait_guard);
                continue;
            }
            if state.segments.is_empty() {
                state.buffer_lsn_start = state.next_lsn;
            }
            let lsn_end = state.next_lsn + total_len_u64;
            state.next_lsn = lsn_end;
            state.buffered_bytes = state.buffered_bytes.saturating_add(total_len);
            state.segments.append(segments);
            state.sync_requested_lsn = state.sync_requested_lsn.max(lsn_end);
            self.cv.notify_one();
            drop(state);
            break lsn_end;
        };
        Ok(lsn_end)
    }

    fn wait_for_durable(&self, lsn_end: u64) -> io::Result<()> {
        if lsn_end == 0 {
            return Ok(());
        }
        let mut state = self.state.lock().unwrap();
        while !state.closed && state.durable_lsn < lsn_end {
            state = self.cv.wait(state).unwrap();
        }
        if state.closed {
            return Err(io::Error::other("WAL manager closed"));
        }
        Ok(())
    }

    pub fn rotate(&self, new_path: PathBuf) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        state.rotate_requested += 1;
        let request_id = state.rotate_requested;
        state.rotate_pending = Some(new_path);
        state.rotate_error = None;
        self.cv.notify_one();
        while !state.closed && state.rotate_completed < request_id {
            state = self.cv.wait(state).unwrap();
        }
        if state.closed {
            return Err(io::Error::other("WAL manager closed"));
        }
        if let Some(err) = state.rotate_error.take() {
            return Err(io::Error::other(err));
        }
        Ok(())
    }

    fn spawn_writer(&mut self, path: PathBuf) -> io::Result<()> {
        let mut writer = WalWriter::new(path, false)?;
        let config = self.config.clone();
        let state = Arc::clone(&self.state);
        let cv = Arc::clone(&self.cv);
        let shards = Arc::clone(&self.shards);
        let handle = thread::spawn(move || {
            let mut last_flush = Instant::now();
            loop {
                let mut guard = state.lock().unwrap();
                if guard.closed {
                    let segments = if config.wal_sync {
                        std::mem::take(&mut guard.segments)
                    } else {
                        Vec::new()
                    };
                    let buffered_bytes = if config.wal_sync {
                        guard.buffered_bytes
                    } else {
                        0
                    };
                    guard.buffered_bytes = 0;
                    if buffered_bytes > 0 {
                        guard.buffer_lsn_start = guard.next_lsn;
                    }
                    drop(guard);

                    if config.wal_sync && !segments.is_empty() {
                        let _ = write_segments(&mut writer, &segments);
                        let _ = writer.flush();
                        let _ = writer.sync_data();
                    }

                    // Drain remaining shard buffers before shutdown.
                    let (data, drained) = drain_shards(&shards);
                    if !data.is_empty() {
                        let _ = writer.write_bytes(&data);
                        let _ = writer.flush();
                        if config.wal_sync {
                            let _ = writer.sync_data();
                        }
                        if drained > 0 {
                            shards.buffered_bytes.fetch_sub(drained, Ordering::AcqRel);
                        }
                    }
                    let _ = writer.flush();
                    if config.wal_sync {
                        let _ = writer.sync_data();
                    }
                    break;
                }

                let interval = Duration::from_millis(config.sync_interval_ms);
                let elapsed = last_flush.elapsed();
                let rotate_pending = guard.rotate_pending.is_some();
                let buffer_len = if config.wal_sync {
                    guard.buffered_bytes
                } else {
                    shards.buffered()
                };
                let pending_sync = config.wal_sync && guard.sync_requested_lsn > guard.durable_lsn;
                let should_flush = rotate_pending
                    || (buffer_len > 0
                        && (buffer_len >= config.sync_bytes
                            || elapsed >= interval
                            || pending_sync));

                if !should_flush {
                    let timeout = if buffer_len == 0 {
                        interval
                    } else {
                        interval.saturating_sub(elapsed)
                    };
                    let result = cv.wait_timeout(guard, timeout).unwrap();
                    guard = result.0;
                    continue;
                }

                let rotate_path = guard.rotate_pending.take();

                if config.wal_sync {
                    let buffered_bytes = guard.buffered_bytes;
                    let segments = if buffered_bytes > 0 {
                        std::mem::take(&mut guard.segments)
                    } else {
                        Vec::new()
                    };
                    let lsn_end = if buffered_bytes > 0 {
                        Some(guard.buffer_lsn_start + buffered_bytes as u64)
                    } else {
                        None
                    };
                    guard.buffered_bytes = 0;
                    if buffered_bytes > 0 {
                        guard.buffer_lsn_start = guard.next_lsn;
                    }
                    drop(guard);

                    if let Some(lsn_end) = lsn_end {
                        let mut write_error: Option<io::Error> = None;
                        if let Err(e) = write_segments(&mut writer, &segments) {
                            write_error = Some(e);
                        } else if let Err(e) = writer.flush() {
                            write_error = Some(e);
                        } else if let Err(e) = writer.sync_data() {
                            write_error = Some(e);
                        }

                        if write_error.is_none() {
                            let mut guard = state.lock().unwrap();
                            guard.durable_lsn = guard.durable_lsn.max(lsn_end);
                            if guard.durable_lsn >= guard.sync_requested_lsn {
                                guard.sync_requested_lsn = guard.durable_lsn;
                            }
                            last_flush = Instant::now();
                            cv.notify_all();
                        } else {
                            let mut guard = state.lock().unwrap();
                            guard.closed = true;
                            guard.rotate_error =
                                Some(format!("WAL write failed: {}", write_error.unwrap()));
                            cv.notify_all();
                            break;
                        }
                    }
                } else {
                    drop(guard);
                    // Merge all shards into one contiguous write buffer.
                    let (data, drained) = drain_shards(&shards);
                    if !data.is_empty() {
                        let mut write_error: Option<io::Error> = None;
                        if let Err(e) = writer.write_bytes(&data) {
                            write_error = Some(e);
                        } else if let Err(e) = writer.flush() {
                            write_error = Some(e);
                        }
                        if write_error.is_none() {
                            if drained > 0 {
                                shards.buffered_bytes.fetch_sub(drained, Ordering::AcqRel);
                            }
                            last_flush = Instant::now();
                            cv.notify_all();
                        } else {
                            let mut guard = state.lock().unwrap();
                            guard.closed = true;
                            guard.rotate_error =
                                Some(format!("WAL write failed: {}", write_error.unwrap()));
                            cv.notify_all();
                            break;
                        }
                    }
                }

                if let Some(path) = rotate_path {
                    let mut rotate_error = None;
                    if let Err(e) = writer.flush() {
                        rotate_error = Some(format!("WAL flush failed: {}", e));
                    } else if config.wal_sync {
                        if let Err(e) = writer.sync_data() {
                            rotate_error = Some(format!("WAL sync failed: {}", e));
                        }
                    }
                    if rotate_error.is_none() {
                        match WalWriter::new(path, false) {
                            Ok(new_writer) => {
                                writer = new_writer;
                            }
                            Err(e) => {
                                rotate_error = Some(format!("Failed to open new WAL file: {}", e));
                            }
                        }
                    }
                    let mut guard = state.lock().unwrap();
                    guard.rotate_completed = guard.rotate_requested;
                    guard.rotate_error = rotate_error;
                    cv.notify_all();
                }
            }
        });
        let old_handle = self.handle.replace(handle);
        if let Some(handle) = old_handle {
            let _ = handle.join();
        }
        Ok(())
    }
}

impl Drop for WalManager {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        self.cv.notify_all();
        drop(state);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn write_segments(writer: &mut WalWriter, segments: &[WalSegment]) -> io::Result<()> {
    for segment in segments {
        writer.write_bytes(&segment.header1)?;
        writer.write_bytes(&segment.key)?;
        writer.write_bytes(&segment.header2)?;
        writer.write_bytes(&segment.value)?;
    }
    Ok(())
}

fn drain_shards(shards: &Arc<WalShards>) -> (Vec<u8>, usize) {
    // Merge all shard buffers into a single buffer for sequential IO.
    let mut data = Vec::new();
    let mut drained = 0usize;
    for shard in &shards.buffers {
        let mut buf = shard.lock().unwrap();
        let len = buf.len();
        if len > 0 {
            drained += len;
            data.append(&mut *buf);
        }
    }
    (data, drained)
}

fn record_size(key: &InternalKey, value: &[u8]) -> usize {
    12 + key.serialized_size() + value.len()
}

fn encode_record_into(buf: &mut Vec<u8>, key: &InternalKey, value: &[u8]) {
    let key_len = key.serialized_size() as u32;
    let value_len = value.len() as u32;
    let checksum = checksum_for(key, key_len, value, value_len);
    buf.extend_from_slice(&checksum.to_le_bytes());
    buf.extend_from_slice(&key_len.to_le_bytes());
    buf.extend_from_slice(key.user_key());
    buf.extend_from_slice(&key.encoded_sequence_number().to_le_bytes());
    buf.extend_from_slice(&value_len.to_le_bytes());
    buf.extend_from_slice(value);
}

fn encode_record(key: &InternalKey, value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(record_size(key, value));
    encode_record_into(&mut buf, key, value);
    buf
}
