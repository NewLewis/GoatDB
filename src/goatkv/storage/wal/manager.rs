use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::goatkv::format::internal_key::InternalKey;
use bytes::Bytes;

use super::codec::WalCodec;
use super::writer::WalWriter;

/// WAL管理器配置
#[derive(Debug, Clone)]
pub struct WalManagerConfig {
    /// 是否启用同步写入（WAL同步）
    pub wal_sync: bool,
    /// 同步间隔（毫秒）
    pub sync_interval_ms: u64,
    /// 触发同步的字节阈值
    pub sync_bytes: usize,
    /// 最大缓冲区字节数
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

/// WAL内部状态
#[derive(Debug)]
struct WalState {
    /// WAL缓冲区（单实例只会运行在同步或异步其中一种模式）
    buffer: Vec<u8>,
    /// 当前缓冲的总字节数
    buffered_bytes: usize,
    /// 当前已分配写入范围的结束偏移（字节）
    offset_end: u64,
    /// 已持久化的WAL偏移（字节）
    durable_offset: u64,
    /// 是否已关闭
    closed: bool,
    /// 待处理的WAL文件轮换路径
    rotate_pending: Option<PathBuf>,
    /// 已请求的轮换次数（用于跟踪请求顺序）
    rotate_requested: u64,
    /// 已完成的轮换次数
    rotate_completed: u64,
    /// 轮换错误信息
    rotate_error: Option<String>,
}

/// 刷新操作结果
#[derive(Debug)]
enum FlushOutcome {
    /// 无需操作
    Noop,
    /// 同步持久化完成（包含持久化偏移）
    SyncDurable(u64),
    /// 异步写入完成
    AsyncWritten,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EnqueueMode {
    Async,
    Sync,
}

/// WAL管理器
///
/// 负责管理Write-Ahead Log的写入、同步和轮换。
/// 支持同步和异步两种写入模式，使用后台线程进行实际文件操作。
#[derive(Debug)]
pub struct WalManager {
    /// 共享状态（线程安全）
    state: Arc<Mutex<WalState>>,
    /// 条件变量，用于线程间同步
    cv: Arc<Condvar>,
    /// 配置
    config: WalManagerConfig,
    /// 后台写入线程句柄
    handle: Option<thread::JoinHandle<()>>,
}

impl WalManager {
    /// 创建新的WAL管理器
    ///
    /// # 参数
    /// - `path`: WAL文件路径
    /// - `config`: 管理器配置
    ///
    /// # 返回值
    /// - `io::Result<Self>`: 成功返回WAL管理器实例
    pub fn new(path: PathBuf, config: WalManagerConfig) -> io::Result<Self> {
        // 初始化状态
        let state = WalState {
            buffer: Vec::new(),
            buffered_bytes: 0,
            offset_end: 0,
            durable_offset: 0,
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
        };

        // 启动后台写入线程
        manager.spawn_writer(path)?;
        Ok(manager)
    }

    /// 追加单个键值对到WAL
    ///
    /// # 参数
    /// - `key`: 内部键
    /// - `value`: 值
    ///
    /// # 返回值
    /// - `io::Result<()>`: 成功返回Ok，失败返回错误
    pub fn append(&self, key: &InternalKey, value: &[u8]) -> io::Result<()> {
        // 编码记录
        let record = WalCodec::encode_record(key, value);

        // 根据配置选择同步或异步模式
        if !self.config.wal_sync {
            return self.enqueue_async_bytes(&record);
        }

        // 同步模式：写入并等待持久化
        let offset_end = self.enqueue_sync_bytes(&record)?;
        self.wait_for_durable(offset_end)
    }

    /// 批量追加键值对到WAL
    ///
    /// # 参数
    /// - `records`: 键值对切片
    ///
    /// # 返回值
    /// - `io::Result<()>`: 成功返回Ok，失败返回错误
    pub fn append_batch(&self, records: &[(InternalKey, Bytes)]) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        // 将所有记录编码到单个缓冲区
        let mut total_record = Vec::new();
        for (key, value) in records {
            WalCodec::encode_record_into(&mut total_record, key, value.as_ref());
        }

        // 根据配置选择同步或异步模式
        if !self.config.wal_sync {
            return self.enqueue_async_bytes(&total_record);
        }

        let offset_end = self.enqueue_sync_bytes(&total_record)?;
        self.wait_for_durable(offset_end)
    }

    /// 将数据加入异步缓冲区（非阻塞）
    ///
    /// # 参数
    /// - `data`: 要写入的数据
    ///
    /// # 返回值
    /// - `io::Result<()>`: 成功返回Ok，失败返回错误
    fn enqueue_async_bytes(&self, data: &[u8]) -> io::Result<()> {
        self.enqueue_bytes(data, EnqueueMode::Async).map(|_| ())
    }

    /// 将数据加入同步缓冲区并返回结束偏移
    ///
    /// # 参数
    /// - `data`: 要写入的数据
    ///
    /// # 返回值
    /// - `io::Result<u64>`: 成功返回结束偏移，失败返回错误
    fn enqueue_sync_bytes(&self, data: &[u8]) -> io::Result<u64> {
        self.enqueue_bytes(data, EnqueueMode::Sync)
    }

    /// 将数据加入缓冲区（根据模式选择背压和偏移处理逻辑）
    ///
    /// # 参数
    /// - `data`: 要写入的数据
    /// - `mode`: 入队模式（同步/异步）
    ///
    /// # 返回值
    /// - `io::Result<u64>`: 同步模式返回结束偏移，异步模式返回0
    fn enqueue_bytes(&self, data: &[u8], mode: EnqueueMode) -> io::Result<u64> {
        if data.is_empty() {
            return Ok(0);
        }

        let data_len = data.len();
        let data_len_u64 = data_len as u64;
        let mut state = self.state.lock().unwrap();

        // 同步模式仅在缓冲非空时背压；异步模式在超过阈值时总是背压。
        while !state.closed
            && state.buffered_bytes + data_len > self.config.max_buffer_bytes
            && (mode == EnqueueMode::Async || !state.buffer.is_empty())
        {
            state = self.cv.wait(state).unwrap();
        }

        if state.closed {
            return Err(io::Error::other("WAL manager closed"));
        }

        let offset_end = if mode == EnqueueMode::Sync {
            // 同步模式需要分配并返回本次写入的结束偏移。
            let new_offset_end = state.offset_end + data_len_u64;
            state.offset_end = new_offset_end;
            state.buffered_bytes = state.buffered_bytes.saturating_add(data_len);
            new_offset_end
        } else {
            state.buffered_bytes += data_len;
            0
        };
        state.buffer.extend_from_slice(data);

        // 通知写入线程
        self.cv.notify_one();
        Ok(offset_end)
    }

    /// 等待数据持久化到指定偏移
    ///
    /// # 参数
    /// - `offset_end`: 需要持久化的结束偏移
    ///
    /// # 返回值
    /// - `io::Result<()>`: 成功返回Ok，失败返回错误
    fn wait_for_durable(&self, offset_end: u64) -> io::Result<()> {
        if offset_end == 0 {
            return Ok(());
        }

        let mut state = self.state.lock().unwrap();

        // 等待持久化偏移达到或超过请求偏移
        while !state.closed && state.durable_offset < offset_end {
            state = self.cv.wait(state).unwrap();
        }

        if state.closed {
            return Err(io::Error::other("WAL manager closed"));
        }
        Ok(())
    }

    /// 轮换WAL文件（切换到新文件）
    ///
    /// # 参数
    /// - `new_path`: 新WAL文件路径
    ///
    /// # 返回值
    /// - `io::Result<()>`: 成功返回Ok，失败返回错误
    pub fn rotate(&self, new_path: PathBuf) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();

        // 递增轮换请求计数器
        state.rotate_requested += 1;
        let request_id = state.rotate_requested;

        // 设置待处理的轮换路径
        state.rotate_pending = Some(new_path);
        state.rotate_error = None;

        // 通知写入线程
        self.cv.notify_one();

        // 等待轮换完成
        while !state.closed && state.rotate_completed < request_id {
            state = self.cv.wait(state).unwrap();
        }

        if state.closed {
            return Err(io::Error::other("WAL manager closed"));
        }

        // 检查轮换错误
        if let Some(err) = state.rotate_error.take() {
            return Err(io::Error::other(err));
        }
        Ok(())
    }

    /// 启动后台写入线程
    ///
    /// # 参数
    /// - `path`: 初始WAL文件路径
    ///
    /// # 返回值
    /// - `io::Result<()>`: 成功返回Ok，失败返回错误
    fn spawn_writer(&mut self, path: PathBuf) -> io::Result<()> {
        // 创建初始WAL写入器
        let writer = WalWriter::new(path)?;
        let config = self.config.clone();
        let state = Arc::clone(&self.state);
        let cv = Arc::clone(&self.cv);

        // 启动写入线程
        let handle = thread::spawn(move || Self::writer_loop(writer, config, state, cv));

        // 保存线程句柄（如果已有旧线程，等待其结束）
        let old_handle = self.handle.replace(handle);
        if let Some(handle) = old_handle {
            let _ = handle.join();
        }
        Ok(())
    }

    /// 写入线程主循环
    ///
    /// # 参数
    /// - `writer`: WAL写入器
    /// - `config`: 配置
    /// - `state`: 共享状态
    /// - `cv`: 条件变量
    fn writer_loop(
        mut writer: WalWriter,
        config: WalManagerConfig,
        state: Arc<Mutex<WalState>>,
        cv: Arc<Condvar>,
    ) {
        let mut last_flush = Instant::now();

        loop {
            let mut guard = state.lock().unwrap();

            // 检查是否已关闭
            if guard.closed {
                // 关闭时，获取剩余缓冲数据
                let data = Self::take_close_data(&mut guard);
                drop(guard);

                // 写入剩余数据
                Self::drain_close_data(&mut writer, &config, &data);
                break;
            }

            let interval = Duration::from_millis(config.sync_interval_ms);
            let elapsed = last_flush.elapsed();

            // 判断是否需要刷新
            if !Self::should_flush(&guard, &config, elapsed, interval) {
                // 等待工作或超时
                drop(Self::wait_for_work(&cv, guard, interval, elapsed));
                continue;
            }

            // 获取待处理的轮换路径
            let rotate_path = guard.rotate_pending.take();

            // 根据模式执行刷新操作
            let flush_result = if config.wal_sync {
                // 同步模式：获取同步数据及其对应结束偏移
                let (data, offset_end) = Self::take_sync_flush_data(&mut guard);
                drop(guard);
                Self::flush_sync_if_needed(&mut writer, data, offset_end)
            } else {
                // 异步模式：获取异步数据
                let data = Self::take_async_flush_data(&mut guard);
                drop(guard);
                Self::flush_async_if_needed(&mut writer, data)
            };

            // 处理刷新结果
            match flush_result {
                Ok(FlushOutcome::Noop) => {
                    // 无需操作
                }
                Ok(FlushOutcome::SyncDurable(offset_end)) => {
                    // 同步持久化成功，更新状态
                    Self::on_sync_flush_success(&state, &cv, offset_end, &mut last_flush);
                }
                Ok(FlushOutcome::AsyncWritten) => {
                    // 异步写入成功
                    Self::on_async_flush_success(&cv, &mut last_flush);
                }
                Err(err) => {
                    // 写入错误，关闭管理器
                    Self::on_write_error(&state, &cv, err);
                    break;
                }
            }

            // 处理WAL文件轮换
            Self::handle_rotate(&mut writer, &config, &state, &cv, rotate_path);
        }
    }

    /// 获取关闭时的数据
    ///
    /// # 参数
    /// - `guard`: 状态守卫
    /// # 返回值
    /// - `Vec<u8>`: 缓冲中的剩余数据
    fn take_close_data(guard: &mut WalState) -> Vec<u8> {
        Self::take_flush_data(guard)
    }

    /// 写入关闭时的剩余数据
    ///
    /// # 参数
    /// - `writer`: WAL写入器
    /// - `config`: 配置
    /// - `data`: 缓冲数据
    fn drain_close_data(writer: &mut WalWriter, config: &WalManagerConfig, data: &[u8]) {
        // 写入剩余缓冲数据
        if !data.is_empty() {
            let _ = writer.write_bytes(data);
        }

        // 刷新并同步
        let _ = writer.flush();
        if config.wal_sync {
            let _ = writer.sync_data();
        }
    }

    /// 判断是否需要刷新缓冲区
    ///
    /// # 参数
    /// - `state`: 当前状态
    /// - `config`: 配置
    /// - `elapsed`: 距上次刷新的时间
    /// - `interval`: 同步间隔
    ///
    /// # 返回值
    /// - `bool`: 是否需要刷新
    fn should_flush(
        state: &WalState,
        config: &WalManagerConfig,
        elapsed: Duration,
        interval: Duration,
    ) -> bool {
        let rotate_pending = state.rotate_pending.is_some();
        let buffer_len = state.buffered_bytes;
        let pending_sync = config.wal_sync && state.offset_end > state.durable_offset;

        // 刷新条件：
        // 1. 有待处理的轮换
        // 2. 缓冲区非空且（达到字节阈值 OR 达到时间间隔 OR 有待处理的同步请求）
        rotate_pending
            || (buffer_len > 0
                && (buffer_len >= config.sync_bytes || elapsed >= interval || pending_sync))
    }

    /// 等待工作或超时
    ///
    /// # 参数
    /// - `cv`: 条件变量
    /// - `guard`: 状态守卫
    /// - `interval`: 间隔时间
    /// - `elapsed`: 已过去的时间
    ///
    /// # 返回值
    /// - 更新后的状态守卫
    fn wait_for_work<'a>(
        cv: &Condvar,
        guard: std::sync::MutexGuard<'a, WalState>,
        interval: Duration,
        elapsed: Duration,
    ) -> std::sync::MutexGuard<'a, WalState> {
        // 计算超时时间：如果缓冲区为空，等待完整间隔；否则等待剩余时间
        let timeout = if guard.buffered_bytes == 0 {
            interval
        } else {
            interval.saturating_sub(elapsed)
        };

        cv.wait_timeout(guard, timeout).unwrap().0
    }

    /// 获取同步刷新数据
    ///
    /// # 参数
    /// - `guard`: 状态守卫
    ///
    /// # 返回值
    /// - `(Vec<u8>, Option<u64>)`: (数据, 结束偏移)
    fn take_sync_flush_data(guard: &mut WalState) -> (Vec<u8>, Option<u64>) {
        // 获取缓冲区数据（同步模式下用于计算持久化结束偏移）
        let data = Self::take_flush_data(guard);
        let drained = data.len();

        // 单缓冲下，drain后对应的结束偏移就是当前offset_end快照
        let offset_end = if drained > 0 {
            Some(guard.offset_end)
        } else {
            None
        };

        (data, offset_end)
    }

    /// 获取异步刷新数据
    ///
    /// # 参数
    /// - `guard`: 状态守卫
    ///
    /// # 返回值
    /// - `Vec<u8>`: 异步数据
    fn take_async_flush_data(guard: &mut WalState) -> Vec<u8> {
        Self::take_flush_data(guard)
    }

    /// 获取并清空刷新缓冲区数据
    ///
    /// # 参数
    /// - `guard`: 状态守卫
    ///
    /// # 返回值
    /// - `Vec<u8>`: 刷新数据
    fn take_flush_data(guard: &mut WalState) -> Vec<u8> {
        let data = std::mem::take(&mut guard.buffer);
        let drained = data.len();
        debug_assert_eq!(
            guard.buffered_bytes, drained,
            "WAL buffer byte accounting mismatch: buffered_bytes={}, drained={}",
            guard.buffered_bytes, drained
        );
        // 缓冲区已被整体take，计数应回到0；即使前面发生漂移也强制修正。
        guard.buffered_bytes = 0;
        data
    }

    /// 将数据写入WAL并flush（空数据直接返回false）
    ///
    /// # 参数
    /// - `writer`: WAL写入器
    /// - `data`: 待写入数据
    ///
    /// # 返回值
    /// - `io::Result<bool>`: 成功返回是否写入了数据
    fn write_and_flush_if_needed(writer: &mut WalWriter, data: &[u8]) -> io::Result<bool> {
        if data.is_empty() {
            return Ok(false);
        }

        writer.write_bytes(data)?;
        writer.flush()?;
        Ok(true)
    }

    /// 执行同步刷新（如果需要）
    ///
    /// # 参数
    /// - `writer`: WAL写入器
    /// - `data`: 要写入的数据
    /// - `offset_end`: 结束偏移
    ///
    /// # 返回值
    /// - `io::Result<FlushOutcome>`: 刷新结果
    fn flush_sync_if_needed(
        writer: &mut WalWriter,
        data: Vec<u8>,
        offset_end: Option<u64>,
    ) -> io::Result<FlushOutcome> {
        let Some(offset_end) = offset_end else {
            return Ok(FlushOutcome::Noop);
        };

        // 写入、刷新并同步
        if !Self::write_and_flush_if_needed(writer, &data)? {
            return Ok(FlushOutcome::Noop);
        }
        writer.sync_data()?;

        Ok(FlushOutcome::SyncDurable(offset_end))
    }

    /// 执行异步刷新（如果需要）
    ///
    /// # 参数
    /// - `writer`: WAL写入器
    /// - `data`: 要写入的数据
    ///
    /// # 返回值
    /// - `io::Result<FlushOutcome>`: 刷新结果
    fn flush_async_if_needed(writer: &mut WalWriter, data: Vec<u8>) -> io::Result<FlushOutcome> {
        if !Self::write_and_flush_if_needed(writer, &data)? {
            return Ok(FlushOutcome::Noop);
        }

        Ok(FlushOutcome::AsyncWritten)
    }

    /// 同步刷新成功处理
    ///
    /// # 参数
    /// - `state`: 共享状态
    /// - `cv`: 条件变量
    /// - `offset_end`: 持久化结束偏移
    /// - `last_flush`: 上次刷新时间
    fn on_sync_flush_success(
        state: &Arc<Mutex<WalState>>,
        cv: &Condvar,
        offset_end: u64,
        last_flush: &mut Instant,
    ) {
        let mut guard = state.lock().unwrap();

        // 更新持久化偏移
        guard.durable_offset = guard.durable_offset.max(offset_end);

        // 更新刷新时间并通知所有等待线程
        *last_flush = Instant::now();
        cv.notify_all();
    }

    /// 异步刷新成功处理
    ///
    /// # 参数
    /// - `cv`: 条件变量
    /// - `last_flush`: 上次刷新时间
    fn on_async_flush_success(cv: &Condvar, last_flush: &mut Instant) {
        *last_flush = Instant::now();
        cv.notify_all();
    }

    /// 写入错误处理
    ///
    /// # 参数
    /// - `state`: 共享状态
    /// - `cv`: 条件变量
    /// - `err`: 错误信息
    fn on_write_error(state: &Arc<Mutex<WalState>>, cv: &Condvar, err: io::Error) {
        let mut guard = state.lock().unwrap();
        guard.closed = true;
        guard.rotate_error = Some(format!("WAL write failed: {}", err));
        cv.notify_all();
    }

    /// 处理WAL文件轮换
    ///
    /// # 参数
    /// - `writer`: WAL写入器
    /// - `config`: 配置
    /// - `state`: 共享状态
    /// - `cv`: 条件变量
    /// - `rotate_path`: 轮换路径
    fn handle_rotate(
        writer: &mut WalWriter,
        config: &WalManagerConfig,
        state: &Arc<Mutex<WalState>>,
        cv: &Condvar,
        rotate_path: Option<PathBuf>,
    ) {
        let Some(path) = rotate_path else {
            return;
        };

        let mut rotate_error = None;

        // 刷新当前文件
        if let Err(e) = writer.flush() {
            rotate_error = Some(format!("WAL flush failed: {}", e));
        } else if config.wal_sync {
            // 同步模式下需要确保数据同步到磁盘
            if let Err(e) = writer.sync_data() {
                rotate_error = Some(format!("WAL sync failed: {}", e));
            }
        }

        // 如果没有错误，创建新的WAL写入器
        if rotate_error.is_none() {
            match WalWriter::new(path) {
                Ok(new_writer) => {
                    *writer = new_writer;
                }
                Err(e) => {
                    rotate_error = Some(format!("Failed to open new WAL file: {}", e));
                }
            }
        }

        // 更新状态并通知等待线程
        let mut guard = state.lock().unwrap();
        guard.rotate_completed = guard.rotate_requested;
        guard.rotate_error = rotate_error;
        cv.notify_all();
    }
}

impl Drop for WalManager {
    /// Drop实现：确保WAL管理器正确关闭
    fn drop(&mut self) {
        // 设置关闭标志
        let mut state = self.state.lock().unwrap();
        state.closed = true;

        // 通知所有等待线程
        self.cv.notify_all();
        drop(state);

        // 等待写入线程结束
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
