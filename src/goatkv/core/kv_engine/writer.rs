use std::collections::VecDeque;
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::Duration;

use bytes::Bytes;
use tracing::{debug, warn};

use crate::goatkv::core::lsm_state::LSMState;
use crate::goatkv::core::sequence_number::SequenceNumber;
use crate::goatkv::error::{Error as GoatError, Result as GoatResult};
use crate::goatkv::format::internal_key::{InternalKey, InternalKeyKind, SEQUENCE_NUMBER_MAX};
use crate::goatkv::storage::wal::WalWriter;
use crate::goatkv::utils::options::KvEngineOptions;

// 写入主流程：
//   业务线程 -> WAL 队列 -> WAL Leader 落盘 -> Mem 队列 -> Mem Leader 写入 MemTable。
//
// 这里刻意把 WAL 与 MemTable 拆成两个阶段：
// - WAL 阶段保证持久化顺序；
// - Mem 阶段保证内存读路径可见性；
// - flush 屏障可以等待两个阶段都完全空闲。

#[derive(Debug)]
pub(crate) enum WriteOp {
    // 写入用户键值。
    Put(Vec<u8>, Vec<u8>),
    // 写入删除标记（tombstone）。
    Delete(Vec<u8>),
}

#[derive(Debug)]
struct MemApplyPayload {
    // WAL 阶段准备、Mem 阶段消费的内部记录。
    records: Vec<(InternalKey, Bytes)>,
    // 当前请求中的最大序列号，用于发布可见性水位。
    max_seq: Option<u64>,
}

#[derive(Debug)]
struct WriteRequest {
    // 原始用户操作，仅在 WAL 阶段消费一次。
    ops: Mutex<Vec<WriteOp>>,
    // 由 WAL 阶段产出、Mem 阶段消费的中间载荷。
    mem_payload: Mutex<Option<MemApplyPayload>>,
    // 缓存的操作数与字节数，用于分组和背压判断。
    ops_len: usize,
    approx_bytes: usize,
    // 完成通知通道（基于 Mutex + Condvar 的一次性结果）。
    result: Mutex<Option<GoatResult<()>>>,
    cv: Condvar,
}

impl WriteRequest {
    fn new(ops: Vec<WriteOp>) -> Self {
        let ops_len = ops.len();
        let mut approx_bytes = 0usize;
        for op in &ops {
            match op {
                WriteOp::Put(key, value) => {
                    approx_bytes = approx_bytes.saturating_add(key.len() + value.len() + 20);
                }
                WriteOp::Delete(key) => {
                    approx_bytes = approx_bytes.saturating_add(key.len() + 20);
                }
            }
        }
        Self {
            ops: Mutex::new(ops),
            mem_payload: Mutex::new(None),
            ops_len,
            approx_bytes,
            result: Mutex::new(None),
            cv: Condvar::new(),
        }
    }

    fn take_ops(&self) -> Vec<WriteOp> {
        // 取走所有权，保证每个请求只会被转换一次。
        let mut guard = self.ops.lock().unwrap();
        mem::take(&mut *guard)
    }

    fn set_mem_payload(&self, payload: MemApplyPayload) {
        let mut guard = self.mem_payload.lock().unwrap();
        *guard = Some(payload);
    }

    fn take_mem_payload(&self) -> Option<MemApplyPayload> {
        let mut guard = self.mem_payload.lock().unwrap();
        guard.take()
    }

    fn complete(&self, result: GoatResult<()>) {
        // 幂等完成：只允许第一次写入结果生效。
        let mut guard = self.result.lock().unwrap();
        if guard.is_some() {
            return;
        }
        *guard = Some(result);
        self.cv.notify_one();
    }

    fn wait(&self) -> GoatResult<()> {
        // 阻塞等待，直到 leader 循环给出成功或失败结果。
        let mut guard = self.result.lock().unwrap();
        while guard.is_none() {
            guard = self.cv.wait(guard).unwrap();
        }
        guard.take().unwrap()
    }
}

#[derive(Debug, Clone)]
enum CloseReason {
    // 用户主动关闭。
    Manual,
    // Writer 内部不可恢复故障关闭。
    Failed(String),
}

enum WritePressureAction {
    Allow,
    Slowdown { delay: Duration },
    Stop(GoatError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WritePressureLevel {
    Normal = 0,
    Slowdown = 1,
    Stop = 2,
}

impl WritePressureLevel {
    const fn as_u8(self) -> u8 {
        self as u8
    }

    fn from_u8(raw: u8) -> Self {
        match raw {
            1 => Self::Slowdown,
            2 => Self::Stop,
            _ => Self::Normal,
        }
    }
}

#[derive(Debug)]
struct WriteState {
    // submitter -> WAL leader 的队列。
    wal_queue: VecDeque<Arc<WriteRequest>>,
    // WAL leader -> Mem leader 的队列。
    mem_queue: VecDeque<Arc<WriteRequest>>,
    // 队列指标缓存，避免背压判断时 O(n) 扫描。
    wal_queue_reqs: usize,
    wal_queue_bytes: usize,
    mem_queue_reqs: usize,
    mem_queue_bytes: usize,
    // 任一时刻最多一个 WAL/Mem leader 循环在运行。
    wal_leader_active: bool,
    mem_leader_active: bool,
    // 当前在 state 锁外执行中的 group 数量。
    wal_inflight_groups: usize,
    mem_inflight_groups: usize,
    // flush 屏障开关：true 表示 submit_write 阶段阻塞准入。
    flush_blocked: bool,
    // 一旦设置，writer 拒绝新请求并清空待处理队列。
    closed_reason: Option<CloseReason>,
}

impl WriteState {
    fn new() -> Self {
        Self {
            wal_queue: VecDeque::new(),
            mem_queue: VecDeque::new(),
            wal_queue_reqs: 0,
            wal_queue_bytes: 0,
            mem_queue_reqs: 0,
            mem_queue_bytes: 0,
            wal_leader_active: false,
            mem_leader_active: false,
            wal_inflight_groups: 0,
            mem_inflight_groups: 0,
            flush_blocked: false,
            closed_reason: None,
        }
    }

    fn is_closed(&self) -> bool {
        self.closed_reason.is_some()
    }

    fn has_pending_work(&self) -> bool {
        // flush 屏障使用：只有队列与 inflight group 都清空时，writer 才算“排空”。
        !self.wal_queue.is_empty()
            || !self.mem_queue.is_empty()
            || self.wal_inflight_groups > 0
            || self.mem_inflight_groups > 0
    }
}

#[derive(Debug)]
pub struct KvWriter {
    // 负责持久化写入的 WAL 写入器。
    wal_writer: Arc<WalWriter>,
    // 全局单调递增序列号分配器。
    sequence_number: Arc<SequenceNumber>,
    // 共享的 mutable/immutable memtable 状态。
    lsm_state: Arc<RwLock<LSMState>>,
    // 与 flush 共享的全局闸门：普通写持读锁，flush 持写锁。
    write_gate: Arc<RwLock<()>>,
    // writer 状态机互斥锁 + 条件变量。
    state: Mutex<WriteState>,
    state_cv: Condvar,
    // 分组与背压相关参数。
    options: Arc<KvEngineOptions>,
    // Mem 阶段提交后的可见序列号水位。
    last_published_seq: AtomicU64,
    // 协作式 flush 触发标志，由请求线程在完成后消费。
    flush_requested: AtomicBool,
    // 写入压力状态（normal/slowdown/stop），用于状态转移可观测。
    write_pressure_level: AtomicU8,
}

pub struct FlushBarrierGuard<'a> {
    writer: &'a KvWriter,
}

impl Drop for FlushBarrierGuard<'_> {
    fn drop(&mut self) {
        self.writer.end_flush_barrier();
    }
}

impl KvWriter {
    pub fn new(
        wal_writer: Arc<WalWriter>,
        sequence_number: Arc<SequenceNumber>,
        lsm_state: Arc<RwLock<LSMState>>,
        write_gate: Arc<RwLock<()>>,
        options: Arc<KvEngineOptions>,
    ) -> Self {
        Self {
            wal_writer,
            sequence_number,
            lsm_state,
            write_gate,
            state: Mutex::new(WriteState::new()),
            state_cv: Condvar::new(),
            options,
            last_published_seq: AtomicU64::new(0),
            flush_requested: AtomicBool::new(false),
            write_pressure_level: AtomicU8::new(WritePressureLevel::Normal.as_u8()),
        }
    }

    pub(crate) fn submit_write<F>(&self, ops: Vec<WriteOp>, flush_fn: F) -> GoatResult<()>
    where
        F: Fn(),
    {
        // 单个请求对象贯穿两个阶段，并携带中间载荷。
        let request = Arc::new(WriteRequest::new(ops));
        let mut is_wal_leader = false;

        {
            let mut state = self.state.lock().unwrap();

            // 准入控制：
            // - flush 屏障开启时阻塞；
            // - WAL 队列背压时阻塞；
            // - compaction 压力触发 slowdown/stop。
            while !state.is_closed() {
                if let Some(err) = self.flush_backpressure_error() {
                    return Err(err);
                }

                if state.flush_blocked || self.wal_queue_backpressured(&state, request.approx_bytes)
                {
                    state = self.state_cv.wait(state).unwrap();
                    continue;
                }

                match self.write_pressure_action() {
                    WritePressureAction::Allow => break,
                    WritePressureAction::Slowdown { delay } => {
                        state = self.state_cv.wait_timeout(state, delay).unwrap().0;
                    }
                    WritePressureAction::Stop(err) => return Err(err),
                }
            }

            // writer 已关闭则立即拒绝写入。
            if let Some(reason) = state.closed_reason.clone() {
                return Err(Self::close_reason_to_error(reason));
            }

            // 进入 WAL 阶段队列。
            state.wal_queue.push_back(request.clone());
            state.wal_queue_reqs = state.wal_queue_reqs.saturating_add(1);
            state.wal_queue_bytes = state.wal_queue_bytes.saturating_add(request.approx_bytes);

            // 从空闲态恢复时，第一个提交者成为 WAL leader。
            if !state.wal_leader_active {
                state.wal_leader_active = true;
                is_wal_leader = true;
            }
            self.state_cv.notify_all();
        }

        if is_wal_leader {
            self.run_wal_leader_loop();
        }

        // 协作式 leader：业务线程可顺带推进 MemTable 阶段。
        self.run_mem_leader_loop_if_needed();

        // 对用户可见的完成语义：要等到 Mem 阶段成功/失败落定。
        let result = request.wait();

        // 只有成功写入才触发 flush 请求。
        if result.is_ok() && self.flush_requested.swap(false, Ordering::AcqRel) {
            flush_fn();
        }
        result
    }

    /// 关闭写入入口（拒绝新写），并允许已接收请求继续完成。
    pub(crate) fn close(&self) {
        let mut state = self.state.lock().unwrap();
        if state.is_closed() {
            return;
        }
        state.closed_reason = Some(CloseReason::Manual);
        self.state_cv.notify_all();
    }

    /// 进入 flush 屏障：阻塞新写入，并等待队列与 inflight group 全部排空。
    pub(crate) fn begin_flush_barrier(&self) {
        let mut state = self.state.lock().unwrap();

        // 串行化多个重叠的 flush 屏障请求。
        while !state.is_closed() && state.flush_blocked {
            state = self.state_cv.wait(state).unwrap();
        }

        // 先打开准入阻塞，再等待当前工作排空。
        state.flush_blocked = true;
        while state.has_pending_work() {
            state = self.state_cv.wait(state).unwrap();
        }
    }

    pub(crate) fn enter_flush_barrier(&self) -> FlushBarrierGuard<'_> {
        self.begin_flush_barrier();
        FlushBarrierGuard { writer: self }
    }

    /// 退出 flush 屏障，恢复写入准入。
    pub(crate) fn end_flush_barrier(&self) {
        let mut state = self.state.lock().unwrap();
        if !state.flush_blocked {
            return;
        }

        // 调用方完成 flush 临界区后，重新开放写入准入。
        state.flush_blocked = false;
        self.state_cv.notify_all();
    }

    fn run_wal_leader_loop(&self) {
        // WAL leader 循环：
        //   1) 弹出一个 WAL group；
        //   2) 分配序列号并写入 WAL；
        //   3) 转交到 Mem 队列。
        // 任一致命错误都会走 fail_fast 关闭 writer。
        loop {
            let group = self.pop_next_wal_group();
            if group.is_empty() {
                return;
            }

            let wal_result = self.apply_wal_group(&group);
            match wal_result {
                Ok(()) => {
                    let enqueue_result = self.enqueue_mem_group(&group);
                    // 该 group 已离开 WAL 执行阶段。
                    self.finish_wal_inflight_group();
                    match enqueue_result {
                        Ok(start_mem_leader) => {
                            if start_mem_leader {
                                // WAL leader 可继续充当 Mem leader，减少阶段切换延迟。
                                self.run_mem_leader_loop();
                            }
                        }
                        Err(err) => {
                            let msg = err.to_string();
                            for req in &group {
                                req.complete(Err(GoatError::internal("write_group", msg.clone())));
                            }
                            self.fail_fast(msg);
                            return;
                        }
                    }
                }
                Err(err) => {
                    self.finish_wal_inflight_group();
                    let msg = err.to_string();
                    for req in &group {
                        req.complete(Err(GoatError::internal("write_group", msg.clone())));
                    }
                    self.fail_fast(msg);
                    return;
                }
            }
        }
    }

    fn run_mem_leader_loop_if_needed(&self) {
        // 机会式辅助路径：若没有 mem leader，提交线程可帮忙清空 mem queue。
        let should_run = {
            let mut state = self.state.lock().unwrap();
            if state.mem_leader_active || state.mem_queue.is_empty() {
                false
            } else {
                state.mem_leader_active = true;
                true
            }
        };

        if should_run {
            self.run_mem_leader_loop();
        }
    }

    fn run_mem_leader_loop(&self) {
        // Mem leader 循环：
        //   1) 弹出一个 mem group；
        //   2) 将准备好的内部记录写入 mutable memtable；
        //   3) 完成对应用户请求；
        //   4) 若 memtable 满则请求 flush。
        loop {
            let group = self.pop_next_mem_group();
            if group.is_empty() {
                return;
            }

            let mem_result = self.apply_mem_group(&group);
            match mem_result {
                Ok((needs_flush, max_seq)) => {
                    if let Some(max_seq) = max_seq {
                        self.publish_sequence(max_seq);
                    }

                    // 请求只有在 Mem 阶段完成后才算对用户成功可见。
                    for req in &group {
                        req.complete(Ok(()));
                    }
                    self.finish_mem_inflight_group();
                    if needs_flush {
                        self.flush_requested.store(true, Ordering::Release);
                    }
                }
                Err(err) => {
                    self.finish_mem_inflight_group();
                    let msg = err.to_string();
                    for req in &group {
                        req.complete(Err(GoatError::internal("write_group", msg.clone())));
                    }
                    self.fail_fast(msg);
                    return;
                }
            }
        }
    }

    fn pop_next_wal_group(&self) -> Vec<Arc<WriteRequest>> {
        let mut state = self.state.lock().unwrap();

        if state.wal_queue.is_empty() {
            // 无剩余工作：释放 WAL leader 角色，后续提交者可重新接管。
            state.wal_leader_active = false;
            self.state_cv.notify_all();
            return Vec::new();
        }

        let wait_us = self.options.wal_group_wait_us;
        if wait_us > 0 && state.wal_queue.len() == 1 {
            // 微等待窗口：短暂等待更多请求，提升 group 合并率。
            let timeout = Duration::from_micros(wait_us);
            state = self.state_cv.wait_timeout(state, timeout).unwrap().0;
            if state.wal_queue.is_empty() {
                state.wal_leader_active = false;
                self.state_cv.notify_all();
                return Vec::new();
            }
        }

        let max_ops = self.options.wal_max_group_ops.max(1);
        let max_bytes = self.options.wal_max_group_bytes.max(1);
        let mut group = Vec::new();
        let mut ops_total = 0usize;
        let mut bytes_total = 0usize;

        while let Some(req) = state.wal_queue.front() {
            let req_ops = req.ops_len;
            let req_bytes = req.approx_bytes;
            let can_take = group.is_empty()
                || (ops_total + req_ops <= max_ops && bytes_total + req_bytes <= max_bytes);
            if !can_take {
                // 保持 FIFO 公平性：遇到第一个超限请求即停止组包。
                break;
            }

            let req = state.wal_queue.pop_front().unwrap();
            state.wal_queue_reqs = state.wal_queue_reqs.saturating_sub(1);
            state.wal_queue_bytes = state.wal_queue_bytes.saturating_sub(req.approx_bytes);
            ops_total = ops_total.saturating_add(req_ops);
            bytes_total = bytes_total.saturating_add(req_bytes);
            group.push(req);
        }

        if !group.is_empty() {
            // 记录 WAL inflight group，供 flush 屏障判断是否已排空。
            state.wal_inflight_groups = state.wal_inflight_groups.saturating_add(1);
        }
        self.state_cv.notify_all();
        group
    }

    fn pop_next_mem_group(&self) -> Vec<Arc<WriteRequest>> {
        let mut state = self.state.lock().unwrap();

        if state.mem_queue.is_empty() {
            // 无 mem 工作：释放 mem leader 角色。
            state.mem_leader_active = false;
            self.state_cv.notify_all();
            return Vec::new();
        }

        let wait_us = self.options.mem_group_wait_us;
        if wait_us > 0 && state.mem_queue.len() == 1 {
            // 可选的 mem 阶段微批量等待。
            let timeout = Duration::from_micros(wait_us);
            state = self.state_cv.wait_timeout(state, timeout).unwrap().0;
            if state.mem_queue.is_empty() {
                state.mem_leader_active = false;
                self.state_cv.notify_all();
                return Vec::new();
            }
        }

        let max_ops = self.options.mem_max_group_ops.max(1);
        let max_bytes = self.options.mem_max_group_bytes.max(1);
        let mut group = Vec::new();
        let mut ops_total = 0usize;
        let mut bytes_total = 0usize;

        while let Some(req) = state.mem_queue.front() {
            let req_ops = req.ops_len;
            let req_bytes = req.approx_bytes;
            let can_take = group.is_empty()
                || (ops_total + req_ops <= max_ops && bytes_total + req_bytes <= max_bytes);
            if !can_take {
                break;
            }

            let req = state.mem_queue.pop_front().unwrap();
            state.mem_queue_reqs = state.mem_queue_reqs.saturating_sub(1);
            state.mem_queue_bytes = state.mem_queue_bytes.saturating_sub(req.approx_bytes);
            ops_total = ops_total.saturating_add(req_ops);
            bytes_total = bytes_total.saturating_add(req_bytes);
            group.push(req);
        }

        if !group.is_empty() {
            // 记录 Mem inflight group，供 flush 屏障判断是否已排空。
            state.mem_inflight_groups = state.mem_inflight_groups.saturating_add(1);
        }
        self.state_cv.notify_all();
        group
    }

    fn apply_wal_group(&self, group: &[Arc<WriteRequest>]) -> GoatResult<()> {
        // 阶段 A：消费每个请求里的用户操作。
        let mut ops_groups = Vec::with_capacity(group.len());
        let mut total_ops = 0u64;
        for req in group {
            let ops = req.take_ops();
            total_ops = total_ops.saturating_add(ops.len() as u64);
            ops_groups.push(ops);
        }

        if total_ops == 0 {
            // 维持流水线不变量：进入 Mem 阶段的请求必须携带 payload。
            for req in group {
                req.set_mem_payload(MemApplyPayload {
                    records: Vec::new(),
                    max_seq: None,
                });
            }
            return Ok(());
        }

        // 阶段 B：为整个 group 分配连续序列号，并将
        // WriteOp 转为 WAL/Mem 阶段共用的 (InternalKey, value)。
        let mut wal_records = Vec::with_capacity(total_ops as usize);
        let mut seq = self
            .sequence_number
            .try_allocate_range(total_ops, SEQUENCE_NUMBER_MAX)
            .ok_or_else(|| {
                GoatError::unavailable("sequence_number", "sequence number exhausted")
            })?;
        for (req, ops) in group.iter().zip(ops_groups.into_iter()) {
            let mut req_records = Vec::with_capacity(ops.len());
            let mut req_max_seq = None;
            for op in ops {
                let record = match op {
                    WriteOp::Put(key, value) => (
                        InternalKey::new(key, seq, InternalKeyKind::Put),
                        Bytes::from(value),
                    ),
                    WriteOp::Delete(key) => (
                        InternalKey::new(key, seq, InternalKeyKind::Delete),
                        Bytes::new(),
                    ),
                };
                req_max_seq = Some(seq);
                wal_records.push(record.clone());
                req_records.push(record);
                seq = seq.checked_add(1).ok_or_else(|| {
                    GoatError::unavailable("sequence_number", "sequence overflow")
                })?;
            }

            // 保存每个请求的 Mem 阶段载荷。
            req.set_mem_payload(MemApplyPayload {
                records: req_records,
                max_seq: req_max_seq,
            });
        }

        // 与 flush 轮转协调：flush 持写锁，普通写持读锁。
        let _gate = self.write_gate.read().unwrap();
        self.wal_writer.append_batch(&wal_records)?;
        Ok(())
    }

    fn enqueue_mem_group(&self, group: &[Arc<WriteRequest>]) -> GoatResult<bool> {
        let mut state = self.state.lock().unwrap();

        for req in group {
            loop {
                // 在交接到 Mem 阶段时，writer 可能已关闭/故障。
                if let Some(reason) = state.closed_reason.clone() {
                    if matches!(reason, CloseReason::Failed(_)) {
                        return Err(Self::close_reason_to_error(reason));
                    }
                }
                if !self.mem_queue_backpressured(&state, req.approx_bytes) {
                    break;
                }

                // 如果 MemQueue 已满且没有 mem leader，本线程必须临时接管
                // mem leader 以避免系统停顿。
                if !state.mem_leader_active && !state.mem_queue.is_empty() {
                    state.mem_leader_active = true;
                    drop(state);
                    self.run_mem_leader_loop();
                    state = self.state.lock().unwrap();
                    continue;
                }

                state = self.state_cv.wait(state).unwrap();
            }

            state.mem_queue.push_back(req.clone());
            state.mem_queue_reqs = state.mem_queue_reqs.saturating_add(1);
            state.mem_queue_bytes = state.mem_queue_bytes.saturating_add(req.approx_bytes);
        }

        // 队列有活且没有 leader 时，由调用方启动 mem leader。
        let mut start_mem_leader = false;
        if !state.mem_leader_active && !state.mem_queue.is_empty() {
            state.mem_leader_active = true;
            start_mem_leader = true;
        }
        self.state_cv.notify_all();
        Ok(start_mem_leader)
    }

    fn apply_mem_group(&self, group: &[Arc<WriteRequest>]) -> GoatResult<(bool, Option<u64>)> {
        // 与 WAL 相同的 gate 约束：flush 持写锁，写入持读锁。
        let _gate = self.write_gate.read().unwrap();
        let mut max_seq = None;
        let state = self.lsm_state.read().unwrap();

        for req in group {
            let payload = req.take_mem_payload().ok_or_else(|| {
                GoatError::internal("mem_group", "missing mem apply payload after WAL stage")
            })?;

            // 按请求内顺序将记录写入 mutable memtable。
            for (internal_key, value) in payload.records {
                state.mem_table.put(internal_key, value);
            }
            if let Some(req_max_seq) = payload.max_seq {
                max_seq = Some(max_seq.map_or(req_max_seq, |cur: u64| cur.max(req_max_seq)));
            }
        }

        // 返回值用于提示调用方是否应异步触发 flush。
        let needs_flush = state.mem_table.should_flush();
        Ok((needs_flush, max_seq))
    }

    fn finish_wal_inflight_group(&self) {
        // 与 pop_next_wal_group 中的 inflight 增量配对。
        let mut state = self.state.lock().unwrap();
        state.wal_inflight_groups = state.wal_inflight_groups.saturating_sub(1);
        self.state_cv.notify_all();
    }

    fn finish_mem_inflight_group(&self) {
        // 与 pop_next_mem_group 中的 inflight 增量配对。
        let mut state = self.state.lock().unwrap();
        state.mem_inflight_groups = state.mem_inflight_groups.saturating_sub(1);
        self.state_cv.notify_all();
    }

    fn publish_sequence(&self, seq: u64) {
        // 单调 CAS 最大值更新：
        // 保留“已完成 Mem 阶段”的最高序列号。
        let mut current = self.last_published_seq.load(Ordering::Acquire);
        while seq > current {
            match self.last_published_seq.compare_exchange(
                current,
                seq,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(new_current) => current = new_current,
            }
        }
    }

    fn fail_fast(&self, message: String) {
        // 终止失败路径：
        // - 关闭 writer，拒绝新写入；
        // - 清理屏障标记，释放被阻塞提交线程；
        // - 失败掉所有排队请求。
        let pending = {
            let mut state = self.state.lock().unwrap();
            if matches!(state.closed_reason, Some(CloseReason::Failed(_))) {
                return;
            }
            state.closed_reason = Some(CloseReason::Failed(message.clone()));
            state.flush_blocked = false;
            let pending = Self::drain_all_queued(&mut state);
            self.state_cv.notify_all();
            pending
        };

        for req in pending {
            req.complete(Err(GoatError::internal("write_group", message.clone())));
        }
    }

    fn wal_queue_backpressured(&self, state: &WriteState, incoming_bytes: usize) -> bool {
        // 字节阈值仅在队列非空时生效，保证单个大请求仍可穿透以维持前进性。
        let max_reqs = self.options.max_wal_queue_reqs.max(1);
        let max_bytes = self.options.max_wal_queue_bytes.max(1);
        state.wal_queue_reqs >= max_reqs
            || (state.wal_queue_reqs > 0
                && state.wal_queue_bytes.saturating_add(incoming_bytes) > max_bytes)
    }

    fn mem_queue_backpressured(&self, state: &WriteState, incoming_bytes: usize) -> bool {
        // 与 WAL 队列相同的背压策略。
        let max_reqs = self.options.max_mem_queue_reqs.max(1);
        let max_bytes = self.options.max_mem_queue_bytes.max(1);
        state.mem_queue_reqs >= max_reqs
            || (state.mem_queue_reqs > 0
                && state.mem_queue_bytes.saturating_add(incoming_bytes) > max_bytes)
    }

    fn flush_backpressure_error(&self) -> Option<GoatError> {
        let lsm_state = self.lsm_state.read().unwrap();
        if lsm_state.flush_circuit_open {
            return Some(GoatError::unavailable(
                "write_backpressure",
                format!(
                    "flush circuit open after {} consecutive flush failures",
                    lsm_state.flush_failure_streak
                ),
            ));
        }

        let immutable_limit = self.options.max_immutable_memtables.max(1);
        let immutable_count = lsm_state.immutable_mem_tables.len();
        if immutable_count >= immutable_limit {
            return Some(GoatError::unavailable(
                "write_backpressure",
                format!(
                    "immutable memtable backlog {} reached limit {}",
                    immutable_count, immutable_limit
                ),
            ));
        }

        None
    }

    fn write_pressure_action(&self) -> WritePressureAction {
        let lsm_state = self.lsm_state.read().unwrap();
        let version = &lsm_state.version;

        let l0_files = version.get_files(0).len();
        let l0_slowdown = self.options.l0_slowdown_writes_trigger.max(1);
        let l0_stop = self.options.l0_stop_writes_trigger.max(l0_slowdown);
        let pending_compaction_bytes = self.estimated_pending_compaction_bytes(version);
        let soft_limit = self.options.soft_pending_compaction_bytes_limit.max(1);
        let hard_limit = self
            .options
            .hard_pending_compaction_bytes_limit
            .max(soft_limit);
        let l0_stop_triggered = l0_files >= l0_stop;
        let pending_stop_triggered = pending_compaction_bytes >= hard_limit;
        let l0_slowdown_triggered = l0_files >= l0_slowdown;
        let pending_slowdown_triggered = pending_compaction_bytes >= soft_limit;

        if l0_stop_triggered {
            self.observe_write_pressure_transition(
                WritePressureLevel::Stop,
                "l0_stop_trigger",
                l0_files,
                pending_compaction_bytes,
            );
            return WritePressureAction::Stop(GoatError::unavailable(
                "write_backpressure",
                format!(
                    "L0 file count {} reached stop trigger {}",
                    l0_files, l0_stop
                ),
            ));
        }

        if pending_stop_triggered {
            self.observe_write_pressure_transition(
                WritePressureLevel::Stop,
                "pending_compaction_hard_limit",
                l0_files,
                pending_compaction_bytes,
            );
            return WritePressureAction::Stop(GoatError::unavailable(
                "write_backpressure",
                format!(
                    "pending compaction bytes {} reached hard limit {}",
                    pending_compaction_bytes, hard_limit
                ),
            ));
        }

        if l0_slowdown_triggered || pending_slowdown_triggered {
            let reason = match (l0_slowdown_triggered, pending_slowdown_triggered) {
                (true, true) => "l0_and_pending_compaction_slowdown",
                (true, false) => "l0_slowdown_trigger",
                (false, true) => "pending_compaction_soft_limit",
                (false, false) => "unknown",
            };
            self.observe_write_pressure_transition(
                WritePressureLevel::Slowdown,
                reason,
                l0_files,
                pending_compaction_bytes,
            );
            return WritePressureAction::Slowdown {
                delay: Duration::from_millis(self.options.write_slowdown_delay_ms.max(1)),
            };
        }

        self.observe_write_pressure_transition(
            WritePressureLevel::Normal,
            "below_thresholds",
            l0_files,
            pending_compaction_bytes,
        );
        WritePressureAction::Allow
    }

    fn observe_write_pressure_transition(
        &self,
        next_level: WritePressureLevel,
        reason: &'static str,
        l0_files: usize,
        pending_compaction_bytes: u64,
    ) {
        let next_raw = next_level.as_u8();
        let current_raw = self.write_pressure_level.load(Ordering::Acquire);
        if current_raw == next_raw {
            return;
        }

        if self
            .write_pressure_level
            .compare_exchange(current_raw, next_raw, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let previous_level = WritePressureLevel::from_u8(current_raw);
        match next_level {
            WritePressureLevel::Normal => debug!(
                previous = ?previous_level,
                reason,
                l0_files,
                pending_compaction_bytes,
                "write pressure recovered to normal"
            ),
            WritePressureLevel::Slowdown => warn!(
                previous = ?previous_level,
                reason,
                l0_files,
                pending_compaction_bytes,
                "write pressure entered slowdown state"
            ),
            WritePressureLevel::Stop => warn!(
                previous = ?previous_level,
                reason,
                l0_files,
                pending_compaction_bytes,
                "write pressure entered stop state"
            ),
        }
    }

    #[cfg(test)]
    fn observed_write_pressure_level_for_test(&self) -> WritePressureLevel {
        WritePressureLevel::from_u8(self.write_pressure_level.load(Ordering::Acquire))
    }

    fn estimated_pending_compaction_bytes(
        &self,
        version: &crate::goatkv::metadata::version::Version,
    ) -> u64 {
        let num_levels = version.num_levels();
        if num_levels <= 1 {
            return 0;
        }

        let base = self.options.compaction_max_bytes_for_level_base.max(1);
        let multiplier = self
            .options
            .compaction_max_bytes_for_level_multiplier
            .max(2);
        let mut level_target = base;
        let mut pending = 0u64;

        // L1+ debt
        for level in 1..num_levels {
            let level_size = version.get_level_size(level);
            if level_size > level_target {
                pending = pending.saturating_add(level_size - level_target);
            }
            level_target = level_target.saturating_mul(multiplier);
        }

        // L0 debt (use level-0 size as coarse signal once file count exceeds trigger).
        if version.get_files(0).len() > self.options.l0_compaction_file_trigger.max(1) {
            pending = pending.saturating_add(version.get_level_size(0));
        }

        pending
    }

    fn drain_all_queued(state: &mut WriteState) -> Vec<Arc<WriteRequest>> {
        // close/fail_fast 共用：重置状态并清空两个队列。
        state.wal_queue_reqs = 0;
        state.wal_queue_bytes = 0;
        state.mem_queue_reqs = 0;
        state.mem_queue_bytes = 0;
        let mut pending = state.wal_queue.drain(..).collect::<Vec<_>>();
        pending.extend(state.mem_queue.drain(..));
        pending
    }

    fn close_reason_to_error(reason: CloseReason) -> GoatError {
        // 将内部关闭原因归一化为对外错误。
        match reason {
            CloseReason::Manual => {
                GoatError::unavailable("write_coordinator", "write coordinator closed")
            }
            CloseReason::Failed(message) => GoatError::internal("write_coordinator", message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{KvWriter, WriteOp};
    use crate::goatkv::core::lsm_state::{ImmutableMemTableEntry, LSMState};
    use crate::goatkv::core::mem_table::{ImmutableMemTable, MemTable};
    use crate::goatkv::core::sequence_number::SequenceNumber;
    use crate::goatkv::error::ErrorKind;
    use crate::goatkv::format::internal_key::{InternalKey, InternalKeyKind, SEQUENCE_NUMBER_MAX};
    use crate::goatkv::metadata::file_metadata::{FileMetadata, TableProperties};
    use crate::goatkv::metadata::version::Version;
    use crate::goatkv::storage::wal::{WalWriter, WalWriterConfig};
    use crate::goatkv::utils::options::KvEngineOptions;
    use crate::goatkv::utils::paths::SstablePaths;
    use std::sync::{Arc, RwLock};
    use tempfile::NamedTempFile;
    use tokio::sync::mpsc::unbounded_channel;

    fn build_writer_with_start(start: u64) -> KvWriter {
        let (writer, _lsm_state) = build_writer_with_options(start, KvEngineOptions::for_test());
        writer
    }

    fn build_writer_with_options(
        start: u64,
        options: KvEngineOptions,
    ) -> (KvWriter, Arc<RwLock<LSMState>>) {
        let wal_file = NamedTempFile::new().expect("create temp wal");
        let wal_writer = WalWriter::new(
            wal_file.path().to_path_buf(),
            WalWriterConfig { wal_sync: false },
        )
        .expect("open wal writer");
        let mem = Arc::new(MemTable::new(1024 * 1024));
        let sstable_paths = Arc::new(SstablePaths::new(
            std::env::temp_dir().join("goatdb_writer_test_data"),
            std::env::temp_dir().join("goatdb_writer_test_tmp"),
        ));
        let version = Arc::new(Version::new(7, sstable_paths));
        let lsm_state = Arc::new(RwLock::new(LSMState::new(mem, version)));
        let write_gate = Arc::new(RwLock::new(()));
        let writer = KvWriter::new(
            Arc::new(wal_writer),
            Arc::new(SequenceNumber::with_start(start)),
            lsm_state.clone(),
            write_gate,
            Arc::new(options),
        );
        (writer, lsm_state)
    }

    fn make_test_file(
        file_id: u64,
        smallest: &[u8],
        largest: &[u8],
        size_bytes: u64,
    ) -> Arc<FileMetadata> {
        let smallest_key =
            InternalKey::new(smallest.to_vec(), 100, InternalKeyKind::Put).serialize();
        let largest_key = InternalKey::new(largest.to_vec(), 90, InternalKeyKind::Put).serialize();
        let props = TableProperties::new(size_bytes, smallest_key, largest_key, 90, 100);
        let (tx, _rx) = unbounded_channel();
        Arc::new(FileMetadata::from_props(file_id, props, tx))
    }

    fn install_version(lsm_state: &Arc<RwLock<LSMState>>, files: Vec<Vec<Arc<FileMetadata>>>) {
        let sstable_paths = Arc::new(SstablePaths::new(
            std::env::temp_dir().join("goatdb_writer_test_ver_data"),
            std::env::temp_dir().join("goatdb_writer_test_ver_tmp"),
        ));
        lsm_state.write().unwrap().version = Arc::new(Version::from_files(files, 0, sstable_paths));
    }

    #[test]
    fn sequence_overflow_returns_error_instead_of_panic() {
        let writer = build_writer_with_start(SEQUENCE_NUMBER_MAX);
        let result = writer.submit_write(
            vec![
                WriteOp::Put(b"k1".to_vec(), b"v1".to_vec()),
                WriteOp::Put(b"k2".to_vec(), b"v2".to_vec()),
            ],
            || {},
        );

        assert!(result.is_err());
        let err = result.expect_err("overflow write should fail");
        assert!(matches!(
            err.kind(),
            ErrorKind::Internal | ErrorKind::Unavailable
        ));
    }

    #[test]
    fn submit_write_fails_fast_when_immutable_backlog_reaches_limit() {
        let options = KvEngineOptions::for_test().with_max_immutable_memtables(1);
        let (writer, lsm_state) = build_writer_with_options(1, options);

        let source = MemTable::new(1024);
        source.put(
            InternalKey::new(b"k".to_vec(), 1, InternalKeyKind::Put),
            b"v".as_ref().into(),
        );
        let immutable = Arc::new(ImmutableMemTable::new(source.inner()));
        lsm_state
            .write()
            .unwrap()
            .immutable_mem_tables
            .push_back(ImmutableMemTableEntry {
                table: immutable,
                wal_handle: None,
            });

        let err = writer
            .submit_write(vec![WriteOp::Put(b"k2".to_vec(), b"v2".to_vec())], || {})
            .expect_err("write should fail when immutable backlog reached limit");
        assert_eq!(err.kind(), ErrorKind::Unavailable);
        assert!(err.to_string().contains("immutable memtable backlog"));
    }

    #[test]
    fn submit_write_fails_fast_when_flush_circuit_is_open() {
        let (writer, lsm_state) = build_writer_with_options(1, KvEngineOptions::for_test());
        {
            let mut state = lsm_state.write().unwrap();
            state.flush_failure_streak = 3;
            state.flush_circuit_open = true;
        }

        let err = writer
            .submit_write(vec![WriteOp::Put(b"k3".to_vec(), b"v3".to_vec())], || {})
            .expect_err("write should fail when flush circuit is open");
        assert_eq!(err.kind(), ErrorKind::Unavailable);
        assert!(err.to_string().contains("flush circuit open"));
    }

    #[test]
    fn submit_write_fails_fast_when_l0_reaches_stop_trigger() {
        let options = KvEngineOptions::for_test()
            .with_l0_slowdown_writes_trigger(2)
            .with_l0_stop_writes_trigger(3);
        let (writer, lsm_state) = build_writer_with_options(1, options);

        let mut files = vec![Vec::new(); 7];
        files[0].push(make_test_file(1, b"a", b"b", 8 * 1024));
        files[0].push(make_test_file(2, b"c", b"d", 8 * 1024));
        files[0].push(make_test_file(3, b"e", b"f", 8 * 1024));
        install_version(&lsm_state, files);

        let err = writer
            .submit_write(vec![WriteOp::Put(b"k4".to_vec(), b"v4".to_vec())], || {})
            .expect_err("write should fail when L0 file count reaches stop trigger");
        assert_eq!(err.kind(), ErrorKind::Unavailable);
        assert!(err.to_string().contains("L0 file count"));
        assert_eq!(
            writer.observed_write_pressure_level_for_test(),
            super::WritePressureLevel::Stop
        );
    }

    #[test]
    fn write_pressure_action_reports_slowdown_before_stop() {
        let options = KvEngineOptions::for_test()
            .with_l0_slowdown_writes_trigger(2)
            .with_l0_stop_writes_trigger(4)
            .with_write_slowdown_delay_ms(1);
        let (writer, lsm_state) = build_writer_with_options(1, options);

        let mut files = vec![Vec::new(); 7];
        files[0].push(make_test_file(10, b"a", b"b", 8 * 1024));
        files[0].push(make_test_file(11, b"c", b"d", 8 * 1024));
        install_version(&lsm_state, files);

        assert!(matches!(
            writer.write_pressure_action(),
            super::WritePressureAction::Slowdown { .. }
        ));
        assert_eq!(
            writer.observed_write_pressure_level_for_test(),
            super::WritePressureLevel::Slowdown
        );

        install_version(&lsm_state, vec![Vec::new(); 7]);
        assert!(matches!(
            writer.write_pressure_action(),
            super::WritePressureAction::Allow
        ));
        assert_eq!(
            writer.observed_write_pressure_level_for_test(),
            super::WritePressureLevel::Normal
        );
    }

    #[test]
    fn submit_write_fails_fast_when_pending_compaction_bytes_reaches_hard_limit() {
        let options = KvEngineOptions::for_test()
            .with_hard_pending_compaction_bytes_limit(1)
            .with_soft_pending_compaction_bytes_limit(1);
        let (writer, lsm_state) = build_writer_with_options(1, options);

        let mut files = vec![Vec::new(); 7];
        // level-1 target default is 64KB, put 256KB to guarantee pending compaction debt > 0
        files[1].push(make_test_file(21, b"aa", b"zz", 256 * 1024));
        install_version(&lsm_state, files);

        let err = writer
            .submit_write(vec![WriteOp::Put(b"k5".to_vec(), b"v5".to_vec())], || {})
            .expect_err("write should fail when pending compaction bytes reach hard limit");
        assert_eq!(err.kind(), ErrorKind::Unavailable);
        assert!(err.to_string().contains("pending compaction bytes"));
        assert_eq!(
            writer.observed_write_pressure_level_for_test(),
            super::WritePressureLevel::Stop
        );
    }
}
