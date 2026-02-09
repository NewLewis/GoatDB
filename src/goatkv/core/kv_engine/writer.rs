use std::collections::VecDeque;
use std::io;
use std::mem;
use std::sync::{Arc, Condvar, Mutex, RwLock};

use bytes::Bytes;

use crate::goatkv::core::lsm_state::LSMState;
use crate::goatkv::core::sequence_number::SequenceNumber;
use crate::goatkv::format::internal_key::{InternalKey, InternalKeyKind};
use crate::goatkv::storage::wal::WalManager;
use crate::goatkv::utils::options::KvEngineOptions;

const MAX_WRITE_GROUP_OPS: usize = 4096;

#[derive(Debug)]
pub(crate) enum WriteOp {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

#[derive(Debug)]
struct WriteRequest {
    ops: Mutex<Vec<WriteOp>>,
    ops_len: usize,
    approx_bytes: usize,
    result: Mutex<Option<Result<(), String>>>,
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
            ops_len,
            approx_bytes,
            result: Mutex::new(None),
            cv: Condvar::new(),
        }
    }

    fn take_ops(&self) -> Vec<WriteOp> {
        let mut guard = self.ops.lock().unwrap();
        mem::take(&mut *guard)
    }

    fn complete(&self, result: Result<(), String>) {
        let mut guard = self.result.lock().unwrap();
        *guard = Some(result);
        self.cv.notify_one();
    }

    fn wait(&self) -> io::Result<()> {
        let mut guard = self.result.lock().unwrap();
        while guard.is_none() {
            guard = self.cv.wait(guard).unwrap();
        }
        match guard.take().unwrap() {
            Ok(()) => Ok(()),
            Err(msg) => Err(io::Error::other(msg)),
        }
    }
}

#[derive(Debug)]
struct WriteState {
    queue: VecDeque<Arc<WriteRequest>>,
    leader_active: bool,
    closed: bool,
}

#[derive(Debug)]
pub struct KvWriter {
    wal_manager: Arc<WalManager>,
    sequence_number: Arc<SequenceNumber>,
    lsm_state: Arc<RwLock<LSMState>>,
    write_gate: Arc<RwLock<()>>,
    write_state: Mutex<WriteState>,
    options: Arc<KvEngineOptions>,
}

impl KvWriter {
    pub fn new(
        wal_manager: Arc<WalManager>,
        sequence_number: Arc<SequenceNumber>,
        lsm_state: Arc<RwLock<LSMState>>,
        write_gate: Arc<RwLock<()>>,
        options: Arc<KvEngineOptions>,
    ) -> Self {
        Self {
            wal_manager,
            sequence_number,
            lsm_state,
            write_gate,
            write_state: Mutex::new(WriteState {
                queue: VecDeque::new(),
                leader_active: false,
                closed: false,
            }),
            options,
        }
    }

    pub(crate) fn submit_write<F>(&self, ops: Vec<WriteOp>, flush_fn: F) -> io::Result<()>
    where
        F: Fn(),
    {
        let request = Arc::new(WriteRequest::new(ops));
        let mut is_leader = false;
        {
            let mut state = self.write_state.lock().unwrap();
            if state.closed {
                return Err(io::Error::other("write coordinator closed"));
            }
            state.queue.push_back(request.clone());
            if !state.leader_active {
                state.leader_active = true;
                is_leader = true;
            }
        }

        if is_leader {
            self.process_write_groups(&flush_fn);
        }

        request.wait()
    }

    fn process_write_groups<F>(&self, flush_fn: &F)
    where
        F: Fn(),
    {
        loop {
            let group = self.pop_next_write_group();
            if group.is_empty() {
                return;
            }

            let result = self.apply_write_group(&group);
            let result_msg = result.as_ref().err().map(|e| e.to_string());
            for req in &group {
                match &result_msg {
                    Some(msg) => req.complete(Err(msg.clone())),
                    None => req.complete(Ok(())),
                }
            }

            if let Some(msg) = result_msg {
                let remaining = {
                    let mut state = self.write_state.lock().unwrap();
                    state.closed = true;
                    let drained = state.queue.drain(..).collect::<Vec<_>>();
                    state.leader_active = false;
                    drained
                };
                for req in remaining {
                    req.complete(Err(msg.clone()));
                }
                return;
            }

            if result.unwrap() {
                flush_fn();
            }
        }
    }

    fn pop_next_write_group(&self) -> Vec<Arc<WriteRequest>> {
        let mut state = self.write_state.lock().unwrap();

        // No queued requests means this leader can stand down. We clear the
        // leader flag here so the next enqueued writer can become the leader.
        if state.queue.is_empty() {
            state.leader_active = false;
            return Vec::new();
        }

        let max_ops = MAX_WRITE_GROUP_OPS;
        let max_bytes = self.options.wal_max_buffer_bytes;
        let mut group = Vec::new();
        let mut ops_total = 0usize;
        let mut bytes_total = 0usize;

        // Build a group by draining from the front of the queue while keeping
        // the total work under both an operation-count limit and a byte-budget.
        // This batches small writes together for throughput, but stops when the
        // next request would push the group over either threshold.
        while let Some(req) = state.queue.front() {
            let req_ops = req.ops_len;
            let req_bytes = req.approx_bytes;

            let can_take = group.is_empty()
                || (ops_total + req_ops <= max_ops && bytes_total + req_bytes <= max_bytes);
            if !can_take {
                break;
            }

            // Safe unwrap: we just observed a front element.
            let req = state.queue.pop_front().unwrap();
            ops_total = ops_total.saturating_add(req_ops);
            bytes_total = bytes_total.saturating_add(req_bytes);
            group.push(req);
        }

        group
    }

    fn apply_write_group(&self, group: &[Arc<WriteRequest>]) -> io::Result<bool> {
        let _gate = self.write_gate.read().unwrap();

        let (ops_groups, total_ops) = Self::collect_ops(group);
        if total_ops == 0 {
            return Ok(false);
        }

        let mut records = Vec::with_capacity(total_ops as usize);
        let mut seq = self.sequence_number.allocate_range(total_ops);
        for ops in ops_groups {
            seq = Self::append_ops(&mut records, ops, seq);
        }

        self.wal_manager.append_batch(&records)?;

        let needs_flush = self.apply_records_to_memtable(records);
        drop(_gate);

        Ok(needs_flush)
    }

    fn collect_ops(group: &[Arc<WriteRequest>]) -> (Vec<Vec<WriteOp>>, u64) {
        let mut ops_groups = Vec::with_capacity(group.len());
        let mut total_ops = 0u64;
        for req in group {
            let ops = req.take_ops();
            total_ops += ops.len() as u64;
            ops_groups.push(ops);
        }
        (ops_groups, total_ops)
    }

    fn append_ops(records: &mut Vec<(InternalKey, Bytes)>, ops: Vec<WriteOp>, mut seq: u64) -> u64 {
        for op in ops {
            match op {
                WriteOp::Put(key, value) => {
                    let internal_key = InternalKey::new(key, seq, InternalKeyKind::Put);
                    records.push((internal_key, Bytes::from(value)));
                }
                WriteOp::Delete(key) => {
                    let internal_key = InternalKey::new(key, seq, InternalKeyKind::Delete);
                    records.push((internal_key, Bytes::new()));
                }
            }
            seq = seq.saturating_add(1);
        }
        seq
    }

    fn apply_records_to_memtable(&self, records: Vec<(InternalKey, Bytes)>) -> bool {
        let guard = self.lsm_state.read().unwrap();
        for (internal_key, value) in records {
            guard.mem_table.put(internal_key, value);
        }
        guard.mem_table.should_flush()
    }
}
