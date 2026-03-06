use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use goat_db::goatkv::storage::sstable::SSTableReader;
use goat_db::goatkv::storage::sstable::SstableBlockCompression;
use goat_db::goatkv::utils::init_logging;
use goat_db::goatkv::{ErrorKind, KvEngine, KvEngineOptions};
#[cfg(feature = "rocksdb")]
use rocksdb::{DBCompressionType, IteratorMode, Options, WriteBatch, WriteOptions, DB};

#[derive(Parser)]
#[command(name = "goatkv_bench")]
#[command(about = "Benchmark for GoatKV")]
#[command(version = "0.1.0")]
struct Cli {
    /// Cargo bench passes --bench to the binary; accept and ignore it.
    #[arg(long, hide = true, global = true)]
    bench: bool,

    /// Database directory
    #[arg(long)]
    directory: PathBuf,

    /// Threads
    #[arg(long, default_value_t = 8)]
    threads: usize,

    /// WAL sync
    #[arg(long, default_value_t = false)]
    wal_sync: bool,

    /// WAL preallocation bytes for GoatKV (0 disables)
    #[arg(long, default_value_t = 0)]
    wal_preallocate_bytes: u64,

    /// WAL periodic sync bytes for GoatKV (0 disables)
    #[arg(long, default_value_t = 0)]
    wal_bytes_per_sync: u64,

    /// Engine to run
    #[arg(long, value_enum, default_value_t = EngineKind::Goatkv)]
    engine: EngineKind,

    /// Table cache entries for GoatKV (0 disables table cache)
    #[arg(long, default_value_t = 64)]
    table_cache_capacity: usize,

    /// Block cache size for GoatKV in MB (0 disables block cache)
    #[arg(long, default_value_t = 64)]
    block_cache_capacity_mb: usize,

    /// Row cache size for GoatKV in MB (0 disables row cache)
    #[arg(long, default_value_t = 32)]
    row_cache_capacity_mb: usize,

    /// Filter cache size for GoatKV in MB (0 disables partitioned filter cache)
    #[arg(long, default_value_t = 16)]
    filter_cache_capacity_mb: usize,

    /// Max subcompactions per compaction task for GoatKV
    #[arg(long, default_value_t = 1)]
    max_subcompactions: usize,

    /// L0 block compression for GoatKV
    #[arg(long, value_enum, default_value_t = BlockCompressionCli::None)]
    l0_compression: BlockCompressionCli,

    /// L1 block compression for GoatKV
    #[arg(long, value_enum, default_value_t = BlockCompressionCli::None)]
    l1_compression: BlockCompressionCli,

    /// L2 block compression for GoatKV
    #[arg(long, value_enum, default_value_t = BlockCompressionCli::None)]
    l2_compression: BlockCompressionCli,

    /// Optional baseline ms_per_iter for regression gate
    #[arg(long)]
    baseline_ms_per_iter: Option<f64>,

    /// Optional baseline throughput (ops/s) for regression gate
    #[arg(long)]
    baseline_throughput_ops_per_sec: Option<f64>,

    /// Allowed regression percentage for baseline gate
    #[arg(long, default_value_t = 10.0)]
    regression_threshold_pct: f64,

    #[command(subcommand)]
    command: Commands,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum EngineKind {
    Goatkv,
    Rocksdb,
    Both,
}

impl EngineKind {
    fn label(self) -> &'static str {
        match self {
            EngineKind::Goatkv => "goatkv",
            EngineKind::Rocksdb => "rocksdb",
            EngineKind::Both => "both",
        }
    }
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum ScanMode {
    Iterator,
    ScanAll,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum BlockCompressionCli {
    None,
    Rle,
}

impl BlockCompressionCli {
    fn into_engine(self) -> SstableBlockCompression {
        match self {
            Self::None => SstableBlockCompression::None,
            Self::Rle => SstableBlockCompression::Rle,
        }
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Build a database with given keys
    Populate {
        /// Key numbers
        #[arg(long, default_value_t = 1024)]
        key_nums: u64,
        /// Pairs in one batch
        #[arg(long, default_value_t = 1000)]
        batch_size: u64,
        /// Value size
        #[arg(long, default_value_t = 1024)]
        value_size: usize,
        /// Write sequentially
        #[arg(long, default_value_t = true)]
        seq: bool,
    },
    /// Single put per operation (no batching)
    Singleput {
        /// Key numbers
        #[arg(long, default_value_t = 1024)]
        key_nums: u64,
        /// Value size
        #[arg(long, default_value_t = 1024)]
        value_size: usize,
        /// Write sequentially
        #[arg(long, default_value_t = true)]
        seq: bool,
    },
    /// Randomly read from database
    Randread {
        /// Read how many times
        #[arg(long, default_value_t = 5)]
        times: u64,
        /// Key numbers
        #[arg(long, default_value_t = 1024)]
        key_nums: u64,
        /// Value size (kept for parity, not used)
        #[arg(long, default_value_t = 1024)]
        value_size: usize,
    },
    /// Hotspot read benchmark (reads concentrated in hot set)
    Hotread {
        /// Read rounds
        #[arg(long, default_value_t = 20)]
        times: u64,
        /// Total keys in DB
        #[arg(long, default_value_t = 1024)]
        key_nums: u64,
        /// Hot key set size
        #[arg(long, default_value_t = 64)]
        hotset: u64,
    },
    /// Batch MultiGet benchmark
    Multiget {
        /// Batch rounds
        #[arg(long, default_value_t = 20)]
        times: u64,
        /// Total keys in DB
        #[arg(long, default_value_t = 1024)]
        key_nums: u64,
        /// Keys per batch request
        #[arg(long, default_value_t = 32)]
        batch_size: u64,
        /// Miss ratio percent [0,100]
        #[arg(long, default_value_t = 0)]
        miss_ratio: u64,
    },
    /// Full scan benchmark over all persisted SSTables/entries
    Scanread {
        /// Scan rounds
        #[arg(long, default_value_t = 5)]
        times: u64,
        /// GoatKV scan mode (rocksdb ignores this and always uses iterator)
        #[arg(long, value_enum, default_value_t = ScanMode::Iterator)]
        mode: ScanMode,
    },
}

struct BenchResult {
    engine: &'static str,
    workload: &'static str,
    total_ms: u128,
    iters: u64,
    ops: u64,
    ms_per_iter: f64,
    throughput_ops_per_sec: f64,
    latency_samples: u64,
    p95_ms: f64,
    p99_ms: f64,
    run_unix_ms: u128,
}

#[derive(Debug, Clone, Copy, Default)]
struct GoatkvWriteStats {
    submitted: u64,
    successful: u64,
    failed: u64,
    unavailable_errors: u64,
    thread_panics: u64,
}

impl GoatkvWriteStats {
    fn add(&mut self, other: Self) {
        self.submitted = self.submitted.saturating_add(other.submitted);
        self.successful = self.successful.saturating_add(other.successful);
        self.failed = self.failed.saturating_add(other.failed);
        self.unavailable_errors = self
            .unavailable_errors
            .saturating_add(other.unavailable_errors);
        self.thread_panics = self.thread_panics.saturating_add(other.thread_panics);
    }
}

#[cfg(feature = "rocksdb")]
#[derive(Debug, Clone, Copy, Default)]
struct RocksdbWriteStats {
    submitted: u64,
    successful: u64,
    failed: u64,
    thread_panics: u64,
}

#[cfg(feature = "rocksdb")]
impl RocksdbWriteStats {
    fn add(&mut self, other: Self) {
        self.submitted = self.submitted.saturating_add(other.submitted);
        self.successful = self.successful.saturating_add(other.successful);
        self.failed = self.failed.saturating_add(other.failed);
        self.thread_panics = self.thread_panics.saturating_add(other.thread_panics);
    }
}

#[cfg(not(feature = "rocksdb"))]
fn ensure_rocksdb_available() -> ! {
    tracing::error!("rocksdb support is disabled; rebuild with --features rocksdb");
    std::process::exit(2);
}

#[cfg(feature = "rocksdb")]
fn ensure_rocksdb_available() {}

fn split_range(total: u64, threads: usize, index: usize) -> (u64, u64) {
    if threads == 0 {
        return (0, 0);
    }
    let threads = threads as u64;
    let base = total / threads;
    let rem = total % threads;
    let idx = index as u64;
    let extra = if idx < rem { 1 } else { 0 };
    let start = idx * base + idx.min(rem);
    let end = start + base + extra;
    (start, end)
}

fn make_key(key_id: u64) -> Vec<u8> {
    format!("key_{:016}", key_id).into_bytes()
}

fn make_value(value_size: usize, key_id: u64) -> Vec<u8> {
    if value_size == 0 {
        return Vec::new();
    }
    let mut value = vec![b'v'; value_size];
    if value_size >= 8 {
        value[..8].copy_from_slice(&key_id.to_le_bytes());
    }
    value
}

fn populate_goatkv(
    engine: Arc<KvEngine>,
    key_nums: u64,
    batch_size: u64,
    value_size: usize,
    seq: bool,
    threads: usize,
) -> (GoatkvWriteStats, Vec<u64>) {
    if key_nums == 0 || threads == 0 {
        return (GoatkvWriteStats::default(), Vec::new());
    }
    let batch_size = batch_size.max(1);
    let mut handles = Vec::with_capacity(threads);

    for index in 0..threads {
        let engine = engine.clone();
        let (start, end) = split_range(key_nums, threads, index);
        let seed = 0x9e37_79b9_u64.wrapping_mul(index as u64 + 1);
        let handle = thread::spawn(move || {
            let mut rng = SmallRng::seed_from_u64(seed);
            let total = end.saturating_sub(start);
            let mut stats = GoatkvWriteStats::default();
            let mut latencies_us = Vec::new();
            if seq {
                let mut processed = 0u64;
                while processed < total {
                    let batch = (total - processed).min(batch_size);
                    let mut entries = Vec::with_capacity(batch as usize);
                    for offset in 0..batch {
                        let key_id = start + processed + offset;
                        let key = make_key(key_id);
                        let value = make_value(value_size, key_id);
                        entries.push((key, value));
                    }
                    stats.submitted = stats.submitted.saturating_add(batch);
                    let begin = Instant::now();
                    match engine.put_batch(entries) {
                        Ok(()) => {
                            stats.successful = stats.successful.saturating_add(batch);
                        }
                        Err(err) => {
                            stats.failed = stats.failed.saturating_add(batch);
                            if err.kind() == ErrorKind::Unavailable {
                                stats.unavailable_errors =
                                    stats.unavailable_errors.saturating_add(batch);
                            }
                        }
                    }
                    latencies_us.push(begin.elapsed().as_micros().min(u64::MAX as u128) as u64);
                    processed += batch;
                }
            } else {
                let mut remaining = total;
                while remaining > 0 {
                    let batch = remaining.min(batch_size);
                    let mut entries = Vec::with_capacity(batch as usize);
                    for _ in 0..batch {
                        let key_id = rng.gen_range(0..key_nums);
                        let key = make_key(key_id);
                        let value = make_value(value_size, key_id);
                        entries.push((key, value));
                    }
                    stats.submitted = stats.submitted.saturating_add(batch);
                    let begin = Instant::now();
                    match engine.put_batch(entries) {
                        Ok(()) => {
                            stats.successful = stats.successful.saturating_add(batch);
                        }
                        Err(err) => {
                            stats.failed = stats.failed.saturating_add(batch);
                            if err.kind() == ErrorKind::Unavailable {
                                stats.unavailable_errors =
                                    stats.unavailable_errors.saturating_add(batch);
                            }
                        }
                    }
                    latencies_us.push(begin.elapsed().as_micros().min(u64::MAX as u128) as u64);
                    remaining = remaining.saturating_sub(batch);
                }
            }
            (stats, latencies_us)
        });
        handles.push(handle);
    }

    let mut aggregate = GoatkvWriteStats::default();
    let mut aggregate_latencies = Vec::new();
    for handle in handles {
        match handle.join() {
            Ok((stats, latencies_us)) => {
                aggregate.add(stats);
                aggregate_latencies.extend(latencies_us);
            }
            Err(_) => aggregate.thread_panics = aggregate.thread_panics.saturating_add(1),
        }
    }
    (aggregate, aggregate_latencies)
}

fn singleput_goatkv(
    engine: Arc<KvEngine>,
    key_nums: u64,
    value_size: usize,
    seq: bool,
    threads: usize,
) -> GoatkvWriteStats {
    if key_nums == 0 || threads == 0 {
        return GoatkvWriteStats::default();
    }
    let mut handles = Vec::with_capacity(threads);

    for index in 0..threads {
        let engine = engine.clone();
        let (start, end) = split_range(key_nums, threads, index);
        let seed = 0x517c_c1b7_u64.wrapping_mul(index as u64 + 1);
        let handle = thread::spawn(move || {
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut stats = GoatkvWriteStats::default();
            if seq {
                for key_id in start..end {
                    let key = make_key(key_id);
                    let value = make_value(value_size, key_id);
                    stats.submitted = stats.submitted.saturating_add(1);
                    match engine.put(key, value) {
                        Ok(()) => {
                            stats.successful = stats.successful.saturating_add(1);
                        }
                        Err(err) => {
                            stats.failed = stats.failed.saturating_add(1);
                            if err.kind() == ErrorKind::Unavailable {
                                stats.unavailable_errors =
                                    stats.unavailable_errors.saturating_add(1);
                            }
                        }
                    }
                }
            } else {
                let mut remaining = end.saturating_sub(start);
                while remaining > 0 {
                    let key_id = rng.gen_range(0..key_nums);
                    let key = make_key(key_id);
                    let value = make_value(value_size, key_id);
                    stats.submitted = stats.submitted.saturating_add(1);
                    match engine.put(key, value) {
                        Ok(()) => {
                            stats.successful = stats.successful.saturating_add(1);
                        }
                        Err(err) => {
                            stats.failed = stats.failed.saturating_add(1);
                            if err.kind() == ErrorKind::Unavailable {
                                stats.unavailable_errors =
                                    stats.unavailable_errors.saturating_add(1);
                            }
                        }
                    }
                    remaining = remaining.saturating_sub(1);
                }
            }
            stats
        });
        handles.push(handle);
    }

    let mut aggregate = GoatkvWriteStats::default();
    for handle in handles {
        match handle.join() {
            Ok(stats) => aggregate.add(stats),
            Err(_) => aggregate.thread_panics = aggregate.thread_panics.saturating_add(1),
        }
    }
    aggregate
}

#[cfg(feature = "rocksdb")]
fn populate_rocksdb(
    db: Arc<DB>,
    key_nums: u64,
    batch_size: u64,
    value_size: usize,
    seq: bool,
    threads: usize,
    wal_sync: bool,
) -> (RocksdbWriteStats, Vec<u64>) {
    if key_nums == 0 || threads == 0 {
        return (RocksdbWriteStats::default(), Vec::new());
    }
    let batch_size = batch_size.max(1);
    let mut handles = Vec::with_capacity(threads);

    for index in 0..threads {
        let db = db.clone();
        let (start, end) = split_range(key_nums, threads, index);
        let seed = 0x9e37_79b9_u64.wrapping_mul(index as u64 + 1);
        let handle = thread::spawn(move || {
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut write_opts = WriteOptions::default();
            write_opts.set_sync(wal_sync);
            let total = end.saturating_sub(start);
            let mut stats = RocksdbWriteStats::default();
            let mut latencies_us = Vec::new();
            if seq {
                let mut processed = 0u64;
                while processed < total {
                    let batch_count = (total - processed).min(batch_size);
                    let mut batch = WriteBatch::default();
                    for offset in 0..batch_count {
                        let key_id = start + processed + offset;
                        let key = make_key(key_id);
                        let value = make_value(value_size, key_id);
                        batch.put(key, value);
                    }
                    stats.submitted = stats.submitted.saturating_add(batch_count);
                    let begin = Instant::now();
                    match db.write_opt(batch, &write_opts) {
                        Ok(()) => {
                            stats.successful = stats.successful.saturating_add(batch_count);
                        }
                        Err(_) => {
                            stats.failed = stats.failed.saturating_add(batch_count);
                        }
                    }
                    latencies_us.push(begin.elapsed().as_micros().min(u64::MAX as u128) as u64);
                    processed += batch_count;
                }
            } else {
                let mut remaining = total;
                while remaining > 0 {
                    let batch_count = remaining.min(batch_size);
                    let mut batch = WriteBatch::default();
                    for _ in 0..batch_count {
                        let key_id = rng.gen_range(0..key_nums);
                        let key = make_key(key_id);
                        let value = make_value(value_size, key_id);
                        batch.put(key, value);
                    }
                    stats.submitted = stats.submitted.saturating_add(batch_count);
                    let begin = Instant::now();
                    match db.write_opt(batch, &write_opts) {
                        Ok(()) => {
                            stats.successful = stats.successful.saturating_add(batch_count);
                        }
                        Err(_) => {
                            stats.failed = stats.failed.saturating_add(batch_count);
                        }
                    }
                    latencies_us.push(begin.elapsed().as_micros().min(u64::MAX as u128) as u64);
                    remaining = remaining.saturating_sub(batch_count);
                }
            }
            (stats, latencies_us)
        });
        handles.push(handle);
    }

    let mut aggregate = RocksdbWriteStats::default();
    let mut aggregate_latencies = Vec::new();
    for handle in handles {
        match handle.join() {
            Ok((stats, latencies_us)) => {
                aggregate.add(stats);
                aggregate_latencies.extend(latencies_us);
            }
            Err(_) => aggregate.thread_panics = aggregate.thread_panics.saturating_add(1),
        }
    }
    (aggregate, aggregate_latencies)
}

#[cfg(feature = "rocksdb")]
fn singleput_rocksdb(
    db: Arc<DB>,
    key_nums: u64,
    value_size: usize,
    seq: bool,
    threads: usize,
    wal_sync: bool,
) -> RocksdbWriteStats {
    if key_nums == 0 || threads == 0 {
        return RocksdbWriteStats::default();
    }
    let mut handles = Vec::with_capacity(threads);

    for index in 0..threads {
        let db = db.clone();
        let (start, end) = split_range(key_nums, threads, index);
        let seed = 0x24b1_4d3f_u64.wrapping_mul(index as u64 + 1);
        let handle = thread::spawn(move || {
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut write_opts = WriteOptions::default();
            write_opts.set_sync(wal_sync);
            let mut stats = RocksdbWriteStats::default();
            if seq {
                for key_id in start..end {
                    let key = make_key(key_id);
                    let value = make_value(value_size, key_id);
                    stats.submitted = stats.submitted.saturating_add(1);
                    match db.put_opt(key, value, &write_opts) {
                        Ok(()) => {
                            stats.successful = stats.successful.saturating_add(1);
                        }
                        Err(_) => {
                            stats.failed = stats.failed.saturating_add(1);
                        }
                    }
                }
            } else {
                let mut remaining = end.saturating_sub(start);
                while remaining > 0 {
                    let key_id = rng.gen_range(0..key_nums);
                    let key = make_key(key_id);
                    let value = make_value(value_size, key_id);
                    stats.submitted = stats.submitted.saturating_add(1);
                    match db.put_opt(key, value, &write_opts) {
                        Ok(()) => {
                            stats.successful = stats.successful.saturating_add(1);
                        }
                        Err(_) => {
                            stats.failed = stats.failed.saturating_add(1);
                        }
                    }
                    remaining = remaining.saturating_sub(1);
                }
            }
            stats
        });
        handles.push(handle);
    }

    let mut aggregate = RocksdbWriteStats::default();
    for handle in handles {
        match handle.join() {
            Ok(stats) => aggregate.add(stats),
            Err(_) => aggregate.thread_panics = aggregate.thread_panics.saturating_add(1),
        }
    }
    aggregate
}

fn randread_goatkv(engine: Arc<KvEngine>, times: u64, key_nums: u64, threads: usize) -> Vec<u64> {
    if key_nums == 0 || times == 0 || threads == 0 {
        return Vec::new();
    }
    let mut round_latencies_us = Vec::with_capacity(times as usize);
    for round in 0..times {
        let round_begin = Instant::now();
        let mut handles = Vec::with_capacity(threads);
        for index in 0..threads {
            let engine = engine.clone();
            let (start, end) = split_range(key_nums, threads, index);
            let count = end.saturating_sub(start);
            let seed = 0x243f_6a88_u64
                .wrapping_add((round + 1).wrapping_mul(0x9e37_79b9))
                .wrapping_mul(index as u64 + 1);
            let handle = thread::spawn(move || {
                let mut rng = SmallRng::seed_from_u64(seed);
                for _ in 0..count {
                    let key_id = rng.gen_range(0..key_nums);
                    let key = make_key(key_id);
                    let _ = engine.get(&key);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.join();
        }
        round_latencies_us.push(round_begin.elapsed().as_micros().min(u64::MAX as u128) as u64);
    }
    round_latencies_us
}

fn hotread_goatkv(
    engine: Arc<KvEngine>,
    times: u64,
    key_nums: u64,
    hotset: u64,
    threads: usize,
) -> Vec<u64> {
    if key_nums == 0 || times == 0 || threads == 0 {
        return Vec::new();
    }
    let hotset = hotset.max(1).min(key_nums);
    let mut round_latencies_us = Vec::with_capacity(times as usize);
    for round in 0..times {
        let round_begin = Instant::now();
        let mut handles = Vec::with_capacity(threads);
        for index in 0..threads {
            let engine = engine.clone();
            let (start, end) = split_range(key_nums, threads, index);
            let count = end.saturating_sub(start);
            let seed = 0x85a3_08d3_u64
                .wrapping_add((round + 1).wrapping_mul(0x9e37_79b9))
                .wrapping_mul(index as u64 + 1);
            let handle = thread::spawn(move || {
                let mut rng = SmallRng::seed_from_u64(seed);
                for _ in 0..count {
                    let key_id = rng.gen_range(0..hotset);
                    let key = make_key(key_id);
                    let _ = engine.get(&key);
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            let _ = handle.join();
        }
        round_latencies_us.push(round_begin.elapsed().as_micros().min(u64::MAX as u128) as u64);
    }
    round_latencies_us
}

fn sample_multiget_key_id(rng: &mut SmallRng, key_nums: u64, miss_ratio: u64) -> u64 {
    let miss_ratio = miss_ratio.min(100);
    let roll = rng.gen_range(0..100);
    if roll < miss_ratio {
        // Keep miss key space separated from populated range.
        key_nums + rng.gen_range(0..key_nums.max(1))
    } else {
        rng.gen_range(0..key_nums)
    }
}

fn multiget_goatkv(
    engine: Arc<KvEngine>,
    times: u64,
    key_nums: u64,
    batch_size: u64,
    miss_ratio: u64,
    threads: usize,
) {
    if key_nums == 0 || times == 0 || threads == 0 || batch_size == 0 {
        return;
    }
    let batch_size = batch_size.max(1);

    for round in 0..times {
        let mut handles = Vec::with_capacity(threads);
        for index in 0..threads {
            let engine = engine.clone();
            let (start, end) = split_range(key_nums, threads, index);
            let batches = (end.saturating_sub(start)).div_ceil(batch_size).max(1);
            let seed = 0x0d15_ea5e_u64
                .wrapping_add((round + 1).wrapping_mul(0x9e37_79b9))
                .wrapping_mul(index as u64 + 1);
            let handle = thread::spawn(move || {
                let mut rng = SmallRng::seed_from_u64(seed);
                for _ in 0..batches {
                    let mut keys = Vec::with_capacity(batch_size as usize);
                    for _ in 0..batch_size {
                        let key_id = sample_multiget_key_id(&mut rng, key_nums, miss_ratio);
                        keys.push(make_key(key_id));
                    }
                    let _ = engine.multi_get(&keys);
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            let _ = handle.join();
        }
    }
}

fn collect_sstable_paths(data_dir: &Path) -> Vec<PathBuf> {
    let mut paths = fs::read_dir(data_dir)
        .ok()
        .into_iter()
        .flat_map(|entries| entries.filter_map(Result::ok))
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("sst"))
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

fn wait_for_goatkv_flush(engine: &KvEngine, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if engine.runtime_metrics().immutable_memtable_backlog == 0 {
            return;
        }
        if Instant::now() >= deadline {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_goatkv_background_idle(engine: &KvEngine, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let metrics = engine.runtime_metrics();
        if metrics.immutable_memtable_backlog == 0 && metrics.pending_compaction_bytes == 0 {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn scanread_goatkv(engine: Arc<KvEngine>, times: u64, mode: ScanMode) -> u64 {
    if times == 0 {
        return 0;
    }
    engine.flush();
    wait_for_goatkv_flush(&engine, Duration::from_secs(5));
    let sstable_paths = collect_sstable_paths(engine.sstable_paths().data_dir());
    if sstable_paths.is_empty() {
        return 0;
    }

    let mut scanned = 0u64;
    for _ in 0..times {
        for path in &sstable_paths {
            let reader = SSTableReader::open(path).expect("open sstable for scanread");
            match mode {
                ScanMode::Iterator => {
                    let mut iter = reader.into_scan_iterator();
                    while iter.next_entry().expect("scan next").is_some() {
                        scanned += 1;
                    }
                }
                ScanMode::ScanAll => {
                    scanned = scanned.saturating_add(
                        reader
                            .scan_all()
                            .expect("scan_all")
                            .len()
                            .try_into()
                            .expect("scan count"),
                    );
                }
            }
        }
    }
    scanned
}

#[cfg(feature = "rocksdb")]
fn randread_rocksdb(db: Arc<DB>, times: u64, key_nums: u64, threads: usize) -> Vec<u64> {
    if key_nums == 0 || times == 0 || threads == 0 {
        return Vec::new();
    }
    let mut round_latencies_us = Vec::with_capacity(times as usize);
    for round in 0..times {
        let round_begin = Instant::now();
        let mut handles = Vec::with_capacity(threads);
        for index in 0..threads {
            let db = db.clone();
            let (start, end) = split_range(key_nums, threads, index);
            let count = end.saturating_sub(start);
            let seed = 0x243f_6a88_u64
                .wrapping_add((round + 1).wrapping_mul(0x9e37_79b9))
                .wrapping_mul(index as u64 + 1);
            let handle = thread::spawn(move || {
                let mut rng = SmallRng::seed_from_u64(seed);
                for _ in 0..count {
                    let key_id = rng.gen_range(0..key_nums);
                    let key = make_key(key_id);
                    let _ = db.get(&key);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.join();
        }
        round_latencies_us.push(round_begin.elapsed().as_micros().min(u64::MAX as u128) as u64);
    }
    round_latencies_us
}

#[cfg(feature = "rocksdb")]
fn hotread_rocksdb(
    db: Arc<DB>,
    times: u64,
    key_nums: u64,
    hotset: u64,
    threads: usize,
) -> Vec<u64> {
    if key_nums == 0 || times == 0 || threads == 0 {
        return Vec::new();
    }
    let hotset = hotset.max(1).min(key_nums);
    let mut round_latencies_us = Vec::with_capacity(times as usize);
    for round in 0..times {
        let round_begin = Instant::now();
        let mut handles = Vec::with_capacity(threads);
        for index in 0..threads {
            let db = db.clone();
            let (start, end) = split_range(key_nums, threads, index);
            let count = end.saturating_sub(start);
            let seed = 0x85a3_08d3_u64
                .wrapping_add((round + 1).wrapping_mul(0x9e37_79b9))
                .wrapping_mul(index as u64 + 1);
            let handle = thread::spawn(move || {
                let mut rng = SmallRng::seed_from_u64(seed);
                for _ in 0..count {
                    let key_id = rng.gen_range(0..hotset);
                    let key = make_key(key_id);
                    let _ = db.get(&key);
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            let _ = handle.join();
        }
        round_latencies_us.push(round_begin.elapsed().as_micros().min(u64::MAX as u128) as u64);
    }
    round_latencies_us
}

#[cfg(feature = "rocksdb")]
fn multiget_rocksdb(
    db: Arc<DB>,
    times: u64,
    key_nums: u64,
    batch_size: u64,
    miss_ratio: u64,
    threads: usize,
) {
    if key_nums == 0 || times == 0 || threads == 0 || batch_size == 0 {
        return;
    }
    let batch_size = batch_size.max(1);

    for round in 0..times {
        let mut handles = Vec::with_capacity(threads);
        for index in 0..threads {
            let db = db.clone();
            let (start, end) = split_range(key_nums, threads, index);
            let batches = (end.saturating_sub(start)).div_ceil(batch_size).max(1);
            let seed = 0x0d15_ea5e_u64
                .wrapping_add((round + 1).wrapping_mul(0x9e37_79b9))
                .wrapping_mul(index as u64 + 1);
            let handle = thread::spawn(move || {
                let mut rng = SmallRng::seed_from_u64(seed);
                for _ in 0..batches {
                    let mut keys = Vec::with_capacity(batch_size as usize);
                    for _ in 0..batch_size {
                        let key_id = sample_multiget_key_id(&mut rng, key_nums, miss_ratio);
                        keys.push(make_key(key_id));
                    }
                    let _ = db.multi_get(keys);
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            let _ = handle.join();
        }
    }
}

#[cfg(feature = "rocksdb")]
fn scanread_rocksdb(db: Arc<DB>, times: u64) -> u64 {
    if times == 0 {
        return 0;
    }
    let mut scanned = 0u64;
    for _ in 0..times {
        for entry in db.iterator(IteratorMode::Start) {
            let _ = entry.expect("iterate rocksdb");
            scanned += 1;
        }
    }
    scanned
}

fn ms_per_iter(total_ms: u128, iters: u64) -> f64 {
    if iters == 0 {
        0.0
    } else {
        total_ms as f64 / iters as f64
    }
}

fn throughput_ops_per_sec(total_ms: u128, ops: u64) -> f64 {
    if total_ms == 0 {
        return ops as f64;
    }
    (ops as f64) / (total_ms as f64 / 1000.0)
}

fn unix_now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn percentile_ms_from_samples(samples_us: &[u64], quantile: f64) -> f64 {
    if samples_us.is_empty() {
        return 0.0;
    }
    let mut sorted = samples_us.to_vec();
    sorted.sort_unstable();
    let target = (sorted.len() as f64 * quantile).ceil() as usize;
    let index = target.saturating_sub(1).min(sorted.len() - 1);
    sorted[index] as f64 / 1000.0
}

fn bench_result_with_samples(
    engine: &'static str,
    workload: &'static str,
    total_ms: u128,
    iters: u64,
    ops: u64,
    latency_samples_us: &[u64],
) -> BenchResult {
    let samples = latency_samples_us.len() as u64;
    let fallback = ms_per_iter(total_ms, iters);
    let p95_ms = if samples == 0 {
        fallback
    } else {
        percentile_ms_from_samples(latency_samples_us, 0.95)
    };
    let p99_ms = if samples == 0 {
        fallback
    } else {
        percentile_ms_from_samples(latency_samples_us, 0.99)
    };

    BenchResult {
        engine,
        workload,
        total_ms,
        iters,
        ops,
        ms_per_iter: fallback,
        throughput_ops_per_sec: throughput_ops_per_sec(total_ms, ops),
        latency_samples: samples,
        p95_ms,
        p99_ms,
        run_unix_ms: unix_now_ms(),
    }
}

fn bench_result_basic(
    engine: &'static str,
    workload: &'static str,
    total_ms: u128,
    iters: u64,
    ops: u64,
) -> BenchResult {
    bench_result_with_samples(engine, workload, total_ms, iters, ops, &[])
}

fn print_result(result: &BenchResult) {
    println!(
        "bench_result engine={} workload={} total_ms={} iters={} ops={} ms_per_iter={:.3} throughput_ops_per_sec={:.3} latency_samples={} p95_ms={:.3} p99_ms={:.3} run_unix_ms={}",
        result.engine,
        result.workload,
        result.total_ms,
        result.iters,
        result.ops,
        result.ms_per_iter,
        result.throughput_ops_per_sec,
        result.latency_samples,
        result.p95_ms,
        result.p99_ms,
        result.run_unix_ms
    );
}

fn evaluate_benchmark_gate(cli: &Cli, result: &BenchResult) -> bool {
    let tolerance = (cli.regression_threshold_pct.max(0.0)) / 100.0;
    let mut pass = true;

    if let Some(baseline_ms_per_iter) = cli.baseline_ms_per_iter {
        if baseline_ms_per_iter.is_finite() && baseline_ms_per_iter > 0.0 {
            let upper_limit = baseline_ms_per_iter * (1.0 + tolerance);
            let metric_pass = result.ms_per_iter <= upper_limit;
            println!(
                "bench_gate metric=ms_per_iter baseline={:.3} observed={:.3} upper_limit={:.3} pass={}",
                baseline_ms_per_iter, result.ms_per_iter, upper_limit, metric_pass
            );
            pass &= metric_pass;
        }
    }

    if let Some(baseline_throughput) = cli.baseline_throughput_ops_per_sec {
        if baseline_throughput.is_finite() && baseline_throughput > 0.0 {
            let lower_limit = baseline_throughput * (1.0 - tolerance);
            let metric_pass = result.throughput_ops_per_sec >= lower_limit;
            println!(
                "bench_gate metric=throughput_ops_per_sec baseline={:.3} observed={:.3} lower_limit={:.3} pass={}",
                baseline_throughput, result.throughput_ops_per_sec, lower_limit, metric_pass
            );
            pass &= metric_pass;
        }
    }

    pass
}

fn print_goatkv_write_stats(workload: &str, stats: GoatkvWriteStats) {
    println!(
        "goatkv_write_stats workload={} submitted={} successful={} failed={} unavailable_errors={} thread_panics={}",
        workload,
        stats.submitted,
        stats.successful,
        stats.failed,
        stats.unavailable_errors,
        stats.thread_panics,
    );
}

#[cfg(feature = "rocksdb")]
fn print_rocksdb_write_stats(workload: &str, stats: RocksdbWriteStats) {
    println!(
        "rocksdb_write_stats workload={} submitted={} successful={} failed={} thread_panics={}",
        workload, stats.submitted, stats.successful, stats.failed, stats.thread_panics,
    );
}

fn print_runtime_metrics(engine: &KvEngine, phase: &str) {
    let metrics = engine.runtime_metrics();
    println!(
        "runtime_metrics phase={} immutable_memtable_backlog={} flush_failure_streak={} flush_circuit_open={} l0_file_count={} pending_compaction_bytes={} write_pressure_level={} wal_queue_reqs={} wal_queue_bytes={} mem_queue_reqs={} mem_queue_bytes={} wal_inflight_groups={} mem_inflight_groups={} flush_blocked={}",
        phase,
        metrics.immutable_memtable_backlog,
        metrics.flush_failure_streak,
        metrics.flush_circuit_open,
        metrics.l0_file_count,
        metrics.pending_compaction_bytes,
        metrics.write_pressure_level,
        metrics.writer_queue_metrics.wal_queue_reqs,
        metrics.writer_queue_metrics.wal_queue_bytes,
        metrics.writer_queue_metrics.mem_queue_reqs,
        metrics.writer_queue_metrics.mem_queue_bytes,
        metrics.writer_queue_metrics.wal_inflight_groups,
        metrics.writer_queue_metrics.mem_inflight_groups,
        metrics.writer_queue_metrics.flush_blocked,
    );
}

fn prepare_engine_dir(base: &Path, engine: EngineKind, both: bool) -> PathBuf {
    if both {
        base.join(engine.label())
    } else {
        base.to_path_buf()
    }
}

fn goatkv_options_from_cli(cli: &Cli, base_dir: &Path) -> KvEngineOptions {
    KvEngineOptions::default()
        .with_data_dir(base_dir)
        .with_wal_sync(cli.wal_sync)
        .with_wal_preallocate_bytes(cli.wal_preallocate_bytes)
        .with_wal_bytes_per_sync(cli.wal_bytes_per_sync)
        .with_table_cache_capacity(cli.table_cache_capacity)
        .with_block_cache_capacity_bytes(cli.block_cache_capacity_mb.saturating_mul(1024 * 1024))
        .with_row_cache_capacity_bytes(cli.row_cache_capacity_mb.saturating_mul(1024 * 1024))
        .with_filter_cache_capacity_bytes(cli.filter_cache_capacity_mb.saturating_mul(1024 * 1024))
        .with_max_subcompactions(cli.max_subcompactions)
        .with_level_compression(0, cli.l0_compression.into_engine())
        .with_level_compression(1, cli.l1_compression.into_engine())
        .with_level_compression(2, cli.l2_compression.into_engine())
}

fn print_cache_metrics(engine: &KvEngine) {
    if let Some(metrics) = engine.read_cache_metrics() {
        println!(
            "cache_metrics table_hit={} table_miss={} table_evict={} row_hit={} row_miss={} row_evict={} block_hit={} block_miss={} block_evict={} filter_hit={} filter_miss={} filter_evict={}",
            metrics.table_hits,
            metrics.table_misses,
            metrics.table_evictions,
            metrics.row_hits,
            metrics.row_misses,
            metrics.row_evictions,
            metrics.block_hits,
            metrics.block_misses,
            metrics.block_evictions,
            metrics.filter_hits,
            metrics.filter_misses,
            metrics.filter_evictions,
        );
    }
}

fn main() {
    if std::env::args_os().len() == 1 {
        let mut cmd = Cli::command();
        let _ = cmd.print_help();
        return;
    }

    let cli = Cli::parse();
    let _log_guards = init_logging("goatkv_bench", &cli.directory, "warn");

    if cli.threads == 0 {
        tracing::error!("threads must be >= 1");
        return;
    }

    // KvEngine 内部 cleanup worker 以 Tokio task 运行，需要当前线程具备 runtime 上下文。
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime for benchmark");
    let _runtime_guard = runtime.enter();

    let engines: Vec<EngineKind> = match cli.engine {
        EngineKind::Both => vec![EngineKind::Goatkv, EngineKind::Rocksdb],
        other => vec![other],
    };

    for engine_kind in engines {
        let base_dir =
            prepare_engine_dir(&cli.directory, engine_kind, cli.engine == EngineKind::Both);
        let _ = std::fs::create_dir_all(&base_dir);

        match cli.command {
            Commands::Populate {
                key_nums,
                batch_size,
                value_size,
                seq,
            } => {
                let iters = if batch_size == 0 {
                    0
                } else {
                    key_nums.div_ceil(batch_size)
                };
                match engine_kind {
                    EngineKind::Goatkv => {
                        let options = goatkv_options_from_cli(&cli, &base_dir);
                        let engine =
                            Arc::new(KvEngine::new_with_options(options).expect("open engine"));
                        let begin = Instant::now();
                        let (stats, latency_samples_us) = populate_goatkv(
                            engine.clone(),
                            key_nums,
                            batch_size,
                            value_size,
                            seq,
                            cli.threads,
                        );
                        let total_ms = begin.elapsed().as_millis();
                        let result = bench_result_with_samples(
                            "goatkv",
                            "populate",
                            total_ms,
                            iters,
                            stats.submitted,
                            &latency_samples_us,
                        );
                        print_result(&result);
                        if !evaluate_benchmark_gate(&cli, &result) {
                            std::process::exit(3);
                        }
                        print_goatkv_write_stats("populate", stats);
                        print_runtime_metrics(&engine, "post_write");
                        let idle = wait_for_goatkv_background_idle(&engine, Duration::from_secs(5));
                        println!("runtime_idle_reached={}", idle);
                        print_runtime_metrics(&engine, "post_wait");
                    }
                    EngineKind::Rocksdb => {
                        ensure_rocksdb_available();
                        #[cfg(feature = "rocksdb")]
                        {
                            let mut rocks_opts = Options::default();
                            rocks_opts.create_if_missing(true);
                            rocks_opts.set_compression_type(DBCompressionType::None);
                            let db =
                                Arc::new(DB::open(&rocks_opts, &base_dir).expect("open rocksdb"));
                            let begin = Instant::now();
                            let (stats, latency_samples_us) = populate_rocksdb(
                                db,
                                key_nums,
                                batch_size,
                                value_size,
                                seq,
                                cli.threads,
                                cli.wal_sync,
                            );
                            let total_ms = begin.elapsed().as_millis();
                            let result = bench_result_with_samples(
                                "rocksdb",
                                "populate",
                                total_ms,
                                iters,
                                stats.submitted,
                                &latency_samples_us,
                            );
                            print_result(&result);
                            if !evaluate_benchmark_gate(&cli, &result) {
                                std::process::exit(3);
                            }
                            print_rocksdb_write_stats("populate", stats);
                        }
                    }
                    EngineKind::Both => {}
                }
            }
            Commands::Singleput {
                key_nums,
                value_size,
                seq,
            } => {
                let iters = key_nums;
                match engine_kind {
                    EngineKind::Goatkv => {
                        let options = goatkv_options_from_cli(&cli, &base_dir);
                        let engine =
                            Arc::new(KvEngine::new_with_options(options).expect("open engine"));
                        let begin = Instant::now();
                        let stats = singleput_goatkv(
                            engine.clone(),
                            key_nums,
                            value_size,
                            seq,
                            cli.threads,
                        );
                        let total_ms = begin.elapsed().as_millis();
                        let result = bench_result_basic(
                            "goatkv",
                            "singleput",
                            total_ms,
                            iters,
                            stats.submitted,
                        );
                        print_result(&result);
                        if !evaluate_benchmark_gate(&cli, &result) {
                            std::process::exit(3);
                        }
                        print_goatkv_write_stats("singleput", stats);
                        print_runtime_metrics(&engine, "post_write");
                    }
                    EngineKind::Rocksdb => {
                        ensure_rocksdb_available();
                        #[cfg(feature = "rocksdb")]
                        {
                            let mut rocks_opts = Options::default();
                            rocks_opts.create_if_missing(true);
                            rocks_opts.set_compression_type(DBCompressionType::None);
                            let db =
                                Arc::new(DB::open(&rocks_opts, &base_dir).expect("open rocksdb"));
                            let begin = Instant::now();
                            let stats = singleput_rocksdb(
                                db,
                                key_nums,
                                value_size,
                                seq,
                                cli.threads,
                                cli.wal_sync,
                            );
                            let total_ms = begin.elapsed().as_millis();
                            let result = bench_result_basic(
                                "rocksdb",
                                "singleput",
                                total_ms,
                                iters,
                                stats.submitted,
                            );
                            print_result(&result);
                            if !evaluate_benchmark_gate(&cli, &result) {
                                std::process::exit(3);
                            }
                            print_rocksdb_write_stats("singleput", stats);
                        }
                    }
                    EngineKind::Both => {}
                }
            }
            Commands::Randread {
                times,
                key_nums,
                value_size: _,
            } => {
                let iters = times;
                match engine_kind {
                    EngineKind::Goatkv => {
                        let options = goatkv_options_from_cli(&cli, &base_dir);
                        let engine =
                            Arc::new(KvEngine::new_with_options(options).expect("open engine"));
                        let begin = Instant::now();
                        let latency_samples_us =
                            randread_goatkv(engine.clone(), times, key_nums, cli.threads);
                        let total_ms = begin.elapsed().as_millis();
                        let result = bench_result_with_samples(
                            "goatkv",
                            "randread",
                            total_ms,
                            iters,
                            times.saturating_mul(key_nums),
                            &latency_samples_us,
                        );
                        print_result(&result);
                        if !evaluate_benchmark_gate(&cli, &result) {
                            std::process::exit(3);
                        }
                        print_cache_metrics(&engine);
                    }
                    EngineKind::Rocksdb => {
                        ensure_rocksdb_available();
                        #[cfg(feature = "rocksdb")]
                        {
                            let mut rocks_opts = Options::default();
                            rocks_opts.create_if_missing(true);
                            rocks_opts.set_compression_type(DBCompressionType::None);
                            let db =
                                Arc::new(DB::open(&rocks_opts, &base_dir).expect("open rocksdb"));
                            let begin = Instant::now();
                            let latency_samples_us =
                                randread_rocksdb(db, times, key_nums, cli.threads);
                            let total_ms = begin.elapsed().as_millis();
                            let result = bench_result_with_samples(
                                "rocksdb",
                                "randread",
                                total_ms,
                                iters,
                                times.saturating_mul(key_nums),
                                &latency_samples_us,
                            );
                            print_result(&result);
                            if !evaluate_benchmark_gate(&cli, &result) {
                                std::process::exit(3);
                            }
                        }
                    }
                    EngineKind::Both => {}
                }
            }
            Commands::Hotread {
                times,
                key_nums,
                hotset,
            } => {
                let iters = times;
                match engine_kind {
                    EngineKind::Goatkv => {
                        let options = goatkv_options_from_cli(&cli, &base_dir);
                        let engine =
                            Arc::new(KvEngine::new_with_options(options).expect("open engine"));
                        let begin = Instant::now();
                        let latency_samples_us =
                            hotread_goatkv(engine.clone(), times, key_nums, hotset, cli.threads);
                        let total_ms = begin.elapsed().as_millis();
                        let result = bench_result_with_samples(
                            "goatkv",
                            "hotread",
                            total_ms,
                            iters,
                            times.saturating_mul(key_nums),
                            &latency_samples_us,
                        );
                        print_result(&result);
                        if !evaluate_benchmark_gate(&cli, &result) {
                            std::process::exit(3);
                        }
                        print_cache_metrics(&engine);
                    }
                    EngineKind::Rocksdb => {
                        ensure_rocksdb_available();
                        #[cfg(feature = "rocksdb")]
                        {
                            let mut rocks_opts = Options::default();
                            rocks_opts.create_if_missing(true);
                            rocks_opts.set_compression_type(DBCompressionType::None);
                            let db =
                                Arc::new(DB::open(&rocks_opts, &base_dir).expect("open rocksdb"));
                            let begin = Instant::now();
                            let latency_samples_us =
                                hotread_rocksdb(db, times, key_nums, hotset, cli.threads);
                            let total_ms = begin.elapsed().as_millis();
                            let result = bench_result_with_samples(
                                "rocksdb",
                                "hotread",
                                total_ms,
                                iters,
                                times.saturating_mul(key_nums),
                                &latency_samples_us,
                            );
                            print_result(&result);
                            if !evaluate_benchmark_gate(&cli, &result) {
                                std::process::exit(3);
                            }
                        }
                    }
                    EngineKind::Both => {}
                }
            }
            Commands::Multiget {
                times,
                key_nums,
                batch_size,
                miss_ratio,
            } => {
                let iters = times;
                match engine_kind {
                    EngineKind::Goatkv => {
                        let options = goatkv_options_from_cli(&cli, &base_dir);
                        let engine =
                            Arc::new(KvEngine::new_with_options(options).expect("open engine"));
                        let begin = Instant::now();
                        multiget_goatkv(
                            engine.clone(),
                            times,
                            key_nums,
                            batch_size,
                            miss_ratio,
                            cli.threads,
                        );
                        let total_ms = begin.elapsed().as_millis();
                        let result =
                            bench_result_basic("goatkv", "multiget", total_ms, iters, iters);
                        print_result(&result);
                        if !evaluate_benchmark_gate(&cli, &result) {
                            std::process::exit(3);
                        }
                        print_cache_metrics(&engine);
                    }
                    EngineKind::Rocksdb => {
                        ensure_rocksdb_available();
                        #[cfg(feature = "rocksdb")]
                        {
                            let mut rocks_opts = Options::default();
                            rocks_opts.create_if_missing(true);
                            rocks_opts.set_compression_type(DBCompressionType::None);
                            let db =
                                Arc::new(DB::open(&rocks_opts, &base_dir).expect("open rocksdb"));
                            let begin = Instant::now();
                            multiget_rocksdb(
                                db,
                                times,
                                key_nums,
                                batch_size,
                                miss_ratio,
                                cli.threads,
                            );
                            let total_ms = begin.elapsed().as_millis();
                            let result =
                                bench_result_basic("rocksdb", "multiget", total_ms, iters, iters);
                            print_result(&result);
                            if !evaluate_benchmark_gate(&cli, &result) {
                                std::process::exit(3);
                            }
                        }
                    }
                    EngineKind::Both => {}
                }
            }
            Commands::Scanread { times, mode } => match engine_kind {
                EngineKind::Goatkv => {
                    let options = goatkv_options_from_cli(&cli, &base_dir);
                    let engine =
                        Arc::new(KvEngine::new_with_options(options).expect("open engine"));
                    let begin = Instant::now();
                    let scanned = scanread_goatkv(engine, times, mode);
                    let total_ms = begin.elapsed().as_millis();
                    let result = bench_result_basic(
                        "goatkv",
                        match mode {
                            ScanMode::Iterator => "scanread_iterator",
                            ScanMode::ScanAll => "scanread_scan_all",
                        },
                        total_ms,
                        scanned,
                        scanned,
                    );
                    print_result(&result);
                    if !evaluate_benchmark_gate(&cli, &result) {
                        std::process::exit(3);
                    }
                }
                EngineKind::Rocksdb => {
                    ensure_rocksdb_available();
                    #[cfg(feature = "rocksdb")]
                    {
                        let mut rocks_opts = Options::default();
                        rocks_opts.create_if_missing(true);
                        rocks_opts.set_compression_type(DBCompressionType::None);
                        let db = Arc::new(DB::open(&rocks_opts, &base_dir).expect("open rocksdb"));
                        let begin = Instant::now();
                        let scanned = scanread_rocksdb(db, times);
                        let total_ms = begin.elapsed().as_millis();
                        let result =
                            bench_result_basic("rocksdb", "scanread", total_ms, scanned, scanned);
                        print_result(&result);
                        if !evaluate_benchmark_gate(&cli, &result) {
                            std::process::exit(3);
                        }
                    }
                }
                EngineKind::Both => {}
            },
        }
    }
}
