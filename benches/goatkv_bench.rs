use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use goat_db::goatkv::utils::init_logging;
use goat_db::goatkv::{KvEngine, KvEngineOptions};
#[cfg(feature = "rocksdb")]
use rocksdb::{DBCompressionType, Options, WriteBatch, WriteOptions, DB};

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

    /// Engine to run
    #[arg(long, value_enum, default_value_t = EngineKind::Goatkv)]
    engine: EngineKind,

    /// Table cache entries for GoatKV (0 disables table cache)
    #[arg(long, default_value_t = 64)]
    table_cache_capacity: usize,

    /// Block cache size for GoatKV in MB (0 disables block cache)
    #[arg(long, default_value_t = 64)]
    block_cache_capacity_mb: usize,

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
}

struct BenchResult {
    engine: &'static str,
    workload: &'static str,
    total_ms: u128,
    iters: u64,
    ms_per_iter: f64,
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
) {
    if key_nums == 0 || threads == 0 {
        return;
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
                    engine.put_batch(entries).expect("goatkv put_batch");
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
                    engine.put_batch(entries).expect("goatkv put_batch");
                    remaining = remaining.saturating_sub(batch);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.join();
    }
}

fn singleput_goatkv(
    engine: Arc<KvEngine>,
    key_nums: u64,
    value_size: usize,
    seq: bool,
    threads: usize,
) {
    if key_nums == 0 || threads == 0 {
        return;
    }
    let mut handles = Vec::with_capacity(threads);

    for index in 0..threads {
        let engine = engine.clone();
        let (start, end) = split_range(key_nums, threads, index);
        let seed = 0x517c_c1b7_u64.wrapping_mul(index as u64 + 1);
        let handle = thread::spawn(move || {
            let mut rng = SmallRng::seed_from_u64(seed);
            if seq {
                for key_id in start..end {
                    let key = make_key(key_id);
                    let value = make_value(value_size, key_id);
                    engine.put(key, value).expect("goatkv put");
                }
            } else {
                let mut remaining = end.saturating_sub(start);
                while remaining > 0 {
                    let key_id = rng.gen_range(0..key_nums);
                    let key = make_key(key_id);
                    let value = make_value(value_size, key_id);
                    engine.put(key, value).expect("goatkv put");
                    remaining = remaining.saturating_sub(1);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.join();
    }
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
) {
    if key_nums == 0 || threads == 0 {
        return;
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
                    let _ = db.write_opt(batch, &write_opts);
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
                    let _ = db.write_opt(batch, &write_opts);
                    remaining = remaining.saturating_sub(batch_count);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.join();
    }
}

#[cfg(feature = "rocksdb")]
fn singleput_rocksdb(
    db: Arc<DB>,
    key_nums: u64,
    value_size: usize,
    seq: bool,
    threads: usize,
    wal_sync: bool,
) {
    if key_nums == 0 || threads == 0 {
        return;
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
            if seq {
                for key_id in start..end {
                    let key = make_key(key_id);
                    let value = make_value(value_size, key_id);
                    let _ = db.put_opt(key, value, &write_opts);
                }
            } else {
                let mut remaining = end.saturating_sub(start);
                while remaining > 0 {
                    let key_id = rng.gen_range(0..key_nums);
                    let key = make_key(key_id);
                    let value = make_value(value_size, key_id);
                    let _ = db.put_opt(key, value, &write_opts);
                    remaining = remaining.saturating_sub(1);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.join();
    }
}

fn randread_goatkv(engine: Arc<KvEngine>, times: u64, key_nums: u64, threads: usize) {
    if key_nums == 0 || times == 0 || threads == 0 {
        return;
    }
    for round in 0..times {
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
    }
}

fn hotread_goatkv(engine: Arc<KvEngine>, times: u64, key_nums: u64, hotset: u64, threads: usize) {
    if key_nums == 0 || times == 0 || threads == 0 {
        return;
    }
    let hotset = hotset.max(1).min(key_nums);
    for round in 0..times {
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
    }
}

#[cfg(feature = "rocksdb")]
fn randread_rocksdb(db: Arc<DB>, times: u64, key_nums: u64, threads: usize) {
    if key_nums == 0 || times == 0 || threads == 0 {
        return;
    }
    for round in 0..times {
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
    }
}

#[cfg(feature = "rocksdb")]
fn hotread_rocksdb(db: Arc<DB>, times: u64, key_nums: u64, hotset: u64, threads: usize) {
    if key_nums == 0 || times == 0 || threads == 0 {
        return;
    }
    let hotset = hotset.max(1).min(key_nums);
    for round in 0..times {
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
    }
}

fn ms_per_iter(total_ms: u128, iters: u64) -> f64 {
    if iters == 0 {
        0.0
    } else {
        total_ms as f64 / iters as f64
    }
}

fn print_result(result: &BenchResult) {
    println!(
        "engine={} workload={} total_ms={} iters={} ms_per_iter={:.3}",
        result.engine, result.workload, result.total_ms, result.iters, result.ms_per_iter
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
        .with_table_cache_capacity(cli.table_cache_capacity)
        .with_block_cache_capacity_bytes(cli.block_cache_capacity_mb.saturating_mul(1024 * 1024))
}

fn print_cache_metrics(engine: &KvEngine) {
    if let Some(metrics) = engine.read_cache_metrics() {
        println!(
            "cache_metrics table_hit={} table_miss={} table_evict={} block_hit={} block_miss={} block_evict={}",
            metrics.table_hits,
            metrics.table_misses,
            metrics.table_evictions,
            metrics.block_hits,
            metrics.block_misses,
            metrics.block_evictions,
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
                        populate_goatkv(engine, key_nums, batch_size, value_size, seq, cli.threads);
                        let total_ms = begin.elapsed().as_millis();
                        let result = BenchResult {
                            engine: "goatkv",
                            workload: "populate",
                            total_ms,
                            iters,
                            ms_per_iter: ms_per_iter(total_ms, iters),
                        };
                        print_result(&result);
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
                            populate_rocksdb(
                                db,
                                key_nums,
                                batch_size,
                                value_size,
                                seq,
                                cli.threads,
                                cli.wal_sync,
                            );
                            let total_ms = begin.elapsed().as_millis();
                            let result = BenchResult {
                                engine: "rocksdb",
                                workload: "populate",
                                total_ms,
                                iters,
                                ms_per_iter: ms_per_iter(total_ms, iters),
                            };
                            print_result(&result);
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
                        singleput_goatkv(engine, key_nums, value_size, seq, cli.threads);
                        let total_ms = begin.elapsed().as_millis();
                        let result = BenchResult {
                            engine: "goatkv",
                            workload: "singleput",
                            total_ms,
                            iters,
                            ms_per_iter: ms_per_iter(total_ms, iters),
                        };
                        print_result(&result);
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
                            singleput_rocksdb(
                                db,
                                key_nums,
                                value_size,
                                seq,
                                cli.threads,
                                cli.wal_sync,
                            );
                            let total_ms = begin.elapsed().as_millis();
                            let result = BenchResult {
                                engine: "rocksdb",
                                workload: "singleput",
                                total_ms,
                                iters,
                                ms_per_iter: ms_per_iter(total_ms, iters),
                            };
                            print_result(&result);
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
                        randread_goatkv(engine.clone(), times, key_nums, cli.threads);
                        let total_ms = begin.elapsed().as_millis();
                        let result = BenchResult {
                            engine: "goatkv",
                            workload: "randread",
                            total_ms,
                            iters,
                            ms_per_iter: ms_per_iter(total_ms, iters),
                        };
                        print_result(&result);
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
                            randread_rocksdb(db, times, key_nums, cli.threads);
                            let total_ms = begin.elapsed().as_millis();
                            let result = BenchResult {
                                engine: "rocksdb",
                                workload: "randread",
                                total_ms,
                                iters,
                                ms_per_iter: ms_per_iter(total_ms, iters),
                            };
                            print_result(&result);
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
                        hotread_goatkv(engine.clone(), times, key_nums, hotset, cli.threads);
                        let total_ms = begin.elapsed().as_millis();
                        let result = BenchResult {
                            engine: "goatkv",
                            workload: "hotread",
                            total_ms,
                            iters,
                            ms_per_iter: ms_per_iter(total_ms, iters),
                        };
                        print_result(&result);
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
                            hotread_rocksdb(db, times, key_nums, hotset, cli.threads);
                            let total_ms = begin.elapsed().as_millis();
                            let result = BenchResult {
                                engine: "rocksdb",
                                workload: "hotread",
                                total_ms,
                                iters,
                                ms_per_iter: ms_per_iter(total_ms, iters),
                            };
                            print_result(&result);
                        }
                    }
                    EngineKind::Both => {}
                }
            }
        }
    }
}
