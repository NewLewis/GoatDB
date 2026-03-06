#[path = "../common/mod.rs"]
mod common;

use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use common::test_server::goatkv::{GetRequest, WriteRequest};
use common::test_server::{
    find_free_port, should_skip_network_e2e, GoatKvServiceClient, TestServer, TestServerOptions,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;

const LATENCY_BUCKET_UPPER_US: [u64; 15] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_500_000, 5_000_000,
];

#[derive(Debug, Clone, Serialize)]
struct SoakConfig {
    duration_secs: u64,
    writers: usize,
    readers: usize,
    key_space: usize,
    sample_interval_ms: u64,
    max_rss_growth_kb: u64,
    max_fd_growth: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
struct CounterSnapshot {
    writes_ok: u64,
    writes_err: u64,
    reads_ok: u64,
    reads_err: u64,
    read_not_found: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
struct LatencySnapshot {
    count: u64,
    avg_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
struct ResourceSample {
    elapsed_secs: f64,
    rss_kb: Option<u64>,
    fd_count: Option<u64>,
    immutable_memtable_backlog: Option<f64>,
    pending_compaction_bytes: Option<f64>,
    writer_pressure_level: Option<f64>,
    rpc_latency_p95_ms: Option<f64>,
    rpc_latency_p99_ms: Option<f64>,
    rpc_error_rate: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct SoakResult {
    pass: bool,
    fail_reasons: Vec<String>,
    counters: CounterSnapshot,
    write_latency: LatencySnapshot,
    read_latency: LatencySnapshot,
    rss_growth_kb: Option<i64>,
    fd_growth: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
struct SoakReport {
    schema_version: u32,
    started_at_unix_ms: u128,
    finished_at_unix_ms: u128,
    config: SoakConfig,
    result: SoakResult,
    resource_samples: Vec<ResourceSample>,
    error_samples: Vec<String>,
}

#[derive(Default)]
struct AtomicCounters {
    writes_ok: AtomicU64,
    writes_err: AtomicU64,
    reads_ok: AtomicU64,
    reads_err: AtomicU64,
    read_not_found: AtomicU64,
}

impl AtomicCounters {
    fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            writes_ok: self.writes_ok.load(Ordering::Relaxed),
            writes_err: self.writes_err.load(Ordering::Relaxed),
            reads_ok: self.reads_ok.load(Ordering::Relaxed),
            reads_err: self.reads_err.load(Ordering::Relaxed),
            read_not_found: self.read_not_found.load(Ordering::Relaxed),
        }
    }
}

struct LatencyHistogram {
    buckets: [u64; LATENCY_BUCKET_UPPER_US.len() + 1],
    count: u64,
    sum_us: u128,
    max_us: u64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: [0; LATENCY_BUCKET_UPPER_US.len() + 1],
            count: 0,
            sum_us: 0,
            max_us: 0,
        }
    }
}

impl LatencyHistogram {
    fn observe(&mut self, latency: Duration) {
        let latency_us = latency.as_micros().min(u64::MAX as u128) as u64;
        self.count = self.count.saturating_add(1);
        self.sum_us = self.sum_us.saturating_add(latency_us as u128);
        self.max_us = self.max_us.max(latency_us);
        let idx = LATENCY_BUCKET_UPPER_US
            .iter()
            .position(|bound| latency_us <= *bound)
            .unwrap_or(LATENCY_BUCKET_UPPER_US.len());
        self.buckets[idx] = self.buckets[idx].saturating_add(1);
    }

    fn snapshot(&self) -> LatencySnapshot {
        if self.count == 0 {
            return LatencySnapshot::default();
        }
        LatencySnapshot {
            count: self.count,
            avg_ms: self.sum_us as f64 / self.count as f64 / 1_000.0,
            p95_ms: self.quantile_ms(0.95),
            p99_ms: self.quantile_ms(0.99),
            max_ms: self.max_us as f64 / 1_000.0,
        }
    }

    fn quantile_ms(&self, quantile: f64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let target = ((self.count as f64) * quantile).ceil() as u64;
        let mut cumulative = 0u64;
        for (idx, bound_us) in LATENCY_BUCKET_UPPER_US.iter().enumerate() {
            cumulative = cumulative.saturating_add(self.buckets[idx]);
            if cumulative >= target {
                return *bound_us as f64 / 1_000.0;
            }
        }
        LATENCY_BUCKET_UPPER_US.last().copied().unwrap_or(0) as f64 / 1_000.0
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "soak test is long-running and intended for explicit stability runs"]
async fn test_e2e_soak_read_write_stability() {
    let config = load_soak_config();
    let report_path = soak_report_path();
    let started_at_unix_ms = unix_now_ms();
    if should_skip_network_e2e() {
        let report = SoakReport {
            schema_version: 1,
            started_at_unix_ms,
            finished_at_unix_ms: unix_now_ms(),
            config,
            result: SoakResult {
                pass: true,
                fail_reasons: Vec::new(),
                counters: CounterSnapshot::default(),
                write_latency: LatencySnapshot::default(),
                read_latency: LatencySnapshot::default(),
                rss_growth_kb: None,
                fd_growth: None,
            },
            resource_samples: Vec::new(),
            error_samples: vec![
                "skipped: loopback bind is not permitted in current environment".to_string(),
            ],
        };
        write_report(&report_path, &report);
        eprintln!(
            "Soak report written to {} (skipped due to network restrictions)",
            report_path.display()
        );
        return;
    }

    let health_port = find_free_port();
    let server = TestServer::start_with_options(TestServerOptions {
        port: None,
        health_port: Some(health_port),
        data_dir: None,
        auth_tokens: Vec::new(),
        show_logs: false,
        capture_stderr: true,
    })
    .await;

    let health_url = server
        .health_address()
        .expect("health endpoint should be configured")
        .to_string();
    let address = server.address.clone();
    let server_pid = server.process.id();

    let mut bootstrap_client = server.client().await;
    for idx in 0..config.key_space {
        bootstrap_client
            .write(WriteRequest {
                key: format!("soak_key_{}", idx).into_bytes(),
                value: b"seed".to_vec(),
            })
            .await
            .expect("bootstrap write should succeed");
    }

    let counters = Arc::new(AtomicCounters::default());
    let write_latency = Arc::new(Mutex::new(LatencyHistogram::default()));
    let read_latency = Arc::new(Mutex::new(LatencyHistogram::default()));
    let error_samples = Arc::new(Mutex::new(Vec::new()));
    let resource_samples = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));

    let start = Instant::now();

    let sampler_handle = {
        let resource_samples = Arc::clone(&resource_samples);
        let stop = Arc::clone(&stop);
        let health_url = health_url.clone();
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let sample = sample_resources(&health_url, server_pid, start.elapsed());
                resource_samples.lock().unwrap().push(sample);
                tokio::time::sleep(Duration::from_millis(config.sample_interval_ms)).await;
            }
        })
    };

    let mut handles = Vec::new();

    for writer_id in 0..config.writers {
        let counters = Arc::clone(&counters);
        let write_latency = Arc::clone(&write_latency);
        let error_samples = Arc::clone(&error_samples);
        let stop = Arc::clone(&stop);
        let address = address.clone();
        let key_space = config.key_space;

        handles.push(tokio::spawn(async move {
            let mut client = match GoatKvServiceClient::connect(address).await {
                Ok(client) => client,
                Err(e) => {
                    record_error(&error_samples, format!("writer connect failed: {}", e));
                    counters.writes_err.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };

            let mut rng = StdRng::seed_from_u64(0x5A5A_0000_u64 + writer_id as u64);
            let mut seq = 0u64;

            while !stop.load(Ordering::Relaxed) {
                let key_id = rng.gen_range(0..key_space);
                let request = WriteRequest {
                    key: format!("soak_key_{}", key_id).into_bytes(),
                    value: format!("writer{}_{}", writer_id, seq).into_bytes(),
                };
                seq = seq.saturating_add(1);

                let op_start = Instant::now();
                match client.write(request).await {
                    Ok(resp) => {
                        if resp.into_inner().success {
                            counters.writes_ok.fetch_add(1, Ordering::Relaxed);
                        } else {
                            counters.writes_err.fetch_add(1, Ordering::Relaxed);
                            record_error(
                                &error_samples,
                                "writer returned success=false".to_string(),
                            );
                        }
                    }
                    Err(e) => {
                        counters.writes_err.fetch_add(1, Ordering::Relaxed);
                        record_error(&error_samples, format!("writer grpc error: {}", e));
                    }
                }
                write_latency.lock().unwrap().observe(op_start.elapsed());
            }
        }));
    }

    for reader_id in 0..config.readers {
        let counters = Arc::clone(&counters);
        let read_latency = Arc::clone(&read_latency);
        let error_samples = Arc::clone(&error_samples);
        let stop = Arc::clone(&stop);
        let address = address.clone();
        let key_space = config.key_space;

        handles.push(tokio::spawn(async move {
            let mut client = match GoatKvServiceClient::connect(address).await {
                Ok(client) => client,
                Err(e) => {
                    record_error(&error_samples, format!("reader connect failed: {}", e));
                    counters.reads_err.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };

            let mut rng = StdRng::seed_from_u64(0xA5A5_0000_u64 + reader_id as u64);
            while !stop.load(Ordering::Relaxed) {
                let key_id = rng.gen_range(0..key_space);
                let request = GetRequest {
                    key: format!("soak_key_{}", key_id).into_bytes(),
                    snapshot_id: 0,
                };

                let op_start = Instant::now();
                match client.get(request).await {
                    Ok(resp) => {
                        if resp.into_inner().success {
                            counters.reads_ok.fetch_add(1, Ordering::Relaxed);
                        } else {
                            counters.read_not_found.fetch_add(1, Ordering::Relaxed);
                            record_error(
                                &error_samples,
                                "reader found missing key in seeded key space".to_string(),
                            );
                        }
                    }
                    Err(e) => {
                        counters.reads_err.fetch_add(1, Ordering::Relaxed);
                        record_error(&error_samples, format!("reader grpc error: {}", e));
                    }
                }
                read_latency.lock().unwrap().observe(op_start.elapsed());
            }
        }));
    }

    tokio::time::sleep(Duration::from_secs(config.duration_secs)).await;
    stop.store(true, Ordering::Relaxed);

    for handle in handles {
        if let Err(e) = handle.await {
            record_error(&error_samples, format!("worker join failed: {}", e));
        }
    }
    let _ = sampler_handle.await;

    tokio::time::sleep(Duration::from_millis(config.sample_interval_ms)).await;
    resource_samples.lock().unwrap().push(sample_resources(
        &health_url,
        server_pid,
        start.elapsed(),
    ));

    let resource_samples = resource_samples.lock().unwrap().clone();
    let counters_snapshot = counters.snapshot();
    let write_latency_snapshot = write_latency.lock().unwrap().snapshot();
    let read_latency_snapshot = read_latency.lock().unwrap().snapshot();
    let error_samples_snapshot = error_samples.lock().unwrap().clone();

    let rss_growth_kb = growth_u64(
        resource_samples.first().and_then(|s| s.rss_kb),
        resource_samples.last().and_then(|s| s.rss_kb),
    );
    let fd_growth = growth_u64(
        resource_samples.first().and_then(|s| s.fd_count),
        resource_samples.last().and_then(|s| s.fd_count),
    );

    let mut fail_reasons = Vec::new();
    if counters_snapshot.writes_err > 0
        || counters_snapshot.reads_err > 0
        || counters_snapshot.read_not_found > 0
    {
        fail_reasons.push(format!(
            "encountered request errors: writes_err={}, reads_err={}, read_not_found={}",
            counters_snapshot.writes_err,
            counters_snapshot.reads_err,
            counters_snapshot.read_not_found
        ));
    }

    if let Some(rss_growth) = rss_growth_kb {
        if rss_growth > config.max_rss_growth_kb as i64 {
            fail_reasons.push(format!(
                "rss growth {}KB exceeds threshold {}KB",
                rss_growth, config.max_rss_growth_kb
            ));
        }
    }

    if let Some(fd_growth) = fd_growth {
        if fd_growth > config.max_fd_growth as i64 {
            fail_reasons.push(format!(
                "fd growth {} exceeds threshold {}",
                fd_growth, config.max_fd_growth
            ));
        }
    }

    if resource_samples.len() < 2 {
        fail_reasons.push("insufficient resource samples collected".to_string());
    }

    let finished_at_unix_ms = unix_now_ms();
    let result = SoakResult {
        pass: fail_reasons.is_empty(),
        fail_reasons,
        counters: counters_snapshot,
        write_latency: write_latency_snapshot,
        read_latency: read_latency_snapshot,
        rss_growth_kb,
        fd_growth,
    };

    let report = SoakReport {
        schema_version: 1,
        started_at_unix_ms,
        finished_at_unix_ms,
        config,
        result,
        resource_samples,
        error_samples: error_samples_snapshot,
    };

    write_report(&report_path, &report);
    eprintln!("Soak report written to {}", report_path.display());

    assert!(
        report.result.pass,
        "soak stability checks failed; report={} fail_reasons={:?}",
        report_path.display(),
        report.result.fail_reasons
    );
}

fn load_soak_config() -> SoakConfig {
    SoakConfig {
        duration_secs: env_u64("GOATKV_SOAK_DURATION_SECS", 30),
        writers: env_usize("GOATKV_SOAK_WRITERS", 4),
        readers: env_usize("GOATKV_SOAK_READERS", 4),
        key_space: env_usize("GOATKV_SOAK_KEY_SPACE", 2_000),
        sample_interval_ms: env_u64("GOATKV_SOAK_SAMPLE_INTERVAL_MS", 1_000),
        max_rss_growth_kb: env_u64("GOATKV_SOAK_MAX_RSS_GROWTH_KB", 262_144),
        max_fd_growth: env_u64("GOATKV_SOAK_MAX_FD_GROWTH", 64),
    }
}

fn soak_report_path() -> PathBuf {
    std::env::var("GOATKV_SOAK_REPORT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("goatkv_soak_report.json"))
}

fn write_report(path: &PathBuf, report: &SoakReport) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("failed to create soak report parent directory");
    }
    let payload =
        serde_json::to_vec_pretty(report).expect("failed to serialize soak report as JSON");
    fs::write(path, payload).expect("failed to write soak report");
}

fn sample_resources(health_url: &str, server_pid: u32, elapsed: Duration) -> ResourceSample {
    let metrics_text = http_get(health_url, "/metrics")
        .ok()
        .and_then(|(status, body)| (status == 200).then_some(body));

    let immutable_memtable_backlog = metrics_text
        .as_deref()
        .and_then(|text| metric_value(text, "goatkv_engine_immutable_memtable_backlog"));
    let pending_compaction_bytes = metrics_text
        .as_deref()
        .and_then(|text| metric_value(text, "goatkv_engine_pending_compaction_bytes"));
    let writer_pressure_level = metrics_text
        .as_deref()
        .and_then(|text| metric_value(text, "goatkv_writer_pressure_level"));
    let rpc_latency_p95_ms = metrics_text
        .as_deref()
        .and_then(|text| metric_value(text, "goatkv_rpc_latency_p95_seconds"))
        .map(|seconds| seconds * 1_000.0);
    let rpc_latency_p99_ms = metrics_text
        .as_deref()
        .and_then(|text| metric_value(text, "goatkv_rpc_latency_p99_seconds"))
        .map(|seconds| seconds * 1_000.0);
    let rpc_error_rate = metrics_text
        .as_deref()
        .and_then(|text| metric_value(text, "goatkv_rpc_error_rate"));

    ResourceSample {
        elapsed_secs: elapsed.as_secs_f64(),
        rss_kb: process_rss_kb(server_pid),
        fd_count: process_fd_count(server_pid),
        immutable_memtable_backlog,
        pending_compaction_bytes,
        writer_pressure_level,
        rpc_latency_p95_ms,
        rpc_latency_p99_ms,
        rpc_error_rate,
    }
}

fn growth_u64(from: Option<u64>, to: Option<u64>) -> Option<i64> {
    match (from, to) {
        (Some(start), Some(end)) => Some(end as i64 - start as i64),
        _ => None,
    }
}

fn record_error(samples: &Arc<Mutex<Vec<String>>>, message: String) {
    let mut guard = samples.lock().unwrap();
    if guard.len() < 64 {
        guard.push(message);
    }
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn unix_now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn process_rss_kb(pid: u32) -> Option<u64> {
    let status = fs::read_to_string(format!("/proc/{}/status", pid)).ok()?;
    status.lines().find_map(|line| {
        if !line.starts_with("VmRSS:") {
            return None;
        }
        line.split_whitespace().nth(1)?.parse::<u64>().ok()
    })
}

fn process_fd_count(pid: u32) -> Option<u64> {
    let fd_dir = format!("/proc/{}/fd", pid);
    let entries = fs::read_dir(fd_dir).ok()?;
    Some(entries.count() as u64)
}

fn http_get(health_base_url: &str, path: &str) -> std::io::Result<(u16, String)> {
    let address = health_base_url.trim_start_matches("http://");
    let mut stream = TcpStream::connect(address)?;
    stream.set_read_timeout(Some(Duration::from_millis(500)))?;
    stream.set_write_timeout(Some(Duration::from_millis(500)))?;

    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        path, address
    );
    stream.write_all(request.as_bytes())?;
    stream.flush()?;

    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let status = response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| std::io::Error::other("invalid HTTP response status line"))?;
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    Ok((status, body))
}

fn metric_value(metrics_text: &str, metric_key: &str) -> Option<f64> {
    metrics_text.lines().find_map(|line| {
        if !line.starts_with(metric_key) {
            return None;
        }
        line.split_whitespace().nth(1)?.parse::<f64>().ok()
    })
}
