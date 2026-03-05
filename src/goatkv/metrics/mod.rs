use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const QPS_WINDOW_SECONDS: u64 = 60;
const LATENCY_BUCKET_UPPER_US: [u64; 16] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_500_000, 5_000_000, 10_000_000,
];
const LATENCY_BUCKET_COUNT: usize = LATENCY_BUCKET_UPPER_US.len() + 1;

#[repr(usize)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcMethod {
    Write = 0,
    Get = 1,
    Update = 2,
    Delete = 3,
    Flush = 4,
    CreateSnapshot = 5,
    ReleaseSnapshot = 6,
}

impl RpcMethod {
    pub const COUNT: usize = 7;
    pub const ALL: [RpcMethod; Self::COUNT] = [
        RpcMethod::Write,
        RpcMethod::Get,
        RpcMethod::Update,
        RpcMethod::Delete,
        RpcMethod::Flush,
        RpcMethod::CreateSnapshot,
        RpcMethod::ReleaseSnapshot,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            RpcMethod::Write => "write",
            RpcMethod::Get => "get",
            RpcMethod::Update => "update",
            RpcMethod::Delete => "delete",
            RpcMethod::Flush => "flush",
            RpcMethod::CreateSnapshot => "create_snapshot",
            RpcMethod::ReleaseSnapshot => "release_snapshot",
        }
    }
}

#[derive(Debug)]
struct QpsWindow {
    slots: [u64; QPS_WINDOW_SECONDS as usize],
    slot_seconds: [u64; QPS_WINDOW_SECONDS as usize],
    rolling_total: u64,
}

impl Default for QpsWindow {
    fn default() -> Self {
        Self {
            slots: [0; QPS_WINDOW_SECONDS as usize],
            slot_seconds: [0; QPS_WINDOW_SECONDS as usize],
            rolling_total: 0,
        }
    }
}

impl QpsWindow {
    fn record(&mut self, now_sec: u64) {
        self.compact(now_sec);
        let idx = (now_sec % QPS_WINDOW_SECONDS) as usize;
        if self.slot_seconds[idx] != now_sec {
            self.rolling_total = self.rolling_total.saturating_sub(self.slots[idx]);
            self.slots[idx] = 0;
            self.slot_seconds[idx] = now_sec;
        }
        self.slots[idx] = self.slots[idx].saturating_add(1);
        self.rolling_total = self.rolling_total.saturating_add(1);
    }

    fn qps_last_window(&mut self, now_sec: u64) -> f64 {
        self.compact(now_sec);
        self.rolling_total as f64 / QPS_WINDOW_SECONDS as f64
    }

    fn compact(&mut self, now_sec: u64) {
        for idx in 0..self.slots.len() {
            let slot_sec = self.slot_seconds[idx];
            if slot_sec == 0 {
                continue;
            }
            if now_sec.saturating_sub(slot_sec) >= QPS_WINDOW_SECONDS {
                self.rolling_total = self.rolling_total.saturating_sub(self.slots[idx]);
                self.slots[idx] = 0;
                self.slot_seconds[idx] = 0;
            }
        }
    }
}

#[derive(Debug)]
pub struct RpcMetricsCollector {
    started_at: Instant,
    requests_total: AtomicU64,
    requests_success_total: AtomicU64,
    requests_error_total: AtomicU64,
    method_requests_total: [AtomicU64; RpcMethod::COUNT],
    method_success_total: [AtomicU64; RpcMethod::COUNT],
    method_error_total: [AtomicU64; RpcMethod::COUNT],
    latency_bucket_counts: [AtomicU64; LATENCY_BUCKET_COUNT],
    latency_sum_us: AtomicU64,
    latency_count: AtomicU64,
    latency_max_us: AtomicU64,
    qps_window: Mutex<QpsWindow>,
}

impl Default for RpcMetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcMetricsCollector {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            requests_total: AtomicU64::new(0),
            requests_success_total: AtomicU64::new(0),
            requests_error_total: AtomicU64::new(0),
            method_requests_total: std::array::from_fn(|_| AtomicU64::new(0)),
            method_success_total: std::array::from_fn(|_| AtomicU64::new(0)),
            method_error_total: std::array::from_fn(|_| AtomicU64::new(0)),
            latency_bucket_counts: std::array::from_fn(|_| AtomicU64::new(0)),
            latency_sum_us: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            latency_max_us: AtomicU64::new(0),
            qps_window: Mutex::new(QpsWindow::default()),
        }
    }

    pub fn observe(&self, method: RpcMethod, success: bool, latency: Duration) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        self.method_requests_total[method as usize].fetch_add(1, Ordering::Relaxed);
        if success {
            self.requests_success_total.fetch_add(1, Ordering::Relaxed);
            self.method_success_total[method as usize].fetch_add(1, Ordering::Relaxed);
        } else {
            self.requests_error_total.fetch_add(1, Ordering::Relaxed);
            self.method_error_total[method as usize].fetch_add(1, Ordering::Relaxed);
        }

        let latency_us = latency.as_micros().min(u64::MAX as u128) as u64;
        self.latency_sum_us.fetch_add(latency_us, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);
        self.latency_max_us.fetch_max(latency_us, Ordering::Relaxed);
        let bucket_index = LATENCY_BUCKET_UPPER_US
            .iter()
            .position(|bound| latency_us <= *bound)
            .unwrap_or(LATENCY_BUCKET_UPPER_US.len());
        self.latency_bucket_counts[bucket_index].fetch_add(1, Ordering::Relaxed);

        let now_sec = unix_now_seconds();
        let mut window = self.qps_window.lock().unwrap();
        window.record(now_sec);
    }

    pub fn render_prometheus<F>(&self, mut append_extra_metrics: F) -> String
    where
        F: FnMut(&mut String),
    {
        let requests_total = self.requests_total.load(Ordering::Relaxed);
        let success_total = self.requests_success_total.load(Ordering::Relaxed);
        let error_total = self.requests_error_total.load(Ordering::Relaxed);
        let latency_count = self.latency_count.load(Ordering::Relaxed);
        let latency_sum_us = self.latency_sum_us.load(Ordering::Relaxed);
        let latency_max_us = self.latency_max_us.load(Ordering::Relaxed);
        let error_rate = if requests_total == 0 {
            0.0
        } else {
            error_total as f64 / requests_total as f64
        };
        let avg_latency_seconds = if latency_count == 0 {
            0.0
        } else {
            (latency_sum_us as f64 / latency_count as f64) / 1_000_000.0
        };
        let p95_seconds = self.latency_quantile_seconds(0.95);
        let p99_seconds = self.latency_quantile_seconds(0.99);
        let qps_60s = {
            let now_sec = unix_now_seconds();
            let mut window = self.qps_window.lock().unwrap();
            window.qps_last_window(now_sec)
        };

        let mut output = String::new();
        output.push_str("# HELP goatkv_rpc_requests_total Total RPC requests\n");
        output.push_str("# TYPE goatkv_rpc_requests_total counter\n");
        output.push_str(&format!("goatkv_rpc_requests_total {}\n", requests_total));

        output.push_str("# HELP goatkv_rpc_requests_success_total Total successful RPC requests\n");
        output.push_str("# TYPE goatkv_rpc_requests_success_total counter\n");
        output.push_str(&format!(
            "goatkv_rpc_requests_success_total {}\n",
            success_total
        ));

        output.push_str("# HELP goatkv_rpc_requests_error_total Total failed RPC requests\n");
        output.push_str("# TYPE goatkv_rpc_requests_error_total counter\n");
        output.push_str(&format!(
            "goatkv_rpc_requests_error_total {}\n",
            error_total
        ));

        output.push_str("# HELP goatkv_rpc_error_rate RPC error rate (errors/total)\n");
        output.push_str("# TYPE goatkv_rpc_error_rate gauge\n");
        output.push_str(&format!("goatkv_rpc_error_rate {:.6}\n", error_rate));

        output.push_str("# HELP goatkv_rpc_qps_60s Average QPS over last 60 seconds\n");
        output.push_str("# TYPE goatkv_rpc_qps_60s gauge\n");
        output.push_str(&format!("goatkv_rpc_qps_60s {:.6}\n", qps_60s));

        output.push_str("# HELP goatkv_process_uptime_seconds Process uptime in seconds\n");
        output.push_str("# TYPE goatkv_process_uptime_seconds gauge\n");
        output.push_str(&format!(
            "goatkv_process_uptime_seconds {:.3}\n",
            self.started_at.elapsed().as_secs_f64()
        ));

        output.push_str("# HELP goatkv_rpc_method_requests_total RPC requests by method\n");
        output.push_str("# TYPE goatkv_rpc_method_requests_total counter\n");
        for method in RpcMethod::ALL {
            let total = self.method_requests_total[method as usize].load(Ordering::Relaxed);
            output.push_str(&format!(
                "goatkv_rpc_method_requests_total{{method=\"{}\"}} {}\n",
                method.as_str(),
                total
            ));
        }

        output.push_str("# HELP goatkv_rpc_method_errors_total RPC errors by method\n");
        output.push_str("# TYPE goatkv_rpc_method_errors_total counter\n");
        for method in RpcMethod::ALL {
            let total = self.method_error_total[method as usize].load(Ordering::Relaxed);
            output.push_str(&format!(
                "goatkv_rpc_method_errors_total{{method=\"{}\"}} {}\n",
                method.as_str(),
                total
            ));
        }

        output.push_str("# HELP goatkv_rpc_latency_seconds RPC latency histogram\n");
        output.push_str("# TYPE goatkv_rpc_latency_seconds histogram\n");
        let mut cumulative = 0u64;
        for (idx, bound_us) in LATENCY_BUCKET_UPPER_US.iter().enumerate() {
            cumulative =
                cumulative.saturating_add(self.latency_bucket_counts[idx].load(Ordering::Relaxed));
            output.push_str(&format!(
                "goatkv_rpc_latency_seconds_bucket{{le=\"{:.6}\"}} {}\n",
                *bound_us as f64 / 1_000_000.0,
                cumulative
            ));
        }
        cumulative = cumulative.saturating_add(
            self.latency_bucket_counts[LATENCY_BUCKET_UPPER_US.len()].load(Ordering::Relaxed),
        );
        output.push_str(&format!(
            "goatkv_rpc_latency_seconds_bucket{{le=\"+Inf\"}} {}\n",
            cumulative
        ));
        output.push_str(&format!(
            "goatkv_rpc_latency_seconds_sum {:.6}\n",
            latency_sum_us as f64 / 1_000_000.0
        ));
        output.push_str(&format!(
            "goatkv_rpc_latency_seconds_count {}\n",
            latency_count
        ));

        output.push_str("# HELP goatkv_rpc_latency_p95_seconds Approximate p95 RPC latency\n");
        output.push_str("# TYPE goatkv_rpc_latency_p95_seconds gauge\n");
        output.push_str(&format!(
            "goatkv_rpc_latency_p95_seconds {:.6}\n",
            p95_seconds
        ));

        output.push_str("# HELP goatkv_rpc_latency_p99_seconds Approximate p99 RPC latency\n");
        output.push_str("# TYPE goatkv_rpc_latency_p99_seconds gauge\n");
        output.push_str(&format!(
            "goatkv_rpc_latency_p99_seconds {:.6}\n",
            p99_seconds
        ));

        output.push_str("# HELP goatkv_rpc_latency_avg_seconds Average RPC latency\n");
        output.push_str("# TYPE goatkv_rpc_latency_avg_seconds gauge\n");
        output.push_str(&format!(
            "goatkv_rpc_latency_avg_seconds {:.6}\n",
            avg_latency_seconds
        ));

        output.push_str("# HELP goatkv_rpc_latency_max_seconds Maximum observed RPC latency\n");
        output.push_str("# TYPE goatkv_rpc_latency_max_seconds gauge\n");
        output.push_str(&format!(
            "goatkv_rpc_latency_max_seconds {:.6}\n",
            latency_max_us as f64 / 1_000_000.0
        ));

        append_extra_metrics(&mut output);
        output
    }

    fn latency_quantile_seconds(&self, quantile: f64) -> f64 {
        let count = self.latency_count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }

        let target = ((count as f64) * quantile).ceil() as u64;
        let mut cumulative = 0u64;
        for (idx, bound_us) in LATENCY_BUCKET_UPPER_US.iter().enumerate() {
            cumulative =
                cumulative.saturating_add(self.latency_bucket_counts[idx].load(Ordering::Relaxed));
            if cumulative >= target {
                return *bound_us as f64 / 1_000_000.0;
            }
        }
        LATENCY_BUCKET_UPPER_US.last().copied().unwrap_or(0) as f64 / 1_000_000.0
    }
}

fn unix_now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{RpcMethod, RpcMetricsCollector};

    #[test]
    fn metrics_collector_records_success_and_error_counts() {
        let collector = RpcMetricsCollector::new();
        collector.observe(RpcMethod::Get, true, Duration::from_micros(300));
        collector.observe(RpcMethod::Get, false, Duration::from_micros(900));
        collector.observe(RpcMethod::Write, true, Duration::from_micros(1_200));

        let text = collector.render_prometheus(|_| {});
        assert!(text.contains("goatkv_rpc_requests_total 3"));
        assert!(text.contains("goatkv_rpc_requests_error_total 1"));
        assert!(text.contains("goatkv_rpc_method_requests_total{method=\"get\"} 2"));
        assert!(text.contains("goatkv_rpc_method_errors_total{method=\"get\"} 1"));
    }

    #[test]
    fn metrics_collector_exports_latency_histogram_and_quantiles() {
        let collector = RpcMetricsCollector::new();
        for latency_us in [150_u64, 220, 1_500, 30_000, 500_000] {
            collector.observe(RpcMethod::Write, true, Duration::from_micros(latency_us));
        }
        let text = collector.render_prometheus(|_| {});
        assert!(text.contains("goatkv_rpc_latency_seconds_bucket"));
        assert!(text.contains("goatkv_rpc_latency_p95_seconds"));
        assert!(text.contains("goatkv_rpc_latency_p99_seconds"));
    }
}
