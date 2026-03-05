#[path = "../common/mod.rs"]
mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use common::test_server::{find_free_port, should_skip_network_e2e, TestServer, TestServerOptions};

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

fn http_get_status(health_base_url: &str, path: &str) -> std::io::Result<u16> {
    http_get(health_base_url, path).map(|(status, _)| status)
}

fn metric_value(metrics_text: &str, metric_key: &str) -> f64 {
    metrics_text
        .lines()
        .find_map(|line| {
            if !line.starts_with(metric_key) {
                return None;
            }
            let value = line.split_whitespace().nth(1)?;
            value.parse::<f64>().ok()
        })
        .unwrap_or_else(|| panic!("metric `{}` not found in output", metric_key))
}

#[tokio::test]
async fn test_health_liveness_and_readiness_transition_on_shutdown() {
    if should_skip_network_e2e() {
        return;
    }

    let health_port = find_free_port();
    let server = TestServer::start_with_options(TestServerOptions {
        port: None,
        health_port: Some(health_port),
        data_dir: None,
        show_logs: false,
        capture_stderr: true,
    })
    .await;

    let health_url = server
        .health_address()
        .expect("health endpoint should be configured");
    assert_eq!(http_get_status(health_url, "/livez").unwrap(), 200);
    assert_eq!(http_get_status(health_url, "/readyz").unwrap(), 200);

    server.send_sigint();

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut observed_unready = false;
    let mut observed_live_while_unready = false;
    while Instant::now() < deadline {
        match http_get_status(health_url, "/readyz") {
            Ok(503) => {
                observed_unready = true;
                if http_get_status(health_url, "/livez").ok() == Some(200) {
                    observed_live_while_unready = true;
                    break;
                }
            }
            Ok(_) => {}
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    assert!(
        observed_unready,
        "expected /readyz to return 503 during graceful shutdown"
    );
    assert!(
        observed_live_while_unready,
        "expected /livez to stay 200 while /readyz is 503 during drain window"
    );
}

#[tokio::test]
async fn test_metrics_endpoint_exposes_core_metrics() {
    if should_skip_network_e2e() {
        return;
    }

    let health_port = find_free_port();
    let server = TestServer::start_with_options(TestServerOptions {
        port: None,
        health_port: Some(health_port),
        data_dir: None,
        show_logs: false,
        capture_stderr: true,
    })
    .await;
    let health_url = server
        .health_address()
        .expect("health endpoint should be configured");

    let mut client = server.client().await;
    client
        .write(common::test_server::goatkv::WriteRequest {
            key: b"metrics_key".to_vec(),
            value: b"metrics_val".to_vec(),
        })
        .await
        .unwrap();

    let (status, body) = http_get(health_url, "/metrics").expect("fetch /metrics");
    assert_eq!(status, 200);
    assert!(body.contains("goatkv_rpc_requests_total"));
    assert!(body.contains("goatkv_rpc_latency_p95_seconds"));
    assert!(body.contains("goatkv_rpc_error_rate"));
    assert!(body.contains("goatkv_engine_immutable_memtable_backlog"));
    assert!(body.contains("goatkv_engine_pending_compaction_bytes"));
    assert!(body.contains("goatkv_cache_table_hits_total"));
}

#[tokio::test]
async fn test_metrics_endpoint_tracks_success_and_error_requests() {
    if should_skip_network_e2e() {
        return;
    }

    let health_port = find_free_port();
    let server = TestServer::start_with_options(TestServerOptions {
        port: None,
        health_port: Some(health_port),
        data_dir: None,
        show_logs: false,
        capture_stderr: true,
    })
    .await;
    let health_url = server
        .health_address()
        .expect("health endpoint should be configured");

    let (_, before) = http_get(health_url, "/metrics").expect("fetch baseline metrics");
    let requests_before = metric_value(&before, "goatkv_rpc_requests_total");
    let errors_before = metric_value(&before, "goatkv_rpc_requests_error_total");
    let write_before = metric_value(
        &before,
        "goatkv_rpc_method_requests_total{method=\"write\"}",
    );
    let get_errors_before = metric_value(&before, "goatkv_rpc_method_errors_total{method=\"get\"}");

    let mut client = server.client().await;
    client
        .write(common::test_server::goatkv::WriteRequest {
            key: b"metrics_counter_key".to_vec(),
            value: b"metrics_counter_val".to_vec(),
        })
        .await
        .expect("write should succeed");
    let get_err = client
        .get(common::test_server::goatkv::GetRequest {
            key: Vec::new(),
            snapshot_id: 0,
        })
        .await
        .expect_err("empty key get should fail");
    assert_eq!(get_err.code(), tonic::Code::InvalidArgument);

    let (_, after) = http_get(health_url, "/metrics").expect("fetch post metrics");
    let requests_after = metric_value(&after, "goatkv_rpc_requests_total");
    let errors_after = metric_value(&after, "goatkv_rpc_requests_error_total");
    let write_after = metric_value(&after, "goatkv_rpc_method_requests_total{method=\"write\"}");
    let get_errors_after = metric_value(&after, "goatkv_rpc_method_errors_total{method=\"get\"}");

    assert!(
        requests_after >= requests_before + 2.0,
        "total requests should increase by at least 2, before={}, after={}",
        requests_before,
        requests_after
    );
    assert!(
        errors_after >= errors_before + 1.0,
        "error requests should increase, before={}, after={}",
        errors_before,
        errors_after
    );
    assert!(
        write_after >= write_before + 1.0,
        "write method counter should increase, before={}, after={}",
        write_before,
        write_after
    );
    assert!(
        get_errors_after >= get_errors_before + 1.0,
        "get method error counter should increase, before={}, after={}",
        get_errors_before,
        get_errors_after
    );
}
