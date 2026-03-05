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
