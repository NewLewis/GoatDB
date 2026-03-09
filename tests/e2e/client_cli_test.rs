#[path = "../common/mod.rs"]
mod common;

use std::path::PathBuf;
use std::process::Command;

use common::test_server::{should_skip_network_e2e, TestServer, TestServerOptions};
use common::tls_server::{self, TlsTestServer};
use tonic::transport::Channel;
use tonic::Code;

fn client_binary() -> &'static str {
    env!("CARGO_BIN_EXE_goatkv_client")
}

fn tls_fixture_path(name: &str) -> PathBuf {
    tls_server::tls_fixture_dir().join(name)
}

fn run_client(args: &[&str]) -> std::process::Output {
    Command::new(client_binary())
        .args(args)
        .output()
        .expect("run goatkv_client")
}

fn stdout_text(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn extract_snapshot_id(stdout: &str) -> u64 {
    let marker = "snapshot_id=";
    let start = stdout
        .find(marker)
        .map(|idx| idx + marker.len())
        .expect("snapshot output should contain snapshot_id=");
    stdout[start..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>()
        .parse::<u64>()
        .expect("parse snapshot id from cli output")
}

async fn bearer_get(
    server: &TestServer,
    token: &str,
    key: &[u8],
) -> common::test_server::goatkv::GetResponse {
    let channel = Channel::from_shared(server.address.clone())
        .expect("build channel")
        .connect()
        .await
        .expect("connect channel");
    let header: tonic::metadata::MetadataValue<tonic::metadata::Ascii> = format!("Bearer {token}")
        .parse()
        .expect("build authorization header");
    let mut client = common::test_server::GoatKvServiceClient::with_interceptor(
        channel,
        move |mut request: tonic::Request<()>| {
            request
                .metadata_mut()
                .insert("authorization", header.clone());
            Ok(request)
        },
    );
    client
        .get(common::test_server::goatkv::GetRequest {
            key: key.to_vec(),
            snapshot_id: 0,
        })
        .await
        .expect("authenticated get should succeed")
        .into_inner()
}

async fn plain_get(
    server: &TestServer,
    key: &[u8],
    snapshot_id: u64,
) -> common::test_server::goatkv::GetResponse {
    let mut client = server.client().await;
    client
        .get(common::test_server::goatkv::GetRequest {
            key: key.to_vec(),
            snapshot_id,
        })
        .await
        .expect("plain get should succeed")
        .into_inner()
}

#[tokio::test]
async fn test_client_cli_auth_token_allows_write_and_persists_data() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start_with_options(TestServerOptions {
        port: None,
        health_port: None,
        data_dir: None,
        auth_tokens: vec!["secret-token".to_string()],
        show_logs: false,
        capture_stderr: true,
    })
    .await;

    let output = run_client(&[
        "--address",
        &server.address,
        "--auth-token",
        "secret-token",
        "put",
        "cli_auth_key",
        "cli_auth_value",
    ]);
    assert!(
        output.status.success(),
        "client should succeed with auth token, stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stored = bearer_get(&server, "secret-token", b"cli_auth_key").await;
    assert!(stored.success);
    assert_eq!(stored.value, b"cli_auth_value".to_vec());
}

#[tokio::test]
async fn test_client_cli_missing_auth_token_fails_without_mutating_database() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start_with_options(TestServerOptions {
        port: None,
        health_port: None,
        data_dir: None,
        auth_tokens: vec!["secret-token".to_string()],
        show_logs: false,
        capture_stderr: true,
    })
    .await;

    let output = run_client(&[
        "--address",
        &server.address,
        "put",
        "cli_auth_blocked",
        "blocked",
    ]);
    assert!(
        !output.status.success(),
        "client should fail without auth token"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("Unauthenticated"),
        "stderr should surface auth failure, stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let missing = bearer_get(&server, "secret-token", b"cli_auth_blocked").await;
    assert!(
        !missing.success && missing.value.is_empty(),
        "failed unauthenticated cli write must not publish any value"
    );
}

#[tokio::test]
async fn test_client_cli_tls_with_ca_certificate_persists_data() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TlsTestServer::start(false).await;
    let output = run_client(&[
        "--address",
        &format!("https://{}", server.address()),
        "--ca-cert-path",
        tls_fixture_path("ca-cert.pem").to_str().unwrap(),
        "put",
        "cli_tls_key",
        "cli_tls_value",
    ]);
    assert!(
        output.status.success(),
        "client should succeed with trusted CA, stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut trusted = server.trusted_client().await;
    let stored = trusted
        .get(tls_server::goatkv::GetRequest {
            key: b"cli_tls_key".to_vec(),
            snapshot_id: 0,
        })
        .await
        .expect("trusted tls get should succeed")
        .into_inner();
    assert!(stored.success);
    assert_eq!(stored.value, b"cli_tls_value".to_vec());
}

#[tokio::test]
async fn test_client_cli_mtls_requires_client_identity() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TlsTestServer::start(true).await;
    let output = run_client(&[
        "--address",
        &format!("https://{}", server.address()),
        "--ca-cert-path",
        tls_fixture_path("ca-cert.pem").to_str().unwrap(),
        "put",
        "cli_mtls_key",
        "blocked",
    ]);
    assert!(
        !output.status.success(),
        "client should fail against mTLS server without client identity"
    );

    let mut authed = server.mtls_client().await;
    let missing = authed
        .get(tls_server::goatkv::GetRequest {
            key: b"cli_mtls_key".to_vec(),
            snapshot_id: 0,
        })
        .await
        .expect("mTLS get should succeed")
        .into_inner();
    assert!(
        !missing.success && missing.value.is_empty(),
        "failed certificate-less cli write must not publish any value"
    );

    let success = run_client(&[
        "--address",
        &format!("https://{}", server.address()),
        "--ca-cert-path",
        tls_fixture_path("ca-cert.pem").to_str().unwrap(),
        "--client-cert-path",
        tls_fixture_path("client-cert.pem").to_str().unwrap(),
        "--client-key-path",
        tls_fixture_path("client-key.pem").to_str().unwrap(),
        "--tls-domain-name",
        "localhost",
        "put",
        "cli_mtls_key",
        "cli_mtls_value",
    ]);
    assert!(
        success.status.success(),
        "client should succeed with mTLS identity, stderr={}",
        String::from_utf8_lossy(&success.stderr)
    );

    let stored = authed
        .get(tls_server::goatkv::GetRequest {
            key: b"cli_mtls_key".to_vec(),
            snapshot_id: 0,
        })
        .await
        .expect("mTLS get should succeed")
        .into_inner();
    assert!(stored.success);
    assert_eq!(stored.value, b"cli_mtls_value".to_vec());
}

#[tokio::test]
async fn test_client_cli_multiget_and_scan_reflect_current_database_state() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;
    for (key, value) in [
        (b"user:1".to_vec(), b"alice".to_vec()),
        (b"user:2".to_vec(), b"bob".to_vec()),
        (b"user:3".to_vec(), b"carol".to_vec()),
        (b"other:1".to_vec(), b"ignore".to_vec()),
    ] {
        client
            .write(common::test_server::goatkv::WriteRequest { key, value })
            .await
            .expect("seed write should succeed");
    }

    let multiget = run_client(&[
        "--address",
        &server.address,
        "multiget",
        "user:1",
        "user:3",
        "missing",
        "user:1",
    ]);
    assert!(
        multiget.status.success(),
        "multiget cli should succeed, stderr={}",
        String::from_utf8_lossy(&multiget.stderr)
    );
    let multiget_stdout = stdout_text(&multiget);
    assert!(multiget_stdout.contains("user:1 => alice"));
    assert!(multiget_stdout.contains("user:3 => carol"));
    assert!(multiget_stdout.contains("missing => <not found>"));

    let scan = run_client(&[
        "--address",
        &server.address,
        "scan",
        "--prefix",
        "user:",
        "--limit",
        "2",
    ]);
    assert!(
        scan.status.success(),
        "scan cli should succeed, stderr={}",
        String::from_utf8_lossy(&scan.stderr)
    );
    let scan_stdout = stdout_text(&scan);
    assert!(scan_stdout.contains("user:1 => alice"));
    assert!(scan_stdout.contains("user:2 => bob"));
    assert!(!scan_stdout.contains("user:3 => carol"));
    assert!(!scan_stdout.contains("other:1 => ignore"));
}

#[tokio::test]
async fn test_client_cli_compare_and_set_and_snapshot_commands_drive_visibility() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;
    client
        .write(common::test_server::goatkv::WriteRequest {
            key: b"cas:key".to_vec(),
            value: b"old".to_vec(),
        })
        .await
        .expect("seed cas key");
    client
        .write(common::test_server::goatkv::WriteRequest {
            key: b"snap:key".to_vec(),
            value: b"v1".to_vec(),
        })
        .await
        .expect("seed snapshot key");

    let cas_success = run_client(&[
        "--address",
        &server.address,
        "compare-and-set",
        "cas:key",
        "--expected",
        "old",
        "--new-value",
        "new",
    ]);
    assert!(cas_success.status.success());
    assert!(stdout_text(&cas_success).contains("Success"));
    let current = plain_get(&server, b"cas:key", 0).await;
    assert!(current.success);
    assert_eq!(current.value, b"new".to_vec());

    let cas_conflict = run_client(&[
        "--address",
        &server.address,
        "compare-and-set",
        "cas:key",
        "--expected",
        "old",
        "--new-value",
        "newer",
    ]);
    assert!(
        cas_conflict.status.success(),
        "logical CAS conflict should still return a successful CLI process"
    );
    assert!(stdout_text(&cas_conflict).contains("Failed"));
    let unchanged = plain_get(&server, b"cas:key", 0).await;
    assert_eq!(unchanged.value, b"new".to_vec());

    let snapshot_create = run_client(&["--address", &server.address, "snapshot-create"]);
    assert!(snapshot_create.status.success());
    let snapshot_stdout = stdout_text(&snapshot_create);
    let snapshot_id = extract_snapshot_id(&snapshot_stdout);
    assert!(snapshot_id > 0);

    client
        .update(common::test_server::goatkv::UpdateRequest {
            key: b"snap:key".to_vec(),
            value: b"v2".to_vec(),
        })
        .await
        .expect("update after snapshot");

    let snap_read = plain_get(&server, b"snap:key", snapshot_id).await;
    assert!(snap_read.success);
    assert_eq!(snap_read.value, b"v1".to_vec());

    let snapshot_release = run_client(&[
        "--address",
        &server.address,
        "snapshot-release",
        &snapshot_id.to_string(),
    ]);
    assert!(snapshot_release.status.success());
    assert!(stdout_text(&snapshot_release).contains("Success"));

    let err = client
        .get(common::test_server::goatkv::GetRequest {
            key: b"snap:key".to_vec(),
            snapshot_id,
        })
        .await
        .expect_err("released snapshot id should not be readable");
    assert_eq!(err.code(), Code::NotFound);
}
