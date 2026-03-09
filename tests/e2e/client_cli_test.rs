#[path = "../common/mod.rs"]
mod common;

use std::path::PathBuf;
use std::process::Command;

use common::test_server::{should_skip_network_e2e, TestServer, TestServerOptions};
use common::tls_server::{self, TlsTestServer};
use tonic::transport::Channel;

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
