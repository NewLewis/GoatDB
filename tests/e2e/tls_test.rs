#[path = "../common/mod.rs"]
mod common;

use common::test_server::should_skip_network_e2e;
use common::tls_server::{
    goatkv::{GetRequest, WriteRequest},
    tls_client, TlsTestServer,
};

#[tokio::test]
async fn test_tls_accepts_trusted_client_and_persists_data() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TlsTestServer::start(false).await;
    let mut client = server.trusted_client().await;
    client
        .write(WriteRequest {
            key: b"tls_key".to_vec(),
            value: b"tls_value".to_vec(),
        })
        .await
        .expect("trusted tls write should succeed");

    let response = client
        .get(GetRequest {
            key: b"tls_key".to_vec(),
            snapshot_id: 0,
        })
        .await
        .expect("trusted tls get should succeed")
        .into_inner();
    assert!(response.success);
    assert_eq!(response.value, b"tls_value".to_vec());
    assert!(
        server.stderr_output().is_some(),
        "server should retain stderr capture for startup diagnostics"
    );
}

#[tokio::test]
async fn test_tls_rejects_untrusted_client_without_mutating_database() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TlsTestServer::start(false).await;
    let err = tls_client(server.address(), false, false)
        .await
        .expect_err("client without trusted CA should fail TLS handshake");
    assert!(
        !err.to_string().is_empty(),
        "tls handshake failure should surface a concrete error"
    );

    let mut trusted = server.trusted_client().await;
    let response = trusted
        .get(GetRequest {
            key: b"tls_blocked".to_vec(),
            snapshot_id: 0,
        })
        .await
        .expect("trusted tls get should succeed")
        .into_inner();
    assert!(
        !response.success && response.value.is_empty(),
        "failed untrusted handshake must not publish any value"
    );
}

#[tokio::test]
async fn test_mtls_requires_client_certificate_and_preserves_state() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TlsTestServer::start(true).await;
    let err = tls_client(server.address(), true, false)
        .await
        .expect_err("mTLS server should reject client without certificate");
    assert!(
        !err.to_string().is_empty(),
        "mTLS rejection should surface a concrete error"
    );

    let mut authed = server.mtls_client().await;
    let missing = authed
        .get(GetRequest {
            key: b"mtls_key".to_vec(),
            snapshot_id: 0,
        })
        .await
        .expect("mTLS client get should succeed")
        .into_inner();
    assert!(
        !missing.success && missing.value.is_empty(),
        "rejected certificate-less client must not mutate database state"
    );

    authed
        .write(WriteRequest {
            key: b"mtls_key".to_vec(),
            value: b"mtls_value".to_vec(),
        })
        .await
        .expect("mTLS client write should succeed");
    let stored = authed
        .get(GetRequest {
            key: b"mtls_key".to_vec(),
            snapshot_id: 0,
        })
        .await
        .expect("mTLS client get should succeed")
        .into_inner();
    assert!(stored.success);
    assert_eq!(stored.value, b"mtls_value".to_vec());
}
