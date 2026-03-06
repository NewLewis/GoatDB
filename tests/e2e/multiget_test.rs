#[path = "../common/mod.rs"]
mod common;

use common::test_server::goatkv::{MultiGetRequest, WriteRequest};
use common::test_server::{should_skip_network_e2e, TestServer};
use tonic::Code;

#[tokio::test]
async fn test_multiget_returns_mixed_hit_and_miss() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    client
        .write(WriteRequest {
            key: b"mg_key_1".to_vec(),
            value: b"v1".to_vec(),
        })
        .await
        .unwrap();
    client
        .write(WriteRequest {
            key: b"mg_key_2".to_vec(),
            value: b"v2".to_vec(),
        })
        .await
        .unwrap();

    let response = client
        .multi_get(MultiGetRequest {
            keys: vec![
                b"mg_key_1".to_vec(),
                b"missing".to_vec(),
                b"mg_key_2".to_vec(),
            ],
            snapshot_id: 0,
        })
        .await
        .unwrap()
        .into_inner();

    assert!(response.success);
    assert_eq!(response.entries.len(), 3);

    assert_eq!(response.entries[0].key, b"mg_key_1".to_vec());
    assert!(response.entries[0].found);
    assert_eq!(response.entries[0].value, b"v1".to_vec());

    assert_eq!(response.entries[1].key, b"missing".to_vec());
    assert!(!response.entries[1].found);
    assert!(response.entries[1].value.is_empty());

    assert_eq!(response.entries[2].key, b"mg_key_2".to_vec());
    assert!(response.entries[2].found);
    assert_eq!(response.entries[2].value, b"v2".to_vec());
}

#[tokio::test]
async fn test_multiget_rejects_empty_keys() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    let err = client
        .multi_get(MultiGetRequest {
            keys: vec![],
            snapshot_id: 0,
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn test_multiget_rejects_nonzero_snapshot_id() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    let err = client
        .multi_get(MultiGetRequest {
            keys: vec![b"any_key".to_vec()],
            snapshot_id: 1,
        })
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::InvalidArgument);
}
