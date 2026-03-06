#[path = "../common/mod.rs"]
mod common;

use common::test_server::goatkv::{CompareAndSetRequest, GetRequest, WriteRequest};
use common::test_server::{should_skip_network_e2e, TestServer};
use tonic::Code;

#[tokio::test]
async fn test_compare_and_set_updates_when_expected_matches() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    client
        .write(WriteRequest {
            key: b"cas:key".to_vec(),
            value: b"v1".to_vec(),
        })
        .await
        .unwrap();

    let response = client
        .compare_and_set(CompareAndSetRequest {
            key: b"cas:key".to_vec(),
            expect_exists: true,
            expected_value: b"v1".to_vec(),
            new_value: b"v2".to_vec(),
            delete_on_match: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(response.success);

    let get = client
        .get(GetRequest {
            key: b"cas:key".to_vec(),
            snapshot_id: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(get.success);
    assert_eq!(get.value, b"v2".to_vec());
}

#[tokio::test]
async fn test_compare_and_set_supports_insert_and_delete() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    let insert = client
        .compare_and_set(CompareAndSetRequest {
            key: b"cas:new".to_vec(),
            expect_exists: false,
            expected_value: Vec::new(),
            new_value: b"v1".to_vec(),
            delete_on_match: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(insert.success);

    let delete = client
        .compare_and_set(CompareAndSetRequest {
            key: b"cas:new".to_vec(),
            expect_exists: true,
            expected_value: b"v1".to_vec(),
            new_value: Vec::new(),
            delete_on_match: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(delete.success);

    let get = client
        .get(GetRequest {
            key: b"cas:new".to_vec(),
            snapshot_id: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!get.success);
}

#[tokio::test]
async fn test_compare_and_set_returns_conflict_on_mismatch() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    client
        .write(WriteRequest {
            key: b"cas:conflict".to_vec(),
            value: b"v1".to_vec(),
        })
        .await
        .unwrap();

    let err = client
        .compare_and_set(CompareAndSetRequest {
            key: b"cas:conflict".to_vec(),
            expect_exists: true,
            expected_value: b"wrong".to_vec(),
            new_value: b"v2".to_vec(),
            delete_on_match: false,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    let get = client
        .get(GetRequest {
            key: b"cas:conflict".to_vec(),
            snapshot_id: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(get.success);
    assert_eq!(get.value, b"v1".to_vec());
}

#[tokio::test]
async fn test_compare_and_set_delete_on_match_takes_precedence_over_new_value() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    client
        .write(WriteRequest {
            key: b"cas:delete-priority".to_vec(),
            value: b"v1".to_vec(),
        })
        .await
        .unwrap();

    let response = client
        .compare_and_set(CompareAndSetRequest {
            key: b"cas:delete-priority".to_vec(),
            expect_exists: true,
            expected_value: b"v1".to_vec(),
            new_value: b"should_be_ignored".to_vec(),
            delete_on_match: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(response.success);

    let get = client
        .get(GetRequest {
            key: b"cas:delete-priority".to_vec(),
            snapshot_id: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!get.success);
}
