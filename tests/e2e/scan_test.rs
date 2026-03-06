#[path = "../common/mod.rs"]
mod common;

use common::test_server::goatkv::{
    CreateSnapshotRequest, DeleteRequest, ReleaseSnapshotRequest, ScanRequest, WriteRequest,
};
use common::test_server::{should_skip_network_e2e, total_wal_bytes, TestServer};
use tonic::Code;

#[tokio::test]
async fn test_scan_respects_prefix_reverse_and_limit() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    for (key, value) in [
        (b"scan:a".to_vec(), b"va".to_vec()),
        (b"scan:b".to_vec(), b"vb".to_vec()),
        (b"scan:c".to_vec(), b"vc".to_vec()),
        (b"other:z".to_vec(), b"vz".to_vec()),
    ] {
        client.write(WriteRequest { key, value }).await.unwrap();
    }

    let response = client
        .scan(ScanRequest {
            start_key: b"scan:a".to_vec(),
            end_key: b"scan:z".to_vec(),
            prefix: b"scan:".to_vec(),
            limit: 2,
            reverse: true,
            snapshot_id: 0,
        })
        .await
        .unwrap()
        .into_inner();

    assert!(response.success);
    assert_eq!(response.entries.len(), 2);
    assert_eq!(response.entries[0].key, b"scan:c".to_vec());
    assert_eq!(response.entries[0].value, b"vc".to_vec());
    assert_eq!(response.entries[1].key, b"scan:b".to_vec());
    assert_eq!(response.entries[1].value, b"vb".to_vec());
}

#[tokio::test]
async fn test_scan_snapshot_sees_old_visible_set() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    client
        .write(WriteRequest {
            key: b"district:1".to_vec(),
            value: b"v1".to_vec(),
        })
        .await
        .unwrap();
    client
        .write(WriteRequest {
            key: b"district:2".to_vec(),
            value: b"v2".to_vec(),
        })
        .await
        .unwrap();

    let snapshot = client
        .create_snapshot(CreateSnapshotRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(snapshot.success);

    client
        .write(WriteRequest {
            key: b"district:1".to_vec(),
            value: b"v1-new".to_vec(),
        })
        .await
        .unwrap();
    client
        .delete(DeleteRequest {
            key: b"district:2".to_vec(),
        })
        .await
        .unwrap();

    let latest = client
        .scan(ScanRequest {
            start_key: Vec::new(),
            end_key: Vec::new(),
            prefix: b"district:".to_vec(),
            limit: 0,
            reverse: false,
            snapshot_id: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(latest.success);
    assert_eq!(latest.entries.len(), 1);
    assert_eq!(latest.entries[0].key, b"district:1".to_vec());
    assert_eq!(latest.entries[0].value, b"v1-new".to_vec());

    let at_snapshot = client
        .scan(ScanRequest {
            start_key: Vec::new(),
            end_key: Vec::new(),
            prefix: b"district:".to_vec(),
            limit: 0,
            reverse: false,
            snapshot_id: snapshot.snapshot_id,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(at_snapshot.success);
    assert_eq!(at_snapshot.entries.len(), 2);
    assert_eq!(at_snapshot.entries[0].key, b"district:1".to_vec());
    assert_eq!(at_snapshot.entries[0].value, b"v1".to_vec());
    assert_eq!(at_snapshot.entries[1].key, b"district:2".to_vec());
    assert_eq!(at_snapshot.entries[1].value, b"v2".to_vec());

    let release = client
        .release_snapshot(ReleaseSnapshotRequest {
            snapshot_id: snapshot.snapshot_id,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(release.success);
}

#[tokio::test]
async fn test_scan_invalid_snapshot_is_not_found_and_read_only() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;
    let wal_before = total_wal_bytes(&server.data_dir);

    let err = client
        .scan(ScanRequest {
            start_key: Vec::new(),
            end_key: Vec::new(),
            prefix: Vec::new(),
            limit: 0,
            reverse: false,
            snapshot_id: 999999,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    assert_eq!(total_wal_bytes(&server.data_dir), wal_before);
}

#[tokio::test]
async fn test_scan_zero_limit_returns_full_visible_set() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    for (key, value) in [
        (b"limit:a".to_vec(), b"va".to_vec()),
        (b"limit:b".to_vec(), b"vb".to_vec()),
        (b"limit:c".to_vec(), b"vc".to_vec()),
    ] {
        client.write(WriteRequest { key, value }).await.unwrap();
    }

    let full = client
        .scan(ScanRequest {
            start_key: Vec::new(),
            end_key: Vec::new(),
            prefix: b"limit:".to_vec(),
            limit: 0,
            reverse: false,
            snapshot_id: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(full.success);
    assert_eq!(full.entries.len(), 3);

    let limited = client
        .scan(ScanRequest {
            start_key: Vec::new(),
            end_key: Vec::new(),
            prefix: b"limit:".to_vec(),
            limit: 2,
            reverse: false,
            snapshot_id: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(limited.success);
    assert_eq!(limited.entries.len(), 2);
    assert_eq!(full.entries[0].key, b"limit:a".to_vec());
    assert_eq!(full.entries[2].key, b"limit:c".to_vec());
}
