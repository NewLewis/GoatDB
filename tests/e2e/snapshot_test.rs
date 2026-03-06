#[path = "../common/mod.rs"]
mod common;

use common::test_server::goatkv::{
    CreateSnapshotRequest, FlushRequest, GetRequest, ReleaseSnapshotRequest, ScanRequest,
    WriteRequest,
};
use common::test_server::{should_skip_network_e2e, total_wal_bytes, TestServer};
use tonic::Code;

#[tokio::test]
async fn test_snapshot_get_survives_updates_and_flushes() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    client
        .write(WriteRequest {
            key: b"snap_key".to_vec(),
            value: b"v1".to_vec(),
        })
        .await
        .unwrap();
    let flush = client.flush(FlushRequest {}).await.unwrap().into_inner();
    assert!(flush.success);

    let snapshot = client
        .create_snapshot(CreateSnapshotRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(snapshot.success);
    assert!(snapshot.snapshot_id > 0);

    for seq in 2..=10 {
        client
            .write(WriteRequest {
                key: b"snap_key".to_vec(),
                value: format!("v{}", seq).into_bytes(),
            })
            .await
            .unwrap();
        let flush = client.flush(FlushRequest {}).await.unwrap().into_inner();
        assert!(flush.success);
    }

    let latest = client
        .get(GetRequest {
            key: b"snap_key".to_vec(),
            snapshot_id: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(latest.success);
    assert_eq!(latest.value, b"v10".to_vec());

    let at_snapshot = client
        .get(GetRequest {
            key: b"snap_key".to_vec(),
            snapshot_id: snapshot.snapshot_id,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(at_snapshot.success);
    assert_eq!(at_snapshot.value, b"v1".to_vec());

    let scan_at_snapshot = client
        .scan(ScanRequest {
            start_key: Vec::new(),
            end_key: Vec::new(),
            prefix: b"snap_".to_vec(),
            limit: 0,
            reverse: false,
            snapshot_id: snapshot.snapshot_id,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(scan_at_snapshot.success);
    assert_eq!(scan_at_snapshot.entries.len(), 1);
    assert_eq!(scan_at_snapshot.entries[0].key, b"snap_key".to_vec());
    assert_eq!(scan_at_snapshot.entries[0].value, b"v1".to_vec());

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
async fn test_released_snapshot_id_is_not_found() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    client
        .write(WriteRequest {
            key: b"snap_key_2".to_vec(),
            value: b"v1".to_vec(),
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
        .release_snapshot(ReleaseSnapshotRequest {
            snapshot_id: snapshot.snapshot_id,
        })
        .await
        .unwrap();
    let wal_before = total_wal_bytes(&server.data_dir);

    let err = client
        .get(GetRequest {
            key: b"snap_key_2".to_vec(),
            snapshot_id: snapshot.snapshot_id,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);

    let err = client
        .release_snapshot(ReleaseSnapshotRequest {
            snapshot_id: snapshot.snapshot_id,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    assert_eq!(total_wal_bytes(&server.data_dir), wal_before);
}

#[tokio::test]
async fn test_releasing_unknown_snapshot_is_not_found_and_read_only() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;
    let wal_before = total_wal_bytes(&server.data_dir);

    let err = client
        .release_snapshot(ReleaseSnapshotRequest { snapshot_id: 0 })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);

    let err = client
        .release_snapshot(ReleaseSnapshotRequest {
            snapshot_id: 424242,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    assert_eq!(total_wal_bytes(&server.data_dir), wal_before);
}
