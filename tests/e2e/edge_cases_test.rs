#[path = "../common/mod.rs"]
mod common;

use std::time::Duration;

use common::test_server::goatkv::{
    DeleteRequest, FlushRequest, GetRequest, UpdateRequest, WriteRequest,
};
use common::test_server::{should_skip_network_e2e, TestServer, TestServerOptions};
use tonic::Code;

#[tokio::test]
async fn test_rejects_empty_key_requests() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    let status = client
        .write(WriteRequest {
            key: vec![],
            value: b"value".to_vec(),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);

    let status = client.get(GetRequest { key: vec![] }).await.unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);

    let status = client
        .update(UpdateRequest {
            key: vec![],
            value: b"value".to_vec(),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);

    let status = client
        .delete(DeleteRequest { key: vec![] })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);

    // 服务器应仍可正常处理请求
    let ok = client
        .write(WriteRequest {
            key: b"ok".to_vec(),
            value: b"v".to_vec(),
        })
        .await
        .unwrap();
    assert!(ok.into_inner().success);
}

#[tokio::test]
async fn test_update_nonexistent_key_is_upsert() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    let resp = client
        .update(UpdateRequest {
            key: b"missing_key".to_vec(),
            value: b"value".to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success);

    let get_resp = client
        .get(GetRequest {
            key: b"missing_key".to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(get_resp.success);
    assert_eq!(get_resp.value, b"value".to_vec());
}

#[tokio::test]
async fn test_binary_and_large_value_roundtrip() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    let binary_key = b"bin\0key".to_vec();
    let binary_value: Vec<u8> = (0u8..=255).collect();
    client
        .write(WriteRequest {
            key: binary_key.clone(),
            value: binary_value.clone(),
        })
        .await
        .unwrap();
    let resp = client
        .get(GetRequest { key: binary_key })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success);
    assert_eq!(resp.value, binary_value);

    let large_key = b"large_value_key".to_vec();
    let large_value = vec![0xAB; 128 * 1024];
    client
        .write(WriteRequest {
            key: large_key.clone(),
            value: large_value.clone(),
        })
        .await
        .unwrap();
    let resp = client
        .get(GetRequest { key: large_key })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success);
    assert_eq!(resp.value, large_value);
}

#[tokio::test]
async fn test_persistence_across_restart() {
    if should_skip_network_e2e() {
        return;
    }

    let temp_dir = tempfile::tempdir().unwrap();
    let data_dir = temp_dir.path().to_path_buf();

    let mut server = TestServer::start_with_options(TestServerOptions {
        port: None,
        data_dir: Some(data_dir.clone()),
        show_logs: false,
        capture_stderr: true,
    })
    .await;
    let mut client = server.client().await;

    client
        .write(WriteRequest {
            key: b"persist_key".to_vec(),
            value: b"persist_value".to_vec(),
        })
        .await
        .unwrap();

    let flush = client.flush(FlushRequest {}).await.unwrap().into_inner();
    assert!(flush.success);

    server.kill();
    drop(server);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let server = TestServer::start_with_options(TestServerOptions {
        port: None,
        data_dir: Some(data_dir),
        show_logs: false,
        capture_stderr: true,
    })
    .await;
    let mut client = server.client().await;
    let resp = client
        .get(GetRequest {
            key: b"persist_key".to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success);
    assert_eq!(resp.value, b"persist_value".to_vec());
}

#[tokio::test]
async fn test_delete_nonexistent_key_is_idempotent() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    for _ in 0..3 {
        let resp = client
            .delete(DeleteRequest {
                key: b"missing_key".to_vec(),
            })
            .await
            .unwrap()
            .into_inner();
        assert!(resp.success);
    }

    let get_resp = client
        .get(GetRequest {
            key: b"missing_key".to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!get_resp.success);
}

#[tokio::test]
async fn test_flush_then_immediate_read_consistency() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    client
        .write(WriteRequest {
            key: b"flush_key".to_vec(),
            value: b"flush_value".to_vec(),
        })
        .await
        .unwrap();

    let flush = client.flush(FlushRequest {}).await.unwrap().into_inner();
    assert!(flush.success);

    let resp = client
        .get(GetRequest {
            key: b"flush_key".to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success);
    assert_eq!(resp.value, b"flush_value".to_vec());
}

#[tokio::test]
async fn test_l0_newest_file_wins_on_overlap() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    client
        .write(WriteRequest {
            key: b"overlap_key".to_vec(),
            value: b"value_v1".to_vec(),
        })
        .await
        .unwrap();
    let flush = client.flush(FlushRequest {}).await.unwrap().into_inner();
    assert!(flush.success);

    client
        .write(WriteRequest {
            key: b"overlap_key".to_vec(),
            value: b"value_v2".to_vec(),
        })
        .await
        .unwrap();
    let flush = client.flush(FlushRequest {}).await.unwrap().into_inner();
    assert!(flush.success);

    let resp = client
        .get(GetRequest {
            key: b"overlap_key".to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success);
    assert_eq!(resp.value, b"value_v2".to_vec());
}

#[tokio::test]
async fn test_empty_flush_is_noop() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    let flush = client.flush(FlushRequest {}).await.unwrap().into_inner();
    assert!(flush.success);

    let flush = client.flush(FlushRequest {}).await.unwrap().into_inner();
    assert!(flush.success);

    let resp = client
        .get(GetRequest {
            key: b"missing_key".to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.success);

    client
        .write(WriteRequest {
            key: b"after_empty_flush".to_vec(),
            value: b"value".to_vec(),
        })
        .await
        .unwrap();

    let resp = client
        .get(GetRequest {
            key: b"after_empty_flush".to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.success);
    assert_eq!(resp.value, b"value".to_vec());
}
