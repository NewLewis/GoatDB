#[path = "../common/mod.rs"]
mod common;

use std::time::Duration;

use common::test_server::goatkv::{GetRequest, WriteRequest};
use common::test_server::{should_skip_network_e2e, GoatKvServiceClient, TestServer};
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use tokio::task::JoinHandle;

/// 多客户端并发写入测试
#[tokio::test]
async fn test_concurrent_writes_from_multiple_clients() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;

    // 启动 10 个并发客户端
    let tasks: Vec<JoinHandle<()>> = (0..10)
        .map(|client_id| {
            let address = server.address.clone();
            tokio::spawn(async move {
                let mut client = GoatKvServiceClient::connect(address).await.unwrap();

                // 每个客户端写入 100 条数据
                for i in 0..100 {
                    let key = format!("client_{}_key_{}", client_id, i);
                    let value = format!("value_{}", i);

                    client
                        .write(WriteRequest {
                            key: key.into_bytes(),
                            value: value.into_bytes(),
                        })
                        .await
                        .unwrap();
                }
            })
        })
        .collect();

    // 等待所有客户端完成
    for task in tasks {
        task.await.unwrap();
    }

    // 验证所有数据都写入成功
    let mut client = server.client().await;
    for client_id in 0..10 {
        for i in 0..100 {
            let key = format!("client_{}_key_{}", client_id, i);
            let response = client
                .get(GetRequest {
                    key: key.into_bytes(),
                    snapshot_id: 0,
                })
                .await
                .unwrap();
            assert!(response.into_inner().success);
        }
    }
}

/// 并发读写混合测试
#[tokio::test]
async fn test_concurrent_read_write() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;

    // 预填充一些数据
    let mut client = server.client().await;
    for i in 0..100 {
        client
            .write(WriteRequest {
                key: format!("key_{}", i).into_bytes(),
                value: format!("value_{}", i).into_bytes(),
            })
            .await
            .unwrap();
    }

    // 启动读写混合任务
    let write_task = {
        let address = server.address.clone();
        tokio::spawn(async move {
            let mut client = GoatKvServiceClient::connect(address).await.unwrap();
            for i in 100..200 {
                client
                    .write(WriteRequest {
                        key: format!("key_{}", i).into_bytes(),
                        value: format!("value_{}", i).into_bytes(),
                    })
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    };

    let read_tasks: Vec<JoinHandle<()>> = (0..5)
        .map(|_| {
            let address = server.address.clone();
            tokio::spawn(async move {
                let mut client = GoatKvServiceClient::connect(address).await.unwrap();
                let mut rng = StdRng::from_entropy();
                for _ in 0..50 {
                    let key_id = rng.gen_range(0..150);
                    let _ = client
                        .get(GetRequest {
                            key: format!("key_{}", key_id).into_bytes(),
                            snapshot_id: 0,
                        })
                        .await;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
        })
        .collect();

    write_task.await.unwrap();
    for task in read_tasks {
        task.await.unwrap();
    }
}
