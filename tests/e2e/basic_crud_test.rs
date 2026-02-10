#[path = "../common/mod.rs"]
mod common;

use common::test_server::goatkv::{DeleteRequest, GetRequest, UpdateRequest, WriteRequest};
use common::test_server::{should_skip_network_e2e, TestServer};

/// 基本写入读取测试
#[tokio::test]
async fn test_write_and_read() {
    if should_skip_network_e2e() {
        return;
    }

    // 启动测试服务器
    let server = TestServer::start().await;
    let mut client = server.client().await;

    // 写入数据
    let write_req = WriteRequest {
        key: b"test_key".to_vec(),
        value: b"test_value".to_vec(),
    };
    let response = client.write(write_req).await.unwrap();
    assert!(response.into_inner().success);

    // 读取数据
    let get_req = GetRequest {
        key: b"test_key".to_vec(),
    };
    let response = client.get(get_req).await.unwrap();
    let data = response.into_inner();
    assert!(data.success);
    assert_eq!(data.value, b"test_value");
}

/// 更新已有键的测试
#[tokio::test]
async fn test_update_existing_key() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    // 1. 写入初始值
    client
        .write(WriteRequest {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        })
        .await
        .unwrap();

    // 2. 更新值
    let response = client
        .update(UpdateRequest {
            key: b"key1".to_vec(),
            value: b"value2".to_vec(),
        })
        .await
        .unwrap();
    assert!(response.into_inner().success);

    // 3. 验证更新成功
    let get_resp = client
        .get(GetRequest {
            key: b"key1".to_vec(),
        })
        .await
        .unwrap();
    assert_eq!(get_resp.into_inner().value, b"value2");
}

/// 删除键的测试
#[tokio::test]
async fn test_delete_key() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    // 1. 写入数据
    client
        .write(WriteRequest {
            key: b"to_delete".to_vec(),
            value: b"value".to_vec(),
        })
        .await
        .unwrap();

    // 2. 删除
    let del_resp = client
        .delete(DeleteRequest {
            key: b"to_delete".to_vec(),
        })
        .await
        .unwrap();
    assert!(del_resp.into_inner().success);

    // 3. 验证已删除
    let get_resp = client
        .get(GetRequest {
            key: b"to_delete".to_vec(),
        })
        .await
        .unwrap();
    assert!(!get_resp.into_inner().success);
}

/// 测试不存在的键读取
#[tokio::test]
async fn test_get_non_existent_key() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    // 尝试读取不存在的键
    let get_resp = client
        .get(GetRequest {
            key: b"non_existent_key".to_vec(),
        })
        .await
        .unwrap();

    let data = get_resp.into_inner();
    assert!(!data.success);
    assert!(data.value.is_empty());
}

/// 测试重复写入同一个键
#[tokio::test]
async fn test_write_same_key_multiple_times() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    // 第一次写入
    let response1 = client
        .write(WriteRequest {
            key: b"repeated_key".to_vec(),
            value: b"first_value".to_vec(),
        })
        .await
        .unwrap();
    assert!(response1.into_inner().success);

    // 第二次写入（应该覆盖）
    let response2 = client
        .write(WriteRequest {
            key: b"repeated_key".to_vec(),
            value: b"second_value".to_vec(),
        })
        .await
        .unwrap();
    assert!(response2.into_inner().success);

    // 验证获取的是最后一个值
    let get_resp = client
        .get(GetRequest {
            key: b"repeated_key".to_vec(),
        })
        .await
        .unwrap();
    assert_eq!(get_resp.into_inner().value, b"second_value");
}

/// 测试多个键值对的CRUD操作
#[tokio::test]
async fn test_multiple_keys_crud() {
    if should_skip_network_e2e() {
        return;
    }

    let server = TestServer::start().await;
    let mut client = server.client().await;

    // 定义测试数据
    let test_cases = vec![
        (b"key_a".to_vec(), b"value_a".to_vec()),
        (b"key_b".to_vec(), b"value_b".to_vec()),
        (b"key_c".to_vec(), b"value_c".to_vec()),
        (b"key_d".to_vec(), b"value_d".to_vec()),
    ];

    // 写入所有数据
    for (key, value) in &test_cases {
        let response = client
            .write(WriteRequest {
                key: key.clone(),
                value: value.clone(),
            })
            .await
            .unwrap();
        assert!(response.into_inner().success);
    }

    // 验证所有数据都能正确读取
    for (key, expected_value) in &test_cases {
        let response = client.get(GetRequest { key: key.clone() }).await.unwrap();

        let data = response.into_inner();
        assert!(data.success);
        assert_eq!(data.value, *expected_value);
    }

    // 更新一些键
    let update_response = client
        .update(UpdateRequest {
            key: b"key_b".to_vec(),
            value: b"updated_value_b".to_vec(),
        })
        .await
        .unwrap();
    assert!(update_response.into_inner().success);

    // 验证更新
    let get_response = client
        .get(GetRequest {
            key: b"key_b".to_vec(),
        })
        .await
        .unwrap();
    assert_eq!(get_response.into_inner().value, b"updated_value_b");

    // 删除一个键
    let delete_response = client
        .delete(DeleteRequest {
            key: b"key_c".to_vec(),
        })
        .await
        .unwrap();
    assert!(delete_response.into_inner().success);

    // 验证删除
    let get_deleted = client
        .get(GetRequest {
            key: b"key_c".to_vec(),
        })
        .await
        .unwrap();
    assert!(!get_deleted.into_inner().success);
}
