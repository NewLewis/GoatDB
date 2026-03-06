#[path = "../common/mod.rs"]
mod common;

use common::test_server::{should_skip_network_e2e, TestServer, TestServerOptions};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Code;

fn bearer_value(token: &str) -> MetadataValue<tonic::metadata::Ascii> {
    format!("Bearer {token}")
        .parse()
        .expect("build authorization header")
}

async fn authed_bearer_client(
    server: &TestServer,
    token: &str,
) -> common::test_server::GoatKvServiceClient<
    tonic::service::interceptor::InterceptedService<
        Channel,
        impl FnMut(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
    >,
> {
    let channel = Channel::from_shared(server.address.clone())
        .expect("build channel")
        .connect()
        .await
        .expect("connect channel");
    let header = bearer_value(token);
    common::test_server::GoatKvServiceClient::with_interceptor(
        channel,
        move |mut request: tonic::Request<()>| {
            request
                .metadata_mut()
                .insert("authorization", header.clone());
            Ok(request)
        },
    )
}

async fn authed_api_key_client(
    server: &TestServer,
    token: &str,
) -> common::test_server::GoatKvServiceClient<
    tonic::service::interceptor::InterceptedService<
        Channel,
        impl FnMut(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
    >,
> {
    let channel = Channel::from_shared(server.address.clone())
        .expect("build channel")
        .connect()
        .await
        .expect("connect channel");
    let header: MetadataValue<tonic::metadata::Ascii> =
        token.parse().expect("build x-api-key header");
    common::test_server::GoatKvServiceClient::with_interceptor(
        channel,
        move |mut request: tonic::Request<()>| {
            request.metadata_mut().insert("x-api-key", header.clone());
            Ok(request)
        },
    )
}

#[tokio::test]
async fn test_auth_rejects_missing_token_without_mutating_database() {
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

    let mut unauthenticated = server.client().await;
    let err = unauthenticated
        .write(common::test_server::goatkv::WriteRequest {
            key: b"auth_missing".to_vec(),
            value: b"blocked".to_vec(),
        })
        .await
        .expect_err("write without auth should be rejected");
    assert_eq!(err.code(), Code::Unauthenticated);

    let mut authorized = authed_bearer_client(&server, "secret-token").await;
    let get = authorized
        .get(common::test_server::goatkv::GetRequest {
            key: b"auth_missing".to_vec(),
            snapshot_id: 0,
        })
        .await
        .expect("authorized get should succeed")
        .into_inner();
    assert!(
        !get.success && get.value.is_empty(),
        "rejected unauthenticated write must not publish any value"
    );
}

#[tokio::test]
async fn test_auth_accepts_bearer_token_and_persists_data() {
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

    let mut client = authed_bearer_client(&server, "secret-token").await;
    client
        .write(common::test_server::goatkv::WriteRequest {
            key: b"auth_bearer".to_vec(),
            value: b"persisted".to_vec(),
        })
        .await
        .expect("bearer-authenticated write should succeed");

    let get = client
        .get(common::test_server::goatkv::GetRequest {
            key: b"auth_bearer".to_vec(),
            snapshot_id: 0,
        })
        .await
        .expect("bearer-authenticated get should succeed")
        .into_inner();
    assert!(get.success);
    assert_eq!(get.value, b"persisted".to_vec());
}

#[tokio::test]
async fn test_auth_rejects_wrong_api_key_without_mutating_database() {
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

    let mut wrong_client = authed_api_key_client(&server, "wrong-token").await;
    let err = wrong_client
        .write(common::test_server::goatkv::WriteRequest {
            key: b"auth_apikey".to_vec(),
            value: b"blocked".to_vec(),
        })
        .await
        .expect_err("write with wrong api key should be rejected");
    assert_eq!(err.code(), Code::Unauthenticated);

    let mut valid_client = authed_api_key_client(&server, "secret-token").await;
    let get = valid_client
        .get(common::test_server::goatkv::GetRequest {
            key: b"auth_apikey".to_vec(),
            snapshot_id: 0,
        })
        .await
        .expect("valid api key get should succeed")
        .into_inner();
    assert!(
        !get.success && get.value.is_empty(),
        "rejected wrong-token write must not publish any value"
    );
}
