use std::sync::Arc;
use std::{collections::HashSet, fs};

use clap::{ArgAction, Parser};
use goat_db::goatkv::core::kv_engine::KvEngine;
use goat_db::goatkv::utils::{init_logging, KvEngineOptions};
use goat_db::goatkv::{Error as GoatError, Result as GoatResult};
use goatkv::{
    goat_kv_service_server::{GoatKvService, GoatKvServiceServer},
    CreateSnapshotRequest, CreateSnapshotResponse, DeleteRequest, DeleteResponse, FlushRequest,
    FlushResponse, GetRequest, GetResponse, ReleaseSnapshotRequest, ReleaseSnapshotResponse,
    UpdateRequest, UpdateResponse, WriteRequest, WriteResponse,
};
use tokio::signal;
use tonic::{
    transport::{Certificate, Identity, Server, ServerTlsConfig},
    Request, Response, Status,
};
use tracing::{debug, info, warn};

// 引入编译生成的代码
pub mod goatkv {
    tonic::include_proto!("goatkv");
}

#[derive(Debug)]
pub struct GoatKVServiceImpl {
    engine: Arc<KvEngine>,
}

impl GoatKVServiceImpl {
    /// 创建新的服务实例，使用指定的 KvEngine
    pub fn new(engine: Arc<KvEngine>) -> Self {
        Self { engine }
    }

    fn map_engine_err(err: GoatError) -> Status {
        err.to_status()
    }
}

// 实现 GoatKvService trait
#[tonic::async_trait]
impl GoatKvService for GoatKVServiceImpl {
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let req = request.into_inner();

        debug!(
            "Received write request - key_len: {}, value_len: {}",
            req.key.len(),
            req.value.len()
        );

        // 验证输入
        if req.key.is_empty() {
            return Err(Status::invalid_argument("Key cannot be empty"));
        }

        self.engine
            .put(req.key, req.value)
            .map_err(Self::map_engine_err)?;

        let reply = WriteResponse {
            success: true,
            message: "Written successfully".to_string(),
        };

        Ok(Response::new(reply))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();

        debug!(
            "Received get request - key_len: {}, snapshot_id: {}",
            req.key.len(),
            req.snapshot_id
        );

        // 验证输入
        if req.key.is_empty() {
            return Err(Status::invalid_argument("Key cannot be empty"));
        }

        let value = if req.snapshot_id == 0 {
            self.engine.get(&req.key)
        } else {
            self.engine.get_with_snapshot(&req.key, req.snapshot_id)
        }
        .map_err(Self::map_engine_err)?;

        match value {
            Some(value) => {
                let reply = GetResponse {
                    success: true,
                    message: format!("Get successfully - key length: {}", req.key.len()),
                    value,
                };
                Ok(Response::new(reply))
            }
            None => {
                let reply = GetResponse {
                    success: false,
                    message: "Key not found".to_string(),
                    value: vec![],
                };
                Ok(Response::new(reply))
            }
        }
    }

    async fn update(
        &self,
        request: Request<UpdateRequest>,
    ) -> Result<Response<UpdateResponse>, Status> {
        let req = request.into_inner();

        debug!(
            "Received update request - key_len: {}, value_len: {}",
            req.key.len(),
            req.value.len()
        );

        // 验证输入
        if req.key.is_empty() {
            return Err(Status::invalid_argument("Key cannot be empty"));
        }

        // update 采用 upsert 语义：并发下不依赖先读后写。
        self.engine
            .put(req.key, req.value)
            .map_err(Self::map_engine_err)?;

        let reply = UpdateResponse {
            success: true,
            message: "Updated successfully (upsert)".to_string(),
        };

        Ok(Response::new(reply))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();

        debug!("Received delete request - key_len: {}", req.key.len());

        // 验证输入
        if req.key.is_empty() {
            return Err(Status::invalid_argument("Key cannot be empty"));
        }

        // 删除 key (即使不存在也会插入删除标记)
        self.engine.delete(req.key).map_err(Self::map_engine_err)?;

        let reply = DeleteResponse {
            success: true,
            message: "Deleted successfully".to_string(),
        };

        Ok(Response::new(reply))
    }

    async fn flush(
        &self,
        _request: Request<FlushRequest>,
    ) -> Result<Response<FlushResponse>, Status> {
        debug!("Received flush request");

        // 调用 engine 的 flush 方法
        self.engine.flush();

        let reply = FlushResponse {
            success: true,
            message: "Flush triggered successfully".to_string(),
        };

        Ok(Response::new(reply))
    }

    async fn create_snapshot(
        &self,
        _request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        debug!("Received create_snapshot request");

        let snapshot = self
            .engine
            .create_snapshot()
            .map_err(Self::map_engine_err)?;
        let reply = CreateSnapshotResponse {
            success: true,
            message: "Snapshot created successfully".to_string(),
            snapshot_id: snapshot.id,
        };
        Ok(Response::new(reply))
    }

    async fn release_snapshot(
        &self,
        request: Request<ReleaseSnapshotRequest>,
    ) -> Result<Response<ReleaseSnapshotResponse>, Status> {
        let req = request.into_inner();
        debug!(
            "Received release_snapshot request - snapshot_id: {}",
            req.snapshot_id
        );

        self.engine
            .release_snapshot(req.snapshot_id)
            .map_err(Self::map_engine_err)?;
        let reply = ReleaseSnapshotResponse {
            success: true,
            message: "Snapshot released successfully".to_string(),
        };
        Ok(Response::new(reply))
    }
}

impl Default for GoatKVServiceImpl {
    fn default() -> Self {
        Self {
            engine: Arc::new(KvEngine::new()),
        }
    }
}
#[derive(Parser)]
#[command(about = "GoatDB gRPC Server")]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:50051")]
    address: String,
    #[arg(short, long, help = "Data directory (default: ./goatdb_data)")]
    data_dir: Option<String>,
    #[arg(
        long,
        default_value_t = 0,
        help = "WAL preallocation bytes (0 disables)"
    )]
    wal_preallocate_bytes: u64,
    #[arg(
        long,
        default_value_t = 0,
        help = "WAL periodic sync bytes when wal_sync is disabled (0 disables)"
    )]
    wal_bytes_per_sync: u64,
    #[arg(long, help = "TLS certificate PEM path")]
    tls_cert_path: Option<String>,
    #[arg(long, help = "TLS private key PEM path")]
    tls_key_path: Option<String>,
    #[arg(long, help = "mTLS client CA certificate PEM path")]
    tls_client_ca_path: Option<String>,
    #[arg(
        long,
        action = ArgAction::Append,
        help = "Enable auth and accept this token (repeatable)"
    )]
    auth_token: Vec<String>,
}

fn parse_bearer_token(value: &str) -> Option<&str> {
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    if token.is_empty() {
        None
    } else {
        Some(token)
    }
}

fn extract_auth_token(metadata: &tonic::metadata::MetadataMap) -> Option<String> {
    if let Some(value) = metadata
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_bearer_token)
    {
        return Some(value.to_string());
    }

    metadata
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn sanitize_auth_tokens(tokens: Vec<String>) -> HashSet<String> {
    tokens
        .into_iter()
        .map(|token| token.trim().to_string())
        .filter(|token| !token.is_empty())
        .collect()
}

fn authorize_request(
    request: Request<()>,
    allowed_tokens: &HashSet<String>,
) -> Result<Request<()>, Status> {
    if allowed_tokens.is_empty() {
        return Ok(request);
    }

    let Some(token) = extract_auth_token(request.metadata()) else {
        return Err(Status::unauthenticated(
            "missing auth token: use `authorization: Bearer <token>` or `x-api-key`",
        ));
    };

    if allowed_tokens.contains(&token) {
        Ok(request)
    } else {
        Err(Status::unauthenticated("invalid auth token"))
    }
}

fn load_tls_config(args: &Args) -> GoatResult<Option<(ServerTlsConfig, bool)>> {
    let cert_path = args.tls_cert_path.as_deref();
    let key_path = args.tls_key_path.as_deref();
    let ca_path = args.tls_client_ca_path.as_deref();

    match (cert_path, key_path) {
        (Some(_), None) | (None, Some(_)) => Err(GoatError::invalid_argument(
            "tls",
            "both --tls-cert-path and --tls-key-path are required to enable TLS",
        )),
        (None, None) => {
            if ca_path.is_some() {
                return Err(GoatError::invalid_argument(
                    "tls_client_ca_path",
                    "--tls-client-ca-path requires --tls-cert-path and --tls-key-path",
                ));
            }
            Ok(None)
        }
        (Some(cert_path), Some(key_path)) => {
            let cert_pem = fs::read(cert_path).map_err(|e| {
                GoatError::internal_with_source(
                    "tls_cert_read",
                    format!("failed to read tls cert file {}", cert_path),
                    e,
                )
            })?;
            let key_pem = fs::read(key_path).map_err(|e| {
                GoatError::internal_with_source(
                    "tls_key_read",
                    format!("failed to read tls key file {}", key_path),
                    e,
                )
            })?;
            let identity = Identity::from_pem(cert_pem, key_pem);
            let mut tls = ServerTlsConfig::new().identity(identity);
            let mut mtls_enabled = false;
            if let Some(ca_path) = ca_path {
                let ca_pem = fs::read(ca_path).map_err(|e| {
                    GoatError::internal_with_source(
                        "tls_client_ca_read",
                        format!("failed to read tls client CA file {}", ca_path),
                        e,
                    )
                })?;
                tls = tls.client_ca_root(Certificate::from_pem(ca_pem));
                mtls_enabled = true;
            }
            Ok(Some((tls, mtls_enabled)))
        }
    }
}

#[tokio::main]
async fn main() -> GoatResult<()> {
    // 解析命令行参数
    let args = Args::parse();

    let addr = args.address.parse().map_err(|e| {
        GoatError::invalid_argument("address", format!("invalid listen address: {}", e))
    })?;

    // 创建 KvEngine，使用指定的数据目录或默认值
    let mut options = KvEngineOptions::default();
    if let Some(ref dir) = args.data_dir {
        options = options.with_data_dir(dir);
    }
    options = options
        .with_wal_preallocate_bytes(args.wal_preallocate_bytes)
        .with_wal_bytes_per_sync(args.wal_bytes_per_sync);
    // 初始化日志
    let _log_guards = init_logging("goatkv_server", &options.data_dir, "info");
    if let Some(dir) = args.data_dir.as_ref() {
        info!("Using data directory: {}", dir);
    } else {
        info!("Using default data directory (./goatdb_data)");
    }

    let tls_config = load_tls_config(&args)?;
    let raw_auth_token_count = args.auth_token.len();
    let auth_tokens = Arc::new(sanitize_auth_tokens(args.auth_token));
    if raw_auth_token_count > 0 && auth_tokens.is_empty() {
        warn!("All provided --auth-token values were empty after trimming; auth stays disabled");
    }
    if auth_tokens.is_empty() {
        info!("Auth disabled (no --auth-token provided)");
    } else {
        info!(
            "Auth enabled with {} configured token(s)",
            auth_tokens.len()
        );
    }

    let engine = Arc::new(KvEngine::new_with_options(options).map_err(|e| {
        GoatError::internal_with_source("server_init_engine", "failed to create kv engine", e)
    })?);

    let service = GoatKVServiceImpl::new(engine.clone());
    let auth_tokens_for_interceptor = Arc::clone(&auth_tokens);
    let service = GoatKvServiceServer::with_interceptor(service, move |request: Request<()>| {
        authorize_request(request, auth_tokens_for_interceptor.as_ref())
    });

    let mut server_builder = Server::builder();
    if let Some((tls, mtls_enabled)) = tls_config {
        server_builder = server_builder.tls_config(tls).map_err(|e| {
            GoatError::internal_with_source("grpc_tls_config", "failed to configure grpc tls", e)
        })?;
        if mtls_enabled {
            info!("TLS enabled with client certificate verification (mTLS)");
        } else {
            info!("TLS enabled (server-side)");
        }
    } else {
        warn!("TLS disabled; traffic is unencrypted");
    }

    info!("gRPC Server listening on {}", addr);
    info!("Starting server...");

    let serve_result = server_builder
        .add_service(service)
        .serve_with_shutdown(addr, async {
            match signal::ctrl_c().await {
                Ok(()) => info!("Received Ctrl+C, initiating graceful shutdown"),
                Err(e) => warn!("Failed to listen for Ctrl+C signal: {}", e),
            }
        })
        .await;

    info!("gRPC server stopped accepting new requests");
    if let Err(e) = engine.shutdown() {
        warn!("Engine graceful shutdown failed: {}", e);
    } else {
        info!("Engine graceful shutdown completed");
    }

    serve_result.map_err(|e| {
        GoatError::internal_with_source("grpc_server_serve", "grpc server failed", e)
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::GoatError;
    use super::{authorize_request, extract_auth_token, load_tls_config, parse_bearer_token, Args};
    use std::collections::HashSet;

    fn make_args(
        cert: Option<&str>,
        key: Option<&str>,
        client_ca: Option<&str>,
        auth_tokens: Vec<&str>,
    ) -> Args {
        Args {
            address: "127.0.0.1:50051".to_string(),
            data_dir: None,
            wal_preallocate_bytes: 0,
            wal_bytes_per_sync: 0,
            tls_cert_path: cert.map(str::to_string),
            tls_key_path: key.map(str::to_string),
            tls_client_ca_path: client_ca.map(str::to_string),
            auth_token: auth_tokens.into_iter().map(str::to_string).collect(),
        }
    }

    fn make_request(authorization: Option<&str>, api_key: Option<&str>) -> tonic::Request<()> {
        let mut request = tonic::Request::new(());
        if let Some(authorization) = authorization {
            request
                .metadata_mut()
                .insert("authorization", authorization.parse().unwrap());
        }
        if let Some(api_key) = api_key {
            request
                .metadata_mut()
                .insert("x-api-key", api_key.parse().unwrap());
        }
        request
    }

    #[test]
    fn parse_bearer_token_requires_bearer_scheme() {
        assert_eq!(parse_bearer_token("Bearer t1"), Some("t1"));
        assert_eq!(parse_bearer_token("bEaReR t2"), Some("t2"));
        assert_eq!(parse_bearer_token("Basic t3"), None);
        assert_eq!(parse_bearer_token("Bearer"), None);
        assert_eq!(parse_bearer_token("Bearer "), None);
    }

    #[test]
    fn extract_auth_token_prefers_authorization_header() {
        let request = make_request(Some("Bearer token-from-auth"), Some("token-from-key"));
        let token = extract_auth_token(request.metadata());
        assert_eq!(token.as_deref(), Some("token-from-auth"));
    }

    #[test]
    fn authorize_request_allows_when_auth_disabled() {
        let request = make_request(None, None);
        let tokens = HashSet::new();
        assert!(authorize_request(request, &tokens).is_ok());
    }

    #[test]
    fn authorize_request_rejects_missing_token_when_auth_enabled() {
        let request = make_request(None, None);
        let tokens = HashSet::from([String::from("s3cr3t")]);
        let err = authorize_request(request, &tokens).expect_err("request should be rejected");
        assert_eq!(err.code(), Code::Unauthenticated);
    }

    #[test]
    fn authorize_request_accepts_x_api_key() {
        let request = make_request(None, Some("s3cr3t"));
        let tokens = HashSet::from([String::from("s3cr3t")]);
        assert!(authorize_request(request, &tokens).is_ok());
    }

    #[test]
    fn load_tls_config_requires_cert_and_key_together() {
        let args = make_args(Some("/tmp/cert.pem"), None, None, vec![]);
        let err = load_tls_config(&args).expect_err("expected invalid tls args");
        match err {
            GoatError::InvalidArgument { param, .. } => assert_eq!(param, "tls"),
            other => panic!("unexpected error: {}", other),
        }
    }

    #[test]
    fn load_tls_config_rejects_client_ca_without_tls_identity() {
        let args = make_args(None, None, Some("/tmp/ca.pem"), vec![]);
        let err = load_tls_config(&args).expect_err("expected invalid mtls args");
        match err {
            GoatError::InvalidArgument { param, .. } => assert_eq!(param, "tls_client_ca_path"),
            other => panic!("unexpected error: {}", other),
        }
    }

    #[test]
    fn load_tls_config_is_none_when_tls_not_configured() {
        let args = make_args(None, None, None, vec![]);
        let tls = load_tls_config(&args).expect("unexpected tls parse error");
        assert!(tls.is_none());
    }
}
