use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashSet, fs};

use clap::{ArgAction, Parser};
use goat_db::goatkv::core::kv_engine::KvEngine;
use goat_db::goatkv::metrics::{RpcMethod, RpcMetricsCollector};
use goat_db::goatkv::server::health::{run_http_health_server, HealthState};
use goat_db::goatkv::utils::{init_logging, KvEngineOptions};
use goat_db::goatkv::{Error as GoatError, Result as GoatResult};
use goatkv::{
    goat_kv_service_server::{GoatKvService, GoatKvServiceServer},
    CreateSnapshotRequest, CreateSnapshotResponse, DeleteRequest, DeleteResponse, FlushRequest,
    FlushResponse, GetRequest, GetResponse, ReleaseSnapshotRequest, ReleaseSnapshotResponse,
    UpdateRequest, UpdateResponse, WriteRequest, WriteResponse,
};
use std::time::Instant;
use tokio::{signal, sync::watch, time::sleep};
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
    metrics: Arc<RpcMetricsCollector>,
}

impl GoatKVServiceImpl {
    /// 创建新的服务实例，使用指定的 KvEngine
    pub fn new(engine: Arc<KvEngine>, metrics: Arc<RpcMetricsCollector>) -> Self {
        Self { engine, metrics }
    }

    fn map_engine_err(err: GoatError) -> Status {
        err.to_status()
    }

    fn ok_response<T>(
        &self,
        method: RpcMethod,
        started_at: Instant,
        response: T,
    ) -> Result<Response<T>, Status> {
        self.metrics.observe(method, true, started_at.elapsed());
        Ok(Response::new(response))
    }

    fn err_response<T>(
        &self,
        method: RpcMethod,
        started_at: Instant,
        status: Status,
    ) -> Result<Response<T>, Status> {
        self.metrics.observe(method, false, started_at.elapsed());
        Err(status)
    }
}

// 实现 GoatKvService trait
#[tonic::async_trait]
impl GoatKvService for GoatKVServiceImpl {
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();

        debug!(
            "Received write request - key_len: {}, value_len: {}",
            req.key.len(),
            req.value.len()
        );

        // 验证输入
        if req.key.is_empty() {
            return self.err_response(
                RpcMethod::Write,
                started_at,
                Status::invalid_argument("Key cannot be empty"),
            );
        }

        if let Err(status) = self
            .engine
            .put(req.key, req.value)
            .map_err(Self::map_engine_err)
        {
            return self.err_response(RpcMethod::Write, started_at, status);
        }

        let reply = WriteResponse {
            success: true,
            message: "Written successfully".to_string(),
        };

        self.ok_response(RpcMethod::Write, started_at, reply)
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();

        debug!(
            "Received get request - key_len: {}, snapshot_id: {}",
            req.key.len(),
            req.snapshot_id
        );

        // 验证输入
        if req.key.is_empty() {
            return self.err_response(
                RpcMethod::Get,
                started_at,
                Status::invalid_argument("Key cannot be empty"),
            );
        }

        let value = if req.snapshot_id == 0 {
            self.engine.get(&req.key)
        } else {
            self.engine.get_with_snapshot(&req.key, req.snapshot_id)
        };
        let value = match value.map_err(Self::map_engine_err) {
            Ok(value) => value,
            Err(status) => return self.err_response(RpcMethod::Get, started_at, status),
        };

        match value {
            Some(value) => {
                let reply = GetResponse {
                    success: true,
                    message: format!("Get successfully - key length: {}", req.key.len()),
                    value,
                };
                self.ok_response(RpcMethod::Get, started_at, reply)
            }
            None => {
                let reply = GetResponse {
                    success: false,
                    message: "Key not found".to_string(),
                    value: vec![],
                };
                self.ok_response(RpcMethod::Get, started_at, reply)
            }
        }
    }

    async fn update(
        &self,
        request: Request<UpdateRequest>,
    ) -> Result<Response<UpdateResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();

        debug!(
            "Received update request - key_len: {}, value_len: {}",
            req.key.len(),
            req.value.len()
        );

        // 验证输入
        if req.key.is_empty() {
            return self.err_response(
                RpcMethod::Update,
                started_at,
                Status::invalid_argument("Key cannot be empty"),
            );
        }

        // update 采用 upsert 语义：并发下不依赖先读后写。
        if let Err(status) = self
            .engine
            .put(req.key, req.value)
            .map_err(Self::map_engine_err)
        {
            return self.err_response(RpcMethod::Update, started_at, status);
        }

        let reply = UpdateResponse {
            success: true,
            message: "Updated successfully (upsert)".to_string(),
        };

        self.ok_response(RpcMethod::Update, started_at, reply)
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();

        debug!("Received delete request - key_len: {}", req.key.len());

        // 验证输入
        if req.key.is_empty() {
            return self.err_response(
                RpcMethod::Delete,
                started_at,
                Status::invalid_argument("Key cannot be empty"),
            );
        }

        // 删除 key (即使不存在也会插入删除标记)
        if let Err(status) = self.engine.delete(req.key).map_err(Self::map_engine_err) {
            return self.err_response(RpcMethod::Delete, started_at, status);
        }

        let reply = DeleteResponse {
            success: true,
            message: "Deleted successfully".to_string(),
        };

        self.ok_response(RpcMethod::Delete, started_at, reply)
    }

    async fn flush(
        &self,
        _request: Request<FlushRequest>,
    ) -> Result<Response<FlushResponse>, Status> {
        let started_at = Instant::now();
        debug!("Received flush request");

        // 调用 engine 的 flush 方法
        self.engine.flush();

        let reply = FlushResponse {
            success: true,
            message: "Flush triggered successfully".to_string(),
        };

        self.ok_response(RpcMethod::Flush, started_at, reply)
    }

    async fn create_snapshot(
        &self,
        _request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let started_at = Instant::now();
        debug!("Received create_snapshot request");

        let snapshot = match self.engine.create_snapshot().map_err(Self::map_engine_err) {
            Ok(snapshot) => snapshot,
            Err(status) => return self.err_response(RpcMethod::CreateSnapshot, started_at, status),
        };
        let reply = CreateSnapshotResponse {
            success: true,
            message: "Snapshot created successfully".to_string(),
            snapshot_id: snapshot.id,
        };
        self.ok_response(RpcMethod::CreateSnapshot, started_at, reply)
    }

    async fn release_snapshot(
        &self,
        request: Request<ReleaseSnapshotRequest>,
    ) -> Result<Response<ReleaseSnapshotResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();
        debug!(
            "Received release_snapshot request - snapshot_id: {}",
            req.snapshot_id
        );

        if let Err(status) = self
            .engine
            .release_snapshot(req.snapshot_id)
            .map_err(Self::map_engine_err)
        {
            return self.err_response(RpcMethod::ReleaseSnapshot, started_at, status);
        }
        let reply = ReleaseSnapshotResponse {
            success: true,
            message: "Snapshot released successfully".to_string(),
        };
        self.ok_response(RpcMethod::ReleaseSnapshot, started_at, reply)
    }
}

impl Default for GoatKVServiceImpl {
    fn default() -> Self {
        Self {
            engine: Arc::new(KvEngine::new()),
            metrics: Arc::new(RpcMetricsCollector::new()),
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
    #[arg(long, help = "HTTP health probe listen address, e.g. 127.0.0.1:18080")]
    health_address: Option<String>,
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

const HEALTH_DRAIN_WINDOW_MS: u64 = 250;

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

fn render_metrics_text(engine: &KvEngine, collector: &RpcMetricsCollector) -> String {
    collector.render_prometheus(|text| append_engine_metrics(text, engine))
}

fn append_engine_metrics(text: &mut String, engine: &KvEngine) {
    let runtime = engine.runtime_metrics();
    text.push_str(
        "# HELP goatkv_engine_immutable_memtable_backlog Immutable memtable backlog length\n",
    );
    text.push_str("# TYPE goatkv_engine_immutable_memtable_backlog gauge\n");
    text.push_str(&format!(
        "goatkv_engine_immutable_memtable_backlog {}\n",
        runtime.immutable_memtable_backlog
    ));

    text.push_str("# HELP goatkv_engine_flush_failure_streak Consecutive flush failure count\n");
    text.push_str("# TYPE goatkv_engine_flush_failure_streak gauge\n");
    text.push_str(&format!(
        "goatkv_engine_flush_failure_streak {}\n",
        runtime.flush_failure_streak
    ));

    text.push_str("# HELP goatkv_engine_flush_circuit_open Flush circuit breaker status\n");
    text.push_str("# TYPE goatkv_engine_flush_circuit_open gauge\n");
    text.push_str(&format!(
        "goatkv_engine_flush_circuit_open {}\n",
        if runtime.flush_circuit_open { 1 } else { 0 }
    ));

    text.push_str("# HELP goatkv_engine_l0_file_count Number of level-0 SST files\n");
    text.push_str("# TYPE goatkv_engine_l0_file_count gauge\n");
    text.push_str(&format!(
        "goatkv_engine_l0_file_count {}\n",
        runtime.l0_file_count
    ));

    text.push_str(
        "# HELP goatkv_engine_pending_compaction_bytes Estimated pending compaction bytes\n",
    );
    text.push_str("# TYPE goatkv_engine_pending_compaction_bytes gauge\n");
    text.push_str(&format!(
        "goatkv_engine_pending_compaction_bytes {}\n",
        runtime.pending_compaction_bytes
    ));

    text.push_str("# HELP goatkv_writer_wal_queue_reqs Pending WAL queue requests\n");
    text.push_str("# TYPE goatkv_writer_wal_queue_reqs gauge\n");
    text.push_str(&format!(
        "goatkv_writer_wal_queue_reqs {}\n",
        runtime.writer_queue_metrics.wal_queue_reqs
    ));

    text.push_str("# HELP goatkv_writer_wal_queue_bytes Pending WAL queue bytes\n");
    text.push_str("# TYPE goatkv_writer_wal_queue_bytes gauge\n");
    text.push_str(&format!(
        "goatkv_writer_wal_queue_bytes {}\n",
        runtime.writer_queue_metrics.wal_queue_bytes
    ));

    text.push_str("# HELP goatkv_writer_mem_queue_reqs Pending Mem queue requests\n");
    text.push_str("# TYPE goatkv_writer_mem_queue_reqs gauge\n");
    text.push_str(&format!(
        "goatkv_writer_mem_queue_reqs {}\n",
        runtime.writer_queue_metrics.mem_queue_reqs
    ));

    text.push_str("# HELP goatkv_writer_mem_queue_bytes Pending Mem queue bytes\n");
    text.push_str("# TYPE goatkv_writer_mem_queue_bytes gauge\n");
    text.push_str(&format!(
        "goatkv_writer_mem_queue_bytes {}\n",
        runtime.writer_queue_metrics.mem_queue_bytes
    ));

    text.push_str("# HELP goatkv_writer_wal_inflight_groups WAL inflight group count\n");
    text.push_str("# TYPE goatkv_writer_wal_inflight_groups gauge\n");
    text.push_str(&format!(
        "goatkv_writer_wal_inflight_groups {}\n",
        runtime.writer_queue_metrics.wal_inflight_groups
    ));

    text.push_str("# HELP goatkv_writer_mem_inflight_groups Mem inflight group count\n");
    text.push_str("# TYPE goatkv_writer_mem_inflight_groups gauge\n");
    text.push_str(&format!(
        "goatkv_writer_mem_inflight_groups {}\n",
        runtime.writer_queue_metrics.mem_inflight_groups
    ));

    text.push_str("# HELP goatkv_writer_flush_blocked Flush barrier active flag\n");
    text.push_str("# TYPE goatkv_writer_flush_blocked gauge\n");
    text.push_str(&format!(
        "goatkv_writer_flush_blocked {}\n",
        if runtime.writer_queue_metrics.flush_blocked {
            1
        } else {
            0
        }
    ));

    text.push_str(
        "# HELP goatkv_writer_pressure_level Write pressure level (0=normal,1=slowdown,2=stop)\n",
    );
    text.push_str("# TYPE goatkv_writer_pressure_level gauge\n");
    text.push_str(&format!(
        "goatkv_writer_pressure_level {}\n",
        runtime.write_pressure_level
    ));

    if let Some(cache) = runtime.read_cache_metrics {
        text.push_str("# HELP goatkv_cache_table_hits_total Table cache hits\n");
        text.push_str("# TYPE goatkv_cache_table_hits_total counter\n");
        text.push_str(&format!(
            "goatkv_cache_table_hits_total {}\n",
            cache.table_hits
        ));

        text.push_str("# HELP goatkv_cache_table_misses_total Table cache misses\n");
        text.push_str("# TYPE goatkv_cache_table_misses_total counter\n");
        text.push_str(&format!(
            "goatkv_cache_table_misses_total {}\n",
            cache.table_misses
        ));

        text.push_str("# HELP goatkv_cache_table_evictions_total Table cache evictions\n");
        text.push_str("# TYPE goatkv_cache_table_evictions_total counter\n");
        text.push_str(&format!(
            "goatkv_cache_table_evictions_total {}\n",
            cache.table_evictions
        ));

        text.push_str("# HELP goatkv_cache_row_hits_total Row cache hits\n");
        text.push_str("# TYPE goatkv_cache_row_hits_total counter\n");
        text.push_str(&format!("goatkv_cache_row_hits_total {}\n", cache.row_hits));

        text.push_str("# HELP goatkv_cache_row_misses_total Row cache misses\n");
        text.push_str("# TYPE goatkv_cache_row_misses_total counter\n");
        text.push_str(&format!(
            "goatkv_cache_row_misses_total {}\n",
            cache.row_misses
        ));

        text.push_str("# HELP goatkv_cache_row_evictions_total Row cache evictions\n");
        text.push_str("# TYPE goatkv_cache_row_evictions_total counter\n");
        text.push_str(&format!(
            "goatkv_cache_row_evictions_total {}\n",
            cache.row_evictions
        ));

        text.push_str("# HELP goatkv_cache_block_hits_total Block cache hits\n");
        text.push_str("# TYPE goatkv_cache_block_hits_total counter\n");
        text.push_str(&format!(
            "goatkv_cache_block_hits_total {}\n",
            cache.block_hits
        ));

        text.push_str("# HELP goatkv_cache_block_misses_total Block cache misses\n");
        text.push_str("# TYPE goatkv_cache_block_misses_total counter\n");
        text.push_str(&format!(
            "goatkv_cache_block_misses_total {}\n",
            cache.block_misses
        ));

        text.push_str("# HELP goatkv_cache_block_evictions_total Block cache evictions\n");
        text.push_str("# TYPE goatkv_cache_block_evictions_total counter\n");
        text.push_str(&format!(
            "goatkv_cache_block_evictions_total {}\n",
            cache.block_evictions
        ));

        text.push_str("# HELP goatkv_cache_filter_hits_total Partitioned filter cache hits\n");
        text.push_str("# TYPE goatkv_cache_filter_hits_total counter\n");
        text.push_str(&format!(
            "goatkv_cache_filter_hits_total {}\n",
            cache.filter_hits
        ));

        text.push_str("# HELP goatkv_cache_filter_misses_total Partitioned filter cache misses\n");
        text.push_str("# TYPE goatkv_cache_filter_misses_total counter\n");
        text.push_str(&format!(
            "goatkv_cache_filter_misses_total {}\n",
            cache.filter_misses
        ));

        text.push_str(
            "# HELP goatkv_cache_filter_evictions_total Partitioned filter cache evictions\n",
        );
        text.push_str("# TYPE goatkv_cache_filter_evictions_total counter\n");
        text.push_str(&format!(
            "goatkv_cache_filter_evictions_total {}\n",
            cache.filter_evictions
        ));
    }
}

#[tokio::main]
async fn main() -> GoatResult<()> {
    // 解析命令行参数
    let args = Args::parse();

    let addr: SocketAddr = args.address.parse().map_err(|e| {
        GoatError::invalid_argument("address", format!("invalid listen address: {}", e))
    })?;
    let health_addr = args
        .health_address
        .as_ref()
        .map(|value| {
            value.parse::<SocketAddr>().map_err(|e| {
                GoatError::invalid_argument(
                    "health_address",
                    format!("invalid health listen address: {}", e),
                )
            })
        })
        .transpose()?;

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
    let rpc_metrics = Arc::new(RpcMetricsCollector::new());

    let service = GoatKVServiceImpl::new(engine.clone(), rpc_metrics.clone());
    let auth_tokens_for_interceptor = Arc::clone(&auth_tokens);
    let service = GoatKvServiceServer::with_interceptor(service, move |request: Request<()>| {
        authorize_request(request, auth_tokens_for_interceptor.as_ref())
    });

    let health_state = Arc::new(HealthState::new());
    let mut health_shutdown_tx: Option<watch::Sender<bool>> = None;
    let mut health_server_task = None;
    if let Some(health_addr) = health_addr {
        let listener = tokio::net::TcpListener::bind(health_addr)
            .await
            .map_err(|e| GoatError::io("health_probe_bind", e))?;
        let bound_addr = listener
            .local_addr()
            .map_err(|e| GoatError::io("health_probe_local_addr", e))?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let state = Arc::clone(&health_state);
        let metrics_engine = Arc::clone(&engine);
        let metrics_collector = Arc::clone(&rpc_metrics);
        let metrics_renderer = Arc::new(move || {
            render_metrics_text(metrics_engine.as_ref(), metrics_collector.as_ref())
        });
        health_server_task = Some(tokio::spawn(async move {
            run_http_health_server(listener, state, Some(metrics_renderer), shutdown_rx).await
        }));
        health_shutdown_tx = Some(shutdown_tx);
        info!(
            "Health probe listening on http://{} (/livez, /readyz, /metrics)",
            bound_addr
        );
    } else {
        info!("Health probe disabled (no --health-address provided)");
    }

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
    health_state.set_ready(true);
    let health_state_for_shutdown = Arc::clone(&health_state);

    let serve_result = server_builder
        .add_service(service)
        .serve_with_shutdown(addr, async move {
            match signal::ctrl_c().await {
                Ok(()) => info!("Received Ctrl+C, initiating graceful shutdown"),
                Err(e) => warn!("Failed to listen for Ctrl+C signal: {}", e),
            }
            health_state_for_shutdown.set_ready(false);
            sleep(Duration::from_millis(HEALTH_DRAIN_WINDOW_MS)).await;
        })
        .await;

    info!("gRPC server stopped accepting new requests");
    health_state.set_ready(false);
    if let Err(e) = engine.shutdown() {
        warn!("Engine graceful shutdown failed: {}", e);
    } else {
        info!("Engine graceful shutdown completed");
    }
    health_state.set_live(false);
    if let Some(shutdown_tx) = health_shutdown_tx {
        let _ = shutdown_tx.send(true);
    }
    if let Some(task) = health_server_task {
        match task.await {
            Ok(Ok(())) => info!("Health probe server stopped"),
            Ok(Err(e)) => warn!("Health probe server stopped with IO error: {}", e),
            Err(e) => warn!("Health probe task join error: {}", e),
        }
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
            health_address: None,
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
