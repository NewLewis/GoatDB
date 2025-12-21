use std::sync::{Arc, Mutex};

use clap::Parser;
use goat_db::goatkv::kv_engine::KvEngine;
use goatkv::{
    goat_kv_service_server::{GoatKvService, GoatKvServiceServer},
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, UpdateRequest, UpdateResponse,
    WriteRequest, WriteResponse,
};
use tonic::{transport::Server, Request, Response, Status};

// 引入编译生成的代码
pub mod goatkv {
    tonic::include_proto!("goatkv");
}

#[derive(Debug)]
pub struct GoatKVServiceImpl {
    engine: Arc<Mutex<KvEngine>>,
}

impl GoatKVServiceImpl {
    /// 创建新的服务实例，使用指定的 KvEngine
    pub fn new(engine: KvEngine) -> Self {
        Self {
            engine: Arc::new(Mutex::new(engine)),
        }
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

        println!(
            "Received write request - key_len: {}, value_len: {}",
            req.key.len(),
            req.value.len()
        );

        // 验证输入
        if req.key.is_empty() {
            return Err(Status::invalid_argument("Key cannot be empty"));
        }

        self.engine.lock().unwrap().put(req.key, req.value);

        let reply = WriteResponse {
            success: true,
            message: format!("Written successfully"),
        };

        Ok(Response::new(reply))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();

        println!("Received get request - key_len: {}", req.key.len());

        // 验证输入
        if req.key.is_empty() {
            return Err(Status::invalid_argument("Key cannot be empty"));
        }

        match self.engine.lock().unwrap().get(&req.key) {
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
                    message: format!("Key not found"),
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

        println!(
            "Received update request - key_len: {}, value_len: {}",
            req.key.len(),
            req.value.len()
        );

        // 验证输入
        if req.key.is_empty() {
            return Err(Status::invalid_argument("Key cannot be empty"));
        }

        // 检查 key 是否存在
        if self.engine.lock().unwrap().get(&req.key).is_none() {
            let reply = UpdateResponse {
                success: false,
                message: format!("Key not found, cannot update"),
            };
            return Ok(Response::new(reply));
        }

        // 更新 key 的值
        self.engine.lock().unwrap().put(req.key, req.value);

        let reply = UpdateResponse {
            success: true,
            message: format!("Updated successfully"),
        };

        Ok(Response::new(reply))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();

        println!("Received delete request - key_len: {}", req.key.len());

        // 验证输入
        if req.key.is_empty() {
            return Err(Status::invalid_argument("Key cannot be empty"));
        }

        // 删除 key (即使不存在也会插入删除标记)
        self.engine.lock().unwrap().delete(req.key);

        let reply = DeleteResponse {
            success: true,
            message: format!("Deleted successfully"),
        };

        Ok(Response::new(reply))
    }
}

impl Default for GoatKVServiceImpl {
    fn default() -> Self {
        Self {
            engine: Arc::new(Mutex::new(KvEngine::new())),
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 解析命令行参数
    let args = Args::parse();

    let addr = args.address.parse()?;

    // 创建 KvEngine，使用指定的数据目录或默认值
    let engine = match args.data_dir {
        Some(dir) => {
            println!("Using data directory: {}", dir);
            KvEngine::new_with_data_dir(&dir).map_err(|e| {
                format!(
                    "Failed to create KvEngine with data directory '{}': {}",
                    dir, e
                )
            })?
        }
        None => {
            println!("Using default data directory (./goatdb_data)");
            KvEngine::new()
        }
    };

    let service = GoatKVServiceImpl::new(engine);

    println!("gRPC Server listening on {}", addr);
    println!("Starting server...");

    Server::builder()
        .add_service(GoatKvServiceServer::new(service))
        .serve(addr)
        .await?;

    println!("Server finished");

    Ok(())
}
