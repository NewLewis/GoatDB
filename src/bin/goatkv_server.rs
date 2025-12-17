use std::sync::{Arc, Mutex};

use goat_db::goatkv::kv_engine::KvEngine;
use goatkv::{
    goat_kv_service_server::{GoatKvService, GoatKvServiceServer},
    GetRequest, GetResponse, WriteRequest, WriteResponse,
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
}

impl Default for GoatKVServiceImpl {
    fn default() -> Self {
        Self {
            engine: Arc::new(Mutex::new(KvEngine::new())),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse()?;
    let service = GoatKVServiceImpl::default();

    println!("gRPC Server listening on {}", addr);
    println!("Starting server...");

    Server::builder()
        .add_service(GoatKvServiceServer::new(service))
        .serve(addr)
        .await?;

    println!("Server finished");

    Ok(())
}
