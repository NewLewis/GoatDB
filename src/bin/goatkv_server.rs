use tonic::{transport::Server, Request, Response, Status};
use wal::wal_service_server::{WalService, WalServiceServer};
use wal::{WriteRequest, WriteResponse};

// 引入编译生成的代码
pub mod wal {
    tonic::include_proto!("wal");
}

#[derive(Debug, Default)]
pub struct MyWalService;

// 实现 Trait
#[tonic::async_trait]
impl WalService for MyWalService {
    async fn write(
        &self,
        request: Request<WriteRequest>, // 拿到请求
    ) -> Result<Response<WriteResponse>, Status> {
        let req = request.into_inner();
        
        println!("收到写入请求: key_len={}, val_len={}", req.key.len(), req.value.len());

        // 这里调用你写的 WalManager.write()
        // 注意：这里需要处理多线程共享 WalManager 的问题 (Arc<Mutex<WalManager>>)
        
        let reply = WriteResponse {
            success: true,
            message: "Written successfully".into(),
        };

        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let service = MyWalService::default();

    println!("gRPC Server listening on {}", addr);

    Server::builder()
        .add_service(WalServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}