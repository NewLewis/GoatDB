use std::net::TcpListener;
use std::process::{Child, Command, Stdio};

use std::time::Duration;

use rand::Rng;
use tempfile::TempDir;
use tokio::time::sleep;
use tonic::transport::Channel;

pub use goatkv::goat_kv_service_client::GoatKvServiceClient;

// 包含生成的gRPC代码
pub mod goatkv {
    tonic::include_proto!("goatkv");
}

/// 测试服务器选项
#[derive(Default)]
pub struct TestServerOptions {
    pub port: Option<u16>,
    pub data_dir: Option<TempDir>,
    pub show_logs: bool,
}

/// 测试服务器管理器
pub struct TestServer {
    pub process: Child,
    pub address: String,
    #[allow(dead_code)]
    pub data_dir: TempDir,
}

impl TestServer {
    /// 使用默认选项启动测试服务器
    pub async fn start() -> Self {
        Self::start_with_options(TestServerOptions::default()).await
    }

    /// 使用指定数据目录启动测试服务器
    pub async fn start_with_dir<P: AsRef<std::path::Path>>(data_dir: P) -> Self {
        let temp_dir = TempDir::new().unwrap();
        let data_dir_path = data_dir.as_ref().to_str().unwrap().to_string();

        // 创建临时目录的副本，但使用指定的数据目录
        let data_dir = temp_dir;
        std::fs::create_dir_all(&data_dir_path).unwrap();

        let options = TestServerOptions {
            port: None,
            data_dir: Some(data_dir),
            show_logs: false,
        };

        Self::start_with_options(options).await
    }

    /// 使用自定义选项启动测试服务器
    pub async fn start_with_options(opts: TestServerOptions) -> Self {
        let port = opts.port.unwrap_or_else(find_free_port);
        let address = format!("127.0.0.1:{}", port);

        let data_dir = opts
            .data_dir
            .unwrap_or_else(|| tempfile::tempdir().unwrap());

        // 构建命令行参数
        let args = vec![
            "run".to_string(),
            "--bin".to_string(),
            "goatkv_server".to_string(),
            "--".to_string(),
            "--address".to_string(),
            address.clone(),
            "--data-dir".to_string(),
            data_dir.path().to_str().unwrap().to_string(),
        ];

        // 启动服务器进程
        let process = Command::new("cargo")
            .args(&args)
            .stdout(if opts.show_logs {
                Stdio::inherit()
            } else {
                Stdio::null()
            })
            .stderr(if opts.show_logs {
                Stdio::inherit()
            } else {
                Stdio::null()
            })
            .spawn()
            .expect("Failed to start test server");

        // 等待服务器就绪
        let server_url = format!("http://{}", address);
        wait_for_server(&server_url, Duration::from_secs(10)).await;

        Self {
            process,
            address: server_url,
            data_dir,
        }
    }

    /// 创建连接到测试服务器的客户端
    pub async fn client(&self) -> GoatKvServiceClient<Channel> {
        GoatKvServiceClient::connect(self.address.clone())
            .await
            .expect("Failed to connect to test server")
    }

    /// 强制杀死服务器进程
    pub fn kill(&mut self) {
        let _ = self.process.kill();
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
    }
}

/// 等待服务器启动并可用
async fn wait_for_server(url: &str, timeout: Duration) {
    let start = std::time::Instant::now();

    loop {
        match GoatKvServiceClient::<Channel>::connect(url.to_string()).await {
            Ok(_) => return,
            Err(_) if start.elapsed() < timeout => {
                sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("Server failed to start within timeout: {}", e),
        }
    }
}

/// 查找空闲端口
fn find_free_port() -> u16 {
    let mut rng = rand::thread_rng();

    for _ in 0..10 {
        let port = 50000 + rng.gen_range(0..10000);

        // 尝试绑定到端口来检查是否可用
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }

    panic!("Could not find free port after 10 attempts");
}
