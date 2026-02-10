use std::io::Read;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use goat_db::goatkv::{Error as GoatError, Result as GoatResult};
use tempfile::TempDir;
use tokio::time::sleep;
use tonic::transport::Channel;

pub use goatkv::goat_kv_service_client::GoatKvServiceClient;

// 包含生成的gRPC代码
pub mod goatkv {
    tonic::include_proto!("goatkv");
}

/// 测试服务器选项
#[derive(Debug)]
pub struct TestServerOptions {
    pub port: Option<u16>,
    pub data_dir: Option<PathBuf>,
    pub show_logs: bool,
    pub capture_stderr: bool,
}

impl Default for TestServerOptions {
    fn default() -> Self {
        Self {
            port: None,
            data_dir: None,
            show_logs: true, // 临时启用日志以便调试
            capture_stderr: true,
        }
    }
}

/// 测试服务器管理器
pub struct TestServer {
    pub process: Child,
    pub address: String,
    #[allow(dead_code)]
    pub data_dir: PathBuf,
    #[allow(dead_code)]
    temp_dir: Option<TempDir>,
    stderr_output: Option<Vec<u8>>,
}

#[allow(dead_code)]
impl TestServer {
    /// 使用默认选项启动测试服务器
    pub async fn start() -> Self {
        Self::start_with_options(TestServerOptions::default()).await
    }

    /// 使用指定数据目录启动测试服务器
    pub async fn start_with_dir<P: AsRef<std::path::Path>>(data_dir: P) -> Self {
        let data_dir_path = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir_path).unwrap();
        let options = TestServerOptions {
            port: None,
            data_dir: Some(data_dir_path),
            show_logs: false,
            capture_stderr: true,
        };

        Self::start_with_options(options).await
    }

    /// 使用自定义选项启动测试服务器
    pub async fn start_with_options(opts: TestServerOptions) -> Self {
        let port = opts.port.unwrap_or_else(find_free_port);
        let address = format!("127.0.0.1:{}", port);

        let (data_dir_path, temp_dir) = match opts.data_dir {
            Some(path) => {
                std::fs::create_dir_all(&path).unwrap();
                (path, None)
            }
            None => {
                let temp_dir = tempfile::tempdir().unwrap();
                let path = temp_dir.path().to_path_buf();
                (path, Some(temp_dir))
            }
        };

        // 构建命令行参数
        let args = vec![
            "run".to_string(),
            "--bin".to_string(),
            "goatkv_server".to_string(),
            "--".to_string(),
            "--address".to_string(),
            address.clone(),
            "--data-dir".to_string(),
            data_dir_path.to_str().unwrap().to_string(),
        ];

        // 决定stderr的处理方式
        let stderr_handle = if opts.capture_stderr {
            Stdio::piped()
        } else if opts.show_logs {
            Stdio::inherit()
        } else {
            Stdio::null()
        };

        // 启动服务器进程
        let mut command = Command::new("cargo");
        command.args(&args);

        if opts.show_logs {
            command.stdout(Stdio::inherit());
        } else {
            command.stdout(Stdio::null());
        }

        command.stderr(stderr_handle);

        let mut process = command.spawn().expect("Failed to start test server");

        // 如果是管道模式，获取stderr管道
        let mut stderr_pipe = if opts.capture_stderr {
            process.stderr.take()
        } else {
            None
        };

        // 等待服务器就绪
        let server_url = format!("http://{}", address);
        let mut stderr_output = Vec::new();

        if let Err(e) = Self::wait_for_server_with_process(
            &server_url,
            Duration::from_secs(30), // 增加超时到30秒
            &mut process,
            stderr_pipe.as_mut(),
            &mut stderr_output,
        )
        .await
        {
            // 如果进程已退出，获取退出状态
            let status = process.try_wait().ok().flatten();
            let exit_code = status.and_then(|s| s.code());

            // 尝试读取剩余的stderr输出
            #[allow(unused_mut)]
            if let Some(mut pipe) = stderr_pipe {
                let _ = pipe.read_to_end(&mut stderr_output);
            }

            let stderr_str = String::from_utf8_lossy(&stderr_output);

            panic!(
                "Server failed to start within timeout: {}\n\
                 Process exit code: {:?}\n\
                 Stderr output:\n{}\n\
                 Server address: {}\n\
                 Data directory: {}",
                e,
                exit_code,
                stderr_str,
                address,
                data_dir_path.display()
            );
        }

        Self {
            process,
            address: server_url,
            data_dir: data_dir_path,
            temp_dir,
            stderr_output: Some(stderr_output),
        }
    }

    /// 带进程监控的等待函数
    async fn wait_for_server_with_process(
        url: &str,
        timeout: Duration,
        process: &mut Child,
        mut stderr_pipe: Option<&mut std::process::ChildStderr>,
        stderr_output: &mut Vec<u8>,
    ) -> GoatResult<()> {
        let start = std::time::Instant::now();

        loop {
            // 首先检查进程是否还活着
            match process.try_wait() {
                Ok(Some(status)) => {
                    // 进程已退出
                    let exit_code = status.code();
                    let mut error_msg = format!("Server process exited with code: {:?}", exit_code);

                    // 尝试读取剩余的stderr输出
                    #[allow(unused_mut)]
                    if let Some(mut pipe) = stderr_pipe.take() {
                        let _ = pipe.read_to_end(stderr_output);
                        if !stderr_output.is_empty() {
                            let stderr_str = String::from_utf8_lossy(stderr_output);
                            error_msg.push_str(&format!("\nStderr output:\n{}", stderr_str));
                        }
                    }

                    return Err(GoatError::unavailable("test_server_process", error_msg));
                }
                Ok(None) => {
                    // 进程还在运行，尝试连接
                    match GoatKvServiceClient::<Channel>::connect(url.to_string()).await {
                        Ok(_) => return Ok(()),
                        Err(e) => {
                            if start.elapsed() < timeout {
                                sleep(Duration::from_millis(100)).await;
                            } else {
                                return Err(GoatError::unavailable(
                                    "test_server_connect_timeout",
                                    e.to_string(),
                                ));
                            }
                        }
                    }
                }
                Err(e) => {
                    return Err(GoatError::internal(
                        "test_server_status_check",
                        format!("Failed to check process status: {}", e),
                    ));
                }
            }
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

    /// 获取服务器进程的标准错误输出
    pub fn stderr_output(&self) -> Option<&[u8]> {
        self.stderr_output.as_deref()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
    }
}

/// 查找空闲端口
fn find_free_port() -> u16 {
    // 让 OS 分配临时端口，避免随机碰撞
    TcpListener::bind(("127.0.0.1", 0))
        .and_then(|listener| listener.local_addr().map(|addr| addr.port()))
        .expect("Failed to allocate an ephemeral port")
}
