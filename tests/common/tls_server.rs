#![allow(dead_code)]

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;
use tokio::time::sleep;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::common::test_server::find_free_port;

pub mod goatkv {
    tonic::include_proto!("goatkv");
}

use goatkv::goat_kv_service_client::GoatKvServiceClient;
use goatkv::GetRequest;

pub fn tls_fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("tls")
}

pub fn read_fixture(name: &str) -> Vec<u8> {
    fs::read(tls_fixture_dir().join(name)).expect("read tls fixture")
}

pub async fn tls_client(
    address: &str,
    trust_server_ca: bool,
    provide_client_identity: bool,
) -> Result<GoatKvServiceClient<Channel>, tonic::transport::Error> {
    let mut tls = ClientTlsConfig::new().domain_name("localhost");
    if trust_server_ca {
        tls = tls.ca_certificate(Certificate::from_pem(read_fixture("ca-cert.pem")));
    }
    if provide_client_identity {
        tls = tls.identity(Identity::from_pem(
            read_fixture("client-cert.pem"),
            read_fixture("client-key.pem"),
        ));
    }

    let endpoint = Endpoint::from_shared(format!("https://{address}"))
        .expect("build tls endpoint")
        .timeout(Duration::from_secs(3))
        .tls_config(tls)
        .expect("configure client tls");
    let channel = endpoint.connect().await?;
    Ok(GoatKvServiceClient::new(channel))
}

pub struct TlsTestServer {
    process: Child,
    address: String,
    _data_dir: TempDir,
    #[allow(dead_code)]
    stderr_output: Option<Vec<u8>>,
}

impl TlsTestServer {
    pub async fn start(require_client_cert: bool) -> Self {
        let port = find_free_port();
        let address = format!("127.0.0.1:{port}");
        let data_dir = tempfile::tempdir().expect("create temp dir");

        let fixture_dir = tls_fixture_dir();
        let mut args = vec![
            "run".to_string(),
            "--bin".to_string(),
            "goatkv_server".to_string(),
            "--".to_string(),
            "--address".to_string(),
            address.clone(),
            "--data-dir".to_string(),
            data_dir.path().display().to_string(),
            "--tls-cert-path".to_string(),
            fixture_dir.join("server-cert.pem").display().to_string(),
            "--tls-key-path".to_string(),
            fixture_dir.join("server-key.pem").display().to_string(),
        ];
        if require_client_cert {
            args.push("--tls-client-ca-path".to_string());
            args.push(fixture_dir.join("ca-cert.pem").display().to_string());
        }

        let mut command = Command::new("cargo");
        command
            .args(&args)
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let mut process = command.spawn().expect("spawn tls test server");
        let mut stderr_pipe = process.stderr.take();
        let mut stderr_output = Vec::new();

        if let Err(e) = Self::wait_for_server(
            &address,
            require_client_cert,
            &mut process,
            stderr_pipe.as_mut(),
            &mut stderr_output,
        )
        .await
        {
            let status = process.try_wait().ok().flatten();
            let exit_code = status.and_then(|s| s.code());
            if let Some(mut pipe) = stderr_pipe {
                let _ = pipe.read_to_end(&mut stderr_output);
            }
            let stderr_str = String::from_utf8_lossy(&stderr_output);
            panic!(
                "TLS server failed to start: {}\nexit_code: {:?}\nstderr:\n{}\naddress: {}",
                e, exit_code, stderr_str, address
            );
        }

        Self {
            process,
            address,
            _data_dir: data_dir,
            stderr_output: Some(stderr_output),
        }
    }

    async fn wait_for_server(
        address: &str,
        require_client_cert: bool,
        process: &mut Child,
        mut stderr_pipe: Option<&mut std::process::ChildStderr>,
        stderr_output: &mut Vec<u8>,
    ) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match process.try_wait() {
                Ok(Some(status)) => {
                    if let Some(pipe) = stderr_pipe.take() {
                        let _ = pipe.read_to_end(stderr_output);
                    }
                    let stderr_str = String::from_utf8_lossy(stderr_output);
                    return Err(format!(
                        "server exited early with status {:?}\nstderr:\n{}",
                        status.code(),
                        stderr_str
                    ));
                }
                Ok(None) => match tls_client(address, true, require_client_cert).await {
                    Ok(mut client) => {
                        let _ = client
                            .get(GetRequest {
                                key: b"tls_ready_probe".to_vec(),
                                snapshot_id: 0,
                            })
                            .await
                            .map_err(|e| e.to_string())?;
                        return Ok(());
                    }
                    Err(err) => {
                        if Instant::now() >= deadline {
                            return Err(format!("timeout waiting for tls server: {err}"));
                        }
                        sleep(Duration::from_millis(100)).await;
                    }
                },
                Err(err) => return Err(format!("failed to inspect child status: {err}")),
            }
        }
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub async fn trusted_client(&self) -> GoatKvServiceClient<Channel> {
        tls_client(&self.address, true, false)
            .await
            .expect("connect trusted tls client")
    }

    pub async fn mtls_client(&self) -> GoatKvServiceClient<Channel> {
        tls_client(&self.address, true, true)
            .await
            .expect("connect mtls client")
    }

    #[allow(dead_code)]
    pub fn stderr_output(&self) -> Option<&[u8]> {
        self.stderr_output.as_deref()
    }
}

impl Drop for TlsTestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
    }
}
