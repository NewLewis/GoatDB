use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

const MAX_REQUEST_BYTES: usize = 1024;

#[derive(Debug)]
pub struct HealthState {
    live: AtomicBool,
    ready: AtomicBool,
}

impl HealthState {
    pub fn new() -> Self {
        Self {
            live: AtomicBool::new(true),
            ready: AtomicBool::new(false),
        }
    }

    pub fn set_live(&self, value: bool) {
        self.live.store(value, Ordering::Release);
    }

    pub fn set_ready(&self, value: bool) {
        self.ready.store(value, Ordering::Release);
    }

    pub fn is_live(&self) -> bool {
        self.live.load(Ordering::Acquire)
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
}

impl Default for HealthState {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn run_http_health_server(
    listener: TcpListener,
    state: Arc<HealthState>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> io::Result<()> {
    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
            accept_result = listener.accept() => {
                let (stream, _) = accept_result?;
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    let _ = handle_connection(stream, state).await;
                });
            }
        }
    }
    Ok(())
}

async fn handle_connection(mut stream: TcpStream, state: Arc<HealthState>) -> io::Result<()> {
    let mut buffer = [0u8; MAX_REQUEST_BYTES];
    let bytes_read = stream.read(&mut buffer).await?;
    if bytes_read == 0 {
        return Ok(());
    }

    let request = String::from_utf8_lossy(&buffer[..bytes_read]);
    let path = request_line_path(&request);
    let response = match path {
        "/livez" | "/healthz" => {
            if state.is_live() {
                http_response(200, "OK", "alive")
            } else {
                http_response(503, "Service Unavailable", "dead")
            }
        }
        "/readyz" => {
            if state.is_ready() {
                http_response(200, "OK", "ready")
            } else {
                http_response(503, "Service Unavailable", "not ready")
            }
        }
        _ => http_response(404, "Not Found", "not found"),
    };

    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

fn request_line_path(request: &str) -> &str {
    request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/")
}

fn http_response(status: u16, reason: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {} {}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        reason,
        body.len(),
        body
    )
}

#[cfg(test)]
mod tests {
    use super::request_line_path;

    #[test]
    fn parse_request_path_from_first_line() {
        let request = "GET /readyz HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n";
        assert_eq!(request_line_path(request), "/readyz");

        let malformed = "\r\nHost: 127.0.0.1\r\n\r\n";
        assert_eq!(request_line_path(malformed), "/");
    }
}
