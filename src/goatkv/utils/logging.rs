use std::path::{Path, PathBuf};

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub struct LogGuards {
    _file_guard: WorkerGuard,
}

pub fn init_logging(app_name: &str, base_dir: impl AsRef<Path>, default_filter: &str) -> LogGuards {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter));

    let log_dir = std::env::var("GOATDB_LOG_DIR")
        .ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| base_dir.as_ref().join("log"));
    let _ = std::fs::create_dir_all(&log_dir);

    let file_appender = tracing_appender::rolling::daily(&log_dir, format!("{}.log", app_name));
    let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);

    let stdout_layer = tracing_subscriber::fmt::layer().with_target(false);
    let file_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(file_writer);

    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(stdout_layer)
        .with(file_layer);

    let _ = subscriber.try_init();

    LogGuards {
        _file_guard: file_guard,
    }
}
