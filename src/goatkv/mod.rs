// 声明子模块
pub mod core;
pub mod error;
pub mod format;
pub mod metadata;
pub mod metrics;
pub mod server;
pub mod storage;
pub mod utils;

// 重新导出公共接口
pub use core::{BatchWriteOp, EngineTransaction, KvEngine, ScanOptions};
pub use error::{Error, ErrorKind, Result};
pub use utils::KvEngineOptions;
