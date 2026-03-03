use std::fmt;
use std::io;

use tonic::Status;

use crate::goatkv::storage::wal::WalError;

/// GoatKV 顶层错误分类。
///
/// 设计目标：
/// - 稳定：对外暴露有限且稳定的错误类别；
/// - 可映射：可统一映射到 gRPC/HTTP 等传输层错误码；
/// - 可观测：分类可直接用于日志与指标聚合。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    InvalidArgument,
    NotFound,
    Corruption,
    Conflict,
    Unavailable,
    Io,
    Internal,
}

/// GoatKV 顶层错误类型。
///
/// 约束：
/// - 库层统一返回 `goatkv::error::Result<T>`；
/// - 尽量保留底层 source，避免丢失排障信息；
/// - 禁止用 panic 表达可恢复错误。
#[derive(Debug)]
pub enum Error {
    InvalidArgument {
        param: &'static str,
        message: String,
    },
    NotFound {
        entity: &'static str,
        message: String,
    },
    Corruption {
        scope: &'static str,
        message: String,
    },
    Conflict {
        scope: &'static str,
        message: String,
    },
    Unavailable {
        scope: &'static str,
        message: String,
    },
    Io {
        op: &'static str,
        source: io::Error,
    },
    Internal {
        scope: &'static str,
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn invalid_argument(param: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidArgument {
            param,
            message: message.into(),
        }
    }

    pub fn not_found(entity: &'static str, message: impl Into<String>) -> Self {
        Self::NotFound {
            entity,
            message: message.into(),
        }
    }

    pub fn corruption(scope: &'static str, message: impl Into<String>) -> Self {
        Self::Corruption {
            scope,
            message: message.into(),
        }
    }

    pub fn conflict(scope: &'static str, message: impl Into<String>) -> Self {
        Self::Conflict {
            scope,
            message: message.into(),
        }
    }

    pub fn unavailable(scope: &'static str, message: impl Into<String>) -> Self {
        Self::Unavailable {
            scope,
            message: message.into(),
        }
    }

    pub fn io(op: &'static str, source: io::Error) -> Self {
        Self::Io { op, source }
    }

    pub fn internal(scope: &'static str, message: impl Into<String>) -> Self {
        Self::Internal {
            scope,
            message: message.into(),
            source: None,
        }
    }

    pub fn internal_with_source<E>(
        scope: &'static str,
        message: impl Into<String>,
        source: E,
    ) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Internal {
            scope,
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::InvalidArgument { .. } => ErrorKind::InvalidArgument,
            Self::NotFound { .. } => ErrorKind::NotFound,
            Self::Corruption { .. } => ErrorKind::Corruption,
            Self::Conflict { .. } => ErrorKind::Conflict,
            Self::Unavailable { .. } => ErrorKind::Unavailable,
            Self::Io { .. } => ErrorKind::Io,
            Self::Internal { .. } => ErrorKind::Internal,
        }
    }

    /// 统一映射到 gRPC 状态码，供 server 层直接复用。
    pub fn to_status(&self) -> Status {
        match self {
            Self::InvalidArgument { .. } => Status::invalid_argument("invalid argument"),
            Self::NotFound { .. } => Status::not_found("not found"),
            Self::Corruption { .. } => Status::data_loss("data corruption"),
            Self::Conflict { .. } => Status::failed_precondition("conflict"),
            Self::Unavailable { .. } => Status::unavailable("service unavailable"),
            Self::Io { .. } => Status::internal("storage io error"),
            Self::Internal { .. } => Status::internal("internal server error"),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument { param, message } => {
                write!(f, "invalid argument `{}`: {}", param, message)
            }
            Self::NotFound { entity, message } => write!(f, "{} not found: {}", entity, message),
            Self::Corruption { scope, message } => {
                write!(f, "corruption in {}: {}", scope, message)
            }
            Self::Conflict { scope, message } => write!(f, "conflict in {}: {}", scope, message),
            Self::Unavailable { scope, message } => {
                write!(f, "unavailable {}: {}", scope, message)
            }
            Self::Io { op, source } => write!(f, "io error during {}: {}", op, source),
            Self::Internal {
                scope,
                message,
                source,
            } => {
                if let Some(source) = source {
                    write!(f, "internal error in {}: {}: {}", scope, message, source)
                } else {
                    write!(f, "internal error in {}: {}", scope, message)
                }
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Internal {
                source: Some(source),
                ..
            } => Some(source.as_ref()),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(source: io::Error) -> Self {
        Self::Io { op: "io", source }
    }
}

impl From<WalError> for Error {
    fn from(err: WalError) -> Self {
        match err {
            WalError::Io(source) => Self::io("wal", source),
            WalError::ChecksumMismatch { key } => Self::corruption(
                "wal_record",
                format!("checksum mismatch (key_len={})", key.len()),
            ),
            WalError::InvalidKeyLen => {
                Self::corruption("wal_record", "invalid internal key length")
            }
            WalError::UnexpectedEof => {
                Self::corruption("wal_record", "unexpected eof while reading wal record")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;
    use std::io;

    use tonic::Code;

    use super::{Error, ErrorKind};
    use crate::goatkv::storage::wal::WalError;

    #[test]
    fn maps_invalid_argument_to_status() {
        let err = Error::invalid_argument("key", "cannot be empty");
        let status = err.to_status();
        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(status.message(), "invalid argument");
        assert_eq!(err.kind(), ErrorKind::InvalidArgument);
    }

    #[test]
    fn maps_not_found_to_status() {
        let err = Error::not_found("key", "user:abc");
        let status = err.to_status();
        assert_eq!(status.code(), Code::NotFound);
        assert_eq!(status.message(), "not found");
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn maps_corruption_to_status() {
        let err = Error::corruption("wal", "checksum mismatch");
        let status = err.to_status();
        assert_eq!(status.code(), Code::DataLoss);
        assert_eq!(status.message(), "data corruption");
        assert_eq!(err.kind(), ErrorKind::Corruption);
    }

    #[test]
    fn maps_conflict_to_status() {
        let err = Error::conflict("manifest", "duplicate file");
        let status = err.to_status();
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert_eq!(status.message(), "conflict");
        assert_eq!(err.kind(), ErrorKind::Conflict);
    }

    #[test]
    fn maps_unavailable_to_status() {
        let err = Error::unavailable("wal_writer", "closed");
        let status = err.to_status();
        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(status.message(), "service unavailable");
        assert_eq!(err.kind(), ErrorKind::Unavailable);
    }

    #[test]
    fn maps_io_and_internal_to_internal_status() {
        let io_err = Error::io("open", io::Error::other("disk full"));
        let io_status = io_err.to_status();
        assert_eq!(io_status.code(), Code::Internal);
        assert_eq!(io_status.message(), "storage io error");
        assert_eq!(io_err.kind(), ErrorKind::Io);

        let internal_err = Error::internal("engine", "unexpected state");
        let internal_status = internal_err.to_status();
        assert_eq!(internal_status.code(), Code::Internal);
        assert_eq!(internal_status.message(), "internal server error");
        assert_eq!(internal_err.kind(), ErrorKind::Internal);
    }

    #[test]
    fn preserves_internal_source_chain() {
        let err =
            Error::internal_with_source("server_init", "boot failed", io::Error::other("boom"));
        assert_eq!(err.kind(), ErrorKind::Internal);
        let source = StdError::source(&err).expect("internal source should exist");
        assert!(source.to_string().contains("boom"));
    }

    #[test]
    fn maps_wal_errors_to_top_level_categories() {
        let io_err = Error::from(WalError::Io(io::Error::other("wal io")));
        assert_eq!(io_err.kind(), ErrorKind::Io);

        let checksum_err = Error::from(WalError::ChecksumMismatch {
            key: b"user:42".to_vec(),
        });
        assert_eq!(checksum_err.kind(), ErrorKind::Corruption);

        let key_len_err = Error::from(WalError::InvalidKeyLen);
        assert_eq!(key_len_err.kind(), ErrorKind::Corruption);

        let eof_err = Error::from(WalError::UnexpectedEof);
        assert_eq!(eof_err.kind(), ErrorKind::Corruption);
    }
}
