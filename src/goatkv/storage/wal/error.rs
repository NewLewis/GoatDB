use std::fmt;
use std::io;

#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    ChecksumMismatch { key: Vec<u8> },
    InvalidKeyLen,
    UnexpectedEof,
}

pub type WalResult<T> = Result<T, WalError>;

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalError::Io(err) => write!(f, "{}", err),
            WalError::ChecksumMismatch { key } => write!(
                f,
                "Checksum mismatch for key: {}",
                String::from_utf8_lossy(key)
            ),
            WalError::InvalidKeyLen => write!(f, "Invalid InternalKey length"),
            WalError::UnexpectedEof => write!(f, "Unexpected EOF while reading WAL record"),
        }
    }
}

impl std::error::Error for WalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WalError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for WalError {
    fn from(err: io::Error) -> Self {
        WalError::Io(err)
    }
}
