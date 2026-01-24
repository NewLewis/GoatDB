use std::path::{Path, PathBuf};

use crate::goatkv::utils::path_helpers;

#[derive(Debug, Clone)]
pub struct WalPaths {
    wal_dir: PathBuf,
}

impl WalPaths {
    pub fn new(wal_dir: PathBuf) -> Self {
        Self { wal_dir }
    }

    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    pub fn main_wal_path(&self) -> PathBuf {
        self.wal_dir.join("goatdb.wal")
    }

    pub fn wal_path_by_id(&self, log_number: u64) -> PathBuf {
        self.wal_dir
            .join(path_helpers::format_wal_filename(log_number))
    }

    pub fn wal_path<S: AsRef<str>>(&self, name: S) -> PathBuf {
        self.wal_dir.join(name.as_ref())
    }
}
