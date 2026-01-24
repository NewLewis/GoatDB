use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::goatkv::storage::wal::WalPaths;
use crate::goatkv::utils::path_helpers;

#[derive(Debug, Clone)]
pub struct SstablePaths {
    data_dir: PathBuf,
    tmp_dir: PathBuf,
}

impl SstablePaths {
    pub fn new(data_dir: PathBuf, tmp_dir: PathBuf) -> Self {
        Self { data_dir, tmp_dir }
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn tmp_dir(&self) -> &Path {
        &self.tmp_dir
    }

    pub fn sstable_path<S: AsRef<str>>(&self, name: S) -> PathBuf {
        self.data_dir.join(name.as_ref())
    }

    pub fn sstable_path_by_id(&self, file_id: u64) -> PathBuf {
        self.data_dir
            .join(path_helpers::format_sstable_filename(file_id))
    }

    pub fn timestamped_sstable_path(&self) -> PathBuf {
        self.data_dir
            .join(path_helpers::timestamped_sstable_filename())
    }

    pub fn tmp_path<S: AsRef<str>>(&self, name: S) -> PathBuf {
        self.tmp_dir.join(name.as_ref())
    }

    /// 清理临时目录（删除所有临时文件）
    pub fn cleanup_tmp_dir(&self) -> Result<(), std::io::Error> {
        if self.tmp_dir.exists() {
            for entry in fs::read_dir(&self.tmp_dir)? {
                let entry = entry?;
                let path = entry.path();

                if path.is_file() {
                    fs::remove_file(&path)?;
                } else if path.is_dir() {
                    fs::remove_dir_all(&path)?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ManifestPaths {
    base_dir: PathBuf,
    data_dir: PathBuf,
}

impl ManifestPaths {
    pub fn new(base_dir: PathBuf, data_dir: PathBuf) -> Self {
        Self { base_dir, data_dir }
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}

pub fn init_db_paths<P: AsRef<Path>>(
    base_dir: P,
) -> Result<(Arc<WalPaths>, Arc<SstablePaths>, Arc<ManifestPaths>), std::io::Error> {
    let base_dir = base_dir.as_ref().to_path_buf();
    let data_dir = base_dir.join("data");
    let wal_dir = base_dir.join("wal");
    let log_dir = base_dir.join("log");
    let tmp_dir = base_dir.join("tmp");

    let dirs = [&base_dir, &data_dir, &wal_dir, &log_dir, &tmp_dir];
    for dir in dirs {
        if !dir.exists() {
            fs::create_dir_all(dir)?;
        }
    }

    let wal_paths = Arc::new(WalPaths::new(wal_dir));
    let sstable_paths = Arc::new(SstablePaths::new(data_dir.clone(), tmp_dir));
    let manifest_paths = Arc::new(ManifestPaths::new(base_dir, data_dir));

    Ok((wal_paths, sstable_paths, manifest_paths))
}
