use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

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
        self.wal_dir.join(Self::format_wal_filename(log_number))
    }

    pub fn wal_path<S: AsRef<str>>(&self, name: S) -> PathBuf {
        self.wal_dir.join(name.as_ref())
    }

    /// WAL 文件名规则
    /// - 如果 log_number < 1,000,000：格式为 `{log_number:06}.wal`（如 000001.wal）
    /// - 如果 log_number >= 1,000,000：格式为 `{log_number}.wal`（如 1234567.wal）
    pub fn format_wal_filename(log_number: u64) -> String {
        if log_number < 1_000_000 {
            format!("{:06}.wal", log_number)
        } else {
            format!("{}.wal", log_number)
        }
    }
}

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
        self.data_dir.join(Self::format_sstable_filename(file_id))
    }

    pub fn timestamped_sstable_path(&self) -> PathBuf {
        self.data_dir.join(Self::timestamped_sstable_filename())
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

    /// SSTable 文件名规则
    /// - 如果 file_id < 1,000,000：格式为 `{file_id:06}.sst`（如 000001.sst）
    /// - 如果 file_id >= 1,000,000：格式为 `{file_id}.sst`（如 1234567.sst）
    pub fn format_sstable_filename(file_id: u64) -> String {
        if file_id < 1_000_000 {
            format!("{:06}.sst", file_id)
        } else {
            format!("{}.sst", file_id)
        }
    }

    /// 基于当前时间戳生成 SSTable 文件名（用于 flush 操作）
    pub fn timestamped_sstable_filename() -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        format!("sstable_{}.db", timestamp)
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
