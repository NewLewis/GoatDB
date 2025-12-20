use std::fs;
use std::path::{Path, PathBuf};

/// 数据库路径管理器，类似PostgreSQL的pgdata目录
/// 统一管理数据库的所有物理文件路径
///
/// 目录结构：
/// - base_path/          # 基础目录（用户指定的数据目录）
///   ├── data/          # 主数据文件（SSTable等）
///   ├── wal/           # WAL（Write-Ahead Log）日志文件
///   ├── log/           # 系统日志文件
///   └── tmp/           # 临时文件
#[derive(Debug, Clone)]
pub struct DbPathManager {
    /// 基础数据目录（类似PostgreSQL的pgdata）
    base_path: PathBuf,
    /// 主数据文件目录
    data_dir: PathBuf,
    /// WAL日志目录
    wal_dir: PathBuf,
    /// 系统日志目录
    log_dir: PathBuf,
    /// 临时文件目录
    tmp_dir: PathBuf,
}

impl DbPathManager {
    /// 创建新的路径管理器，指定基础目录
    ///
    /// # 参数
    /// - `base_path`: 基础数据目录路径
    ///
    /// # 返回
    /// - `Ok(DbPathManager)`: 创建成功
    /// - `Err(std::io::Error)`: 创建目录失败
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<Self, std::io::Error> {
        let base_path = base_path.as_ref().to_path_buf();

        // 构建子目录路径
        let data_dir = base_path.join("data");
        let wal_dir = base_path.join("wal");
        let log_dir = base_path.join("log");
        let tmp_dir = base_path.join("tmp");

        let manager = Self {
            base_path,
            data_dir,
            wal_dir,
            log_dir,
            tmp_dir,
        };

        // 创建所有目录
        manager.create_directories()?;

        Ok(manager)
    }

    /// 创建用于测试的路径管理器，使用系统临时目录
    ///
    /// # 返回
    /// - `Ok(DbPathManager)`: 创建成功
    /// - `Err(std::io::Error)`: 创建目录失败
    ///
    /// # 注意
    /// 此方法主要用于测试，但也可用于需要临时数据库目录的场景
    #[cfg(test)]
    pub fn for_test() -> Result<Self, std::io::Error> {
        use std::env;

        let mut temp_dir = env::temp_dir();
        temp_dir.push("goatdb_test");
        temp_dir.push(format!(
            "test_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        Self::new(temp_dir)
    }

    /// 创建所有必要的目录结构
    fn create_directories(&self) -> Result<(), std::io::Error> {
        // 创建基础目录
        if !self.base_path.exists() {
            fs::create_dir_all(&self.base_path)?;
        }

        // 创建子目录
        let dirs = [&self.data_dir, &self.wal_dir, &self.log_dir, &self.tmp_dir];

        for dir in dirs.iter() {
            if !dir.exists() {
                fs::create_dir_all(dir)?;
            }
        }

        Ok(())
    }

    /// 获取基础数据目录路径
    pub fn base_path(&self) -> &Path {
        &self.base_path
    }

    /// 获取基础数据目录的绝对路径
    pub fn base_path_absolute(&self) -> std::io::Result<PathBuf> {
        self.base_path.canonicalize()
    }

    /// 获取主数据文件目录路径
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// 获取WAL日志目录路径
    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    /// 获取系统日志目录路径
    pub fn log_dir(&self) -> &Path {
        &self.log_dir
    }

    /// 获取临时文件目录路径
    pub fn tmp_dir(&self) -> &Path {
        &self.tmp_dir
    }

    /// 获取默认WAL日志文件路径（主WAL文件）
    pub fn main_wal_path(&self) -> PathBuf {
        self.wal_dir.join("goatdb.wal")
    }

    /// 获取指定名称的WAL日志文件路径
    pub fn wal_path<S: AsRef<str>>(&self, name: S) -> PathBuf {
        self.wal_dir.join(name.as_ref())
    }

    /// 获取指定名称的SSTable文件路径
    pub fn sstable_path<S: AsRef<str>>(&self, name: S) -> PathBuf {
        self.data_dir.join(name.as_ref())
    }

    /// 获取当前时间戳命名的SSTable文件路径（用于flush操作）
    pub fn timestamped_sstable_path(&self) -> PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();

        let filename = format!("sstable_{}.db", timestamp);
        self.data_dir.join(filename)
    }

    /// 获取指定名称的日志文件路径
    pub fn log_path<S: AsRef<str>>(&self, name: S) -> PathBuf {
        self.log_dir.join(name.as_ref())
    }

    /// 获取主日志文件路径
    pub fn main_log_path(&self) -> PathBuf {
        self.log_dir.join("goatdb.log")
    }

    /// 获取错误日志文件路径
    pub fn error_log_path(&self) -> PathBuf {
        self.log_dir.join("error.log")
    }

    /// 获取审计日志文件路径
    pub fn audit_log_path(&self) -> PathBuf {
        self.log_dir.join("audit.log")
    }

    /// 获取指定名称的临时文件路径
    pub fn tmp_path<S: AsRef<str>>(&self, name: S) -> PathBuf {
        self.tmp_dir.join(name.as_ref())
    }

    /// 清理临时目录（删除所有临时文件）
    pub fn cleanup_tmp_dir(&self) -> Result<(), std::io::Error> {
        if self.tmp_dir.exists() {
            // 删除临时目录中的所有内容，但保留目录本身
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

    /// 获取配置文件路径
    pub fn config_path<S: AsRef<str>>(&self, name: S) -> PathBuf {
        self.base_path.join(name.as_ref())
    }

    /// 获取备份目录路径
    pub fn backup_dir(&self) -> PathBuf {
        self.base_path.join("backup")
    }

    /// 获取指定备份的名称路径
    pub fn backup_path<S: AsRef<str>>(&self, name: S) -> PathBuf {
        self.backup_dir().join(name.as_ref())
    }

    /// 创建备份目录
    pub fn create_backup_dir(&self) -> Result<(), std::io::Error> {
        let backup_dir = self.backup_dir();
        if !backup_dir.exists() {
            fs::create_dir_all(&backup_dir)?;
        }
        Ok(())
    }

    /// 检查磁盘空间（返回可用字节数）
    /// 注意：这是一个简化实现，实际生产环境需要平台特定实现
    pub fn available_disk_space(&self) -> Option<u64> {
        // 简化实现：返回 None，表示无法获取磁盘空间信息
        // 在实际生产环境中，需要根据平台实现：
        // - Unix: 使用 statvfs 系统调用
        // - Windows: 使用 GetDiskFreeSpaceEx API
        None
    }

    /// 检查目录大小（递归计算，单位：字节）
    pub fn directory_size(&self) -> std::io::Result<u64> {
        fn dir_size(path: &Path) -> std::io::Result<u64> {
            let mut total = 0;
            if path.is_dir() {
                for entry in fs::read_dir(path)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_file() {
                        total += fs::metadata(&path)?.len();
                    } else if path.is_dir() {
                        total += dir_size(&path)?;
                    }
                }
            }
            Ok(total)
        }

        dir_size(&self.base_path)
    }

    /// 获取目录大小的人类可读格式
    pub fn directory_size_human(&self) -> String {
        match self.directory_size() {
            Ok(size) => Self::format_bytes(size),
            Err(e) => format!("Error: {}", e),
        }
    }

    /// 格式化字节数为人类可读格式
    fn format_bytes(bytes: u64) -> String {
        const UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];
        let mut size = bytes as f64;
        let mut unit_index = 0;

        while size >= 1024.0 && unit_index < UNITS.len() - 1 {
            size /= 1024.0;
            unit_index += 1;
        }

        format!("{:.2} {}", size, UNITS[unit_index])
    }

    /// 检查是否有足够的磁盘空间（最小要求字节数）
    pub fn has_enough_space(&self, required_bytes: u64) -> bool {
        match self.available_disk_space() {
            Some(available) => available >= required_bytes,
            None => true, // 如果无法获取空间信息，假设足够
        }
    }

    /// 获取当前工作目录下的默认数据目录路径
    pub fn default_data_dir() -> PathBuf {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join("goatdb_data")
    }

    /// 检查所有目录是否存在且可访问
    pub fn validate_paths(&self) -> Result<(), std::io::Error> {
        // 检查目录是否存在且可读取
        let dirs = [
            (&self.base_path, "base_path"),
            (&self.data_dir, "data_dir"),
            (&self.wal_dir, "wal_dir"),
            (&self.log_dir, "log_dir"),
            (&self.tmp_dir, "tmp_dir"),
        ];

        for (dir, name) in dirs.iter() {
            if !dir.exists() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Directory '{}' does not exist: {:?}", name, dir),
                ));
            }

            // 检查是否可读取
            fs::read_dir(dir)?;
        }

        Ok(())
    }

    /// 获取目录结构的字符串表示（用于日志/调试）
    pub fn path_summary(&self) -> String {
        format!(
            "Database Paths:\n\
             Base: {:?}\n\
             Data: {:?}\n\
             WAL:  {:?}\n\
             Log:  {:?}\n\
             Temp: {:?}",
            self.base_path, self.data_dir, self.wal_dir, self.log_dir, self.tmp_dir,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_create_path_manager() {
        let temp_dir = tempdir().unwrap();
        let manager = DbPathManager::new(temp_dir.path()).unwrap();

        // 检查所有目录是否创建
        assert!(manager.base_path().exists());
        assert!(manager.data_dir().exists());
        assert!(manager.wal_dir().exists());
        assert!(manager.log_dir().exists());
        assert!(manager.tmp_dir().exists());

        // 检查路径构建
        let wal_path = manager.main_wal_path();
        assert!(wal_path.parent().unwrap() == manager.wal_dir());
        assert!(wal_path.file_name().unwrap() == "goatdb.wal");
    }

    #[test]
    fn test_path_methods() {
        let temp_dir = tempdir().unwrap();
        let manager = DbPathManager::new(temp_dir.path()).unwrap();

        // 测试自定义路径方法
        let custom_wal = manager.wal_path("custom.wal");
        assert!(custom_wal.parent().unwrap() == manager.wal_dir());
        assert!(custom_wal.file_name().unwrap() == "custom.wal");

        let sstable = manager.sstable_path("level0.sst");
        assert!(sstable.parent().unwrap() == manager.data_dir());
        assert!(sstable.file_name().unwrap() == "level0.sst");

        // 时间戳SSTable路径应该包含时间戳
        let timestamped = manager.timestamped_sstable_path();
        assert!(timestamped.parent().unwrap() == manager.data_dir());
        let filename = timestamped.file_name().unwrap().to_str().unwrap();
        assert!(filename.starts_with("sstable_"));
        assert!(filename.ends_with(".db"));
    }

    #[test]
    fn test_cleanup_tmp_dir() {
        let temp_dir = tempdir().unwrap();
        let manager = DbPathManager::new(temp_dir.path()).unwrap();

        // 创建一些临时文件
        let tmp_file1 = manager.tmp_path("file1.tmp");
        let tmp_file2 = manager.tmp_path("file2.tmp");
        let tmp_subdir = manager.tmp_path("subdir");

        fs::write(&tmp_file1, "test data 1").unwrap();
        fs::write(&tmp_file2, "test data 2").unwrap();
        fs::create_dir(&tmp_subdir).unwrap();
        fs::write(tmp_subdir.join("nested.tmp"), "nested data").unwrap();

        // 验证文件创建成功
        assert!(tmp_file1.exists());
        assert!(tmp_file2.exists());
        assert!(tmp_subdir.exists());

        // 清理临时目录
        manager.cleanup_tmp_dir().unwrap();

        // 验证文件已删除
        assert!(!tmp_file1.exists());
        assert!(!tmp_file2.exists());
        assert!(!tmp_subdir.exists());

        // 临时目录本身应该仍然存在
        assert!(manager.tmp_dir().exists());
    }

    #[test]
    fn test_validate_paths() {
        let temp_dir = tempdir().unwrap();
        let manager = DbPathManager::new(temp_dir.path()).unwrap();

        // 应该验证通过
        manager.validate_paths().unwrap();

        // 删除一个目录应该导致验证失败
        fs::remove_dir(&manager.data_dir).unwrap();
        let result = manager.validate_paths();
        assert!(result.is_err());

        // 错误信息应该包含缺失的目录名
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("data_dir"));
    }

    #[test]
    fn test_path_summary() {
        let temp_dir = tempdir().unwrap();
        let manager = DbPathManager::new(temp_dir.path()).unwrap();

        let summary = manager.path_summary();

        // 检查摘要是否包含所有路径
        assert!(summary.contains("Database Paths:"));
        assert!(summary.contains("Base:"));
        assert!(summary.contains("Data:"));
        assert!(summary.contains("WAL:"));
        assert!(summary.contains("Log:"));
        assert!(summary.contains("Temp:"));

        // 检查路径是否正确显示
        let base_str = format!("{:?}", manager.base_path());
        assert!(summary.contains(&base_str));
    }

    #[test]
    fn test_for_test_method() {
        let manager = DbPathManager::for_test().unwrap();

        // 检查目录是否创建
        assert!(manager.base_path().exists());
        assert!(manager.data_dir().exists());
        assert!(manager.wal_dir().exists());
        assert!(manager.log_dir().exists());
        assert!(manager.tmp_dir().exists());

        // 检查路径是否在临时目录中
        let base_str = manager.base_path().to_str().unwrap();
        assert!(base_str.contains("goatdb_test"));
        assert!(base_str.contains("test_"));
    }
}
