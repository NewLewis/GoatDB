use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::io;

use crate::goatkv::metadata::current;
use crate::goatkv::metadata::file_metadata::FileMetadata;
use crate::goatkv::metadata::manifest::{ManifestReader, ManifestWriter, INIT_MANIFEST_FILE_NAME};
use crate::goatkv::metadata::version::Version;
use crate::goatkv::metadata::version_edit::VersionEdit;
use crate::goatkv::utils::db_path_manager::DbPathManager;
use crate::goatkv::utils::options::KvEngineOptions;

/// VersionSet 管理所有版本和增量变更
#[derive(Debug)]
pub struct VersionSet {
    // ---------------- 版本管理 ----------------
    /// 只是一个简单的 ID 计数器，还是实际的 Version 链表头？
    /// 建议：使用双向链表管理 Version 或者是 VecDeque，
    /// dummy_versions 节点通常用于作为链表头方便插入删除。
    /// 这里简化为使用 Vec，但在高频写入下 Vec 的 remove 开销可能是 O(N)。
    versions: VecDeque<Arc<Version>>,

    // 注意：`current` 其实通常就是 versions.back()，
    // 单独存一个字段为了方便读取是可以的。
    current: Arc<Version>,

    // ---------------- 持久化 & 元数据 ----------------

    // 建议把 Manifest 相关的封装到一个独立的 struct 中，分离关注点
    manifest_writer: Option<ManifestWriter>,
    manifest_file_number: u64,

    // 全局序列号生成器
    log_number: u64,
    // todo 防止青黄不接的状态
    // prev_log_number: u64, // 很多时候需要记录前一个日志号以防崩溃
    next_file_number: u64,
    last_sequence: u64,

    /// 待物理删除的文件列表。
    /// 建议直接存 FileMetadata 或 file_number
    obsolete_sender: Sender<u64>,

    // ---------------- 环境配置 ----------------
    db_path: PathBuf,
    options: Arc<VersionSetOptions>, // Options 通常只读，用 Arc 共享
}

/// VersionSet 配置选项（内部使用，通过 KvEngineOptions 配置）
#[derive(Debug, Clone)]
pub struct VersionSetOptions {
    /// 保留的历史版本数量
    pub max_versions: usize,

    /// MANIFEST 文件大小限制（超过则重写）
    pub manifest_max_size: u64,

    /// 触发 MANIFEST 重写的版本编辑数量
    pub manifest_rewrite_edit_count: usize,

    /// 层级数量
    pub num_levels: usize,
}

impl Default for VersionSetOptions {
    fn default() -> Self {
        Self {
            max_versions: 10,
            manifest_max_size: 32 * 1024 * 1024, // 32MB
            manifest_rewrite_edit_count: 10000,
            num_levels: 7,
        }
    }
}

impl From<&KvEngineOptions> for VersionSetOptions {
    fn from(options: &KvEngineOptions) -> Self {
        Self {
            max_versions: options.max_versions,
            manifest_max_size: options.manifest_max_size,
            manifest_rewrite_edit_count: options.manifest_rewrite_edit_count,
            num_levels: options.num_levels,
        }
    }
}

impl VersionSet {
    /// 创建一个新的空 VersionSet（用于新数据库）
    pub fn new(db_path: &Path, obsolete_sender: Sender<u64>) -> Result<Self, std::io::Error> {
        let options = VersionSetOptions::default();
        Self::new_with_options(db_path, options, obsolete_sender)
    }

    /// 使用指定选项创建 VersionSet
    pub fn new_with_options(
        db_path: &Path,
        options: VersionSetOptions,
        obsolete_sender: Sender<u64>,
    ) -> Result<Self, std::io::Error> {
        // 创建空的当前版本
        let current = Arc::new(Version::new(options.num_levels));

        Ok(Self {
            current,
            versions: VecDeque::new(),
            manifest_writer: None,
            manifest_file_number: 0,
            log_number: 0,
            next_file_number: 1,
            last_sequence: 0,
            obsolete_sender,
            db_path: db_path.to_path_buf(),
            options: Arc::new(options),
        })
    }

    /// 打开已有数据库或创建新数据库（包含 MANIFEST 恢复）
    pub fn open(
        db_path: &Path,
        options: VersionSetOptions,
        obsolete_sender: Sender<u64>,
    ) -> Result<Self, std::io::Error> {
        let mut version_set = Self::new_with_options(db_path, options, obsolete_sender)?;
        version_set.recover_manifest()?;
        version_set.validate_recovery()?;
        Ok(version_set)
    }

    fn recover_manifest(&mut self) -> Result<(), std::io::Error> {
        let data_dir = DbPathManager::global().data_dir();

        // CURRENT 不存在时：尝试找到最新 MANIFEST 或创建初始 MANIFEST
        if !current::current_path().exists() {
            if let Some(latest) = current::find_latest_manifest()? {
                current::write_current(&latest)?;
            } else {
                let _ = self.create_initial_manifest()?;
            }
        }

        // 读取 CURRENT 指向的 MANIFEST
        let manifest_name = match current::read_current()? {
            Some(name) => name,
            None => self.create_initial_manifest()?,
        };

        let mut manifest_path = data_dir.join(&manifest_name);
        if !manifest_path.exists() {
            // CURRENT 指向的 MANIFEST 缺失，尝试回退到最新 MANIFEST
            if let Some(latest) = current::find_latest_manifest()? {
                current::write_current(&latest)?;
                manifest_path = data_dir.join(&latest);
            }
        }

        if !manifest_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("MANIFEST not found: {:?}", manifest_path),
            ));
        }

        let mut reader = ManifestReader::new(&manifest_path)?;
        let edits = reader
            .read_all_edits()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        for edit in edits {
            self.apply_edit_internal(edit, false)?;
        }

        self.ensure_next_file_number();

        let manifest_file_number = Self::parse_manifest_number(
            manifest_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("MANIFEST-0"),
        )
        .unwrap_or(0);
        self.manifest_writer =
            Some(ManifestWriter::open_for_append(&manifest_path, manifest_file_number)?);
        self.manifest_file_number = manifest_file_number;

        Ok(())
    }

    fn validate_recovery(&self) -> Result<(), std::io::Error> {
        let version = &self.current;
        let mut seen_files = std::collections::HashSet::new();

        for (level, file) in version.all_files() {
            if !seen_files.insert(file.file_id) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Duplicate file id detected: {}", file.file_id),
                ));
            }

            if file.smallest_key() > file.largest_key() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Invalid key range in level {} file {}",
                        level, file.file_id
                    ),
                ));
            }

            let path = DbPathManager::global().sstable_path_by_id(file.file_id);
            let metadata = std::fs::metadata(&path).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("SSTable missing {:?}: {}", path, e),
                )
            })?;
            if metadata.len() < file.file_size() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "SSTable size smaller than manifest (file {}, expected {}, got {})",
                        file.file_id,
                        file.file_size(),
                        metadata.len()
                    ),
                ));
            }

            // 尝试打开 SSTable，验证 footer/index/bloom 结构
            crate::goatkv::storage::sstable_reader::SSTableReader::open(&path).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid SSTable {:?}: {}", path, e),
                )
            })?;
        }

        // 检查 Level 1+ 的文件不重叠
        for level in 1..version.num_levels() {
            let files = version.get_files(level);
            for window in files.windows(2) {
                if window[0].largest_key() >= window[1].smallest_key() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Overlapping SSTables in level {}", level),
                    ));
                }
            }
        }

        // last_sequence 必须单调非递减（基础检查）
        if self.last_sequence < version.creation_seqno() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "last_sequence is behind current version",
            ));
        }

        // 清理未被引用的 SSTable 文件
        let data_dir = DbPathManager::global().data_dir();
        if data_dir.exists() {
            for entry in std::fs::read_dir(data_dir)? {
                let entry = entry?;
                let path = entry.path();
                if !path.is_file() {
                    continue;
                }
                if path.extension().and_then(|ext| ext.to_str()) != Some("sst") {
                    continue;
                }
                let file_id = match path.file_stem().and_then(|s| s.to_str()) {
                    Some(stem) => stem.parse::<u64>().ok(),
                    None => None,
                };
                if let Some(file_id) = file_id {
                    if !seen_files.contains(&file_id) {
                        if let Err(e) = std::fs::remove_file(&path) {
                            eprintln!("Failed to remove orphan SSTable {:?}: {}", path, e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn create_initial_manifest(&self) -> Result<String, std::io::Error> {
        let manifest_name = INIT_MANIFEST_FILE_NAME.to_string();
        let manifest_path = DbPathManager::global().data_dir().join(&manifest_name);
        let _ = ManifestWriter::create(&manifest_path)?;
        current::write_current(&manifest_name)?;
        Ok(manifest_name)
    }

    fn ensure_next_file_number(&mut self) {
        let max_file_id = self
            .current
            .all_file_ids()
            .iter()
            .copied()
            .max()
            .unwrap_or(0);
        if self.next_file_number <= max_file_id {
            self.next_file_number = max_file_id + 1;
        }
    }

    fn parse_manifest_number(manifest_name: &str) -> Option<u64> {
        manifest_name
            .strip_prefix("MANIFEST-")
            .and_then(|s| s.parse::<u64>().ok())
    }

    /// 应用 VersionEdit
    pub fn apply_edit(&mut self, edit: VersionEdit) -> Result<(), std::io::Error> {
        self.apply_edit_internal(edit, true)
    }

    fn apply_edit_internal(
        &mut self,
        edit: VersionEdit,
        write_manifest: bool,
    ) -> Result<(), std::io::Error> {
        // 1. 验证 VersionEdit 的合法性
        self.validate_edit(&edit)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // 先写 MANIFEST
        if write_manifest {
            let manifest = self.manifest_writer.as_mut().ok_or_else(|| {
                io::Error::new(io::ErrorKind::Other, "manifest writer not initialized")
            })?;
            manifest.append_edit(&edit)?;
            manifest.sync()?; // ⚠️ 必须 fsync
        }

        // 2. 更新全局状态
        if let Some(log_num) = edit.log_number {
            self.log_number = log_num;
        }
        if let Some(next_file) = edit.next_file_number {
            self.next_file_number = next_file;
        }
        if let Some(last_seq) = edit.last_sequence {
            self.last_sequence = last_seq;
        }

        // 4. 创建新 Version
        let new_version = self
            .create_new_version(&edit)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // 6. 更新当前版本
        let old_version = std::mem::replace(&mut self.current, new_version);
        self.append_old_version(old_version);

        Ok(())
    }

    /// 验证 VersionEdit 的合法性
    fn validate_edit(&self, edit: &VersionEdit) -> Result<(), String> {
        // 检查删除的文件是否存在
        for (level, file_num) in &edit.deleted_files {
            if !self.contains_file(*level, *file_num) {
                return Err(format!(
                    "Trying to delete non-existent file {} at level {}",
                    file_num, level
                ));
            }
        }

        // 检查新增文件的 ID 是否有效
        for (level, new_file) in &edit.new_files {
            if *level >= self.options.num_levels {
                return Err(format!(
                    "Invalid level {} (max {})",
                    level, self.options.num_levels
                ));
            }

            // 检查文件 ID 是否已被使用
            if self.contains_file_any_level(new_file.file_id) {
                return Err(format!("File ID {} already exists", new_file.file_id));
            }

            // 验证 key 范围
            if new_file.smallest_key() > new_file.largest_key() {
                return Err("Invalid key range: smallest > largest".to_string());
            }
        }

        Ok(())
    }

    /// 检查文件是否存在于指定层级
    fn contains_file(&self, level: usize, file_id: u64) -> bool {
        self.current
            .get_files(level)
            .iter()
            .any(|f| f.file_id == file_id)
    }

    /// 检查文件是否存在于任何层级
    fn contains_file_any_level(&self, file_id: u64) -> bool {
        for level in 0..self.current.num_levels() {
            if self.contains_file(level, file_id) {
                return true;
            }
        }
        false
    }

    /// 创建新 Version
    fn create_new_version(&self, edit: &VersionEdit) -> Result<Arc<Version>, String> {
        // 复制当前版本的所有文件
        let mut new_files: Vec<Vec<Arc<FileMetadata>>> = self.current.all_files().fold(
            vec![Vec::new(); self.options.num_levels],
            |mut acc, (level, file)| {
                acc[level].push(file);
                acc
            },
        );

        // 应用删除的文件
        for (level, file_num) in &edit.deleted_files {
            if *level < new_files.len() {
                new_files[*level].retain(|f| f.file_id != *file_num);
            }
        }

        // 应用新增的文件
        for (level, new_file) in &edit.new_files {
            if new_files.len() <= *level {
                new_files.resize(*level + 1, Vec::new());
            }
            new_files[*level].push(Arc::new(FileMetadata::from_new_file(
                new_file.clone(),
                self.obsolete_sender.clone(),
            )));
        }

        // 对 Level 0 以外的层级排序（保证不重叠且有序）
        for level_files in new_files.iter_mut().skip(1) {
            level_files.sort_by(|a, b| a.smallest_key().cmp(b.smallest_key()));
        }

        for level_files in new_files.iter().skip(1) {
            for w in level_files.windows(2) {
                debug_assert!(w[0].largest_key() < w[1].smallest_key());
            }
        }

        Ok(Arc::new(Version::from_files(new_files, self.last_sequence)))
    }

    /// 添加旧版本到历史列表
    fn append_old_version(&mut self, version: Arc<Version>) {
        self.versions.push_back(version);
        if self.versions.len() > self.options.max_versions {
            self.versions.pop_front();
        }
    }

    /// 获取当前版本
    pub fn current(&self) -> Arc<Version> {
        Arc::clone(&self.current)
    }

    /// 获取日志编号
    pub fn log_number(&self) -> u64 {
        self.log_number
    }

    /// 获取下一个文件编号
    pub fn next_file_number(&self) -> u64 {
        self.next_file_number
    }

    /// 分配文件编号
    pub fn allocate_file_number(&mut self) -> u64 {
        let num = self.next_file_number;
        self.next_file_number += 1;
        num
    }

    /// 分配新的 WAL 日志编号
    pub fn allocate_log_number(&mut self) -> u64 {
        self.log_number += 1;
        self.log_number
    }

    /// 获取最后序列号
    pub fn last_sequence(&self) -> u64 {
        self.last_sequence
    }
}
