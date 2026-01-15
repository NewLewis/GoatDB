use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::goatkv::metadata::version::Version;
use crate::goatkv::metadata::version_edit::{FileMetaData, VersionEdit};

/// VersionSet 管理所有版本和增量变更
pub struct VersionSet {
    /// 当前活跃版本（读取操作的视图）
    current: Arc<Version>,

    /// 历史版本列表（用于旧读取操作和压缩）
    versions: Vec<Arc<Version>>,

    /// 增量变更列表（用于重放）
    version_edits: Vec<VersionEdit>,

    /// MANIFEST 文件管理
    manifest_file: Option<ManifestWriter>,
    manifest_file_number: u64,

    /// 全局状态
    log_number: u64,
    next_file_number: u64,
    last_sequence: u64,
    comparator_name: String,

    /// SSTable 引用计数
    /// file_id -> count，用于判断文件是否可删除
    file_refs: HashMap<u64, usize>,

    /// 待删除的 SSTable 文件
    obsolete_files: Vec<FileMetaData>,

    /// 待处理的删除文件 ID（引用计数归零但还未从版本中移除）
    pending_deletion: HashSet<u64>,

    /// 数据库目录
    db_path: PathBuf,

    /// 配置选项
    options: VersionSetOptions,
}

/// VersionSet 配置选项
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

/// MANIFEST 文件写入器
pub struct ManifestWriter {
    writer: BufWriter<File>,
    file_number: u64,
    current_size: u64,
}

impl ManifestWriter {
    /// 创建新的 MANIFEST 文件
    pub fn create(path: &Path) -> Result<Self, std::io::Error> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        let writer = BufWriter::new(file);
        let current_size = 0u64;

        // 写入 MANIFEST 头部（可选，未来可以添加魔数和版本号）
        // 目前先保持简单，直接写 VersionEdit

        Ok(ManifestWriter {
            writer,
            file_number: 0,
            current_size,
        })
    }

    /// 从已有文件创建（用于恢复后追加）
    pub fn open_for_append(path: &Path, file_number: u64) -> Result<Self, std::io::Error> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        // 获取当前文件大小
        let metadata = file.metadata()?;
        let current_size = metadata.len();

        let writer = BufWriter::new(file);

        Ok(ManifestWriter {
            writer,
            file_number,
            current_size,
        })
    }

    /// 追加一条 VersionEdit
    pub fn append_edit(&mut self, edit: &VersionEdit) -> Result<(), std::io::Error> {
        let encoded = edit.encode();
        let len = encoded.len() as u64;

        // 写入长度前缀
        self.writer.write_all(&len.to_be_bytes())?;
        // 写入编码后的数据
        self.writer.write_all(&encoded)?;
        self.writer.flush()?;

        self.current_size += 8 + len; // 8 bytes for length

        Ok(())
    }

    /// 获取当前文件大小
    pub fn size(&self) -> u64 {
        self.current_size
    }

    /// 获取文件编号
    pub fn file_number(&self) -> u64 {
        self.file_number
    }

    /// 同步到磁盘
    pub fn sync(&mut self) -> Result<(), std::io::Error> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }
}

impl VersionSet {
    /// 创建一个新的空 VersionSet（用于新数据库）
    pub fn new(db_path: &Path, comparator_name: String) -> Result<Self, std::io::Error> {
        let options = VersionSetOptions::default();
        Self::new_with_options(db_path, comparator_name, options)
    }

    /// 使用指定选项创建 VersionSet
    pub fn new_with_options(
        db_path: &Path,
        comparator_name: String,
        options: VersionSetOptions,
    ) -> Result<Self, std::io::Error> {
        // 创建空的当前版本
        let current = Arc::new(Version::new(options.num_levels));

        Ok(Self {
            current,
            versions: Vec::new(),
            version_edits: Vec::new(),
            manifest_file: None,
            manifest_file_number: 0,
            log_number: 0,
            next_file_number: 1,
            last_sequence: 0,
            comparator_name,
            file_refs: HashMap::new(),
            obsolete_files: Vec::new(),
            pending_deletion: HashSet::new(),
            db_path: db_path.to_path_buf(),
            options,
        })
    }

    /// 应用 VersionEdit
    pub fn apply_edit(&mut self, edit: VersionEdit) -> Result<(), String> {
        // 1. 验证 VersionEdit 的合法性
        self.validate_edit(&edit)?;

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
        if let Some(ref comparator) = edit.comparator_name {
            if comparator != &self.comparator_name {
                return Err(format!(
                    "Comparator mismatch: expected {}, got {}",
                    self.comparator_name, comparator
                ));
            }
        }

        // 3. 保存到历史
        self.version_edits.push(edit.clone());

        // 4. 创建新 Version
        let new_version = self.create_new_version(&edit)?;

        // 5. 持久化到 MANIFEST（如果已打开）
        if let Some(ref mut manifest) = self.manifest_file {
            manifest.append_edit(&edit).map_err(|e| e.to_string())?;
        }

        // 6. 更新当前版本
        let old_version = std::mem::replace(&mut self.current, new_version);
        self.append_old_version(old_version);

        // 7. 清理和引用计数
        self.update_file_refs(&edit);
        self.cleanup_old_versions();

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
        for (level, meta) in &edit.new_files {
            if *level >= self.options.num_levels {
                return Err(format!(
                    "Invalid level {} (max {})",
                    level,
                    self.options.num_levels
                ));
            }

            // 检查文件 ID 是否已被使用
            if self.contains_file_any_level(meta.file_id) {
                return Err(format!("File ID {} already exists", meta.file_id));
            }

            // 验证 key 范围
            if meta.smallest_key > meta.largest_key {
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
        let mut new_files: Vec<Vec<Arc<FileMetaData>>> = self
            .current
            .all_files()
            .fold(vec![Vec::new(); self.options.num_levels], |mut acc, (level, file)| {
                acc[level].push(file);
                acc
            });

        // 应用删除的文件
        for (level, file_num) in &edit.deleted_files {
            if *level < new_files.len() {
                new_files[*level].retain(|f| f.file_id != *file_num);
            }
        }

        // 应用新增的文件
        for (level, meta) in &edit.new_files {
            if new_files.len() <= *level {
                new_files.resize(*level + 1, Vec::new());
            }
            new_files[*level].push(Arc::new(meta.clone()));
        }

        // 对 Level 0 以外的层级排序（保证不重叠且有序）
        for level_files in new_files.iter_mut().skip(1) {
            level_files.sort_by_key(|f| f.smallest_key.clone());
        }

        // 使用简单的字节比较器
        Ok(Arc::new(Version::from_files(
            new_files,
            self.last_sequence,
            |a, b| a.cmp(b),
        )))
    }

    /// 添加旧版本到历史列表
    fn append_old_version(&mut self, version: Arc<Version>) {
        self.versions.push(version);
    }

    /// 更新文件引用计数
    fn update_file_refs(&mut self, edit: &VersionEdit) {
        // 增加新文件的引用
        for (_, meta) in &edit.new_files {
            *self.file_refs.entry(meta.file_id).or_insert(0) += 1;
        }

        // 减少删除文件的引用
        for (_level, file_num) in &edit.deleted_files {
            if let Some(count) = self.file_refs.get_mut(file_num) {
                *count -= 1;
                if *count == 0 {
                    // 引用计数为 0，从 file_refs 中移除
                    // 并记录到 pending_deletion，在 cleanup_old_versions 时处理
                    self.file_refs.remove(file_num);
                    self.pending_deletion.insert(*file_num);
                }
            }
        }
    }

    /// 查找文件元数据（从所有版本中查找）
    fn find_file_meta(&self, file_id: u64) -> Option<FileMetaData> {
        // 先从当前版本查找
        for (_, file) in self.current.all_files() {
            if file.file_id == file_id {
                return Some((*file).clone());
            }
        }

        // 从历史版本查找
        for version in &self.versions {
            for (_, file) in version.all_files() {
                if file.file_id == file_id {
                    return Some((*file).clone());
                }
            }
        }

        None
    }

    /// 清理旧版本
    fn cleanup_old_versions(&mut self) {
        // 保留最新 max_versions 个版本
        while self.versions.len() > self.options.max_versions {
            let old = self.versions.remove(0);

            // 减少该版本中所有文件的引用计数
            for (_level, file) in old.all_files() {
                if let Some(count) = self.file_refs.get_mut(&file.file_id) {
                    *count -= 1;
                    if *count == 0 {
                        // 引用计数归零，标记为可删除
                        self.obsolete_files.push((*file).clone());
                        self.file_refs.remove(&file.file_id);
                        self.pending_deletion.remove(&file.file_id);
                    }
                }
            }
        }

        // 处理待删除的文件（引用计数已归零）
        // 这些文件需要从所有版本中查找元数据并标记为 obsolete
        let pending: Vec<u64> = self.pending_deletion.drain().collect();
        for file_id in pending {
            if let Some(meta) = self.find_file_meta(file_id) {
                self.obsolete_files.push(meta);
            }
        }
    }

    /// 打开 MANIFEST 文件用于写入
    pub fn open_manifest(&mut self, file_number: u64) -> Result<(), std::io::Error> {
        let manifest_path = self.db_path.join(format!("MANIFEST-{:06}", file_number));
        let manifest = ManifestWriter::open_for_append(&manifest_path, file_number)?;
        self.manifest_file = Some(manifest);
        self.manifest_file_number = file_number;
        Ok(())
    }

    /// 创建新的 MANIFEST 文件
    pub fn create_manifest(&mut self, file_number: u64) -> Result<(), std::io::Error> {
        let manifest_path = self.db_path.join(format!("MANIFEST-{:06}", file_number));
        let manifest = ManifestWriter::create(&manifest_path)?;
        self.manifest_file = Some(manifest);
        self.manifest_file_number = file_number;
        Ok(())
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

    /// 获取最后序列号
    pub fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    /// 获取待删除的文件
    pub fn obsolete_files(&self) -> &[FileMetaData] {
        &self.obsolete_files
    }

    /// 清空待删除文件列表（在物理删除后调用）
    pub fn clear_obsolete_files(&mut self) {
        self.obsolete_files.clear();
    }

    /// 检查是否需要重写 MANIFEST
    pub fn should_rewrite_manifest(&self) -> bool {
        if let Some(ref manifest) = self.manifest_file {
            // 检查文件大小
            if manifest.size() > self.options.manifest_max_size {
                return true;
            }
        }

        // 检查编辑数量
        if self.version_edits.len() > self.options.manifest_rewrite_edit_count {
            return true;
        }

        false
    }

    /// 获取所有 VersionEdit（用于重放）
    pub fn version_edits(&self) -> &[VersionEdit] {
        &self.version_edits
    }

    /// 获取比较器名称
    pub fn comparator_name(&self) -> &str {
        &self.comparator_name
    }

    /// 获取 MANIFEST 文件编号
    pub fn manifest_file_number(&self) -> u64 {
        self.manifest_file_number
    }
}

/// MANIFEST 文件读取器（用于恢复）
pub struct ManifestReader {
    reader: BufReader<File>,
}

impl ManifestReader {
    /// 打开 MANIFEST 文件读取
    pub fn open(path: &Path) -> Result<Self, std::io::Error> {
        let file = File::open(path)?;
        Ok(ManifestReader {
            reader: BufReader::new(file),
        })
    }

    /// 读取所有 VersionEdit
    pub fn read_all_edits(&mut self) -> Result<Vec<VersionEdit>, String> {
        let mut edits = Vec::new();

        loop {
            match self.read_next_edit() {
                Ok(Some(edit)) => edits.push(edit),
                Ok(None) => break, // EOF
                Err(e) => return Err(e),
            }
        }

        Ok(edits)
    }

    /// 读取下一个 VersionEdit
    fn read_next_edit(&mut self) -> Result<Option<VersionEdit>, String> {
        // 读取长度前缀
        let mut len_bytes = [0u8; 8];
        match self.reader.read_exact(&mut len_bytes) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.to_string()),
        }

        let len = u64::from_be_bytes(len_bytes) as usize;

        // 读取编码数据
        let mut buffer = vec![0u8; len];
        self.reader
            .read_exact(&mut buffer)
            .map_err(|e| e.to_string())?;

        // 解码
        let edit = VersionEdit::decode(&buffer)?;
        Ok(Some(edit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_file_meta(
        file_id: u64,
        smallest_key: &[u8],
        largest_key: &[u8],
        size: u64,
    ) -> FileMetaData {
        FileMetaData {
            file_id,
            file_size: size,
            smallest_key: smallest_key.to_vec(),
            largest_key: largest_key.to_vec(),
            smallest_seqno: 0,
            largest_seqno: 0,
        }
    }

    #[test]
    fn test_versionset_new() {
        let temp_dir = TempDir::new().unwrap();
        let vs = VersionSet::new(temp_dir.path(), "leveldb.BytewiseComparator".to_string()).unwrap();

        assert_eq!(vs.current().num_levels(), 7);
        assert_eq!(vs.next_file_number(), 1);
        assert_eq!(vs.last_sequence(), 0);
        assert_eq!(vs.log_number(), 0);
    }

    #[test]
    fn test_apply_edit_add_file() {
        let temp_dir = TempDir::new().unwrap();
        let mut vs =
            VersionSet::new(temp_dir.path(), "leveldb.BytewiseComparator".to_string()).unwrap();

        let mut edit = VersionEdit::new();
        edit.add_file(
            0,
            make_file_meta(1, b"a", b"z", 1000),
        );

        vs.apply_edit(edit).unwrap();

        // 检查文件已添加
        assert_eq!(vs.current().get_files(0).len(), 1);
        assert_eq!(vs.current().get_files(0)[0].file_id, 1);
    }

    #[test]
    fn test_apply_edit_delete_file() {
        let temp_dir = TempDir::new().unwrap();
        let mut vs =
            VersionSet::new(temp_dir.path(), "leveldb.BytewiseComparator".to_string()).unwrap();

        // 先添加文件
        let mut edit1 = VersionEdit::new();
        edit1.add_file(0, make_file_meta(1, b"a", b"z", 1000));
        vs.apply_edit(edit1).unwrap();

        // 删除文件
        let mut edit2 = VersionEdit::new();
        edit2.delete_file(0, 1);
        vs.apply_edit(edit2).unwrap();

        assert_eq!(vs.current().get_files(0).len(), 0);
    }

    #[test]
    fn test_validate_edit_invalid_delete() {
        let temp_dir = TempDir::new().unwrap();
        let mut vs =
            VersionSet::new(temp_dir.path(), "leveldb.BytewiseComparator".to_string()).unwrap();

        let mut edit = VersionEdit::new();
        edit.delete_file(0, 999);

        let result = vs.apply_edit(edit);
        assert!(result.is_err());
    }

    #[test]
    fn test_file_refs() {
        let temp_dir = TempDir::new().unwrap();
        let mut vs =
            VersionSet::new(temp_dir.path(), "leveldb.BytewiseComparator".to_string()).unwrap();

        let mut edit = VersionEdit::new();
        edit.add_file(0, make_file_meta(1, b"a", b"z", 1000));
        vs.apply_edit(edit).unwrap();

        // 文件引用计数应该为 1
        assert_eq!(vs.file_refs.get(&1), Some(&1));

        // 删除文件
        let mut edit2 = VersionEdit::new();
        edit2.delete_file(0, 1);
        vs.apply_edit(edit2).unwrap();

        // 文件应该被标记为 obsolete
        assert_eq!(vs.obsolete_files().len(), 1);
        assert_eq!(vs.obsolete_files()[0].file_id, 1);
    }

    #[test]
    fn test_manifest_io() {
        let temp_dir = TempDir::new().unwrap();
        let manifest_path = temp_dir.path().join("MANIFEST-000001");

        // 写入
        {
            let mut writer = ManifestWriter::create(&manifest_path).unwrap();
            assert_eq!(writer.size(), 0);

            let mut edit = VersionEdit::new();
            edit.set_log_number(42);
            edit.set_next_file_number(100);

            writer.append_edit(&edit).unwrap();
            assert!(writer.size() > 0);
        }

        // 读取
        {
            let mut reader = ManifestReader::open(&manifest_path).unwrap();
            let edits = reader.read_all_edits().unwrap();

            assert_eq!(edits.len(), 1);
            assert_eq!(edits[0].log_number, Some(42));
            assert_eq!(edits[0].next_file_number, Some(100));
        }
    }

    #[test]
    fn test_allocate_file_number() {
        let temp_dir = TempDir::new().unwrap();
        let mut vs =
            VersionSet::new(temp_dir.path(), "leveldb.BytewiseComparator".to_string()).unwrap();

        assert_eq!(vs.allocate_file_number(), 1);
        assert_eq!(vs.allocate_file_number(), 2);
        assert_eq!(vs.next_file_number(), 3);
    }

    #[test]
    fn test_should_rewrite_manifest() {
        let temp_dir = TempDir::new().unwrap();
        let mut vs =
            VersionSet::new(temp_dir.path(), "leveldb.BytewiseComparator".to_string()).unwrap();

        // 初始不应该重写
        assert!(!vs.should_rewrite_manifest());

        // 添加很多编辑
        for i in 0..20000 {
            let mut edit = VersionEdit::new();
            edit.set_log_number(i);
            vs.apply_edit(edit).unwrap();
        }

        // 现在应该重写
        assert!(vs.should_rewrite_manifest());
    }
}
