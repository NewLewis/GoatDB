use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::goatkv::error::{Error as GoatError, Result as GoatResult};
use crate::goatkv::metadata::current;
use crate::goatkv::metadata::file_metadata::FileMetadata;
use crate::goatkv::metadata::manifest::{ManifestReader, ManifestWriter, INIT_MANIFEST_FILE_NAME};
use crate::goatkv::metadata::version::Version;
use crate::goatkv::metadata::version_edit::{NewFile, VersionEdit};
use crate::goatkv::storage::sstable::TableCache;
use crate::goatkv::utils::cleanup_task::CleanupTask;
use crate::goatkv::utils::options::KvEngineOptions;
use crate::goatkv::utils::paths::{ManifestPaths, SstablePaths};
use tokio::sync::mpsc::UnboundedSender;
use tracing::warn;

/// VersionSet 管理所有版本和增量变更。
///
/// 设计理由（核心取舍）：
/// - `Version` 是不可变快照，读路径可无锁读取；`VersionSet` 是可变管理器，负责生成快照。
/// - 将 MANIFEST 读写与版本切换集中在这里，避免不同模块重复维护元数据逻辑。
/// - 文件编号/日志编号/序列号统一由 VersionSet 分配，保证全局单调与唯一性。
/// - 通过保留有限数量历史版本，允许读路径持有旧快照而不阻塞写路径更新。
///
/// 核心职责：
/// - 维护当前版本和历史版本列表；
/// - 分配全局递增的文件编号、日志编号、序列号；
/// - 持久化 VersionEdit 到 MANIFEST，并支持恢复；
/// - 提供读取 current 版本的入口，供读路径无锁读取版本快照。
#[derive(Debug)]
pub struct VersionSet {
    // ---------------- 版本管理 ----------------
    /// 历史版本链表，用于保留最近的版本快照。
    /// 这些版本只读共享，读路径可并发访问。
    ///
    /// 设计理由：
    /// - 允许读路径短暂持有旧版本（读已落盘数据），不影响写路径推进 current；
    /// - VecDeque 便于按 max_versions 截断，避免无限增长。
    versions: VecDeque<Arc<Version>>,

    /// 当前版本（最新的元数据快照）。
    ///
    /// 读路径只需要 clone 这个 Arc，即可无锁读元数据。
    /// 注意：`current` 通常等于 `versions.back()`，但单独字段能减少访问开销。
    ///
    /// 设计理由：
    /// - 读路径频繁访问 current，独立字段可以减少 deque 访问成本；
    /// - 保障读路径不因历史版本管理逻辑而增加复杂度。
    current: Arc<Version>,

    // ---------------- 持久化 & 元数据 ----------------
    /// MANIFEST 写入器：用于追加 VersionEdit。
    /// 如果为 None，表示尚未初始化或尚未打开 MANIFEST。
    ///
    /// 设计理由：
    /// - MANIFEST 是元数据的唯一持久化来源，追加写更高效；
    /// - 通过 append + fsync 保证崩溃恢复的可重放性。
    manifest_writer: Option<ManifestWriter>,
    /// 当前 MANIFEST 文件编号。
    manifest_file_number: u64,
    /// 当前 MANIFEST 自上次重写以来累计的 edit 数量。
    manifest_edit_count: usize,

    /// 最近一次记录的 WAL 日志编号。
    ///
    /// 设计理由：
    /// - 通过记录最新 WAL 号，恢复时可以忽略更早 WAL；
    /// - 与 VersionEdit 一起持久化，保证恢复一致。
    log_number: u64,
    ///
    /// 设计理由：
    /// - 全局唯一编号避免文件覆盖、重复引用；
    /// - 恢复时可从 current 推断最大编号并继续分配。
    next_file_number: u64,
    /// 当前最后序列号（用于读写一致性）。
    ///
    /// 设计理由：
    /// - 保证 MVCC 语义下的单调序列号；
    /// - 用于恢复与读路径可见性判断。
    last_sequence: u64,
    /// 每层 compaction pointer（记录下一次优先开始的 user key）
    compact_pointers: Vec<Option<Vec<u8>>>,

    /// 待物理删除的文件列表。
    /// 建议直接存 FileMetadata 或 file_number。
    ///
    /// 设计理由：
    /// - 删除由后台异步执行，避免阻塞写路径；
    /// - 读路径可能仍持有旧版本，延迟删除避免读到缺失文件。
    obsolete_sender: UnboundedSender<CleanupTask>,

    // ---------------- 环境配置 ----------------
    manifest_paths: Arc<ManifestPaths>,
    sstable_paths: Arc<SstablePaths>,
    table_cache: Option<Arc<TableCache>>,
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

    /// Table cache 最大条目数（0 表示禁用 table cache）
    pub table_cache_capacity: usize,

    /// Block cache 容量（字节，0 表示禁用 block cache）
    pub block_cache_capacity_bytes: usize,
}

impl Default for VersionSetOptions {
    fn default() -> Self {
        Self {
            max_versions: 10,
            manifest_max_size: 32 * 1024 * 1024, // 32MB
            manifest_rewrite_edit_count: 10000,
            num_levels: 7,
            table_cache_capacity: 64,
            block_cache_capacity_bytes: 64 * 1024 * 1024,
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
            table_cache_capacity: options.table_cache_capacity,
            block_cache_capacity_bytes: options.block_cache_capacity_bytes,
        }
    }
}

impl VersionSet {
    /// 创建一个新的空 VersionSet（用于新数据库）
    pub fn new(
        manifest_paths: Arc<ManifestPaths>,
        sstable_paths: Arc<SstablePaths>,
        obsolete_sender: UnboundedSender<CleanupTask>,
    ) -> GoatResult<Self> {
        let options = VersionSetOptions::default();
        Self::new_with_options(manifest_paths, sstable_paths, options, obsolete_sender)
    }

    /// 使用指定选项创建 VersionSet
    pub fn new_with_options(
        manifest_paths: Arc<ManifestPaths>,
        sstable_paths: Arc<SstablePaths>,
        options: VersionSetOptions,
        obsolete_sender: UnboundedSender<CleanupTask>,
    ) -> GoatResult<Self> {
        let table_cache =
            if options.table_cache_capacity == 0 && options.block_cache_capacity_bytes == 0 {
                None
            } else {
                Some(Arc::new(TableCache::new(
                    options.table_cache_capacity,
                    options.block_cache_capacity_bytes,
                )))
            };

        // 创建空的当前版本：没有任何 SSTable
        let current = Arc::new(Version::new_with_cache(
            options.num_levels,
            sstable_paths.clone(),
            table_cache.clone(),
        ));

        Ok(Self {
            current,
            versions: VecDeque::new(),
            manifest_writer: None,
            manifest_file_number: 0,
            manifest_edit_count: 0,
            log_number: 0,
            next_file_number: 1,
            last_sequence: 0,
            compact_pointers: vec![None; options.num_levels],
            obsolete_sender,
            manifest_paths,
            sstable_paths,
            table_cache,
            options: Arc::new(options),
        })
    }

    /// 打开已有数据库或创建新数据库（包含 MANIFEST 恢复）
    pub fn open(
        manifest_paths: Arc<ManifestPaths>,
        sstable_paths: Arc<SstablePaths>,
        options: VersionSetOptions,
        obsolete_sender: UnboundedSender<CleanupTask>,
    ) -> GoatResult<Self> {
        // 先创建空 VersionSet，再通过 MANIFEST 恢复到最新状态
        //
        // 设计理由：
        // - 统一恢复入口，避免分散在多个模块；
        // - 通过 replay edits 复原 current，保证与历史行为一致。
        let mut version_set =
            Self::new_with_options(manifest_paths, sstable_paths, options, obsolete_sender)?;
        version_set.recover_manifest()?;
        // 恢复后做一致性校验，避免使用损坏或不完整的元数据
        version_set.validate_recovery()?;
        Ok(version_set)
    }

    fn recover_manifest(&mut self) -> GoatResult<()> {
        let data_dir = self.manifest_paths.data_dir();

        let manifest_path = self.resolve_manifest(data_dir)?;

        // 顺序读取 MANIFEST 中的所有 VersionEdit
        //
        // 设计理由：
        // - MANIFEST 为 append-only，顺序回放即可恢复最新状态；
        // - 通过 replay 可保证元数据与历史行为一致。
        let mut reader = ManifestReader::new(&manifest_path)?;
        let edits = reader.read_all_edits()?;
        let recovered_edit_count = edits.len();

        // 按顺序应用 edit，重建 current 版本（恢复快路径）
        // 设计理由：
        // - 恢复时 edit 数量可能很大，逐条创建 Version 会导致 O(E * F) 开销；
        // - 使用原地更新文件列表，最后一次性构建 Version，降低恢复成本。
        self.apply_edits_for_recovery(edits)?;

        // 确保 next_file_number 大于现有最大文件编号
        //
        // 设计理由：
        // - 恢复后避免分配与现有文件冲突的编号；
        // - 即使 MANIFEST 缺失部分编号，也能保证递增唯一。
        self.ensure_next_file_number();

        // 记录当前使用的 MANIFEST 文件编号，并以追加模式打开
        let manifest_file_number = Self::parse_manifest_number(
            manifest_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("MANIFEST-0"),
        )
        .unwrap_or(0);
        self.manifest_writer = Some(ManifestWriter::open_for_append(
            &manifest_path,
            manifest_file_number,
        )?);
        self.manifest_file_number = manifest_file_number;
        self.manifest_edit_count = recovered_edit_count;

        Ok(())
    }

    fn validate_recovery(&self) -> GoatResult<()> {
        let version = &self.current;
        let mut seen_files = std::collections::HashSet::new();

        // 校验：文件 ID 不重复、key 范围合法、SSTable 文件存在且尺寸匹配
        //
        // 设计理由：
        // - 通过恢复后校验，尽早发现损坏或不一致；
        // - 防止读路径访问损坏文件引发不可控错误。
        for (level, file) in version.all_files() {
            if !seen_files.insert(file.file_id) {
                return Err(GoatError::corruption(
                    "version_recovery",
                    format!("Duplicate file id detected: {}", file.file_id),
                ));
            }

            if file.smallest_key() > file.largest_key() {
                return Err(GoatError::corruption(
                    "version_recovery",
                    format!("Invalid key range in level {} file {}", level, file.file_id),
                ));
            }

            let path = self.sstable_paths.sstable_path_by_id(file.file_id);
            let metadata = std::fs::metadata(&path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    GoatError::not_found("sstable", format!("{:?}", path))
                } else {
                    GoatError::io("validate_recovery_sstable_metadata", e)
                }
            })?;
            if metadata.len() < file.file_size() {
                return Err(GoatError::corruption(
                    "version_recovery",
                    format!(
                        "SSTable size smaller than manifest (file {}, expected {}, got {})",
                        file.file_id,
                        file.file_size(),
                        metadata.len()
                    ),
                ));
            }

            // 尝试打开 SSTable，验证 footer/index/bloom 结构
            crate::goatkv::storage::sstable::SSTableReader::open(&path).map_err(|e| {
                GoatError::corruption(
                    "version_recovery",
                    format!("Invalid SSTable {:?}: {}", path, e),
                )
            })?;
        }

        // 检查 Level 1+ 的文件不重叠（按顺序且不相交）
        //
        // 设计理由：
        // - Level 0 允许重叠，其他层级必须有序且不相交；
        // - 保障读路径可以二分查找。
        for level in 1..version.num_levels() {
            let files = version.get_files(level);
            for window in files.windows(2) {
                if window[0].largest_key() >= window[1].smallest_key() {
                    return Err(GoatError::corruption(
                        "version_recovery",
                        format!("Overlapping SSTables in level {}", level),
                    ));
                }
            }
        }

        // last_sequence 必须单调非递减（基础检查）
        if self.last_sequence < version.creation_seqno() {
            return Err(GoatError::corruption(
                "version_recovery",
                "last_sequence is behind current version",
            ));
        }

        // 清理未被引用的 SSTable 文件
        //
        // 设计理由：
        // - 避免磁盘泄漏；
        // - 恢复后对齐 manifest 状态，保持目录整洁。
        let data_dir = self.sstable_paths.data_dir();
        if data_dir.exists() {
            for entry in
                std::fs::read_dir(data_dir).map_err(|e| GoatError::io("list_sstable_dir", e))?
            {
                let entry = entry.map_err(|e| GoatError::io("read_sstable_dir_entry", e))?;
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
                            warn!("Failed to remove orphan SSTable {:?}: {}", path, e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn create_initial_manifest(&self) -> GoatResult<String> {
        let manifest_name = INIT_MANIFEST_FILE_NAME.to_string();
        let manifest_path = self.manifest_paths.data_dir().join(&manifest_name);
        let _ = ManifestWriter::create(&manifest_path)?;
        current::write_current(&self.manifest_paths, &manifest_name)?;
        Ok(manifest_name)
    }

    fn ensure_next_file_number(&mut self) {
        // 从 current 版本里找最大文件 ID，确保 next_file_number 递增
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

    fn is_manifest_name_valid(manifest_name: &str, manifest_path: &Path) -> bool {
        Self::parse_manifest_number(manifest_name).is_some() && manifest_path.is_file()
    }

    fn resolve_manifest(&self, data_dir: &Path) -> GoatResult<PathBuf> {
        // 获取 CURRENT 指向的 MANIFEST；若 CURRENT 缺失，则回退到最新 MANIFEST
        // 或创建初始 MANIFEST。
        let mut manifest_name = match current::read_current(&self.manifest_paths)? {
            Some(name) => name,
            None => {
                if let Some(latest) = current::find_latest_manifest(&self.manifest_paths)? {
                    current::write_current(&self.manifest_paths, &latest)?;
                    latest
                } else {
                    self.create_initial_manifest()?
                }
            }
        };

        let mut manifest_path = data_dir.join(&manifest_name);
        let mut manifest_valid = Self::is_manifest_name_valid(&manifest_name, &manifest_path);
        if !manifest_valid {
            // CURRENT 内容非法或指向不存在的 MANIFEST，尝试回退到最新 MANIFEST
            if let Some(latest) = current::find_latest_manifest(&self.manifest_paths)? {
                current::write_current(&self.manifest_paths, &latest)?;
                manifest_name = latest;
                manifest_path = data_dir.join(&manifest_name);
                manifest_valid = Self::is_manifest_name_valid(&manifest_name, &manifest_path);
            } else {
                manifest_name = self.create_initial_manifest()?;
                manifest_path = data_dir.join(&manifest_name);
                manifest_valid = Self::is_manifest_name_valid(&manifest_name, &manifest_path);
            }
        }

        if !manifest_valid {
            return Err(GoatError::not_found(
                "manifest",
                format!("{:?}", manifest_path),
            ));
        }

        Ok(manifest_path)
    }

    fn apply_edits_for_recovery(&mut self, edits: Vec<VersionEdit>) -> GoatResult<()> {
        let mut files = self.current.clone_files();
        self.versions.clear();

        for edit in edits {
            self.apply_edit_in_place(edit, &mut files)
                .map_err(|e| GoatError::corruption("manifest_recovery", e.to_string()))?;
        }

        Self::sort_level_files(&mut files);
        self.current = Arc::new(Version::from_files_with_cache(
            files,
            self.last_sequence,
            self.sstable_paths.clone(),
            self.table_cache.clone(),
        ));

        Ok(())
    }

    /// 应用 VersionEdit
    pub fn apply_edit(&mut self, edit: VersionEdit) -> GoatResult<()> {
        self.apply_edit_internal(edit, true)
    }

    fn apply_edit_internal(&mut self, edit: VersionEdit, write_manifest: bool) -> GoatResult<()> {
        // 1. 验证 VersionEdit 的合法性，避免非法删除或重复新增
        //
        // 设计理由：
        // - 将安全性检查集中在此处；
        // - 防止错误 edit 破坏 current 结构。
        self.validate_edit(&edit)?;

        // 2. 先写 MANIFEST，保证崩溃恢复时元数据可重放
        //
        // 设计理由：
        // - 采用 WAL 思路：先持久化 edit，再更新内存；
        // - 崩溃后能通过 MANIFEST 重放恢复一致性。
        if write_manifest {
            let manifest = self.manifest_writer.as_mut().ok_or_else(|| {
                GoatError::internal("version_set", "manifest writer not initialized")
            })?;
            manifest.append_edit(&edit)?;
            manifest.sync()?; // ⚠️ 必须 fsync
            self.manifest_edit_count = self.manifest_edit_count.saturating_add(1);
        }

        // 3. 更新全局状态（日志号、文件号、序列号）
        //
        // 设计理由：
        // - 全局计数器必须与 edit 同步推进；
        // - 保证后续分配的编号与恢复逻辑一致。
        if let Some(log_num) = edit.log_number {
            self.log_number = log_num;
        }
        if let Some(next_file) = edit.next_file_number {
            // 并发 flush/compaction 可能提交“过期”的 next_file_number。
            // 这里必须单调推进，避免文件号回退后重复分配同一 file_id。
            self.next_file_number = self.next_file_number.max(next_file);
        }
        if let Some(last_seq) = edit.last_sequence {
            self.last_sequence = last_seq;
        }
        self.apply_compact_pointers(&edit)?;

        // 4. 基于 edit 创建新的 Version（不可变快照）
        //
        // 设计理由：
        // - 旧版本保持不变，读路径可继续使用；
        // - 写路径通过生成新版本完成原子切换。
        let new_version = self.create_new_version(&edit)?;

        // 5. 更新当前版本，并将旧版本加入历史列表
        //
        // 设计理由：
        // - 允许读路径持有旧版本，降低锁争用；
        // - 历史版本数量受限，避免内存膨胀。
        let old_version = std::mem::replace(&mut self.current, new_version);
        self.append_old_version(old_version);
        if write_manifest {
            self.maybe_rewrite_manifest()?;
        }

        Ok(())
    }

    /// 验证 VersionEdit 的合法性
    fn validate_edit(&self, edit: &VersionEdit) -> GoatResult<()> {
        let mut seen_new_file_ids = std::collections::HashSet::new();

        // 检查删除的文件是否存在
        for (level, file_num) in &edit.deleted_files {
            if !self.contains_file(*level, *file_num) {
                return Err(GoatError::conflict(
                    "version_edit",
                    format!(
                        "Trying to delete non-existent file {} at level {}",
                        file_num, level
                    ),
                ));
            }
        }

        // 检查新增文件的 ID 是否有效
        for (level, new_file) in &edit.new_files {
            if !seen_new_file_ids.insert(new_file.file_id) {
                return Err(GoatError::conflict(
                    "version_edit",
                    format!("Duplicate new file id {} in one edit", new_file.file_id),
                ));
            }

            if *level >= self.options.num_levels {
                return Err(GoatError::conflict(
                    "version_edit",
                    format!("Invalid level {} (max {})", level, self.options.num_levels),
                ));
            }

            // 检查文件 ID 是否已被使用
            let reused_after_delete = edit
                .deleted_files
                .iter()
                .any(|(_, deleted_id)| *deleted_id == new_file.file_id);
            if self.contains_file_any_level(new_file.file_id) && !reused_after_delete {
                return Err(GoatError::conflict(
                    "version_edit",
                    format!("File ID {} already exists", new_file.file_id),
                ));
            }

            // 验证 key 范围
            if new_file.smallest_key() > new_file.largest_key() {
                return Err(GoatError::conflict(
                    "version_edit",
                    "Invalid key range: smallest > largest",
                ));
            }
        }

        for (level, _) in &edit.compact_pointers {
            if *level >= self.options.num_levels {
                return Err(GoatError::conflict(
                    "version_edit",
                    format!(
                        "Invalid compact pointer level {} (max {})",
                        level, self.options.num_levels
                    ),
                ));
            }
        }

        Ok(())
    }

    fn validate_edit_for_files(
        &self,
        edit: &VersionEdit,
        files: &[Vec<Arc<FileMetadata>>],
    ) -> GoatResult<()> {
        let mut seen_new_file_ids = std::collections::HashSet::new();

        for (level, file_num) in &edit.deleted_files {
            if !Self::contains_file_in_files(files, *level, *file_num) {
                return Err(GoatError::conflict(
                    "version_edit",
                    format!(
                        "Trying to delete non-existent file {} at level {}",
                        file_num, level
                    ),
                ));
            }
        }

        for (level, new_file) in &edit.new_files {
            if !seen_new_file_ids.insert(new_file.file_id) {
                return Err(GoatError::conflict(
                    "version_edit",
                    format!("Duplicate new file id {} in one edit", new_file.file_id),
                ));
            }

            if *level >= self.options.num_levels {
                return Err(GoatError::conflict(
                    "version_edit",
                    format!("Invalid level {} (max {})", level, self.options.num_levels),
                ));
            }

            let reused_after_delete = edit
                .deleted_files
                .iter()
                .any(|(_, deleted_id)| *deleted_id == new_file.file_id);
            if Self::contains_file_any_level_in_files(files, new_file.file_id)
                && !reused_after_delete
            {
                return Err(GoatError::conflict(
                    "version_edit",
                    format!("File ID {} already exists", new_file.file_id),
                ));
            }

            if new_file.smallest_key() > new_file.largest_key() {
                return Err(GoatError::conflict(
                    "version_edit",
                    "Invalid key range: smallest > largest",
                ));
            }
        }

        for (level, _) in &edit.compact_pointers {
            if *level >= self.options.num_levels {
                return Err(GoatError::conflict(
                    "version_edit",
                    format!(
                        "Invalid compact pointer level {} (max {})",
                        level, self.options.num_levels
                    ),
                ));
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

    fn contains_file_in_files(
        files: &[Vec<Arc<FileMetadata>>],
        level: usize,
        file_id: u64,
    ) -> bool {
        files
            .get(level)
            .map(|level_files| level_files.iter().any(|f| f.file_id == file_id))
            .unwrap_or(false)
    }

    fn contains_file_any_level_in_files(files: &[Vec<Arc<FileMetadata>>], file_id: u64) -> bool {
        files
            .iter()
            .any(|level_files| level_files.iter().any(|f| f.file_id == file_id))
    }

    /// 创建新 Version
    fn create_new_version(&self, edit: &VersionEdit) -> GoatResult<Arc<Version>> {
        // 复制当前版本的所有文件到新的列表中
        //
        // 设计理由：
        // - 以 copy-on-write 的方式构建新版本；
        // - 只在元数据层复制 Arc，不复制实际 SSTable。
        let mut new_files: Vec<Vec<Arc<FileMetadata>>> = self.current.clone_files();

        // 应用删除的文件（从对应层级移除）
        //
        // 设计理由：
        // - edit 只描述增量变更；
        // - 删除通过过滤完成，不影响旧版本。
        for (level, file_num) in &edit.deleted_files {
            if *level < new_files.len() {
                new_files[*level].retain(|f| f.file_id != *file_num);
            }
        }

        // 应用新增的文件（插入到对应层级）
        //
        // 设计理由：
        // - 新文件由 compaction/flush 产生；
        // - 用 Arc 包装 FileMetadata 便于跨版本共享。
        for (level, new_file) in &edit.new_files {
            if new_files.len() <= *level {
                new_files.resize(*level + 1, Vec::new());
            }
            new_files[*level].push(Arc::new(FileMetadata::from_new_file_with_sstable_paths(
                new_file.clone(),
                self.obsolete_sender.clone(),
                self.sstable_paths.as_ref(),
            )));
        }

        Self::sort_level_files(&mut new_files);

        // 使用最新的 last_sequence 创建 Version 快照
        //
        // 设计理由：
        // - 将 last_sequence 绑定到版本，便于恢复时做一致性校验。
        Ok(Arc::new(Version::from_files_with_cache(
            new_files,
            self.last_sequence,
            self.sstable_paths.clone(),
            self.table_cache.clone(),
        )))
    }

    /// 添加旧版本到历史列表
    fn append_old_version(&mut self, version: Arc<Version>) {
        // 保留最近 max_versions 个历史版本
        //
        // 设计理由：
        // - 限制历史版本数量，避免无限增长；
        // - 读路径通常只需要短期持有旧版本。
        self.versions.push_back(version);
        if self.versions.len() > self.options.max_versions {
            if let Some(dropped) = self.versions.pop_front() {
                self.enqueue_obsolete_files_from_dropped_version(&dropped);
            }
        }
    }

    fn collect_live_file_ids(&self) -> HashSet<u64> {
        let mut live = self.current.all_file_ids();
        for version in &self.versions {
            live.extend(version.all_file_ids());
        }
        live
    }

    fn enqueue_obsolete_files_from_dropped_version(&self, dropped: &Arc<Version>) {
        let live = self.collect_live_file_ids();
        for file_id in dropped.all_file_ids() {
            if !live.contains(&file_id) {
                let _ = self.obsolete_sender.send(CleanupTask::Sstable(file_id));
            }
        }
    }

    fn should_rewrite_manifest(&self) -> bool {
        let hit_edit_count = self.options.manifest_rewrite_edit_count > 0
            && self.manifest_edit_count >= self.options.manifest_rewrite_edit_count;
        let hit_size = self.options.manifest_max_size > 0
            && self
                .manifest_writer
                .as_ref()
                .map(|m| m.size() >= self.options.manifest_max_size)
                .unwrap_or(false);
        hit_edit_count || hit_size
    }

    fn build_snapshot_edit(&self) -> VersionEdit {
        let mut edit = VersionEdit::new();
        edit.set_log_number(self.log_number);
        edit.set_next_file_number(self.next_file_number);
        edit.set_last_sequence(self.last_sequence);
        for (level, key) in self.compact_pointers.iter().enumerate() {
            if let Some(key) = key {
                edit.compact_pointers.push((level, key.clone()));
            }
        }
        for (level, file) in self.current.all_files() {
            edit.add_file(
                level,
                NewFile::new_with_props(file.file_id, file.props.clone()),
            );
        }
        edit
    }

    fn maybe_rewrite_manifest(&mut self) -> GoatResult<()> {
        if !self.should_rewrite_manifest() {
            return Ok(());
        }

        let new_manifest_number = self.manifest_file_number.saturating_add(1);
        let new_manifest_name = format!("MANIFEST-{}", new_manifest_number);
        let new_manifest_path = self.manifest_paths.data_dir().join(&new_manifest_name);

        let _ = ManifestWriter::create(&new_manifest_path)?;
        let mut new_writer =
            ManifestWriter::open_for_append(&new_manifest_path, new_manifest_number)?;
        let snapshot = self.build_snapshot_edit();
        new_writer.append_edit(&snapshot)?;
        new_writer.sync()?;
        current::write_current(&self.manifest_paths, &new_manifest_name)?;

        self.manifest_writer = Some(new_writer);
        self.manifest_file_number = new_manifest_number;
        self.manifest_edit_count = 1;
        Ok(())
    }

    fn apply_edit_in_place(
        &mut self,
        edit: VersionEdit,
        files: &mut Vec<Vec<Arc<FileMetadata>>>,
    ) -> GoatResult<()> {
        self.validate_edit_for_files(&edit, files)?;

        if let Some(log_num) = edit.log_number {
            self.log_number = log_num;
        }
        if let Some(next_file) = edit.next_file_number {
            // 恢复阶段同样保证单调推进，避免 MANIFEST 中过期值导致回退。
            self.next_file_number = self.next_file_number.max(next_file);
        }
        if let Some(last_seq) = edit.last_sequence {
            self.last_sequence = last_seq;
        }
        self.apply_compact_pointers(&edit)?;

        for (level, file_num) in edit.deleted_files {
            if level < files.len() {
                files[level].retain(|f| f.file_id != file_num);
            }
        }

        for (level, new_file) in edit.new_files {
            if files.len() <= level {
                files.resize(level + 1, Vec::new());
            }
            files[level].push(Arc::new(FileMetadata::from_new_file_with_sstable_paths(
                new_file,
                self.obsolete_sender.clone(),
                self.sstable_paths.as_ref(),
            )));
        }

        Ok(())
    }

    fn sort_level_files(files: &mut [Vec<Arc<FileMetadata>>]) {
        // 对 Level 0 以外的层级排序（保证不重叠且有序）
        //
        // 设计理由：
        // - Level 1+ 需要有序且不重叠，便于二分查找；
        // - 排序确保元数据符合读路径假设。
        for level_files in files.iter_mut().skip(1) {
            level_files.sort_by(|a, b| a.smallest_key().cmp(b.smallest_key()));
            for w in level_files.windows(2) {
                debug_assert!(w[0].largest_key() < w[1].smallest_key());
            }
        }
    }

    /// 获取当前版本
    pub fn current(&self) -> Arc<Version> {
        // 读路径通过 clone Arc 获取快照，不需要加锁
        //
        // 设计理由：
        // - 读路径只依赖不可变快照；
        // - 降低锁争用，提高并发读性能。
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
        // 单调递增分配，保证文件编号不重复
        let num = self.next_file_number;
        self.next_file_number += 1;
        num
    }

    /// 分配新的 WAL 日志编号
    pub fn allocate_log_number(&mut self) -> u64 {
        // WAL 编号单调递增
        self.log_number += 1;
        self.log_number
    }

    /// 获取最后序列号
    pub fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    /// 获取某层 compaction pointer（user key）
    pub fn compact_pointer(&self, level: usize) -> Option<Vec<u8>> {
        self.compact_pointers.get(level).and_then(|k| k.clone())
    }

    /// 获取 compaction pointers 快照
    pub fn compact_pointers_snapshot(&self) -> Vec<Option<Vec<u8>>> {
        self.compact_pointers.clone()
    }

    fn apply_compact_pointers(&mut self, edit: &VersionEdit) -> GoatResult<()> {
        for (level, key) in &edit.compact_pointers {
            if *level >= self.compact_pointers.len() {
                return Err(GoatError::conflict(
                    "version_edit",
                    format!(
                        "Invalid compact pointer level {} (max {})",
                        level,
                        self.compact_pointers.len()
                    ),
                ));
            }
            self.compact_pointers[*level] = Some(key.clone());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{VersionEdit, VersionSet, VersionSetOptions};
    use crate::goatkv::core::kv_engine::KvEngine;
    use crate::goatkv::utils::cleanup_task::CleanupTask;
    use tempfile::TempDir;
    use tokio::sync::mpsc::unbounded_channel;

    fn open_version_set(base: &std::path::Path) -> VersionSet {
        let (_wal_paths, sstable_paths, manifest_paths) =
            KvEngine::init_db_paths(base).expect("init db paths");
        let (obsolete_tx, _obsolete_rx) = unbounded_channel::<CleanupTask>();
        VersionSet::open(
            manifest_paths,
            sstable_paths,
            VersionSetOptions::default(),
            obsolete_tx,
        )
        .expect("open version set")
    }

    #[test]
    fn next_file_number_never_moves_backward_on_stale_edit() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let mut version_set = open_version_set(temp_dir.path());

        let mut edit_high = VersionEdit::new();
        edit_high.set_next_file_number(32);
        version_set.apply_edit(edit_high).expect("apply high edit");
        assert_eq!(version_set.next_file_number(), 32);

        let mut edit_stale = VersionEdit::new();
        edit_stale.set_next_file_number(5);
        version_set
            .apply_edit(edit_stale)
            .expect("apply stale edit should not roll back");
        assert_eq!(version_set.next_file_number(), 32);
    }

    #[test]
    fn recovery_keeps_next_file_number_monotonic_with_stale_manifest_edits() {
        let temp_dir = TempDir::new().expect("create temp dir");

        let mut version_set = open_version_set(temp_dir.path());
        let mut edit_high = VersionEdit::new();
        edit_high.set_next_file_number(64);
        version_set.apply_edit(edit_high).expect("apply high edit");

        let mut edit_stale = VersionEdit::new();
        edit_stale.set_next_file_number(7);
        version_set
            .apply_edit(edit_stale)
            .expect("apply stale edit should not roll back");
        drop(version_set);

        let reopened = open_version_set(temp_dir.path());
        assert_eq!(reopened.next_file_number(), 64);
    }
}
