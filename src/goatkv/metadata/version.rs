use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use crate::goatkv::error::{Error as GoatError, ErrorKind as GoatErrorKind, Result as GoatResult};
use crate::goatkv::format::internal_key::InternalKey;
use crate::goatkv::metadata::file_metadata::FileMetadata;
use crate::goatkv::storage::sstable::{
    PinnedValue, ReadCacheMetrics, RowCacheValue, SSTableReader, TableCache,
};
use crate::goatkv::utils::paths::SstablePaths;

/// Version 代表某一时刻数据库的完整状态
/// 一旦创建后不可修改，支持并发无锁读取
#[derive(Debug, Clone)]
pub struct Version {
    /// 每层包含的 SSTable 文件元数据
    /// files[level] = Vec<Arc<FileMetadata>>
    files: Vec<Vec<Arc<FileMetadata>>>,

    /// 每层的总大小（用于触发压缩）
    level_size_bytes: Vec<u64>,

    /// 该版本创建时的序列号
    creation_seqno: u64,
    /// 可选 table/block cache（跨版本共享）
    table_cache: Option<Arc<TableCache>>,
}

impl Version {
    fn map_sstable_open_err(path: &Path, err: GoatError) -> GoatError {
        if matches!(
            &err,
            GoatError::Io { source, .. } if source.kind() == std::io::ErrorKind::NotFound
        ) || err.kind() == GoatErrorKind::NotFound
        {
            return GoatError::not_found("sstable", format!("missing during open: {:?}", path));
        }

        match err.kind() {
            GoatErrorKind::Corruption => {
                GoatError::corruption("sstable_open", format!("{:?}: {}", path, err))
            }
            _ => GoatError::internal_with_source(
                "sstable_open",
                format!("failed to open sstable: {:?}", path),
                err,
            ),
        }
    }

    fn map_sstable_read_err(path: &Path, err: GoatError) -> GoatError {
        if matches!(
            &err,
            GoatError::Io { source, .. } if source.kind() == std::io::ErrorKind::NotFound
        ) || err.kind() == GoatErrorKind::NotFound
        {
            return GoatError::not_found("sstable", format!("missing during read: {:?}", path));
        }

        match err.kind() {
            GoatErrorKind::Corruption => {
                GoatError::corruption("sstable_read", format!("{:?}: {}", path, err))
            }
            _ => GoatError::internal_with_source(
                "sstable_read",
                format!("failed to read sstable: {:?}", path),
                err,
            ),
        }
    }

    /// 创建一个新的空 Version
    pub fn new(num_levels: usize, sstable_paths: Arc<SstablePaths>) -> Self {
        Self::new_with_cache(num_levels, sstable_paths, None)
    }

    /// 创建一个新的空 Version（可选读缓存）
    pub fn new_with_cache(
        num_levels: usize,
        _sstable_paths: Arc<SstablePaths>,
        table_cache: Option<Arc<TableCache>>,
    ) -> Self {
        Self {
            files: vec![Vec::new(); num_levels],
            level_size_bytes: vec![0; num_levels],
            creation_seqno: 0,
            table_cache,
        }
    }

    /// 从文件列表构建 Version
    pub fn from_files(
        files: Vec<Vec<Arc<FileMetadata>>>,
        creation_seqno: u64,
        sstable_paths: Arc<SstablePaths>,
    ) -> Self {
        Self::from_files_with_cache(files, creation_seqno, sstable_paths, None)
    }

    /// 从文件列表构建 Version（可选读缓存）
    pub fn from_files_with_cache(
        files: Vec<Vec<Arc<FileMetadata>>>,
        creation_seqno: u64,
        _sstable_paths: Arc<SstablePaths>,
        table_cache: Option<Arc<TableCache>>,
    ) -> Self {
        // 计算层级大小
        let level_size_bytes: Vec<u64> = files
            .iter()
            .map(|level_files| level_files.iter().map(|f| f.file_size()).sum())
            .collect();

        Self {
            files,
            level_size_bytes,
            creation_seqno,
            table_cache,
        }
    }

    fn open_sstable_reader(&self, file_id: u64, path: &Path) -> GoatResult<Arc<SSTableReader>> {
        if let Some(cache) = self.table_cache.as_ref() {
            cache.get_or_open(file_id, path)
        } else {
            Ok(Arc::new(SSTableReader::open(path)?))
        }
    }

    /// 查找包含指定 key 的 SSTable
    /// 返回 (level, file_meta) 如果找到
    pub fn get(&self, key: &[u8]) -> GoatResult<Option<(InternalKey, Vec<u8>)>> {
        self.get_pinned(key)
            .map(|result| result.map(|(internal_key, value)| (internal_key, value.to_vec())))
    }

    pub(crate) fn get_pinned(&self, key: &[u8]) -> GoatResult<Option<(InternalKey, PinnedValue)>> {
        if let Some(cached) = self.get_cached_row(key) {
            return Ok(cached);
        }

        let result = self.load_from_sstables_pinned(key)?;
        self.cache_row_result(key, &result);
        Ok(result)
    }

    fn load_from_sstables_pinned(
        &self,
        key: &[u8],
    ) -> GoatResult<Option<(InternalKey, PinnedValue)>> {
        // 先检查 Level 0
        // Level 0 文件可能重叠，必须从最新的 SSTable 开始查找，避免返回旧值
        for file in self.files[0].iter().rev() {
            // Level 0 的文件可能重叠，需要检查所有文件
            // smallest_key和largest_key都存在的是internal_key不能直接用于比较，需要转换为user_key进行比较
            if key >= file.smallest_user_key() && key <= file.largest_user_key() {
                // key在文件范围中，说明该文件中可能包含key
                let sstable_path = file.sstable_path();
                let reader = self
                    .open_sstable_reader(file.file_id, sstable_path)
                    .map_err(|e| Self::map_sstable_open_err(sstable_path, e))?;
                match reader
                    .get_pinned(key)
                    .map_err(|e| Self::map_sstable_read_err(sstable_path, e))?
                {
                    Some(result) => return Ok(Some(result)),
                    None => continue, // Not found in this sstable, check next
                };
            }
        }

        // 对于其他层级，由于文件不重叠且有序，可以使用二分查找
        for level in 1..self.files.len() {
            if let Some(file) = self.search_level(level, key) {
                let sstable_path = file.sstable_path();
                let reader = self
                    .open_sstable_reader(file.file_id, sstable_path)
                    .map_err(|e| Self::map_sstable_open_err(sstable_path, e))?;
                return reader
                    .get_pinned(key)
                    .map_err(|e| Self::map_sstable_read_err(sstable_path, e));
            }
        }

        Ok(None)
    }

    fn get_cached_row(&self, key: &[u8]) -> Option<Option<(InternalKey, PinnedValue)>> {
        let cache = self.table_cache.as_ref()?;
        let cached = cache.row_cache_get(self.creation_seqno, key)?;
        match cached {
            RowCacheValue::Hit {
                internal_key,
                value,
            } => Some(Some((internal_key, PinnedValue::from_bytes(value)))),
            RowCacheValue::Miss => Some(None),
        }
    }

    fn cache_row_result(&self, key: &[u8], result: &Option<(InternalKey, PinnedValue)>) {
        let Some(cache) = self.table_cache.as_ref() else {
            return;
        };
        let cached = match result {
            Some((internal_key, value)) => RowCacheValue::Hit {
                internal_key: internal_key.clone(),
                value: bytes::Bytes::copy_from_slice(value.as_slice()),
            },
            None => RowCacheValue::Miss,
        };
        cache.row_cache_insert(self.creation_seqno, key, cached);
    }

    /// 在指定层级（非 Level 0）中查找包含 key 的文件
    /// 使用二分查找，因为该层级的文件按键有序且不重叠
    fn search_level(&self, level: usize, key: &[u8]) -> Option<Arc<FileMetadata>> {
        let files = &self.files[level];
        if files.is_empty() {
            return None;
        }

        // 二分查找
        let mut left = 0;
        let mut right = files.len();

        while left < right {
            let mid = left + (right - left) / 2;
            let file = &files[mid];

            if key < file.smallest_user_key() {
                right = mid;
            } else if key > file.largest_user_key() {
                left = mid + 1;
            } else {
                // key 在这个文件的范围内
                return Some(Arc::clone(file));
            }
        }

        None
    }

    /// 获取指定层级的所有文件
    pub fn get_files(&self, level: usize) -> &[Arc<FileMetadata>] {
        if level >= self.files.len() {
            &[]
        } else {
            &self.files[level]
        }
    }

    /// 获取所有层级
    pub fn num_levels(&self) -> usize {
        self.files.len()
    }

    /// 计算层级总大小
    pub fn get_level_size(&self, level: usize) -> u64 {
        if level >= self.level_size_bytes.len() {
            0
        } else {
            self.level_size_bytes[level]
        }
    }

    /// 获取版本创建时的序列号
    pub fn creation_seqno(&self) -> u64 {
        self.creation_seqno
    }

    /// 获取所有文件（用于遍历）
    pub fn all_files(&self) -> impl Iterator<Item = (usize, Arc<FileMetadata>)> + '_ {
        self.files
            .iter()
            .enumerate()
            .flat_map(|(level, files)| files.iter().map(move |file| (level, Arc::clone(file))))
    }

    /// 复制所有层级的文件列表（仅克隆 Arc，不复制实际数据）
    pub fn clone_files(&self) -> Vec<Vec<Arc<FileMetadata>>> {
        self.files.clone()
    }

    /// 获取所有文件 ID（用于引用计数）
    pub fn all_file_ids(&self) -> HashSet<u64> {
        self.files
            .iter()
            .flat_map(|files| files.iter().map(|f| f.file_id))
            .collect()
    }

    pub fn read_cache_metrics(&self) -> Option<ReadCacheMetrics> {
        self.table_cache.as_ref().map(|cache| cache.metrics())
    }

    /// 检查是否需要压缩
    /// 简单实现：如果 Level 0 文件数超过 4，或者其他层超过目标大小
    pub fn needs_compaction(&self, level_targets: &[u64]) -> bool {
        // Level 0: 检查文件数量
        if self.files[0].len() > 4 {
            return true;
        }

        // 其他层级: 检查大小
        for (level, &target_size) in level_targets.iter().enumerate().skip(1) {
            if level < self.level_size_bytes.len() && self.level_size_bytes[level] > target_size {
                return true;
            }
        }

        false
    }

    /// 获取重叠的文件（用于压缩）
    /// 返回与给定 key 范围重叠的所有文件
    pub fn get_overlapping_files(
        &self,
        level: usize,
        smallest_key: &[u8],
        largest_key: &[u8],
    ) -> Vec<Arc<FileMetadata>> {
        if level >= self.files.len() {
            return Vec::new();
        }

        let mut overlapping = Vec::new();

        if level == 0 {
            // Level 0: 需要检查所有文件
            for file in &self.files[level] {
                if file.smallest_key() <= largest_key && file.largest_key() >= smallest_key {
                    overlapping.push(Arc::clone(file));
                }
            }
        } else {
            // 其他层级: 文件有序且不重叠，可以高效查找
            let files = &self.files[level];
            for file in files {
                // 文件在范围左侧
                if file.largest_key() < smallest_key {
                    continue;
                }
                // 文件在范围右侧
                if file.smallest_key() > largest_key {
                    break;
                }
                // 文件与范围重叠
                overlapping.push(Arc::clone(file));
            }
        }

        overlapping
    }
}
