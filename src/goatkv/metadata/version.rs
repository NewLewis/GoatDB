use std::collections::HashSet;
use std::sync::Arc;

use crate::goatkv::encoding::internal_key::InternalKey;
use crate::goatkv::metadata::file_metadata::FileMetadata;
use crate::goatkv::storage::sstable::SSTableReader;
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
    /// 路径管理器（用于定位 SSTable）
    sstable_paths: Arc<SstablePaths>,
}

impl Version {
    /// 创建一个新的空 Version
    pub fn new(num_levels: usize, sstable_paths: Arc<SstablePaths>) -> Self {
        Self {
            files: vec![Vec::new(); num_levels],
            level_size_bytes: vec![0; num_levels],
            creation_seqno: 0,
            sstable_paths,
        }
    }

    /// 从文件列表构建 Version
    pub fn from_files(
        files: Vec<Vec<Arc<FileMetadata>>>,
        creation_seqno: u64,
        sstable_paths: Arc<SstablePaths>,
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
            sstable_paths,
        }
    }

    /// 查找包含指定 key 的 SSTable
    /// 返回 (level, file_meta) 如果找到
    // todo table cache
    pub fn get(&self, key: &[u8]) -> Option<(InternalKey, Vec<u8>)> {
        // 先检查 Level 0
        // todo level 0的遍历顺序问题
        for file in &self.files[0] {
            // Level 0 的文件可能重叠，需要检查所有文件
            // smallest_key和largest_key都存在的是internal_key不能直接用于比较，需要转换为user_key进行比较
            if key >= file.smallest_user_key() && key <= file.largest_user_key() {
                // key在文件范围中，说明该文件中可能包含key
                let sstable_path = self.sstable_paths.sstable_path_by_id(file.file_id);
                match SSTableReader::open(&sstable_path) {
                    Ok(mut reader) => {
                        match reader.get(key) {
                            Ok(Some(result)) => return Some(result),
                            Ok(None) => continue, // Not found in this sstable, check next
                            Err(e) => {
                                println!("Failed to read from sstable {:?}: {}", sstable_path, e);
                                return None;
                            }
                        }
                    }
                    Err(e) => {
                        println!("Failed to open sstable {:?}: {}", sstable_path, e);
                        return None;
                    }
                }
            }
        }

        // 对于其他层级，由于文件不重叠且有序，可以使用二分查找
        for level in 1..self.files.len() {
            if let Some(file) = self.search_level(level, key) {
                let sstable_path = self.sstable_paths.sstable_path_by_id(file.file_id);
                match SSTableReader::open(&sstable_path) {
                    Ok(mut reader) => {
                        match reader.get(key) {
                            Ok(Some(result)) => return Some(result),
                            Ok(None) => return None, // Not found in this sstable, check next
                            Err(e) => {
                                println!("Failed to read from sstable {:?}: {}", sstable_path, e);
                                return None;
                            }
                        }
                    }
                    Err(e) => {
                        println!("Failed to open sstable {:?}: {}", sstable_path, e);
                        return None;
                    }
                }
            }
        }

        None
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
