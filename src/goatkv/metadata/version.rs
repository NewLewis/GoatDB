use std::collections::HashSet;
use std::sync::Arc;

use crate::goatkv::encoding::internal_key::InternalKey;
use crate::goatkv::metadata::version_edit::FileMetaData;
use crate::goatkv::storage::sstable_reader::SSTableReader;
use crate::goatkv::utils::db_path_manager::DbPathManager;

/// Version 代表某一时刻数据库的完整状态
/// 一旦创建后不可修改，支持并发无锁读取
#[derive(Debug, Clone)]
pub struct Version {
    /// 每层包含的 SSTable 文件元数据
    /// files[level] = Vec<Arc<FileMetaData>>
    files: Vec<Vec<Arc<FileMetaData>>>,

    /// 每层的总大小（用于触发压缩）
    level_size_bytes: Vec<u64>,

    /// 该版本创建时的序列号
    creation_seqno: u64,
}

impl Version {
    /// 创建一个新的空 Version
    pub fn new(num_levels: usize) -> Self {
        Self {
            files: vec![Vec::new(); num_levels],
            level_size_bytes: vec![0; num_levels],
            creation_seqno: 0,
        }
    }

    /// 从文件列表构建 Version
    pub fn from_files(files: Vec<Vec<Arc<FileMetaData>>>, creation_seqno: u64) -> Self {
        // 计算层级大小
        let level_size_bytes: Vec<u64> = files
            .iter()
            .map(|level_files| level_files.iter().map(|f| f.file_size).sum())
            .collect();

        Self {
            files,
            level_size_bytes,
            creation_seqno,
        }
    }

    /// 查找包含指定 key 的 SSTable
    /// 返回 (level, file_meta) 如果找到
    pub fn get(&self, key: &[u8]) -> Option<(InternalKey, Vec<u8>)> {
        // 先检查 Level 0
        // todo level 0的遍历顺序问题
        for file in &self.files[0] {
            // Level 0 的文件可能重叠，需要检查所有文件
            // todo key的比较，以及smallest_key存的是什么？
            if key >= file.smallest_key.as_slice() && key <= file.largest_key.as_slice() {
                // key在文件范围中，说明该文件中可能包含key
                let sstable_path = DbPathManager::global().sstable_path_by_id(file.file_id);
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
                let sstable_path = DbPathManager::global().sstable_path_by_id(file.file_id);
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
    fn search_level(&self, level: usize, key: &[u8]) -> Option<Arc<FileMetaData>> {
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

            if key < file.smallest_key.as_slice() {
                right = mid;
            } else if key > file.largest_key.as_slice() {
                left = mid + 1;
            } else {
                // key 在这个文件的范围内
                return Some(Arc::clone(file));
            }
        }

        None
    }

    /// 获取指定层级的所有文件
    pub fn get_files(&self, level: usize) -> &[Arc<FileMetaData>] {
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
    pub fn all_files(&self) -> impl Iterator<Item = (usize, Arc<FileMetaData>)> + '_ {
        self.files
            .iter()
            .enumerate()
            .flat_map(|(level, files)| files.iter().map(move |file| (level, Arc::clone(file))))
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
    ) -> Vec<Arc<FileMetaData>> {
        if level >= self.files.len() {
            return Vec::new();
        }

        let mut overlapping = Vec::new();

        if level == 0 {
            // Level 0: 需要检查所有文件
            for file in &self.files[level] {
                if file.smallest_key.as_slice() <= largest_key
                    && file.largest_key.as_slice() >= smallest_key
                {
                    overlapping.push(Arc::clone(file));
                }
            }
        } else {
            // 其他层级: 文件有序且不重叠，可以高效查找
            let files = &self.files[level];
            for file in files {
                // 文件在范围左侧
                if file.largest_key.as_slice() < smallest_key {
                    continue;
                }
                // 文件在范围右侧
                if file.smallest_key.as_slice() > largest_key {
                    break;
                }
                // 文件与范围重叠
                overlapping.push(Arc::clone(file));
            }
        }

        overlapping
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_file_meta(
        file_id: u64,
        smallest_key: &[u8],
        largest_key: &[u8],
        size: u64,
    ) -> Arc<FileMetaData> {
        Arc::new(FileMetaData {
            file_id,
            file_size: size,
            smallest_key: smallest_key.to_vec(),
            largest_key: largest_key.to_vec(),
            smallest_seqno: 0,
            largest_seqno: 0,
        })
    }

    #[test]
    fn test_version_new() {
        let version = Version::new(7);
        assert_eq!(version.num_levels(), 7);
        assert_eq!(version.get_files(0).len(), 0);
        assert_eq!(version.get_level_size(0), 0);
    }

    #[test]
    fn test_version_from_files() {
        let files = vec![
            vec![make_file_meta(1, b"a", b"z", 1000)],
            vec![
                make_file_meta(2, b"a", b"m", 500),
                make_file_meta(3, b"n", b"z", 500),
            ],
        ];

        let version = Version::from_files(files, 100);

        assert_eq!(version.num_levels(), 2);
        assert_eq!(version.get_files(0).len(), 1);
        assert_eq!(version.get_files(1).len(), 2);
        assert_eq!(version.get_level_size(0), 1000);
        assert_eq!(version.get_level_size(1), 1000);
        assert_eq!(version.creation_seqno(), 100);
    }

    #[test]
    fn test_search_level() {
        let files = vec![
            vec![],
            vec![
                make_file_meta(1, b"a", b"f", 100),
                make_file_meta(2, b"g", b"m", 100),
                make_file_meta(3, b"n", b"z", 100),
            ],
        ];

        let version = Version::from_files(files, 0);

        // 查找存在的 key
        assert!(version.search_level(1, b"b").is_some());
        assert!(version.search_level(1, b"h").is_some());
        assert!(version.search_level(1, b"p").is_some());

        // 查找边界
        assert!(version.search_level(1, b"a").is_some());
        assert!(version.search_level(1, b"z").is_some());

        // 查找不存在的 key
        assert!(version.search_level(1, b"0").is_none());
        assert!(version.search_level(1, b"zz").is_none());
    }

    #[test]
    fn test_get_overlapping_files() {
        let files = vec![
            vec![
                make_file_meta(1, b"a", b"c", 100),
                make_file_meta(2, b"b", b"d", 100),
                make_file_meta(3, b"e", b"g", 100),
            ],
            vec![
                make_file_meta(4, b"a", b"f", 200),
                make_file_meta(5, b"g", b"z", 200),
            ],
        ];

        let version = Version::from_files(files, 0);

        // Level 0: 多个文件重叠
        let overlapping = version.get_overlapping_files(0, b"b", b"c");
        assert_eq!(overlapping.len(), 2); // 文件 1 和 2

        // Level 1: 文件不重叠
        let overlapping = version.get_overlapping_files(1, b"c", b"e");
        assert_eq!(overlapping.len(), 1); // 只有文件 4
    }

    #[test]
    fn test_needs_compaction() {
        let mut files = vec![Vec::new(); 7];

        // Level 0: 添加 5 个文件
        for i in 1..=5 {
            files[0].push(make_file_meta(i, b"a", b"z", 100));
        }

        let version = Version::from_files(files.clone(), 0);

        let level_targets = [0, 64 * 1024 * 1024, 512 * 1024 * 1024, 0, 0, 0, 0];
        assert!(version.needs_compaction(&level_targets));
    }

    #[test]
    fn test_all_file_ids() {
        let files = vec![
            vec![make_file_meta(1, b"a", b"z", 100)],
            vec![
                make_file_meta(2, b"a", b"m", 100),
                make_file_meta(3, b"n", b"z", 100),
            ],
        ];

        let version = Version::from_files(files, 0);

        let ids = version.all_file_ids();
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
        assert!(ids.contains(&3));
    }
}
