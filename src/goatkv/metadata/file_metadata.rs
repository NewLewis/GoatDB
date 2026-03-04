use std::path::{Path, PathBuf};

use crate::goatkv::metadata::version_edit::NewFile;
use crate::goatkv::utils::cleanup_task::CleanupTask;
use crate::goatkv::utils::paths::SstablePaths;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Clone)]
pub struct TableProperties {
    pub file_size: u64,
    // 最小 Key 和 最大 Key 用于读路径上的快速过滤
    // 必须存储为 Vec<u8> 因为 Key 是二进制安全的
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,

    // 可选：记录该文件的生成序列号范围，用于 MVCC
    pub smallest_seqno: u64,
    pub largest_seqno: u64,
}

impl TableProperties {
    pub fn new(
        file_size: u64,
        smallest_key: Vec<u8>,
        largest_key: Vec<u8>,
        smallest_seqno: u64,
        largest_seqno: u64,
    ) -> Self {
        TableProperties {
            file_size,
            smallest_key,
            largest_key,
            smallest_seqno,
            largest_seqno,
        }
    }
}

/// FileMetadata 描述了一个具体的 SSTable 文件的元数据
#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub file_id: u64,
    pub props: TableProperties,
    sstable_path: PathBuf,
    pub obsolete_sender: UnboundedSender<CleanupTask>,
}

impl FileMetadata {
    pub fn from_props(
        file_id: u64,
        props: TableProperties,
        obsolete_sender: UnboundedSender<CleanupTask>,
    ) -> Self {
        Self::from_props_with_paths(file_id, props, obsolete_sender, None)
    }

    pub fn from_props_with_sstable_paths(
        file_id: u64,
        props: TableProperties,
        obsolete_sender: UnboundedSender<CleanupTask>,
        sstable_paths: &SstablePaths,
    ) -> Self {
        Self::from_props_with_paths(file_id, props, obsolete_sender, Some(sstable_paths))
    }

    fn from_props_with_paths(
        file_id: u64,
        props: TableProperties,
        obsolete_sender: UnboundedSender<CleanupTask>,
        sstable_paths: Option<&SstablePaths>,
    ) -> Self {
        FileMetadata {
            file_id,
            props,
            sstable_path: Self::build_sstable_path(file_id, sstable_paths),
            obsolete_sender,
        }
    }

    pub fn from_new_file(new_file: NewFile, obsolete_sender: UnboundedSender<CleanupTask>) -> Self {
        Self::from_new_file_with_paths(new_file, obsolete_sender, None)
    }

    pub fn from_new_file_with_sstable_paths(
        new_file: NewFile,
        obsolete_sender: UnboundedSender<CleanupTask>,
        sstable_paths: &SstablePaths,
    ) -> Self {
        Self::from_new_file_with_paths(new_file, obsolete_sender, Some(sstable_paths))
    }

    fn from_new_file_with_paths(
        new_file: NewFile,
        obsolete_sender: UnboundedSender<CleanupTask>,
        sstable_paths: Option<&SstablePaths>,
    ) -> Self {
        let file_id = new_file.file_id;
        FileMetadata {
            file_id,
            props: new_file.props,
            sstable_path: Self::build_sstable_path(file_id, sstable_paths),
            obsolete_sender,
        }
    }

    fn build_sstable_path(file_id: u64, sstable_paths: Option<&SstablePaths>) -> PathBuf {
        if let Some(paths) = sstable_paths {
            paths.sstable_path_by_id(file_id)
        } else {
            PathBuf::from(SstablePaths::format_sstable_filename(file_id))
        }
    }

    pub fn smallest_user_key(&self) -> &[u8] {
        assert!(self.props.smallest_key.len() >= 8);
        &self.props.smallest_key[..self.props.smallest_key.len() - 8]
    }

    pub fn largest_user_key(&self) -> &[u8] {
        assert!(self.props.largest_key.len() >= 8);
        &self.props.largest_key[..self.props.largest_key.len() - 8]
    }

    pub fn file_size(&self) -> u64 {
        self.props.file_size
    }

    pub fn smallest_key(&self) -> &[u8] {
        &self.props.smallest_key
    }

    pub fn largest_key(&self) -> &[u8] {
        &self.props.largest_key
    }

    pub fn sstable_path(&self) -> &Path {
        &self.sstable_path
    }
}

impl From<FileMetadata> for NewFile {
    fn from(metadata: FileMetadata) -> Self {
        NewFile {
            file_id: metadata.file_id,
            props: metadata.props,
        }
    }
}
