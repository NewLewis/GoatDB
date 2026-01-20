use std::sync::mpsc::Sender;

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
    pub obsolete_sender: Sender<u64>,
}

impl FileMetadata {
    pub fn new(file_id: u64, props: TableProperties, obsolete_sender: Sender<u64>) -> Self {
        FileMetadata {
            file_id,
            props,
            obsolete_sender,
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
}
