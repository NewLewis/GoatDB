use std::collections::HashSet;

use crate::goatkv::error::{Error as GoatError, Result as GoatResult};
use crate::goatkv::format::coding;
use crate::goatkv::metadata::file_metadata::TableProperties;

// 类型别名，增加代码可读性
type Level = usize;
type FileId = u64;

const TAG_COMPARATOR: u32 = 1;
const TAG_LOG_NUMBER: u32 = 2;
const TAG_NEXT_FILE_NUMBER: u32 = 3;
const TAG_LAST_SEQUENCE: u32 = 4;
const TAG_COMPACT_POINTER: u32 = 5;
const TAG_DELETED_FILE: u32 = 6;
const TAG_NEW_FILE: u32 = 7;
const TAG_NEW_FILE_V2: u32 = 8;
const TAG_FORMAT_VERSION: u32 = 9;

pub const MANIFEST_FORMAT_VERSION_LEGACY: u32 = 0;
pub const MANIFEST_FORMAT_VERSION_CURRENT: u32 = 1;

#[derive(Debug, Clone)]
pub struct NewFile {
    pub file_id: FileId,
    pub props: TableProperties,
}

impl NewFile {
    pub fn new(
        file_id: FileId,
        file_size: u64,
        smallest_key: Vec<u8>,
        largest_key: Vec<u8>,
        smallest_seqno: u64,
        largest_seqno: u64,
    ) -> Self {
        Self {
            file_id,
            props: TableProperties {
                file_size,
                smallest_key,
                largest_key,
                smallest_seqno,
                largest_seqno,
            },
        }
    }

    pub fn new_with_props(file_id: FileId, props: TableProperties) -> Self {
        NewFile { file_id, props }
    }

    pub fn file_id(&self) -> FileId {
        self.file_id
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

/// VersionEdit 记录了一次状态变更的增量
/// 它可以被序列化后追加写到 MANIFEST 文件中
#[derive(Debug, Default)]
pub struct VersionEdit {
    pub format_version: Option<u32>, // MANIFEST 记录格式版本
    // 1. 全局状态字段 (Option 表示该次 Edit 是否修改了这个值)
    pub comparator_name: Option<String>, // 用于检查打开 DB 时比较器是否匹配
    pub log_number: Option<u64>,         // 当前有效的 WAL 日志编号
    pub next_file_number: Option<u64>,   // 下一个可用的文件编号
    pub last_sequence: Option<u64>,      // 全局最大的 Sequence Number

    // 2. 压缩指针 (Compaction Pointer)
    // 记录每一层下一次 Compaction 应该从哪个 Key 开始
    // Vec<(Level, Key)>
    pub compact_pointers: Vec<(Level, Vec<u8>)>,

    // 3. 删除的文件
    // 记录 (Level, FileId)。为什么需要 Level？因为不同 Level 可能恰好有同名文件（虽然严格设计下很难，但带上 Level 更安全）
    pub deleted_files: HashSet<(Level, FileId)>,

    // 4. 新增的文件
    // 记录 (Level, FileMetadata)。Compaction 会产生新文件并放到特定 Level。
    pub new_files: Vec<(Level, NewFile)>,
}

impl VersionEdit {
    pub fn new() -> Self {
        Self::default()
    }

    // 设置 Log Number
    pub fn set_log_number(&mut self, num: u64) {
        self.log_number = Some(num);
    }

    pub fn set_format_version(&mut self, version: u32) {
        self.format_version = Some(version);
    }

    // 设置 Next File Number
    pub fn set_next_file_number(&mut self, num: u64) {
        self.next_file_number = Some(num);
    }

    // 设置 Last Sequence
    pub fn set_last_sequence(&mut self, seq: u64) {
        self.last_sequence = Some(seq);
    }

    // 设置 Comparator
    pub fn set_comparator_name(&mut self, name: String) {
        self.comparator_name = Some(name);
    }

    // 记录需要物理删除的文件
    pub fn delete_file(&mut self, level: Level, file_id: FileId) {
        self.deleted_files.insert((level, file_id));
    }

    // 更新某一层的 compaction pointer（记录下一次优先开始的 user key）
    pub fn set_compact_pointer(&mut self, level: Level, key: Vec<u8>) {
        self.compact_pointers.push((level, key));
    }

    // 记录新生成的文件
    // 注意：这里传入 FileMetadata，通常在 Compaction 完成后构建
    pub fn add_file(&mut self, level: Level, file: NewFile) {
        self.new_files.push((level, file));
    }

    // 序列化为字节流（用于写 MANIFEST）
    // 这里以 JSON 为例，实际高性能场景建议用 bincode
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // 0. Format Version
        if let Some(version) = self.format_version {
            coding::put_varint64(&mut buf, TAG_FORMAT_VERSION as u64);
            coding::put_varint64(&mut buf, version as u64);
        }

        // 1. Comparator
        if let Some(ref name) = self.comparator_name {
            coding::put_varint64(&mut buf, TAG_COMPARATOR as u64);
            coding::put_length_prefixed_slice(&mut buf, name.as_bytes());
        }

        // 2. Log Number
        if let Some(num) = self.log_number {
            coding::put_varint64(&mut buf, TAG_LOG_NUMBER as u64);
            coding::put_varint64(&mut buf, num);
        }

        // 3. Next File Number
        if let Some(num) = self.next_file_number {
            coding::put_varint64(&mut buf, TAG_NEXT_FILE_NUMBER as u64);
            coding::put_varint64(&mut buf, num);
        }

        // 4. Last Sequence
        if let Some(seq) = self.last_sequence {
            coding::put_varint64(&mut buf, TAG_LAST_SEQUENCE as u64);
            coding::put_varint64(&mut buf, seq);
        }

        // 5. Compact Pointers
        for (level, key) in &self.compact_pointers {
            coding::put_varint64(&mut buf, TAG_COMPACT_POINTER as u64);
            coding::put_varint64(&mut buf, *level as u64);
            coding::put_length_prefixed_slice(&mut buf, key);
        }

        // 6. Deleted Files
        for (level, file_id) in &self.deleted_files {
            coding::put_varint64(&mut buf, TAG_DELETED_FILE as u64);
            coding::put_varint64(&mut buf, *level as u64);
            coding::put_varint64(&mut buf, *file_id);
        }

        // 7. New Files (v2)
        // 格式:
        // Tag -> Level -> FileId -> FileSize -> SmallestKey -> LargestKey
        //     -> SmallestSeqno -> LargestSeqno
        for (level, meta) in &self.new_files {
            coding::put_varint64(&mut buf, TAG_NEW_FILE_V2 as u64);
            coding::put_varint64(&mut buf, *level as u64);
            coding::put_varint64(&mut buf, meta.file_id);
            coding::put_varint64(&mut buf, meta.file_size());
            coding::put_length_prefixed_slice(&mut buf, meta.smallest_key());
            coding::put_length_prefixed_slice(&mut buf, meta.largest_key());
            coding::put_varint64(&mut buf, meta.props.smallest_seqno);
            coding::put_varint64(&mut buf, meta.props.largest_seqno);
        }

        buf
    }

    // 反序列化
    pub fn decode(data: &[u8]) -> GoatResult<Self> {
        let mut edit = Self::default();
        let mut cursor = 0;

        while cursor < data.len() {
            // 读取标签
            let (tag, bytes_read) = coding::decode_varint64_with_length(&data[cursor..])?;
            cursor += bytes_read;

            match tag as u32 {
                TAG_FORMAT_VERSION => {
                    let (version, bytes_read) =
                        coding::decode_varint64_with_length(&data[cursor..])?;
                    cursor += bytes_read;
                    if version > u32::MAX as u64 {
                        return Err(GoatError::corruption(
                            "version_edit_decode",
                            format!("manifest format version overflow: {}", version),
                        ));
                    }
                    edit.format_version = Some(version as u32);
                }
                TAG_COMPARATOR => {
                    let (name_data, bytes_read) =
                        coding::get_length_prefixed_slice(&data[cursor..])?;
                    cursor += bytes_read;
                    edit.comparator_name = Some(String::from_utf8_lossy(name_data).to_string());
                }
                TAG_LOG_NUMBER => {
                    let (num, bytes_read) = coding::decode_varint64_with_length(&data[cursor..])?;
                    cursor += bytes_read;
                    edit.log_number = Some(num);
                }
                TAG_NEXT_FILE_NUMBER => {
                    let (num, bytes_read) = coding::decode_varint64_with_length(&data[cursor..])?;
                    cursor += bytes_read;
                    edit.next_file_number = Some(num);
                }
                TAG_LAST_SEQUENCE => {
                    let (seq, bytes_read) = coding::decode_varint64_with_length(&data[cursor..])?;
                    cursor += bytes_read;
                    edit.last_sequence = Some(seq);
                }
                TAG_COMPACT_POINTER => {
                    let (level, bytes_read) = coding::decode_varint64_with_length(&data[cursor..])?;
                    cursor += bytes_read;
                    let (key, bytes_read) = coding::get_length_prefixed_slice(&data[cursor..])?;
                    cursor += bytes_read;
                    edit.compact_pointers.push((level as usize, key.to_vec()));
                }
                TAG_DELETED_FILE => {
                    let (level, bytes_read) = coding::decode_varint64_with_length(&data[cursor..])?;
                    cursor += bytes_read;
                    let (file_id, bytes_read) =
                        coding::decode_varint64_with_length(&data[cursor..])?;
                    cursor += bytes_read;
                    edit.deleted_files.insert((level as usize, file_id));
                }
                TAG_NEW_FILE => {
                    // 兼容旧格式：不含 seqno，默认置 0。
                    let (level, bytes_read) = coding::decode_varint64_with_length(&data[cursor..])?;
                    cursor += bytes_read;
                    let (file_id, bytes_read) =
                        coding::decode_varint64_with_length(&data[cursor..])?;
                    cursor += bytes_read;
                    let (file_size, bytes_read) =
                        coding::decode_varint64_with_length(&data[cursor..])?;
                    cursor += bytes_read;
                    let (smallest_key, bytes_read) =
                        coding::get_length_prefixed_slice(&data[cursor..])?;
                    cursor += bytes_read;
                    let (largest_key, bytes_read) =
                        coding::get_length_prefixed_slice(&data[cursor..])?;
                    cursor += bytes_read;
                    edit.new_files.push((
                        level as usize,
                        NewFile::new(
                            file_id,
                            file_size,
                            smallest_key.to_vec(),
                            largest_key.to_vec(),
                            0,
                            0,
                        ),
                    ));
                }
                TAG_NEW_FILE_V2 => {
                    let (level, bytes_read) = coding::decode_varint64_with_length(&data[cursor..])?;
                    cursor += bytes_read;
                    let (file_id, bytes_read) =
                        coding::decode_varint64_with_length(&data[cursor..])?;
                    cursor += bytes_read;
                    let (file_size, bytes_read) =
                        coding::decode_varint64_with_length(&data[cursor..])?;
                    cursor += bytes_read;
                    let (smallest_key, bytes_read) =
                        coding::get_length_prefixed_slice(&data[cursor..])?;
                    cursor += bytes_read;
                    let (largest_key, bytes_read) =
                        coding::get_length_prefixed_slice(&data[cursor..])?;
                    cursor += bytes_read;
                    let (smallest_seqno, bytes_read) =
                        coding::decode_varint64_with_length(&data[cursor..])?;
                    cursor += bytes_read;
                    let (largest_seqno, bytes_read) =
                        coding::decode_varint64_with_length(&data[cursor..])?;
                    cursor += bytes_read;
                    edit.new_files.push((
                        level as usize,
                        NewFile::new(
                            file_id,
                            file_size,
                            smallest_key.to_vec(),
                            largest_key.to_vec(),
                            smallest_seqno,
                            largest_seqno,
                        ),
                    ));
                }
                _ => {
                    return Err(GoatError::corruption(
                        "version_edit_decode",
                        format!("unknown tag {}", tag),
                    ));
                }
            }
        }

        Ok(edit)
    }
}

#[cfg(test)]
mod tests {
    use super::{VersionEdit, MANIFEST_FORMAT_VERSION_CURRENT, MANIFEST_FORMAT_VERSION_LEGACY};

    #[test]
    fn encode_decode_preserves_manifest_format_version() {
        let mut edit = VersionEdit::new();
        edit.set_format_version(MANIFEST_FORMAT_VERSION_CURRENT);
        edit.set_log_number(7);

        let decoded = VersionEdit::decode(&edit.encode()).expect("decode version edit");
        assert_eq!(
            decoded.format_version,
            Some(MANIFEST_FORMAT_VERSION_CURRENT),
            "manifest format version should be preserved"
        );
        assert_eq!(decoded.log_number, Some(7));
    }

    #[test]
    fn decode_legacy_edit_without_format_version_is_compatible() {
        let mut edit = VersionEdit::new();
        edit.set_log_number(11);
        let decoded = VersionEdit::decode(&edit.encode()).expect("decode legacy version edit");
        assert_eq!(
            decoded
                .format_version
                .unwrap_or(MANIFEST_FORMAT_VERSION_LEGACY),
            MANIFEST_FORMAT_VERSION_LEGACY
        );
        assert_eq!(decoded.log_number, Some(11));
    }
}
