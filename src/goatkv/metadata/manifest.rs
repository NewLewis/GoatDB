use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use crate::goatkv::metadata::version_edit::VersionEdit;

/// MANIFEST 文件写入器
#[derive(Debug)]
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
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_manifest_write_and_read() {
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
    fn test_manifest_open_for_append() {
        let temp_dir = TempDir::new().unwrap();
        let manifest_path = temp_dir.path().join("MANIFEST-000002");

        // 初始写入
        {
            let mut writer = ManifestWriter::create(&manifest_path).unwrap();
            let mut edit = VersionEdit::new();
            edit.set_log_number(1);
            writer.append_edit(&edit).unwrap();
        }

        // 追加写入
        {
            let mut writer = ManifestWriter::open_for_append(&manifest_path, 2).unwrap();
            let initial_size = writer.size();
            assert_eq!(writer.file_number(), 2);

            let mut edit = VersionEdit::new();
            edit.set_log_number(2);
            writer.append_edit(&edit).unwrap();

            assert!(writer.size() > initial_size);
        }

        // 读取所有编辑
        {
            let mut reader = ManifestReader::open(&manifest_path).unwrap();
            let edits = reader.read_all_edits().unwrap();

            assert_eq!(edits.len(), 2);
            assert_eq!(edits[0].log_number, Some(1));
            assert_eq!(edits[1].log_number, Some(2));
        }
    }

    #[test]
    fn test_manifest_reader_empty_file() {
        let temp_dir = TempDir::new().unwrap();
        let manifest_path = temp_dir.path().join("MANIFEST-000003");

        // 创建空文件
        fs::File::create(&manifest_path).unwrap();

        // 读取应该返回空列表
        {
            let mut reader = ManifestReader::open(&manifest_path).unwrap();
            let edits = reader.read_all_edits().unwrap();

            assert_eq!(edits.len(), 0);
        }
    }
}
