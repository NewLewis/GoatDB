use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use crate::goatkv::metadata::current;
use crate::goatkv::metadata::version_edit::VersionEdit;

pub const INIT_MANIFEST_FILE_NAME: &str = "MANIFEST-0";

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
        let file = OpenOptions::new().create(true).append(true).open(path)?;

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

#[derive(Debug)]
pub struct ManifestHandler {
    writer: Option<ManifestWriter>,
    file_number: u64,
}

impl ManifestHandler {
    // 1. 恢复流程：读取 CURRENT，找到并重放 Manifest
    pub fn recover(db_path: &Path) -> Result<Vec<VersionEdit>, std::io::Error> {
        // Step 1: Read CURRENT file content (e.g., "MANIFEST-00005\n")
        match current::read_current() {
            Ok(Some(file_name)) => {
                // Step 2: Open that MANIFEST file
                let manifest_path = db_path.join(&file_name);
                let mut reader = ManifestReader::new(&manifest_path)?;
                let mut edits = Vec::new();

                // Step 3: Replay all edits
                loop {
                    if let Ok(Some(edit)) = reader.read_next_edit() {
                        edits.push(edit);
                    } else {
                        break;
                    }
                }

                // Step 4: Return (last_sequence, edits)
                return Ok(edits);
            }
            Ok(None) => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    }

    // 2. 写入新状态：LogAndApply 的核心部分
    pub fn add_record(&mut self, edit: &VersionEdit) -> Result<(), std::io::Error> {
        // Step 1: 将 edit 序列化并 append 到当前的 MANIFEST 文件
        self.writer.as_mut().unwrap().append_edit(edit)?;

        // Step 2 (Optional but recommended):
        // 如果 Manifest 太大了，生成一个新的 MANIFEST 文件，
        // 并原子更新 CURRENT 文件指向它。
        // todo
        Ok(())
    }

    // 3. 原子更新 CURRENT 文件 (关键!)
    pub fn update_current_file(&self, manifest_file_number: u64) -> Result<()> {
        // 技巧：先写到临时文件 CURRENT.tmp，然后 rename 为 CURRENT
        // 保证 crash safe
        let tmp_path = self.db_path.join("CURRENT.tmp");
        let content = format!("MANIFEST-{:06}\n", manifest_file_number);
        std::fs::write(&tmp_path, content)?;
        std::fs::rename(&tmp_path, self.db_path.join("CURRENT"))?;
        Ok(())
    }
}

/// MANIFEST 文件读取器（用于恢复）
pub struct ManifestReader {
    reader: BufReader<File>,
}

impl ManifestReader {
    /// 打开 MANIFEST 文件读取
    pub fn new(path: &Path) -> Result<Self, std::io::Error> {
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
