use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

use crate::goatkv::error::{Error as GoatError, Result as GoatResult};
use crate::goatkv::metadata::version_edit::VersionEdit;
use crate::goatkv::utils::io_helpers::{read_exact_or_eof, ReadOutcome};

pub const INIT_MANIFEST_FILE_NAME: &str = "MANIFEST-0";

/// MANIFEST 文件写入器
#[derive(Debug)]
pub struct ManifestWriter {
    writer: BufWriter<File>,
    file_number: u64,
    current_size: u64,
}

impl ManifestWriter {
    pub fn create(path: &Path) -> GoatResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(|e| GoatError::io("manifest_create", e))?;

        let writer = BufWriter::new(file);
        let current_size = 0u64;

        Ok(ManifestWriter {
            writer,
            file_number: 0,
            current_size,
        })
    }

    pub fn open_for_append(path: &Path, file_number: u64) -> GoatResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| GoatError::io("manifest_open_for_append", e))?;

        let metadata = file
            .metadata()
            .map_err(|e| GoatError::io("manifest_read_metadata", e))?;
        let current_size = metadata.len();

        let writer = BufWriter::new(file);

        Ok(ManifestWriter {
            writer,
            file_number,
            current_size,
        })
    }

    pub fn append_edit(&mut self, edit: &VersionEdit) -> GoatResult<()> {
        let encoded = edit.encode();
        let len = encoded.len() as u64;

        self.writer
            .write_all(&len.to_be_bytes())
            .map_err(|e| GoatError::io("manifest_write_len", e))?;
        self.writer
            .write_all(&encoded)
            .map_err(|e| GoatError::io("manifest_write_edit", e))?;
        self.writer
            .flush()
            .map_err(|e| GoatError::io("manifest_flush", e))?;

        self.current_size += 8 + len;

        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.current_size
    }

    pub fn file_number(&self) -> u64 {
        self.file_number
    }

    pub fn sync(&mut self) -> GoatResult<()> {
        self.writer
            .flush()
            .map_err(|e| GoatError::io("manifest_sync_flush", e))?;
        self.writer
            .get_ref()
            .sync_all()
            .map_err(|e| GoatError::io("manifest_sync_all", e))?;
        Ok(())
    }
}

/// MANIFEST 文件读取器（用于恢复）
pub struct ManifestReader {
    reader: BufReader<File>,
    last_good_offset: u64,
}

impl ManifestReader {
    /// 打开 MANIFEST 文件读取
    pub fn new(path: &Path) -> GoatResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| GoatError::io("manifest_open_reader", e))?;
        Ok(ManifestReader {
            reader: BufReader::new(file),
            last_good_offset: 0,
        })
    }

    /// 读取所有 VersionEdit
    pub fn read_all_edits(&mut self) -> GoatResult<Vec<VersionEdit>> {
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
    fn read_next_edit(&mut self) -> GoatResult<Option<VersionEdit>> {
        let record_start = self.last_good_offset;

        // 读取长度前缀
        let mut len_bytes = [0u8; 8];
        match read_exact_or_eof(&mut self.reader, &mut len_bytes)? {
            ReadOutcome::Eof => return Ok(None),
            ReadOutcome::Partial => {
                self.truncate_to(record_start)?;
                return Ok(None);
            }
            ReadOutcome::Complete => {}
        }

        let len = u64::from_be_bytes(len_bytes) as usize;

        // 读取编码数据
        let mut buffer = vec![0u8; len];
        match read_exact_or_eof(&mut self.reader, &mut buffer)? {
            ReadOutcome::Eof | ReadOutcome::Partial => {
                self.truncate_to(record_start)?;
                return Ok(None);
            }
            ReadOutcome::Complete => {}
        }

        // 解码
        let edit = VersionEdit::decode(&buffer)?;
        self.last_good_offset = record_start + 8 + len as u64;
        Ok(Some(edit))
    }

    fn truncate_to(&mut self, offset: u64) -> GoatResult<()> {
        let file = self.reader.get_ref();
        file.set_len(offset)
            .map_err(|e| GoatError::io("manifest_truncate", e))?;
        file.sync_data()
            .map_err(|e| GoatError::io("manifest_truncate_sync", e))?;
        Ok(())
    }
}
