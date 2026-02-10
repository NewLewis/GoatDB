use std::fs::{File, OpenOptions};
use std::io;
use std::path::PathBuf;

use crate::goatkv::error::{Error as GoatError, Result as GoatResult};
use crate::goatkv::format::internal_key::InternalKey;

use super::error::WalError;
use super::format::{read_record, PartialStage, RecordRead};

/// WAL reader that yields validated entries.
#[derive(Debug)]
pub struct WalReader {
    reader: io::BufReader<File>,
}

impl WalReader {
    pub fn new(file_path: &PathBuf) -> GoatResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .open(file_path)
            .map_err(|e| GoatError::io("wal_reader_open", e))?;
        let reader = io::BufReader::new(file);
        Ok(Self { reader })
    }
}

impl Iterator for WalReader {
    type Item = GoatResult<(InternalKey, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match read_record(&mut self.reader) {
            Ok(RecordRead::Eof) => None,
            Ok(RecordRead::Partial(PartialStage::Checksum)) => None,
            Ok(RecordRead::Partial(PartialStage::Body)) => {
                Some(Err(GoatError::from(WalError::UnexpectedEof)))
            }
            Ok(RecordRead::InvalidKeyLen) => Some(Err(GoatError::from(WalError::InvalidKeyLen))),
            Ok(RecordRead::Record(record)) => {
                if !record.checksum_matches() {
                    return Some(Err(GoatError::from(WalError::ChecksumMismatch {
                        key: record.key.user_key().to_vec(),
                    })));
                }
                Some(Ok(record.into_parts()))
            }
            Err(e) => Some(Err(e)),
        }
    }
}
