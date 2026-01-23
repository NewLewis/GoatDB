use std::fs::{File, OpenOptions};
use std::io;
use std::path::PathBuf;

use crate::goatkv::encoding::internal_key::InternalKey;

use super::error::{WalError, WalResult};
use super::format::{read_record, PartialStage, RecordRead};

/// WAL reader that yields validated entries.
#[derive(Debug)]
pub struct WalReader {
    reader: io::BufReader<File>,
}

impl WalReader {
    pub fn new(file_path: &PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(file_path)?;
        let reader = io::BufReader::new(file);
        Ok(Self { reader })
    }
}

impl Iterator for WalReader {
    type Item = WalResult<(InternalKey, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match read_record(&mut self.reader) {
            Ok(RecordRead::Eof) => None,
            Ok(RecordRead::Partial(PartialStage::Checksum)) => None,
            Ok(RecordRead::Partial(PartialStage::Body)) => Some(Err(WalError::UnexpectedEof)),
            Ok(RecordRead::InvalidKeyLen) => Some(Err(WalError::InvalidKeyLen)),
            Ok(RecordRead::Record(record)) => {
                if !record.checksum_matches() {
                    return Some(Err(WalError::ChecksumMismatch {
                        key: record.key.user_key().to_vec(),
                    }));
                }
                Some(Ok(record.into_parts()))
            }
            Err(e) => Some(Err(WalError::Io(e))),
        }
    }
}
