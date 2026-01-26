use std::io::{self, Read};

use crc32fast::Hasher;

use crate::goatkv::format::internal_key::InternalKey;
use crate::goatkv::utils::io_helpers::{read_exact_or_eof, ReadOutcome};

/// Write-ahead log record format.
///
/// ```text
/// |                         Checksum (4 bytes)                       |
/// |                       u32, little-endian                         |
/// |               InternalKey Total Length (4 bytes)                 |
/// |                       u32, little-endian                         |
/// |                      User Key (variable)                         |
/// |              Encoded Sequence Number (8 bytes)                   |
/// |                       u64, little-endian                         |
/// |                      Value Length (4 bytes)                      |
/// |                       u32, little-endian                         |
/// |                      Value (variable)                            |
/// ```

#[derive(Debug)]
pub(crate) enum PartialStage {
    Checksum,
    Body,
}

#[derive(Debug)]
pub(crate) enum RecordRead {
    Eof,
    Partial(PartialStage),
    InvalidKeyLen,
    Record(WalRecord),
}

#[derive(Debug)]
pub(crate) struct WalRecord {
    pub(crate) checksum: u32,
    pub(crate) key_len: u32,
    pub(crate) key: InternalKey,
    pub(crate) value_len: u32,
    pub(crate) value: Vec<u8>,
}

impl WalRecord {
    pub(crate) fn checksum_matches(&self) -> bool {
        checksum_for(&self.key, self.key_len, &self.value, self.value_len) == self.checksum
    }

    pub(crate) fn total_len(&self) -> u64 {
        4 + 4 + self.key_len as u64 + 4 + self.value_len as u64
    }

    pub(crate) fn into_parts(self) -> (InternalKey, Vec<u8>) {
        (self.key, self.value)
    }
}

pub(crate) fn checksum_for(key: &InternalKey, key_len: u32, value: &[u8], value_len: u32) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(&key_len.to_le_bytes());
    hasher.update(key.user_key());
    hasher.update(&key.encoded_sequence_number().to_le_bytes());
    hasher.update(&value_len.to_le_bytes());
    hasher.update(value);
    hasher.finalize()
}

pub(crate) fn read_record<R: Read>(reader: &mut R) -> io::Result<RecordRead> {
    let mut checksum_bytes = [0u8; 4];
    match read_exact_or_eof(reader, &mut checksum_bytes)? {
        ReadOutcome::Eof => return Ok(RecordRead::Eof),
        ReadOutcome::Partial => return Ok(RecordRead::Partial(PartialStage::Checksum)),
        ReadOutcome::Complete => {}
    }
    let checksum = u32::from_le_bytes(checksum_bytes);

    let mut key_len_bytes = [0u8; 4];
    if read_exact_or_eof(reader, &mut key_len_bytes)? != ReadOutcome::Complete {
        return Ok(RecordRead::Partial(PartialStage::Body));
    }
    let key_len = u32::from_le_bytes(key_len_bytes);
    if key_len < 8 {
        return Ok(RecordRead::InvalidKeyLen);
    }

    let user_key_len = key_len as usize - 8;
    let mut user_key = vec![0u8; user_key_len];
    if read_exact_or_eof(reader, &mut user_key)? != ReadOutcome::Complete {
        return Ok(RecordRead::Partial(PartialStage::Body));
    }

    let mut encoded_seq_bytes = [0u8; 8];
    if read_exact_or_eof(reader, &mut encoded_seq_bytes)? != ReadOutcome::Complete {
        return Ok(RecordRead::Partial(PartialStage::Body));
    }
    let encoded_seq = u64::from_le_bytes(encoded_seq_bytes);
    let key = InternalKey::from_encoded(user_key, encoded_seq);

    let mut value_len_bytes = [0u8; 4];
    if read_exact_or_eof(reader, &mut value_len_bytes)? != ReadOutcome::Complete {
        return Ok(RecordRead::Partial(PartialStage::Body));
    }
    let value_len = u32::from_le_bytes(value_len_bytes);

    let mut value = vec![0u8; value_len as usize];
    if read_exact_or_eof(reader, &mut value)? != ReadOutcome::Complete {
        return Ok(RecordRead::Partial(PartialStage::Body));
    }

    Ok(RecordRead::Record(WalRecord {
        checksum,
        key_len,
        key,
        value_len,
        value,
    }))
}
