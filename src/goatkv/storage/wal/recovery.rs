use std::fs::OpenOptions;
use std::io;
use std::path::PathBuf;

use crate::goatkv::format::internal_key::InternalKey;

use super::format::{read_record, RecordRead};

#[derive(Debug, Clone, Copy)]
pub struct WalReplayStats {
    pub max_sequence: u64,
    pub entries: u64,
    pub truncated: bool,
}

pub fn replay_wal_file<F>(path: &PathBuf, mut on_entry: F) -> io::Result<WalReplayStats>
where
    F: FnMut(InternalKey, Vec<u8>),
{
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    let mut reader = io::BufReader::new(file.try_clone()?);
    let mut last_good_offset = 0u64;
    let mut max_sequence = 0u64;
    let mut entries = 0u64;
    let mut truncated = false;

    loop {
        let record_start = last_good_offset;
        let record = match read_record(&mut reader)? {
            RecordRead::Eof => break,
            RecordRead::Partial(_) | RecordRead::InvalidKeyLen => {
                file.set_len(record_start)?;
                file.sync_data()?;
                truncated = true;
                break;
            }
            RecordRead::Record(record) => record,
        };

        if !record.checksum_matches() {
            file.set_len(record_start)?;
            file.sync_data()?;
            truncated = true;
            break;
        }

        last_good_offset = record_start + record.total_len();
        max_sequence = max_sequence.max(record.key.sequence_number());
        entries += 1;
        on_entry(record.key, record.value);
    }

    Ok(WalReplayStats {
        max_sequence,
        entries,
        truncated,
    })
}
