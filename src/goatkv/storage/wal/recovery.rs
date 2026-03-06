use std::fs::{File, OpenOptions};
use std::io;
use std::path::PathBuf;

use crate::goatkv::error::{Error as GoatError, Result as GoatResult};
use crate::goatkv::format::internal_key::{InternalKey, InternalKeyKind};

use super::codec::WalCodec;
use super::format::{read_record, RecordRead};

#[derive(Debug, Clone, Copy)]
pub struct WalReplayStats {
    pub max_sequence: u64,
    pub entries: u64,
    pub truncated: bool,
}

#[derive(Debug)]
struct PendingBatch {
    start_offset: u64,
    start_sequence: u64,
    expected_ops: usize,
    entries: Vec<(InternalKey, Vec<u8>)>,
}

pub fn replay_wal_file<F>(path: &PathBuf, mut on_entry: F) -> GoatResult<WalReplayStats>
where
    F: FnMut(InternalKey, Vec<u8>),
{
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| GoatError::io("wal_replay_open", e))?;
    let mut reader = io::BufReader::new(
        file.try_clone()
            .map_err(|e| GoatError::io("wal_replay_clone", e))?,
    );
    let mut last_good_offset = 0u64;
    let mut max_sequence = 0u64;
    let mut entries = 0u64;
    let mut truncated = false;
    let mut pending_batch: Option<PendingBatch> = None;

    loop {
        let record_start = last_good_offset;
        let record = match read_record(&mut reader)? {
            RecordRead::Eof => {
                if let Some(batch) = pending_batch.take() {
                    truncate_and_sync(
                        &file,
                        batch.start_offset,
                        "wal_replay_truncate_incomplete_batch_eof",
                        "wal_replay_sync_incomplete_batch_eof",
                    )?;
                    truncated = true;
                }
                break;
            }
            RecordRead::Partial(_) | RecordRead::InvalidKeyLen => {
                let truncate_offset = pending_batch
                    .as_ref()
                    .map(|batch| batch.start_offset)
                    .unwrap_or(record_start);
                truncate_and_sync(
                    &file,
                    truncate_offset,
                    "wal_replay_truncate_partial",
                    "wal_replay_sync_partial",
                )?;
                truncated = true;
                break;
            }
            RecordRead::Record(record) => record,
        };

        if !record.checksum_matches() {
            let truncate_offset = pending_batch
                .as_ref()
                .map(|batch| batch.start_offset)
                .unwrap_or(record_start);
            truncate_and_sync(
                &file,
                truncate_offset,
                "wal_replay_truncate_checksum",
                "wal_replay_sync_checksum",
            )?;
            truncated = true;
            break;
        }

        last_good_offset = record_start + record.total_len();
        let (key, value) = record.into_parts();
        match key.kind()? {
            InternalKeyKind::TxnBatchBegin => {
                if !key.user_key().is_empty() {
                    return Err(GoatError::corruption(
                        "wal_batch_begin_user_key",
                        "batch begin marker must use empty user key",
                    ));
                }
                if let Some(batch) = pending_batch.take() {
                    truncate_and_sync(
                        &file,
                        batch.start_offset,
                        "wal_replay_truncate_nested_batch",
                        "wal_replay_sync_nested_batch",
                    )?;
                    truncated = true;
                    break;
                }
                let expected_ops = WalCodec::decode_batch_begin_value(&value)?;
                if expected_ops == 0 {
                    return Err(GoatError::corruption(
                        "wal_batch_begin_op_count",
                        "batch begin marker must contain at least one operation",
                    ));
                }
                pending_batch = Some(PendingBatch {
                    start_offset: record_start,
                    start_sequence: key.sequence_number(),
                    expected_ops,
                    entries: Vec::with_capacity(expected_ops),
                });
            }
            InternalKeyKind::Put | InternalKeyKind::Delete => {
                if let Some(batch) = pending_batch.as_mut() {
                    let expected_seq = batch
                        .start_sequence
                        .checked_add(batch.entries.len() as u64)
                        .ok_or_else(|| {
                            GoatError::corruption(
                                "wal_batch_sequence_overflow",
                                "batch sequence overflow during replay",
                            )
                        })?;
                    if key.sequence_number() != expected_seq {
                        let truncate_offset = batch.start_offset;
                        truncate_and_sync(
                            &file,
                            truncate_offset,
                            "wal_replay_truncate_batch_sequence",
                            "wal_replay_sync_batch_sequence",
                        )?;
                        truncated = true;
                        break;
                    }
                    batch.entries.push((key, value));
                    if batch.entries.len() == batch.expected_ops {
                        let batch = pending_batch.take().unwrap();
                        for (entry_key, entry_value) in batch.entries {
                            max_sequence = max_sequence.max(entry_key.sequence_number());
                            entries += 1;
                            on_entry(entry_key, entry_value);
                        }
                    }
                } else {
                    max_sequence = max_sequence.max(key.sequence_number());
                    entries += 1;
                    on_entry(key, value);
                }
            }
        }
    }

    Ok(WalReplayStats {
        max_sequence,
        entries,
        truncated,
    })
}

fn truncate_and_sync(
    file: &File,
    offset: u64,
    truncate_op: &'static str,
    sync_op: &'static str,
) -> GoatResult<()> {
    file.set_len(offset)
        .map_err(|e| GoatError::io(truncate_op, e))?;
    file.sync_data().map_err(|e| GoatError::io(sync_op, e))?;
    Ok(())
}
