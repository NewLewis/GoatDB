use bytes::Bytes;

use crate::goatkv::error::{Error as GoatError, Result as GoatResult};
use crate::goatkv::format::internal_key::{InternalKey, InternalKeyKind};

use super::format::checksum_for;

const BATCH_BEGIN_MAGIC: [u8; 4] = *b"GKTB";
const BATCH_BEGIN_VALUE_LEN: usize = 8;

/// WAL record encoder.
pub struct WalCodec;

impl WalCodec {
    pub fn record_size(key: &InternalKey, value: &[u8]) -> usize {
        12 + key.serialized_size() + value.len()
    }

    pub fn encode_record_into(buf: &mut Vec<u8>, key: &InternalKey, value: &[u8]) {
        let key_len = key.serialized_size() as u32;
        let value_len = value.len() as u32;
        let checksum = checksum_for(key, key_len, value, value_len);
        buf.extend_from_slice(&checksum.to_le_bytes());
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(key.user_key());
        buf.extend_from_slice(&key.encoded_sequence_number().to_le_bytes());
        buf.extend_from_slice(&value_len.to_le_bytes());
        buf.extend_from_slice(value);
    }

    pub fn encode_record(key: &InternalKey, value: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::record_size(key, value));
        Self::encode_record_into(&mut buf, key, value);
        buf
    }

    pub fn encode_batch_begin_value(op_count: usize) -> GoatResult<[u8; BATCH_BEGIN_VALUE_LEN]> {
        let op_count = u32::try_from(op_count).map_err(|_| {
            GoatError::invalid_argument(
                "wal_batch_begin_op_count",
                format!("op count {} exceeds u32", op_count),
            )
        })?;
        let mut value = [0u8; BATCH_BEGIN_VALUE_LEN];
        value[..4].copy_from_slice(&BATCH_BEGIN_MAGIC);
        value[4..].copy_from_slice(&op_count.to_le_bytes());
        Ok(value)
    }

    pub fn decode_batch_begin_value(value: &[u8]) -> GoatResult<usize> {
        if value.len() != BATCH_BEGIN_VALUE_LEN {
            return Err(GoatError::corruption(
                "wal_batch_begin_value_len",
                format!(
                    "invalid batch begin marker length {}, expected {}",
                    value.len(),
                    BATCH_BEGIN_VALUE_LEN
                ),
            ));
        }
        if value[..4] != BATCH_BEGIN_MAGIC {
            return Err(GoatError::corruption(
                "wal_batch_begin_magic",
                "invalid batch begin marker magic",
            ));
        }
        Ok(u32::from_le_bytes(value[4..8].try_into().unwrap()) as usize)
    }

    pub fn encode_atomic_batch_into(
        buf: &mut Vec<u8>,
        records: &[(InternalKey, Bytes)],
    ) -> GoatResult<()> {
        if records.is_empty() {
            return Ok(());
        }

        let start_seq = records[0].0.sequence_number();
        let marker = InternalKey::new(Vec::new(), start_seq, InternalKeyKind::TxnBatchBegin);
        let marker_value = Self::encode_batch_begin_value(records.len())?;
        Self::encode_record_into(buf, &marker, &marker_value);

        let mut expected_seq = start_seq;
        for (key, value) in records {
            if key.sequence_number() != expected_seq {
                return Err(GoatError::corruption(
                    "wal_batch_sequence",
                    format!(
                        "non-contiguous batch sequence: expected {}, got {}",
                        expected_seq,
                        key.sequence_number()
                    ),
                ));
            }
            let kind = key.kind()?;
            if kind == InternalKeyKind::TxnBatchBegin {
                return Err(GoatError::corruption(
                    "wal_batch_data_kind",
                    "batch payload cannot contain TxnBatchBegin record",
                ));
            }
            Self::encode_record_into(buf, key, value.as_ref());
            expected_seq = expected_seq.saturating_add(1);
        }

        Ok(())
    }
}
