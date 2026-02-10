use crate::goatkv::format::internal_key::InternalKey;

use super::format::checksum_for;

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
}
