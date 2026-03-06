mod codec;
mod error;
mod format;
// WalPaths moved to utils/paths.rs
mod handle;
mod reader;
mod recovery;
mod writer;

pub use crate::goatkv::utils::paths::WalPaths;
pub use codec::WalCodec;
pub use error::{WalError, WalResult};
pub use handle::WalHandle;
pub use reader::WalReader;
pub use recovery::{replay_wal_file, WalReplayStats};
pub use writer::{WalWriter, WalWriterConfig};

#[cfg(test)]
mod tests {
    use super::format::checksum_for;
    use super::{replay_wal_file, WalCodec, WalReader, WalWriter, WalWriterConfig};
    use crate::goatkv::format::internal_key::{InternalKey, InternalKeyKind};
    use crate::goatkv::ErrorKind;
    use bytes::Bytes;
    use std::fs;
    use std::path::PathBuf;
    use tempfile::NamedTempFile;

    #[test]
    fn test_wal_writer_new() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let wal = WalWriter::new(temp_file.path().to_path_buf(), WalWriterConfig::default());
        assert!(wal.is_ok());
    }

    #[test]
    fn test_wal_writer_write_and_checksum() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let wal = WalWriter::new(temp_file.path().to_path_buf(), WalWriterConfig::default())
            .expect("Failed to create WalWriter");

        let key = InternalKey::new(b"test_key".to_vec(), 1, InternalKeyKind::Put);
        let value = b"test_value".to_vec();
        wal.append(&key, &value).expect("Failed to append to WAL");

        let metadata = fs::metadata(temp_file.path()).expect("Failed to get metadata");
        assert!(metadata.len() > 0);

        let checksum = checksum_for(
            &key,
            key.serialized_size() as u32,
            &value,
            value.len() as u32,
        );
        let file_content = fs::read(temp_file.path()).expect("Failed to read file");
        let stored_checksum = u32::from_le_bytes(file_content[0..4].try_into().unwrap());
        assert_eq!(checksum, stored_checksum);
    }

    #[test]
    fn test_wal_writer_preallocate_and_truncate_on_drop() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_path_buf();
        let config = WalWriterConfig {
            wal_sync: false,
            wal_preallocate_bytes: 4096,
            ..WalWriterConfig::default()
        };

        let key = InternalKey::new(b"prealloc".to_vec(), 1, InternalKeyKind::Put);
        let value = b"value".to_vec();

        let logical_size = {
            let wal = WalWriter::new(path.clone(), config).expect("open wal");
            wal.append(&key, &value).expect("append wal");
            let metadata = fs::metadata(&path).expect("metadata after append");
            assert!(
                metadata.len() >= 4096,
                "file should be preallocated while writer is alive"
            );
            wal.logical_size_for_test()
        };

        let final_size = fs::metadata(&path).expect("metadata after drop").len();
        assert_eq!(final_size, logical_size);
    }

    #[test]
    fn test_wal_writer_periodic_sync_when_wal_sync_disabled() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_path_buf();
        let config = WalWriterConfig {
            wal_sync: false,
            wal_bytes_per_sync: 1,
            ..WalWriterConfig::default()
        };

        let wal = WalWriter::new(path, config).expect("open wal");
        let key = InternalKey::new(b"sync".to_vec(), 1, InternalKeyKind::Put);
        wal.append(&key, b"v1").expect("append first record");
        assert!(
            wal.sync_calls_for_test() >= 1,
            "periodic sync should trigger once bytes_per_sync is reached"
        );
    }

    #[test]
    fn test_wal_reader_empty() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let reader = WalReader::new(&temp_file.path().to_path_buf());
        assert!(reader.is_ok());

        let mut reader = reader.unwrap();
        assert!(reader.next().is_none());
    }

    #[test]
    fn test_wal_write_and_read() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_path_buf();

        {
            let wal = WalWriter::new(path.clone(), WalWriterConfig::default()).expect("open wal");

            let key1 = InternalKey::new(b"key1".to_vec(), 1, InternalKeyKind::Put);
            let value1 = b"value1".to_vec();
            wal.append(&key1, &value1).expect("Failed to write key1");

            let key2 = InternalKey::new(b"key2".to_vec(), 2, InternalKeyKind::Delete);
            let value2 = b"";
            wal.append(&key2, value2).expect("Failed to write key2");
        }

        let mut reader = WalReader::new(&path).expect("Failed to create reader");

        let entry1 = reader
            .next()
            .expect("Expected first entry")
            .expect("Failed to read entry");
        assert_eq!(entry1.0.user_key(), b"key1");
        assert_eq!(entry1.0.sequence_number(), 1);
        assert_eq!(entry1.1, b"value1");

        let entry2 = reader
            .next()
            .expect("Expected second entry")
            .expect("Failed to read entry");
        assert_eq!(entry2.0.user_key(), b"key2");
        assert_eq!(entry2.0.sequence_number(), 2);
        assert_eq!(entry2.1, b"");

        assert!(reader.next().is_none());
    }

    #[test]
    fn test_wal_reader_skips_atomic_batch_markers() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_path_buf();

        {
            let wal = WalWriter::new(path.clone(), WalWriterConfig::default()).expect("open wal");
            let records = vec![
                (
                    InternalKey::new(b"key1".to_vec(), 10, InternalKeyKind::Put),
                    Bytes::from_static(b"value1"),
                ),
                (
                    InternalKey::new(b"key2".to_vec(), 11, InternalKeyKind::Delete),
                    Bytes::new(),
                ),
            ];
            wal.append_batch(&records).expect("append atomic batch");
        }

        let mut reader = WalReader::new(&path).expect("open wal reader");
        let entry1 = reader.next().expect("entry1").expect("entry1 ok");
        let entry2 = reader.next().expect("entry2").expect("entry2 ok");
        assert_eq!(entry1.0.user_key(), b"key1");
        assert_eq!(entry1.1, b"value1");
        assert_eq!(entry2.0.user_key(), b"key2");
        assert!(entry2.1.is_empty());
        assert!(reader.next().is_none());
    }

    #[test]
    fn test_wal_checksum_validation() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_path_buf();

        {
            let wal = WalWriter::new(path.clone(), WalWriterConfig::default()).expect("open wal");

            let key = InternalKey::new(b"test".to_vec(), 1, InternalKeyKind::Put);
            let value = b"valid".to_vec();
            wal.append(&key, &value).expect("Failed to write");
        }

        {
            let mut file_content = fs::read(&path).expect("Failed to read file");
            file_content[0] = file_content[0].wrapping_add(1);
            fs::write(&path, &file_content).expect("Failed to write corrupted file");
        }

        let mut reader = WalReader::new(&path).expect("Failed to create reader");
        let result = reader.next().expect("Expected entry");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Corruption);
        assert!(error.to_string().contains("checksum mismatch"));
    }

    #[test]
    fn test_wal_corrupted_file_handling() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_path_buf();

        fs::write(&path, [0x01, 0x02]).expect("Failed to write corrupted file");

        let reader = WalReader::new(&path);
        assert!(reader.is_ok());

        let mut reader = reader.unwrap();
        let result = reader.next();
        assert!(result.is_none());
    }

    #[test]
    fn test_wal_reader_reports_invalid_internal_key_kind() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_path_buf();

        let key = InternalKey::from_encoded(b"bad_kind".to_vec(), (7 << 8) | 3);
        let value = b"value".to_vec();
        let key_len = key.serialized_size() as u32;
        let value_len = value.len() as u32;
        let checksum = checksum_for(&key, key_len, &value, value_len);

        let mut record = Vec::new();
        record.extend_from_slice(&checksum.to_le_bytes());
        record.extend_from_slice(&key_len.to_le_bytes());
        record.extend_from_slice(key.user_key());
        record.extend_from_slice(&key.encoded_sequence_number().to_le_bytes());
        record.extend_from_slice(&value_len.to_le_bytes());
        record.extend_from_slice(&value);
        fs::write(&path, &record).expect("Failed to write WAL record");

        let mut reader = WalReader::new(&path).expect("Failed to create reader");
        let err = reader
            .next()
            .expect("Expected entry")
            .expect_err("invalid kind should return corruption");
        assert_eq!(err.kind(), ErrorKind::Corruption);
        assert!(err.to_string().contains("invalid internal key kind"));
    }

    #[test]
    fn test_wal_replay_reports_invalid_internal_key_kind() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_path_buf();

        let key = InternalKey::from_encoded(b"bad_kind".to_vec(), (11 << 8) | 3);
        let value = b"v".to_vec();
        let key_len = key.serialized_size() as u32;
        let value_len = value.len() as u32;
        let checksum = checksum_for(&key, key_len, &value, value_len);

        let mut record = Vec::new();
        record.extend_from_slice(&checksum.to_le_bytes());
        record.extend_from_slice(&key_len.to_le_bytes());
        record.extend_from_slice(key.user_key());
        record.extend_from_slice(&key.encoded_sequence_number().to_le_bytes());
        record.extend_from_slice(&value_len.to_le_bytes());
        record.extend_from_slice(&value);
        fs::write(&path, &record).expect("Failed to write WAL record");

        let err = replay_wal_file(&path, |_key, _value| {})
            .expect_err("invalid kind should fail WAL replay");
        assert_eq!(err.kind(), ErrorKind::Corruption);
        assert!(err.to_string().contains("invalid internal key kind"));
    }

    #[test]
    fn test_wal_replay_truncates_preallocated_zero_tail() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_path_buf();

        {
            let config = WalWriterConfig {
                wal_sync: false,
                ..WalWriterConfig::default()
            };
            let wal = WalWriter::new(path.clone(), config).expect("open wal");
            let key = InternalKey::new(b"ok".to_vec(), 7, InternalKeyKind::Put);
            wal.append(&key, b"value").expect("append wal");
        }

        let good_len = fs::metadata(&path).expect("metadata after close").len();
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open file")
            .set_len(good_len + 4096)
            .expect("extend file with zero tail");

        let mut replayed = 0usize;
        let stats = replay_wal_file(&path, |_key, _value| replayed += 1).expect("replay wal");
        assert_eq!(replayed, 1);
        assert_eq!(stats.entries, 1);
        assert!(
            stats.truncated,
            "zero tail should be truncated during replay"
        );
        assert_eq!(
            fs::metadata(&path).expect("metadata after replay").len(),
            good_len
        );
    }

    #[test]
    fn test_wal_replay_discards_incomplete_atomic_batch() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_path_buf();

        let records = vec![
            (
                InternalKey::new(b"k1".to_vec(), 21, InternalKeyKind::Put),
                Bytes::from_static(b"v1"),
            ),
            (
                InternalKey::new(b"k2".to_vec(), 22, InternalKeyKind::Put),
                Bytes::from_static(b"v2"),
            ),
        ];

        let mut encoded = Vec::new();
        WalCodec::encode_atomic_batch_into(&mut encoded, &records).expect("encode batch");

        let marker = InternalKey::new(Vec::new(), 21, InternalKeyKind::TxnBatchBegin);
        let marker_value = WalCodec::encode_batch_begin_value(records.len()).expect("marker");
        let marker_len = WalCodec::record_size(&marker, &marker_value);
        let first_record_len = WalCodec::record_size(&records[0].0, records[0].1.as_ref());
        let second_record_prefix_len =
            WalCodec::record_size(&records[1].0, records[1].1.as_ref()) / 2;
        let truncated_len = marker_len + first_record_len + second_record_prefix_len;
        fs::write(&path, &encoded[..truncated_len]).expect("write truncated batch");

        let mut replayed = Vec::new();
        let stats =
            replay_wal_file(&path, |key, value| replayed.push((key, value))).expect("replay");
        assert!(
            replayed.is_empty(),
            "incomplete batch must not partially replay"
        );
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.max_sequence, 0);
        assert!(
            stats.truncated,
            "incomplete batch should trigger truncation"
        );
        assert_eq!(
            fs::metadata(&path).expect("metadata after replay").len(),
            0,
            "recovery should roll back the whole incomplete batch"
        );
    }

    fn decode_hex_corpus(text: &str) -> Vec<u8> {
        let mut compact = String::new();
        for line in text.lines() {
            let line = line.split('#').next().unwrap_or("");
            for ch in line.chars() {
                if !ch.is_ascii_whitespace() {
                    compact.push(ch);
                }
            }
        }
        if compact.is_empty() {
            return Vec::new();
        }
        assert!(
            compact.len().is_multiple_of(2),
            "hex corpus must have even number of nibbles"
        );
        let mut out = Vec::with_capacity(compact.len() / 2);
        let bytes = compact.as_bytes();
        for idx in (0..bytes.len()).step_by(2) {
            let hi = bytes[idx] as char;
            let lo = bytes[idx + 1] as char;
            let pair = [hi, lo].iter().collect::<String>();
            let value = u8::from_str_radix(&pair, 16).expect("valid hex byte");
            out.push(value);
        }
        out
    }

    #[test]
    #[ignore = "corpus-based fuzz replay, run with --ignored"]
    fn test_wal_fuzz_corpus_replay_is_total() {
        let corpus_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fuzz/wal_corpus");
        let mut corpus_files = fs::read_dir(&corpus_dir)
            .expect("read wal corpus dir")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("hex"))
            .collect::<Vec<_>>();
        corpus_files.sort();
        assert!(!corpus_files.is_empty(), "wal corpus set must not be empty");

        let mut saw_error = false;
        for path in corpus_files {
            let content = fs::read_to_string(&path).expect("read corpus file");
            let bytes = decode_hex_corpus(&content);
            let temp_file = NamedTempFile::new().expect("create temp wal file");
            fs::write(temp_file.path(), &bytes).expect("write corpus bytes");
            let wal_path = temp_file.path().to_path_buf();

            let mut reader = WalReader::new(&wal_path).expect("open wal reader");
            while let Some(result) = reader.next() {
                if result.is_err() {
                    saw_error = true;
                    break;
                }
            }

            if replay_wal_file(&wal_path, |_key, _value| {}).is_err() {
                saw_error = true;
            }
        }

        assert!(saw_error, "corpus should hit at least one error path");
    }
}
