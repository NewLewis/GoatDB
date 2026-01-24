mod error;
mod format;
// WalPaths moved to utils/paths.rs
mod reader;
mod recovery;
mod writer;

pub use crate::goatkv::utils::paths::WalPaths;
pub use error::{WalError, WalResult};
pub use reader::WalReader;
pub use recovery::{replay_wal_file, WalReplayStats};
pub use writer::WalWriter;

#[cfg(test)]
mod tests {
    use super::format::checksum_for;
    use super::{WalReader, WalWriter};
    use crate::goatkv::encoding::internal_key::{InternalKey, InternalKeyKind};
    use std::fs;
    use tempfile::NamedTempFile;

    #[test]
    fn test_wal_writer_new() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let wal_writer = WalWriter::new(temp_file.path().to_path_buf(), true);
        assert!(wal_writer.is_ok());
    }

    #[test]
    fn test_wal_writer_write_and_checksum() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let mut wal_writer = WalWriter::new(temp_file.path().to_path_buf(), false)
            .expect("Failed to create WalWriter");

        let key = InternalKey::new(b"test_key".to_vec(), 1, InternalKeyKind::Put);
        let value = b"test_value".to_vec();

        let result = wal_writer.append(&key, &value);
        assert!(result.is_ok());

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
            let mut wal_writer =
                WalWriter::new(path.clone(), false).expect("Failed to create WalWriter");

            let key1 = InternalKey::new(b"key1".to_vec(), 1, InternalKeyKind::Put);
            let value1 = b"value1".to_vec();
            wal_writer
                .append(&key1, &value1)
                .expect("Failed to write key1");

            let key2 = InternalKey::new(b"key2".to_vec(), 2, InternalKeyKind::Delete);
            let value2 = b"".to_vec();
            wal_writer
                .append(&key2, &value2)
                .expect("Failed to write key2");
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
    fn test_wal_checksum_validation() {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_path_buf();

        {
            let mut wal_writer =
                WalWriter::new(path.clone(), false).expect("Failed to create WalWriter");

            let key = InternalKey::new(b"test".to_vec(), 1, InternalKeyKind::Put);
            let value = b"valid".to_vec();
            wal_writer.append(&key, &value).expect("Failed to write");
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
        assert!(error.to_string().contains("Checksum mismatch"));
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
}
