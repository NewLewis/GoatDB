use std::cmp::Ordering;

/// InternalKey encoding format:
/// - Total size: 8 bytes (64 bits)
/// - Sequence number: 7 bytes (56 bits), stored in the most significant bits
/// - Kind: 1 byte (8 bits), stored in the least significant bits
///
/// Encoding: (sequence_number << 8) | kind_byte
/// Decoding:
///   - sequence_number = encoded >> 8
///   - kind = (encoded & 0xFF) as u8
///
/// Sequence number range: 0 to 2^56 - 1 (≈7.2e16)
/// Kind values: 0 = Put, 1 = Delete

const SEQUENCE_NUMBER_BITS: u32 = 56;
const KIND_BITS: u32 = 8;
const SEQUENCE_NUMBER_MAX: u64 = (1 << SEQUENCE_NUMBER_BITS) - 1;
const KIND_MASK: u64 = 0xFF;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InternalKeyKind {
    Put,
    Delete,
}

impl From<u8> for InternalKeyKind {
    fn from(value: u8) -> Self {
        match value {
            0 => InternalKeyKind::Put,
            1 => InternalKeyKind::Delete,
            _ => panic!("Invalid kind value: {}", value),
        }
    }
}

impl From<InternalKeyKind> for u8 {
    fn from(kind: InternalKeyKind) -> Self {
        match kind {
            InternalKeyKind::Put => 0,
            InternalKeyKind::Delete => 1,
        }
    }
}

impl std::fmt::Display for InternalKeyKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InternalKeyKind::Put => write!(f, "Put"),
            InternalKeyKind::Delete => write!(f, "Delete"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalKey {
    user_key: Vec<u8>,
    encoded_sequence_number: u64,
}

impl InternalKey {
    /// Create a new InternalKey.
    ///
    /// # Arguments
    /// - `user_key`: The user key bytes
    /// - `sequence_number`: Sequence number (must be ≤ 2^56 - 1)
    /// - `kind`: Operation kind (Put or Delete)
    ///
    /// # Panics
    /// Panics if sequence_number exceeds 2^56 - 1.
    pub fn new(user_key: Vec<u8>, sequence_number: u64, kind: InternalKeyKind) -> Self {
        // Ensure sequence_number fits in 56 bits
        if sequence_number > SEQUENCE_NUMBER_MAX {
            panic!(
                "Sequence number {} exceeds maximum value {}",
                sequence_number, SEQUENCE_NUMBER_MAX
            );
        }

        let kind_byte: u8 = kind.into();
        let encoded_sequence_number = (sequence_number << KIND_BITS) | (kind_byte as u64);

        Self {
            user_key,
            encoded_sequence_number,
        }
    }

    /// Create an InternalKey from raw encoded value.
    ///
    /// # Arguments
    /// - `user_key`: The user key bytes
    /// - `encoded_sequence_number`: Raw encoded value (sequence_number << 8 | kind)
    pub fn from_encoded(user_key: Vec<u8>, encoded_sequence_number: u64) -> Self {
        Self {
            user_key,
            encoded_sequence_number,
        }
    }

    /// Get the user key.
    pub fn user_key(&self) -> &[u8] {
        &self.user_key
    }

    /// Get the sequence number (56 bits).
    pub fn sequence_number(&self) -> u64 {
        self.encoded_sequence_number >> KIND_BITS
    }

    /// Get the operation kind.
    pub fn kind(&self) -> InternalKeyKind {
        let kind_byte = (self.encoded_sequence_number & KIND_MASK) as u8;
        kind_byte.into()
    }

    /// Get the raw encoded sequence number (including kind).
    pub fn encoded_sequence_number(&self) -> u64 {
        self.encoded_sequence_number
    }

    /// Get the total size in bytes when serialized.
    /// Returns: user_key.len() + 8 (for encoded sequence number)
    pub fn serialized_size(&self) -> usize {
        self.user_key.len() + 8
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        // In LSM-Tree, InternalKeys are compared by:
        // 1. user_key ascending (lexicographically)
        // 2. sequence_number descending (newer entries first)
        match self.user_key.cmp(&other.user_key) {
            Ordering::Equal => {
                // For equal user keys, compare sequence numbers in reverse order
                let seq1 = self.sequence_number();
                let seq2 = other.sequence_number();
                seq2.cmp(&seq1) // Higher sequence numbers come first
            }
            ordering => ordering,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_internal_key_creation() {
        let key = InternalKey::new(b"key1".to_vec(), 123, InternalKeyKind::Put);
        assert_eq!(key.user_key(), b"key1");
        assert_eq!(key.sequence_number(), 123);
        assert_eq!(key.kind(), InternalKeyKind::Put);
        assert_eq!(key.encoded_sequence_number(), 123 << 8);
    }

    #[test]
    fn test_internal_key_creation_delete() {
        let key = InternalKey::new(b"key2".to_vec(), 456, InternalKeyKind::Delete);
        assert_eq!(key.user_key(), b"key2");
        assert_eq!(key.sequence_number(), 456);
        assert_eq!(key.kind(), InternalKeyKind::Delete);
        assert_eq!(key.encoded_sequence_number(), (456 << 8) | 1);
    }

    #[test]
    fn test_internal_key_encoding() {
        let key1 = InternalKey::new(b"key".to_vec(), 100, InternalKeyKind::Put);
        let key2 = InternalKey::new(b"key".to_vec(), 100, InternalKeyKind::Delete);

        // Kind should be encoded in the lower 8 bits
        assert_eq!(key1.encoded_sequence_number() & KIND_MASK, 0);
        assert_eq!(key2.encoded_sequence_number() & KIND_MASK, 1);

        // Sequence number should be recoverable by shifting right 8 bits
        assert_eq!(key1.encoded_sequence_number() >> 8, 100);
        assert_eq!(key2.encoded_sequence_number() >> 8, 100);

        // Test from_encoded
        let encoded = (100 << 8) | 1;
        let key3 = InternalKey::from_encoded(b"key".to_vec(), encoded);
        assert_eq!(key3.sequence_number(), 100);
        assert_eq!(key3.kind(), InternalKeyKind::Delete);
    }

    #[test]
    fn test_internal_key_ordering() {
        // Same user key, different sequence numbers
        let key1 = InternalKey::new(b"key".to_vec(), 100, InternalKeyKind::Put);
        let key2 = InternalKey::new(b"key".to_vec(), 200, InternalKeyKind::Put);

        // Higher sequence number should come first (reverse order)
        assert!(key2 < key1);

        // Different user keys
        let key3 = InternalKey::new(b"aaa".to_vec(), 100, InternalKeyKind::Put);
        let key4 = InternalKey::new(b"bbb".to_vec(), 50, InternalKeyKind::Put);

        // "aaa" < "bbb" regardless of sequence number
        assert!(key3 < key4);

        // Same user key, same sequence number, different kind
        let key5 = InternalKey::new(b"key".to_vec(), 100, InternalKeyKind::Put);
        let key6 = InternalKey::new(b"key".to_vec(), 100, InternalKeyKind::Delete);

        // Same user key and sequence number should be considered equal
        // regardless of kind (kind is not part of comparison)
        assert_eq!(key5.cmp(&key6), Ordering::Equal);
    }

    #[test]
    fn test_kind_conversions() {
        assert_eq!(u8::from(InternalKeyKind::Put), 0);
        assert_eq!(u8::from(InternalKeyKind::Delete), 1);

        assert_eq!(InternalKeyKind::from(0), InternalKeyKind::Put);
        assert_eq!(InternalKeyKind::from(1), InternalKeyKind::Delete);
    }

    #[test]
    #[should_panic(expected = "Invalid kind value")]
    fn test_invalid_kind_conversion() {
        let _ = InternalKeyKind::from(2);
    }

    #[test]
    fn test_max_sequence_number() {
        // Test maximum valid sequence number (2^56 - 1)
        let max_seq = SEQUENCE_NUMBER_MAX;
        let key = InternalKey::new(b"key".to_vec(), max_seq, InternalKeyKind::Put);
        assert_eq!(key.sequence_number(), max_seq);

        // Encoded value should not overflow
        let encoded = key.encoded_sequence_number();
        assert_eq!(encoded >> 8, max_seq);
        assert_eq!(encoded & KIND_MASK, 0);
    }

    #[test]
    #[should_panic(expected = "exceeds maximum value")]
    fn test_sequence_number_overflow() {
        // This should panic because sequence_number exceeds 56 bits
        let _ = InternalKey::new(
            b"key".to_vec(),
            SEQUENCE_NUMBER_MAX + 1,
            InternalKeyKind::Put,
        );
    }

    #[test]
    fn test_serialized_size() {
        let key = InternalKey::new(b"hello".to_vec(), 123, InternalKeyKind::Put);
        // user_key: 5 bytes + encoded_sequence_number: 8 bytes = 13 bytes
        assert_eq!(key.serialized_size(), 5 + 8);

        let empty_key = InternalKey::new(vec![], 456, InternalKeyKind::Delete);
        assert_eq!(empty_key.serialized_size(), 0 + 8);
    }
}
