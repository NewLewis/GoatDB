use crate::goatkv::error::{Error as GoatError, Result as GoatResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SstableBlockCompression {
    #[default]
    None,
    Rle,
}

impl SstableBlockCompression {
    pub fn as_tag(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Rle => 1,
        }
    }

    pub fn from_tag(tag: u8) -> GoatResult<Self> {
        match tag {
            0 => Ok(Self::None),
            1 => Ok(Self::Rle),
            _ => Err(GoatError::corruption(
                "sstable_block_compression",
                format!("unknown compression tag {}", tag),
            )),
        }
    }

    pub fn compress(self, input: &[u8]) -> Vec<u8> {
        match self {
            Self::None => input.to_vec(),
            Self::Rle => rle_encode(input),
        }
    }

    pub fn decompress(self, input: &[u8], expected_uncompressed_len: usize) -> GoatResult<Vec<u8>> {
        match self {
            Self::None => {
                if input.len() != expected_uncompressed_len {
                    return Err(GoatError::corruption(
                        "sstable_block_compression",
                        format!(
                            "none compression length mismatch: expected {}, got {}",
                            expected_uncompressed_len,
                            input.len()
                        ),
                    ));
                }
                Ok(input.to_vec())
            }
            Self::Rle => rle_decode(input, expected_uncompressed_len),
        }
    }
}

fn rle_encode(input: &[u8]) -> Vec<u8> {
    if input.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(input.len());
    let mut i = 0usize;
    while i < input.len() {
        let byte = input[i];
        let mut run = 1usize;
        while i + run < input.len() && input[i + run] == byte && run < u8::MAX as usize {
            run += 1;
        }
        out.push(run as u8);
        out.push(byte);
        i += run;
    }
    out
}

fn rle_decode(input: &[u8], expected_uncompressed_len: usize) -> GoatResult<Vec<u8>> {
    if !input.len().is_multiple_of(2) {
        return Err(GoatError::corruption(
            "sstable_block_compression",
            format!("rle payload length {} is not even", input.len()),
        ));
    }

    let mut out = Vec::with_capacity(expected_uncompressed_len);
    for pair in input.chunks_exact(2) {
        let run = pair[0] as usize;
        if run == 0 {
            return Err(GoatError::corruption(
                "sstable_block_compression",
                "rle run length must be > 0",
            ));
        }
        let byte = pair[1];
        let new_len = out.len().saturating_add(run);
        if new_len > expected_uncompressed_len {
            return Err(GoatError::corruption(
                "sstable_block_compression",
                format!(
                    "rle expands beyond expected length: expected {}, would become {}",
                    expected_uncompressed_len, new_len
                ),
            ));
        }
        out.resize(new_len, byte);
    }

    if out.len() != expected_uncompressed_len {
        return Err(GoatError::corruption(
            "sstable_block_compression",
            format!(
                "rle decoded length mismatch: expected {}, got {}",
                expected_uncompressed_len,
                out.len()
            ),
        ));
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::SstableBlockCompression;

    #[test]
    fn none_roundtrip_preserves_bytes() {
        let input = b"abcabcabc";
        let encoded = SstableBlockCompression::None.compress(input);
        let decoded = SstableBlockCompression::None
            .decompress(&encoded, input.len())
            .expect("decode");
        assert_eq!(decoded, input);
    }

    #[test]
    fn rle_roundtrip_preserves_bytes() {
        let input = b"aaaaabbbbbccccccccccccddd";
        let encoded = SstableBlockCompression::Rle.compress(input);
        assert!(encoded.len() < input.len());
        let decoded = SstableBlockCompression::Rle
            .decompress(&encoded, input.len())
            .expect("decode");
        assert_eq!(decoded, input);
    }

    #[test]
    fn from_tag_rejects_unknown_value() {
        let err = SstableBlockCompression::from_tag(9).expect_err("unknown tag");
        assert!(err.to_string().contains("unknown compression tag"));
    }
}
