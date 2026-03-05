use std::sync::Arc;

use crate::goatkv::metadata::file_metadata::FileMetadata;

use super::plan::{split_user_key_ranges, SubcompactionRange};

/// Collect sorted unique cut boundaries from file key ranges.
///
/// We use each file's largest user key as split candidate and drop the global
/// maximum boundary to avoid generating an empty tail range.
pub fn collect_user_key_boundaries(
    source_files: &[Arc<FileMetadata>],
    target_files: &[Arc<FileMetadata>],
) -> Vec<Vec<u8>> {
    let mut boundaries = source_files
        .iter()
        .chain(target_files.iter())
        .map(|file| file.largest_user_key().to_vec())
        .collect::<Vec<_>>();
    boundaries.sort();
    boundaries.dedup();
    if !boundaries.is_empty() {
        boundaries.pop();
    }
    boundaries
}

pub fn build_subcompaction_ranges(
    source_files: &[Arc<FileMetadata>],
    target_files: &[Arc<FileMetadata>],
    max_subcompactions: usize,
) -> Vec<SubcompactionRange> {
    let boundaries = collect_user_key_boundaries(source_files, target_files);
    split_user_key_ranges(&boundaries, max_subcompactions.max(1))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::mpsc;

    use crate::goatkv::format::internal_key::{InternalKey, InternalKeyKind};
    use crate::goatkv::metadata::file_metadata::{FileMetadata, TableProperties};

    use super::{build_subcompaction_ranges, collect_user_key_boundaries};

    fn make_file(file_id: u64, smallest_user: &[u8], largest_user: &[u8]) -> Arc<FileMetadata> {
        let smallest =
            InternalKey::new(smallest_user.to_vec(), 20, InternalKeyKind::Put).serialize();
        let largest = InternalKey::new(largest_user.to_vec(), 10, InternalKeyKind::Put).serialize();
        let props = TableProperties::new(1024, smallest, largest, 10, 20);
        let (tx, _rx) = mpsc::unbounded_channel();
        Arc::new(FileMetadata::from_props(file_id, props, tx))
    }

    #[test]
    fn collect_user_key_boundaries_drops_global_max() {
        let source = vec![make_file(1, b"a", b"f"), make_file(2, b"g", b"m")];
        let target = vec![make_file(3, b"n", b"z")];
        let boundaries = collect_user_key_boundaries(&source, &target);
        assert_eq!(boundaries, vec![b"f".to_vec(), b"m".to_vec()]);
    }

    #[test]
    fn build_subcompaction_ranges_respects_order() {
        let source = vec![
            make_file(1, b"a", b"c"),
            make_file(2, b"d", b"f"),
            make_file(3, b"g", b"i"),
            make_file(4, b"j", b"l"),
        ];
        let ranges = build_subcompaction_ranges(&source, &[], 3);
        assert!(ranges.len() <= 3);
        for window in ranges.windows(2) {
            assert_eq!(window[0].end_user_key, window[1].start_user_key);
        }
    }
}
