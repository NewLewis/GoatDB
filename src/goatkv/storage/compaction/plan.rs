#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubcompactionRange {
    pub start_user_key: Option<Vec<u8>>,
    pub end_user_key: Option<Vec<u8>>,
}

impl SubcompactionRange {
    pub fn full() -> Self {
        Self {
            start_user_key: None,
            end_user_key: None,
        }
    }
}

/// Split compaction keyspace into non-overlapping ordered ranges.
///
/// `cut_boundaries` must be sorted ascending and unique. Each boundary is treated
/// as the exclusive end key of one range and inclusive start of the next range.
pub fn split_user_key_ranges(
    cut_boundaries: &[Vec<u8>],
    max_subcompactions: usize,
) -> Vec<SubcompactionRange> {
    if max_subcompactions <= 1 || cut_boundaries.is_empty() {
        return vec![SubcompactionRange::full()];
    }

    let cuts_needed = (max_subcompactions - 1).min(cut_boundaries.len());
    let mut selected_cuts = Vec::with_capacity(cuts_needed);
    for i in 1..=cuts_needed {
        let idx = i * cut_boundaries.len() / (cuts_needed + 1);
        let candidate = cut_boundaries[idx].clone();
        if selected_cuts.last().is_some_and(|last| *last == candidate) {
            continue;
        }
        selected_cuts.push(candidate);
    }

    if selected_cuts.is_empty() {
        return vec![SubcompactionRange::full()];
    }

    let mut ranges = Vec::with_capacity(selected_cuts.len() + 1);
    let mut start = None;
    for cut in selected_cuts {
        ranges.push(SubcompactionRange {
            start_user_key: start.take(),
            end_user_key: Some(cut.clone()),
        });
        start = Some(cut);
    }
    ranges.push(SubcompactionRange {
        start_user_key: start,
        end_user_key: None,
    });
    ranges
}

#[cfg(test)]
mod tests {
    use super::{split_user_key_ranges, SubcompactionRange};

    #[test]
    fn split_user_key_ranges_returns_full_when_no_boundary() {
        let ranges = split_user_key_ranges(&[], 4);
        assert_eq!(ranges, vec![SubcompactionRange::full()]);
    }

    #[test]
    fn split_user_key_ranges_respects_requested_upper_bound() {
        let boundaries = vec![
            b"c".to_vec(),
            b"f".to_vec(),
            b"j".to_vec(),
            b"p".to_vec(),
            b"t".to_vec(),
        ];
        let ranges = split_user_key_ranges(&boundaries, 3);
        assert!(ranges.len() <= 3);
    }

    #[test]
    fn split_user_key_ranges_are_ordered_and_non_overlapping() {
        let boundaries = vec![b"d".to_vec(), b"h".to_vec(), b"m".to_vec(), b"z".to_vec()];
        let ranges = split_user_key_ranges(&boundaries, 5);
        assert_eq!(ranges.len(), 5);

        for window in ranges.windows(2) {
            let left = &window[0];
            let right = &window[1];
            assert_eq!(left.end_user_key, right.start_user_key);
            if let (Some(start), Some(end)) = (&left.start_user_key, &left.end_user_key) {
                assert!(start < end);
            }
        }
        assert!(ranges.first().unwrap().start_user_key.is_none());
        assert!(ranges.last().unwrap().end_user_key.is_none());
    }
}
