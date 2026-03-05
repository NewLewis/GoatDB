use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotHandle {
    pub id: u64,
    pub sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SnapshotEntry {
    sequence: u64,
}

#[derive(Debug)]
pub struct SnapshotManager {
    next_snapshot_id: u64,
    by_id: HashMap<u64, SnapshotEntry>,
    seq_refcnt: BTreeMap<u64, usize>,
}

impl Default for SnapshotManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotManager {
    pub fn new() -> Self {
        Self {
            next_snapshot_id: 1,
            by_id: HashMap::new(),
            seq_refcnt: BTreeMap::new(),
        }
    }

    pub fn create(&mut self, sequence: u64) -> SnapshotHandle {
        let id = self.next_snapshot_id;
        self.next_snapshot_id = self.next_snapshot_id.saturating_add(1);
        self.by_id.insert(id, SnapshotEntry { sequence });
        *self.seq_refcnt.entry(sequence).or_insert(0) += 1;
        SnapshotHandle { id, sequence }
    }

    pub fn release(&mut self, snapshot_id: u64) -> bool {
        let Some(entry) = self.by_id.remove(&snapshot_id) else {
            return false;
        };
        if let Some(refcnt) = self.seq_refcnt.get_mut(&entry.sequence) {
            *refcnt = refcnt.saturating_sub(1);
            if *refcnt == 0 {
                self.seq_refcnt.remove(&entry.sequence);
            }
        }
        true
    }

    pub fn lookup_sequence(&self, snapshot_id: u64) -> Option<u64> {
        self.by_id.get(&snapshot_id).map(|entry| entry.sequence)
    }

    pub fn snapshot_sequences_sorted(&self) -> Vec<u64> {
        self.seq_refcnt.keys().copied().collect()
    }

    pub fn oldest_snapshot_sequence(&self) -> Option<u64> {
        self.seq_refcnt.first_key_value().map(|(seq, _)| *seq)
    }

    pub fn active_count(&self) -> usize {
        self.by_id.len()
    }
}

#[cfg(test)]
mod tests {
    use super::SnapshotManager;

    #[test]
    fn create_and_lookup_snapshot() {
        let mut manager = SnapshotManager::new();
        let handle = manager.create(42);
        assert_eq!(manager.lookup_sequence(handle.id), Some(42));
        assert_eq!(manager.oldest_snapshot_sequence(), Some(42));
        assert_eq!(manager.active_count(), 1);
    }

    #[test]
    fn release_unknown_snapshot_returns_false() {
        let mut manager = SnapshotManager::new();
        assert!(!manager.release(404));
    }

    #[test]
    fn refcount_tracks_shared_sequence() {
        let mut manager = SnapshotManager::new();
        let s1 = manager.create(100);
        let s2 = manager.create(100);
        let s3 = manager.create(120);

        assert_eq!(manager.snapshot_sequences_sorted(), vec![100, 120]);
        assert_eq!(manager.oldest_snapshot_sequence(), Some(100));

        assert!(manager.release(s1.id));
        assert_eq!(manager.snapshot_sequences_sorted(), vec![100, 120]);

        assert!(manager.release(s2.id));
        assert_eq!(manager.snapshot_sequences_sorted(), vec![120]);
        assert_eq!(manager.oldest_snapshot_sequence(), Some(120));

        assert!(manager.release(s3.id));
        assert!(manager.snapshot_sequences_sorted().is_empty());
        assert_eq!(manager.oldest_snapshot_sequence(), None);
    }
}
