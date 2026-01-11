use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct SequenceNumber {
    counter: AtomicU64,
}

impl Default for SequenceNumber {
    fn default() -> Self {
        Self::new()
    }
}

impl SequenceNumber {
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }

    pub fn next(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }
}
