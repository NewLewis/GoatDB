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

    pub fn with_start(start: u64) -> Self {
        Self {
            counter: AtomicU64::new(start),
        }
    }

    pub fn next(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }

    pub fn ensure_at_least(&self, next: u64) {
        let mut current = self.counter.load(Ordering::SeqCst);
        while current < next {
            match self
                .counter
                .compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    pub fn set(&self, value: u64) {
        self.counter.store(value, Ordering::SeqCst);
    }
}
