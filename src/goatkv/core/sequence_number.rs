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

    pub fn allocate_range(&self, count: u64) -> u64 {
        self.counter.fetch_add(count, Ordering::SeqCst)
    }

    /// Try to allocate a contiguous range `[start, start + count - 1]` without
    /// exceeding `max_inclusive`. Returns `None` when the range would overflow
    /// or cross the bound.
    pub fn try_allocate_range(&self, count: u64, max_inclusive: u64) -> Option<u64> {
        if count == 0 {
            return Some(self.counter.load(Ordering::SeqCst));
        }

        let mut current = self.counter.load(Ordering::SeqCst);
        loop {
            let end = current.checked_add(count - 1)?;
            if end > max_inclusive {
                return None;
            }
            let next = current.checked_add(count)?;
            match self
                .counter
                .compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return Some(current),
                Err(observed) => current = observed,
            }
        }
    }

    pub fn set(&self, value: u64) {
        self.counter.store(value, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::SequenceNumber;

    #[test]
    fn try_allocate_range_respects_upper_bound() {
        let seq = SequenceNumber::with_start(10);
        assert_eq!(seq.try_allocate_range(3, 20), Some(10));
        assert_eq!(seq.try_allocate_range(8, 20), Some(13));
        assert_eq!(seq.try_allocate_range(1, 20), None);
    }
}
