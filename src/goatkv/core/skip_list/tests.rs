use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::{SkipList, UserKey};
use crate::goatkv::format::internal_key::{InternalKey, InternalKeyKind};

fn make_key(key: &[u8], seq: u64) -> InternalKey {
    InternalKey::new(key.to_vec(), seq, InternalKeyKind::Put)
}

#[test]
fn test_basic_operations() {
    let mut sl: SkipList<InternalKey> = SkipList::new();

    sl.insert(make_key(b"3", 1), Bytes::from("three"));
    sl.insert(make_key(b"1", 1), Bytes::from("one"));
    sl.insert(make_key(b"4", 1), Bytes::from("four"));
    sl.insert(make_key(b"5", 1), Bytes::from("five"));
    sl.insert(make_key(b"9", 1), Bytes::from("nine"));
    sl.insert(make_key(b"2", 1), Bytes::from("two"));

    assert_eq!(sl.get(b"1"), Some(&Bytes::from("one")));
    assert_eq!(sl.get(b"5"), Some(&Bytes::from("five")));
    assert_eq!(sl.get(b"100"), None);

    let keys: Vec<_> = sl.iter().map(|(k, _)| k.user_key().to_vec()).collect();
    assert_eq!(
        keys,
        vec![
            b"1".to_vec(),
            b"2".to_vec(),
            b"3".to_vec(),
            b"4".to_vec(),
            b"5".to_vec(),
            b"9".to_vec()
        ]
    );
}

#[test]
fn test_seek() {
    let mut sl: SkipList<InternalKey> = SkipList::new();

    for i in (0..100).step_by(10) {
        let key_str = format!("{:02}", i);
        sl.insert(
            make_key(key_str.as_bytes(), i as u64),
            Bytes::from((i * 10).to_string()),
        );
    }

    let result = sl.seek(b"50");
    assert!(result.is_some());
    assert_eq!(result.unwrap().0.user_key(), b"50".as_ref());

    let result = sl.seek(b"55");
    assert!(result.is_some());
    assert_eq!(result.unwrap().0.user_key(), b"60".as_ref());

    assert_eq!(sl.seek(b"99"), None);
}

#[test]
fn test_range() {
    let mut sl: SkipList<InternalKey> = SkipList::new();

    for i in 0..100 {
        let key_str = format!("{:02}", i);
        sl.insert(
            make_key(key_str.as_bytes(), i as u64),
            Bytes::from((i * 10).to_string()),
        );
    }

    let start = make_key(b"20", 20);
    let end = make_key(b"30", 30);

    let range: Vec<_> = sl
        .range(&start, &end)
        .map(|(k, _)| k.user_key().to_vec())
        .collect();

    assert_eq!(range.len(), 10);
    for (i, item) in range.iter().enumerate().take(10) {
        assert_eq!(item.as_slice(), format!("{:02}", 20 + i).as_bytes());
    }
}

#[test]
fn test_large_scale() {
    let mut sl: SkipList<InternalKey> = SkipList::new();
    let n = 100_000;

    for i in 0..n {
        let key_str = format!("key_{:010}", i);
        sl.insert(
            make_key(key_str.as_bytes(), 1),
            Bytes::from(format!("value_{}", i)),
        );
    }
    assert_eq!(sl.len(), n);

    for i in 0..n {
        let key_str = format!("key_{:010}", i);
        assert!(sl.get(key_str.as_bytes()).is_some());
    }

    let mut prev_key: Option<Vec<u8>> = None;
    for (k, _) in sl.iter() {
        if let Some(ref prev) = prev_key {
            assert!(k.user_key() > prev.as_slice());
        }
        prev_key = Some(k.user_key().to_vec());
    }

    tracing::info!("Memory usage: {} bytes", sl.memory_usage());
}

struct DropKey {
    data: Vec<u8>,
    dropped: Arc<AtomicUsize>,
}

impl Clone for DropKey {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            dropped: Arc::clone(&self.dropped),
        }
    }
}

impl PartialEq for DropKey {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for DropKey {}

impl PartialOrd for DropKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DropKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.data.cmp(&other.data)
    }
}

impl Drop for DropKey {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl UserKey for DropKey {
    fn user_key(&self) -> &[u8] {
        &self.data
    }
}

#[test]
fn test_drop_reclaims_node_keys() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let total = 1024usize;
    {
        let mut sl: SkipList<DropKey> = SkipList::new();
        for i in 0..total {
            sl.insert(
                DropKey {
                    data: format!("key_{:04}", i).into_bytes(),
                    dropped: Arc::clone(&dropped),
                },
                Bytes::from_static(b"v"),
            );
        }
        assert_eq!(sl.len(), total);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
    }

    assert_eq!(dropped.load(Ordering::SeqCst), total);
}
