use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use goat_db::goatkv::{KvEngine, KvEngineOptions};

fn shard_index(key: &[u8], shard_count: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % shard_count
}

fn keys_for_shards(shard_count: usize) -> Vec<Vec<u8>> {
    let mut keys: Vec<Option<Vec<u8>>> = vec![None; shard_count];
    let mut i = 0u64;
    while keys.iter().any(|k| k.is_none()) {
        let key = format!("key_{}", i).into_bytes();
        let idx = shard_index(&key, shard_count);
        if keys[idx].is_none() {
            keys[idx] = Some(key);
        }
        i += 1;
        if i > 100_000 {
            panic!("failed to find keys for all shards");
        }
    }
    keys.into_iter().map(|k| k.unwrap()).collect()
}

#[test]
fn shard_recovery_replays_wal_across_shards() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let shard_count = 3;
    let options = KvEngineOptions::default()
        .with_data_dir(tmp.path())
        .with_shard_count(shard_count)
        .with_mem_table_size(1024 * 1024)
        .with_wal_sync(false)
        .with_recover_from_wal(true);

    let keys = keys_for_shards(shard_count);
    {
        let engine = KvEngine::new_with_options(options.clone()).expect("create engine");
        for (i, key) in keys.iter().enumerate() {
            let value = format!("v{}", i).into_bytes();
            engine.put(key.clone(), value);
        }
    }

    // Restart and verify all shard data is recovered from WAL.
    let engine = KvEngine::new_with_options(options).expect("reopen engine");
    for (i, key) in keys.iter().enumerate() {
        let value = format!("v{}", i).into_bytes();
        assert_eq!(engine.get(key), Some(value));
    }
}

#[test]
fn shard_single_recovery_replays_wal() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let options = KvEngineOptions::default()
        .with_data_dir(tmp.path())
        .with_shard_count(1)
        .with_mem_table_size(1024 * 1024)
        .with_wal_sync(false)
        .with_recover_from_wal(true);

    {
        let engine = KvEngine::new_with_options(options.clone()).expect("create engine");
        engine.put(b"k".to_vec(), b"v".to_vec());
    }

    let engine = KvEngine::new_with_options(options).expect("reopen engine");
    assert_eq!(engine.get(b"k"), Some(b"v".to_vec()));
}
