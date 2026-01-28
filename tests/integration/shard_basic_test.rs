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
fn shard_crud_across_shards() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let shard_count = 4;
    let options = KvEngineOptions::default()
        .with_data_dir(tmp.path())
        .with_shard_count(shard_count)
        .with_mem_table_size(64 * 1024)
        .with_wal_sync(false)
        .with_recover_from_wal(false);
    let engine = KvEngine::new_with_options(options).expect("create engine");

    let keys = keys_for_shards(shard_count);

    // Put + get
    for (i, key) in keys.iter().enumerate() {
        let value = format!("v{}", i).into_bytes();
        engine.put(key.clone(), value.clone());
        assert_eq!(engine.get(key), Some(value));
    }

    // Update
    for (i, key) in keys.iter().enumerate() {
        let value = format!("v{}_u", i).into_bytes();
        engine.put(key.clone(), value.clone());
        assert_eq!(engine.get(key), Some(value));
    }

    // Delete
    for key in &keys {
        engine.delete(key.clone());
        assert_eq!(engine.get(key), None);
    }

    // Reinsert
    for (i, key) in keys.iter().enumerate() {
        let value = format!("v{}_r", i).into_bytes();
        engine.put(key.clone(), value.clone());
        assert_eq!(engine.get(key), Some(value));
    }

    // Verify each shard has its own WAL directory and files.
    for shard_idx in 0..shard_count {
        let wal_dir = tmp.path().join(format!("shard{}", shard_idx)).join("wal");
        assert!(wal_dir.is_dir(), "missing wal dir: {:?}", wal_dir);

        let mut has_wal = false;
        for entry in std::fs::read_dir(&wal_dir).expect("read wal dir") {
            if let Ok(entry) = entry {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("wal") {
                    has_wal = true;
                    break;
                }
            }
        }
        assert!(has_wal, "no wal file found in {:?}", wal_dir);
    }
}

#[test]
fn shard_single_crud_and_dirs() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let options = KvEngineOptions::default()
        .with_data_dir(tmp.path())
        .with_shard_count(1)
        .with_mem_table_size(64 * 1024)
        .with_wal_sync(false)
        .with_recover_from_wal(false);
    let engine = KvEngine::new_with_options(options).expect("create engine");

    let key = b"single_key".to_vec();
    engine.put(key.clone(), b"v1".to_vec());
    assert_eq!(engine.get(&key), Some(b"v1".to_vec()));

    engine.put(key.clone(), b"v2".to_vec());
    assert_eq!(engine.get(&key), Some(b"v2".to_vec()));

    engine.delete(key.clone());
    assert_eq!(engine.get(&key), None);

    engine.put(key.clone(), b"v3".to_vec());
    assert_eq!(engine.get(&key), Some(b"v3".to_vec()));

    let shard_base = tmp.path().join("shard0");
    assert!(
        shard_base.is_dir(),
        "missing shard base dir: {:?}",
        shard_base
    );
    assert!(
        shard_base.join("wal").is_dir(),
        "missing wal dir: {:?}",
        shard_base.join("wal")
    );
    assert!(
        shard_base.join("data").is_dir(),
        "missing data dir: {:?}",
        shard_base.join("data")
    );
    assert!(
        shard_base.join("log").is_dir(),
        "missing log dir: {:?}",
        shard_base.join("log")
    );
    assert!(
        shard_base.join("tmp").is_dir(),
        "missing tmp dir: {:?}",
        shard_base.join("tmp")
    );
}
