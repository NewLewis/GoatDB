use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use bytes::Bytes;
use goat_db::goatkv::core::kv_engine::KvEngine;
use goat_db::goatkv::error::ErrorKind;
use goat_db::goatkv::format::internal_key::{InternalKey, InternalKeyKind};
use goat_db::goatkv::metadata::current;
use goat_db::goatkv::metadata::manifest::{ManifestWriter, INIT_MANIFEST_FILE_NAME};
use goat_db::goatkv::metadata::version_edit::VersionEdit;
use goat_db::goatkv::storage::wal::WalCodec;
use goat_db::goatkv::storage::wal::WalPaths;
use goat_db::goatkv::storage::wal::WalWriter;
use goat_db::goatkv::storage::wal::WalWriterConfig;
use goat_db::goatkv::utils::options::KvEngineOptions;

fn temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("create tempdir")
}

fn wal_path(log_number: u64, base: &WalPaths) -> PathBuf {
    base.wal_path_by_id(log_number)
}

fn count_sstable_files(data_dir: &std::path::Path) -> usize {
    fs::read_dir(data_dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("sst"))
                .count()
        })
        .unwrap_or(0)
}

fn test_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime for recovery tests")
}

#[test]
fn recovery_handles_truncated_wal_tail() {
    let rt = test_runtime();
    let _guard = rt.enter();
    let tmp = temp_dir();

    let (wal_paths, _sstable_paths, _manifest_paths) = KvEngine::init_db_paths(tmp.path()).unwrap();

    // 写入一个完整记录
    {
        let wal = WalWriter::new(
            wal_path(0, &wal_paths),
            WalWriterConfig {
                wal_sync: false,
                ..WalWriterConfig::default()
            },
        )
        .unwrap();
        let key = goat_db::goatkv::format::internal_key::InternalKey::new(
            b"ok".to_vec(),
            1,
            goat_db::goatkv::format::internal_key::InternalKeyKind::Put,
        );
        wal.append(&key, b"v1").unwrap();
    }
    // 追加半条记录，制造尾部截断
    {
        let mut f = fs::OpenOptions::new()
            .append(true)
            .open(wal_path(0, &wal_paths))
            .unwrap();
        f.write_all(&[0xAA, 0xBB, 0xCC]).unwrap(); // 不完整数据
        f.flush().unwrap();
    }

    let options = KvEngineOptions::default()
        .with_data_dir(tmp.path())
        .with_mem_table_size(64 * 1024)
        .with_wal_sync(false)
        .with_recover_from_wal(true);

    let engine = KvEngine::new_with_options(options).unwrap();

    assert_eq!(engine.get(b"ok").unwrap(), Some(b"v1".to_vec()));

    // 截断应已发生：文件现在能被重新打开且尾部无多余无效记录（无法精确比对长度）
    WalWriter::new(
        wal_path(0, &wal_paths),
        WalWriterConfig {
            wal_sync: false,
            ..WalWriterConfig::default()
        },
    )
    .expect("truncated WAL should be openable");

    drop(engine);
}

#[test]
fn recovery_discards_incomplete_atomic_batch_instead_of_partial_replay() {
    let rt = test_runtime();
    let _guard = rt.enter();
    let tmp = temp_dir();

    let (wal_paths, _sstable_paths, _manifest_paths) = KvEngine::init_db_paths(tmp.path()).unwrap();
    let wal_file = wal_path(1, &wal_paths);

    let records = vec![
        (
            InternalKey::new(b"order:1".to_vec(), 100, InternalKeyKind::Put),
            Bytes::from_static(b"new-order"),
        ),
        (
            InternalKey::new(b"stock:1".to_vec(), 101, InternalKeyKind::Put),
            Bytes::from_static(b"updated-stock"),
        ),
    ];
    let mut encoded = Vec::new();
    WalCodec::encode_atomic_batch_into(&mut encoded, &records).expect("encode atomic batch");
    let marker = InternalKey::new(Vec::new(), 100, InternalKeyKind::TxnBatchBegin);
    let marker_value = WalCodec::encode_batch_begin_value(records.len()).expect("marker");
    let marker_len = WalCodec::record_size(&marker, &marker_value);
    let first_record_len = WalCodec::record_size(&records[0].0, records[0].1.as_ref());
    let truncated_len = marker_len + first_record_len + 3;
    fs::write(&wal_file, &encoded[..truncated_len]).expect("write truncated wal");

    let options = KvEngineOptions::default()
        .with_data_dir(tmp.path())
        .with_mem_table_size(64 * 1024)
        .with_wal_sync(false)
        .with_recover_from_wal(true);

    let engine = KvEngine::new_with_options(options).unwrap();
    assert_eq!(
        engine.get(b"order:1").unwrap(),
        None,
        "truncated batch must not partially recover the first key"
    );
    assert_eq!(
        engine.get(b"stock:1").unwrap(),
        None,
        "truncated batch must not partially recover the second key"
    );
    assert_eq!(
        fs::metadata(&wal_file).unwrap().len(),
        0,
        "recovery should roll back the incomplete batch from WAL"
    );
    drop(engine);
}

#[test]
fn recovery_replays_wal_across_prealloc_and_bytes_per_sync_profiles() {
    let rt = test_runtime();
    let _guard = rt.enter();

    let scenarios = [
        ("default", 0_u64, 0_u64),
        ("high_sync_freq", 4096_u64, 1_u64),
        ("low_sync_freq", 4096_u64, 4 * 1024 * 1024_u64),
    ];

    for (name, preallocate_bytes, bytes_per_sync) in scenarios {
        let tmp = temp_dir();
        let (wal_paths, _sstable_paths, _manifest_paths) =
            KvEngine::init_db_paths(tmp.path()).unwrap();
        let wal_file = wal_path(1, &wal_paths);
        let key = format!("key_{}", name).into_bytes();
        let value = format!("value_{}", name).into_bytes();

        {
            let wal = WalWriter::new(
                wal_file.clone(),
                WalWriterConfig {
                    wal_sync: false,
                    wal_preallocate_bytes: preallocate_bytes,
                    wal_bytes_per_sync: bytes_per_sync,
                },
            )
            .unwrap_or_else(|e| panic!("create wal writer for scenario {}: {}", name, e));
            let internal_key = InternalKey::new(key.clone(), 1, InternalKeyKind::Put);
            wal.append(&internal_key, &value)
                .unwrap_or_else(|e| panic!("append wal for scenario {}: {}", name, e));
        }

        // 模拟“预分配后异常退出”留下的零尾巴。
        let stable_len = fs::metadata(&wal_file).unwrap().len();
        fs::OpenOptions::new()
            .write(true)
            .open(&wal_file)
            .unwrap()
            .set_len(stable_len + 512)
            .unwrap();

        let options = KvEngineOptions::default()
            .with_data_dir(tmp.path())
            .with_mem_table_size(64 * 1024)
            .with_wal_sync(false)
            .with_wal_preallocate_bytes(preallocate_bytes)
            .with_wal_bytes_per_sync(bytes_per_sync)
            .with_recover_from_wal(true);

        let engine = KvEngine::new_with_options(options)
            .unwrap_or_else(|e| panic!("open engine for scenario {}: {}", name, e));
        assert_eq!(
            engine.get(&key).unwrap(),
            Some(value.clone()),
            "scenario {} should recover appended key",
            name
        );
        assert_eq!(
            fs::metadata(&wal_file).unwrap().len(),
            stable_len,
            "scenario {} should truncate zero tail during replay",
            name
        );
        drop(engine);
    }
}

#[test]
fn recovery_replays_multiple_wals_in_order() {
    let rt = test_runtime();
    let _guard = rt.enter();
    let tmp = temp_dir();

    let (wal_paths, _sstable_paths, _manifest_paths) = KvEngine::init_db_paths(tmp.path()).unwrap();

    // WAL 0
    {
        let wal = WalWriter::new(
            wal_path(0, &wal_paths),
            WalWriterConfig {
                wal_sync: false,
                ..WalWriterConfig::default()
            },
        )
        .unwrap();
        let key = goat_db::goatkv::format::internal_key::InternalKey::new(
            b"a".to_vec(),
            1,
            goat_db::goatkv::format::internal_key::InternalKeyKind::Put,
        );
        wal.append(&key, b"va").unwrap();
    }

    // WAL 1
    {
        let wal = WalWriter::new(
            wal_path(1, &wal_paths),
            WalWriterConfig {
                wal_sync: false,
                ..WalWriterConfig::default()
            },
        )
        .unwrap();
        let key = goat_db::goatkv::format::internal_key::InternalKey::new(
            b"b".to_vec(),
            2,
            goat_db::goatkv::format::internal_key::InternalKeyKind::Put,
        );
        wal.append(&key, b"vb").unwrap();
    }

    let options = KvEngineOptions::default()
        .with_data_dir(tmp.path())
        .with_mem_table_size(8 * 1024) // 小一些，便于触发 flush
        .with_wal_sync(false)
        .with_recover_from_wal(true);

    let engine = KvEngine::new_with_options(options).unwrap();

    assert_eq!(engine.get(b"a").unwrap(), Some(b"va".to_vec()));
    assert_eq!(engine.get(b"b").unwrap(), Some(b"vb".to_vec()));

    // 主动 flush，触发 WAL 轮转与清理
    engine.flush();

    // 等待后台 flush 完成并删除旧 WAL（最多 1 秒）
    for _ in 0..50 {
        std::thread::sleep(std::time::Duration::from_millis(20));
        let wal0 = wal_path(0, &wal_paths);
        let wal1 = wal_path(1, &wal_paths);
        if !wal1.exists() {
            break;
        }
        // wal0 可能保留（旧主 WAL），不强制删除
        let _ = wal0;
    }

    assert!(
        !wal_path(1, &wal_paths).exists(),
        "WAL 1 should be cleaned after flush"
    );

    drop(engine);
}

#[test]
fn recovery_advances_log_number_past_existing_wals() {
    let rt = test_runtime();
    let _guard = rt.enter();
    let tmp = temp_dir();

    let (wal_paths, _sstable_paths, _manifest_paths) = KvEngine::init_db_paths(tmp.path()).unwrap();

    // 制造已存在的更大 WAL 编号
    for num in [2u64, 4u64] {
        fs::write(wal_path(num, &wal_paths), []).unwrap();
    }

    let options = KvEngineOptions::default()
        .with_data_dir(tmp.path())
        .with_mem_table_size(64 * 1024)
        .with_wal_sync(false)
        .with_recover_from_wal(true);

    let engine = KvEngine::new_with_options(options).unwrap();

    // 新的 WAL 编号应当是 max(existing)+1
    let expected_new = wal_path(5, &wal_paths);
    assert!(
        expected_new.exists(),
        "expected new WAL {:?} to be created",
        expected_new
    );
    assert!(
        wal_path(4, &wal_paths).exists(),
        "existing WAL should remain"
    );

    drop(engine);
}

#[test]
fn recovery_truncates_manifest_tail() {
    let rt = test_runtime();
    let _guard = rt.enter();
    let tmp = temp_dir();

    let (_wal_paths, sstable_paths, manifest_paths) = KvEngine::init_db_paths(tmp.path()).unwrap();

    let manifest_path = sstable_paths.data_dir().join(INIT_MANIFEST_FILE_NAME);
    let mut edit = VersionEdit::new();
    edit.set_log_number(1);
    let encoded = edit.encode();

    {
        let mut writer = ManifestWriter::create(&manifest_path).unwrap();
        writer.append_edit(&edit).unwrap();
        writer.sync().unwrap();
    }
    current::write_current(&manifest_paths, INIT_MANIFEST_FILE_NAME).unwrap();

    // 追加半条 edit，制造尾部截断
    let mut partial_edit = VersionEdit::new();
    partial_edit.set_log_number(2);
    let partial_encoded = partial_edit.encode();
    let partial_len = (partial_encoded.len() / 2).max(1);
    {
        let mut f = fs::OpenOptions::new()
            .append(true)
            .open(&manifest_path)
            .unwrap();
        let len = partial_encoded.len() as u64;
        f.write_all(&len.to_be_bytes()).unwrap();
        f.write_all(&partial_encoded[..partial_len]).unwrap();
        f.flush().unwrap();
    }

    let options = KvEngineOptions::default()
        .with_data_dir(tmp.path())
        .with_mem_table_size(64 * 1024)
        .with_wal_sync(false)
        .with_recover_from_wal(true);

    let engine = KvEngine::new_with_options(options).unwrap();

    let expected_len = 8 + encoded.len() as u64;
    let metadata = fs::metadata(&manifest_path).unwrap();
    assert_eq!(
        metadata.len(),
        expected_len,
        "manifest should be truncated to last good edit"
    );

    drop(engine);
}

#[test]
fn recovery_errors_on_corrupted_manifest_edit() {
    let rt = test_runtime();
    let _guard = rt.enter();
    let tmp = temp_dir();

    let (_wal_paths, sstable_paths, manifest_paths) = KvEngine::init_db_paths(tmp.path()).unwrap();

    let manifest_path = sstable_paths.data_dir().join(INIT_MANIFEST_FILE_NAME);
    {
        let mut f = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&manifest_path)
            .unwrap();
        // 写入一个完整但无法解码的 edit（未知 tag）
        let payload = vec![0x63u8]; // tag = 99
        let len = payload.len() as u64;
        f.write_all(&len.to_be_bytes()).unwrap();
        f.write_all(&payload).unwrap();
        f.flush().unwrap();
    }
    current::write_current(&manifest_paths, INIT_MANIFEST_FILE_NAME).unwrap();

    let options = KvEngineOptions::default()
        .with_data_dir(tmp.path())
        .with_mem_table_size(64 * 1024)
        .with_wal_sync(false)
        .with_recover_from_wal(true);

    let result = KvEngine::new_with_options(options);
    assert!(result.is_err(), "corrupted manifest should error");
    let err = result.unwrap_err();
    assert_eq!(
        err.kind(),
        ErrorKind::Corruption,
        "unexpected error kind for corrupted manifest: {err}"
    );
}

#[test]
fn read_path_reports_missing_sstable_as_error() {
    let rt = test_runtime();
    let _guard = rt.enter();
    let tmp = temp_dir();
    let options = KvEngineOptions::default()
        .with_data_dir(tmp.path())
        .with_mem_table_size(1)
        .with_wal_sync(false)
        .with_recover_from_wal(true);
    let engine = KvEngine::new_with_options(options).unwrap();

    engine.put(b"k".to_vec(), b"v".to_vec()).unwrap();
    engine.flush();

    let data_dir = engine.sstable_paths().data_dir().to_path_buf();
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && count_sstable_files(&data_dir) == 0 {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        count_sstable_files(&data_dir) > 0,
        "flush should create at least one SSTable"
    );
    drop(engine);

    let options = KvEngineOptions::default()
        .with_data_dir(tmp.path())
        .with_mem_table_size(1)
        .with_wal_sync(false)
        .with_recover_from_wal(false);
    let engine = KvEngine::new_with_options(options).unwrap();

    let missing_file = fs::read_dir(engine.sstable_paths().data_dir())
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("sst"))
        .expect("expect at least one sstable");
    fs::remove_file(&missing_file).unwrap();

    let err = engine.get(b"k").unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
}

#[cfg(unix)]
#[test]
fn recovery_replays_wal_if_flush_never_completed() {
    let rt = test_runtime();
    let _guard = rt.enter();
    use std::os::unix::fs::PermissionsExt;

    let tmp = temp_dir();

    let (wal_paths, sstable_paths, _manifest_paths) = KvEngine::init_db_paths(tmp.path()).unwrap();

    // WAL 1 写入一条记录
    {
        let wal = WalWriter::new(
            wal_path(1, &wal_paths),
            WalWriterConfig {
                wal_sync: false,
                ..WalWriterConfig::default()
            },
        )
        .unwrap();
        let key = goat_db::goatkv::format::internal_key::InternalKey::new(
            b"k".to_vec(),
            1,
            goat_db::goatkv::format::internal_key::InternalKeyKind::Put,
        );
        wal.append(&key, b"v").unwrap();
    }

    // 让 tmp dir 只读，触发恢复后 flush 失败，模拟“恢复后立即崩溃未落盘”
    {
        let tmp_dir = sstable_paths.tmp_dir();
        let mut perms = fs::metadata(tmp_dir).unwrap().permissions();
        perms.set_mode(0o555);
        fs::set_permissions(tmp_dir, perms).unwrap();
    }

    let options = KvEngineOptions::default()
        .with_data_dir(tmp.path())
        .with_mem_table_size(64 * 1024)
        .with_wal_sync(false)
        .with_recover_from_wal(true);
    let engine = KvEngine::new_with_options(options).unwrap();
    drop(engine);

    // 恢复权限，便于后续启动与清理临时目录
    {
        let tmp_dir = sstable_paths.tmp_dir();
        let mut perms = fs::metadata(tmp_dir).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(tmp_dir, perms).unwrap();
    }

    assert!(wal_path(1, &wal_paths).exists(), "WAL 1 should still exist");

    // 再次启动：如果没有不安全的 log_number 推进，应当能 replay WAL 1
    let options = KvEngineOptions::default()
        .with_data_dir(tmp.path())
        .with_mem_table_size(64 * 1024)
        .with_wal_sync(false)
        .with_recover_from_wal(true);
    let engine = KvEngine::new_with_options(options).unwrap();

    assert_eq!(engine.get(b"k").unwrap(), Some(b"v".to_vec()));

    drop(engine);
}

#[cfg(unix)]
#[test]
fn flush_failed_task_does_not_evict_other_immutable_memtables() {
    let rt = test_runtime();
    let _guard = rt.enter();
    use std::os::unix::fs::PermissionsExt;
    use std::time::{Duration, Instant};

    let tmp = temp_dir();
    let options = KvEngineOptions::default()
        .with_data_dir(tmp.path())
        .with_mem_table_size(1)
        .with_wal_sync(false)
        .with_recover_from_wal(true);
    let engine = KvEngine::new_with_options(options).unwrap();

    let tmp_dir = engine.sstable_paths().tmp_dir().to_path_buf();
    let data_dir = engine.sstable_paths().data_dir().to_path_buf();

    // 让第一个 flush 失败
    {
        let mut perms = fs::metadata(&tmp_dir).unwrap().permissions();
        perms.set_mode(0o555);
        fs::set_permissions(&tmp_dir, perms).unwrap();
    }

    engine.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
    engine.flush();
    std::thread::sleep(Duration::from_millis(200));

    assert_eq!(
        count_sstable_files(&data_dir),
        0,
        "first flush should fail while tmp dir is readonly"
    );
    assert_eq!(engine.get(b"k1").unwrap(), Some(b"v1".to_vec()));

    // 恢复权限，让第二个 flush 成功
    {
        let mut perms = fs::metadata(&tmp_dir).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&tmp_dir, perms).unwrap();
    }

    engine.put(b"k2".to_vec(), b"v2".to_vec()).unwrap();
    engine.flush();

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && count_sstable_files(&data_dir) == 0 {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        count_sstable_files(&data_dir) > 0,
        "second flush should create an SSTable after tmp dir becomes writable"
    );

    assert_eq!(engine.get(b"k2").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(
        engine.get(b"k1").unwrap(),
        Some(b"v1".to_vec()),
        "successful flush of a later task must not evict data from an earlier failed flush task"
    );
}
