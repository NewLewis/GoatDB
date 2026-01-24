use std::fs;
use std::io::Write;
use std::path::PathBuf;

use goat_db::goatkv::core::kv_engine::KvEngine;
use goat_db::goatkv::metadata::current;
use goat_db::goatkv::metadata::manifest::{ManifestWriter, INIT_MANIFEST_FILE_NAME};
use goat_db::goatkv::metadata::version_edit::VersionEdit;
use goat_db::goatkv::storage::wal::WalPaths;
use goat_db::goatkv::storage::wal::WalWriter;
use goat_db::goatkv::utils::options::KvEngineOptions;

fn temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("create tempdir")
}

fn wal_path(log_number: u64, base: &WalPaths) -> PathBuf {
    base.wal_path_by_id(log_number)
}

#[test]
fn recovery_handles_truncated_wal_tail() {
    let tmp = temp_dir();

    let (wal_paths, _sstable_paths, _manifest_paths) = KvEngine::init_db_paths(tmp.path()).unwrap();

    // 写入一个完整记录
    {
        let mut wal = WalWriter::new(wal_path(0, &wal_paths), false).unwrap();
        let key = goat_db::goatkv::encoding::internal_key::InternalKey::new(
            b"ok".to_vec(),
            1,
            goat_db::goatkv::encoding::internal_key::InternalKeyKind::Put,
        );
        wal.write(&key, b"v1").unwrap();
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

    assert_eq!(engine.get(b"ok"), Some(b"v1".to_vec()));

    // 截断应已发生：文件现在能被重新打开且尾部无多余无效记录（无法精确比对长度，但能重新打开 WalWriter）
    WalWriter::new(wal_path(0, &wal_paths), false).expect("truncated WAL should be openable");

    drop(engine);
}

#[test]
fn recovery_replays_multiple_wals_in_order() {
    let tmp = temp_dir();

    let (wal_paths, _sstable_paths, _manifest_paths) = KvEngine::init_db_paths(tmp.path()).unwrap();

    // WAL 0
    {
        let mut wal = WalWriter::new(wal_path(0, &wal_paths), false).unwrap();
        let key = goat_db::goatkv::encoding::internal_key::InternalKey::new(
            b"a".to_vec(),
            1,
            goat_db::goatkv::encoding::internal_key::InternalKeyKind::Put,
        );
        wal.write(&key, b"va").unwrap();
    }

    // WAL 1
    {
        let mut wal = WalWriter::new(wal_path(1, &wal_paths), false).unwrap();
        let key = goat_db::goatkv::encoding::internal_key::InternalKey::new(
            b"b".to_vec(),
            2,
            goat_db::goatkv::encoding::internal_key::InternalKeyKind::Put,
        );
        wal.write(&key, b"vb").unwrap();
    }

    let options = KvEngineOptions::default()
        .with_data_dir(tmp.path())
        .with_mem_table_size(8 * 1024) // 小一些，便于触发 flush
        .with_wal_sync(false)
        .with_recover_from_wal(true);

    let engine = KvEngine::new_with_options(options).unwrap();

    assert_eq!(engine.get(b"a"), Some(b"va".to_vec()));
    assert_eq!(engine.get(b"b"), Some(b"vb".to_vec()));

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
        std::io::ErrorKind::InvalidData,
        "unexpected error kind for corrupted manifest: {err}"
    );
}

#[cfg(unix)]
#[test]
fn recovery_replays_wal_if_flush_never_completed() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = temp_dir();

    let (wal_paths, sstable_paths, _manifest_paths) = KvEngine::init_db_paths(tmp.path()).unwrap();

    // WAL 1 写入一条记录
    {
        let mut wal = WalWriter::new(wal_path(1, &wal_paths), false).unwrap();
        let key = goat_db::goatkv::encoding::internal_key::InternalKey::new(
            b"k".to_vec(),
            1,
            goat_db::goatkv::encoding::internal_key::InternalKeyKind::Put,
        );
        wal.write(&key, b"v").unwrap();
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

    assert_eq!(engine.get(b"k"), Some(b"v".to_vec()));

    drop(engine);
}
