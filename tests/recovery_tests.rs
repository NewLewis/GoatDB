use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::OnceLock;

use goat_db::goatkv::core::kv_engine::KvEngine;
use goat_db::goatkv::storage::wal_manager::WalManager;
use goat_db::goatkv::utils::db_path_manager::DbPathManager;
use goat_db::goatkv::utils::options::KvEngineOptions;

// 全局串行锁，避免 DbPathManager 全局单例在并行测试下冲突
static TEST_MUTEX: OnceLock<std::sync::Mutex<()>> = OnceLock::new();

fn lock_tests() -> std::sync::MutexGuard<'static, ()> {
    TEST_MUTEX.get_or_init(|| std::sync::Mutex::new(())).lock().unwrap()
}

fn reset_db_path_manager() {
    let _ = DbPathManager::reset_for_tests();
}

fn temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("create tempdir")
}

fn wal_path(log_number: u64, base: &DbPathManager) -> PathBuf {
    base.wal_path_by_id(log_number)
}

#[test]
fn recovery_handles_truncated_wal_tail() {
    let _guard = lock_tests();
    reset_db_path_manager();
    let tmp = temp_dir();

    DbPathManager::init(tmp.path()).unwrap();
    let pm = DbPathManager::global().clone();

    // 写入一个完整记录
    {
        let mut wal = WalManager::new(wal_path(0, &pm), false).unwrap();
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
            .open(wal_path(0, &pm))
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

    // 截断应已发生：文件现在能被重新打开且尾部无多余无效记录（无法精确比对长度，但能重新打开 WalManager）
    WalManager::new(wal_path(0, &pm), false).expect("truncated WAL should be openable");

    reset_db_path_manager();
}

#[test]
fn recovery_replays_multiple_wals_in_order() {
    let _guard = lock_tests();
    reset_db_path_manager();
    let tmp = temp_dir();

    DbPathManager::init(tmp.path()).unwrap();
    let pm = DbPathManager::global().clone();

    // WAL 0
    {
        let mut wal = WalManager::new(wal_path(0, &pm), false).unwrap();
        let key = goat_db::goatkv::encoding::internal_key::InternalKey::new(
            b"a".to_vec(),
            1,
            goat_db::goatkv::encoding::internal_key::InternalKeyKind::Put,
        );
        wal.write(&key, b"va").unwrap();
    }

    // WAL 1
    {
        let mut wal = WalManager::new(wal_path(1, &pm), false).unwrap();
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
        let wal0 = wal_path(0, &pm);
        let wal1 = wal_path(1, &pm);
        if !wal1.exists() {
            break;
        }
        // wal0 可能保留（旧主 WAL），不强制删除
        let _ = wal0;
    }

    assert!(!wal_path(1, &pm).exists(), "WAL 1 should be cleaned after flush");

    reset_db_path_manager();
}
