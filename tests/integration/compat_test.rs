use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use goat_db::goatkv::core::kv_engine::KvEngine;
use goat_db::goatkv::error::ErrorKind;
use goat_db::goatkv::format::coding;
use goat_db::goatkv::metadata::current;
use goat_db::goatkv::metadata::manifest::ManifestWriter;
use goat_db::goatkv::metadata::version_edit::{VersionEdit, MANIFEST_FORMAT_VERSION_CURRENT};
use goat_db::goatkv::utils::options::KvEngineOptions;
use goat_db::goatkv::utils::paths::ManifestPaths;

const FOOTER_SIZE: usize = 48;
const FOOTER_MAGIC_SIZE: usize = 8;
const FOOTER_FORMAT_MARKER: [u8; 4] = *b"GKFV";
const FOOTER_FORMAT_METADATA_SIZE: usize = 8;
const SSTABLE_FORMAT_VERSION_CURRENT: u8 = 1;

#[derive(Clone, Copy)]
enum ManifestMutation {
    None,
    AppendLegacyEditWithoutFormatVersion,
    AppendFutureFormatVersionEdit,
}

#[derive(Clone, Copy)]
enum SstableMutation {
    None,
    RewriteLegacyFooter,
    RewriteFutureFooter,
}

#[derive(Clone, Copy)]
struct CompatScenario {
    name: &'static str,
    manifest_mutation: ManifestMutation,
    sstable_mutation: SstableMutation,
    expected_error_substr: Option<&'static str>,
}

fn test_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime for compat tests")
}

fn engine_options(base_dir: &Path) -> KvEngineOptions {
    KvEngineOptions::default()
        .with_data_dir(base_dir)
        .with_mem_table_size(8 * 1024)
        .with_wal_sync(false)
        .with_recover_from_wal(true)
}

fn seed_dataset(base_dir: &Path) {
    let engine = KvEngine::new_with_options(engine_options(base_dir)).expect("open seed engine");
    engine
        .put(b"apple".to_vec(), b"red".to_vec())
        .expect("seed put apple");
    engine
        .put(b"banana".to_vec(), b"yellow".to_vec())
        .expect("seed put banana");
    engine.flush();
    engine.shutdown().expect("shutdown seed engine");
}

fn list_sstable_files(data_dir: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = fs::read_dir(data_dir)
        .expect("list data dir")
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("sst"))
        .collect();
    paths.sort();
    paths
}

fn wait_for_latest_sstable(data_dir: &Path) -> PathBuf {
    for _ in 0..100 {
        let mut files = list_sstable_files(data_dir);
        if let Some(path) = files.pop() {
            return path;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("expected sstable file under {}", data_dir.display());
}

fn footer_padding_range(file_content: &[u8]) -> (usize, usize) {
    assert!(
        file_content.len() >= FOOTER_SIZE,
        "sstable too small: {}",
        file_content.len()
    );
    let footer_start = file_content.len() - FOOTER_SIZE;
    let footer = &file_content[footer_start..];
    let (_, bloom_len) =
        coding::decode_varint64_with_length(footer).expect("decode bloom offset varint");
    let (_, index_len) = coding::decode_varint64_with_length(&footer[bloom_len..])
        .expect("decode index offset varint");
    let padding_start = footer_start + bloom_len + index_len;
    let padding_end = file_content.len() - FOOTER_MAGIC_SIZE;
    assert!(
        padding_start <= padding_end,
        "invalid footer layout, padding_start={} padding_end={}",
        padding_start,
        padding_end
    );
    (padding_start, padding_end)
}

fn rewrite_sstable_footer_to_legacy(path: &Path) {
    let mut file_content = fs::read(path).expect("read sstable file");
    let (padding_start, padding_end) = footer_padding_range(&file_content);
    file_content[padding_start..padding_end].fill(0);
    fs::write(path, file_content).expect("rewrite sstable footer to legacy");
}

fn rewrite_sstable_footer_to_future(path: &Path) {
    let mut file_content = fs::read(path).expect("read sstable file");
    let (padding_start, padding_end) = footer_padding_range(&file_content);
    let padding = &mut file_content[padding_start..padding_end];
    assert!(
        padding.len() >= FOOTER_FORMAT_METADATA_SIZE,
        "footer padding too small for format metadata: {}",
        padding.len()
    );
    padding.fill(0);
    padding[..4].copy_from_slice(&FOOTER_FORMAT_MARKER);
    padding[4] = SSTABLE_FORMAT_VERSION_CURRENT + 1;
    fs::write(path, file_content).expect("rewrite sstable footer to future version");
}

fn current_manifest_path(manifest_paths: &ManifestPaths) -> PathBuf {
    let manifest_name = current::read_current(manifest_paths)
        .expect("read CURRENT file")
        .expect("CURRENT must exist after seed");
    manifest_paths.data_dir().join(manifest_name)
}

fn parse_manifest_file_number(manifest_path: &Path) -> u64 {
    manifest_path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("MANIFEST-"))
        .and_then(|number| number.parse::<u64>().ok())
        .expect("parse manifest file number")
}

fn append_manifest_edit(manifest_path: &Path, edit: &VersionEdit) {
    let file_number = parse_manifest_file_number(manifest_path);
    let mut writer =
        ManifestWriter::open_for_append(manifest_path, file_number).expect("open manifest append");
    writer.append_edit(edit).expect("append manifest edit");
    writer.sync().expect("sync manifest edit");
}

fn apply_manifest_mutation(manifest_paths: &ManifestPaths, mutation: ManifestMutation) {
    let manifest_path = current_manifest_path(manifest_paths);
    match mutation {
        ManifestMutation::None => {}
        ManifestMutation::AppendLegacyEditWithoutFormatVersion => {
            let mut edit = VersionEdit::new();
            edit.set_next_file_number(10_000);
            append_manifest_edit(&manifest_path, &edit);
        }
        ManifestMutation::AppendFutureFormatVersionEdit => {
            let mut edit = VersionEdit::new();
            edit.set_format_version(MANIFEST_FORMAT_VERSION_CURRENT + 1);
            append_manifest_edit(&manifest_path, &edit);
        }
    }
}

fn apply_sstable_mutation(data_dir: &Path, mutation: SstableMutation) {
    let sstable_path = wait_for_latest_sstable(data_dir);
    match mutation {
        SstableMutation::None => {}
        SstableMutation::RewriteLegacyFooter => rewrite_sstable_footer_to_legacy(&sstable_path),
        SstableMutation::RewriteFutureFooter => rewrite_sstable_footer_to_future(&sstable_path),
    }
}

fn run_scenario(scenario: CompatScenario) {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    seed_dataset(temp_dir.path());

    let (_wal_paths, sstable_paths, manifest_paths) =
        KvEngine::init_db_paths(temp_dir.path()).expect("init db paths");

    apply_manifest_mutation(manifest_paths.as_ref(), scenario.manifest_mutation);
    apply_sstable_mutation(sstable_paths.data_dir(), scenario.sstable_mutation);

    let reopen = KvEngine::new_with_options(engine_options(temp_dir.path()));
    match scenario.expected_error_substr {
        None => {
            let engine =
                reopen.unwrap_or_else(|e| panic!("scenario {} failed: {}", scenario.name, e));
            assert_eq!(
                engine.get(b"apple").expect("read apple"),
                Some(b"red".to_vec()),
                "scenario {} should preserve seeded data",
                scenario.name
            );
        }
        Some(err_substr) => {
            let err =
                reopen.expect_err("forward incompatible scenario should fail during recovery/open");
            assert_eq!(
                err.kind(),
                ErrorKind::Corruption,
                "scenario {} should fail with corruption",
                scenario.name
            );
            assert!(
                err.to_string().contains(err_substr),
                "scenario {} should contain error marker `{}` but got `{}`",
                scenario.name,
                err_substr,
                err
            );
        }
    }
}

#[test]
fn compatibility_matrix_covers_forward_and_backward_paths() {
    let rt = test_runtime();
    let _guard = rt.enter();

    let scenarios = [
        CompatScenario {
            name: "baseline_current_manifest_current_sstable",
            manifest_mutation: ManifestMutation::None,
            sstable_mutation: SstableMutation::None,
            expected_error_substr: None,
        },
        CompatScenario {
            name: "backward_manifest_legacy_edit_without_format_version",
            manifest_mutation: ManifestMutation::AppendLegacyEditWithoutFormatVersion,
            sstable_mutation: SstableMutation::None,
            expected_error_substr: None,
        },
        CompatScenario {
            name: "backward_sstable_legacy_footer_without_format_marker",
            manifest_mutation: ManifestMutation::None,
            sstable_mutation: SstableMutation::RewriteLegacyFooter,
            expected_error_substr: None,
        },
        CompatScenario {
            name: "forward_manifest_future_format_version_rejected",
            manifest_mutation: ManifestMutation::AppendFutureFormatVersionEdit,
            sstable_mutation: SstableMutation::None,
            expected_error_substr: Some("unsupported manifest format version"),
        },
        CompatScenario {
            name: "forward_sstable_future_format_version_rejected",
            manifest_mutation: ManifestMutation::None,
            sstable_mutation: SstableMutation::RewriteFutureFooter,
            expected_error_substr: Some("unsupported sstable format version"),
        },
    ];

    for scenario in scenarios {
        run_scenario(scenario);
    }
}
