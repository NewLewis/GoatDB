use std::fs::{self, OpenOptions};
use std::io::Write; // 引入 Write trait 以便使用 write_all

use crate::goatkv::utils::db_path_manager::DbPathManager;

pub const CURRENT_FILE_NAME: &str = "CURRENT";

pub fn current_path() -> std::path::PathBuf {
    DbPathManager::global().base_path().join(CURRENT_FILE_NAME)
}

/// 读取CURRENT文件内容
pub fn read_current() -> std::io::Result<Option<String>> {
    if !current_path().exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(current_path())?;
    Ok(Some(content.trim().to_string()))
}

pub fn write_current(manifest_name: &str) -> std::io::Result<()> {
    let current_path = current_path();
    let temp_path = current_path.with_extension("tmp");

    // 1. 使用 OpenOptions 打开文件，赋予写入权限 (Write Mode)
    // create(true): 不存在则创建
    // truncate(true): 存在则清空内容 (相当于覆盖写入)
    // write(true): 赋予写权限 (关键！有了这个才能 sync_all)
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp_path)?;

    // 2. 写入内容
    file.write_all(manifest_name.as_bytes())?;

    // 3. 同步到磁盘
    // 因为 file 是以 write 模式打开的，Windows 允许在这里 sync
    file.sync_all()?;

    drop(file);

    // 5. 原子重命
    fs::rename(&temp_path, &current_path)?;

    // 6. 同步目录
    if let Some(parent) = current_path.parent() {
        if let Ok(dir) = fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }

    Ok(())
}

/// 查找最新的MANIFEST文件编号
pub fn find_latest_manifest() -> std::io::Result<Option<String>> {
    let data_dir = DbPathManager::global().data_dir();
    let mut max_number: Option<u64> = None;

    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        if name_str.starts_with("MANIFEST-") {
            if let Ok(number) = name_str.trim_start_matches("MANIFEST-").parse::<u64>() {
                max_number = Some(max_number.map_or(number, |max| max.max(number)));
            }
        }
    }

    if let Some(max_number) = max_number {
        Ok(Some(format!("MANIFEST-{}", max_number)))
    } else {
        Ok(None)
    }
}
