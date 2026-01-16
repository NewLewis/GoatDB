use std::fs;

use crate::goatkv::utils::db_path_manager::DbPathManager;

/// 读取CURRENT文件内容
pub fn read_current() -> std::io::Result<Option<String>> {
    let current_path = DbPathManager::global().current_file();

    if !current_path.exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(current_path)?;
    Ok(Some(content.trim().to_string()))
}

/// 原子性写入CURRENT文件
pub fn write_current(manifest_name: &str) -> std::io::Result<()> {
    let current_path = DbPathManager::global().current_file();
    let temp_path = current_path.with_extension("tmp");

    // 1. 写入临时文件
    fs::write(&temp_path, manifest_name)?;

    // 2. 同步到磁盘
    let file = fs::File::open(&temp_path)?;
    file.sync_all()?;

    // 3. 原子重命名
    fs::rename(&temp_path, &current_path)?;

    // 4. 同步目录
    if let Some(parent) = current_path.parent() {
        let dir = fs::File::open(parent)?;
        dir.sync_all()?;
    }

    Ok(())
}

/// 查找最新的MANIFEST文件编号
pub fn find_latest_manifest() -> std::io::Result<Option<u64>> {
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

    Ok(max_number)
}
