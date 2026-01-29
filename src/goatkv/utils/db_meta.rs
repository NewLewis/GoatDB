use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

pub(crate) const META_FILE_NAME: &str = "GOATDB_META";
pub(crate) const META_FORMAT_VERSION: u32 = 1;
pub(crate) const HASH_ALGO: &str = "xxhash64";
pub(crate) const HASH_SEED: u64 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
struct DbMeta {
    format_version: u32,
    shard_count: usize,
    hash_algo: String,
    hash_seed: u64,
}

impl DbMeta {
    fn for_options(shard_count: usize) -> Self {
        Self {
            format_version: META_FORMAT_VERSION,
            shard_count,
            hash_algo: HASH_ALGO.to_string(),
            hash_seed: HASH_SEED,
        }
    }
}

pub(crate) fn ensure_db_meta<P: AsRef<Path>>(
    data_dir: P,
    shard_count: usize,
) -> Result<(), io::Error> {
    let data_dir = data_dir.as_ref();
    if !data_dir.exists() {
        fs::create_dir_all(data_dir)?;
    }

    let meta_path = data_dir.join(META_FILE_NAME);
    if meta_path.exists() {
        let meta = read_meta(&meta_path)?;
        validate_meta(&meta, shard_count)?;
        return Ok(());
    }

    if let Some(existing) = detect_existing_shards(data_dir)? {
        if existing != shard_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "db shard directory count mismatch: expected {}, found {}",
                    shard_count, existing
                ),
            ));
        }
    }

    let meta = DbMeta::for_options(shard_count);
    write_meta(&meta_path, &meta)
}

fn read_meta(path: &Path) -> Result<DbMeta, io::Error> {
    let content = fs::read_to_string(path)?;
    let mut format_version: Option<u32> = None;
    let mut shard_count: Option<usize> = None;
    let mut hash_algo: Option<String> = None;
    let mut hash_seed: Option<u64> = None;

    for raw_line in content.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.splitn(2, '=');
        let key = parts
            .next()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid meta line"))?;
        let value = parts
            .next()
            .map(str::trim)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid meta line"))?;

        match key {
            "format_version" => {
                format_version = Some(value.parse::<u32>().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid format_version")
                })?);
            }
            "shard_count" => {
                shard_count = Some(value.parse::<usize>().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid shard_count")
                })?);
            }
            "hash_algo" => {
                hash_algo = Some(value.to_string());
            }
            "hash_seed" => {
                hash_seed = Some(value.parse::<u64>().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid hash_seed")
                })?);
            }
            _ => {}
        }
    }

    let format_version = format_version.ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "missing format_version")
    })?;
    let shard_count = shard_count
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing shard_count"))?;
    let hash_algo =
        hash_algo.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing hash_algo"))?;
    let hash_seed =
        hash_seed.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing hash_seed"))?;

    Ok(DbMeta {
        format_version,
        shard_count,
        hash_algo,
        hash_seed,
    })
}

fn validate_meta(meta: &DbMeta, shard_count: usize) -> Result<(), io::Error> {
    if meta.format_version != META_FORMAT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "db meta format_version mismatch: expected {}, found {}",
                META_FORMAT_VERSION, meta.format_version
            ),
        ));
    }

    if meta.shard_count != shard_count {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "db meta shard_count mismatch: expected {}, found {}",
                shard_count, meta.shard_count
            ),
        ));
    }

    if meta.hash_algo != HASH_ALGO {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "db meta hash_algo mismatch: expected {}, found {}",
                HASH_ALGO, meta.hash_algo
            ),
        ));
    }

    if meta.hash_seed != HASH_SEED {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "db meta hash_seed mismatch: expected {}, found {}",
                HASH_SEED, meta.hash_seed
            ),
        ));
    }

    Ok(())
}

fn write_meta(path: &Path, meta: &DbMeta) -> Result<(), io::Error> {
    let temp_path = meta_temp_path(path);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp_path)?;

    writeln!(file, "format_version={}", meta.format_version)?;
    writeln!(file, "shard_count={}", meta.shard_count)?;
    writeln!(file, "hash_algo={}", meta.hash_algo)?;
    writeln!(file, "hash_seed={}", meta.hash_seed)?;

    file.sync_all()?;
    drop(file);
    fs::rename(&temp_path, path)?;

    if let Some(parent) = path.parent() {
        if let Ok(dir) = fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }

    Ok(())
}

fn meta_temp_path(path: &Path) -> PathBuf {
    let mut temp = path.to_path_buf();
    temp.set_extension("tmp");
    temp
}

fn detect_existing_shards(data_dir: &Path) -> Result<Option<usize>, io::Error> {
    if !data_dir.exists() {
        return Ok(None);
    }

    let mut indices: Vec<usize> = Vec::new();
    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = match name.to_str() {
            Some(name) => name,
            None => continue,
        };
        if let Some(rest) = name.strip_prefix("shard") {
            if let Ok(index) = rest.parse::<usize>() {
                indices.push(index);
            }
        }
    }

    if indices.is_empty() {
        return Ok(None);
    }

    indices.sort_unstable();
    indices.dedup();
    for (expected, actual) in indices.iter().enumerate() {
        if *actual != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "non-contiguous shard directories: expected shard{}, found shard{}",
                    expected, actual
                ),
            ));
        }
    }

    Ok(Some(indices.len()))
}
