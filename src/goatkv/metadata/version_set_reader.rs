use std::sync::Arc;
use std::sync::RwLock;

use crate::goatkv::encoding::internal_key::InternalKey;
use crate::goatkv::metadata::version_set::VersionSet;

struct VersionSetReader {
    version_set: Arc<RwLock<VersionSet>>,
}

impl VersionSetReader {
    fn new(version_set: Arc<RwLock<VersionSet>>) -> Self {
        VersionSetReader { version_set }
    }

    fn get(&self, key: &[u8]) -> Option<(InternalKey, Vec<u8>)> {
        let version = {
            let vs_guard = self.version_set.read().unwrap();
            vs_guard.current().clone()
        };

        // Iterate through all levels and files (from newest to oldest)
        // Level 0 first (newest, may overlap), then higher levels
        for level in 0..version.num_levels() {
            for file_meta in version.get_files(level) {
                // Construct the SSTable file path using file_id
                let sstable_path = DbPathManager::global().sstable_path_by_id(file_meta.file_id);

                // Open the file and read from it
                // Note: Opening file every time is slower, but this allows the SSTableReader
                // to serve as a cache layer in the future
                match SSTableReader::open(&sstable_path) {
                    Ok(mut reader) => {
                        match reader.get(key) {
                            Ok(Some(value)) => return Some(value),
                            Ok(None) => continue, // Not found in this sstable, check next
                            Err(e) => {
                                eprintln!("Failed to read from sstable {:?}: {}", sstable_path, e);
                                continue;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to open sstable {:?}: {}", sstable_path, e);
                        continue;
                    }
                }
            }
        }
    }
}
