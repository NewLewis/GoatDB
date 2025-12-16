use std::env;
use std::path::PathBuf;

use crate::goatkv::mem_table::{self, MemTable};
use crate::goatkv::wal_manager::{WalIterator, WalManager};

#[derive(Debug)]
pub struct KvEngine {
    wal_manager: WalManager,
    mem_table: MemTable,
}

impl KvEngine {
    pub fn new() -> Self {
        // todo 暂时将启动路径定位wal日志的存放路径
        let mut exec_path = env::current_exe().unwrap();
        exec_path.pop();
        exec_path.push("wal.log");

        let mut mem_table = mem_table::MemTable::new(1024 * 1024); // 初始化为1MB大小
        let _ = Self::replay(&mut mem_table, &exec_path);

        let wal_manager = WalManager::new(exec_path).expect("failed to open wal log file");
        Self {
            wal_manager,
            mem_table, // 初始化为1MB大小
        }
    }

    fn replay(mem_table: &mut MemTable, exec_path: &PathBuf) -> Result<(), std::io::Error> {
        let wal_iterator = WalIterator::new(exec_path)?;
        for entry in wal_iterator {
            match entry {
                Ok((key, value)) => {
                    mem_table.put(key, value);
                    return Ok(());
                }
                Err(err) => {
                    println!("Failed to replay WAL entry: {}, skiped", err);
                }
            }
        }
        Ok(())
    }
}

impl KvEngine {
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.mem_table.get(key).map(|value| value.to_vec())
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.wal_manager
            .write(&key, &value)
            .expect("Failed to write to WAL");
        self.mem_table.put(key, value);
    }
}
