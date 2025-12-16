use std::env;

use crate::goatkv::mem_table::MemTable;
use crate::goatkv::wal_manager::WalManager;

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

        println!("{}", exec_path.display());

        let wal_manager = WalManager::new(exec_path).expect("failed to open wal log file");
        Self {
            wal_manager,
            mem_table: MemTable::new(1024 * 1024), // 初始化为1MB大小
        }
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
