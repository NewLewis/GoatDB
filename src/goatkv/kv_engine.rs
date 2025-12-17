use std::collections::VecDeque;
use std::env;
use std::path::PathBuf;

use crate::goatkv::immu_mem_table::ImmutableMemTable;
use crate::goatkv::mem_table::{self, MemTable};
use crate::goatkv::wal_manager::{WalIterator, WalManager};

#[derive(Debug)]
pub struct KvEngine {
    wal_manager: WalManager,
    mem_table: MemTable,
    immutable_mem_tables: VecDeque<ImmutableMemTable>,
}

impl KvEngine {
    const DEFAULT_MEM_TABLE_SIZE: usize = 1024 * 1024; // 默认大小为1MB

    pub fn new() -> Self {
        // todo 暂时将启动路径定位wal日志的存放路径
        let mut exec_path = env::current_exe().unwrap();
        exec_path.pop();
        exec_path.push("wal.log");

        let mut mem_table = mem_table::MemTable::new(Self::DEFAULT_MEM_TABLE_SIZE);
        let _ = Self::replay(&mut mem_table, &exec_path);

        let wal_manager = WalManager::new(exec_path).expect("failed to open wal log file");
        Self {
            wal_manager,
            mem_table,
            immutable_mem_tables: VecDeque::new(),
        }
    }

    fn replay(mem_table: &mut MemTable, exec_path: &PathBuf) -> Result<(), std::io::Error> {
        let wal_iterator = WalIterator::new(exec_path)?;
        for entry in wal_iterator {
            match entry {
                Ok((key, value)) => {
                    mem_table.put(key, value);
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
        // 先从memtable中查找
        let value = self.mem_table.get(key).map(|value| value.to_vec());
        if value.is_some() {
            return value;
        }

        // 再从immutable_mem_tables中查找
        for table in &self.immutable_mem_tables {
            if let Some(value) = table.get(key) {
                return Some(value.to_vec());
            }
        }

        // 如果都没有找到，则返回None
        None
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // 先写入wal
        self.wal_manager
            .write(&key, &value)
            .expect("Failed to write to WAL");

        // 再写入memtable
        self.mem_table.put(key.clone(), value.clone());

        // 判断memtable是否已达到容量限制，
        // 达到容量限制则转换成immutable_mem_tables
        if self.mem_table.should_flush() {
            self.flush();
        }
    }

    fn flush(&mut self) {
        // memtable中的跳表取出旧值，并放入新的空的跳表
        let old_skiplist = self.mem_table.replace_skiplist().unwrap();
        // 用旧的跳表初始化一个immutable_mem_table
        let immutable_mem_table = ImmutableMemTable::new(old_skiplist);
        // 将immutable_mem_table放入immutable_mem_tables中
        self.immutable_mem_tables.push_front(immutable_mem_table);
    }
}
