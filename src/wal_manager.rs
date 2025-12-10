use std::env;
use std::path::{ Path, PathBuf };

pub struct WalManager {
    exec_path: PathBuf,
}

impl WalManager {
    pub fn new() -> Self {
        let mut exec_path = env::current_exe().unwrap();
        exec_path.pop();
        exec_path.push("wal.log");

        Self {
            exec_path,
        }
    }

    pub fn write(&self, key: &Vec<u8>, value: &Vec<u8>) {
        let content = Self::searial(key, value);
        std::fs::write(&self.exec_path, content).unwrap_or_else(|err| {
            println!("write wal failed. err:{}, key:{:?}", err, key);
        })
    }

    fn searial(key: &Vec<u8>, value: &Vec<u8>) -> Vec<u8> {
        let total_len = 4 + key.len() + 4 + value.len();
        let mut result = Vec::<u8>::with_capacity(total_len);

        result.extend_from_slice(&(key.len() as u32).to_le_bytes());
        result.extend_from_slice(key.as_slice());

        result.extend_from_slice(&(value.len() as u32).to_le_bytes());
        result.extend_from_slice(value.as_slice());

        result
    }
}
