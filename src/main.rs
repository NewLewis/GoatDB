mod mem_table;
mod skip_list;

use std::io::{self, Write};

use crate::mem_table::MemTable;

fn main() {
    // 1. 初始化 MemTable，比如限制大小为 1MB
    let mut memtable = MemTable::new(1024 * 1024);

    println!("GoatDB Client Started!");
    println!("Commands:");
    println!("  put <key> <value>   - Insert a key-value pair");
    println!("  get <key>           - Get a value by key");
    println!("  scan <start> <end>  - Range scan");
    println!("  iter                - Iterate all keys");
    println!("  exit                - Quit");

    // 2. 循环读取用户输入
    loop {
        print!("> "); // 打印提示符
        io::stdout().flush().unwrap(); // 确保提示符立即显示

        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(_) => {
                let input = input.trim(); // 去掉回车换行
                if input.is_empty() {
                    continue;
                }

                // 3. 解析命令
                let parts: Vec<&str> = input.split_whitespace().collect();
                
                if parts.is_empty() {
                    continue;
                }

                match parts[0] {
                    "put" => handle_put(&mut memtable, &parts),
                    "get" => handle_get(&memtable, &parts),
                    "scan" => handle_scan(&memtable, &parts),
                    "iter" => handle_iter(&memtable),
                    "exit" | "quit" => {
                        println!("Bye!");
                        break;
                    }
                    _ => println!("Unknown command: {}", parts[0]),
                }
            }
            Err(error) => println!("Error reading input: {}", error),
        }
    }
}

// 处理 PUT 命令
fn handle_put(memtable: &mut MemTable, parts: &[&str]) {
    if parts.len() < 3 {
        println!("Usage: put <key> <value>");
        return;
    }
    let key = parts[1].as_bytes().to_vec();
    let value = parts[2].as_bytes().to_vec();
    
    // 如果你的 put 返回 bool (need_flush)
    let need_flush = memtable.put(key, value);
    println!("OK (flush needed: {})", need_flush);
}

// 处理 GET 命令
fn handle_get(memtable: &MemTable, parts: &[&str]) {
    if parts.len() < 2 {
        println!("Usage: get <key>");
        return;
    }
    let key = parts[1].as_bytes();
    
    match memtable.get(key) {
        Some(value) => {
            // 这里用上了刚才学的 from_utf8_lossy
            let val_str = String::from_utf8_lossy(value);
            println!("Value: {}", val_str);
        }
        None => println!("Key not found"),
    }
}

// 处理 SCAN 命令 (Range Iter)
fn handle_scan(memtable: &MemTable, parts: &[&str]) {
    if parts.len() < 3 {
        println!("Usage: scan <start_key> <end_key>");
        return;
    }
    
    // 注意：这里我们需要把输入的字符串转换成 Vec<u8>
    // 你的 range_iter 签名如果是 range_iter(&self, start: &Vec<u8>, end: &Vec<u8>)
    let start = parts[1].as_bytes().to_vec();
    let end = parts[2].as_bytes().to_vec();

    println!("Scan range [{}, {}):", parts[1], parts[2]);
    let mut count = 0;
    
    // 调用你的 range_iter
    for (k, v) in memtable.range_iter(&start, &end) {
        let key_str = String::from_utf8_lossy(k);
        let val_str = String::from_utf8_lossy(v);
        println!("  {} => {}", key_str, val_str);
        count += 1;
    }
    println!("Found {} items", count);
}

// 处理 ITER 命令 (全量遍历)
fn handle_iter(memtable: &MemTable) {
    println!("Iterate all keys:");
    let mut count = 0;
    for (k, v) in memtable.iter() {
        let key_str = String::from_utf8_lossy(k);
        let val_str = String::from_utf8_lossy(v);
        println!("  {} => {}", key_str, val_str);
        count += 1;
    }
    println!("Total {} items", count);
}