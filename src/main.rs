mod goatkv;
mod mem_table;
mod skip_list;
mod wal_manager;

use std::io::{self, Write};

use crate::goatkv::GoatKV;

fn main() {
    // 1. 初始化 MemTable，比如限制大小为 1MB
    let mut goatkv = GoatKV::new();

    println!("GoatDB Client Started!");
    println!("Commands:");
    println!("  put <key> <value>   - Insert a key-value pair");
    println!("  get <key>           - Get a value by key");
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
                    "put" => handle_put(&mut goatkv, &parts),
                    "get" => handle_get(&goatkv, &parts),
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
fn handle_put(goatkv: &mut GoatKV, parts: &[&str]) {
    if parts.len() < 3 {
        println!("Usage: put <key> <value>");
        return;
    }
    let key = parts[1].as_bytes().to_vec();
    let value = parts[2].as_bytes().to_vec();

    // 如果你的 put 返回 bool (need_flush)
    goatkv.put(key, value);
}

// 处理 GET 命令
fn handle_get(goatkv: &GoatKV, parts: &[&str]) {
    if parts.len() < 2 {
        println!("Usage: get <key>");
        return;
    }
    let key = parts[1].as_bytes();

    match goatkv.get(key) {
        Some(value) => {
            // 这里用上了刚才学的 from_utf8_lossy
            let val_str = String::from_utf8_lossy(&value);
            println!("Value: {}", val_str);
        }
        None => println!("Key not found"),
    }
}
