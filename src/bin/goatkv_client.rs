use goatkv::{goat_kv_service_client::GoatKvServiceClient, GetRequest, WriteRequest};
use std::io::{self, Write};

pub mod goatkv {
    tonic::include_proto!("goatkv");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("GoatDB Client Started!");
    println!("Commands:");
    println!("  put <key> <value>   - Insert a key-value pair");
    println!("  get <key>           - Get a value by key");
    println!("  exit                - Quit");

    // 初始化client
    let addr = "http://127.0.0.1:50051";
    let mut client = GoatKvServiceClient::connect(addr).await?;

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
                let parts: Vec<&str> = input.splitn(3, char::is_whitespace).collect();

                if parts.is_empty() {
                    continue;
                }

                match parts[0] {
                    "put" => handle_put(&mut client, &parts).await?,
                    "get" => handle_get(&mut client, &parts).await?,
                    "exit" | "quit" => {
                        println!("Bye!");
                        break;
                    }
                    _ => {
                        println!("Unknown command: {}", parts[0]);
                        break;
                    }
                }
            }
            Err(error) => println!("Error reading input: {}", error),
        }
    }
    Ok(())
}

// 处理 PUT 命令
async fn handle_put(
    client: &mut GoatKvServiceClient<tonic::transport::Channel>,
    parts: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    if parts.len() < 3 {
        println!("Usage: put <key> <value>");
        return Err("parts length must be at least 3".into());
    }
    let key = parts[1].as_bytes().to_vec();
    let value = parts[2].as_bytes().to_vec();

    let request = tonic::Request::new(WriteRequest { key, value });

    // 如果你的 put 返回 bool (need_flush)
    let response = client.write(request).await?;
    let resp_data = response.into_inner();

    println!("Response received:");
    println!("  Success: {}", resp_data.success);
    println!("  Message: {}", resp_data.message);

    Ok(())
}

// 处理 GET 命令
async fn handle_get(
    client: &mut GoatKvServiceClient<tonic::transport::Channel>,
    parts: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    if parts.len() < 2 {
        println!("Usage: get <key>");
        return Err("parts length must be at least 2".into());
    }

    let key = parts[1].as_bytes().to_vec();

    let request = tonic::Request::new(GetRequest { key });

    // 如果你的 put 返回 bool (need_flush)
    let response = client.get(request).await?;
    let resp_data = response.into_inner();

    println!("Response received:");
    println!("  Success: {}", resp_data.success);
    println!("  Message: {}", resp_data.message);
    println!("  Value: {}", String::from_utf8_lossy(&resp_data.value));

    Ok(())
}
