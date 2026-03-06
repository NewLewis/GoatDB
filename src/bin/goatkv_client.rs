use clap::{Parser, Subcommand};
use goat_db::goatkv::{Error as GoatError, Result as GoatResult};
use goatkv::goat_kv_service_client::GoatKvServiceClient;
use rustyline::{error::ReadlineError, DefaultEditor};
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

pub mod goatkv {
    tonic::include_proto!("goatkv");
}

/// 解析带引号的命令行参数
///
/// 支持单引号(')和双引号(")，并处理转义字符
/// 示例: `put "key with spaces" 'value with spaces'`
fn parse_quoted_args(input: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current_arg = String::new();
    let mut in_quote = false;
    let mut quote_char = '\0';
    let mut escape_next = false;
    let chars = input.chars().peekable();

    for ch in chars {
        if escape_next {
            current_arg.push(ch);
            escape_next = false;
            continue;
        }

        match ch {
            '\\' if in_quote => {
                escape_next = true;
            }
            '"' | '\'' => {
                if !in_quote {
                    in_quote = true;
                    quote_char = ch;
                } else if ch == quote_char {
                    in_quote = false;
                    quote_char = '\0';
                } else {
                    current_arg.push(ch);
                }
            }
            ' ' | '\t' if !in_quote => {
                if !current_arg.is_empty() {
                    args.push(current_arg.clone());
                    current_arg.clear();
                }
            }
            _ => {
                current_arg.push(ch);
            }
        }
    }

    if !current_arg.is_empty() {
        args.push(current_arg);
    }

    args
}

/// GoatDB 客户端命令行工具
#[derive(Parser)]
#[command(name = "goatkv_client")]
#[command(about = "GoatDB Key-Value Store Client")]
#[command(version = "0.1.0")]
struct Cli {
    /// gRPC 服务器地址
    #[arg(short, long, default_value = "http://127.0.0.1:50051")]
    address: String,

    /// 交互模式（默认）或单次命令模式
    #[command(subcommand)]
    command: Option<Commands>,
}

/// 支持的子命令
#[derive(Subcommand)]
enum Commands {
    /// 插入键值对
    Put {
        /// 键
        key: String,
        /// 值
        value: String,
    },
    /// 获取值
    Get {
        /// 键
        key: String,
        /// 快照 ID（0 表示最新读）
        #[arg(long, default_value_t = 0)]
        snapshot_id: u64,
    },
    /// 批量获取值
    MultiGet {
        /// 键列表（至少 1 个）
        #[arg(required = true, num_args = 1..)]
        keys: Vec<String>,
        /// 快照 ID（当前仅支持 0）
        #[arg(long, default_value_t = 0)]
        snapshot_id: u64,
    },
    /// 范围扫描
    Scan {
        /// 起始 key（包含）
        #[arg(long)]
        start: Option<String>,
        /// 结束 key（不包含）
        #[arg(long)]
        end: Option<String>,
        /// 前缀过滤
        #[arg(long)]
        prefix: Option<String>,
        /// 返回条数上限（0 表示不限制）
        #[arg(long, default_value_t = 0)]
        limit: u32,
        /// 是否倒序返回
        #[arg(long, default_value_t = false)]
        reverse: bool,
        /// 快照 ID（0 表示最新读）
        #[arg(long, default_value_t = 0)]
        snapshot_id: u64,
    },
    /// 条件写入（CAS）
    CompareAndSet {
        /// 键
        key: String,
        /// 期望值；若省略，则要求当前 key 不存在
        #[arg(long)]
        expected: Option<String>,
        /// 新值；若省略且指定 --delete，则匹配后删除
        #[arg(long)]
        new_value: Option<String>,
        /// 匹配后删除 key
        #[arg(long, default_value_t = false)]
        delete: bool,
    },
    /// 更新键的值
    Update {
        /// 键
        key: String,
        /// 新值
        value: String,
    },
    /// 删除键值对
    Delete {
        /// 键
        key: String,
    },
    /// 手动触发 flush
    Flush,
    /// 创建快照
    SnapshotCreate,
    /// 释放快照
    SnapshotRelease {
        /// 快照 ID
        snapshot_id: u64,
    },
}

/// 交互式 REPL 客户端
async fn run_interactive(mut client: GoatKvServiceClient<Channel>) -> GoatResult<()> {
    println!("GoatDB Interactive Client");
    println!("Connected to server successfully!");
    println!(
        "Commands: put <key> <value>, get <key> [snapshot_id], multiget <key1> <key2>..., scan [--start k] [--end k] [--prefix p] [--limit n] [--reverse] [--snapshot-id id], cas <key> [--expected v] [--new-value v|--delete], update <key> <value>, delete <key>, flush, snapshot-create, snapshot-release <id>, exit"
    );
    println!("Use Tab for auto-completion, ↑↓ for history, Ctrl+C to exit");

    let mut rl = DefaultEditor::new().map_err(|e| {
        GoatError::internal_with_source("client_repl_init", "failed to initialize repl", e)
    })?;
    let history_file = get_history_path()?;

    // 尝试加载历史文件
    if rl.load_history(&history_file).is_err() {
        println!("No previous command history found.");
    }

    loop {
        let readline = rl.readline("> ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                rl.add_history_entry(line).map_err(|e| {
                    GoatError::internal_with_source(
                        "client_repl_add_history",
                        "failed to add history entry",
                        e,
                    )
                })?;

                match line {
                    "exit" | "quit" | ":q" => {
                        println!("Goodbye!");
                        break;
                    }
                    "help" | ":help" => {
                        print_help();
                        continue;
                    }
                    _ => {
                        // 解析用户输入（支持带引号的字符串）
                        let args = parse_quoted_args(line);
                        if args.is_empty() {
                            continue;
                        }

                        // 将 Vec<String> 转换为 Vec<&str> 以保持兼容性
                        let args_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();

                        let result = match args_refs[0] {
                            "put" => handle_put(&mut client, &args_refs[1..]).await,
                            "get" => handle_get(&mut client, &args_refs[1..]).await,
                            "multiget" | "multi-get" | "multi_get" => {
                                handle_multiget(&mut client, &args_refs[1..]).await
                            }
                            "scan" => handle_scan(&mut client, &args_refs[1..]).await,
                            "cas" | "compare-and-set" | "compare_and_set" => {
                                handle_compare_and_set(&mut client, &args_refs[1..]).await
                            }
                            "update" => handle_update(&mut client, &args_refs[1..]).await,
                            "delete" => handle_delete(&mut client, &args_refs[1..]).await,
                            "flush" => handle_flush(&mut client).await,
                            "snapshot-create" | "snapshot_create" => {
                                handle_create_snapshot(&mut client).await
                            }
                            "snapshot-release" | "snapshot_release" => {
                                handle_release_snapshot(&mut client, &args_refs[1..]).await
                            }
                            _ => {
                                println!(
                                    "Unknown command: {}. Type 'help' for available commands.",
                                    args_refs[0]
                                );
                                continue;
                            }
                        };

                        if let Err(e) = result {
                            println!("Error: {}", e);
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("Ctrl+C - use 'exit' to quit or type 'help' for help");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("Ctrl+D - exiting...");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    // 保存历史记录
    rl.save_history(&history_file).map_err(|e| {
        GoatError::internal_with_source("client_repl_save_history", "failed to save history", e)
    })?;
    Ok(())
}

/// 打印帮助信息
fn print_help() {
    println!("Available commands:");
    println!("  put <key> <value>   - Insert or update a key-value pair");
    println!("  get <key> [snapshot_id] - Get the value of a key");
    println!("  multiget <key1> <key2>... - Batch get values by keys");
    println!("  scan [--start k] [--end k] [--prefix p] [--limit n] [--reverse] [--snapshot-id id] - Scan visible keys");
    println!("  cas <key> [--expected v] [--new-value v|--delete] - Compare and set");
    println!("  update <key> <value> - Update an existing key's value");
    println!("  delete <key>        - Delete a key-value pair");
    println!("  flush               - Manually trigger flush");
    println!("  snapshot-create     - Create a read snapshot");
    println!("  snapshot-release <id> - Release a snapshot");
    println!("  exit, quit, :q      - Exit the client");
    println!("  help, :help         - Show this help message");
    println!("\nExamples:");
    println!("  put my_key \"my value\"");
    println!("  put \"key with spaces\" \"value with spaces\"");
    println!("  get my_key");
    println!("  get my_key 42");
    println!("  multiget key1 key2 key3");
    println!("  scan --prefix warehouse: --limit 10");
    println!("  cas district:1 --expected old --new-value new");
    println!("  snapshot-create");
    println!("  snapshot-release 42");
    println!("  delete \"key with spaces\"");
    println!("  flush");
}

/// 处理 put 命令
async fn handle_put(client: &mut GoatKvServiceClient<Channel>, args: &[&str]) -> GoatResult<()> {
    if args.len() < 2 {
        return Err(GoatError::invalid_argument(
            "put",
            "Usage: put <key> <value>",
        ));
    }

    let key = args[0].as_bytes().to_vec();
    let value = args[1].as_bytes().to_vec();

    let request = tonic::Request::new(goatkv::WriteRequest { key, value });
    let response = client
        .write(request)
        .await
        .map_err(|e| GoatError::unavailable("grpc_write", e.to_string()))?;
    let resp_data = response.into_inner();

    if resp_data.success {
        println!("✓ Success: {}", resp_data.message);
    } else {
        println!("✗ Failed: {}", resp_data.message);
    }

    Ok(())
}

/// 处理 get 命令
async fn handle_get(client: &mut GoatKvServiceClient<Channel>, args: &[&str]) -> GoatResult<()> {
    if args.is_empty() {
        return Err(GoatError::invalid_argument(
            "get",
            "Usage: get <key> [snapshot_id]",
        ));
    }
    if args.len() > 2 {
        return Err(GoatError::invalid_argument(
            "get",
            "Usage: get <key> [snapshot_id]",
        ));
    }

    let snapshot_id = if args.len() == 2 {
        args[1].parse::<u64>().map_err(|e| {
            GoatError::invalid_argument(
                "snapshot_id",
                format!("snapshot_id must be an unsigned integer: {}", e),
            )
        })?
    } else {
        0
    };

    let key = args[0].as_bytes().to_vec();

    let request = tonic::Request::new(goatkv::GetRequest { key, snapshot_id });
    let response = client
        .get(request)
        .await
        .map_err(|e| GoatError::unavailable("grpc_get", e.to_string()))?;
    let resp_data = response.into_inner();

    if resp_data.success {
        let value_str = String::from_utf8_lossy(&resp_data.value);
        println!("✓ Value: {}", value_str);
    } else {
        println!("✗ Key not found: {}", resp_data.message);
    }

    Ok(())
}

/// 处理 multiget 命令
async fn handle_multiget(
    client: &mut GoatKvServiceClient<Channel>,
    args: &[&str],
) -> GoatResult<()> {
    if args.is_empty() {
        return Err(GoatError::invalid_argument(
            "multiget",
            "Usage: multiget <key1> <key2> ...",
        ));
    }

    let keys = args
        .iter()
        .map(|key| key.as_bytes().to_vec())
        .collect::<Vec<_>>();
    let request = tonic::Request::new(goatkv::MultiGetRequest {
        keys,
        snapshot_id: 0,
    });
    let response = client
        .multi_get(request)
        .await
        .map_err(|e| GoatError::unavailable("grpc_multiget", e.to_string()))?;
    let resp_data = response.into_inner();

    if !resp_data.success {
        println!("✗ Failed: {}", resp_data.message);
        return Ok(());
    }

    println!("✓ Success: {}", resp_data.message);
    for item in resp_data.entries {
        let key = String::from_utf8_lossy(&item.key);
        if item.found {
            let value = String::from_utf8_lossy(&item.value);
            println!("  {} => {}", key, value);
        } else {
            println!("  {} => <not found>", key);
        }
    }
    Ok(())
}

fn parse_scan_args(args: &[&str]) -> GoatResult<goatkv::ScanRequest> {
    let mut request = goatkv::ScanRequest {
        start_key: Vec::new(),
        end_key: Vec::new(),
        prefix: Vec::new(),
        limit: 0,
        reverse: false,
        snapshot_id: 0,
    };

    let mut idx = 0usize;
    while idx < args.len() {
        match args[idx] {
            "--start" => {
                idx += 1;
                let value = args.get(idx).ok_or_else(|| {
                    GoatError::invalid_argument("scan", "Usage: scan [--start k] [--end k] [--prefix p] [--limit n] [--reverse] [--snapshot-id id]")
                })?;
                request.start_key = value.as_bytes().to_vec();
            }
            "--end" => {
                idx += 1;
                let value = args.get(idx).ok_or_else(|| {
                    GoatError::invalid_argument("scan", "Usage: scan [--start k] [--end k] [--prefix p] [--limit n] [--reverse] [--snapshot-id id]")
                })?;
                request.end_key = value.as_bytes().to_vec();
            }
            "--prefix" => {
                idx += 1;
                let value = args.get(idx).ok_or_else(|| {
                    GoatError::invalid_argument("scan", "Usage: scan [--start k] [--end k] [--prefix p] [--limit n] [--reverse] [--snapshot-id id]")
                })?;
                request.prefix = value.as_bytes().to_vec();
            }
            "--limit" => {
                idx += 1;
                let value = args.get(idx).ok_or_else(|| {
                    GoatError::invalid_argument("scan", "Usage: scan [--start k] [--end k] [--prefix p] [--limit n] [--reverse] [--snapshot-id id]")
                })?;
                request.limit = value.parse::<u32>().map_err(|e| {
                    GoatError::invalid_argument(
                        "limit",
                        format!("limit must be an unsigned integer: {}", e),
                    )
                })?;
            }
            "--snapshot-id" => {
                idx += 1;
                let value = args.get(idx).ok_or_else(|| {
                    GoatError::invalid_argument("scan", "Usage: scan [--start k] [--end k] [--prefix p] [--limit n] [--reverse] [--snapshot-id id]")
                })?;
                request.snapshot_id = value.parse::<u64>().map_err(|e| {
                    GoatError::invalid_argument(
                        "snapshot_id",
                        format!("snapshot_id must be an unsigned integer: {}", e),
                    )
                })?;
            }
            "--reverse" => {
                request.reverse = true;
            }
            other => {
                return Err(GoatError::invalid_argument(
                    "scan",
                    format!("unknown scan option `{}`", other),
                ));
            }
        }
        idx += 1;
    }

    Ok(request)
}

async fn handle_scan(client: &mut GoatKvServiceClient<Channel>, args: &[&str]) -> GoatResult<()> {
    let request = parse_scan_args(args)?;
    handle_scan_request(client, request).await
}

async fn handle_scan_request(
    client: &mut GoatKvServiceClient<Channel>,
    request: goatkv::ScanRequest,
) -> GoatResult<()> {
    let request = tonic::Request::new(request);
    let response = client
        .scan(request)
        .await
        .map_err(|e| GoatError::unavailable("grpc_scan", e.to_string()))?;
    let resp_data = response.into_inner();

    if !resp_data.success {
        println!("✗ Failed: {}", resp_data.message);
        return Ok(());
    }

    println!("✓ Success: {}", resp_data.message);
    for item in resp_data.entries {
        let key = String::from_utf8_lossy(&item.key);
        let value = String::from_utf8_lossy(&item.value);
        println!("  {} => {}", key, value);
    }
    Ok(())
}

fn parse_compare_and_set_args(args: &[&str]) -> GoatResult<goatkv::CompareAndSetRequest> {
    if args.is_empty() {
        return Err(GoatError::invalid_argument(
            "cas",
            "Usage: cas <key> [--expected v] [--new-value v|--delete]",
        ));
    }

    let key = args[0].as_bytes().to_vec();
    let mut expected = None;
    let mut new_value = None;
    let mut delete_on_match = false;
    let mut idx = 1usize;

    while idx < args.len() {
        match args[idx] {
            "--expected" => {
                idx += 1;
                let value = args.get(idx).ok_or_else(|| {
                    GoatError::invalid_argument(
                        "cas",
                        "Usage: cas <key> [--expected v] [--new-value v|--delete]",
                    )
                })?;
                expected = Some(value.as_bytes().to_vec());
            }
            "--new-value" => {
                idx += 1;
                let value = args.get(idx).ok_or_else(|| {
                    GoatError::invalid_argument(
                        "cas",
                        "Usage: cas <key> [--expected v] [--new-value v|--delete]",
                    )
                })?;
                new_value = Some(value.as_bytes().to_vec());
            }
            "--delete" => {
                delete_on_match = true;
            }
            other => {
                return Err(GoatError::invalid_argument(
                    "cas",
                    format!("unknown cas option `{}`", other),
                ));
            }
        }
        idx += 1;
    }

    if delete_on_match && new_value.is_some() {
        return Err(GoatError::invalid_argument(
            "cas",
            "--new-value and --delete cannot be used together",
        ));
    }
    if !delete_on_match && new_value.is_none() {
        return Err(GoatError::invalid_argument(
            "cas",
            "must provide either --new-value or --delete",
        ));
    }

    Ok(goatkv::CompareAndSetRequest {
        key,
        expect_exists: expected.is_some(),
        expected_value: expected.unwrap_or_default(),
        new_value: new_value.unwrap_or_default(),
        delete_on_match,
    })
}

async fn handle_compare_and_set(
    client: &mut GoatKvServiceClient<Channel>,
    args: &[&str],
) -> GoatResult<()> {
    let request = tonic::Request::new(parse_compare_and_set_args(args)?);
    let response = client
        .compare_and_set(request)
        .await
        .map_err(|e| GoatError::unavailable("grpc_compare_and_set", e.to_string()))?;
    let resp_data = response.into_inner();

    if resp_data.success {
        println!("✓ Success: {}", resp_data.message);
    } else {
        println!("✗ Failed: {}", resp_data.message);
    }
    Ok(())
}

/// 处理 update 命令
async fn handle_update(client: &mut GoatKvServiceClient<Channel>, args: &[&str]) -> GoatResult<()> {
    if args.len() < 2 {
        return Err(GoatError::invalid_argument(
            "update",
            "Usage: update <key> <value>",
        ));
    }

    let key = args[0].as_bytes().to_vec();
    let value = args[1].as_bytes().to_vec();

    let request = tonic::Request::new(goatkv::UpdateRequest { key, value });
    let response = client
        .update(request)
        .await
        .map_err(|e| GoatError::unavailable("grpc_update", e.to_string()))?;
    let resp_data = response.into_inner();

    if resp_data.success {
        println!("✓ Success: {}", resp_data.message);
    } else {
        println!("✗ Failed: {}", resp_data.message);
    }

    Ok(())
}

/// 处理 delete 命令
async fn handle_delete(client: &mut GoatKvServiceClient<Channel>, args: &[&str]) -> GoatResult<()> {
    if args.is_empty() {
        return Err(GoatError::invalid_argument("delete", "Usage: delete <key>"));
    }

    let key = args[0].as_bytes().to_vec();

    let request = tonic::Request::new(goatkv::DeleteRequest { key });
    let response = client
        .delete(request)
        .await
        .map_err(|e| GoatError::unavailable("grpc_delete", e.to_string()))?;
    let resp_data = response.into_inner();

    if resp_data.success {
        println!("✓ Success: {}", resp_data.message);
    } else {
        println!("✗ Failed: {}", resp_data.message);
    }

    Ok(())
}

/// 处理 flush 命令
async fn handle_flush(client: &mut GoatKvServiceClient<Channel>) -> GoatResult<()> {
    let request = tonic::Request::new(goatkv::FlushRequest {});
    let response = client
        .flush(request)
        .await
        .map_err(|e| GoatError::unavailable("grpc_flush", e.to_string()))?;
    let resp_data = response.into_inner();

    if resp_data.success {
        println!("✓ Success: {}", resp_data.message);
    } else {
        println!("✗ Failed: {}", resp_data.message);
    }

    Ok(())
}

async fn handle_create_snapshot(client: &mut GoatKvServiceClient<Channel>) -> GoatResult<()> {
    let request = tonic::Request::new(goatkv::CreateSnapshotRequest {});
    let response = client
        .create_snapshot(request)
        .await
        .map_err(|e| GoatError::unavailable("grpc_create_snapshot", e.to_string()))?;
    let resp_data = response.into_inner();

    if resp_data.success {
        println!(
            "✓ Success: {} (snapshot_id={})",
            resp_data.message, resp_data.snapshot_id
        );
    } else {
        println!("✗ Failed: {}", resp_data.message);
    }

    Ok(())
}

async fn handle_release_snapshot(
    client: &mut GoatKvServiceClient<Channel>,
    args: &[&str],
) -> GoatResult<()> {
    if args.len() != 1 {
        return Err(GoatError::invalid_argument(
            "snapshot-release",
            "Usage: snapshot-release <snapshot_id>",
        ));
    }
    let snapshot_id = args[0].parse::<u64>().map_err(|e| {
        GoatError::invalid_argument(
            "snapshot_id",
            format!("snapshot_id must be an unsigned integer: {}", e),
        )
    })?;

    let request = tonic::Request::new(goatkv::ReleaseSnapshotRequest { snapshot_id });
    let response = client
        .release_snapshot(request)
        .await
        .map_err(|e| GoatError::unavailable("grpc_release_snapshot", e.to_string()))?;
    let resp_data = response.into_inner();

    if resp_data.success {
        println!("✓ Success: {}", resp_data.message);
    } else {
        println!("✗ Failed: {}", resp_data.message);
    }

    Ok(())
}

/// 执行单次命令
async fn execute_command(
    mut client: GoatKvServiceClient<Channel>,
    command: Commands,
) -> GoatResult<()> {
    match command {
        Commands::Put { key, value } => {
            let args = vec![key.as_str(), value.as_str()];
            handle_put(&mut client, &args).await
        }
        Commands::Get { key, snapshot_id } => {
            let snapshot_id_string;
            let args: Vec<&str> = if snapshot_id == 0 {
                vec![key.as_str()]
            } else {
                snapshot_id_string = snapshot_id.to_string();
                vec![key.as_str(), snapshot_id_string.as_str()]
            };
            handle_get(&mut client, &args).await
        }
        Commands::MultiGet { keys, snapshot_id } => {
            if snapshot_id != 0 {
                return Err(GoatError::invalid_argument(
                    "snapshot_id",
                    "multiget currently only supports snapshot_id=0",
                ));
            }
            let args = keys.iter().map(String::as_str).collect::<Vec<_>>();
            handle_multiget(&mut client, &args).await
        }
        Commands::Scan {
            start,
            end,
            prefix,
            limit,
            reverse,
            snapshot_id,
        } => {
            handle_scan_request(
                &mut client,
                goatkv::ScanRequest {
                    start_key: start.map(|v| v.into_bytes()).unwrap_or_default(),
                    end_key: end.map(|v| v.into_bytes()).unwrap_or_default(),
                    prefix: prefix.map(|v| v.into_bytes()).unwrap_or_default(),
                    limit,
                    reverse,
                    snapshot_id,
                },
            )
            .await
        }
        Commands::CompareAndSet {
            key,
            expected,
            new_value,
            delete,
        } => {
            let mut args = vec![key.as_str()];
            if let Some(expected) = expected.as_ref() {
                args.push("--expected");
                args.push(expected.as_str());
            }
            if let Some(new_value) = new_value.as_ref() {
                args.push("--new-value");
                args.push(new_value.as_str());
            }
            if delete {
                args.push("--delete");
            }
            handle_compare_and_set(&mut client, &args).await
        }
        Commands::Update { key, value } => {
            let args = vec![key.as_str(), value.as_str()];
            handle_update(&mut client, &args).await
        }
        Commands::Delete { key } => {
            let args = vec![key.as_str()];
            handle_delete(&mut client, &args).await
        }
        Commands::Flush => handle_flush(&mut client).await,
        Commands::SnapshotCreate => handle_create_snapshot(&mut client).await,
        Commands::SnapshotRelease { snapshot_id } => {
            let snapshot_id_string = snapshot_id.to_string();
            let args = vec![snapshot_id_string.as_str()];
            handle_release_snapshot(&mut client, &args).await
        }
    }
}

/// 获取历史文件路径（位于用户主目录）
fn get_history_path() -> GoatResult<std::path::PathBuf> {
    // 获取用户主目录
    let home_dir = if cfg!(windows) {
        std::env::var("USERPROFILE").map_err(|e| {
            GoatError::internal_with_source(
                "client_history_home",
                "cannot find USERPROFILE environment variable",
                e,
            )
        })?
    } else {
        std::env::var("HOME").map_err(|e| {
            GoatError::internal_with_source(
                "client_history_home",
                "cannot find HOME environment variable",
                e,
            )
        })?
    };

    let mut path = std::path::PathBuf::from(home_dir);
    path.push(".goatkv");

    // 确保目录存在
    std::fs::create_dir_all(&path).map_err(|e| GoatError::io("client_history_mkdir", e))?;

    path.push("history");
    Ok(path)
}

#[tokio::main]
async fn main() -> GoatResult<()> {
    let cli = Cli::parse();

    // 创建带有超时的连接端点
    println!("Connecting to {}...", cli.address);

    let endpoint = Endpoint::from_shared(cli.address)
        .map_err(|e| GoatError::invalid_argument("address", e.to_string()))?
        .timeout(Duration::from_secs(5)) // 连接和请求超时
        .connect_timeout(Duration::from_secs(3)) // 连接建立超时
        .tcp_keepalive(Some(Duration::from_secs(30))) // TCP keepalive
        .http2_keep_alive_interval(Duration::from_secs(30)); // HTTP/2 keepalive

    let channel = match endpoint.connect().await {
        Ok(channel) => {
            println!("✓ Connected to server successfully!");
            channel
        }
        Err(e) => {
            eprintln!("✗ Failed to connect to server: {}", e);
            eprintln!("  Please ensure the server is running at the specified address.");
            eprintln!("  You can start the server with: cargo run --bin goatkv_server");
            return Err(GoatError::unavailable("grpc_connect", e.to_string()));
        }
    };

    let client = GoatKvServiceClient::new(channel);

    match cli.command {
        Some(command) => {
            // 执行单次命令
            execute_command(client, command).await
        }
        None => {
            // 进入交互模式
            run_interactive(client).await
        }
    }
}
