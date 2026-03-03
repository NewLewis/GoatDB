# Write-Ahead Log (WAL) 设计与实现（当前实现对齐）

本文档描述当前代码实现（`src/goatkv/storage/wal/`）下的 WAL 设计。

当前版本关键点：

- `WalManager` 不再使用后台写线程。
- WAL 由业务调用线程直接写入（在 `WalManager` 内部互斥串行）。
- 旧参数 `sync_interval_ms / sync_bytes / max_buffer_bytes` 已移除。

## 1. 设计目标

- 持久性：`wal_sync=true` 时，写请求返回前完成 `flush + sync_data`。
- 简化语义：去掉后台队列与异步刷盘状态机，降低并发复杂度。
- 可恢复：崩溃后按 WAL 顺序回放，遇坏尾截断到最后有效记录。

## 2. 模块结构

```text
src/goatkv/storage/wal/
├── mod.rs
├── codec.rs      # WAL 记录编码
├── format.rs     # 记录格式、校验和、底层 read_record
├── writer.rs     # 低层文件写入器（write_bytes/flush/sync_data）
├── reader.rs     # 迭代读取 WAL 记录
├── manager.rs    # WAL 管理器（调用线程内联写入）
├── recovery.rs   # WAL 回放与坏尾截断
├── handle.rs     # WAL 生命周期句柄（清理协同）
└── error.rs
```

与引擎集成路径：

```text
src/goatkv/core/kv_engine/writer.rs   # 分配 sequence，组 batch，调用 WalManager
src/goatkv/core/kv_engine/engine.rs   # 打开 WalManager、恢复、轮转协同
src/goatkv/utils/paths.rs             # WAL 路径与命名
```

## 3. WAL 记录格式

单条记录二进制布局（LE）：

```text
checksum(u32) | key_len(u32) | user_key(bytes) | encoded_seq_num(u64) | value_len(u32) | value(bytes)
```

- `encoded_seq_num = (sequence_number << 8) | kind`
- `checksum` 覆盖：`key_len + user_key + encoded_seq_num + value_len + value`
- 编码入口：`WalCodec::encode_record[_into]`

说明：`sequence_number` 是 LSM 逻辑版本顺序，不是文件字节偏移。

## 4. WalManager 并发模型

`WalManager` 使用：

- `Mutex<WalState>`：串行化 WAL 文件操作。
- `WalWriter`：底层 `write_bytes / flush / sync_data`。

没有后台线程、没有条件变量、没有内部缓冲队列。

语义上，所有 `append/append_batch/rotate` 都在调用线程完成。

## 5. 写入路径

### 5.1 `append` / `append_batch`

1. 前台把记录编码为字节串。
2. 获取 `WalManager` 内部互斥锁。
3. 执行 `write_bytes + flush`。
4. 若 `wal_sync=true`，再执行 `sync_data`。
5. 返回结果。

### 5.2 `wal_sync=false`

`wal_sync=false` 时仍会 `flush`，但不调用 `sync_data`。

即：

- 保证数据从用户态缓冲写入内核页缓存。
- 不保证落盘（进程崩溃恢复通常可见，系统掉电不保证）。

## 6. 轮转与关闭

### 6.1 轮转（`rotate`）

`rotate(new_path)` 在同一把锁下执行：

1. 先对当前 writer 做 `flush`。
2. 若 `wal_sync=true`，执行 `sync_data`。
3. 打开新 WAL 文件，替换 writer。

### 6.2 关闭（Drop）

`Drop` 时：

1. 标记 `closed=true`。
2. 尝试 `flush`。
3. 若 `wal_sync=true`，再尝试 `sync_data`。

## 7. 恢复流程

`replay_wal_file` 核心行为：

1. 顺序读取记录。
2. 校验 checksum。
3. 遇坏尾（partial/invalid/checksum mismatch）时，截断到最后有效记录起点。
4. 统计 `max_sequence / entries / truncated`。
5. 将记录回放给上层回调。

恢复语义：保证回放到“最后一个有效前缀”。

## 8. 配置项

`WalManagerConfig` 当前只保留：

- `wal_sync: bool`

引擎侧映射来自 `KvEngineOptions::wal_sync`。

## 9. 已移除项

以下参数属于旧的“后台线程 + 缓冲队列”实现，当前已移除：

- `sync_interval_ms`
- `sync_bytes`
- `max_buffer_bytes`
- `KvEngineOptions::wal_sync_interval_ms`
- `KvEngineOptions::wal_sync_bytes`
- `KvEngineOptions::wal_max_buffer_bytes`

对应原因：当前实现不再有后台刷盘周期与缓冲背压状态机。
