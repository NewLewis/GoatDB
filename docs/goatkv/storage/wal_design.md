# Write-Ahead Log (WAL) 设计与实现（当前实现对齐）

本文档描述当前代码实现（`src/goatkv/storage/wal/`）下的 WAL 设计，重点覆盖：

- 记录格式与校验
- `WalManager` 的并发模型
- 同步/异步写入差异
- 轮转、关闭与恢复行为

## 1. 设计目标

- 持久性：`wal_sync=true` 时，写请求在返回前保证 WAL 已 `flush + sync_data`
- 吞吐：`wal_sync=false` 时，前台只入队到缓冲区，由后台批量刷盘
- 可恢复：崩溃后按 WAL 顺序回放，截断坏尾
- 有界内存：通过 `max_buffer_bytes` 做背压，避免缓冲无限增长

## 2. 模块结构

当前 WAL 相关代码位于：

```text
src/goatkv/storage/wal/
├── mod.rs
├── codec.rs      # WAL 记录编码
├── format.rs     # 记录格式、校验和、底层 read_record
├── writer.rs     # 低层文件写入器（write_bytes/flush/sync_data）
├── reader.rs     # 迭代读取 WAL 记录
├── manager.rs    # WAL 管理器（统一同步/异步写入）
├── recovery.rs   # WAL 回放与坏尾截断
├── handle.rs     # WAL 生命周期句柄（清理协同）
└── error.rs
```

与引擎集成的关键路径：

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

说明：`sequence_number` 是 LSM 逻辑版本顺序；不等同于 `WalManager` 内部的偏移量。

## 4. WalManager 并发模型

`WalManager` 使用一个共享状态 `WalState` + `Mutex` + `Condvar` + 一个后台写线程。

核心状态字段：

- `buffer: Vec<u8>`：单缓冲区（不再使用分片缓冲）
- `buffered_bytes: usize`：当前缓冲字节数
- `offset_end: u64`：已分配写入范围的结束偏移（字节）
- `durable_offset: u64`：已持久化到磁盘的结束偏移
- `closed`、`rotate_*`：关闭与轮转协同

语义分工：

- `offset_end`：接收进度（前台同步写入分配）
- `durable_offset`：持久化进度（后台 `flush+sync_data` 成功后推进）

## 5. 写入路径

### 5.1 同步模式（`wal_sync=true`）

1. 前台编码记录
2. `enqueue_sync_bytes` 入缓冲，分配并返回本次 `offset_end`
3. `wait_for_durable(offset_end)` 阻塞，直到 `durable_offset >= offset_end`
4. 返回成功

后台线程在 flush 时执行：

- `write_bytes`
- `flush`
- `sync_data`
- 成功后更新 `durable_offset`

### 5.2 异步模式（`wal_sync=false`）

1. 前台编码记录
2. `enqueue_async_bytes` 入缓冲后立即返回（不等待 durable）
3. 后台按阈值或时间触发 `write_bytes + flush`（不 `sync_data`）

### 5.3 入队背压差异（保留现有行为）

入队由一个公共函数实现，按模式区分背压条件：

- 异步：`buffered_bytes + data_len > max_buffer_bytes` 就等待
- 同步：只有“缓冲非空且超限”才等待

这样保留了同步/异步原有行为差异。

## 6. Flush 触发条件

后台线程 `should_flush` 条件：

- 有 `rotate_pending`，或
- `buffered_bytes > 0` 且满足以下任一：
  - `buffered_bytes >= sync_bytes`
  - 距离上次 flush 时间 `>= sync_interval_ms`
  - `wal_sync=true` 且 `offset_end > durable_offset`（存在同步请求尚未持久化）

等待策略使用 `Condvar::wait_timeout`：

- 空缓冲：等待完整 `interval`
- 非空缓冲：等待 `interval - elapsed`

## 7. 轮转与关闭

### 7.1 轮转（`rotate`）

`rotate(new_path)` 会：

1. 记录轮转请求并唤醒后台线程
2. 后台先 `flush` 当前文件
3. 若 `wal_sync=true`，再 `sync_data`
4. 打开新 WAL 文件替换 writer
5. 回填 `rotate_completed/rotate_error`，唤醒等待方

### 7.2 关闭（Drop）

`Drop` 时：

1. 设置 `closed=true` 并 `notify_all`
2. 后台线程退出前会尝试 drain 剩余缓冲并 `flush`
3. 若 `wal_sync=true`，关闭时还会尝试 `sync_data`

## 8. 恢复流程

`replay_wal_file` 的关键行为：

1. 顺序读取记录
2. 校验 checksum
3. 遇到坏尾（partial/invalid/checksum mismatch）时，截断到最后一个有效记录起点
4. 统计 `max_sequence / entries / truncated`
5. 把记录回放给上层回调

恢复语义：保证回放到“最后一个有效前缀”。

## 9. 配置项

`WalManagerConfig`：

- `wal_sync: bool`
- `sync_interval_ms: u64`
- `sync_bytes: usize`
- `max_buffer_bytes: usize`

与 `KvEngineOptions` 中 WAL 相关选项一一映射。

## 10. 当前实现特性与限制

### 特性

- 单写线程顺序写 WAL，简化了持久化语义
- 同步模式用 `offset_end/durable_offset` 精确等待本次请求
- 异步模式通过批量 flush 获得更高吞吐

### 限制

- 后台写入线程是单线程，峰值吞吐受单线程与磁盘带宽约束
- `wal_sync=true` 延迟受 `flush + fsync` 影响明显
- 背压是全局缓冲维度（不是分片维度）

## 11. 术语说明

- `sequence_number`：LSM 逻辑版本序号（在 `InternalKey` 中）
- `offset_end`：WAL 字节偏移进度（`WalManager` 内部）
- `durable_offset`：已持久化的 `offset_end` 水位

两者职责不同：前者定义“版本顺序”，后者定义“持久化边界”。

