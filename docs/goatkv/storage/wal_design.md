# Write-Ahead Log (WAL) 设计与实现文档

## 📋 概述

Write-Ahead Log (WAL) 是 GoatDB 的核心持久化组件，确保在系统崩溃或异常终止时数据的持久性和一致性。WAL 实现了写前日志机制：所有写操作在应用到内存表（MemTable）之前，必须先进入 WAL 写入路径；`wal_sync=true` 时保证落盘后再更新 MemTable，`wal_sync=false` 则先进入 WAL 缓冲并由后台刷盘。

### 设计目标
- **持久性保证**：确保已确认的写操作在崩溃后不会丢失
- **高性能**：最小化 WAL 对写操作延迟的影响
- **可恢复性**：支持从崩溃状态完整恢复数据
- **空间效率**：及时清理不再需要的 WAL 文件
- **并发支持**：通过分片缓冲支持并发写入（内存级别）

## 🏗️ 架构与组件

### 核心组件

```
src/goatkv/storage/wal/
├── mod.rs              # 模块导出
├── format.rs           # 记录格式与校验和
├── writer.rs           # WAL 写入器
├── reader.rs           # 记录读取器
├── recovery.rs         # 崩溃恢复
├── wal_manager.rs      # 异步 WAL 管理器
└── error.rs            # 错误类型定义
```

```
src/goatkv/core/
├── kv_engine.rs        # WAL 写入/恢复与轮转入口
├── wal_handle.rs       # WAL 句柄（清理管理）
└── lsm_state.rs        # Immutable MemTable 与 WAL 绑定
```

```
src/goatkv/utils/
└── paths.rs           # WAL 路径管理
```

### 组件职责

1. **WalWriter** - 低层写入器
   - 负责把记录写入文件（提供 `append()` / `write_bytes()` / `flush()` / `sync_data()`）
   - `append()` 会 `flush()`，并在 `wal_sync=true` 时调用 `sync_data()`
   - 引擎正常路径通过 `WalManager` 间接使用：后台线程调用 `write_bytes/flush/sync_data`（`append()` 主要用于测试或直接写入场景）

2. **WalManager** - WAL 管理器（同步/异步统一入口）
   - 多线程安全的写入入口（`KvEngine` 始终使用）
   - `wal_sync=false` 时启用分片缓冲减少锁竞争
   - 后台线程合并缓冲区进行顺序写入
   - 负责刷盘、可选同步和日志轮转

3. **WalReader** - 记录读取器
   - 迭代式读取 WAL 记录
   - 验证校验和
   - 处理损坏记录和部分写入

4. **WalRecovery** - 恢复引擎
   - 从 WAL 文件重建内存状态
   - 检测并截断损坏的记录
   - 报告恢复统计信息

5. **WalHandle** - 清理句柄
   - 自动管理 WAL 文件生命周期
   - 在不再需要时触发清理
   - 防止在关闭期间错误删除

## 📝 记录格式规范

WAL 记录采用二进制格式，具有校验和保证数据完整性：

### 二进制布局

```text
+----------------+----------------+----------------+----------------+
|   Checksum     |   Key Length   |   User Key    | Encoded SeqNum |
|   (4 bytes)    |   (4 bytes)    |  (variable)   |   (8 bytes)    |
|  u32, LE       |  u32, LE       |               |   u64, LE      |
+----------------+----------------+----------------+----------------+
|  Value Length  |      Value     |
|   (4 bytes)    |    (variable)  |
|  u32, LE       |                |
+----------------+----------------+
```

### 字段说明

1. **Checksum** (4 bytes)
   - 使用 CRC32 算法计算
   - 覆盖：Key Length + User Key + Encoded SeqNum + Value Length + Value
   - 目的：检测数据损坏和部分写入

2. **Key Length** (4 bytes)
   - InternalKey 的总字节数
   - 包含：User Key 长度 + 8 字节序列号编码
   - 最小值为 8（仅序列号）

3. **User Key** (variable)
   - 用户提供的键字节
   - 原始字节，无编码

4. **Encoded SeqNum** (8 bytes)
   - 序列号编码：`(sequence_number << 8) | kind as u64`
   - 高 56 位：序列号
   - 低 8 位：操作类型（Put = 0, Delete = 1）

5. **Value Length** (4 bytes)
   - 值字节数
   - Delete 操作时值为 0

6. **Value** (variable)
   - 用户提供的值字节
   - Delete 操作时空字节

### 校验和计算

```rust
fn checksum_for(key: &InternalKey, key_len: u32, value: &[u8], value_len: u32) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(&key_len.to_le_bytes());          // Key 长度
    hasher.update(key.user_key());                  // 用户键
    hasher.update(&key.encoded_sequence_number().to_le_bytes()); // 编码序列号
    hasher.update(&value_len.to_le_bytes());        // 值长度
    hasher.update(value);                           // 值内容
    hasher.finalize()
}
```

## 🚀 写入路径设计

### 同步写入模式（`wal_sync=true`）

```
客户端写入请求
    ↓
获取写入门闩（`write_gate` 读锁）
    ↓
生成序列号
    ↓
构建 InternalKey
    ↓
调用 WalManager::append()
    ↓
编码记录并追加到共享缓冲区
    ↓
唤醒后台 WAL 写线程
    ↓
（后台）写入文件并 flush
    ↓
（后台）`wal_sync=true` 时 `sync_data` 到磁盘
    ↓
调用线程等待 `durable_lsn` ≥ 本次写入结束位置
    ↓
更新 MemTable
    ↓
返回客户端成功
```

### 异步写入模式（`wal_sync=false`）

```
客户端写入请求
    ↓
获取写入门闩（`write_gate` 读锁）
    ↓
生成序列号
    ↓
构建 InternalKey
    ↓
调用 WalManager::append()
    ↓
若全局缓冲超过 `max_buffer_bytes`，等待后台写入释放空间
    ↓
根据线程 ID 哈希选择分片缓冲区
    ↓
编码记录到分片缓冲区
    ↓
更新全局缓冲区大小统计
    ↓
唤醒后台写入线程
    ↓
更新 MemTable
    ↓
返回客户端成功
    ↓
（后台）定时或定量刷盘
    ↓
（后台）合并分片缓冲区
    ↓
（后台）批量写入磁盘并 flush（不做 `sync_data`）
```

### 性能优化

1. **分片缓冲区**
   - 每个写入线程使用独立的缓冲区
   - 减少锁竞争
   - 缓冲区数量 = CPU 核心数 × 2

2. **批量合并**
   - 后台线程定期合并所有分片缓冲区
   - 单次大块顺序写入
   - 减少系统调用开销

3. **背压控制**
   - 全局缓冲区大小限制
   - 达到阈值时阻塞写入
   - 防止内存无限制增长

## 🔄 读取与恢复路径设计

### 正常读取（迭代器模式）

```rust
let mut reader = WalReader::new(&wal_path)?;
while let Some(entry) = reader.next() {
    match entry {
        Ok((key, value)) => {
            // 处理有效记录
        }
        Err(WalError::ChecksumMismatch { key }) => {
            // 校验和失败，停止读取
            break;
        }
        Err(e) => {
            // 其他错误处理
            return Err(e);
        }
    }
}
```

### 崩溃恢复流程

```
启动时检查 recover_from_wal 选项
    ↓
扫描 WAL 目录查找日志文件
    ↓
按日志编号排序（低到高）
    ↓
（若存在 `goatdb.wal` 且 log_number=0，会一并读取）
    ↓
对每个 WAL 文件执行：
    ↓
打开文件准备读取和截断
    ↓
逐记录读取直到 EOF
    ↓
验证每个记录的校验和
    ↓
有效记录 → 重建到内存表
    ↓
校验和失败 → 截断文件到最后一个有效记录
    ↓
记录最大序列号和条目数
    ↓
完成所有文件恢复
    ↓
选择新的 WAL 编号（max(log_number) + 1）
    ↓
创建新的 WAL 文件用于后续写入
    ↓
旧 WAL 文件保留；待对应 memtable flush 完成后由 WalHandle 触发清理
```

### 部分写入处理

WAL 恢复能处理以下异常情况：

1. **部分记录写入**
   - 读取时检测到 EOF
   - 截断文件到最后一个完整记录
   - 标记恢复为 "truncated"

2. **校验和失败**
   - 检测到数据损坏
   - 截断文件到损坏记录之前
   - 确保数据一致性

3. **无效键长度**
   - 键长度 < 8 字节
   - 视为文件损坏
   - 安全截断

## ⚙️ 配置与调优

### 引擎选项

```rust
pub struct KvEngineOptions {
    // ... 其他选项
    pub recover_from_wal: bool,      // 启动时是否从 WAL 恢复
    pub wal_sync: bool,              // 是否同步刷盘
    pub wal_sync_interval_ms: u64,   // 异步刷盘间隔（毫秒）
    pub wal_sync_bytes: usize,       // 触发刷盘的字节阈值
    pub wal_max_buffer_bytes: usize, // WAL 最大缓冲区大小
    pub data_dir: PathBuf,           // 数据目录（包含 WAL）
}
```

### WalManager 配置

```rust
pub struct WalManagerConfig {
    pub wal_sync: bool,              // 同步刷盘
    pub sync_interval_ms: u64,       // 异步刷盘间隔（毫秒）
    pub sync_bytes: usize,           // 触发刷盘的字节阈值
    pub max_buffer_bytes: usize,     // 最大缓冲区大小
}
```

### 性能调优建议

1. **高吞吐场景**
   - `wal_sync = false`
   - `sync_interval_ms = 10-100ms`
   - `max_buffer_bytes = 64-256MB`
   - 权衡：性能 vs 数据丢失窗口

2. **强持久性场景**
   - `wal_sync = true`
   - 每次写入都会等待其数据被 flush + sync（可批量合并）
   - 性能较低但保证持久性

3. **混合模式**
   - 使用默认配置（`sync_interval_ms = 10ms`）
   - 平衡性能和持久性

## 🔧 错误处理与恢复

### 错误类型

```rust
pub enum WalError {
    Io(io::Error),                    // I/O 错误
    ChecksumMismatch { key: Vec<u8> }, // 校验和不匹配
    InvalidKeyLen,                    // 无效键长度
    UnexpectedEof,                    // 意外 EOF
}
```

### 恢复策略

1. **校验和失败**
   - 立即停止读取当前文件
   - 截断到最后一个有效记录
   - 警告日志记录

2. **I/O 错误**
   - 读取路径：直接返回错误给上层
   - 写入路径：WalManager 关闭并传播错误（后续写入会失败）

3. **磁盘空间不足**
   - 作为 I/O 错误处理（当前实现无自动降级/只读模式）

## 🔄 与 LSM-Tree 集成

### 写入流程集成

```
KvEngine::put()
 / KvEngine::delete()
    ↓
获取序列号
    ↓
构建 InternalKey
    ↓
WAL 写入（`WalManager::append`）
    ↓
内存表插入（易失）
    ↓
若达到阈值则触发 `flush()`
    ↓
返回成功
```

### 内存表刷盘协调

```
MemTable 达到容量阈值，`KvEngine::flush()` 触发
    ↓
（持有写锁）先请求 WAL 轮转，切换到新文件
    ↓
创建不可变内存表快照
    ↓
提交 FlushTask 给后台线程
    ↓
SSTable 写入完成
    ↓
更新 VersionSet（新文件）
    ↓
从 immutable 队列移除（触发 WalHandle Drop）
    ↓
异步清理旧 WAL 文件
```

### WAL 轮转机制

1. **触发条件**
   - MemTable 触发 `flush()`（在封存 memtable 之前轮转）
   - 显式调用 `WalManager::rotate()`（目前无基于文件大小的自动轮转）

2. **轮转流程**
   ```rust
   // 请求轮转
   wal_manager.rotate(new_wal_path)?;
   
   // 后台执行
   // 1. 刷新当前缓冲区
   // 2. 同步到磁盘（如配置）
   // 3. 打开新文件
   // 4. 继续写入新文件
   ```

3. **清理策略**
   - 通过 `WalHandle` 管理生命周期
   - 对应 immutable memtable 刷盘完成后自动删除旧 WAL
   - 防止过早删除（崩溃恢复需要）

## 📁 文件管理与命名

### 目录结构

```
goatdb_data/
├── wal/
│   ├── 000001.wal
│   ├── 000002.wal
│   └── goatdb.wal    # 可选：旧主 WAL 文件（兼容读取）
├── data/             # SSTable 文件
├── log/              # 操作日志
└── tmp/              # 临时文件
```

### 命名规则

```rust
// 日志文件命名
if log_number < 1_000_000 {
    format!("{:06}.wal", log_number)  // 000001.wal
} else {
    format!("{}.wal", log_number)     // 1234567.wal
}
```

> 说明：`log_number = 0` 对应 `000000.wal`。目录中可能还存在 `goatdb.wal`（旧主 WAL 文件，仅用于兼容读取）。

### 文件管理

1. **当前日志文件**
   - 正在写入的活跃文件
   - 命名：`{log_number}.wal`

2. **归档日志文件**
   - 已完成刷盘的旧文件
   - 等待清理

3. **清理策略**
   - 异步后台清理
   - 仅在 WAL 轮转成功且 `log_number > 0` 时创建 `WalHandle`
   - `WalHandle` 在对应 immutable memtable flush 完成后触发删除
   - 关闭期间禁用清理（避免误删）

## 🧪 测试与验证

### 单元测试覆盖

1. **格式验证**
   - 校验和计算正确性
   - 记录编码/解码
   - 边界条件处理

2. **写入/读取测试**
   - WalWriter 写入与 WalReader 读取
   - 校验和错误检测

3. **恢复测试**
   - 截断尾部恢复
   - 多 WAL 顺序回放

### 集成测试

1. **端到端测试**
   - WAL 截断恢复
   - 多 WAL 回放顺序
   - log_number 推进与“未完成 flush”场景

## 📊 性能特性

### 优势

1. **高吞吐量**
   - 分片缓冲区减少锁竞争
   - 批量顺序写入
   - 异步刷盘重叠 I/O

2. **低延迟**
   - `wal_sync=false` 时可立即确认写入并由后台持久化
   - `wal_sync=true` 时延迟取决于 flush/sync 周期
   - 可配置持久性级别

3. **可扩展性**
   - 分片缓冲降低写入锁竞争
   - 实际吞吐受单写线程与磁盘带宽限制

### 限制

1. **同步模式性能**
   - `fsync` 操作昂贵
   - 受磁盘性能限制
   - 建议使用带电池的 RAID 控制器

2. **内存使用**
   - 缓冲区占用内存
   - 背压控制防止 OOM
   - 可配置上限

3. **恢复时间**
   - 与 WAL 文件大小相关
   - 大文件恢复较慢
   - 定期刷盘减少恢复时间

## 🔮 未来优化方向

### 短期优化

1. **压缩支持**
   - 记录级压缩
   - 块级压缩
   - 可配置压缩算法

2. **加密支持**
   - 透明数据加密
   - 记录级加密
   - 密钥管理集成

### 长期演进

1. **分布式 WAL**
   - 多副本持久化
   - 跨节点复制
   - 一致性协议集成

2. **分层存储**
   - 热/温/冷数据分离
   - SSD/HDD 分层
   - 自动数据迁移

3. **智能缓冲**
   - 机器学习预测模式
   - 自适应缓冲区大小
   - 智能预取

---

## 📚 相关文档

- [SSTable 格式规范](../storage/sstable_format.md)
- [跳表实现详解](../core/skip_list_implementation.md)
- [LSM-Tree 架构概述](../core/lsm_architecture.md)

## 🔗 源码参考

- `src/goatkv/storage/wal/` - WAL 核心实现
- `src/goatkv/core/wal_handle.rs` - WAL 生命周期管理
- `tests/integration/recovery_test.rs` - WAL 恢复相关集成测试
- `benches/goatkv_bench.rs` - 基础基准测试（非 WAL 专用）
