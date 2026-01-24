# 崩溃恢复（WAL + MANIFEST）

本文档说明 GoatDB 当前实现的崩溃恢复原理与流程，包括相关的磁盘组件、
恢复算法以及可覆盖的失败场景。内容对应 `src/goatkv/` 现有代码。

## 1）磁盘组件与目录结构

由路径集合（WalPaths/SstablePaths/ManifestPaths）管理的目录布局：

- `data/`：SSTable（`*.sst`）与 MANIFEST（如 `MANIFEST-0`）
- `wal/`：WAL 文件（`000001.wal`、`000002.wal` …）。历史主 WAL `goatdb.wal`
  视为 log number 0。
- `tmp/`：SSTable 构建过程中的临时文件
- `log/`：日志（如启用）
- `CURRENT`：指向当前 MANIFEST 的小文件

关键文件：

- **WAL**：写前日志，记录每次 Put/Delete
- **MANIFEST**：`VersionEdit` 的追加日志，描述 SSTable 集合与全局元数据
- **CURRENT**：指向当前 MANIFEST 文件

## 2）WAL 记录格式（概要）

每条记录顺序：

- CRC32 校验（u32，小端）
- InternalKey 长度（u32，小端）
- User key 字节
- 编码后的序列号（u64，小端，低 8 位为 Kind）
- Value 长度（u32，小端）
- Value 字节

校验和覆盖除自身之外的所有字段。

## 3）正常写入路径（稳态）

1) 客户端发起 Put/Delete。
2) 先写 WAL（是否 fsync 由 `wal_sync` 决定）。
3) 再写入可变 MemTable。
4) MemTable 达到阈值后封存为 Immutable MemTable，后台 `FlushWorker`
   开始构建 SSTable。
5) SSTable 写入并 sync 后，生成 `VersionEdit` 追加到 MANIFEST 并 fsync。
6) WAL 轮转发生在 flush 触发时；flush 提交成功后旧 WAL 才可能被删除
   （依赖 refcount 保护）。

## 4）恢复入口

当 `options.recover_from_wal = true` 时，恢复在
`KvEngine::new_with_options()` 中执行。

高层步骤：

1) 初始化路径（`init_db_paths`），清理临时目录。
2) 打开 `VersionSet`，回放 MANIFEST 并校验 SSTable。
3) 从 MANIFEST 取出 `min_log_number`（当前持久化的 WAL 边界）。
4) 从 `min_log_number` 起按序回放 WAL。
5) 创建新的 WAL 供后续写入。
6) 为恢复出的 Immutable MemTable 提交后台 flush。

## 5）MANIFEST 恢复细节

`VersionSet::open()` 执行：

- 读取 `CURRENT` 找到 MANIFEST（缺失时创建）。
- 依次回放 MANIFEST 的 `VersionEdit`。
- 校验：文件范围、L1+ 不重叠、SSTable 大小与格式。
- 打开 MANIFEST 以追加。

MANIFEST 尾部若存在半条记录，会被截断到最后一个完整偏移。
若记录可读但无法解码（如未知 tag），恢复返回 `InvalidData`。

## 6）WAL 回放细节

通过 `KvEngine::replay_into_state()`：

1) 收集 `min_log_number` 及以上的 WAL（log=0 时包含 `goatdb.wal`）。
2) 按 log number 排序依次回放。
3) 对每条记录：
   - 写入可变 MemTable。
   - MemTable 达到阈值时封存为 Immutable MemTable，并记录当前 WAL 号。
4) 每个 WAL 回放完毕后，将剩余 MemTable 封存成 Immutable MemTable，
   以保持 WAL 边界清晰。

`replay_wal_file` 的损坏处理：

- 尾部半条记录或校验和不匹配：截断到最后完整偏移，并标记 `truncated`。
- 截断后继续回放后续 WAL 文件。

## 7）新 WAL 号选择策略

回放完成后选取新的 WAL 编号：

```
current_log_number = max(version_set.log_number(>=1), wal_max_number + 1)
```

关键点：**恢复阶段不推进 MANIFEST 中的 log_number**，log_number 只在
成功 flush 并提交 MANIFEST 后推进。这样可避免“恢复后未 flush 又崩溃”
导致跳过旧 WAL 的数据丢失问题。

## 8）恢复后 flush 调度

恢复得到的 Immutable MemTable 会立即提交给 `FlushWorker`。每个条目
携带 `wal_log_number`，以保证：

- 依赖旧 WAL 的 memtable 未 flush 前，不会删除旧 WAL。
- 通过 WAL refcount 延迟删除。

## 9）覆盖的崩溃场景

1) **WAL 写入过程中崩溃**
   - 半条记录或校验失败会被截断。
   - 之前的完整记录可恢复。

2) **MANIFEST 追加过程中崩溃**
   - 半条 edit 被截断，前序 edit 可恢复。

3) **flush 已开始但 MANIFEST 未提交**
   - MANIFEST log_number 未推进，旧 WAL 仍保留。
   - 恢复时会回放 WAL，确保不丢数据。

4) **恢复后尚未 flush 又再次崩溃**
   - MANIFEST log_number 未推进。
   - 下次启动仍会回放旧 WAL，不会跳过数据。

## 10）一致性与耐久性说明

- `wal_sync = true` 时，WAL 每次写入都会 fsync，已确认写入具有更强耐久性。
- `wal_sync = false` 时，崩溃可能丢最近写入（预期行为）。
- 若 MANIFEST 尚未推进，恢复可能回放已落盘的数据，造成冗余 L0 文件，
  后续 compaction 可清理。

## 11）当前限制与可改进点

- WAL/MANIFEST 的长度字段缺少硬上限，严重损坏可能造成 OOM。
- L0 SSTable 读取顺序应优先最新文件（与恢复无关但影响正确性）。
- 可考虑显式持久化 `min_log_number` / `prev_log_number` 以进一步明确边界。
