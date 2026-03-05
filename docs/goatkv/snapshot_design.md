# GoatKV 快照能力设计（v1 草案）

更新时间：2026-03-05

## 1. 目标

- 提供显式快照能力（Create/Release），支持“按快照读”。
- 快照读语义与 RocksDB 对齐：读取 `seq <= snapshot_seq` 的最新版本。
- 在 flush/compaction 持续运行时保持快照可见性正确。
- 首期聚焦点查（`Get`）闭环；`Scan/CAS` 在同一语义基础上扩展。

## 2. RocksDB 参考与对齐点

### 2.1 对外语义

- `ReadOptions::snapshot`：`rocksdb/include/rocksdb/options.h:1949`
  - `snapshot == nullptr` 时，读请求使用隐式快照。
- `DB::GetSnapshot/ReleaseSnapshot`：`rocksdb/include/rocksdb/db.h:1148`

### 2.2 读路径关键顺序

- `DBImpl::GetImpl`：`rocksdb/db/db_impl/db_impl.cc:2382`
  - 先获取 `SuperVersion`，再决定隐式 snapshot seq，避免读视图与序列窗口错位。
  - `LookupKey(key, snapshot_seq, ...)` 在 mem/imm/version 上统一按 seq 可见性过滤。

### 2.3 快照管理

- `SnapshotList`：`rocksdb/db/snapshot_impl.h:54`
  - 维护活跃快照链表、最老快照、去重后的快照序列集合。

### 2.4 Compaction 与快照

- `CompactionIterator`：`rocksdb/db/compaction/compaction_iterator.cc:866`
  - 通过“snapshot stripe（可见性分层）”丢弃不会被任何快照看到的版本（rule A）。
  - 不以“同 user_key 只保留一条”粗暴折叠历史版本。

## 3. GoatKV 当前差距（与快照直接相关）

- 当前 `Get` 仅查询“最新状态”，不支持指定 `read_seq`：
  - `src/goatkv/core/kv_engine/reader.rs:21`
- MemTable/SSTable 点查默认返回同 key 最新版本：
  - `src/goatkv/core/mem_table.rs:35`
  - `src/goatkv/storage/sstable/reader.rs:646`
  - `src/goatkv/storage/sstable/block_reader.rs:245`
- Compaction 当前按 `user_key` 无条件去重，只保留一条：
  - `src/goatkv/core/flush_worker.rs:858`
  - 该行为会破坏历史快照可见性。
- 对外 API 无快照创建/释放接口：
  - `proto/goatkv.proto:4`
- 写路径虽维护了“已发布可见序列号”但未对外暴露 getter：
  - `src/goatkv/core/kv_engine/writer.rs:220`

## 4. 语义定义（GoatKV v1）

### 4.1 快照定义

- 快照是一个 `(snapshot_id, snapshot_seq)` 映射。
- `snapshot_seq` 取自写路径已发布可见水位（`last_published_seq`），而非已分配未发布序列。
- 快照仅内存态，不持久化；进程重启后全部失效。

### 4.2 可见性规则

- 对任意 `read_seq`，返回 `user_key` 下 `seq <= read_seq` 的最新版本：
  - `Put` => 返回值；
  - `Delete` => 返回 NotFound（逻辑删除）；
  - 不存在 `seq <= read_seq` => 继续向更老层查找（或最终 NotFound）。

### 4.3 隐式快照

- 未指定 `snapshot_id` 的读取使用隐式快照（读取瞬间的可见 seq）。
- 与 RocksDB 一致，隐式快照不需要显式创建与释放。

## 5. 架构设计

### 5.1 SnapshotManager（新增）

- 建议新增模块：`src/goatkv/core/snapshot_manager.rs`
- 结构：
  - `next_snapshot_id: u64`
  - `by_id: HashMap<u64, SnapshotEntry>`
  - `seq_refcnt: BTreeMap<u64, usize>`（支持多个快照指向同一 seq）
- 关键接口：
  - `create(seq) -> SnapshotHandle { id, seq }`
  - `release(id) -> bool`
  - `lookup_seq(id) -> Option<u64>`
  - `snapshot_seqs_sorted() -> Vec<u64>`
  - `oldest_snapshot_seq() -> Option<u64>`
  - `active_count() -> usize`

### 5.2 Engine 与 Writer 协作

- `KvWriter` 新增只读接口：
  - `last_published_sequence() -> u64`
- `KvEngine` 新增：
  - `create_snapshot() -> GoatResult<SnapshotHandle>`
  - `release_snapshot(snapshot_id: u64) -> GoatResult<()>`
  - `get_with_snapshot_id(key, snapshot_id) -> GoatResult<Option<Vec<u8>>>`

### 5.3 读路径改造（按 seq）

- `KvReader` 改为按 `read_seq` 查询：
  - `get_at_seq(key, read_seq)`
- `MemTable/ImmutableMemTable` 增加：
  - `get_pinned_at_seq(key, read_seq)`
  - 行为：扫描同 user_key 版本链，返回首个 `seq <= read_seq`。
- `Version` 增加：
  - `get_pinned_at_seq(key, read_seq)`

### 5.4 SSTable/BlockReader 改造（按 seq）

- `SSTableReader` 增加：
  - `get_pinned_at_seq(key, read_seq)`
- `BlockReader` 增加：
  - `get_by_user_key_with_value_range_at_seq(user_key, read_seq)`
- 文件级快速过滤（已有元数据可复用）：
  - 若 `file.props.smallest_seqno > read_seq`，该文件可直接跳过。
  - 状态：2026-03-05 已实现块内按 `read_seq` 命中并返回 pinned value，且在 `read_seq >= file.props.largest_seqno` 时走普通点查快路径。

### 5.5 Row cache 语义

- v1（先保证正确性）：显式快照读不走 row cache（只用于隐式最新读）。
- v2（优化）：row cache key 增加 `read_seq` 维度（或与 RocksDB 一样按文件/seq 构造可见性键前缀）。
  - 状态：2026-03-05 已实现 `row cache key = (version_seqno, read_seq, user_key)`，`Get(read_seq)` 路径启用 row cache 并按可见性序列隔离。

### 5.6 Compaction 保留规则（核心）

当前 `last_emitted_user_key` 去重必须替换为“快照条带（stripe）保留”：

- 输入：活跃快照序列 `S = [s1 < s2 < ... < sn]`。
- 定义：版本 `seq=v` 的可见条带 `stripe(v)`：
  - `stripe(v) = first s_i where s_i >= v`
  - 若不存在，则 `stripe(v) = +INF`（仅“最新视图”可见）
- 对同一 `user_key`，按 seq 降序扫描时：
  - 当 `stripe(v)` 与上一个已输出版本相同 -> 可丢弃；
  - 否则保留该版本。

伪代码：

```text
for key_group in merged_entries.group_by(user_key):
    last_emitted_stripe = None
    for entry in key_group (seq desc):
        stripe = first_snapshot_ge(entry.seq) or INF
        if last_emitted_stripe == Some(stripe):
            drop(entry)   // 该版本不会被任何快照单独观察到
        else:
            emit(entry)
            last_emitted_stripe = Some(stripe)
```

这个规则与 RocksDB `CompactionIterator` 的 rule A 一致，能够在保证正确性的同时避免“保留全部历史版本”。

### 5.7 Compaction 输入快照时机

- 每次 compaction job 启动时，从 `SnapshotManager` 获取排序后的 `snapshot_seqs` 快照并固定在本 job 内。
- 与 RocksDB 一致，不要求运行中动态更新该列表；新建快照的 seq 总是晚于已发布可见水位，不会要求恢复已被安全丢弃的旧版本。

## 6. 对外接口设计（proto / gRPC）

建议在 `proto/goatkv.proto` 增加：

- RPC：
  - `CreateSnapshot(CreateSnapshotRequest) returns (CreateSnapshotResponse)`
  - `ReleaseSnapshot(ReleaseSnapshotRequest) returns (ReleaseSnapshotResponse)`
- `GetRequest` 增加 `snapshot_id` 字段（`0` 表示隐式最新读）。

返回语义：

- 非法/已释放 `snapshot_id`：返回 `NotFound(snapshot)`。
- 快照数超限（若配置 `max_active_snapshots`）：返回 `Unavailable(snapshot_limit)`。

## 7. 分阶段落地计划

### Phase 1（正确性闭环，必须）

- SnapshotManager + Engine/Writer 接口。
- `Get(read_seq)` 全链路（mem/imm/version/sstable/block）按 seq 可见性过滤。
- Compaction 从“按 key 只保留一条”改为“按 snapshot stripe 保留”。
- 显式快照读先禁用 row cache。

涉及文件（预估）：

- `src/goatkv/core/kv_engine/engine.rs`
- `src/goatkv/core/kv_engine/reader.rs`
- `src/goatkv/core/kv_engine/writer.rs`
- `src/goatkv/core/mem_table.rs`
- `src/goatkv/metadata/version.rs`
- `src/goatkv/storage/sstable/reader.rs`
- `src/goatkv/storage/sstable/block_reader.rs`
- `src/goatkv/core/flush_worker.rs`
- `src/goatkv/core/snapshot_manager.rs`（新增）

### Phase 2（API 能力对外）

- `proto/goatkv.proto`、server/client 接入快照 API。
- `goatkv_client` 增加 snapshot create/release 与 snapshot get 命令。

### Phase 3（性能补齐）

- 快照读 row cache 可见性键设计。
- 读路径文件级 seq 过滤与块级扫描微优化。
- 可观测指标与告警阈值。

## 8. 测试计划

### 8.1 单元测试

- SnapshotManager：create/release/refcnt/oldest_seq 正确性。
- MemTable：同 key 多版本下 `get_at_seq` 返回正确版本。
- BlockReader：跨 restart boundary 的 `get_by_user_key_at_seq`。
- Compaction stripe 规则：多快照、多版本下保留集合正确。

### 8.2 集成测试

- `snapshot_get_sees_old_value_after_put`
- `snapshot_get_sees_old_state_after_delete`
- `snapshot_survives_flush_and_compaction`
- `release_snapshot_allows_history_gc`（释放后经 compaction 历史版本回收）

### 8.3 并发与回归

- 写入并发创建快照：快照 seq 单调且只读取已发布数据。
- 快照大量持有时 compaction 不违反可见性（可做随机对拍）。

## 9. 风险与约束

- 长时间持有快照会增加空间放大与 compaction 压力（与 RocksDB 一致）。
- v1 不做快照持久化，重启后快照句柄失效（需在接口文档明确）。
- 若只实现读路径而未改 compaction 保留规则，会产生“快照读偶发错读/丢历史”风险；因此 Phase 1 必须整体交付。
