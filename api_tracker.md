# GoatKV API Tracker

## 目标

本文记录 GoatKV 当前对外暴露接口的测试方案。目标不是只验证“接口返回值对不对”，而是同时验证：

- 接口契约：返回码、错误语义、返回数据、顺序、幂等性。
- 最新视图：调用后数据库对最新读路径是否符合预期。
- 快照视图：涉及版本可见性的接口是否保持 snapshot 一致性。
- 持久化状态：重启恢复后数据是否仍然符合预期。
- 存储结构变化：WAL、memtable、immutable memtable、SSTable、manifest 是否发生了正确变化。
- 运行时指标变化：只读接口不应误写；读缓存、backlog、L0 文件数等指标应与行为一致。

## 覆盖范围

### gRPC 接口

- `Write`
- `Get`
- `MultiGet`
- `Scan`
- `CompareAndSet`
- `Update`
- `Delete`
- `Flush`
- `CreateSnapshot`
- `ReleaseSnapshot`

### 嵌入式 `KvEngine` 公共接口

- `KvEngine::init_db_paths`
- `KvEngine::new`
- `KvEngine::new_with_options`
- `KvEngine::wal_paths`
- `KvEngine::sstable_paths`
- `KvEngine::manifest_paths`
- `KvEngine::get`
- `KvEngine::multi_get`
- `KvEngine::create_snapshot`
- `KvEngine::release_snapshot`
- `KvEngine::get_with_snapshot`
- `KvEngine::scan`
- `KvEngine::scan_with_snapshot`
- `KvEngine::put`
- `KvEngine::commit_batch`
- `KvEngine::put_batch`
- `KvEngine::delete`
- `KvEngine::compare_and_set`
- `KvEngine::with_transaction`
- `KvEngine::runtime_metrics`
- `KvEngine::read_cache_metrics`
- `KvEngine::flush`
- `KvEngine::shutdown`

### 事务子接口

- `EngineTransaction::get`
- `EngineTransaction::scan`
- `EngineTransaction::put`
- `EngineTransaction::delete`
- `EngineTransaction::compare_and_set`
- `EngineTransaction::commit`
- `EngineTransaction::rollback`

### 不在本文范围

- `KvEngine::new_for_test`：测试辅助构造器，不属于生产暴露接口。
- 私有辅助函数、路径内部工具、`Drop` 隐式行为。

## 通用测试方法

### 统一观测点

- `接口断言`
  - gRPC：`StatusCode`、`success`、`message`、返回内容。
  - engine：`GoatResult<T>`、错误 `kind()`、返回内容。
- `最新状态断言`
  - 通过 `get`、`multi_get`、`scan` 验证最新可见数据。
- `快照状态断言`
  - 通过 `create_snapshot` 后再读，验证旧值、旧可见集、旧排序。
- `持久化状态断言`
  - 关闭引擎并用相同 `data_dir` 重开；验证结果不变。
- `存储结构断言`
  - 记录 `wal/` 文件集合、文件大小。
  - 记录 `data/` 目录 `.sst` 数量。
  - 记录 `runtime_metrics().immutable_memtable_backlog`、`l0_file_count`。
- `只读无副作用断言`
  - 对 `Get`、`MultiGet`、`Scan`、`runtime_metrics`、`read_cache_metrics`，对比调用前后 WAL 文件大小、SST 数量、最新读结果，确认没有数据面副作用。
- `恢复断言`
  - 对写接口，至少覆盖一次 “写入后未 flush 重启恢复” 和一次 “flush 后重启恢复”。
- `并发断言`
  - 对 `CAS`、事务、批量提交，验证热点 key 不丢更新、不半提交。

### 建议测试层级标签

- `[UT]`：库级单元测试，直接调用 `KvEngine`。
- `[IT]`：文件结构/恢复/故障注入测试。
- `[E2E]`：gRPC 端到端测试，覆盖传输层契约。

## 接口覆盖矩阵

- `Write`：`PUT-UT-001`、`PUT-E2E-003`、`PUT-E2E-005`
- `Get`：`GET-UT-001`、`GET-UT-002`、`GET-E2E-003`、`GET-E2E-004`
- `MultiGet`：`MGET-UT-001`、`MGET-UT-002`、`MGET-E2E-003`、`MGET-E2E-004`、`MGET-E2E-005`
- `Scan`：`SCAN-UT-001`、`SCAN-UT-002`、`SCAN-UT-003`、`SCAN-UT-005`、`SCAN-E2E-004`、`SCAN-E2E-006`
- `CompareAndSet`：`CAS-UT-001`、`CAS-UT-002`、`CAS-UT-003`、`CAS-UT-005`、`CAS-E2E-004`、`CAS-E2E-006`
- `Update`：`PUT-E2E-003`、`UPDATE-E2E-004`、`PUT-E2E-005`
- `Delete`：`DEL-UT-001`、`DEL-UT-002`、`DEL-E2E-003`、`DEL-E2E-004`
- `Flush`：`FLUSH-UT-001`、`FLUSH-UT-002`、`FLUSH-IT-003`、`FLUSH-E2E-004`
- `CreateSnapshot`：`SNAP-UT-001`、`SNAP-UT-004`、`SNAP-E2E-005`
- `ReleaseSnapshot`：`SNAP-UT-002`、`SNAP-E2E-005`、`SNAP-E2E-006`
- `KvEngine::init_db_paths`：`BOOT-UT-001`
- `KvEngine::new`：`BOOT-UT-004`
- `KvEngine::new_with_options`：`BOOT-UT-001`、`BOOT-IT-002`、`BOOT-IT-003`、`BOOT-UT-005`
- `KvEngine::wal_paths`：`BOOT-UT-001`、`BOOT-UT-006`
- `KvEngine::sstable_paths`：`BOOT-UT-001`、`BOOT-UT-006`
- `KvEngine::manifest_paths`：`BOOT-UT-001`、`BOOT-UT-006`
- `KvEngine::get`：`GET-UT-001`
- `KvEngine::multi_get`：`MGET-UT-001`、`MGET-UT-002`
- `KvEngine::create_snapshot`：`PUT-UT-002`、`SNAP-UT-001`、`SNAP-UT-004`
- `KvEngine::release_snapshot`：`SNAP-UT-002`
- `KvEngine::get_with_snapshot`：`PUT-UT-002`、`GET-UT-002`、`SNAP-UT-001`
- `KvEngine::scan`：`SCAN-UT-001`、`SCAN-UT-002`、`SCAN-UT-005`
- `KvEngine::scan_with_snapshot`：`PUT-UT-002`、`SCAN-UT-003`、`SNAP-UT-001`
- `KvEngine::put`：`PUT-UT-001`、`PUT-UT-002`
- `KvEngine::commit_batch`：`BATCH-UT-001`、`BATCH-IT-002`、`BATCH-UT-004`
- `KvEngine::put_batch`：`BATCH-UT-003`、`BATCH-UT-004`
- `KvEngine::delete`：`DEL-UT-001`、`DEL-UT-002`
- `KvEngine::compare_and_set`：`CAS-UT-001`、`CAS-UT-002`、`CAS-UT-003`、`CAS-UT-005`
- `KvEngine::with_transaction`：`TXN-UT-001`、`TXN-UT-002`、`TXN-UT-003`、`TXN-UT-006`、`TXN-UT-008`、`TXN-UT-009`
- `KvEngine::runtime_metrics`：`METRIC-UT-001`、`METRIC-UT-003`
- `KvEngine::read_cache_metrics`：`GET-UT-002`、`MGET-UT-002`、`METRIC-UT-002`、`METRIC-UT-003`
- `KvEngine::flush`：`FLUSH-UT-001`、`FLUSH-UT-002`、`FLUSH-IT-003`
- `KvEngine::shutdown`：`SHUT-UT-001`、`SHUT-UT-002`、`SHUT-UT-003`
- `EngineTransaction::get`：`TXN-UT-001`
- `EngineTransaction::scan`：`TXN-UT-001`
- `EngineTransaction::put`：`TXN-UT-001`、`TXN-UT-002`
- `EngineTransaction::delete`：`TXN-UT-001`、`TXN-UT-002`
- `EngineTransaction::compare_and_set`：`TXN-UT-004`
- `EngineTransaction::commit`：`TXN-UT-002`、`TXN-IT-007`、`TXN-UT-009`
- `EngineTransaction::rollback`：`TXN-UT-003`

## 接口测试方案

### 1. 初始化与路径接口

覆盖接口：

- `KvEngine::init_db_paths`
- `KvEngine::new`
- `KvEngine::new_with_options`
- `KvEngine::wal_paths`
- `KvEngine::sstable_paths`
- `KvEngine::manifest_paths`

测试项：

- [x] `BOOT-UT-001` Fresh bootstrap creates complete directory layout
  - 前置：空临时目录。
  - 操作：调用 `KvEngine::init_db_paths`，随后 `KvEngine::new_with_options`。
  - 接口断言：
    - 返回 `Ok`.
    - `wal_paths/sstable_paths/manifest_paths` 可访问。
  - 数据库状态断言：
    - `wal/`、`data/`、`tmp/`、`log/` 目录存在。
    - 当前 WAL 文件已创建。
    - 尚无用户数据时，`scan(default)` 返回空。

- [x] `BOOT-IT-002` Reopen recovers data from existing WAL and manifest
  - 前置：同一 `data_dir` 下先写入若干 key，但不主动 flush。
  - 操作：销毁引擎，再用相同 `data_dir` 调用 `new_with_options`。
  - 接口断言：
    - 重开成功。
    - `get/scan` 与关闭前一致。
  - 数据库状态断言：
    - WAL 恢复后数据可见。
    - 新引擎打开后 WAL 编号向前推进，不覆盖旧日志。

- [x] `BOOT-IT-003` Invalid storage state fails fast without silent partial open
  - 前置：构造损坏 manifest 或损坏 WAL。
  - 操作：调用 `new_with_options`。
  - 接口断言：
    - 返回 `Err`，错误类型符合预期。
  - 数据库状态断言：
    - 不能出现“部分 key 可读、部分 key 消失但启动成功”的 silent corruption。
    - 对尾部截断类损坏，只允许按恢复规则截断后成功启动。

- [x] `BOOT-UT-004` Default constructor uses `./goatdb_data` and does not touch unrelated directories
  - 前置：切换到临时工作目录，保证目录下不存在 `goatdb_data/`。
  - 操作：调用 `KvEngine::new`。
  - 接口断言：
    - 构造成功。
    - `wal_paths/sstable_paths/manifest_paths` 都位于当前工作目录下的 `goatdb_data/`。
  - 数据库状态断言：
    - 当前工作目录生成 `goatdb_data/{wal,data,log,tmp}`。
    - 未指定的其他目录没有被创建或写入。

- [x] `BOOT-UT-005` Custom options isolate storage into requested `data_dir`
  - 前置：准备 `dir_a`、`dir_b` 两个独立临时目录。
  - 操作：用 `dir_a` 构造引擎并写入数据，再用 `dir_b` 构造第二个空引擎。
  - 接口断言：
    - 两个引擎都能成功启动。
    - `dir_b` 引擎读不到 `dir_a` 的数据。
  - 数据库状态断言：
    - `dir_a` 与 `dir_b` 各自生成独立 `wal/data/log/tmp`。
    - `dir_a` 的 WAL/SST/manifest 变化不会污染 `dir_b`。

- [x] `BOOT-UT-006` Path accessors stay stable and point to real artifacts across reopen
  - 前置：写入若干 key，触发一次 `flush`。
  - 操作：记录 `wal_paths/sstable_paths/manifest_paths` 指向的目录和关键文件，再重开引擎。
  - 接口断言：
    - 重开前后 path accessor 返回的基础目录一致。
  - 数据库状态断言：
    - accessor 指向的目录下确实能找到当前 WAL、`CURRENT`/`MANIFEST-*`、`.sst`。
    - 重开后旧数据仍可读。

### 2. `Write` / `Update` / `KvEngine::put`

覆盖接口：

- `Write`
- `Update`
- `KvEngine::put`

测试项：

- [x] `PUT-UT-001` Put on new key writes latest value and survives reopen
  - 前置：空库。
  - 操作：`put("k1","v1")` 或 gRPC `Write`.
  - 接口断言：
    - 返回成功。
    - `get("k1") == "v1"`.
  - 数据库状态断言：
    - WAL 文件大小增长。
    - 未 flush 前 `scan(prefix="k")` 已可见。
    - 重启后仍可见。

- [x] `PUT-UT-002` Overwrite updates latest view but old snapshot still sees previous version
  - 前置：先写入 `"k1"="v1"` 并创建快照。
  - 操作：再次 `put("k1","v2")`.
  - 接口断言：
    - 最新 `get("k1") == "v2"`.
    - `get_with_snapshot("k1", snapshot_id) == "v1"`.
  - 数据库状态断言：
    - `scan(latest)` 返回新值。
    - `scan(snapshot)` 返回旧值。
    - flush + compaction 后快照语义仍成立，直到释放快照。

- [x] `PUT-E2E-003` Transport validation differs from engine contract
  - 前置：空库。
  - 操作：gRPC `Write` 和 `Update` 传空 key；engine 层直接 `put(vec![], value)`.
  - 接口断言：
    - gRPC 返回 `InvalidArgument`.
    - engine 层当前若允许空 key，应明确记录现状；若未来改约束，此用例应随契约更新。
  - 数据库状态断言：
    - 被拒绝的 gRPC 请求不能产生 WAL 追加。
    - 数据库可见集不变。

- [x] `UPDATE-E2E-004` Update behaves as upsert, not compare-and-update
  - 前置：分别准备“key 存在”和“key 不存在”两种场景。
  - 操作：gRPC `Update`.
  - 接口断言：
    - 两种场景都返回成功。
    - message 与 upsert 语义一致。
  - 数据库状态断言：
    - 不存在时生成新 key。
    - 存在时覆盖旧值。
    - 重启后结果保持。

- [x] `PUT-E2E-005` Empty value is stored as a zero-length value, not treated as delete
  - 前置：空库。
  - 操作：分别通过 gRPC `Write` 和 `Update` 写入 `key="k_empty", value=""`。
  - 接口断言：
    - 两个接口都返回成功。
    - `Get(k_empty)` 返回 `success=true` 且 `value.len() == 0`。
  - 数据库状态断言：
    - `scan(prefix="k_")` 中存在 `k_empty`。
    - 重启后仍能读到长度为 0 的值，而不是 miss。

### 3. `Delete` / `KvEngine::delete`

覆盖接口：

- `Delete`
- `KvEngine::delete`

测试项：

- [x] `DEL-UT-001` Delete existing key hides it from latest reads and scans
  - 前置：写入 `"k1"="v1"`.
  - 操作：`delete("k1")`.
  - 接口断言：
    - 返回成功。
    - 最新 `get("k1") == None`.
  - 数据库状态断言：
    - `scan` 中不再出现 `k1`.
    - WAL 增长。
    - 重启后仍不可见。

- [x] `DEL-UT-002` Delete preserves historical snapshot
  - 前置：写入 `"k1"="v1"` 后创建快照。
  - 操作：`delete("k1")`.
  - 接口断言：
    - 最新视图不存在。
    - 快照读仍返回 `"v1"`.
  - 数据库状态断言：
    - flush/compaction 后，未释放快照前仍能读到旧值。

- [x] `DEL-E2E-003` Delete missing key is idempotent success but still tests database invariants
  - 前置：不存在的 key。
  - 操作：gRPC `Delete`.
  - 接口断言：
    - 返回成功。
  - 数据库状态断言：
    - 最新视图仍为不存在。
    - 若实现写 tombstone，则 WAL 可能增长；无论是否增长，重启后都不得出现伪造值。
    - 不得影响其他 key.

- [x] `DEL-E2E-004` Empty key is rejected before reaching storage engine
  - 前置：记录调用前 WAL 文件大小和若干现有 key 的读结果。
  - 操作：gRPC `Delete(key="")`.
  - 接口断言：
    - 返回 `InvalidArgument`。
  - 数据库状态断言：
    - WAL 不增长。
    - 既有数据集完全不变。

### 4. `KvEngine::commit_batch` / `KvEngine::put_batch`

覆盖接口：

- `KvEngine::commit_batch`
- `KvEngine::put_batch`

测试项：

- [x] `BATCH-UT-001` Mixed batch is atomically visible
  - 前置：写入 `k1=old1`, `k2=old2`.
  - 操作：`commit_batch([Put(k1,new1), Delete(k2), Put(k3,new3)])`.
  - 接口断言：
    - 返回成功。
  - 数据库状态断言：
    - 最新视图一次性变为 `{k1=new1, k2=None, k3=new3}`。
    - 不允许出现中间态。
    - 重启后保持一致。

- [x] `BATCH-IT-002` Incomplete tail batch is rolled back during recovery
  - 前置：人工构造批量 WAL，仅写入 marker + 前半段 payload.
  - 操作：重开引擎恢复。
  - 接口断言：
    - 启动成功或按恢复规则成功截断。
  - 数据库状态断言：
    - 整批数据全部不可见。
    - WAL 被回滚到 batch 起点，而不是保留半批。

- [x] `BATCH-UT-003` Duplicate keys inside batch obey sequence order
  - 前置：空库。
  - 操作：`put_batch([("k1","v1"), ("k1","v2")])` 或 `commit_batch([Put(k1,v1), Put(k1,v2)])`.
  - 接口断言：
    - 返回成功。
  - 数据库状态断言：
    - 最新 `get("k1") == "v2"`.
    - 快照若创建在 batch 前，应看不到 batch 结果。

- [x] `BATCH-UT-004` Empty batch is a no-op
  - 前置：记录调用前 WAL 文件大小和 `scan` 结果。
  - 操作：`commit_batch([])` / `put_batch([])`.
  - 接口断言：
    - 返回成功。
  - 数据库状态断言：
    - WAL 文件大小不变。
    - 可见数据集不变。

### 5. `Get` / `KvEngine::get` / `KvEngine::get_with_snapshot`

覆盖接口：

- `Get`
- `KvEngine::get`
- `KvEngine::get_with_snapshot`

测试项：

- [x] `GET-UT-001` Hit and miss semantics are stable
  - 前置：写入 `k1=v1`.
  - 操作：读取 `k1` 与不存在的 `k2`.
  - 接口断言：
    - 命中返回值。
    - 未命中返回 `None` 或 `success=false`.
  - 数据库状态断言：
    - WAL、SST 数量、`immutable_memtable_backlog` 不变。
    - 只读路径不产生数据面副作用。

- [x] `GET-UT-002` Snapshot read is frozen at create time
  - 前置：写入 `k1=v1`，创建快照，再覆盖为 `v2`.
  - 操作：最新读和快照读同时进行。
  - 接口断言：
    - 最新读为 `v2`.
    - 快照读为 `v1`.
  - 数据库状态断言：
    - 快照读不改变任何持久化状态。
    - `read_cache_metrics` 可变化，但数据集不变。

- [x] `GET-E2E-003` Invalid or released snapshot returns NotFound and does not mutate database
  - 前置：创建并释放 snapshot，或使用不存在的 snapshot_id.
  - 操作：gRPC `Get(snapshot_id != 0)`.
  - 接口断言：
    - 返回 `NotFound`.
  - 数据库状态断言：
    - WAL、SST、可见数据集不变。

- [x] `GET-E2E-004` Empty key is rejected and does not produce a read-side mutation
  - 前置：记录调用前 WAL、SST、`read_cache_metrics`。
  - 操作：gRPC `Get(key="")`.
  - 接口断言：
    - 返回 `InvalidArgument`。
  - 数据库状态断言：
    - WAL、SST、不带 snapshot 的最新可见集均不变化。
    - 不应因为参数非法污染读缓存统计。

### 6. `MultiGet` / `KvEngine::multi_get`

覆盖接口：

- `MultiGet`
- `KvEngine::multi_get`

测试项：

- [x] `MGET-UT-001` Mixed hits and misses preserve request order
  - 前置：写入 `k1=v1`, `k3=v3`.
  - 操作：按 `[k1, missing, k3]` 调用 `multi_get`.
  - 接口断言：
    - 返回长度与请求相同。
    - 顺序与请求一致。
  - 数据库状态断言：
    - 只读，无 WAL 变化。

- [x] `MGET-UT-002` Duplicate keys reuse result but do not change state
  - 前置：写入 `dup=v`.
  - 操作：请求 `[dup, dup, dup]`.
  - 接口断言：
    - 三个结果一致。
  - 数据库状态断言：
    - 可选检查 `read_cache_metrics` 的 hit/miss 行为。
    - 数据集不变。

- [x] `MGET-E2E-003` Current transport contract rejects nonzero snapshot_id
  - 前置：任意库状态。
  - 操作：gRPC `MultiGet(snapshot_id=1)`.
  - 接口断言：
    - 返回 `InvalidArgument`.
  - 数据库状态断言：
    - 数据集和 WAL 均不变。

- [x] `MGET-E2E-004` Empty request and empty key are both rejected
  - 前置：任意库状态。
  - 操作：空 `keys`，以及 `keys` 中包含空 key.
  - 接口断言：
    - 都返回 `InvalidArgument`.
  - 数据库状态断言：
    - 数据集不变。

- [x] `MGET-E2E-005` Response entries preserve duplicates and found flags one by one
  - 前置：写入 `k1=v1`，保留一个不存在 key `k_missing`。
  - 操作：gRPC `MultiGet([k1, k_missing, k1])`.
  - 接口断言：
    - 返回 3 条 entry。
    - 第 1 和第 3 条都 `found=true` 且值相同。
    - 第 2 条 `found=false` 且 `message` 明确是 miss。
  - 数据库状态断言：
    - 只读路径不写 WAL。
    - 数据集不变。

### 7. `Scan` / `KvEngine::scan` / `KvEngine::scan_with_snapshot`

覆盖接口：

- `Scan`
- `KvEngine::scan`
- `KvEngine::scan_with_snapshot`

测试项：

- [x] `SCAN-UT-001` Forward scan respects start/end/prefix/limit
  - 前置：写入多组有序 key，如 `a1 a2 a3 b1`.
  - 操作：组合 `start_key`、`end_key`、`prefix`、`limit`.
  - 接口断言：
    - 结果顺序正确。
    - 上界 `end_key` 为排他。
    - `limit=0` 表示不限制。
  - 数据库状态断言：
    - 只读，不写 WAL。

- [x] `SCAN-UT-002` Reverse scan returns reversed visible order and still hides tombstones
  - 前置：写入 `k1 k2 k3`，删除 `k2`.
  - 操作：`scan(reverse=true)`.
  - 接口断言：
    - 返回顺序为 `k3 k1`.
    - `k2` 不出现。
  - 数据库状态断言：
    - 删除记录只影响可见性，不应在 scan 结果中泄漏 tombstone。

- [x] `SCAN-UT-003` Snapshot scan keeps historical visible set
  - 前置：写入一组前缀 key，创建快照，再执行覆盖与删除。
  - 操作：`scan(latest)` 与 `scan_with_snapshot(snapshot)`.
  - 接口断言：
    - 最新 scan 返回新视图。
    - snapshot scan 返回旧视图。
  - 数据库状态断言：
    - flush/compaction 后 snapshot scan 仍稳定。

- [x] `SCAN-E2E-004` Invalid snapshot on gRPC scan returns NotFound without state mutation
  - 前置：释放 snapshot 或使用未知 snapshot_id.
  - 操作：gRPC `Scan(snapshot_id=unknown)`.
  - 接口断言：
    - 返回 `NotFound`.
  - 数据库状态断言：
    - 数据、WAL、SST 均不变化。

- [x] `SCAN-UT-005` Empty intersection returns empty set instead of leaking out-of-range rows
  - 前置：写入 `a1 a2 b1 b2`.
  - 操作：构造 `prefix="a"` 且 `start_key="b"`，以及 `start_key > end_key` 两组 scan。
  - 接口断言：
    - 都返回空结果，不报错。
  - 数据库状态断言：
    - 不得返回任何超出交集的 key。
    - 只读路径不改变 WAL/SST。

- [x] `SCAN-E2E-006` Zero limit means unbounded full result under current transport contract
  - 前置：写入超过 3 条可扫描记录。
  - 操作：gRPC `Scan(limit=0)` 与 `Scan(limit=2)` 对比。
  - 接口断言：
    - `limit=0` 返回全量可见集。
    - `limit=2` 只返回前 2 条。
  - 数据库状态断言：
    - 两次调用都不改变数据库状态。

### 8. `CompareAndSet` / `KvEngine::compare_and_set`

覆盖接口：

- `CompareAndSet`
- `KvEngine::compare_and_set`

测试项：

- [x] `CAS-UT-001` Match update changes exactly one key and survives restart
  - 前置：写入 `k1=v1`.
  - 操作：`compare_and_set(k1, expected=v1, new=v2)`.
  - 接口断言：
    - 返回成功。
  - 数据库状态断言：
    - 最新读为 `v2`.
    - WAL 增长。
    - 重启后仍为 `v2`.

- [x] `CAS-UT-002` Mismatch returns conflict and leaves database untouched
  - 前置：写入 `k1=v1`，记录调用前 WAL 大小。
  - 操作：`compare_and_set(k1, expected=wrong, new=v2)`.
  - 接口断言：
    - 返回 `Conflict` / `FailedPrecondition`.
  - 数据库状态断言：
    - `get(k1) == v1`.
    - `scan` 结果不变。
    - WAL 大小不变。

- [x] `CAS-UT-003` Supports insert-on-absent and delete-on-match
  - 前置：空 key `k_new`；已有 key `k_del=v1`.
  - 操作：
    - `compare_and_set(k_new, expected=None, new=v1)`
    - `compare_and_set(k_del, expected=v1, new=None)`
  - 接口断言：
    - 两者都成功。
  - 数据库状态断言：
    - 插入后可见。
    - 删除后不可见。
    - 重启后状态保持。

- [x] `CAS-E2E-004` Transport field precedence is explicit
  - 前置：写入 `k1=v1`.
  - 操作：gRPC `CompareAndSet(delete_on_match=true, new_value=nonempty)`.
  - 接口断言：
    - 明确记录当前语义：`delete_on_match` 优先时，`new_value` 被忽略。
  - 数据库状态断言：
    - `k1` 被删除。
    - 不产生意外新值。

- [x] `CAS-UT-005` Absent-vs-present expectation mismatch never creates phantom writes
  - 前置：准备 `k_present=v1`，并保证 `k_absent` 不存在。
  - 操作：
    - 对 `k_present` 执行 `expected=None, new=v2`
    - 对 `k_absent` 执行 `expected=Some(v1), new=v2`
  - 接口断言：
    - 两次都返回 `Conflict`。
  - 数据库状态断言：
    - `k_present` 仍为 `v1`。
    - `k_absent` 仍不存在。
    - WAL 不增长。

- [x] `CAS-E2E-006` Empty key is rejected before compare-and-set reaches engine
  - 前置：记录调用前 WAL 大小和样本 key 的读结果。
  - 操作：gRPC `CompareAndSet(key="")`.
  - 接口断言：
    - 返回 `InvalidArgument`。
  - 数据库状态断言：
    - WAL 和可见数据集不变。

### 9. `CreateSnapshot` / `ReleaseSnapshot` / `KvEngine::create_snapshot` / `KvEngine::release_snapshot`

覆盖接口：

- `CreateSnapshot`
- `ReleaseSnapshot`
- `KvEngine::create_snapshot`
- `KvEngine::release_snapshot`

测试项：

- [x] `SNAP-UT-001` Snapshot id is usable for both point reads and scans
  - 前置：写入一组 key.
  - 操作：创建 snapshot，之后覆盖和删除部分 key.
  - 接口断言：
    - snapshot id 大于 0.
    - `get_with_snapshot` 与 `scan_with_snapshot` 可同时工作。
  - 数据库状态断言：
    - snapshot 期间旧版本保留。

- [x] `SNAP-UT-002` Release invalidates snapshot immediately
  - 前置：创建 snapshot.
  - 操作：`release_snapshot(snapshot_id)`, 再尝试 snapshot read.
  - 接口断言：
    - 第一次 release 成功。
    - 再读返回 `NotFound`.
    - 第二次 release 返回 `NotFound`.
  - 数据库状态断言：
    - release 不应改变最新数据集。

- [x] `SNAP-IT-003` Snapshot survives flush and compaction until release
  - 前置：创建 snapshot，随后执行大量写入并触发 flush/compaction.
  - 操作：读取 snapshot.
  - 接口断言：
    - snapshot 仍可读到旧版本。
  - 数据库状态断言：
    - release 后旧 stripe 才允许在后续 compaction 中回收。

- [x] `SNAP-UT-004` Creating snapshot is read-side only and does not mutate storage layout
  - 前置：记录调用前 WAL 文件大小、`.sst` 数量、当前可见数据集。
  - 操作：连续调用两次 `create_snapshot`。
  - 接口断言：
    - 返回两个不同且单调递增的 `snapshot_id`。
  - 数据库状态断言：
    - WAL 与 `.sst` 数量不变。
    - 最新可见数据集不变。

- [x] `SNAP-E2E-005` gRPC create-release round trip is usable by `Get` and `Scan`
  - 前置：写入一组 key，调用 gRPC `CreateSnapshot`，随后执行覆盖和删除。
  - 操作：使用该 `snapshot_id` 调用 gRPC `Get/Scan`，最后调用 gRPC `ReleaseSnapshot`。
  - 接口断言：
    - `CreateSnapshot` 返回 `success=true` 且 `snapshot_id > 0`。
    - `Get/Scan` 能读到快照时刻视图。
    - `ReleaseSnapshot` 返回成功。
  - 数据库状态断言：
    - `CreateSnapshot/ReleaseSnapshot` 本身不改动最新数据集。
    - release 前旧视图可读，release 后该 id 不再可用。

- [x] `SNAP-E2E-006` Releasing unknown snapshot returns NotFound without data mutation
  - 前置：记录调用前 WAL、SST、样本 key 结果。
  - 操作：gRPC `ReleaseSnapshot(snapshot_id=0)` 与 `ReleaseSnapshot(snapshot_id=unknown)`.
  - 接口断言：
    - 都返回 `NotFound`。
  - 数据库状态断言：
    - WAL、SST、最新数据集都不变。

### 10. `Flush` / `KvEngine::flush`

覆盖接口：

- `Flush`
- `KvEngine::flush`

测试项：

- [ ] `FLUSH-UT-001` Flush moves mutable state to SSTable without changing logical results
  - 前置：写入多条数据，记录调用前 `get/scan` 结果。
  - 操作：调用 `flush`.
  - 接口断言：
    - 返回成功或正常结束。
  - 数据库状态断言：
    - 逻辑结果不变。
    - `.sst` 文件数量增加。
    - `immutable_memtable_backlog` 最终归零。

- [ ] `FLUSH-UT-002` Empty flush is a no-op
  - 前置：空库或刚 flush 完毕。
  - 操作：再次 `flush`.
  - 接口断言：
    - 返回成功。
  - 数据库状态断言：
    - 不新增 `.sst`.
    - 数据集不变。

- [ ] `FLUSH-IT-003` Flush after write rotates WAL and keeps data recoverable
  - 前置：写入数据，记录旧 WAL 文件集合。
  - 操作：`flush`，等待后台 flush 完成，再重启。
  - 接口断言：
    - 重启后数据存在。
  - 数据库状态断言：
    - 新 WAL 已创建。
    - 被 SSTable 覆盖的旧 WAL 最终可被清理。

- [ ] `FLUSH-E2E-004` gRPC flush acknowledges trigger and preserves logical state
  - 前置：写入多条数据并记录调用前 `get/scan` 结果。
  - 操作：gRPC `Flush`，随后轮询直到 `immutable_memtable_backlog == 0` 或 `.sst` 增加。
  - 接口断言：
    - 返回 `success=true`，message 明确为 trigger 型语义，而非“已完全落盘”。
  - 数据库状态断言：
    - 逻辑读结果前后一致。
    - 最终出现 flush 对应的 `.sst` 或 WAL 轮转结果。

### 11. `KvEngine::with_transaction` / `EngineTransaction::*`

覆盖接口：

- `KvEngine::with_transaction`
- `EngineTransaction::get`
- `EngineTransaction::scan`
- `EngineTransaction::put`
- `EngineTransaction::delete`
- `EngineTransaction::compare_and_set`
- `EngineTransaction::commit`
- `EngineTransaction::rollback`

测试项：

- [ ] `TXN-UT-001` Staged writes are invisible before commit but visible inside transaction overlay
  - 前置：写入 `k1=v1`.
  - 操作：进入 `with_transaction`，执行 `put/delete`，在事务内调用 `txn.get` 和 `txn.scan`，事务外并发调用普通 `get/scan`.
  - 接口断言：
    - 事务内看到 overlay.
    - 事务外在 commit 前看不到 staged changes.
  - 数据库状态断言：
    - commit 前 WAL 大小不变。
    - commit 前重启不应看到 staged changes.

- [ ] `TXN-UT-002` Commit applies all staged operations atomically
  - 前置：初始数据集包含主记录与“模拟索引项”。
  - 操作：事务内同时更新主记录和索引项，然后 `commit`.
  - 接口断言：
    - 返回成功。
  - 数据库状态断言：
    - commit 后主记录与索引项同时可见。
    - 任一普通读不应看到半更新状态。
    - 重启后保持一致。

- [ ] `TXN-UT-003` Rollback leaves database unchanged
  - 前置：记录调用前 `get/scan/WAL size`.
  - 操作：事务内 staged 多个写入，然后 `rollback`.
  - 接口断言：
    - rollback 成功。
  - 数据库状态断言：
    - 最新数据集不变。
    - WAL 不增长。

- [ ] `TXN-UT-004` Transaction CAS uses overlay + base view and returns conflict correctly
  - 前置：事务开始前 `k1=v1`.
  - 操作：事务内 `txn.compare_and_set(k1, expected=v1, new=v2)` 与 mismatch case.
  - 接口断言：
    - match 成功，mismatch 返回 `Conflict`.
  - 数据库状态断言：
    - mismatch 不写 WAL。
    - match 只有在 commit 后才持久可见。

- [ ] `TXN-UT-005` Post-commit and post-rollback method guards are explicit
  - 前置：创建事务。
  - 操作：
    - commit 后再次 `put/get/delete/compare_and_set/rollback`.
    - rollback 后再次 `rollback` 或 `commit`.
  - 接口断言：
    - 当前允许或拒绝的行为必须有稳定约束，并由测试固定。
  - 数据库状态断言：
    - 禁止“已提交事务再次写入”污染数据库。

- [ ] `TXN-UT-006` Conflicting concurrent transactions are serialized without lost update
  - 前置：`counter=0`.
  - 操作：两个线程同时用 `with_transaction` 读取、加一、提交。
  - 接口断言：
    - 两个事务都成功。
  - 数据库状态断言：
    - 最终 `counter=2`.
    - 不能出现 `counter=1`.

- [ ] `TXN-IT-007` Commit survives crash-recovery as one atomic unit
  - 前置：手工构造“事务 commit 之后、flush 之前”的持久化状态。
  - 操作：重启恢复。
  - 接口断言：
    - 事务结果全部恢复。
  - 数据库状态断言：
    - 不出现主记录已恢复、索引项未恢复的半提交状态。

- [ ] `TXN-UT-008` Closure error before commit aborts staged changes by drop, not by hidden partial commit
  - 前置：记录事务开始前的数据集和 WAL 大小。
  - 操作：`with_transaction` 中执行 `txn.put/txn.delete` 后直接返回 `Err`，不调用 `commit`。
  - 接口断言：
    - `with_transaction` 向外返回原始错误。
  - 数据库状态断言：
    - staged changes 完全不可见。
    - WAL 不增长。
    - 重启后也不存在这些变更。

- [ ] `TXN-UT-009` Error returned after explicit commit does not roll back committed writes
  - 前置：写入基线数据集。
  - 操作：`with_transaction` 中先 `txn.put`、`txn.commit()`，随后 closure 再返回 `Err`。
  - 接口断言：
    - `with_transaction` 向外返回该错误。
    - 同时需要固定当前契约：显式 `commit` 已完成时，closure 后续错误不会隐式回滚。
  - 数据库状态断言：
    - 提交过的写入已经对外可见并可恢复。
    - 不允许出现“接口返回 Err，但数据一半提交一半没提交”的歧义状态。

### 12. `KvEngine::runtime_metrics` / `KvEngine::read_cache_metrics`

覆盖接口：

- `KvEngine::runtime_metrics`
- `KvEngine::read_cache_metrics`

测试项：

- [ ] `METRIC-UT-001` Runtime metrics reflect write and flush lifecycle
  - 前置：空库。
  - 操作：采集一次 metrics，执行写入、flush、等待后台完成，再采集。
  - 接口断言：
    - 返回结构完整。
  - 数据库状态断言：
    - `immutable_memtable_backlog`、`l0_file_count`、`pending_compaction_bytes` 与实际过程匹配。

- [ ] `METRIC-UT-002` Read cache metrics change on repeated reads but do not change data state
  - 前置：写入数据后多次 `get` / `get_with_snapshot`.
  - 操作：读取前后对比 `read_cache_metrics`.
  - 接口断言：
    - 首次 miss、重复 hit 的趋势成立。
  - 数据库状态断言：
    - WAL、SST、scan 结果不变。

- [ ] `METRIC-UT-003` Metric reads are side-effect free on storage plane
  - 前置：记录 WAL 大小、SST 数量、数据集。
  - 操作：反复调用 `runtime_metrics` / `read_cache_metrics`.
  - 接口断言：
    - 返回成功。
  - 数据库状态断言：
    - 存储面无变化。

### 13. `KvEngine::shutdown`

覆盖接口：

- `KvEngine::shutdown`

测试项：

- [ ] `SHUT-UT-001` Shutdown rejects new writes after close
  - 前置：写入若干 key.
  - 操作：调用 `shutdown`，随后再次 `put/delete/compare_and_set`.
  - 接口断言：
    - shutdown 成功。
    - 后续写入返回 `Unavailable`.
  - 数据库状态断言：
    - 已有数据仍可读取。
    - 不得接受新写入。

- [ ] `SHUT-UT-002` Shutdown drains pending flush work before returning
  - 前置：构造有 pending immutable memtable 的场景。
  - 操作：调用 `shutdown`.
  - 接口断言：
    - 正常返回。
  - 数据库状态断言：
    - 重启后数据完整。
    - 不应因为 shutdown 丢失刚刚提交的数据。

- [ ] `SHUT-UT-003` Shutdown is idempotent
  - 前置：任意库状态。
  - 操作：调用两次 `shutdown`.
  - 接口断言：
    - 两次都不 panic.
  - 数据库状态断言：
    - 数据集不变。

## 建议落地顺序

1. 先补生命周期与恢复基线：`BOOT-*`、`FLUSH-*`、`SHUT-*`。
2. 再补核心写路径：`PUT-*`、`DEL-*`、`BATCH-*`、`CAS-*`。
3. 再补读路径：`GET-*`、`MGET-*`、`SCAN-*`、`SNAP-*`。
4. 最后补事务和指标：`TXN-*`、`METRIC-*`。

## 备注

- 本文刻意把“接口行为”和“数据库状态变化”放在同一条测试项里，后续不建议只做 transport assertion.
- 对所有写接口，至少要有一条 “调用后立即重启恢复” 用例。
- 对所有读接口，至少要有一条 “调用前后 WAL/SST/可见数据集不变” 用例。
