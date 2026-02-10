# GoatKV 引擎问题跟踪清单

更新时间：2026-02-10

目标：把当前已识别的风险和缺口落成可执行 backlog，按优先级逐条解决并验证。

## 本轮验证结果

- `cargo test --lib --test integration_recovery`：通过。
- `cargo fmt -- --check`：通过。
- `cargo clippy --all-targets --all-features -- -D warnings`：失败，`too_many_arguments`（`src/goatkv/core/kv_engine/engine.rs:322`）。
- `cargo test`：单元测试通过；E2E 在当前沙箱环境失败（无法申请临时端口，`PermissionDenied`，非业务断言失败）。

## 问题清单

### P0（优先修复）

- [x] `P0-FLUSH-QUEUE-MISMATCH`
  - 现象：flush 任务失败后 `continue`，但后续成功路径仍 `pop_front`，可能把错误任务“错位弹出”。
  - 影响：潜在数据丢失/不可预期一致性问题。
  - 代码定位：
    - `src/goatkv/core/flush_worker.rs:128`
    - `src/goatkv/core/flush_worker.rs:150`
    - `src/goatkv/core/flush_worker.rs:172`
    - `src/goatkv/core/flush_worker.rs:183`
  - 验收标准：
    - flush 失败不会误移除队头非对应任务。
    - 增加针对 flush 失败+后续任务成功的测试，验证队列顺序和数据完整性。
  - 关闭记录：
    - 2026-02-10：已修复。flush 成功后改为按 `Arc::ptr_eq` 精确移除任务对应 immutable memtable，不再无条件 `pop_front`。
    - 回归测试：`flush_failed_task_does_not_evict_other_immutable_memtables`（`tests/integration/recovery_test.rs`）。

- [x] `P0-WRITE-PATH-PANIC`
  - 现象：写路径发生 I/O 错误时使用 `expect` 直接 panic。
  - 影响：线上服务在可恢复错误下进程崩溃。
  - 代码定位：
    - `src/goatkv/core/kv_engine/engine.rs:178`
    - `src/goatkv/core/kv_engine/engine.rs:191`
    - `src/goatkv/core/kv_engine/engine.rs:195`
    - `src/bin/goatkv_server.rs:53`
    - `src/bin/goatkv_server.rs:120`
    - `src/bin/goatkv_server.rs:144`
  - 验收标准：
    - `KvEngine` 写接口返回 `Result`。
    - gRPC 层将写失败映射为 `tonic::Status`，不发生 panic。
  - 关闭记录：
    - 2026-02-10：`KvEngine::put/put_batch/delete` 改为返回 `goatkv::Result<()>`，不再内部 `expect`。
    - 2026-02-10：gRPC server 写路径统一 `map_err(|e| e.to_status())`。

- [x] `P0-SSTABLE-BUILDER-PANIC`
  - 现象：SSTable 构建流程内大量 `unwrap/expect`。
  - 影响：磁盘异常会导致 flush 线程直接崩溃。
  - 代码定位（示例）：
    - `src/goatkv/storage/sstable/builder.rs:297`
    - `src/goatkv/storage/sstable/builder.rs:364`
    - `src/goatkv/storage/sstable/builder.rs:377`
    - `src/goatkv/storage/sstable/builder.rs:388`
  - 验收标准：
    - 统一改为 `io::Result` 透传。
    - flush worker 收到错误可记录并按策略处理，不崩线程。
  - 关闭记录：
    - 2026-02-10：`SSTableBuilder::new/new_with_manager/write/finish` 已接入 `goatkv::Result`，移除内部 `unwrap/expect`。
    - 2026-02-10：`FlushWorker` 接入 builder 写入错误处理，失败仅记录并跳过本次任务，不会线程 panic。

### P1（核心能力缺口）

- [x] `P1-READ-ERROR-HIDDEN`
  - 现象：SSTable 打开/读取失败时，读路径返回 `None`（看起来像 key 不存在）。
  - 影响：错误语义被吞，排障困难，可能误判数据状态。
  - 代码定位：
    - `src/goatkv/metadata/version.rs:75`
    - `src/goatkv/metadata/version.rs:82`
    - `src/goatkv/metadata/version.rs:99`
    - `src/goatkv/metadata/version.rs:105`
  - 验收标准：
    - 读路径可区分“未命中”和“I/O 错误”。
    - 上层 API 的错误语义明确（内部/外部可见按设计决定）。
  - 关闭记录：
    - 2026-02-10：`Version::get` 改为返回 `goatkv::Result<Option<...>>`，不再吞掉 SSTable 打开/读取错误。
    - 2026-02-10：`KvReader::get`、`KvEngine::get` 连带改为 `goatkv::Result<Option<Vec<u8>>>`。
    - 2026-02-10：gRPC `get/update` 路径接入 `map_err(|e| e.to_status())`，读错误可透传为服务端错误。
    - 2026-02-10：新增 `read_path_reports_missing_sstable_as_error` 回归测试（`tests/integration/recovery_test.rs`）。

- [ ] `P1-NO-COMPACTION`
  - 现象：只有 `needs_compaction` 判定，没有实际 compaction 调度与执行。
  - 影响：L0 文件增长，读放大和空间放大持续恶化。
  - 代码定位：
    - `src/goatkv/metadata/version.rs:195`
  - 验收标准：
    - 先实现最小可用 `L0 -> L1` compaction。
    - 完成后能删除被替换旧文件并验证读一致性。

- [ ] `P1-MANIFEST-REWRITE-NOT-EFFECTIVE`
  - 现象：`manifest_max_size` 和 `manifest_rewrite_edit_count` 仅定义，未见重写触发逻辑。
  - 影响：MANIFEST 可能持续膨胀，恢复时间变长。
  - 代码定位：
    - `src/goatkv/metadata/version_set.rs:101`
    - `src/goatkv/metadata/version_set.rs:104`
  - 验收标准：
    - 支持 MANIFEST 条件重写（大小/编辑数）。
    - 重写后 `CURRENT` 原子切换，崩溃恢复可通过。

- [ ] `P1-SSTABLE-SEQNO-METADATA-MISSING`
  - 现象：SSTable 属性里的 `smallest_seqno/largest_seqno` 固定写 0；VersionEdit 编解码未携带 seqno。
  - 影响：后续 MVCC/compaction 策略无法利用该元信息。
  - 代码定位：
    - `src/goatkv/storage/sstable/builder.rs:457`
    - `src/goatkv/storage/sstable/builder.rs:458`
    - `src/goatkv/metadata/version_edit.rs:170`
    - `src/goatkv/metadata/version_edit.rs:247`
  - 验收标准：
    - flush 产出的 seqno 范围正确写入并持久化到 MANIFEST。
    - recovery 后该属性保持一致。

- [ ] `P1-SSTABLE-CLEANUP-PIPELINE-INCOMPLETE`
  - 现象：`CleanupTask::Sstable` 有消费端，但无明确发送闭环。
  - 影响：旧 SSTable 清理风险（磁盘泄露或清理时机不明确）。
  - 代码定位：
    - `src/goatkv/core/cleanup_worker.rs:56`
    - `src/goatkv/metadata/file_metadata.rs:42`
  - 验收标准：
    - 在 Version 变更中明确发送 SSTable 清理任务。
    - 增加“旧表被清理且不影响在途读”的测试。

### P2（工程质量）

- [ ] `P2-CLIPPY-TOO-MANY-ARGS`
  - 现象：`KvEngine::build_engine` 参数过多导致 clippy fail。
  - 代码定位：
    - `src/goatkv/core/kv_engine/engine.rs:322`
  - 验收标准：
    - 重构为上下文结构体或 builder，`cargo clippy --all-targets --all-features -- -D warnings` 通过。

- [ ] `P2-E2E-ENV-DEPENDENCY`
  - 现象：E2E 依赖本地网络端口，在受限环境中不可执行。
  - 代码定位：
    - `tests/common/test_server.rs:256`
  - 验收标准：
    - 在 CI/本地提供清晰的可运行条件说明，或补充可替代的无端口集成测试路径。

## 建议修复顺序

1. `P0-FLUSH-QUEUE-MISMATCH`
2. `P0-WRITE-PATH-PANIC`
3. `P0-SSTABLE-BUILDER-PANIC`
4. `P1-READ-ERROR-HIDDEN`
5. `P1-NO-COMPACTION`
6. `P1-MANIFEST-REWRITE-NOT-EFFECTIVE`
7. `P1-SSTABLE-SEQNO-METADATA-MISSING`
8. `P1-SSTABLE-CLEANUP-PIPELINE-INCOMPLETE`
9. `P2-CLIPPY-TOO-MANY-ARGS`

## 逐项关闭记录（执行时填写）

- Issue:
- 方案摘要:
- 关键改动文件:
- 测试命令:
- 结果:
- 备注:
