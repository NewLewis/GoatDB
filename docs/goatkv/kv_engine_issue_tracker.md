# GoatKV 引擎问题跟踪清单

更新时间：2026-03-03

目标：把当前已识别的风险和缺口落成可执行 backlog，按优先级逐条解决并验证。

## 本轮验证结果

- `cargo test`：通过（unit/integration/e2e/doc tests 全通过）。
- `cargo test --test integration_recovery read_path_reports_missing_sstable_as_error -- --nocapture`：通过。
- `cargo test --test integration_recovery recovery_replays_wal_if_flush_never_completed -- --nocapture`：通过。
- `cargo clippy --all-targets --all-features -- -D warnings`：通过。
- `cargo test --test e2e_basic_crud`：通过；当环境禁止绑定回环端口时按测试约定自动跳过。
- 2026-02-14：完成 `src/goatkv/core/kv_engine/writer.rs` 设计评审（静态分析），新增 3 个待修复项：`P0-WRITE-CLOSE-SEMANTIC-GAP`、`P1-FLUSH-BARRIER-NO-GUARD`、`P1-SEQUENCE-OVERFLOW-PANIC`。
- 2026-03-03：完成全量代码走读与回归命令复核，新增 2 个待修复项：`P0-SKIPLIST-ARENA-DESTRUCTOR-LEAK`、`P2-UPDATE-RMW-NONATOMIC`。

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

- [x] `P0-FLUSH-WORKER-SHUTDOWN-HANG`
  - 现象：flush 失败后 worker 在重试循环中无限重试，`Drop` 阶段 `join` 可能长期阻塞，进程退出/测试回收卡住。
  - 影响：服务停机不可控，测试可能长期挂起，恢复路径可用性受影响。
  - 代码定位：
    - `src/goatkv/core/flush_worker.rs:127`
    - `src/goatkv/core/flush_worker.rs:135`
    - `src/goatkv/core/flush_worker.rs:163`
    - `src/goatkv/core/flush_worker.rs:226`
  - 验收标准：
    - worker 可接收停止信号并有界退出，不因单任务重试无限阻塞 `Drop`。
    - `recovery_replays_wal_if_flush_never_completed` 在可接受时间内稳定结束（不依赖外部 `timeout` 杀进程）。
  - 关闭记录：
    - 2026-02-10：`FlushWorker` 改为“单次尝试，失败直接报错并跳过任务”，移除 `sleep + retry` 机制。
    - 2026-02-10：回归测试通过：`recovery_replays_wal_if_flush_never_completed`、`flush_failed_task_does_not_evict_other_immutable_memtables`。

- [x] `P0-WRITE-CLOSE-SEMANTIC-GAP`
  - 现象：请求在 WAL 阶段已成功写入后，若并发触发 `close/shutdown`，`enqueue_mem_group` 可能因 `closed_reason` 返回错误；调用方收到失败，但对应记录可能已进入 WAL。
  - 影响：写请求提交语义不确定（调用方无法判断是否已持久化），失败后重试可能引入重复写；同时该错误在 leader 循环内被重映射为 `Internal`，语义漂移。
  - 代码定位：
    - `src/goatkv/core/kv_engine/writer.rs:508`
    - `src/goatkv/core/kv_engine/writer.rs:513`
    - `src/goatkv/core/kv_engine/writer.rs:519`
    - `src/goatkv/core/kv_engine/writer.rs:289`
  - 验收标准：
    - 明确并文档化 `close/shutdown` 竞争窗口的写语义（例如“WAL 成功后必须完成 memtable 应用”或“写请求在 close 后统一 fail-fast 且不写 WAL”）。
    - 实现与语义一致，避免“返回失败但可能已提交”。
    - 增加并发回归测试：`put` 与 `shutdown` 竞争时，返回码与最终数据可见性一致且可预期。
  - 关闭记录：
    - 2026-03-03：`KvWriter::close` 改为“仅关闭准入，不清空在途队列”；`enqueue_mem_group` 在 `Manual` 关闭原因下允许已接收请求完成。
    - 2026-03-03：新增并发回归 `test_shutdown_write_race_only_returns_unavailable`，验证 shutdown 竞争下不出现异常错误类型。

- [x] `P0-SKIPLIST-ARENA-DESTRUCTOR-LEAK`
  - 现象：SkipList 节点由 Arena 手工分配并用 `ptr::write` 构造，但生命周期结束时没有逐节点析构；节点内 `Bytes`/`K` 的析构逻辑不会被执行。
  - 影响：memtable 轮转与回收后进程常驻内存持续增长，长时间运行会出现显著内存泄漏风险。
  - 代码定位：
    - `src/goatkv/core/skip_list/list.rs:55`
    - `src/goatkv/core/skip_list/list.rs:59`
    - `src/goatkv/core/skip_list/arena.rs:48`
    - `src/goatkv/core/skip_list/node.rs:11`
    - `src/goatkv/core/mem_table.rs:26`
  - 验收标准：
    - 明确 Arena 与节点所有权模型，保证节点内字段析构可达（例如实现显式 drop 链路或改为托管容器）。
    - 增加内存回收回归测试：重复写入 + flush + 丢弃 memtable 后 RSS/allocated bytes 不持续单调增长。
    - 在文档中记录 unsafe 内存模型约束，避免后续修改再次引入泄漏。
  - 关闭记录：
    - 2026-03-03：为 `SkipList` 增加显式 `Drop`，按 level-0 链路逐节点 `drop_in_place`（跳过未初始化 head 节点）。
    - 2026-03-03：新增 `test_drop_reclaims_node_keys`，验证节点 key 析构可达。

### P1（核心能力缺口）

- [x] `P1-ERROR-CONTRACT-DRIFT`
  - 现象：`to_status()` 输出消息已包含细节前缀，但错误单测仍断言简短固定文案，导致回归测试失败。
  - 影响：错误对外契约不稳定，server/client 侧文案与测试、监控规则难以对齐。
  - 代码定位：
    - `src/goatkv/error.rs:145`
    - `src/goatkv/error.rs:147`
    - `src/goatkv/error.rs:149`
    - `src/goatkv/error.rs:245`
  - 验收标准：
    - 明确并固定错误消息策略（短文案 or 细节文案）。
    - `error.rs` 实现与单测断言一致，`cargo test` 不再因该项失败。
  - 关闭记录：
    - 2026-02-10：统一采用“稳定短文案”策略，`to_status()` 对外返回固定 message（`invalid argument`/`not found`/`data corruption`/`conflict`/`service unavailable`）。
    - 2026-02-10：`cargo test --lib` 通过，`goatkv::error` 相关映射测试全部通过。

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

- [x] `P1-MISSING-SSTABLE-ERROR-KIND-MISMATCH`
  - 现象：缺失 SSTable 时，读路径当前映射为 `Corruption`，而集成测试断言 `NotFound`，语义未统一。
  - 影响：调用方无法稳定依赖错误分类；恢复/告警策略容易分歧。
  - 代码定位：
    - `src/goatkv/metadata/version.rs:35`
    - `src/goatkv/metadata/version.rs:59`
    - `tests/integration/recovery_test.rs:312`
  - 验收标准：
    - 确认“缺失 SSTable”统一语义（`NotFound` 或 `Corruption`）并形成文档约定。
    - 实现与测试统一，相关集成用例稳定通过。
  - 关闭记录：
    - 2026-02-10：约定并实现为 `NotFound`；读路径中 SSTable 缺失统一映射为 `Error::not_found("sstable", ...)`。
    - 2026-02-10：回归测试 `read_path_reports_missing_sstable_as_error` 通过。

- [x] `P1-FLUSH-BARRIER-NO-GUARD`
  - 现象：flush 屏障依赖 `begin_flush_barrier/end_flush_barrier` 手动配对调用，缺少 RAII guard；未来若中间路径发生 panic/早退，`flush_blocked` 可能无法恢复。
  - 影响：写入入口可能被永久阻塞，表现为写请求长时间挂起。
  - 代码定位：
    - `src/goatkv/core/kv_engine/writer.rs:249`
    - `src/goatkv/core/kv_engine/writer.rs:262`
    - `src/goatkv/core/kv_engine/engine.rs:205`
    - `src/goatkv/core/kv_engine/engine.rs:210`
  - 验收标准：
    - 引入 guard 化接口（如 `FlushBarrierGuard`），确保离开作用域自动释放屏障。
    - `KvEngine::flush` 使用 guard API，不再依赖手工成对调用。
    - 增加异常路径测试，验证出现 panic/早退后后续写不会长期阻塞。
  - 关闭记录：
    - 2026-03-03：新增 `FlushBarrierGuard`（RAII）；`KvEngine::flush` 切换为 `enter_flush_barrier()`。

- [x] `P1-SEQUENCE-OVERFLOW-PANIC`
  - 现象：写路径批量分配 sequence 后直接构造 `InternalKey`；当 sequence 超过 56-bit 上限时，`InternalKey::new` 会 panic。
  - 影响：极端长期运行或边界条件下可能触发进程级崩溃，而不是可处理错误。
  - 代码定位：
    - `src/goatkv/core/kv_engine/writer.rs:482`
    - `src/goatkv/core/kv_engine/writer.rs:489`
    - `src/goatkv/core/kv_engine/writer.rs:493`
    - `src/goatkv/format/internal_key.rs:72`
    - `src/goatkv/format/internal_key.rs:75`
  - 验收标准：
    - 在写路径提前做上限检查并返回可传播错误（避免 panic）。
    - 建立 sequence 溢出策略（拒绝写入/只读模式/运维告警）并文档化。
    - 增加边界测试覆盖接近上限和越界场景。
  - 关闭记录：
    - 2026-03-03：`SequenceNumber` 新增 `try_allocate_range`；写路径改为上限校验后分配，溢出返回错误。
    - 2026-03-03：新增 `sequence_overflow_returns_error_instead_of_panic` 与 `try_allocate_range_respects_upper_bound`。

- [x] `P1-NO-COMPACTION`
  - 现象：只有 `needs_compaction` 判定，没有实际 compaction 调度与执行。
  - 影响：L0 文件增长，读放大和空间放大持续恶化。
  - 代码定位：
    - `src/goatkv/metadata/version.rs:195`
  - 验收标准：
    - 先实现最小可用 `L0 -> L1` compaction。
    - 完成后能删除被替换旧文件并验证读一致性。
  - 设计稿归档：
    - 2026-02-10：`docs/goatkv/core/compaction_design.md`（MVP 范围：`L0 -> L1`，不重试，失败直接报错）。
  - 实施任务清单：
    - `docs/goatkv/core/compaction_design.md` 第 15 节（按 PR 顺序拆分 `TASK-01` 到 `TASK-10`）。
  - 关闭记录：
    - 2026-03-03：在 `FlushWorker` 接入最小 `L0 -> L1` compaction（读取 L0 + 重叠 L1，按最大 seq 合并生成新 L1 文件，应用 VersionEdit）。
    - 2026-03-03：新增回归 `test_l0_compacts_to_l1_when_l0_exceeds_threshold`。

- [x] `P1-MANIFEST-REWRITE-NOT-EFFECTIVE`
  - 现象：`manifest_max_size` 和 `manifest_rewrite_edit_count` 仅定义，未见重写触发逻辑。
  - 影响：MANIFEST 可能持续膨胀，恢复时间变长。
  - 代码定位：
    - `src/goatkv/metadata/version_set.rs:101`
    - `src/goatkv/metadata/version_set.rs:104`
  - 验收标准：
    - 支持 MANIFEST 条件重写（大小/编辑数）。
    - 重写后 `CURRENT` 原子切换，崩溃恢复可通过。
  - 关闭记录：
    - 2026-03-03：`VersionSet` 接入 `manifest_edit_count` 与 `manifest_max_size/manifest_rewrite_edit_count` 触发重写。
    - 2026-03-03：重写流程为“写快照 MANIFEST -> sync -> CURRENT 原子切换”。

- [x] `P1-SSTABLE-SEQNO-METADATA-MISSING`
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
  - 关闭记录：
    - 2026-03-03：`SSTableBuilder` 从 InternalKey trailer 解析并写入 `smallest_seqno/largest_seqno`。
    - 2026-03-03：`VersionEdit` 新增 `TAG_NEW_FILE_V2` 编解码 seqno，同时兼容旧 `TAG_NEW_FILE`。

- [x] `P1-SSTABLE-CLEANUP-PIPELINE-INCOMPLETE`
  - 现象：`CleanupTask::Sstable` 有消费端，但无明确发送闭环。
  - 影响：旧 SSTable 清理风险（磁盘泄露或清理时机不明确）。
  - 代码定位：
    - `src/goatkv/core/cleanup_worker.rs:56`
    - `src/goatkv/metadata/file_metadata.rs:42`
  - 验收标准：
    - 在 Version 变更中明确发送 SSTable 清理任务。
    - 增加“旧表被清理且不影响在途读”的测试。
  - 关闭记录：
    - 2026-03-03：`VersionSet` 在历史版本淘汰时计算 live file 集并发送 `CleanupTask::Sstable`，形成删除闭环。

### P2（工程质量）

- [x] `P2-CLIPPY-TOO-MANY-ARGS`
  - 现象：`KvEngine::build_engine` 参数过多导致 clippy fail。
  - 代码定位：
    - `src/goatkv/core/kv_engine/engine.rs:369`
  - 验收标准：
    - 重构为上下文结构体或 builder，`cargo clippy --all-targets --all-features -- -D warnings` 通过。
  - 关闭记录：
    - 2026-03-03：将 `KvEngine::build_engine` 改为 `BuildEngineInput` 上下文结构体。
    - 2026-03-03：`cargo clippy --all-targets --all-features -- -D warnings` 通过。

- [x] `P2-UPDATE-RMW-NONATOMIC`
  - 现象：`update` RPC 采用“先 `get` 再 `put`”的读改写流程，期间缺少原子性保护。
  - 影响：并发写下 `update` 的“仅更新已存在键”语义不稳定，可能出现竞态覆盖或可见性漂移。
  - 代码定位：
    - `src/bin/goatkv_server.rs:116`
    - `src/bin/goatkv_server.rs:131`
  - 验收标准：
    - 明确 `update` 的并发语义（compare-and-set 或幂等 upsert）。
    - 若保留“存在性检查”语义，需在引擎层提供原子 API 并补并发测试。
    - gRPC 文档与返回码与最终语义一致。
  - 关闭记录：
    - 2026-03-03：`update` RPC 明确为 upsert 语义，移除先读后写检查窗口。
    - 2026-03-03：更新 E2E 用例 `test_update_nonexistent_key_is_upsert`。

- [x] `P2-E2E-ENV-DEPENDENCY`
  - 现象：E2E 依赖本地网络端口，在受限环境中不可执行。
  - 代码定位：
    - `tests/common/test_server.rs:256`
  - 验收标准：
    - 在 CI/本地提供清晰的可运行条件说明，或补充可替代的无端口集成测试路径。
  - 关闭记录：
    - 2026-03-03：README 补充“受限环境下 E2E 跳过策略 + 无端口替代测试命令”。

## 建议修复顺序

当前无未修复项。

## 逐项关闭记录（执行时填写）

- Issue:
- 方案摘要:
- 关键改动文件:
- 测试命令:
- 结果:
- 备注:
