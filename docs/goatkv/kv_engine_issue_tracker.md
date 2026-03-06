# GoatKV 引擎问题跟踪清单

更新时间：2026-03-06

目标：把当前已识别的风险和缺口落成可执行 backlog，按优先级逐条解决并验证。

## 本轮验证结果

- `cargo test`：通过（unit/integration/e2e/doc tests 全通过）。
- `cargo test --test integration_recovery read_path_reports_missing_sstable_as_error -- --nocapture`：通过。
- `cargo test --test integration_recovery recovery_replays_wal_if_flush_never_completed -- --nocapture`：通过。
- `cargo clippy --all-targets --all-features -- -D warnings`：通过。
- `cargo test --test e2e_basic_crud`：通过；当环境禁止绑定回环端口时按测试约定自动跳过。
- 2026-02-14：完成 `src/goatkv/core/kv_engine/writer.rs` 设计评审（静态分析），新增 3 个待修复项：`P0-WRITE-CLOSE-SEMANTIC-GAP`、`P1-FLUSH-BARRIER-NO-GUARD`、`P1-SEQUENCE-OVERFLOW-PANIC`。
- 2026-03-03：完成全量代码走读与回归命令复核，新增 2 个待修复项：`P0-SKIPLIST-ARENA-DESTRUCTOR-LEAK`、`P2-UPDATE-RMW-NONATOMIC`。
- 2026-03-04：完成新一轮全量静态走读（重点覆盖 compaction/manifest/recovery/读路径错误传播），新增 4 个待修复项：`P0-INTERNALKEY-KIND-PANIC-ON-CORRUPTION`、`P1-COMPACTION-ORPHAN-SSTABLE-LEAK`、`P1-COMPACTION-MEMORY-AMPLIFICATION`、`P1-FLUSH-COMPACTION-COUPLED-BACKPRESSURE`。
- 2026-03-04：完成生产就绪差距评审（重点覆盖数据完整性/运维可观测/安全基线/接口能力），新增 9 个待修复项：`P0-SSTABLE-BLOCK-CHECKSUM-MISSING`、`P0-FLUSH-FAILURE-IMMUTABLE-BACKLOG-UNBOUNDED`、`P0-TRANSPORT-SECURITY-AUTH-MISSING`、`P1-TABLE-BLOCK-CACHE-MISSING`、`P1-OBSERVABILITY-HEALTH-GAP`、`P1-COMPACTION-POLICY-HARDCODED`、`P1-API-SCAN-SNAPSHOT-CAS-MISSING`、`P1-ONDISK-FORMAT-VERSIONING-GAP`、`P2-UNSAFE-VALIDATION-COVERAGE-GAP`。
- 2026-03-04：完成 `P0-SSTABLE-BLOCK-CHECKSUM-MISSING` 修复与回归，`cargo test` 全量通过。
- 2026-03-04：完成 `P0-FLUSH-FAILURE-IMMUTABLE-BACKLOG-UNBOUNDED` 修复与回归，`cargo test` 与 `cargo clippy --all-targets --all-features -- -D warnings` 通过。
- 2026-03-04：完成 `P0-TRANSPORT-SECURITY-AUTH-MISSING` 修复与回归，新增 server TLS/mTLS 选项、token 鉴权拦截器与 README 安全部署说明；`cargo test` 与 `cargo clippy --all-targets --all-features -- -D warnings` 通过。
- 2026-03-04：完成 `P1-TABLE-BLOCK-CACHE-MISSING` 修复与回归，新增可配置 table/block cache、缓存指标快照与热点读基准命令；`cargo test` 与 `cargo clippy --all-targets --all-features -- -D warnings` 通过。
- 2026-03-04：完成 RocksDB 对齐差距评审，新增 7 个待修复项：`P1-WRITE-STALL-BY-COMPACTION-PRESSURE`、`P1-PREFIX-BLOOM-PARTITIONED-FILTER`、`P1-READAHEAD-ITERATOR-OPT`、`P1-MULTIGET-BATCH-READ-PATH`、`P1-PARALLEL-COMPACTION-SUBCOMPACTION`、`P1-PER-LEVEL-COMPRESSION`、`P2-WAL-PREALLOC-BYTES-PER-SYNC`。
- 2026-03-04：完成 `P1-COMPACTION-POLICY-HARDCODED` 修复与回归，compaction 关键阈值已由 `KvEngineOptions` 配置驱动；`cargo test test_l0_compacts_to_base_level_when_l0_exceeds_threshold`、`cargo test test_compaction_cascades_to_l2_when_l1_exceeds_threshold` 通过。
- 2026-03-04：完成 `P1-WRITE-STALL-BY-COMPACTION-PRESSURE` 修复与回归，新增 L0/pending-compaction 两级 slowdown/stop 策略、可配置阈值与写压状态转移日志；`cargo test --lib`、`cargo clippy --all-targets --all-features -- -D warnings` 通过。
- 2026-03-04：启动 `P1-PREFIX-BLOOM-PARTITIONED-FILTER` 修复，已落地 prefix extractor 与 partitioned bloom/filter-index（按 data-block 分区并懒加载），待补充误报率与点查 miss 路径 benchmark。
- 2026-03-04：完成 GoatKV vs RocksDB 读路径基准对齐：`randread(times=80,key_nums=20000,threads=16)` GoatKV `1542ms` vs RocksDB `436ms`（约慢 `3.54x`）；`hotread(times=120,key_nums=20000,hotset=512,threads=16)` GoatKV `1463ms` vs RocksDB `529ms`（约慢 `2.77x`）。基于代码走读新增 3 个待修复项：`P1-POINT-GET-HOTPATH-DECODE-COPY-AMPLIFICATION`、`P1-PINNED-READ-API-MISSING`、`P1-ROW-CACHE-MISSING`。
- 2026-03-04：完成读路径阶段 3（block 内 user-key hash index + pinned value + row cache）并复测：`/tmp/goatkv_bench_cmp_stage3` 下 `randread(times=80,key_nums=20000,threads=16)` GoatKV `351ms` vs RocksDB `450ms`；`hotread(times=120,key_nums=20000,hotset=512,threads=16)` GoatKV `461ms` vs RocksDB `563ms`。对照 `row_cache=0`（`/tmp/goatkv_bench_cmp_stage3_norow`）GoatKV `randread=1557ms`、`hotread=1357ms`，确认热点收益主要来自 row cache 与 pinned 短路径。
- 2026-03-05：完成 GoatKV vs RocksDB 读路径再对齐走读，新增“读路径差异记录（2026-03-05）”章节，沉淀当前实现与配置差异。
- 2026-03-05：完成快照能力设计草案（参考 RocksDB `ReadOptions::snapshot` / `SnapshotList` / `CompactionIterator` 规则），新增文档 `docs/goatkv/snapshot_design.md`。
- 2026-03-06：完成 `SM5-01/SM5-02`，subcompaction 已支持按 key-range 并行执行与线程上限控制（`max_subcompactions`）；补充并行/串行结果一致性回归与高写入基准对照（含 debt 收敛观测）。
- 2026-03-06：完成 `SM6-03` 长稳压测作业骨架，新增 ignored `e2e_soak`、标准化 JSON 报告、失败样本归档脚本与复盘模板。
- 2026-03-06：完成 `SM0-02` 基线 benchmark 模板与阈值登记，`goatkv_bench` 输出已标准化（吞吐/p95/p99/样本规模/执行时间戳）并支持 10% 默认回退门禁。
- 2026-03-06：完成 `SM1-04` MultiGet API 链路打通（proto/server/client/e2e），并接入 `multiget` RPC 指标维度。

## 读路径差异记录（2026-03-05）

- 快照读取机制：GoatKV 在每次 `get` 里通过 `RwLock` 读锁克隆 `mem_table/immutable/version` 快照；RocksDB 通过 `SuperVersion` 引用完成读视图获取。
- SSTable 文件挑选：GoatKV 采用 L0 反向线扫 + L1+ 单层二分；RocksDB 采用 `FilePicker` 做跨层边界推进与候选范围收缩。
- 读语义覆盖范围：GoatKV 当前点查语义以 `Put/Delete` 为主；RocksDB 读路径还覆盖 range tombstone、merge、blob、timestamp 等分支。
- Row cache 键与失效粒度：GoatKV 使用 `(version_seqno, user_key)`，版本切换后旧版本缓存整体失效；RocksDB row cache key 前缀包含文件与序列信息，失效粒度更细。
- Row cache 填充策略：GoatKV 会缓存 miss 结果；RocksDB 主要在命中并生成 replay log 时写入 row cache。
- 数据块内点查索引：GoatKV 读取块后会构建 `user_key -> value_range` 哈希索引；RocksDB 默认数据块仍是 binary 查找（需显式配置才启用 data block hash index）。
- Filter 默认配置：GoatKV 当前构建路径默认写入分区 bloom；RocksDB 默认 `filter_policy=nullptr`，不自动开启 bloom。
- 基准配置对齐现状：当前 bench 中 GoatKV 显式配置 table/block/row cache；RocksDB 读路径配置大多走默认值，结果仍受配置基线差异影响。

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

- [x] `P0-INTERNALKEY-KIND-PANIC-ON-CORRUPTION`
  - 现象：`InternalKeyKind` 从字节解码遇到非法值会 `panic!`；读路径在解析磁盘记录后会调用 `kind()`，可被损坏 SST/WAL 触发进程级崩溃。
  - 影响：数据损坏场景下服务不可用（崩溃而非返回 `Corruption`），恢复与诊断链路中断。
  - 代码定位：
    - `src/goatkv/format/internal_key.rs:29`
    - `src/goatkv/format/internal_key.rs:34`
    - `src/goatkv/core/kv_engine/reader.rs:51`
    - `src/goatkv/storage/sstable/reader.rs:323`
    - `src/goatkv/storage/wal/format.rs:100`
  - 验收标准：
    - 将 kind 解码改为可失败路径（`Result`/受控默认分支），禁止 panic。
    - SST/WAL 读取链路对非法 kind 统一返回 `Corruption`。
    - 增加回归测试：伪造非法 kind trailer，读请求返回错误而非崩溃。
  - 关闭记录：
    - 2026-03-04：`InternalKeyKind` 解码从 `From<u8>`（panic）改为 `TryFrom<u8>`（`Corruption`）。
    - 2026-03-04：`KvReader`/`SSTableReader`/`WAL format` 解码链路接入非法 kind 校验并统一返回 `Corruption`。
    - 2026-03-04：新增回归 `test_sstable_reader_reports_invalid_internal_key_kind`、`test_wal_reader_reports_invalid_internal_key_kind`、`test_wal_replay_reports_invalid_internal_key_kind`。

- [x] `P0-SSTABLE-BLOCK-CHECKSUM-MISSING`
  - 现象：SSTable 读取主要依赖 footer+magic 校验；数据块/索引块本身没有校验和，磁盘静默损坏可能在读路径晚发现或误判。
  - 影响：坏块检测能力不足，可能导致数据错误传播或延迟暴露，影响线上可靠性。
  - 代码定位：
    - `src/goatkv/storage/sstable/builder.rs:309`
    - `src/goatkv/storage/sstable/builder.rs:391`
    - `src/goatkv/storage/sstable/builder.rs:405`
    - `src/goatkv/storage/sstable/reader.rs:188`
    - `src/goatkv/storage/wal/format.rs:61`（对比 WAL 已有 checksum）
  - 验收标准：
    - 为 SSTable 数据块/索引块增加 checksum trailer 并在读路径强校验。
    - 坏块统一返回 `Corruption`，禁止返回伪正常结果。
    - 增加故障注入测试：随机 bit-flip 能稳定触发校验失败。
  - 关闭记录：
    - 2026-03-04：`SSTableBuilder` 为数据块与索引块写入 4-byte checksum trailer，并将数据块索引长度升级为“内容+checksum”总长度。
    - 2026-03-04：`SSTableReader` 在 open/get/scan 流程强制校验块 checksum，校验失败统一返回 `Corruption`。
    - 2026-03-04：新增回归 `test_sstable_reader_reports_data_block_checksum_mismatch`、`test_sstable_open_reports_index_block_checksum_mismatch`。

- [x] `P0-FLUSH-FAILURE-IMMUTABLE-BACKLOG-UNBOUNDED`
  - 现象：flush 失败时 worker 记录错误后 `continue`；系统缺少“连续失败后停写/强背压”机制，immutable 队列可持续累积。
  - 影响：磁盘异常或权限异常场景下可能导致内存占用持续增长，最终触发 OOM 或整体不可用。
  - 代码定位：
    - `src/goatkv/core/flush_worker.rs:239`
    - `src/goatkv/core/flush_worker.rs:314`
    - `src/goatkv/core/kv_engine/engine.rs:503`
    - `src/goatkv/core/lsm_state.rs:13`
  - 验收标准：
    - 引入 immutable backlog 上限与连续 flush 失败熔断策略。
    - 超限后写入快速失败（`Unavailable`/`ResourceExhausted`），避免内存无界增长。
    - 增加故障注入测试：模拟磁盘写失败时 backlog 与内存占用保持有界。
  - 关闭记录：
    - 2026-03-04：`LSMState` 新增 `flush_failure_streak/flush_circuit_open`；`FlushWorker` 对 flush 失败累计计数，达到阈值后打开熔断，成功 flush 后自动复位。
    - 2026-03-04：`KvWriter::submit_write` 新增 fail-fast 准入：当 immutable backlog 达上限或 flush 熔断开启时，直接返回 `Unavailable`，阻止内存继续增长。
    - 2026-03-04：`KvEngineOptions` 新增可配置项 `max_immutable_memtables`、`flush_failure_streak_limit`（测试环境默认放宽 backlog 上限）。
    - 2026-03-04：新增回归 `flush_failure_streak_opens_and_success_resets_circuit`、`submit_write_fails_fast_when_immutable_backlog_reaches_limit`、`submit_write_fails_fast_when_flush_circuit_is_open`。

- [x] `P0-TRANSPORT-SECURITY-AUTH-MISSING`
  - 现象：gRPC 服务当前未启用 TLS/mTLS，也没有认证鉴权拦截器。
  - 影响：生产环境下存在明文传输、未授权访问与横向移动风险。
  - 代码定位：
    - `src/bin/goatkv_server.rs:182`
    - `src/bin/goatkv_server.rs:241`
    - `src/bin/goatkv_server.rs:262`
    - `src/bin/goatkv_server.rs:356`
    - `README.md:42`
  - 验收标准：
    - 支持 TLS（至少 server-side TLS），并提供可选 mTLS 模式。
    - 增加统一认证/鉴权拦截层（token/API key/证书主体映射）。
    - 提供安全配置文档与回归测试（握手失败、未授权拒绝、证书轮换）。
  - 关闭记录：
    - 2026-03-04：`goatkv_server` 新增 `--tls-cert-path/--tls-key-path/--tls-client-ca-path`，支持 server TLS 与可选 mTLS；参数组合非法时 fail-fast 返回 `InvalidArgument`。
    - 2026-03-04：新增认证拦截器 `authorize_request`；支持 `authorization: Bearer <token>` 与 `x-api-key`，未授权请求返回 `Unauthenticated`。
    - 2026-03-04：`Cargo.toml` 启用 `tonic` `tls-ring` feature；README 增加 TLS/mTLS 与 token 鉴权启动示例。
    - 2026-03-04：新增单测 `authorize_request_*` 与 `load_tls_config_*`，并通过 `cargo test --bin goatkv_server`。
    - 2026-03-04：当前 mTLS 采用 CA 信任链校验客户端证书，尚未引入证书主体到租户/账号的细粒度映射（如有多租户需求可拆分后续 issue）。

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

- [x] `P1-COMPACTION-ORPHAN-SSTABLE-LEAK`
  - 现象：compaction 生成新 SST 后若 `apply_edit` 失败，函数直接返回；新生成文件未入 manifest 且无主动清理路径。
  - 影响：运行期磁盘空间泄漏（需重启后依赖 recovery 扫描才可能清理），极端情况下导致磁盘压力与写失败。
  - 代码定位：
    - `src/goatkv/core/flush_worker.rs:535`
    - `src/goatkv/core/flush_worker.rs:553`
    - `src/goatkv/core/flush_worker.rs:565`
    - `src/goatkv/core/flush_worker.rs:572`
  - 验收标准：
    - `apply_edit` 失败时能同步/异步清理本次 compaction 产物（含多输出）。
    - 增加故障注入测试：强制 manifest 提交失败后无 orphan `.sst` 残留。
    - 日志可关联一次 compaction 的输出文件 ID 与清理结果。
  - 关闭记录：
    - 2026-03-04：compaction 在 `apply_edit` 失败分支新增 `cleanup_generated_sstables`，会回收本轮已生成输出文件。
    - 2026-03-04：新增故障注入测试 `compaction_apply_edit_failure_cleans_generated_sstable`，验证提交失败后无 orphan `.sst`。

- [x] `P1-COMPACTION-MEMORY-AMPLIFICATION`
  - 现象：compaction 先 `scan_all()` 全量读入，再用 `BTreeMap` 聚合，最终再整体构建输出；内存占用与输入总键数线性增长。
  - 影响：大范围 compaction 易出现内存峰值飙升，触发 OOM 或长时间 GC/调度抖动。
  - 代码定位：
    - `src/goatkv/core/flush_worker.rs:517`
    - `src/goatkv/core/flush_worker.rs:675`
    - `src/goatkv/core/flush_worker.rs:680`
    - `src/goatkv/core/flush_worker.rs:690`
  - 验收标准：
    - 改为流式 merge（k-way iterator），避免全量物化输入。
    - 限制单次 compaction 输入规模（文件数/字节）并提供可配置阈值。
    - 增加压力测试：大数据 compaction 下峰值内存受控且无 OOM。
  - 关闭记录：
    - 2026-03-04：将 compaction 输入从 `scan_all + BTreeMap` 改为 `SSTableScanIterator + BinaryHeap` 的流式 k-way merge，移除全量物化。
    - 2026-03-04：基于 level size target + overlap 扩展策略约束单轮 compaction 输入范围，并加入 grandparent overlap 上限切分输出。
    - 2026-03-04：回归覆盖 `test_l0_compacts_to_base_level_when_l0_exceeds_threshold`、`test_compaction_cascades_to_l2_when_l1_exceeds_threshold`（持续写入下多层 compaction 可收敛且读一致）。

- [x] `P1-FLUSH-COMPACTION-COUPLED-BACKPRESSURE`
  - 现象：flush 与 compaction 复用同一 worker 线程；一次 flush 结束后在同线程内循环执行 `maybe_compact_levels`，新 flush 任务必须排队等待 compaction 完成。
  - 影响：高写入下可能放大 flush 延迟与 immutable 堆积，形成写路径背压甚至超时。
  - 代码定位：
    - `src/goatkv/core/flush_worker.rs:97`
    - `src/goatkv/core/flush_worker.rs:207`
    - `src/goatkv/core/flush_worker.rs:229`
    - `src/goatkv/core/flush_worker.rs:234`
  - 验收标准：
    - 将 compaction 调度/执行与 flush 解耦（独立 worker 或有界预算执行）。
    - 为 compaction 设置单轮预算（时间/任务数），避免长期占用 flush 线程。
    - 增加并发压测：持续写入下 flush 延迟和 immutable 队列长度保持有界。
  - 关闭记录：
    - 2026-03-04：`FlushWorker` 拆分为独立 flush 线程与 compaction 线程；flush 成功后仅发送 compaction 触发信号，不在 flush 线程内执行 compaction。
    - 2026-03-04：新增/更新回归 `test_l0_compacts_to_base_level_when_l0_exceeds_threshold`、`test_compaction_cascades_to_l2_when_l1_exceeds_threshold`，验证持续 flush 下 compaction 可后台推进并保持读正确性。

- [x] `P1-TABLE-BLOCK-CACHE-MISSING`
  - 现象：读路径未引入 table cache/block cache，SSTable 读取仍频繁打开文件与重复解码。
  - 影响：高并发读场景下延迟与 IOPS 放大，FD 压力上升，吞吐受限。
  - 代码定位：
    - `src/goatkv/storage/sstable/cache.rs:1`
    - `src/goatkv/metadata/version.rs:109`
    - `src/goatkv/metadata/version_set.rs:164`
    - `src/goatkv/storage/sstable/reader.rs:38`
    - `src/goatkv/utils/options.rs:93`
    - `benches/goatkv_bench.rs:97`
  - 验收标准：
    - 引入可配置 table cache 与 block cache（容量/淘汰策略可调）。
    - 暴露缓存命中率与驱逐指标。
    - 增加基准测试：热点读场景下延迟与吞吐显著改善。
  - 关闭记录：
    - 2026-03-04：新增 `TableCache + BlockCache`（LRU 驱逐）并接入 `Version::get` 与 `SSTableReader` 数据块读取；table/block cache 分别支持按条目数/字节数配置。
    - 2026-03-04：`KvEngineOptions` 新增 `table_cache_capacity`、`block_cache_capacity_bytes`，`VersionSetOptions` 同步透传并在版本快照间共享缓存实例。
    - 2026-03-04：新增缓存指标快照 `ReadCacheMetrics`，并在 `KvEngine::read_cache_metrics()` 暴露命中/未命中/驱逐计数。
    - 2026-03-04：`benches/goatkv_bench.rs` 新增 `hotread` 工作负载与缓存容量参数，支持热点读对比（可通过 `--table-cache-capacity/--block-cache-capacity-mb` 调优或置 0 关闭）。
    - 2026-03-04：新增回归 `table_cache_reports_hits_and_evictions`、`block_cache_reports_hit_after_warmup`、`table_cache_can_be_disabled`。

- [x] `P1-OBSERVABILITY-HEALTH-GAP`
  - 现象：当前仅有日志输出，缺少标准化 metrics 与健康检查接口。
  - 影响：故障发现与容量规划依赖人工日志，缺少可观测闭环与自动告警基础。
  - 代码定位：
    - `src/bin/goatkv_server.rs:218`
    - `src/goatkv/utils/logging.rs:12`
    - `proto/goatkv.proto:5`
  - 验收标准：
    - 增加健康检查（liveness/readiness）接口。
    - 增加核心指标：QPS、延迟分位数、flush/compaction backlog、错误率、队列水位。
    - 补充运维看板与告警建议阈值。
  - 关闭记录：
    - 2026-03-05：完成 `SM3-01`，新增 `--health-address` 与 `/livez`、`/readyz`，补充 `e2e_health` 探针状态分离回归。
    - 2026-03-05：完成 `SM3-02`，新增 `/metrics` 与核心运行指标（QPS/错误率/延迟、backlog、queue、cache 命中/未命中/驱逐）。
    - 2026-03-05：完成 `SM3-03`，新增 `ops_alert_runbook.md` 与 `metrics_reference.md`，给出告警阈值与处置顺序。

- [x] `P1-COMPACTION-POLICY-HARDCODED`
  - 现象：compaction 触发阈值和层级大小目标仍以内嵌常量形式存在，运行时不可调。
  - 影响：不同硬件和业务负载下难以调优，可能导致写放大或读放大不受控。
  - 代码定位：
    - `src/goatkv/core/flush_worker.rs:18`
    - `src/goatkv/core/flush_worker.rs:19`
    - `src/goatkv/core/flush_worker.rs:20`
    - `src/goatkv/core/flush_worker.rs:21`
  - 验收标准：
    - 将 compaction 关键策略参数纳入 `KvEngineOptions` 并支持持久配置。
    - 提供合理默认值和参数边界校验。
    - 增加回归与基准：不同配置下 compaction 收敛行为可预测。
  - 关闭记录：
    - 2026-03-04：新增 `KvEngineOptions` 参数 `l0_compaction_file_trigger`、`compaction_max_bytes_for_level_base`、`compaction_max_bytes_for_level_multiplier`、`compaction_max_grandparent_overlap_bytes_factor`，并提供 builder API。
    - 2026-03-04：`FlushWorker` compaction 策略改为由 `CompactionConfig` 注入，移除对硬编码常量的依赖并增加参数归一化保护。
    - 2026-03-04：`KvEngine` 初始化将 options 映射到 `CompactionConfig`，默认行为保持与改动前一致。
    - 2026-03-04：回归通过 `test_l0_compacts_to_base_level_when_l0_exceeds_threshold`、`test_compaction_cascades_to_l2_when_l1_exceeds_threshold`、`test_with_l0_compaction_file_trigger`、`test_with_compaction_level_targets`。

- [ ] `P1-API-SCAN-SNAPSHOT-CAS-MISSING`
  - 现象：对外 API 仅覆盖点查点写；缺少范围扫描、快照读、条件写（CAS）等关键能力。
  - 影响：上层业务需要自行拼装，易引入一致性窗口和性能问题，接口可用性不足。
  - 代码定位：
    - `proto/goatkv.proto:5`
    - `proto/goatkv.proto:9`
    - `src/bin/goatkv_server.rs:38`
  - 验收标准：
    - 新增 `Scan`/`SnapshotGet`/`CompareAndSet`（或等价）RPC 及错误语义约定。
    - 保证范围读取在快照下的可重复性。
    - 增加并发一致性测试与接口文档。
  - 设计草案：
    - `docs/goatkv/snapshot_design.md`（v1 先交付快照点查闭环，再扩展到 Scan/CAS）。
    - Phase 1（必须整体交付）：`SnapshotManager + Get(read_seq) 全链路 + snapshot-aware compaction`。
    - Phase 2：对外 gRPC `CreateSnapshot/ReleaseSnapshot/Get(snapshot_id)`。
    - Phase 3：快照读 row cache 可见性键与读路径性能补齐。
  - 进展记录：
    - 2026-03-05：Phase 1 已落地（`SnapshotManager`、`get_at_seq` 读链路、snapshot-aware compaction stripe 保留）；新增并通过 `test_snapshot_get_sees_old_value_after_put`、`test_snapshot_get_sees_old_state_after_delete`、`test_snapshot_survives_flush_and_compaction`、`compaction_keeps_snapshot_stripes_for_same_user_key`。
    - 2026-03-05：Phase 2 已落地：`proto` 新增 `CreateSnapshot/ReleaseSnapshot`，`GetRequest` 增加 `snapshot_id`（`0`=最新读）；server/client 已接入快照 API；新增 e2e `tests/e2e/snapshot_test.rs` 覆盖“快照读旧值”和“释放后 NotFound”。
    - 2026-03-05：Phase 3 第一阶段已落地：row cache 键从 `(version_seqno, user_key)` 扩展为 `(version_seqno, read_seq, user_key)`，显式快照读启用 row cache 且按可见性序列隔离；新增 `test_snapshot_row_cache_respects_read_seq_visibility` 与 `row_cache_distinguishes_visibility_sequence`。
    - 2026-03-05：Phase 3 第二阶段已落地：`BlockReader` 增加 `get_by_user_key_with_value_range_at_seq`，`SSTableReader::get_pinned_at_seq` 改为块内按 seq 命中并返回 pinned value（避免全块线扫+value 拷贝）；`Version` 增加 `read_seq >= largest_seqno` 快路径复用普通点查；新增 `test_block_reader_get_by_user_key_at_seq_with_versions`、`test_block_reader_get_by_user_key_at_seq_cross_restart_boundary`、`test_sstable_reader_get_pinned_at_seq_returns_visible_version`、`test_sstable_reader_get_pinned_at_seq_crosses_blocks`。

- [ ] `P1-ONDISK-FORMAT-VERSIONING-GAP`
  - 现象：SSTable/MANIFEST 缺少明确的格式版本演进策略与兼容矩阵定义。
  - 影响：后续协议变更或升级回滚时存在兼容风险，易造成恢复失败或灰度困难。
  - 代码定位：
    - `src/goatkv/storage/sstable/builder.rs:13`
    - `src/goatkv/storage/sstable/reader.rs:12`
    - `src/goatkv/metadata/manifest.rs:56`
    - `src/goatkv/metadata/version_edit.rs:81`
  - 验收标准：
    - 定义并实现 on-disk format version 字段与兼容读取策略。
    - 给出升级/回滚策略文档（forward/backward compatibility）。
    - 增加跨版本读写兼容测试。
  - 进展记录：
    - 2026-03-05：完成第一阶段格式版本落地：
      - MANIFEST：`VersionEdit` 新增 `format_version` 字段（默认兼容 legacy=0，当前写入=1），恢复阶段对高于支持上限的版本直接报 `Corruption` 拒绝启动。
      - SSTable：footer padding 区新增格式标记与 `format_version`（当前=1），读取路径兼容无标记旧文件（按 legacy=0 处理）。
      - 新增/通过回归：`encode_decode_preserves_manifest_format_version`、`decode_legacy_edit_without_format_version_is_compatible`、`apply_edit_writes_current_manifest_format_version`、`recovery_rejects_unsupported_manifest_format_version`、`test_sstable_reader_compat_legacy_footer_without_format_marker`、`test_sstable_reader_rejects_unsupported_format_version`。

- [x] `P1-WRITE-STALL-BY-COMPACTION-PRESSURE`
  - 现象：当前写入背压主要基于 WAL/Mem 队列与 immutable backlog，缺少基于 `L0` 文件数和 compaction debt 的分级限速/停写策略。
  - 影响：高写入+compaction 落后时，可能出现 L0 快速膨胀、读放大恶化、尾延迟抖动，且恢复速度不可预测。
  - 代码定位：
    - `src/goatkv/core/kv_engine/writer.rs:244`
    - `src/goatkv/core/kv_engine/writer.rs:733`
    - `src/goatkv/core/kv_engine/writer.rs:751`
    - `src/goatkv/core/lsm_state.rs:18`
  - 验收标准：
    - 增加 `L0` 与 pending compaction bytes 维度的 slowdown/stop 两级策略。
    - 将阈值纳入配置并提供默认值与边界校验。
    - 压测下 L0 与写延迟保持有界，策略触发可观测。
  - 关闭记录：
    - 2026-03-04：`KvWriter::submit_write` 准入阶段新增 compaction pressure 判定，支持 `Allow/Slowdown/Stop` 三级行为；L0 或 pending compaction bytes 达硬阈值时 fail-fast 返回 `Unavailable`。
    - 2026-03-04：`KvEngineOptions` 新增 `l0_slowdown_writes_trigger`、`l0_stop_writes_trigger`、`soft_pending_compaction_bytes_limit`、`hard_pending_compaction_bytes_limit`、`write_slowdown_delay_ms`，并对输入做下限归一化（`>=1`）。
    - 2026-03-04：新增写压状态转移日志（normal/slowdown/stop）与回归测试：`submit_write_fails_fast_when_l0_reaches_stop_trigger`、`write_pressure_action_reports_slowdown_before_stop`、`submit_write_fails_fast_when_pending_compaction_bytes_reaches_hard_limit`、`test_with_write_stall_thresholds`。

- [ ] `P1-PREFIX-BLOOM-PARTITIONED-FILTER`
  - 现象：prefix extractor 与 partitioned bloom 已落地，但 filter 分区缓存仍是 reader 内部 `Mutex<HashMap<...>>`，未接入统一 block cache 的容量管理与淘汰策略。
  - 影响：热点/高并发点查下可能出现锁竞争与分区缓存无界增长，过滤路径稳定性与可观测性弱于 RocksDB 的 metadata cache 体系。
  - 代码定位：
    - `src/goatkv/storage/sstable/bloom.rs:4`
    - `src/goatkv/storage/sstable/builder.rs:512`
    - `src/goatkv/storage/sstable/reader.rs:58`
    - `src/goatkv/storage/sstable/reader.rs:673`
  - 验收标准：
    - 支持可配置 prefix extractor 与 prefix bloom。
    - partitioned filter 分区缓存纳入统一容量治理（可配置上限/淘汰策略/指标）。
    - 增加误报率、点查 miss 路径与高并发热点读基准，验证过滤路径 CPU 与内存占用可控。
  - 进展：
    - 2026-03-04：`SSTableBuilder` 已改为写入 partitioned bloom 段（按 data-block 分区），并通过 `KvEngineOptions::bloom_prefix_extractor_len` 支持 prefix bloom。
    - 2026-03-04：`SSTableReader` 已支持 partitioned bloom 解析与按分区懒加载，`may_contain/get` 复用 data-block 索引定位 filter 分区，避免 open 时一次性读取整段 bloom。
    - 2026-03-04：新增回归 `test_partitioned_bloom_respects_prefix_extractor`、`test_partitioned_bloom_loads_partitions_lazily`。
    - 2026-03-05：`SM4-02` 第一阶段落地：新增有上限 `FilterPartitionCache`（HyperClock 风格淘汰）并接入 `TableCache` 共享缓存层；`PartitionedBloomFilter::may_contain` 优先命中共享 cache，回退路径仅用于无共享 cache 场景。
    - 2026-03-05：新增 filter cache 指标 `filter_hits/misses/evictions`（bench 输出与 `/metrics`），并补充回归 `test_filter_cache_reports_hit_after_warmup`。
    - 2026-03-05：补充 `filter_cache on/off` 对照（`/tmp/goatkv_sm402_filter_on` vs `/tmp/goatkv_sm402_filter_off`，`multiget times=120,key_nums=20000,batch=32,miss_ratio=80,row_cache=0`）：on=`517ms`、off=`462ms`；workset-fit（`key_nums=2000,miss_ratio=0`）on=`152ms`、off=`157ms`，确认命中率可观测且容量策略生效。

- [x] `P1-POINT-GET-HOTPATH-DECODE-COPY-AMPLIFICATION`
  - 现象：点查命中路径会重复构建 `BlockReader`、重复解码 restart/entry，且 `decode_entry_at`/返回路径存在多次 `Vec` 拷贝。
  - 影响：在 table/block cache 命中较高时，读性能主要受 CPU 解码与内存拷贝限制，导致 randread/hotread 仍明显落后 RocksDB。
  - 代码定位：
    - `src/goatkv/storage/sstable/reader.rs:570`
    - `src/goatkv/storage/sstable/block_reader.rs:29`
    - `src/goatkv/storage/sstable/block_reader.rs:526`
    - `src/goatkv/storage/sstable/block_reader.rs:530`
  - 验收标准：
    - 命中路径复用已解析块结构，避免每次 `get` 重建 restart 索引。
    - 减少 entry 解码中的临时分配与不必要拷贝（优先借用切片或延迟拷贝）。
    - randread/hotread 基准下 CPU 占比下降且吞吐显著提升。
  - 进展：
    - 2026-03-04：`SSTableReader::get` 新增 data-block restart 索引复用路径：首次命中时解析并缓存 `BlockSearchIndex`，后续命中复用，避免重复构建 `BlockReader` 的 restart 索引。
    - 2026-03-04：新增回归 `test_sstable_reader_reuses_cached_block_search_index_for_hot_get`，并通过 `cargo test --lib test_sstable_reader_reuses_cached_block_search_index_for_hot_get`、`cargo test --lib test_block_reader_get_by_user_key`。
    - 2026-03-04：阶段 2 完成：`BlockReader::get_by_user_key` 改为直接返回 `InternalKey`，去掉命中路径 `raw_internal_key -> decode_internal_key` 的二次解码与额外拷贝；`SSTableReader::get` 保留 kind 校验语义。
    - 2026-03-04：新增/回归验证通过：`cargo test --lib test_block_reader_get_by_user_key -- --nocapture`、`cargo test --lib test_sstable_reader_reuses_cached_block_search_index_for_hot_get -- --nocapture`、`cargo test --lib test_sstable_reader_reports_invalid_internal_key_kind -- --nocapture`。
    - 2026-03-04：基准复测（`/tmp/goatkv_bench_cmp_stage2`）`randread`：GoatKV `1768ms` vs RocksDB `466ms`（约 `3.79x`）；`hotread`：GoatKV `1600ms` vs RocksDB `560ms`（约 `2.86x`）。同目录 GoatKV 复测（`/tmp/goatkv_bench_cmp/goatkv`）`randread=1677ms`、`hotread=1746ms`；当前阶段尚未观察到稳定收益，保持 issue open。
  - 关闭记录：
    - 2026-03-04：`BlockSearchIndex` 新增 block 内 `user_key -> (InternalKey, value_range)` 哈希索引，`get_by_user_key` 优先走哈希命中，回退 restart/range 扫描。
    - 2026-03-04：`SSTableReader::get_pinned` 改为消费 `value_range`，避免点查命中路径重复 entry 解码与 value 二次拷贝。
    - 2026-03-04：`cargo test --lib` 全量通过（135/135）。

- [x] `P1-PINNED-READ-API-MISSING`
  - 现象：当前引擎读接口统一返回 `Vec<u8>`，缺少 pinned/zero-copy 读取语义与生命周期管理接口。
  - 影响：热点读下即便 block cache 命中，仍需拷贝 value，放大内存带宽与分配开销。
  - 代码定位：
    - `src/goatkv/core/kv_engine/reader.rs:20`
    - `src/goatkv/metadata/version.rs:131`
    - `src/goatkv/storage/sstable/reader.rs:554`
    - `src/goatkv/core/mem_table.rs:30`
  - 验收标准：
    - 增加引擎内部 pinned value 表达（如借用切片/引用计数块句柄）。
    - 在不破坏现有 API 的前提下提供 zero-copy 快路径（可先内部使用）。
    - 热点读 benchmark 显示单位请求拷贝字节下降。
  - 关闭记录：
    - 2026-03-04：新增 `PinnedValue` 内部表达（支持 `Arc<[u8]> + range`），`SSTableReader::get_pinned` 命中 block cache 时可零拷贝 pin value。
    - 2026-03-04：`Version::get_pinned`、`KvReader`、`MemTable::get_pinned` 全链路接入 pinned 快路径；外部 `KvEngine::get` API 仍保持 `Vec<u8>` 兼容。
    - 2026-03-04：`cargo test --lib` 全量通过（135/135）。

- [ ] `P1-READAHEAD-ITERATOR-OPT`
  - 现象：SSTable 迭代读取仍按块同步拉取，缺少 readahead/prefetch 机制和顺序扫描专项优化。
  - 影响：范围扫描与混合负载下系统调用与随机 I/O 偏高，吞吐受限。
  - 代码定位：
    - `src/goatkv/storage/sstable/reader.rs:378`
    - `src/goatkv/storage/sstable/reader.rs:386`
    - `src/goatkv/storage/sstable/reader.rs:463`
    - `src/goatkv/storage/sstable/reader.rs:529`
  - 验收标准：
    - 引入扫描路径 readahead/prefetch 策略（可配阈值）。
    - 减少顺序扫描中的重复 decode/读取开销。
    - scan benchmark 吞吐提升且点查延迟不回退。
  - 进展：
    - 2026-03-05：`SSTableScanIterator` 新增 block 级 readahead/prefetch（默认预取 2 个 upcoming blocks），优先消费预取块并滚动补充后续块。
    - 2026-03-05：新增回归 `test_scan_iterator_prefetches_upcoming_blocks`、`test_scan_iterator_disables_readahead_for_single_block_sstable`，验证多块预取与单块退化行为。
    - 2026-03-05：新增 `scanread` benchmark（支持 `mode=scan-all|iterator`）并完成对照：`/tmp/goatkv_sm401_scan_opt` 下 GoatKV `scanread_scan_all=533ms`、`scanread_iterator=360ms`（约提升 `32.5%`）。

- [ ] `P1-MULTIGET-BATCH-READ-PATH`
  - 现象：对外接口与内部读路径以单 key 查询为主，缺少 MultiGet 批量探测/复用路径。
  - 影响：批量读场景重复 table/block probe 成本高，吞吐与 CPU 利用率偏低。
  - 代码定位：
    - `proto/goatkv.proto:4`
    - `proto/goatkv.proto:5`
    - `src/goatkv/metadata/version.rs:130`
    - `src/goatkv/storage/sstable/reader.rs:409`
  - 验收标准：
    - 增加 MultiGet API 与引擎批量读入口。
    - 批量路径复用 table/block cache probe 与文件打开结果。
    - 增加 batch size 梯度基准，验证吞吐提升。
  - 进展：
    - 2026-03-05：新增 `KvEngine::multi_get`/`KvReader::multi_get` 批量读入口，批次内复用读快照（mem/immutable/version）并对重复 key 做结果复用，减少重复 probe。
    - 2026-03-05：`goatkv_bench` 新增 `multiget` workload（`batch_size`、`miss_ratio`），支持 GoatKV/RocksDB 对照与基线梯度验证。
  - 进展：
    - 2026-03-05：新增 `KvEngine::multi_get`/`KvReader::multi_get` 批量读入口，单次请求内复用同一份读快照（mem/immutable/version）避免逐 key 重复抓取引擎读状态。
    - 2026-03-05：`goatkv_bench` 新增 `multiget` workload（支持 `batch_size`、`miss_ratio`，可跑 GoatKV/RocksDB 对照），后续补 batch-size 梯度与 miss-heavy/workset-fit 对照结果。

- [x] `P1-ROW-CACHE-MISSING`
  - 现象：当前读缓存仅覆盖 table cache 与 data block cache，缺少按 user key/value 结果缓存层。
  - 影响：热点 key 高命中场景仍需经历索引定位、块解码与版本过滤，无法获得与 RocksDB row cache 类似的短路径收益。
  - 代码定位：
    - `src/goatkv/storage/sstable/cache.rs:25`
    - `src/goatkv/storage/sstable/cache.rs:241`
    - `src/goatkv/metadata/version.rs:120`
  - 验收标准：
    - 增加可配置 row cache（容量、命中/驱逐指标、与 snapshot/seq 可见性约束）。
    - 与现有 table/block cache 协同，避免重复缓存和不可控内存膨胀。
    - hotread benchmark 在热点 key 分布下有可量化提升。
  - 关闭记录：
    - 2026-03-04：新增可配置 `row_cache_capacity_bytes`（`KvEngineOptions`/`VersionSetOptions`）与 row cache 指标（`row_hit/miss/evict`），并接入 benchmark CLI 参数 `--row-cache-capacity-mb`。
    - 2026-03-04：`Version` 读路径新增按 `(version_seqno, user_key)` 维度 row cache，满足 snapshot/可见性隔离；支持 miss 结果缓存（negative cache）。
    - 2026-03-04：基准验证：`/tmp/goatkv_bench_cmp_stage3` 下 GoatKV `randread=351ms`、`hotread=461ms`；对照 `row_cache=0`（`/tmp/goatkv_bench_cmp_stage3_norow`）分别为 `1557ms`、`1357ms`。

- [x] `P1-PARALLEL-COMPACTION-SUBCOMPACTION`
  - 现象：当前 compaction 由单线程循环串行执行，未支持 subcompaction 并行拆分。
  - 影响：大 compaction debt 时后台追赶速度不足，易形成写放大与读放大叠加。
  - 代码定位：
    - `src/goatkv/core/flush_worker.rs:154`
    - `src/goatkv/core/flush_worker.rs:360`
    - `src/goatkv/core/flush_worker.rs:415`
    - `src/goatkv/core/flush_worker.rs:681`
  - 验收标准：
    - 支持按 key range 切片并行执行 subcompaction。
    - 保证输出文件范围不重叠且 VersionEdit 提交一致。
    - 在高写入压测下 compaction backlog 能持续下降。
  - 关闭记录：
    - 2026-03-05：`SM5-01` 第一阶段落地：新增 `storage/compaction` 模块，包含 `SubcompactionRange` 与 key-range 切分器（`split_user_key_ranges`、`build_subcompaction_ranges`），并接入 compaction 计划流程。
    - 2026-03-06：`FlushWorker` compaction 路径接入并行 subcompaction（按 `max_subcompactions` 控制线程上限），支持子范围过滤构建与统一排序提交。
    - 2026-03-06：补齐错误收敛与清理路径：并行线程全部 join 后统一收敛错误，失败时清理已生成 SST 文件，避免 orphan 文件残留。
    - 2026-03-06：新增一致性回归 `subcompaction_parallel_matches_single_thread_output`，验证并行输出与单线程语义一致（key/value 集合相同）。
    - 2026-03-06：高写入基准（`populate --threads 16 --key-nums 120000 --batch-size 128 --value-size 512 --wal-sync`）3 轮对照：
      - `max_subcompactions=1`：`total_ms=423/196/132`（avg `250.3ms`）。
      - `max_subcompactions=4`：`total_ms=204/121/107`（avg `144.0ms`，较 `=1` 提升约 `42.5%`）。
      - 两组均 `submitted=successful=120000`、`failed=0`，且 `post_wait pending_compaction_bytes=0`，满足 debt 可回落要求。

- [ ] `P1-PER-LEVEL-COMPRESSION`
  - 现象：SSTable 数据块当前未支持按层压缩策略（如 LZ4/ZSTD/Snappy）。
  - 影响：磁盘占用与读 I/O 放大偏高，难以针对冷热层级做空间/CPU 权衡。
  - 代码定位：
    - `src/goatkv/storage/sstable/builder.rs:313`
    - `src/goatkv/storage/sstable/reader.rs:386`
    - `src/goatkv/utils/options.rs:31`
    - `src/goatkv/metadata/version_set.rs:141`
  - 验收标准：
    - 支持 per-level compression 配置并持久化格式标记。
    - 读取路径兼容解压并保留校验逻辑。
    - 给出空间占用/读延迟基准对比。

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

- [ ] `P2-UNSAFE-VALIDATION-COVERAGE-GAP`
  - 现象：SkipList/Arena 存在较多 `unsafe` 与 `unsafe impl Send/Sync`，但缺少系统化并发模型验证与 fuzz 覆盖。
  - 影响：极端并发和边界输入下可能出现 UB/竞态/内存破坏，问题定位成本高。
  - 代码定位：
    - `src/goatkv/core/skip_list/list.rs:247`
    - `src/goatkv/core/skip_list/list.rs:248`
    - `src/goatkv/core/skip_list/arena.rs:74`
    - `src/goatkv/core/skip_list/node.rs:24`
  - 验收标准：
    - 为 skip list 关键路径补充 Miri/Loom/Fuzz 验证流水线（至少覆盖插入/查询/迭代/析构）。
    - 为 `unsafe` 代码块补充不变量注释与审计清单。
    - 增加长稳压测用例并纳入 CI 的周期性任务。

- [ ] `P2-WAL-PREALLOC-BYTES-PER-SYNC`
  - 现象：WAL 写入缺少预分配与 `bytes_per_sync` 类节流参数，当前批次写入后通常直接 flush/sync。
  - 影响：高吞吐写入下 fsync 抖动与文件扩容碎片化风险较高，尾延迟不稳定。
  - 代码定位：
    - `src/goatkv/storage/wal/writer.rs:17`
    - `src/goatkv/storage/wal/writer.rs:89`
    - `src/goatkv/storage/wal/writer.rs:125`
    - `src/goatkv/storage/wal/writer.rs:131`
  - 验收标准：
    - 增加 WAL 预分配与 `bytes_per_sync` 参数配置。
    - 增加周期性 sync 策略并验证崩溃恢复正确性不回退。
    - 压测下写入尾延迟波动收敛。
  - 进展记录：
    - 2026-03-05：`WalWriterConfig` 新增 `wal_preallocate_bytes` 与 `wal_bytes_per_sync`。
      - 预分配：写入前按步长扩容（`set_len`），关闭/轮转时回收到 `logical_size`。
      - 周期 sync：`wal_sync=false` 时按累计写入字节触发 `sync_data`，`wal_sync=true` 仍保持每次 append 同步语义。
    - 2026-03-05：`KvEngineOptions` 增加 `with_wal_preallocate_bytes`、`with_wal_bytes_per_sync`，并透传到 `KvEngine::open_wal_writer`。
    - 2026-03-05：`goatkv_server` 与 `goatkv_bench` 新增对应 CLI 参数，支持运行期/基准参数化。
    - 2026-03-05：新增 WAL 回归：
      - `test_wal_writer_preallocate_and_truncate_on_drop`
      - `test_wal_writer_periodic_sync_when_wal_sync_disabled`
      - `test_wal_replay_truncates_preallocated_zero_tail`
      恢复回归 `integration_recovery` 全量通过。
    - 2026-03-05：新增参数扫描恢复回归 `recovery_replays_wal_across_prealloc_and_bytes_per_sync_profiles`，覆盖
      - default：`wal_preallocate_bytes=0`，`wal_bytes_per_sync=0`
      - 高频 sync：`wal_preallocate_bytes=4096`，`wal_bytes_per_sync=1`
      - 低频 sync：`wal_preallocate_bytes=4096`，`wal_bytes_per_sync=4MB`
      三档配置均验证“零尾截断 + 数据可见性恢复”。

## 单机 KV 补全计划（2026-03-05）

目标：在不引入分布式复制/事务协调前，完成“可作为关系型上层存储引擎的单机 KV 能力闭环”。

### 里程碑 0：基线冻结（0.5 周）

- 目标：
  - 冻结单机验收门槛与基准配置，作为后续阶段统一回归标准。
- 任务：
  - 固化回归门槛：`cargo test --lib --tests`、`cargo clippy --all-targets --all-features -- -D warnings`。
  - 固化性能基线：`goatkv_bench` `populate/randread/hotread`（包含 `row_cache=0` 对照）。
  - 在 issue 里维护阶段状态（`planned/in-progress/done`）与实际完成日期。
- 验收：
  - 在文档中记录“基线命令 + 当前基线结果 + 目标回归阈值（允许波动范围）”。

### 里程碑 1：KV 语义补全（1.5~2 周）

- 目标：
  - 补齐单机上层最常用的 KV 访问语义：`Scan`、`MultiGet`、`CAS`。
- 对应 issue：
  - `P1-API-SCAN-SNAPSHOT-CAS-MISSING`
  - `P1-MULTIGET-BATCH-READ-PATH`
- 任务：
  - proto/server/client 增加 `Scan`、`MultiGet`、`CompareAndSet`。
  - engine 层提供原子 `CAS`、快照一致 `Scan(snapshot_id)`、批量读短路径。
  - 增加 e2e：扫描一致性、CAS 并发冲突、批量读命中复用。
- 验收：
  - API 文档明确错误语义（`NotFound/Conflict/Unavailable`）。
  - e2e 覆盖“点读写 + 扫描 + CAS + 快照”基本矩阵。

### 里程碑 2：持久化与格式演进（1.5 周）

- 目标：
  - 提升可升级性与长期运行稳定性，避免格式升级/写入尾延迟成为瓶颈。
- 对应 issue：
  - `P1-ONDISK-FORMAT-VERSIONING-GAP`
  - `P2-WAL-PREALLOC-BYTES-PER-SYNC`
- 任务：
  - SSTable/MANIFEST 增加显式 format version 与兼容读取策略。
  - WAL 增加预分配与 `bytes_per_sync` 配置。
  - 增加跨版本读写兼容测试与恢复回归测试。
- 验收：
  - 输出格式兼容矩阵（forward/backward）。
  - 崩溃恢复测试不回退，写入尾延迟抖动收敛。

### 里程碑 3：可观测与健康检查（1 周）

- 目标：
  - 建立单机运维闭环，支持自动探活、容量与性能告警。
- 对应 issue：
  - `P1-OBSERVABILITY-HEALTH-GAP`
- 任务：
  - 增加 liveness/readiness 接口。
  - 暴露核心指标：QPS、错误率、p95/p99、flush/compaction backlog、cache hit/miss、队列水位。
  - 补充看板字段与告警建议阈值。
- 验收：
  - 本地可通过接口和指标快速定位“写阻塞/compaction 积压/缓存退化”。

### 里程碑 4：读路径剩余优化（1.5 周）

- 目标：
  - 补齐 scan/multiget 场景下的吞吐短板，稳定点查优势。
- 对应 issue：
  - `P1-PREFIX-BLOOM-PARTITIONED-FILTER`
  - `P1-READAHEAD-ITERATOR-OPT`
  - `P1-MULTIGET-BATCH-READ-PATH`（性能部分）
- 任务：
  - 迭代器 readahead/prefetch。
  - partitioned filter 分区缓存纳入统一容量治理与指标。
  - MultiGet 路径复用 table/block probe，减少重复开销。
- 验收：
  - scan/multiget benchmark 吞吐提升且点查不回退。

### 里程碑 5：compaction 吞吐与空间效率（1.5 周）

- 目标：
  - 提升持续写入场景下后台追赶能力与空间利用率。
- 对应 issue：
  - `P1-PARALLEL-COMPACTION-SUBCOMPACTION`
  - `P1-PER-LEVEL-COMPRESSION`
- 任务：
  - 引入 subcompaction 并行切片。
  - 支持 per-level compression 策略与读取解压兼容。
  - 增加 backlog 收敛与空间占用基准。
- 验收：
  - 高写入压测下 compaction debt 可持续下降，空间占用可预测。

### 里程碑 6：unsafe 稳定性封口（1 周）

- 目标：
  - 降低 skiplist/arena `unsafe` 路径的长期维护风险。
- 对应 issue：
  - `P2-UNSAFE-VALIDATION-COVERAGE-GAP`
- 任务：
  - 增加 Miri/Loom/Fuzz 覆盖关键并发路径。
  - 为 unsafe 代码块补充不变量注释与审计清单。
  - 增加长稳压测（soak）并纳入周期性任务。
- 验收：
  - 形成可重复执行的“并发正确性 + 内存安全”验证流水线。

### PR 级拆分清单（单机 KV）

说明：以下任务按“单个 PR 可评审、可回滚、可独立验收”拆分，默认状态 `planned`，执行时在任务后标记 `in-progress/done` 并补充完成日期。

#### Milestone 0（基线冻结）

- [x] `TASK-SM0-01` 基线回归脚本与门槛固化（status: done, 2026-03-05）
  - 目标：统一功能/静态检查入口，避免阶段回归口径漂移。
  - 关键改动文件：
    - `scripts/baseline-gate.sh`
    - `docs/goatkv/kv_engine_issue_tracker.md`
  - 回归命令：
    - `./scripts/baseline-gate.sh`
  - DoD：
    - 单命令可执行全量基线回归。
    - 文档记录失败处理规范（失败即阻断后续阶段合并）。
  - 执行规范（已固化）：
    - 脚本顺序执行：`cargo test --lib --tests`、`cargo clippy --all-targets --all-features -- -D warnings`。
    - 任一子命令返回非零即立即退出，并阻断后续里程碑任务合并。

- [x] `TASK-SM0-02` 基线 benchmark 模板与阈值登记（status: done, 2026-03-06）
  - 目标：固定 `populate/randread/hotread` 的运行参数、结果格式、阈值。
  - 关键改动文件：
    - `benches/goatkv_bench.rs`
    - `docs/goatkv/kv_engine_issue_tracker.md`
  - 回归命令：
    - `cargo check --features rocksdb --bench goatkv_bench`
    - `cargo bench --features rocksdb --bench goatkv_bench -- --directory /tmp/goatkv_sm002_baseline --engine goatkv --threads 4 --wal-sync populate --key-nums 20000 --batch-size 128 --value-size 512 --seq`
    - `cargo bench --features rocksdb --bench goatkv_bench -- --directory /tmp/goatkv_sm002_baseline --engine goatkv --threads 4 --wal-sync randread --times 40 --key-nums 20000`
    - `cargo bench --features rocksdb --bench goatkv_bench -- --directory /tmp/goatkv_sm002_baseline --engine goatkv --threads 4 --wal-sync hotread --times 60 --key-nums 20000 --hotset 512`
    - `cargo bench --features rocksdb --bench goatkv_bench -- --directory /tmp/goatkv_sm002_baseline --engine goatkv --threads 4 --wal-sync --baseline-ms-per-iter 10 --regression-threshold-pct 10 randread --times 40 --key-nums 20000`
  - DoD：
    - 结果包含吞吐、p95/p99、样本规模、执行日期。
    - 设定回退阈值（建议默认 5%~10%，按场景落盘）。
  - 关闭记录：
    - 2026-03-06：`goatkv_bench` 统一输出 `bench_result` 行，字段包含：`throughput_ops_per_sec`、`latency_samples`、`p95_ms`、`p99_ms`、`run_unix_ms`（执行时间戳）。
    - 2026-03-06：新增可选基线门禁参数：`--baseline-ms-per-iter`、`--baseline-throughput-ops-per-sec`、`--regression-threshold-pct`（默认 `10%`）；超阈值时进程以非零码退出，支持 CI gate。
    - 2026-03-06：基线参数模板（`threads=4,key_nums=20000,value=512,wal_sync`）登记：
      - `populate`: `ms_per_iter=0.153`，`throughput_ops_per_sec=833333.333`，`p95_ms=0.842`，`p99_ms=2.443`，`latency_samples=160`。
      - `randread`: `ms_per_iter=8.525`，`throughput_ops_per_sec=2346041.056`，`p95_ms=10.736`，`p99_ms=42.174`，`latency_samples=40`。
      - `hotread`: `ms_per_iter=5.800`，`throughput_ops_per_sec=3448275.862`，`p95_ms=6.289`，`p99_ms=7.464`，`latency_samples=60`。

#### Milestone 1（KV 语义补全）

- [ ] `TASK-SM1-01` Proto/Server/Client 增加 `Scan`、`MultiGet`、`CompareAndSet`（status: planned）
  - 目标：打通 API 面与调用链，定义清晰错误语义。
  - 关键改动文件：
    - `proto/goatkv.proto`
    - `src/bin/goatkv_server.rs`
    - `src/bin/goatkv_client.rs`
  - 回归命令：
    - `cargo test --lib --tests`
  - DoD：
    - gRPC 接口可端到端调用。
    - `NotFound/Conflict/Unavailable` 返回语义文档化。

- [ ] `TASK-SM1-02` 引擎层快照一致 `Scan(snapshot_id)`（status: planned）
  - 目标：扫描在快照下获得稳定视图，不受并发写入影响。
  - 关键改动文件：
    - `src/goatkv/engine.rs`
    - `src/goatkv/storage/mvcc/`
    - `src/goatkv/storage/memtable/`
  - 回归命令：
    - `cargo test --test e2e_scan`
  - DoD：
    - 并发写入 + 扫描用例结果一致可复现。
    - 快照生命周期无资源泄露。

- [ ] `TASK-SM1-03` 原子 CAS 语义与冲突检测（status: planned）
  - 目标：保证 CAS 比较与写入原子执行，冲突可观测。
  - 关键改动文件：
    - `src/goatkv/engine.rs`
    - `src/goatkv/storage/sequence/`
    - `tests/e2e/`
  - 回归命令：
    - `cargo test --test e2e_cas_conflict`
  - DoD：
    - 并发 CAS 冲突率与返回码符合预期。
    - 不引入写路径明显退化（以基线阈值判定）。

- [x] `TASK-SM1-04` `MultiGet` 批量读接口与基础复用（status: done, 2026-03-06）
  - 目标：减少 RPC 往返和重复 probe，建立后续读优化基础。
  - 关键改动文件：
    - `proto/goatkv.proto`
    - `src/bin/goatkv_server.rs`
    - `src/bin/goatkv_client.rs`
    - `src/goatkv/metrics/mod.rs`
    - `tests/e2e/`
    - `Cargo.toml`
  - 回归命令：
    - `cargo test --bin goatkv_server`
    - `cargo test --bin goatkv_client`
    - `cargo test --test e2e_multiget -- --nocapture`
  - DoD：
    - `MultiGet` 正确返回部分命中/全部 miss。
    - 比逐 key get 具备可测吞吐收益（同参数下对照）。
  - 关闭记录：
    - 2026-03-06：新增 gRPC `MultiGet`（`MultiGetRequest/MultiGetResponse/MultiGetItem`），服务端接入引擎 `multi_get` 并返回逐 key 命中状态与 value。
    - 2026-03-06：客户端新增 `multiget` 命令（单次模式 + REPL），支持一次请求多个 key 并按输入顺序打印结果。
    - 2026-03-06：新增 `e2e_multiget` 覆盖 mixed hit/miss、空 keys 非法参数、非零 `snapshot_id` 非法参数。
    - 2026-03-06：`RpcMetricsCollector` 新增 `multiget` 方法维度计数。

#### Milestone 2（持久化与格式演进）

- [x] `TASK-SM2-01` SST/MANIFEST format version 字段落地（status: done, 2026-03-05）
  - 目标：显式格式版本，支持后续演进与兼容策略。
  - 关键改动文件：
    - `src/goatkv/storage/sstable/`
    - `src/goatkv/storage/manifest/`
    - `src/goatkv/metadata/version_edit.rs`
    - `src/goatkv/metadata/version_set.rs`
  - 回归命令：
    - `cargo test --lib format_version`
    - `cargo test --lib goatkv::storage::sstable::reader::tests`
    - `cargo test --lib goatkv::metadata::version_set::tests`
    - `./scripts/baseline-gate.sh`
  - DoD：
    - 新文件写入包含版本信息。
    - 旧版本读取路径明确（接受/拒绝）并可测试。

- [x] `TASK-SM2-02` 兼容矩阵测试（forward/backward）（status: done, 2026-03-05）
  - 目标：验证跨版本读写、恢复路径行为稳定。
  - 关键改动文件：
    - `tests/integration/compat_test.rs`
    - `Cargo.toml`
    - `docs/goatkv/kv_engine_issue_tracker.md`
  - 回归命令：
    - `cargo test --test integration_compat`
  - DoD：
    - 兼容矩阵按版本维度可执行并记录结果。
    - 不兼容场景返回明确错误而非 panic。
  - 关闭记录：
    - 2026-03-05：新增 `integration_compat`，覆盖 5 组版本场景（baseline、manifest backward、sstable backward、manifest forward reject、sstable forward reject）。

- [x] `TASK-SM2-03` WAL 预分配与 `bytes_per_sync` 参数化（status: done, 2026-03-05）
  - 目标：降低尾延迟抖动，提升持续写入平滑性。
  - 关键改动文件：
    - `src/goatkv/storage/wal/writer.rs`
    - `src/goatkv/utils/options.rs`
    - `src/goatkv/core/kv_engine/engine.rs`
    - `src/bin/goatkv_server.rs`
    - `benches/goatkv_bench.rs`
  - 回归命令：
    - `cargo test --lib goatkv::storage::wal::tests`
    - `cargo test --test integration_recovery`
    - `cargo test --bin goatkv_server`
    - `cargo test --lib goatkv::utils::options::tests`
    - `./scripts/baseline-gate.sh`
  - DoD：
    - 支持配置 WAL 预分配大小与周期性 sync。
    - 崩溃恢复语义不回退。

- [x] `TASK-SM2-04` 崩溃恢复回归与参数扫描（status: done, 2026-03-05）
  - 目标：覆盖预分配/sync 配置组合下的恢复正确性。
  - 关键改动文件：
    - `tests/integration/`
    - `tests/e2e/`
  - 回归命令：
    - `cargo test --test integration_recovery`
    - `./scripts/baseline-gate.sh`
  - DoD：
    - 至少覆盖“默认/高频 sync/低频 sync”三档配置。
    - 恢复后可见数据满足 WAL durability 约束。

#### Milestone 3（可观测与健康检查）

- [x] `TASK-SM3-01` liveness/readiness 接口接入（status: done, 2026-03-05）
  - 目标：提供标准探活与就绪检查入口。
  - 关键改动文件：
    - `src/bin/goatkv_server.rs`
    - `src/goatkv/server/health.rs`
    - `tests/common/test_server.rs`
    - `tests/e2e/health_test.rs`
    - `Cargo.toml`
  - 回归命令：
    - `cargo test --test e2e_health`
    - `cargo test --bin goatkv_server`
  - DoD：
    - 进程可存活但未就绪场景返回不同状态。
    - 支持被部署系统直接探测。
  - 关闭记录：
    - 2026-03-05：新增可选 HTTP 健康探针地址 `--health-address`，暴露 `/livez`、`/readyz`。
    - 2026-03-05：优雅停机时先将 readiness 置为 not-ready，再进入 drain window，liveness 保持存活态直到进程退出。
    - 2026-03-05：新增 `e2e_health`，验证启动后探针可用及停机阶段 `live=200`、`ready=503` 的状态分离。

- [x] `TASK-SM3-02` 核心指标导出（QPS/延迟/错误率/backlog/cache）（status: done, 2026-03-05）
  - 目标：覆盖定位写阻塞和读退化所需最小指标集。
  - 关键改动文件：
    - `src/goatkv/metrics/mod.rs`
    - `src/goatkv/core/kv_engine/engine.rs`
    - `src/goatkv/core/kv_engine/writer.rs`
    - `src/bin/goatkv_server.rs`
    - `src/goatkv/server/health.rs`
    - `tests/e2e/health_test.rs`
    - `docs/goatkv/metrics_reference.md`
  - 回归命令：
    - `cargo test --bin goatkv_server`
    - `cargo test --lib goatkv::metrics::tests`
    - `cargo test --test e2e_health`
    - `cargo test --tests --no-run`
  - DoD：
    - 指标命名、标签、单位有文档定义。
    - 关键路径埋点不引入显著性能回退。
  - 关闭记录：
    - 2026-03-05：新增 `RpcMetricsCollector`，导出 RPC `requests/qps/error_rate/latency(histogram+p95+p99+avg+max)`。
    - 2026-03-05：`goatkv_server` 读写路径接入观测埋点，按 RPC method 输出请求与错误计数。
    - 2026-03-05：`/metrics` 接入健康探针 HTTP 服务，导出引擎 backlog 与队列水位（immutable backlog、pending compaction bytes、wal/mem queue、write pressure）及 cache hit/miss/evictions。
    - 2026-03-05：新增 `e2e_health::test_metrics_endpoint_exposes_core_metrics` 验证指标端点可用与关键指标存在。
    - 2026-03-05：补充 `metrics_reference.md`，定义指标命名、标签和单位；新增 `e2e_health::test_metrics_endpoint_tracks_success_and_error_requests` 验证请求/错误计数随流量增长。

- [x] `TASK-SM3-03` 告警阈值建议与运维手册（status: done, 2026-03-05）
  - 目标：给出可执行的排障基线，缩短故障定位时间。
  - 关键改动文件：
    - `docs/goatkv/ops_alert_runbook.md`
    - `docs/goatkv/metrics_reference.md`
    - `docs/goatkv/kv_engine_issue_tracker.md`
  - 回归命令：
    - 文档评审（Checklist）
  - DoD：
    - 覆盖“写阻塞/compaction 积压/cache 退化”三类场景。
    - 每类场景给出触发阈值与处理顺序。
  - 关闭记录：
    - 2026-03-05：新增 runbook，落地三类故障的告警阈值、看板字段、处置顺序和值班 checklist。
    - 2026-03-05：阈值与默认配置显式对齐（`max_immutable_memtables`、`l0_*_writes_trigger`、`soft/hard_pending_compaction_bytes_limit` 等）。

#### Milestone 4（读路径剩余优化）

- [x] `TASK-SM4-01` Iterator readahead/prefetch（status: done, 2026-03-05）
  - 目标：提升长扫描吞吐，降低块读取抖动。
  - 关键改动文件：
    - `src/goatkv/storage/sstable/reader.rs`
    - `docs/goatkv/kv_engine_issue_tracker.md`
  - 回归命令：
    - `cargo test --lib goatkv::storage::sstable::reader::tests`
    - `cargo bench --features rocksdb --bench goatkv_bench -- --directory /tmp/goatkv_sm401_scan_opt --engine both --wal-sync --threads 16 scanread --times 20 --mode scan-all`
    - `cargo bench --features rocksdb --bench goatkv_bench -- --directory /tmp/goatkv_sm401_scan_opt --engine both --wal-sync --threads 16 scanread --times 20 --mode iterator`
    - `cargo bench --features rocksdb --bench goatkv_bench -- --directory /tmp/goatkv_sm401_scan_opt --engine both --wal-sync --threads 16 randread --times 80 --key-nums 20000`
  - DoD：
    - scan 吞吐提升且点查（single get）不回退超阈值。
  - 关闭记录：
    - 2026-03-05：完成迭代器预取代码与单元回归（多块预取、单块退化）。
    - 2026-03-05：`scanread` 对照（`/tmp/goatkv_sm401_scan_opt`）显示 `scan_all=533ms` -> `scan_iterator=360ms`（约提升 `32.5%`）。
    - 2026-03-05：点查守护（同目录 `randread`）GoatKV `383ms` vs RocksDB `439ms`，未出现回退。

- [x] `TASK-SM4-02` partitioned filter 缓存治理与指标（status: done, 2026-03-05）
  - 目标：将 partitioned filter 纳入统一容量与命中率管理。
  - 关键改动文件：
    - `src/goatkv/storage/sstable/cache.rs`
    - `src/goatkv/storage/sstable/reader.rs`
    - `src/goatkv/utils/options.rs`
    - `src/goatkv/metadata/version_set.rs`
    - `src/bin/goatkv_server.rs`
    - `benches/goatkv_bench.rs`
    - `docs/goatkv/metrics_reference.md`
  - 回归命令：
    - `cargo test --lib goatkv::storage::sstable::cache::tests`
    - `cargo test --lib goatkv::storage::sstable::reader::tests`
    - `cargo test --bin goatkv_server`
    - `cargo test --test e2e_health test_metrics_endpoint_exposes_core_metrics`
  - DoD：
    - filter 分区缓存命中率可观测。
    - 缓存上限受统一配额控制。
  - 关闭记录：
    - 2026-03-05：完成共享 `FilterPartitionCache`（有容量上限与淘汰）并接入 `TableCache`，`PartitionedBloomFilter` 默认走共享缓存。
    - 2026-03-05：新增 filter cache 指标导出（bench + `/metrics`），文档补充 `goatkv_cache_filter_*` 指标定义。
    - 2026-03-05：完成 `on/off` 对照验证并保留默认 `16MB` 容量配置，确认命中率与缓存行为可观测。

- [x] `TASK-SM4-03` MultiGet 批量 probe 复用与对照基准（status: done, 2026-03-05）
  - 目标：减少重复 table/block 定位，提升批量读效率。
  - 关键改动文件：
    - `src/goatkv/core/kv_engine/engine.rs`
    - `src/goatkv/core/kv_engine/reader.rs`
    - `benches/goatkv_bench.rs`
  - 回归命令：
    - `cargo test --lib goatkv::core::kv_engine::engine::tests::test_multi_get_mixed_hits_misses_and_delete`
    - `cargo bench --features rocksdb --bench goatkv_bench -- --directory /tmp/goatkv_bench --engine goatkv --wal-sync multiget`
  - DoD：
    - `MultiGet` 吞吐达到阶段目标（相对基线可量化提升）。
    - miss-heavy/workset-fit 两类负载均有结果记录。
  - 关闭记录：
    - 2026-03-05：完成批量读 API（`KvEngine::multi_get`）与 `multiget` 基准命令；批次内复用读快照并去重重复 key，减少重复 table/block probe。
    - 2026-03-05：基线对照（`/tmp/goatkv_sm403_opt`，`threads=16,row_cache=0,filter_cache=16MB`）：miss-heavy `batch=1:484ms` -> `batch=32:430ms`（约提升 `11.2%`）；workset-fit `batch=1:137ms` -> `batch=32:144ms`（约 `5.1%` 回退，已收敛到小幅波动区间）。
    - 2026-03-05：引擎对照（`/tmp/goatkv_sm403_cmp_opt`）：miss-heavy（`key_nums=20000,batch=32,miss_ratio=80,times=120`）GoatKV `405ms` vs RocksDB `349ms`；workset-fit（`key_nums=2000,batch=32,miss_ratio=0,times=120`）GoatKV `131ms` vs RocksDB `146ms`。

#### Milestone 5（compaction 吞吐与空间效率）

- [x] `TASK-SM5-01` subcompaction 范围切分器（status: done, 2026-03-06）
  - 目标：将大 compaction task 拆为可并行子任务。
  - 关键改动文件：
    - `src/goatkv/storage/compaction/mod.rs`
    - `src/goatkv/storage/compaction/picker.rs`
    - `src/goatkv/storage/compaction/plan.rs`
    - `src/goatkv/core/flush_worker.rs`
  - 回归命令：
    - `cargo test --lib goatkv::storage::compaction::plan::tests`
    - `cargo test --lib goatkv::storage::compaction::picker::tests`
    - `cargo test --lib goatkv::core::flush_worker::tests::trivial_move_respects_grandparent_overlap_limit`
    - `cargo test --lib goatkv::core::flush_worker::tests::subcompaction_parallel_matches_single_thread_output`
  - DoD：
    - 切分逻辑遵守 key-range 不重叠与顺序约束。
    - 单线程与并行结果一致。
  - 关闭记录：
    - 2026-03-05：范围切分器与边界提取已实现并接入 compaction 计划流程。
    - 2026-03-06：补齐“单线程 vs 并行结果一致性”回归，确认切分边界无重叠且语义一致。

- [x] `TASK-SM5-02` 并行 subcompaction 执行与资源控制（status: done, 2026-03-06）
  - 目标：提高后台追赶速度且不击穿前台延迟。
  - 关键改动文件：
    - `src/goatkv/core/flush_worker.rs`
    - `src/goatkv/core/kv_engine/engine.rs`
    - `src/goatkv/utils/options.rs`
    - `benches/goatkv_bench.rs`
  - 回归命令：
    - `cargo test --lib`
    - `cargo bench --bench goatkv_bench -- --directory /tmp/goatkv_sm502_cmp2_m1_r1 --engine goatkv --wal-sync --threads 16 --max-subcompactions 1 populate --key-nums 120000 --batch-size 128 --value-size 512 --seq`
    - `cargo bench --bench goatkv_bench -- --directory /tmp/goatkv_sm502_cmp2_m4_r1 --engine goatkv --wal-sync --threads 16 --max-subcompactions 4 populate --key-nums 120000 --batch-size 128 --value-size 512 --seq`
  - DoD：
    - compaction debt 在高写入下可持续下降。
    - 写路径延迟退化在阈值内。
  - 关闭记录：
    - 2026-03-06：新增 `KvEngineOptions::max_subcompactions`（默认 1）并接入 `CompactionConfig` 与 bench CLI `--max-subcompactions`。
    - 2026-03-06：compaction 执行路径支持按 sub-range 并行构建输出 SST，串行路径保持兼容（`max_subcompactions=1`）。
    - 2026-03-06：并行失败路径改为“全部 join 后统一收敛错误 + 清理已生成文件”，避免中途返回导致后台线程游离与 orphan 输出。
    - 2026-03-06：`goatkv_bench` 新增写入错误统计（`submitted/successful/failed/unavailable_errors/thread_panics`）与 `runtime_metrics` 打点（`post_write/post_wait`）。
    - 2026-03-06：高写入 3 轮对照（`threads=16,key_nums=120000,batch_size=128,value_size=512,wal_sync`）：
      - `max_subcompactions=1`：`total_ms=423/196/132`（avg `250.3ms`）。
      - `max_subcompactions=4`：`total_ms=204/121/107`（avg `144.0ms`）。
      - 两组均 `failed=0`；`post_wait` 阶段 `pending_compaction_bytes` 与 `immutable_memtable_backlog` 均回落到 0，满足 debt 追赶目标。

- [x] `TASK-SM5-03` per-level compression 配置与读兼容（status: done, 2026-03-06）
  - 目标：按层配置压缩策略，平衡 CPU 与空间放大。
  - 关键改动文件：
    - `src/goatkv/storage/sstable/compression.rs`
    - `src/goatkv/storage/sstable/builder.rs`
    - `src/goatkv/storage/sstable/reader.rs`
    - `src/goatkv/storage/sstable/mod.rs`
    - `src/goatkv/core/flush_worker.rs`
    - `src/goatkv/core/kv_engine/engine.rs`
    - `src/goatkv/utils/options.rs`
    - `benches/goatkv_bench.rs`
    - `tests/integration/compat_test.rs`
  - 回归命令：
    - `cargo test --lib`
    - `cargo test --test integration_compat`
    - `cargo clippy --all-targets --all-features -- -D warnings`
  - DoD：
    - 各层压缩策略可配置且默认值安全。
    - 读取路径对混合压缩文件兼容。
  - 关闭记录：
    - 2026-03-06：新增 `SstableBlockCompression`（`None`/`Rle`）并将 SSTable block 编码扩展到 format v2；v2 block payload 带压缩 tag 与原始长度，v0/v1 读路径保持兼容。
    - 2026-03-06：新增 `KvEngineOptions::per_level_compression` 与 `with_level_compression`，flush(L0) 与 compaction(目标层)按层选择压缩策略；bench CLI 新增 `--l0-compression/--l1-compression/--l2-compression`。
    - 2026-03-06：收敛负载对照（`threads=16,key_nums=40000,value_size=1024,wal_sync`）中，`l1/l2=rle` 相比 `none`：磁盘占用 `50M -> 13M`（约 `-74%`），populate `73ms -> 92ms`（约 `+26%`），randread `2330ms -> 2115ms`（约 `-9%`）。

- [x] `TASK-SM5-04` 空间占用与 compaction backlog 基准（status: done, 2026-03-06）
  - 目标：量化空间效率和后台追赶效果，形成回归阈值。
  - 关键改动文件：
    - `benches/goatkv_bench.rs`
    - `docs/goatkv/kv_engine_issue_tracker.md`
  - 回归命令：
    - `cargo check --features rocksdb --bench goatkv_bench`
    - `cargo bench --features rocksdb --bench goatkv_bench -- --directory /tmp/goatkv_sm504_final/none_r1 --engine both --wal-sync --threads 16 --l0-compression none --l1-compression none --l2-compression none populate --key-nums 80000 --batch-size 128 --value-size 1024 --seq`
    - `cargo bench --features rocksdb --bench goatkv_bench -- --directory /tmp/goatkv_sm504_final/none_r1 --engine both --wal-sync --threads 16 --l0-compression none --l1-compression none --l2-compression none randread --times 80 --key-nums 80000 --value-size 1024`
    - `cargo bench --features rocksdb --bench goatkv_bench -- --directory /tmp/goatkv_sm504_final/allrle_r1 --engine goatkv --wal-sync --threads 16 --l0-compression rle --l1-compression rle --l2-compression rle populate --key-nums 80000 --batch-size 128 --value-size 1024 --seq`
    - `cargo bench --features rocksdb --bench goatkv_bench -- --directory /tmp/goatkv_sm504_final/allrle_r1 --engine goatkv --wal-sync --threads 16 --l0-compression rle --l1-compression rle --l2-compression rle randread --times 80 --key-nums 80000 --value-size 1024`
  - DoD：
    - 输出空间放大、debt 曲线、吞吐/延迟对照。
    - 写入稳定期内 backlog 斜率可解释并可复现。
  - 关闭记录：
    - 2026-03-06：`goatkv_bench` 补充 `rocksdb_write_stats`（`submitted/successful/failed/thread_panics`），避免 RocksDB 写错误被静默吞掉；本轮 3 轮对照 `write_failed=0`。
    - 2026-03-06：3 轮均值（`threads=16,key_nums=80000,value_size=1024,batch=128,wal_sync`）：
      - populate：GoatKV-none `176.0ms`，GoatKV-allRLE `107.3ms`，RocksDB-none `168.3ms`。
      - randread：GoatKV-none `6904.7ms`，GoatKV-allRLE `6921.7ms`，RocksDB-none `5160.0ms`。
      - 磁盘占用：GoatKV-none `100MB`，GoatKV-allRLE `96MB`，RocksDB-none `81MB`。
    - 2026-03-06：compaction debt 轨迹（populate 后）可复现并可解释：
      - GoatKV-none：`pending_compaction_bytes` 平均 `11,948,976`（post_write）-> `0`（post_wait），`runtime_idle_reached=true`（3/3）。
      - GoatKV-allRLE：`pending_compaction_bytes` 平均 `16,474,782`（post_write）-> `0`（post_wait），`runtime_idle_reached=true`（3/3）。

#### Milestone 6（unsafe 稳定性封口）

- [x] `TASK-SM6-01` unsafe 不变量注释与审计清单（status: done, 2026-03-06）
  - 目标：显式化 unsafe 前提，降低维护误用风险。
  - 关键改动文件：
    - `src/goatkv/core/skip_list/arena.rs`
    - `src/goatkv/core/skip_list/node.rs`
    - `src/goatkv/core/skip_list/list.rs`
    - `src/goatkv/core/skip_list/iter.rs`
    - `docs/goatkv/unsafe_audit_checklist.md`
    - `docs/goatkv/kv_engine_issue_tracker.md`
  - 回归命令：
    - `cargo test --lib goatkv::core::skip_list::tests`
    - `cargo test --lib`
  - DoD：
    - 关键 unsafe 块均有不变量说明。
    - 审计清单可用于 code review 对照。
  - 关闭记录：
    - 2026-03-06：为 `skip_list`/`arena` 中所有 `unsafe` 读写与指针转换补充 `Safety:` 注释，覆盖指针来源、生命周期、别名约束与 head-node 特例。
    - 2026-03-06：新增 `docs/goatkv/unsafe_audit_checklist.md`，建立“unsafe 点位 -> 不变量 -> review 检查项 -> 回归命令”的审计模板。

- [x] `TASK-SM6-02` Miri/Loom/Fuzz harness 建设（status: done, 2026-03-06）
  - 目标：覆盖并发交错与内存安全高风险路径。
  - 关键改动文件：
    - `src/goatkv/core/sequence_number.rs`
    - `src/goatkv/storage/wal/mod.rs`
    - `tests/fuzz/wal_corpus/`
    - `scripts/verify-ci.sh`
    - `scripts/verify-ci.ps1`
    - `Cargo.toml`
    - `docs/goatkv/kv_engine_issue_tracker.md`
  - 回归命令：
    - `cargo test -- --ignored`
    - `cargo test --features loom`
  - DoD：
    - 至少 1 条 Loom 场景、1 组 fuzz 输入集可重复执行。
    - 在 CI 或周期任务中可自动触发。
  - 关闭记录：
    - 2026-03-06：新增 Loom 并发模型用例 `loom_try_allocate_range_non_overlapping`（`sequence_number`），验证并发 range 分配不重叠。
    - 2026-03-06：新增可复现 WAL fuzz corpus（`tests/fuzz/wal_corpus/*.hex`）与 ignored 回放用例 `test_wal_fuzz_corpus_replay_is_total`。
    - 2026-03-06：`scripts/verify-ci.sh` / `scripts/verify-ci.ps1` 接入 Loom 与 fuzz corpus 回放步骤，形成自动触发路径。

- [x] `TASK-SM6-03` 长稳压测（soak）作业与失败归档（status: done, 2026-03-06）
  - 目标：验证长时间运行下内存/句柄/延迟稳定性。
  - 关键改动文件：
    - `tests/e2e/soak_test.rs`
    - `scripts/run-soak.sh`
    - `docs/goatkv/soak_failure_postmortem_template.md`
    - `docs/goatkv/kv_engine_issue_tracker.md`
    - `Cargo.toml`
  - 回归命令：
    - `GOATKV_SOAK_DURATION_SECS=5 cargo test --test e2e_soak -- --ignored --nocapture`
    - `GOATKV_SOAK_DURATION_SECS=300 scripts/run-soak.sh`
  - DoD：
    - 输出标准化报告（持续时长、错误、资源曲线）。
    - 明确失败样本归档路径和复盘模板。
  - 关闭记录：
    - 2026-03-06：新增 ignored 测试 `test_e2e_soak_read_write_stability`，支持通过环境变量配置时长/并发/阈值，并输出标准化报告（默认 `GOATKV_SOAK_REPORT=/tmp/goatkv_soak_report.json`）。
    - 2026-03-06：报告覆盖持续时长、请求错误计数、读写延迟分位、RSS/FD 变化，以及 `pending_compaction_bytes`/`write_pressure_level`/RPC 延迟等资源曲线样本。
    - 2026-03-06：新增 `scripts/run-soak.sh`，失败时归档到 `artifacts/soak_failures/<timestamp>/`，包含 `test.log`、`soak_report.json`、`postmortem_template.md` 与运行参数快照 `run_config.env`。

### 预计总工期

- 约 8~9 周（按单人连续推进估算，不含分布式能力开发）。

## 建议修复顺序

1. `P1-ONDISK-FORMAT-VERSIONING-GAP`
2. `P1-API-SCAN-SNAPSHOT-CAS-MISSING`
3. `P1-PREFIX-BLOOM-PARTITIONED-FILTER`
4. `P1-READAHEAD-ITERATOR-OPT`
5. `P1-MULTIGET-BATCH-READ-PATH`
6. `P1-PARALLEL-COMPACTION-SUBCOMPACTION`
7. `P1-PER-LEVEL-COMPRESSION`
8. `P2-WAL-PREALLOC-BYTES-PER-SYNC`
9. `P2-UNSAFE-VALIDATION-COVERAGE-GAP`

## 逐项关闭记录（执行时填写）

- Issue:
- 方案摘要:
- 关键改动文件:
- 测试命令:
- 结果:
- 备注:
