# GoatKV Compaction 设计方案（P1-NO-COMPACTION）

状态：设计归档（待实现）  
更新时间：2026-02-10  
关联问题：`docs/goatkv/kv_engine_issue_tracker.md` -> `P1-NO-COMPACTION`

## 1. 背景

当前引擎仅有 `Version::needs_compaction()` 判定，但没有 compaction 调度与执行路径。结果是：

- L0 文件数量持续增长，读路径要反复扫描重叠文件；
- 旧版本和 tombstone 长期滞留，空间放大不可控；
- 后续多层级策略（L1->L2 及更高层）无法展开。

## 2. 目标与非目标

### 2.1 目标（MVP）

- 实现最小可用 `L0 -> L1` compaction，形成稳定后台闭环。
- 保证 compaction 失败不影响现有可读数据（原子提交 + 失败回滚）。
- **不做重试**：出错立即上报日志并结束本次任务。
- 提交后读路径一致，能够通过回归测试验证。

### 2.2 非目标（本期不做）

- 不实现 `L1+ -> L2+` 多层级级联 compaction。
- 不实现 sub-compaction、并行 compaction、压缩算法选择。
- 不做 snapshot-aware 的 tombstone 剪枝优化（先保守正确）。

## 3. 现状约束（与现有代码对齐）

- 元数据提交入口：`VersionSet::apply_edit()`（先 MANIFEST append+sync，再切换 current）。
- Flush 线程模型：`FlushWorker` 后台单线程 + `mpsc`。
- 读路径模型：`KvReader` 读取 `LSMState` 快照；L0 逆序，L1+ 二分。
- SSTable 读能力：有 point get；全量迭代能力需要补充公开接口。
- 清理通道能力：`CleanupTask::Sstable` 已定义并有消费端，但发送闭环未完成（见 `P1-SSTABLE-CLEANUP-PIPELINE-INCOMPLETE`）。

## 4. 方案总览

新增 compaction 后台组件，设计为与 flush 解耦的单独 worker：

1. `CompactionWorker` 接收触发任务（mpsc）。
2. 基于当前 `Version` 做 candidate 选择（仅 `L0 -> L1`）。
3. 读取输入 SSTable，k-way merge 生成新 SSTable（默认单输出文件）。
4. 构造 `VersionEdit`（删除输入文件 + 新增输出文件）并原子提交。
5. 切换 `LSMState.version` 到 `VersionSet.current()`。
6. 旧文件清理通过统一 cleanup 闭环异步执行。

## 5. 组件与职责

### 5.1 `CompactionWorker`

- 位置建议：`src/goatkv/core/compaction_worker.rs`
- 职责：
  - 单线程消费 compaction 任务，避免并发冲突；
  - 执行“选择 -> 构建 -> 提交 -> 发布新版本”完整流程；
  - 失败直接记录错误并结束本任务，不重试。

### 5.2 `CompactionPicker`

- 位置建议：`src/goatkv/metadata/compaction_picker.rs`（或并入 `version.rs`）
- 职责：
  - 从 `Version` 选择 `L0` seed 文件；
  - 扩展到与其重叠的全部 `L0` 文件；
  - 再选择与最终 key range 重叠的 `L1` 文件；
  - 输出不可变 plan（输入文件列表、key 范围、目标层级）。

### 5.3 `SSTable Merge Iterator`

- 位置建议：`src/goatkv/storage/sstable/merge_iter.rs`
- 职责：
  - 提供 SSTable 全量顺序迭代（按 InternalKey 升序字节序）；
  - 对多个输入源做 k-way merge；
  - 按 user key 执行“只保留最新版本”规则。

## 6. 核心流程（L0 -> L1）

### 6.1 触发策略

MVP 采用简单策略：

- flush 成功提交后触发一次 `maybe_compact`；
- 若 `L0` 文件数 `> l0_compaction_trigger`（默认 4）则入队；
- 若 worker 正在执行，则只保留一个“待处理信号”（避免队列膨胀）。

### 6.2 选文件算法

输入：`current Version`

步骤：

1. 选择最老的 `L0` 文件作为 seed（`L0` 内部顺序按 flush 产生顺序维护）。
2. 用 seed 的 key range 扫描 `L0`，加入所有重叠文件。
3. 若范围扩大，重复步骤 2 直到范围收敛（fixed-point）。
4. 用收敛后的范围选择全部重叠 `L1` 文件。

输出：`CompactionPlan { inputs_l0, inputs_l1, smallest_key, largest_key, target_level=1 }`

### 6.3 合并规则

按 InternalKey 有序合并（user_key 升序，seq 降序）：

- 同一 `user_key` 仅输出第一条（最新版本）。
- `Delete` tombstone 在 MVP **保留写出**，不提前丢弃。
- 生成单个 `L1` 输出文件（后续可扩展按大小切分多个文件）。

说明：保留 tombstone 是保守正确策略，避免在尚未实现下层 compaction 时误删可见删除标记。

### 6.4 提交协议（原子）

两阶段：

1. **Prepare（无锁或短锁）**
   - 分配新 file_id；
   - 落盘新 SSTable（临时文件 -> rename）。
2. **Commit（持 `VersionSet` 写锁）**
   - 校验输入文件仍存在于 current（防止 plan 漂移）；
   - 构造并提交 `VersionEdit`：删除输入 L0/L1，新增输出 L1；
   - 更新 `LSMState.version = vs.current()`。

若 Commit 前发现 plan 已漂移：放弃本次输出并删除新文件，等待下轮触发。

## 7. 并发与一致性

- compaction 线程不长时间持有 `VersionSet` 锁；重 I/O 阶段在锁外执行。
- `VersionSet::apply_edit()` 与 flush 串行化提交，保证 MANIFEST 线性顺序。
- 读路径继续通过 `Arc<Version>` 快照读取，不需要额外读锁改造。

## 8. 错误处理策略（按当前约定）

- 不重试；失败直接 `error!` 打点并结束任务。
- 失败不提交 `VersionEdit` 时，旧版本不变，数据可读性不受影响。
- 若已写出新 SSTable 但提交失败，立即发送/执行清理，避免 orphan 文件泄漏。

## 9. 与现有问题项的依赖关系

`P1-NO-COMPACTION` 想完整闭环“删除旧文件且不影响读一致性”，依赖 `P1-SSTABLE-CLEANUP-PIPELINE-INCOMPLETE`：

- 需要保证旧 SSTable 在“无引用”后再异步删除；
- 建议以 `FileMetadata` 生命周期或统一引用计数机制驱动 `CleanupTask::Sstable`；
- 在该闭环未完成前，compaction 可先只做逻辑替换并延后物理删除，但不能作为最终验收状态。

## 10. 配置项建议（MVP）

在 `KvEngineOptions` 增加：

- `enable_compaction: bool`（default `true`）
- `l0_compaction_trigger: usize`（default `4`）
- `l0_compaction_max_inputs: usize`（default `8`，防止单次任务过大）

可选（后续）：

- `target_sstable_size_bytes`
- `compaction_pending_limit`

## 11. 代码改动建议清单

最小落地文件：

- 新增：`src/goatkv/core/compaction_worker.rs`
- 新增：`src/goatkv/metadata/compaction_picker.rs`
- 新增：`src/goatkv/storage/sstable/merge_iter.rs`
- 修改：`src/goatkv/core/kv_engine/engine.rs`（初始化 worker + flush 后触发）
- 修改：`src/goatkv/utils/options.rs`（新增 compaction 配置）
- 修改：`src/goatkv/metadata/version.rs`（补充 picker 所需辅助接口，若必要）
- 修改：`src/goatkv/metadata/version_set.rs`（提交阶段 revalidate 辅助函数，若必要）

## 12. 测试计划

### 12.1 单元测试

- `compaction_picker`：
  - L0 重叠扩展正确；
  - L1 overlap 选择正确；
  - fixed-point 收敛正确。
- `merge_iter`：
  - 多输入有序合并；
  - 同 user key 只保留最新；
  - tombstone 保留行为正确。

### 12.2 集成测试

- `l0_to_l1_compaction_reduces_l0_file_count`
- `l0_to_l1_compaction_preserves_latest_value`
- `l0_to_l1_compaction_preserves_delete_tombstone`
- `compaction_commit_drift_aborts_without_data_loss`
- `compaction_failure_does_not_block_drop_or_flush_path`

### 12.3 恢复测试

- compaction 输出文件生成后、MANIFEST 提交前崩溃：重启后仍以旧 Version 为准。
- compaction 提交后崩溃：重启后可从 MANIFEST 恢复到新 Version。

## 13. 分阶段实施建议

1. Phase 1：实现 `L0 -> L1` 单线程 compaction（可手动触发 + 自动触发）。
2. Phase 2：打通 SSTable 清理闭环，满足“删除旧文件且不破坏读”。
3. Phase 3：扩展到 `L1+`、tombstone 下推清理与大小分片策略。

## 14. 验收标准（对应 Issue）

完成以下条件可关闭 `P1-NO-COMPACTION`：

- `L0` 达阈值时自动触发 `L0 -> L1` compaction。
- compaction 成功后 `L0` 文件数下降，`L1` 新文件可读。
- 替换掉的旧文件进入安全清理流程（不是永久遗留）。
- 关键集成测试与恢复测试通过，且无读一致性回归。

## 15. 实施任务清单（按 PR 顺序）

以下任务按“前置依赖最小 + 可独立回归”的顺序拆分，建议一项一 PR：

- [ ] `TASK-01` 配置与引擎接线
  - 改动：`KvEngineOptions` 增加 `enable_compaction`、`l0_compaction_trigger`、`l0_compaction_max_inputs`，并接入默认值和 builder。
  - 改动：`KvEngine` 初始化路径预留 compaction worker 字段和触发入口（先可空实现）。
  - 验收：编译通过，现有行为不变，`cargo test --lib` 通过。

- [ ] `TASK-02` CompactionWorker 骨架与生命周期
  - 改动：新增 `src/goatkv/core/compaction_worker.rs`，实现单线程 run loop、任务通道、去重触发信号（pending bit）。
  - 约束：失败不重试；单任务失败仅记录错误并返回循环。
  - 验收：新增单测覆盖“重复触发不膨胀队列”“worker drop 可退出”。

- [ ] `TASK-03` CompactionPicker（L0/L1 candidate 选择）
  - 改动：新增 `CompactionPlan` 与 picker 逻辑（L0 fixed-point 重叠扩展 + L1 overlap）。
  - 依赖：仅读 `Version` 快照，不改写元数据。
  - 验收：单测覆盖重叠扩展、范围收敛、空计划返回。

- [ ] `TASK-04` SSTable 全量迭代接口
  - 改动：为 `SSTableReader` 增加稳定的全量迭代能力（返回 InternalKey+value），供 merge 使用。
  - 约束：不破坏现有 `get()` 语义与性能路径。
  - 验收：新增 reader 迭代单测，验证有序性与边界文件行为。

- [ ] `TASK-05` Merge 执行器（多路归并 + 去重规则）
  - 改动：新增 `merge_iter`/`compaction_executor`，实现 k-way merge。
  - 规则：同 `user_key` 仅保留最新版本；tombstone 在 MVP 保留输出。
  - 验收：单测覆盖“多输入同 key 去重”“delete 保留”“输入为空/单输入退化”。

- [ ] `TASK-06` 原子提交与计划漂移保护
  - 改动：在 compaction commit 前 revalidate 输入文件仍在 current。
  - 改动：提交 `VersionEdit`（删除输入 L0/L1 + 新增输出 L1）并同步 `LSMState.version`。
  - 失败路径：若漂移或提交失败，清理新生成文件并退出本任务。
  - 验收：集成测试覆盖“commit drift abort 不丢数据”。

- [ ] `TASK-07` 触发点落地（flush -> maybe_compact）
  - 改动：flush 成功提交后调用 `maybe_schedule_compaction`。
  - 约束：仅在 `enable_compaction=true` 且 `L0 > trigger` 时触发。
  - 验收：集成测试 `l0_to_l1_compaction_reduces_l0_file_count` 通过。

- [ ] `TASK-08` SSTable 清理闭环（与 P1-SSTABLE-CLEANUP-PIPELINE-INCOMPLETE 联动）
  - 改动：打通 `CleanupTask::Sstable` 发送闭环（基于引用安全时机）。
  - 说明：该任务可单独成 PR，但必须在 `P1-NO-COMPACTION` 关闭前完成。
  - 验收：集成测试覆盖“旧表被清理且不影响在途读”。

- [ ] `TASK-09` 恢复与回归测试补齐
  - 改动：新增 compaction 场景恢复测试（提交前崩溃/提交后崩溃）。
  - 验收：`cargo test` 全量通过；新增测试在受限环境下不依赖网络端口。

- [ ] `TASK-10` 观测性与文档收口
  - 改动：关键路径日志（触发、选中输入、输出文件、提交耗时、失败原因）。
  - 改动：更新 `docs/goatkv/kv_engine_issue_tracker.md` 的关闭记录与测试命令。
  - 验收：问题项可复盘，日志能定位单次 compaction 生命周期。

## 16. 最小里程碑定义

- 里程碑 M1（可用）：完成 `TASK-01` 到 `TASK-07`，实现 `L0 -> L1` 自动 compaction。
- 里程碑 M2（可关单）：完成 `TASK-08` 到 `TASK-10`，形成“替换 + 清理 + 恢复”完整闭环。
