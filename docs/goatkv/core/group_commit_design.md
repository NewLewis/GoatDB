# GoatKV 分组提交最终设计 V2（RocksDB 风格流水线，MemTable 单线程）

状态：最终设计（待实现）  
范围：只升级写入提交流程；不实现无锁跳表，不实现并发 MemTable 插入。

## 1. 最终决策（定稿）

1. **采用双队列流水线**：`WAL 队列` 与 `MemTable 队列` 分离。
2. **保留单活跃 MemTable writer**：同一时刻只有一个线程执行 MemTable apply。
3. **允许 WAL 与 MemTable 阶段并行重叠**：`组 N` 在写 MemTable 时，`组 N+1` 可以写 WAL。
4. **保留组提交**：两阶段都按 group 处理，降低固定成本。
5. **严格顺序与可见性**：仅当 MemTable 阶段成功后，请求才返回成功。
6. **失败策略 fail-fast**：WAL/MemTable 不可恢复错误时关闭写入口，后续靠 WAL 回放恢复。

> 这版是对齐 RocksDB `pipelined write` 思路的最小可落地子集：先做“阶段流水线”，暂不做“MemTable 并行写”。

## 2. 设计目标

1. 在不改跳表并发模型的前提下，显著提升多线程写吞吐。
2. 减少写请求在单 leader 串行路径上的等待时间。
3. 保持一致性语义简单可验证：`WAL durable -> MemTable apply -> publish`。
4. 为后续并发 MemTable、unordered write 保留扩展位。

## 3. 非目标

1. 不实现 `allow_concurrent_memtable_write` 等价能力。
2. 不实现 unordered write 或快照语义放宽。
3. 不在本次改造 compaction、SSTable 结构。

## 4. 总体架构

```text
Writer threads
  -> 进入 WAL 队列
  -> WAL 组长批量写 WAL (write + optional sync)
  -> 已写 WAL 的请求转入 MemTable 队列
  -> MemTable 组长按序串行 apply
  -> 发布可见并完成请求
```

### 核心收益点

1. **阶段重叠**：WAL 阶段与 MemTable 阶段可并行执行（不同组）。
2. **等待拆分**：WAL 等待不再阻塞下一组 WAL 的组装。
3. **固定成本摊薄**：WAL 组提交继续降低 `write/sync` 每条请求成本。

## 5. 队列与状态模型

## 5.1 两条队列

1. `wal_queue`：接收所有新请求，负责 WAL 分组与提交。
2. `mem_queue`：仅接收“WAL 已成功”的请求，负责 MemTable 分组与应用。

## 5.2 Writer 状态机

1. `INIT`
2. `IN_WAL_QUEUE`
3. `WAL_DONE`（已获得 sequence 并写入 WAL）
4. `IN_MEM_QUEUE`
5. `MEM_DONE`
6. `COMPLETED` / `FAILED`

## 5.3 全局序号

1. `last_allocated_seq`：WAL 阶段分配（`fetch_add`）。
2. `last_published_seq`：MemTable 阶段成功后推进。

## 6. 两阶段协议（最终）

## 6.1 阶段 W：WAL Group Commit

1. 从 `wal_queue` 队头构建 WAL group（受 `max_group_ops/max_group_bytes/group_wait_us` 限制）。
2. 为 group 一次性分配连续序号区间。
3. 合并编码 group payload，一次 `write_all`。
4. 若需要 durable（全局 `wal_sync` 或组内 `need_sync`），执行一次 `sync_data`。
5. 成功后把 group 按原顺序转移到 `mem_queue`。
6. 立即尝试唤醒/交接下一个 WAL leader（不等待 MemTable 完成）。

## 6.2 阶段 M：MemTable Apply（单线程）

1. 从 `mem_queue` 队头构建 MemTable group（可与 WAL group 大小参数独立）。
2. 按 sequence 顺序串行写入 MemTable。
3. 组成功后推进 `last_published_seq` 到 group 最大 seq。
4. 批量完成组内请求并唤醒等待线程。
5. 如触发 flush，仅做“封存 + 投递 flush 任务”，不在关键路径做重 I/O。

## 6.3 请求完成条件

请求返回成功必须同时满足：

1. 所在组 WAL 阶段成功。
2. 所在组 MemTable 阶段成功并已发布可见。

## 7. 顺序与一致性不变量

1. `wal_queue` 出队顺序决定序号分配顺序。
2. 同一 group 内序号连续。
3. `mem_queue` 入队顺序与 WAL 成功顺序一致。
4. `last_published_seq` 单调递增。
5. 任意请求 `ack` 前必须满足 `request.seq_end <= last_published_seq`。

## 8. 锁模型与并发规则

1. `wal_queue_lock` 只用于 WAL 队列入队/出队与 leader 交接，禁止持锁做 I/O。
2. `mem_queue_lock` 只用于 MemTable 队列入队/出队与 leader 交接，禁止持锁写 MemTable。
3. 锁顺序固定：不允许嵌套持有 `wal_queue_lock` 与 `mem_queue_lock`。
4. `write_gate`（若保留）仅保护 flush 边界，不包裹等待/排队逻辑。

## 9. 失败语义（矩阵）

1. WAL 失败（阶段 W）：
   - 当前 WAL group 全部失败；
   - 不进入 `mem_queue`；
   - 写入口置 `closed`，队列剩余请求快速失败。
2. MemTable 失败（阶段 M，WAL 已成功）：
   - 当前 MemTable group 失败；
   - 写入口置 `closed`；
   - 依赖重启 WAL 回放恢复。
3. Flush 失败：
   - 不影响已确认写；
   - 仅影响持久化推进和后续清理。

## 10. 背压策略（必须）

背压维度：

1. `max_wal_queue_reqs`
2. `max_wal_queue_bytes`
3. `max_mem_queue_reqs`
4. `max_mem_queue_bytes`

策略：

1. 任一队列超阈值，生产者阻塞或返回 busy（可配置）。
2. WAL/MemTable leader 每完成一组后 `notify_all`。
3. 默认先用阻塞背压，避免写放大期间内存失控。

## 11. 配置参数（建议初值）

1. `wal_max_group_ops = 4096`
2. `wal_max_group_bytes = 2MB`
3. `wal_group_wait_us = 20`
4. `mem_max_group_ops = 4096`
5. `mem_max_group_bytes = 2MB`
6. `mem_group_wait_us = 0`（先不开微等待）
7. `max_wal_queue_bytes = 256MB`
8. `max_mem_queue_bytes = 256MB`

## 12. 预期性能效果与上限

## 12.1 预期提升来源

1. WAL 与 MemTable 阶段重叠，降低端到端空转等待。
2. WAL 阶段持续组批，提升磁盘提交效率。
3. 多线程场景下，writer 等待从“全路径串行”降为“分阶段排队”。

## 12.2 仍然存在的上限

1. MemTable 仍单线程，极高并发下会受单核 apply 限制。
2. 跳表写入锁争用未消除。
3. 不含并发 MemTable 插入时，吞吐上限仍低于 RocksDB 全量配置。

## 13. 可观测性（验收必需）

必须新增指标：

1. `wal_group_size_ops_hist`
2. `wal_group_size_bytes_hist`
3. `wal_stage_wait_us_hist`
4. `wal_write_us_hist`
5. `wal_sync_us_hist`
6. `mem_group_size_ops_hist`
7. `mem_stage_wait_us_hist`
8. `mem_apply_us_hist`
9. `wal_queue_depth_gauge`
10. `mem_queue_depth_gauge`
11. `backpressure_wait_us_hist`
12. `write_fail_total`

## 14. 代码落点（只设计，不实现）

1. `src/goatkv/core/kv_engine/writer.rs`
   - 引入双队列与两套 leader 协调逻辑。
   - 拆分 `run_wal_leader_loop` / `run_mem_leader_loop`。
2. `src/goatkv/storage/wal/manager.rs`
   - 提供面向 WAL leader 的批量提交接口（同步语义明确）。
3. `src/goatkv/utils/options.rs`
   - 新增两阶段 group/backpressure 参数。
4. `src/goatkv/core/kv_engine/engine.rs`
   - flush 仍保持异步投递，不进入请求确认路径。

## 15. 落地计划（文档级）

### PR-1：协议重构骨架

1. 双队列数据结构与状态机落位。
2. 指标埋点与日志埋点。
3. 保留旧路径开关以便回滚。

### PR-2：启用流水线

1. WAL 成功后转入 MemQueue。
2. WAL/Mem 阶段并行重叠生效。
3. 故障注入验证失败矩阵。

### PR-3：调参与基准

1. 开启背压阈值。
2. 迭代 group 参数。
3. 用 `singleput/populate` 基准确认收益。

## 16. 验收标准

1. 正确性：
   - 并发写无丢写、无乱序可见；
   - 故障注入符合失败矩阵。
2. 性能：
   - 多线程 `singleput` 吞吐显著高于“单队列串行提交”版本；
   - p99 写延迟在目标负载下改善或持平且吞吐提升。
3. 稳定性：
   - 无死锁；
   - 背压触发后队列内存受控。

## 17. 与 RocksDB 的对应关系（本阶段）

已对齐：

1. 分组提交（group commit）
2. 阶段流水线（pipelined write 思路）
3. leader/follower 协调模型

暂未对齐：

1. 并发 MemTable 写入（`allow_concurrent_memtable_write`）
2. 更激进的自适应等待/自旋策略
3. unordered write 与相关快照语义

---

本设计是“先把流水线做对”的最终版。它不依赖无锁跳表，也不要求一次性重写存储结构，但能把当前瓶颈从“单路径串行”升级为“分阶段并行”，这是在你现有约束下最接近 RocksDB 且有实际收益的方案。
