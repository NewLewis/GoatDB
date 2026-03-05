# GoatKV 告警阈值与运维手册（SM3-03）

目标：基于现有 `/livez` `/readyz` `/metrics` 能力，提供可执行的告警阈值与排障顺序，覆盖：

1. 写阻塞（write stall）
2. compaction 积压（compaction backlog）
3. cache 退化（cache degradation）

## 1. 监控入口

- 健康检查：
  - `GET /livez`：进程是否存活
  - `GET /readyz`：是否可接收业务流量
- 指标：
  - `GET /metrics`（Prometheus text 格式）
- 指标定义总表：
  - 见 `docs/goatkv/metrics_reference.md`

## 2. 默认阈值基线（来自 `KvEngineOptions::default`）

- `max_immutable_memtables = 8`
- `flush_failure_streak_limit = 3`
- `l0_slowdown_writes_trigger = 20`
- `l0_stop_writes_trigger = 36`
- `soft_pending_compaction_bytes_limit = 64MB`
- `hard_pending_compaction_bytes_limit = 256MB`
- `table_cache_capacity = 64`
- `block_cache_capacity_bytes = 64MB`
- `row_cache_capacity_bytes = 32MB`

说明：若线上配置与默认值不同，告警应以线上实际值替换阈值。

## 3. 看板最小字段（建议）

- 流量与错误：
  - `goatkv_rpc_qps_60s`
  - `goatkv_rpc_error_rate`
  - `goatkv_rpc_method_requests_total{method=*}`
  - `goatkv_rpc_method_errors_total{method=*}`
- 延迟：
  - `goatkv_rpc_latency_p95_seconds`
  - `goatkv_rpc_latency_p99_seconds`
  - `goatkv_rpc_latency_max_seconds`
- 写压与积压：
  - `goatkv_writer_pressure_level`
  - `goatkv_engine_flush_circuit_open`
  - `goatkv_engine_immutable_memtable_backlog`
  - `goatkv_engine_l0_file_count`
  - `goatkv_engine_pending_compaction_bytes`
  - `goatkv_writer_wal_queue_reqs` / `goatkv_writer_mem_queue_reqs`
  - `goatkv_writer_wal_queue_bytes` / `goatkv_writer_mem_queue_bytes`
- 读缓存：
  - `goatkv_cache_table_hits_total` / `goatkv_cache_table_misses_total`
  - `goatkv_cache_row_hits_total` / `goatkv_cache_row_misses_total`
  - `goatkv_cache_block_hits_total` / `goatkv_cache_block_misses_total`

## 4. 场景 A：写阻塞（write stall）

### 4.1 告警阈值

- `Critical`（任一满足）：
  - `goatkv_writer_pressure_level >= 2` 持续 1m
  - `goatkv_engine_flush_circuit_open == 1` 持续 30s
  - `goatkv_engine_immutable_memtable_backlog >= max_immutable_memtables` 持续 1m
- `Warning`（任一满足）：
  - `goatkv_writer_pressure_level == 1` 持续 3m
  - `goatkv_engine_immutable_memtable_backlog >= 0.75 * max_immutable_memtables` 持续 5m
  - `goatkv_writer_wal_queue_reqs` 或 `goatkv_writer_mem_queue_reqs` 高于队列上限的 80% 持续 5m

### 4.2 处理顺序

1. 先确认健康状态：`/livez=200` 且 `/readyz` 是否已降为 503。
2. 若 `flush_circuit_open=1`：按“存储故障”处理（磁盘权限、磁盘满、I/O 错误），优先恢复 flush 路径。
3. 若 `pressure_level=2` 且 `l0_file_count` / `pending_compaction_bytes` 高：按“compaction 积压”处理（见场景 B）。
4. 若主要是 queue 水位高：先限流上游写入，再观察 queue 是否回落。
5. 临时缓解后，复盘并调整参数：
   - `max_immutable_memtables`
   - `l0_*_writes_trigger`
   - `soft/hard_pending_compaction_bytes_limit`

## 5. 场景 B：compaction 积压

### 5.1 告警阈值

- `Critical`：
  - `goatkv_engine_l0_file_count >= l0_stop_writes_trigger` 持续 2m
  - `goatkv_engine_pending_compaction_bytes >= hard_pending_compaction_bytes_limit` 持续 2m
- `Warning`：
  - `goatkv_engine_l0_file_count >= l0_slowdown_writes_trigger` 持续 5m
  - `goatkv_engine_pending_compaction_bytes >= soft_pending_compaction_bytes_limit` 持续 5m

### 5.2 处理顺序

1. 查看 `l0_file_count` 与 `pending_compaction_bytes` 是否持续上升（非瞬时尖峰）。
2. 同时检查写入速率：`goatkv_rpc_qps_60s` 是否异常放大。
3. 短期动作：降低写入流量，避免进入 stop 区间。
4. 中期动作：按负载调整 compaction 参数（L0 trigger、level 目标大小、pending bytes 阈值）。
5. 验证标准：`pending_compaction_bytes` 斜率转负，`pressure_level` 从 `2/1` 回落到 `0`。

## 6. 场景 C：cache 退化

### 6.1 统计口径（用 5m 增量计算）

- `block_miss_ratio = Δblock_misses / (Δblock_hits + Δblock_misses)`
- `row_miss_ratio = Δrow_misses / (Δrow_hits + Δrow_misses)`
- `table_miss_ratio = Δtable_misses / (Δtable_hits + Δtable_misses)`

建议加流量门槛：分母 `< 1000` 时不触发该类告警（避免低流量噪声）。

### 6.2 告警阈值

- `Critical`：
  - `block_miss_ratio > 0.60` 持续 10m，且 `goatkv_rpc_latency_p99_seconds` 同比基线上升 > 2x
- `Warning`：
  - `block_miss_ratio > 0.40` 持续 10m
  - 或 `row_miss_ratio > 0.50` 持续 10m
  - 或 `table_miss_ratio > 0.30` 持续 10m

### 6.3 处理顺序

1. 先确认是否工作集变化（热键迁移/扫描流量突增）。
2. 若 miss 持续且延迟同步恶化，优先扩大 cache 容量：
   - `table_cache_capacity`
   - `block_cache_capacity_bytes`
   - `row_cache_capacity_bytes`
3. 复核读流量类型（点查 vs 扫描），必要时隔离扫描任务窗口。
4. 验证标准：miss ratio 回落、`p95/p99` 恢复到基线区间。

## 7. 值班执行 Checklist

1. 先判定可用性：`/livez`、`/readyz`。
2. 确认主告警归类：写阻塞 / compaction 积压 / cache 退化。
3. 按对应场景执行“短期止血 -> 参数/容量调整 -> 指标回归验证”。
4. 记录事件：
   - 触发指标与时间窗
   - 临时动作和生效时间
   - 恢复时间与复盘结论
