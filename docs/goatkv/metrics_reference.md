# GoatKV Metrics Reference

This document defines the exported `/metrics` names, label keys, units, and semantics for GoatKV single-node runtime observability.

## Endpoint

- Path: `/metrics`
- Format: Prometheus text exposition (`text/plain; version=0.0.4`)
- Enablement: available when server starts with `--health-address`.

## RPC Metrics

| Metric | Type | Labels | Unit | Meaning |
|---|---|---|---|---|
| `goatkv_rpc_requests_total` | counter | none | requests | Total RPC requests observed by server handlers. |
| `goatkv_rpc_requests_success_total` | counter | none | requests | Total successful RPC requests. |
| `goatkv_rpc_requests_error_total` | counter | none | requests | Total failed RPC requests. |
| `goatkv_rpc_error_rate` | gauge | none | ratio (0~1) | `error_total / requests_total`. |
| `goatkv_rpc_qps_60s` | gauge | none | requests/s | Average QPS over trailing 60s window. |
| `goatkv_rpc_method_requests_total` | counter | `method` | requests | Requests grouped by RPC method. |
| `goatkv_rpc_method_errors_total` | counter | `method` | requests | Errors grouped by RPC method. |
| `goatkv_rpc_latency_seconds_bucket` | counter | `le` | seconds | Latency histogram buckets. |
| `goatkv_rpc_latency_seconds_sum` | counter | none | seconds | Sum of observed RPC latencies. |
| `goatkv_rpc_latency_seconds_count` | counter | none | requests | Count of observed RPC latencies. |
| `goatkv_rpc_latency_p95_seconds` | gauge | none | seconds | Approximate p95 latency from histogram buckets. |
| `goatkv_rpc_latency_p99_seconds` | gauge | none | seconds | Approximate p99 latency from histogram buckets. |
| `goatkv_rpc_latency_avg_seconds` | gauge | none | seconds | Average RPC latency. |
| `goatkv_rpc_latency_max_seconds` | gauge | none | seconds | Maximum observed RPC latency since process start. |

### `method` Label Values

- `write`
- `get`
- `update`
- `delete`
- `flush`
- `create_snapshot`
- `release_snapshot`

## Engine Backlog/Queue Metrics

| Metric | Type | Labels | Unit | Meaning |
|---|---|---|---|---|
| `goatkv_engine_immutable_memtable_backlog` | gauge | none | count | Number of immutable memtables pending flush. |
| `goatkv_engine_flush_failure_streak` | gauge | none | count | Consecutive flush failure streak. |
| `goatkv_engine_flush_circuit_open` | gauge | none | bool (0/1) | Flush circuit breaker status. |
| `goatkv_engine_l0_file_count` | gauge | none | count | Number of L0 SST files in current version. |
| `goatkv_engine_pending_compaction_bytes` | gauge | none | bytes | Estimated compaction debt bytes. |
| `goatkv_writer_wal_queue_reqs` | gauge | none | requests | Pending requests in WAL queue. |
| `goatkv_writer_wal_queue_bytes` | gauge | none | bytes | Pending bytes in WAL queue. |
| `goatkv_writer_mem_queue_reqs` | gauge | none | requests | Pending requests in Mem queue. |
| `goatkv_writer_mem_queue_bytes` | gauge | none | bytes | Pending bytes in Mem queue. |
| `goatkv_writer_wal_inflight_groups` | gauge | none | groups | WAL write groups currently in-flight. |
| `goatkv_writer_mem_inflight_groups` | gauge | none | groups | Mem apply groups currently in-flight. |
| `goatkv_writer_flush_blocked` | gauge | none | bool (0/1) | Flush barrier status (`1` means writes blocked). |
| `goatkv_writer_pressure_level` | gauge | none | enum (0/1/2) | Write pressure level: `0=normal`, `1=slowdown`, `2=stop`. |

## Read Cache Metrics

| Metric | Type | Labels | Unit | Meaning |
|---|---|---|---|---|
| `goatkv_cache_table_hits_total` | counter | none | hits | Table cache hit count. |
| `goatkv_cache_table_misses_total` | counter | none | misses | Table cache miss count. |
| `goatkv_cache_table_evictions_total` | counter | none | evictions | Table cache eviction count. |
| `goatkv_cache_row_hits_total` | counter | none | hits | Row cache hit count. |
| `goatkv_cache_row_misses_total` | counter | none | misses | Row cache miss count. |
| `goatkv_cache_row_evictions_total` | counter | none | evictions | Row cache eviction count. |
| `goatkv_cache_block_hits_total` | counter | none | hits | Block cache hit count. |
| `goatkv_cache_block_misses_total` | counter | none | misses | Block cache miss count. |
| `goatkv_cache_block_evictions_total` | counter | none | evictions | Block cache eviction count. |
| `goatkv_cache_filter_hits_total` | counter | none | hits | Partitioned filter cache hit count. |
| `goatkv_cache_filter_misses_total` | counter | none | misses | Partitioned filter cache miss count. |
| `goatkv_cache_filter_evictions_total` | counter | none | evictions | Partitioned filter cache eviction count. |

## Process Metric

| Metric | Type | Labels | Unit | Meaning |
|---|---|---|---|---|
| `goatkv_process_uptime_seconds` | gauge | none | seconds | Process uptime since server start. |
