# GoatKV 统一错误处理设计（v1）

更新时间：2026-02-10

## 目标

- 消除 `panic` 驱动的可恢复错误处理。
- 提供统一的库层错误类型，支持跨模块传播。
- 提供统一的传输层映射（当前为 gRPC `tonic::Status`）。
- 为后续 `P0-WRITE-PATH-PANIC` 与 `P0-SSTABLE-BUILDER-PANIC` 改造打基础。

## 模块结构

- 新增：`src/goatkv/error.rs`
- 对外导出：`src/goatkv/mod.rs`
  - `pub use error::{Error, ErrorKind, Result};`

## 类型定义

- `ErrorKind`
  - `InvalidArgument`
  - `NotFound`
  - `Corruption`
  - `Conflict`
  - `Unavailable`
  - `Io`
  - `Internal`

- `Error`
  - 结构化变体：`InvalidArgument/NotFound/Corruption/...`
  - 保留底层 source：`Io { source: io::Error }`
  - `Internal` 支持可选 `source`（保留跨层错误链）
  - 兼容现有 WAL 错误：通过 `From<WalError>` 映射到顶层统一语义（`Io/Corruption`）

- `Result<T> = std::result::Result<T, Error>`

## 映射策略

- `Error::kind()`：统一分类，供指标与策略判断。
- `Error::to_status()`：统一映射 gRPC 状态码。
  - 返回对外安全的固定消息（避免将内部细节直接透传给客户端）。
  - `InvalidArgument -> INVALID_ARGUMENT`
  - `NotFound -> NOT_FOUND`
  - `Corruption -> DATA_LOSS`
  - `Conflict -> FAILED_PRECONDITION`
  - `Unavailable -> UNAVAILABLE`
  - `Io/Internal -> INTERNAL`

## 使用约束

- 库层 API：优先返回 `goatkv::Result<T>`。
- server 层：统一 `err.to_status()`，不要手工散落映射逻辑。
- 子模块错误：保留细粒度错误类型，通过 `From` 汇总到顶层 `Error`。
- 原则：可恢复错误返回 `Err`，不要 `panic!`。

## 迁移建议（按优先级）

1. `KvEngine::put/delete/put_batch` 改为返回 `Result<()>`，移除 `expect`。
2. gRPC handler 改为 `map_err(|e| e.to_status())`。
3. SSTable builder 内部 `unwrap/expect` 改 `io::Result`，并在上层转为 `Error`。
4. 逐步将 `io::Error::other` 字符串错误替换为结构化 `Error`。

## 当前状态

- 已完成：错误模块定义、导出、基础测试。
- 已完成：`KvEngine::put/put_batch/delete` 与 gRPC 写路径接入统一错误映射。
- 已完成：`SSTableBuilder` 接入 `goatkv::Result` 并移除内部 panic 风险点。
- 未完成：读路径与元数据路径的错误语义统一（区分未命中与 I/O/损坏）。
