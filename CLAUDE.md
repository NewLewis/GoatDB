# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GoatDB is a high-performance key-value storage engine written in Rust using LSM-Tree (Log-Structured Merge-Tree) architecture. It provides persistent storage with crash recovery via WAL (Write-Ahead Log) and a gRPC API for client access.

## Common Development Commands

### Build
```bash
cargo build --release              # Optimized release build
cargo build --bin goatkv_server    # Build server only
cargo build --bin goatkv_client    # Build client only
```

### Testing
```bash
cargo test                         # Run all tests
cargo test --lib                   # Unit tests only
cargo test --test '*'              # Integration and E2E tests
cargo test --test e2e_basic_crud   # Specific E2E test
cargo test -- --ignored            # Run ignored (slow/load) tests
cargo test -- --test-threads=1     # Single-threaded for debugging
```

### Linting and Formatting
```bash
cargo fmt --all -- --check         # Check formatting
cargo fmt                          # Format code
cargo clippy --all-targets --all-features -- -D warnings  # Lint with clippy
```

### Running
```bash
cargo run --bin goatkv_server      # Start gRPC server
cargo run --bin goatkv_client      # Interactive CLI client
cargo run --bin goatkv_client -- put key value  # Single command
```

### Protocol Buffers
The project uses `tonic-prost-build` in `build.rs` to compile `proto/goatkv.proto`. The build happens automatically via `cargo build`.

## Architecture Overview

### LSM-Tree Data Flow

1. **Write Path**: Client → WAL (for durability) → MemTable (SkipList) → [background flush] → SSTable
2. **Read Path**: MemTable → Immutable MemTable → SSTables (newest to oldest)

### Core Components

#### Storage Engine (`src/goatkv/core/kv_engine.rs`)
- **KvEngine**: Main orchestrator coordinating all components
- Manages WAL, MemTable flush, SSTable compaction, and sequence numbers
- Thread-safe via Arc<Mutex> and Arc<RwLock> wrappers

#### In-Memory Storage
- **MemTable** (`mem_table.rs`): Thread-safe wrapper around SkipList with size limits
- **SkipList** (`skip_list.rs`): Arena-allocated SkipList implementation
  - Uses `#[repr(C)]` for optimal memory layout
  - Custom arena allocator reduces fragmentation
  - Max height: 32 levels for probabilistic height distribution
  - See `docs/goatkv/core/skip_list_implementation.md` for details

#### Storage Layer (`src/goatkv/storage/`)
- **SSTable** (`sstable_*.rs`): Immutable on-disk files with block-based format
  - See `docs/goatkv/storage/sstable_format.md` for format specification
- **WAL Manager** (`wal_manager.rs`): Write-Ahead Log for crash recovery
- **Bloom Filter** (`bloom_*.rs`): Probabilistic structure for fast negative lookups
- **Block** (`block_*.rs`): Data block encoding with prefix compression

#### Encoding (`src/goatkv/encoding/`)
- **InternalKey**: Encodes user keys with sequence number and operation kind (PUT/DELETE/TOMBSTONE)
- **coding.rs**: Varint encoding and length-prefixed slice utilities

#### Metadata (`src/goatkv/metadata/`)
- **VersionEdit**: Incremental manifest changes for serialization
  - Tracks: log number, file numbers, compact pointers, added/deleted files
  - Used for MANIFEST file format

### gRPC API

The service definition is in `proto/goatkv.proto`:
- `Write`: Insert a new key-value pair
- `Get`: Retrieve value by key
- `Update`: Update existing key-value pair
- `Delete`: Remove a key
- `Flush`: Force memtable flush to SSTable

### Multi-Level Organization

The LSM tree organizes SSTables into levels (typically 0-6):
- Level 0: Overlapping key ranges from flushed memtables
- Level 1-6: Non-overlapping key ranges, exponentially increasing size

### Concurrency Model

- **Writes**: WAL append (mutex-protected) → MemTable insert (concurrent via SkipList)
- **Reads**: Lock-free reads from current MemTable and immutable snapshots
- **Flush**: Background worker freezes memtable, writes to SSTable atomically
- **Compaction**: (To be implemented) Merges multiple SSTables

## Key Design Patterns

### Arena Allocation
SkipList nodes allocate from a custom arena (`Arena` in `skip_list.rs`) for better cache locality and reduced malloc overhead.

### Internal Keys
All keys are encoded as `InternalKey`:
```
[user_key] [sequence_number (7 bytes)] [kind (1 byte)]
```
This enables MVCC and proper ordering of operations.

### Immutable Snapshots
When flushing, the current MemTable becomes immutable (`ImmutableMemTable`) while a new MemTable becomes active. This allows reads to proceed without blocking writes.

### Path Management
`DbPathManager` centralizes all file path generation for:
- WAL files
- SSTables by level and file number
- Manifest files

## Testing Structure

### Unit Tests
Inline `#[cfg(test)]` modules within source files.

### Integration Tests
Located in `tests/common/` with shared utilities:
- `test_server.rs`: Test server fixture
- `fixtures.rs`: Shared test data

### E2E Tests
In `tests/e2e/` with dedicated harness files:
- `basic_crud_test.rs`: Basic operations
- `multi_client_test.rs`: Concurrent client access

Tests in subdirectories (`tests/e2e/`, `tests/integration/`) must be explicitly configured in `Cargo.toml` under `[[test]]` sections.

## Configuration

### Engine Options (`src/goatkv/utils/options.rs`)
- `data_dir`: Database directory (default: `./goatdb_data`)
- `mem_table_size`: Maximum memtable size before flush (default: 4MB)
- `recover_from_wal`: Enable crash recovery on startup (default: true)

### Test Configuration
`KvEngineOptions::for_test()` creates in-memory databases for testing.

## Important Invariants

1. **Sequence Numbers**: Monotonically increasing, assigned per write operation
2. **WAL Ordering**: All writes must be appended to WAL before memtable insert
3. **Immutable SSTables**: Once written, SSTable files never change (deleted via manifest)
4. **Level Ordering**: Within each level (except L0), key ranges never overlap

## Code Organization Notes

- **Modular design**: Each module has clear interfaces and minimal dependencies
- **Error handling**: Extensive use of `Result<T, E>` with descriptive errors
- **Thread safety**: Shared state uses `Arc<Mutex>` or `Arc<RwLock>` appropriately
- **Memory efficiency**: Uses `bytes::Bytes` for zero-copy buffer management

## Current Development Focus

The `metadata` branch contains work on VersionEdit for manifest file serialization, tracking which SSTables exist at each level and their key ranges.
