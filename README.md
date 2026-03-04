# GoatDB

[![CI](https://github.com/NewLewis/GoatDB/actions/workflows/ci.yml/badge.svg)](https://github.com/NewLewis/GoatDB/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/NewLewis/GoatDB/branch/main/graph/badge.svg)](https://codecov.io/gh/NewLewis/GoatDB)

A high-performance Key-Value database built in Rust using LSM-Tree architecture.

## Features

- 🚀 **High Performance** - LSM-Tree with SkipList MemTable
- 💾 **Persistent Storage** - SSTable-based storage with WAL
- 🔍 **Bloom Filters** - Fast negative lookups
- 🌐 **gRPC API** - Modern RPC interface
- 🛡️ **Crash Recovery** - Write-Ahead Log for durability
- 📊 **Concurrent Access** - Multi-client support

## Quick Start

### Prerequisites

- Rust 1.70 or later
- Protocol Buffers compiler (protoc)

### Build

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/GoatDB.git
cd GoatDB

# Build the project
cargo build --release
```

### Run Server

```bash
cargo run --bin goatkv_server
```

### Run Server with TLS/Auth

```bash
# Server-side TLS
cargo run --bin goatkv_server -- \
  --tls-cert-path /path/to/server.crt \
  --tls-key-path /path/to/server.key

# Optional mTLS (client cert required)
cargo run --bin goatkv_server -- \
  --tls-cert-path /path/to/server.crt \
  --tls-key-path /path/to/server.key \
  --tls-client-ca-path /path/to/clients_ca.crt

# Token auth (repeatable)
cargo run --bin goatkv_server -- \
  --auth-token tokenA \
  --auth-token tokenB
```

- If `--auth-token` is configured, requests must carry either:
  - `authorization: Bearer <token>`
  - `x-api-key: <token>`
- `--tls-cert-path` and `--tls-key-path` must be provided together.
- `--tls-client-ca-path` requires TLS to be enabled.

### Run Client

```bash
# Interactive mode
cargo run --bin goatkv_client

# Single command mode
cargo run --bin goatkv_client -- put mykey myvalue
cargo run --bin goatkv_client -- get mykey
```

## Testing

```bash
# Run all tests
cargo test

# Run unit tests only
cargo test --lib

# Run E2E tests
cargo test --test 'e2e_*'

# Run load tests (slow)
cargo test --release --test 'e2e_load' -- --ignored

# Run benchmarks
cargo bench --bench goatkv_bench --features rocksdb -- --directory ./bench_data --threads 1 --engine both populate --key-nums 100000 --batch-size 1000 --value-size 1024 --seq

# Hotspot read benchmark (GoatKV table/block cache)
cargo bench --bench goatkv_bench -- --directory ./bench_data --engine goatkv --threads 8 --table-cache-capacity 128 --block-cache-capacity-mb 64 hotread --key-nums 100000 --hotset 256 --times 20
```

### E2E Behavior In Restricted Environments

- E2E tests require binding a loopback TCP port (`127.0.0.1:0`) to launch the test gRPC server.
- In sandboxed/restricted environments where loopback bind is denied (`PermissionDenied`), E2E tests now return early and are treated as skipped.
- This skip behavior is implemented in `tests/common/test_server.rs` via `should_skip_network_e2e()` and is checked at the beginning of each E2E test.
- When E2E is skipped, use non-network coverage as fallback:
  - `cargo test --lib`
  - `cargo test --test integration_recovery`

## Architecture

GoatDB uses an LSM-Tree (Log-Structured Merge-Tree) architecture:

- **MemTable**: In-memory SkipList for fast writes
- **Immutable MemTable**: Frozen MemTable waiting for flush
- **SSTable**: On-disk sorted string table
- **WAL**: Write-Ahead Log for crash recovery
- **Bloom Filter**: Probabilistic data structure for fast lookups

## CI/CD

This project uses GitHub Actions for continuous integration:

- **Multi-platform testing** (Linux, Windows, macOS)
- **Code formatting** checks with `rustfmt`
- **Linting** with `clippy`
- **Code coverage** reporting
- **Automated releases** on version tags

See [.github/workflows/README.md](.github/workflows/README.md) for details.

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
