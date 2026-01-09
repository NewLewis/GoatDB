# GoatDB

[![CI](https://github.com/YOUR_USERNAME/GoatDB/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/GoatDB/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/YOUR_USERNAME/GoatDB/branch/main/graph/badge.svg)](https://codecov.io/gh/YOUR_USERNAME/GoatDB)

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
```

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
