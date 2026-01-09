# GoatDB CI/CD Configuration

This directory contains GitHub Actions workflows for GoatDB.

## Workflows

### CI Workflow (`ci.yml`)

Runs on every push and pull request to main branches.

**Jobs:**

1. **Test Suite** - Runs on Linux, Windows, and macOS
   - Code formatting check (`cargo fmt`)
   - Linting with Clippy
   - Unit tests
   - Integration tests
   - E2E tests

2. **Slow Tests** - Runs only on Linux after main tests pass
   - Load tests with large datasets
   - Performance regression tests

3. **Coverage** - Generates code coverage report
   - Uses `cargo-tarpaulin`
   - Uploads to Codecov

**Key Features:**
- ✅ Automatic protoc installation via `arduino/setup-protoc`
- ✅ Cargo caching for faster builds
- ✅ Multi-platform testing
- ✅ Separate slow test job to keep CI fast

### Release Workflow (`release.yml`)

Triggers on version tags (e.g., `v0.1.0`).

**Builds:**
- Linux (x86_64-unknown-linux-gnu)
- Windows (x86_64-pc-windows-msvc)
- macOS (x86_64-apple-darwin)

**Outputs:**
- Stripped release binaries
- GitHub Release with attached assets

## Running Locally

### All tests
```bash
cargo test
```

### Unit tests only
```bash
cargo test --lib
```

### E2E tests only
```bash
cargo test --test 'e2e_*'
```

### Load tests
```bash
cargo test --release --test 'e2e_load' -- --ignored
```

## Required Secrets

For full CI/CD functionality, configure these secrets in GitHub:

- `CODECOV_TOKEN` - For code coverage uploads (optional)
- `GITHUB_TOKEN` - Automatically provided by GitHub Actions

## Cache Strategy

The CI uses three cache layers:
1. Cargo registry (`~/.cargo/registry`)
2. Cargo git index (`~/.cargo/git`)
3. Build artifacts (`target/`)

Caches are invalidated when `Cargo.lock` changes.

## Troubleshooting

### Protoc not found
The workflow uses `arduino/setup-protoc@v3` which should handle all platforms. If it fails:
- Check the protoc version (currently set to `25.x`)
- Verify the action is up to date

### Tests timeout
Default timeouts:
- Integration tests: 15 minutes
- E2E tests: 20 minutes
- Load tests: 60 minutes

Adjust in the workflow file if needed.

### Windows-specific issues
Windows runners may have different path handling. The workflow uses:
- PowerShell-compatible commands
- Cross-platform cargo commands
