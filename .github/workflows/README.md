# GoatDB CI/CD Configuration

This directory contains GitHub Actions workflows for GoatDB.

## Workflows

### CI Workflow (`ci.yml`)

Runs on every push and pull request to main branches.

**Jobs:**

1. **Test Suite** - Runs on Linux, Windows, and macOS
   - Code formatting check (`cargo fmt`)
   - Linting with Clippy (`cargo clippy`)
   - Build verification (`cargo build`)
   - Unit tests (`cargo test --lib`)
   - Integration and E2E tests (`cargo test --test '*'`)

2. **Slow Tests** - Runs only on Linux after main tests pass
   - Placeholder for future load and performance tests
   - Currently outputs message indicating tests not yet implemented

3. **Coverage** - Generates code coverage report
   - Uses `cargo-tarpaulin`
   - Uploads to Codecov (optional)

**Key Features:**
- ✅ Automatic protoc installation via `arduino/setup-protoc`
- ✅ Unified cargo caching for faster builds
- ✅ Multi-platform testing (Linux, Windows, macOS)
- ✅ Separate slow test job structure for future expansion

### Release Workflow (`release.yml`)

Triggers on version tags (e.g., `v0.1.0`).

**Builds:**
- Linux (x86_64-unknown-linux-gnu)
- Windows (x86_64-pc-windows-msvc)
- macOS (x86_64-apple-darwin)

**Outputs:**
- Stripped release binaries (Linux/macOS)
- GitHub Release with attached assets

**Key Features:**
- ✅ Multi-platform release builds
- ✅ Caching for faster builds
- ✅ Automatic asset upload to GitHub Releases

## Running Locally

### All tests
```bash
cargo test
```

### Unit tests only
```bash
cargo test --lib
```

### Integration and E2E tests only
```bash
cargo test --test '*'
```

### Future load tests (when implemented)
```bash
cargo test --release --test 'e2e_load' -- --ignored --test-threads=1
```

## Required Secrets

For full CI/CD functionality, configure these secrets in GitHub:

- `CODECOV_TOKEN` - For code coverage uploads (optional)
- `GITHUB_TOKEN` - Automatically provided by GitHub Actions

## Cache Strategy

The CI uses a unified cache strategy:
- Cargo registry (`~/.cargo/registry`)
- Cargo git index (`~/.cargo/git`)
- Build artifacts (`target/`)

Caches are invalidated when `Cargo.lock` changes and are shared across all jobs for consistency.

## Performance Optimizations

1. **Reduced test duplication** - Integration and E2E tests run in a single step
2. **Unified caching** - Single cache configuration for better performance
3. **Platform-specific optimizations** - Binary stripping for Linux/macOS releases

## Troubleshooting

### Protoc not found
The workflow uses `arduino/setup-protoc@v3` which should handle all platforms. If it fails:
- Check the protoc version (currently set to `25.x`)
- Verify the action is up to date

### Tests timeout
Default timeout:
- Integration and E2E tests: 25 minutes

Adjust in the workflow file if needed.

### Slow tests placeholder
The slow-tests job currently only outputs a message. To add actual slow tests:
1. Create test files with `#[ignore]` attribute
2. Update the workflow to run them with `--ignored` flag

### Windows-specific issues
Windows runners may have different path handling. The workflow uses:
- PowerShell-compatible commands
- Cross-platform cargo commands
- Conditional steps for platform-specific operations (e.g., binary stripping)

## Recent Changes

### CI Configuration Updates (2024)
- **Fixed test duplication**: Merged "Run integration tests" and "Run E2E tests" into a single step
- **Unified caching**: Consolidated three separate cache steps into one unified cache configuration
- **Fixed non-existent load tests**: Replaced reference to non-existent `e2e_load` tests with placeholder
- **Added caching to release workflow**: Improved release build performance with dependency caching
- **Formatting improvements**: Consistent quoting and indentation across workflow files

These changes reduce CI runtime, improve maintainability, and ensure consistent behavior across all jobs.