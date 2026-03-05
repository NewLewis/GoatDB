#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

echo "[baseline-gate] step 1/2: cargo test --lib --tests"
cargo test --lib --tests

echo "[baseline-gate] step 2/2: cargo clippy --all-targets --all-features -- -D warnings"
cargo clippy --all-targets --all-features -- -D warnings

echo "[baseline-gate] all checks passed"
