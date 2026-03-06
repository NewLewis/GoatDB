#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DURATION_SECS="${GOATKV_SOAK_DURATION_SECS:-300}"
SAMPLE_INTERVAL_MS="${GOATKV_SOAK_SAMPLE_INTERVAL_MS:-1000}"
REPORT_PATH="${GOATKV_SOAK_REPORT:-/tmp/goatkv_soak_report_${TIMESTAMP}.json}"
LOG_PATH="/tmp/goatkv_soak_${TIMESTAMP}.log"
ARCHIVE_ROOT="${GOATKV_SOAK_ARCHIVE_ROOT:-${ROOT_DIR}/artifacts/soak_failures}"

cat <<CONFIG
[soak] start timestamp: ${TIMESTAMP}
[soak] duration_secs: ${DURATION_SECS}
[soak] sample_interval_ms: ${SAMPLE_INTERVAL_MS}
[soak] report_path: ${REPORT_PATH}
[soak] log_path: ${LOG_PATH}
CONFIG

set +e
GOATKV_SOAK_DURATION_SECS="${DURATION_SECS}" \
GOATKV_SOAK_SAMPLE_INTERVAL_MS="${SAMPLE_INTERVAL_MS}" \
GOATKV_SOAK_REPORT="${REPORT_PATH}" \
cargo test --test e2e_soak -- --ignored --nocapture 2>&1 | tee "${LOG_PATH}"
status=${PIPESTATUS[0]}
set -e

if [[ ${status} -ne 0 ]]; then
  ARCHIVE_DIR="${ARCHIVE_ROOT}/${TIMESTAMP}"
  mkdir -p "${ARCHIVE_DIR}"

  cp "${LOG_PATH}" "${ARCHIVE_DIR}/test.log"
  if [[ -f "${REPORT_PATH}" ]]; then
    cp "${REPORT_PATH}" "${ARCHIVE_DIR}/soak_report.json"
  fi
  cp "${ROOT_DIR}/docs/goatkv/soak_failure_postmortem_template.md" \
     "${ARCHIVE_DIR}/postmortem_template.md"

  {
    echo "GOATKV_SOAK_DURATION_SECS=${DURATION_SECS}"
    echo "GOATKV_SOAK_SAMPLE_INTERVAL_MS=${SAMPLE_INTERVAL_MS}"
    echo "GOATKV_SOAK_REPORT=${REPORT_PATH}"
    echo "GOATKV_SOAK_ARCHIVE_ROOT=${ARCHIVE_ROOT}"
  } > "${ARCHIVE_DIR}/run_config.env"

  echo "[soak] FAILED; archived failure sample at ${ARCHIVE_DIR}" >&2
  exit ${status}
fi

if [[ -f "${REPORT_PATH}" ]]; then
  echo "[soak] PASS; report available at ${REPORT_PATH}"
else
  echo "[soak] PASS; no report file was generated (likely skipped by environment constraints)"
fi
