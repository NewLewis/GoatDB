# GoatKV Soak Failure Postmortem Template

## 1. Incident Metadata
- Incident ID:
- Date (UTC):
- Owner:
- Branch/Commit:
- Soak job command:

## 2. Environment
- Host/OS:
- Rust toolchain:
- Build profile:
- Test env vars:

## 3. Failure Summary
- Failing assertion or error:
- First failure timestamp:
- Reproducibility (always/intermittent):

## 4. Artifacts
- `test.log` path:
- `soak_report.json` path:
- Additional traces/profiles:

## 5. Resource Curve Highlights
- RSS trend (start/end/growth):
- FD trend (start/end/growth):
- Pending compaction trend:
- Write pressure levels observed:
- RPC p95/p99 trend:

## 6. Impact Assessment
- Data correctness impact:
- Availability impact:
- Performance impact:
- Security impact:

## 7. Root Cause
- Technical root cause:
- Trigger condition:
- Why existing tests/checks missed it:

## 8. Mitigation and Recovery
- Immediate mitigation:
- Recovery validation steps:
- Customer/operator communication notes:

## 9. Corrective Actions
- Code changes:
- Test additions:
- Monitoring/alert changes:
- Owner and ETA for each action:

## 10. Closure Criteria
- Required verification commands:
- Success metrics thresholds:
- Sign-off:
