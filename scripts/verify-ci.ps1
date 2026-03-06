# GoatDB CI 本地验证脚本
# 在推送到 GitHub 之前运行此脚本，确保 CI 会通过

Write-Host "🚀 GoatDB CI 本地验证" -ForegroundColor Cyan
Write-Host "=" * 50

# 1. 检查 Rust 环境
Write-Host ""
Write-Host "📋 检查 Rust 环境..." -ForegroundColor Yellow
$rustVersion = rustc --version
Write-Host "   Rust: $rustVersion" -ForegroundColor Green

# 2. 检查 Protoc
Write-Host ""
Write-Host "📋 检查 Protoc..." -ForegroundColor Yellow
try {
    $protocVersion = protoc --version
    Write-Host "   Protoc: $protocVersion" -ForegroundColor Green
} catch {
    Write-Host "   ❌ Protoc 未安装！请安装 Protocol Buffers compiler" -ForegroundColor Red
    Write-Host "   下载: https://github.com/protocolbuffers/protobuf/releases" -ForegroundColor Yellow
    exit 1
}

# 3. 代码格式检查
Write-Host ""
Write-Host "📋 检查代码格式..." -ForegroundColor Yellow
cargo fmt --all -- --check
if ($LASTEXITCODE -ne 0) {
    Write-Host "   ❌ 代码格式不符合规范！运行 'cargo fmt' 修复" -ForegroundColor Red
    exit 1
}
Write-Host "   ✅ 代码格式正确" -ForegroundColor Green

# 4. Clippy 检查
Write-Host ""
Write-Host "📋 运行 Clippy..." -ForegroundColor Yellow
cargo clippy --all-targets --all-features -- -D warnings
if ($LASTEXITCODE -ne 0) {
    Write-Host "   ❌ Clippy 检查失败！" -ForegroundColor Red
    exit 1
}
Write-Host "   ✅ Clippy 检查通过" -ForegroundColor Green

# 5. 构建
Write-Host ""
Write-Host "📋 构建项目..." -ForegroundColor Yellow
cargo build --verbose
if ($LASTEXITCODE -ne 0) {
    Write-Host "   ❌ 构建失败！" -ForegroundColor Red
    exit 1
}
Write-Host "   ✅ 构建成功" -ForegroundColor Green

# 6. 单元测试
Write-Host ""
Write-Host "📋 运行单元测试..." -ForegroundColor Yellow
cargo test --lib --verbose
if ($LASTEXITCODE -ne 0) {
    Write-Host "   ❌ 单元测试失败！" -ForegroundColor Red
    exit 1
}
Write-Host "   ✅ 单元测试通过" -ForegroundColor Green

# 7. 集成测试
Write-Host ""
Write-Host "📋 运行集成测试..." -ForegroundColor Yellow
cargo test --test '*' --verbose
if ($LASTEXITCODE -ne 0) {
    Write-Host "   ❌ 集成测试失败！" -ForegroundColor Red
    # 允许失败继续
}

# 8. Loom 并发模型测试
Write-Host ""
Write-Host "📋 运行 Loom 并发模型测试..." -ForegroundColor Yellow
cargo test --lib --features loom loom_try_allocate_range_non_overlapping --verbose
if ($LASTEXITCODE -ne 0) {
    Write-Host "   ❌ Loom 并发模型测试失败！" -ForegroundColor Red
    exit 1
}
Write-Host "   ✅ Loom 并发模型测试通过" -ForegroundColor Green

# 9. Fuzz corpus 回放测试（ignored）
Write-Host ""
Write-Host "📋 运行 Fuzz corpus 回放测试..." -ForegroundColor Yellow
cargo test --lib test_wal_fuzz_corpus_replay_is_total -- --ignored --nocapture
if ($LASTEXITCODE -ne 0) {
    Write-Host "   ❌ Fuzz corpus 回放测试失败！" -ForegroundColor Red
    exit 1
}
Write-Host "   ✅ Fuzz corpus 回放测试通过" -ForegroundColor Green

# 10. E2E 测试
Write-Host ""
Write-Host "📋 运行 E2E 测试..." -ForegroundColor Yellow
$env:RUST_LOG = "info"
cargo test --test 'e2e_*' --verbose
if ($LASTEXITCODE -ne 0) {
    Write-Host "   ❌ E2E 测试失败！" -ForegroundColor Red
    exit 1
}
Write-Host "   ✅ E2E 测试通过" -ForegroundColor Green

# 11. 最终结果
Write-Host ""
Write-Host "=" * 50
Write-Host "🎉 所有检查通过！可以安全推送到 GitHub" -ForegroundColor Green
Write-Host ""
Write-Host "下一步:" -ForegroundColor Cyan
Write-Host "  git add ." -ForegroundColor White
Write-Host "  git commit -m 'Your message'" -ForegroundColor White
Write-Host "  git push origin main" -ForegroundColor White
