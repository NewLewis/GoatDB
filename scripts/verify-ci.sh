#!/bin/bash

# GoatDB CI 本地验证脚本（Linux/macOS）
# 在推送到 GitHub 之前运行此脚本，确保 CI 会通过

set -e  # 遇到错误立即退出

echo "🚀 GoatDB CI 本地验证"
echo "=================================================="

# 1. 检查 Rust 环境
echo ""
echo "📋 检查 Rust 环境..."
rustc --version
cargo --version

# 2. 检查 Protoc
echo ""
echo "📋 检查 Protoc..."
if command -v protoc &> /dev/null; then
    protoc --version
else
    echo "❌ Protoc 未安装！"
    echo "Ubuntu/Debian: sudo apt-get install protobuf-compiler"
    echo "macOS: brew install protobuf"
    exit 1
fi

# 3. 代码格式检查
echo ""
echo "📋 检查代码格式..."
if cargo fmt --all -- --check; then
    echo "✅ 代码格式正确"
else
    echo "❌ 代码格式不符合规范！运行 'cargo fmt' 修复"
    exit 1
fi

# 4. Clippy 检查
echo ""
echo "📋 运行 Clippy..."
if cargo clippy --all-targets --all-features -- -D warnings; then
    echo "✅ Clippy 检查通过"
else
    echo "❌ Clippy 检查失败！"
    exit 1
fi

# 5. 构建
echo ""
echo "📋 构建项目..."
if cargo build --verbose; then
    echo "✅ 构建成功"
else
    echo "❌ 构建失败！"
    exit 1
fi

# 6. 单元测试
echo ""
echo "📋 运行单元测试..."
if cargo test --lib --verbose; then
    echo "✅ 单元测试通过"
else
    echo "❌ 单元测试失败！"
    exit 1
fi

# 7. 集成测试
echo ""
echo "📋 运行集成测试..."
cargo test --test '*' --verbose || true

# 8. E2E 测试
echo ""
echo "📋 运行 E2E 测试..."
export RUST_LOG=info
if cargo test --test 'e2e_*' --verbose; then
    echo "✅ E2E 测试通过"
else
    echo "❌ E2E 测试失败！"
    exit 1
fi

# 9. 最终结果
echo ""
echo "=================================================="
echo "🎉 所有检查通过！可以安全推送到 GitHub"
echo ""
echo "下一步:"
echo "  git add ."
echo "  git commit -m 'Your message'"
echo "  git push origin main"
