# GoatDB 设计文档

这是 GoatDB 的设计文档仓库，包含核心数据结构和算法的详细说明、实现原理和规范定义。

## 📚 文档索引

| 文档 | 描述 | 路径 |
|------|------|------|
| **跳表（Skip List）实现详解** | 详细说明了 GoatDB 中跳表数据结构的实现原理、内存布局、核心算法和性能特性。跳表被用作 MemTable 的核心数据结构。 | [`goatkv/core/skip_list_implementation.md`](goatkv/core/skip_list_implementation.md) |
| **Compaction 设计方案（P1-NO-COMPACTION）** | 归档 `L0 -> L1` 最小可用 compaction 方案，覆盖触发、选文件、合并规则、提交流程与测试计划。 | [`goatkv/core/compaction_design.md`](goatkv/core/compaction_design.md) |
| **SSTable 格式规范** | 定义了 GoatDB 中 Sorted String Table（SSTable）的文件格式，包括数据块、布隆过滤器、索引块和页脚的结构，以及编码细节和性能特性。 | [`goatkv/storage/sstable_format.md`](goatkv/storage/sstable_format.md) |
| **Write-Ahead Log (WAL) 设计与实现** | 详细说明了 GoatDB 中写前日志的设计原理、记录格式、同步/异步写入路径、崩溃恢复机制和性能优化策略。 | [`goatkv/storage/wal_design.md`](goatkv/storage/wal_design.md) |

## 🎯 文档目的

这些文档旨在：

1. **记录设计决策**：解释为什么选择特定的算法和数据结构。
2. **指导实现**：为开发人员提供详细的实现规范和技术细节。
3. **便于知识共享**：帮助新成员快速理解系统架构。
4. **支持代码审查**：提供设计的理论基础，便于评估实现正确性。

## 📁 目录结构

```
docs/
├── README.md                    # 本文档
└── goatkv/
    ├── core/
    │   ├── compaction_design.md
    │   └── skip_list_implementation.md
    └── storage/
        ├── sstable_format.md
        └── wal_design.md
```

文档目录结构镜像了源码的模块结构，便于查找相关设计文档。
## 🔧 使用说明

- 阅读文档时，建议同时查看对应的源码实现以获得完整理解。
- 文档中的术语和概念与代码中的命名保持一致。
- 如果发现文档与实现不一致，请优先考虑代码实现，并考虑更新文档。

## 📝 贡献指南

如果您想添加或修改设计文档：

1. **创建新文档**：在对应的模块目录下创建 `.md` 文件。
2. **保持结构一致**：遵循现有文档的格式和深度。
3. **链接到源码**：在相关源码文件的顶部添加文档链接注释。
4. **更新索引**：修改本 README 文件以包含新文档。

## 📖 相关资源

- **源码目录**：`src/goatkv/` - 实际实现代码
- **Rustdoc**：通过 `cargo doc --open` 生成的 API 文档
- **测试文件**：包含单元测试和集成测试，展示具体用法

## 📄 许可证

设计文档与 GoatDB 项目采用相同的许可证。
