# GoatDB SSTable 格式规范

## 概述

Sorted String Table（SSTable）是 GoatDB 中用于持久化存储有序键值对的核心数据结构。SSTable 是不可变的（immutable），一旦创建就不能修改，这简化了并发控制和数据一致性。

### 设计目标

1. **高效查询**：支持快速点查询和范围查询
2. **空间效率**：使用前缀压缩和重启点机制减少存储开销
3. **快速过滤**：集成布隆过滤器（Bloom Filter）快速排除不存在的键
4. **顺序访问**：数据按键顺序存储，支持高效的范围扫描
5. **容错性**：包含完整性校验和错误检测机制

## 整体文件结构

SSTable 文件按以下顺序组织：

```
+=========================================+
|                                         |
|           数据区域 (Data Region)         |
|                                         |
|            Data Block 0                 |
|                                         |
+-----------------------------------------+
|                                         |
|            Data Block 1                 |
|                                         |
+-----------------------------------------+
|                  ...                    |
+-----------------------------------------+
|                                         |
|            Data Block N                 |
|                                         |
+=========================================+
|                                         |
|          元数据区域 (Metadata Region)     |
|                                         |
|           Bloom Filter                  |
|                                         |
+=========================================+
|                                         |
|           Index Block                   |
|                                         |
+=========================================+
|                                         |
|              Footer                     |
|              (48 字节)                   |
|                                         |
+=========================================+
```

### 各部分描述

| 组件 | 大小 | 描述 |
|------|------|------|
| Data Blocks | 可变 | 存储实际的键值对数据，每个块通常为 4KB |
| Bloom Filter | 可变 | 位图，用于快速判断键是否可能存在于文件中 |
| Index Block | 可变 | 索引，用于快速定位包含特定键的数据块 |
| Footer | 48 字节 | 固定大小的尾部，包含其他组件的偏移量和魔数 |

## 数据块（Data Block）格式

数据块是 SSTable 的核心组成部分，存储实际的键值对。每个块使用前缀压缩和重启点机制优化存储和查询。

### 数据块布局

```
+===============================+
|        Entry 1 (完整存储)     |
+-------------------------------+
|        Entry 2 (前缀压缩)     |
+-------------------------------+
|        Entry 3 (前缀压缩)     |
+-------------------------------+
|             ...               |
+-------------------------------+
|        Entry N (前缀压缩)     |
+===============================+
|      Restart Point Array      |
| (每16个条目一个重启点，4字节)  |
+===============================+
|      Restart Count (4字节)    |
+===============================+
```

### 条目（Entry）格式

每个条目使用前缀压缩编码：

```
+----------------+----------------+----------------+----------------+----------------+
|  Shared Length | Unshared Length|   Value Length |  Key Suffix    |      Value     |
|   (varint)     |    (varint)    |     (varint)   |  (byte[])      |    (byte[])    |
+----------------+----------------+----------------+----------------+----------------+
```

#### 字段说明

1. **Shared Length** (varint)：与上一个键共享的前缀长度
   - 如果这是重启点后的第一个条目，Shared Length = 0
   - 示例：上一个键 "apple"，当前键 "application"，共享长度 = 3 ("app")

2. **Unshared Length** (varint)：当前键的非共享部分长度
   - 示例：键 "application"，共享长度 3，非共享部分 = "lication" (长度 7)

3. **Value Length** (varint)：值的字节长度

4. **Key Suffix** (byte[])：键的非共享部分
   - 示例：对于键 "application"，共享 "app"，后缀 = "lication"

5. **Value** (byte[])：实际的值数据

### 前缀压缩示例

假设连续写入以下键值对：

```
1. key="apple", value="fruit1"
2. key="application", value="app1"
3. key="apply", value="verb1"
```

编码过程：

```
Entry 1: shared=0, unshared=5, value_len=6, key_suffix="apple", value="fruit1"
Entry 2: shared=3, unshared=8, value_len=4, key_suffix="lication", value="app1"
Entry 3: shared=3, unshared=2, value_len=5, key_suffix="ly", value="verb1"
```

### 重启点（Restart Point）机制

为了减少前缀压缩带来的依赖链，每 16 个条目创建一个重启点：

```
条目索引:   0    1    2    ...    15    16    17    ...
重启点:     ^                        ^                 ^
         重启点0                  重启点1            重启点2
```

**重启点的作用**：
1. 中断长依赖链，允许从任意重启点开始解码
2. 加速二分查找：可以从最近的重启点开始搜索
3. 支持随机访问和快速定位

### 重启点数组格式

重启点数组位于数据块末尾，包含：
1. 重启点偏移量数组：每个重启点 4 字节（小端序）
2. 重启点数量：4 字节（小端序）

```
+================================+
|   Restart Point 0 (4字节)      |
+--------------------------------+
|   Restart Point 1 (4字节)      |
+--------------------------------+
|             ...                |
+--------------------------------+
|   Restart Point K-1 (4字节)    |
+================================+
|   Restart Count K (4字节)      |
+================================+
```

## 布隆过滤器（Bloom Filter）格式

布隆过滤器用于快速判断键是否可能存在于 SSTable 中，避免不必要的磁盘读取。

### 位图结构

布隆过滤器是一个简单的位图：

```
+=========================================+
|  Byte 0  |  Byte 1  | ... |  Byte N-1  |
+=========================================+
```

### 哈希函数

GoatDB 使用 xxHash64 哈希函数和位旋转技术生成多个哈希值：

1. 使用种子 0 计算键的 xxHash64 哈希值：`h = xxhash64(key, seed=0)`
2. 通过位旋转生成增量：`delta = (h >> 17) | (h << 15)`
3. 生成 7 个哈希位置（k=7）：
   ```
   bit_pos[0] = h % bitmap_size
   bit_pos[1] = (h + delta) % bitmap_size
   bit_pos[2] = (h + 2*delta) % bitmap_size
   ...
   bit_pos[6] = (h + 6*delta) % bitmap_size
   ```

### 操作流程

**添加键**：
```rust
for i in 0..7 {
    let bit_pos = (h + i * delta) % bitmap_size;
    bitmap[bit_pos / 8] |= 1 << (bit_pos % 8);
}
```

**检查键**：
```rust
for i in 0..7 {
    let bit_pos = (h + i * delta) % bitmap_size;
    let byte = bitmap[bit_pos / 8];
    let mask = 1 << (bit_pos % 8);
    if (byte & mask) == 0 {
        return false; // 键肯定不存在
    }
}
return true; // 键可能存在（可能有误报）
```

### 误报率

默认配置（7 个哈希函数）提供约 1% 的误报率。布隆过滤器只会有误报（false positive），不会有漏报（false negative）。

## 索引块（Index Block）格式

索引块用于快速定位包含特定键的数据块，避免顺序扫描所有数据块。索引块本身是一个标准的数据块（Block），使用与数据块相同的格式（前缀压缩、重启点机制）。

### 索引块内部结构

索引块遵循标准的数据块格式：

```
+===============================+
|        Entry 1 (完整存储)     |
++------------------------------+
|        Entry 2 (前缀压缩)     |
++------------------------------+
|        Entry 3 (前缀压缩)     |
++------------------------------+
|             ...               |
++------------------------------+
|        Entry N (前缀压缩)     |
+===============================+
|      Restart Point Array      |
| (每16个条目一个重启点，4字节)  |
+===============================+
|      Restart Count (4字节)    |
+===============================+
```

### 索引条目编码

每个索引条目是一个键值对，遵循标准的数据块条目格式：

```
+----------------+----------------+----------------+----------------+----------------+
|  Shared Length | Unshared Length|   Value Length |  Key Suffix    |      Value     |
|   (varint)     |    (varint)    |     (varint)   |  (byte[])      |    (byte[])    |
+----------------+----------------+----------------+----------------+----------------+
```

**键（Key）**：分隔符键（Separator Key）
- 分隔符键是一个特殊计算的键，表示该数据块中最大的键或两个键之间的最小分隔符

**值（Value）**：数据块位置信息，包含两个 varint：
1. **Block Offset** (varint)：数据块在文件中的起始位置
2. **Block Size** (varint)：数据块的大小（字节）

**值编码示例**：
```
假设：block_offset = 4096, block_size = 2048
编码：varint(4096) + varint(2048)
字节：[0x80, 0x20, 0x80, 0x10]
```

### 索引块与前缀压缩

由于索引条目中的分隔符键通常有公共前缀（如 "key001"、"key002"、"key003"），前缀压缩可以显著减少索引块的大小：

**示例**：
```
索引条目1: 分隔符="apple", block_offset=0, block_size=4096
索引条目2: 分隔符="application", block_offset=4096, block_size=4096

编码：
条目1: shared=0, unshared=5, value_len=... key_suffix="apple", value=...
条目2: shared=3, unshared=8, value_len=... key_suffix="lication", value=...
```

### 索引块与重启点

与数据块一样，索引块也使用重启点机制：
- 每16个索引条目创建一个重启点
- 重启点存储索引条目在块中的偏移量（4字节）
- 重启点数组位于块末尾，后跟重启点数量（4字节）

**重启点的作用**：
1. 加速索引查找：可以使用二分查找在重启点之间定位
2. 减少解码依赖：重启点后的条目不依赖之前的键

### 分隔符键计算

分隔符键是一个特殊计算的键，用于表示数据块中键的范围：

```rust
fn compute_separator(last_key: &[u8], key: &[u8]) -> Vec<u8> {
    // 1. 如果两个键长度相同且是连续递增序列
    //    例如："key001" -> "key002"
    if last_key.len() == key.len() && 
       last_key[last_key.len() - 1] + 1 == key[key.len() - 1] &&
       last_key[..last_key.len() - 1] == key[..key.len() - 1] {
        return last_key.to_vec();
    }
    
    // 2. 找到第一个不同的字节位置
    let mut i = 0;
    while i < last_key.len() && i < key.len() && last_key[i] == key[i] {
        i += 1;
    }
    
    // 3. 如果 last_key[i] < 0xFF，返回 last_key[0..i] + (last_key[i] + 1)
    if i < last_key.len() && last_key[i] < 0xFF {
        let mut result = last_key[0..i].to_vec();
        result.push(last_key[i] + 1);
        return result;
    }
    
    // 4. 否则返回完整的 last_key
    last_key.to_vec()
}
```

### 索引块示例

假设有三个数据块：
- 块 0：包含键 "apple" 到 "banana"
- 块 1：包含键 "cherry" 到 "date"  
- 块 2：包含键 "elderberry" 到 "fig"

索引块可能包含：
```
"banana"   -> [offset=0, size=4096]
"date"     -> [offset=4096, size=4096]
"fig"      -> [offset=8192, size=2048]
```

### 查询流程

1. 使用二分查找在索引块中找到第一个分隔符键 >= 目标键的条目
2. 如果找到，读取对应的数据块
3. 在数据块中使用二分查找（基于重启点）找到目标键

## 页脚（Footer）格式

页脚是 SSTable 文件的固定大小尾部，用于定位其他组件的位置。

### 页脚布局（48 字节）

```
+=========================================+
|  Bloom Filter Offset (varint，最多10字节) |
+-----------------------------------------+
|  Index Block Offset (varint，最多10字节)  |
+-----------------------------------------+
|            Padding (0-28字节)            |
+-----------------------------------------+
|        Magic Number (8字节)              |
+=========================================+
```

### 字段说明

1. **Bloom Filter Offset**：布隆过滤器在文件中的起始位置
   - varint 编码，1-10 字节
   - 必须小于 Index Block Offset

2. **Index Block Offset**：索引块在文件中的起始位置
   - varint 编码，1-10 字节
   - 必须大于 Bloom Filter Offset

3. **Padding**：0 字节填充，确保页脚总大小为 48 字节
   - 所有填充字节应为 0
   - 大小 = 48 - 8(magic) - bloom_varint_len - index_varint_len

4. **Magic Number**：文件格式标识
   - 固定值：`0x706A725F676F6174`
   - 对应的 ASCII 字符串为 "pjr_goat"（反向读取）

### 页脚读取流程

```rust
// 1. 读取文件最后48字节
file.seek(SeekFrom::End(-48))?;
let mut footer = vec![0u8; 48];
file.read_exact(&mut footer)?;

// 2. 验证魔数（最后8字节）
let magic_bytes = &footer[footer.len()-8..];
let magic = u64::from_le_bytes(magic_bytes.try_into()?);
assert_eq!(magic, MAGIC_NUMBER);

// 3. 解析偏移量
let mut cursor = 0;
let (bloom_offset, bloom_len) = varint::decode_with_length(&footer[cursor..])?;
cursor += bloom_len;

let (index_offset, index_len) = varint::decode_with_length(&footer[cursor..])?;
```

## 编码细节

### Varint 编码

Varint（可变长度整数）使用 1-10 字节编码 64 位整数：

**编码规则**：
- 每个字节使用 7 位存储数据，最高位（MSB）作为延续标志
- MSB = 1：后续还有字节；MSB = 0：这是最后一个字节
- 整数按小端序存储（最低有效位在前）

**示例**：
- 值 1 → `[0x01]`
- 值 127 → `[0x7F]`
- 值 128 → `[0x80, 0x01]`
- 值 300 → `[0xAC, 0x02]`

### 前缀压缩算法

```rust
fn encode_entry(prev_key: &[u8], key: &[u8], value: &[u8]) -> Vec<u8> {
    // 1. 计算共享前缀长度
    let shared = compute_shared_prefix(prev_key, key);
    
    // 2. 编码三个长度字段
    let mut encoded = varint::encode(shared as u64);
    encoded.extend(varint::encode((key.len() - shared) as u64));
    encoded.extend(varint::encode(value.len() as u64));
    
    // 3. 添加键的非共享部分和值
    encoded.extend(&key[shared..]);
    encoded.extend(value);
    
    encoded
}
```

## 文件创建流程

### SSTable 构建步骤

1. **初始化**：
   ```rust
   let mut builder = SSTableBuilder::new(id, dir_path)?;
   ```

2. **写入数据**：
   ```rust
   for (key, value) in sorted_entries {
       builder.write(key, value);
       
       // 数据块达到4KB时自动完成
       if builder.should_finish() {
           let (block_data, last_key) = builder.finish_data_block()?;
           // 更新索引块
           index_builder.add(last_key, current_offset, block_data.len());
           // 更新布隆过滤器
           bloom_builder.add(key);
       }
   }
   ```

3. **完成构建**：
   ```rust
   builder.finish()?;
   ```

### `finish()` 方法执行流程

1. 完成最后一个数据块
2. 写入布隆过滤器位图
3. 写入索引块
4. 计算并写入页脚
5. 刷新缓冲区到磁盘

## 文件读取流程

### SSTable 打开流程

1. **读取和验证页脚**：
   - 检查文件大小 >= 48 字节
   - 读取最后 48 字节
   - 验证魔数
   - 解析布隆过滤器和索引块偏移量

2. **加载布隆过滤器**：
   - 读取 `[bloom_offset, index_offset)` 区间的数据
   - 创建 `BloomFilter` 对象

3. **加载索引块**：
   - 读取 `[index_offset, file_size-48)` 区间的数据
   - 解析索引条目
   - 创建索引条目列表（按键排序）

4. **创建读取器**：
   ```rust
   let reader = SSTableReader {
       file_path,
       file,
       bloom_filter,
       index_entries,
   };
   ```

### 键查询流程

```rust
fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
    // 1. 布隆过滤器快速过滤
    if !self.bloom_filter.contains(key) {
        return Ok(None);
    }
    
    // 2. 在索引中查找数据块
    let (block_offset, block_size) = self.find_block_for_key(key)?;
    
    // 3. 读取数据块
    self.file.seek(SeekFrom::Start(block_offset))?;
    let mut block_data = vec![0u8; block_size as usize];
    self.file.read_exact(&mut block_data)?;
    
    // 4. 在数据块中查找键
    let block_reader = BlockReader::new(&block_data)?;
    Ok(block_reader.get(key))
}
```

### 数据块查找算法

```rust
fn find_block_for_key(&self, key: &[u8]) -> Option<(u64, u64)> {
    // 二分查找第一个分隔符键 >= 目标键
    match self.index_entries.binary_search_by(|e| e.separator.cmp(key)) {
        Ok(i) => {
            // 精确匹配
            let entry = &self.index_entries[i];
            Some((entry.block_offset, entry.block_size))
        }
        Err(i) => {
            // 插入位置：键位于第 i 个条目之前
            if i < self.index_entries.len() {
                let entry = &self.index_entries[i];
                Some((entry.block_offset, entry.block_size))
            } else if !self.index_entries.is_empty() {
                // 键大于所有分隔符，使用最后一个块
                let entry = &self.index_entries.last().unwrap();
                Some((entry.block_offset, entry.block_size))
            } else {
                None
            }
        }
    }
}
```

## 错误处理和完整性校验

### 文件完整性检查

1. **魔数验证**：确保是有效的 SSTable 文件
2. **偏移量验证**：
   - `bloom_offset < index_offset < file_size - 48`
   - 所有偏移量在文件范围内
3. **数据块验证**：
   - 重启点数组格式正确
   - 条目不会跨越块边界
4. **键顺序验证**：
   - 数据块内按键升序排列
   - 索引分隔符按键升序排列

### 错误恢复

- **损坏的页脚**：文件无法打开，需要从备份恢复
- **损坏的数据块**：只影响该块中的数据，其他块仍可访问
- **损坏的索引块**：可能影响查询性能，但可通过扫描数据块恢复

## 性能特性

### 空间效率

| 技术 | 节省空间 | 适用场景 |
|------|----------|----------|
| 前缀压缩 | 30-70% | 键有公共前缀 |
| Varint 编码 | 50-90% | 小整数（长度、偏移量） |
| 重启点 | 额外开销 | 大块（>16个条目） |

### 时间复杂度

| 操作 | 时间复杂度 | 备注 |
|------|------------|------|
| 布隆过滤器检查 | O(1) | 7次哈希计算 |
| 索引查找 | O(log N) | N = 数据块数量 |
| 数据块内查找 | O(log M) | M = 块内条目数，使用重启点 |
| 顺序扫描 | O(N) | 全表扫描 |

### 内存使用

| 组件 | 内存占用 | 备注 |
|------|----------|------|
| 索引条目 | O(N) | N = 数据块数量 |
| 布隆过滤器 | 固定大小 | 默认 1KB |
| 数据块缓存 | 可选 | 按需加载 |

## 文件命名和版本控制

### 文件命名规则

SSTable 文件按 ID 命名：
- ID < 1,000,000：`{id:06}.sst`（如 `000001.sst`）
- ID ≥ 1,000,000：`{id}.sst`（如 `1234567.sst`）

### 版本兼容性

当前版本：v1.0
- 魔数：`0x706A725F676F6174`
- 页脚大小：48 字节
- 块大小：4KB
- 重启点间隔：16 条目

未来扩展：
- 可配置的块大小
- 压缩支持（Snappy、Zstd）
- 校验和（CRC32）

## 示例文件结构

### 小型 SSTable 示例

假设包含以下键值对：
- `"apple" -> "fruit1"`
- `"banana" -> "fruit2"`
- `"cherry" -> "fruit3"`

文件布局：
```
偏移量  内容
0x0000  [Data Block 0 - 256字节]
0x0100  [Bloom Filter - 1024字节]
0x0500  [Index Block - 128字节]
0x0580  [Footer - 48字节]
```

页脚内容：
```
Bloom Offset: 0x0100 (varint: 0x80 0x02)
Index Offset: 0x0500 (varint: 0x80 0x0A)
Padding: 28个0字节
Magic: 0x706A725F676F6174
```

## 实现注意事项

### 1. 共享长度边界检查
在 `sstable/block_reader.rs` 中，`linear_search_from_start` 和 `linear_search_from_restart` 函数需要添加共享长度边界检查，防止因数据损坏导致的索引越界 panic：

```rust
// 修复前（危险）：
full_key.extend_from_slice(&prev_key[..shared as usize]);

// 修复后（安全）：
if shared as usize <= prev_key.len() {
    full_key.extend_from_slice(&prev_key[..shared as usize]);
}
```

### 2. 索引条目错误处理
在 `sstable/reader.rs` 的索引块解析中，应改进错误处理策略。当前实现静默跳过损坏的索引条目，建议改为：

```rust
// 当前（宽松）：
let (block_offset, offset_len) = match varint::decode_with_length(&offset_data) {
    Ok(result) => result,
    Err(_) => continue,  // 静默跳过
};

// 建议（严格）：
let (block_offset, offset_len) = varint::decode_with_length(&offset_data)
    .map_err(|e| io::Error::new(
        io::ErrorKind::InvalidData,
        format!("Failed to decode block offset: {}", e)
    ))?;
```

### 3. 边界条件优化
- **空 SSTable 文件**：当前代码在 `index_block_size == 0` 时返回错误，但可支持空文件作为有效情况
- **Bloom Filter 大小为零**：当 `bloom_offset == index_offset` 时可视为空布隆过滤器而非错误
- **填充字节检查**：当前仅记录警告，可改为严格验证或完全忽略

### 4. 性能优化建议
- **块缓存**：为频繁访问的数据块添加 LRU 缓存
- **预读取**：对于范围查询，可预读取连续的数据块
- **内存映射**：对于大型 SSTable，考虑使用内存映射文件提高读取性能

### 5. 并发访问
当前 `SSTableReader` 不是线程安全的（未实现 `Send`/`Sync`）。如果需要在多线程环境中使用，建议：
- 为每个线程创建独立的读取器副本
- 或使用 `Arc<Mutex<SSTableReader>>` 包装
- 或实现内部引用计数的共享读取器

### 6. 资源管理
- 文件句柄保持打开状态直到 `SSTableReader` 被丢弃
- 大型 SSTable 可能同时打开多个文件，需注意文件描述符限制
- 考虑添加 `close()` 方法显式释放资源

### 7. 测试覆盖扩展
当前测试覆盖基本功能，建议添加：
- 损坏数据恢复测试
- 并发访问测试
- 内存泄漏测试
- 性能基准测试

### 8. 兼容性考虑
- 魔数验证确保文件格式兼容性
- Varint 编码向后兼容（小值编码不变）
- 可考虑在页脚添加版本号字段以便未来扩展

## 总结

GoatDB 的 SSTable 格式设计考虑了查询性能、存储效率和实现简洁性。通过前缀压缩、布隆过滤器和多级索引的组合，提供了高效的点查询和范围查询能力。固定大小的页脚和完整的完整性校验确保了数据可靠性。

这种设计借鉴了 LevelDB/RocksDB 的 SSTable 格式，但进行了简化和优化，更适合中等规模的数据集和嵌入式使用场景。
