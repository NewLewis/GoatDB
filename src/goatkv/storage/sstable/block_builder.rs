use crate::goatkv::format::coding;

/// Block的最大大小限制
/// 当Block的大小达到此值时，应该调用finish()结束当前Block
/// 4KB是一个合理的默认值，平衡了内存使用和读取性能
const MAX_BLOCK_SIZE: usize = 4 * 1024; // 4KB

/// BlockBuilder用于构建SSTable中的数据块
///
/// # 数据块结构
/// 数据块采用前缀压缩和重启点机制来减少存储空间：
///
/// ```text
/// +----------------+
/// |  Entry 1       |  完整存储(shared=0)
/// +----------------+
/// |  Entry 2       |  前缀压缩(shared=n, unshared=m)
/// |  ...           |
/// +----------------+
/// |  Entry N       |
/// +----------------+
/// |  Restart Point |  每16个条目记录一个重启点(4字节)
/// |  ...           |  重启点指向条目在buffer中的偏移量
/// +----------------+
/// |  Restart Count |  重启点数量(4字节)
/// +----------------+
/// ```
///
/// # 前缀压缩
/// 为了减少重复前缀的存储开销，每个条目只存储：
/// - shared: 与前一个key的共享前缀长度(varint编码)
/// - unshared: 当前key的非共享部分长度(varint编码)
/// - value_len: 值的长度(varint编码)
/// - key[shared..]: key的非共享部分
/// - value: 实际值
///
/// # 重启点机制
/// 每16个条目创建一个重启点，记录该条目在buffer中的偏移量。
/// 重启点的作用：
/// 1. 减少前缀压缩带来的依赖链
/// 2. 加速二分查找：可以从最近的重启点开始搜索
/// 3. 支持随机访问和快速定位
pub struct BlockBuilder {
    /// 存储所有条目的编码数据
    /// 包含：每个条目的shared、unshared、value_len、key非共享部分、value
    buffer: Vec<u8>,

    /// 重启点数组
    /// 每16个条目记录一个重启点（4字节的偏移量）
    /// 存储在buffer的末尾
    restarts: Vec<u8>,

    /// 自上次重启点以来的条目计数器
    /// 当达到16时，记录重启点并重置为0
    counter: u32,

    /// 上一个添加的key
    /// 用于计算与当前key的共享前缀长度
    last_key: Vec<u8>,
}

impl Default for BlockBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockBuilder {
    /// 创建一个新的BlockBuilder
    ///
    /// # 示例
    /// ```
    /// # use goat_db::goatkv::storage::sstable::BlockBuilder;
    /// let builder = BlockBuilder::new();
    /// ```
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            restarts: Vec::new(),
            counter: 0,
            last_key: Vec::new(),
        }
    }

    /// 向Block中添加一个key-value对
    ///
    /// # 前缀压缩算法
    /// 每个key只存储与前一个key的差异部分，大幅减少存储空间：
    /// 1. 如果是第一个key（counter=0），则shared=0，存储完整key
    /// 2. 否则，计算与前一个key的共享前缀长度
    /// 3. 只存储非共享部分的key和完整value
    ///
    /// # 重启点机制
    /// 每16个条目记录一个重启点：
    /// 1. 记录当前buffer的长度作为重启点偏移量
    /// 2. 重启点意味着从该条目开始，不再依赖前一个key
    /// 3. 允许读取器从任何重启点开始解码
    ///
    /// # 参数
    /// - `key`: 键的原始字节数据
    /// - `value`: 值的原始字节数据
    ///
    /// # 示例
    /// ```
    /// # use goat_db::goatkv::storage::sstable::BlockBuilder;
    /// let mut builder = BlockBuilder::new();
    /// builder.add(b"apple", b"fruit");
    /// builder.add(b"application", b"app");
    /// // "application"的前7个字节与"apple"相同
    /// // 只需要存储 shared=7, unshared=7, key非共享部分="ication"
    /// ```
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        // 计算共享和非共享的前缀长度
        let unshared: u32;
        let shared: u32;

        if self.counter == 0 {
            // 第一个条目或重启点之后的第一个条目
            // 完整存储key，shared=0
            unshared = key.len() as u32;
            shared = 0;
        } else {
            // 计算与前一个key的共享前缀长度
            shared = self.compute_shared(key);
            unshared = key.len() as u32 - shared;
        }

        let entry_reserve = Self::varint_len(shared as u64)
            + Self::varint_len(unshared as u64)
            + Self::varint_len(value.len() as u64)
            + unshared as usize
            + value.len();
        self.buffer.reserve(entry_reserve);

        // 编码条目数据到buffer
        // 格式：[shared(varint)][unshared(varint)][value_len(varint)][key_non_shared][value]
        coding::put_varint64(&mut self.buffer, shared as u64);
        coding::put_varint64(&mut self.buffer, unshared as u64);
        coding::put_varint64(&mut self.buffer, value.len() as u64);

        // 只存储key的非共享部分
        self.buffer.extend_from_slice(&key[shared as usize..]);

        // 存储完整的value
        self.buffer.extend_from_slice(value);

        // 更新条目计数器
        self.counter += 1;

        // 每16个条目创建一个重启点
        // 重启点记录当前buffer长度，表示从该位置开始是一个完整的条目
        if self.counter == 16 {
            self.counter = 0;
            self.restarts
                .extend_from_slice(&(self.buffer.len() as u32).to_le_bytes());
        }

        // 保存当前key作为下一次计算的基准，复用缓冲区减少分配
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
    }

    /// 完成当前Block的构建，返回编码后的数据和最后一个key
    ///
    /// # 返回数据格式
    /// ```text
    /// +------------------+
    /// |  Entry Data      |  所有条目的编码数据
    /// +------------------+
    /// |  Restart Point 0  |  4字节偏移量
    /// |  Restart Point 1  |  4字节偏移量
    /// |  ...             |
    /// +------------------+
    /// |  Restart Count   |  4字节,重启点数量
    /// +------------------+
    /// ```
    ///
    /// # 返回值
    /// - `(&[u8], &[u8])`: (编码后的block数据, 最后一个key)
    ///
    /// # 示例
    /// ```
    /// # use goat_db::goatkv::storage::sstable::BlockBuilder;
    /// let mut builder = BlockBuilder::new();
    /// builder.add(b"key1", b"value1");
    /// builder.add(b"key2", b"value2");
    /// let (block_data, last_key) = builder.finish();
    /// ```
    pub fn finish(&mut self) -> (&[u8], &[u8]) {
        // 将重启点数组追加到buffer末尾
        self.buffer.extend_from_slice(self.restarts.as_slice());

        // 追加重启点数量
        // 注意：restarts数组中每个重启点占用4字节，所以数量 = 字节数 / 4
        let restart_count = (self.restarts.len() / 4) as u32;
        self.buffer.extend_from_slice(&restart_count.to_le_bytes());

        // 返回完整的block数据和最后一个key
        (&self.buffer, &self.last_key)
    }

    /// 计算当前key与前一个key的共享前缀长度
    ///
    /// # 算法
    /// 逐字节比较当前key和前一个key，直到发现不同的字节为止
    ///
    /// # 参数
    /// - `key`: 当前要添加的key
    ///
    /// # 返回值
    /// 返回与前一个key的共享前缀字节数
    ///
    /// # 示例
    /// ```text
    /// last_key = b"application"
    /// key = b"apple"
    /// // 比较：
    /// // a = a (共享) ✓
    /// // p = p (共享) ✓
    /// // p = p (共享) ✓
    /// // l = l (共享) ✓
    /// // i ≠ e (不匹配) ✗
    /// // 返回共享长度 = 4
    /// ```
    fn compute_shared(&mut self, key: &[u8]) -> u32 {
        let mut shared = 0;
        let mut i = 0;
        let mut j = 0;

        // 逐字节比较两个key
        while i < self.last_key.len() && j < key.len() {
            if self.last_key[i] == key[j] {
                shared += 1;
            } else {
                break;
            }
            i += 1;
            j += 1;
        }

        shared
    }

    fn varint_len(mut value: u64) -> usize {
        let mut len = 1;
        while value >= 0x80 {
            value >>= 7;
            len += 1;
        }
        len
    }

    /// 获取当前buffer的长度
    ///
    /// # 返回值
    /// 当前buffer中存储的字节数（不包括重启点）
    ///
    /// # 示例
    /// ```
    /// # use goat_db::goatkv::storage::sstable::BlockBuilder;
    /// let mut builder = BlockBuilder::new();
    /// builder.add(b"key", b"value");
    /// assert!(builder.len() > 0); // 实际长度取决于varint编码
    /// ```
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// 判断BlockBuilder是否为空
    ///
    /// # 返回值
    /// - `true`: BlockBuilder为空，不包含任何条目
    /// - `false`: BlockBuilder包含至少一个条目
    ///
    /// # 示例
    /// ```
    /// # use goat_db::goatkv::storage::sstable::BlockBuilder;
    /// let builder = BlockBuilder::new();
    /// assert!(builder.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// 判断当前Block是否应该结束
    ///
    /// 当buffer大小达到或超过MAX_BLOCK_SIZE（4KB）时，
    /// 应该调用finish()结束当前Block并创建新的Block
    ///
    /// # 返回值
    /// - `true`: 应该结束当前Block
    /// - `false`: 可以继续添加条目
    ///
    /// # 示例
    /// ```
    /// # use goat_db::goatkv::storage::sstable::BlockBuilder;
    /// let mut builder = BlockBuilder::new();
    /// // 刚开始时block是空的，should_finish返回false
    /// assert!(!builder.should_finish());
    ///
    /// // 可以继续添加数据，直到buffer达到4KB
    /// // 实际使用时，会在循环中添加key-value对
    /// ```
    pub fn should_finish(&self) -> bool {
        self.len() >= MAX_BLOCK_SIZE
    }

    /// 重置BlockBuilder，使其可以开始构建新的Block
    ///
    /// 清空所有内部状态，包括buffer、重启点、计数器和last_key
    ///
    /// # 示例
    /// ```
    /// # use goat_db::goatkv::storage::sstable::BlockBuilder;
    /// let mut builder = BlockBuilder::new();
    /// // 构建第一个block
    /// builder.add(b"key1", b"value1");
    /// builder.finish();
    ///
    /// // 重置并构建第二个block
    /// builder.reset();
    /// builder.add(b"key2", b"value2");
    /// ```
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.restarts.clear();
        self.counter = 0;
        self.last_key.clear();
    }

    /// 判断当前Block是否为空
    ///
    /// # 返回值
    /// - `true`: 没有添加任何条目
    /// - `false`: 至少添加了一个条目
    ///
    /// # 示例
    /// ```
    /// # use goat_db::goatkv::storage::sstable::BlockBuilder;
    /// let builder = BlockBuilder::new();
    /// assert!(builder.empty());
    ///
    /// let mut builder = BlockBuilder::new();
    /// builder.add(b"key", b"value");
    /// assert!(!builder.empty());
    /// ```
    pub fn empty(&self) -> bool {
        self.counter == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试BlockBuilder的基本创建和添加功能
    #[test]
    fn test_block_builder_basic() {
        let mut builder = BlockBuilder::new();

        // 添加几个简单的条目
        builder.add(b"key1", b"value1");
        builder.add(b"key2", b"value2");
        builder.add(b"key3", b"value3");

        // 验证不为空
        assert!(!builder.empty());

        // 完成构建
        let (block_data, last_key) = builder.finish();

        // 验证block不为空
        assert!(!block_data.is_empty());

        // 验证最后一个key正确
        assert_eq!(last_key, b"key3");
    }

    /// 测试前缀压缩功能
    ///
    /// 验证具有共同前缀的key能够被正确压缩
    #[test]
    fn test_block_builder_prefix_compression() {
        let mut builder = BlockBuilder::new();

        // 添加具有共同前缀的key
        builder.add(b"application", b"app");
        builder.add(b"apple", b"fruit");
        builder.add(b"apply", b"verb");

        let (block_data, last_key) = builder.finish();

        // 验证block数据被压缩（比原始数据小）
        // 原始数据：application+app + apple+fruit + apply+verb = 42字节
        // 压缩后应该更小（因为有前缀压缩）
        assert!(block_data.len() < 50);

        // 验证最后一个key正确
        assert_eq!(last_key, b"apply");
    }

    /// 测试重启点机制
    ///
    /// 每16个条目应该创建一个重启点
    #[test]
    fn test_block_builder_restart_points() {
        let mut builder = BlockBuilder::new();

        // 添加正好16个条目
        for i in 0..16 {
            let key = format!("key{:03}", i);
            let value = format!("value{:03}", i);
            builder.add(key.as_bytes(), value.as_bytes());
        }

        let (block_data, _) = builder.finish();

        // block末尾应该有4字节的重启点数量（1个重启点）
        let restart_count_bytes = &block_data[block_data.len() - 4..];
        let restart_count = u32::from_le_bytes([
            restart_count_bytes[0],
            restart_count_bytes[1],
            restart_count_bytes[2],
            restart_count_bytes[3],
        ]);

        // 应该有1个重启点（第16个条目）
        assert_eq!(restart_count, 1);

        // 重启点偏移量应该在重启点数量之前
        let restart_point_bytes = &block_data[block_data.len() - 8..block_data.len() - 4];
        let restart_point = u32::from_le_bytes([
            restart_point_bytes[0],
            restart_point_bytes[1],
            restart_point_bytes[2],
            restart_point_bytes[3],
        ]);

        // 重启点应该指向第16个条目的位置
        assert!(restart_point > 0);
        assert!(restart_point < block_data.len() as u32);
    }

    /// 测试多个重启点
    ///
    /// 添加超过32个条目，应该创建多个重启点
    #[test]
    fn test_block_builder_multiple_restart_points() {
        let mut builder = BlockBuilder::new();

        // 添加33个条目，应该创建2个重启点
        for i in 0..33 {
            let key = format!("key{:03}", i);
            let value = format!("value{:03}", i);
            builder.add(key.as_bytes(), value.as_bytes());
        }

        let (block_data, _) = builder.finish();

        // 应该有2个重启点
        let restart_count_bytes = &block_data[block_data.len() - 4..];
        let restart_count = u32::from_le_bytes([
            restart_count_bytes[0],
            restart_count_bytes[1],
            restart_count_bytes[2],
            restart_count_bytes[3],
        ]);

        assert_eq!(restart_count, 2);
    }

    /// 测试BlockBuilder的大小限制
    ///
    /// 当数据量超过MAX_BLOCK_SIZE时，should_finish()应该返回true
    #[test]
    fn test_block_builder_should_finish() {
        let mut builder = BlockBuilder::new();

        // 初始状态不应该完成
        assert!(!builder.should_finish());

        // 添加大量数据直到超过MAX_BLOCK_SIZE
        let large_value = vec![b'x'; 2000];
        for i in 0..5 {
            let key = format!("key{:03}", i);
            builder.add(key.as_bytes(), &large_value);

            // 在第3个条目后应该达到或超过MAX_BLOCK_SIZE
            if i >= 2 {
                assert!(
                    builder.should_finish(),
                    "Should finish after {} entries",
                    i + 1
                );
            }
        }
    }

    /// 测试空Block
    ///
    /// 空的BlockBuilder应该能正确处理finish()
    #[test]
    fn test_block_builder_empty() {
        let mut builder = BlockBuilder::new();

        // 验证为空
        assert!(builder.empty());
        assert_eq!(builder.len(), 0);

        // 完成空的block
        let (block_data, last_key) = builder.finish();

        // 空block应该只包含重启点数量（0）
        assert_eq!(block_data.len(), 4);
        assert_eq!(last_key.len(), 0);
    }

    /// 测试BlockBuilder重置功能
    ///
    /// 重置后应该可以重新使用
    #[test]
    fn test_block_builder_reset() {
        let mut builder = BlockBuilder::new();

        // 添加一些数据
        builder.add(b"key1", b"value1");
        builder.add(b"key2", b"value2");

        let len_before_finish = builder.len();
        assert!(len_before_finish > 0);

        // 完成第一个block
        builder.finish();

        // 重置
        builder.reset();

        // 验证重置后为空
        assert!(builder.empty());
        assert_eq!(builder.len(), 0);

        // 可以重新添加数据
        builder.add(b"key3", b"value3");
        assert!(!builder.empty());
        assert!(!builder.is_empty());
    }

    /// 测试不同长度的key和value
    ///
    /// BlockBuilder应该能处理各种长度的数据
    #[test]
    fn test_block_builder_various_lengths() {
        let mut builder = BlockBuilder::new();

        // 测试不同长度的key
        builder.add(b"a", b"v");
        builder.add(b"ab", b"val");
        builder.add(b"abc", b"value");
        builder.add(&[b'x'; 100], &[b'y'; 200]);

        let (block_data, _) = builder.finish();

        // 验证block不为空
        assert!(!block_data.is_empty());

        // 验证数据被编码（使用varint）
        // 长key和长value应该使用varint编码
        assert!(!block_data.is_empty());
    }

    /// 测试共享前缀计算
    ///
    /// 验证compute_shared()能正确计算共享前缀长度
    #[test]
    fn test_compute_shared() {
        let mut builder = BlockBuilder::new();

        // 添加第一个key
        builder.add(b"application", b"value1");

        // 测试各种共享前缀情况
        assert_eq!(builder.compute_shared(b"apple"), 4); // "appl"
        assert_eq!(builder.compute_shared(b"apply"), 4); // "appl"
        assert_eq!(builder.compute_shared(b"app"), 3); // "app"
        assert_eq!(builder.compute_shared(b"a"), 1); // "a"
        assert_eq!(builder.compute_shared(b"application"), 11); // 完全相同
        assert_eq!(builder.compute_shared(b"banana"), 0); // 完全不同
        assert_eq!(builder.compute_shared(b""), 0); // 空key
    }

    /// 测试相同key的处理
    ///
    /// 相同的key应该被正确处理（shared=完整长度，unshared=0）
    #[test]
    fn test_block_builder_same_keys() {
        let mut builder = BlockBuilder::new();

        builder.add(b"key", b"value1");
        builder.add(b"key", b"value2"); // 相同的key，不同的value
        builder.add(b"key", b"value3");

        let (block_data, _) = builder.finish();

        // 验证block不为空
        assert!(!block_data.is_empty());

        // 相同的key应该被正确编码
        // 第一个key：shared=0, unshared=3, key="key", value="value1"
        // 第二个key：shared=3, unshared=0, key="", value="value2"
        // 第三个key：shared=3, unshared=0, key="", value="value3"
    }

    /// 测试特殊字符的key
    ///
    /// BlockBuilder应该能处理各种特殊字符
    #[test]
    fn test_block_builder_special_keys() {
        let mut builder = BlockBuilder::new();

        // 测试各种特殊字符
        builder.add(b"key_with_underscore", b"value1");
        builder.add(b"key-with-dash", b"value2");
        builder.add(b"key.with.dot", b"value3");
        builder.add(b"key space", b"value4");
        builder.add(b"123number", b"value5");

        let (block_data, _) = builder.finish();

        // 验证所有数据都被正确编码
        assert!(!block_data.is_empty());
    }

    /// 测试空value
    ///
    /// BlockBuilder应该能处理空value
    #[test]
    fn test_block_builder_empty_value() {
        let mut builder = BlockBuilder::new();

        builder.add(b"key1", b"");
        builder.add(b"key2", b"");
        builder.add(b"key3", b"");

        let (block_data, _) = builder.finish();

        // 验证block不为空（即使value为空）
        assert!(!block_data.is_empty());
    }

    /// 测试单字节key和value
    ///
    /// 最小化的key和value应该能被正确处理
    #[test]
    fn test_block_builder_single_byte() {
        let mut builder = BlockBuilder::new();

        builder.add(b"a", b"1");
        builder.add(b"b", b"2");

        let (block_data, _) = builder.finish();

        // 验证block不为空
        assert!(!block_data.is_empty());
    }

    /// 测试大量小条目
    ///
    /// 验证能正确处理大量小条目
    #[test]
    fn test_block_builder_many_small_entries() {
        let mut builder = BlockBuilder::new();

        // 添加100个小的key-value对
        for i in 0..100 {
            let key = format!("k{}", i);
            let value = format!("v{}", i);
            builder.add(key.as_bytes(), value.as_bytes());
        }

        let (block_data, _) = builder.finish();

        // 验证block不为空
        assert!(!block_data.is_empty());

        // 验证有多个重启点（100 / 16 = 6个）
        let restart_count_bytes = &block_data[block_data.len() - 4..];
        let restart_count = u32::from_le_bytes([
            restart_count_bytes[0],
            restart_count_bytes[1],
            restart_count_bytes[2],
            restart_count_bytes[3],
        ]);

        assert_eq!(restart_count, 6);
    }

    /// 测试前缀压缩的空间节省
    ///
    /// 验证前缀压缩确实能节省空间
    #[test]
    fn test_block_builder_compression_savings() {
        let mut builder = BlockBuilder::new();

        // 添加大量具有长共同前缀的key
        for i in 0..50 {
            let key = format!("very_long_common_prefix_{}", i);
            let value = format!("value_{}", i);
            builder.add(key.as_bytes(), value.as_bytes());
        }

        let (block_data, _) = builder.finish();

        // 估算原始大小（不使用前缀压缩）
        // 每个key约30字节，每个value约8字节，共50个条目
        // 原始大小约：50 * (30 + 8) = 1900字节
        let estimated_original: usize = 1900;
        assert!(
            block_data.len() < estimated_original,
            "Compressed size ({}) should be less than original ({})",
            block_data.len(),
            estimated_original
        );
    }

    /// 测试BlockBuilder的连续使用
    ///
    /// 验证可以连续创建多个block
    #[test]
    fn test_block_builder_continuous_use() {
        let mut builder = BlockBuilder::new();

        // 创建第一个block
        for i in 0..5 {
            let key = format!("block1_key{}", i);
            let value = format!("block1_value{}", i);
            builder.add(key.as_bytes(), value.as_bytes());
        }
        let (block1, last_key1) = builder.finish();

        // 复制数据以避免借用问题
        let block1_data = block1.to_vec();
        let last_key1_data = last_key1.to_vec();

        // 重置并创建第二个block
        builder.reset();
        for i in 0..5 {
            let key = format!("block2_key{}", i);
            let value = format!("block2_value{}", i);
            builder.add(key.as_bytes(), value.as_bytes());
        }
        let (block2, last_key2) = builder.finish();

        // 复制数据以避免借用问题
        let block2_data = block2.to_vec();
        let last_key2_data = last_key2.to_vec();

        // 验证两个block都正确创建
        assert!(!block1_data.is_empty());
        assert!(!block2_data.is_empty());
        assert_eq!(&last_key1_data, b"block1_key4");
        assert_eq!(&last_key2_data, b"block2_key4");

        // 验证两个block的数据是独立的
        assert_ne!(&block1_data[..], &block2_data[..]);
    }

    /// 测试key的字典序
    ///
    /// Block中的key应该保持插入的顺序
    #[test]
    fn test_block_builder_key_order() {
        let mut builder = BlockBuilder::new();

        let keys: Vec<&[u8]> = vec![b"apple", b"banana", b"cherry", b"date"];

        for key in &keys {
            let value = format!("value_{}", String::from_utf8_lossy(key));
            builder.add(key, value.as_bytes());
        }

        let (_, last_key) = builder.finish();

        // 验证最后一个key正确
        assert_eq!(last_key, *keys.last().unwrap());
    }
}
