use std::hash::Hasher as _;
use twox_hash::XxHash64;

/// BloomFilter 构建器，用于构建 SSTable 的 BloomFilter
pub struct BloomBuilder {
    bitmap: Vec<u8>,
    /// BloomFilter 的哈希函数数量
    k: usize,
}

impl BloomBuilder {
    /// 创建一个默认的 BloomBuilder，使用 1024 字节 (8192 位) 的位图
    pub fn new() -> Self {
        // 默认使用 1024 字节 (8192 位)
        // 这个大小对于大多数小型 SSTable 是足够的
        Self::with_capacity(1024)
    }

    /// 创建一个指定字节大小的 BloomBuilder
    pub fn with_capacity(bytes: usize) -> Self {
        Self {
            bitmap: vec![0u8; bytes],
            k: 7, // 默认使用 7 个哈希函数
        }
    }

    /// 根据预期的键数量和期望的误报率创建 BloomBuilder
    pub fn with_estimated_capacity(expected_items: usize, false_positive_rate: f64) -> Self {
        // 根据公式计算所需的 bit 数: m = - (n * ln(p)) / (ln(2)^2)
        let m = (-((expected_items as f64) * false_positive_rate.ln()) / (2.0_f64.ln().powi(2)))
            .ceil() as usize;
        let bytes = (m + 7) / 8; // 转换为字节数
        Self::with_capacity(bytes.max(1)) // 至少 1 字节
    }

    pub fn add(&mut self, key: &[u8]) {
        // 1. 固定 Seed 为 0，算一次哈希
        let mut hasher = XxHash64::with_seed(0);
        hasher.write(key);
        let hash_val = hasher.finish(); // 得到一个 64 位的哈希值

        // 2. 把 64 位拆成两个 32 位，模拟两个不同的哈希函数
        // 或者是直接利用这个 64 位值进行旋转
        let mut h = hash_val;

        // 骚操作：用位旋转来生成增量 (delta)，这在 LevelDB 里很常用
        // 这样就不用算第二遍哈希了
        let delta = (h >> 17) | (h << 15);

        let m = self.bitmap.len() * 8; // 总 bit 数

        for _ in 0..self.k {
            let bit_pos = (h as usize) % m;
            self.bitmap[bit_pos / 8] |= 1 << (bit_pos % 8);

            // 模拟下一个哈希值：加上增量即可
            h = h.wrapping_add(delta);
        }
    }

    pub fn bitmap(&self) -> &[u8] {
        &self.bitmap
    }

    /// 从构建器创建 BloomFilter
    pub fn build(self) -> BloomFilter {
        BloomFilter::new(self.bitmap)
    }
}

/// BloomFilter 查询器，用于快速判断 key 是否可能存在于 SSTable 中
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bitmap: Vec<u8>,
}

impl BloomFilter {
    /// 从字节数组创建 BloomFilter
    pub fn new(bitmap: Vec<u8>) -> Self {
        Self { bitmap }
    }

    /// 检查 key 是否可能存在于 BloomFilter 中
    /// 注意：BloomFilter 可能有误报，但不会有漏报
    /// 如果 BloomFilter 为空（bitmap 为空），则认为所有 key 都可能存在
    pub fn contains(&self, key: &[u8]) -> bool {
        if self.bitmap.is_empty() {
            // 空的 BloomFilter 表示所有 key 都可能存在
            return true;
        }

        // 1. 固定 Seed 为 0，算一次哈希（与 BloomBuilder 保持一致）
        let mut hasher = XxHash64::with_seed(0);
        hasher.write(key);
        let mut h = hasher.finish(); // 得到一个 64 位的哈希值

        // 2. 使用位旋转生成增量
        let delta = (h >> 17) | (h << 15);

        let k = 7; // 与 BloomBuilder 保持一致，使用 7 个哈希函数
        let m = self.bitmap.len() * 8; // 总 bit 数

        for _ in 0..k {
            let bit_pos = (h as usize) % m;
            let byte_pos = bit_pos / 8;
            let bit_mask = 1 << (bit_pos % 8);

            // 检查该位是否被设置
            if (self.bitmap[byte_pos] & bit_mask) == 0 {
                return false; // 任何一位为 0，key 肯定不存在
            }

            // 模拟下一个哈希值：加上增量即可
            h = h.wrapping_add(delta);
        }

        true // 所有位都为 1，key 可能存在
    }

    /// 获取 BloomFilter 的字节大小
    pub fn size(&self) -> usize {
        self.bitmap.len()
    }

    /// 获取底层的 bitmap 数据
    pub fn bitmap(&self) -> &[u8] {
        &self.bitmap
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_builder_build() {
        let mut builder = BloomBuilder::new();
        builder.add(b"key1");
        builder.add(b"key2");

        let filter = builder.build();
        assert_eq!(filter.size(), 1024);
    }

    #[test]
    fn test_bloom_builder_with_capacity() {
        let mut builder = BloomBuilder::with_capacity(512);
        builder.add(b"key1");
        builder.add(b"key2");

        let filter = builder.build();
        assert_eq!(filter.size(), 512);
    }

    #[test]
    fn test_bloom_builder_with_estimated_capacity() {
        // 100个键，期望误报率1%
        let mut builder = BloomBuilder::with_estimated_capacity(100, 0.01);
        builder.add(b"key1");
        builder.add(b"key2");

        let filter = builder.build();
        // 对于100个键，1%误报率，大约需要958位（约120字节）
        assert!(filter.size() > 50);
        assert!(filter.size() < 200);
    }

    #[test]
    fn test_bloom_filter_contains() {
        let mut builder = BloomBuilder::new();
        builder.add(b"key1");
        builder.add(b"key2");

        let filter = builder.build();

        // 添加的 key 应该存在
        assert!(filter.contains(b"key1"));
        assert!(filter.contains(b"key2"));

        // 未添加的 key 可能返回 true（误报）或 false
        // 我们只验证方法调用不会 panic
        let _ = filter.contains(b"key3");
    }

    #[test]
    fn test_bloom_filter_empty() {
        let filter = BloomFilter::new(vec![]);

        // 空的 BloomFilter 应该返回 true（所有 key 都可能存在）
        assert!(filter.contains(b"any_key"));
        assert_eq!(filter.size(), 0);
    }

    #[test]
    fn test_bloom_filter_from_bitmap() {
        // 创建一个全 1 的 bitmap
        let bitmap = vec![0xFFu8; 1024];
        let filter = BloomFilter::new(bitmap);

        // 全 1 的 bitmap 应该返回 true
        assert!(filter.contains(b"test_key"));
        assert_eq!(filter.size(), 1024);
    }

    #[test]
    fn test_bloom_builder_multiple_keys() {
        let mut builder = BloomBuilder::new();

        // 添加多个 key
        for i in 0..100 {
            let key = format!("key{}", i);
            builder.add(key.as_bytes());
        }

        let filter = builder.build();

        // 验证所有添加的 key 都返回 true
        for i in 0..100 {
            let key = format!("key{}", i);
            assert!(filter.contains(key.as_bytes()));
        }
    }
}
