use std::hash::Hasher as _;
use twox_hash::XxHash64;

pub struct BloomBuilder {
    bitmap: Vec<u8>,
}

impl BloomBuilder {
    pub fn new() -> Self {
        // 初始化 bitmap 为 1024 字节 (8192 位)
        // 避免在 add 方法中出现除零错误
        Self {
            bitmap: vec![0u8; 1024],
        }
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

        let k = 7; // 假设我们要 7 次
        let m = self.bitmap.len() * 8; // 总 bit 数

        for _ in 0..k {
            let bit_pos = (h as usize) % m;
            self.bitmap[bit_pos / 8] |= 1 << (bit_pos % 8);

            // 模拟下一个哈希值：加上增量即可
            h = h.wrapping_add(delta);
        }
    }

    pub fn bitmap(&self) -> &[u8] {
        &self.bitmap
    }
}
