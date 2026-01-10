#[allow(dead_code)]
#[allow(clippy::module_inception)]
pub mod fixtures {
    /// 生成随机测试数据
    pub fn random_key_value() -> (Vec<u8>, Vec<u8>) {
        use rand::Rng;

        let mut rng = rand::thread_rng();
        let key_len = rng.gen_range(1..100);
        let value_len = rng.gen_range(1..1000);

        let key: Vec<u8> = (0..key_len).map(|_| rng.gen()).collect();
        let value: Vec<u8> = (0..value_len).map(|_| rng.gen()).collect();

        (key, value)
    }

    /// 生成指定数量的随机键值对
    pub fn random_key_values(count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..count).map(|_| random_key_value()).collect()
    }

    /// 生成简单的测试键值对
    pub fn simple_key_value(key: &str, value: &str) -> (Vec<u8>, Vec<u8>) {
        (key.as_bytes().to_vec(), value.as_bytes().to_vec())
    }
}
