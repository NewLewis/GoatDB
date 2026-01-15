//! 变长整数编码，用于紧凑序列化。
//!
//! 本模块实现了 varint 编码方案，该方案使用可变字节数（1-10）编码
//! 无符号 64 位整数。较小的值占用较少的字节。
//!
//! # 编码格式
//!
//! 每个字节使用 7 位存储数据，1 位作为延续标志：
//! - 位 7 (MSB): 1 = 后续还有字节, 0 = 这是最后一个字节
//! - 位 0-6: 整数值的 7 个位
//!
//! 编码过程：
//! 1. 取数值的最低 7 位，如果还有更多字节则将 MSB 设为 1
//! 2. 将数值右移 7 位
//! 3. 重复直到数值 < 0x80
//! 4. 写入最终字节，MSB=0
//!
//! # 示例
//!
//! 值 300 (0x12C) 编码为 [0xAC, 0x02]：
//! - 第一个字节：0xAC = 0x2C (低 7 位) | 0x80 (延续)
//! - 第二个字节：0x02 = 0x02 (剩余) | 0x00 (结束)
//!
//! 值 1 编码为 [0x01]
//! 值 127 (0x7F) 编码为 [0x7F]
//! 值 128 (0x80) 编码为 [0x80, 0x01]

/// 将 64 位无符号整数编码为 varint 字节。
///
/// # 参数
/// * `value` - 要编码的整数（0 到 2^64-1）
///
/// # 返回值
/// 包含 varint 编码字节的 `Vec<u8>`（1 到 10 字节）。
///
/// # 示例
/// ```
/// use goat_db::goatkv::encoding::coding;
///
/// assert_eq!(coding::encode_varint64(0), vec![0x00]);
/// assert_eq!(coding::encode_varint64(1), vec![0x01]);
/// assert_eq!(coding::encode_varint64(127), vec![0x7F]);
/// assert_eq!(coding::encode_varint64(128), vec![0x80, 0x01]);
/// assert_eq!(coding::encode_varint64(300), vec![0xAC, 0x02]);
/// ```
pub fn encode_varint64(value: u64) -> Vec<u8> {
    let mut result = Vec::new();
    let mut value = value;

    // 将数值按 7 位块处理，直到所有位都被消耗
    while value >= 0x80 {
        // 取最低 7 位，设置 MSB=1 表示延续
        result.push(((value & 0x7F) as u8) | 0x80);
        value >>= 7; // 移除已处理的 7 位
    }
    // 写入最终字节，MSB=0
    result.push(value as u8);
    result
}

pub fn put_varint64(buf: &mut Vec<u8>, mut value: u64) {
    // 将数值按 7 位块处理，直到所有位都被消耗
    while value >= 0x80 {
        // 取最低 7 位，设置 MSB=1 表示延续
        buf.push(((value & 0x7F) as u8) | 0x80);
        value >>= 7; // 移除已处理的 7 位
    }
    // 写入最终字节，MSB=0
    buf.push(value as u8);
}

/// 将 varint 字节解码为 64 位无符号整数。
///
/// # 参数
/// * `bytes` - varint 编码的字节切片
///
/// # 返回值
/// * `Ok(u64)` - 成功解码的整数
/// * `Err(&'static str)` - 解码错误：
///   - `"Overflow"`: 编码值超过 64 位
///   - `"Incomplete"`: 缺少终止字节（所有字节的 MSB 都为 1）
///
/// # 示例
/// ```
/// use goat_db::goatkv::encoding::coding;
///
/// assert_eq!(coding::decode_varint64(&[0x00]), Ok(0));
/// assert_eq!(coding::decode_varint64(&[0x01]), Ok(1));
/// assert_eq!(coding::decode_varint64(&[0x7F]), Ok(127));
/// assert_eq!(coding::decode_varint64(&[0x80, 0x01]), Ok(128));
/// assert_eq!(coding::decode_varint64(&[0xAC, 0x02]), Ok(300));
/// ```
pub fn decode_varint64(bytes: &[u8]) -> Result<u64, &'static str> {
    decode_varint64_with_length(bytes).map(|(value, _)| value)
}

/// 将 varint 字节解码为 64 位无符号整数，并返回读取的字节数。
///
/// # 参数
/// * `bytes` - varint 编码的字节切片
///
/// # 返回值
/// * `Ok((u64, usize))` - 成功解码的整数和读取的字节数
/// * `Err(&'static str)` - 解码错误：
///   - `"Overflow"`: 编码值超过 64 位
///   - `"Incomplete"`: 缺少终止字节（所有字节的 MSB 都为 1）
///
/// # 示例
/// ```
/// use goat_db::goatkv::encoding::coding;
///
/// assert_eq!(coding::decode_varint64_with_length(&[0x00]), Ok((0, 1)));
/// assert_eq!(coding::decode_varint64_with_length(&[0x01]), Ok((1, 1)));
/// assert_eq!(coding::decode_varint64_with_length(&[0x7F]), Ok((127, 1)));
/// assert_eq!(coding::decode_varint64_with_length(&[0x80, 0x01]), Ok((128, 2)));
/// assert_eq!(coding::decode_varint64_with_length(&[0xAC, 0x02]), Ok((300, 2)));
/// ```
pub fn decode_varint64_with_length(bytes: &[u8]) -> Result<(u64, usize), &'static str> {
    let mut result = 0u64;
    let mut shift = 0u32;
    let mut bytes_read = 0;

    for &byte in bytes {
        // 检查溢出：varint 最多为 u64 编码 10 字节
        // (10 字节 * 7 位 = 70 位，但我们只使用 64 位)
        if shift >= 64 {
            return Err("Overflow");
        }

        // 将 7 位数据添加到结果的当前移位位置
        result |= ((byte & 0x7F) as u64) << shift;
        shift += 7;
        bytes_read += 1;

        // 如果 MSB=0，这是最后一个字节
        if byte & 0x80 == 0 {
            return Ok((result, bytes_read));
        }
    }

    // 如果执行到这里，说明消耗了所有字节但从未看到终止
    Err("Incomplete")
}

/// 写入带长度前缀的字节切片
pub fn put_length_prefixed_slice(buf: &mut Vec<u8>, data: &[u8]) {
    put_varint64(buf, data.len() as u64);
    buf.extend_from_slice(data);
}

/// 读取带长度前缀的字节切片，返回 (data, bytes_read)
pub fn get_length_prefixed_slice(bytes: &[u8]) -> Result<(&[u8], usize), &'static str> {
    let (length, bytes_read) = decode_varint64_with_length(bytes)?;
    let length = length as usize;

    if bytes_read + length > bytes.len() {
        return Err("Incomplete");
    }

    Ok((&bytes[bytes_read..bytes_read + length], bytes_read + length))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试基本单字节编码（0-127）
    #[test]
    fn test_encode_single_byte() {
        // 测试单字节编码的边界值
        assert_eq!(encode_varint64(0), vec![0x00], "0 应编码为 [0x00]");
        assert_eq!(encode_varint64(1), vec![0x01], "1 应编码为 [0x01]");
        assert_eq!(encode_varint64(127), vec![0x7F], "127 应编码为 [0x7F]");

        // 测试一些随机单字节值
        assert_eq!(encode_varint64(42), vec![0x2A], "42 应编码为 [0x2A]");
        assert_eq!(encode_varint64(100), vec![0x64], "100 应编码为 [0x64]");
    }

    /// 测试多字节编码（128 及以上）
    #[test]
    fn test_encode_multi_byte() {
        // 测试边界：第一个需要 2 字节的值
        assert_eq!(
            encode_varint64(128),
            vec![0x80, 0x01],
            "128 应编码为 [0x80, 0x01]"
        );
        assert_eq!(
            encode_varint64(255),
            vec![0xFF, 0x01],
            "255 应编码为 [0xFF, 0x01]"
        );
        assert_eq!(
            encode_varint64(300),
            vec![0xAC, 0x02],
            "300 应编码为 [0xAC, 0x02]"
        );

        // 测试 3 字节编码
        assert_eq!(
            encode_varint64(16384),
            vec![0x80, 0x80, 0x01],
            "16384 应编码为 [0x80, 0x80, 0x01]"
        );

        // 测试最大 2 字节值（2^14 - 1 = 16383）
        assert_eq!(
            encode_varint64(16383),
            vec![0xFF, 0x7F],
            "16383 应编码为 [0xFF, 0x7F]"
        );
    }

    /// 测试需要最大字节数的大数值
    #[test]
    fn test_encode_large_values() {
        // 测试最大 u64 值
        assert_eq!(
            encode_varint64(u64::MAX),
            vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01],
            "u64::MAX 应编码为 10 字节"
        );

        // 测试 2^63 - 1（最大有符号 64 位）
        assert_eq!(
            encode_varint64(0x7FFFFFFFFFFFFFFF),
            vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F],
            "2^63-1 应编码为 9 字节"
        );

        // 测试恰好需要 10 字节的值（2^63）
        assert_eq!(
            encode_varint64(0x8000000000000000),
            vec![0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01],
            "2^63 应编码为 10 字节"
        );
    }

    /// 测试解码单字节值
    #[test]
    fn test_decode_single_byte() {
        assert_eq!(decode_varint64(&[0x00]), Ok(0), "[0x00] 应解码为 0");
        assert_eq!(decode_varint64(&[0x01]), Ok(1), "[0x01] 应解码为 1");
        assert_eq!(decode_varint64(&[0x7F]), Ok(127), "[0x7F] 应解码为 127");
        assert_eq!(decode_varint64(&[0x2A]), Ok(42), "[0x2A] 应解码为 42");
        assert_eq!(decode_varint64(&[0x64]), Ok(100), "[0x64] 应解码为 100");
    }

    /// 测试解码多字节值
    #[test]
    fn test_decode_multi_byte() {
        assert_eq!(decode_varint64(&[0x80, 0x01]), Ok(128), "2 字节的 128");
        assert_eq!(decode_varint64(&[0xFF, 0x01]), Ok(255), "2 字节的 255");
        assert_eq!(decode_varint64(&[0xAC, 0x02]), Ok(300), "2 字节的 300");
        assert_eq!(
            decode_varint64(&[0x80, 0x80, 0x01]),
            Ok(16384),
            "3 字节的 16384"
        );
        assert_eq!(
            decode_varint64(&[0xFF, 0x7F]),
            Ok(16383),
            "2 字节最大值 (16383)"
        );
    }

    /// 测试解码大数值
    #[test]
    fn test_decode_large_values() {
        // 测试最大 u64 值
        assert_eq!(
            decode_varint64(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]),
            Ok(u64::MAX),
            "解码 u64::MAX"
        );

        // 测试 2^63 - 1
        assert_eq!(
            decode_varint64(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]),
            Ok(0x7FFFFFFFFFFFFFFF),
            "解码 2^63-1"
        );
    }

    /// 测试往返：编码后解码应返回原始值
    #[test]
    fn test_encode_decode_roundtrip() {
        // 测试一系列值
        let test_values = vec![
            0,
            1,
            10,
            42,
            100,
            127,
            128,
            255,
            300,
            1000,
            10000,
            65535,
            100000,
            1000000,
            10000000,
            100000000,
            1000000000,
            10000000000,
            0x7FFFFFFF,
            0xFFFFFFFF,
            0x7FFFFFFFFFFFFFFF,
            u64::MAX,
        ];

        for value in test_values {
            let encoded = encode_varint64(value);
            let decoded =
                decode_varint64(&encoded).unwrap_or_else(|_| panic!("解码编码值 {} 失败", value));
            assert_eq!(decoded, value, "值 {} 的往返测试失败", value);
        }
    }

    /// 测试解码错误：不完整的输入（缺少终止字节）
    #[test]
    fn test_decode_incomplete() {
        // 设置了延续位的单字节
        assert_eq!(decode_varint64(&[0x80]), Err("Incomplete"), "单字节延续");

        // 所有字节都有延续位
        assert_eq!(
            decode_varint64(&[0x80, 0x80, 0x80, 0x80, 0x80]),
            Err("Incomplete"),
            "所有字节都有延续位"
        );

        // 空切片
        assert_eq!(decode_varint64(&[]), Err("Incomplete"), "空切片");
    }

    /// 测试解码错误：溢出（超过 u64 的 10 字节限制）
    #[test]
    fn test_decode_overflow() {
        // 10 个有效字节，但第 11 个会溢出 u64
        // 这是一个有效的 10 字节 varint，解码为 u64::MAX
        let max_valid = vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01];
        assert_eq!(
            decode_varint64(&max_valid),
            Ok(u64::MAX),
            "有效的 10 字节 varint"
        );

        // 创建一个无效的 11 字节 varint（需要 >64 位）
        // 11 字节 * 7 位 = 77 位潜力，但我们限制为 64 位
        let mut overflow_bytes = vec![0x80; 10]; // 10 个延续字节
        overflow_bytes.push(0x00); // 最终字节

        // 解码器应在 shift >= 64 时检测溢出
        // 实际上，在我们的实现中，10 字节后（shift=70），
        // 我们会在第 10 个字节检测到溢出（第 10 个字节开始时 shift=63）
        // 让我们用不同的方法测试：

        // 创建会编码 >64 位值的字节
        // 10 个 0x80（延续）然后 0x00
        // 这实际上是 0 的有效编码，但让我们测试真正的溢出：
        // 使用 10 个都有延续位的字节，然后是第 11 个
        let mut long_varint = vec![0x80; 11]; // 所有字节都有延续
        long_varint[10] = 0x00; // 在第 11 字节终止

        // 应该溢出，因为我们尝试处理 11 字节 * 7 位 = 77 位
        assert_eq!(
            decode_varint64(&long_varint),
            Err("Overflow"),
            "11 字节 varint 应溢出"
        );
    }

    /// 测试编码产生最小表示
    #[test]
    fn test_encode_minimal() {
        // 127 应为 1 字节，不是 2
        assert_eq!(encode_varint64(127).len(), 1, "127 应编码为 1 字节");

        // 128 应为 2 字节，不是更多
        assert_eq!(encode_varint64(128).len(), 2, "128 应编码为 2 字节");

        // 检查更大的值：2^14-1 = 16383 应为 2 字节
        assert_eq!(encode_varint64(16383).len(), 2, "16383 应编码为 2 字节");

        // 2^14 = 16384 应为 3 字节
        assert_eq!(encode_varint64(16384).len(), 3, "16384 应编码为 3 字节");
    }

    /// 测试解码终止后的额外字节
    #[test]
    fn test_decode_extra_bytes() {
        // 有效的 varint 后面跟着垃圾数据
        assert_eq!(
            decode_varint64(&[0x01, 0x00, 0x00, 0x00]),
            Ok(1),
            "应忽略终止后的字节"
        );

        // 带额外字节的多字节 varint
        assert_eq!(
            decode_varint64(&[0x80, 0x01, 0xFF, 0xFF]),
            Ok(128),
            "应在终止字节处停止"
        );
    }

    /// 基于属性的样式测试：所有 u64 值往返正确
    #[test]
    fn test_property_based_roundtrip() {
        // 测试 2 的幂次及其附近值
        for i in 0..=63 {
            let power = 1u64 << i;
            // 测试幂本身
            test_roundtrip_value(power);
            // 测试 power - 1（如果不是 0）
            if power > 1 {
                test_roundtrip_value(power - 1);
            }
            // 测试 power + 1（如果不溢出）
            if power < u64::MAX {
                test_roundtrip_value(power + 1);
            }
        }

        // 测试整个范围内的一些随机值
        let random_values = [
            123456789,
            987654321,
            0x123456789ABCDEF,
            0xFEDCBA9876543210,
            0x5555555555555555,
            0xAAAAAAAAAAAAAAAA,
        ];

        for &value in &random_values {
            test_roundtrip_value(value);
        }
    }

    /// 基于属性测试的辅助函数
    fn test_roundtrip_value(value: u64) {
        let encoded = encode_varint64(value);
        let decoded = decode_varint64(&encoded).unwrap_or_else(|e| {
            panic!("解码值 {} 失败: {}", value, e);
        });
        assert_eq!(decoded, value, "值 {} 的往返测试失败", value);

        // 同时验证最小编码
        if value < 0x80 {
            assert_eq!(encoded.len(), 1, "值 {} 应为 1 字节", value);
        } else if value < 0x4000 {
            assert_eq!(encoded.len(), 2, "值 {} 应为 2 字节", value);
        } else if value < 0x200000 {
            assert_eq!(encoded.len(), 3, "值 {} 应为 3 字节", value);
        }
        // 可以为更大的字节数添加更多检查
    }
}
