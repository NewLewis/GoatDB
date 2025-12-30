pub fn encode(value: u64) -> Vec<u8> {
    let mut result = Vec::new();
    let mut value = value;

    while value >= 0x80 {
        result.push((value & 0x7F) as u8 | 0x80);
        value >>= 7;
    }
    result.push(value as u8);
    result
}

pub fn decode(bytes: &[u8]) -> Result<u64, &'static str> {
    let mut result = 0;
    let mut shift = 0;

    for &byte in bytes {
        if shift >= 64 {
            return Err("Overflow");
        }
        result |= ((byte & 0x7F) as u64) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
    }
    Err("Incomplete")
}
