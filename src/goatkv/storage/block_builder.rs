use crate::goatkv::encoding::varint;

const MAX_BLOCK_SIZE: usize = 4 * 1024; // 4KB

pub struct BlockBuilder {
    buffer: Vec<u8>,
    restarts: Vec<u8>,
    counter: u32,
    last_key: Vec<u8>,
}

impl BlockBuilder {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            restarts: Vec::new(),
            counter: 0,
            last_key: Vec::new(),
        }
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        let unshared: u32;
        let shared: u32;

        if self.counter == 0 {
            unshared = key.len() as u32;
            shared = 0;
        } else {
            shared = self.compute_shared(key);
            unshared = key.len() as u32 - shared;
        }

        self.buffer
            .extend_from_slice(&varint::encode(shared as u64));
        self.buffer
            .extend_from_slice(&varint::encode(unshared as u64));
        self.buffer
            .extend_from_slice(&varint::encode(value.len() as u64));
        self.buffer.extend_from_slice(&key[shared as usize..]);
        self.buffer.extend_from_slice(value);

        self.counter += 1;
        if self.counter == 16 {
            self.counter = 0;
            self.restarts
                .extend_from_slice(&(self.buffer.len() as u32).to_le_bytes());
        }

        self.last_key = key.to_vec();
    }

    pub fn finish(&mut self) -> (&[u8], &[u8]) {
        self.buffer.extend_from_slice(&self.restarts.as_slice());
        // restarts数组中每个重启点是4字节，所以需要除以4得到重启点数量
        let restart_count = (self.restarts.len() / 4) as u32;
        self.buffer.extend_from_slice(&restart_count.to_le_bytes());

        (&self.buffer, &self.last_key)
    }

    fn compute_shared(&mut self, key: &[u8]) -> u32 {
        let mut shared = 0;
        let mut i = 0;
        let mut j = 0;

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

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn should_finish(&self) -> bool {
        self.len() >= MAX_BLOCK_SIZE
    }

    pub fn reset(&mut self) {
        self.buffer.clear();
        self.restarts.clear();
        self.counter = 0;
        self.last_key.clear();
    }

    pub fn empty(&self) -> bool {
        self.counter == 0
    }
}
