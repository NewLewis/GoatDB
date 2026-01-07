use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;

use crate::goatkv::encoding::varint;
use crate::goatkv::storage::block_builder::BlockBuilder;
use crate::goatkv::storage::bloom_builder::BloomBuilder;

const MAGIC_NUMBER: u64 = 0x706A725F676F6174;

pub struct SSTableBuilder {
    writer: io::BufWriter<File>,
    data_block_builder: BlockBuilder,
    index_block_builder: BlockBuilder,
    bloom_builder: BloomBuilder,
    offset: u64,
}

impl SSTableBuilder {
    pub fn new(id: u64, path: PathBuf) -> io::Result<Self> {
        let filename = Self::get_file_name(id, path);

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&filename)?;

        Ok(Self {
            writer: io::BufWriter::new(file),
            data_block_builder: BlockBuilder::new(),
            index_block_builder: BlockBuilder::new(),
            bloom_builder: BloomBuilder::new(),
            offset: 0,
        })
    }

    fn get_file_name(id: u64, path: PathBuf) -> String {
        if id < 1000000 {
            format!("{}/{:06}.sst", path.display(), id)
        } else {
            format!("{}/{}.sst", path.display(), id)
        }
    }

    pub fn write(&mut self, key: &[u8], value: &[u8]) {
        if self.data_block_builder.should_finish() {
            self.finish_data_block(key);
        }

        // 加入data_block
        self.data_block_builder.add(key, value);
        // 加入布隆过滤器
        self.bloom_builder.add(key);
    }

    fn finish_data_block(&mut self, key: &[u8]) {
        // 先finsh block_builder`
        // 这是会写入restart array和resatrt len
        // 然后拿到data_block的内容
        let (block_content, last_key) = self.data_block_builder.finish();

        // 获取separator
        let separator = Self::compute_separator(&last_key, key);

        // 将separator加入索引块
        let mut separator_val = Vec::new();
        separator_val.extend_from_slice(&varint::encode(self.offset));
        separator_val.extend_from_slice(&varint::encode(block_content.len() as u64));
        self.index_block_builder.add(&separator, &separator_val);

        // 写入data block
        self.writer.write_all(&block_content).unwrap();
        self.offset += block_content.len() as u64;

        // 重置data_block_builder
        self.data_block_builder.reset();
    }

    pub fn finish(&mut self) {
        if !self.data_block_builder.empty() {
            let (block_content, last_key) = self.data_block_builder.finish();

            // 写index_block_builder
            let mut separator_val = Vec::new();
            separator_val.extend_from_slice(&varint::encode(self.offset));
            separator_val.extend_from_slice(&varint::encode(block_content.len() as u64));
            self.index_block_builder.add(&last_key, &separator_val);

            // 写入data block
            self.writer.write_all(&block_content).unwrap();
            self.offset += block_content.len() as u64;

            // 重置data_block_builder
            self.data_block_builder.reset();
        }

        // 写入bloom block
        self.writer.write_all(self.bloom_builder.bitmap()).unwrap();
        let bloom_offset = self.offset;
        self.offset += self.bloom_builder.bitmap().len() as u64;

        // 写入index block
        let (block_content, _) = self.index_block_builder.finish();
        self.writer.write_all(&block_content).unwrap();
        let index_offset = self.offset;
        self.offset += block_content.len() as u64;

        // 写入footer
        let bloom_offset_bytes = varint::encode(bloom_offset);
        let bloom_offset_len = bloom_offset_bytes.len();

        let index_offset_bytes = varint::encode(index_offset);
        let index_offset_len = index_offset_bytes.len();

        self.writer.write_all(&bloom_offset_bytes).unwrap();
        self.writer.write_all(&index_offset_bytes).unwrap();

        let padding = vec![0; 40 - (bloom_offset_len + index_offset_len)];
        self.writer.write_all(&padding).unwrap();

        //写入magic number
        self.writer.write_all(&MAGIC_NUMBER.to_le_bytes()).unwrap();

        // 确保所有数据都被写入文件
        self.writer.flush().unwrap();
    }

    fn compute_separator(last_key: &[u8], key: &[u8]) -> Vec<u8> {
        let mut result = Vec::new();
        let mut i = 0;

        if last_key.len() == key.len() && last_key[last_key.len() - 1] + 1 == key[key.len() - 1] {
            return last_key.to_vec();
        }

        while i < last_key.len() && i < key.len() {
            if last_key[i] != key[i] {
                if last_key[i] < 0xff {
                    result.push(last_key[i] + 1);
                    return result.to_vec();
                }
                return last_key.to_vec();
            }
            result.push(last_key[i]);
            i += 1;
        }

        last_key.to_vec()
    }
}
