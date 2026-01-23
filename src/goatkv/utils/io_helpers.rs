use std::io::{self, Read};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOutcome {
    Eof,
    Partial,
    Complete,
}

pub fn read_exact_or_eof<R: Read>(reader: &mut R, buf: &mut [u8]) -> io::Result<ReadOutcome> {
    let mut read = 0;
    while read < buf.len() {
        match reader.read(&mut buf[read..])? {
            0 => break,
            n => read += n,
        }
    }
    if read == 0 {
        Ok(ReadOutcome::Eof)
    } else if read < buf.len() {
        Ok(ReadOutcome::Partial)
    } else {
        Ok(ReadOutcome::Complete)
    }
}
