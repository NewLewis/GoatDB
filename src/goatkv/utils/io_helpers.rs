use std::io::Read;

use crate::goatkv::error::{Error as GoatError, Result as GoatResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOutcome {
    Eof,
    Partial,
    Complete,
}

pub fn read_exact_or_eof<R: Read>(reader: &mut R, buf: &mut [u8]) -> GoatResult<ReadOutcome> {
    if buf.is_empty() {
        return Ok(ReadOutcome::Complete);
    }
    let mut read = 0;
    while read < buf.len() {
        match reader
            .read(&mut buf[read..])
            .map_err(|e| GoatError::io("read_exact_or_eof", e))?
        {
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
