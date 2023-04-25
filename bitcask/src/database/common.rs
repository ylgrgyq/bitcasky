use std::{
    fs::File,
    io::{ErrorKind, Read, Seek, SeekFrom},
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
    vec,
};

use bytes::{Buf, Bytes, BytesMut};
use crc::{Crc, CRC_32_CKSUM};

use crate::error::{BitcaskError, BitcaskResult};

use super::constants::{
    DATA_FILE_KEY_OFFSET, DATA_FILE_KEY_SIZE_OFFSET, DATA_FILE_VALUE_SIZE_OFFSET, KEY_SIZE_SIZE,
    VALUE_SIZE_SIZE,
};

pub trait Encoder<T> {
    fn encode(obj: T) -> Bytes;
}

pub trait Decoder<T> {
    fn decode(bytes: Bytes) -> T;
}

#[derive(Debug)]
pub struct RowToWrite<'a> {
    pub crc: u32,
    pub tstamp: u64,
    pub key_size: u64,
    pub value_size: u64,
    pub key: &'a Vec<u8>,
    pub value: &'a [u8],
    pub size: usize,
}

impl<'a> RowToWrite<'a> {
    pub fn new(key: &'a Vec<u8>, value: &'a [u8]) -> RowToWrite<'a> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;
        RowToWrite::new_with_timestamp(key, value, now)
    }

    pub fn new_with_timestamp(key: &'a Vec<u8>, value: &'a [u8], timestamp: u64) -> RowToWrite<'a> {
        let key_size = key.len() as u64;
        let value_size = value.len() as u64;
        let crc32 = Crc::<u32>::new(&CRC_32_CKSUM);
        let mut ck = crc32.digest();
        ck.update(&timestamp.to_be_bytes());
        ck.update(&key_size.to_be_bytes());
        ck.update(&value_size.to_be_bytes());
        ck.update(key);
        ck.update(value);
        RowToWrite {
            crc: ck.finalize(),
            tstamp: timestamp,
            key_size,
            value_size,
            key,
            value,
            size: DATA_FILE_KEY_OFFSET + key_size as usize + value_size as usize,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut bs = BytesMut::with_capacity(self.size);
        bs.extend_from_slice(&self.crc.to_be_bytes());
        bs.extend_from_slice(&self.tstamp.to_be_bytes());
        bs.extend_from_slice(&self.key_size.to_be_bytes());
        bs.extend_from_slice(&self.value_size.to_be_bytes());
        bs.extend_from_slice(self.key);
        bs.extend_from_slice(self.value);
        bs.freeze()
    }
}

pub fn io_error_to_bitcask_error(
    database_dir: &Path,
    file_id: u32,
    e: std::io::Error,
    hint: &str,
) -> BitcaskError {
    match e.kind() {
        ErrorKind::UnexpectedEof => {
            return BitcaskError::DataFileCorrupted(
                database_dir.display().to_string(),
                file_id,
                hint.into(),
            );
        }
        _ => BitcaskError::IoError(e),
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct RowPosition {
    pub file_id: u32,
    pub row_offset: u64,
    pub row_size: usize,
    pub timestamp: u64,
}

pub fn read_value_from_file(
    file_id: u32,
    data_file: &mut File,
    value_offset: u64,
    size: usize,
) -> BitcaskResult<Vec<u8>> {
    data_file.seek(SeekFrom::Start(value_offset))?;
    let mut buf = vec![0; size];
    data_file.read_exact(&mut buf)?;

    let bs = Bytes::from(buf);
    let expected_crc = bs.slice(0..4).get_u32();

    let crc32 = Crc::<u32>::new(&CRC_32_CKSUM);
    let mut ck = crc32.digest();
    ck.update(&bs.slice(4..));
    let actual_crc = ck.finalize();
    if expected_crc != actual_crc {
        return Err(BitcaskError::CrcCheckFailed(
            file_id,
            value_offset,
            expected_crc,
            actual_crc,
        ));
    }

    let key_size = bs
        .slice(DATA_FILE_KEY_SIZE_OFFSET..(DATA_FILE_KEY_SIZE_OFFSET + KEY_SIZE_SIZE))
        .get_u64() as usize;
    let val_size = bs
        .slice(DATA_FILE_VALUE_SIZE_OFFSET..(DATA_FILE_VALUE_SIZE_OFFSET + VALUE_SIZE_SIZE))
        .get_u64() as usize;
    let val_offset = DATA_FILE_KEY_OFFSET + key_size;
    let ret = bs.slice(val_offset..val_offset + val_size).into();
    Ok(ret)
}

pub trait BitcaskDataFile {
    fn read_value(&mut self, value_offset: u64, size: usize) -> BitcaskResult<Vec<u8>>;
}

#[derive(Debug)]
pub struct RowToRead {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub row_position: RowPosition,
}

pub struct RecoveredRow {
    pub file_id: u32,
    pub timestamp: u64,
    pub row_offset: u64,
    pub row_size: usize,
    pub key: Vec<u8>,
    pub is_tombstone: bool,
}
