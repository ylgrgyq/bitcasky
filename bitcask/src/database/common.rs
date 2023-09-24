use std::{
    fs::File,
    io::{ErrorKind, Read, Seek, SeekFrom},
    ops::Deref,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
    vec,
};

use bytes::{Buf, Bytes, BytesMut};
use crc::{Crc, CRC_32_CKSUM};

use crate::{
    error::{BitcaskError, BitcaskResult},
    file_id::FileId,
};

use super::constants::{
    DATA_FILE_KEY_OFFSET, DATA_FILE_KEY_SIZE_OFFSET, DATA_FILE_TSTAMP_OFFSET,
    DATA_FILE_VALUE_SIZE_OFFSET, KEY_SIZE_SIZE, VALUE_SIZE_SIZE,
};

pub trait Encoder<T> {
    fn encode(obj: T) -> Bytes;
}

pub trait Decoder<T> {
    fn decode(bytes: Bytes) -> T;
}

#[derive(Debug)]
pub struct RowToWrite<'a, V: Deref<Target = [u8]>> {
    pub crc: u32,
    pub timestamp: u64,
    pub key_size: u64,
    pub value_size: u64,
    pub key: &'a Vec<u8>,
    pub value: V,
    pub size: u64,
}

impl<'a, V: Deref<Target = [u8]>> RowToWrite<'a, V> {
    pub fn new(key: &'a Vec<u8>, value: V) -> RowToWrite<'a, V> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;
        RowToWrite::new_with_timestamp(key, value, now)
    }

    pub fn new_with_timestamp(key: &'a Vec<u8>, value: V, timestamp: u64) -> RowToWrite<'a, V> {
        let key_size = key.len() as u64;
        let value_size = value.len() as u64;
        let crc32 = Crc::<u32>::new(&CRC_32_CKSUM);
        let mut ck = crc32.digest();
        ck.update(&timestamp.to_be_bytes());
        ck.update(&key_size.to_be_bytes());
        ck.update(&value_size.to_be_bytes());
        ck.update(key);
        ck.update(&*value);
        RowToWrite {
            crc: ck.finalize(),
            timestamp,
            key_size,
            value_size,
            key,
            value,
            size: DATA_FILE_KEY_OFFSET as u64 + key_size + value_size,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut bs = BytesMut::with_capacity(self.size as usize);
        bs.extend_from_slice(&self.crc.to_be_bytes());
        bs.extend_from_slice(&self.timestamp.to_be_bytes());
        bs.extend_from_slice(&self.key_size.to_be_bytes());
        bs.extend_from_slice(&self.value_size.to_be_bytes());
        bs.extend_from_slice(self.key);
        bs.extend_from_slice(&self.value);
        bs.freeze()
    }
}

pub fn io_error_to_bitcask_error(
    database_dir: &Path,
    file_id: FileId,
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

pub fn read_value_from_file(
    file_id: FileId,
    data_file: &mut File,
    value_offset: u64,
    size: u64,
) -> BitcaskResult<TimedValue> {
    data_file.seek(SeekFrom::Start(value_offset))?;
    let mut buf = vec![0; size as usize];
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
    let timestamp = bs
        .slice(DATA_FILE_TSTAMP_OFFSET..DATA_FILE_KEY_SIZE_OFFSET)
        .get_u64();

    let key_size = bs
        .slice(DATA_FILE_KEY_SIZE_OFFSET..(DATA_FILE_KEY_SIZE_OFFSET + KEY_SIZE_SIZE))
        .get_u64() as usize;
    let val_size = bs
        .slice(DATA_FILE_VALUE_SIZE_OFFSET..(DATA_FILE_VALUE_SIZE_OFFSET + VALUE_SIZE_SIZE))
        .get_u64() as usize;
    let val_offset = DATA_FILE_KEY_OFFSET + key_size;
    let ret = bs.slice(val_offset..val_offset + val_size);

    Ok(TimedValue {
        value: Value::VectorBytes(ret.into()),
        timestamp,
    })
}

pub trait BitcaskDataFile {
    fn read_value(&mut self, value_offset: u64, size: usize) -> BitcaskResult<Vec<u8>>;
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct RowLocation {
    pub file_id: FileId,
    pub row_offset: u64,
    pub row_size: u64,
}

#[derive(Debug)]
pub enum Value {
    VectorBytes(Vec<u8>),
}

impl Deref for Value {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Value::VectorBytes(v) => v,
        }
    }
}

#[derive(Debug)]
pub struct TimedValue {
    pub value: Value,
    pub timestamp: u64,
}

impl Deref for TimedValue {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

#[derive(Debug)]
pub struct RowToRead {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub row_position: RowLocation,
    pub timestamp: u64,
}

pub struct RecoveredRow {
    pub file_id: FileId,
    pub timestamp: u64,
    pub row_offset: u64,
    pub row_size: u64,
    pub key: Vec<u8>,
    pub is_tombstone: bool,
}
