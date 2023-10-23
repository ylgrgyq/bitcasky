use std::{
    ops::Deref,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use crc::{Crc, CRC_32_CKSUM};

use crate::{error::BitcaskResult, storage_id::StorageId, utils::TOMBSTONE_VALUE};

use super::constants::DATA_FILE_KEY_OFFSET;

#[derive(Debug)]
pub struct RowMeta {
    pub timestamp: u64,
    pub key_size: u64,
    pub value_size: u64,
}

#[derive(Debug)]
pub struct RowToWrite<'a, V: Deref<Target = [u8]>> {
    pub meta: RowMeta,
    pub key: &'a Vec<u8>,
    pub value: V,
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
        RowToWrite {
            meta: RowMeta {
                timestamp,
                key_size,
                value_size,
            },
            key,
            value,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let crc32 = Crc::<u32>::new(&CRC_32_CKSUM);
        let mut ck = crc32.digest();
        ck.update(&self.meta.timestamp.to_be_bytes());
        ck.update(&self.meta.key_size.to_be_bytes());
        ck.update(&self.meta.value_size.to_be_bytes());
        ck.update(self.key);
        ck.update(&self.value);
        let crc = ck.finalize();

        let size = DATA_FILE_KEY_OFFSET + self.key.len() + self.value.len();
        let mut bs = BytesMut::with_capacity(size);
        bs.extend_from_slice(&crc.to_be_bytes());
        bs.extend_from_slice(&self.meta.timestamp.to_be_bytes());
        bs.extend_from_slice(&self.meta.key_size.to_be_bytes());
        bs.extend_from_slice(&self.meta.value_size.to_be_bytes());
        bs.extend_from_slice(self.key);
        bs.extend_from_slice(&self.value);
        bs.freeze()
    }
}

pub trait BitcaskDataFile {
    fn read_value(&mut self, value_offset: u64, size: usize) -> BitcaskResult<Vec<u8>>;
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct RowLocation {
    pub storage_id: StorageId,
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
pub struct TimedValue<V: Deref<Target = [u8]>> {
    pub value: V,
    pub timestamp: u64,
}

impl<V: Deref<Target = [u8]>> Deref for TimedValue<V> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

pub fn deleted_value() -> TimedValue<Vec<u8>> {
    TimedValue::immortal_value(TOMBSTONE_VALUE.as_bytes().to_vec())
}

impl<V: Deref<Target = [u8]>> TimedValue<V> {
    pub fn immortal_value(value: V) -> TimedValue<V> {
        TimedValue {
            value,
            timestamp: 0,
        }
    }

    pub fn has_time_value(value: V, timestamp: u64) -> TimedValue<V> {
        TimedValue { value, timestamp }
    }
}

#[derive(Debug)]
pub struct RowToRead {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub row_location: RowLocation,
    pub timestamp: u64,
}

pub struct RecoveredRow {
    pub row_location: RowLocation,
    pub timestamp: u64,
    pub key: Vec<u8>,
    pub is_tombstone: bool,
}
