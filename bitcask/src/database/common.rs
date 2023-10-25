use std::{
    ops::Deref,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{error::BitcaskResult, storage_id::StorageId, utils::TOMBSTONE_VALUE};

#[derive(Debug)]
pub struct RowMeta {
    pub timestamp: u64,
    pub key_size: u64,
    pub value_size: u64,
}

pub struct RowHeader {
    pub crc: u32,
    pub meta: RowMeta,
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
}

pub trait BitcaskDataFile {
    fn read_value(&mut self, value_offset: u64, size: usize) -> BitcaskResult<Vec<u8>>;
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct RowLocation {
    pub storage_id: StorageId,
    pub row_offset: u64,
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
