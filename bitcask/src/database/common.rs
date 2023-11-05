use std::ops::Deref;

use crate::{storage_id::StorageId, utils::TOMBSTONE_VALUE};

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
