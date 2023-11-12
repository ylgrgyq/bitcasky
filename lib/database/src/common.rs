use common::formatter::FormatterError;
use common::{storage_id::StorageId, tombstone::TOMBSTONE_VALUE};
use std::ops::Deref;
use thiserror::Error;

use crate::DataStorageError;

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

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Permission Denied: \"{0}\"")]
    PermissionDenied(String),
    #[error("Database is broken due to previos unrecoverable error: {0}.")]
    DatabaseBroken(String),
    #[error("Hint file with file id {1} under path {2} corrupted")]
    HintFileCorrupted(#[source] FormatterError, u32, String),
    #[error("Read non-existent file with id {0}")]
    TargetFileIdNotFound(u32),
    #[error(transparent)]
    StorageError(#[from] DataStorageError),
}

pub type DatabaseResult<T> = Result<T, DatabaseError>;
