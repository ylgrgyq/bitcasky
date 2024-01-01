use bitcasky_common::formatter::FormatterError;
use bitcasky_common::tombstone::is_tombstone;
use bitcasky_common::{storage_id::StorageId, tombstone::TOMBSTONE_VALUE};
use std::ops::Deref;
use thiserror::Error;

use crate::DataStorageError;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct RowLocation {
    pub storage_id: StorageId,
    pub row_offset: usize,
}

#[derive(Debug)]
pub struct TimedValue<V: Deref<Target = [u8]>> {
    pub value: V,
    pub expire_timestamp: u64,
}

impl<V: Deref<Target = [u8]>> TimedValue<V> {
    pub fn is_valid(&self, now: u64) -> bool {
        if is_tombstone(&self.value) {
            return false;
        }

        self.expire_timestamp == 0 || self.expire_timestamp > now
    }

    pub fn validate(self) -> Option<TimedValue<V>> {
        if !is_tombstone(&self.value) {
            Some(self)
        } else {
            None
        }
    }
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
            expire_timestamp: 0,
        }
    }

    pub fn expirable_value(value: V, expire_timestamp: u64) -> TimedValue<V> {
        TimedValue {
            value,
            expire_timestamp,
        }
    }
}

#[derive(Debug)]
pub struct RowToRead {
    pub key: Vec<u8>,
    pub row_location: RowLocation,
    pub value: TimedValue<Vec<u8>>,
}

pub struct RecoveredRow {
    pub row_location: RowLocation,
    pub key: Vec<u8>,
    pub invalid: bool,
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
