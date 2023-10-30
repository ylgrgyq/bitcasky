mod v1;
use std::{fs::File, ops::Deref};

pub use self::v1::FormatterV1;

use bytes::Bytes;
use thiserror::Error;

use super::common::{RowHeader, RowMeta, RowToWrite};

#[derive(Error, Debug)]
#[error("{}")]
pub enum FormatterError {
    #[error("Crc check failed. expect crc is: {expected_crc}, actual crc is: {actual_crc}")]
    CrcCheckFailed { expected_crc: u32, actual_crc: u32 },
}

pub type Result<T> = std::result::Result<T, FormatterError>;

pub trait Formatter: std::marker::Send + 'static + Copy {
    fn header_size(&self) -> usize;

    fn row_size<V: Deref<Target = [u8]>>(&self, row: &RowToWrite<'_, V>) -> usize;

    fn encode_row<V: Deref<Target = [u8]>>(&self, row: &RowToWrite<'_, V>) -> Bytes;

    fn decode_row_meta(&self, buf: Bytes) -> Result<RowMeta>;

    fn decode_row_header(&self, bs: Bytes) -> Result<RowHeader>;

    fn validate_key_value(&self, header: &RowHeader, kv: &Bytes) -> Result<()>;
}

#[derive(Clone, Copy, Debug)]
pub enum DataStorageFormatter {
    V1(FormatterV1),
}

impl Formatter for DataStorageFormatter {
    fn header_size(&self) -> usize {
        match self {
            DataStorageFormatter::V1(f) => f.header_size(),
        }
    }

    fn row_size<V: Deref<Target = [u8]>>(&self, row: &RowToWrite<'_, V>) -> usize {
        match self {
            DataStorageFormatter::V1(f) => f.row_size(row),
        }
    }

    fn encode_row<V: Deref<Target = [u8]>>(&self, row: &RowToWrite<'_, V>) -> Bytes {
        match self {
            DataStorageFormatter::V1(f) => f.encode_row(row),
        }
    }

    fn decode_row_meta(&self, buf: Bytes) -> Result<RowMeta> {
        match self {
            DataStorageFormatter::V1(f) => f.decode_row_meta(buf),
        }
    }

    fn decode_row_header(&self, bs: Bytes) -> Result<RowHeader> {
        match self {
            DataStorageFormatter::V1(f) => f.decode_row_header(bs),
        }
    }

    fn validate_key_value(&self, header: &RowHeader, kv: &Bytes) -> Result<()> {
        match self {
            DataStorageFormatter::V1(f) => f.validate_key_value(header, kv),
        }
    }
}

pub fn initialize_new_file(file: File) -> File {
    file
}

pub fn get_formatter_from_file(file: &File) -> DataStorageFormatter {
    DataStorageFormatter::V1(FormatterV1::new())
}
