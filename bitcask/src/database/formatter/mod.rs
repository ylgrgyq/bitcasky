mod v1;
use std::ops::Deref;

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
